"""S3 / Parquet 路由与 IO 工具。

从 helpers.py 拆分出来，降低单文件耦合度。
向后兼容：原 helpers.py 仍 re-export 所有符号。
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def _get_storage_backend(cfg: dict[str, Any]) -> str:
    """从 cfg 读 storage.backend，缺省 'local_csv'."""
    return cfg.get("storage", {}).get("backend", "local_csv")


def _resolve_s3_path(path: str, cfg: dict[str, Any], scheme: str = "s3a") -> str:
    """把逻辑路径解析为 S3 URI.

    参见 docs/evolution.md §4.3.1.2.
    """
    if path.startswith("s3a://"):
        return path
    if path.startswith("s3://"):
        return scheme + "://" + path[len("s3://") :]
    storage = cfg.get("storage", {})
    bucket = storage.get("bucket", "autobatch")
    prefix = storage.get("prefix", "").strip("/")
    warehouse = storage.get("warehouse", "warehouse").strip("/")
    rel = path.lstrip("/")
    if os.path.isabs(path):
        run_root = cfg.get("pipeline", {}).get("run_dir", "")
        if run_root:
            try:
                rel = os.path.relpath(path, run_root).replace(os.sep, "/")
            except ValueError:
                rel = os.path.basename(path)
        if ".." in rel.split("/"):
            drive, tail = os.path.splitdrive(path)
            safe_drive = drive.replace(":", "_").replace(os.sep, "/").strip("/")
            safe_tail = tail.replace(os.sep, "/").strip("/")
            rel = (safe_drive + "/" + safe_tail) if safe_drive else safe_tail
    if not rel.endswith(".parquet"):
        rel = rel + ".parquet"
    parts = [p for p in (prefix, warehouse, rel) if p]
    return f"{scheme}://{bucket}/" + "/".join(parts)


def _is_s3_target(path: str, cfg: dict[str, Any]) -> bool:
    """path 是 S3 URI 或逻辑路径（无盘符、无本地分隔符）时返回 True."""
    if path.startswith("s3a://") or path.startswith("s3://"):
        return True
    if os.path.isabs(path):
        return False
    return "/" not in path and "\\" not in path


def _s3_uri_to_bucket_key(s3_uri: str) -> str:
    """s3a://bucket/warehouse/... → warehouse/..."""
    if s3_uri.startswith("s3a://"):
        return s3_uri[len("s3a://") :]
    if s3_uri.startswith("s3://"):
        return s3_uri[len("s3://") :]
    return s3_uri


def _get_parquet_compression(cfg: dict[str, Any]) -> str:
    """优先读 storage.compression（顶层简化字段），回退到
    storage.parquet.compression（设计文档 §5.5.3 的嵌套字段）。"""
    storage = cfg.get("storage", {})
    if "compression" in storage:
        return storage["compression"]
    return storage.get("parquet", {}).get("compression", "zstd")


def _get_s3_filesystem(cfg: dict[str, Any]) -> Any:
    """创建 pyarrow.fs.S3FileSystem（lazy import）."""
    import pyarrow.fs as fs  # lazy import

    storage = cfg.get("storage", {})
    endpoint = storage.get("endpoint", "localhost:9000")
    access_key = storage.get("access_key", "")
    secret_key = storage.get("secret_key", "")
    secure = storage.get("secure", False)
    region = storage.get("region", "us-east-1")
    return fs.S3FileSystem(
        endpoint=endpoint,
        access_key=access_key or None,
        secret_key=secret_key or None,
        scheme="https" if secure else "http",
        region=region,
    )


def _build_polars_s3_options(cfg: dict[str, Any]) -> dict[str, Any]:
    """构建 polars read_parquet / write_parquet 的 S3 storage_options.

    polars 需要单独的 endpoint_url/access_key/secret_key 参数格式.
    """
    storage = cfg.get("storage", {})
    endpoint = storage.get("endpoint", "localhost:9000")
    access_key = storage.get("access_key", "")
    secret_key = storage.get("secret_key", "")
    secure = storage.get("secure", False)
    opts: dict[str, Any] = {"endpoint_url": f"http{'s' if secure else ''}://{endpoint}"}
    if access_key:
        opts["access_key_id"] = access_key
    if secret_key:
        opts["secret_access_key"] = secret_key
    return opts


def _table_exists(path: str, cfg: dict[str, Any]) -> bool:
    """检查 table 文件是否存在，兼容 local_csv / 本地 parquet / S3 parquet."""
    if _get_storage_backend(cfg) != "parquet":
        return os.path.exists(path)
    if _is_s3_target(path, cfg):
        import pyarrow.fs as fs  # lazy import

        target = _resolve_s3_path(path, cfg)
        s3fs = _get_s3_filesystem(cfg)
        try:
            info = s3fs.get_file_info(_s3_uri_to_bucket_key(target))
        except (FileNotFoundError, OSError):
            return False
        return info.type in (fs.FileType.File, fs.FileType.Directory)
    return os.path.exists(path) or os.path.exists(path + ".parquet")


def _table_read_parquet(
    path: str,
    cfg: dict[str, Any],
    engine_backend: str,
    spark: Any = None,
) -> Any:
    """storage.backend='parquet' 时的读路径."""
    is_s3 = _is_s3_target(path, cfg)
    if is_s3:
        target = _resolve_s3_path(path, cfg)
    else:
        target = path if path.endswith(".parquet") else path + ".parquet"

    if engine_backend == "spark":
        from ..helpers import _get_spark_session  # noqa: PLC0415  lazy import

        if spark is None:
            spark = _get_spark_session(cfg)
        return spark.read.parquet(target)
    elif engine_backend == "polars":
        import polars as pl  # lazy import

        if is_s3:
            opts = _build_polars_s3_options(cfg)
            return pl.read_parquet(target, storage_options=opts)
        else:
            return pl.read_parquet(target)
    else:
        import pyarrow.parquet as pq  # lazy import

        if is_s3:
            s3fs = _get_s3_filesystem(cfg)
            table = pq.read_table(_s3_uri_to_bucket_key(target), filesystem=s3fs)
        else:
            table = pq.read_table(target)
        return table.to_pylist(), table.column_names


def _table_write_parquet(
    path: str,
    df_or_rows: Any,
    cfg: dict[str, Any],
    engine_backend: str,
    fields=None,
    spark: Any = None,
) -> int:
    """storage.backend='parquet' 时的写路径."""
    is_s3 = _is_s3_target(path, cfg)
    if is_s3:
        target = _resolve_s3_path(path, cfg)
    else:
        target = path if path.endswith(".parquet") else path + ".parquet"
        os.makedirs(os.path.dirname(target), exist_ok=True)

    compression = _get_parquet_compression(cfg)

    if engine_backend == "spark":
        from ..helpers import _get_spark_session  # noqa: PLC0415  lazy import

        if spark is None:
            spark = _get_spark_session(cfg)
        n = df_or_rows.count()
        df_or_rows.write.mode("overwrite").parquet(target)
        return n
    elif engine_backend == "polars":
        if is_s3:
            opts = _build_polars_s3_options(cfg)
            df_or_rows.write_parquet(target, compression=compression, storage_options=opts)
        else:
            df_or_rows.write_parquet(target, compression=compression)
        return df_or_rows.height
    else:
        import pyarrow as pa  # lazy import
        import pyarrow.parquet as pq  # lazy import

        rows = df_or_rows
        if fields is None:
            fields = list(rows[0].keys()) if rows else []
        str_rows = [
            {f: (str(r.get(f)) if r.get(f) is not None else "") for f in fields} for r in rows
        ]
        schema = pa.schema([(f, pa.string()) for f in fields])
        table = pa.Table.from_pylist(str_rows, schema=schema)
        if is_s3:
            s3fs = _get_s3_filesystem(cfg)
            pq.write_table(
                table, _s3_uri_to_bucket_key(target), filesystem=s3fs, compression=compression
            )
        else:
            pq.write_table(table, target, compression=compression)
        return len(rows)


# 向后兼容别名（保持 helpers.py 的公共 API 不变）
__all__ = [
    "_get_storage_backend",
    "_resolve_s3_path",
    "_is_s3_target",
    "_s3_uri_to_bucket_key",
    "_get_parquet_compression",
    "_get_s3_filesystem",
    "_build_polars_s3_options",
    "_table_exists",
    "_table_read_parquet",
    "_table_write_parquet",
]
