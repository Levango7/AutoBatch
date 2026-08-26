"""S3 / Parquet 路由与 IO 工具。

从 helpers.py 拆分出来，降低单文件耦合度。
向后兼容：原 helpers.py 仍 re-export 所有符号。
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def s3_credentials(cfg: dict[str, Any]) -> tuple[str, str]:
    """解析 S3/MinIO 凭证，返回 (access_key, secret_key).

    优先级：storage.access_key/secret_key 显式配置 > 环境变量 > 空串。
    环境变量依次尝试 MINIO_ROOT_USER / MINIO_ROOT_PASSWORD（本地 MinIO
    惯例，与 docker/iceberg/docker-compose.yml 注入的变量一致）和
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY（AWS 通用惯例）。凭证经
    环境注入即可跑通 S3 路径，避免 secret 写进配置文件进版本库.
    """
    storage = cfg.get("storage", {}) or {}
    access = storage.get("access_key", "") or ""
    secret = storage.get("secret_key", "") or ""
    if access and secret:
        return access, secret
    env_access = os.environ.get("MINIO_ROOT_USER") or os.environ.get("AWS_ACCESS_KEY_ID") or ""
    env_secret = (
        os.environ.get("MINIO_ROOT_PASSWORD") or os.environ.get("AWS_SECRET_ACCESS_KEY") or ""
    )
    return access or env_access, secret or env_secret


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
    """判断 path 在 storage.backend="parquet" 下应走 S3 还是本地.

    判断规则（按优先级，与初版 helpers.py 语义一致——bbf94ee 拆分重构时
    误丢了"配置判定"规则导致 cluster 多机模式把产物写往宿主机本地路径，
    Worker 容器无法访问该路径而任务失败，2026-08-26 恢复）：
      1. path 以 "s3://" 或 "s3a://" 开头 → S3
      2. path 指向本地已存在的文件/目录 → 本地（允许 storage.backend="parquet"
         时读写本地 .parquet，便于单测与无 MinIO 环境的降级）
      3. cfg["storage"] 配了 bucket + endpoint → S3（path 是逻辑/绝对路径，
         用 _resolve_s3_path 解析为 s3a:// URI）
      4. 其余 → 本地

    Args:
        path: 数据文件路径（逻辑路径、本地路径或 s3:// / s3a:// URI）.
        cfg: Pipeline 配置 dict.

    Returns:
        True 走 S3，False 走本地.
    """
    if path.startswith("s3://") or path.startswith("s3a://"):
        return True
    if os.path.exists(path):
        return False
    storage = cfg.get("storage", {})
    return bool(storage.get("bucket") and storage.get("endpoint"))


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
    """创建 pyarrow.fs.S3FileSystem（lazy import）.

    注意参数名是 ``endpoint_override``（pyarrow 24 实测）：bbf94ee 拆分重构时
    误写成 ``endpoint`` 导致 TypeError: __init__() got an unexpected keyword
    argument 'endpoint'，2026-08-26 恢复初版语义。
    """
    import pyarrow.fs as fs  # lazy import

    storage = cfg.get("storage", {})
    endpoint = storage.get("endpoint", "localhost:9000")
    access_key, secret_key = s3_credentials(cfg)
    secure = storage.get("secure", False)
    region = storage.get("region", "us-east-1")
    # endpoint_override 不带 scheme（pyarrow 用 scheme 参数区分 http/https）
    endpoint_override = endpoint.replace("http://", "").replace("https://", "")
    return fs.S3FileSystem(
        endpoint_override=endpoint_override,
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
    access_key, secret_key = s3_credentials(cfg)
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
        import polars as pl  # lazy import

        # 兼容 List[Dict] 输入（ingest 的 parquet 分支传 rows，python 路径
        # 用 pyarrow 处理 list，polars 路径需显式转 DataFrame 再写）。
        if not hasattr(df_or_rows, "write_parquet"):
            fields = fields or (list(df_or_rows[0].keys()) if df_or_rows else [])
            df_or_rows = pl.DataFrame(df_or_rows, schema=fields, orient="row")
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


def apply_s3a_hadoop_conf(builder: Any, cfg: dict[str, Any]) -> Any:
    """把 fs.s3a.* Hadoop 配置注入 SparkSession.builder（parquet+S3 场景）.

    与 pipeline._init_spark_session 的内联注入保持同一组键值；供
    helpers._get_spark_session 在 session 重建场景（如 run_pipeline 结束
    spark.stop() 之后，测试层 table_read 触发的惰性重建）复用——否则新
    session 及其 executor 无凭证读 s3a://，报 NoAuthWithAWSException。

    endpoint 选择规则与 pipeline 一致：cluster.enabled 且配置了
    cluster.s3_endpoint 时优先（Worker 经容器内 socat 用 localhost:9000），
    否则回退 storage.endpoint（Driver 端直连地址）。
    非 parquet+S3 场景原样返回 builder，零影响。
    """
    storage = cfg.get("storage", {})
    if storage.get("backend") != "parquet" or not storage.get("endpoint"):
        return builder
    cluster = (cfg.get("engine", {}).get("spark", {}) or {}).get("cluster", {}) or {}
    if cluster.get("enabled") and cluster.get("s3_endpoint"):
        s3a_endpoint = cluster["s3_endpoint"]
    else:
        s3a_endpoint = storage["endpoint"]
    # 去掉 scheme 前缀（fs.s3a.endpoint 需要 scheme 前缀，见下）
    s3a_endpoint_clean = str(s3a_endpoint).replace("http://", "").replace("https://", "")
    scheme = "https" if storage.get("secure") else "http"
    access, secret = s3_credentials(cfg)
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"{scheme}://{s3a_endpoint_clean}")
    builder = builder.config("spark.hadoop.fs.s3a.access.key", access)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    # Windows Driver 端缺 hadoop.dll 时，S3A 默认 disk buffer 会触发
    # NativeIO$Windows.access0 → UnsatisfiedLinkError。改用内存 buffer 避免
    # 创建本地临时文件（Worker 在 Linux 容器中不受影响，内存 buffer 也可用）。
    builder = builder.config("spark.hadoop.fs.s3a.fast.upload", "true")
    builder = builder.config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
    # FileOutputCommitter v2 避免 commitJob 时 list _temporary/0（S3 eventual
    # consistency 可能导致 list 不到刚写入的 task 输出）。
    builder = builder.config(
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
    )
    return builder


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
    "apply_s3a_hadoop_conf",
]
