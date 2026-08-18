"""Shared helpers: timestamps, batch ids, paths, csv/json io, hashing, stage logging."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import shutil
import threading
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:  # avoid runtime circular import: lineage imports helpers
    from .lineage import Manifest

VERSION = "1.0.0"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def local_ts_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def batch_id_new(prefix: str = "B") -> str:
    return "{}-{}-{}".format(
        prefix,
        datetime.now().strftime("%Y%m%d-%H%M%S"),
        uuid.uuid4().hex[:6].upper(),
    )


def abs_path(p: str) -> str:
    if os.path.isabs(p):
        return p
    return os.path.join(ROOT, p)


def sha256_of(path: str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def csv_lines(path: str) -> int:
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def csv_read(path: str) -> tuple[list[dict[str, str]], list[str]]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames or [])
        data = list(reader)
    return data, fields


def csv_write(path: str, fields: Sequence[str], data: Sequence[dict[str, Any]]) -> int:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    return len(data)


# ---------------------------------------------------------------------------
# Phase 2a/2b: 统一 IO 路由层（engine.backend 调度）
# ---------------------------------------------------------------------------
# 设计参见 docs/evolution.md §4.3.1.1-4.3.1.4（Polars）与 §4.3.2.1-4.3.2.3（Spark）。
# backend="python" 时行为与 csv_read/csv_write 完全一致（向后兼容）；
# backend="polars" 时返回/接受 polars.DataFrame，列式加速；
# backend="spark"  时返回/接受 Spark DataFrame，分布式跨 executor 分区。
# polars / pyspark 均采用 lazy import：仅对应 backend 路径才 import，保持
# backend="python" 路径零额外依赖。


def _get_engine_backend(cfg: dict[str, Any]) -> str:
    """从 cfg 读 engine.backend，缺省 'python'.

    Args:
        cfg: Pipeline 配置 dict（pipeline.json 顶层）。

    Returns:
        "python"、"polars" 或 "spark"。
    """
    return cfg.get("engine", {}).get("backend", "python")


def _get_spark_session(cfg: dict[str, Any]) -> Any:
    """按 cfg["engine"]["spark"] 创建或复用 SparkSession（lazy import pyspark）。

    仅在 ``backend="spark"`` 且调用方未显式传入 spark session 时使用。
    推荐做法：``pipeline.py`` 在初始化时调用本函数创建 SparkSession 并存入
    ``ctx.spark_session``，各 stage 通过 ``ctx.spark_session`` 访问，避免重复
    创建（``getOrCreate`` 内部会复用同一进程的活跃 session）。

    读取的配置项（参见 docs/evolution.md §4.3.2.3）：
        master:                    "local[*]" 缺省
        app_name:                  "autobatch" 缺省
        executor_memory / cores:   executor 资源
        num_executors:             executor 实例数（spark.executor.instances）
        driver_memory:             driver 堆内存
        shuffle_partitions:        spark.sql.shuffle.partitions
        adaptive_query_execution:  AQE 开关（缺省 True）

    Args:
        cfg: Pipeline 配置 dict.

    Returns:
        pyspark.sql.SparkSession.
    """
    from pyspark.sql import SparkSession  # lazy import：仅 spark 路径需要

    spark_cfg = cfg.get("engine", {}).get("spark", {}) or {}
    builder = SparkSession.builder
    builder = _apply_spark_base_config(builder, spark_cfg)
    return builder.getOrCreate()


def _apply_spark_base_config(builder: Any, scfg: dict[str, Any]) -> Any:
    """把 Spark 基础配置（appName/master/资源/AQE）应用到 builder 并返回。

    抽自 ``_get_spark_session`` 与 ``pipeline._init_spark_session`` 的公共部分，
    避免两处重复维护同一组配置项。调用方在调用本函数后仍可继续追加 S3/Iceberg/
    cluster 等场景化配置（``builder.config(...)``），最后 ``builder.getOrCreate()``。

    读取的配置项（参见 docs/evolution.md §4.3.2.3）：
        app_name:                  "autobatch" 缺省
        master:                    "local[*]" 缺省
        executor_memory / cores:   executor 资源
        num_executors:             executor 实例数（spark.executor.instances）
        driver_memory:             driver 堆内存
        shuffle_partitions:        spark.sql.shuffle.partitions
        adaptive_query_execution:  AQE 开关（缺省 True）

    Args:
        builder: ``SparkSession.Builder`` 实例（已由调用方创建）。
        scfg:    ``cfg["engine"]["spark"]`` 子段（缺省空 dict 走全部缺省值）。

    Returns:
        应用了基础配置的 builder（链式 API 同一实例）。
    """
    builder = builder.appName(scfg.get("app_name", "autobatch"))
    builder = builder.master(scfg.get("master", "local[*]"))
    if scfg.get("executor_memory"):
        builder = builder.config("spark.executor.memory", scfg["executor_memory"])
    if scfg.get("executor_cores") is not None:
        builder = builder.config("spark.executor.cores", scfg["executor_cores"])
    if scfg.get("num_executors") is not None:
        builder = builder.config("spark.executor.instances", scfg["num_executors"])
    if scfg.get("driver_memory"):
        builder = builder.config("spark.driver.memory", scfg["driver_memory"])
    if scfg.get("shuffle_partitions") is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions", scfg["shuffle_partitions"]
        )
    # AQE 缺省开启，自动合并小分区、处理倾斜
    aqe = scfg.get("adaptive_query_execution", True)
    builder = builder.config("spark.sql.adaptiveQueryExecution", "true" if aqe else "false")
    return builder


# ---------------------------------------------------------------------------
# Phase 3: 正交的 storage.backend 维度（local_csv / parquet）
# ---------------------------------------------------------------------------
# 设计参见 docs/evolution.md §5.5.1-5.5.4。storage.backend 决定**存储介质**，
# engine.backend 决定**计算引擎**，两者解耦正交组合：
#   storage.backend="local_csv"（缺省）→ 走 engine.backend 路由（python/polars/spark 读 CSV）
#   storage.backend="parquet"           → pyarrow/polars/spark 读 Parquet（本地或 S3/MinIO）
# pyarrow / minio 均采用 lazy import：仅 storage.backend="parquet" 路径才 import，
# 保持 storage.backend="local_csv" 路径零额外依赖。


def _get_storage_backend(cfg: dict[str, Any]) -> str:
    """从 cfg 读 storage.backend，缺省 'local_csv'.

    Args:
        cfg: Pipeline 配置 dict（pipeline.json 顶层）.

    Returns:
        "local_csv"（本地 CSV，向后兼容）/ "parquet"（S3 或本地 Parquet）.
    """
    return cfg.get("storage", {}).get("backend", "local_csv")


def _resolve_s3_path(path: str, cfg: dict[str, Any], scheme: str = "s3a") -> str:
    """把逻辑路径解析为 S3 URI.

    输入 path 形式：
      - "orders/orders_clean"     → s3a://bucket/warehouse/orders/orders_clean.parquet
      - "s3://bucket/xxx.parquet" → s3a://bucket/xxx.parquet（统一转为 s3a）
      - "s3a://bucket/xxx.parquet" → 原样返回
      - 绝对路径（如 F:\\...\\run\\<batch>\\01_raw\\orders.csv）
        → 取相对于 cfg["pipeline"]["run_dir"] 的相对路径作为 rel，
          避免 S3 key 中混入 Windows 绝对路径（Spark FileOutputCommitter
          对路径敏感，含盘符/冒号的 key 会导致 commitJob 时 listStatus 失败）

    读取 cfg["storage"] 的 bucket / warehouse / prefix 配置拼接.

    Args:
        path: 逻辑路径、本地绝对路径或完整 S3 URI.
        cfg: Pipeline 配置 dict.
        scheme: S3 URI scheme，缺省 "s3a"（Spark hadoop-aws 使用 s3a://）。
                pyarrow 使用 s3://，但 pyarrow 不走此函数（用 _get_s3_filesystem）。

    Returns:
        完整 S3 URI（s3a://bucket/.../xxx.parquet）.
    """
    if path.startswith("s3a://"):
        return path
    if path.startswith("s3://"):
        # 统一转为 s3a://（Spark hadoop-aws 需要 s3a scheme）
        return scheme + "://" + path[len("s3://"):]
    storage = cfg.get("storage", {})
    bucket = storage.get("bucket", "autobatch")
    prefix = storage.get("prefix", "").strip("/")
    warehouse = storage.get("warehouse", "warehouse").strip("/")
    rel = path.lstrip("/")
    # 当 path 是绝对路径（Windows 盘符或 Unix /）时，取相对于
    # cfg["pipeline"]["run_dir"] 的相对路径，避免 S3 key 中混入本地绝对路径。
    # Spark FileOutputCommitter 在 commitJob 时 listStatus _temporary/0，
    # S3 key 含盘符（如 F:）会触发 FileNotFoundException。
    if os.path.isabs(path):
        run_root = cfg.get("pipeline", {}).get("run_dir", "")
        if run_root:
            try:
                rel = os.path.relpath(path, run_root).replace(os.sep, "/")
            except ValueError:
                # Windows 不同盘符无法 relpath，回退到 basename
                rel = os.path.basename(path)
        # 当 path 不在 run_root 下时，relpath 会产生含 ".." 的路径
        # （如 ../../<work_dir>/state/aggregates/daily_sales.csv）。
        # S3 不接受含 ".." 的 key（BadRequest），且 ".." 会跳出 prefix
        # 目录导致 cleanup 按前缀清理失效。此时用绝对路径的安全编码
        # （盘符冒号替换为下划线，分隔符统一为 /）作为 rel，确保 S3 key
        # 唯一且仍在 prefix/warehouse/ 下。典型场景：cluster+S3 模式下
        # state/aggregates/ 下的历史聚合产物（state_dir 不在 run_root 下）。
        if ".." in rel.split("/"):
            drive, tail = os.path.splitdrive(path)
            safe_drive = drive.replace(":", "_").replace(os.sep, "/").strip("/")
            safe_tail = tail.replace(os.sep, "/").strip("/")
            rel = (safe_drive + "/" + safe_tail) if safe_drive else safe_tail
    if not rel.endswith(".parquet"):
        rel = rel + ".parquet"
    parts = [p for p in (prefix, warehouse, rel) if p]
    return f"{scheme}://{bucket}/" + "/".join(parts)


def _get_s3_filesystem(cfg: dict[str, Any]) -> Any:
    """创建 pyarrow.fs.S3FileSystem（lazy import pyarrow.fs）.

    从 cfg["storage"] 读 endpoint / access_key / secret_key / secure / region.
    用于 pyarrow.parquet.read_table / write_table 的 filesystem 参数，连接
    MinIO 或任意 S3 兼容对象存储。

    Args:
        cfg: Pipeline 配置 dict，读 storage 段.

    Returns:
        pyarrow.fs.S3FileSystem 实例.
    """
    import pyarrow.fs as fs  # lazy import：仅 parquet+s3 路径需要

    storage = cfg.get("storage", {})
    endpoint = storage.get("endpoint", "localhost:9000")
    # endpoint_override 不带 scheme（pyarrow 用 scheme 参数区分 http/https）
    endpoint_override = endpoint.replace("http://", "").replace("https://", "")
    return fs.S3FileSystem(
        endpoint_override=endpoint_override,
        access_key=storage.get("access_key", "minioadmin"),
        secret_key=storage.get("secret_key", "minioadmin"),
        region=storage.get("region", "us-east-1"),
        scheme="https" if storage.get("secure", False) else "http",
    )


def _build_polars_s3_options(cfg: dict[str, Any]) -> dict[str, Any]:
    """构造 Polars read_parquet/write_parquet 的 storage_options（S3 协议）.

    Polars 通过 storage_options 把 S3 凭证传给底层 fsspec/aiobotocore.

    Args:
        cfg: Pipeline 配置 dict，读 storage 段.

    Returns:
        storage_options dict（aws_access_key_id / aws_secret_access_key /
        endpoint_url / region）.
    """
    s = cfg.get("storage", {})
    endpoint = s.get("endpoint", "localhost:9000")
    # Polars/fsspec 需要 endpoint_url 带 scheme
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        scheme = "https" if s.get("secure", False) else "http"
        endpoint_url = f"{scheme}://{endpoint}"
    else:
        endpoint_url = endpoint
    return {
        "aws_access_key_id": s.get("access_key", "minioadmin"),
        "aws_secret_access_key": s.get("secret_key", "minioadmin"),
        "endpoint_url": endpoint_url,
        "region": s.get("region", "us-east-1"),
    }


def _is_s3_target(path: str, cfg: dict[str, Any]) -> bool:
    """判断 path 在 storage.backend="parquet" 下应走 S3 还是本地.

    判断规则（按优先级）：
      1. path 以 "s3://" 或 "s3a://" 开头 → S3
      2. path 指向本地已存在的文件 → 本地（允许 storage.backend="parquet"
         时读写本地 .parquet，便于单测与无 MinIO 环境的降级）
      3. cfg["storage"] 配了 bucket + endpoint → S3（path 是逻辑路径，
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
    """把 s3a:// 或 s3://bucket/key URI 转为 pyarrow.fs.S3FileSystem 期望的 bucket/key 形式.

    pyarrow.fs.S3FileSystem 的 filesystem 参数不接受 ``s3://`` 或 ``s3a://`` 前缀，
    期望 ``bucket/key`` 相对路径。Polars/Spark 路径仍用完整 ``s3a://`` URI。

    Args:
        s3_uri: 完整 S3 URI（s3a://bucket/key 或 s3://bucket/key）.

    Returns:
        bucket/key 形式（去掉 s3a:// 或 s3:// 前缀）.
    """
    if s3_uri.startswith("s3a://"):
        return s3_uri[len("s3a://"):]
    if s3_uri.startswith("s3://"):
        return s3_uri[len("s3://"):]
    return s3_uri


def _get_parquet_compression(cfg: dict[str, Any]) -> str:
    """从 cfg 读 Parquet 压缩算法，缺省 'zstd'.

    优先读 storage.compression（顶层简化字段），回退到
    storage.parquet.compression（设计文档 §5.5.3 的嵌套字段）.

    Args:
        cfg: Pipeline 配置 dict.

    Returns:
        压缩算法名（"zstd" / "snappy" / "gzip" / "none"）.
    """
    storage = cfg.get("storage", {})
    if "compression" in storage:
        return storage["compression"]
    return storage.get("parquet", {}).get("compression", "zstd")


def _table_exists(path: str, cfg: dict[str, Any]) -> bool:
    """检查 table 文件是否存在，兼容 local_csv / 本地 parquet / S3 parquet.

    storage.backend="local_csv"（缺省）→ os.path.exists(path)
    storage.backend="parquet" + 本地  → os.path.exists(path) or os.path.exists(path + ".parquet")
    storage.backend="parquet" + S3    → pyarrow.fs.S3FileSystem.get_file_info 检查

    Args:
        path: 数据文件路径（逻辑路径、本地路径或 s3:// URI）.
        cfg: Pipeline 配置 dict.

    Returns:
        True 若文件存在.
    """
    if _get_storage_backend(cfg) != "parquet":
        return os.path.exists(path)
    # parquet 模式
    if _is_s3_target(path, cfg):
        import pyarrow.fs as fs  # lazy import
        target = _resolve_s3_path(path, cfg)
        s3fs = _get_s3_filesystem(cfg)
        try:
            info = s3fs.get_file_info(_s3_uri_to_bucket_key(target))
        except (FileNotFoundError, OSError):
            # S3 NoSuchKey / NoSuchBucket / 本地文件不存在 → 视为表不存在
            return False
        # 其他异常（S3 权限错误、网络错误、凭证失效等）向上传播，避免
        # 把"无法访问"误判为"表不存在"导致后续写入覆盖远端数据。
        # Spark write.parquet 写出目录（含 part-00000-*.parquet），
        # pyarrow write_table 写单个 .parquet 文件，两者都应识别为存在。
        return info.type in (fs.FileType.File, fs.FileType.Directory)
    # 本地 parquet：path 可能是 .csv（不带 .parquet 后缀），实际文件是 .csv.parquet
    return os.path.exists(path) or os.path.exists(path + ".parquet")


def _table_read_parquet(
    path: str,
    cfg: dict[str, Any],
    engine_backend: str,
    spark: Any = None,
) -> Any:
    """storage.backend="parquet" 时的读路径（本地或 S3）.

    按 engine_backend 路由返回类型（与 local_csv 路径一致）：
      python → (List[Dict], fields)   # pyarrow.parquet.read_table → to_pylist
      polars → polars.DataFrame        # pl.read_parquet
      spark  → SparkDataFrame          # spark.read.parquet

    Args:
        path: 数据文件路径（本地 .parquet、逻辑路径或 s3:// URI）.
        cfg: Pipeline 配置 dict.
        engine_backend: "python" / "polars" / "spark".
        spark: SparkSession（spark 路径用）.

    Returns:
        按 engine_backend 对应类型（见上）.
    """
    is_s3 = _is_s3_target(path, cfg)
    if is_s3:
        target = _resolve_s3_path(path, cfg)
    else:
        target = path if path.endswith(".parquet") else path + ".parquet"

    if engine_backend == "spark":
        if spark is None:
            spark = _get_spark_session(cfg)
        return spark.read.parquet(target)
    elif engine_backend == "polars":
        import polars as pl  # lazy import：仅 polars 路径需要
        if is_s3:
            opts = _build_polars_s3_options(cfg)
            return pl.read_parquet(target, storage_options=opts)
        else:
            return pl.read_parquet(target)
    else:
        # python backend：pyarrow 读 Parquet → (List[Dict], fields)
        import pyarrow.parquet as pq  # lazy import：仅 parquet 路径需要
        if is_s3:
            fs = _get_s3_filesystem(cfg)
            # pyarrow.fs.S3FileSystem 期望 bucket/key 形式，不带 s3:// 前缀
            table = pq.read_table(_s3_uri_to_bucket_key(target), filesystem=fs)
        else:
            table = pq.read_table(target)
        return table.to_pylist(), table.column_names


def _table_write_parquet(
    path: str,
    df_or_rows: Any,
    cfg: dict[str, Any],
    engine_backend: str,
    fields: Optional[list[str]] = None,
    spark: Any = None,
) -> int:
    """storage.backend="parquet" 时的写路径（本地或 S3）.

    按 engine_backend 路由：
      python → pyarrow.parquet.write_table(pa.Table.from_pylist(rows, schema=string))
      polars → df.write_parquet(compression=...)
      spark  → df.write.mode("overwrite").parquet(target)

    python 路径用全 string schema 保持与 CSV 一致的类型语义（CSV 读入全是
    字符串），确保 round-trip 一致性。

    Args:
        path: 目标文件路径（本地 .parquet、逻辑路径或 s3:// URI）.
        df_or_rows: python 时 List[Dict]；polars 时 polars.DataFrame；
                    spark 时 SparkDataFrame.
        cfg: Pipeline 配置 dict.
        engine_backend: "python" / "polars" / "spark".
        fields: python 路径指定列顺序；缺省从 rows[0] 推断.
        spark: SparkSession（spark 路径用）.

    Returns:
        写出行数.
    """
    is_s3 = _is_s3_target(path, cfg)
    if is_s3:
        target = _resolve_s3_path(path, cfg)
    else:
        target = path if path.endswith(".parquet") else path + ".parquet"
        os.makedirs(os.path.dirname(target), exist_ok=True)

    compression = _get_parquet_compression(cfg)

    if engine_backend == "spark":
        if spark is None:
            spark = _get_spark_session(cfg)
        n = df_or_rows.count()
        df_or_rows.write.mode("overwrite").parquet(target)
        return n
    elif engine_backend == "polars":
        if is_s3:
            opts = _build_polars_s3_options(cfg)
            df_or_rows.write_parquet(
                target, compression=compression, storage_options=opts
            )
        else:
            df_or_rows.write_parquet(target, compression=compression)
        return df_or_rows.height
    else:
        # python backend：List[Dict] → pyarrow Table → Parquet
        import pyarrow as pa  # lazy import
        import pyarrow.parquet as pq  # lazy import
        rows = df_or_rows
        if fields is None:
            fields = list(rows[0].keys()) if rows else []
        # 用 string schema 保持与 CSV 一致的类型语义（CSV 读入全是字符串），
        # 确保 CSV→Parquet→CSV round-trip 行为一致。
        # 把所有值转成 string（CSV 写出时也是 str(v)），避免 int/float 值
        # 与 string schema 不匹配导致 ArrowTypeError。
        str_rows = [
            {f: (str(r.get(f)) if r.get(f) is not None else "") for f in fields}
            for r in rows
        ]
        schema = pa.schema([(f, pa.string()) for f in fields])
        table = pa.Table.from_pylist(str_rows, schema=schema)
        if is_s3:
            fs = _get_s3_filesystem(cfg)
            # pyarrow.fs.S3FileSystem 期望 bucket/key 形式，不带 s3:// 前缀
            pq.write_table(
                table, _s3_uri_to_bucket_key(target),
                filesystem=fs, compression=compression,
            )
        else:
            pq.write_table(table, target, compression=compression)
        return len(rows)


# ---------------------------------------------------------------------------
# Phase 4: Iceberg 湖存储分支（storage.backend="iceberg"）
# ---------------------------------------------------------------------------
# 设计参见 docs/evolution.md §6.x。storage.backend="iceberg" 时：
#   - path 参数是 Iceberg 表名（如 "warehouse.orders"），不是文件路径
#   - 用 pyiceberg catalog（SQL+SQLite 开发 / REST 生产）管理表元数据
#   - 数据文件由 Iceberg 写到 warehouse 目录（本地或 S3/MinIO）
#   - snapshot diff 增量：incremental_append_scan(from_snapshot_id_exclusive=...)
#   - time travel：scan(snapshot_id=...)
# pyiceberg 采用 lazy import：仅 storage.backend="iceberg" 路径才 import，
# 保持 storage.backend="local_csv"/"parquet" 路径零额外依赖。
# pyiceberg 0.12.0rc1 是预发布版，所有操作加 try-except 防御潜在 bug。

# 模块级锁：串行化 Iceberg catalog 初始化（SQLAlchemy create_all 非线程安全）
_ICEBERG_CATALOG_LOCK = threading.Lock()


def _get_iceberg_catalog(cfg: dict[str, Any]) -> Any:
    """加载 Iceberg catalog（lazy import pyiceberg）.

    从 cfg["storage"]["iceberg"] 读 catalog_name / catalog_type / catalog_uri /
    warehouse，调 ``pyiceberg.catalog.load_catalog`` 创建 catalog 实例.

    支持的 catalog_type：
        "sql"      — SQL catalog（开发测试用 SQLite，生产用 PostgreSQL）
        "rest"     — REST catalog（生产，配合 Iceberg REST server）
        "in-memory" — 内存 catalog（纯测试，不持久化）

    Args:
        cfg: Pipeline 配置 dict，读 storage.iceberg 段.

    Returns:
        pyiceberg.catalog.Catalog 实例.

    Raises:
        RuntimeError: pyiceberg 未安装或 catalog 加载失败.
    """
    try:
        from pyiceberg.catalog import load_catalog  # lazy import
    except ImportError as e:
        raise RuntimeError(
            "pyiceberg is required for storage.backend='iceberg' "
            "(install: pip install --pre pyiceberg==0.12.0rc1)"
        ) from e
    ice_cfg = cfg.get("storage", {}).get("iceberg", {}) or {}
    name = ice_cfg.get("catalog_name", "autobatch")
    catalog_type = ice_cfg.get("catalog_type", "sql")
    catalog_uri = ice_cfg.get("catalog_uri", "sqlite:///state/iceberg_catalog.db")
    warehouse = ice_cfg.get("warehouse", "state/warehouse")
    # Windows 路径修正：pyiceberg pyarrow FileIO 不接受 file:/// + Windows 盘符
    # （会触发 WinError 123），用纯路径形式 C:/path 即可.
    if warehouse.startswith("file:///"):
        warehouse = warehouse[len("file:///"):]
    # 并发安全：多线程同时初始化 SQL catalog 会触发
    # "table iceberg_tables already exists"（SQLAlchemy create_all 非线程安全）.
    # 用模块级锁串行化 catalog 初始化.
    try:
        _ICEBERG_CATALOG_LOCK.acquire()
        return load_catalog(
            name=name,
            type=catalog_type,
            uri=catalog_uri,
            warehouse=warehouse,
        )
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to load Iceberg catalog: {e}") from e
    finally:
        _ICEBERG_CATALOG_LOCK.release()


def _iceberg_table_identifier(path: str) -> tuple[str, ...]:
    """把 Iceberg 表名解析为 (namespace, table) 元组.

    输入形式：
        "warehouse.orders"    → ("warehouse", "orders")
        "warehouse.sub.orders" → ("warehouse", "sub", "orders")
        "orders"              → ("warehouse", "orders")（缺省 namespace）

    Args:
        path: Iceberg 表名（点分隔的 namespace.table）.

    Returns:
        pyiceberg 期望的 Identifier 元组.
    """
    parts = path.split(".")
    if len(parts) == 1:
        return ("warehouse", parts[0])
    return tuple(parts)


def _iceberg_ensure_namespace(catalog: Any, identifier: tuple[str, ...]) -> None:
    """确保 namespace 存在（不存在则创建）.

    Args:
        catalog: pyiceberg Catalog 实例.
        identifier: 表 identifier 元组（namespace..., table）.
    """
    namespace = identifier[:-1]
    if not namespace:
        return
    try:
        catalog.create_namespace_if_not_exists(namespace)
    except Exception:  # noqa: BLE001
        # 部分 catalog 类型不支持 create_namespace_if_not_exists，忽略
        pass


def _iceberg_infer_schema(rows: list[dict[str, Any]],
                          fields: Optional[list[str]] = None) -> Any:
    """从 List[Dict] 推断 pyiceberg Schema（全部用 StringType，与 CSV 语义一致）.

    Args:
        rows: 数据行列表.
        fields: 列顺序；缺省从 rows[0] 推断.

    Returns:
        pyiceberg.schema.Schema 实例.
    """
    from pyiceberg.schema import Schema  # lazy import
    from pyiceberg.types import NestedField, StringType  # lazy import

    if fields is None:
        fields = list(rows[0].keys()) if rows else []
    # pyiceberg Schema 不接受 generator，必须传 list/tuple
    nested_fields = [
        NestedField(i + 1, f, StringType(), required=False)
        for i, f in enumerate(fields)
    ]
    return Schema(*nested_fields)


def _rows_to_arrow_table(rows: list[dict[str, Any]],
                         fields: Optional[list[str]] = None) -> Any:
    """List[Dict] → pyarrow.Table（全 string schema，与 CSV 语义一致）.

    Args:
        rows: 数据行列表.
        fields: 列顺序；缺省从 rows[0] 推断.

    Returns:
        pyarrow.Table 实例.
    """
    import pyarrow as pa  # lazy import

    if fields is None:
        fields = list(rows[0].keys()) if rows else []
    # 全部值转 string（与 parquet 路径一致，CSV 读入全是字符串）
    str_rows = [
        {f: (str(r.get(f)) if r.get(f) is not None else "") for f in fields}
        for r in rows
    ]
    schema = pa.schema([(f, pa.string()) for f in fields])
    return pa.Table.from_pylist(str_rows, schema=schema)


def _iceberg_spark_full_name(path: str, cfg: dict[str, Any]) -> str:
    """把 pyiceberg 表名（namespace.table）解析为 Spark 三段式全名.

    Spark Iceberg 表名格式：``catalog.namespace.table``（catalog 前缀来自
    ``cfg["storage"]["iceberg"]["catalog_name"]``）；pyiceberg 用
    ``namespace.table``（无 catalog 前缀）. 本函数把 path 前面加上 catalog
    名，使 ``spark.read.table(full_name)`` 能正确路由到 Iceberg 表.

    Args:
        path: Iceberg 表名（namespace.table，如 "warehouse.orders"）.
        cfg: Pipeline 配置 dict，读 storage.iceberg.catalog_name.

    Returns:
        Spark 三段式全名（如 "autobatch.warehouse.orders"）.
    """
    ice_cfg = cfg.get("storage", {}).get("iceberg", {}) or {}
    catalog_name = ice_cfg.get("catalog_name", "autobatch")
    return f"{catalog_name}.{path}"


def _table_read_iceberg(
    path: str,
    cfg: dict[str, Any],
    engine_backend: str,
    spark: Any = None,
    snapshot_id: Optional[int] = None,
) -> Any:
    """storage.backend="iceberg" 时的读路径.

    path 是 Iceberg 表名（如 "warehouse.orders"），不是文件路径.
    按 engine_backend 路由返回类型（与 local_csv/parquet 路径一致）：
      python → (List[Dict], fields)
      polars → polars.DataFrame
      spark  → SparkDataFrame（spark.read.table("catalog.namespace.table")）

    Spark 路径用 ``spark.read.option("snapshot-id", ...).table(full_name)``
    实现 time travel；缺省 snapshot_id 时读 current snapshot.
    Spark Iceberg 表名格式：``catalog.namespace.table``（catalog 前缀来自
    storage.iceberg.catalog_name），与 pyiceberg 的 ``namespace.table`` 不同，
    由 ``_iceberg_spark_full_name`` 统一拼接.

    Args:
        path: Iceberg 表名（namespace.table）.
        cfg: Pipeline 配置 dict.
        engine_backend: "python" / "polars" / "spark".
        spark: SparkSession（spark 路径必须传入；缺省时通过
               ``_get_spark_session(cfg)`` 创建/复用）.
        snapshot_id: 可选，time travel 到指定 snapshot；缺省读 current snapshot.

    Returns:
        按 engine_backend 对应类型（见上）.
    """
    if engine_backend == "spark":
        if spark is None:
            spark = _get_spark_session(cfg)
        full_name = _iceberg_spark_full_name(path, cfg)
        try:
            reader = spark.read
            if snapshot_id is not None:
                # Iceberg time travel：snapshot-id 选项（Long）
                reader = reader.option("snapshot-id", int(snapshot_id))
            return reader.table(full_name)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"failed to read Iceberg table {full_name} via Spark: {e}"
            ) from e
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(path)
    try:
        table = catalog.load_table(identifier)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to load Iceberg table {path}: {e}") from e
    try:
        scan = table.scan(snapshot_id=snapshot_id) if snapshot_id is not None else table.scan()
        arrow_table = scan.to_arrow()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to scan Iceberg table {path}: {e}") from e
    if engine_backend == "polars":
        import polars as pl  # lazy import
        return pl.from_arrow(arrow_table)
    # python backend：返回 (rows, fields)
    return arrow_table.to_pylist(), list(arrow_table.column_names)


def _table_write_iceberg(
    path: str,
    df_or_rows: Any,
    cfg: dict[str, Any],
    engine_backend: str,
    fields: Optional[list[str]] = None,
    spark: Any = None,
    mode: str = "append",
) -> int:
    """storage.backend="iceberg" 时的写路径.

    path 是 Iceberg 表名（如 "warehouse.orders"）.
    表不存在时自动创建（用推断的 schema）.

    Spark 路径用 ``df.writeTo(full_name).overwrite()`` / ``.append()`` 写入
    Iceberg 表（DF v2 API，由 IcebergSparkSessionExtensions 提供）.
    Spark Iceberg 表名格式：``catalog.namespace.table``（catalog 前缀来自
    storage.iceberg.catalog_name），由 ``_iceberg_spark_full_name`` 统一拼接.

    Args:
        path: Iceberg 表名（namespace.table）.
        df_or_rows: python 时 List[Dict]；polars 时 polars.DataFrame；
                    spark 时 SparkDataFrame.
        cfg: Pipeline 配置 dict.
        engine_backend: "python" / "polars" / "spark".
        fields: python 路径指定列顺序；缺省从 rows[0] 推断.
        spark: SparkSession（spark 路径必须传入；缺省时通过
               ``_get_spark_session(cfg)`` 创建/复用）.
        mode: "append"（追加，缺省）或 "overwrite"（覆盖）.

    Returns:
        写出行数.
    """
    if engine_backend == "spark":
        if spark is None:
            spark = _get_spark_session(cfg)
        full_name = _iceberg_spark_full_name(path, cfg)
        df = df_or_rows
        # count() 触发 Spark action，先用原 df 计数（writeTo 不返回行数）
        try:
            n_rows = df.count()
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"failed to count rows before writing Iceberg table "
                f"{full_name} via Spark: {e}"
            ) from e
        try:
            if mode == "overwrite":
                # DFv2 API：createOrReplace 与 overwrite 等价；用 overwrite
                # 兼容表已存在与不存在两种场景（Iceberg Spark 会自动建表）.
                df.writeTo(full_name).createOrReplace()
            else:
                # append 模式：表不存在时自动创建（createOrReplace 不适用 append）
                # 用 overwritePartitions() 不合适（无分区）；改用 append() + 表存在性判断
                try:
                    df.writeTo(full_name).append()
                except Exception:  # noqa: BLE001
                    # 表不存在时 append 会失败，回退到 createOrReplace 建表
                    df.writeTo(full_name).createOrReplace()
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"failed to {mode} Iceberg table {full_name} via Spark: {e}"
            ) from e
        return n_rows
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(path)
    _iceberg_ensure_namespace(catalog, identifier)

    # 统一转为 pyarrow.Table
    if engine_backend == "polars" and hasattr(df_or_rows, "to_arrow"):
        # polars DataFrame → pyarrow Table
        arrow_table = df_or_rows.to_arrow()
        if fields is None:
            fields = list(arrow_table.column_names)
    else:
        # python backend：List[Dict] → pyarrow.Table
        # （polars backend 但传入 List[Dict] 时也走此分支，兼容测试场景）
        rows = df_or_rows
        if fields is None:
            fields = list(rows[0].keys()) if rows else []
        arrow_table = _rows_to_arrow_table(rows, fields)

    n_rows = arrow_table.num_rows

    # 加载或创建表
    try:
        table = catalog.load_table(identifier)
    except Exception:  # noqa: BLE001
        # 表不存在：用推断 schema 创建
        schema = _iceberg_infer_schema([], fields)
        try:
            table = catalog.create_table(identifier, schema=schema)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"failed to create Iceberg table {path}: {e}"
            ) from e

    # 写入（append / overwrite）
    try:
        if mode == "overwrite":
            table.overwrite(arrow_table)
        else:
            table.append(arrow_table)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            f"failed to {mode} Iceberg table {path}: {e}"
        ) from e
    return n_rows


def iceberg_snapshot_diff(
    table_name: str,
    cfg: dict[str, Any],
    from_snapshot: Optional[int] = None,
) -> dict[str, Any]:
    """Iceberg snapshot diff：返回两个 snapshot 之间的增量数据文件信息.

    用 ``table.incremental_append_scan(from_snapshot_id_exclusive=from_snapshot,
    to_snapshot_id_inclusive=current_id)`` 获取增量数据.

    Args:
        table_name: Iceberg 表名（namespace.table）.
        cfg: Pipeline 配置 dict.
        from_snapshot: 起始 snapshot id（exclusive）；None 表示从初始 snapshot 开始.

    Returns:
        dict 包含：
            from_snapshot  — 起始 snapshot id（exclusive）
            to_snapshot    — 结束 snapshot id（inclusive，即 current snapshot）
            added_data_files — 增量数据文件数
            added_rows_count — 增量行数
            rows            — 增量数据行（List[Dict]，便于调用方直接使用）
            fields          — 列名列表
    """
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(table_name)
    try:
        table = catalog.load_table(identifier)
        current = table.current_snapshot()
        to_snapshot = current.snapshot_id if current else None
        scan = table.incremental_append_scan(
            from_snapshot_id_exclusive=from_snapshot,
            to_snapshot_id_inclusive=to_snapshot,
        )
        arrow_table = scan.to_arrow()
        rows = arrow_table.to_pylist()
        fields = list(arrow_table.column_names)
        # 从 snapshot summary 提取 added-data-files 计数
        # pyiceberg Summary 是 Mapping 子类，手动提取 _additional_properties
        added_files = 0
        if current is not None and current.summary is not None:
            try:
                extra = getattr(current.summary, "_additional_properties", {}) or {}
                added_files = int(extra.get("added-data-files", 0) or 0)
            except Exception:  # noqa: BLE001
                added_files = 0
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            f"failed to compute snapshot diff for {table_name}: {e}"
        ) from e
    return {
        "from_snapshot": from_snapshot,
        "to_snapshot": to_snapshot,
        "added_data_files": added_files,
        "added_rows_count": len(rows),
        "rows": rows,
        "fields": fields,
    }


def iceberg_snapshot_diff_spark(
    table_name: str,
    cfg: dict[str, Any],
    from_snapshot: Optional[int] = None,
    spark: Any = None,
) -> dict[str, Any]:
    """Spark 原生 incremental scan：分布式 snapshot diff.

    用 ``spark.read.option("start-snapshot-id", from).option("end-snapshot-id",
    to).table(full_name)`` 读取两个 snapshot 之间的增量数据，由 Spark executor
    并行扫描数据文件，避免单机 pyiceberg ``incremental_append_scan`` 的内存瓶颈.

    与 ``iceberg_snapshot_diff`` 行为一致，但返回 SparkDataFrame（lazy）+
    行数（count 触发 action）. 调用方可继续做 Spark 聚合 / join 等.

    Args:
        table_name: Iceberg 表名（namespace.table，pyiceberg 格式）.
        cfg: Pipeline 配置 dict.
        from_snapshot: 起始 snapshot id（exclusive）；None 表示从初始 snapshot 开始.
                       None 时 Spark Iceberg 不支持 start-snapshot-id 为空，
                       退化为全表扫描（current snapshot）.
        spark: SparkSession；缺省时通过 ``_get_spark_session(cfg)`` 创建/复用.

    Returns:
        dict 包含：
            from_snapshot  — 起始 snapshot id（exclusive）
            to_snapshot    — 结束 snapshot id（inclusive，None 表示 current）
            df             — SparkDataFrame（增量数据，lazy）
            added_rows_count — 增量行数（count() 已触发 action）
            fields         — 列名列表（从 df.schema 提取）
    """
    if spark is None:
        spark = _get_spark_session(cfg)
    full_name = _iceberg_spark_full_name(table_name, cfg)
    # 先用 pyiceberg 拿 current snapshot id（轻量，不读数据）
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(table_name)
    try:
        table = catalog.load_table(identifier)
        current = table.current_snapshot()
        to_snapshot = current.snapshot_id if current else None
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            f"failed to load Iceberg table {table_name} for snapshot id: {e}"
        ) from e
    try:
        reader = spark.read
        if from_snapshot is not None:
            # Spark Iceberg incremental scan：start-snapshot-id (exclusive)
            # + end-snapshot-id (inclusive)
            reader = reader.option("start-snapshot-id", int(from_snapshot))
            if to_snapshot is not None:
                reader = reader.option("end-snapshot-id", int(to_snapshot))
        df = reader.table(full_name)
        fields = [f.name for f in df.schema.fields]
        # count() 触发 action，得到增量行数
        added_rows_count = df.count()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            f"failed to compute Spark snapshot diff for {table_name}: {e}"
        ) from e
    return {
        "from_snapshot": from_snapshot,
        "to_snapshot": to_snapshot,
        "df": df,
        "added_rows_count": added_rows_count,
        "fields": fields,
    }


def read_history_snapshot(
    table_name: str,
    cfg: dict[str, Any],
    snapshot_id: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Iceberg time travel：读取指定 snapshot 的历史数据.

    Args:
        table_name: Iceberg 表名（namespace.table）.
        cfg: Pipeline 配置 dict.
        snapshot_id: 目标 snapshot id.

    Returns:
        (rows, fields) 元组，与 table_read python 路径一致.
    """
    return _table_read_iceberg(
        table_name, cfg, "python", spark=None, snapshot_id=snapshot_id
    )


def list_snapshots(table_name: str, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """列出 Iceberg 表的所有 snapshot.

    Args:
        table_name: Iceberg 表名（namespace.table）.
        cfg: Pipeline 配置 dict.

    Returns:
        snapshot 信息列表，每项包含：
            snapshot_id   — snapshot id
            parent_id     — 父 snapshot id（首个 snapshot 为 None）
            timestamp_ms  — snapshot 时间戳（毫秒）
            summary       — snapshot summary dict（operation + additional_properties）
    """
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(table_name)
    try:
        table = catalog.load_table(identifier)
        result = []
        for snap in table.snapshots():
            # pyiceberg Summary 是 Mapping 子类，但 dict(summary) 可能触发
            # __getitem__ 错误（key 类型不匹配）。手动提取 operation 和
            # _additional_properties 构造 dict.
            summary_dict: dict[str, Any] = {}
            if snap.summary is not None:
                try:
                    summary_dict["operation"] = str(snap.summary.operation)
                    summary_dict.update(getattr(snap.summary, "_additional_properties", {}) or {})
                except Exception:  # noqa: BLE001
                    summary_dict = {}
            result.append({
                "snapshot_id": snap.snapshot_id,
                "parent_id": snap.parent_snapshot_id,
                "timestamp_ms": snap.timestamp_ms,
                "summary": summary_dict,
            })
        return result
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            f"failed to list snapshots for {table_name}: {e}"
        ) from e


def _iceberg_path_is_table_name(path: str) -> bool:
    """判断 path 是 Iceberg 表名还是文件路径.

    Iceberg 表名形式："warehouse.orders"、"warehouse.sub.orders"（点分隔，无路径分隔符）.
    文件路径形式：含盘符（C:）、路径分隔符（/ \\）、或文件后缀（.csv .parquet）.

    Args:
        path: 待判断的路径.

    Returns:
        True 若 path 是 Iceberg 表名；False 若是文件路径.
    """
    if not path:
        return False
    # 含盘符或路径分隔符 → 文件路径
    if os.sep in path or "/" in path or "\\" in path:
        return False
    if len(path) >= 2 and path[1] == ":":
        return False
    # 含文件后缀 → 文件路径
    if path.endswith(".csv") or path.endswith(".parquet"):
        return False
    # 点分隔的 namespace.table → Iceberg 表名
    return True


# ---------------------------------------------------------------------------
# table_read / table_write 类型标注
# ---------------------------------------------------------------------------
# engine.backend 在运行时由 cfg 决定，无法静态区分；主签名返回 Any，
# 调用方据 cfg["engine"]["backend"] 自行 narrow 类型。
#   python  → (List[Dict[str, Any]], List[str])
#   polars  → polars.DataFrame
#   spark   → pyspark.sql.DataFrame
# polars / pyspark 仅在 TYPE_CHECKING 下导入（避免运行时强依赖）。
if TYPE_CHECKING:
    import polars as pl  # noqa: F401  type-only
    from pyspark.sql import DataFrame as SparkDataFrame  # noqa: F401  type-only


def table_read(
    path: str,
    cfg: dict[str, Any],
    spark: Any = None,
    snapshot_id: Optional[int] = None,
) -> Any:
    """统一读接口，按 (storage.backend, engine.backend) 组合路由.

    storage.backend="local_csv"（缺省）→ 走 engine.backend 路由（python/polars/spark 读 CSV）
    storage.backend="parquet"           → pyarrow/polars/spark 读 Parquet（本地或 S3/MinIO）
    storage.backend="iceberg"           → pyiceberg/polars 读 Iceberg 表（path 是表名）

    返回类型按 engine.backend（与 local_csv 路径一致）：
      engine.backend="python" → (List[Dict], fields)
      engine.backend="polars" → polars.DataFrame
      engine.backend="spark"  → SparkDataFrame

    Args:
        path: 数据文件路径。storage.backend="parquet" 时 path 可以是本地
              .parquet 文件、逻辑路径（由 _resolve_s3_path 解析为 s3:// URI）
              或完整 s3:// URI。storage.backend="iceberg" 时 path 是 Iceberg
              表名（如 "warehouse.orders"）；若 path 看起来像文件路径（含盘符/
              分隔符），则回退到 local_csv 逻辑（向后兼容中间产物 CSV）。
              engine.backend="spark" 时 path 也可以是 Spark 写出的目录（多分区
              part-00000-* 文件），spark.read 会自动扫描目录下所有分区文件。
        cfg: Pipeline 配置 dict，读 storage 段与 engine 段。
        spark: engine.backend="spark" 时使用的 SparkSession。缺省 None 时通过
               ``_get_spark_session(cfg)`` 创建/复用。推荐由 pipeline.py 显式
               传入 ``ctx.spark_session`` 以复用同一 session。
        snapshot_id: storage.backend="iceberg" 时 time travel 到指定 snapshot；
                     其他 backend 忽略。缺省 None 读 current snapshot。

    Returns:
        engine.backend="python" 时返回 (rows, fields) 元组；
        engine.backend="polars" 时返回 polars.DataFrame；
        engine.backend="spark"  时返回 pyspark.sql.DataFrame（lazy，未触发 action）。
    """
    engine_backend = _get_engine_backend(cfg)
    storage_backend = _get_storage_backend(cfg)

    # storage.backend="iceberg" 分支：pyiceberg/polars 读 Iceberg 表
    # 但 path 看起来像文件路径时回退到 local_csv（中间产物 CSV 向后兼容）
    if storage_backend == "iceberg" and _iceberg_path_is_table_name(path):
        return _table_read_iceberg(path, cfg, engine_backend, spark, snapshot_id)

    # storage.backend="parquet" 分支：pyarrow/polars/spark 读 Parquet（本地或 S3）
    if storage_backend == "parquet":
        return _table_read_parquet(path, cfg, engine_backend, spark)

    # storage.backend="local_csv" 分支（现有逻辑，向后兼容）
    backend = engine_backend
    if backend == "spark":
        if spark is None:
            spark = _get_spark_session(cfg)
        fmt = cfg.get("engine", {}).get("format", "csv")
        if fmt == "parquet" and (
            path.endswith(".parquet") or os.path.exists(path + ".parquet")
        ):
            p = path if path.endswith(".parquet") else path + ".parquet"
            return spark.read.parquet(p)
        else:
            opts = cfg.get("engine", {}).get("spark", {}).get("read_options", {}) or {}
            return spark.read.csv(
                path, header=True, inferSchema=True, **opts
            )
    elif backend == "polars":
        import polars as pl  # lazy import：仅 polars 路径需要
        fmt = cfg.get("engine", {}).get("format", "csv")
        opts = cfg.get("engine", {}).get("polars", {}).get("read_options", {}) or {}
        if fmt == "parquet" and (
            path.endswith(".parquet") or os.path.exists(path + ".parquet")
        ):
            p = path if path.endswith(".parquet") else path + ".parquet"
            return pl.read_parquet(p)
        else:
            return pl.read_csv(path, **opts)
    else:
        # python backend：与 csv_read 行为完全一致
        return csv_read(path)



def table_write(
    path: str,
    df_or_rows: Any,
    cfg: dict[str, Any],
    fields: Optional[list[str]] = None,
    spark: Any = None,
    mode: str = "overwrite",
) -> int:
    """统一写接口.

    storage.backend="local_csv"（缺省）→ 走 engine.backend 路由（python/polars/spark 写 CSV）
    storage.backend="parquet"           → pyarrow/polars/spark 写 Parquet（本地或 S3/MinIO）
    storage.backend="iceberg"           → pyiceberg 写 Iceberg 表（path 是表名）

    engine.backend="python" → csv_write / pyarrow.parquet.write_table，返回行数
    engine.backend="polars" → df.write_csv / df.write_parquet，返回 df.height
    engine.backend="spark"  → df.write.mode("overwrite").csv/parquet(path)，
                              返回 df.count()。注意 Spark 写出的是**目录**（多分区
                              part-00000-* 文件），不是单文件；若后续 stage 用 Spark
                              读则直接读目录即可，若需单文件可设
                              engine.spark.write_single_file=true（内部用 coalesce(1)）。

    Args:
        path: 目标文件路径。storage.backend="parquet" 时 path 可以是本地
              .parquet 文件、逻辑路径（由 _resolve_s3_path 解析为 s3:// URI）
              或完整 s3:// URI。storage.backend="iceberg" 时 path 是 Iceberg
              表名（如 "warehouse.orders"）。engine.backend="spark" 时 path
              实际作为目录路径，Spark 会在其下写出 part-00000-* 分区文件。
        df_or_rows: engine.backend="python" 时是 List[Dict]；polars 时是
                    polars.DataFrame；spark 时是 SparkDataFrame。
        cfg: Pipeline 配置 dict，读 storage 段与 engine 段。
        fields: engine.backend="python" 时指定列顺序；缺省从 rows[0] 推断。
                    engine.backend="polars"/"spark" 时忽略（DataFrame 自带 schema）。
        spark: engine.backend="spark" 时使用的 SparkSession。缺省 None 时通过
               ``_get_spark_session(cfg)`` 创建/复用。
        mode: 写模式。storage.backend="iceberg" 时 "append"（追加）/ "overwrite"
              （覆盖，缺省）；其他 backend 忽略此参数（local_csv/parquet 始终覆盖）。

    Returns:
        写出行数。
    """
    engine_backend = _get_engine_backend(cfg)
    storage_backend = _get_storage_backend(cfg)

    # storage.backend="iceberg" 分支：pyiceberg 写 Iceberg 表
    # 但 path 看起来像文件路径时回退到 local_csv（中间产物 CSV 向后兼容）
    if storage_backend == "iceberg" and _iceberg_path_is_table_name(path):
        return _table_write_iceberg(
            path, df_or_rows, cfg, engine_backend, fields, spark, mode=mode
        )

    # storage.backend="parquet" 分支：pyarrow/polars/spark 写 Parquet（本地或 S3）
    if storage_backend == "parquet":
        return _table_write_parquet(
            path, df_or_rows, cfg, engine_backend, fields, spark
        )

    # storage.backend="local_csv" 分支（现有逻辑，向后兼容）
    backend = engine_backend
    if backend == "spark":
        if spark is None:
            spark = _get_spark_session(cfg)
        fmt = cfg.get("engine", {}).get("format", "csv")
        spark_cfg = cfg.get("engine", {}).get("spark", {}) or {}
        single_file = spark_cfg.get("write_single_file", False)

        df = df_or_rows
        # count() 触发 Spark action，先用原 df 计数（coalesce 不改行数但会拉数据
        # 到单分区，先 count 再 coalesce 避免单分区计数瓶颈）。
        n = df.count()
        if single_file:
            df = df.coalesce(1)

        if fmt == "parquet":
            p = path if path.endswith(".parquet") else path + ".parquet"
            os.makedirs(os.path.dirname(p), exist_ok=True)
            df.write.mode("overwrite").parquet(p)
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.write.mode("overwrite").option("header", True).csv(path)
        return n
    elif backend == "polars":
        import polars as pl  # noqa: F401  lazy import
        fmt = cfg.get("engine", {}).get("format", "csv")
        if fmt == "parquet":
            p = path if path.endswith(".parquet") else path + ".parquet"
            compression = (
                cfg.get("engine", {}).get("polars", {}).get("parquet_compression", "zstd")
            )
            os.makedirs(os.path.dirname(p), exist_ok=True)
            df_or_rows.write_parquet(p, compression=compression)
            return df_or_rows.height
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df_or_rows.write_csv(path)
            return df_or_rows.height
    else:
        # python backend：df_or_rows 是 List[Dict]
        rows = df_or_rows
        if fields is None:
            fields = list(rows[0].keys()) if rows else []
        return csv_write(path, fields, rows)


def json_load(path: str) -> Any:
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


def json_save(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def copy_file(src: str, dst: str) -> str:
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)
    return sha256_of(dst)


def num_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"null", "none", "nan"}:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def num_int(v: Any) -> Optional[int]:
    f = num_float(v)
    if f is None:
        return None
    return int(f) if f.is_integer() else None


def date_parse(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


class StageLog:
    """Per-stage structured JSONL logger.

    每条日志输出 JSON 对象到 ``<run_dir>/logs/<stage>.jsonl``，包含：
        ts        — UTC ISO8601 毫秒精度
        level     — INFO / WARN / ERROR
        batch_id  — 批次 ID（运行追踪 ID，关联所有 stage 日志）
        stage     — stage 名（ingest/validate/clean/compute/output）
        msg       — 消息
        <extra>   — 调用方传的额外字段（rows、source、error 等）

    batch_id / stage 为可选参数（向后兼容）：未传时缺省空串 / "pipeline"，
    既有调用方（如 tests/conftest.py 的 _make_log）无需修改即可工作.
    """

    def __init__(self, path: str, batch_id: str = "", stage: str = "pipeline"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.batch_id = batch_id
        self.stage = stage
        self._fh = open(path, "a", encoding="utf-8")

    def emit(self, level: str, msg: str, **extra: Any) -> None:
        rec = {
            "ts": utc_ts(),
            "level": level,
            "batch_id": self.batch_id,
            "stage": self.stage,
            "msg": msg,
        }
        rec.update(extra)
        self._fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self._fh.flush()

    def info(self, msg: str, **extra: Any) -> None:
        self.emit("INFO", msg, **extra)

    def warn(self, msg: str, **extra: Any) -> None:
        self.emit("WARN", msg, **extra)

    def error(self, msg: str, **extra: Any) -> None:
        self.emit("ERROR", msg, **extra)

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> StageLog:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def logger_setup(level: str = "INFO") -> logging.Logger:
    """配置 root logger（向后兼容入口）.

    历史入口：仅设置 basicConfig，不写文件、不注入 batch_id/stage.
    保留以避免破坏外部调用方.新代码应改用 src.logging_setup.setup_logging，
    它支持 JSON/text 双格式 + 文件输出 + batch_id 追踪.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("pipeline")


# compatibility aliases
load_csv = csv_read
as_float = num_float
as_int = num_int
file_sha256 = sha256_of


@dataclass
class PipelineContext:
    """Strongly-typed container passed between pipeline stages.

    Replaces the previous ``ctx: Dict[str, Any]`` bag so that IDEs and type
    checkers can reason about stage inputs/outputs. Field names mirror the keys
    that were historically written into the dict context. All stages read and
    write these attributes directly (``ctx.config`` instead of ``ctx["config"]``).

    Fields:
        config:        Loaded pipeline configuration dict.
        run_dir:       Absolute path of this batch's run output directory.
        batch_id:      Human-readable batch identifier (e.g. ``B-20260815-...``).
        manifest:      Run ledger collecting artifacts, stages and lineage.
        ingested:      List of source-file descriptors produced by the ingest stage.
        outlier_keys:  Set of order_ids flagged as outliers by validate.
        aggregates:    Aggregation results dict produced by compute.
        clean_orders:  Cleaned order rows produced by clean (in-memory cache).
        lineage_decls: Lineage declarations collected from every stage. Keys are
                       product paths relative to ``run_dir`` (e.g.
                       ``"03_clean/orders_clean.csv"``); values are lists of
                       upstream product paths relative to ``run_dir``. The output
                       stage consumes this map to auto-build manifest lineage.
    """

    config: dict[str, Any]
    run_dir: str
    batch_id: str
    manifest: Manifest
    ingested: list[dict[str, Any]] = field(default_factory=list)
    outlier_keys: set[str] = field(default_factory=set)
    aggregates: dict[str, Any] = field(default_factory=dict)
    clean_orders: list[dict[str, Any]] = field(default_factory=list)
    lineage_decls: dict[str, list[str]] = field(default_factory=dict)
    # Incremental-mode state (Phase 1, see docs/evolution.md §3.3.1).
    # `state` holds the in-memory state.json dict; `state_path` is its absolute
    # path; `incremental_enabled` gates the incremental code path; `new_orders`
    # caches the delta rows produced by ingest for downstream stages.
    state: dict[str, Any] = field(default_factory=dict)
    state_path: str = ""
    incremental_enabled: bool = False
    new_orders: list[dict[str, Any]] = field(default_factory=list)
    # Phase 2a 列式加速（参见 docs/evolution.md §4.3.1）。
    # engine_backend 镜像 cfg["engine"]["backend"]，缺省 "python" 走 csv_read/csv_write
    # 路径（向后兼容）；"polars" 时 stages 走 polars.DataFrame 列式路径。
    engine_backend: str = "python"
    # Phase 2b Spark 分布式加速（参见 docs/evolution.md §4.3.2）。
    # spark_session 由 pipeline.py 在初始化时创建（_get_spark_session(cfg)）并注入，
    # 各 stage 通过 ctx.spark_session 访问，避免在 helpers/stages 内重复创建。
    # backend!="spark" 时保持 None，不影响其他路径。
    spark_session: Optional[Any] = None
