"""Shared helpers: timestamps, batch ids, paths, csv/json io, hashing, stage logging."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import shutil
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

# Re-export from split modules for backward compatibility
from .iceberg import (  # noqa: F401
    _get_iceberg_catalog,
    _iceberg_ensure_namespace,
    _iceberg_infer_schema,
    _iceberg_path_is_table_name,
    _iceberg_spark_full_name,
    _iceberg_table_identifier,
    _rows_to_arrow_table,
    _table_read_iceberg,
    _table_write_iceberg,
    iceberg_snapshot_diff,
    iceberg_snapshot_diff_spark,
    list_snapshots,
    read_history_snapshot,
)
from .io._s3_parquet import (  # noqa: F401
    _build_polars_s3_options,
    _get_parquet_compression,
    _get_s3_filesystem,
    _get_storage_backend,
    _is_s3_target,
    _resolve_s3_path,
    _s3_uri_to_bucket_key,
    _table_exists,
    _table_read_parquet,
    _table_write_parquet,
    apply_s3a_hadoop_conf,
)

if TYPE_CHECKING:  # avoid runtime circular import: lineage imports helpers
    from .lineage import Manifest

VERSION = "1.5.0"
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
    # 对象存储 URI（s3a:// 等）不是文件系统路径，原样返回；
    # 否则会被 join(ROOT, ...) 拼成非法路径（2026-08 亿行基准实测踩坑）.
    if p.startswith(("s3a://", "s3://", "s3n://", "gs://", "abfs://", "wasbs://")):
        return p
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


def _ensure_parent_dir(path: str) -> None:
    """为文件路径创建父目录；path 无目录分量（如 "foo.csv"）时跳过.

    os.path.dirname("foo.csv") == ""，直接 os.makedirs("") 会 FileNotFoundError.
    """
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _strip_bom_spark(df: Any) -> Any:
    """spark.read.csv 不剥 UTF-8 BOM：首列名带 \\ufeff 前缀时重命名修正.

    与 python 路径的 utf-8-sig 语义对齐，保证三引擎对带 BOM 的源文件行为一致.
    """
    try:
        cols = df.columns
        if cols and cols[0].startswith("\ufeff"):
            df = df.withColumnRenamed(cols[0], cols[0][1:])
    except Exception:  # noqa: BLE001  防御：列名访问失败时保持原样
        pass
    return df


def _strip_bom_polars(df: Any) -> Any:
    """pl.read_csv/scan_csv 不剥 UTF-8 BOM：首列名带 \\ufeff 前缀时重命名修正.

    对 LazyFrame 同样适用：LazyFrame 取列名用 collect_schema().names()
    （直接 .columns 触发 polars PerformanceWarning）.
    """
    try:
        if hasattr(df, "collect_schema"):  # LazyFrame
            cols = list(df.collect_schema().names())
        else:
            cols = list(df.columns)
        if cols and cols[0].startswith("\ufeff"):
            df = df.rename({cols[0]: cols[0][1:]})
    except Exception:  # noqa: BLE001
        pass
    return df


def csv_write(path: str, fields: Sequence[str], data: Sequence[dict[str, Any]]) -> int:
    # 原子写：先写同目录临时文件再 os.replace，并发读方永远不会看到半截
    # 文件（与 json_save 同口径）。generator 的 data/raw 写出与各 stage 的
    # 产物写出都经此函数——旧实现直接 open(path, "w") 流式写，进程崩溃留下
    # 半截 CSV，双进程并发写同一目标（如 generator.enabled 的两个批次同时
    # 跑）会互相撕裂内容（2026-08 审查 B10）。tmp 名含 pid+uuid 唯一化，
    # 避免并发双写碰撞同一 tmp；os.replace 在 Windows/POSIX 同卷均为原子。
    _ensure_parent_dir(path)
    tmp = f"{path}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(fields), extrasaction="ignore")
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    return len(data)


# ---------------------------------------------------------------------------
# Phase 2a/2b: 统一 IO 路由层（engine.backend 调度）
# ---------------------------------------------------------------------------
# engine.backend 决定计算引擎（python / polars / spark），storage.backend
# 决定存储介质（local_csv / parquet / iceberg），两者解耦正交组合。
# S3/Parquet 路由与 Iceberg 逻辑已拆分到独立模块以降低单文件耦合度：
#   src/io/_s3_parquet.py  — _get_storage_backend、_table_read/write_parquet 等
#   src/iceberg.py          — Iceberg catalog/snapshot/time-travel 逻辑
# 向后兼容：本模块仍 re-export 所有符号，现有 from .helpers import ... 调用不变。


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
    # parquet+S3 场景注入 fs.s3a.* 凭证/endpoint（与 pipeline._init_spark_session
    # 同一组键值）。缺失时重建的 session 无凭证读 s3a://，报
    # NoAuthWithAWSException——典型触发路径：run_pipeline 结束 spark.stop() 后，
    # table_read 惰性重建 session 读 MinIO 产物（2026-08-26 cluster 测试实测）。
    builder = apply_s3a_hadoop_conf(builder, cfg)
    # cluster 多机场景注入 driver.bindAddress/driver.host/pyspark.python（与
    # pipeline._init_spark_session 同一组键值）。缺失时重建 session 的 driver
    # 以自动探测的本机 IP 通告，容器内 executor 回连失败无限重启，读操作挂死
    # ——典型触发路径：run_pipeline 结束 spark.stop() 后，table_read 惰性重建
    # session 读 cluster 产物（2026-08-28 cluster 等价性测试实测）。
    builder = apply_cluster_conf(builder, cfg)
    # iceberg 场景注入 spark.sql.extensions + spark.sql.catalog.<name>（与
    # pipeline._init_spark_session 同一组键值）。缺失时重建的 session 未注册
    # catalog，三段式表名落回 spark_catalog 抛 REQUIRES_SINGLE_PART_NAMESPACE
    # ——典型触发路径：测试/工具直接经 table_read/table_write 访问 Iceberg 表。
    builder = apply_iceberg_spark_conf(builder, cfg)
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
        builder = builder.config("spark.sql.shuffle.partitions", scfg["shuffle_partitions"])
    # driver 端 action 结果序列化上限：千万行级 join/broadcast 的单 task 序列化
    # 结果会超 1G 缺省（2026-08 亿行基准实测 clean 阶段即因此失败），可经配置放大
    if scfg.get("max_result_size"):
        builder = builder.config("spark.driver.maxResultSize", scfg["max_result_size"])
    # JVM 附加参数（堆转储/GC 日志等），大规模基准排障用
    if scfg.get("driver_extra_java_options"):
        builder = builder.config("spark.driver.extraJavaOptions", scfg["driver_extra_java_options"])
    # AQE 缺省开启，自动合并小分区、处理倾斜。
    # ⚠ 键名必须是 spark.sql.adaptive.enabled——旧代码误用
    # spark.sql.adaptiveQueryExecution（无效键被 Spark 静默忽略），导致
    # AQE 实际从未生效（2026-08 亿行基准 OOM 排查中发现）
    aqe = scfg.get("adaptive_query_execution", True)
    builder = builder.config("spark.sql.adaptive.enabled", "true" if aqe else "false")
    return builder


def apply_cluster_conf(builder: Any, cfg: dict[str, Any]) -> Any:
    """cluster.enabled=true 时向 Spark builder 注入 Driver↔Worker 反向连接配置.

    注入 spark.driver.bindAddress=0.0.0.0、spark.driver.host（cluster.driver_host，
    缺省 "host.docker.internal"）、spark.pyspark.python（cluster.worker_python，
    缺省 "python3"，容器内 Worker 的 Python 解释器）。与 pipeline._init_spark_session
    的内联注入保持同一组键值；供 helpers._get_spark_session 在 session 重建场景
    （run_pipeline 结束 spark.stop() 之后，测试层 table_read 触发的惰性重建）复用——
    否则重建 session 的 driver 以自动探测的本机 IP 对外通告（WSL2 下为容器不可达
    地址），容器内 executor 回连失败约 60s 后 exit 1，master 无限重发 executor，
    读操作永久挂起（2026-08-28 cluster 等价性测试实测）。
    cluster.enabled 非 true 时原样返回 builder，零影响。
    """
    cluster = (cfg.get("engine", {}).get("spark", {}) or {}).get("cluster", {}) or {}
    if not cluster.get("enabled"):
        return builder
    builder = builder.config("spark.driver.bindAddress", "0.0.0.0")
    driver_host = cluster.get("driver_host", "host.docker.internal")
    builder = builder.config("spark.driver.host", driver_host)
    # Worker 在 Docker Linux 容器中运行，PYSPARK_PYTHON（Windows 路径如
    # F:\Py314\python.exe）在容器内不存在。设置 spark.pyspark.python 为
    # 容器内的 Python 路径（缺省 python3），使 Worker 能启动 Python worker。
    # spark.pyspark.driver.python 保持环境变量 PYSPARK_DRIVER_PYTHON（Driver
    # 端在宿主机上运行，使用 Windows 路径）。
    worker_python = cluster.get("worker_python", "python3")
    builder = builder.config("spark.pyspark.python", worker_python)
    return builder


def apply_iceberg_spark_conf(builder: Any, cfg: dict[str, Any]) -> Any:
    """storage.backend="iceberg" 时向 Spark builder 注入 Iceberg catalog 配置.

    注入 spark.sql.extensions（IcebergSparkSessionExtensions）与
    spark.sql.catalog.<name>（SparkCatalog + type/uri/warehouse），使
    spark.read.table("catalog.ns.tbl") / df.writeTo(...) 路由到 Iceberg 表。
    catalog_type="sql" 时改经 catalog-impl=JdbcCatalog 连同一 SQLite 库
    （SparkCatalog 不支持 sql 类型，详见下方分支注释）。
    非 iceberg backend 原样返回 builder。

    与 pipeline._init_spark_session 共用（原内联实现下沉于此）：
    helpers._get_spark_session 惰性重建 session 时（如 run_pipeline 结束后
    table_read/table_write 再访问 Iceberg 表）也必须注册 catalog，否则
    三段式表名落回 spark_catalog 抛 REQUIRES_SINGLE_PART_NAMESPACE。

    关键约束：Iceberg 官方 JAR 最高支持 Spark 4.1（不支持 4.2）。
    推荐组合：Spark 4.1.x + Iceberg 1.11.0（Scala 2.13）。
    """
    storage = cfg.get("storage", {}) or {}
    if storage.get("backend") != "iceberg":
        return builder
    ice_cfg = storage.get("iceberg", {}) or {}
    catalog_name = ice_cfg.get("catalog_name", "autobatch")
    spark_extensions = ice_cfg.get(
        "spark_extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    builder = builder.config("spark.sql.extensions", spark_extensions)
    spark_catalog_class = ice_cfg.get(
        "spark_catalog_class",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    builder = builder.config(f"spark.sql.catalog.{catalog_name}", spark_catalog_class)
    # catalog 类型：rest（生产）/ sql（开发测试）/ hive（兼容 Hive Metastore）
    catalog_type = ice_cfg.get("catalog_type", "rest")
    catalog_uri = ice_cfg.get("catalog_uri", "")
    if catalog_type == "sql":
        # Iceberg SparkCatalog 不认识 "sql" 类型（那是 pyiceberg SqlCatalog
        # 的类型；Spark 侧仅支持 hive/hadoop/rest 等，1.11.0 实测抛
        # Unknown catalog type: sql）。改用 catalog-impl 指定 JdbcCatalog
        # 连同一 SQLite 库：JdbcCatalog 与 pyiceberg SqlCatalog 共用
        # iceberg_tables 元数据表 schema，两引擎可互操作。需
        # SPARK_HOME/jars/ 下有 sqlite-jdbc 驱动 JAR（如
        # sqlite-jdbc-3.50.3.0.jar）。
        builder = builder.config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        if catalog_uri.startswith("sqlite:///"):
            # SQLAlchemy 形式 sqlite:///path → JDBC 形式 jdbc:sqlite:path
            catalog_uri = "jdbc:sqlite:" + catalog_uri[len("sqlite:///") :]
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.uri", catalog_uri)
    else:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.uri", catalog_uri)
    builder = builder.config(
        f"spark.sql.catalog.{catalog_name}.warehouse",
        ice_cfg.get("warehouse", ""),
    )
    return builder


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
        if fmt == "parquet" and (path.endswith(".parquet") or os.path.exists(path + ".parquet")):
            p = path if path.endswith(".parquet") else path + ".parquet"
            return spark.read.parquet(p)
        else:
            opts = cfg.get("engine", {}).get("spark", {}).get("read_options", {}) or {}
            return _strip_bom_spark(spark.read.csv(path, header=True, inferSchema=True, **opts))
    elif backend == "polars":
        import polars as pl  # lazy import：仅 polars 路径需要

        fmt = cfg.get("engine", {}).get("format", "csv")
        opts = cfg.get("engine", {}).get("polars", {}).get("read_options", {}) or {}
        if fmt == "parquet" and (path.endswith(".parquet") or os.path.exists(path + ".parquet")):
            p = path if path.endswith(".parquet") else path + ".parquet"
            return pl.read_parquet(p)
        else:
            return _strip_bom_polars(pl.read_csv(path, **opts))
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
        return _table_write_iceberg(path, df_or_rows, cfg, engine_backend, fields, spark, mode=mode)

    # storage.backend="parquet" 分支：pyarrow/polars/spark 写 Parquet（本地或 S3）
    if storage_backend == "parquet":
        return _table_write_parquet(path, df_or_rows, cfg, engine_backend, fields, spark)

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
            _ensure_parent_dir(p)
            df.write.mode("overwrite").parquet(p)
        else:
            _ensure_parent_dir(path)
            df.write.mode("overwrite").option("header", True).csv(path)
        return n
    elif backend == "polars":
        import polars as pl  # noqa: F401  lazy import

        fmt = cfg.get("engine", {}).get("format", "csv")
        if fmt == "parquet":
            p = path if path.endswith(".parquet") else path + ".parquet"
            compression = cfg.get("engine", {}).get("polars", {}).get("parquet_compression", "zstd")
            _ensure_parent_dir(p)
            df_or_rows.write_parquet(p, compression=compression)
            return df_or_rows.height
        else:
            _ensure_parent_dir(path)
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
    # 原子写：先写临时文件再 os.replace，避免崩溃留下半截 JSON
    # （latest.json / metrics.json 的读取方会把解析失败当批次缺失）。
    # tmp 名含 pid+uuid 唯一化：并发双进程写同一目标（如 latest.json）
    # 时不会碰撞同一 tmp 互相覆盖（2026-08 审查 B10 同批加固）。
    _ensure_parent_dir(path)
    tmp = f"{path}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def copy_file(src: str, dst: str) -> str:
    _ensure_parent_dir(dst)
    shutil.copy2(src, dst)
    return sha256_of(dst)


def detect_spark_paths() -> dict[str, str]:
    """探测 SPARK_HOME / JAVA_HOME / HADOOP_HOME / PYSPARK_PYTHON.

    优先级：环境变量 > 系统常见路径 > 平台默认路径（回退）。
    从 tests/conftest.py 下沉为产品工具函数：Spark 集群基准等非 pytest
    入口（如 tools/bench_scale_cluster.py）需要同样的 Driver 环境引导.
    """
    env = os.environ

    def find_path(candidates: list[str]) -> str:
        for c in candidates:
            if os.path.isdir(c):
                return c
        return candidates[0] if candidates else ""

    result: dict[str, str] = {}
    result["SPARK_HOME"] = env.get("SPARK_HOME") or find_path(
        ["/opt/spark", "/usr/local/spark", "C:\\spark", "F:\\spark_home"]
    )
    result["JAVA_HOME"] = (
        env.get("JAVA_HOME")
        or env.get("JAVA_HOME_17_X64")
        or find_path(
            [
                "/usr/lib/jvm/java-17-openjdk-amd64",
                "/usr/lib/jvm/java-17-openjdk-x86_64",
                "/usr/lib/jvm/default-java",
                "/usr/local/opt/openjdk",
                "C:\\Program Files\\Java\\jdk-17",
                "C:\\Program Files\\Java\\jdk17",
                "F:\\jdk17",
            ]
        )
    )
    result["HADOOP_HOME"] = env.get("HADOOP_HOME") or find_path(
        ["/opt/hadoop", "/usr/local/hadoop", "C:\\hadoop", "F:\\hadoop"]
    )
    result["PYSPARK_PYTHON"] = (
        env.get("PYSPARK_PYTHON")
        or env.get("PYTHON")
        or shutil.which("python3")
        or shutil.which("python")
        or ""
    )
    result["PYSPARK_DRIVER_PYTHON"] = env.get("PYSPARK_DRIVER_PYTHON") or result["PYSPARK_PYTHON"]
    return result


def apply_spark_env(spark_paths: dict[str, str]) -> None:
    """按探测结果设置环境变量（setdefault 不覆盖已有值），PATH 前置 bin 目录.

    探测回退会给出不存在的占位路径（如 POSIX 风格 /opt/hadoop）——这类值一旦
    注入会让 Driver JVM 以 'Hadoop home directory ... is not an absolute path'
    拒绝写文件（2026-08 亿行基准实测），故目录不存在时跳过不设.

    Windows 上还需把 HADOOP_HOME\\bin 前置到 PATH，使 Spark NativeCodeLoader
    能找到 hadoop.dll（NativeIO JNI 方法由此提供）。
    """
    for key, value in spark_paths.items():
        if not value:
            continue
        if key in ("SPARK_HOME", "HADOOP_HOME", "JAVA_HOME") and not os.path.isdir(value):
            continue
        os.environ.setdefault(key, value)
    # Windows: prepend HADOOP_HOME/bin to PATH so NativeCodeLoader finds hadoop.dll
    if os.name == "nt" and spark_paths.get("HADOOP_HOME"):
        hadoop_bin = os.path.join(spark_paths["HADOOP_HOME"], "bin")
        if os.path.isdir(hadoop_bin):
            current = os.environ.get("PATH", "")
            if hadoop_bin not in current:
                os.environ["PATH"] = hadoop_bin + os.pathsep + current


def rmtree_retry(path: str, attempts: int = 4, base_delay: float = 0.3) -> bool:
    """带重试的递归删除，返回是否成功删净.

    Windows 下 SQLite（Iceberg catalog）与 Spark JVM 的文件句柄释放有延迟，
    一次性 rmtree 常因句柄未关而失败；测试 fixture teardown 若静默吞掉失败
    会在盘根积累临时目录。指数退避重试给句柄释放留出时间；已不存在的路径
    视为成功。全部尝试耗尽仍失败时返回 False，由调用方决定兜底策略。
    """
    import time

    for i in range(attempts):
        try:
            shutil.rmtree(path)
            return True
        except FileNotFoundError:
            return True
        except OSError:
            if i < attempts - 1:
                time.sleep(base_delay * (i + 1))
    try:
        shutil.rmtree(path)
        return True
    except OSError:
        return False


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

    close 后置 ``_closed`` 标志：close 幂等（双重 close 安全），close 后再
    emit 为安全 no-op（原实现会因写已关闭文件句柄抛 ValueError，异常清理
    路径上容易产生二次异常）.
    """

    def __init__(self, path: str, batch_id: str = "", stage: str = "pipeline"):
        _ensure_parent_dir(path)
        self.path = path
        self.batch_id = batch_id
        self.stage = stage
        self._closed = False
        self._fh = open(path, "a", encoding="utf-8")

    def emit(self, level: str, msg: str, **extra: Any) -> None:
        if self._closed:
            # close 后 emit 安全 no-op：不再写已关闭的文件句柄（原行为抛
            # ValueError）。丢弃的日志属于生命周期结束后的尾随调用.
            return
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
        if self._closed:
            # 幂等：双重 close 安全 no-op
            return
        self._closed = True
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
