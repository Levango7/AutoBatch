"""Pipeline entry: orchestrate stages, per-stage logging, status, failure locating."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import shutil
import sys
import threading
import time
import traceback
from typing import Any, Callable, Optional

try:
    # 配置校验是可选增强：pydantic 未安装时跳过校验，保持核心路径零第三方依赖
    from .config_schema import ConfigValidationError, validate_config
except ImportError:  # pragma: no cover - 仅在无 pydantic 的最小环境触发
    ConfigValidationError = None  # type: ignore[assignment,misc]
    validate_config = None  # type: ignore[assignment]
from .exceptions import StageExecutionError, StageTimeoutError
from .helpers import (
    VERSION,
    PipelineContext,
    StageLog,
    _apply_spark_base_config,
    _get_engine_backend,
    _table_exists,
    abs_path,
    batch_id_new,
    json_load,
    json_save,
    table_read,
    table_write,
)
from .lineage import Manifest, save_latest_pointer
from .logging_setup import close_logging, setup_logging
from .metrics import MetricsRecorder
from .monitoring import HealthServer, MetricsSampler, check_alerts, load_monitoring_config
from .openlineage import OpenLineageEmitter
from .state import StateStore

STAGES = ["ingest", "validate", "clean", "compute", "output"]

# Stage 输出目录前缀映射（任务39 幂等性保证）.
# 每个 stage 把产物写到 run/<batch>/<NN>_<name>/ 下；重试前清理这些目录
# 确保不残留部分产物.**注意**：state/ 目录由 incremental 模式管理，
# 永远不在此清理（水位文件必须跨批次保留）.
# validate stage 除 02_valid/ 外还写 quarantine/（隔离坏行）与 report/
# （质量报告），重试前必须一并清理，否则上次失败的坏行/报告残留会污染
# 本次结果。其他 stage 仅写单一目录，list 仅含一项。
_STAGE_OUTPUT_DIRS: dict[str, list[str]] = {
    "ingest": ["01_raw"],
    "validate": ["02_valid", "quarantine", "report"],
    "clean": ["03_clean"],
    "compute": ["04_aggregates"],
    "output": ["05_output"],
}

# Aggregate merge spec: (name, fields, key_cols) for each aggregate product
# written to 04_aggregates/ by the compute stage. After a successful batch the
# pipeline merges these into state/aggregates/ so the next incremental run has
# the full historical view. See docs/evolution.md §3.3.4 / §3.3.5.
_AGGREGATE_SPECS = [
    (
        "daily_sales",
        ["order_date", "orders", "units", "revenue", "avg_order_value"],
        ["order_date"],
    ),
    ("category_stats", ["category", "orders", "units", "revenue", "revenue_share"], ["category"]),
    ("region_channel_stats", ["region", "channel", "orders", "revenue"], ["region", "channel"]),
    (
        "customer_value",
        ["customer_id", "tier", "city", "orders", "revenue", "rank"],
        ["customer_id"],
    ),
    ("customer_tier", ["tier", "customers", "revenue"], ["tier"]),
]


def config_digest(cfg: dict[str, Any]) -> str:
    raw = json.dumps(cfg, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_stage(name: str):
    # __package__ 使包名不再硬编码为 "src"（以 autobatch 名安装时同样可用）
    return importlib.import_module(f"{__package__}.stages." + name)


def _init_spark_session(cfg: dict[str, Any], logger) -> Any:
    """按 cfg["engine"]["spark"] 创建 SparkSession（lazy import pyspark）。

    仅在 ``backend="spark"`` 时由 ``run_pipeline`` 调用。读取的配置项参见
    docs/evolution.md §4.3.2.3 / §4.4.2.2：
        master / app_name / executor_memory / executor_cores / num_executors /
        driver_memory / shuffle_partitions / adaptive_query_execution

    额外注入 ``spark.hadoop.io.nativeio=false`` 以尝试绕过 Windows 上
    hadoop.dll 缺失导致的 NativeIO$Windows.access0 JNI 错误（在缺
    hadoop.dll 的环境下写文件仍会失败，但内存操作可正常进行）。

    Phase 2b 多机模式新增配置项：
        S3/MinIO connector（当 storage.backend="parquet" 且有 endpoint 时注入）：
            spark.hadoop.fs.s3a.endpoint / fs.s3a.access.key / fs.s3a.secret.key /
            fs.s3a.path.style.access / fs.s3a.impl
        Driver ↔ Worker 反向连接（当 engine.spark.cluster.enabled=true 时注入）：
            spark.driver.bindAddress=0.0.0.0 / spark.driver.host
            （driver_host 缺省 "host.docker.internal"，可通过 cluster.driver_host 覆盖）

    Args:
        cfg: Pipeline 配置 dict。
        logger: logging.Logger。

    Returns:
        pyspark.sql.SparkSession。
    """
    from pyspark.sql import SparkSession  # lazy import：仅 spark 路径需要

    scfg = cfg.get("engine", {}).get("spark", {}) or {}
    builder = SparkSession.builder
    # 基础配置（appName/master/资源/AQE）抽到 helpers._apply_spark_base_config，
    # 与 helpers._get_spark_session 共享同一份配置项，避免两处重复维护。
    builder = _apply_spark_base_config(builder, scfg)
    # 尝试绕过 Windows NativeIO（缺 hadoop.dll 时写文件仍会失败，但内存操作可进行）
    builder = builder.config("spark.hadoop.io.nativeio", "false")

    # --- S3/MinIO connector（Phase 2b 多机模式）---
    # 当 storage.backend="parquet" 且配置了 endpoint 时，注入 hadoop-aws S3A connector，
    # 使 Spark 可通过 s3a:// URI 读写 MinIO 上的 Parquet 文件。
    # 不影响 local_csv 模式（backend 不是 parquet 时跳过）。
    #
    # 多机模式下，Spark Worker 在 Docker 容器中运行，需要通过 Docker 内部网络
    # （如 minio:9000）访问 MinIO；而 Driver 在宿主机上通过 localhost:9000 访问。
    # 因此引入 cluster.s3_endpoint 配置项：多机模式下 s3a endpoint 优先使用
    # cluster.s3_endpoint（Worker 可达的地址），缺省回退到 storage.endpoint。
    # storage.endpoint 始终用于 Driver 端 pyarrow 操作（_table_exists 等）。
    storage = cfg.get("storage", {})
    cluster = scfg.get("cluster", {})
    if storage.get("backend") == "parquet" and storage.get("endpoint"):
        # 多机模式：s3a endpoint 优先用 cluster.s3_endpoint（Docker 内部地址）
        if cluster.get("enabled") and cluster.get("s3_endpoint"):
            s3a_endpoint = cluster["s3_endpoint"]
        else:
            s3a_endpoint = storage["endpoint"]
        # 去掉 scheme 前缀
        s3a_endpoint_clean = s3a_endpoint.replace("http://", "").replace("https://", "")
        scheme = "https" if storage.get("secure") else "http"
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"{scheme}://{s3a_endpoint_clean}")
        builder = builder.config("spark.hadoop.fs.s3a.access.key", storage.get("access_key", ""))
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", storage.get("secret_key", ""))
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
        # 多机模式下，Worker 容器和 Driver 端都需要 hadoop-aws + aws-sdk JAR。
        # 前提：这些 JAR 已预装在 SPARK_HOME/jars/（Driver）和 Docker 容器的
        # /opt/spark/jars/（Worker）中。不使用 spark.jars 分发（aws-java-sdk-bundle
        # 388MB 传输会超时/OOM）。

    # --- Driver ↔ Worker 反向连接（多机模式）---
    # 当 engine.spark.cluster.enabled=true 时，Driver 绑定 0.0.0.0 并通过
    # driver_host 暴露给 Docker 容器中的 Worker，使 Worker 可反向连接 Driver。
    # cluster.enabled=false（缺省）时不注入，不影响本地单机模式。
    if cluster.get("enabled"):
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

    # --- Phase 5: Spark + Iceberg 三合一 ---
    # 当 storage.backend="iceberg" 时，注入 IcebergSparkSessionExtensions +
    # SparkCatalog 配置，使 spark.read.table("catalog.ns.tbl") / df.writeTo(...)
    # 能路由到 Iceberg 表. 详见 docs/evolution.md §6.x（Phase 5）.
    #
    # 关键约束：Iceberg 官方 JAR 最高支持 Spark 4.1（不支持 4.2）.
    # 推荐组合：Spark 4.1.0 + Iceberg 1.11.0（Scala 2.13）.
    # 若 SPARK_HOME/jars/ 缺 iceberg-spark-runtime JAR，spark.read.table 会抛
    # ClassNotFoundException，由测试层 skipif 跳过，不阻塞回归.
    #
    # 表名格式：Spark 用 catalog.namespace.table（catalog 前缀），
    # pyiceberg 用 namespace.table（无前缀），由 helpers._iceberg_spark_full_name 统一.
    if storage.get("backend") == "iceberg":
        ice_cfg = storage.get("iceberg", {}) or {}
        catalog_name = ice_cfg.get("catalog_name", "autobatch")
        # spark.sql.extensions：Iceberg SQL 扩展（DFv2 writeTo / MERGE INTO 等）
        spark_extensions = ice_cfg.get(
            "spark_extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        builder = builder.config("spark.sql.extensions", spark_extensions)
        # spark.sql.catalog.<name>：注册 Iceberg catalog
        spark_catalog_class = ice_cfg.get(
            "spark_catalog_class",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        builder = builder.config(f"spark.sql.catalog.{catalog_name}", spark_catalog_class)
        # catalog 类型：rest（生产）/ sql（开发测试）/ hive（兼容 Hive Metastore）
        builder = builder.config(
            f"spark.sql.catalog.{catalog_name}.type",
            ice_cfg.get("catalog_type", "rest"),
        )
        builder = builder.config(
            f"spark.sql.catalog.{catalog_name}.uri",
            ice_cfg.get("catalog_uri", ""),
        )
        builder = builder.config(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            ice_cfg.get("warehouse", ""),
        )
        # S3/MinIO 访问：复用前面注入的 hadoop-aws S3A connector.
        # 当 catalog_type="rest" 且 warehouse="s3://..." 时，Iceberg REST
        # server 通过 S3A 访问数据文件，需要 fs.s3a.* 配置（已在 parquet
        # 分支注入；iceberg 模式下若 endpoint 非空也注入，使 S3A 可用）.
        if storage.get("endpoint"):
            s3a_endpoint = storage["endpoint"]
            if cluster.get("enabled") and cluster.get("s3_endpoint"):
                s3a_endpoint = cluster["s3_endpoint"]
            s3a_endpoint_clean = s3a_endpoint.replace("http://", "").replace("https://", "")
            scheme = "https" if storage.get("secure") else "http"
            builder = builder.config(
                "spark.hadoop.fs.s3a.endpoint",
                f"{scheme}://{s3a_endpoint_clean}",
            )
            builder = builder.config(
                "spark.hadoop.fs.s3a.access.key", storage.get("access_key", "")
            )
            builder = builder.config(
                "spark.hadoop.fs.s3a.secret.key", storage.get("secret_key", "")
            )
            builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
            builder = builder.config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            builder = builder.config("spark.hadoop.fs.s3a.fast.upload", "true")
            builder = builder.config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
            builder = builder.config(
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
            )

    spark = builder.getOrCreate()
    logger.info(
        "spark session created",
        extra={
            "stage": "pipeline",
            "master": scfg.get("master", "local[*]"),
            "app": scfg.get("app_name", "autobatch"),
        },
    )
    return spark


# ---------------------------------------------------------------------------
# 任务39 错误处理加固：stage 级 try-except + 重试 + 超时 + 幂等
# ---------------------------------------------------------------------------
# 设计目标（向后兼容优先级最高）：
#   1. error_handling 段缺省 / max_retries=0 时，行为与原 pipeline 完全一致
#      （单次执行，失败即 break，不重试不清理）.
#   2. max_retries>0 时，stage 失败后按指数退避 sleep 后重试，重试前可选
#      清理该 stage 输出目录（cleanup_on_retry=true）确保幂等.
#   3. stage_timeouts[stage] 限制单次 stage 执行墙钟时间，超时抛 StageTimeoutError.
#      用工作线程 + threading.Event.wait(timeout) 实现（Windows 不支持 signal.alarm）.
#   4. 重试耗尽后抛 StageExecutionError，携带 stage_name/batch_id/attempt/
#      original_error/traceback_str 上下文，由 run_pipeline 捕获并记录 failed 状态.
#
# 关键不变量：
#   - state/ 目录永不清理（增量模式水位必须跨批次保留）.
#   - 首次执行也清理输出目录（cleanup_on_retry=true 时），确保重复运行同批次
#     不产生残留产物（幂等性）.
#   - 重试日志用 structured logging（任务38 的 logger.info/warning/error + extra）.


def _cleanup_stage_output(stage_name: str, run_dir: str, logger) -> None:
    """删除指定 stage 的输出目录（幂等性保证）.

    清理 run/<batch>/<NN>_<stage>/ 目录（见 _STAGE_OUTPUT_DIRS 映射）.
    一个 stage 可能写多个目录（如 validate 写 02_valid/ + quarantine/ +
    report/），全部清理确保重试幂等。**永不清理** state/ 目录（增量模式
    水位文件必须保留）. 缺省 stage 不在映射中时静默跳过（向后兼容自定义 stage）.

    Args:
        stage_name:  stage 名（ingest/validate/clean/compute/output）.
        run_dir:     批次运行目录绝对路径.
        logger:      logging.Logger，用于记录清理动作.
    """
    subs = _STAGE_OUTPUT_DIRS.get(stage_name)
    if not subs:
        return
    for sub in subs:
        target = os.path.join(run_dir, sub)
        if not os.path.exists(target):
            continue
        try:
            shutil.rmtree(target)
            logger.info(
                "stage output cleaned for retry", extra={"stage": stage_name, "cleaned_dir": sub}
            )
        except Exception as exc:  # noqa: BLE001
            # 清理失败不应阻塞重试；记录 warning 后继续.
            logger.warning(
                "stage output cleanup failed, continuing retry",
                extra={
                    "stage": stage_name,
                    "cleaned_dir": sub,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )


def _run_with_timeout(
    fn: Callable[[], Any],
    timeout_seconds: Optional[float],
    stage_name: str,
    batch_id: str,
    attempt: int,
) -> Any:
    """在墙钟超时控制下执行 fn.

    fn 在独立 daemon 线程中运行，主线程用 ``Event.wait(timeout)`` 等待；
    超时后放弃等待并抛 StageTimeoutError（daemon 线程随主进程退出，
    Python 无法强杀线程——若 fn 持有临界资源，由运维层面介入）。
    兼容 Windows（不支持 signal.alarm）。

    timeout_seconds=None / <=0 时不启用超时，直接在当前线程执行 fn（向后兼容，
    也避免无谓的线程开销）.

    Args:
        fn:              无参可调用，封装了 stage 执行逻辑.
        timeout_seconds: 超时阈值（秒），None 或 <=0 表示不限制.
        stage_name:      stage 名（用于异常上下文）.
        batch_id:        批次 ID（用于异常上下文）.
        attempt:         当前 attempt 序号（用于异常上下文）.

    Returns:
        fn() 的返回值.

    Raises:
        StageTimeoutError: 超时（fn 未在阈值内完成）.
        Exception:        fn 抛出的任何异常原样向上传播.
    """
    if not timeout_seconds or timeout_seconds <= 0:
        return fn()

    result_holder: dict[str, Any] = {}
    done_event = threading.Event()

    def _runner():
        try:
            result_holder["value"] = fn()
        except Exception as exc:  # noqa: BLE001
            # 仅捕获 Exception；KeyboardInterrupt/SystemExit 不入 holder
            result_holder["error"] = exc
        finally:
            done_event.set()

    start = time.monotonic()
    worker = threading.Thread(target=_runner, daemon=True, name=f"stage-{stage_name}")
    worker.start()
    if not done_event.wait(timeout=timeout_seconds):
        elapsed = time.monotonic() - start
        raise StageTimeoutError(stage_name, batch_id, attempt, timeout_seconds, elapsed)
    if "error" in result_holder:
        raise result_holder["error"]
    return result_holder["value"]


def _run_stage_with_retry(
    stage_name: str,
    stage_fn: Callable[[Any, Any], dict[str, Any]],
    ctx: PipelineContext,
    slog: StageLog,
    cfg: dict[str, Any],
    logger,
) -> dict[str, Any]:
    """带重试 + 超时 + 幂等清理的 stage 执行包装.

    流程：
        1. 读取 error_handling 配置（缺省 max_retries=0，行为与原 pipeline 一致）.
        2. attempt 0..max_retries：
            a. 若 cleanup_on_retry=true，清理该 stage 输出目录（首次也清理，
               确保重复运行同批次不产生残留）.
            b. 用 _run_with_timeout 包裹 stage_fn(ctx, slog) 执行.
            c. 成功 → 返回 summary.
            d. 失败 → 记录结构化日志（stage/batch/attempt/error/traceback），
               若还有 retries 剩余，计算退避时间 sleep 后继续；否则抛
               StageExecutionError.

    退避公式：min(backoff_base * 2^attempt, backoff_max)
        attempt=0 失败后等待 backoff_base * 1 = backoff_base 秒
        attempt=1 失败后等待 backoff_base * 2 秒
        attempt=2 失败后等待 backoff_base * 4 秒 ... 上限 backoff_max.

    Args:
        stage_name:  stage 名.
        stage_fn:    stage 模块的 run(ctx, slog) 函数.
        ctx:         PipelineContext.
        slog:        StageLog 实例（已打开）.
        cfg:         Pipeline 配置 dict（顶层）.
        logger:      logging.Logger.

    Returns:
        stage 的 summary dict（含 rows_in/rows_out/lineage 等）.

    Raises:
        StageExecutionError: 重试耗尽仍失败.
        StageTimeoutError:   超时（是 StageExecutionError 子类）.
    """
    eh = cfg.get("error_handling", {}) or {}
    max_retries = int(eh.get("max_retries", 0) or 0)
    backoff_base = float(eh.get("backoff_base_seconds", 2) or 2)
    backoff_max = float(eh.get("backoff_max_seconds", 60) or 60)
    cleanup_on_retry = bool(eh.get("cleanup_on_retry", True))
    timeouts = eh.get("stage_timeouts", {}) or {}
    timeout_s = timeouts.get(stage_name)
    # 显式 None / 0 / 负值 → 不限制
    if timeout_s is not None:
        timeout_s = float(timeout_s)
        if timeout_s <= 0:
            timeout_s = None

    batch_id = ctx.batch_id
    last_exc: Optional[Exception] = None
    last_tb = ""

    for attempt in range(max_retries + 1):
        # 幂等性：首次执行与每次重试前都清理输出目录（若启用）.
        # state/ 目录由 _cleanup_stage_output 保证不清理.
        if cleanup_on_retry:
            _cleanup_stage_output(stage_name, ctx.run_dir, logger)

        try:
            summary = _run_with_timeout(
                lambda: stage_fn(ctx, slog),
                timeout_s,
                stage_name,
                batch_id,
                attempt,
            )
            if attempt > 0:
                logger.info(
                    "stage succeeded after retry",
                    extra={"stage": stage_name, "batch": batch_id, "attempt": attempt},
                )
                slog.info("stage succeeded after retry", attempt=attempt)
            return summary
        except Exception as exc:  # noqa: BLE001
            # 仅捕获 Exception，让 KeyboardInterrupt/SystemExit 正常传播
            # （重试逻辑不应吞掉用户主动中断信号）。
            last_exc = exc
            last_tb = traceback.format_exc()
            err_msg = f"{type(exc).__name__}: {exc}"
            # 结构化日志：stage / batch / attempt / error / traceback
            logger.error(
                "stage attempt failed",
                extra={
                    "stage": stage_name,
                    "batch": batch_id,
                    "attempt": attempt,
                    "max_retries": max_retries,
                    "error": err_msg,
                },
            )
            slog.error(
                "stage attempt failed",
                attempt=attempt,
                max_retries=max_retries,
                error=err_msg,
                traceback=last_tb,
            )

            if attempt >= max_retries:
                # 重试耗尽，跳出循环抛 StageExecutionError
                break

            # 计算指数退避：min(base * 2^attempt, max)
            backoff = min(backoff_base * (2**attempt), backoff_max)
            logger.info(
                "stage retry scheduled",
                extra={
                    "stage": stage_name,
                    "batch": batch_id,
                    "attempt": attempt,
                    "backoff_seconds": backoff,
                },
            )
            slog.info("stage retry scheduled", attempt=attempt, backoff_seconds=backoff)
            time.sleep(backoff)

    # 重试耗尽仍失败 → 抛 StageExecutionError（携带完整上下文）。
    # 超时例外：StageTimeoutError 是 StageExecutionError 子类，保持原类型
    # 向上传播（docstring 契约），供调用方区分"超时"与"执行失败"。
    assert last_exc is not None  # 循环至少执行一次，last_exc 必被赋值
    if isinstance(last_exc, StageTimeoutError):
        raise last_exc
    raise StageExecutionError(
        stage_name=stage_name,
        batch_id=batch_id,
        attempt=max_retries,
        original_error=last_exc,
        traceback_str=last_tb,
    )


# ---------------------------------------------------------------------------
# 断点续跑（resume）：同 batch_id 失败重跑时跳过已成功且产物仍在的 stage.
#
# 前置条件（全部满足才启用，否则静默走全量路径）：
#   1. error_handling.resume=true（缺省 false，向后兼容 100%）
#   2. 显式传入 batch_id（非 auto）且 run/<batch_id>/manifest.json 存在
#   3. 上次 manifest status=="failed"（成功的批次重跑视为全新执行）
#   4. pipeline_version 与 config_digest 与上次一致（配置漂移禁止续跑）
#
# 跳过判据：上次该 stage status=="success" 且其**主输出目录**存在且非空
# （_STAGE_OUTPUT_DIRS 第一项；quarantine/report 等终端产物目录不参与判据，
# 干净数据下 quarantine 为空是正常态）。output 阶段永不跳过——它是血缘/产物
# 登记的汇总点，重跑成本低且必须基于最新 ctx 重建 dashboard 与 lineage 边。
#
# 恢复的状态：quality（validate 写入 manifest）、source、lineage 边
# （output 的 _register_edges 按 target 键覆盖写，恢复旧边后重跑 output
# 会幂等重建）。水位不涉及——失败路径本就不推进水位（两阶段提交），
# 续跑成功后由 _advance_and_merge 正常推进一次。
# ---------------------------------------------------------------------------
# 必需文件相对 run_dir 根目录（quality_summary.json 由 validate 写在批次根，
# 见 stages/validate.py 的 json_save(ctx.run_dir, ...)）
_RESUME_MIN_FILES = {"validate": ["quality_summary.json"]}


def _load_resume_plan(
    cfg: dict[str, Any],
    batch_id: str,
    run_dir: str,
    digest: str,
    logger,
) -> Optional[dict[str, Any]]:
    """返回可续跑的上次 manifest dict；不可续跑返回 None 并说明原因."""
    eh = cfg.get("error_handling", {}) or {}
    if not bool(eh.get("resume", False)):
        return None
    if not batch_id or batch_id == "auto":
        return None
    manifest_path = os.path.join(run_dir, "manifest.json")
    if not os.path.isfile(manifest_path):
        return None
    prev = json_load(manifest_path)
    if not isinstance(prev, dict) or prev.get("status") != "failed":
        return None
    if prev.get("pipeline_version") != VERSION:
        if logger is not None:
            logger.info(
                "resume skipped: pipeline version changed",
                extra={"stage": "pipeline", "batch": batch_id},
            )
        return None
    if prev.get("config_digest") != digest:
        if logger is not None:
            logger.info(
                "resume skipped: config changed since failed run",
                extra={"stage": "pipeline", "batch": batch_id},
            )
        return None
    return prev


def _stage_outputs_intact(stage: str, run_dir: str) -> bool:
    """stage 的主输出目录非空且必需文件在位 → True.

    只校验第一个（主）输出子目录——下游 stage 仅消费它；其余子目录是终端
    产物（如 validate 的 quarantine/report），不参与续跑判据：干净数据下
    quarantine 为空目录是正常态，若据此回退全量，resume 会在最常见的
    零坏行场景静默失效（2026-08 审计 P0）。
    _RESUME_MIN_FILES 的路径相对 **run_dir 根目录**（如 quality_summary.json
    由 validate 写在批次根目录，见 stages/validate.py），而非主输出子目录内。
    """
    dirs = _STAGE_OUTPUT_DIRS.get(stage, [])
    if not dirs:
        return True
    base = os.path.join(run_dir, dirs[0])
    if not os.path.isdir(base) or not os.listdir(base):
        return False
    for f in _RESUME_MIN_FILES.get(stage, []):
        if not os.path.isfile(os.path.join(run_dir, f)):
            return False
    return True


def run_pipeline(cfg: dict[str, Any], batch_id: str, fail_at: str) -> int:
    run_root = abs_path(cfg["pipeline"].get("run_dir", "run"))
    os.makedirs(run_root, exist_ok=True)
    if not batch_id or batch_id == "auto":
        batch_id = batch_id_new()
    run_dir = os.path.join(run_root, batch_id)
    os.makedirs(run_dir, exist_ok=True)

    # 任务38 日志规范化：在批次开始时配置 root logger，输出到
    # run/<batch>/logs/pipeline.log + 控制台，支持 text/json 双格式，
    # 注入 batch_id 关联所有 stage 日志（运行追踪 ID）.
    # 优先读 cfg["logging"]（新段），回退 cfg["monitoring"]["log_level"]（向后兼容）.
    logging_cfg = cfg.get("logging", {})
    log_fmt = logging_cfg.get("format", "text")
    log_level = logging_cfg.get("level", cfg.get("monitoring", {}).get("log_level", "INFO"))
    logger = setup_logging(batch_id, run_dir, fmt=log_fmt, level=log_level)

    # config_digest 只算一次：Manifest 与 resume 校验共用同一摘要
    digest = config_digest(cfg)
    manifest = Manifest(batch_id, digest, run_dir)
    ctx = PipelineContext(config=cfg, run_dir=run_dir, batch_id=batch_id, manifest=manifest)
    # 指标记录器前移到 resume 逻辑之前：续跑恢复的 stage 也要写入本轮 metrics，
    # 否则 metrics.json 缺这些阶段的行数/耗时，跨批次对比失真.
    metrics = MetricsRecorder(batch_id)
    # Phase 2a/2b: 从 cfg 同步 engine_backend 到 ctx，使各 stage 据此走 python/polars/spark 路径。
    # 见 docs/evolution.md §4.3.1.1（polars）/ §4.3.2.1（spark）。缺省 "python"
    # 保持向后兼容；"polars" 走列式路径；"spark" 走分布式路径。
    ctx.engine_backend = _get_engine_backend(cfg)

    # --- Resume (断点续跑) ---
    resumed_stages = set()
    resume_active = False
    prev = _load_resume_plan(cfg, batch_id, run_dir, digest, logger)
    if prev is not None:
        resume_active = True
        # 恢复 source
        if prev.get("source"):
            manifest.set_source(prev["source"].get("name", ""), prev["source"].get("files", []))
        # 恢复 quality
        if prev.get("quality"):
            manifest.set_quality(prev["quality"])
        # 恢复已成功的 stages（除 output 外）
        for st in prev.get("stages", []):
            if st.get("status") == "success" and st.get("name") != "output":
                stage_name = st["name"]
                # 检查产物是否完整
                if _stage_outputs_intact(stage_name, run_dir):
                    resumed_stages.add(stage_name)
                    # 复制 stage 记录到当前 manifest
                    extra = st.get("extra", {}) or {}
                    extra["resumed"] = True
                    # lineage_decl 由 add_stage(extra={...}) 平铺到 stage entry 顶层，
                    # 也可能嵌套在 extra 里（两种写法都兼容）.
                    lineage_decl = st.get("lineage_decl") or extra.get("lineage_decl", {})
                    if lineage_decl:
                        for target, ups in lineage_decl.items():
                            # 快照是全量累积视图：按 target 覆盖写，与正常运行期
                            # 的合并语义一致。用 extend 会因多个续跑 stage 携带
                            # 同一份快照而把每条边的上游重复叠加 N 次.
                            ctx.lineage_decls[target] = list(ups)
                    metrics.record_stage(
                        stage_name,
                        st.get("status", "success"),
                        st.get("duration_ms", 0),
                        st.get("rows_in", 0),
                        st.get("rows_out", 0),
                    )
                    manifest.add_stage(
                        stage_name,
                        st["status"],
                        st.get("rows_in", 0),
                        st.get("rows_out", 0),
                        st.get("duration_ms", 0),
                        st.get("log", ""),
                        st.get("error"),
                        extra=extra,
                    )
                else:
                    # 产物不完整，不能续跑，回退全量执行
                    resume_active = False
                    resumed_stages.clear()
                    # 重置 manifest/ctx/metrics 为全新状态——恢复循环可能已把
                    # 前序 stage 写进 metrics，不重置会导致全量重跑后 stages 重复
                    manifest = Manifest(batch_id, digest, run_dir)
                    ctx = PipelineContext(config=cfg, run_dir=run_dir, batch_id=batch_id, manifest=manifest)
                    ctx.engine_backend = _get_engine_backend(cfg)
                    metrics = MetricsRecorder(batch_id)
                    # 后续 spark 初始化会使用新的 ctx
                    break
        if resume_active:
            logger.info("resume enabled, skipping stages", extra={"stage": "pipeline", "resumed": list(resumed_stages)})
        else:
            logger.info("resume disabled, full run", extra={"stage": "pipeline"})
    else:
        logger.info("full run (no resume)", extra={"stage": "pipeline"})

    # --- 初始化窗口（spark / OpenLineage / 增量状态 / 监控健康服务）---
    # 统一包进内层 try：此窗口内任何异常都必须先释放已建资源再原样抛出——
    # 外层主 try 的 finally 此时还未进入，不清理会泄漏 SparkSession /
    # HealthServer 线程，且 OpenLineage 会留下悬空 START（2026-08 审计 P1）。
    spark: Optional[Any] = None
    health_server: Optional[HealthServer] = None
    emitter: Optional[OpenLineageEmitter] = None
    store: StateStore | None = None
    try:
        # Phase 2b: backend="spark" 时初始化 SparkSession 并存入 ctx.spark_session，
        # 各 stage 通过 ctx.spark_session 访问，避免重复创建。参见 §4.4.2.2。
        if ctx.engine_backend == "spark":
            spark = _init_spark_session(cfg, logger)
            ctx.spark_session = spark

        # OpenLineage：默认关闭（openlineage.enabled=false），启用后不影响主流程.
        ol_cfg = cfg.get("openlineage", {}) or {}
        if bool(ol_cfg.get("enabled", False)):
            emitter = OpenLineageEmitter(
                batch_id,
                namespace=str(ol_cfg.get("namespace", "autobatch")),
                endpoint=str(ol_cfg.get("endpoint", "")).strip(),
                out_path=os.path.join(run_dir, "openlineage.ndjson"),
                logger=logger,
            )
            emitter.pipeline_event("START")

        # Incremental mode: load cross-batch state.json into ctx.state. The store
        # is constructed unconditionally so the success path can commit even if
        # state.json did not exist yet (first run builds the watermark). See
        # docs/evolution.md §3.3.1 / §3.3.5.
        inc_cfg = cfg.get("incremental", {})
        incremental_enabled = bool(inc_cfg.get("enabled", False))
        if incremental_enabled:
            state_dir = abs_path(inc_cfg.get("state_dir", "state"))
            store = StateStore(state_dir)
            ctx.state = store.load()
            ctx.state_path = store.state_path
            ctx.incremental_enabled = True
            logger.info("incremental mode enabled", extra={"stage": "pipeline", "state_dir": state_dir})

        # 任务41 监控告警：加载 config/monitoring.json（缺省 disabled）.
        monitoring_cfg = load_monitoring_config(
            abs_path(cfg.get("monitoring_config", "config/monitoring.json"))
        )
        monitoring_enabled = bool(monitoring_cfg.get("enabled", False))
        if monitoring_enabled:
            hc_cfg = monitoring_cfg.get("health_check", {}) or {}
            if hc_cfg.get("enabled", False):
                health_server = HealthServer(
                    host=hc_cfg.get("host", "0.0.0.0"),
                    port=int(hc_cfg.get("port", 8086)),
                    run_dir=run_root,
                )
                health_server.start()
                logger.info(
                    "health server started",
                    extra={"stage": "pipeline", "host": health_server.host, "port": health_server.port},
                )
    except Exception:
        # 尽力清理后原样抛出：emitter 已发 START 则补 FAILED 配对终态
        init_error = f"{type(sys.exc_info()[1]).__name__}: {sys.exc_info()[1]}"
        if emitter is not None:
            emitter.pipeline_event("FAILED", error_message=init_error)
        if health_server is not None:
            try:
                health_server.stop()
            except Exception:  # noqa: BLE001 - 清理路径不再叠加异常
                pass
        if spark is not None:
            try:
                spark.stop()
            except Exception:  # noqa: BLE001 - 清理路径不再叠加异常
                pass
        logger.error(
            "pipeline failed during initialization",
            extra={"stage": "pipeline", "error": init_error},
            exc_info=True,
        )
        close_logging()
        raise

    logger.info("pipeline start", extra={"stage": "pipeline", "batch": batch_id})
    overall = "success"
    error_msg = None
    pipeline_start = time.monotonic()

    try:
        for name in STAGES:
            # 断点续跑：跳过已成功的 stage（output 永不跳过）
            if resume_active and name in resumed_stages:
                logger.info("stage resumed (skipped)", extra={"stage": "pipeline", "resumed_stage": name})
                if emitter is not None:
                    emitter.stage_event(name, "COMPLETE")
                continue

            log_path = os.path.join("logs", name + ".jsonl")
            if emitter is not None:
                emitter.stage_event(name, "START")
            if fail_at == name:
                overall = "failed"
                error_msg = "demo failure injected at stage " + name
                with StageLog(
                    os.path.join(run_dir, log_path), batch_id=batch_id, stage=name
                ) as slog:
                    slog.error(error_msg, injected=True)
                manifest.add_stage(name, "failed", 0, 0, 0, log_path, error_msg)
                metrics.record_stage(name, "failed", 0, 0, 0)
                if emitter is not None:
                    emitter.stage_event(name, "FAILED", error_message=error_msg)
                logger.error("stage failed (demo injection)", extra={"stage": name})
                break
            stage_mod = load_stage(name)
            start = time.monotonic()
            try:
                with StageLog(
                    os.path.join(run_dir, log_path), batch_id=batch_id, stage=name
                ) as slog:
                    # 任务39 错误处理加固：用 _run_stage_with_retry 包装 stage 执行，
                    # 提供 try-except + 重试 + 超时 + 幂等清理.
                    # max_retries=0（缺省）时行为与原 pipeline 完全一致.
                    summary = _run_stage_with_retry(name, stage_mod.run, ctx, slog, cfg, logger)
                rows_in = summary.get("rows_in", 0)
                rows_out = summary.get("rows_out", 0)
                dur = int((time.monotonic() - start) * 1000)
                manifest.add_stage(name, "success", rows_in, rows_out, dur, log_path)
                metrics.record_stage(name, "success", dur, rows_in, rows_out)
                # Collect lineage declarations from this stage into the shared map.
                for target, ups in summary.get("lineage", {}).items():
                    ctx.lineage_decls[target] = list(ups)
                if emitter is not None:
                    emitter.stage_event(name, "COMPLETE")
                logger.info(
                    "stage done",
                    extra={"stage": name, "rows_in": rows_in, "rows_out": rows_out, "dur_ms": dur},
                )
            except StageExecutionError as exc:  # noqa: BLE001
                # 任务39：stage 重试耗尽后抛 StageExecutionError，携带完整上下文.
                # 捕获后记录 failed 状态并终止本轮批次（与原行为一致）.
                overall = "failed"
                error_msg = f"{type(exc.original_error).__name__}: {exc.original_error}"
                trace_tail = exc.traceback_str.splitlines()[-8:] if exc.traceback_str else []
                with StageLog(
                    os.path.join(run_dir, log_path), batch_id=batch_id, stage=name
                ) as slog:
                    slog.error(
                        "stage failed",
                        error=error_msg,
                        trace=trace_tail,
                        stage_name=exc.stage_name,
                        batch_id=exc.batch_id,
                        attempt=exc.attempt,
                    )
                dur = int((time.monotonic() - start) * 1000)
                manifest.add_stage(name, "failed", 0, 0, dur, log_path, error_msg)
                metrics.record_stage(name, "failed", dur, 0, 0)
                if emitter is not None:
                    emitter.stage_event(name, "FAILED", error_message=error_msg)
                logger.error(
                    "stage failed (after retries)",
                    extra={
                        "stage": name,
                        "batch": batch_id,
                        "error": error_msg,
                        "attempt": exc.attempt,
                    },
                )
                break
            except Exception as exc:  # noqa: BLE001
                overall = "failed"
                error_msg = f"{type(exc).__name__}: {str(exc)}"
                trace_tail = traceback.format_exc().splitlines()[-8:]
                with StageLog(
                    os.path.join(run_dir, log_path), batch_id=batch_id, stage=name
                ) as slog:
                    slog.error("stage failed", error=error_msg, trace=trace_tail)
                dur = int((time.monotonic() - start) * 1000)
                manifest.add_stage(name, "failed", 0, 0, dur, log_path, error_msg)
                metrics.record_stage(name, "failed", dur, 0, 0)
                if emitter is not None:
                    emitter.stage_event(name, "FAILED", error_message=error_msg)
                logger.error("stage failed", extra={"stage": name, "error": error_msg})

                break

        # 把本轮收集的 lineage_decls 快照写回各 stage 条目，供下次 resume 恢复.
        # 只覆盖成功或失败（已记录）的 stage；output 是最后一个必然执行的 stage.
        for entry in manifest.stages:
            declared = entry.get("extra", {}) or {}
            declared["lineage_decl"] = dict(ctx.lineage_decls)
            entry["extra"] = declared
        manifest.finish(overall, error_msg)
        manifest.save()
        save_latest_pointer(run_root, batch_id, run_dir)
        json_save(
            os.path.join(run_dir, "status.json"),
            {
                "batch_id": batch_id,
                "status": overall,
                "error": error_msg,
                "started_at": manifest.started_at,
                "finished_at": manifest.finished_at,
                "stages": manifest.stages,
            },
        )

        # Two-phase commit: advance watermarks + merge aggregates into state/ ONLY
        # when every stage succeeded. On failure we deliberately skip this block so
        # state.json keeps the old watermark and the next run re-reads the same
        # delta (idempotent retry). See docs/evolution.md §3.3.5.
        if overall == "success" and incremental_enabled and store is not None:
            _advance_and_merge(ctx, store, logger)

        # Finalise and persist metrics.
        total_dur = int((time.monotonic() - pipeline_start) * 1000)
        dq_score = None
        quarantined_rows: dict[str, Any] = {}
        if manifest.quality is not None:
            dq_score = manifest.quality.get("dq_score")
            quarantined_rows = manifest.quality.get("quarantined_rows", {})
        metrics.finish(overall, total_dur, dq_score=dq_score, quarantined_rows=quarantined_rows)
        metrics.save(run_dir)

        # 任务41 监控告警：monitoring.enabled=true 时，
        # 1) 采样当前进程 CPU/内存，追加到 metrics.json
        # 2) 调用 check_alerts 扫描最近 N 个批次，超阈值则 log WARNING
        # enabled=false 时跳过，行为 100% 不变.
        if monitoring_enabled:
            try:
                sampler = MetricsSampler()
                resource_sample = sampler.sample()
                # 把采样结果追加到 metrics.json（不破坏原有结构）
                metrics_path = os.path.join(run_dir, "metrics.json")
                existing = json_load(metrics_path)
                existing["resource_sample"] = resource_sample
                json_save(metrics_path, existing)
                logger.info(
                    "resource sampled",
                    extra={
                        "stage": "pipeline",
                        "cpu_percent": resource_sample.get("cpu_percent"),
                        "memory_mb": resource_sample.get("memory_mb"),
                    },
                )

                alerts = check_alerts(run_root, monitoring_cfg)
                for alert in alerts:
                    logger.warning(
                        "alert: " + alert.message,
                        extra={
                            "stage": "pipeline",
                            "alert_rule": alert.rule,
                            "alert_value": alert.value,
                            "alert_threshold": alert.threshold,
                            "alert_batch_id": alert.batch_id,
                            "alert_stage": alert.stage,
                        },
                    )
                if alerts:
                    logger.warning(
                        "alerts triggered", extra={"stage": "pipeline", "alert_count": len(alerts)}
                    )
                else:
                    logger.info("no alerts", extra={"stage": "pipeline"})
            except Exception:  # noqa: BLE001
                # 监控失败不应影响 pipeline 主流程
                logger.warning(
                    "monitoring check failed, ignoring", extra={"stage": "pipeline"}, exc_info=True
                )

        # OpenLineage 批次终态：与初始化窗口发出的 START 配对（runId 相同，
        # 下游按幂等去重后得到完整的 running→COMPLETE/FAILED 生命周期）。
        if emitter is not None:
            if overall == "success":
                emitter.pipeline_event("COMPLETE")
            else:
                emitter.pipeline_event("FAILED", error_message=error_msg)

        logger.info(
            "pipeline finished",
            extra={"stage": "pipeline", "batch": batch_id, "status": overall, "run_dir": run_dir},
        )
        return 0 if overall == "success" else 1
    except Exception as exc:
        # 逃逸异常（如 stage 模块加载失败、水位提交崩溃）：先补发 OL FAILED
        # 与 START 配对，避免下游看到悬空 running；随后保持原传播语义不变.
        if emitter is not None:
            emitter.pipeline_event("FAILED", error_message=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        # 任务41：停止 HealthServer（若已启动）
        if health_server is not None:
            try:
                health_server.stop()
                logger.info("health server stopped", extra={"stage": "pipeline", "batch": batch_id})
            except Exception:  # noqa: BLE001
                logger.warning(
                    "health_server.stop() raised, ignoring",
                    extra={"stage": "pipeline"},
                    exc_info=True,
                )
        # Phase 2b: 无论成功/失败/异常，都停止 SparkSession 释放 executor 资源。
        # 参见 docs/evolution.md §4.4.2.2 / §4.8.2。
        if spark is not None:
            try:
                spark.stop()
                logger.info("spark session stopped", extra={"stage": "pipeline", "batch": batch_id})
            except Exception:  # noqa: BLE001
                logger.warning(
                    "spark.stop() raised, ignoring", extra={"stage": "pipeline"}, exc_info=True
                )
        # 任务38：清理 root logger 上由 setup_logging 添加的 handler，
        # 避免 pytest 多次调用 run_pipeline 时 handler 累积导致重复输出.
        close_logging()


def _advance_and_merge(ctx: PipelineContext, store: StateStore, logger) -> None:
    """Commit watermarks and merge this batch's aggregates into state/.

    Called only after every stage succeeded. Watermarks staged by ingest via
    ``StateStore.set_new_watermark`` are promoted to ``watermark_value`` and
    persisted. Each aggregate csv in ``04_aggregates/`` is merged into
    ``state/aggregates/`` so the next incremental run sees the full history.

    backend 路由（参见 docs/evolution.md §4.4.2.2）：
        "python" — Python dict 累加（StateStore.merge_aggregate），行为不变
        "polars" — pl.concat + group_by.agg 列式合并（Phase 2a）
        "spark"  — history.union(delta).groupBy(key).agg() 分布式合并（Phase 2b）

    Phase 4: incremental.mode="iceberg_snapshot_diff" 时，额外提交 Iceberg
    snapshot id（``store.commit_snapshot_id``），与 watermark commit 并存。
    Iceberg 表的 merge 由 pyiceberg table.append/overwrite 在 stage 内完成，
    此处不重复 merge 数据，只持久化 snapshot id 用于下次增量.
    """
    # Phase 4: Iceberg snapshot id 提交与 watermark 提交合并为单次原子
    # commit_all，避免原两步提交中间失败导致 (watermark, snapshot_id)
    # 不一致。仅在 incremental.mode="iceberg_snapshot_diff" 且有 staged
    # snapshot id 时才同时提交两者；否则 commit_all 退化为仅提升 watermark
    # （snapshot 段无 new_snapshot_id 时跳过，行为与 commit_watermark 等价）。
    inc_mode = ctx.config.get("incremental", {}).get("mode", "high_watermark")
    has_iceberg_snaps = bool(ctx.state.get("iceberg_snapshots"))
    use_commit_all = inc_mode == "iceberg_snapshot_diff" and has_iceberg_snaps

    if use_commit_all:
        try:
            store.commit_all(ctx.state, ctx.batch_id)
            logger.info(
                "watermark + iceberg snapshot id committed atomically",
                extra={"stage": "pipeline", "batch": ctx.batch_id},
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "commit_all failed, ignoring", extra={"stage": "pipeline"}, exc_info=True
            )
    else:
        store.commit_watermark(ctx.state, ctx.batch_id)
        logger.info("watermark committed", extra={"stage": "pipeline", "batch": ctx.batch_id})

    agg_dir = os.path.join(ctx.run_dir, "04_aggregates")
    if not os.path.isdir(agg_dir):
        return
    for name, fields, key_cols in _AGGREGATE_SPECS:
        batch_csv = os.path.join(agg_dir, name + ".csv")
        if not _table_exists(batch_csv, ctx.config):
            continue
        if ctx.engine_backend == "spark":
            _merge_aggregate_spark(ctx, store, name, fields, key_cols, batch_csv, logger)
            continue
        # python/polars 路径：用 table_read 读 04_aggregates（兼容 parquet storage）
        # polars backend 下 table_read 返回 polars.DataFrame，需转 List[Dict]
        result = table_read(batch_csv, ctx.config)
        if ctx.engine_backend == "polars":
            new_rows = result.to_dicts() if result is not None and result.height > 0 else []
        else:
            new_rows, _ = result
        if not new_rows:
            # No rows in this batch for the aggregate; still ensure the
            # historical file exists so downstream reads do not fail.
            if not os.path.exists(store.get_aggregate_path(name)):
                store.save_aggregate(name, fields, [])
            continue
        merged_count = store.merge_aggregate(name, fields, new_rows, key_cols)
        logger.info(
            "aggregate merged",
            extra={"stage": "compute", "agg": name, "new": len(new_rows), "total": merged_count},
        )


def _merge_aggregate_spark(
    ctx: PipelineContext,
    store: StateStore,
    name: str,
    fields: list[str],
    key_cols: list[str],
    batch_csv: str,
    logger,
) -> None:
    """Spark 分布式聚合合并：history.union(delta).groupBy(key).agg().

    用 ``table_read`` 读历史聚合（若存在）与本批次增量，``unionByName``
    合并后 ``groupBy(key_cols).agg(F.sum(num_cols))`` 累加数值列，写出用
    ``table_write`` 统一 IO 层（Spark CSV 目录或 S3 Parquet）。派生列
    （avg_order_value/revenue_share/rank）在 Spark 路径下不重算——与
    Phase 2a polars 分支保持一致的设计简化，下游若需精确派生列可回退
    python backend。参见 docs/evolution.md §4.4.2.2 / §4.6.1。

    读路径用 ``table_read`` 而非 ``spark.read.csv``，使 cluster+S3 模式下
    能自动从 S3 读 Parquet 产物（compute stage 用 table_write 写到 S3），
    非 cluster 模式（local_csv/本地 parquet）行为不变。写路径同理用
    ``table_write``，使历史聚合在 cluster+S3 模式下写到 S3，下次增量运行
    可读到。

    注意：Spark 写本地文件需要 hadoop.dll（Windows NativeIO）。在缺
    hadoop.dll 的环境下本函数会抛 Py4J 错误——这是环境限制，由调用方
    （增量+Spark 测试）用 pytest.mark.skipif 跳过。代码逻辑本身完整正确，
    在有 hadoop.dll 的环境（Linux / 装齐 hadoop.dll 的 Windows）可直接运行。
    """
    from pyspark.sql import functions as F  # lazy import

    spark = ctx.spark_session
    assert spark is not None
    # 用 table_read 统一 IO 层读取：cluster+S3 模式下从 S3 读 Parquet，
    # local_csv/本地 parquet 模式下读 CSV/Parquet（向后兼容）。
    delta_df = table_read(batch_csv, ctx.config, spark=ctx.spark_session)
    delta_count = delta_df.count()
    hist_path = store.get_aggregate_path(name)

    if delta_count == 0:
        # 无增量行：确保历史文件存在（空 schema），与 python 路径行为一致。
        # 用 _table_exists 检查（兼容 S3 parquet），用 table_write 写空 schema。
        if not _table_exists(hist_path, ctx.config):
            empty_df = spark.createDataFrame([], delta_df.schema)
            table_write(hist_path, empty_df, ctx.config, spark=ctx.spark_session)
        logger.info(
            "aggregate merged (spark, empty delta)", extra={"stage": "compute", "agg": name}
        )
        return

    # 数值列累加；非数值非派生列（tier/city 等）取 delta 最新值；
    # 派生列（avg_order_value/revenue_share/rank）不参与累加。
    derived_cols = {"avg_order_value", "revenue_share", "rank"}
    non_key = [f for f in fields if f not in key_cols]
    num_cols = [
        f
        for f in non_key
        if f not in derived_cols and f not in ("tier", "city", "category", "region", "channel")
    ]

    # 辅助函数：按 fields 对齐 df 的列，缺失列用 null 填充。
    # 历史聚合产物（_merge_aggregate_spark 写回的）只含 key_cols + num_cols，
    # 缺派生列（avg_order_value/revenue_share/rank），直接 select(fields) 会
    # 报 UNRESOLVED_COLUMN。delta_df 由 compute stage 写出，含全部 fields。
    def _align_columns(df):
        existing = set(df.columns)
        return df.select(*[F.col(f) if f in existing else F.lit(None).alias(f) for f in fields])

    if _table_exists(hist_path, ctx.config):
        hist_df = table_read(hist_path, ctx.config, spark=ctx.spark_session)
        # unionByName 按列名对齐，避免列顺序差异
        merged = (
            _align_columns(hist_df)
            .unionByName(_align_columns(delta_df))
            .groupBy(list(key_cols))
            .agg(*[F.sum(c).alias(c) for c in num_cols])
        )
    else:
        # 无历史：本批次即全量，仍按 key 聚合一次（去重 + 累加）
        merged = (
            _align_columns(delta_df)
            .groupBy(list(key_cols))
            .agg(*[F.sum(c).alias(c) for c in num_cols])
        )

    # 写回 state/aggregates/{name}.csv（统一 IO 层：cluster+S3 写 S3 Parquet，
    # local_csv 写 Spark CSV 目录，多分区 part 文件）。
    # 注意：必须在 table_write 之前计算 total，或直接用 table_write 的返回值。
    # 因为 table_write 会覆盖 hist_path，之后再触发 merged.count() 会重新读
    # hist_df（依赖 hist_path），此时 hist_path 已被覆盖，旧 part 文件不存在，
    # 导致 FAILED_READ_FILE.FILE_NOT_EXIST。
    spark_cfg = ctx.config.get("engine", {}).get("spark", {}) or {}
    if spark_cfg.get("write_single_file", False):
        merged = merged.coalesce(1)
    total = table_write(hist_path, merged, ctx.config, spark=ctx.spark_session)
    logger.info(
        "aggregate merged (spark)",
        extra={"stage": "compute", "agg": name, "new": delta_count, "total": total},
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Big-data batch pipeline")
    parser.add_argument("--config", default="config/pipeline.json")
    parser.add_argument("--batch-id", default="auto")
    parser.add_argument("--fail-at", default="")
    args = parser.parse_args(argv)
    cfg = json_load(abs_path(args.config))
    if validate_config is not None:
        try:
            cfg = validate_config(cfg)
        except ConfigValidationError as e:
            # 退出码 2 = 配置内容非法（区别于 argparse 用法错误的退出码 2 之外的语义，
            # 调度脚本可用 stderr 文本区分）
            print(f"config validation error: {e}", file=sys.stderr)
            return 2
    fail_at = args.fail_at or cfg.get("demo", {}).get("fail_at") or ""
    # 白名单校验：仅允许合法 stage 名，防止拼写错误静默失效
    if fail_at and fail_at not in STAGES:
        raise ValueError(f"invalid fail_at stage {fail_at!r}; must be one of {STAGES}")
    if cfg.get("generator", {}).get("enabled", False):
        from .generator import main as gen_main

        meta = gen_main(cfg)
        print(
            "generated data: orders={} customers={} products={}".format(
                meta["rows"]["orders"], meta["rows"]["customers"], meta["rows"]["products"]
            )
        )
    return run_pipeline(cfg, args.batch_id, fail_at)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
