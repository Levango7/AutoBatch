"""Pipeline entry: orchestrate stages, per-stage logging, status, failure locating."""

from __future__ import annotations

import argparse
import contextlib
import functools
import hashlib
import importlib
import json
import os
import shutil
import sys
import threading
import time
import traceback
from collections.abc import Callable
from typing import Any, Optional

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
    _get_storage_backend,
    _table_exists,
    abs_path,
    batch_id_new,
    json_load,
    json_save,
    table_read,
    table_write,
)
from .io._s3_parquet import s3_credentials
from .lineage import Manifest, save_latest_pointer
from .logging_setup import close_logging, setup_logging
from .metrics import MetricsRecorder
from .monitoring import HealthServer, MetricsSampler, check_alerts, load_monitoring_config
from .openlineage import OpenLineageEmitter
from .state import _DERIVED_COLS, _DIMENSION_COLS, StateStore, recompute_derived

STAGES = ["ingest", "validate", "clean", "compute", "output"]

# 合法计算引擎后端；_get_engine_backend 读到的未知值会经 dispatch 兜底走 python
# 路径（_dispatch.py），行为不中断但性能预期（polars/spark）落空——在此告警。
_KNOWN_ENGINE_BACKENDS = ("python", "polars", "spark")


def _warn_unknown_engine_backend(engine_backend: str, logger) -> None:
    if engine_backend not in _KNOWN_ENGINE_BACKENDS:
        logger.warning(
            "unknown engine.backend %r (expected python/polars/spark); "
            "falling back to the python engine — install pydantic to enable "
            "config validation and catch typos at startup",
            engine_backend,
            extra={"stage": "pipeline"},
        )


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
        _access, _secret = s3_credentials(cfg)
        builder = builder.config("spark.hadoop.fs.s3a.access.key", _access)
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", _secret)
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
            _access, _secret = s3_credentials(cfg)
            builder = builder.config("spark.hadoop.fs.s3a.access.key", _access)
            builder = builder.config("spark.hadoop.fs.s3a.secret.key", _secret)
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

    任务 #74 M17：
    - 超时路径把未结束的 worker 线程挂到异常的 ``stage_thread`` 属性上
      （setattr：StageTimeoutError 未声明该字段），供重试逻辑在退避窗口内
      做 bounded join——尽量等僵尸线程自己退出后再重试，缩小新旧 attempt
      并发写产物的竞态窗口（线程无法强杀，残留风险见任务报告）.
    - done_event 已置位但 holder 既无 value 也无 error 的极端竞态下，原实现
      直接取 result_holder["value"] 会抛 KeyError（对调用方不可辨识）；
      改为抛 RuntimeError 显式化，可被重试机制正常处理.

    Args:
        fn:              无参可调用，封装了 stage 执行逻辑.
        timeout_seconds: 超时阈值（秒），None 或 <=0 表示不限制.
        stage_name:      stage 名（用于异常上下文）.
        batch_id:        批次 ID（用于异常上下文）.
        attempt:         当前 attempt 序号（用于异常上下文）.

    Returns:
        fn() 的返回值.

    Raises:
        StageTimeoutError: 超时（fn 未在阈值内完成）；携带 stage_thread 引用.
        RuntimeError:      工作线程置位完成事件但未留下结果/异常.
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
        err = StageTimeoutError(stage_name, batch_id, attempt, timeout_seconds, elapsed)
        # 任务 #74 M17：挂上僵尸 worker 引用，重试逻辑据此 bounded join.
        # 用 setattr：StageTimeoutError 未声明该动态字段，直接赋值会被 mypy 拒绝.
        setattr(err, "stage_thread", worker)  # noqa: B010 - 动态附加字段（见 docstring）
        raise err
    if "error" in result_holder:
        raise result_holder["error"]
    if "value" not in result_holder:
        # 任务 #74 M17：极端竞态防护——done_event 已 set 但 holder 无 value/error
        # （如工作线程在 finally 前被中断）。原实现取 ["value"] 抛 KeyError，
        # 调用方无法区分"stage 失败"与"基础设施故障"；显式 RuntimeError 后
        # 可进入重试/backoff 常规路径.
        raise RuntimeError(
            f"stage thread terminated without result (stage={stage_name}, batch={batch_id})"
        )
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

        # 任务 #74 M17：重试 attempt 使用独立 StageLog（logs/<stage>_attempt<n>.jsonl），
        # 避免超时后无法强杀的前一 attempt 僵尸线程继续写同一份日志造成交错
        # （attempt 0 沿用调用方打开的主日志文件，成功后无需额外清理）.
        attempt_ctx: contextlib.AbstractContextManager[StageLog]
        if attempt == 0:
            attempt_ctx = contextlib.nullcontext(slog)
        else:
            attempt_ctx = StageLog(
                os.path.join(ctx.run_dir, "logs", f"{stage_name}_attempt{attempt}.jsonl"),
                batch_id=batch_id,
                stage=stage_name,
            )

        with attempt_ctx as attempt_slog:
            try:
                summary = _run_with_timeout(
                    # partial 立即绑定当前 attempt 的 slog（lambda 捕获循环变量
                    # 会触发 B023，且语义上更脆弱）
                    functools.partial(stage_fn, ctx, attempt_slog),
                    timeout_s,
                    stage_name,
                    batch_id,
                    attempt,
                )
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
                attempt_slog.error(
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
                attempt_slog.info("stage retry scheduled", attempt=attempt, backoff_seconds=backoff)
                # 任务 #74 M17：超时失败的旧 worker 线程仍在运行（Python 线程不可强杀）。
                # 退避前先 bounded join：线程在窗口内退出则只补睡剩余时间；
                # 窗口耗尽仍存活则记录泄漏告警后继续重试（重试前的输出目录清理
                # 覆盖大部分残留产物；ctx/state 的僵尸写为残留风险，见任务报告）。
                zombie_worker = getattr(exc, "stage_thread", None)
                if zombie_worker is not None and zombie_worker.is_alive():
                    wait_start = time.monotonic()
                    zombie_worker.join(timeout=backoff)
                    if zombie_worker.is_alive():
                        logger.error(
                            "timed-out stage thread still alive after backoff window; "
                            "retrying anyway (Python threads cannot be killed)",
                            extra={
                                "stage": stage_name,
                                "batch": batch_id,
                                "attempt": attempt,
                            },
                        )
                    remaining = backoff - (time.monotonic() - wait_start)
                    if remaining > 0:
                        time.sleep(remaining)
                else:
                    # 非超时失败路径：恰好 sleep 一次完整退避
                    # （test_error_handling.py 的退避行为契约）.
                    time.sleep(backoff)
                continue
            if attempt > 0:
                logger.info(
                    "stage succeeded after retry",
                    extra={"stage": stage_name, "batch": batch_id, "attempt": attempt},
                )
                slog.info("stage succeeded after retry", attempt=attempt)
            return summary

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
# 会幂等重建）。此外任务 #74 C1/C2 还恢复**重跑下游 stage 所需的内存态**：
#   - ingest 条目携带 ingested 文件描述列表（validate 的输入）与该批次
#     staged 的水位/快照（staged_state，增量两阶段提交的暂存值）；
#   - validate 条目携带 outlier_keys（clean 的输入）。
# 不恢复会导致续跑 validate 零校验（DQ 虚报 1.0）、clean 缺键，以及
# 增量提交丢失 staged 水位（下批重复聚合翻倍）。水位正式值不涉及——失败
# 路径本就不推进水位（两阶段提交），续跑成功后由 _advance_and_merge 的
# commit_batch 正常提升一次。
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


# ---------------------------------------------------------------------------
# 任务 #74 C1/C2：续跑内存态的持久化/恢复辅助
# ---------------------------------------------------------------------------
# stage 成功时仅在 ctx 内存里产生下游必需的中间态（ingest 的 ingested 文件
# 列表与 staged 水位；validate 的 outlier_keys）。续跑跳过成功 stage 后，
# 重跑的下游 stage 读到的是全新空 ctx：validate 迭代空 ingested → 零校验
# （DQ 虚报 1.0）；clean 缺 outlier_keys。因此把各 stage 的内存态随 manifest
# 条目持久化，续跑恢复段再搬回 ctx；staged 水位的真正回填发生在初始化窗口
# 的 ctx.state = store.load() 之后（见 _restore_staged_state 调用点）。

# staged 字段集（与 StateStore.set_new_watermark / set_new_snapshot_id 的
# 暂存键保持一致；抽取/回填只搬这些键，不扩散其他状态）。
_STAGED_TABLE_KEYS = ("new_watermark", "new_seen_row_count", "new_batch_id")
_STAGED_SNAPSHOT_KEYS = ("new_snapshot_id", "new_batch_id")


def _extract_staged_state(state: dict[str, Any]) -> dict[str, Any]:
    """Extract staged watermarks/snapshot ids from ctx.state ({} when none).

    Task #74 C2: staged fields exist only in the crashed process's memory
    (two-phase commit never writes them to disk). Persisting them in the
    ingest stage's manifest entry lets a resume run restore them, so
    ``commit_batch`` can promote this batch's watermark/snapshot exactly
    once; without the restore the watermark would not advance and the NEXT
    batch would re-aggregate this batch's rows (double counting).
    """
    out: dict[str, Any] = {}
    for tname, info in (state.get("tables") or {}).items():
        staged = {k: info[k] for k in _STAGED_TABLE_KEYS if k in info}
        if staged:
            out.setdefault("tables", {})[tname] = staged
    for sname, info in (state.get("iceberg_snapshots") or {}).items():
        staged = {k: info[k] for k in _STAGED_SNAPSHOT_KEYS if k in info}
        if staged:
            out.setdefault("iceberg_snapshots", {})[sname] = staged
    return out


def _restore_staged_state(state: dict[str, Any], staged: dict[str, Any]) -> None:
    """Write back staged keys produced by ``_extract_staged_state``.

    MUST run after ``store.load()`` (initialization window): ctx.state first
    reflects on-disk state, then the staged values are overlaid so the
    batch-end ``commit_batch`` promotes them exactly once. Missing table
    sections are recreated (first-batch crash recovery: state.json may be
    empty while the failed manifest carries the staged values).
    """
    for tname, info in (staged.get("tables") or {}).items():
        state.setdefault("tables", {}).setdefault(tname, {}).update(info)
    for sname, info in (staged.get("iceberg_snapshots") or {}).items():
        state.setdefault("iceberg_snapshots", {}).setdefault(sname, {}).update(info)


def _stage_resume_payload(st: dict[str, Any]) -> dict[str, Any]:
    """Collect resume-relevant keys from a previous manifest stage entry.

    ``Manifest.add_stage(extra=...)`` flattens keys onto the entry top level
    (see lineage.py ``entry.update(extra)``), while the lineage wrap loop
    nests ``lineage_decl`` under ``entry["extra"]``. Read leniently: top
    level first, nested ``extra`` second.
    """
    extra = st.get("extra", {}) or {}
    payload: dict[str, Any] = {}
    for key in ("ingested", "staged_state", "outlier_keys", "lineage_decl"):
        if key in st:
            payload[key] = st[key]
        elif key in extra:
            payload[key] = extra[key]
    return payload


def _rebuild_aggregates_from_disk(ctx: PipelineContext, logger) -> dict[str, Any]:
    """compute 被续跑跳过时，从 04_aggregates 产物重建 ctx.aggregates.

    任务 #74 C1 补丁：output stage 消费 compute 写入 ctx.aggregates 的内存态
    （dashboard_data.json 等）。compute 被续跑跳过后若不做重建，output 拿到
    空聚合直接报错。产物完整性已由 _stage_outputs_intact 校验（04_aggregates
    非空是 compute 续跑的前置条件），此处用 table_read 按 backend/storage
    自动路由读回，形状与 compute 写入时一致：
        {"daily", "category", "region_channel",
         "customer_value": {"top", "tiers"}, "kpi"}.
    必须在初始化窗口内调用（spark 路径需要 ctx.spark_session 已就绪）。
    """
    agg_dir = os.path.join(ctx.run_dir, "04_aggregates")

    def _rows(fname: str) -> list[dict[str, Any]]:
        path = os.path.join(agg_dir, fname)
        if not _table_exists(path, ctx.config):
            return []
        result = table_read(path, ctx.config, spark=ctx.spark_session)
        if ctx.engine_backend == "polars":
            return result.to_dicts() if result is not None and result.height > 0 else []
        if ctx.engine_backend == "spark":
            return [row.asDict() for row in result.collect()]
        rows, _ = result
        return rows

    kpi_path = os.path.join(agg_dir, "kpi.json")
    kpi = json_load(kpi_path) if os.path.isfile(kpi_path) else {}
    return {
        "daily": _rows("daily_sales.csv"),
        "category": _rows("category_stats.csv"),
        "region_channel": _rows("region_channel_stats.csv"),
        "customer_value": {
            "top": _rows("customer_value.csv"),
            "tiers": _rows("customer_tier.csv"),
        },
        "kpi": kpi,
    }


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
    _warn_unknown_engine_backend(ctx.engine_backend, logger)

    # --- Resume (断点续跑) ---
    resumed_stages = set()
    resume_active = False
    # 任务 #74 C2：从失败 manifest 收集 ingest 暂存的水位/快照，待初始化窗口
    # ctx.state = store.load() 之后回填（此处 ctx.state 尚未加载，先收集）。
    staged_to_restore: dict[str, Any] = {}
    # 任务 #74 C1 补丁：compute 被续跑跳过时，其产物 ctx.aggregates（内存态）
    # 不会被重建，而 output 直接消费它 → 必须在初始化窗口从 04_aggregates
    # 磁盘产物重建（见 _rebuild_aggregates_from_disk）。
    resume_aggregates_restore = False
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
                    if stage_name == "compute":
                        # compute 不会重跑 → 其内存产物 ctx.aggregates 需从磁盘重建
                        resume_aggregates_restore = True
                    # 任务 #74 C1+C2：从上次 manifest 条目取回续跑所需的内存态.
                    # add_stage(extra=...) 平铺到条目顶层，两种位置都兼容读取.
                    payload = _stage_resume_payload(st)
                    if payload.get("ingested"):
                        ctx.ingested = [dict(x) for x in payload["ingested"]]
                    if payload.get("outlier_keys"):
                        ctx.outlier_keys = set(payload["outlier_keys"])
                    for section, tables in (payload.get("staged_state") or {}).items():
                        for tname, info in tables.items():
                            staged_to_restore.setdefault(section, {})[tname] = dict(info)
                    # 复制 stage 记录到当前 manifest（并把续跑键再次持久化进
                    # 新条目——若本轮再次失败，下一次 resume 仍能恢复）.
                    extra = st.get("extra", {}) or {}
                    extra["resumed"] = True
                    for key in ("ingested", "staged_state", "outlier_keys"):
                        if key in payload:
                            extra[key] = payload[key]
                    lineage_decl = payload.get("lineage_decl") or {}
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
                    resume_aggregates_restore = False
                    # 任务 #74 C2：全量重跑会由 ingest 重新 stage 水位，
                    # 已收集的旧 staged 值必须丢弃（否则会叠加出幻影水位）.
                    staged_to_restore = {}
                    # 重置 manifest/ctx/metrics 为全新状态——恢复循环可能已把
                    # 前序 stage 写进 metrics，不重置会导致全量重跑后 stages 重复
                    manifest = Manifest(batch_id, digest, run_dir)
                    ctx = PipelineContext(
                        config=cfg, run_dir=run_dir, batch_id=batch_id, manifest=manifest
                    )
                    ctx.engine_backend = _get_engine_backend(cfg)
                    _warn_unknown_engine_backend(ctx.engine_backend, logger)
                    metrics = MetricsRecorder(batch_id)
                    # 后续 spark 初始化会使用新的 ctx
                    break
        if resume_active:
            logger.info(
                "resume enabled, skipping stages",
                extra={"stage": "pipeline", "resumed": list(resumed_stages)},
            )
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
            # 任务 #74 C4 崩溃恢复：把上一次已越过原子提交点、但替换未完成的
            # 暂存聚合收尾（幂等；无 pending 标记时为空操作）。必须早于任何
            # stage 执行与本批次的 merge 暂存，保证所有读方看到的都是正式聚合.
            completed_pending = store.complete_pending_aggregates(ctx.state)
            if completed_pending:
                logger.info(
                    "pending aggregates completed from prior commit",
                    extra={"stage": "pipeline", "aggregates": completed_pending},
                )
            # 任务 #74 C2：回填 resume 段从失败 manifest 收集的 staged 水位/
            # 快照。必须在 store.load() 之后执行——ctx.state 此刻才反映磁盘
            # 态；批次末尾 commit_batch 会把它们正式提升（恰好一次）。
            if staged_to_restore:
                _restore_staged_state(ctx.state, staged_to_restore)
                logger.info(
                    "staged watermark/snapshot restored from failed manifest",
                    extra={"stage": "pipeline", "batch": batch_id},
                )
            logger.info(
                "incremental mode enabled", extra={"stage": "pipeline", "state_dir": state_dir}
            )

        # 任务 #74 C1 补丁：compute 被续跑跳过时，从 04_aggregates 磁盘产物
        # 重建 ctx.aggregates（output 直接消费该内存态）。必须放在 spark 会
        # 话初始化之后（spark backend 读回需要 ctx.spark_session 已就绪）。
        if resume_active and resume_aggregates_restore:
            ctx.aggregates = _rebuild_aggregates_from_disk(ctx, logger)
            logger.info(
                "ctx.aggregates rebuilt from disk for resumed compute",
                extra={"stage": "pipeline", "batch": batch_id},
            )

        # 任务41 监控告警：加载 config/monitoring.json（缺省 disabled）.
        monitoring_cfg = load_monitoring_config(
            abs_path(cfg.get("monitoring_config", "config/monitoring.json"))
        )
        monitoring_enabled = bool(monitoring_cfg.get("enabled", False))
        if monitoring_enabled:
            hc_cfg = monitoring_cfg.get("health_check", {}) or {}
            if hc_cfg.get("enabled", False):
                health_server = HealthServer(
                    # 缺省绑定回环地址：健康端点含批次/数据概况，暴露到所有网卡
                    # 会成为信息泄露面；容器外探活场景显式配置 host.
                    host=hc_cfg.get("host", "127.0.0.1"),
                    port=int(hc_cfg.get("port", 8086)),
                    run_dir=run_root,
                )
                health_server.start()
                logger.info(
                    "health server started",
                    extra={
                        "stage": "pipeline",
                        "host": health_server.host,
                        "port": health_server.port,
                    },
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
                logger.info(
                    "stage resumed (skipped)", extra={"stage": "pipeline", "resumed_stage": name}
                )
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
                # 任务 #74 C1+C2：把重跑下游 stage 必需的内存态持久化到
                # manifest 条目，供续跑恢复（读侧见 _stage_resume_payload）：
                #   ingest   → ingested（validate 的输入）+ staged_state（C2）
                #   validate → outlier_keys（clean 的输入）
                stage_extra: dict[str, Any] = {}
                if name == "ingest":
                    if ctx.ingested:
                        stage_extra["ingested"] = [dict(x) for x in ctx.ingested]
                    staged_state = _extract_staged_state(ctx.state)
                    if staged_state:
                        stage_extra["staged_state"] = staged_state
                elif name == "validate":
                    if ctx.outlier_keys:
                        stage_extra["outlier_keys"] = sorted(ctx.outlier_keys)
                manifest.add_stage(
                    name,
                    "success",
                    rows_in,
                    rows_out,
                    dur,
                    log_path,
                    extra=stage_extra or None,
                )
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

        # 任务 #74 C3+C4：两阶段提交前移到 manifest 定稿之前。全部 stage 成功
        # 时才提交（水位/快照提升 + 聚合合并，见 _advance_and_merge 的暂存台账
        # 协议）。提交失败不再被吞掉：把批次标记为 failed，使随后持久化的
        # manifest/status.json 反映真实终态；staged 值未提升、台账未登记，
        # 重跑本批次幂等（详见 docs/evolution.md §3.3.5 更新说明）。
        if overall == "success" and incremental_enabled and store is not None:
            try:
                _advance_and_merge(ctx, store, logger)
            except Exception as exc:  # noqa: BLE001
                overall = "failed"
                error_msg = f"commit phase failed: {type(exc).__name__}: {exc}"
                logger.error(
                    "commit phase failed, batch marked failed",
                    extra={"stage": "pipeline", "batch": batch_id, "error": error_msg},
                )

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

        # Two-phase commit 已前移到本块之前（任务 #74 C3/C4）；失败批次到此
        # 保持 state.json 不变，重跑同批次重新读取同一 delta（幂等）。

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
    """Commit watermarks/snapshots and merge aggregates (task #74 C3/C4).

    Called only after every stage succeeded. Order of operations implements the
    "staging ledger" atomic commit protocol (task #74 C4):

        1. Idempotency gate: batch_id already in the ``merged_batches`` ledger
           (crash AFTER a successful commit point, e.g. inside metrics.finish)
           → skip everything; re-merging would double-count aggregates.
        2. Aggregate staging: merged results are written ONLY to
           ``state/aggregates_pending/{name}.csv`` (``merge_aggregate_staged``
           on the python/polars path; ``write_pending_aggregate`` on the
           spark/local path). Official aggregates are untouched.
        3. ATOMIC COMMIT POINT: ``commit_batch`` — one state.json save that
           (a) promotes staged watermarks (``new_watermark`` →
           ``watermark_value`` + ``cumulative_row_count``), (b) promotes staged
           Iceberg snapshot ids (superset of the old ``commit_all``), (c)
           registers batch_id in the ledger, (d) records the pending marker.
           Failure propagates to run_pipeline (task #74 C3: never swallow).
        4. Replacement finish: ``complete_pending_aggregates`` os.replaces each
           staged file into the official aggregate. A crash between 3 and 4 is
           recovered at next startup by the initialization window.

    Crash-window analysis: crash BEFORE step 3 → staged files are recomputed
    and overwritten on re-run; watermarks/ledger untouched; re-run idempotent.
    Crash AFTER step 3 → ledger blocks re-merge, marker drives replacement
    recovery; staged files hold the COMPLETE merged result, so replacing is
    idempotent.

    Residual window (declared trade-off): spark + S3/parquet storage cannot
    atomically os.replace into S3, so that path writes the merged result
    directly to the official aggregate via table_write (merge-before-commit),
    guarded only by the ledger; a "merge succeeded, crash before commit_batch"
    re-run would double-count that batch once — see _merge_aggregate_spark.

    backend 路由不变（python/polars/spark，参见 docs/evolution.md §4.4.2.2）。
    Iceberg 表的数据 merge 仍由 stage 内 pyiceberg append/overwrite 完成，
    此处只经 commit_batch 持久化 staged snapshot id。
    """
    state = ctx.state
    batch_id = ctx.batch_id

    if store.is_batch_merged(state, batch_id):
        logger.info(
            "batch already in merged_batches ledger, skipping merge (idempotent)",
            extra={"stage": "pipeline", "batch": batch_id},
        )
        return

    pending: dict[str, str] = {}
    agg_dir = os.path.join(ctx.run_dir, "04_aggregates")
    if os.path.isdir(agg_dir):
        for name, fields, key_cols in _AGGREGATE_SPECS:
            batch_csv = os.path.join(agg_dir, name + ".csv")
            if not _table_exists(batch_csv, ctx.config):
                continue
            if ctx.engine_backend == "spark":
                staged_path = _merge_aggregate_spark(
                    ctx, store, name, fields, key_cols, batch_csv, logger
                )
                if staged_path:
                    pending[name] = staged_path
                continue
            # python/polars 路径：用 table_read 读批次聚合（兼容 parquet storage）；
            # polars backend 下 table_read 返回 polars.DataFrame，需转 List[Dict]
            result = table_read(batch_csv, ctx.config)
            if ctx.engine_backend == "polars":
                new_rows = result.to_dicts() if result is not None and result.height > 0 else []
            else:
                new_rows, _ = result
            if not new_rows:
                # 本批次该聚合无行；仍确保历史文件存在使下游读取不失败。
                # 空文件创建不涉及累加，直接写正式路径（与暂存协议无冲突）.
                if not os.path.exists(store.get_aggregate_path(name)):
                    store.save_aggregate(name, fields, [])
                continue
            # 任务 #74 C4：合并结果只写暂存文件，正式聚合不动.
            merged_count, staged_path = store.merge_aggregate_staged(
                name, fields, new_rows, key_cols
            )
            pending[name] = staged_path
            logger.info(
                "aggregate merge staged",
                extra={
                    "stage": "compute",
                    "agg": name,
                    "new": len(new_rows),
                    "total": merged_count,
                },
            )

    # 原子提交点（任务 #74 C4）：水位/快照提升 + 台账登记 + pending 标记
    # 一次性保存。异常直接向上传播（任务 #74 C3：不得吞掉），由 run_pipeline
    # 把批次标记 failed——提交未完成时 staged 值未提升、台账未登记，
    # 重跑本批次是幂等的。
    store.commit_batch(state, batch_id, pending_aggregates=pending or None)
    logger.info(
        "batch committed (watermark + ledger + aggregates marker)",
        extra={"stage": "pipeline", "batch": batch_id, "pending_aggregates": len(pending)},
    )

    # 替换收尾：暂存文件原子替换进正式聚合；此后清除标记。
    if pending:
        completed = store.complete_pending_aggregates(state)
        logger.info(
            "staged aggregates replaced into official paths",
            extra={"stage": "pipeline", "batch": batch_id, "aggregates": completed},
        )


def _merge_aggregate_spark(
    ctx: PipelineContext,
    store: StateStore,
    name: str,
    fields: list[str],
    key_cols: list[str],
    batch_csv: str,
    logger,
) -> Optional[str]:
    """Spark 分布式聚合合并：history.union(delta).groupBy(key).agg().

    任务 #74 M12：全字段列保留（修复 tier/city/rank 等列丢失）：
      - 非派生数值列（orders/units/revenue/customers）：sum 累加；
      - 维度列（tier/city/category/region/channel）："delta 优先，历史兜底"
        ``coalesce(max(when(_merge_src=1, col)), max(col))``，避免 union+sum
        路径把字符串维度列直接丢掉；
      - 派生列（avg_order_value/revenue_share/rank）：聚合结果 collect 回
        driver 后用 ``state.recompute_derived`` 重算（与 python 路径共享同一
        实现，两引擎产物一致）。

    任务 #74 C4 暂存协议：local 存储下合并结果写暂存文件
    （write_pending_aggregate），返回路径由调用方交给 commit_batch 登记、
    complete_pending_aggregates 原子替换进正式聚合。S3/parquet 目标无法
    os.replace 原子替换 → 保持 table_write 直写正式聚合（merge-before-commit），
    重复累加由 merged_batches 台账兜底；"merge 成功但 commit_batch 前崩溃"
    的重跑窗口会令该批次重复累加一次——S3 场景以运维监控补偿（显式权衡，
    与调用方 _advance_and_merge docstring 的残留窗口声明对应）。

    读路径用 ``table_read`` 而非 ``spark.read.csv``，使 cluster+S3 模式下
    能自动从 S3 读 Parquet 产物（compute stage 用 table_write 写到 S3），
    非 cluster 模式（local_csv/本地 parquet）行为不变。写路径同理。

    注意：Spark 写本地文件需要 hadoop.dll（Windows NativeIO）。在缺
    hadoop.dll 的环境下本函数会抛 Py4J 错误——这是环境限制，由调用方
    （增量+Spark 测试）用 pytest.mark.skipif 跳过。

    Returns:
        暂存文件路径（local_csv 存储；待提交点登记后替换进正式聚合）；
        空 delta 落空文件或 S3/parquet 直写时返回 None.
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
        return None

    non_key = [f for f in fields if f not in key_cols]
    num_cols = [f for f in non_key if f not in _DERIVED_COLS and f not in _DIMENSION_COLS]
    # 任务 #76：排除分组键——维度列同时是 groupBy key 时（category_stats 的
    # category、region_channel_stats 的 region+channel、customer_tier 的 tier），
    # 若再 alias 为同名的聚合列会与分组键列重名，下游 select 抛
    # [AMBIGUOUS_REFERENCE]。分组键列由 groupBy 天然保留且组内取值相同，
    # "delta 优先" coalesce 对其无意义（与 python 路径 _merge_into 跳过
    # key_set 的语义一致）。
    dim_cols = [f for f in non_key if f in _DIMENSION_COLS]

    # 辅助函数：按 fields 对齐 df 的列，缺失列用 null 填充。
    # 旧版本写回的历史产物只含 key_cols + num_cols，缺派生列/维度列，
    # 直接 select 会报 UNRESOLVED_COLUMN。delta_df 由 compute stage 写出，
    # 含全部 fields。
    def _align_columns(df):
        existing = set(df.columns)
        return df.select(*[F.col(f) if f in existing else F.lit(None).alias(f) for f in fields])

    # _merge_src 标记列（hist=0, delta=1）：维度列 "delta 优先、历史兜底"
    # 靠它实现（任务 #74 M12）。
    src_col = "_merge_src"
    agg_exprs = [F.sum(c).alias(c) for c in num_cols]
    agg_exprs += [
        F.coalesce(
            F.max(F.when(F.col(src_col) == 1, F.col(c))),
            F.max(F.col(c)),
        ).alias(c)
        for c in dim_cols
    ]
    if not agg_exprs:
        # 仅含 key/派生列的规格：占位聚合保证 groupBy.agg 合法（当前
        # _AGGREGATE_SPECS 不会走到此分支，防御性保留）。
        agg_exprs = [F.max(F.lit(0)).alias("_placeholder")]

    if _table_exists(hist_path, ctx.config):
        hist_df = table_read(hist_path, ctx.config, spark=ctx.spark_session)
        # unionByName 按列名对齐，避免列顺序差异
        combined = (
            _align_columns(hist_df)
            .withColumn(src_col, F.lit(0))
            .unionByName(_align_columns(delta_df).withColumn(src_col, F.lit(1)))
        )
    else:
        # 无历史：本批次即全量，src=0 → 维度列 coalesce 回退 max(col)，
        # 语义等价于批内去重 + 累加
        combined = _align_columns(delta_df).withColumn(src_col, F.lit(0))

    merged = combined.groupBy(list(key_cols)).agg(*agg_exprs)
    spark_cfg = ctx.config.get("engine", {}).get("spark", {}) or {}
    if spark_cfg.get("write_single_file", False):
        merged = merged.coalesce(1)

    # 任务 #74 M12：collect 回 driver 重算派生列（与 python 路径共享
    # recompute_derived；缺失的派生键由该函数直接填充）。当前聚合产物维度
    # （按日/类目/区域渠道/客户粒度，量级 < 10^5 行）对 driver 安全。
    keep_cols = [f for f in fields if f not in _DERIVED_COLS]
    rows = [row.asDict() for row in merged.select(keep_cols).collect()]
    recompute_derived(rows, fields)

    if _get_storage_backend(ctx.config) == "local_csv":
        # 任务 #74 C4：写暂存文件 → commit_batch 登记标记 →
        # complete_pending_aggregates 原子替换进正式聚合。
        staged_path = store.write_pending_aggregate(name, fields, rows)
        logger.info(
            "aggregate merge staged (spark)",
            extra={"stage": "compute", "agg": name, "new": delta_count, "total": len(rows)},
        )
        return staged_path

    # S3/parquet：无法原子替换，直写正式聚合（残留窗口见 docstring）。
    # 全列转字符串后建 DataFrame，与 table_write 的 S3 写出约定一致。
    out_rows = [["" if row.get(f) is None else str(row.get(f)) for f in fields] for row in rows]
    out_df = spark.createDataFrame(out_rows, schema=list(fields))
    if spark_cfg.get("write_single_file", False):
        out_df = out_df.coalesce(1)
    total = table_write(hist_path, out_df, ctx.config, spark=ctx.spark_session)
    logger.info(
        "aggregate merged (spark, direct write)",
        extra={"stage": "compute", "agg": name, "new": delta_count, "total": total},
    )
    return None


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
