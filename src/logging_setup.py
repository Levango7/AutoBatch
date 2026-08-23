"""日志规范化：JSON structured logging + 运行追踪 ID（batch_id）.

设计目标（任务38）：
  1. 统一日志格式为 JSON structured logging：每条日志输出 JSON 对象，包含
     timestamp、level、batch_id、stage、message、extra 字段.
  2. 加运行追踪 ID：用 batch_id 关联所有 stage 的日志，便于追踪一个批次的
     全生命周期.
  3. 级别控制：DEBUG/INFO/WARN/ERROR，可通过配置控制输出级别.
  4. 日志输出到 run/<batch>/logs/ 目录：每个批次一个日志文件（pipeline.log）.
  5. 向后兼容：现有日志文本仍可读，JSON 格式可配置开关
     （config.logging.format: "text" 缺省 / "json"）.

入口函数：
  setup_logging(batch_id, run_dir, fmt, level) → logging.Logger

  在 pipeline.run_pipeline 批次开始时调用，配置 root logger：
    - 文件 handler：run/<batch>/logs/pipeline.log（按 fmt 选 JSON / text formatter）
    - 控制台 handler：与文件 handler 同 formatter
    - BatchLogFilter 注入 batch_id / stage 字段到每条 LogRecord

  各 stage 通过 logger.info(msg, extra={"stage": name, ...}) 传 stage 名；
  未传时 filter 用缺省 "pipeline".

辅助函数：
  get_stage_logger(batch_id, stage) → LoggerAdapter
    返回自带 batch_id/stage 上下文的 logger，调用方无需每次传 extra.

  close_logging()
    清理 root logger 上由 setup_logging 添加的 handler，避免 pytest 多次
    调用 run_pipeline 时 handler 累积.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

# 标准字段，不放入 extra
_STD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "getMessage",
    "taskName",
    # 由 BatchLogFilter 注入的结构化字段，单独输出不进 extra
    "batch_id",
    "stage",
}


def _utc_iso() -> str:
    """UTC ISO8601 毫秒精度，与 helpers.utc_ts 一致."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class BatchLogFilter(logging.Filter):
    """注入 batch_id / stage 字段到每条 LogRecord.

    若 record 已有 batch_id / stage 属性（调用方通过 extra 传入），则保留；
    否则用本 filter 缺省值填充.这样各 stage 调用
    ``logger.info(msg, extra={"stage": "ingest"})`` 时 stage 字段正确，
    未传时回退到 "pipeline".

    Attributes:
        batch_id:       批次 ID，关联所有 stage 日志.
        default_stage:  未显式传 stage 时的缺省值，缺省 "pipeline".
    """

    def __init__(self, batch_id: str, default_stage: str = "pipeline"):
        super().__init__()
        self.batch_id = batch_id
        self.default_stage = default_stage

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "batch_id"):
            record.batch_id = self.batch_id
        if not hasattr(record, "stage"):
            record.stage = self.default_stage
        return True


class JsonFormatter(logging.Formatter):
    """JSON structured logging formatter.

    输出 JSON 对象：{timestamp, level, batch_id, stage, message, extra}.
    extra 收集 LogRecord 上除标准字段外的所有属性，便于结构化检索.
    """

    def format(self, record: logging.LogRecord) -> str:
        # 收集 extra 字段：record 上非标准属性都放进 extra
        extra: dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key in _STD_ATTRS or key.startswith("_"):
                continue
            try:
                json.dumps(value)
                extra[key] = value
            except (TypeError, ValueError):
                extra[key] = repr(value)
        # 异常信息放进 extra
        if record.exc_info:
            extra["exc_text"] = self.formatException(record.exc_info)
        obj = {
            "timestamp": _utc_iso(),
            "level": record.levelname,
            "batch_id": getattr(record, "batch_id", ""),
            "stage": getattr(record, "stage", "pipeline"),
            "message": record.getMessage(),
            "extra": extra,
        }
        return json.dumps(obj, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """文本 formatter，向后兼容.

    格式：%(asctime)s %(levelname)s [batch=%(batch_id)s stage=%(stage)s] %(name)s: %(message)s
    与原 logger_setup 输出风格一致，仅多 [batch=... stage=...] 上下文段.
    """

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s %(levelname)s [batch=%(batch_id)s stage=%(stage)s] "
            "%(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def _make_formatter(fmt: str) -> logging.Formatter:
    """按 fmt 选 JSON / text formatter."""
    if fmt.lower() == "json":
        return JsonFormatter()
    return TextFormatter()


def _resolve_level(level: str) -> int:
    """把字符串级别解析为 logging 模块的 int 级别.

    兼容 WARN 别名（logging 模块用 WARNING）.无效值回退 INFO.
    """
    normalized = level.upper().replace("WARN", "WARNING")
    return getattr(logging, normalized, logging.INFO)


def setup_logging(
    batch_id: str,
    run_dir: str,
    fmt: str = "text",
    level: str = "INFO",
) -> logging.Logger:
    """配置 root logger，输出到 run/<batch>/logs/pipeline.log + 控制台.

    幂等：先清掉 root logger 上由本函数添加的旧 handler，避免 pytest 多次
    调用 run_pipeline 时 handler 累积导致重复输出.

    Args:
        batch_id:  批次 ID，注入每条日志的 batch_id 字段.
        run_dir:   批次运行目录（run/<batch>），日志写到 <run_dir>/logs/pipeline.log.
        fmt:       "text"（缺省，向后兼容）/ "json".
        level:     "DEBUG" / "INFO" / "WARN" / "ERROR"（缺省 INFO）.

    Returns:
        logging.Logger("pipeline")，调用方用它 info/warn/error.
    """
    py_level = _resolve_level(level)
    formatter = _make_formatter(fmt)
    log_filter = BatchLogFilter(batch_id)

    # 清掉旧 handler 避免重复（pytest 多次调用 run_pipeline / 多批次场景）
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(py_level)

    # 文件 handler：run/<batch>/logs/pipeline.log
    log_dir = os.path.join(run_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    file_path = os.path.join(log_dir, "pipeline.log")
    file_handler = logging.FileHandler(file_path, encoding="utf-8")
    file_handler.setLevel(py_level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(log_filter)
    root.addHandler(file_handler)

    # 控制台 handler：与文件同 formatter，便于开发期观察
    console_handler = logging.StreamHandler()
    console_handler.setLevel(py_level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(log_filter)
    root.addHandler(console_handler)

    return logging.getLogger("pipeline")


def close_logging() -> None:
    """清理 root logger 上由 setup_logging 添加的 handler.

    在 pipeline.run_pipeline 的 finally 块中调用，避免 pytest 多次调用时
    handler 累积.同时 close 文件 handler 释放文件句柄.
    """
    root = logging.getLogger()
    for h in list(root.handlers):
        try:
            h.close()
        except Exception:  # noqa: BLE001
            pass
        root.removeHandler(h)


class _StageLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter 注入 batch_id / stage 到 extra.

    调用方通过 get_stage_logger 获取，info/warn/error 调用时自动在 LogRecord
    上注入 batch_id / stage 字段，无需每次传 extra={"stage": ...}.
    """

    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {}) or {}
        # 合并 adapter 自带 extra（batch_id / stage），调用方 extra 优先级更高
        adapter_extra = self.extra or {}
        merged = {**adapter_extra, **extra}
        kwargs["extra"] = merged
        return msg, kwargs


def get_stage_logger(batch_id: str, stage: str) -> logging.LoggerAdapter:
    """获取带 stage 上下文的 logger.

    用法（各 stage）：
        slog = get_stage_logger(ctx.batch_id, "ingest")
        slog.info("ingested", extra={"rows": 100})

    返回的 logger 调用时自动在 LogRecord 上注入 batch_id / stage 字段，
    无需调用方每次传 extra={"stage": ...}.

    Args:
        batch_id:  批次 ID.
        stage:     stage 名（ingest/validate/clean/compute/output）.

    Returns:
        logging.LoggerAdapter，调用 info/warn/error 时自动注入 batch_id/stage.
    """
    base = logging.getLogger("pipeline." + stage)
    return _StageLoggerAdapter(base, {"batch_id": batch_id, "stage": stage})
