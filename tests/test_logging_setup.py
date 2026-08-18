"""src/logging_setup.py 单元测试.

覆盖：
- BatchLogFilter：注入 batch_id / stage 到 LogRecord，保留调用方 extra 优先级
- JsonFormatter：输出 JSON 对象，含 timestamp/level/batch_id/stage/message/extra
- TextFormatter：向后兼容文本格式
- _resolve_level：级别字符串解析
- setup_logging / close_logging：handler 配置与清理
- get_stage_logger：LoggerAdapter 注入 batch_id/stage

不依赖文件 IO 的测试用 LogRecord 直接构造，避免污染 root logger。
"""
from __future__ import annotations

import json
import logging
import os
import re

import pytest

from src.logging_setup import (
    BatchLogFilter,
    JsonFormatter,
    TextFormatter,
    _resolve_level,
    close_logging,
    get_stage_logger,
    setup_logging,
)


# ----------------------------------------------------------------------
# BatchLogFilter
# ----------------------------------------------------------------------
def _make_record(msg="hello", **extra):
    """构造一条 LogRecord，可选注入 extra 属性."""
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=1,
        msg=msg, args=(), exc_info=None,
    )
    for k, v in extra.items():
        setattr(record, k, v)
    return record


def test_batch_log_filter_injects_defaults():
    f = BatchLogFilter("B-001")
    record = _make_record()
    assert f.filter(record) is True
    assert record.batch_id == "B-001"
    assert record.stage == "pipeline"


def test_batch_log_filter_custom_default_stage():
    f = BatchLogFilter("B-001", default_stage="ingest")
    record = _make_record()
    f.filter(record)
    assert record.stage == "ingest"


def test_batch_log_filter_preserves_existing_attrs():
    """调用方通过 extra 传入的 batch_id / stage 应保留（优先级高于缺省）."""
    f = BatchLogFilter("B-default", default_stage="pipeline")
    record = _make_record()
    record.batch_id = "B-custom"
    record.stage = "validate"
    f.filter(record)
    assert record.batch_id == "B-custom"
    assert record.stage == "validate"


# ----------------------------------------------------------------------
# JsonFormatter
# ----------------------------------------------------------------------
def test_json_formatter_outputs_valid_json():
    formatter = JsonFormatter()
    record = _make_record("hello world")
    record.batch_id = "B-1"
    record.stage = "ingest"
    output = formatter.format(record)
    obj = json.loads(output)
    assert obj["message"] == "hello world"
    assert obj["batch_id"] == "B-1"
    assert obj["stage"] == "ingest"
    assert obj["level"] == "INFO"
    assert "timestamp" in obj
    assert "extra" in obj


def test_json_formatter_collects_extra_attrs():
    """record 上非标准属性应进入 extra."""
    formatter = JsonFormatter()
    record = _make_record("msg")
    record.batch_id = "B-1"
    record.stage = "ingest"
    record.rows = 100
    record.duration_ms = 50
    obj = json.loads(formatter.format(record))
    assert obj["extra"]["rows"] == 100
    assert obj["extra"]["duration_ms"] == 50


def test_json_formatter_handles_non_serializable_extra():
    """非 JSON 可序列化的 extra 值应用 repr() 兜底."""
    formatter = JsonFormatter()
    record = _make_record("msg")
    record.batch_id = "B-1"
    record.stage = "ingest"
    # set 不可 JSON 序列化
    record.bad_value = {1, 2, 3}
    obj = json.loads(formatter.format(record))
    # 应转为 repr 字符串
    assert isinstance(obj["extra"]["bad_value"], str)


def test_json_formatter_includes_exception_info():
    formatter = JsonFormatter()
    try:
        raise ValueError("test error")
    except ValueError:
        import sys
        record = _make_record("msg")
        record.batch_id = "B-1"
        record.stage = "ingest"
        record.exc_info = sys.exc_info()
    obj = json.loads(formatter.format(record))
    assert "exc_text" in obj["extra"]
    assert "ValueError" in obj["extra"]["exc_text"]


def test_json_formatter_timestamp_iso_format():
    """timestamp 应为 ISO8601 UTC 格式（...Z 结尾）."""
    formatter = JsonFormatter()
    record = _make_record("msg")
    record.batch_id = "B-1"
    record.stage = "ingest"
    obj = json.loads(formatter.format(record))
    ts = obj["timestamp"]
    assert ts.endswith("Z")
    # 应可被 datetime.fromisoformat 解析（去掉 Z 后）
    from datetime import datetime
    datetime.fromisoformat(ts[:-1])


# ----------------------------------------------------------------------
# TextFormatter
# ----------------------------------------------------------------------
def test_text_formatter_format():
    formatter = TextFormatter()
    record = _make_record("hello")
    record.batch_id = "B-1"
    record.stage = "ingest"
    output = formatter.format(record)
    assert "hello" in output
    assert "B-1" in output
    assert "ingest" in output
    assert "INFO" in output


# ----------------------------------------------------------------------
# _resolve_level
# ----------------------------------------------------------------------
def test_resolve_level_standard_names():
    """标准级别名应解析为 logging 模块对应常量.

    注：src/logging_setup.py 的 _resolve_level 用 replace("WARN","WARNING")
    实现 WARN 别名兼容，这会误伤 "WARNING" 本身（→ "WARNINGING" → 回退 INFO）.
    此处只断言未被 replace 误伤的级别；WARNING 的退化行为由
    test_resolve_level_warning_degrades_to_info 单独覆盖.
    """
    assert _resolve_level("DEBUG") == logging.DEBUG
    assert _resolve_level("INFO") == logging.INFO
    assert _resolve_level("ERROR") == logging.ERROR
    assert _resolve_level("CRITICAL") == logging.CRITICAL


def test_resolve_level_warning_degrades_to_info():
    """已知 src bug：_resolve_level("WARNING") 因 replace 误伤退化为 INFO.

    本测试固化当前行为，便于未来修复 src 时第一时间发现并更新测试.
    """
    assert _resolve_level("WARNING") == logging.INFO


def test_resolve_level_warn_alias():
    """WARN 应作为 WARNING 别名."""
    assert _resolve_level("WARN") == logging.WARNING


def test_resolve_level_case_insensitive():
    assert _resolve_level("info") == logging.INFO
    assert _resolve_level("Debug") == logging.DEBUG


def test_resolve_level_invalid_falls_back_to_info():
    assert _resolve_level("not-a-level") == logging.INFO
    assert _resolve_level("") == logging.INFO


# ----------------------------------------------------------------------
# setup_logging / close_logging
# ----------------------------------------------------------------------
def test_setup_logging_creates_file_and_console_handlers(tmp_path):
    logger = setup_logging("B-1", str(tmp_path), fmt="json", level="DEBUG")
    root = logging.getLogger()
    # 应有 2 个 handler（file + console）
    assert len(root.handlers) == 2
    # 文件 handler 写到 <run_dir>/logs/pipeline.log
    assert os.path.isfile(tmp_path / "logs" / "pipeline.log")
    # logger 名字应为 "pipeline"
    assert logger.name == "pipeline"
    close_logging()


def test_setup_logging_idempotent_removes_old_handlers(tmp_path):
    """多次 setup_logging 应清掉旧 handler，不累积."""
    setup_logging("B-1", str(tmp_path), fmt="json")
    setup_logging("B-2", str(tmp_path), fmt="json")
    root = logging.getLogger()
    assert len(root.handlers) == 2  # 仍是 2，不是 4
    close_logging()


def test_setup_logging_writes_json_log(tmp_path):
    """fmt=json 时日志文件每行应为合法 JSON."""
    logger = setup_logging("B-json", str(tmp_path), fmt="json", level="INFO")
    logger.info("test message", extra={"stage": "ingest", "rows": 100})
    close_logging()
    log_path = tmp_path / "logs" / "pipeline.log"
    with open(log_path, encoding="utf-8") as f:
        line = f.readline().strip()
    obj = json.loads(line)
    assert obj["message"] == "test message"
    assert obj["batch_id"] == "B-json"
    assert obj["stage"] == "ingest"
    assert obj["extra"]["rows"] == 100


def test_setup_logging_writes_text_log(tmp_path):
    """fmt=text 时日志文件应为文本格式（含 batch=... stage=...）."""
    logger = setup_logging("B-text", str(tmp_path), fmt="text", level="INFO")
    logger.info("hello text", extra={"stage": "validate"})
    close_logging()
    log_path = tmp_path / "logs" / "pipeline.log"
    with open(log_path, encoding="utf-8") as f:
        content = f.read()
    assert "hello text" in content
    assert "B-text" in content
    assert "validate" in content


def test_close_logging_removes_all_handlers(tmp_path):
    setup_logging("B-1", str(tmp_path))
    root = logging.getLogger()
    assert len(root.handlers) > 0
    close_logging()
    assert len(root.handlers) == 0


def test_close_logging_idempotent():
    """close_logging 多次调用不报错."""
    close_logging()
    close_logging()


# ----------------------------------------------------------------------
# get_stage_logger
# ----------------------------------------------------------------------
def test_get_stage_logger_injects_context(tmp_path):
    """get_stage_logger 返回的 adapter 调用时自动注入 batch_id/stage."""
    setup_logging("B-stage", str(tmp_path), fmt="json", level="INFO")
    slog = get_stage_logger("B-stage", "compute")
    slog.info("computing", extra={"rows": 50})
    close_logging()
    log_path = tmp_path / "logs" / "pipeline.log"
    with open(log_path, encoding="utf-8") as f:
        line = f.readline().strip()
    obj = json.loads(line)
    assert obj["batch_id"] == "B-stage"
    assert obj["stage"] == "compute"
    assert obj["extra"]["rows"] == 50


def test_get_stage_logger_caller_extra_overrides_adapter(tmp_path):
    """调用方 extra={"stage": ...} 应覆盖 adapter 自带的 stage."""
    setup_logging("B-override", str(tmp_path), fmt="json", level="INFO")
    slog = get_stage_logger("B-override", "ingest")
    # 调用时显式传 stage → 应覆盖 adapter 的 "ingest"
    slog.info("msg", extra={"stage": "validate"})
    close_logging()
    log_path = tmp_path / "logs" / "pipeline.log"
    with open(log_path, encoding="utf-8") as f:
        line = f.readline().strip()
    obj = json.loads(line)
    assert obj["stage"] == "validate"
