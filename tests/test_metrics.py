"""src/metrics.py 单元测试.

覆盖 MetricsRecorder：
- 构造与缺省值
- record_stage 累加 stage 记录
- finish 终结 pipeline-level 字段
- to_dict 输出 labelled + flat 两种视图
- save 写盘 → json_load → 与 to_dict 相等（往返一致性）

MetricsRecorder 没有 load 类方法，"往返"测试以 save → json_load → 与
to_dict 相等的形式验证。
"""
from __future__ import annotations

import os

import pytest

from src.helpers import json_load
from src.metrics import MetricsRecorder


# ----------------------------------------------------------------------
# 构造与缺省值
# ----------------------------------------------------------------------
def test_recorder_defaults():
    r = MetricsRecorder("B-001")
    assert r.batch_id == "B-001"
    assert r.status == "running"
    assert r.finished_at is None
    assert r.stages == []
    assert r.total_duration_ms == 0
    assert r.dq_score is None
    assert r.quarantined_rows == {}
    assert isinstance(r.started_at, str) and len(r.started_at) > 0


# ----------------------------------------------------------------------
# record_stage
# ----------------------------------------------------------------------
def test_record_stage_appends_record():
    r = MetricsRecorder("B-1")
    r.record_stage("ingest", "success", 50, 100, 100)
    r.record_stage("validate", "success", 20, 100, 95)
    assert len(r.stages) == 2
    s = r.stages[0]
    assert s["name"] == "ingest"
    assert s["status"] == "success"
    assert s["duration_ms"] == 50
    assert s["rows_in"] == 100
    assert s["rows_out"] == 100


def test_record_stage_with_extra():
    r = MetricsRecorder("B-1")
    r.record_stage("clean", "success", 30, 95, 90,
                   extra={"quarantined": 5, "rules_triggered": 2})
    s = r.stages[0]
    assert s["quarantined"] == 5
    assert s["rules_triggered"] == 2


# ----------------------------------------------------------------------
# finish
# ----------------------------------------------------------------------
def test_finish_success():
    r = MetricsRecorder("B-1")
    r.finish("success", 1000, dq_score=0.98,
             quarantined_rows={"missing": 5, "negative": 2})
    assert r.status == "success"
    assert r.finished_at is not None
    assert r.total_duration_ms == 1000
    assert r.dq_score == 0.98
    assert r.quarantined_rows == {"missing": 5, "negative": 2}


def test_finish_without_optional_fields():
    r = MetricsRecorder("B-1")
    r.finish("failed", 500)
    assert r.status == "failed"
    assert r.total_duration_ms == 500
    # dq_score / quarantined_rows 保持缺省
    assert r.dq_score is None
    assert r.quarantined_rows == {}


# ----------------------------------------------------------------------
# to_dict: labelled + flat 视图
# ----------------------------------------------------------------------
def test_to_dict_keys_complete():
    r = MetricsRecorder("B-1")
    r.record_stage("ingest", "success", 50, 100, 100)
    r.finish("success", 1000, dq_score=0.98, quarantined_rows={"missing": 5})
    d = r.to_dict()
    expected_keys = {
        "batch_id", "started_at", "finished_at", "status",
        "total_duration_ms", "dq_score", "quarantined_rows",
        "quarantined_total", "stages", "metrics",
    }
    assert set(d.keys()) == expected_keys


def test_to_dict_quarantined_total():
    r = MetricsRecorder("B-1")
    r.finish("success", 100, quarantined_rows={"missing": 5, "negative": 3})
    d = r.to_dict()
    assert d["quarantined_total"] == 8


def test_to_dict_flat_metrics_pipeline_level():
    r = MetricsRecorder("B-1")
    r.finish("success", 1000, dq_score=0.95, quarantined_rows={"missing": 2})
    d = r.to_dict()
    flat = d["metrics"]
    assert flat["pipeline_duration_ms"] == 1000
    assert flat["pipeline_status_success"] == 1
    assert flat["pipeline_quarantined_total"] == 2
    assert flat["pipeline_dq_score"] == 0.95


def test_to_dict_flat_metrics_failed_status():
    r = MetricsRecorder("B-1")
    r.finish("failed", 500)
    d = r.to_dict()
    assert d["metrics"]["pipeline_status_success"] == 0


def test_to_dict_flat_metrics_stage_level():
    r = MetricsRecorder("B-1")
    r.record_stage("ingest", "success", 50, 100, 100)
    r.record_stage("validate", "failed", 20, 100, 95)
    r.finish("success", 70)
    flat = r.to_dict()["metrics"]
    assert flat["stage_ingest_duration_ms"] == 50
    assert flat["stage_ingest_rows_in"] == 100
    assert flat["stage_ingest_rows_out"] == 100
    assert flat["stage_ingest_status_success"] == 1
    assert flat["stage_validate_status_success"] == 0


def test_to_dict_no_dq_score_omits_flat_key():
    """dq_score=None 时 flat 不含 pipeline_dq_score."""
    r = MetricsRecorder("B-1")
    r.finish("success", 100)
    flat = r.to_dict()["metrics"]
    assert "pipeline_dq_score" not in flat


# ----------------------------------------------------------------------
# save → json_load 往返
# ----------------------------------------------------------------------
def test_save_writes_metrics_json(tmp_path):
    r = MetricsRecorder("B-save")
    r.record_stage("ingest", "success", 50, 100, 100)
    r.record_stage("validate", "success", 20, 100, 95)
    r.finish("success", 70, dq_score=0.98, quarantined_rows={"missing": 5})
    path = r.save(str(tmp_path))
    assert os.path.isfile(path)
    assert path.endswith("metrics.json")
    loaded = json_load(path)
    assert loaded["batch_id"] == "B-save"
    assert loaded["status"] == "success"
    assert len(loaded["stages"]) == 2
    assert loaded["dq_score"] == 0.98


def test_save_to_dict_roundtrip(tmp_path):
    """save → json_load → 与 to_dict 完全相等."""
    r = MetricsRecorder("B-rt")
    r.record_stage("ingest", "success", 50, 100, 100,
                   extra={"foo": "bar"})
    r.record_stage("compute", "success", 30, 95, 95)
    r.finish("success", 80, dq_score=0.97,
             quarantined_rows={"missing": 3, "negative": 1})
    original = r.to_dict()
    path = r.save(str(tmp_path))
    loaded = json_load(path)
    assert loaded == original


def test_save_multiple_stages_in_flat(tmp_path):
    """多个 stage 应在 flat 视图中各生成一组 key."""
    r = MetricsRecorder("B-1")
    for name in ["ingest", "validate", "clean", "compute", "output"]:
        r.record_stage(name, "success", 10, 100, 100)
    r.finish("success", 50)
    path = r.save(str(tmp_path))
    loaded = json_load(path)
    flat = loaded["metrics"]
    for name in ["ingest", "validate", "clean", "compute", "output"]:
        assert f"stage_{name}_duration_ms" in flat
        assert f"stage_{name}_rows_in" in flat
        assert f"stage_{name}_rows_out" in flat
        assert f"stage_{name}_status_success" in flat
