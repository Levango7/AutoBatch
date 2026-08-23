"""src/openlineage.py 单元测试.

覆盖 OpenLineageEmitter 的全部公开路径：
- pipeline_event / stage_event（START / COMPLETE / FAILED）
- NDJSON 写出 + 确定性 runId
- parent facet 正确链接 stage → pipeline
- endpoint 缺省不发 HTTP
- 空 namespace / 未知 stage 行为
- 构造函数默认参数
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Optional

import pytest

from src.openlineage import (
    PRODUCER,
    SCHEMA_VERSION,
    STAGE_IO,
    OpenLineageEmitter,
    _dataset,
    _event,
    _now_iso,
    _stable_runid,
)


# ----------------------------------------------------------------------
# 模块级辅助函数
# ----------------------------------------------------------------------
def test_now_iso_format():
    s = _now_iso()
    assert s.endswith("Z")
    assert "T" in s
    # 格式 YYY-MM-DDTHH:MM:SS.mmmZ
    parts = s.split("T")
    assert len(parts[0].split("-")) == 3
    assert len(parts[1].rstrip("Z").split(":")) == 3


def test_stable_runid_deterministic():
    r1 = _stable_runid("b-1", "ingest")
    r2 = _stable_runid("b-1", "ingest")
    assert r1 == r2
    r3 = _stable_runid("b-1", "validate")
    assert r1 != r3
    r4 = _stable_runid("b-2", "ingest")
    assert r1 != r4
    # 标准 UUID v5 格式
    import uuid as _uuid
    _uuid.UUID(r1)  # 不会抛即合法


def test_dataset():
    d = _dataset("ns", "orders", "b-1")
    assert d == {"namespace": "ns", "name": "b-1/orders"}


def test_event_required_fields():
    ev = _event("START", "ns", "ns.pipeline", "run-1", [], [])
    assert ev["eventTime"].endswith("Z")
    assert ev["eventType"] == "START"
    assert ev["producer"] == PRODUCER
    assert ev["schemaVersion"] == SCHEMA_VERSION
    assert ev["job"] == {"namespace": "ns", "name": "ns.pipeline"}
    assert ev["run"] == {"runId": "run-1"}
    assert ev["inputs"] == []
    assert ev["outputs"] == []
    assert "runFacets" not in ev


def test_event_with_parent():
    ev = _event("START", "ns", "ns.ingest", "r", [], [], parent={"run": {"runId": "p"}})
    assert "runFacets" in ev
    assert ev["runFacets"]["parent"]["run"]["runId"] == "p"


def test_event_with_error():
    ev = _event("FAILED", "ns", "ns.ingest", "r", [], [], error_message="boom")
    assert ev["runFacets"]["error"]["message"] == "boom"


def test_event_error_truncated():
    long_msg = "x" * 600
    ev = _event("FAILED", "ns", "ns.ingest", "r", [], [], error_message=long_msg)
    assert len(ev["runFacets"]["error"]["message"]) == 500


def test_stage_io_map_complete():
    """5 个 stage 全部在映射中."""
    assert sorted(STAGE_IO.keys()) == ["clean", "compute", "ingest", "output", "validate"]


# ----------------------------------------------------------------------
# OpenLineageEmitter
# ----------------------------------------------------------------------
@pytest.fixture
def emitter(tmp_path):
    return OpenLineageEmitter(
        batch_id="b-001",
        namespace="testns",
        endpoint="",
        out_path=str(tmp_path / "openlineage.ndjson"),
    )


def _read_events(emitter) -> list[dict]:
    """读取 emitter.out_path 里的所有 JSON 事件."""
    path = emitter.out_path
    text = open(path, encoding="utf-8").read().strip()
    return [json.loads(line) for line in text.splitlines()] if text else []


def test_emitter_init_defaults():
    e = OpenLineageEmitter("b-x")
    assert e.batch_id == "b-x"
    assert e.namespace == "autobatch"
    assert e.endpoint == ""
    assert e.out_path is None
    assert e.pipeline_run_id == _stable_runid("b-x", "pipeline")


def test_emitter_pipeline_start(emitter):
    emitter.pipeline_event("START")
    events = _read_events(emitter)
    assert len(events) == 1
    ev = events[0]
    assert ev["eventType"] == "START"
    assert ev["job"]["name"] == "testns.pipeline"
    assert ev["run"]["runId"] == emitter.pipeline_run_id
    assert ev["inputs"] == []
    assert ev["outputs"] == []


def test_emitter_pipeline_finish(emitter):
    emitter.pipeline_event("COMPLETE")
    events = _read_events(emitter)
    assert len(events) == 1
    assert events[0]["eventType"] == "COMPLETE"


def test_emitter_pipeline_failed_with_error(emitter):
    emitter.pipeline_event("FAILED", error_message="OOM")
    events = _read_events(emitter)
    assert events[0]["runFacets"]["error"]["message"] == "OOM"


def test_emitter_stage_start(emitter):
    emitter.stage_event("ingest", "START")
    ev = _read_events(emitter)[0]
    assert ev["eventType"] == "START"
    assert ev["job"]["name"] == "testns.ingest"
    assert ev["run"]["runId"] == _stable_runid("b-001", "ingest")
    assert ev["inputs"] == [{"namespace": "testns", "name": "b-001/source"}]
    assert ev["outputs"] == [{"namespace": "testns", "name": "b-001/01_raw"}]
    assert ev["runFacets"]["parent"]["run"]["runId"] == emitter.pipeline_run_id


def test_emitter_stage_complete(emitter):
    emitter.stage_event("validate", "COMPLETE")
    ev = _read_events(emitter)[0]
    assert ev["eventType"] == "COMPLETE"
    assert ev["inputs"] == [{"namespace": "testns", "name": "b-001/01_raw"}]
    assert ev["outputs"] == [{"namespace": "testns", "name": "b-001/02_valid"}]


def test_emitter_stage_failed(emitter):
    emitter.stage_event("compute", "FAILED", error_message="null pointer")
    ev = _read_events(emitter)[0]
    assert ev["eventType"] == "FAILED"
    assert ev["runFacets"]["error"]["message"] == "null pointer"


def test_emitter_unknown_stage_no_io(emitter):
    """未知 stage 不抛异常，inputs/outputs 为空."""
    emitter.stage_event("custom_stage", "START")
    ev = _read_events(emitter)[0]
    assert ev["inputs"] == []
    assert ev["outputs"] == []


def test_emitter_multiple_events_single_file(emitter):
    emitter.pipeline_event("START")
    emitter.stage_event("ingest", "START")
    emitter.stage_event("ingest", "COMPLETE")
    events = _read_events(emitter)
    assert len(events) == 3
    assert events[0]["eventType"] == "START"
    assert events[1]["eventType"] == "START"
    assert events[2]["eventType"] == "COMPLETE"


def test_emitter_append_mode(tmp_path):
    """多次构造写入同一文件应追加而非覆盖."""
    p = str(tmp_path / "out.ndjson")
    e1 = OpenLineageEmitter("b1", out_path=p)
    e1.pipeline_event("START")
    e2 = OpenLineageEmitter("b2", out_path=p)
    e2.pipeline_event("START")
    lines = tmp_path.joinpath("out.ndjson").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


def test_emitter_no_endpoint_no_post(monkeypatch):
    """endpoint 为空时不调用 urllib.request.urlopen."""
    called = []

    def fake_urlopen(*a, **kw):
        called.append(True)
        raise RuntimeError("should not be called")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    e = OpenLineageEmitter("b", endpoint="")
    e.pipeline_event("START")
    assert called == []


def test_emitter_http_post_failure_ignored(monkeypatch, tmp_path):
    """HTTP 上报失败不影响 NDJSON 写出."""
    import urllib.error

    def fake_urlopen(req, timeout):
        raise urllib.error.HTTPError("http://x", 500, "boom", {}, None)

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    e = OpenLineageEmitter("b", endpoint="http://bad-host:9999", out_path=str(tmp_path / "out.ndjson"))
    e.pipeline_event("START")  # 不应抛
    assert tmp_path.joinpath("out.ndjson").exists()
    lines = tmp_path.joinpath("out.ndjson").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
