"""src/lineage.py 单元测试.

覆盖 Manifest 类方法：
- 构造与缺省值
- set_source / add_stage / add_artifact / add_edge / set_quality / finish
- to_dict 输出结构完整
- to_dict → JSON → json_load → 等价 dict 往返一致性
- save() 写盘
- save_latest_pointer / lineage_view 模块级函数

Manifest 没有 from_dict 类方法，"往返"测试以 to_dict → JSON 序列化 →
json_load 反序列化 → 与原 to_dict 相等的形式验证。
"""
from __future__ import annotations

import json
import os

import pytest

from src.helpers import VERSION, json_load
from src.lineage import Manifest, lineage_view, save_latest_pointer


# ----------------------------------------------------------------------
# 构造与缺省值
# ----------------------------------------------------------------------
def test_manifest_defaults():
    m = Manifest("B-001", "abc123", "/tmp/run/B-001")
    assert m.batch_id == "B-001"
    assert m.pipeline_version == VERSION
    assert m.config_digest == "abc123"
    assert m.run_dir == "/tmp/run/B-001"
    assert m.status == "running"
    assert m.finished_at is None
    assert m.source == {"name": "", "files": []}
    assert m.stages == []
    assert m.artifacts == {}
    assert m.lineage == {}
    assert m.quality is None
    assert m.error is None
    # started_at 应为非空字符串（UTC ISO）
    assert isinstance(m.started_at, str) and len(m.started_at) > 0


# ----------------------------------------------------------------------
# set_source
# ----------------------------------------------------------------------
def test_set_source():
    m = Manifest("B-1", "d", "/tmp/x")
    files = [{"name": "orders.csv", "rows": 100}]
    m.set_source("ecommerce", files)
    assert m.source == {"name": "ecommerce", "files": files}


# ----------------------------------------------------------------------
# add_stage
# ----------------------------------------------------------------------
def test_add_stage_appends_record():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_stage("ingest", "success", 100, 100, 50, "logs/ingest.jsonl")
    m.add_stage("validate", "success", 100, 95, 20, "logs/validate.jsonl")
    assert len(m.stages) == 2
    s = m.stages[0]
    assert s["name"] == "ingest"
    assert s["status"] == "success"
    assert s["rows_in"] == 100
    assert s["rows_out"] == 100
    assert s["duration_ms"] == 50
    assert s["log"] == "logs/ingest.jsonl"
    assert s["error"] is None


def test_add_stage_with_error():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_stage("compute", "failed", 100, 0, 30, "logs/compute.jsonl",
                error="ZeroDivisionError")
    assert m.stages[0]["error"] == "ZeroDivisionError"


# ----------------------------------------------------------------------
# add_artifact
# ----------------------------------------------------------------------
def test_add_artifact_basic():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_artifact("orders_clean.csv", "csv", 95, "sha256hex")
    a = m.artifacts["orders_clean.csv"]
    assert a["path"] == "orders_clean.csv"
    assert a["kind"] == "csv"
    assert a["rows"] == 95
    assert a["sha256"] == "sha256hex"
    assert a["batch_id"] == "B-1"


def test_add_artifact_with_extra():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_artifact("kpi.csv", "csv", 10, "hex", extra={"compression": "zstd"})
    assert m.artifacts["kpi.csv"]["compression"] == "zstd"


def test_add_artifact_overwrites_same_path():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_artifact("p.csv", "csv", 10, "h1")
    m.add_artifact("p.csv", "csv", 20, "h2")
    assert m.artifacts["p.csv"]["rows"] == 20
    assert m.artifacts["p.csv"]["sha256"] == "h2"


# ----------------------------------------------------------------------
# add_edge
# ----------------------------------------------------------------------
def test_add_edge_basic():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_edge("orders_clean.csv", ["orders_raw.csv"])
    assert m.lineage == {"orders_clean.csv": ["orders_raw.csv"]}


def test_add_edge_multiple_upstreams():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_edge("kpi.csv", ["orders_clean.csv", "customers_clean.csv"])
    assert m.lineage["kpi.csv"] == ["orders_clean.csv", "customers_clean.csv"]


def test_add_edge_overwrites_same_target():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_edge("t.csv", ["a.csv"])
    m.add_edge("t.csv", ["b.csv"])
    assert m.lineage["t.csv"] == ["b.csv"]


def test_add_edge_copies_upstream_list():
    """add_edge 应复制 upstreams，避免外部修改影响内部状态."""
    m = Manifest("B-1", "d", "/tmp/x")
    ups = ["a.csv", "b.csv"]
    m.add_edge("t.csv", ups)
    ups.append("c.csv")
    assert m.lineage["t.csv"] == ["a.csv", "b.csv"]


# ----------------------------------------------------------------------
# set_quality / finish
# ----------------------------------------------------------------------
def test_set_quality():
    m = Manifest("B-1", "d", "/tmp/x")
    summary = {"dq_score": 0.98, "rules_checked": 10}
    m.set_quality(summary)
    assert m.quality == summary


def test_finish_success():
    m = Manifest("B-1", "d", "/tmp/x")
    m.finish("success")
    assert m.status == "success"
    assert m.error is None
    assert isinstance(m.finished_at, str) and len(m.finished_at) > 0


def test_finish_with_error():
    m = Manifest("B-1", "d", "/tmp/x")
    m.finish("failed", error="stage compute crashed")
    assert m.status == "failed"
    assert m.error == "stage compute crashed"


# ----------------------------------------------------------------------
# to_dict
# ----------------------------------------------------------------------
def test_to_dict_keys_complete():
    m = Manifest("B-1", "digest", "/tmp/x")
    d = m.to_dict()
    expected_keys = {
        "batch_id", "pipeline_version", "config_digest", "started_at",
        "finished_at", "status", "run_dir", "source", "stages",
        "artifacts", "lineage", "quality", "error",
    }
    assert set(d.keys()) == expected_keys


def test_to_dict_reflects_mutations():
    m = Manifest("B-1", "d", "/tmp/x")
    m.set_source("s", [])
    m.add_stage("ingest", "success", 10, 10, 5, "log")
    m.add_artifact("a.csv", "csv", 10, "h")
    m.add_edge("a.csv", ["raw.csv"])
    m.set_quality({"dq_score": 1.0})
    m.finish("success")
    d = m.to_dict()
    assert d["source"]["name"] == "s"
    assert len(d["stages"]) == 1
    assert "a.csv" in d["artifacts"]
    assert d["lineage"]["a.csv"] == ["raw.csv"]
    assert d["quality"] == {"dq_score": 1.0}
    assert d["status"] == "success"
    assert d["finished_at"] is not None


# ----------------------------------------------------------------------
# to_dict → JSON → json_load 往返
# ----------------------------------------------------------------------
def test_to_dict_json_roundtrip(tmp_path):
    """to_dict → JSON 序列化 → json_load → 与原 to_dict 相等."""
    m = Manifest("B-rt", "digest-rt", str(tmp_path))
    m.set_source("ecommerce", [{"name": "orders.csv", "rows": 100}])
    m.add_stage("ingest", "success", 100, 100, 50, "logs/ingest.jsonl")
    m.add_stage("validate", "success", 100, 95, 20, "logs/validate.jsonl")
    m.add_artifact("orders_clean.csv", "csv", 95, "sha256hex",
                   extra={"compression": "zstd"})
    m.add_edge("orders_clean.csv", ["orders_raw.csv"])
    m.set_quality({"dq_score": 0.98, "rules": 10})
    m.finish("success")

    original = m.to_dict()
    # 写盘再读回
    path = tmp_path / "manifest.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(original, f, ensure_ascii=False, indent=2)
    loaded = json_load(str(path))
    assert loaded == original


def test_to_dict_json_roundtrip_with_error(tmp_path):
    """finish("failed", error=...) → JSON 往返."""
    m = Manifest("B-err", "d", str(tmp_path))
    m.add_stage("compute", "failed", 100, 0, 30, "log", error="ZeroDivision")
    m.finish("failed", error="compute stage crashed")
    original = m.to_dict()
    path = tmp_path / "manifest.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(original, f, ensure_ascii=False, indent=2)
    loaded = json_load(str(path))
    assert loaded == original
    assert loaded["error"] == "compute stage crashed"
    assert loaded["stages"][0]["error"] == "ZeroDivision"


# ----------------------------------------------------------------------
# save()
# ----------------------------------------------------------------------
def test_save_writes_manifest_json(tmp_path):
    m = Manifest("B-save", "d", str(tmp_path))
    m.add_artifact("a.csv", "csv", 10, "h")
    m.finish("success")
    path = m.save()
    assert os.path.isfile(path)
    assert path.endswith("manifest.json")
    loaded = json_load(path)
    assert loaded["batch_id"] == "B-save"
    assert "a.csv" in loaded["artifacts"]


# ----------------------------------------------------------------------
# save_latest_pointer
# ----------------------------------------------------------------------
def test_save_latest_pointer(tmp_path):
    run_root = str(tmp_path)
    save_latest_pointer(run_root, "B-2026", os.path.join(run_root, "B-2026"))
    p = json_load(os.path.join(run_root, "latest.json"))
    assert p["batch_id"] == "B-2026"
    assert p["run_dir"].endswith("B-2026")
    assert "updated_at" in p


# ----------------------------------------------------------------------
# lineage_view
# ----------------------------------------------------------------------
def test_lineage_view_nodes_and_edges():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_artifact("orders_raw.csv", "csv", 100, "rawhex1234567890")
    m.add_artifact("orders_clean.csv", "csv", 95, "cleanhex1234567890")
    m.add_edge("orders_clean.csv", ["orders_raw.csv"])
    view = lineage_view(m)
    # 两个节点
    assert len(view["nodes"]) == 2
    node_paths = {n["id"] for n in view["nodes"]}
    assert node_paths == {"orders_raw.csv", "orders_clean.csv"}
    # 一条边
    assert view["edges"] == [{"from": "orders_raw.csv", "to": "orders_clean.csv"}]
    # sha256 截断为 12 字符
    for n in view["nodes"]:
        assert len(n["sha256"]) <= 12


def test_lineage_view_empty_manifest():
    m = Manifest("B-1", "d", "/tmp/x")
    view = lineage_view(m)
    assert view == {"nodes": [], "edges": []}


def test_lineage_view_node_fields():
    m = Manifest("B-1", "d", "/tmp/x")
    m.add_artifact("a.csv", "csv", 10, "abcdef0123456789")
    view = lineage_view(m)
    n = view["nodes"][0]
    assert set(n.keys()) == {"id", "kind", "rows", "sha256", "batch_id"}
    assert n["id"] == "a.csv"
    assert n["kind"] == "csv"
    assert n["rows"] == 10
    assert n["batch_id"] == "B-1"
