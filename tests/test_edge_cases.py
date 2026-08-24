"""边界测试：空数据、单行数据、坏配置（缺字段/空字符串/None 值）.

覆盖 src 各模块对边界输入的处理，确保不抛未捕获异常或产生静默错误结果.
"""

from __future__ import annotations

import os
import random
from datetime import datetime

import pytest

from src.generator import gen_customers, gen_orders, gen_products
from src.helpers import csv_read, csv_write, json_load, json_save
from src.lineage import Manifest, lineage_view
from src.metrics import MetricsRecorder
from src.state import StateStore

BASE_DATE = datetime(2026, 8, 15)


# ----------------------------------------------------------------------
# 空数据
# ----------------------------------------------------------------------
def test_gen_customers_zero():
    rng = random.Random(42)
    assert gen_customers(rng, 0, BASE_DATE) == []


def test_gen_products_zero():
    rng = random.Random(42)
    assert gen_products(rng, 0) == []


def test_gen_orders_zero():
    rng = random.Random(42)
    customers = gen_customers(rng, 5, BASE_DATE)
    products = gen_products(rng, 5)
    rows = gen_orders(rng, 0, customers, products, BASE_DATE, {}, 90)
    assert rows == []


def test_csv_write_then_read_empty(tmp_path):
    """csv_write 写 0 行 → csv_read 读回 0 行 + 表头."""
    path = tmp_path / "empty.csv"
    csv_write(str(path), ["a", "b"], [])
    data, fields = csv_read(str(path))
    assert data == []
    assert fields == ["a", "b"]


def test_state_store_load_empty(tmp_path):
    s = StateStore(str(tmp_path / "no_state"))
    state = s.load()
    assert state["tables"] == {}
    assert state["iceberg_snapshots"] == {}


def test_manifest_empty_lineage_view():
    m = Manifest("B-empty", "d", "/tmp")
    view = lineage_view(m)
    assert view == {"nodes": [], "edges": []}


def test_metrics_recorder_no_stages():
    r = MetricsRecorder("B-1")
    r.finish("success", 0)
    d = r.to_dict()
    assert d["stages"] == []
    assert d["quarantined_total"] == 0
    assert d["metrics"]["pipeline_quarantined_total"] == 0


# ----------------------------------------------------------------------
# 单行数据
# ----------------------------------------------------------------------
def test_gen_customers_single():
    rng = random.Random(42)
    rows = gen_customers(rng, 1, BASE_DATE)
    assert len(rows) == 1
    assert rows[0]["customer_id"] == "CUS-000001"


def test_gen_products_single():
    rng = random.Random(42)
    rows = gen_products(rng, 1)
    assert len(rows) == 1
    assert rows[0]["product_id"] == "PRD-000001"


def test_gen_orders_single():
    rng = random.Random(42)
    customers = gen_customers(rng, 1, BASE_DATE)
    products = gen_products(rng, 1)
    rows = gen_orders(rng, 1, customers, products, BASE_DATE, {}, 90)
    assert len(rows) == 1
    assert rows[0]["order_id"] == "ORD-00000001"


def test_csv_write_then_read_single(tmp_path):
    path = tmp_path / "one.csv"
    rows = [{"a": "1", "b": "x"}]
    csv_write(str(path), ["a", "b"], rows)
    data, fields = csv_read(str(path))
    assert len(data) == 1
    assert data[0]["a"] == "1"
    assert data[0]["b"] == "x"


def test_state_store_single_commit(tmp_path):
    s = StateStore(str(tmp_path / "state"))
    state = s.load()
    s.set_new_watermark(state, "orders", "2026-01-01", 1, "B-1")
    s.commit_watermark(state, "B-1")
    assert s.get_watermark("orders") == "2026-01-01"
    info = s.load()["tables"]["orders"]
    assert info["cumulative_row_count"] == 1


# ----------------------------------------------------------------------
# 坏配置：缺字段
# ----------------------------------------------------------------------
def test_gen_orders_missing_defect_keys():
    """defects dict 缺某些键（用 .get 缺省 0）应不抛异常."""
    rng = random.Random(42)
    customers = gen_customers(rng, 5, BASE_DATE)
    products = gen_products(rng, 5)
    # 只配 missing，其他键缺失
    rows = gen_orders(rng, 50, customers, products, BASE_DATE, {"missing": 0.1}, 90)
    assert len(rows) == 50


def test_metrics_recorder_record_stage_no_extra():
    """record_stage 不传 extra 应正常工作."""
    r = MetricsRecorder("B-1")
    r.record_stage("ingest", "success", 10, 100, 100)
    assert r.stages[0]["name"] == "ingest"
    # 不应有额外字段
    assert set(r.stages[0].keys()) == {"name", "status", "duration_ms", "rows_in", "rows_out"}


# ----------------------------------------------------------------------
# 坏配置：空字符串
# ----------------------------------------------------------------------
def test_manifest_empty_batch_id():
    """空 batch_id 应允许（不抛异常），to_dict 正常输出."""
    m = Manifest("", "digest", "/tmp")
    m.add_artifact("a.csv", "csv", 10, "hex")
    d = m.to_dict()
    assert d["batch_id"] == ""
    assert d["artifacts"]["a.csv"]["batch_id"] == ""


def test_state_store_empty_watermark_value(tmp_path):
    """watermark value 为空字符串应能 commit（不抛异常）."""
    s = StateStore(str(tmp_path / "state"))
    state = s.load()
    s.set_new_watermark(state, "orders", "", 100, "B-1")
    s.commit_watermark(state, "B-1")
    assert s.get_watermark("orders") == ""


def test_state_store_empty_snapshot_id(tmp_path):
    """snapshot_id 为 None 应能 staged + commit（不抛异常）."""
    s = StateStore(str(tmp_path / "state"))
    state = s.load()
    s.set_new_snapshot_id(state, "orders", None, "B-1")
    s.commit_snapshot_id(state, "B-1")
    assert s.get_snapshot_id("orders") is None


# ----------------------------------------------------------------------
# 坏配置：None 值
# ----------------------------------------------------------------------
def test_state_store_none_watermark_value(tmp_path):
    """watermark value 为 None 应能 commit（不抛异常）."""
    s = StateStore(str(tmp_path / "state"))
    state = s.load()
    s.set_new_watermark(state, "orders", None, 100, "B-1")
    s.commit_watermark(state, "B-1")
    assert s.get_watermark("orders") is None


def test_metrics_recorder_none_dq_score():
    """finish 不传 dq_score 应保持 None，to_dict 不含 pipeline_dq_score."""
    r = MetricsRecorder("B-1")
    r.finish("success", 100)
    d = r.to_dict()
    assert d["dq_score"] is None
    assert "pipeline_dq_score" not in d["metrics"]


def test_metrics_recorder_none_quarantined_rows():
    """finish 不传 quarantined_rows 应保持 {}，quarantined_total=0."""
    r = MetricsRecorder("B-1")
    r.finish("success", 100)
    d = r.to_dict()
    assert d["quarantined_rows"] == {}
    assert d["quarantined_total"] == 0


# ----------------------------------------------------------------------
# 坏配置：CSV 字段含 None 值
# ----------------------------------------------------------------------
def test_csv_write_none_values(tmp_path):
    """csv_write 行中 None 值应写为空字符串（csv.DictWriter 默认行为）."""
    path = tmp_path / "none.csv"
    rows = [{"a": "1", "b": None, "c": "x"}]
    csv_write(str(path), ["a", "b", "c"], rows)
    data, _ = csv_read(str(path))
    assert len(data) == 1
    assert data[0]["a"] == "1"
    # None 写为空字符串
    assert data[0]["b"] == ""
    assert data[0]["c"] == "x"


def test_csv_write_mixed_none_and_empty(tmp_path):
    """csv_write 行中混合 None / 空字符串 / 正常值."""
    path = tmp_path / "mixed.csv"
    rows = [
        {"a": "1", "b": "x", "c": "y"},
        {"a": None, "b": "", "c": "z"},
        {"a": "3", "b": "w", "c": None},
    ]
    csv_write(str(path), ["a", "b", "c"], rows)
    data, _ = csv_read(str(path))
    assert len(data) == 3
    assert data[1]["a"] == ""
    assert data[1]["b"] == ""
    assert data[2]["c"] == ""


# ----------------------------------------------------------------------
# 坏配置：JSON 序列化边界
# ----------------------------------------------------------------------
def test_json_save_load_empty_dict(tmp_path):
    path = tmp_path / "empty.json"
    json_save(str(path), {})
    assert json_load(str(path)) == {}


def test_json_save_load_none_value(tmp_path):
    """JSON 中 None 值应能往返."""
    path = tmp_path / "null.json"
    obj = {"a": None, "b": 1, "c": "x"}
    json_save(str(path), obj)
    assert json_load(str(path)) == obj


def test_json_save_load_nested_empty(tmp_path):
    """嵌套空 dict / list 应能往返."""
    path = tmp_path / "nested.json"
    obj = {"outer": {"inner": {}}, "list": [], "items": [{}]}
    json_save(str(path), obj)
    assert json_load(str(path)) == obj


# ----------------------------------------------------------------------
# 坏配置：StateStore merge_aggregate 边界
# ----------------------------------------------------------------------
def test_merge_aggregate_empty_new_rows(tmp_path):
    """merge_aggregate 传空 new_rows 应不抛异常，返回 0."""
    s = StateStore(str(tmp_path / "state"))
    n = s.merge_aggregate("kpi", ["date", "orders"], [], key_cols=["date"])
    assert n == 0


def test_merge_aggregate_single_row(tmp_path):
    """merge_aggregate 单行 new_rows 应正常合并."""
    s = StateStore(str(tmp_path / "state"))
    n = s.merge_aggregate(
        "kpi", ["date", "orders"], [{"date": "2026-01-01", "orders": "5"}], key_cols=["date"]
    )
    assert n == 1
    data, _ = s.load_aggregate("kpi")
    assert len(data) == 1


def test_merge_aggregate_none_numeric_value(tmp_path):
    """merge_aggregate 行中数值字段为 None 应不抛异常（_is_numeric(None)=False）."""
    s = StateStore(str(tmp_path / "state"))
    n = s.merge_aggregate(
        "kpi", ["date", "orders"], [{"date": "2026-01-01", "orders": None}], key_cols=["date"]
    )
    assert n == 1


def test_merge_aggregate_empty_string_numeric_value(tmp_path):
    """merge_aggregate 行中数值字段为空字符串应不抛异常."""
    s = StateStore(str(tmp_path / "state"))
    n = s.merge_aggregate(
        "kpi", ["date", "orders"], [{"date": "2026-01-01", "orders": ""}], key_cols=["date"]
    )
    assert n == 1


# ----------------------------------------------------------------------
# 坏配置：Manifest 边界
# ----------------------------------------------------------------------
def test_manifest_add_artifact_none_rows():
    """add_artifact rows=None 应允许（某些产物行数未知）."""
    m = Manifest("B-1", "d", "/tmp")
    m.add_artifact("meta.json", "json", None, "hex")
    assert m.artifacts["meta.json"]["rows"] is None


def test_manifest_add_stage_zero_rows():
    """add_stage rows_in=0 / rows_out=0 应允许（空 stage）."""
    m = Manifest("B-1", "d", "/tmp")
    m.add_stage("noop", "success", 0, 0, 0, "log")
    assert m.stages[0]["rows_in"] == 0
    assert m.stages[0]["rows_out"] == 0


def test_manifest_finish_failed_no_error_msg():
    """finish("failed") 不传 error 应允许（error 保持 None）."""
    m = Manifest("B-1", "d", "/tmp")
    m.finish("failed")
    assert m.status == "failed"
    assert m.error is None


# ----------------------------------------------------------------------
# 坏配置：MetricsRecorder 边界
# ----------------------------------------------------------------------
def test_metrics_recorder_zero_duration():
    """finish total_duration_ms=0 应允许（极快 pipeline）."""
    r = MetricsRecorder("B-1")
    r.finish("success", 0)
    assert r.to_dict()["total_duration_ms"] == 0


def test_metrics_recorder_negative_quarantined_not_validated():
    """MetricsRecorder 不校验 quarantined_rows 值的正负（仅 sum）."""
    r = MetricsRecorder("B-1")
    r.finish("success", 100, quarantined_rows={"missing": -5})
    d = r.to_dict()
    # sum 不校验语义，仅算术求和
    assert d["quarantined_total"] == -5


# ----------------------------------------------------------------------
# s3_credentials：配置显式凭证 > 环境变量回退（安全收敛，2026-08）
# ----------------------------------------------------------------------
_ENV_KEYS = (
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
)


@pytest.fixture
def _clean_s3_env(monkeypatch):
    for k in _ENV_KEYS:
        monkeypatch.delenv(k, raising=False)
    return monkeypatch


def test_s3_credentials_explicit_config_wins(_clean_s3_env):
    from src.io._s3_parquet import s3_credentials

    _clean_s3_env.setenv("MINIO_ROOT_USER", "envuser")
    _clean_s3_env.setenv("MINIO_ROOT_PASSWORD", "envpass")
    cfg = {"storage": {"access_key": "ak", "secret_key": "sk"}}
    assert s3_credentials(cfg) == ("ak", "sk")


def test_s3_credentials_env_fallback_minio(_clean_s3_env):
    from src.io._s3_parquet import s3_credentials

    _clean_s3_env.setenv("MINIO_ROOT_USER", "envuser")
    _clean_s3_env.setenv("MINIO_ROOT_PASSWORD", "envpass")
    assert s3_credentials({"storage": {}}) == ("envuser", "envpass")


def test_s3_credentials_env_fallback_aws(_clean_s3_env):
    from src.io._s3_parquet import s3_credentials

    _clean_s3_env.setenv("AWS_ACCESS_KEY_ID", "awsid")
    _clean_s3_env.setenv("AWS_SECRET_ACCESS_KEY", "awskey")
    assert s3_credentials({"storage": {}}) == ("awsid", "awskey")


def test_s3_credentials_partial_config_backfills_from_env(_clean_s3_env):
    """只配了 access_key 时 secret 从环境回补（逐字段回退）."""
    from src.io._s3_parquet import s3_credentials

    _clean_s3_env.setenv("MINIO_ROOT_PASSWORD", "envpass")
    cfg = {"storage": {"access_key": "ak"}}
    assert s3_credentials(cfg) == ("ak", "envpass")


def test_s3_credentials_absent_everywhere(_clean_s3_env):
    from src.io._s3_parquet import s3_credentials

    assert s3_credentials({"storage": {}}) == ("", "")
