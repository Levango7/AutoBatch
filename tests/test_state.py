"""src/state.py 单元测试.

覆盖 StateStore 两阶段提交语义：
- load / save 基本读写
- watermark: set_new_watermark 暂存不持久化、commit_watermark 提升并持久化
- snapshot_id: set_new_snapshot_id / commit_snapshot_id 两阶段
- commit_all 原子性：watermark + snapshot 一次持久化
- 失败不推进：未 commit 时 get_watermark/get_snapshot_id 返回旧值
- merge_aggregate：累加 + 派生列重算
"""
from __future__ import annotations

import json
import os

import pytest

from src.state import StateStore


@pytest.fixture
def store(tmp_path):
    return StateStore(str(tmp_path / "state"))


# ----------------------------------------------------------------------
# load / save
# ----------------------------------------------------------------------
def test_load_empty_returns_skeleton(store):
    state = store.load()
    assert state["version"] == "1.0"
    assert state["tables"] == {}
    assert state["aggregates"] == {}
    assert state["iceberg_snapshots"] == {}


def test_save_persists_state_json(store):
    state = {"version": "1.0", "tables": {"orders": {"watermark_value": "2026-01-01"}}}
    store.save(state)
    assert os.path.isfile(store.state_path)
    loaded = store.load()
    assert loaded["tables"]["orders"]["watermark_value"] == "2026-01-01"


def test_save_atomic_uses_tmp_then_replace(store):
    """save 用 .tmp + os.replace 原子写盘，完成后无残留 .tmp 文件."""
    store.save({"version": "1.0", "tables": {}})
    assert not os.path.exists(store.state_path + ".tmp")


# ----------------------------------------------------------------------
# watermark: get / set_new / commit
# ----------------------------------------------------------------------
def test_get_watermark_none_when_absent(store):
    assert store.get_watermark("orders") is None


def test_set_new_watermark_does_not_persist(store):
    """set_new_watermark 只暂存到 in-memory state，不写盘."""
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    # state.json 不存在 → get_watermark 仍为 None
    assert store.get_watermark("orders") is None
    # 但 state dict 中已暂存 new_watermark
    assert state["tables"]["orders"]["new_watermark"] == "2026-02-01"


def test_commit_watermark_promotes_and_persists(store):
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.commit_watermark(state, "B-1")
    # 持久化后 get_watermark 返回新值
    assert store.get_watermark("orders") == "2026-02-01"
    # new_watermark 已被 pop
    state2 = store.load()
    info = state2["tables"]["orders"]
    assert "new_watermark" not in info
    assert info["watermark_value"] == "2026-02-01"
    assert info["last_seen_row_count"] == 100
    assert info["cumulative_row_count"] == 100
    assert info["last_batch_id"] == "B-1"
    assert "last_processed_at" in info


def test_commit_watermark_accumulates_row_count(store):
    """多次 commit 累加 cumulative_row_count."""
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.commit_watermark(state, "B-1")
    store.set_new_watermark(state, "orders", "2026-03-01", 200, "B-2")
    store.commit_watermark(state, "B-2")
    info = store.load()["tables"]["orders"]
    assert info["cumulative_row_count"] == 300
    assert info["watermark_value"] == "2026-03-01"
    assert info["last_batch_id"] == "B-2"


def test_commit_watermark_skips_tables_without_staged(store):
    """commit_watermark 不动没有 new_watermark 的表."""
    state = store.load()
    state["tables"]["customers"] = {"watermark_value": "2026-01-01"}
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.commit_watermark(state, "B-1")
    info = store.load()
    # customers 未被改动
    assert info["tables"]["customers"]["watermark_value"] == "2026-01-01"
    # orders 已提升
    assert info["tables"]["orders"]["watermark_value"] == "2026-02-01"


def test_failure_does_not_advance_watermark(store):
    """模拟失败：set_new_watermark 后不 commit，下次 load 仍读到旧值."""
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.commit_watermark(state, "B-1")
    assert store.get_watermark("orders") == "2026-02-01"
    # 第二次：set_new_watermark 后失败（不 commit）
    state2 = store.load()
    store.set_new_watermark(state2, "orders", "2026-03-01", 200, "B-2")
    # 不 commit → state.json 仍是 B-1 的状态
    assert store.get_watermark("orders") == "2026-02-01"


# ----------------------------------------------------------------------
# snapshot_id: get / set_new / commit
# ----------------------------------------------------------------------
def test_get_snapshot_id_none_when_absent(store):
    assert store.get_snapshot_id("orders") is None


def test_set_new_snapshot_id_does_not_persist(store):
    state = store.load()
    store.set_new_snapshot_id(state, "orders", 1001, "B-1")
    assert store.get_snapshot_id("orders") is None
    assert state["iceberg_snapshots"]["orders"]["new_snapshot_id"] == 1001


def test_commit_snapshot_id_promotes_and_persists(store):
    state = store.load()
    store.set_new_snapshot_id(state, "orders", 1001, "B-1")
    store.commit_snapshot_id(state, "B-1")
    assert store.get_snapshot_id("orders") == 1001
    info = store.load()["iceberg_snapshots"]["orders"]
    assert "new_snapshot_id" not in info
    assert info["snapshot_id"] == 1001
    assert info["last_batch_id"] == "B-1"


def test_commit_snapshot_id_returns_int(store):
    """get_snapshot_id 应返回 int（即使 state.json 中存的是字符串数字）."""
    state = store.load()
    store.set_new_snapshot_id(state, "orders", 42, "B-1")
    store.commit_snapshot_id(state, "B-1")
    sid = store.get_snapshot_id("orders")
    assert isinstance(sid, int)
    assert sid == 42


def test_failure_does_not_advance_snapshot_id(store):
    state = store.load()
    store.set_new_snapshot_id(state, "orders", 1001, "B-1")
    store.commit_snapshot_id(state, "B-1")
    # 第二次失败：set_new 后不 commit
    state2 = store.load()
    store.set_new_snapshot_id(state2, "orders", 1002, "B-2")
    assert store.get_snapshot_id("orders") == 1001


# ----------------------------------------------------------------------
# commit_all: 原子性
# ----------------------------------------------------------------------
def test_commit_all_promotes_both_watermark_and_snapshot(store):
    """commit_all 同时提升 watermark 与 snapshot_id，单次持久化."""
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.set_new_snapshot_id(state, "orders", 1001, "B-1")
    store.commit_all(state, "B-1")
    # 两者都已提升
    assert store.get_watermark("orders") == "2026-02-01"
    assert store.get_snapshot_id("orders") == 1001
    info = store.load()
    assert "new_watermark" not in info["tables"]["orders"]
    assert "new_snapshot_id" not in info["iceberg_snapshots"]["orders"]


def test_commit_all_only_watermark(store):
    """只有 staged watermark 时 commit_all 仅提升 watermark."""
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.commit_all(state, "B-1")
    assert store.get_watermark("orders") == "2026-02-01"
    assert store.get_snapshot_id("orders") is None


def test_commit_all_only_snapshot(store):
    """只有 staged snapshot_id 时 commit_all 仅提升 snapshot."""
    state = store.load()
    store.set_new_snapshot_id(state, "orders", 1001, "B-1")
    store.commit_all(state, "B-1")
    assert store.get_snapshot_id("orders") == 1001
    assert store.get_watermark("orders") is None


def test_commit_all_no_tmp_residual(store):
    """commit_all 单次 save，无中间 .tmp 残留."""
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.set_new_snapshot_id(state, "orders", 1001, "B-1")
    store.commit_all(state, "B-1")
    assert not os.path.exists(store.state_path + ".tmp")


def test_commit_all_equivalent_to_separate_commits(store):
    """commit_all 与 commit_watermark + commit_snapshot_id 等价（最终状态相同）.

    注意：分别 commit 会 save 两次，commit_all 只 save 一次，但最终
    state.json 内容应等价（除了 last_processed_at 时间戳可能不同）.
    """
    # 用 commit_all
    store_a = StateStore(str(store.state_dir + "_a"))
    state_a = store_a.load()
    store_a.set_new_watermark(state_a, "orders", "2026-02-01", 100, "B-1")
    store_a.set_new_snapshot_id(state_a, "orders", 1001, "B-1")
    store_a.commit_all(state_a, "B-1")

    # 用分别 commit
    store_b = StateStore(str(store.state_dir + "_b"))
    state_b = store_b.load()
    store_b.set_new_watermark(state_b, "orders", "2026-02-01", 100, "B-1")
    store_b.set_new_snapshot_id(state_b, "orders", 1001, "B-1")
    store_b.commit_snapshot_id(state_b, "B-1")
    store_b.commit_watermark(state_b, "B-1")

    a = store_a.load()
    b = store_b.load()
    assert a["tables"]["orders"]["watermark_value"] == b["tables"]["orders"]["watermark_value"]
    assert a["iceberg_snapshots"]["orders"]["snapshot_id"] == b["iceberg_snapshots"]["orders"]["snapshot_id"]
    assert a["tables"]["orders"]["cumulative_row_count"] == b["tables"]["orders"]["cumulative_row_count"]


# ----------------------------------------------------------------------
# aggregate persistence
# ----------------------------------------------------------------------
def test_get_aggregate_path(store):
    p = store.get_aggregate_path("kpi_daily")
    assert p.endswith(os.path.join("aggregates", "kpi_daily.csv"))


def test_load_aggregate_empty_when_absent(store):
    data, fields = store.load_aggregate("kpi_daily")
    assert data == []
    assert fields == []


def test_save_then_load_aggregate(store):
    fields = ["date", "orders", "revenue"]
    rows = [
        {"date": "2026-01-01", "orders": "10", "revenue": "1000.0"},
        {"date": "2026-01-02", "orders": "20", "revenue": "2000.0"},
    ]
    store.save_aggregate("kpi_daily", fields, rows)
    data, loaded_fields = store.load_aggregate("kpi_daily")
    assert loaded_fields == fields
    assert len(data) == 2
    assert data[0]["date"] == "2026-01-01"


# ----------------------------------------------------------------------
# merge_aggregate
# ----------------------------------------------------------------------
def test_merge_aggregate_appends_new_keys(store):
    fields = ["date", "orders", "revenue"]
    new_rows = [{"date": "2026-01-01", "orders": "10", "revenue": "1000.0"}]
    n = store.merge_aggregate("kpi_daily", fields, new_rows, key_cols=["date"])
    assert n == 1
    data, _ = store.load_aggregate("kpi_daily")
    assert data[0]["orders"] == "10"


def test_merge_aggregate_accumulates_existing_key(store):
    fields = ["date", "orders", "revenue"]
    # 第一次写入
    store.merge_aggregate("kpi", fields,
                          [{"date": "2026-01-01", "orders": "10", "revenue": "1000.0"}],
                          key_cols=["date"])
    # 第二次同 key → 累加
    n = store.merge_aggregate("kpi", fields,
                              [{"date": "2026-01-01", "orders": "5", "revenue": "500.0"}],
                              key_cols=["date"])
    assert n == 1
    data, _ = store.load_aggregate("kpi")
    # CSV 读回为字符串，但数值应等于累加结果
    assert float(data[0]["orders"]) == 15.0
    assert float(data[0]["revenue"]) == 1500.0


def test_merge_aggregate_recomputes_avg_order_value(store):
    fields = ["date", "orders", "revenue", "avg_order_value"]
    store.merge_aggregate("kpi", fields,
                          [{"date": "2026-01-01", "orders": "10", "revenue": "1000.0",
                            "avg_order_value": "100.0"}],
                          key_cols=["date"])
    store.merge_aggregate("kpi", fields,
                          [{"date": "2026-01-01", "orders": "5", "revenue": "500.0",
                            "avg_order_value": "100.0"}],
                          key_cols=["date"])
    data, _ = store.load_aggregate("kpi")
    # orders=15, revenue=1500 → avg = 100.0
    assert float(data[0]["avg_order_value"]) == 100.0


def test_merge_aggregate_recomputes_revenue_share_and_rank(store):
    fields = ["date", "orders", "revenue", "revenue_share", "rank"]
    store.merge_aggregate("kpi", fields, [
        {"date": "2026-01-01", "orders": "10", "revenue": "1000.0",
         "revenue_share": "1.0", "rank": "1"},
        {"date": "2026-01-02", "orders": "20", "revenue": "3000.0",
         "revenue_share": "1.0", "rank": "1"},
    ], key_cols=["date"])
    data, _ = store.load_aggregate("kpi")
    by_date = {r["date"]: r for r in data}
    # total = 4000
    assert float(by_date["2026-01-01"]["revenue_share"]) == 0.25
    assert float(by_date["2026-01-02"]["revenue_share"]) == 0.75
    # rank by revenue desc: 2026-01-02 → 1, 2026-01-01 → 2
    assert int(by_date["2026-01-02"]["rank"]) == 1
    assert int(by_date["2026-01-01"]["rank"]) == 2
