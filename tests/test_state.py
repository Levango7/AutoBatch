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

import csv
import json
import os
import subprocess
import sys

import pytest

from src.state import StateStore, recompute_derived


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
    assert (
        a["iceberg_snapshots"]["orders"]["snapshot_id"]
        == b["iceberg_snapshots"]["orders"]["snapshot_id"]
    )
    assert (
        a["tables"]["orders"]["cumulative_row_count"]
        == b["tables"]["orders"]["cumulative_row_count"]
    )


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
    store.merge_aggregate(
        "kpi",
        fields,
        [{"date": "2026-01-01", "orders": "10", "revenue": "1000.0"}],
        key_cols=["date"],
    )
    # 第二次同 key → 累加
    n = store.merge_aggregate(
        "kpi",
        fields,
        [{"date": "2026-01-01", "orders": "5", "revenue": "500.0"}],
        key_cols=["date"],
    )
    assert n == 1
    data, _ = store.load_aggregate("kpi")
    # CSV 读回为字符串，但数值应等于累加结果
    assert float(data[0]["orders"]) == 15.0
    assert float(data[0]["revenue"]) == 1500.0


def test_merge_aggregate_recomputes_avg_order_value(store):
    fields = ["date", "orders", "revenue", "avg_order_value"]
    store.merge_aggregate(
        "kpi",
        fields,
        [{"date": "2026-01-01", "orders": "10", "revenue": "1000.0", "avg_order_value": "100.0"}],
        key_cols=["date"],
    )
    store.merge_aggregate(
        "kpi",
        fields,
        [{"date": "2026-01-01", "orders": "5", "revenue": "500.0", "avg_order_value": "100.0"}],
        key_cols=["date"],
    )
    data, _ = store.load_aggregate("kpi")
    # orders=15, revenue=1500 → avg = 100.0
    assert float(data[0]["avg_order_value"]) == 100.0


def test_merge_aggregate_recomputes_revenue_share_and_rank(store):
    fields = ["date", "orders", "revenue", "revenue_share", "rank"]
    store.merge_aggregate(
        "kpi",
        fields,
        [
            {
                "date": "2026-01-01",
                "orders": "10",
                "revenue": "1000.0",
                "revenue_share": "1.0",
                "rank": "1",
            },
            {
                "date": "2026-01-02",
                "orders": "20",
                "revenue": "3000.0",
                "revenue_share": "1.0",
                "rank": "1",
            },
        ],
        key_cols=["date"],
    )
    data, _ = store.load_aggregate("kpi")
    by_date = {r["date"]: r for r in data}
    # total = 4000
    assert float(by_date["2026-01-01"]["revenue_share"]) == 0.25
    assert float(by_date["2026-01-02"]["revenue_share"]) == 0.75
    # rank by revenue desc: 2026-01-02 → 1, 2026-01-01 → 2
    assert int(by_date["2026-01-02"]["rank"]) == 1
    assert int(by_date["2026-01-01"]["rank"]) == 2


# ----------------------------------------------------------------------
# 任务 #74 M15：save_aggregate 原子写（tmp + os.replace）
# ----------------------------------------------------------------------
def test_save_aggregate_atomic_keeps_original_on_crash(store, monkeypatch):
    """写盘中途崩溃（writerow 抛异常）→ 正式 CSV 保持原样，永不半写.

    monkeypatch csv.DictWriter.writerow 首次调用即抛异常，模拟 os.replace
    之前的磁盘故障/崩溃；此时 tmp 文件可能残留，但目标文件必须仍是旧内容。
    """
    fields = ["date", "orders"]
    store.save_aggregate("kpi", fields, [{"date": "2026-01-01", "orders": "7"}])

    def _boom(self, row):
        raise RuntimeError("simulated crash mid-write")

    monkeypatch.setattr(csv.DictWriter, "writerow", _boom)
    with pytest.raises(RuntimeError, match="simulated crash"):
        store.save_aggregate("kpi", fields, [{"date": "2026-01-02", "orders": "99"}])

    data, loaded_fields = store.load_aggregate("kpi")
    assert loaded_fields == fields
    assert len(data) == 1
    assert data[0] == {"date": "2026-01-01", "orders": "7"}


def test_save_aggregate_no_tmp_residue_on_success(store):
    """成功路径无 .tmp 残留."""
    store.save_aggregate("kpi", ["date", "orders"], [{"date": "d", "orders": "1"}])
    assert not os.path.exists(store.get_aggregate_path("kpi") + ".tmp")


# ----------------------------------------------------------------------
# 任务 #74 M16：跨进程文件锁
# ----------------------------------------------------------------------
def test_locked_reentrant_within_same_thread(store):
    """同线程嵌套 locked() 不死锁；save() 内部再次取锁仍正常."""
    with store.locked():
        with store.locked():
            store.save({"version": "1.0", "tables": {}})
    with store.locked():
        pass


_CHILD_LOCK_SCRIPT = """
import sys
import time

sys.path.insert(0, {root!r})
from src.state import StateStore

store = StateStore({state_dir!r})
with store.locked():
    print("READY", flush=True)
    time.sleep({hold_seconds})
print("RELEASED", flush=True)
"""


def test_locked_cross_process_exclusion(store, tmp_path):
    """子进程持锁期间父进程 timeout 内取锁抛 TimeoutError；子进程退出后可取."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = tmp_path / "child_lock_holder.py"
    script.write_text(
        _CHILD_LOCK_SCRIPT.format(root=root, state_dir=store.state_dir, hold_seconds=1.5),
        encoding="utf-8",
    )
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        line = proc.stdout.readline()
        assert "READY" in line, f"child failed to acquire lock; stderr={proc.stderr.read()}"
        # 子进程持锁中：短超时取锁必须失败
        with pytest.raises(TimeoutError):
            with store.locked(timeout=0.5):
                pass
        proc.wait(timeout=15)
        assert proc.returncode == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
    # 子进程释放后：取锁必须成功
    with store.locked(timeout=5.0):
        pass


# ----------------------------------------------------------------------
# 任务 #74 minor：recompute_derived 防御性行为
# ----------------------------------------------------------------------
def test_recompute_derived_bad_orders_keeps_original_avg():
    """非数值 orders/revenue → 保留该行原 avg_order_value（不抛 ValueError）."""
    rows = [{"orders": "abc", "revenue": "100.0", "avg_order_value": "9.99"}]
    recompute_derived(rows, ["orders", "revenue", "avg_order_value"])
    assert rows[0]["avg_order_value"] == "9.99"


def test_recompute_derived_bad_revenue_keeps_share_and_ranks_last():
    """坏 revenue 行：保留原 revenue_share；rank 按 0 计（排最后）."""
    rows = [
        {"revenue": "300.0", "revenue_share": "0.0"},
        {"revenue": "bad", "revenue_share": "0.5"},
    ]
    recompute_derived(rows, ["revenue", "revenue_share", "rank"])
    # 有效合计 300 → 好行 share = 1.0；坏行保留原值
    assert float(rows[0]["revenue_share"]) == 1.0
    assert rows[1]["revenue_share"] == "0.5"
    assert int(rows[0]["rank"]) == 1
    assert int(rows[1]["rank"]) == 2


# ----------------------------------------------------------------------
# 任务 #74 minor：_merge_into 类型防护
# ----------------------------------------------------------------------
def test_merge_aggregate_rejects_non_numeric_into_numeric_column(store):
    """脏 delta 的非数值值不得覆盖数值列的历史累加值."""
    fields = ["date", "orders", "revenue"]
    store.merge_aggregate(
        "kpi", fields, [{"date": "d1", "orders": "10", "revenue": "100.0"}], key_cols=["date"]
    )
    store.merge_aggregate(
        "kpi",
        fields,
        [{"date": "d1", "orders": "bad-poison", "revenue": "not-a-number"}],
        key_cols=["date"],
    )
    data, _ = store.load_aggregate("kpi")
    assert data[0]["orders"] == "10"
    assert data[0]["revenue"] == "100.0"


def test_merge_aggregate_dimension_column_string_overwrite(store):
    """维度列（tier/city）允许字符串覆盖；数值列正常累加."""
    fields = ["customer_id", "tier", "city", "orders", "revenue"]
    store.merge_aggregate(
        "cv",
        fields,
        [
            {
                "customer_id": "c1",
                "tier": "bronze",
                "city": "shanghai",
                "orders": "5",
                "revenue": "100.0",
            }
        ],
        key_cols=["customer_id"],
    )
    store.merge_aggregate(
        "cv",
        fields,
        [
            {
                "customer_id": "c1",
                "tier": "gold",
                "city": "beijing",
                "orders": "2",
                "revenue": "50.0",
            }
        ],
        key_cols=["customer_id"],
    )
    data, _ = store.load_aggregate("cv")
    row = data[0]
    assert row["tier"] == "gold"
    assert row["city"] == "beijing"
    assert float(row["orders"]) == 7.0
    assert float(row["revenue"]) == 150.0


# ----------------------------------------------------------------------
# 任务 #74 C4：暂存台账提交协议
# ----------------------------------------------------------------------
def test_merge_aggregate_staged_only_writes_pending(store):
    """merge_aggregate_staged 只写暂存文件，正式聚合不动."""
    fields = ["date", "orders", "revenue"]
    store.save_aggregate("kpi", fields, [{"date": "d0", "orders": "1", "revenue": "10.0"}])
    count, pending = store.merge_aggregate_staged(
        "kpi", fields, [{"date": "d0", "orders": "4", "revenue": "40.0"}], key_cols=["date"]
    )
    assert count == 1
    assert pending == store.get_pending_aggregate_path("kpi")
    assert os.path.isfile(pending)
    # 正式聚合仍是旧值
    data, _ = store.load_aggregate("kpi")
    assert float(data[0]["orders"]) == 1.0
    # 暂存文件已是合并后的完整结果（替换幂等的基础）
    with open(pending, newline="", encoding="utf-8-sig") as f:
        pending_rows = list(csv.DictReader(f))
    assert float(pending_rows[0]["orders"]) == 5.0


def test_commit_batch_is_the_atomic_commit_point(store):
    """commit_batch 单次持久化完成：水位/快照提升 + 台账登记 + pending 标记."""
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.set_new_snapshot_id(state, "orders", 1001, "B-1")
    fields = ["date", "orders", "revenue"]
    _, pending = store.merge_aggregate_staged(
        "kpi", fields, [{"date": "d1", "orders": "3", "revenue": "30.0"}], key_cols=["date"]
    )
    store.commit_batch(state, "B-1", pending_aggregates={"kpi": pending})

    persisted = store.load()
    info = persisted["tables"]["orders"]
    assert info["watermark_value"] == "2026-02-01"
    assert info["cumulative_row_count"] == 100
    assert "new_watermark" not in info
    assert "new_snapshot_id" not in persisted["iceberg_snapshots"]["orders"]
    assert persisted["iceberg_snapshots"]["orders"]["snapshot_id"] == 1001
    assert persisted["merged_batches"] == ["B-1"]
    assert persisted["aggregates_pending"] == {"kpi": os.path.abspath(pending)}
    assert store.is_batch_merged(persisted, "B-1")
    assert not store.is_batch_merged(persisted, "B-999")
    # 正式聚合在 complete_pending_aggregates 之前不得被替换
    assert not os.path.exists(store.get_aggregate_path("kpi"))


def test_complete_pending_aggregates_replaces_then_idempotent(store):
    """崩溃恢复替换：新 Store 实例（模拟重启）按标记替换，且幂等."""
    state = store.load()
    fields = ["date", "orders", "revenue"]
    _, pending = store.merge_aggregate_staged(
        "kpi", fields, [{"date": "d1", "orders": "3", "revenue": "30.0"}], key_cols=["date"]
    )
    store.commit_batch(state, "B-1", pending_aggregates={"kpi": pending})

    store2 = StateStore(store.state_dir)
    state2 = store2.load()
    assert store2.complete_pending_aggregates(state2) == ["kpi"]
    data, _ = store2.load_aggregate("kpi")
    assert len(data) == 1
    assert float(data[0]["orders"]) == 3.0
    assert not os.path.exists(pending)
    assert "aggregates_pending" not in store2.load()
    # 幂等：无标记 → 空操作
    assert store2.complete_pending_aggregates(store2.load()) == []


def test_complete_pending_aggregates_handles_spark_directory(store):
    """spark 风格 part 文件目录暂存 → 替换为目录型正式聚合."""
    state = store.load()
    staged = store.get_pending_aggregate_path("spark_agg")
    os.makedirs(staged)
    with open(os.path.join(staged, "part-00000.csv"), "w", encoding="utf-8") as f:
        f.write("date,orders\n2026-01-01,5\n")
    store.commit_batch(state, "B-d", pending_aggregates={"spark_agg": staged})

    store2 = StateStore(store.state_dir)
    state2 = store2.load()
    assert store2.complete_pending_aggregates(state2) == ["spark_agg"]
    target = store2.get_aggregate_path("spark_agg")
    assert os.path.isdir(target)
    assert os.path.isfile(os.path.join(target, "part-00000.csv"))


def test_commit_batch_duplicate_keeps_single_ledger_entry(store):
    """台账幂等在 is_batch_merged 闸门执行；重复 commit_batch 只登记一条."""
    state = store.load()
    store.set_new_watermark(state, "orders", "2026-02-01", 100, "B-1")
    store.commit_batch(state, "B-1")
    state2 = store.load()
    store.set_new_watermark(state2, "orders", "2026-02-01", 100, "B-1")
    store.commit_batch(state2, "B-1")
    assert store.load()["merged_batches"].count("B-1") == 1


def test_commit_batch_caps_ledger(store):
    """台账限最近 200 条，避免 state.json 无限增长."""
    state = store.load()
    for i in range(205):
        store.commit_batch(state, f"B-{i}")
    ledger = store.load()["merged_batches"]
    assert len(ledger) == 200
    assert ledger[-1] == "B-204"
    assert "B-0" not in ledger


def test_commit_batch_without_pending_clears_stale_marker(store):
    """无暂存聚合的新提交点必须清掉陈旧 aggregates_pending 标记."""
    state = store.load()
    fields = ["date", "orders"]
    _, pending = store.merge_aggregate_staged(
        "kpi", fields, [{"date": "d1", "orders": "1"}], key_cols=["date"]
    )
    store.commit_batch(state, "B-1", pending_aggregates={"kpi": pending})
    store.complete_pending_aggregates(store.load())
    state2 = store.load()
    store.set_new_watermark(state2, "orders", "2026-03-01", 5, "B-2")
    store.commit_batch(state2, "B-2")
    assert "aggregates_pending" not in store.load()
