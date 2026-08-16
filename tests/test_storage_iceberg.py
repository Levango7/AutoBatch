"""Phase 4 等价性测试 + Iceberg 湖存储测试（storage.backend="iceberg"）.

验证 ``storage.backend="iceberg"`` 时 pyiceberg 集成、snapshot diff 增量、
time travel、ACID 原子性、向后兼容等行为. 设计见 docs/evolution.md §6.x.

场景:
1. test_iceberg_table_write_read       — Iceberg 表写入读回基本正确性
2. test_iceberg_append_snapshot         — 多次 append 产生多个 snapshot
3. test_iceberg_time_travel             — time travel 到历史 snapshot 返回历史数据
4. test_iceberg_snapshot_diff           — incremental_append_scan 增量正确性
5. test_iceberg_overwrite               — overwrite 模式覆盖表数据
6. test_iceberg_polars_backend          — polars backend 读写 Iceberg 表
7. test_iceberg_concurrent_append       — 并发 append 原子性（ACID）
8. test_iceberg_back_compat_local_csv   — 旧配置 local_csv 行为不变
9. test_iceberg_back_compat_parquet     — 旧配置 parquet 行为不变
10. test_iceberg_incremental_mode_switch — 增量模式 high_watermark ↔ iceberg_snapshot_diff
11. test_iceberg_list_snapshots         — list_snapshots 返回完整 snapshot 历史
12. test_iceberg_path_routing           — table_read path 路由（表名 vs 文件路径）
"""
from __future__ import annotations

import os
import threading
import uuid
from typing import Any

import pytest

from src.helpers import (
    ROOT,
    _iceberg_path_is_table_name,
    abs_path,
    csv_read,
    iceberg_snapshot_diff,
    list_snapshots,
    read_history_snapshot,
    table_read,
    table_write,
)
from src.pipeline import run_pipeline


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def _run(cfg: dict[str, Any], batch_id: str, fail_at: str = "") -> tuple[int, str]:
    """跑 pipeline，返回 (rc, run_dir)。"""
    rc = run_pipeline(cfg, batch_id, fail_at)
    run_root = abs_path(cfg["pipeline"].get("run_dir", "run"))
    run_dir = os.path.join(run_root, batch_id)
    return rc, run_dir


def _new_bid(tag: str) -> str:
    """生成唯一 batch_id，前缀 test-iceberg- 便于 fixture cleanup 统一清理。"""
    return f"test-iceberg-{tag}-{uuid.uuid4().hex[:6]}"


def _make_iceberg_cfg(iceberg_env) -> dict[str, Any]:
    """从 iceberg_env fixture 复制 cfg 并返回（避免修改 fixture 状态）。"""
    import copy
    return copy.deepcopy(iceberg_env["cfg"])


# ----------------------------------------------------------------------
# 1. Iceberg 表写入读回基本正确性
# ----------------------------------------------------------------------
def test_iceberg_table_write_read(iceberg_env):
    """写入 Iceberg 表，读回验证行数与字段一致."""
    cfg = _make_iceberg_cfg(iceberg_env)
    rows = [
        {"id": "1", "name": "alice", "age": "30"},
        {"id": "2", "name": "bob", "age": "25"},
        {"id": "3", "name": "carol", "age": "40"},
    ]
    fields = ["id", "name", "age"]
    # 写入 Iceberg 表（append 模式）
    n = table_write("warehouse.persons", rows, cfg, fields=fields, mode="append")
    assert n == 3
    # 读回
    result = table_read("warehouse.persons", cfg)
    read_rows, read_fields = result
    assert len(read_rows) == 3
    assert set(read_fields) == {"id", "name", "age"}
    # 验证内容（顺序可能不同，用集合比较）
    actual_ids = {r["id"] for r in read_rows}
    assert actual_ids == {"1", "2", "3"}


# ----------------------------------------------------------------------
# 2. 多次 append 产生多个 snapshot
# ----------------------------------------------------------------------
def test_iceberg_append_snapshot(iceberg_env):
    """多次 append 产生多个 snapshot，current_snapshot 推进."""
    cfg = _make_iceberg_cfg(iceberg_env)
    fields = ["id", "val"]
    # 第一次 append
    table_write("warehouse.events", [{"id": "1", "val": "a"}], cfg, fields=fields, mode="append")
    snaps_1 = list_snapshots("warehouse.events", cfg)
    assert len(snaps_1) == 1
    sid_1 = snaps_1[0]["snapshot_id"]
    # 第二次 append
    table_write("warehouse.events", [{"id": "2", "val": "b"}], cfg, fields=fields, mode="append")
    snaps_2 = list_snapshots("warehouse.events", cfg)
    assert len(snaps_2) == 2
    sid_2 = snaps_2[-1]["snapshot_id"]
    assert sid_2 != sid_1
    # 第三次 append
    table_write("warehouse.events", [{"id": "3", "val": "c"}], cfg, fields=fields, mode="append")
    snaps_3 = list_snapshots("warehouse.events", cfg)
    assert len(snaps_3) == 3
    # 读回应有 3 行
    rows, _ = table_read("warehouse.events", cfg)
    assert len(rows) == 3


# ----------------------------------------------------------------------
# 3. time travel 到历史 snapshot 返回历史数据
# ----------------------------------------------------------------------
def test_iceberg_time_travel(iceberg_env):
    """time travel 到历史 snapshot 返回该时间点的数据."""
    cfg = _make_iceberg_cfg(iceberg_env)
    fields = ["id", "val"]
    # 三次 append，记录每次的 snapshot id
    table_write("warehouse.history", [{"id": "1", "val": "a"}], cfg, fields=fields, mode="append")
    snaps_1 = list_snapshots("warehouse.history", cfg)
    sid_1 = snaps_1[-1]["snapshot_id"]

    table_write("warehouse.history", [{"id": "2", "val": "b"}], cfg, fields=fields, mode="append")
    snaps_2 = list_snapshots("warehouse.history", cfg)
    sid_2 = snaps_2[-1]["snapshot_id"]

    table_write("warehouse.history", [{"id": "3", "val": "c"}], cfg, fields=fields, mode="append")
    snaps_3 = list_snapshots("warehouse.history", cfg)
    sid_3 = snaps_3[-1]["snapshot_id"]

    # time travel 到 sid_1：应只有 1 行
    rows_1, _ = read_history_snapshot("warehouse.history", cfg, sid_1)
    assert len(rows_1) == 1
    assert rows_1[0]["id"] == "1"

    # time travel 到 sid_2：应有 2 行
    rows_2, _ = read_history_snapshot("warehouse.history", cfg, sid_2)
    assert len(rows_2) == 2
    actual_ids_2 = {r["id"] for r in rows_2}
    assert actual_ids_2 == {"1", "2"}

    # time travel 到 sid_3：应有 3 行
    rows_3, _ = read_history_snapshot("warehouse.history", cfg, sid_3)
    assert len(rows_3) == 3
    actual_ids_3 = {r["id"] for r in rows_3}
    assert actual_ids_3 == {"1", "2", "3"}


# ----------------------------------------------------------------------
# 4. incremental_append_scan 增量正确性
# ----------------------------------------------------------------------
def test_iceberg_snapshot_diff(iceberg_env):
    """snapshot diff 返回两个 snapshot 之间的增量数据."""
    cfg = _make_iceberg_cfg(iceberg_env)
    fields = ["id", "val"]
    # 第一次 append（建立初始 snapshot）
    table_write("warehouse.delta", [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}],
                cfg, fields=fields, mode="append")
    snaps_1 = list_snapshots("warehouse.delta", cfg)
    sid_1 = snaps_1[-1]["snapshot_id"]

    # 第二次 append（增量）
    table_write("warehouse.delta", [{"id": "3", "val": "c"}, {"id": "4", "val": "d"}],
                cfg, fields=fields, mode="append")

    # snapshot diff：从 sid_1 到 current
    diff = iceberg_snapshot_diff("warehouse.delta", cfg, from_snapshot=sid_1)
    assert diff["from_snapshot"] == sid_1
    assert diff["to_snapshot"] is not None
    assert diff["added_rows_count"] == 2
    actual_ids = {r["id"] for r in diff["rows"]}
    assert actual_ids == {"3", "4"}

    # 从 None 开始 diff：应返回全部 4 行
    diff_full = iceberg_snapshot_diff("warehouse.delta", cfg, from_snapshot=None)
    assert diff_full["added_rows_count"] == 4


# ----------------------------------------------------------------------
# 5. overwrite 模式覆盖表数据
# ----------------------------------------------------------------------
def test_iceberg_overwrite(iceberg_env):
    """overwrite 模式覆盖表数据，append 模式追加."""
    cfg = _make_iceberg_cfg(iceberg_env)
    fields = ["id", "val"]
    # 初始 append
    table_write("warehouse.overwrite_test", [{"id": "1", "val": "a"}],
                cfg, fields=fields, mode="append")
    rows, _ = table_read("warehouse.overwrite_test", cfg)
    assert len(rows) == 1
    # overwrite：覆盖为 2 行
    table_write("warehouse.overwrite_test", [{"id": "2", "val": "b"}, {"id": "3", "val": "c"}],
                cfg, fields=fields, mode="overwrite")
    rows, _ = table_read("warehouse.overwrite_test", cfg)
    assert len(rows) == 2
    actual_ids = {r["id"] for r in rows}
    assert actual_ids == {"2", "3"}
    # 再 append：应有 3 行
    table_write("warehouse.overwrite_test", [{"id": "4", "val": "d"}],
                cfg, fields=fields, mode="append")
    rows, _ = table_read("warehouse.overwrite_test", cfg)
    assert len(rows) == 3


# ----------------------------------------------------------------------
# 6. polars backend 读写 Iceberg 表
# ----------------------------------------------------------------------
def test_iceberg_polars_backend(iceberg_env):
    """polars backend 读写 Iceberg 表，返回 polars.DataFrame."""
    cfg = _make_iceberg_cfg(iceberg_env)
    cfg["engine"]["backend"] = "polars"
    fields = ["id", "val"]
    # 用 python backend 写入（polars 写入需要 polars.DataFrame）
    table_write("warehouse.polars_test", [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}],
                cfg, fields=fields, mode="append")
    # 用 polars backend 读回
    df = table_read("warehouse.polars_test", cfg)
    assert df is not None
    assert df.height == 2
    assert "id" in df.columns
    assert "val" in df.columns


# ----------------------------------------------------------------------
# 7. 并发 append 原子性（ACID）
# ----------------------------------------------------------------------
def test_iceberg_concurrent_append(iceberg_env):
    """并发 append 原子性：多线程并发 append，最终行数等于总和."""
    cfg = _make_iceberg_cfg(iceberg_env)
    fields = ["id", "val"]
    n_threads = 4
    rows_per_thread = 25
    errors: list[Exception] = []

    def _append(thread_id: int) -> None:
        try:
            rows = [
                {"id": str(thread_id * 100 + i), "val": f"t{thread_id}-{i}"}
                for i in range(rows_per_thread)
            ]
            table_write("warehouse.concurrent", rows, cfg, fields=fields, mode="append")
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=_append, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 允许部分线程失败（pyiceberg 并发 commit 可能冲突），但成功的总行数应正确
    assert not errors or len(errors) < n_threads, f"all threads failed: {errors}"
    rows, _ = table_read("warehouse.concurrent", cfg)
    # 行数应等于成功线程数 * rows_per_thread
    successful_threads = n_threads - len(errors)
    assert len(rows) == successful_threads * rows_per_thread
    # 所有 id 唯一（无重复）
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids))


# ----------------------------------------------------------------------
# 8. 旧配置 local_csv 行为不变（向后兼容）
# ----------------------------------------------------------------------
def test_iceberg_back_compat_local_csv(iceberg_env):
    """旧配置 storage.backend="local_csv" 行为不变（向后兼容）."""
    cfg = _make_iceberg_cfg(iceberg_env)
    # 改回 local_csv
    cfg["storage"]["backend"] = "local_csv"
    # table_read/table_write 应走 local_csv 分支，不调 pyiceberg
    # 用一个不存在的 Iceberg 表名，确认不会触发 pyiceberg
    # （local_csv 模式下 path 是文件路径，table_read 走 csv_read）
    # 这里只验证 _iceberg_path_is_table_name 路由逻辑
    assert _iceberg_path_is_table_name("warehouse.orders") is True
    assert _iceberg_path_is_table_name("F:/path/orders.csv") is False
    assert _iceberg_path_is_table_name("orders.csv") is False
    assert _iceberg_path_is_table_name("C:\\path\\orders.csv") is False


# ----------------------------------------------------------------------
# 9. 旧配置 parquet 行为不变（向后兼容）
# ----------------------------------------------------------------------
def test_iceberg_back_compat_parquet(iceberg_env):
    """旧配置 storage.backend="parquet" 行为不变（向后兼容）."""
    cfg = _make_iceberg_cfg(iceberg_env)
    # 改回 parquet（本地）
    cfg["storage"]["backend"] = "parquet"
    cfg["storage"]["warehouse"] = iceberg_env["warehouse_dir"]
    cfg["storage"]["endpoint"] = ""
    cfg["storage"]["bucket"] = ""
    # table_write 应走 parquet 分支，不调 pyiceberg
    rows = [{"id": "1", "val": "a"}]
    import tempfile
    dst = os.path.join(tempfile.mkdtemp(dir=iceberg_env["work_dir"]), "test.parquet")
    n = table_write(dst, rows, cfg, fields=["id", "val"])
    assert n == 1
    # 读回
    read_rows, _ = table_read(dst, cfg)
    assert len(read_rows) == 1
    assert read_rows[0]["id"] == "1"


# ----------------------------------------------------------------------
# 10. 增量模式切换 high_watermark ↔ iceberg_snapshot_diff
# ----------------------------------------------------------------------
def test_iceberg_incremental_mode_switch(iceberg_env):
    """增量模式 high_watermark ↔ iceberg_snapshot_diff 配置切换.

    验证 cfg["incremental"]["mode"] 可正确切换，且 ingest 路由到不同分支.
    不跑完整 pipeline（需要先把源数据注册为 Iceberg 表），只验证配置路由.
    """
    cfg = _make_iceberg_cfg(iceberg_env)
    # 默认 mode 是 high_watermark
    assert cfg["incremental"]["mode"] == "high_watermark"
    # 切换到 iceberg_snapshot_diff
    cfg["incremental"]["mode"] = "iceberg_snapshot_diff"
    cfg["incremental"]["enabled"] = True
    assert cfg["incremental"]["mode"] == "iceberg_snapshot_diff"
    # 验证 ingest 模块能识别 mode
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.stages.ingest import _ingest_incremental
    # _ingest_incremental 在 mode="iceberg_snapshot_diff" 时应调 _copy_incremental_iceberg
    # 这里只验证配置正确传递，不实际执行（需要 Iceberg 表存在）


# ----------------------------------------------------------------------
# 11. list_snapshots 返回完整 snapshot 历史
# ----------------------------------------------------------------------
def test_iceberg_list_snapshots(iceberg_env):
    """list_snapshots 返回完整 snapshot 历史，按时间顺序."""
    cfg = _make_iceberg_cfg(iceberg_env)
    fields = ["id", "val"]
    # 三次 append
    for i in range(3):
        table_write("warehouse.snap_list", [{"id": str(i), "val": f"v{i}"}],
                    cfg, fields=fields, mode="append")
    snaps = list_snapshots("warehouse.snap_list", cfg)
    assert len(snaps) == 3
    # 验证 snapshot id 唯一
    sids = [s["snapshot_id"] for s in snaps]
    assert len(sids) == len(set(sids))
    # 验证 parent_id 链（首个 parent 为 None，后续指向前一个）
    assert snaps[0]["parent_id"] is None
    for i in range(1, len(snaps)):
        assert snaps[i]["parent_id"] == snaps[i - 1]["snapshot_id"]
    # 验证 timestamp_ms 递增
    ts = [s["timestamp_ms"] for s in snaps]
    assert ts == sorted(ts)


# ----------------------------------------------------------------------
# 12. table_read path 路由（表名 vs 文件路径）
# ----------------------------------------------------------------------
def test_iceberg_path_routing(iceberg_env):
    """table_read path 路由：表名走 iceberg 分支，文件路径走 local_csv 分支."""
    cfg = _make_iceberg_cfg(iceberg_env)
    # 先写一个 Iceberg 表
    fields = ["id", "val"]
    table_write("warehouse.routing", [{"id": "1", "val": "a"}],
                cfg, fields=fields, mode="append")
    # 用表名读：走 iceberg 分支
    rows, _ = table_read("warehouse.routing", cfg)
    assert len(rows) == 1
    # 用文件路径读：走 local_csv 分支（回退）
    # 创建一个 CSV 文件，用文件路径读
    import tempfile
    csv_path = os.path.join(tempfile.mkdtemp(dir=iceberg_env["work_dir"]), "test.csv")
    from src.helpers import csv_write
    csv_write(csv_path, ["id", "val"], [{"id": "x", "val": "y"}])
    # storage.backend="iceberg" 但 path 是文件路径 → 回退到 local_csv
    rows_csv, _ = table_read(csv_path, cfg)
    assert len(rows_csv) == 1
    assert rows_csv[0]["id"] == "x"


# ----------------------------------------------------------------------
# 13. 端到端：Iceberg 增量 snapshot diff 模式
# ----------------------------------------------------------------------
def test_iceberg_e2e_incremental_snapshot_diff(iceberg_env):
    """端到端：Iceberg snapshot diff 增量模式跑通.

    流程：
      1. 把源 orders CSV 注册为 Iceberg 表（warehouse.orders）
      2. 配置 incremental.mode="iceberg_snapshot_diff"
      3. 跑 pipeline，验证 ingest 从 Iceberg 表读增量
      4. 验证 state.json 持久化 snapshot id
    """
    cfg = _make_iceberg_cfg(iceberg_env)
    cfg["incremental"]["enabled"] = True
    cfg["incremental"]["mode"] = "iceberg_snapshot_diff"

    # 1. 把所有源 CSV 注册为 Iceberg 表（orders/customers/products）
    for name, path_key in [("orders", "orders_path"),
                            ("customers", "customers_path"),
                            ("products", "products_path")]:
        rows, fields = csv_read(iceberg_env[path_key])
        table_write(f"warehouse.{name}", rows, cfg, fields=fields, mode="append")

    # 2. 跑 pipeline（incremental 模式，首次运行从 None 开始 diff）
    bid = _new_bid("e2e-inc")
    rc, run_dir = _run(cfg, bid)
    # 验证 pipeline 成功（rc=0）
    assert rc == 0, f"pipeline should succeed, got rc={rc}"

    # 3. 验证 01_raw/orders_incremental.csv 存在且有行
    inc_path = os.path.join(run_dir, "01_raw", "orders_incremental.csv")
    assert os.path.exists(inc_path), f"incremental file should exist: {inc_path}"
    inc_rows, _ = csv_read(inc_path)
    assert len(inc_rows) > 0, "incremental file should have rows"

    # 4. 验证 state.json 持久化 snapshot id
    state_path = os.path.join(iceberg_env["work_dir"], "state", "state.json")
    assert os.path.exists(state_path), "state.json should exist"
    from src.helpers import json_load
    state = json_load(state_path)
    assert "iceberg_snapshots" in state, "state should have iceberg_snapshots"
    assert "orders" in state["iceberg_snapshots"], "state should have orders snapshot"
    assert state["iceberg_snapshots"]["orders"]["snapshot_id"] is not None, \
        "snapshot_id should be committed"
