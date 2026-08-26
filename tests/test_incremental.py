"""增量处理测试（Phase 1）：5 个场景覆盖首次运行、零增量、追加、失败重跑、全量回归。

设计见 docs/evolution.md §3.3.1~§3.3.6。每个测试用 inc_env fixture 隔离
state 目录和数据目录，run_dir 用唯一 batch_id 隔离。

场景:
1. test_incremental_first_run_equals_full    — 首次增量 = 全量 + 建立水位
2. test_incremental_no_new_data_second_run   — 无新数据二跑 = 零增量
3. test_incremental_append_new_data          — 追加新数据后 = 只处理新增行，聚合 merge 正确
4. test_incremental_failure_idempotent       — 失败重跑幂等（水位不推进，重跑同批增量）
5. test_full_mode_regression                 — 全量模式（enabled:false）行为不变回归
"""

from __future__ import annotations

import copy
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

from src.helpers import PipelineContext, abs_path, csv_read, csv_write, json_load
from src.lineage import Manifest
from src.pipeline import _advance_and_merge, run_pipeline
from src.state import StateStore


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
    """生成唯一 batch_id，前缀 test-inc- 便于 inc_env cleanup 统一清理。"""
    return f"test-inc-{tag}-{uuid.uuid4().hex[:6]}"


def _csv_count(path: str) -> int:
    """CSV 数据行数（不含 header）；文件不存在返回 -1。"""
    if not os.path.exists(path):
        return -1
    rows, _ = csv_read(path)
    return len(rows)


def _read_state(state_dir: str) -> dict[str, Any]:
    return json_load(os.path.join(state_dir, "state.json"))


def _max_col(path: str, col: str) -> str:
    rows, _ = csv_read(path)
    return max(r[col] for r in rows if r.get(col))


def _next_date(date_str: str, n: int = 1) -> str:
    """date_str + n 天，返回 'YYYY-MM-DD'。"""
    d = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=n)
    return d.strftime("%Y-%m-%d")


def _append_orders(orders_path: str, new_rows: list[dict[str, str]]) -> None:
    """向 orders.csv 追加行（保留原 header）。"""
    existing, fields = csv_read(orders_path)
    csv_write(orders_path, fields, existing + new_rows)


def _make_new_orders(
    n: int, start_id: int, cid: str, pid: str, base_date: str, unit_price: str = "100000.00"
) -> list[dict[str, str]]:
    """生成 n 个合法新订单，order_date 从 base_date 开始每日递增。

    order_id 用 {:08d} 格式化，确保匹配 ^ORD-\\d{8}$ 规则；start_id 取 100001+
    避免与生成器 ORD-00000001~ORD-00005000 冲突。unit_price 默认 100000.00
    （range 上限），让新订单 revenue 足够高以确保 cid 进入 customer_value top。
    """
    rows = []
    for i in range(n):
        date_str = _next_date(base_date, i)
        rows.append(
            {
                "order_id": f"ORD-{start_id + i:08d}",
                "customer_id": cid,
                "product_id": pid,
                "order_date": date_str,
                "created_ts": date_str + "T10:00:00",
                "region": "华东",
                "channel": "web",
                "quantity": "5",
                "unit_price": unit_price,
                "status": "completed",
            }
        )
    return rows


# ----------------------------------------------------------------------
# 场景 1: 首次增量运行 = 全量 + 建立水位
# ----------------------------------------------------------------------
def test_incremental_first_run_equals_full(inc_env):
    """首次增量运行（enabled:true，无 state.json）应等价于全量运行。

    验证：status=success、state.json 生成且水位正确、产物行数与全量模式一致。
    """
    env = inc_env
    cfg = env["cfg"]
    state_dir = env["state_dir"]

    # 首次增量运行（无 state.json）
    bid_inc = _new_bid("first-inc")
    rc_inc, run_dir_inc = _run(cfg, bid_inc)
    assert rc_inc == 0, "首次增量运行应成功"

    # status=success
    status = json_load(os.path.join(run_dir_inc, "status.json"))
    assert status["status"] == "success"

    # state.json 生成且水位正确（orders/customers 的 watermark = max(列)）
    state = _read_state(state_dir)
    assert "tables" in state
    assert "orders" in state["tables"]
    assert "customers" in state["tables"]
    expected_orders_wm = _max_col(env["orders_path"], "order_date")
    expected_cust_wm = _max_col(env["customers_path"], "join_date")
    assert state["tables"]["orders"]["watermark_value"] == expected_orders_wm, (
        "orders 水位应为 max(order_date)"
    )
    assert state["tables"]["customers"]["watermark_value"] == expected_cust_wm, (
        "customers 水位应为 max(join_date)"
    )

    # 产物行数与全量模式一致
    inc_final = _csv_count(os.path.join(run_dir_inc, "05_output", "orders_final.csv"))

    cfg_full = copy.deepcopy(cfg)
    cfg_full["incremental"]["enabled"] = False
    bid_full = _new_bid("first-full")
    rc_full, run_dir_full = _run(cfg_full, bid_full)
    assert rc_full == 0, "全量运行应成功"
    full_final = _csv_count(os.path.join(run_dir_full, "05_output", "orders_final.csv"))

    assert inc_final == full_final, f"首次增量产物行数 {inc_final} 应等于全量 {full_final}"


# ----------------------------------------------------------------------
# 场景 2: 无新数据二跑 = 零增量
# ----------------------------------------------------------------------
def test_incremental_no_new_data_second_run(inc_env):
    """首次增量运行后，不改数据源，第二次增量运行应零增量。

    验证：status=success、orders_incremental.csv 行数为 0、水位不变、
    DQ Score=1.0（无新增检查项）。
    """
    env = inc_env
    cfg = env["cfg"]
    state_dir = env["state_dir"]

    # 首次增量运行
    bid1 = _new_bid("2run-1")
    _run(cfg, bid1)
    state1 = _read_state(state_dir)
    wm1 = state1["tables"]["orders"]["watermark_value"]

    # 第二次增量运行（不改数据源）
    bid2 = _new_bid("2run-2")
    rc2, run_dir2 = _run(cfg, bid2)
    assert rc2 == 0, "第二次增量运行应成功"

    # status=success
    status2 = json_load(os.path.join(run_dir2, "status.json"))
    assert status2["status"] == "success"

    # orders_incremental.csv 行数为 0
    inc_csv = os.path.join(run_dir2, "01_raw", "orders_incremental.csv")
    assert _csv_count(inc_csv) == 0, "无新数据时 orders_incremental.csv 应为 0 行"

    # 水位不变
    state2 = _read_state(state_dir)
    assert state2["tables"]["orders"]["watermark_value"] == wm1, "无新数据时水位应不变"

    # DQ Score=1.0（无新增检查项：orders/customers 增量为空，products 全量但无缺陷）
    manifest = json_load(os.path.join(run_dir2, "manifest.json"))
    dq = manifest["quality"]["dq_score"]
    assert dq == 1.0, f"无新增检查项时 DQ Score 应为 1.0，实际 {dq}"


# ----------------------------------------------------------------------
# 场景 3: 追加新数据后 = 只处理新增行，聚合 merge 正确
# ----------------------------------------------------------------------
def test_incremental_append_new_data(inc_env):
    """首次增量运行后，向 orders.csv 追加新行（新日期>水位），第二次增量只处理新增行。

    验证：orders_incremental.csv 只含新增行、水位推进到新 max、
    state/aggregates/ 聚合 merge 正确（daily_sales 行数增加、customer_value 累加）。
    """
    env = inc_env
    cfg = env["cfg"]
    state_dir = env["state_dir"]

    # 首次增量运行
    bid1 = _new_bid("append-1")
    _run(cfg, bid1)
    state1 = _read_state(state_dir)
    wm1 = state1["tables"]["orders"]["watermark_value"]

    # 首次运行后的 daily_sales 行数
    daily_path = os.path.join(state_dir, "aggregates", "daily_sales.csv")
    n_daily1 = _csv_count(daily_path)

    # 追加新订单（order_date > 水位，10 个不同日期）
    cust_rows, _ = csv_read(env["customers_path"])
    prod_rows, _ = csv_read(env["products_path"])
    cid = cust_rows[0]["customer_id"]
    pid = prod_rows[0]["product_id"]

    n_new = 10
    base_date = _next_date(wm1)  # wm1 + 1 天，确保 > 水位
    new_orders = _make_new_orders(
        n_new, start_id=100001, cid=cid, pid=pid, base_date=base_date, unit_price="100000.00"
    )
    _append_orders(env["orders_path"], new_orders)

    # 第二次增量运行（处理新增行）
    bid2 = _new_bid("append-2")
    rc2, run_dir2 = _run(cfg, bid2)
    assert rc2 == 0, "追加新数据后增量运行应成功"

    # orders_incremental.csv 只含新增行
    inc_csv = os.path.join(run_dir2, "01_raw", "orders_incremental.csv")
    assert _csv_count(inc_csv) == n_new, f"orders_incremental.csv 应只含新增 {n_new} 行"

    # 水位推进到新 max
    state2 = _read_state(state_dir)
    expected_new_wm = max(o["order_date"] for o in new_orders)
    assert state2["tables"]["orders"]["watermark_value"] == expected_new_wm, "水位应推进到新 max"

    # daily_sales 行数增加（新增 10 个日期桶）
    n_daily2 = _csv_count(daily_path)
    assert n_daily2 == n_daily1 + n_new, (
        f"daily_sales 行数应增加 {n_new}，实际 {n_daily1} -> {n_daily2}"
    )

    # 新日期都在 daily_sales 中
    daily_rows2, _ = csv_read(daily_path)
    daily_dates = {r["order_date"] for r in daily_rows2}
    for o in new_orders:
        assert o["order_date"] in daily_dates, "新日期 {} 应在 daily_sales 中".format(
            o["order_date"]
        )

    # customer_value 累加正确（cid 的 orders 累加 n_new）
    # unit_price=100000 让 cid 的增量 revenue = 10*5*100000 = 5,000,000，
    # 足以让 cid 进入 customer_value top（merge 不截断 top_n，全部保留）。
    cv_path = os.path.join(state_dir, "aggregates", "customer_value.csv")
    cv_rows, _ = csv_read(cv_path)
    cv_by_cid = {r["customer_id"]: r for r in cv_rows}
    assert cid in cv_by_cid, "cid 应在 customer_value 中（revenue 足够高）"
    assert int(cv_by_cid[cid]["orders"]) >= n_new, f"customer_value 的 orders 应累加 >= {n_new}"


# ----------------------------------------------------------------------
# 场景 4: 失败重跑幂等（fail-at 后 state 不推进，重跑同批增量）
# ----------------------------------------------------------------------
def test_incremental_failure_idempotent(inc_env):
    """增量运行中模拟失败（fail_at=output），验证 state.json 水位未推进。
    重跑同批数据，水位应推进到正确值。

    验证：失败时 state.json 保持旧水位（set_new_watermark 但未 commit_watermark）；
    重跑成功后水位推进到新 max。
    """
    env = inc_env
    cfg = env["cfg"]
    state_dir = env["state_dir"]

    # 首次增量运行（成功）
    bid1 = _new_bid("fail-1")
    _run(cfg, bid1)
    state1 = _read_state(state_dir)
    wm1 = state1["tables"]["orders"]["watermark_value"]

    # 追加新订单
    cust_rows, _ = csv_read(env["customers_path"])
    prod_rows, _ = csv_read(env["products_path"])
    cid = cust_rows[0]["customer_id"]
    pid = prod_rows[0]["product_id"]

    n_new = 5
    base_date = _next_date(wm1)
    new_orders = _make_new_orders(
        n_new, start_id=200001, cid=cid, pid=pid, base_date=base_date, unit_price="100.00"
    )
    _append_orders(env["orders_path"], new_orders)

    # 失败运行（fail_at=output：ingest/validate/clean/compute 已成功，output 注入失败）
    # pipeline 跳过 _advance_and_merge，state.json 保持旧水位
    bid_fail = _new_bid("fail-fail")
    rc_fail, _ = _run(cfg, bid_fail, fail_at="output")
    assert rc_fail == 1, "fail_at=output 应失败"

    # state.json 的水位未推进（两阶段提交：set_new_watermark 在内存，未 commit）
    state_fail = _read_state(state_dir)
    assert state_fail["tables"]["orders"]["watermark_value"] == wm1, "失败时水位不应推进"

    # 重跑同批数据（无 fail_at）：ingest 重读同一增量（水位未推进），全阶段成功后 commit
    bid_retry = _new_bid("fail-retry")
    rc_retry, _ = _run(cfg, bid_retry)
    assert rc_retry == 0, "重跑应成功"

    # 水位推进到正确值
    state_retry = _read_state(state_dir)
    expected_new_wm = max(o["order_date"] for o in new_orders)
    assert state_retry["tables"]["orders"]["watermark_value"] == expected_new_wm, (
        "重跑后水位应推进到新 max"
    )


# ----------------------------------------------------------------------
# 场景 5: 全量模式（enabled:false）行为不变回归
# ----------------------------------------------------------------------
def test_full_mode_regression(inc_env):
    """enabled:false 时行为与全量模式 100% 一致。

    验证：不生成 state.json、产物与现有 test_pipeline_e2e.py 的全量断言一致
    （status=success、DQ Score in [0.95, 1.0]、manifest lineage nonempty、
    metrics 5 stages）、DQ Score 在预期区间。
    """
    env = inc_env
    cfg = env["cfg"]
    state_dir = env["state_dir"]

    # 全量模式（enabled=false）
    cfg_full = copy.deepcopy(cfg)
    cfg_full["incremental"]["enabled"] = False

    bid = _new_bid("full-reg")
    rc, run_dir = _run(cfg_full, bid)
    assert rc == 0, "全量模式应成功"

    # 不生成 state.json（inc_env 未跑增量，state_dir 为空；全量模式不碰 state）
    assert not os.path.exists(os.path.join(state_dir, "state.json")), "全量模式不应生成 state.json"

    # status=success
    status = json_load(os.path.join(run_dir, "status.json"))
    assert status["status"] == "success"

    # DQ Score 在 [0.95, 1.0]（与 test_pipeline_e2e.py::test_dq_score_in_range 一致）
    manifest = json_load(os.path.join(run_dir, "manifest.json"))
    dq = manifest["quality"]["dq_score"]
    assert 0.95 <= dq <= 1.0, f"DQ Score {dq} 不在 [0.95, 1.0]"

    # manifest lineage nonempty（与 test_pipeline_e2e.py::test_manifest_lineage_nonempty 一致）
    assert len(manifest["lineage"]) > 0

    # metrics 5 stages（与 test_pipeline_e2e.py::test_metrics_json_exists 一致）
    metrics = json_load(os.path.join(run_dir, "metrics.json"))
    assert "stages" in metrics
    assert len(metrics["stages"]) == 5
    assert metrics["status"] == "success"


# ----------------------------------------------------------------------
# 场景 6（任务 #74 C2+C4）：resume 后水位恰好推进一次，追加新数据不翻倍
# ----------------------------------------------------------------------
def test_resume_incremental_watermark_advance_once(inc_env):
    """validate 失败 → 同 batch_id 续跑成功：staged 水位恢复并提升恰好一次；
    再追加新数据跑第三批：旧日期桶不得重复累加（修复前双倍聚合回归锁）.

    修复前缺陷链：崩溃丢失 staged 水位 → 续跑提交无水位可提升 →
    第三批从旧水位重读本批增量 → daily_sales 旧桶翻倍。
    """
    env = inc_env
    cfg = env["cfg"]
    cfg["error_handling"]["resume"] = True
    state_dir = env["state_dir"]
    daily_path = os.path.join(state_dir, "aggregates", "daily_sales.csv")

    # --- 第一次：validate 失败（两阶段提交 → 水位不推进）---
    bid = _new_bid("resume-wm")
    rc1, _ = _run(cfg, bid, fail_at="validate")
    assert rc1 == 1
    state_path = os.path.join(state_dir, "state.json")
    if os.path.exists(state_path):
        state_fail = _read_state(state_dir)
        assert not state_fail.get("tables", {}).get("orders", {}).get("watermark_value"), (
            "失败时水位不得推进"
        )

    expected_wm = _max_col(env["orders_path"], "order_date")

    # --- 续跑：同 cfg 同 batch_id ---
    rc2, _ = _run(cfg, bid, fail_at="")
    assert rc2 == 0
    state2 = _read_state(state_dir)
    info2 = state2["tables"]["orders"]
    assert info2["watermark_value"] == expected_wm, "续跑应把 staged 水位正式提升（C2）"
    assert info2["cumulative_row_count"] == info2["last_seen_row_count"] > 0, (
        "水位恰好推进一次（无丢失、无双提交）"
    )
    assert bid in state2.get("merged_batches", []), "台账应登记批次（C4）"
    daily1, _ = csv_read(daily_path)
    n_daily1 = len(daily1)
    assert n_daily1 > 0

    # --- 追加新订单，第三批（新 batch_id）---
    cust_rows, _ = csv_read(env["customers_path"])
    prod_rows, _ = csv_read(env["products_path"])
    n_new = 5
    new_orders = _make_new_orders(
        n_new,
        start_id=300001,
        cid=cust_rows[0]["customer_id"],
        pid=prod_rows[0]["product_id"],
        base_date=_next_date(expected_wm),
    )
    _append_orders(env["orders_path"], new_orders)

    bid3 = _new_bid("resume-wm-next")
    rc3, _ = _run(cfg, bid3)
    assert rc3 == 0

    state3 = _read_state(state_dir)
    info3 = state3["tables"]["orders"]
    assert info3["watermark_value"] == max(o["order_date"] for o in new_orders), (
        "第三批应只从续跑后的新水位起读增量"
    )
    assert (
        info3["cumulative_row_count"]
        == info2["cumulative_row_count"] + info3["last_seen_row_count"]
    ), "第三批只见新增行（修复前会重读第二批增量）"

    daily2, _ = csv_read(daily_path)
    assert len(daily2) == n_daily1 + n_new, "新日期各成一桶，旧桶不分裂不重复"
    by_date1 = {r["order_date"]: r for r in daily1}
    by_date2 = {r["order_date"]: r for r in daily2}
    for date_str, row1 in by_date1.items():
        row2 = by_date2[date_str]
        assert row2["orders"] == row1["orders"], f"{date_str} 桶 orders 被重复累加"
        assert row2["revenue"] == row1["revenue"], f"{date_str} 桶 revenue 被重复累加"


# ----------------------------------------------------------------------
# 场景 7（任务 #74 C2）：output 失败续跑——全部前序 stage resume，
# staged 水位仍从失败 manifest 恢复并恰好提交一次
# ----------------------------------------------------------------------
def test_resume_after_output_failure_commits_staged_watermark(inc_env):
    """fail_at=output → 同 batch_id 续跑：ingest/validate/clean/compute 全部
    resume，staged 水位经恢复后在提交点提升恰好一次，聚合并入正式路径.
    """
    env = inc_env
    cfg = env["cfg"]
    cfg["error_handling"]["resume"] = True
    state_dir = env["state_dir"]

    bid = _new_bid("resume-out")
    rc1, run_dir1 = _run(cfg, bid, fail_at="output")
    assert rc1 == 1
    # 失败 manifest 的 ingest 条目携带 staged_state
    manifest1 = json_load(os.path.join(run_dir1, "manifest.json"))
    ingest1 = next(s for s in manifest1["stages"] if s["name"] == "ingest")
    assert ingest1.get("staged_state"), "失败 manifest 应携带 staged 水位（C2）"

    rc2, run_dir2 = _run(cfg, bid, fail_at="")
    assert rc2 == 0

    manifest2 = json_load(os.path.join(run_dir2, "manifest.json"))
    by_name = {s["name"]: s for s in manifest2["stages"]}
    for skipped in ("ingest", "validate", "clean", "compute"):
        assert by_name[skipped].get("resumed") is True, skipped
    assert by_name["output"].get("resumed") is not True, "output 必须重跑"

    state2 = _read_state(state_dir)
    info2 = state2["tables"]["orders"]
    expected_wm = _max_col(env["orders_path"], "order_date")
    assert info2["watermark_value"] == expected_wm, "staged 水位恢复后应恰好提交一次"
    assert info2["cumulative_row_count"] == info2["last_seen_row_count"] > 0
    assert bid in state2.get("merged_batches", [])
    assert os.path.isfile(os.path.join(state_dir, "aggregates", "daily_sales.csv"))


# ----------------------------------------------------------------------
# 场景 8（任务 #74 C4）：台账幂等闸门——已提交批次重放提交阶段不重复累加
# ----------------------------------------------------------------------
def test_batch_ledger_idempotent_skip_merge(inc_env):
    """崩溃在提交点之后（如 metrics 阶段）重放入 _advance_and_merge：
    merged_batches 台账命中 → 跳过合并，水位/聚合/台账全部不变.
    """
    env = inc_env
    cfg = env["cfg"]
    state_dir = env["state_dir"]
    daily_path = os.path.join(state_dir, "aggregates", "daily_sales.csv")

    bid = _new_bid("ledger")
    rc, run_dir = _run(cfg, bid)
    assert rc == 0

    store = StateStore(state_dir)
    state_before = store.load()
    assert store.is_batch_merged(state_before, bid), "成功批次应已登记台账"
    daily_before, _ = csv_read(daily_path)
    wm_before = state_before["tables"]["orders"]["watermark_value"]
    cum_before = state_before["tables"]["orders"]["cumulative_row_count"]

    # 模拟提交阶段重放（老实现无闸门会二次累加聚合与水位）
    ctx = PipelineContext(
        config=cfg,
        run_dir=run_dir,
        batch_id=bid,
        manifest=Manifest(bid, "replay-digest", run_dir),
    )
    ctx.engine_backend = "python"
    ctx.state = store.load()
    _advance_and_merge(ctx, store, logging.getLogger("test-ledger"))

    state_after = store.load()
    assert state_after["tables"]["orders"]["watermark_value"] == wm_before, "水位不得二次推进"
    assert state_after["tables"]["orders"]["cumulative_row_count"] == cum_before
    assert state_after["merged_batches"].count(bid) == 1, "台账不得重复登记"
    daily_after, _ = csv_read(daily_path)
    assert daily_after == daily_before, "聚合不得二次累加"
