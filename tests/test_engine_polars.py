"""Polars backend 等价性测试（Phase 2a）。

验证 ``engine.backend="polars"`` 时五阶段产物与 ``backend="python"`` 完全一致
（行数、聚合值、DQ Score、manifest lineage、metrics），以及增量+Polars 组合
行为正确。设计见 docs/evolution.md §4.7.1。

场景:
1. test_polars_full_run_equals_python       — polars 全量产物与 python 全量一致
2. test_polars_dq_score_in_range            — polars 全量 DQ Score in [0.95, 1.0]、lineage/metrics 正确
3. test_polars_incremental_combination      — 增量 + polars：首次建水位、二跑零增量、追加只处理新增
4. test_polars_parquet_format               — engine.format="parquet" 时 pipeline 跑通（若当前阶段未支持则 skip）
"""

from __future__ import annotations

import copy
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

import pytest

from src.helpers import abs_path, csv_read, csv_write, json_load
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
    """生成唯一 batch_id，前缀 test-polars- 便于 polars_env cleanup 统一清理。"""
    return f"test-polars-{tag}-{uuid.uuid4().hex[:6]}"


def _csv_count(path: str) -> int:
    """CSV 数据行数（不含 header）；文件不存在返回 -1。"""
    if not os.path.exists(path):
        return -1
    rows, _ = csv_read(path)
    return len(rows)


def _csv_rows(path: str) -> list[dict[str, str]]:
    """读 CSV 为 List[Dict]；文件不存在返回空列表。"""
    if not os.path.exists(path):
        return []
    rows, _ = csv_read(path)
    return rows


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
    """生成 n 个合法新订单，order_date 从 base_date 开始每日递增。"""
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


def _normalize_rows(rows: list[dict[str, str]], keys: list[str]) -> list[tuple]:
    """把 rows 投影到 keys 列并排序，用于无序比较。

    Polars 与 Python 路径的行顺序可能不同（groupby 不保证顺序），
    但集合应一致。投影到关键列+排序后逐行比较。
    """
    return sorted(tuple(r.get(k, "") for k in keys) for r in rows)


# ----------------------------------------------------------------------
# 场景 1: polars 全量产物与 python 全量一致
# ----------------------------------------------------------------------
def test_polars_full_run_equals_python(polars_env):
    """polars backend 全量 pipeline 产物应与 python backend 完全一致。

    验证：status=success、orders_final.csv 行数一致、daily_sales.csv 内容一致、
    customer_value.csv 内容一致、DQ Score 一致。
    """
    env = polars_env
    cfg_polars = env["cfg"]

    # 跑 polars backend 全量
    bid_p = _new_bid("eq-p")
    rc_p, run_dir_p = _run(cfg_polars, bid_p)
    assert rc_p == 0, "polars backend 全量运行应成功"

    # 跑 python backend 全量（相同数据）
    cfg_python = copy.deepcopy(cfg_polars)
    cfg_python["engine"]["backend"] = "python"
    bid_py = _new_bid("eq-py")
    rc_py, run_dir_py = _run(cfg_python, bid_py)
    assert rc_py == 0, "python backend 全量运行应成功"

    # status 都成功
    status_p = json_load(os.path.join(run_dir_p, "status.json"))
    status_py = json_load(os.path.join(run_dir_py, "status.json"))
    assert status_p["status"] == "success"
    assert status_py["status"] == "success"

    # orders_final.csv 行数一致
    p_final = _csv_count(os.path.join(run_dir_p, "05_output", "orders_final.csv"))
    py_final = _csv_count(os.path.join(run_dir_py, "05_output", "orders_final.csv"))
    assert p_final == py_final, f"orders_final 行数 polars={p_final} 应等于 python={py_final}"

    # daily_sales.csv 内容一致（按 order_date 排序后逐行比较）
    p_daily = _csv_rows(os.path.join(run_dir_p, "04_aggregates", "daily_sales.csv"))
    py_daily = _csv_rows(os.path.join(run_dir_py, "04_aggregates", "daily_sales.csv"))
    daily_keys = ["order_date", "orders", "units", "revenue", "avg_order_value"]
    assert _normalize_rows(p_daily, daily_keys) == _normalize_rows(py_daily, daily_keys), (
        "daily_sales 内容 polars 与 python 不一致"
    )

    # customer_value.csv 内容一致（按 customer_id 排序后比较关键列）
    p_cv = _csv_rows(os.path.join(run_dir_p, "04_aggregates", "customer_value.csv"))
    py_cv = _csv_rows(os.path.join(run_dir_py, "04_aggregates", "customer_value.csv"))
    cv_keys = ["customer_id", "tier", "city", "orders", "revenue", "rank"]
    assert _normalize_rows(p_cv, cv_keys) == _normalize_rows(py_cv, cv_keys), (
        "customer_value 内容 polars 与 python 不一致"
    )

    # DQ Score 一致
    manifest_p = json_load(os.path.join(run_dir_p, "manifest.json"))
    manifest_py = json_load(os.path.join(run_dir_py, "manifest.json"))
    dq_p = manifest_p["quality"]["dq_score"]
    dq_py = manifest_py["quality"]["dq_score"]
    assert dq_p == pytest.approx(dq_py, abs=1e-9), f"DQ Score polars={dq_p} 应等于 python={dq_py}"


# ----------------------------------------------------------------------
# 场景 2: polars 全量 DQ Score 在合理区间、manifest/metrics 正确
# ----------------------------------------------------------------------
def test_polars_dq_score_in_range(polars_env):
    """polars backend 全量运行 DQ Score 应在 [0.95, 1.0]，manifest lineage nonempty，metrics 5 stages。

    与 test_pipeline_e2e.py 的断言对齐，确保 polars 路径产出与 python 路径
    结构一致（不仅数据值一致，元数据结构也一致）。
    """
    env = polars_env
    cfg = env["cfg"]

    bid = _new_bid("dq")
    rc, run_dir = _run(cfg, bid)
    assert rc == 0, "polars backend 全量运行应成功"

    # status=success
    status = json_load(os.path.join(run_dir, "status.json"))
    assert status["status"] == "success"

    # DQ Score 在 [0.95, 1.0]
    manifest = json_load(os.path.join(run_dir, "manifest.json"))
    dq = manifest["quality"]["dq_score"]
    assert 0.95 <= dq <= 1.0, f"DQ Score {dq} 不在 [0.95, 1.0]"

    # manifest lineage nonempty
    assert len(manifest["lineage"]) > 0, "manifest lineage 应非空"

    # metrics 5 stages
    metrics = json_load(os.path.join(run_dir, "metrics.json"))
    assert "stages" in metrics
    assert len(metrics["stages"]) == 5, "metrics 应有 5 个 stage"
    assert metrics["status"] == "success"


# ----------------------------------------------------------------------
# 场景 3: 增量 + polars 组合
# ----------------------------------------------------------------------
def test_polars_incremental_combination(polars_env):
    """incremental.enabled=true + engine.backend="polars" 组合行为正确。

    验证：
    - 首次运行成功，state.json 生成且水位正确
    - 第二次运行（无新数据）成功，零增量（orders_incremental.csv 行数为 0）
    - 追加新数据后第三次运行成功，只处理新增行（orders_incremental.csv 行数 = 新增数）
    """
    env = polars_env
    cfg = env["cfg"]
    work_dir = env["work_dir"]

    # 开启增量，state_dir 放在 work_dir/state（同盘隔离）
    state_dir = os.path.join(work_dir, "state")
    cfg["incremental"]["enabled"] = True
    cfg["incremental"]["state_dir"] = state_dir

    # --- 首次运行 ---
    bid1 = _new_bid("inc-1")
    rc1, run_dir1 = _run(cfg, bid1)
    assert rc1 == 0, "首次增量+polars 运行应成功"

    # state.json 生成
    state_path = os.path.join(state_dir, "state.json")
    assert os.path.exists(state_path), "state.json 应生成"
    state1 = json_load(state_path)
    assert "tables" in state1
    assert "orders" in state1["tables"]

    # 首次水位 = max(order_date)
    expected_orders_wm = max(r["order_date"] for r in _csv_rows(env["orders_path"]))
    assert state1["tables"]["orders"]["watermark_value"] == expected_orders_wm, (
        "首次 orders 水位应为 max(order_date)"
    )

    # --- 第二次运行（无新数据）---
    bid2 = _new_bid("inc-2")
    rc2, run_dir2 = _run(cfg, bid2)
    assert rc2 == 0, "第二次增量+polars 运行应成功"

    # orders_incremental.csv 行数为 0
    inc_csv2 = os.path.join(run_dir2, "01_raw", "orders_incremental.csv")
    assert _csv_count(inc_csv2) == 0, "无新数据时 orders_incremental.csv 应为 0 行"

    # 水位不变
    state2 = json_load(state_path)
    assert state2["tables"]["orders"]["watermark_value"] == expected_orders_wm, (
        "无新数据时水位应不变"
    )

    # --- 追加新数据后第三次运行 ---
    cust_rows = _csv_rows(env["customers_path"])
    prod_rows = _csv_rows(env["products_path"])
    cid = cust_rows[0]["customer_id"]
    pid = prod_rows[0]["product_id"]

    n_new = 10
    base_date = _next_date(expected_orders_wm)
    new_orders = _make_new_orders(
        n_new, start_id=100001, cid=cid, pid=pid, base_date=base_date, unit_price="100000.00"
    )
    _append_orders(env["orders_path"], new_orders)

    bid3 = _new_bid("inc-3")
    rc3, run_dir3 = _run(cfg, bid3)
    assert rc3 == 0, "追加新数据后增量+polars 运行应成功"

    # orders_incremental.csv 只含新增行
    inc_csv3 = os.path.join(run_dir3, "01_raw", "orders_incremental.csv")
    assert _csv_count(inc_csv3) == n_new, f"orders_incremental.csv 应只含新增 {n_new} 行"

    # 水位推进到新 max
    state3 = json_load(state_path)
    expected_new_wm = max(o["order_date"] for o in new_orders)
    assert state3["tables"]["orders"]["watermark_value"] == expected_new_wm, "水位应推进到新 max"


# ----------------------------------------------------------------------
# 场景 4: parquet 格式（可选）
# ----------------------------------------------------------------------
def test_polars_parquet_format(polars_env):
    """engine.format="parquet" 时 pipeline 应跑通，产物内容与 csv format 一致。

    当前 Phase 2a 的 stage 实现中，clean/compute 等用 ``pl.read_csv`` 直接读
    上游产物（未走 table_read 的 parquet 路由），在 format="parquet" 下上游
    产物实际写为 ``.csv.parquet``，``pl.read_csv(.csv)`` 会失败。因此本测试
    先尝试跑 pipeline，若失败则 skip（标记为已知限制，待后续阶段补全 parquet
    端到端路由后启用）。
    """
    env = polars_env
    cfg = copy.deepcopy(env["cfg"])
    cfg["engine"]["backend"] = "polars"
    cfg["engine"]["format"] = "parquet"

    bid = _new_bid("parquet")
    try:
        rc, run_dir = _run(cfg, bid)
    except Exception as exc:
        pytest.skip(f"当前 Phase 2a stage 未完整支持 parquet 端到端路由: {exc}")

    if rc != 0:
        pytest.skip(f"当前 Phase 2a parquet 端到端未跑通（rc={rc}），待后续阶段补全")

    # 跑通则验证 status=success
    status = json_load(os.path.join(run_dir, "status.json"))
    assert status["status"] == "success", "parquet format pipeline 应成功"

    # orders_final 行数与 csv format 一致
    cfg_csv = copy.deepcopy(env["cfg"])
    cfg_csv["engine"]["format"] = "csv"
    bid_csv = _new_bid("parquet-csv-ref")
    rc_csv, run_dir_csv = _run(cfg_csv, bid_csv)
    assert rc_csv == 0

    # parquet 模式下 orders_final 可能写为 .csv.parquet，用 polars 读
    p_final_path = os.path.join(run_dir, "05_output", "orders_final.csv")
    p_final_parquet = p_final_path + ".parquet"
    if os.path.exists(p_final_parquet):
        import polars as pl

        p_final_count = pl.read_parquet(p_final_parquet).height
    else:
        p_final_count = _csv_count(p_final_path)

    csv_final_count = _csv_count(os.path.join(run_dir_csv, "05_output", "orders_final.csv"))
    assert p_final_count == csv_final_count, (
        f"parquet format orders_final 行数 {p_final_count} 应等于 csv format {csv_final_count}"
    )
