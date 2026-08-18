"""Phase 3 等价性测试 + S3 集成测试（storage.backend="parquet"）.

验证 ``storage.backend="parquet"`` 时五阶段产物与 ``storage.backend="local_csv"``
完全一致（行数、聚合值、DQ Score），以及增量+Parquet 组合行为正确。
设计见 docs/evolution.md §5.8.1。

场景:
1. test_local_parquet_equivalence    — 本地 Parquet 全量产物与 local_csv 一致
2. test_s3_parquet_equivalence       — S3（MinIO）Parquet 全量产物与 local_csv 一致
3. test_parquet_compression_ratio    — 同一数据 CSV vs Parquet 文件大小对比
4. test_incremental_parquet          — 增量 + Parquet 组合：首次建水位、追加只处理新增
"""
from __future__ import annotations

import copy
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

import pytest

from src.helpers import abs_path, csv_read, csv_write, json_load, table_read
from src.pipeline import run_pipeline


# ----------------------------------------------------------------------
# MinIO 可用性检查（用于 S3 测试 skipif）
# ----------------------------------------------------------------------
def _minio_available() -> bool:
    """检查 MinIO 是否可用（localhost:9000, bucket=autobatch）."""
    try:
        from minio import Minio
        client = Minio("localhost:9000", access_key="minioadmin",
                       secret_key="minioadmin", secure=False)
        if not client.bucket_exists("autobatch"):
            client.make_bucket("autobatch")
        return True
    except Exception:  # noqa: BLE001
        return False


MINIO_AVAILABLE = _minio_available()


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
    """生成唯一 batch_id，前缀 test-parquet- 便于 fixture cleanup 统一清理。"""
    return f"test-parquet-{tag}-{uuid.uuid4().hex[:6]}"


def _new_bid_s3(tag: str) -> str:
    return f"test-s3-{tag}-{uuid.uuid4().hex[:6]}"


def _table_rows(path: str, cfg: dict[str, Any]) -> list[dict[str, str]]:
    """读 table 为 List[Dict]；文件不存在返回空列表.

    用 table_read 路由（兼容 local_csv 和 parquet storage）。
    """
    from src.helpers import _table_exists
    if not _table_exists(path, cfg):
        return []
    result = table_read(path, cfg)
    # python backend 返回 (rows, fields)
    if isinstance(result, tuple):
        return result[0]
    # polars backend 返回 DataFrame
    return result.to_dicts() if result is not None and result.height > 0 else []


def _table_count(path: str, cfg: dict[str, Any]) -> int:
    """table 行数；文件不存在返回 -1。"""
    from src.helpers import _table_exists
    if not _table_exists(path, cfg):
        return -1
    return len(_table_rows(path, cfg))


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


def _make_new_orders(n: int, start_id: int, cid: str, pid: str,
                     base_date: str, unit_price: str = "100000.00") -> list[dict[str, str]]:
    """生成 n 个合法新订单，order_date 从 base_date 开始每日递增。"""
    rows = []
    for i in range(n):
        date_str = _next_date(base_date, i)
        rows.append({
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
        })
    return rows


def _normalize_rows(rows: list[dict[str, str]], keys: list[str]) -> list[tuple]:
    """把 rows 投影到 keys 列并排序，用于无序比较。"""
    return sorted(tuple(str(r.get(k, "")) for k in keys) for r in rows)


def _make_local_csv_env(env) -> dict[str, Any]:
    """从 parquet_env/s3_env 派生 local_csv 配置（相同数据，storage.backend="local_csv"）."""
    cfg = copy.deepcopy(env["cfg"])
    cfg["storage"]["backend"] = "local_csv"
    return cfg


# ----------------------------------------------------------------------
# 场景 1: 本地 Parquet 等价性测试
# ----------------------------------------------------------------------
def test_local_parquet_equivalence(parquet_env):
    """storage.backend="parquet" + 本地路径，五阶段产物应与 local_csv 完全一致.

    验证：status=success、orders_final 行数一致、daily_sales/category_stats/
    region_channel_stats/customer_value 内容一致、DQ Score 一致。
    """
    env = parquet_env
    cfg_parquet = env["cfg"]

    # 跑 parquet storage 全量
    bid_p = _new_bid("eq-p")
    rc_p, run_dir_p = _run(cfg_parquet, bid_p)
    assert rc_p == 0, "parquet storage 全量运行应成功"

    # 跑 local_csv storage 全量（相同数据）
    cfg_csv = _make_local_csv_env(env)
    bid_c = _new_bid("eq-c")
    rc_c, run_dir_c = _run(cfg_csv, bid_c)
    assert rc_c == 0, "local_csv storage 全量运行应成功"

    # status 都成功
    status_p = json_load(os.path.join(run_dir_p, "status.json"))
    status_c = json_load(os.path.join(run_dir_c, "status.json"))
    assert status_p["status"] == "success", "parquet 模式 status 应为 success"
    assert status_c["status"] == "success", "local_csv 模式 status 应为 success"

    # orders_final 行数一致
    p_final = _table_count(
        os.path.join(run_dir_p, "05_output", "orders_final.csv"), cfg_parquet)
    c_final = _table_count(
        os.path.join(run_dir_c, "05_output", "orders_final.csv"), cfg_csv)
    assert p_final == c_final, \
        f"orders_final 行数 parquet={p_final} 应等于 local_csv={c_final}"

    # daily_sales 内容一致
    p_daily = _table_rows(os.path.join(run_dir_p, "04_aggregates", "daily_sales.csv"), cfg_parquet)
    c_daily = _table_rows(os.path.join(run_dir_c, "04_aggregates", "daily_sales.csv"), cfg_csv)
    daily_keys = ["order_date", "orders", "units", "revenue", "avg_order_value"]
    assert _normalize_rows(p_daily, daily_keys) == _normalize_rows(c_daily, daily_keys), \
        "daily_sales 内容 parquet 与 local_csv 不一致"

    # category_stats 内容一致
    p_cat = _table_rows(os.path.join(run_dir_p, "04_aggregates", "category_stats.csv"), cfg_parquet)
    c_cat = _table_rows(os.path.join(run_dir_c, "04_aggregates", "category_stats.csv"), cfg_csv)
    cat_keys = ["category", "orders", "units", "revenue", "revenue_share"]
    assert _normalize_rows(p_cat, cat_keys) == _normalize_rows(c_cat, cat_keys), \
        "category_stats 内容 parquet 与 local_csv 不一致"

    # region_channel_stats 内容一致
    p_rc = _table_rows(os.path.join(run_dir_p, "04_aggregates", "region_channel_stats.csv"), cfg_parquet)
    c_rc = _table_rows(os.path.join(run_dir_c, "04_aggregates", "region_channel_stats.csv"), cfg_csv)
    rc_keys = ["region", "channel", "orders", "revenue"]
    assert _normalize_rows(p_rc, rc_keys) == _normalize_rows(c_rc, rc_keys), \
        "region_channel_stats 内容 parquet 与 local_csv 不一致"

    # customer_value 内容一致
    p_cv = _table_rows(os.path.join(run_dir_p, "04_aggregates", "customer_value.csv"), cfg_parquet)
    c_cv = _table_rows(os.path.join(run_dir_c, "04_aggregates", "customer_value.csv"), cfg_csv)
    cv_keys = ["customer_id", "tier", "city", "orders", "revenue", "rank"]
    assert _normalize_rows(p_cv, cv_keys) == _normalize_rows(c_cv, cv_keys), \
        "customer_value 内容 parquet 与 local_csv 不一致"

    # DQ Score 一致
    manifest_p = json_load(os.path.join(run_dir_p, "manifest.json"))
    manifest_c = json_load(os.path.join(run_dir_c, "manifest.json"))
    dq_p = manifest_p["quality"]["dq_score"]
    dq_c = manifest_c["quality"]["dq_score"]
    assert dq_p == pytest.approx(dq_c, abs=1e-9), \
        f"DQ Score parquet={dq_p} 应等于 local_csv={dq_c}"


# ----------------------------------------------------------------------
# 场景 2: S3 Parquet 等价性测试（MinIO）
# ----------------------------------------------------------------------
@pytest.mark.skipif(not MINIO_AVAILABLE,
                    reason="MinIO 不可用（localhost:9000, bucket=autobatch）")
def test_s3_parquet_equivalence(s3_env):
    """storage.backend="parquet" + S3 路径（MinIO），五阶段产物应与 local_csv 完全一致.

    验证：status=success、orders_final 行数一致、聚合值一致、DQ Score 一致。
    MinIO 不可用时用 skipif 跳过。
    """
    env = s3_env
    cfg_s3 = env["cfg"]

    # 跑 S3 parquet storage 全量
    bid_s = _new_bid_s3("eq-s")
    rc_s, run_dir_s = _run(cfg_s3, bid_s)
    assert rc_s == 0, "S3 parquet storage 全量运行应成功"

    # 跑 local_csv storage 全量（相同数据）
    cfg_csv = _make_local_csv_env(env)
    bid_c = _new_bid("eq-c")  # 用 parquet 前缀便于 cleanup
    rc_c, run_dir_c = _run(cfg_csv, bid_c)
    assert rc_c == 0, "local_csv storage 全量运行应成功"

    # status 都成功
    status_s = json_load(os.path.join(run_dir_s, "status.json"))
    status_c = json_load(os.path.join(run_dir_c, "status.json"))
    assert status_s["status"] == "success", "S3 parquet 模式 status 应为 success"
    assert status_c["status"] == "success", "local_csv 模式 status 应为 success"

    # orders_final 行数一致
    s_final = _table_count(
        os.path.join(run_dir_s, "05_output", "orders_final.csv"), cfg_s3)
    c_final = _table_count(
        os.path.join(run_dir_c, "05_output", "orders_final.csv"), cfg_csv)
    assert s_final == c_final, \
        f"orders_final 行数 S3={s_final} 应等于 local_csv={c_final}"

    # daily_sales 内容一致
    s_daily = _table_rows(os.path.join(run_dir_s, "04_aggregates", "daily_sales.csv"), cfg_s3)
    c_daily = _table_rows(os.path.join(run_dir_c, "04_aggregates", "daily_sales.csv"), cfg_csv)
    daily_keys = ["order_date", "orders", "units", "revenue", "avg_order_value"]
    assert _normalize_rows(s_daily, daily_keys) == _normalize_rows(c_daily, daily_keys), \
        "daily_sales 内容 S3 与 local_csv 不一致"

    # category_stats 内容一致
    s_cat = _table_rows(os.path.join(run_dir_s, "04_aggregates", "category_stats.csv"), cfg_s3)
    c_cat = _table_rows(os.path.join(run_dir_c, "04_aggregates", "category_stats.csv"), cfg_csv)
    cat_keys = ["category", "orders", "units", "revenue", "revenue_share"]
    assert _normalize_rows(s_cat, cat_keys) == _normalize_rows(c_cat, cat_keys), \
        "category_stats 内容 S3 与 local_csv 不一致"

    # region_channel_stats 内容一致
    s_rc = _table_rows(os.path.join(run_dir_s, "04_aggregates", "region_channel_stats.csv"), cfg_s3)
    c_rc = _table_rows(os.path.join(run_dir_c, "04_aggregates", "region_channel_stats.csv"), cfg_csv)
    rc_keys = ["region", "channel", "orders", "revenue"]
    assert _normalize_rows(s_rc, rc_keys) == _normalize_rows(c_rc, rc_keys), \
        "region_channel_stats 内容 S3 与 local_csv 不一致"

    # customer_value 内容一致
    s_cv = _table_rows(os.path.join(run_dir_s, "04_aggregates", "customer_value.csv"), cfg_s3)
    c_cv = _table_rows(os.path.join(run_dir_c, "04_aggregates", "customer_value.csv"), cfg_csv)
    cv_keys = ["customer_id", "tier", "city", "orders", "revenue", "rank"]
    assert _normalize_rows(s_cv, cv_keys) == _normalize_rows(c_cv, cv_keys), \
        "customer_value 内容 S3 与 local_csv 不一致"

    # DQ Score 一致
    manifest_s = json_load(os.path.join(run_dir_s, "manifest.json"))
    manifest_c = json_load(os.path.join(run_dir_c, "manifest.json"))
    dq_s = manifest_s["quality"]["dq_score"]
    dq_c = manifest_c["quality"]["dq_score"]
    assert dq_s == pytest.approx(dq_c, abs=1e-9), \
        f"DQ Score S3={dq_s} 应等于 local_csv={dq_c}"


# ----------------------------------------------------------------------
# 场景 3: 压缩比基准测试
# ----------------------------------------------------------------------
def test_parquet_compression_ratio(parquet_env):
    """同一批数据，CSV vs Parquet（zstd 压缩）文件大小对比.

    断言 parquet_size < csv_size * 0.8（至少省 20%）。
    通常 zstd 压缩比 3:1 以上，这里用宽松阈值 0.8 确保稳定。
    """
    env = parquet_env
    cfg = env["cfg"]

    # 跑 parquet 全量，得到 orders_clean 的 parquet 文件
    bid_p = _new_bid("comp-p")
    rc_p, run_dir_p = _run(cfg, bid_p)
    assert rc_p == 0, "parquet 全量运行应成功"

    # 跑 local_csv 全量，得到 orders_clean 的 csv 文件
    cfg_csv = _make_local_csv_env(env)
    bid_c = _new_bid("comp-c")
    rc_c, run_dir_c = _run(cfg_csv, bid_c)
    assert rc_c == 0, "local_csv 全量运行应成功"

    # orders_clean 文件大小对比
    csv_path = os.path.join(run_dir_c, "03_clean", "orders_clean.csv")
    parquet_path = os.path.join(run_dir_p, "03_clean", "orders_clean.csv.parquet")

    assert os.path.exists(csv_path), f"CSV orders_clean 应存在: {csv_path}"
    assert os.path.exists(parquet_path), f"Parquet orders_clean 应存在: {parquet_path}"

    csv_size = os.path.getsize(csv_path)
    parquet_size = os.path.getsize(parquet_path)

    assert parquet_size < csv_size * 0.8, \
        f"Parquet 文件大小 {parquet_size} 应小于 CSV {csv_size} 的 80%（{int(csv_size * 0.8)}）"

    # 打印压缩比（信息性，不断言）
    ratio = csv_size / parquet_size if parquet_size > 0 else 0
    print(f"\n压缩比 CSV:Parquet = {csv_size}:{parquet_size} = {ratio:.2f}:1")


# ----------------------------------------------------------------------
# 场景 4: 增量 + Parquet 组合测试
# ----------------------------------------------------------------------
def test_incremental_parquet(parquet_env):
    """incremental.enabled=true + storage.backend="parquet" 组合行为正确.

    验证：
    - 首次运行成功，state.json 生成且水位正确
    - 追加新数据后二次运行成功，只处理新增行（orders_incremental 行数 = 新增数）
    - 聚合 merge 正确（state/aggregates/daily_sales.csv 累加正确）
    - state.json 水位推进正确
    """
    env = parquet_env
    cfg = env["cfg"]
    work_dir = env["work_dir"]

    # 开启增量，state_dir 放在 work_dir/state（同盘隔离）
    state_dir = os.path.join(work_dir, "state")
    cfg["incremental"]["enabled"] = True
    cfg["incremental"]["state_dir"] = state_dir

    # --- 首次运行（全量建立水位）---
    bid1 = _new_bid("inc-1")
    rc1, run_dir1 = _run(cfg, bid1)
    assert rc1 == 0, "首次增量+parquet 运行应成功"

    # state.json 生成
    state_path = os.path.join(state_dir, "state.json")
    assert os.path.exists(state_path), "state.json 应生成"
    state1 = json_load(state_path)
    assert "tables" in state1
    assert "orders" in state1["tables"]

    # 首次水位 = max(order_date)
    expected_orders_wm = max(r["order_date"] for r in _csv_rows(env["orders_path"]))
    assert state1["tables"]["orders"]["watermark_value"] == expected_orders_wm, \
        "首次 orders 水位应为 max(order_date)"

    # 首次运行的 daily_sales 聚合（用于后续 merge 对比）
    daily1 = _table_rows(os.path.join(run_dir1, "04_aggregates", "daily_sales.csv"), cfg)
    daily1_total_orders = sum(int(r.get("orders", 0)) for r in daily1)

    # --- 追加新数据后二次运行（增量）---
    cust_rows = _csv_rows(env["customers_path"])
    prod_rows = _csv_rows(env["products_path"])
    cid = cust_rows[0]["customer_id"]
    pid = prod_rows[0]["product_id"]

    n_new = 10
    base_date = _next_date(expected_orders_wm)
    new_orders = _make_new_orders(n_new, start_id=100001, cid=cid, pid=pid,
                                  base_date=base_date, unit_price="100000.00")
    _append_orders(env["orders_path"], new_orders)

    bid2 = _new_bid("inc-2")
    rc2, run_dir2 = _run(cfg, bid2)
    assert rc2 == 0, "追加新数据后增量+parquet 运行应成功"

    # orders_incremental 只含新增行
    inc_count = _table_count(
        os.path.join(run_dir2, "01_raw", "orders_incremental.csv"), cfg)
    assert inc_count == n_new, \
        f"orders_incremental 行数 {inc_count} 应等于新增 {n_new}"

    # 水位推进到新 max
    state2 = json_load(state_path)
    expected_new_wm = max(o["order_date"] for o in new_orders)
    assert state2["tables"]["orders"]["watermark_value"] == expected_new_wm, \
        "水位应推进到新 max"

    # 聚合 merge 正确：state/aggregates/daily_sales.csv 应包含首次 + 新增的聚合
    # state/aggregates 是 StateStore 管理的 CSV（非 parquet），用 csv_read
    state_daily_path = os.path.join(state_dir, "aggregates", "daily_sales.csv")
    state_daily = _csv_rows(state_daily_path)
    state_daily_total_orders = sum(int(r.get("orders", 0)) for r in state_daily)

    # 首次全量行数 + 新增行数 = state/aggregates 累计 orders 数
    expected_total = daily1_total_orders + n_new
    assert state_daily_total_orders == expected_total, \
        f"state/aggregates/daily_sales 累计 orders {state_daily_total_orders} 应等于 首次{daily1_total_orders} + 新增{n_new}"
