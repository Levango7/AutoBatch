"""各 stage 单元测试：断言 rows_in/rows_out、产物文件、lineage 声明。

另含 clean stage 三引擎统一语义回归（任务76）：discount 扣减（空/非法值按 0）、
空 region 填充（polars null 与 python 空串路径一致）、keep-first 去重。"""

from __future__ import annotations

import os

import pytest

from src.helpers import StageLog, csv_read, csv_write


def test_ingest(base_ctx):
    from src.stages import ingest

    with StageLog(os.path.join(base_ctx.run_dir, "logs", "ingest.jsonl")) as log:
        summary = ingest.run(base_ctx, log)
    assert summary["rows_out"] == 7
    assert os.path.exists(os.path.join(base_ctx.run_dir, "01_raw", "orders.csv"))
    assert os.path.exists(os.path.join(base_ctx.run_dir, "01_raw", "customers.csv"))
    assert os.path.exists(os.path.join(base_ctx.run_dir, "01_raw", "products.csv"))


def test_validate(ingested_ctx):
    from src.stages import validate

    with StageLog(os.path.join(ingested_ctx.run_dir, "logs", "validate.jsonl")) as log:
        summary = validate.run(ingested_ctx, log)
    assert summary["rows_in"] == 7
    assert summary["rows_out"] == 7
    assert os.path.exists(os.path.join(ingested_ctx.run_dir, "02_valid", "valid_orders.csv"))
    assert "02_valid/valid_orders.csv" in summary.get("lineage", {})


def test_clean(validated_ctx):
    from src.stages import clean

    with StageLog(os.path.join(validated_ctx.run_dir, "logs", "clean.jsonl")) as log:
        summary = clean.run(validated_ctx, log)
    assert summary["rows_in"] == 3
    assert os.path.exists(os.path.join(validated_ctx.run_dir, "03_clean", "orders_clean.csv"))
    assert "03_clean/orders_clean.csv" in summary.get("lineage", {})


def test_compute(cleaned_ctx):
    from src.stages import compute

    with StageLog(os.path.join(cleaned_ctx.run_dir, "logs", "compute.jsonl")) as log:
        summary = compute.run(cleaned_ctx, log)
    assert summary["rows_in"] == 3
    assert os.path.exists(os.path.join(cleaned_ctx.run_dir, "04_aggregates", "daily_sales.csv"))
    assert os.path.exists(os.path.join(cleaned_ctx.run_dir, "04_aggregates", "kpi.json"))
    assert "04_aggregates/daily_sales.csv" in summary.get("lineage", {})


def test_output(computed_ctx):
    from src.stages import output

    with StageLog(os.path.join(computed_ctx.run_dir, "logs", "output.jsonl")) as log:
        summary = output.run(computed_ctx, log)
    assert summary["rows_in"] == 3
    assert summary["rows_out"] == 3
    assert os.path.exists(os.path.join(computed_ctx.run_dir, "05_output", "orders_final.csv"))
    assert os.path.exists(os.path.join(computed_ctx.run_dir, "05_output", "dashboard_data.json"))
    assert "05_output/orders_final.csv" in summary.get("lineage", {})


# ----------------------------------------------------------------------
# clean stage 三引擎统一语义回归（任务76）：
# discount 扣减 / 空 region 填充 / keep-first 去重——python 与 polars 路径
# 产物逐字段一致。绕过 ingest/validate，直接向 02_valid/ 写带 discount 列的
# 合法数据，隔离 clean 自身行为。
# ----------------------------------------------------------------------

_DISCOUNT_FIELDS = [
    "order_id",
    "customer_id",
    "order_date",
    "region",
    "channel",
    "quantity",
    "unit_price",
    "status",
    "discount",
]


def _discount_order_rows() -> list[dict[str, str]]:
    """构造覆盖 discount 三种取值的 5 行订单（含 1 组重复 order_id）。"""
    base = {
        "customer_id": "CUS-000001",
        "order_date": "2026-01-15",
        "region": "",  # 空 region → fill_missing 填 unknown
        "channel": "web",
        "quantity": "5",
        "unit_price": "100.00",
        "status": "completed",
    }
    return [
        dict(base, order_id="ORD-90000001", discount="0.10"),  # 5*100*(1-0.10)=450.0
        dict(base, order_id="ORD-90000002", discount=""),  # 空 → 0 折 → 500.0
        dict(base, order_id="ORD-90000003", discount="abc"),  # 非法 → 0 折 → 500.0
        dict(base, order_id="ORD-90000004", discount=""),  # 重复组首行（保留）
        dict(base, order_id="ORD-90000004", quantity="9", discount=""),  # 重复行（丢弃）
    ]


def _write_valid_orders(ctx) -> None:
    """直接向 02_valid/valid_orders.csv 写数据（不经 ingest/validate）。"""
    path = os.path.join(ctx.run_dir, "02_valid", "valid_orders.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    csv_write(path, _DISCOUNT_FIELDS, _discount_order_rows())


def _run_clean_and_read(ctx) -> tuple[dict, list[dict[str, str]]]:
    from src.stages import clean

    with StageLog(os.path.join(ctx.run_dir, "logs", "clean.jsonl")) as log:
        summary = clean.run(ctx, log)
    rows, _ = csv_read(os.path.join(ctx.run_dir, "03_clean", "orders_clean.csv"))
    return summary, rows


def _assert_discount_clean_semantics(summary: dict, rows: list[dict[str, str]]) -> None:
    """python/polars 共用的 clean 产物断言。"""
    assert summary["rows_in"] == 5
    by_id = {r["order_id"]: r for r in rows}
    assert set(by_id) == {
        "ORD-90000001",
        "ORD-90000002",
        "ORD-90000003",
        "ORD-90000004",
    }, "keep-first 去重后应剩 4 个订单"
    # discount 扣减：合法折扣生效，空/非法折扣按 0（与 python as_float or 0.0 一致）
    assert by_id["ORD-90000001"]["total_amount"] == "450.0"
    assert by_id["ORD-90000002"]["total_amount"] == "500.0"
    assert by_id["ORD-90000003"]["total_amount"] == "500.0"
    # keep-first：重复 order_id 保留首行（quantity=5，而非 9）
    assert by_id["ORD-90000004"]["quantity"] == "5"
    assert by_id["ORD-90000004"]["total_amount"] == "500.0"
    # 空 region 填充 unknown（python 空串路径与 polars null 路径一致）
    assert all(r["region"] == "unknown" for r in rows)
    assert all(r["channel"] == "web" for r in rows)
    assert all(r["is_anomaly"] == "0" for r in rows)


def test_clean_discount_semantics_python(base_ctx):
    """python 路径：discount 扣减 / 空值填充 / keep-first 去重。"""
    _write_valid_orders(base_ctx)
    summary, rows = _run_clean_and_read(base_ctx)
    _assert_discount_clean_semantics(summary, rows)


def test_clean_discount_semantics_polars(base_ctx):
    """polars 路径与 python 路径逐字段等价。

    覆盖任务76 三处分歧修复：
    1. discount 扣减（旧 polars 公式无 (1 - discount) 因子，450 会被算成 500）
    2. fill_missing 补 is_null 条件（polars read_csv 把空字段读为 null，
       旧实现只替换空串 → 空 region 残留不填）
    3. discount 非法值 strict=False cast → null → fill_null(0)（对齐 as_float）
    """
    pytest.importorskip("polars")
    _write_valid_orders(base_ctx)
    base_ctx.config["engine"]["backend"] = "polars"
    base_ctx.engine_backend = "polars"
    summary, rows = _run_clean_and_read(base_ctx)
    _assert_discount_clean_semantics(summary, rows)
