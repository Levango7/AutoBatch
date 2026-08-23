"""各 stage 单元测试：断言 rows_in/rows_out、产物文件、lineage 声明。"""

from __future__ import annotations

import os

from src.helpers import StageLog


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
