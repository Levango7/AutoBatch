"""端到端冒烟：跑完整流水线，断言 status/DQ Score/manifest/metrics/产物/KPI."""
from __future__ import annotations

import csv
import os

import pytest

from src.helpers import json_load


def _csv_count(path: str) -> int:
    """数 CSV 行数（不含表头）."""
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def _csv_rows(path: str) -> list[dict[str, str]]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ----------------------------------------------------------------------
# 基础断言：status / DQ / manifest / metrics
# ----------------------------------------------------------------------
def test_pipeline_success(small_batch_dir):
    status = json_load(os.path.join(small_batch_dir, "status.json"))
    assert status["status"] == "success"


def test_dq_score_in_range(small_batch_dir):
    manifest = json_load(os.path.join(small_batch_dir, "manifest.json"))
    dq = manifest["quality"]["dq_score"]
    assert 0.95 <= dq <= 1.0, f"DQ Score {dq} 不在 [0.95, 1.0]"


def test_manifest_lineage_nonempty(small_batch_dir):
    manifest = json_load(os.path.join(small_batch_dir, "manifest.json"))
    assert len(manifest["lineage"]) > 0


def test_metrics_json_exists(small_batch_dir):
    metrics = json_load(os.path.join(small_batch_dir, "metrics.json"))
    assert "stages" in metrics
    assert len(metrics["stages"]) == 5
    assert metrics["status"] == "success"


# ----------------------------------------------------------------------
# 加强断言：产物行数
# ----------------------------------------------------------------------
def test_orders_final_has_rows(small_batch_dir):
    """orders_final.csv 应有正向行数（输入 5000 行，清洗后仍应有大量行）."""
    path = os.path.join(small_batch_dir, "05_output", "orders_final.csv")
    assert os.path.isfile(path), "orders_final.csv 应存在"
    n = _csv_count(path)
    assert n > 0, "orders_final.csv 行数应 > 0"
    # 输入 5000 行，清洗 + 去重后至少应保留 50%
    assert n >= 2500, f"orders_final.csv 行数 {n} 应 >= 2500（输入 5000 的 50%）"


def test_daily_sales_has_rows(small_batch_dir):
    """daily_sales.csv 应有正向行数（按日期聚合，date_range_days=90 → 约 91 行）."""
    path = os.path.join(small_batch_dir, "04_aggregates", "daily_sales.csv")
    assert os.path.isfile(path), "daily_sales.csv 应存在"
    n = _csv_count(path)
    assert n > 0, "daily_sales.csv 行数应 > 0"


def test_category_stats_six_categories(small_batch_dir):
    """category_stats.csv 应有 6 个类目（数码/服饰/家居/食品/美妆/图书）."""
    path = os.path.join(small_batch_dir, "04_aggregates", "category_stats.csv")
    rows = _csv_rows(path)
    assert len(rows) == 6, f"category_stats 应有 6 行，实际 {len(rows)}"
    cats = {r["category"] for r in rows}
    assert cats == {"数码", "服饰", "家居", "食品", "美妆", "图书"}


# ----------------------------------------------------------------------
# 加强断言：KPI 字段存在性 + 数值合法
# ----------------------------------------------------------------------
def test_kpi_json_fields(small_batch_dir):
    """kpi.json 应含完整字段：orders/units/total_revenue/avg_order_value/days/currency."""
    path = os.path.join(small_batch_dir, "04_aggregates", "kpi.json")
    assert os.path.isfile(path), "kpi.json 应存在"
    kpi = json_load(path)
    expected_keys = {"orders", "units", "total_revenue",
                     "avg_order_value", "days", "currency"}
    assert set(kpi.keys()) == expected_keys, f"kpi 字段缺失：{set(kpi.keys())}"
    # 数值合法性
    assert kpi["orders"] > 0
    assert kpi["units"] > 0
    assert kpi["total_revenue"] > 0
    assert kpi["avg_order_value"] > 0
    assert kpi["days"] > 0
    assert kpi["currency"] == "CNY"


# ----------------------------------------------------------------------
# 加强断言：聚合值一致性
# ----------------------------------------------------------------------
def test_kpi_total_revenue_matches_daily_sales_sum(small_batch_dir):
    """kpi.total_revenue 应等于 daily_sales.csv 中所有 revenue 之和（浮点容差）."""
    kpi = json_load(os.path.join(small_batch_dir, "04_aggregates", "kpi.json"))
    daily_path = os.path.join(small_batch_dir, "04_aggregates", "daily_sales.csv")
    rows = _csv_rows(daily_path)
    total = sum(float(r["revenue"]) for r in rows)
    assert total == pytest.approx(kpi["total_revenue"], rel=1e-6), \
        f"daily_sales revenue 之和 {total} 应等于 kpi.total_revenue {kpi['total_revenue']}"


def test_kpi_orders_matches_daily_sales_sum(small_batch_dir):
    """kpi.orders 应等于 daily_sales.csv 中所有 orders 之和."""
    kpi = json_load(os.path.join(small_batch_dir, "04_aggregates", "kpi.json"))
    daily_path = os.path.join(small_batch_dir, "04_aggregates", "daily_sales.csv")
    rows = _csv_rows(daily_path)
    total = sum(int(r["orders"]) for r in rows)
    assert total == kpi["orders"], \
        f"daily_sales orders 之和 {total} 应等于 kpi.orders {kpi['orders']}"


def test_category_stats_revenue_share_sums_to_one(small_batch_dir):
    """category_stats.csv 的 revenue_share 列之和应≈1.0.

    revenue_share 每个值四舍五入到 4 位小数（见 src/state.py _recompute_derived），
    6 个类目累加后误差可达 6×0.00005 = 0.0003，故用 abs=1e-3 容差.
    """
    path = os.path.join(small_batch_dir, "04_aggregates", "category_stats.csv")
    rows = _csv_rows(path)
    total = sum(float(r["revenue_share"]) for r in rows)
    assert total == pytest.approx(1.0, abs=1e-3), \
        f"category_stats revenue_share 之和 {total} 应≈1.0"


def test_daily_sales_avg_order_value_consistent(small_batch_dir):
    """daily_sales 每行 avg_order_value 应≈revenue/orders."""
    path = os.path.join(small_batch_dir, "04_aggregates", "daily_sales.csv")
    rows = _csv_rows(path)
    for r in rows:
        orders = int(r["orders"])
        revenue = float(r["revenue"])
        avg = float(r["avg_order_value"])
        expected = revenue / orders if orders else 0.0
        assert avg == pytest.approx(expected, rel=1e-4), \
            f"avg_order_value {avg} 应≈revenue/orders {expected}（date={r['order_date']})"


# ----------------------------------------------------------------------
# 加强断言：manifest artifacts 完整性
# ----------------------------------------------------------------------
def test_manifest_artifacts_nonempty(small_batch_dir):
    """manifest.artifacts 应非空，且每个 artifact 含 path/kind/sha256/batch_id."""
    manifest = json_load(os.path.join(small_batch_dir, "manifest.json"))
    assert len(manifest["artifacts"]) > 0, "artifacts 应非空"
    for _rel, info in manifest["artifacts"].items():
        assert "path" in info
        assert "kind" in info
        assert "sha256" in info
        assert "batch_id" in info
        assert info["batch_id"] == manifest["batch_id"]


def test_manifest_stages_all_success(small_batch_dir):
    """manifest.stages 5 个 stage 全部 success."""
    manifest = json_load(os.path.join(small_batch_dir, "manifest.json"))
    stages = manifest["stages"]
    assert len(stages) == 5
    for s in stages:
        assert s["status"] == "success", f"stage {s['name']} 状态应为 success"
    # stage 顺序应为 ingest/validate/clean/compute/output
    names = [s["name"] for s in stages]
    assert names == ["ingest", "validate", "clean", "compute", "output"]
