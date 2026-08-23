"""RuleEngine 单元测试：8 类规则正例/反例 + referential 性能回归。"""

from __future__ import annotations

import time

from src.quality import RuleEngine


def test_completeness_pass(orders_rules, ref_data, good_order):
    rows = [good_order()]
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert len(good) == 1 and len(bad) == 0


def test_completeness_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["order_id"] = ""
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(good) == 0 and len(bad) == 1
    assert "missing_required:order_id" in bad[0]["_reasons"]


def test_uniqueness_pass(orders_rules, ref_data, good_order):
    rows = [good_order("ORD-00000001"), good_order("ORD-00000002")]
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert len(good) == 2 and len(bad) == 0


def test_uniqueness_fail(orders_rules, ref_data, good_order):
    rows = [good_order("ORD-00000001"), good_order("ORD-00000001")]
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert len(bad) == 1
    assert "duplicate_key:order_id" in bad[0]["_reasons"]


def test_range_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_range_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["quantity"] = "0"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "range_violation:quantity" in bad[0]["_reasons"]


def test_allowed_values_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_allowed_values_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["status"] = "shipped"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "invalid_value:status" in bad[0]["_reasons"]


def test_format_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_format_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["order_id"] = "ORD-XXX"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "format_violation:order_id" in bad[0]["_reasons"]


def test_date_valid_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_date_valid_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["order_date"] = "2019-06-30"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "invalid_date:order_date" in bad[0]["_reasons"]


def test_referential_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_referential_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["customer_id"] = "CUS-999999"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "orphan_reference:customer_id" in bad[0]["_reasons"]


def test_outlier_pass(orders_rules, ref_data, good_order):
    rows = []
    for i in range(100):
        r = good_order(f"ORD-{i + 1:08d}")
        r["total_amount"] = "500.00"
        rows.append(r)
    _, _, _, outlier_indices = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert len(outlier_indices) == 0


def test_outlier_fail(orders_rules, ref_data, good_order):
    rows = []
    for i in range(100):
        r = good_order(f"ORD-{i + 1:08d}")
        r["total_amount"] = "500.00"
        rows.append(r)
    outlier_row = good_order("ORD-00000101")
    outlier_row["total_amount"] = "999999.00"
    rows.append(outlier_row)
    _, _, _, outlier_indices = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert 100 in outlier_indices


def test_referential_performance(orders_rules):
    customers = [
        {"customer_id": f"CUS-{i:06d}", "tier": "silver", "city": "上海", "join_date": "2022-01-01"}
        for i in range(1, 1001)
    ]
    products = [
        {"product_id": f"PRD-{i:06d}", "name": "p", "category": "数码", "cost": "10"}
        for i in range(1, 201)
    ]
    ref = {"customers": customers, "products": products}
    rows = []
    for i in range(5000):
        rows.append(
            {
                "order_id": f"ORD-{i + 1:08d}",
                "customer_id": f"CUS-{(i % 1000) + 1:06d}",
                "product_id": f"PRD-{(i % 200) + 1:06d}",
                "order_date": "2026-01-15",
                "created_ts": "2026-01-15T10:00:00",
                "region": "华东",
                "channel": "web",
                "quantity": "5",
                "unit_price": "100.00",
                "status": "completed",
            }
        )
    engine = RuleEngine("orders", orders_rules, ref)
    start = time.monotonic()
    engine.check(rows)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0, f"referential check should be < 1s, got {elapsed:.2f}s"
