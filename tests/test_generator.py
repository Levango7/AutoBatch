"""src/generator.py 单元测试.

覆盖 gen_customers / gen_products / gen_orders 三个核心函数：
- 正常生成：行数正确、字段完整、值域合法
- 缺陷注入：missing/negative_qty/invalid_status/bad_date/orphan_fk/bad_channel/outlier/duplicate
- 随机性：相同 seed 复现一致，不同 seed 产生差异
- main()：端到端生成 meta 与文件

仅依赖 stdlib + src.helpers，不启动 pipeline，跑得快。
"""
from __future__ import annotations

import os
import random
import tempfile
from datetime import datetime

import pytest

from src.generator import (
    CATEGORIES,
    CHANNELS,
    CITIES,
    REGIONS,
    STATUSES,
    TIERS,
    gen_customers,
    gen_orders,
    gen_products,
    main,
)

BASE_DATE = datetime(2026, 8, 15)
ORDER_FIELDS = {
    "order_id", "customer_id", "product_id", "order_date", "created_ts",
    "region", "channel", "quantity", "unit_price", "status",
}
CUSTOMER_FIELDS = {"customer_id", "tier", "city", "join_date"}
PRODUCT_FIELDS = {"product_id", "name", "category", "cost"}


# ----------------------------------------------------------------------
# gen_customers
# ----------------------------------------------------------------------
def test_gen_customers_row_count():
    rng = random.Random(42)
    rows = gen_customers(rng, 100, BASE_DATE)
    assert len(rows) == 100


def test_gen_customers_fields_complete():
    rng = random.Random(42)
    rows = gen_customers(rng, 10, BASE_DATE)
    for r in rows:
        assert set(r.keys()) == CUSTOMER_FIELDS


def test_gen_customers_id_format():
    rng = random.Random(42)
    rows = gen_customers(rng, 5, BASE_DATE)
    for i, r in enumerate(rows, 1):
        assert r["customer_id"] == f"CUS-{i:06d}"


def test_gen_customers_value_domain():
    rng = random.Random(42)
    rows = gen_customers(rng, 200, BASE_DATE)
    for r in rows:
        assert r["tier"] in TIERS
        assert r["city"] in CITIES
        # join_date 应为 YYYY-MM-DD
        datetime.strptime(r["join_date"], "%Y-%m-%d")


def test_gen_customers_empty():
    rng = random.Random(42)
    assert gen_customers(rng, 0, BASE_DATE) == []


# ----------------------------------------------------------------------
# gen_products
# ----------------------------------------------------------------------
def test_gen_products_row_count():
    rng = random.Random(42)
    assert len(gen_products(rng, 50)) == 50


def test_gen_products_fields_complete():
    rng = random.Random(42)
    rows = gen_products(rng, 10, )
    for r in rows:
        assert set(r.keys()) == PRODUCT_FIELDS


def test_gen_products_id_format():
    rng = random.Random(42)
    rows = gen_products(rng, 5)
    for i, r in enumerate(rows, 1):
        assert r["product_id"] == f"PRD-{i:06d}"


def test_gen_products_cost_range():
    rng = random.Random(42)
    rows = gen_products(rng, 200)
    for r in rows:
        # cost = round(uniform(5, 2000), 2)
        assert 5.0 <= r["cost"] <= 2000.0
        # round 到 2 位小数
        assert round(r["cost"], 2) == r["cost"]


def test_gen_products_category_in_name():
    rng = random.Random(42)
    rows = gen_products(rng, 100)
    for r in rows:
        assert r["category"] in CATEGORIES
        assert r["name"].startswith(r["category"] + "-")


# ----------------------------------------------------------------------
# gen_orders: 正常生成
# ----------------------------------------------------------------------
def _make_ref(n_customers=20, n_products=10):
    rng = random.Random(42)
    customers = gen_customers(rng, n_customers, BASE_DATE)
    products = gen_products(rng, n_products)
    return customers, products


def test_gen_orders_row_count_no_duplicates():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 100, customers, products, BASE_DATE, {}, 90)
    assert len(rows) == 100


def test_gen_orders_fields_complete():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 50, customers, products, BASE_DATE, {}, 90)
    for r in rows:
        assert set(r.keys()) == ORDER_FIELDS


def test_gen_orders_id_format():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 30, customers, products, BASE_DATE, {}, 90)
    # 应包含 ORD-00000001..ORD-00000030（无 duplicate 时）
    oids = {r["order_id"] for r in rows}
    expected = {f"ORD-{i:08d}" for i in range(1, 31)}
    assert oids == expected


def test_gen_orders_value_domain_clean():
    """无缺陷时所有字段值域合法."""
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 200, customers, products, BASE_DATE, {}, 90)
    cust_ids = {c["customer_id"] for c in customers}
    prod_ids = {p["product_id"] for p in products}
    for r in rows:
        assert r["region"] in REGIONS
        assert r["channel"] in CHANNELS
        assert r["status"] in STATUSES
        assert r["customer_id"] in cust_ids
        assert r["product_id"] in prod_ids
        assert isinstance(r["quantity"], int) and 1 <= r["quantity"] <= 20
        assert 5.0 <= r["unit_price"] <= 1500.0
        # order_date 应为合法日期
        datetime.strptime(r["order_date"], "%Y-%m-%d")


# ----------------------------------------------------------------------
# gen_orders: 缺陷注入
# ----------------------------------------------------------------------
def test_gen_orders_duplicate_increases_rows():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 100, customers, products, BASE_DATE,
                      {"duplicate": 0.1}, 90)
    # dup_count = int(100 * 0.1) = 10 → 总行数 110
    assert len(rows) == 110


def test_gen_orders_missing_introduces_empty_string():
    """missing=1.0 强制每条记录至少触发一次 missing 缺陷（victim 随机选字段）."""
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 200, customers, products, BASE_DATE,
                      {"missing": 1.0}, 90)
    # 至少有一条记录的 region/channel/customer_id/unit_price/quantity 之一为空
    empty_fields = 0
    for r in rows:
        if r["region"] == "" or r["channel"] == "" or r["customer_id"] == "":
            empty_fields += 1
    assert empty_fields > 0, "missing=1.0 应注入空字段"


def test_gen_orders_negative_qty():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 500, customers, products, BASE_DATE,
                      {"negative_qty": 1.0}, 90)
    neg = [r for r in rows if isinstance(r["quantity"], int) and r["quantity"] < 0]
    assert len(neg) > 0, "negative_qty=1.0 应产生负数 quantity"


def test_gen_orders_invalid_status():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 500, customers, products, BASE_DATE,
                      {"invalid_status": 1.0}, 90)
    bad = [r for r in rows if r["status"] not in STATUSES]
    assert len(bad) > 0, "invalid_status=1.0 应产生非标准 status"


def test_gen_orders_bad_date():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 500, customers, products, BASE_DATE,
                      {"bad_date": 1.0}, 90)
    bad = []
    for r in rows:
        try:
            datetime.strptime(r["order_date"], "%Y-%m-%d")
        except ValueError:
            bad.append(r)
    assert len(bad) > 0, "bad_date=1.0 应产生非法 order_date"


def test_gen_orders_orphan_fk():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 500, customers, products, BASE_DATE,
                      {"orphan_fk": 1.0}, 90)
    orphans = [r for r in rows if r["customer_id"] == "CUS-999999"]
    assert len(orphans) > 0, "orphan_fk=1.0 应产生悬空 customer_id"


def test_gen_orders_bad_channel():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 500, customers, products, BASE_DATE,
                      {"bad_channel": 1.0}, 90)
    bad = [r for r in rows if r["channel"] not in CHANNELS]
    assert len(bad) > 0, "bad_channel=1.0 应产生非标准 channel"


def test_gen_orders_outlier_unit_price():
    rng = random.Random(7)
    customers, products = _make_ref()
    rows = gen_orders(rng, 500, customers, products, BASE_DATE,
                      {"outlier": 1.0}, 90)
    outliers = [r for r in rows
                if isinstance(r["unit_price"], (int, float)) and r["unit_price"] > 1500.0]
    assert len(outliers) > 0, "outlier=1.0 应产生 unit_price > 1500 的异常值"


# ----------------------------------------------------------------------
# gen_orders: 随机性
# ----------------------------------------------------------------------
def test_gen_orders_same_seed_reproducible():
    customers, products = _make_ref()
    rng1 = random.Random(123)
    rng2 = random.Random(123)
    rows1 = gen_orders(rng1, 50, customers, products, BASE_DATE, {}, 90)
    rows2 = gen_orders(rng2, 50, customers, products, BASE_DATE, {}, 90)
    assert rows1 == rows2


def test_gen_orders_different_seed_differs():
    customers, products = _make_ref()
    rng1 = random.Random(123)
    rng2 = random.Random(456)
    rows1 = gen_orders(rng1, 50, customers, products, BASE_DATE, {}, 90)
    rows2 = gen_orders(rng2, 50, customers, products, BASE_DATE, {}, 90)
    assert rows1 != rows2


# ----------------------------------------------------------------------
# main: 端到端
# ----------------------------------------------------------------------
def test_main_writes_files_and_meta(tmp_path):
    cfg = {
        "source": {"name": "test-src"},
        "generator": {
            "rows": 50,
            "seed": 42,
            "output_dir": str(tmp_path / "raw"),
            "customer_count": 20,
            "product_count": 10,
            "date_range_days": 60,
            "defect_rates": {"duplicate": 0.1, "missing": 0.05},
        },
    }
    meta = main(cfg)
    # meta 字段
    assert meta["name"] == "test-src"
    assert meta["seed"] == 42
    assert meta["rows"]["orders"] == 55  # 50 + int(50*0.1) duplicates
    assert meta["rows"]["customers"] == 20
    assert meta["rows"]["products"] == 10
    # 文件实际生成
    assert os.path.isfile(tmp_path / "raw" / "orders.csv")
    assert os.path.isfile(tmp_path / "raw" / "customers.csv")
    assert os.path.isfile(tmp_path / "raw" / "products.csv")
    assert os.path.isfile(tmp_path / "raw" / "_SOURCE.json")


def test_main_minimal_config(tmp_path):
    """最小配置（仅 rows/seed）应使用缺省 customer_count/product_count/date_range_days."""
    cfg = {
        "source": {"name": "d"},
        "generator": {
            "rows": 5,
            "seed": 1,
            "output_dir": str(tmp_path / "raw"),
        },
    }
    meta = main(cfg)
    assert meta["rows"]["orders"] == 5
    # 缺省 customer_count=3000 / product_count=200
    assert meta["rows"]["customers"] == 3000
    assert meta["rows"]["products"] == 200
    assert os.path.isfile(tmp_path / "raw" / "orders.csv")
