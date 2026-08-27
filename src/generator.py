"""Demo data generator: e-commerce orders with injected defects."""

from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import datetime, timedelta
from typing import Any

from .helpers import abs_path, csv_write, json_load, json_save, local_ts_str

REGIONS = ["华东", "华北", "华南", "西南", "西北", "东北", "华中"]
CHANNELS = ["web", "app", "store"]
STATUSES = ["completed", "pending", "cancelled", "refunded"]
TIERS = ["bronze", "silver", "gold", "platinum"]
CATEGORIES = ["数码", "服饰", "家居", "食品", "美妆", "图书"]
CITIES = ["上海", "北京", "广州", "深圳", "杭州", "成都", "武汉", "西安", "南京", "重庆"]


def gen_customers(rng: random.Random, n: int, base_date: datetime) -> list[dict[str, str]]:
    rows = []
    for i in range(1, n + 1):
        rows.append(
            {
                "customer_id": f"CUS-{i:06d}",
                "tier": rng.choices(TIERS, weights=[50, 30, 15, 5])[0],
                "city": rng.choice(CITIES),
                "join_date": (base_date - timedelta(days=rng.randint(0, 1800))).strftime(
                    "%Y-%m-%d"
                ),
            }
        )
    return rows


def gen_products(rng: random.Random, n: int) -> list[dict[str, Any]]:
    rows = []
    for i in range(1, n + 1):
        cat = rng.choice(CATEGORIES)
        rows.append(
            {
                "product_id": f"PRD-{i:06d}",
                "name": f"{cat}-商品{i:03d}",
                "category": cat,
                "cost": round(rng.uniform(5, 2000), 2),
            }
        )
    return rows


def gen_orders(
    rng: random.Random,
    n: int,
    customers: list[dict[str, str]],
    products: list[dict[str, Any]],
    base_date: datetime,
    defects: dict[str, float],
    date_days: int,
) -> list[dict[str, Any]]:
    cust_ids = [c["customer_id"] for c in customers]
    prod_ids = [p["product_id"] for p in products]
    rows = []
    for i in range(1, n + 1):
        oid = f"ORD-{i:08d}"
        customer_id = rng.choice(cust_ids)
        product_id = rng.choice(prod_ids)
        days_ago = rng.randint(0, date_days)
        order_date = (base_date - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        created_ts = (base_date - timedelta(days=days_ago, minutes=rng.randint(0, 1200))).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        region = rng.choice(REGIONS)
        channel = rng.choice(CHANNELS)
        quantity: float | str = rng.randint(1, 20)
        unit_price: float | str = round(rng.uniform(5, 1500), 2)
        status = rng.choices(STATUSES, weights=[70, 15, 10, 5])[0]

        if rng.random() < defects.get("missing", 0):
            victim = rng.choice(["region", "channel", "customer_id", "unit_price", "quantity"])
            if victim == "region":
                region = ""
            elif victim == "channel":
                channel = ""
            elif victim == "customer_id":
                customer_id = ""
            elif victim == "unit_price":
                unit_price = ""
            else:
                quantity = ""
        if rng.random() < defects.get("negative_qty", 0):
            quantity = -rng.randint(1, 9)
        if rng.random() < defects.get("invalid_status", 0):
            status = rng.choice(["shipped", "processing", "unknown", ""])
        if rng.random() < defects.get("bad_date", 0):
            order_date = rng.choice(["2027-13-01", "2027-01-01", "2019-06-30", "not-a-date"])
        if rng.random() < defects.get("orphan_fk", 0):
            customer_id = "CUS-999999"
        if rng.random() < defects.get("bad_channel", 0):
            channel = rng.choice(["wechat", "phone", "tv"])
        if rng.random() < defects.get("outlier", 0):
            unit_price = round(rng.uniform(50000, 200000), 2)

        rows.append(
            {
                "order_id": oid,
                "customer_id": customer_id,
                "product_id": product_id,
                "order_date": order_date,
                "created_ts": created_ts,
                "region": region,
                "channel": channel,
                "quantity": quantity,
                "unit_price": unit_price,
                "status": status,
            }
        )

    dup_count = int(n * defects.get("duplicate", 0))
    for _ in range(dup_count):
        rows.append(dict(rng.choice(rows)))
    rng.shuffle(rows)
    return rows


def main(cfg: dict[str, Any]) -> dict[str, Any]:
    gen = cfg.get("generator", {})
    n = int(gen.get("rows", 20000))
    seed = int(gen.get("seed", 42))
    rng = random.Random(seed)
    out_dir = abs_path(gen.get("output_dir", "data/raw"))
    os.makedirs(out_dir, exist_ok=True)
    base_date = datetime(2026, 8, 15)
    defects = gen.get("defect_rates", {})

    customers = gen_customers(rng, int(gen.get("customer_count", 3000)), base_date)
    products = gen_products(rng, int(gen.get("product_count", 200)))
    orders = gen_orders(
        rng, n, customers, products, base_date, defects, int(gen.get("date_range_days", 90))
    )

    # rows=0 / 空参考表时 orders[0] 会 IndexError；用固定列序兜底。
    # 兜底列序必须与 gen_orders / gen_products 的实际字段一致（2026-08 审查 B9：
    # 旧兜底 orders 8 列 vs 实际 10 列、products "price" vs 实际 "cost"，空表
    # 边界下会产出 schema 错位的 CSV）。
    order_fields = (
        [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "created_ts",
            "region",
            "channel",
            "quantity",
            "unit_price",
            "status",
        ]
        if not orders
        else list(orders[0].keys())
    )
    csv_write(os.path.join(out_dir, "orders.csv"), order_fields, orders)
    csv_write(
        os.path.join(out_dir, "customers.csv"),
        list(customers[0].keys()) if customers else ["customer_id", "tier", "city", "join_date"],
        customers,
    )
    csv_write(
        os.path.join(out_dir, "products.csv"),
        list(products[0].keys()) if products else ["product_id", "name", "category", "cost"],
        products,
    )

    meta = {
        "name": cfg.get("source", {}).get("name", "ecommerce-demo"),
        "generated_at": local_ts_str(),
        "seed": seed,
        "rows": {"orders": len(orders), "customers": len(customers), "products": len(products)},
        "defect_rates": defects,
        "note": "orders 含注入缺陷：缺失/重复/负值/非法状态/坏日期/悬空外键/异常值；customers/products 为参考表。",
    }
    json_save(os.path.join(out_dir, "_SOURCE.json"), meta)
    return meta


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate demo data")
    parser.add_argument("--config", default="config/pipeline.json")
    args = parser.parse_args(sys.argv[1:])
    result = main(json_load(abs_path(args.config)))
    print("generated:", result["rows"])
