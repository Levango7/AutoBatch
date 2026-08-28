"""Stage 4: business aggregations over clean orders and reference tables.

Full-load mode (default): read ``03_clean/orders_clean.csv`` (full clean orders),
compute four aggregations from scratch, write to ``04_aggregates/``.

Incremental mode (``ctx.incremental_enabled``): ``03_clean/orders_clean.csv``
contains only this batch's delta orders. compute produces this batch's
**incremental buckets** to ``04_aggregates/``; the pipeline's
``_advance_and_merge`` then merges them into ``state/aggregates/`` via
``StateStore.merge_aggregate`` (accumulate numeric cols by key, recompute
derived cols). See docs/evolution.md §3.3.4 / §3.3.5.

Design notes
------------
- daily_sales / category_stats / region_channel_stats: the existing full-load
  functions already bucket their input rows, so feeding them the delta orders
  yields the delta buckets directly — no history read needed.
- customer_value / customer_tier: in incremental mode the ``customers`` table
  only carries this batch's new customers, so historical customers' tier/city
  must be recovered from ``state/aggregates/customer_value.csv``. The
  ``customers`` column of customer_tier is a distinct count, so we emit only
  genuinely new customers (those not present in history) and let the pipeline
  accumulate them.

Phase 2a Polars 分支（``ctx.engine_backend == "polars"``）：
四个聚合用 ``group_by().agg()`` 列式实现，customer_value Top N 用
``sort().head()``。读 CSV 时 ``infer_schema_length=0`` 保留所有列为 Utf8，
聚合时 cast 数值列，保证写出 CSV 与 Python 路径逐字段一致。
向后兼容：``engine.backend="python"`` 时走原 Python 循环路径，行为不变。
参见 docs/evolution.md §4.3.1.2 / §4.4.1.2。

Phase 2b Spark 分支（``ctx.engine_backend == "spark"``）：
四个聚合用 ``groupBy().agg()`` 分布式实现，customer_value Top N 用窗口函数
``F.row_number().over(Window.orderBy(F.desc("revenue")))``。读入用
``table_read``，写出用 ``table_write``，``ctx.aggregates`` 用 ``df.collect()``
+ ``row.asDict()`` 收集到 driver 为 List[Dict]（小数据量安全）。增量模式下 customer_value/tier 用
``_customer_value_incremental_spark`` 输出未按 top_n 截断的 delta 客户 buckets
（tiers 仅计 history 中不存在的真新客户），与 Python/Polars 增量语义对齐。
向后兼容：``engine.backend="python"/"polars"`` 时行为不变。
参见 docs/evolution.md §4.3.2.2 / §4.4.2.1。
"""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Any

from ..helpers import (
    PipelineContext,
    _table_exists,
    as_float,
    as_int,
    json_save,
    load_csv,
    table_read,
    table_write,
)
from ._dispatch import dispatch_by_engine


def _bucket(rows):
    buckets: defaultdict[str, dict[str, Any]] = defaultdict(
        lambda: {"orders": 0, "units": 0, "revenue": 0.0}
    )
    for r in rows:
        b = buckets[r["order_date"]]
        b["orders"] += 1
        b["units"] += as_int(r.get("quantity")) or 0
        b["revenue"] += as_float(r.get("total_amount")) or 0.0
    return buckets


def daily_sales(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    buckets = _bucket(rows)

    out = []
    for d in sorted(buckets):
        b = buckets[d]
        out.append(
            {
                "order_date": d,
                "orders": b["orders"],
                "units": b["units"],
                "revenue": round(b["revenue"], 2),
                "avg_order_value": round(b["revenue"] / b["orders"], 2),
            }
        )
    return out


def category_stats(
    orders: list[dict[str, str]], products: list[dict[str, str]]
) -> list[dict[str, Any]]:
    pcat = {p["product_id"]: p.get("category", "未知") for p in products}
    buckets: defaultdict[str, dict[str, Any]] = defaultdict(
        lambda: {"orders": 0, "units": 0, "revenue": 0.0}
    )
    for r in orders:
        cat = pcat.get(r.get("product_id", ""), "未知")
        b = buckets[cat]
        b["orders"] += 1
        b["units"] += as_int(r.get("quantity")) or 0
        b["revenue"] += as_float(r.get("total_amount")) or 0.0
    total = sum(b["revenue"] for b in buckets.values()) or 1.0
    out = []
    for cat in sorted(buckets, key=lambda c: -buckets[c]["revenue"]):
        b = buckets[cat]
        out.append(
            {
                "category": cat,
                "orders": b["orders"],
                "units": b["units"],
                "revenue": round(b["revenue"], 2),
                "revenue_share": round(b["revenue"] / total, 4),
            }
        )
    return out


def region_channel_stats(orders: list[dict[str, str]]) -> list[dict[str, Any]]:
    buckets: defaultdict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {"orders": 0, "revenue": 0.0}
    )
    for r in orders:
        key = (r.get("region", "unknown"), r.get("channel", "unknown"))
        b = buckets[key]
        b["orders"] += 1
        b["revenue"] += as_float(r.get("total_amount")) or 0.0
    out = []
    for region, channel in sorted(buckets):
        b = buckets[(region, channel)]
        out.append(
            {
                "region": region,
                "channel": channel,
                "orders": b["orders"],
                "revenue": round(b["revenue"], 2),
            }
        )
    return out


def customer_value(
    orders: list[dict[str, str]], customers: list[dict[str, str]], top_n: int
) -> dict[str, Any]:
    cmeta = {c["customer_id"]: c for c in customers}
    buckets: defaultdict[str, dict[str, Any]] = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    for r in orders:
        cid = r.get("customer_id", "")
        b = buckets[cid]
        b["orders"] += 1
        b["revenue"] += as_float(r.get("total_amount")) or 0.0
    ranked = sorted(buckets.items(), key=lambda kv: -kv[1]["revenue"])
    top: list[dict[str, Any]] = []
    for cid, b in ranked[:top_n]:
        c = cmeta.get(cid, {})
        top.append(
            {
                "customer_id": cid,
                "tier": c.get("tier", ""),
                "city": c.get("city", ""),
                "orders": b["orders"],
                "revenue": round(b["revenue"], 2),
                "rank": len(top) + 1,
            }
        )
    tier_agg: defaultdict[str, dict[str, Any]] = defaultdict(
        lambda: {"customers": 0, "revenue": 0.0}
    )
    for cid, b in buckets.items():
        tier = cmeta.get(cid, {}).get("tier", "unknown")
        t = tier_agg[tier]
        t["customers"] += 1
        t["revenue"] += b["revenue"]
    tiers = [
        {"tier": t, "customers": v["customers"], "revenue": round(v["revenue"], 2)}
        for t, v in sorted(tier_agg.items())
    ]
    return {"top": top, "tiers": tiers}


def _load_clean(ctx: PipelineContext, name: str) -> list[dict[str, str]]:
    cfg = ctx.config
    path = os.path.join(ctx.run_dir, "03_clean", name + "_clean.csv")
    if not _table_exists(path, cfg):
        return []
    rows, _ = table_read(path, cfg)
    return rows


def _history_customer_meta(ctx: PipelineContext) -> dict[str, dict[str, str]]:
    """Read ``state/aggregates/customer_value.csv`` → {cid: {tier, city}}.

    In incremental mode the ``customers`` table only carries this batch's new
    customers, so historical customers' tier/city must be recovered from the
    persisted aggregate. Returns ``{}`` when state is unavailable (first
    incremental run).

    Phase 4: storage.backend="iceberg" 时历史聚合仍是 CSV（state/aggregates/ 下），
    用 load_csv 读；Iceberg 表的 merge 由 pipeline._advance_and_merge 的
    commit_snapshot_id 处理 snapshot id 持久化.
    """
    if not ctx.state_path:
        return {}
    agg_path = os.path.join(os.path.dirname(ctx.state_path), "aggregates", "customer_value.csv")
    if not os.path.exists(agg_path):
        return {}
    rows, _ = load_csv(agg_path)
    return {
        r["customer_id"]: {"tier": r.get("tier", ""), "city": r.get("city", "")}
        for r in rows
        if r.get("customer_id")
    }


def _customer_value_incremental(
    orders: list[dict[str, str]],
    customers: list[dict[str, str]],
    history_meta: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build this batch's incremental customer_value / customer_tier buckets.

    Parameters
    ----------
    orders:
        This batch's delta clean orders.
    customers:
        This batch's clean customers (new customers only in incremental mode).
    history_meta:
        ``{cid: {tier, city}}`` recovered from ``state/aggregates/customer_value.csv``.

    Returns
    -------
    (cv_rows, tier_rows):
        ``cv_rows`` – per affected customer incremental ``orders`` / ``revenue``
        (the pipeline accumulates these and recomputes ``rank``).
        ``tier_rows`` – per tier incremental ``revenue`` plus ``customers``
        counting only genuinely new customers (those absent from ``history_meta``)
        so the pipeline's accumulation yields the correct distinct customer count.

    Tier/city for historical customers is taken from ``history_meta`` (the
    pipeline's merge keeps the historical value when the new value is blank, but
    filling it here makes the batch output self-describing). New customers use
    this batch's ``customers`` table.
    """
    cmeta = {c["customer_id"]: c for c in customers}
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    for r in orders:
        cid = r.get("customer_id", "")
        b = buckets[cid]
        b["orders"] += 1
        b["revenue"] += as_float(r.get("total_amount")) or 0.0

    ranked = sorted(buckets.items(), key=lambda kv: -kv[1]["revenue"])
    cv_rows: list[dict[str, Any]] = []
    for i, (cid, b) in enumerate(ranked):
        if cid in history_meta:
            tier = history_meta[cid]["tier"]
            city = history_meta[cid]["city"]
        else:
            c = cmeta.get(cid, {})
            tier = c.get("tier", "")
            city = c.get("city", "")
        cv_rows.append(
            {
                "customer_id": cid,
                "tier": tier,
                "city": city,
                "orders": b["orders"],
                "revenue": round(b["revenue"], 2),
                "rank": i + 1,
            }
        )

    tier_buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"customers": 0, "revenue": 0.0})
    for cid, b in buckets.items():
        if cid in history_meta:
            tier = history_meta[cid]["tier"] or "unknown"
        else:
            tier = cmeta.get(cid, {}).get("tier", "unknown")
        t = tier_buckets[tier]
        if cid not in history_meta:
            t["customers"] += 1
        t["revenue"] += b["revenue"]
    tier_rows = [
        {"tier": t, "customers": v["customers"], "revenue": round(v["revenue"], 2)}
        for t, v in sorted(tier_buckets.items())
    ]
    return cv_rows, tier_rows


# ---------------------------------------------------------------------------
# Phase 2a: Polars 列式聚合分支
# ---------------------------------------------------------------------------
# 设计参见 docs/evolution.md §4.4.1.2。读 CSV 时 infer_schema_length=0 让所有
# 列保留为 Utf8 字符串（与 clean 阶段写出格式对齐），聚合时 cast 数值列。
# 这样写出 CSV 与 Python 路径逐字段一致（order_date 保持 "2026-01-15" 而非
# Date 类型写出，数值列由 polars 直接写 str(float) 与 Python str(round) 一致）。


def _load_clean_polars(ctx: PipelineContext, name: str):
    """读 03_clean/<name>_clean.csv 为 polars.DataFrame（所有列 Utf8）.

    local_csv 下用 ``pl.read_csv(infer_schema_length=0)`` 保留 Utf8；
    parquet/iceberg 下读 .csv.parquet（上游 pyarrow 写全 String，读回保持
    Utf8 与 local_csv 一致）。Returns None when file missing.
    """
    import polars as pl  # lazy import

    from ..helpers import _get_storage_backend, table_read

    path = os.path.join(ctx.run_dir, "03_clean", name + "_clean.csv")
    if not _table_exists(path, ctx.config):
        return None
    if _get_storage_backend(ctx.config) != "local_csv":
        return table_read(path, ctx.config)
    return pl.read_csv(path, infer_schema_length=0)


def _qty_expr():
    """quantity 列表达式：cast Int64，非法值/空 → 0（对齐 as_int(...) or 0）."""
    import polars as pl

    return pl.col("quantity").cast(pl.Int64, strict=False).fill_null(0)


def _amt_expr():
    """total_amount 列表达式：cast Float64，非法值/空 → 0.0（对齐 as_float(...) or 0.0）."""
    import polars as pl

    return pl.col("total_amount").cast(pl.Float64, strict=False).fill_null(0.0)


def daily_sales_polars(orders):
    """daily_sales 聚合，Polars 列式实现.

    Returns polars.DataFrame with columns:
        order_date, orders, units, revenue, avg_order_value
    """
    import polars as pl

    return (
        orders.group_by("order_date", maintain_order=True)
        .agg(
            pl.len().cast(pl.Int64).alias("orders"),
            _qty_expr().sum().alias("units"),
            _amt_expr().sum().alias("revenue"),
        )
        .sort("order_date")
        .with_columns(
            pl.col("revenue").round(2).alias("revenue"),
            (pl.col("revenue") / pl.col("orders")).round(2).alias("avg_order_value"),
        )
        .select(["order_date", "orders", "units", "revenue", "avg_order_value"])
    )


def category_stats_polars(orders, products):
    """category_stats 聚合，Polars join + group_by.

    Returns polars.DataFrame with columns:
        category, orders, units, revenue, revenue_share
    """
    import polars as pl

    if products is not None and products.height > 0:
        pcat = products.select(["product_id", "category"])
        df = orders.join(pcat, on="product_id", how="left").with_columns(
            pl.col("category").fill_null("未知")
        )
    else:
        df = orders.with_columns(pl.lit("未知").alias("category"))
    agg = df.group_by("category", maintain_order=True).agg(
        pl.len().cast(pl.Int64).alias("orders"),
        _qty_expr().sum().alias("units"),
        _amt_expr().sum().alias("revenue"),
    )
    total = agg.select(pl.col("revenue").sum()).item() or 1.0
    # 排序用未 round 的 revenue（与 Python `key=lambda c: -buckets[c]["revenue"]` 一致）
    agg = agg.sort("revenue", descending=True, maintain_order=True)
    return agg.with_columns(
        pl.col("revenue").round(2).alias("revenue"),
        (pl.col("revenue") / total).round(4).alias("revenue_share"),
    ).select(["category", "orders", "units", "revenue", "revenue_share"])


def region_channel_stats_polars(orders):
    """region_channel_stats 聚合，Polars group_by.

    Returns polars.DataFrame with columns:
        region, channel, orders, revenue
    """
    import polars as pl

    return (
        orders.group_by(["region", "channel"], maintain_order=True)
        .agg(
            pl.len().cast(pl.Int64).alias("orders"),
            _amt_expr().sum().round(2).alias("revenue"),
        )
        .sort(["region", "channel"])
        .select(["region", "channel", "orders", "revenue"])
    )


def customer_value_polars(orders, customers, top_n: int):
    """customer_value 聚合，Polars group_by + sort + head.

    Returns dict {"top": polars.DataFrame, "tiers": polars.DataFrame}.
    top columns: customer_id, tier, city, orders, revenue, rank
    tiers columns: tier, customers, revenue
    """
    import polars as pl

    buckets = orders.group_by("customer_id", maintain_order=True).agg(
        pl.len().cast(pl.Int64).alias("orders"),
        _amt_expr().sum().alias("revenue"),
    )
    # 排序用未 round 的 revenue（与 Python `key=lambda kv: -kv[1]["revenue"]` 一致）
    ranked = buckets.sort("revenue", descending=True, maintain_order=True)

    # top N + tier/city
    if customers is not None and customers.height > 0:
        cmeta = customers.select(["customer_id", "tier", "city"])
        top = (
            ranked.head(top_n)
            .join(cmeta, on="customer_id", how="left")
            .with_columns(
                pl.col("tier").fill_null(""),
                pl.col("city").fill_null(""),
            )
        )
    else:
        top = ranked.head(top_n).with_columns(
            pl.lit("").alias("tier"),
            pl.lit("").alias("city"),
        )
    top = (
        top.with_columns(pl.col("revenue").round(2).alias("revenue"))
        .with_row_index("rank", offset=1)
        .select(["customer_id", "tier", "city", "orders", "revenue", "rank"])
    )

    # tier 聚合
    if customers is not None and customers.height > 0:
        ctier = customers.select(["customer_id", "tier"])
        tier_df = buckets.join(ctier, on="customer_id", how="left").with_columns(
            pl.col("tier").fill_null("unknown")
        )
    else:
        tier_df = buckets.with_columns(pl.lit("unknown").alias("tier"))
    tiers = (
        tier_df.group_by("tier", maintain_order=True)
        .agg(
            pl.len().cast(pl.Int64).alias("customers"),
            pl.col("revenue").sum().round(2).alias("revenue"),
        )
        .sort("tier")
        .select(["tier", "customers", "revenue"])
    )
    return {"top": top, "tiers": tiers}


def _customer_value_incremental_polars(
    orders,
    customers,
    history_meta: dict[str, dict[str, str]],
):
    """增量 customer_value / customer_tier buckets，Polars 实现.

    与 ``_customer_value_incremental`` 逻辑对齐：
    - tier/city 优先取 history_meta，否则取 customers 表
    - tier_rows 的 customers 计数只对 genuinely new customers（不在 history_meta）
    - rank 按 delta revenue 降序（与 Python sorted key=-revenue 一致）
    """
    import polars as pl

    buckets = orders.group_by("customer_id", maintain_order=True).agg(
        pl.len().cast(pl.Int64).alias("orders"),
        _amt_expr().sum().alias("revenue"),
    )
    ranked = buckets.sort("revenue", descending=True, maintain_order=True)

    # 构建 tier/city：优先 history_meta，否则 customers 表
    if history_meta:
        hist_df = pl.DataFrame(
            {
                "customer_id": list(history_meta.keys()),
                "tier_h": [history_meta[k]["tier"] for k in history_meta],
                "city_h": [history_meta[k]["city"] for k in history_meta],
            },
            schema_overrides={
                "customer_id": pl.Utf8,
                "tier_h": pl.Utf8,
                "city_h": pl.Utf8,
            },
        )
        ranked = ranked.join(hist_df, on="customer_id", how="left")
    else:
        ranked = ranked.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("tier_h"),
            pl.lit(None, dtype=pl.Utf8).alias("city_h"),
        )

    if customers is not None and customers.height > 0:
        ranked = ranked.join(
            customers.select(["customer_id", "tier", "city"]),
            on="customer_id",
            how="left",
        )
    else:
        ranked = ranked.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("tier"),
            pl.lit(None, dtype=pl.Utf8).alias("city"),
        )

    # tier/city: 优先 history (tier_h)，否则 customers (tier)，否则 ""
    # 注：tier_for_agg 需把空串视同缺失（对齐 python
    # `history_meta[cid]["tier"] or "unknown"`）——coalesce 只跳过 null 不跳过
    # ""，历史客户 tier 为 "" 时旧实现分桶到 ""，python 分桶到 "unknown"。
    # tier_final/city_final 保持原式：python cv 行对历史空 tier 原样保留，
    # coalesce(tier_h, tier) 已与之等价（"" 非 null，直接命中）。
    def _blank_to_null(name: str):
        return pl.when(pl.col(name) == "").then(pl.lit(None, dtype=pl.Utf8)).otherwise(pl.col(name))

    ranked = ranked.with_columns(
        pl.coalesce(["tier_h", "tier"]).fill_null("").alias("tier_final"),
        pl.coalesce(["city_h", "city"]).fill_null("").alias("city_final"),
        # tier for aggregation: 优先 tier_h，否则 tier，否则 "unknown"
        pl.coalesce([_blank_to_null("tier_h"), _blank_to_null("tier")])
        .fill_null("unknown")
        .alias("tier_for_agg"),
        # is_new: 不在 history_meta（tier_h is null）→ 1，否则 0
        pl.when(pl.col("tier_h").is_not_null()).then(0).otherwise(1).alias("is_new"),
    )

    cv_rows = (
        ranked.with_columns(pl.col("revenue").round(2).alias("revenue"))
        .with_row_index("rank", offset=1)
        .select(["customer_id", "tier_final", "city_final", "orders", "revenue", "rank"])
        .rename({"tier_final": "tier", "city_final": "city"})
    )

    tier_rows = (
        ranked.group_by("tier_for_agg", maintain_order=True)
        .agg(
            pl.col("is_new").cast(pl.Int64).sum().alias("customers"),
            pl.col("revenue").sum().round(2).alias("revenue"),
        )
        .sort("tier_for_agg")
        .rename({"tier_for_agg": "tier"})
        .select(["tier", "customers", "revenue"])
    )
    return cv_rows, tier_rows


def _df_to_dicts(df):
    """polars.DataFrame → list of dict（与 Python 路径 ctx.aggregates 格式对齐）."""
    if df is None or df.height == 0:
        return []
    return df.to_dicts()


# ---------------------------------------------------------------------------
# Phase 2b: Spark 分布式聚合分支
# ---------------------------------------------------------------------------
# 设计参见 docs/evolution.md §4.4.2.1。读入用 table_read（backend="spark" 下
# 返回 SparkDataFrame），聚合用 Spark DataFrame API（groupBy+agg+window），
# 写出用 table_write。ctx.aggregates 用 df.collect()+asDict() 收集到 driver
# 为 List[Dict]（小数据量安全，与 Python/Polars 路径格式对齐供 output 消费）。


def _load_clean_spark(ctx: PipelineContext, name: str):
    """读 03_clean/<name>_clean.csv 为 SparkDataFrame（通过 table_read 路由）.

    Returns None when file missing.
    """
    from ..helpers import table_read

    path = os.path.join(ctx.run_dir, "03_clean", name + "_clean.csv")
    if not _table_exists(path, ctx.config):
        return None
    return table_read(path, ctx.config, spark=ctx.spark_session)


def daily_sales_spark(orders):
    """daily_sales 聚合，Spark 分布式 groupBy 实现.

    Returns SparkDataFrame with columns:
        order_date, orders, units, revenue, avg_order_value
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    # total_amount 已 round 到 2 位小数，用 decimal(20,2) 累加避免浮点误差
    # （Spark 分布式 sum 的浮点累加顺序与 Python 顺序累加不同，可能导致微小差异，
    # 在 avg_order_value 的 .5 边界触发 HALF_UP 进位，与 Python banker's rounding 不一致）。
    # avg_order_value 用未 round 的 raw_revenue 计算（与 Python 路径
    # `round(b["revenue"] / b["orders"], 2)` 一致，b["revenue"] 是未 round 的累加值）
    # F.bround 用 banker's rounding (HALF_EVEN)，对齐 Python round 的舍入模式
    return (
        orders.groupBy("order_date")
        .agg(
            F.count("*").alias("orders"),
            # 先 cast 再 sum：cluster 模式 createDataFrame 的列全是 string，
            # F.sum("quantity") 直接抛 AnalysisException；try_cast long 还与
            # Python as_int 语义对齐（非整数值如 "5.5" → null，不贡献 units，
            # 等价 as_int(...) or 0）。用 try_cast 而非 cast：Spark 4.x 默认
            # ANSI 开启，cast 对 "5.5" 等非整数值抛 CAST_INVALID_INPUT，
            # 只有 try_cast 在 ANSI 开/关下都返回 null。单机 inferSchema
            # 数值列 try_cast 无副作用。
            F.sum(F.col("quantity").try_cast("long")).cast("int").alias("units"),
            F.sum(F.col("total_amount").cast(DecimalType(20, 2))).alias("_raw_revenue_dec"),
        )
        .withColumn("_raw_revenue", F.col("_raw_revenue_dec").cast("double"))
        .withColumn("revenue", F.round(F.col("_raw_revenue"), 2))
        .withColumn("avg_order_value", F.bround(F.col("_raw_revenue") / F.col("orders"), 2))
        .drop("_raw_revenue", "_raw_revenue_dec")
        .orderBy("order_date")
    )


def category_stats_spark(orders, products):
    """category_stats 聚合，Spark join + groupBy.

    Returns SparkDataFrame with columns:
        category, orders, units, revenue, revenue_share
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    if products is not None and products.count() > 0:
        pcat = products.select("product_id", "category")
        df = orders.join(pcat, "product_id", "left").fillna({"category": "未知"})
    else:
        df = orders.withColumn("category", F.lit("未知"))
    # decimal(20,2) 累加 total_amount 避免浮点误差（与 daily_sales_spark 同理）
    agg = df.groupBy("category").agg(
        F.count("*").alias("orders"),
        # 先 cast 再 sum（同 daily_sales_spark）：string 列直接 sum 抛
        # AnalysisException；try_cast long 对齐 as_int 语义（非法值 → null → 0），
        # 且 ANSI 开启时不抛异常（cast 会抛 CAST_INVALID_INPUT）
        F.sum(F.col("quantity").try_cast("long")).cast("int").alias("units"),
        F.sum(F.col("total_amount").cast(DecimalType(20, 2))).cast("double").alias("revenue"),
    )
    # total 用未 round 的 revenue（与 Python `sum(b["revenue"] for b in buckets.values())` 一致）
    total = agg.agg(F.sum("revenue").alias("total")).collect()[0]["total"] or 1.0
    # 排序用未 round 的 revenue（与 Python `key=lambda c: -buckets[c]["revenue"]` 一致）
    # revenue_share 用未 round 的 revenue / total（与 Python `round(b["revenue"] / total, 4)` 一致）
    return (
        agg.orderBy(F.desc("revenue"))
        .withColumn("revenue_share", F.round(F.col("revenue") / F.lit(total), 4))
        .withColumn("revenue", F.round(F.col("revenue"), 2))
        .select("category", "orders", "units", "revenue", "revenue_share")
    )


def region_channel_stats_spark(orders):
    """region_channel_stats 聚合，Spark groupBy.

    Returns SparkDataFrame with columns:
        region, channel, orders, revenue
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    # decimal(20,2) 累加 total_amount 避免浮点误差（与 daily_sales_spark 同理）
    return (
        orders.groupBy("region", "channel")
        .agg(
            F.count("*").alias("orders"),
            F.round(F.sum(F.col("total_amount").cast(DecimalType(20, 2))).cast("double"), 2).alias(
                "revenue"
            ),
        )
        .orderBy("region", "channel")
        .select("region", "channel", "orders", "revenue")
    )


def customer_value_spark(orders, customers, top_n: int):
    """customer_value 聚合，Spark groupBy + 窗口函数取 Top N.

    Returns dict {"top": SparkDataFrame, "tiers": SparkDataFrame}.
    top columns: customer_id, tier, city, orders, revenue, rank
    tiers columns: tier, customers, revenue
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType
    from pyspark.sql.window import Window

    # 聚合每个客户的 orders / revenue
    # decimal(20,2) 累加 total_amount 避免浮点误差（与 daily_sales_spark 同理）
    # buckets._raw_revenue 保留未 round 的累加值，revenue 是 round 后的值
    buckets = (
        orders.groupBy("customer_id")
        .agg(
            F.count("*").alias("orders"),
            F.sum(F.col("total_amount").cast(DecimalType(20, 2)))
            .cast("double")
            .alias("_raw_revenue"),
        )
        .withColumn("revenue", F.round(F.col("_raw_revenue"), 2))
    )

    # 窗口函数取 Top N：row_number().over(Window.orderBy(F.desc("revenue")))
    w = Window.orderBy(F.desc("revenue"))
    ranked = buckets.withColumn("rank", F.row_number().over(w))

    # top N + tier/city
    if customers is not None and customers.count() > 0:
        cmeta = customers.select("customer_id", "tier", "city")
        top = (
            ranked.filter(F.col("rank") <= top_n)
            .join(cmeta, "customer_id", "left")
            .fillna({"tier": "", "city": ""})
        )
    else:
        top = (
            ranked.filter(F.col("rank") <= top_n)
            .withColumn("tier", F.lit(""))
            .withColumn("city", F.lit(""))
        )
    top = top.select("customer_id", "tier", "city", "orders", "revenue", "rank")

    # tier 汇总：revenue 用未 round 的 _raw_revenue 累加（与 Python 路径
    # `t["revenue"] += b["revenue"]` 一致，b["revenue"] 是未 round 的累加值）
    if customers is not None and customers.count() > 0:
        ctier = customers.select("customer_id", "tier")
        tier_df = buckets.join(ctier, "customer_id", "left").fillna({"tier": "unknown"})
    else:
        tier_df = buckets.withColumn("tier", F.lit("unknown"))
    tiers = (
        tier_df.groupBy("tier")
        .agg(
            F.count("*").alias("customers"),
            F.round(F.sum("_raw_revenue"), 2).alias("revenue"),
        )
        .orderBy("tier")
        .select("tier", "customers", "revenue")
    )
    return {"top": top, "tiers": tiers}


def _customer_value_incremental_spark(
    orders: Any,
    customers: Any,
    history_meta: dict[str, dict[str, str]],
    spark_session: Any,
) -> tuple[Any, Any]:
    """增量 customer_value / customer_tier buckets，Spark 实现.

    语义与 ``_customer_value_incremental``（python）/
    ``_customer_value_incremental_polars`` 对齐：

    - cv 输出 delta 内**全部**受影响客户的聚合行（不做 top_n 截断），供
      pipeline ``_advance_and_merge`` 按 key 累加。旧实现直接复用全量语义的
      ``customer_value_spark``：rank<=top_n 会截断 top_n 之外的老客户增量
      revenue（跨批少计），且 tiers 用 count(*) 把 delta 内全部客户（含老
      客户）计入 tier 客户数（跨批重复膨胀）——两者都在此修正。
    - tier/city：优先 ``history_meta``（来自 state/aggregates/
      customer_value.csv，读取路径复用 ``_history_customer_meta``），否则取
      本批 customers 表，否则 ""。
    - tier_rows 的 customers 仅计 history 中不存在的真新客户（anti join
      语义：tier_h 为 null ⟺ cid 不在 history），revenue 用未 round 的
      累加值（对齐 python `t["revenue"] += b["revenue"]` 后输出再 round）。
    - rank 按 delta revenue 降序（对齐 python sorted key=-revenue）；
      revenue 并列时按 customer_id 升序保证 spark 内部确定性。
    - tier 分桶把空串视同缺失（对齐 python `or "unknown"`）。

    Returns:
        (cv_df, tier_df)，列结构分别与 customer_value.csv / customer_tier.csv
        一致（customer_id/tier/city/orders/revenue/rank；tier/customers/revenue）。
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType
    from pyspark.sql.window import Window

    # delta orders 按客户聚合 orders / revenue（decimal 累加对齐全量路径）
    buckets = orders.groupBy("customer_id").agg(
        F.count("*").alias("orders"),
        F.sum(F.col("total_amount").cast(DecimalType(20, 2))).cast("double").alias("_raw_revenue"),
    )
    # 关键：不做 row_number <= top_n 截断——输出全部受影响客户
    ranked = buckets.withColumn(
        "rank",
        F.row_number().over(Window.orderBy(F.desc("_raw_revenue"), F.asc("customer_id"))),
    ).withColumn("revenue", F.round(F.col("_raw_revenue"), 2))

    # history_meta → 小表（列名 tier_h/city_h 避免与 customers 表 tier/city 冲突）
    if history_meta:
        hist_df = spark_session.createDataFrame(
            [
                (cid, meta.get("tier", ""), meta.get("city", ""))
                for cid, meta in history_meta.items()
            ],
            ["customer_id", "tier_h", "city_h"],
        )
        ranked = ranked.join(hist_df, "customer_id", "left")
    else:
        ranked = ranked.withColumn("tier_h", F.lit(None).cast("string")).withColumn(
            "city_h", F.lit(None).cast("string")
        )

    if customers is not None and customers.count() > 0:
        ranked = ranked.join(customers.select("customer_id", "tier", "city"), "customer_id", "left")
    else:
        ranked = ranked.withColumn("tier", F.lit(None).cast("string")).withColumn(
            "city", F.lit(None).cast("string")
        )

    # cv 行：tier/city 优先 history → customers → ""（对齐 python；历史客户
    # 不在本批 customers 表，coalesce(tier_h, tier) 即"历史优先"）
    cv_df = ranked.select(
        "customer_id",
        F.coalesce(F.col("tier_h"), F.col("tier"), F.lit("")).alias("tier"),
        F.coalesce(F.col("city_h"), F.col("city"), F.lit("")).alias("city"),
        "orders",
        "revenue",
        "rank",
    )

    # tier 行：空串视同缺失（对齐 python `history tier or "unknown"`），
    # 故 coalesce 前把 tier_h/tier 的 "" 转 null；customers 仅计真新客户
    def _blank_to_null(name: str) -> Any:
        return F.when(F.col(name) == "", F.lit(None).cast("string")).otherwise(F.col(name))

    tier_for_agg = F.coalesce(
        _blank_to_null("tier_h"), _blank_to_null("tier"), F.lit("unknown")
    ).alias("tier")
    is_new = F.when(F.col("tier_h").isNull(), F.lit(1)).otherwise(F.lit(0))
    tier_df = (
        ranked.groupBy(tier_for_agg)
        .agg(
            F.sum(is_new).cast("long").alias("customers"),
            F.round(F.sum("_raw_revenue"), 2).alias("revenue"),
        )
        .orderBy("tier")
        .select("tier", "customers", "revenue")
    )
    return cv_df, tier_df


def _spark_df_to_dicts(df):
    """SparkDataFrame → list of dict（与 Python 路径 ctx.aggregates 格式对齐）.

    用 df.collect() + row.asDict() 收集到 driver 为 List[Dict]。仅用于小数据量
    （聚合结果，dashboard 数据）。不用 df.toPandas()：它引入未声明的 pandas
    硬依赖（spark extra 只装 pyspark），且聚合输出列已全部 cast 为
    string/int/double，asDict 直接给出原生类型，语义等价。
    """
    if df is None:
        return []
    return [row.asDict() for row in df.collect()]


# ---------------------------------------------------------------------------
# 三引擎公共逻辑：skip KPI / KPI 计算 / lineage 声明
# ---------------------------------------------------------------------------
# _run_python / _run_polars / _run_spark 在以下三处逻辑完全相同：
#   1. orders 为空时构造 skip KPI + 空 aggregates + 写 kpi.json + 返回
#   2. 从 daily buckets 计算 KPI dict
#   3. 声明 lineage 边（04_aggregates/* ← 03_clean/*）
# 抽取为公共函数避免重复代码（本次语义统一，策略模式留待后续重构）。


def _skip_kpi(cconf: dict[str, Any]) -> dict[str, Any]:
    """构造 compute skip 时的 kpi dict（无 orders 输入，全 0）."""
    return {
        "orders": 0,
        "units": 0,
        "total_revenue": 0.0,
        "avg_order_value": 0.0,
        "days": 0,
        "currency": cconf.get("currency", "CNY"),
    }


def _skip_result(ctx: PipelineContext, agg_dir: str, cconf: dict[str, Any], log) -> dict[str, Any]:
    """构造 compute skip 时的 ctx.aggregates / kpi.json / return dict.

    三引擎 ``_run_*`` 在 orders 为空时调用，行为一致：
      - 写 kpi.json（全 0 KPI）
      - 设置 ctx.aggregates 为空 buckets
      - 返回 ``{"rows_in": 0, "rows_out": 0, "lineage": {}}``

    Args:
        ctx:     PipelineContext.
        agg_dir: 04_aggregates 目录.
        cconf:   cfg["compute"] 配置 dict.
        log:     StageLog（用于 warn）.

    Returns:
        skip 时的 stage result dict（无 lineage 边）.
    """
    log.warn("no clean orders; compute skipped", reason="missing_input")
    kpi = _skip_kpi(cconf)
    ctx.aggregates = {
        "daily": [],
        "category": [],
        "region_channel": [],
        "customer_value": {"top": [], "tiers": []},
        "kpi": kpi,
    }
    json_save(os.path.join(agg_dir, "kpi.json"), kpi)
    # No lineage edges: kpi.json has no upstream products when skipped.
    return {"rows_in": 0, "rows_out": 0, "lineage": {}}


def _compute_kpi(
    orders_count: int, daily_dicts: list[dict[str, Any]], currency: str
) -> dict[str, Any]:
    """从 daily buckets 计算 KPI dict（三引擎共用）.

    与原 ``_run_python`` / ``_run_polars`` / ``_run_spark`` 中 KPI 计算逻辑一致：
      - total_revenue = round(sum(daily.revenue), 2)
      - avg_order_value = round(total_revenue / orders_count, 2) if orders_count else 0.0

    Args:
        orders_count: orders 行数（Python=len(orders), Polars=df.height, Spark=df.count()）.
        daily_dicts:  daily_sales buckets 转成的 List[Dict].
        currency:     币种字符串（cconf.get("currency", "CNY")）.

    Returns:
        KPI dict（orders/units/total_revenue/avg_order_value/days/currency）.
    """
    total_revenue = round(sum(b["revenue"] for b in daily_dicts), 2)
    return {
        "orders": orders_count,
        "units": sum(b["units"] for b in daily_dicts),
        "total_revenue": total_revenue,
        "avg_order_value": round(total_revenue / orders_count, 2) if orders_count else 0.0,
        "days": len(daily_dicts),
        "currency": currency,
    }


def _compute_lineage(ctx: PipelineContext, cfg: dict[str, Any]) -> dict[str, list]:
    """构造 compute stage 的 lineage dict（三引擎共用）.

    lineage 边以 ``03_clean/*.csv`` 为上游，``04_aggregates/*.csv/json`` 为下游：
      - daily_sales / region_channel_stats / kpi.json 仅依赖 orders
      - category_stats 依赖 orders + products（若 products 存在）
      - customer_value / customer_tier 依赖 orders + customers（若 customers 存在）

    Args:
        ctx: PipelineContext.
        cfg: Pipeline 配置 dict（用于 _table_exists 检查 parquet/S3 backend）.

    Returns:
        lineage dict（key=下游相对路径，value=上游相对路径列表）.
    """
    orders_rel = "03_clean/orders_clean.csv"
    customers_rel = "03_clean/customers_clean.csv"
    products_rel = "03_clean/products_clean.csv"
    has_customers = _table_exists(os.path.join(ctx.run_dir, customers_rel), cfg)
    has_products = _table_exists(os.path.join(ctx.run_dir, products_rel), cfg)
    lineage: dict[str, list] = {
        "04_aggregates/daily_sales.csv": [orders_rel],
        "04_aggregates/region_channel_stats.csv": [orders_rel],
        "04_aggregates/kpi.json": [orders_rel],
    }
    cat_up = [orders_rel] + ([products_rel] if has_products else [])
    lineage["04_aggregates/category_stats.csv"] = cat_up
    cv_up = [orders_rel] + ([customers_rel] if has_customers else [])
    lineage["04_aggregates/customer_value.csv"] = cv_up
    lineage["04_aggregates/customer_tier.csv"] = cv_up
    return lineage


def run(ctx: PipelineContext, log) -> dict[str, Any]:
    cfg = ctx.config
    cconf = cfg.get("compute", {})
    agg_dir = os.path.join(ctx.run_dir, "04_aggregates")
    os.makedirs(agg_dir, exist_ok=True)

    return dispatch_by_engine(
        ctx.engine_backend, _run_python, _run_polars, _run_spark, ctx, log, agg_dir, cconf
    )


def _run_python(ctx: PipelineContext, log, agg_dir: str, cconf: dict[str, Any]) -> dict[str, Any]:
    cfg = ctx.config
    orders = _load_clean(ctx, "orders")
    customers = _load_clean(ctx, "customers")
    products = _load_clean(ctx, "products")

    if not orders:
        return _skip_result(ctx, agg_dir, cconf, log)

    if ctx.incremental_enabled:
        # Incremental: feed delta orders to the bucketing functions → delta
        # buckets. customer_value/tier need historical tier/city (the customers
        # table only carries new customers in incremental mode).
        daily = daily_sales(orders)
        cats = category_stats(orders, products)
        rcs = region_channel_stats(orders)
        history_meta = _history_customer_meta(ctx)
        cv_top, cv_tiers = _customer_value_incremental(orders, customers, history_meta)
        cv = {"top": cv_top, "tiers": cv_tiers}
        log.info(
            "compute incremental buckets",
            new_orders=len(orders),
            affected_customers=len(cv_top),
            history_customers=len(history_meta),
        )
    else:
        daily = daily_sales(orders)
        cats = category_stats(orders, products)
        rcs = region_channel_stats(orders)
        cv = customer_value(orders, customers, int(cconf.get("top_n_customers", 20)))

    table_write(
        os.path.join(agg_dir, "daily_sales.csv"),
        daily,
        cfg,
        fields=["order_date", "orders", "units", "revenue", "avg_order_value"],
    )
    table_write(
        os.path.join(agg_dir, "category_stats.csv"),
        cats,
        cfg,
        fields=["category", "orders", "units", "revenue", "revenue_share"],
    )
    table_write(
        os.path.join(agg_dir, "region_channel_stats.csv"),
        rcs,
        cfg,
        fields=["region", "channel", "orders", "revenue"],
    )
    table_write(
        os.path.join(agg_dir, "customer_value.csv"),
        cv["top"],
        cfg,
        fields=["customer_id", "tier", "city", "orders", "revenue", "rank"],
    )
    table_write(
        os.path.join(agg_dir, "customer_tier.csv"),
        cv["tiers"],
        cfg,
        fields=["tier", "customers", "revenue"],
    )

    kpi = _compute_kpi(len(orders), daily, cconf.get("currency", "CNY"))
    json_save(os.path.join(agg_dir, "kpi.json"), kpi)
    ctx.aggregates = {
        "daily": daily,
        "category": cats,
        "region_channel": rcs,
        "customer_value": cv,
        "kpi": kpi,
    }
    log.info("compute done", kpi=kpi)

    lineage = _compute_lineage(ctx, cfg)
    return {"rows_in": len(orders), "rows_out": len(daily), "lineage": lineage}


def _run_polars(ctx: PipelineContext, log, agg_dir: str, cconf: dict[str, Any]) -> dict[str, Any]:
    """Polars 列式聚合路径."""
    import polars as pl  # noqa: F401  lazy import

    from ..helpers import table_write

    orders = _load_clean_polars(ctx, "orders")
    customers = _load_clean_polars(ctx, "customers")
    products = _load_clean_polars(ctx, "products")

    if orders is None or orders.height == 0:
        return _skip_result(ctx, agg_dir, cconf, log)

    if ctx.incremental_enabled:
        daily_df = daily_sales_polars(orders)
        cats_df = category_stats_polars(orders, products)
        rcs_df = region_channel_stats_polars(orders)
        history_meta = _history_customer_meta(ctx)
        cv_top_df, cv_tiers_df = _customer_value_incremental_polars(orders, customers, history_meta)
        cv = {"top": cv_top_df, "tiers": cv_tiers_df}
        log.info(
            "compute incremental buckets (polars)",
            new_orders=orders.height,
            affected_customers=cv_top_df.height,
            history_customers=len(history_meta),
        )
    else:
        daily_df = daily_sales_polars(orders)
        cats_df = category_stats_polars(orders, products)
        rcs_df = region_channel_stats_polars(orders)
        cv = customer_value_polars(orders, customers, int(cconf.get("top_n_customers", 20)))

    # 写出 CSV（table_write 在 polars backend 下调 df.write_csv）
    table_write(os.path.join(agg_dir, "daily_sales.csv"), daily_df, ctx.config)
    table_write(os.path.join(agg_dir, "category_stats.csv"), cats_df, ctx.config)
    table_write(os.path.join(agg_dir, "region_channel_stats.csv"), rcs_df, ctx.config)
    table_write(os.path.join(agg_dir, "customer_value.csv"), cv["top"], ctx.config)
    table_write(os.path.join(agg_dir, "customer_tier.csv"), cv["tiers"], ctx.config)

    # ctx.aggregates 存 List[Dict]（与 Python 路径格式对齐，供 output stage 消费）
    daily_dicts = _df_to_dicts(daily_df)
    kpi = _compute_kpi(orders.height, daily_dicts, cconf.get("currency", "CNY"))
    json_save(os.path.join(agg_dir, "kpi.json"), kpi)
    ctx.aggregates = {
        "daily": daily_dicts,
        "category": _df_to_dicts(cats_df),
        "region_channel": _df_to_dicts(rcs_df),
        "customer_value": {
            "top": _df_to_dicts(cv["top"]),
            "tiers": _df_to_dicts(cv["tiers"]),
        },
        "kpi": kpi,
    }
    log.info("compute done (polars)", kpi=kpi)

    lineage = _compute_lineage(ctx, ctx.config)
    return {"rows_in": orders.height, "rows_out": daily_df.height, "lineage": lineage}


def _run_spark(ctx: PipelineContext, log, agg_dir: str, cconf: dict[str, Any]) -> dict[str, Any]:
    """Spark 分布式聚合路径.

    四个聚合用 Spark DataFrame API（groupBy+agg+window），写出用 table_write
    （backend="spark" 下调 df.write.mode("overwrite").csv/parquet）。
    ctx.aggregates 用 df.collect()+asDict() 收集到 driver 为 List[Dict]
    （小数据量安全，与 Python/Polars 路径格式对齐供 output 消费）。
    """
    from ..helpers import table_write

    orders = _load_clean_spark(ctx, "orders")
    customers = _load_clean_spark(ctx, "customers")
    products = _load_clean_spark(ctx, "products")

    orders_count = 0 if orders is None else orders.count()
    if orders_count == 0:
        return _skip_result(ctx, agg_dir, cconf, log)

    daily_df = daily_sales_spark(orders)
    cats_df = category_stats_spark(orders, products)
    rcs_df = region_channel_stats_spark(orders)

    if ctx.incremental_enabled:
        # 增量模式：输出与 Python/Polars 语义对齐的增量 buckets
        # （_customer_value_incremental_spark）。
        # 旧实现直接复用全量语义 customer_value_spark，存在两处跨引擎分歧：
        # (1) rank<=top_n 截断使 top_n 之外老客户的增量 revenue 被丢弃、跨批少计；
        # (2) tiers 用 count(*) 把 delta 内全部客户计进 tier 客户数，跨批重复膨胀
        # （python 只统计 cid not in history_meta 的真新客户）。
        # history_meta 读取路径复用 _history_customer_meta（load_csv），与
        # Python/Polars 路径一致。
        history_meta = _history_customer_meta(ctx)
        spark_session = ctx.spark_session
        assert spark_session is not None, "spark compute 路径必须持有 SparkSession"
        cv_top_df, cv_tiers_df = _customer_value_incremental_spark(
            orders, customers, history_meta, spark_session
        )
        cv = {"top": cv_top_df, "tiers": cv_tiers_df}
        log.info(
            "compute incremental buckets (spark)",
            new_orders=orders_count,
            affected_customers=cv_top_df.count(),
            history_customers=len(history_meta),
        )
    else:
        cv = customer_value_spark(orders, customers, int(cconf.get("top_n_customers", 20)))

    # 写出 CSV（table_write 在 spark backend 下调 df.write.mode("overwrite").csv）
    table_write(
        os.path.join(agg_dir, "daily_sales.csv"), daily_df, ctx.config, spark=ctx.spark_session
    )
    table_write(
        os.path.join(agg_dir, "category_stats.csv"), cats_df, ctx.config, spark=ctx.spark_session
    )
    table_write(
        os.path.join(agg_dir, "region_channel_stats.csv"),
        rcs_df,
        ctx.config,
        spark=ctx.spark_session,
    )
    table_write(
        os.path.join(agg_dir, "customer_value.csv"), cv["top"], ctx.config, spark=ctx.spark_session
    )
    table_write(
        os.path.join(agg_dir, "customer_tier.csv"), cv["tiers"], ctx.config, spark=ctx.spark_session
    )

    # ctx.aggregates 存 List[Dict]（与 Python/Polars 路径格式对齐，供 output stage 消费）
    # orders_count 复用入口处的 count() 结果，避免重复 action
    daily_dicts = _spark_df_to_dicts(daily_df)
    kpi = _compute_kpi(orders_count, daily_dicts, cconf.get("currency", "CNY"))
    json_save(os.path.join(agg_dir, "kpi.json"), kpi)
    ctx.aggregates = {
        "daily": daily_dicts,
        "category": _spark_df_to_dicts(cats_df),
        "region_channel": _spark_df_to_dicts(rcs_df),
        "customer_value": {
            "top": _spark_df_to_dicts(cv["top"]),
            "tiers": _spark_df_to_dicts(cv["tiers"]),
        },
        "kpi": kpi,
    }
    log.info("compute done (spark)", kpi=kpi)

    lineage = _compute_lineage(ctx, ctx.config)
    daily_count = daily_df.count()
    return {"rows_in": orders_count, "rows_out": daily_count, "lineage": lineage}
