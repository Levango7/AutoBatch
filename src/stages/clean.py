"""Stage 3: dedup, fill missing, type coercion, total_amount, outlier flag.

Phase 2a 增加 Polars 列式分支（``ctx.engine_backend == "polars"``）：
- 去重用 ``df.unique(subset=..., keep="first", maintain_order=True)``
- 补缺用 ``pl.when(col == "").then(default).otherwise(col)``
- ``total_amount`` 用 ``(quantity * unit_price).round(2)`` 表达式
- ``is_anomaly`` 用 ``pl.col("order_id").is_in(outlier_keys)`` 标记
- 写出用 ``table_write``

Phase 2b 增加 Spark 分布式分支（``ctx.engine_backend == "spark"``）：
- 去重用 ``df.dropDuplicates(["order_id"])``（Spark DataFrame API）
- 补缺用 ``df.fillna(fill_missing)``（Spark 用 fillna，不是 fill_null）
- ``total_amount`` 用 ``F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2)``；
  当前数据集无 ``discount`` 列时退化为 ``F.round(qty * price, 2)`` 与 Python/Polars 路径对齐
- ``is_anomaly`` 用 ``F.when(col("order_id").isin(outlier_keys), lit("1")).otherwise(lit("0"))``
  条件表达式（与 Python/Polars 路径的 outlier_keys 集合标记语义一致，参见 docs/evolution.md §4.3.2.2）
- 读入用 ``table_read(path, cfg, spark=ctx.spark_session)``
- 写出用 ``table_write(path, df, cfg, spark=ctx.spark_session)``

向后兼容：``engine.backend="python"``（缺省）时走原 Python 循环路径，
行为 100% 不变。Polars 路径读 CSV 时强制 ``infer_schema_length=0``（所有
列保留为 Utf8 字符串），保证写出 CSV 与 Python 路径逐字段一致（日期/
数值不因类型推断改变字符串表示，参见 docs/evolution.md §4.3.1.2）。
"""

from __future__ import annotations

import os
from typing import Any

from ..helpers import (
    PipelineContext,
    _table_exists,
    as_float,
    as_int,
    table_read,
    table_write,
)


def _clean_orders(ctx: PipelineContext, log) -> tuple[int, list[dict[str, Any]], list[str]]:
    cfg = ctx.config
    cconf = cfg.get("clean", {})
    src = os.path.join(ctx.run_dir, "02_valid", "valid_orders.csv")
    if not _table_exists(src, cfg):
        log.warn("no valid orders", reason="file_missing")
        return 0, [], []
    rows, fields = table_read(src, cfg)
    dedup_cols = cconf.get("dedup_columns", ["order_id"])
    fill = cconf.get("fill_missing", {})
    flag_col = cconf.get("flag_column", "is_anomaly")
    outlier_keys = ctx.outlier_keys

    seen = set()
    kept = []
    dropped = 0
    for row in rows:
        key = tuple(row.get(c, "") for c in dedup_cols)
        if key in seen:
            dropped += 1
            continue
        seen.add(key)
        for col, default in fill.items():
            if row.get(col) is None or str(row.get(col)).strip() == "":
                row[col] = default
        qty = as_int(row.get("quantity")) or 0
        price = as_float(row.get("unit_price")) or 0.0
        row["total_amount"] = round(qty * price, 2)
        row[flag_col] = "1" if row.get("order_id") in outlier_keys else "0"
        kept.append(row)
    log.info("clean orders", rows_in=len(rows), rows_out=len(kept), dup_dropped=dropped)
    return len(rows), kept, fields + ["total_amount", flag_col]


def _clean_orders_polars(ctx: PipelineContext, log) -> tuple[int, Any, list[str]]:
    """Polars 列式实现：去重 / 补缺 / total_amount / is_anomaly.

    读时 ``infer_schema_length=0`` 让所有列保留为 Utf8 字符串，保证写出
    CSV 与 Python 路径逐字段一致。仅 ``total_amount`` 计算时 cast 为
    Float64，结果保留 Float64（写出时 polars 输出 ``str(float)`` 与
    Python ``str(round(...))`` 一致）。

    Returns:
        (rows_in, df_or_None, out_fields)。无源文件时 df 为 None。
    """
    import polars as pl  # lazy import：仅 polars 路径需要

    cfg = ctx.config
    cconf = cfg.get("clean", {})
    src = os.path.join(ctx.run_dir, "02_valid", "valid_orders.csv")
    if not os.path.exists(src):
        log.warn("no valid orders", reason="file_missing")
        return 0, None, []

    # 所有列 Utf8，保留原字符串表示（产物兼容）
    df = pl.read_csv(src, infer_schema_length=0)
    rows_in = df.height
    dedup_cols = cconf.get("dedup_columns", ["order_id"])
    fill = cconf.get("fill_missing", {})
    flag_col = cconf.get("flag_column", "is_anomaly")
    outlier_keys = list(ctx.outlier_keys)
    base_fields = list(df.columns)

    # 去重（maintain_order=True 保留首次出现顺序，与 Python seen-set 一致）
    df = df.unique(subset=dedup_cols, keep="first", maintain_order=True)
    dropped = rows_in - df.height

    # 补缺：空字符串替换为 default（与 Python `str(v).strip() == ""` 一致）
    for col, default in fill.items():
        if col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(col).cast(pl.Utf8).str.strip_chars() == "")
                .then(pl.lit(str(default)))
                .otherwise(pl.col(col))
                .alias(col)
            )

    # total_amount = round(quantity * unit_price, 2)
    # cast(strict=False) 把非法值变 null，fill_null(0) 对齐 as_int/as_float 的 `or 0`
    qty = pl.col("quantity").cast(pl.Float64, strict=False).fill_null(0.0)
    price = pl.col("unit_price").cast(pl.Float64, strict=False).fill_null(0.0)
    df = df.with_columns(((qty * price).round(2)).alias("total_amount"))

    # is_anomaly 标记
    df = df.with_columns(
        pl.when(pl.col("order_id").is_in(outlier_keys))
        .then(pl.lit("1"))
        .otherwise(pl.lit("0"))
        .alias(flag_col)
    )

    # out_fields：base_fields + 新增列，避免重复（validate stage 在 polars 路径下
    # 已把 total_amount 写入 valid_orders.csv，base_fields 可能已含该列）。
    extra = [c for c in ["total_amount", flag_col] if c not in base_fields]
    out_fields = base_fields + extra
    df = df.select(out_fields)
    log.info(
        "clean orders (polars)",
        rows_in=rows_in,
        rows_out=df.height,
        dup_dropped=dropped,
    )
    return rows_in, df, out_fields


def _clean_orders_spark(ctx: PipelineContext, log) -> tuple[int, Any, list[str]]:
    """Spark 分布式实现：去重 / 补缺 / total_amount / is_anomaly.

    通过 ``table_read`` 读入 SparkDataFrame（``spark.read.csv`` 默认 inferSchema），
    去重用 ``dropDuplicates``，补缺用 ``fillna``，``total_amount`` 用 Spark 表达式，
    ``is_anomaly`` 用 ``F.when(...)`` 条件表达式标记。

    Args:
        ctx: PipelineContext，``ctx.spark_session`` 提供 SparkSession。
        log: StageLog。

    Returns:
        (rows_in, df_or_None, out_fields)。无源文件时 df 为 None。
    """
    from pyspark.sql import functions as F  # lazy import：仅 spark 路径需要

    from ..helpers import table_read

    cfg = ctx.config
    cconf = cfg.get("clean", {})
    src = os.path.join(ctx.run_dir, "02_valid", "valid_orders.csv")
    if not _table_exists(src, cfg):
        log.warn("no valid orders", reason="file_missing")
        return 0, None, []

    # Spark 路径读入：table_read 在 backend="spark" 下返回 SparkDataFrame
    df = table_read(src, cfg, spark=ctx.spark_session)
    rows_in = df.count()  # 触发 action 取行数
    dedup_cols = cconf.get("dedup_columns", ["order_id"])
    fill = cconf.get("fill_missing", {})
    flag_col = cconf.get("flag_column", "is_anomaly")

    # 去重：dropDuplicates 保留首次出现（与 Python seen-set / Polars unique(keep="first") 对齐）
    df = df.dropDuplicates(dedup_cols)
    dropped = rows_in - df.count()

    # 补缺：Spark fillna 按列填默认值（与 Python `str(v).strip() == ""` 替换语义对齐）
    if fill:
        df = df.fillna(fill)

    # total_amount = round(quantity * unit_price * (1 - discount), 2)
    # 若 discount 列存在则用完整公式，否则退化为 qty * price（与 Python/Polars 路径对齐）
    # cluster 模式 ingest 用 createDataFrame(str_rows)，quantity/unit_price 是 string，
    # 需先 cast 成 double（单机模式 inferSchema 已是 double，cast 无副作用）。
    qty = F.col("quantity").cast("double")
    price = F.col("unit_price").cast("double")
    if "discount" in df.columns:
        amt_expr = F.round(qty * price * (F.lit(1.0) - F.col("discount").cast("double")), 2)
    else:
        amt_expr = F.round(qty * price, 2)
    df = df.withColumn("total_amount", amt_expr)

    # is_anomaly：用 outlier_keys 集合标记（与 Python/Polars 路径语义一致）
    # 用 "1"/"0" 字符串与 Python/Polars 路径写出格式对齐。
    # 大规模下 isin(数万元素) 会构建巨型表达式树（driver 内存 + 每 task 序列化
    # 双重爆炸，2026-08 千万行实测 OOM）——改 broadcast join 一张 ids 小表，
    # 语义等价且内存恒定。
    outlier_keys = list(ctx.outlier_keys)
    if outlier_keys:
        from pyspark.sql import functions as F  # noqa: F811 - 局部别名保持可读

        spark_session = ctx.spark_session
        assert spark_session is not None, "spark clean 路径必须持有 SparkSession"
        ids_df = spark_session.createDataFrame([(k,) for k in outlier_keys], schema=["_outlier_id"])
        df = (
            df.join(F.broadcast(ids_df), df["order_id"] == ids_df["_outlier_id"], "left")
            .withColumn(
                flag_col,
                F.when(F.col("_outlier_id").isNotNull(), F.lit("1")).otherwise(F.lit("0")),
            )
            .drop("_outlier_id")
        )
    else:
        df = df.withColumn(flag_col, F.lit("0"))

    # out_fields：base_fields + 新增列，避免重复
    base_fields = df.columns
    extra = [c for c in ["total_amount", flag_col] if c not in base_fields]
    out_fields = base_fields + extra
    df = df.select(*out_fields)
    log.info(
        "clean orders (spark)",
        rows_in=rows_in,
        rows_out=df.count(),
        dup_dropped=dropped,
    )
    return rows_in, df, out_fields


def run(ctx: PipelineContext, log) -> dict[str, Any]:
    cl_dir = os.path.join(ctx.run_dir, "03_clean")
    os.makedirs(cl_dir, exist_ok=True)

    if ctx.engine_backend == "spark":
        return _run_spark(ctx, log, cl_dir)
    if ctx.engine_backend == "polars":
        return _run_polars(ctx, log, cl_dir)
    return _run_python(ctx, log, cl_dir)


def _run_python(ctx: PipelineContext, log, cl_dir: str) -> dict[str, Any]:
    cfg = ctx.config
    rows_in, orders, o_fields = _clean_orders(ctx, log)
    table_write(os.path.join(cl_dir, "orders_clean.csv"), orders, cfg, fields=o_fields)
    rows_out = len(orders)

    # Declare lineage for clean products (paths relative to run_dir).
    lineage: dict[str, list] = {}
    if _table_exists(os.path.join(ctx.run_dir, "02_valid", "valid_orders.csv"), cfg):
        lineage["03_clean/orders_clean.csv"] = ["02_valid/valid_orders.csv"]
    for name in ("customers", "products"):
        src = os.path.join(ctx.run_dir, "02_valid", "valid_" + name + ".csv")
        if _table_exists(src, cfg):
            rows, fields = table_read(src, cfg)
            table_write(os.path.join(cl_dir, name + "_clean.csv"), rows, cfg, fields=fields)
            rows_out += len(rows)
            lineage[f"03_clean/{name}_clean.csv"] = [f"02_valid/valid_{name}.csv"]
    ctx.clean_orders = orders
    return {"rows_in": rows_in, "rows_out": rows_out, "lineage": lineage}


def _run_polars(ctx: PipelineContext, log, cl_dir: str) -> dict[str, Any]:
    """Polars 路径：orders 用列式去重/补缺/计算，customers/products 透传."""
    import polars as pl  # lazy import

    from ..helpers import table_write

    rows_in, df, o_fields = _clean_orders_polars(ctx, log)

    rows_out = 0
    if df is not None:
        table_write(os.path.join(cl_dir, "orders_clean.csv"), df, ctx.config, o_fields)
        rows_out = df.height
        # 缓存 List[Dict] 供下游（与 Python 路径类型对齐：total_amount 为 float）
        ctx.clean_orders = df.to_dicts()
    else:
        ctx.clean_orders = []

    # Declare lineage for clean products (paths relative to run_dir).
    lineage: dict[str, list] = {}
    if os.path.exists(os.path.join(ctx.run_dir, "02_valid", "valid_orders.csv")):
        lineage["03_clean/orders_clean.csv"] = ["02_valid/valid_orders.csv"]
    # customers/products 透传：读 Utf8 写 Utf8，保证产物与 Python 路径一致
    for name in ("customers", "products"):
        src = os.path.join(ctx.run_dir, "02_valid", "valid_" + name + ".csv")
        if os.path.exists(src):
            ref_df = pl.read_csv(src, infer_schema_length=0)
            ref_fields = list(ref_df.columns)
            table_write(
                os.path.join(cl_dir, name + "_clean.csv"),
                ref_df,
                ctx.config,
                ref_fields,
            )
            rows_out += ref_df.height
            lineage[f"03_clean/{name}_clean.csv"] = [f"02_valid/valid_{name}.csv"]
    return {"rows_in": rows_in, "rows_out": rows_out, "lineage": lineage}


def _run_spark(ctx: PipelineContext, log, cl_dir: str) -> dict[str, Any]:
    """Spark 路径：orders 用分布式去重/补缺/计算，customers/products 透传.

    orders 通过 ``_clean_orders_spark`` 走 Spark DataFrame API；
    customers/products 透传用 ``table_read``/``table_write`` 走 Spark IO 路由
    （backend="spark" 下读 CSV 为 SparkDataFrame，写出为多分区 CSV 目录）。
    """
    from ..helpers import table_read, table_write

    rows_in, df, o_fields = _clean_orders_spark(ctx, log)

    rows_out = 0
    if df is not None:
        table_write(
            os.path.join(cl_dir, "orders_clean.csv"),
            df,
            ctx.config,
            o_fields,
            spark=ctx.spark_session,
        )
        rows_out = df.count()
        # Spark path: skip toPandas cache (OOM at 10M+ rows, RSS 9.8GB).
        # Downstream reads from disk (03_clean/).
        ctx.clean_orders = []
    else:
        ctx.clean_orders = []

    # Declare lineage for clean products (paths relative to run_dir).
    lineage: dict[str, list] = {}
    if os.path.exists(os.path.join(ctx.run_dir, "02_valid", "valid_orders.csv")):
        lineage["03_clean/orders_clean.csv"] = ["02_valid/valid_orders.csv"]
    # customers/products 透传：table_read/table_write 走 Spark IO 路由
    for name in ("customers", "products"):
        src = os.path.join(ctx.run_dir, "02_valid", "valid_" + name + ".csv")
        if _table_exists(src, ctx.config):
            ref_df = table_read(src, ctx.config, spark=ctx.spark_session)
            table_write(
                os.path.join(cl_dir, name + "_clean.csv"),
                ref_df,
                ctx.config,
                spark=ctx.spark_session,
            )
            rows_out += ref_df.count()
            lineage[f"03_clean/{name}_clean.csv"] = [f"02_valid/valid_{name}.csv"]
    return {"rows_in": rows_in, "rows_out": rows_out, "lineage": lineage}
