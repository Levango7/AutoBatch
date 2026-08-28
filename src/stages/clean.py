"""Stage 3: dedup, fill missing, type coercion, total_amount, outlier flag.

Phase 2a 增加 Polars 列式分支（``ctx.engine_backend == "polars"``）：
- 去重用 ``df.unique(subset=..., keep="first", maintain_order=True)``
- 补缺用 ``pl.when(is_null() | strip=="" ).then(default).otherwise(col)``
- ``total_amount`` 用 ``(quantity * unit_price * (1 - discount)).round(2)`` 表达式
  （三引擎统一：discount 列存在时扣减，解析失败/缺失按 0）
- ``is_anomaly`` 用 ``pl.col("order_id").is_in(outlier_keys)`` 标记
- 写出用 ``table_write``

Phase 2b 增加 Spark 分布式分支（``ctx.engine_backend == "spark"``）：
- 去重用窗口 ``row_number()==1`` 按稳定行序（rdd.zipWithIndex + cache 物化）
  保留首次出现，对齐 Python seen-set keep-first（原 dropDuplicates 非确定）
- 补缺用 ``F.when(null 或 strip=="" → default)``（对齐 Python 语义）
- ``total_amount`` 用 ``F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2)``；
  三引擎统一：discount 列存在时扣减（解析失败/null 按 0），无该列时退化为
  ``F.round(qty * price, 2)``，与 Python/Polars 路径结果一致
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
    _get_storage_backend,
    _table_exists,
    as_float,
    as_int,
    table_read,
    table_write,
)
from ._dispatch import dispatch_by_engine


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
        # 三引擎统一：discount 列存在（含值为空/不可解析）时按 0 折扣参与计算；
        # 无 discount 列时 row.get 返回 None → 0.0，结果与旧公式一致。
        discount = as_float(row.get("discount")) or 0.0
        row["total_amount"] = round(qty * price * (1.0 - discount), 2)
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
    if not _table_exists(src, cfg):
        log.warn("no valid orders", reason="file_missing")
        return 0, None, []

    # storage.backend='parquet'/'iceberg' 时读 .csv.parquet（上游 pyarrow 写全 String，
    # 读回保持 Utf8 与 local_csv 的 infer_schema_length=0 一致）；local_csv 保持
    # 原有 pl.read_csv 行为（table_read 的 read_options 会解析日期/数值，破坏
    # 与 Python 路径逐字段一致）。
    if _get_storage_backend(cfg) != "local_csv":
        df = table_read(src, cfg)
    else:
        df = pl.read_csv(src, infer_schema_length=0)
    # Polars backend 下 table_read 返回 DataFrame；派生列需要 base_fields 为原始列名
    rows_in = df.height
    dedup_cols = cconf.get("dedup_columns", ["order_id"])
    fill = cconf.get("fill_missing", {})
    flag_col = cconf.get("flag_column", "is_anomaly")
    outlier_keys = list(ctx.outlier_keys)
    base_fields = list(df.columns)

    # 去重（maintain_order=True 保留首次出现顺序，与 Python seen-set 一致）
    df = df.unique(subset=dedup_cols, keep="first", maintain_order=True)
    dropped = rows_in - df.height

    # 补缺：null 或空白字符串替换为 default（与 Python
    # `v is None or str(v).strip() == ""` 一致）。
    # 注：旧实现只替换空字符串，polars read_csv 把空字段读为 null 时不触发
    # 替换，与 Python 路径分歧——条件补上 is_null。
    for col, default in fill.items():
        if col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(col).is_null() | (pl.col(col).cast(pl.Utf8).str.strip_chars() == ""))
                .then(pl.lit(str(default)))
                .otherwise(pl.col(col))
                .alias(col)
            )

    # total_amount = round(quantity * unit_price * (1 - discount), 2)
    # 三引擎统一：discount 列存在时扣减折扣（对齐 Spark 公式）；列缺失或
    # 解析失败/空值按 0 处理（strict=False cast → null → fill_null(0)，
    # 对齐 as_float(...) or 0.0）。
    # cast(strict=False) 把非法值变 null，fill_null(0) 对齐 as_int/as_float 的 `or 0`
    qty = pl.col("quantity").cast(pl.Float64, strict=False).fill_null(0.0)
    price = pl.col("unit_price").cast(pl.Float64, strict=False).fill_null(0.0)
    if "discount" in df.columns:
        disc = pl.col("discount").cast(pl.Float64, strict=False).fill_null(0.0)
    else:
        disc = pl.lit(0.0)
    # 转回 Utf8：与 python 路径 str(round(...)) 一致，parquet 写出时保留 String
    # （否则 polars 分支写 .parquet 会把 total_amount 存为 Float64，与 python
    # 路径的 pa.string() schema 不一致，破坏“逐字段一致”承诺）。
    df = df.with_columns(
        ((qty * price * (pl.lit(1.0) - disc)).round(2)).cast(pl.Utf8).alias("total_amount")
    )

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


def _dedup_keep_first_spark(df: Any, dedup_cols: list[str]) -> tuple[int, Any]:
    """Spark 确定性去重：按稳定行序保留重复组第一条，返回 (rows_in, deduped_df).

    ``dropDuplicates`` 不保证保留重复组中的哪一行（Spark 文档明确非确定），
    与 Python seen-set keep-first / Polars unique(keep="first") 分歧。这里用
    窗口 ``row_number()==1`` 判定：

    - 行号由 ``rdd.zipWithIndex()`` 在数据本身上派生，随后立即 ``cache()`` +
      ``count()`` 物化——所有下游 action（dropped 计数、写出、rows_out）
      共享同一份物理行序，保留哪一行完全确定且可复现。
    - zipWithIndex 后 ``rdd.toDF()`` 的结构为 ``_1``（原行 Row 折叠成的
      struct 列）+ ``_2``（Long 索引列），需把 ``_1.<col>`` 逐一展开还原，
      并把 ``_2`` 重命名为 ``_row_idx``；返回前剔除 ``_row_idx`` 保持原 schema。
    - null key：partitionBy 把 null 视为同一组参与去重，与 Python seen-set 中
      ``row.get(c, "") 组成的 tuple``（None 作为真实 key）语义一致。
    - 窗口函数不允许直接出现在 WHERE 子句（SQLSTATE 42601
      WINDOW_FUNCTION_NOT_ALLOWED_IN_CLAUSE），必须先 ``withColumn`` 物化
      ``row_number`` 再按列过滤。
    - 空表短路：``rdd.zipWithIndex().toDF()`` 在空 RDD 上做 schema 推断会调
      ``rdd.first()`` 抛 ``ValueError: RDD is empty``（增量批次零新增行时
      clean 即拿到空 DataFrame，与 quality.py 同源问题，同样短路处理）。
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType, StructField, StructType
    from pyspark.sql.window import Window

    # isEmpty() 底层调 take(1)，Python 3.14 + PySpark 4.2.0 下同样会触发
    # worker pickle 崩溃（Connection reset by peer）。改用 count()==0。
    if df.rdd.count() == 0:
        return 0, df

    # 显式 schema 绕开 rdd.first() 推断（Python 3.14 + PySpark 4.2.0 crash）
    inner = StructType(df.schema.fields)
    zw_schema = StructType(
        [
            StructField("_1", inner, nullable=False),
            StructField("_2", LongType(), nullable=False),
        ]
    )
    indexed = (
        df.rdd.zipWithIndex()
        .toDF(zw_schema)
        .select(
            *[F.col("_1." + c).alias(c) for c in df.columns],
            F.col("_2").alias("_row_idx"),
        )
    )
    indexed = indexed.cache()
    rows_in = indexed.count()  # 立即物化缓存，后续所有 action 行序一致
    w = Window.partitionBy(*dedup_cols).orderBy(F.col("_row_idx"))
    deduped = (
        indexed.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_row_idx", "_rn")
    )
    return rows_in, deduped


def _clean_orders_spark(ctx: PipelineContext, log) -> tuple[int, Any, list[str]]:
    """Spark 分布式实现：去重 / 补缺 / total_amount / is_anomaly.

    通过 ``table_read`` 读入 SparkDataFrame（``spark.read.csv`` 默认 inferSchema），
    去重用窗口 ``row_number()==1`` 按稳定行序保留首次出现（``_dedup_keep_first_spark``，
    替代非确定的 dropDuplicates），补缺用 ``F.when(null 或空白, default)``
    （对齐 Python 的 None/strip=="" 语义），``total_amount`` 用 Spark 表达式，
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
    dedup_cols = cconf.get("dedup_columns", ["order_id"])
    fill = cconf.get("fill_missing", {})
    flag_col = cconf.get("flag_column", "is_anomaly")

    # 去重：按稳定行序保留首次出现（对齐 Python seen-set / Polars
    # unique(keep="first")）。旧实现的 dropDuplicates 不保证保留重复组中的
    # 哪一行（Spark 文档明确非确定），注释却声称"保留首次出现"，与 Python
    # 路径实际分歧。改为窗口 row_number()==1 判定：
    # - orderBy 用 rdd.zipWithIndex() 派生的行号，zipWithIndex 后立即
    #   cache()+count() 物化——所有下游 action 共享同一份行序，判定确定；
    #   （zipWithIndex 后 toDF 把原行折叠为 struct 列 `_1` + 索引列 `_2`，
    #    须展开 `_1.*` 还原原始列）
    # - partitionBy 把 null key 视为同一组，与 Python seen-set 中
    #   tuple(None)/tuple("") 作为真实 key 参与去重的行为一致。
    rows_in, df = _dedup_keep_first_spark(df, dedup_cols)
    dropped = rows_in - df.count()  # 去重丢弃数 = 读入行数 - 保留行数

    # 补缺：null 或空白字符串 → default（对齐 Python `v is None or
    # str(v).strip() == ""`；旧实现 fillna 只填 null，parquet/cluster 模式下
    # 字符串列残留的 "" 不会被替换，与 Python 分歧）。
    for col, default in fill.items():
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(
                    F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == ""),
                    F.lit(str(default)),
                ).otherwise(F.col(col)),
            )

    # total_amount = round(quantity * unit_price * (1 - discount), 2)
    # 三引擎统一：discount 列存在时扣减折扣，解析失败/null 按 0 处理
    # （coalesce 对齐 python as_float(...) or 0.0；旧实现直接
    # `1 - cast(discount)`，非法 discount 会产出 null total_amount）。
    # 用 try_cast 而非 cast：Spark 4.x 默认 ANSI 开启，cast 对非法值
    # （如 "abc"）抛 CAST_INVALID_INPUT 而不是返回 null，只有 try_cast 在
    # ANSI 开/关下都保证"解析失败 → null → coalesce 0"的 python 语义。
    # quantity/unit_price 同样用 try_cast + coalesce 0：与 python 路径
    # `as_int(...) or 0` / polars 路径 `cast(strict=False).fill_null(0.0)`
    # 对齐。shipped config 下 validate 的 completeness+range 规则保证这两列
    # 必为合法数值，try_cast 不会命中失败分支；但用户自定义 config 若未配
    # 相应规则，旧实现的裸 cast 会让 null 传播为 null total_amount，与
    # python/polars 的 0.0 语义分歧（2026-08 审查 B3 残留项）。
    qty = F.coalesce(F.col("quantity").try_cast("double"), F.lit(0.0))
    price = F.coalesce(F.col("unit_price").try_cast("double"), F.lit(0.0))
    if "discount" in df.columns:
        disc = F.coalesce(F.col("discount").try_cast("double"), F.lit(0.0))
        amt_expr = F.round(qty * price * (F.lit(1.0) - disc), 2)
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

    return dispatch_by_engine(
        ctx.engine_backend, _run_python, _run_polars, _run_spark, ctx, log, cl_dir
    )


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
        if _table_exists(src, ctx.config):
            if _get_storage_backend(ctx.config) != "local_csv":
                ref_df = table_read(src, ctx.config)
            else:
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
