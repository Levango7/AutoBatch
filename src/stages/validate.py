"""Stage 2: quality rule checks; bad rows quarantined with reason code.

Full mode (default): load every ingested table, run all configured rules row
by row, write ``02_valid/valid_<name>.csv`` for good rows and
``quarantine/quarantine_<name>.csv`` for bad rows. DQ Score aggregates the
checks across all datasets.

Incremental mode (``ctx.incremental_enabled``): reference tables (customers,
products) are still loaded fully into ``ref_data`` (small and stable). For
each dataset the stage prefers ``01_raw/<name>_incremental.csv`` (the delta
produced by ingest) and falls back to the file recorded in ``finfo`` when the
delta is absent (first run that establishes the watermark, or a
``full_load`` reference table). Only the new rows are validated, so DQ Score
reflects this batch's incremental checks only. ``quality_summary`` is tagged
with ``"mode": "incremental"``. See docs/evolution.md §3.3.3.
"""

from __future__ import annotations

import os
from typing import Any, Callable

from ..helpers import (
    PipelineContext,
    _table_exists,
    abs_path,
    as_float,
    as_int,
    json_save,
    load_csv,
    table_read,
    table_write,
)
from ..quality import RuleEngine, quality_summary, render_markdown_report


def _derive_amount(row: dict[str, str]) -> None:
    """Add derived total_amount column so statistical rules can run on raw rows."""
    qty = as_int(row.get("quantity"))
    price = as_float(row.get("unit_price"))
    if qty is not None and price is not None:
        row["total_amount"] = str(round(qty * price, 2))
    else:
        row["total_amount"] = ""


def _select_source_path(ctx: PipelineContext, raw_dir: str, finfo: dict[str, Any]) -> str:
    """Pick the CSV file to validate for this dataset.

    Full mode: use the file ingest copied (always the full table).

    Incremental mode: prefer ``01_raw/<name>_incremental.csv`` when ingest
    produced a delta file; fall back to the file recorded in ``finfo`` (the
    full table copied on the first run to establish the watermark, or a
    ``full_load`` reference table such as products). This keeps the first
    incremental run (full-load-to-establish-watermark) and subsequent delta
    runs both working correctly.
    """
    if ctx.incremental_enabled:
        inc_path = os.path.join(raw_dir, "{}_incremental.csv".format(finfo["name"]))
        if _table_exists(inc_path, ctx.config):
            return inc_path
    return abs_path(finfo["copied_to"])


def _emit_valid_quarantine(
    ctx: PipelineContext,
    name: str,
    good_count: int,
    bad_count: int,
    log,
    write_valid: Callable[..., Any],
    write_quarantine: Callable[..., Any],
) -> tuple[bool, bool]:
    """三引擎共用：处理 valid / quarantine 文件写出决策 + log warn.

    全量模式：仅当 good_count > 0 时写 valid_<name>.csv（保留原 skip-when-empty 行为）；
    增量模式：总是写 valid 文件（即使 0 行 good），保证下游 stage 可依赖文件存在.
    quarantine_<name>.csv 仅在 bad_count > 0 时写，并 log.warn.

    Args:
        ctx:              PipelineContext（用 ctx.incremental_enabled 判断模式）.
        name:             数据集名（log.warn 用）.
        good_count:       good 行数（spark=df.count(), polars=df.height, python=len(good)）.
        bad_count:        bad 行数.
        log:              StageLog.
        write_valid:      无参 callable，写出 valid 文件（各引擎封装自己的 table_write 调用）.
        write_quarantine: 无参 callable，写出 quarantine 文件.

    Returns:
        (emitted_valid, emitted_quarantine) 是否写出了 valid / quarantine.
    """
    emitted_valid = False
    if good_count > 0 or ctx.incremental_enabled:
        write_valid()
        emitted_valid = True
    emitted_quarantine = False
    if bad_count > 0:
        write_quarantine()
        emitted_quarantine = True
        log.warn("quarantined", dataset=name, count=bad_count)
    return emitted_valid, emitted_quarantine


def run(ctx: PipelineContext, log) -> dict[str, Any]:
    cfg = ctx.config
    rules = cfg.get("quality", {}).get("rules", {})
    raw_dir = os.path.join(ctx.run_dir, "01_raw")
    val_dir = os.path.join(ctx.run_dir, "02_valid")
    qu_dir = os.path.join(ctx.run_dir, "quarantine")
    os.makedirs(val_dir, exist_ok=True)
    os.makedirs(qu_dir, exist_ok=True)

    # Reference tables are always loaded fully: they are small and stable,
    # and referential-integrity checks need the complete key space. This is
    # unchanged from the full-mode behaviour (docs/evolution.md §3.3.3).
    # ref_data 始终为 List[Dict] 格式（RuleEngine._ref_keys / check_polars
    # 的 referential 均以此格式提取 key 集合），与 engine.backend 无关。
    # Phase 4: storage.backend="iceberg" 时源参考表仍是 CSV（generator 生成），
    # 用 load_csv 读；中间产物通过 table_read 自动路由（path 是文件路径时
    # 回退到 local_csv，path 是 Iceberg 表名时走 iceberg 分支）.
    ref_data = {}
    for name in ("customers", "products"):
        rel = cfg["source"]["files"].get(name)
        if rel:
            rows, _ = load_csv(abs_path(rel))
            ref_data[name] = rows

    stats_by_dataset = {}
    quarantined = {}
    outlier_keys = set()
    total_in = 0
    total_good = 0
    total_bad = 0

    # Track which products this stage actually emits so we can declare lineage.
    produced_valid: list = []
    produced_quarantine: list = []
    # Map dataset name -> source path relative to run_dir (for lineage edges).
    # In full mode this is always "01_raw/<name>.csv"; in incremental mode it
    # may be "01_raw/<name>_incremental.csv" for delta datasets.
    src_rel_by_name: dict[str, str] = {}

    # 引擎特定逻辑——非函数级 dispatch：for 循环内三分支，分支体各 30-60 行
    # 且共享循环迭代变量（finfo/name/path）、累积器（total_in/total_good/
    # outlier_keys/produced_*）与后续逻辑（_emit_valid_quarantine/produced_*.append），
    # 提取为三个独立函数会割裂共享状态，保留内联三分支。
    is_polars = ctx.engine_backend == "polars"
    is_spark = ctx.engine_backend == "spark"

    for finfo in ctx.ingested:
        name = finfo["name"]
        if name not in rules:
            log.warn("no rules for dataset", dataset=name)
            continue
        path = _select_source_path(ctx, raw_dir, finfo)
        # 血缘/manifest 声明统一用 "/" 分隔：Windows 上 os.path.relpath 产生
        # 反斜杠路径，而 output._register_edges 用 "/" 规范化的 artifact key
        # 匹配，不归一的话 validate 阶段的全部血缘边在 Windows 上被静默丢弃.
        src_rel_by_name[name] = os.path.relpath(path, ctx.run_dir).replace("\\", "/")

        if is_spark:
            from pyspark.sql import functions as F

            spark = ctx.spark_session
            df = table_read(path, cfg, spark=spark)
            total_in += df.count()
            # _derive_amount 的 spark 等价：total_amount = str(round(qty*price, 2))
            # 与 polars 路径一致：cast double、round 2、cast string
            if "quantity" in df.columns and "unit_price" in df.columns:
                qty = F.col("quantity").cast("double")
                price = F.col("unit_price").cast("double")
                amt = F.round(qty * price, 2).cast("string")
                df = df.withColumn("total_amount", amt)
            else:
                df = df.withColumn("total_amount", F.lit(""))
            engine = RuleEngine(name, rules[name], ref_data)
            good_df, bad_df, stats, outlier_indices = engine.check(df=df, spark=spark)
            stats_by_dataset[name] = stats
            # 缓存 count 结果避免重复 action
            good_count = good_df.count()
            bad_count = bad_df.count()

            quarantined[name] = bad_count
            total_good += good_count
            total_bad += bad_count
            if outlier_indices:
                # outlier(action=flag) 只标记不拒收。bounds 用分布式
                # approxQuantile 求 Q1/Q3（driver 仅收 2 个标量），再只 collect
                # 越界行的 order_id（约 1% 行）——旧实现把 (order_id, 值) 两列
                # 全表 collect 到 driver 算精确 IQR，千万行级即 OOM
                # （2026-08 亿行基准实测）。近似分位数 ε=0.001 与精确 IQR 在
                # 大表上的标记差异可忽略，且 flag 结果不影响任何聚合值.
                oc = rules.get(name, {}).get("outlier") or {}
                oc_col = oc.get("column")
                if (
                    oc.get("action") == "flag"
                    and oc_col
                    and oc_col in df.columns
                    and "order_id" in df.columns
                ):
                    factor = float(oc.get("factor", 1.5))
                    # total_amount 等派生列在 Spark 路径为 StringType，须先落
                    # double 派生列再求分位数（approxQuantile 不收字符串列）
                    dfn = df.withColumn("_oc_num", F.col(oc_col).cast("double"))
                    qs = dfn.approxQuantile("_oc_num", [0.25, 0.75], 0.001)
                    if qs and all(q is not None for q in qs):
                        iqr = qs[1] - qs[0]
                        lo, hi = qs[0] - factor * iqr, qs[1] + factor * iqr
                        out_rows = (
                            dfn.select(F.col("order_id"), F.col("_oc_num").alias("_v"))
                            .where(
                                ((F.col("_v") < lo) | (F.col("_v") > hi))
                                & F.col("order_id").isNotNull()
                            )
                            .collect()
                        )
                        for r in out_rows:
                            if r["order_id"]:
                                outlier_keys.add(r["order_id"])
            emitted_v, emitted_q = _emit_valid_quarantine(
                ctx,
                name,
                good_count,
                bad_count,
                log,
                write_valid=lambda name=name, good_df=good_df, spark=spark: table_write(
                    os.path.join(val_dir, "valid_" + name + ".csv"), good_df, cfg, spark=spark
                ),
                write_quarantine=lambda name=name, bad_df=bad_df, spark=spark: table_write(
                    os.path.join(qu_dir, "quarantine_" + name + ".csv"), bad_df, cfg, spark=spark
                ),
            )
            if emitted_v:
                produced_valid.append(name)
            if emitted_q:
                produced_quarantine.append(name)
        elif is_polars:
            import polars as pl

            df = table_read(path, cfg)
            total_in += df.height
            # _derive_amount 的 polars 等价：total_amount = str(round(qty*price, 2)) 或 ""
            if "quantity" in df.columns and "unit_price" in df.columns:
                pl_qty = (
                    pl.col("quantity")
                    .cast(pl.Utf8)
                    .str.replace_all(",", "")
                    .cast(pl.Int64, strict=False)
                )
                pl_price = (
                    pl.col("unit_price")
                    .cast(pl.Utf8)
                    .str.replace_all(",", "")
                    .cast(pl.Float64, strict=False)
                )
                pl_amt = pl_qty * pl_price
                df = df.with_columns(
                    pl_amt.round(2).cast(pl.Utf8).fill_null("").alias("total_amount")
                )
            else:
                df = df.with_columns(pl.lit("").alias("total_amount"))
            engine = RuleEngine(name, rules[name], ref_data)
            good_df, bad_df, stats, outlier_indices = engine.check(df=df)
            stats_by_dataset[name] = stats
            quarantined[name] = bad_df.height
            total_good += good_df.height
            total_bad += bad_df.height
            if outlier_indices:
                order_ids = df.select(pl.col("order_id").cast(pl.Utf8)).to_series().to_list()
                for i in outlier_indices:
                    oid = order_ids[i] if i < len(order_ids) else None
                    if oid:
                        outlier_keys.add(oid)
            emitted_v, emitted_q = _emit_valid_quarantine(
                ctx,
                name,
                good_df.height,
                bad_df.height,
                log,
                write_valid=lambda name=name, good_df=good_df: table_write(
                    os.path.join(val_dir, "valid_" + name + ".csv"), good_df, cfg
                ),
                write_quarantine=lambda name=name, bad_df=bad_df: table_write(
                    os.path.join(qu_dir, "quarantine_" + name + ".csv"), bad_df, cfg
                ),
            )
            if emitted_v:
                produced_valid.append(name)
            if emitted_q:
                produced_quarantine.append(name)
        else:
            rows, fields = table_read(path, cfg)
            total_in += len(rows)
            for row in rows:
                _derive_amount(row)
            engine = RuleEngine(name, rules[name], ref_data)
            good, bad, stats, outlier_indices = engine.check(rows=rows)
            stats_by_dataset[name] = stats
            quarantined[name] = len(bad)
            total_good += len(good)
            total_bad += len(bad)
            for i in outlier_indices:
                oid = rows[i].get("order_id")
                if oid:
                    outlier_keys.add(oid)
            bad_fields = fields + ["_line", "_reasons"]
            emitted_v, emitted_q = _emit_valid_quarantine(
                ctx,
                name,
                len(good),
                len(bad),
                log,
                write_valid=lambda name=name, good=good, fields=fields: table_write(
                    os.path.join(val_dir, "valid_" + name + ".csv"), good, cfg, fields=fields
                ),
                write_quarantine=lambda name=name, bad=bad, bad_fields=bad_fields: table_write(
                    os.path.join(qu_dir, "quarantine_" + name + ".csv"), bad, cfg, fields=bad_fields
                ),
            )
            if emitted_v:
                produced_valid.append(name)
            if emitted_q:
                produced_quarantine.append(name)

    summary = quality_summary(stats_by_dataset, quarantined)
    # Tag the run mode so consumers can distinguish full vs incremental DQ
    # scores (incremental score only covers this batch's new rows).
    summary["mode"] = "incremental" if ctx.incremental_enabled else "full"
    ctx.outlier_keys = outlier_keys
    ctx.manifest.set_quality(summary)
    json_save(os.path.join(ctx.run_dir, "quality_summary.json"), summary)
    report_dir = os.path.join(ctx.run_dir, "report")
    os.makedirs(report_dir, exist_ok=True)
    with open(os.path.join(report_dir, "quality_report.md"), "w", encoding="utf-8") as f:
        f.write(render_markdown_report(summary))
    json_save(os.path.join(report_dir, "quality_report.json"), summary)
    log.info(
        "quality done", dq_score=summary["dq_score"], quarantined=total_bad, mode=summary["mode"]
    )

    # Declare lineage for products of this stage (paths relative to run_dir).
    lineage: dict[str, list] = {}
    for name in produced_valid:
        lineage[f"02_valid/valid_{name}.csv"] = [src_rel_by_name[name]]
    for name in produced_quarantine:
        lineage[f"quarantine/quarantine_{name}.csv"] = [src_rel_by_name[name]]
    # Quality reports aggregate stats over every validated dataset, so their
    # upstreams are all valid products emitted above.
    valid_upstreams = [f"02_valid/valid_{n}.csv" for n in produced_valid]
    if valid_upstreams:
        lineage["report/quality_report.md"] = list(valid_upstreams)
        lineage["report/quality_report.json"] = list(valid_upstreams)

    return {
        "rows_in": total_in,
        "rows_out": total_good,
        "quarantined": total_bad,
        "lineage": lineage,
    }
