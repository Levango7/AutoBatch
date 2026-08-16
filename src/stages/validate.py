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
from typing import Any

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


def _select_source_path(ctx: PipelineContext, raw_dir: str,
                        finfo: dict[str, Any]) -> str:
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
    # ref_data 始终为 List[Dict] 格式（RuleEngine._ref_keys / _check_polars
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

    is_polars = (ctx.engine_backend == "polars")
    is_spark = (ctx.engine_backend == "spark")

    for finfo in ctx.ingested:
        name = finfo["name"]
        if name not in rules:
            log.warn("no rules for dataset", dataset=name)
            continue
        path = _select_source_path(ctx, raw_dir, finfo)
        src_rel_by_name[name] = os.path.relpath(path, ctx.run_dir)

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
                # 用 collect 取 order_id 列表，按 outlier_indices 取对应 order_id
                # 注：Spark collect 顺序不确定，outlier_keys 集合内容正确
                # （outlier 行的 order_id 集合是确定的，只是顺序不同）
                order_ids = [row["order_id"] for row in df.select("order_id").collect()]
                for i in outlier_indices:
                    oid = order_ids[i] if i < len(order_ids) else None
                    if oid:
                        outlier_keys.add(oid)
            if good_count > 0:
                table_write(os.path.join(val_dir, "valid_" + name + ".csv"),
                            good_df, cfg, spark=spark)
                produced_valid.append(name)
            elif ctx.incremental_enabled:
                # Incremental mode: always emit a valid file (even when this batch
                # produced zero good rows) so downstream stages can rely on its
                # presence. Full mode preserves the existing skip-when-empty
                # behaviour exactly.
                table_write(os.path.join(val_dir, "valid_" + name + ".csv"),
                            good_df, cfg, spark=spark)
                produced_valid.append(name)
            if bad_count > 0:
                table_write(os.path.join(qu_dir, "quarantine_" + name + ".csv"),
                            bad_df, cfg, spark=spark)
                produced_quarantine.append(name)
                log.warn("quarantined", dataset=name, count=bad_count)
        elif is_polars:
            import polars as pl
            df = table_read(path, cfg)
            total_in += df.height
            # _derive_amount 的 polars 等价：total_amount = str(round(qty*price, 2)) 或 ""
            if "quantity" in df.columns and "unit_price" in df.columns:
                qty = pl.col("quantity").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Int64, strict=False)
                price = pl.col("unit_price").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False)
                amt = (qty * price).round(2).cast(pl.Utf8).fill_null("")
                df = df.with_columns(amt.alias("total_amount"))
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
            fields = df.columns
            if good_df.height > 0:
                table_write(os.path.join(val_dir, "valid_" + name + ".csv"), good_df, cfg)
                produced_valid.append(name)
            elif ctx.incremental_enabled:
                # Incremental mode: always emit a valid file (even when this batch
                # produced zero good rows) so downstream stages can rely on its
                # presence. Full mode preserves the existing skip-when-empty
                # behaviour exactly.
                table_write(os.path.join(val_dir, "valid_" + name + ".csv"), good_df, cfg)
                produced_valid.append(name)
            if bad_df.height > 0:
                table_write(os.path.join(qu_dir, "quarantine_" + name + ".csv"), bad_df, cfg)
                produced_quarantine.append(name)
                log.warn("quarantined", dataset=name, count=bad_df.height)
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
            if good:
                table_write(os.path.join(val_dir, "valid_" + name + ".csv"),
                            good, cfg, fields=fields)
                produced_valid.append(name)
            elif ctx.incremental_enabled:
                # Incremental mode: always emit a valid file (even when this batch
                # produced zero good rows) so downstream stages can rely on its
                # presence. Full mode preserves the existing skip-when-empty
                # behaviour exactly.
                table_write(os.path.join(val_dir, "valid_" + name + ".csv"),
                            [], cfg, fields=fields)
                produced_valid.append(name)
            if bad:
                bad_fields = fields + ["_line", "_reasons"]
                table_write(os.path.join(qu_dir, "quarantine_" + name + ".csv"),
                            bad, cfg, fields=bad_fields)
                produced_quarantine.append(name)
                log.warn("quarantined", dataset=name, count=len(bad))

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
    log.info("quality done", dq_score=summary["dq_score"], quarantined=total_bad,
             mode=summary["mode"])

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

    return {"rows_in": total_in, "rows_out": total_good,
            "quarantined": total_bad, "lineage": lineage}
