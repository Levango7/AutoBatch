"""Stage 5: final artifacts, dashboard data, manifest registration, run summary.

Phase 2a 增加 Polars 列式分支（``ctx.engine_backend == "polars"``）：
- ``load_csv`` 替换为 ``pl.read_csv(infer_schema_length=0)``（保留 Utf8）
- 写出用 ``table_write``
- ``dashboard_data.json`` 生成逻辑不变（从 ``ctx.aggregates`` 读，已是 List[Dict]）

Phase 2b 增加 Spark 分布式分支（``ctx.engine_backend == "spark"``）：
- 读入用 ``table_read(path, cfg, spark=ctx.spark_session)`` 返回 SparkDataFrame
- 写出用 ``table_write(path, df, cfg, spark=ctx.spark_session)`` 路由到
  ``df.write.mode("overwrite").csv/parquet``
- ``orders_final.csv`` 加标记列用 ``df.withColumn``（Spark DataFrame API）
- ``dashboard_data.json`` 生成逻辑不变（从 ``ctx.aggregates`` 读，已是 List[Dict]，
  Spark 路径下 compute stage 已用 ``df.toPandas().to_dict()`` 收集到 driver）
- manifest/血缘：与现有路径一致（manifest 是 Python dict，不依赖引擎）

向后兼容：``engine.backend="python"/"polars"`` 时行为不变。
参见 docs/evolution.md §4.3.1.2 / §4.3.2.2 / §4.4.2.1。
"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any

from ..helpers import (
    ROOT,
    PipelineContext,
    _get_storage_backend,
    csv_lines,
    file_sha256,
    json_save,
    table_read,
    table_write,
    utc_ts,
)


def _jsonify(value: Any) -> Any:
    """递归把 date/datetime 转 ISO 字符串，保证 dashboard JSON 可序列化.

    Spark 路径下 parquet inferSchema 会把 order_date 读成 datetime.date，
    直接 json.dump 抛 TypeError（python/polars 路径全为字符串故从未暴露，
    2026-08 亿行基准实测发现）。
    """
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    return value


def _register_artifacts(ctx: PipelineContext) -> None:
    manifest = ctx.manifest
    kind_map = {
        "01_raw": "raw",
        "02_valid": "valid",
        "03_clean": "clean",
        "04_aggregates": "aggregate",
        "05_output": "output",
        "quarantine": "quarantine",
        "report": "report",
    }
    for dirname, kind in kind_map.items():
        base = os.path.join(ctx.run_dir, dirname)
        if not os.path.isdir(base):
            continue
        for fn in sorted(os.listdir(base)):
            fp = os.path.join(base, fn)
            if not os.path.isfile(fp):
                continue
            rel = os.path.relpath(fp, ROOT).replace("\\", "/")
            rows = csv_lines(fp) if fn.endswith(".csv") else None
            manifest.add_artifact(rel, kind, rows, file_sha256(fp))


def _register_edges(ctx: PipelineContext) -> int:
    """Auto-build lineage edges from per-stage declarations.

    Each upstream stage populated ``ctx.lineage_decls`` with mappings of the form
    ``{target_rel: [upstream_rel, ...]}`` where paths are relative to ``run_dir``.
    We convert them to the manifest's absolute-relpath namespace
    (``run/<batch_id>/<rel>``), drop targets/upstreams that were never materialised
    as artifacts, and register the surviving edges. Returns the edge count.
    """
    manifest = ctx.manifest
    # run_dir 可能被配置改名（缺省 "run"），前缀必须与 _register_stage_artifacts
    # 里 os.path.relpath(fp, ROOT) 产生的命名空间一致，否则所有血缘边被静默丢弃。
    # 注意 ctx.run_dir 已包含 batch_id（= run_root/<batch_id>），不可再拼接。
    run_dir_rel = os.path.relpath(ctx.run_dir, ROOT).replace("\\", "/").strip("/")
    prefix = f"{run_dir_rel}/"
    count = 0
    for target_rel, upstream_rels in ctx.lineage_decls.items():
        target = prefix + target_rel
        if target not in manifest.artifacts:
            continue
        surviving = [prefix + up for up in upstream_rels if (prefix + up) in manifest.artifacts]
        if not surviving:
            # No materialised upstreams (e.g. a root product) -> no edge.
            continue
        manifest.add_edge(target, surviving)
        count += 1
    return count


def _build_dashboard(ctx: PipelineContext) -> dict[str, Any]:
    """dashboard_data.json 内容（与 backend 无关，从 ctx.aggregates 读）."""
    agg = ctx.aggregates
    return {
        "batch_id": ctx.batch_id,
        "pipeline": ctx.config["pipeline"],
        "generated_at": utc_ts(),
        "kpi": agg["kpi"],
        "daily": agg["daily"],
        "category": agg["category"],
        "region_channel": agg["region_channel"],
        "customer_value": agg["customer_value"]["top"],
        "tiers": agg["customer_value"]["tiers"],
        "quality": ctx.manifest.quality,
        "stages": ctx.manifest.stages,
        "source": ctx.manifest.source,
    }


def _declare_lineage(ctx: PipelineContext) -> dict[str, list]:
    """声明 output 阶段产物的 lineage（与 backend 无关）."""
    lineage: dict[str, list] = {
        "05_output/orders_final.csv": ["03_clean/orders_clean.csv"],
    }
    agg_dir = os.path.join(ctx.run_dir, "04_aggregates")
    agg_upstreams = []
    if os.path.isdir(agg_dir):
        for fn in sorted(os.listdir(agg_dir)):
            if os.path.isfile(os.path.join(agg_dir, fn)):
                agg_upstreams.append("04_aggregates/" + fn)
    if agg_upstreams:
        lineage["05_output/dashboard_data.json"] = agg_upstreams
    return lineage


def run(ctx: PipelineContext, log) -> dict[str, Any]:
    out_dir = os.path.join(ctx.run_dir, "05_output")
    os.makedirs(out_dir, exist_ok=True)

    if ctx.engine_backend == "spark":
        rows_in, rows_out = _write_orders_final_spark(ctx, out_dir)
    elif ctx.engine_backend == "polars":
        rows_in, rows_out = _write_orders_final_polars(ctx, out_dir)
    else:
        rows_in, rows_out = _write_orders_final_python(ctx, out_dir)

    dashboard_data = _jsonify(_build_dashboard(ctx))
    json_save(os.path.join(out_dir, "dashboard_data.json"), dashboard_data)

    lineage = _declare_lineage(ctx)

    _register_artifacts(ctx)
    # Merge this stage's own declarations with those collected from upstreams.
    for k, v in lineage.items():
        ctx.lineage_decls.setdefault(k, list(v))
    edge_count = _register_edges(ctx)
    manifest_path = ctx.manifest.save()
    log.info(
        "output done",
        artifacts=len(ctx.manifest.artifacts),
        lineage_edges=edge_count,
        manifest=manifest_path,
    )
    return {"rows_in": rows_in, "rows_out": rows_out, "lineage": lineage}


def _write_orders_final_python(ctx: PipelineContext, out_dir: str) -> tuple[int, int]:
    """Python 路径：table_read → 加标记列 → table_write.

    storage.backend="local_csv" 时 table_read/table_write 等价于 load_csv/csv_write，
    行为与 Phase 1 完全一致。storage.backend="parquet" 时读写 Parquet（本地或 S3）。
    """
    cfg = ctx.config
    src = os.path.join(ctx.run_dir, "03_clean", "orders_clean.csv")
    orders, fields = table_read(src, cfg)
    marker_fields = fields + ["_batch_id", "_source_file"]
    for r in orders:
        r["_batch_id"] = ctx.batch_id
        r["_source_file"] = "data/raw/orders.csv"
    table_write(os.path.join(out_dir, "orders_final.csv"), orders, cfg, fields=marker_fields)
    return len(orders), len(orders)


def _write_orders_final_polars(ctx: PipelineContext, out_dir: str) -> tuple[int, int]:
    """Polars 路径：读 03_clean → 加标记列 → table_write.

    local_csv 下用 ``pl.read_csv(infer_schema_length=0)`` 保留所有列为 Utf8；
    parquet/iceberg 下读 .csv.parquet（上游 pyarrow 写全 String，读回保持
    Utf8 与 Python 路径一致）。保证写出产物与 Python 路径逐字段一致。
    """
    import polars as pl  # lazy import

    from ..helpers import table_read, table_write

    src = os.path.join(ctx.run_dir, "03_clean", "orders_clean.csv")
    if _get_storage_backend(ctx.config) != "local_csv":
        df = table_read(src, ctx.config)
    else:
        df = pl.read_csv(src, infer_schema_length=0)
    rows_in = df.height
    # 加标记列（与 Python 路径一致：_batch_id, _source_file）
    df = df.with_columns(
        pl.lit(ctx.batch_id).alias("_batch_id"),
        pl.lit("data/raw/orders.csv").alias("_source_file"),
    )
    table_write(os.path.join(out_dir, "orders_final.csv"), df, ctx.config)
    return rows_in, df.height


def _write_orders_final_spark(ctx: PipelineContext, out_dir: str) -> tuple[int, int]:
    """Spark 路径：table_read → 加标记列 → table_write.

    读入用 ``table_read``（backend="spark" 下返回 SparkDataFrame），加标记列用
    ``df.withColumn``（Spark DataFrame API），写出用 ``table_write`` 路由到
    ``df.write.mode("overwrite").csv/parquet``。
    """
    from pyspark.sql import functions as F  # lazy import：仅 spark 路径需要

    from ..helpers import table_read, table_write

    src = os.path.join(ctx.run_dir, "03_clean", "orders_clean.csv")
    df = table_read(src, ctx.config, spark=ctx.spark_session)
    rows_in = df.count()  # 触发 action 取行数

    # 加标记列（与 Python/Polars 路径一致：_batch_id, _source_file）
    df = df.withColumn("_batch_id", F.lit(ctx.batch_id)).withColumn(
        "_source_file", F.lit("data/raw/orders.csv")
    )
    table_write(
        os.path.join(out_dir, "orders_final.csv"),
        df,
        ctx.config,
        spark=ctx.spark_session,
    )

    return rows_in, df.count()
