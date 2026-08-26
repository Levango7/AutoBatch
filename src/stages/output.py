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

import hashlib
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
from ._dispatch import dispatch_by_engine


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


def _rel_to_root(p: str) -> str:
    """计算 p 相对 ROOT 的路径并归一为 "/" 分隔.

    Windows 上跨盘符调用 os.path.relpath 抛 ValueError，此时回退为绝对路径
    （同样 "/" 归一）。_register_artifacts 与 _register_edges 必须使用同一个
    回退逻辑，保证回退后血缘边前缀匹配与 artifact key 命名空间仍然一致.
    """
    try:
        return os.path.relpath(p, ROOT).replace("\\", "/")
    except ValueError:
        return os.path.abspath(p).replace("\\", "/")


def _dir_part_files(dirpath: str) -> list[str]:
    """收集目录型产物下的数据分片文件（相对路径、"/" 分隔、排序）.

    Spark local_csv 后端把 stage 产物写为目录（内含 part-00000-* 分区文件）。
    跳过 "_" / "." 开头的元数据文件（_SUCCESS、.crc 等）。返回排序后的相对
    路径列表，保证 digest 计算的确定性.
    """
    parts: list[str] = []
    for root, _dirs, files in os.walk(dirpath):
        for fname in files:
            if fname.startswith(("_", ".")):
                continue
            rel = os.path.relpath(os.path.join(root, fname), dirpath)
            parts.append(rel.replace("\\", "/"))
    return sorted(parts)


def _dir_artifact_digest(dirpath: str, parts: list[str]) -> str:
    """目录型产物摘要："dir:" 前缀 + 排序文件清单及各文件内容的联合 sha256.

    单文件 sha256 无法表示目录产物；对（排序文件名 \\0 文件内容 sha256）
    取摘要既保留清单信息又保留内容敏感性。"dir:" 前缀让消费方（dashboard/
    血缘工具）可区分目录型与文件型产物.
    """
    h = hashlib.sha256()
    for rel in parts:
        h.update(rel.encode("utf-8") + b"\0")
        h.update(file_sha256(os.path.join(dirpath, rel)).encode("ascii"))
    return "dir:" + h.hexdigest()


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
            rel = _rel_to_root(fp)
            if os.path.isdir(fp):
                # spark local_csv 后端的 stage 产物是目录（part-00000-* 分区
                # 文件）。旧实现 os.path.isfile 过滤把目录全部跳过 → spark 后端
                # manifest 零产物、血缘边全部被 _register_edges 静默丢弃（M14）。
                # 现在聚合目录内分片文件登记为一个 artifact：行数为各分片之和
                # （Spark CSV 每个分片都带 header，csv_lines 逐文件去头后求和），
                # 摘要用 "dir:" 前缀的联合 sha256。parquet 分片为二进制，
                # rows 置 None（csv_lines 无语义）.
                parts = _dir_part_files(fp)
                rows = None
                if fn.endswith(".csv"):
                    rows = sum(csv_lines(os.path.join(fp, part)) for part in parts)
                manifest.add_artifact(rel, kind, rows, _dir_artifact_digest(fp, parts))
            elif os.path.isfile(fp):
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
    # run_dir 可能被配置改名（缺省 "run"），前缀必须与 _register_artifacts
    # 里 _rel_to_root(fp) 产生的命名空间一致，否则所有血缘边被静默丢弃。
    # 注意 ctx.run_dir 已包含 batch_id（= run_root/<batch_id>），不可再拼接。
    # _rel_to_root 内含跨盘符 ValueError 回退（与 artifact key 同一回退逻辑）。
    run_dir_rel = _rel_to_root(ctx.run_dir).strip("/")
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
            fp = os.path.join(agg_dir, fn)
            # 目录也算聚合产物：spark local_csv 后端把聚合结果写为目录
            # （part-* 分区文件），与 _register_artifacts 的目录登记口径一致，
            # 否则 spark 后端 dashboard_data.json 的血缘边丢失.
            if os.path.isfile(fp) or os.path.isdir(fp):
                agg_upstreams.append("04_aggregates/" + fn)
    if agg_upstreams:
        lineage["05_output/dashboard_data.json"] = agg_upstreams
    return lineage


def run(ctx: PipelineContext, log) -> dict[str, Any]:
    out_dir = os.path.join(ctx.run_dir, "05_output")
    os.makedirs(out_dir, exist_ok=True)

    rows_in, rows_out = dispatch_by_engine(
        ctx.engine_backend,
        _write_orders_final_python,
        _write_orders_final_polars,
        _write_orders_final_spark,
        ctx,
        out_dir,
    )

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


def _resolve_orders_source_rel(ctx: PipelineContext) -> str:
    """推导 orders_final 的 _source_file 标记列（实际读取的上游路径）.

    历史实现硬编码 "data/raw/orders.csv"，与 ingest 阶段实际拷贝的源路径
    （可能是绝对路径/其他盘符目录，增量模式还可能是 orders_incremental.csv）
    不符。现从 ctx.ingested 中 name="orders" 条目的 copied_to 推导并归一
    "/" 分隔；条目缺失时（如 output 阶段被单独调用的测试场景）回退到历史
    常量，保证兼容。copied_to 由 ingest 相对 ROOT 计算，跨盘时 ingest 侧已
    有自身处理，这里仅做分隔符归一与防御.
    """
    for finfo in ctx.ingested:
        if finfo.get("name") == "orders":
            rel = str(finfo.get("copied_to") or "").replace("\\", "/")
            if rel:
                return rel
    return "data/raw/orders.csv"


def _write_orders_final_python(ctx: PipelineContext, out_dir: str) -> tuple[int, int]:
    """Python 路径：table_read → 加标记列 → table_write.

    storage.backend="local_csv" 时 table_read/table_write 等价于 load_csv/csv_write，
    行为与 Phase 1 完全一致。storage.backend="parquet" 时读写 Parquet（本地或 S3）。
    """
    cfg = ctx.config
    src = os.path.join(ctx.run_dir, "03_clean", "orders_clean.csv")
    orders, fields = table_read(src, cfg)
    marker_fields = fields + ["_batch_id", "_source_file"]
    source_rel = _resolve_orders_source_rel(ctx)
    for r in orders:
        r["_batch_id"] = ctx.batch_id
        r["_source_file"] = source_rel
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
        pl.lit(_resolve_orders_source_rel(ctx)).alias("_source_file"),
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
        "_source_file", F.lit(_resolve_orders_source_rel(ctx))
    )
    table_write(
        os.path.join(out_dir, "orders_final.csv"),
        df,
        ctx.config,
        spark=ctx.spark_session,
    )

    return rows_in, df.count()
