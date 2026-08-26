"""Stage 1: ingest source files into the run dir.

Full-load mode (default): byte-identical copy of each source file to
``01_raw/{name}.csv`` plus a sha256 hash.

Incremental mode (``incremental.enabled: true``): for tables with a
``watermark_type`` other than ``full_load`` the stage reads the staged
watermark from ``ctx.state`` and either

* performs a one-off full load to ``01_raw/{name}.csv`` and records the
  ``max(watermark_column)`` as the initial watermark (first run), or
* streams the source and writes only rows with ``watermark_column >
  watermark_value`` to ``01_raw/{name}_incremental.csv`` (subsequent runs).

The new watermark is staged into ``ctx.state["tables"][name]["new_watermark"]``
in memory only; ``pipeline.py`` commits it after every stage succeeds. Tables
marked ``watermark_type: full_load`` (e.g. products) are always fully copied.
See docs/evolution.md §3.3.2.
"""

from __future__ import annotations

import csv
import os
from typing import Any, Optional

from ..helpers import (
    ROOT,
    PipelineContext,
    _get_storage_backend,
    _strip_bom_polars,
    _strip_bom_spark,
    abs_path,
    copy_file,
    csv_lines,
    csv_read,
    csv_write,
    iceberg_snapshot_diff,
    sha256_of,
    table_write,
)


def run(ctx: PipelineContext, log) -> dict[str, Any]:
    cfg = ctx.config
    source_cfg = cfg.get("source", {})
    files_cfg = source_cfg.get("files", {})
    inc_cfg = cfg.get("incremental", {})
    tables_cfg = inc_cfg.get("tables", {})
    raw_dir = os.path.join(ctx.run_dir, "01_raw")
    os.makedirs(raw_dir, exist_ok=True)
    source_files = []

    for name, rel in files_cfg.items():
        src = abs_path(rel)
        table_cfg = tables_cfg.get(name, {})
        wm_type = table_cfg.get("watermark_type", "full_load")

        if ctx.incremental_enabled and wm_type != "full_load":
            entry = _ingest_incremental(ctx, name, rel, src, raw_dir, table_cfg, log)
        else:
            entry = _ingest_full(
                ctx, name, rel, src, raw_dir, log, incremental=ctx.incremental_enabled
            )
        source_files.append(entry)

    ctx.manifest.set_source(source_cfg.get("name", "unknown"), source_files)
    ctx.ingested = source_files
    # Ingest is the lineage source: 01_raw/* have no upstream products.
    return {"rows_in": 0, "rows_out": sum(f["rows"] for f in source_files), "lineage": {}}


# ----------------------------------------------------------------------
# full-load path (existing behaviour, used when incremental is off or the
# table is marked full_load)
# ----------------------------------------------------------------------
def _ingest_full(
    ctx: PipelineContext, name: str, rel: str, src: str, raw_dir: str, log, incremental: bool
) -> dict[str, Any]:
    """全量拷贝源文件到 01_raw.

    backend="python"/"polars"：byte-identical copy（copy_file + csv_lines），
    行为与 Phase 1 完全一致。

    backend="spark"：用 table_read 读源 CSV 为 SparkDataFrame，table_write 写到
    01_raw/{name}.csv（Spark 写出的是目录，多分区 part-00000-* 文件）。
    参见 docs/evolution.md §4.3.2.4 / §4.4.2.1。

    storage.backend="parquet"（python 路径）：用 load_csv 读源 CSV 为 List[Dict]，
    table_write 写到 01_raw/{name}.csv（实际 01_raw/{name}.csv.parquet），
    获得列式压缩。sha256 用文件内容计算（本地）或 "parquet" 占位（S3）。
    """
    if ctx.engine_backend == "spark":
        # 引擎特定逻辑——非函数级三分支 dispatch（仅 spark early return，
        # python/polars 共用下方 copy_file/csv_read 路径），保留内联。
        return _ingest_full_spark(ctx, name, rel, src, raw_dir, log, incremental)
    cfg = ctx.config
    dst = os.path.join(raw_dir, os.path.basename(rel))
    if _get_storage_backend(cfg) == "parquet":
        # parquet storage：读源 CSV → table_write 写 parquet（本地 .csv.parquet 或 S3）
        rows, fields = csv_read(src)
        table_write(dst, rows, cfg, fields=fields)
        sha = sha256_of(dst) if os.path.exists(dst) else "parquet"
        rows_count = len(rows)
    else:
        sha = copy_file(src, dst)
        rows_count = csv_lines(dst)
    entry = {
        "name": name,
        "path": rel,
        "local_path": src,
        "copied_to": os.path.relpath(dst, ROOT),
        "sha256": sha,
        "rows": rows_count,
        "incremental": False,
    }
    log.info(
        "ingested",
        source=name,
        rows=rows_count,
        sha256=sha[:12] if isinstance(sha, str) and len(sha) >= 12 else sha,
        dest=os.path.relpath(dst, ROOT),
        mode="incremental_full_load" if incremental else "full",
    )
    return entry


def _ingest_full_spark(
    ctx: PipelineContext, name: str, rel: str, src: str, raw_dir: str, log, incremental: bool
) -> dict[str, Any]:
    """Spark 全量读+写路径.

    单机模式（cluster.enabled=false）：用 ``spark.read.csv(src)`` 直接读源 CSV
    为 SparkDataFrame，``table_write(dst, df, cfg, spark=...)`` 写到 01_raw。

    S3 源（src 为 s3a:// / s3:// URI）：executor 经已注入的 fs.s3a 配置直读
    对象存储，单机/多机同路——多机模式下 Worker 无法访问宿主机路径，把大
    规模源数据预置到 MinIO 后走本分支，避免 Driver 端 csv_read 全量进内存
    （亿行级会 OOM）。

    多机模式 + 本地源（cluster.enabled=true）：Worker 在 Docker 容器中无法
    访问宿主机文件路径，因此 Driver 端先用 ``csv_read(src)`` 读取源 CSV 为
    List[Dict]，再 ``spark.createDataFrame(rows)`` 转为 SparkDataFrame，最后
    ``table_write`` 写到 01_raw（S3/MinIO）。仅适合小规模源数据。

    Spark 写出的是目录（多分区 part-00000-* 文件），sha256 不适用，用
    ``"spark_dir"`` 占位符。rows 由 table_write 返回（内部 df.count()）。
    """
    cfg = ctx.config
    spark = ctx.spark_session
    assert spark is not None
    dst = os.path.join(raw_dir, os.path.basename(rel))
    cluster = cfg.get("engine", {}).get("spark", {}).get("cluster", {})
    if src.startswith(("s3a://", "s3://")):
        # S3 源：executor 直读对象存储（fs.s3a.* 已在 session 构建时注入），
        # 单机/多机同路；Driver 不经手数据面.
        opts = cfg.get("engine", {}).get("spark", {}).get("read_options", {}) or {}
        df = _strip_bom_spark(spark.read.csv(src, header=True, inferSchema=True, **opts))
        rows = table_write(dst, df, cfg, spark=spark)
    elif cluster.get("enabled"):
        # 多机模式：Driver 端读本地 CSV → createDataFrame → table_write
        # Worker 无法访问宿主机文件路径，必须由 Driver 读入数据再分发
        rows_data, fields = csv_read(src)
        # 将 List[Dict] 转为 Spark 可接受的格式：非空值转 string（与 CSV 语义一致），
        # 空字符串 '' 转 None（与单机模式 spark.read.csv inferSchema=True 一致，
        # 避免 Spark 4.x 严格 cast 对 '' 抛 CAST_INVALID_INPUT）。
        str_rows = [
            {f: (v if v != "" else None) for f in fields for v in [str(r.get(f, ""))]}
            for r in rows_data
        ]
        df = spark.createDataFrame(str_rows)
        rows = table_write(dst, df, cfg, spark=spark)
    else:
        # 单机模式：spark.read.csv 直接读源文件
        opts = cfg.get("engine", {}).get("spark", {}).get("read_options", {}) or {}
        df = _strip_bom_spark(spark.read.csv(src, header=True, inferSchema=True, **opts))
        rows = table_write(dst, df, cfg, spark=spark)
    sha = "spark_dir"  # Spark 写出的是目录，sha256 不适用
    entry = {
        "name": name,
        "path": rel,
        "local_path": src,
        "copied_to": os.path.relpath(dst, ROOT),
        "sha256": sha,
        "rows": rows,
        "incremental": False,
    }
    log.info(
        "ingested",
        source=name,
        rows=rows,
        sha256=sha,
        dest=os.path.relpath(dst, ROOT),
        mode="incremental_full_load" if incremental else "full",
        backend="spark",
    )
    return entry


# ----------------------------------------------------------------------
# incremental path
# ----------------------------------------------------------------------
def _ingest_incremental(
    ctx: PipelineContext,
    name: str,
    rel: str,
    src: str,
    raw_dir: str,
    table_cfg: dict[str, Any],
    log,
) -> dict[str, Any]:
    # Phase 4: incremental.mode 路由
    #   "high_watermark"（缺省）→ 现有水位增量逻辑（_copy_incremental）
    #   "iceberg_snapshot_diff"  → Iceberg snapshot diff 增量（_copy_incremental_iceberg）
    inc_mode = ctx.config.get("incremental", {}).get("mode", "high_watermark")
    if inc_mode == "iceberg_snapshot_diff":
        return _copy_incremental_iceberg(ctx, name, raw_dir, table_cfg, log)

    wm_col = table_cfg.get("watermark_column")
    if not wm_col:
        # No watermark column configured: fall back to full load.
        return _ingest_full(ctx, name, rel, src, raw_dir, log, incremental=True)

    wm_value = ctx.state.get("tables", {}).get(name, {}).get("watermark_value")

    if wm_value is None:
        # First run (or state.json absent): full load + establish initial watermark.
        dst = os.path.join(raw_dir, f"{name}.csv")
        if ctx.engine_backend == "spark":
            # 引擎特定逻辑——非函数级 dispatch：first-run 内联三分支，
            # 条件混合 engine_backend 与 _get_storage_backend（parquet），
            # 且分支体含 _stage_new_watermark 等共享后续逻辑，保留内联。
            # Spark 全量读+写，然后从已读入的 DataFrame 算水位
            cfg = ctx.config
            spark = ctx.spark_session
            assert spark is not None
            cluster = cfg.get("engine", {}).get("spark", {}).get("cluster", {})
            if cluster.get("enabled"):
                # 多机模式：Driver 端读本地 CSV → createDataFrame → table_write
                rows_data, fields = csv_read(src)
                str_rows = [
                    {f: (v if v != "" else None) for f in fields for v in [str(r.get(f, ""))]}
                    for r in rows_data
                ]
                df = spark.createDataFrame(str_rows)
            else:
                opts = cfg.get("engine", {}).get("spark", {}).get("read_options", {}) or {}
                df = _strip_bom_spark(spark.read.csv(src, header=True, inferSchema=True, **opts))
            rows = table_write(dst, df, cfg, spark=spark)
            sha = "spark_dir"
            # 直接从已读入的 DataFrame 计算水位，避免二次读取
            # （storage.backend="parquet" 时写出的是 parquet，spark.read.csv 会失败）
            from pyspark.sql import functions as _F

            new_wm_raw = df.agg(_F.max(wm_col).alias("m")).collect()[0]["m"]
            new_wm = str(new_wm_raw) if new_wm_raw is not None else None
        elif _get_storage_backend(ctx.config) == "parquet":
            # parquet storage：读源 CSV → table_write 写 parquet，水位从源 CSV 算
            cfg = ctx.config
            rows_data, fields = csv_read(src)
            rows = table_write(dst, rows_data, cfg, fields=fields)
            sha = sha256_of(dst) if os.path.exists(dst) else "parquet"
            new_wm = _compute_watermark(src, wm_col, backend="python")
        else:
            sha = copy_file(src, dst)
            rows = csv_lines(dst)
            new_wm = _compute_watermark(dst, wm_col, backend=ctx.engine_backend)
        _stage_new_watermark(ctx, name, new_wm, rows)
        entry = {
            "name": name,
            "path": rel,
            "local_path": src,
            "copied_to": os.path.relpath(dst, ROOT),
            "sha256": sha,
            "rows": rows,
            "incremental": True,
            "incremental_mode": "init_full_load",
            "old_watermark": None,
            "new_watermark": new_wm,
        }
        log.info(
            "ingest init full load",
            source=name,
            rows=rows,
            watermark=new_wm,
            sha256=sha[:12] if isinstance(sha, str) and len(sha) >= 12 else sha,
            dest=os.path.relpath(dst, ROOT),
        )
        return entry

    # Subsequent run: stream source, keep rows where watermark > wm_value.
    dst = os.path.join(raw_dir, f"{name}_incremental.csv")
    rows, new_wm, fields = _copy_incremental(
        src, dst, wm_col, wm_value, backend=ctx.engine_backend, ctx=ctx
    )
    # sha256：spark 路径用占位符；parquet 路径下实际文件是 .csv.parquet
    # 引擎特定逻辑——非函数级 dispatch：sha 计算内联三分支，条件混合
    # engine_backend 与 _get_storage_backend（parquet），保留内联。
    if ctx.engine_backend == "spark":
        sha = "spark_dir"
    elif _get_storage_backend(ctx.config) == "parquet" and os.path.exists(dst + ".parquet"):
        sha = sha256_of(dst + ".parquet")
    else:
        sha = sha256_of(dst)
    _stage_new_watermark(ctx, name, new_wm, rows)
    entry = {
        "name": name,
        "path": rel,
        "local_path": src,
        "copied_to": os.path.relpath(dst, ROOT),
        "sha256": sha,
        "rows": rows,
        "incremental": True,
        "incremental_mode": "delta",
        "old_watermark": wm_value,
        "new_watermark": new_wm,
        "fields": fields,
    }
    log.info(
        "ingest incremental",
        source=name,
        rows=rows,
        old_watermark=wm_value,
        new_watermark=new_wm,
        sha256=sha[:12],
        dest=os.path.relpath(dst, ROOT),
    )
    return entry


def _stage_new_watermark(
    ctx: PipelineContext, table: str, value: Optional[str], row_count: int
) -> None:
    """Stage new watermark into ctx.state in memory (mirrors StateStore.set_new_watermark).

    Keeps existing per-table metadata (watermark_column / watermark_type) and
    only adds the ``new_watermark`` staging fields consumed by
    ``StateStore.commit_watermark``.
    """
    tables = ctx.state.setdefault("tables", {})
    info = tables.setdefault(table, {})
    info["new_watermark"] = value
    info["new_seen_row_count"] = row_count
    info["new_batch_id"] = ctx.batch_id


def _copy_incremental(
    src: str,
    dst: str,
    wm_col: str,
    wm_value: str,
    backend: str = "python",
    ctx: Optional[PipelineContext] = None,
) -> tuple[int, Optional[str], list[str]]:
    """Stream ``src``, write rows with ``wm_col > wm_value`` to ``dst``.

    Returns ``(new_row_count, new_watermark, fields)``. ``new_watermark`` is
    ``max(wm_col)`` over the emitted rows, or ``wm_value`` when no rows
    qualified (keeps the watermark stationary). An empty ``_incremental.csv``
    with just the header is still produced so downstream stages can open it.
    String comparison is correct for ISO-8601 dates / timestamps.

    ``backend="polars"`` 时用 ``pl.scan_csv(src).filter(...).collect()`` 流式
    扫描 + 谓词下推，水位用 ``df.select(pl.col(wm_col).max())``；参见
    docs/evolution.md §4.3.1.5。

    ``backend="spark"`` 时用 ``spark.read.csv(src).filter(F.col(wm_col) > wm_value)``
    分区并行过滤，水位用 ``filtered.agg(F.max(wm_col))``；写出用 coalesce(1)
    保证单文件且 header 正确（增量数据通常小，coalesce(1) 可接受）；参见
    docs/evolution.md §4.3.2.4。

    ``backend="python"`` 走原 csv.DictReader 路径，行为 100% 不变。
    """
    # 引擎特定逻辑——非函数级 dispatch：backend 已作为参数透传，
    # 这是参数化分支（caller 传入 ctx.engine_backend），保留内联。
    if backend == "spark":
        assert ctx is not None
        return _copy_incremental_spark(ctx, src, dst, wm_col, wm_value)
    if backend == "polars":
        return _copy_incremental_polars(src, dst, wm_col, wm_value)
    new_rows: list[dict[str, str]] = []
    max_wm: Optional[str] = wm_value
    fields: list[str] = []
    with open(src, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames or [])
        for row in reader:
            val = row.get(wm_col, "")
            if val and val > wm_value:
                new_rows.append(row)
                if max_wm is None or val > max_wm:
                    max_wm = val
    # storage.backend="parquet" 时用 table_write 写 parquet（本地 .csv.parquet 或 S3）
    if ctx is not None and _get_storage_backend(ctx.config) == "parquet":
        table_write(dst, new_rows, ctx.config, fields=fields)
    else:
        csv_write(dst, fields, new_rows)
    return len(new_rows), max_wm, fields


def _copy_incremental_spark(
    ctx: PipelineContext, src: str, dst: str, wm_col: str, wm_value: str
) -> tuple[int, Optional[str], list[str]]:
    """Spark 分布式增量过滤拷贝.

    单机模式：用 ``spark.read.csv(src).filter(F.col(wm_col) > wm_value)``
    分区并行扫描 + 过滤。

    多机模式（cluster.enabled=true）：Worker 无法访问宿主机文件，Driver 端
    用 ``csv_read(src)`` 读源 CSV → ``spark.createDataFrame`` → filter。

    水位用 ``F.max(wm_col)`` 表达式（Spark DataFrame 聚合）。写出用
    ``table_write`` 以兼容 parquet storage（S3/MinIO）。

    参见 docs/evolution.md §4.3.2.4。
    """
    from pyspark.sql import functions as F

    cfg = ctx.config
    spark = ctx.spark_session
    assert spark is not None
    cluster = cfg.get("engine", {}).get("spark", {}).get("cluster", {})
    if cluster.get("enabled"):
        # 多机模式：Driver 端读本地 CSV → createDataFrame → filter
        rows_data, fields = csv_read(src)
        str_rows = [
            {f: (v if v != "" else None) for f in fields for v in [str(r.get(f, ""))]}
            for r in rows_data
        ]
        df = spark.createDataFrame(str_rows)
    else:
        # 单机模式：spark.read.csv 直接读源文件
        df = _strip_bom_spark(spark.read.csv(src, header=True, inferSchema=True))
        fields = list(df.columns)
    filtered = df.filter(F.col(wm_col) > wm_value)
    rows = filtered.count()  # 触发 action 获取行数
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    # 使用 table_write 写出，兼容 parquet storage（S3/MinIO）
    table_write(dst, filtered, cfg, spark=spark)
    if rows > 0:
        new_wm_raw = filtered.agg(F.max(wm_col).alias("m")).collect()[0]["m"]
        new_wm = str(new_wm_raw) if new_wm_raw is not None else wm_value
    else:
        # 无符合行：水位保持不动
        new_wm = wm_value
    return rows, new_wm, fields


def _copy_incremental_polars(
    src: str, dst: str, wm_col: str, wm_value: str
) -> tuple[int, Optional[str], list[str]]:
    """Polars 流式过滤增量拷贝.

    用 ``pl.scan_csv(src).filter(pl.col(wm_col) > wm_value).collect()`` 流式
    扫描 + 谓词下推，比 Python 逐行 ``csv.DictReader`` 快 5-10 倍。无符合行
    时仍写只含 header 的空 CSV，保证下游可打开。水位用 polars max 表达式。
    """
    import polars as pl

    # 取 header（用 csv 模块，避免 polars 读全量；与 python 路径字段顺序一致）
    with open(src, encoding="utf-8-sig", newline="") as f:
        src_fields = next(csv.reader(f))
    lf = _strip_bom_polars(pl.scan_csv(src))
    df = lf.filter(pl.col(wm_col) > wm_value).collect()
    if df.height > 0:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        df.write_csv(dst)
        new_wm_raw = df.select(pl.col(wm_col).max()).item()
        new_wm = str(new_wm_raw) if new_wm_raw is not None else wm_value
    else:
        # 写空 CSV（只有 header），水位保持不动
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        pl.DataFrame(schema={f: pl.Utf8 for f in src_fields}).write_csv(dst)
        new_wm = wm_value
    return df.height, new_wm, src_fields


def _compute_watermark(
    path: str, wm_col: str, backend: str = "python", spark: Any = None
) -> Optional[str]:
    """Scan a csv and return the max value of ``wm_col`` (string compare).

    ``backend="polars"`` 时用 ``pl.scan_csv(path).select(pl.col(wm_col).max())``
    流式聚合；``backend="python"`` 走原 csv_read 路径，行为不变。

    ``backend="spark"`` 时用 ``spark.read.csv(path).agg(F.max(wm_col))``
    分布式聚合（Spark DataFrame 聚合表达式），参见 docs/evolution.md §4.3.2.4。
    """
    # 引擎特定逻辑——非函数级 dispatch：backend 已作为参数透传，
    # 这是参数化分支（caller 传入 ctx.engine_backend），保留内联。
    if backend == "spark":
        from pyspark.sql import functions as F

        df = _strip_bom_spark(spark.read.csv(path, header=True, inferSchema=True))
        val = df.agg(F.max(wm_col).alias("m")).collect()[0]["m"]
        return str(val) if val is not None else None
    if backend == "polars":
        import polars as pl

        lf = _strip_bom_polars(pl.scan_csv(path))
        val = lf.select(pl.col(wm_col).max()).collect().item()
        return str(val) if val is not None else None
    rows, _ = csv_read(path)
    vals = [r[wm_col] for r in rows if r.get(wm_col)]
    return max(vals) if vals else None


# ----------------------------------------------------------------------
# Phase 4: Iceberg snapshot diff 增量路径
# ----------------------------------------------------------------------
def _iceberg_table_name(ctx: PipelineContext, name: str, table_cfg: dict[str, Any]) -> str:
    """解析 Iceberg 表名.

    优先用 table_cfg["iceberg_table"]（显式配置），缺省用 "warehouse.<name>".

    Args:
        ctx: PipelineContext.
        name: 表逻辑名（如 "orders"）.
        table_cfg: incremental.tables.<name> 配置 dict.

    Returns:
        Iceberg 表名（如 "warehouse.orders"）.
    """
    explicit = table_cfg.get("iceberg_table")
    if explicit:
        return explicit
    warehouse = ctx.config.get("storage", {}).get("warehouse", "warehouse")
    # warehouse 配置可能是路径（如 "state/warehouse"），取末段作为 namespace
    ns = os.path.basename(warehouse.rstrip("/\\")) or "warehouse"
    return f"{ns}.{name}"


def _copy_incremental_iceberg(
    ctx: PipelineContext, name: str, raw_dir: str, table_cfg: dict[str, Any], log
) -> dict[str, Any]:
    """Iceberg snapshot diff 增量路径.

    流程：
      1. 从 ctx.state 读已提交的 from_snapshot_id（state.get_snapshot_id）
      2. 调用 iceberg_snapshot_diff(table_name, cfg, from_snapshot) 获取增量
      3. 把增量行写到 01_raw/{name}_incremental.csv（用 table_write，兼容 parquet storage）
      4. 暂存新 snapshot id 到 ctx.state（StateStore.set_new_snapshot_id）
      5. 返回 entry dict

    首次运行（from_snapshot is None）：snapshot diff 返回全量数据（从初始
    snapshot 到 current），相当于 init full load.

    Args:
        ctx: PipelineContext.
        name: 表逻辑名.
        raw_dir: 01_raw 目录.
        table_cfg: incremental.tables.<name> 配置 dict.
        log: StageLog 实例.

    Returns:
        entry dict（与 _ingest_incremental 返回格式一致）.
    """
    from ..state import StateStore

    cfg = ctx.config
    table_name = _iceberg_table_name(ctx, name, table_cfg)

    # 1. 读已提交的 from_snapshot_id
    #    优先用 ctx.state_path（外部显式指定的 state 文件路径），
    #    否则用 cfg.incremental.state_dir（缺省 "state"）。
    state_dir = cfg.get("incremental", {}).get("state_dir", "state")
    if ctx.state_path:
        store = StateStore(os.path.dirname(ctx.state_path))
    else:
        store = StateStore(abs_path(state_dir))
    from_snapshot = store.get_snapshot_id(name)

    # 2. 调用 iceberg_snapshot_diff 获取增量
    diff = iceberg_snapshot_diff(table_name, cfg, from_snapshot)
    rows = diff["rows"]
    fields = diff["fields"]
    to_snapshot = diff["to_snapshot"]
    added_files = diff["added_data_files"]

    # 3. 写增量到 01_raw/{name}_incremental.csv
    # Iceberg 模式下中间产物仍是 CSV（向后兼容后续 stage 的 load_csv 读取），
    # Iceberg 表是源数据湖，01_raw/ 是 AutoBatch 内部中间产物.
    dst = os.path.join(raw_dir, f"{name}_incremental.csv")
    if rows:
        # storage.backend="parquet" 时用 table_write 写 parquet（兼容 parquet storage）
        # storage.backend="iceberg" 时用 csv_write 写 CSV（中间产物，非 Iceberg 表）
        if _get_storage_backend(cfg) == "parquet":
            table_write(dst, rows, cfg, fields=fields)
        else:
            csv_write(dst, fields, rows)
    else:
        # 无增量行：写空 CSV（只有 header），保证下游可打开
        csv_write(dst, fields or [], [])
    rows_count = len(rows)

    # sha256：iceberg 模式下数据在 Iceberg 表中，01_raw 是中间产物
    if _get_storage_backend(cfg) == "parquet" and os.path.exists(dst + ".parquet"):
        sha = sha256_of(dst + ".parquet")
    elif os.path.exists(dst):
        sha = sha256_of(dst)
    else:
        sha = "iceberg"

    # 4. 暂存新 snapshot id 到 ctx.state（两阶段提交）
    snaps = ctx.state.setdefault("iceberg_snapshots", {})
    info = snaps.setdefault(name, {})
    info["new_snapshot_id"] = to_snapshot
    info["new_batch_id"] = ctx.batch_id
    # 同时暂存 watermark（兼容 _advance_and_merge 的 commit_watermark）
    tables = ctx.state.setdefault("tables", {})
    tinfo = tables.setdefault(name, {})
    tinfo["new_watermark"] = str(to_snapshot) if to_snapshot is not None else None
    tinfo["new_seen_row_count"] = rows_count
    tinfo["new_batch_id"] = ctx.batch_id

    # 5. 返回 entry
    entry = {
        "name": name,
        "path": table_name,
        "local_path": table_name,
        "copied_to": os.path.relpath(dst, ROOT),
        "sha256": sha,
        "rows": rows_count,
        "incremental": True,
        "incremental_mode": "iceberg_snapshot_diff",
        "incremental_init": from_snapshot is None,
        "old_snapshot_id": from_snapshot,
        "new_snapshot_id": to_snapshot,
        "added_data_files": added_files,
        "fields": fields,
    }
    log.info(
        "ingest iceberg snapshot diff",
        source=name,
        rows=rows_count,
        old_snapshot=from_snapshot,
        new_snapshot=to_snapshot,
        added_data_files=added_files,
        sha256=sha[:12] if isinstance(sha, str) and len(sha) >= 12 else sha,
        dest=os.path.relpath(dst, ROOT),
    )
    return entry
