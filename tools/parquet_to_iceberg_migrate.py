"""Phase 4 迁移脚本：把 Phase 3 的 S3 Parquet 注册为 Iceberg 表.

设计参见 docs/evolution.md §6.x。把 storage.backend="parquet" 时写到 S3/MinIO
的 Parquet 文件注册为 Iceberg 表，使后续可用 storage.backend="iceberg" +
incremental.mode="iceberg_snapshot_diff" 跑增量.

用法：
    # 把 S3 上的 orders/orders_clean.parquet 注册为 Iceberg 表 warehouse.orders_clean
    F:\\Py314\\python.exe tools\\parquet_to_iceberg_migrate.py \\
        --config config/pipeline.json \\
        --parquet-path s3://autobatch/warehouse/orders/orders_clean.parquet \\
        --iceberg-table warehouse.orders_clean

    # 批量迁移多个表
    F:\\Py314\\python.exe tools\\parquet_to_iceberg_migrate.py \\
        --config config/pipeline.json \\
        --batch warehouse.orders=orders/orders_clean \\
        --batch warehouse.customers=customers/customers_clean

流程：
    1. 加载 pipeline config（读 storage.iceberg 段配置 catalog）
    2. 用 pyarrow 从 S3/本地读 Parquet 文件
    3. 用 pyiceberg catalog 创建 Iceberg 表（用推断的 schema）
    4. table.append(arrow_table) 把数据写入 Iceberg 表
    5. 验证行数一致

注意：
    - 迁移是**一次性**操作，迁移后应把 config storage.backend 改为 "iceberg"
    - 迁移不删除原 Parquet 文件（保留作为备份）
    - Iceberg 表的 schema 用全 StringType（与 CSV 语义一致，便于 round-trip）
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any

# 把项目根加入 sys.path，使 from src.helpers import ... 可用
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.helpers import (  # noqa: E402
    _get_iceberg_catalog,  # noqa: E402
    _iceberg_ensure_namespace,  # noqa: E402
    _iceberg_infer_schema,  # noqa: E402
    _iceberg_table_identifier,  # noqa: E402
    abs_path,  # noqa: E402
    json_load,  # noqa: E402
)  # noqa: E402


def _read_parquet_to_arrow(parquet_path: str, cfg: dict[str, Any]) -> Any:
    """读 Parquet 文件为 pyarrow.Table（支持本地和 S3）.

    Args:
        parquet_path: Parquet 文件路径（本地或 s3:// URI）.
        cfg: Pipeline 配置 dict（读 storage 段获取 S3 凭证）.

    Returns:
        pyarrow.Table 实例.
    """
    import pyarrow.parquet as pq  # lazy import

    if parquet_path.startswith("s3://") or parquet_path.startswith("s3a://"):
        # S3/MinIO：用 pyarrow.fs.S3FileSystem
        from src.helpers import _get_s3_filesystem, _s3_uri_to_bucket_key  # noqa: E402
        fs = _get_s3_filesystem(cfg)
        key = _s3_uri_to_bucket_key(parquet_path)
        return pq.read_table(key, filesystem=fs)
    else:
        # 本地 Parquet
        return pq.read_table(parquet_path)


def _arrow_to_string_schema(arrow_table: Any) -> Any:
    """把 pyarrow.Table 转为全 string schema（与 CSV 语义一致）.

    Args:
        arrow_table: 原始 pyarrow.Table.

    Returns:
        全 string schema 的 pyarrow.Table.
    """
    import pyarrow as pa  # lazy import

    fields = list(arrow_table.column_names)
    # 把每列 cast 为 string
    arrays = []
    for col_name in fields:
        col = arrow_table[col_name]
        # null 列直接用 string null
        if col.type == pa.null():
            arrays.append(pa.nulls(len(col), type=pa.string()))
        else:
            arrays.append(col.cast(pa.string()))
    return pa.Table.from_arrays(arrays, names=fields)


def migrate_one(
    parquet_path: str,
    iceberg_table: str,
    cfg: dict[str, Any],
    overwrite: bool = False,
) -> dict[str, Any]:
    """迁移单个 Parquet 文件到 Iceberg 表.

    Args:
        parquet_path: Parquet 文件路径（本地或 s3:// URI）.
        iceberg_table: 目标 Iceberg 表名（如 "warehouse.orders_clean"）.
        cfg: Pipeline 配置 dict.
        overwrite: True 则覆盖已有 Iceberg 表；False 则追加.

    Returns:
        dict 包含：
            parquet_path  — 源 Parquet 路径
            iceberg_table — 目标 Iceberg 表名
            rows          — 迁移行数
            snapshots     — 迁移后 snapshot 数
            snapshot_id   — 当前 snapshot id
    """
    # 1. 读 Parquet 为 pyarrow.Table
    arrow_table = _read_parquet_to_arrow(parquet_path, cfg)
    n_rows = arrow_table.num_rows
    fields = list(arrow_table.column_names)

    # 2. 转为全 string schema（与 CSV 语义一致）
    str_table = _arrow_to_string_schema(arrow_table)

    # 3. 加载 Iceberg catalog
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(iceberg_table)
    _iceberg_ensure_namespace(catalog, identifier)

    # 4. 创建或加载 Iceberg 表
    table_existed = False
    try:
        table = catalog.load_table(identifier)
        table_existed = True
    except Exception:  # noqa: BLE001
        # 表不存在：用推断 schema 创建
        schema = _iceberg_infer_schema([], fields)
        table = catalog.create_table(identifier, schema=schema)

    # 5. 写入数据
    if overwrite and table_existed:
        table.overwrite(str_table)
    else:
        table.append(str_table)

    # 6. 验证并返回
    current = table.current_snapshot()
    return {
        "parquet_path": parquet_path,
        "iceberg_table": iceberg_table,
        "rows": n_rows,
        "snapshots": len(table.snapshots()),
        "snapshot_id": current.snapshot_id if current else None,
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Migrate Phase 3 S3 Parquet to Iceberg table"
    )
    parser.add_argument("--config", default="config/pipeline.json",
                        help="Pipeline config file path")
    parser.add_argument("--parquet-path",
                        help="Source Parquet file path (local or s3:// URI)")
    parser.add_argument("--iceberg-table",
                        help="Target Iceberg table name (e.g. warehouse.orders_clean)")
    parser.add_argument("--batch", action="append", default=[],
                        help="Batch migrate: iceberg_table=parquet_logical_path "
                             "(e.g. warehouse.orders=orders/orders_clean)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing Iceberg table (default: append)")
    args = parser.parse_args(argv)

    # 加载 config
    cfg = json_load(abs_path(args.config))

    # 确保 storage.backend="iceberg"（迁移需要 Iceberg catalog 配置）
    if cfg.get("storage", {}).get("backend") != "iceberg":
        print(f"WARNING: storage.backend is '{cfg.get('storage', {}).get('backend')}', "
              f"not 'iceberg'. Using storage.iceberg config for catalog.")

    results = []

    # 单个迁移
    if args.parquet_path and args.iceberg_table:
        result = migrate_one(args.parquet_path, args.iceberg_table, cfg,
                             overwrite=args.overwrite)
        results.append(result)

    # 批量迁移
    for batch_spec in args.batch:
        if "=" not in batch_spec:
            print(f"ERROR: invalid --batch spec '{batch_spec}', "
                  f"expected iceberg_table=parquet_logical_path")
            return 1
        iceberg_table, parquet_logical = batch_spec.split("=", 1)
        # 把逻辑路径解析为 S3 URI
        from src.helpers import _resolve_s3_path  # noqa: E402
        parquet_uri = _resolve_s3_path(parquet_logical, cfg)
        result = migrate_one(parquet_uri, iceberg_table, cfg,
                             overwrite=args.overwrite)
        results.append(result)

    if not results:
        print("ERROR: no migration specified. Use --parquet-path + --iceberg-table "
              "or --batch iceberg_table=parquet_logical_path")
        return 1

    # 输出结果
    print(f"\nMigration complete: {len(results)} table(s)")
    for r in results:
        print(f"  {r['parquet_path']} -> {r['iceberg_table']}: "
              f"{r['rows']} rows, {r['snapshots']} snapshot(s), "
              f"current snapshot_id={r['snapshot_id']}")
    return 0


if __name__ == "__main__":

    sys.exit(main(sys.argv[1:]))
