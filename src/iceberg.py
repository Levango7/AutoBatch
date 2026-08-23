"""Iceberg 湖存储所有逻辑。

从 helpers.py 拆分出来，降低单文件耦合度。
向后兼容：原 helpers.py 仍 re-export 所有符号。
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

# 模块级锁：串行化 Iceberg catalog 初始化（SQLAlchemy create_all 非线程安全）
_ICEBERG_CATALOG_LOCK = threading.Lock()

# catalog 实例缓存（key = (name, type, uri, warehouse)）：SQL catalog 下每次
# load_catalog 都要走一遍 SQLAlchemy 初始化，缓存后同配置复用同一实例
_ICEBERG_CATALOG_CACHE: dict[tuple[str, str, str, str], Any] = {}


def _get_iceberg_catalog(cfg: dict[str, Any]) -> Any:
    """加载 Iceberg catalog（lazy import pyiceberg；同配置复用缓存实例）."""
    try:
        from pyiceberg.catalog import load_catalog  # lazy import
    except ImportError as e:
        raise RuntimeError(
            "pyiceberg is required for storage.backend='iceberg' "
            "(install: pip install --pre pyiceberg==0.12.0rc1)"
        ) from e
    ice_cfg = cfg.get("storage", {}).get("iceberg", {}) or {}
    name = ice_cfg.get("catalog_name", "autobatch")
    catalog_type = ice_cfg.get("catalog_type", "sql")
    catalog_uri = ice_cfg.get("catalog_uri", "sqlite:///state/iceberg_catalog.db")
    warehouse = ice_cfg.get("warehouse", "state/warehouse")
    if warehouse.startswith("file:///"):
        warehouse = warehouse[len("file:///") :]
    cache_key = (name, catalog_type, catalog_uri, warehouse)
    with _ICEBERG_CATALOG_LOCK:
        try:
            cached = _ICEBERG_CATALOG_CACHE.get(cache_key)
            if cached is not None:
                return cached
            catalog = load_catalog(
                name=name, type=catalog_type, uri=catalog_uri, warehouse=warehouse
            )
            _ICEBERG_CATALOG_CACHE[cache_key] = catalog
            return catalog
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"failed to load Iceberg catalog: {e}") from e


def _iceberg_table_identifier(path: str) -> tuple[str, ...]:
    """把 Iceberg 表名解析为 (namespace, table) 元组."""
    parts = path.split(".")
    if len(parts) == 1:
        return ("warehouse", parts[0])
    return tuple(parts)


def _iceberg_ensure_namespace(catalog: Any, identifier: tuple[str, ...]) -> None:
    """确保 namespace 存在（不存在则创建）."""
    namespace = identifier[:-1]
    if not namespace:
        return
    try:
        catalog.create_namespace_if_not_exists(namespace)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "create_namespace_if_not_exists failed for %r: %s — skipping",
            namespace,
            e,
        )


def _iceberg_infer_schema(rows: list[dict[str, Any]], fields=None) -> Any:
    """从 List[Dict] 推断 pyiceberg Schema（全部用 StringType，与 CSV 语义一致）."""
    from pyiceberg.schema import Schema  # lazy import
    from pyiceberg.types import NestedField, StringType  # lazy import

    if fields is None:
        fields = list(rows[0].keys()) if rows else []
    nested_fields = [
        NestedField(i + 1, f, StringType(), required=False) for i, f in enumerate(fields)
    ]
    return Schema(*nested_fields)


def _rows_to_arrow_table(rows: list[dict[str, Any]], fields=None) -> Any:
    """List[Dict] → pyarrow.Table（全 string schema，与 CSV 语义一致）."""
    import pyarrow as pa  # lazy import

    if fields is None:
        fields = list(rows[0].keys()) if rows else []
    str_rows = [{f: (str(r.get(f)) if r.get(f) is not None else "") for f in fields} for r in rows]
    schema = pa.schema([(f, pa.string()) for f in fields])
    return pa.Table.from_pylist(str_rows, schema=schema)


def _iceberg_spark_full_name(path: str, cfg: dict[str, Any]) -> str:
    """把 pyiceberg 表名（namespace.table）解析为 Spark 三段式全名."""
    ice_cfg = cfg.get("storage", {}).get("iceberg", {}) or {}
    catalog_name = ice_cfg.get("catalog_name", "autobatch")
    return f"{catalog_name}.{path}"


def _table_read_iceberg(
    path: str,
    cfg: dict[str, Any],
    engine_backend: str,
    spark: Any = None,
    snapshot_id=None,
) -> Any:
    """storage.backend='iceberg' 时的读路径."""
    if engine_backend == "spark":
        from .helpers import _get_spark_session

        if spark is None:
            spark = _get_spark_session(cfg)
        full_name = _iceberg_spark_full_name(path, cfg)
        try:
            reader = spark.read
            if snapshot_id is not None:
                reader = reader.option("snapshot-id", int(snapshot_id))
            return reader.table(full_name)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"failed to read Iceberg table {full_name} via Spark: {e}") from e
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(path)
    try:
        table = catalog.load_table(identifier)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to load Iceberg table {path}: {e}") from e
    try:
        scan = table.scan(snapshot_id=snapshot_id) if snapshot_id is not None else table.scan()
        arrow_table = scan.to_arrow()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to scan Iceberg table {path}: {e}") from e
    if engine_backend == "polars":
        import polars as pl  # lazy import

        return pl.from_arrow(arrow_table)
    return arrow_table.to_pylist(), list(arrow_table.column_names)


def _table_write_iceberg(
    path: str,
    df_or_rows: Any,
    cfg: dict[str, Any],
    engine_backend: str,
    fields=None,
    spark: Any = None,
    mode: str = "append",
) -> int:
    """storage.backend='iceberg' 时的写路径."""
    if engine_backend == "spark":
        from .helpers import _get_spark_session

        if spark is None:
            spark = _get_spark_session(cfg)
        full_name = _iceberg_spark_full_name(path, cfg)
        df = df_or_rows
        try:
            n_rows = df.count()
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"failed to count rows before writing Iceberg table {full_name} via Spark: {e}"
            ) from e
        try:
            if mode == "overwrite":
                df.writeTo(full_name).createOrReplace()
            else:
                try:
                    df.writeTo(full_name).append()
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "append failed (table may not exist), falling back to createOrReplace for %s",
                        full_name,
                    )
                    df.writeTo(full_name).createOrReplace()
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"failed to {mode} Iceberg table {full_name} via Spark: {e}") from e
        return n_rows
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(path)
    _iceberg_ensure_namespace(catalog, identifier)

    if engine_backend == "polars" and hasattr(df_or_rows, "to_arrow"):
        arrow_table = df_or_rows.to_arrow()
        if fields is None:
            fields = list(arrow_table.column_names)
    else:
        rows = df_or_rows
        if fields is None:
            fields = list(rows[0].keys()) if rows else []
        arrow_table = _rows_to_arrow_table(rows, fields)

    n_rows = arrow_table.num_rows

    try:
        table = catalog.load_table(identifier)
    except Exception:  # noqa: BLE001
        logger.info("table %s does not exist, creating with inferred schema", identifier)
        schema = _iceberg_infer_schema([], fields)
        try:
            table = catalog.create_table(identifier, schema=schema)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"failed to create Iceberg table {path}: {e}") from e

    try:
        if mode == "overwrite":
            table.overwrite(arrow_table)
        else:
            table.append(arrow_table)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to {mode} Iceberg table {path}: {e}") from e
    return n_rows


def iceberg_snapshot_diff(
    table_name: str,
    cfg: dict[str, Any],
    from_snapshot=None,
) -> dict[str, Any]:
    """Iceberg snapshot diff：返回两个 snapshot 之间的增量数据文件信息."""
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(table_name)
    try:
        table = catalog.load_table(identifier)
        current = table.current_snapshot()
        to_snapshot = current.snapshot_id if current else None
        if to_snapshot is None:
            # 空表（无任何 snapshot）：incremental_append_scan 对 None 边界行为
            # 未定义（pyiceberg 预发布版），直接返回空增量
            return {
                "from_snapshot": from_snapshot,
                "to_snapshot": None,
                "added_data_files": 0,
                "added_rows_count": 0,
                "rows": [],
                "fields": [],
            }
        scan = table.incremental_append_scan(
            from_snapshot_id_exclusive=from_snapshot,
            to_snapshot_id_inclusive=to_snapshot,
        )
        arrow_table = scan.to_arrow()
        rows = arrow_table.to_pylist()
        fields = list(arrow_table.column_names)
        added_files = 0
        if current is not None and current.summary is not None:
            try:
                extra = getattr(current.summary, "_additional_properties", {}) or {}
                added_files = int(extra.get("added-data-files", 0) or 0)
            except Exception:  # noqa: BLE001
                added_files = 0
                logger.debug(
                    "failed to parse snapshot summary extra properties for %s",
                    table_name,
                )
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to compute snapshot diff for {table_name}: {e}") from e
    return {
        "from_snapshot": from_snapshot,
        "to_snapshot": to_snapshot,
        "added_data_files": added_files,
        "added_rows_count": len(rows),
        "rows": rows,
        "fields": fields,
    }


def iceberg_snapshot_diff_spark(
    table_name: str,
    cfg: dict[str, Any],
    from_snapshot=None,
    spark: Any = None,
) -> dict[str, Any]:
    """Spark 原生 incremental scan：分布式 snapshot diff."""
    from .helpers import _get_spark_session

    if spark is None:
        spark = _get_spark_session(cfg)
    full_name = _iceberg_spark_full_name(table_name, cfg)
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(table_name)
    try:
        table = catalog.load_table(identifier)
        current = table.current_snapshot()
        to_snapshot = current.snapshot_id if current else None
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to load Iceberg table {table_name} for snapshot id: {e}") from e
    try:
        reader = spark.read
        if from_snapshot is not None:
            reader = reader.option("start-snapshot-id", int(from_snapshot))
            if to_snapshot is not None:
                reader = reader.option("end-snapshot-id", int(to_snapshot))
        df = reader.table(full_name)
        fields = [f.name for f in df.schema.fields]
        added_rows_count = df.count()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to compute Spark snapshot diff for {table_name}: {e}") from e
    return {
        "from_snapshot": from_snapshot,
        "to_snapshot": to_snapshot,
        "df": df,
        "added_rows_count": added_rows_count,
        "fields": fields,
    }


def read_history_snapshot(
    table_name: str,
    cfg: dict[str, Any],
    snapshot_id: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Iceberg time travel：读取指定 snapshot 的历史数据."""
    return _table_read_iceberg(table_name, cfg, "python", spark=None, snapshot_id=snapshot_id)


def list_snapshots(table_name: str, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """列出 Iceberg 表的所有 snapshot."""
    catalog = _get_iceberg_catalog(cfg)
    identifier = _iceberg_table_identifier(table_name)
    try:
        table = catalog.load_table(identifier)
        result = []
        for snap in table.snapshots():
            summary_dict: dict[str, Any] = {}
            if snap.summary is not None:
                try:
                    summary_dict["operation"] = str(snap.summary.operation)
                    summary_dict.update(getattr(snap.summary, "_additional_properties", {}) or {})
                except Exception:  # noqa: BLE001
                    summary_dict = {}
                    logger.debug(
                        "failed to parse snapshot summary for snapshot_id=%s",
                        snap.snapshot_id,
                    )
            result.append(
                {
                    "snapshot_id": snap.snapshot_id,
                    "parent_id": snap.parent_snapshot_id,
                    "timestamp_ms": snap.timestamp_ms,
                    "summary": summary_dict,
                }
            )
        return result
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to list snapshots for {table_name}: {e}") from e


def _iceberg_path_is_table_name(path: str) -> bool:
    """判断 path 是 Iceberg 表名还是文件路径."""
    if not path:
        return False
    if os.sep in path or "/" in path or "\\" in path:
        return False
    if len(path) >= 2 and path[1] == ":":
        return False
    if path.endswith(".csv") or path.endswith(".parquet"):
        return False
    return True


# 向后兼容：所有导出名称与原 helpers.py 保持一致
__all__ = [
    "_get_iceberg_catalog",
    "_iceberg_table_identifier",
    "_iceberg_ensure_namespace",
    "_iceberg_infer_schema",
    "_rows_to_arrow_table",
    "_iceberg_spark_full_name",
    "_table_read_iceberg",
    "_table_write_iceberg",
    "iceberg_snapshot_diff",
    "iceberg_snapshot_diff_spark",
    "read_history_snapshot",
    "list_snapshots",
    "_iceberg_path_is_table_name",
]
