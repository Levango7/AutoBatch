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


def _normalize_catalog_uri(catalog_uri: str) -> str:
    """把相对路径的 SQLite catalog URI 解析为相对项目 ROOT 的绝对路径.

    缺省值 ``sqlite:///state/iceberg_catalog.db``（及配置中同样的相对路径写法）
    的路径部分是相对路径，catalog DB 落盘位置将依赖进程 CWD——不同入口/不同
    CWD 下会用上不同的 DB 文件，只读 CWD 下还会直接失败。统一把 sqlite URI 的
    路径部分解析为基于 ROOT 的绝对路径，保证跨入口行为一致。
    已是绝对路径的 URI（包括测试显式传入的临时路径）与其他 scheme
    （http://、s3:// 等）原样返回，不受影响.
    """
    prefix = "sqlite:///"
    if not catalog_uri.startswith(prefix):
        return catalog_uri
    rel = catalog_uri[len(prefix) :]
    if not rel or os.path.isabs(rel):
        return catalog_uri
    # 局部导入避免循环依赖（helpers.py 反向导入本模块全部符号）
    from .helpers import ROOT

    return prefix + os.path.join(ROOT, rel).replace(os.sep, "/")


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
    catalog_uri = _normalize_catalog_uri(
        ice_cfg.get("catalog_uri", "sqlite:///state/iceberg_catalog.db")
    )
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


def _spark_table_exists(spark: Any, full_name: str) -> bool:
    """判断 Spark catalog 中目标表是否已存在.

    Spark 3.4+ 提供 ``spark.catalog.tableExists``（Spark 4.x 为标准 API），
    旧版本回退到 ``spark.table()`` 元数据探测。探测也失败（如 catalog 连接
    异常）时视为表不存在：此时走建表路径，若表实际存在 createOrReplace 会
    因与 append 相同的写入路径（同 catalog、同权限）而同样失败并快速报错，
    不会出现"探测误判 + 覆盖写成功"的数据丢失组合.
    """
    try:
        return bool(spark.catalog.tableExists(full_name))
    except AttributeError:
        # Spark < 3.4 无 tableExists API，走下方 spark.table 探测
        pass
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "tableExists check failed for %s (%s), falling back to spark.table probe",
            full_name,
            e,
        )
    try:
        spark.table(full_name)
        return True
    except Exception:  # noqa: BLE001
        return False


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
                # Iceberg 1.11 移除了 snapshot-id reader option（抛
                # IllegalArgumentException: "no longer supported, use Spark
                # built-in versionAsOf"）。versionAsOf 是 Spark 标准 time
                # travel 选项，Iceberg 把值解析为 snapshot id（与 snapshot
                # id 相等时优先按 id 匹配）。
                reader = reader.option("versionAsOf", int(snapshot_id))
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
    """storage.backend='iceberg' 时的写路径.

    分派按输入类型而非仅 engine_backend：engine.backend="spark" 时调用方仍可能
    传入 List[Dict]（如 pyiceberg 建表+写初始数据、增量 merge 的 python 产物），
    这类输入必须走 pyiceberg 路径——误入 Spark 分支会对 list 调 df.count()
    （list.count 需 1 个参数）直接崩溃。仅当输入真是 SparkDataFrame（有 writeTo）
    时才走 Spark 写路径。
    """
    if engine_backend == "spark" and hasattr(df_or_rows, "writeTo"):
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
                if _spark_table_exists(spark, full_name):
                    # 表已存在：INSERT OVERWRITE 在同一张表上以单个新 snapshot 原子
                    # 替换全部数据文件，旧 snapshot 仍可 time travel——与 python 路径
                    # table.overwrite() 语义一致。旧实现无条件 createOrReplace：每次
                    # 全量跑都重建表，历史 snapshot 全部不可达，spark 路径的 time
                    # travel 承诺被打破（2026-08 审查 B5）。
                    # 用 SQL 语句而非 DFv2 overwrite(lit(True))：后者需要 F.lit（依赖
                    # 活跃 SparkContext，无 JVM 的 fake 单测无法覆盖），前者分支决策
                    # 是纯 catalog 查询 + SQL 字符串。
                    df.createOrReplaceTempView("_autobatch_overwrite_src")
                    try:
                        spark.sql(
                            f"INSERT OVERWRITE TABLE {full_name} "
                            "SELECT * FROM _autobatch_overwrite_src"
                        )
                    finally:
                        spark.catalog.dropTempView("_autobatch_overwrite_src")
                else:
                    # 表不存在：建表并写入（与旧实现的建表路径一致）
                    df.writeTo(full_name).createOrReplace()
            elif _spark_table_exists(spark, full_name):
                # 表已存在：只允许 append。旧实现里 append 失败即无条件
                # createOrReplace 回退，任何瞬态错误（S3 抖动、catalog 超时）
                # 都会用本批 df 覆盖整表历史数据（C6 数据丢失级缺陷）。
                # 现在 append 真正失败时记录原始异常后原样抛出，绝不覆盖.
                try:
                    df.writeTo(full_name).append()
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "failed to append to existing Iceberg table %s via Spark; "
                        "refusing to overwrite table history, re-raising",
                        full_name,
                    )
                    raise
            else:
                # 合法兜底：表不存在时建表（行为与旧实现的建表路径一致）
                logger.info(
                    "Iceberg table %s not found in Spark catalog, creating via createOrReplace",
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
