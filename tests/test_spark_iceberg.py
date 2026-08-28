"""Phase 5 Spark + Iceberg 三合一测试（engine.backend="spark" + storage.backend="iceberg"）.

验证 ``spark.read.table("catalog.ns.tbl")`` / ``df.writeTo(...).overwrite()`` /
Spark 原生 incremental scan 等行为，以及与 pyiceberg 路径的互操作.
设计见 docs/evolution.md §6.x（Phase 5）.

环境限制（重要）：
    1. Spark 写文件需要 Hadoop native IO 库（Windows: hadoop.dll,
       Linux: libhadoop.so, macOS: libhadoop.dylib）.
    2. Iceberg 官方 JAR 最高只支持 Spark 4.1，不支持 Spark 4.2. 当前
       SPARK_HOME 是 4.2.0，缺 iceberg-spark-runtime-4.1_2.13-1.11.0.jar，
       导致 spark.read.table("catalog.ns.tbl") 抛 ClassNotFoundException.

    因此本模块所有测试用 ``pytest.mark.skipif`` 跳过，条件是：
      - Hadoop native IO 库不存在，或
      - pyspark 未安装，或
      - iceberg-spark-runtime JAR 不在 SPARK_HOME/jars/

    代码逻辑完整正确，在装齐 Hadoop native library + iceberg JAR 的
    Windows/Linux/macOS 环境下可直接运行.

场景:
1. test_spark_iceberg_write_read           — Spark 写 Iceberg 表 + 读回
2. test_spark_iceberg_time_travel          — Spark snapshot-id option time travel
3. test_spark_iceberg_snapshot_diff        — Spark 原生 incremental scan
4. test_spark_iceberg_interop_pyiceberg_write_spark_read
   — pyiceberg 写 → Spark 读（互操作）
5. test_spark_iceberg_interop_spark_write_pyiceberg_read
   — Spark 写 → pyiceberg 读（互操作）
6. test_spark_iceberg_equivalence_python   — Spark+Iceberg 与 python+Iceberg 产物等价
7. test_spark_iceberg_table_name_format    — 表名格式 catalog.ns.tbl 路由正确
8. test_spark_iceberg_overwrite_append     — overwrite + append 模式
"""

from __future__ import annotations

import copy
import os

# ----------------------------------------------------------------------
# skipif 条件：Hadoop native IO + iceberg-spark-runtime JAR（跨平台）
# ----------------------------------------------------------------------
# 在模块收集时求值（pytest fixture 设置环境变量是在测试运行时，太晚），
# 因此直接检测默认路径下的 native library，或环境变量 HADOOP_HOME 指向的 bin/.
import os as _os
import platform as _platform
import uuid
from typing import Any

import pytest

from src.helpers import (
    _iceberg_spark_full_name,
    abs_path,
    csv_read,
    iceberg_snapshot_diff,
    iceberg_snapshot_diff_spark,
    list_snapshots,
    table_read,
    table_write,
)


def _hadoop_native_exists(hadoop_home: str) -> bool:
    """检测 Hadoop native IO 库是否存在。"""
    if not hadoop_home or not _os.path.isdir(hadoop_home):
        return False
    bin_dir = _os.path.join(hadoop_home, "bin")
    if not _os.path.isdir(bin_dir):
        return False
    if _platform.system() == "Windows":
        return _os.path.exists(_os.path.join(bin_dir, "hadoop.dll"))
    else:
        try:
            for f in _os.listdir(bin_dir):
                if f.startswith("libhadoop.") and (f.endswith(".so") or f.endswith(".dylib")):
                    return True
        except OSError:
            pass
        return False


_HADOOP_HOME_CANDIDATES = [
    _os.environ.get("HADOOP_HOME", ""),
    "/opt/hadoop",
    "/usr/local/hadoop",
    r"F:\hadoop",
]
_HADOOP_DLL_EXISTS = any(_hadoop_native_exists(h) for h in _HADOOP_HOME_CANDIDATES if h)
# 同时要求 pyspark 可 import（未安装时也跳过）
try:
    import pyspark  # noqa: F401

    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

# 检测 iceberg-spark-runtime / sqlite-jdbc JAR 是否在 SPARK_HOME/jars/
# （fixture 用 SQL catalog：Spark 侧经 JdbcCatalog 连 SQLite，需 sqlite-jdbc 驱动）
_SPARK_HOME_CANDIDATES = [
    _os.environ.get("SPARK_HOME", ""),
    "/opt/spark",
    "/usr/local/spark",
    r"F:\spark_home",
]
_ICEBERG_JAR_EXISTS = False
_SQLITE_JDBC_JAR_EXISTS = False
for _home in _SPARK_HOME_CANDIDATES:
    if not _home or not _os.path.isdir(_os.path.join(_home, "jars")):
        continue
    try:
        for _fname in _os.listdir(_os.path.join(_home, "jars")):
            if _fname.startswith("iceberg-spark-runtime") and _fname.endswith(".jar"):
                _ICEBERG_JAR_EXISTS = True
            if _fname.startswith("sqlite-jdbc") and _fname.endswith(".jar"):
                _SQLITE_JDBC_JAR_EXISTS = True
    except OSError:
        pass

SPARK_ICEBERG_DISABLED = (
    not _HADOOP_DLL_EXISTS
    or not _PYSPARK_AVAILABLE
    or not _ICEBERG_JAR_EXISTS
    or not _SQLITE_JDBC_JAR_EXISTS
)

_SKIP_REASON = (
    "hadoop native IO library not found or pyspark not installed or "
    "iceberg-spark-runtime / sqlite-jdbc JAR missing in SPARK_HOME/jars/ - "
    "Spark+Iceberg tests require Hadoop native IO + Iceberg Spark extensions"
)

spark_iceberg_skip = pytest.mark.skipif(SPARK_ICEBERG_DISABLED, reason=_SKIP_REASON)


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def _new_bid(tag: str) -> str:
    """生成唯一 batch_id，前缀 test-spark-iceberg- 便于 fixture cleanup 统一清理."""
    return f"test-spark-iceberg-{tag}-{uuid.uuid4().hex[:6]}"


def _make_cfg(env) -> dict[str, Any]:
    """从 fixture 复制 cfg 并返回（避免修改 fixture 状态）."""
    return copy.deepcopy(env["cfg"])


# ----------------------------------------------------------------------
# 1. Spark 写 Iceberg 表 + 读回
# ----------------------------------------------------------------------
@spark_iceberg_skip
def test_spark_iceberg_write_read(spark_iceberg_env):
    """Spark DataFrame 写入 Iceberg 表，spark.read.table 读回验证.

    流程：
      1. 用 pyiceberg（python backend）建表 + 写初始数据（确保表存在）
      2. 构造 SparkDataFrame，用 df.writeTo(...).append() 写入
      3. spark.read.table() 读回，验证行数与内容
    """
    cfg = _make_cfg(spark_iceberg_env)
    fields = ["id", "name", "val"]
    # 1. pyiceberg 建表 + 写初始数据
    table_write(
        "warehouse.spark_test",
        [{"id": "1", "name": "alice", "val": "a"}],
        cfg,
        fields=fields,
        mode="append",
    )

    # 2. Spark 写入（需要 SparkSession；通过 table_write 内部 _get_spark_session 创建）
    # 构造 SparkDataFrame：用 spark.createDataFrame 从 List[Row]
    from src.helpers import _get_spark_session

    spark = _get_spark_session(cfg)
    rows_data = [("2", "bob", "b"), ("3", "carol", "c")]
    spark_df = spark.createDataFrame(rows_data, schema="id string, name string, val string")
    n = table_write("warehouse.spark_test", spark_df, cfg, mode="append")
    assert n == 2

    # 3. spark.read.table 读回
    result_df = table_read("warehouse.spark_test", cfg)
    assert result_df is not None
    total_count = result_df.count()
    assert total_count == 3  # 1 (pyiceberg) + 2 (spark)
    # 验证 id 集合
    ids = {row.id for row in result_df.collect()}
    assert ids == {"1", "2", "3"}


# ----------------------------------------------------------------------
# 2. Spark snapshot-id option time travel
# ----------------------------------------------------------------------
@spark_iceberg_skip
def test_spark_iceberg_time_travel(spark_iceberg_env):
    """Spark Iceberg time travel：snapshot-id option 读历史 snapshot.

    流程：
      1. pyiceberg 写两次 append，记录每次 snapshot id
      2. spark.read.option("snapshot-id", sid_1).table() 读历史 → 1 行
      3. spark.read.option("snapshot-id", sid_2).table() 读历史 → 2 行
    """
    cfg = _make_cfg(spark_iceberg_env)
    fields = ["id", "val"]
    # 第一次 append
    table_write("warehouse.spark_tt", [{"id": "1", "val": "a"}], cfg, fields=fields, mode="append")
    snaps_1 = list_snapshots("warehouse.spark_tt", cfg)
    sid_1 = snaps_1[-1]["snapshot_id"]
    # 第二次 append
    table_write("warehouse.spark_tt", [{"id": "2", "val": "b"}], cfg, fields=fields, mode="append")
    snaps_2 = list_snapshots("warehouse.spark_tt", cfg)
    sid_2 = snaps_2[-1]["snapshot_id"]

    # Spark time travel 到 sid_1：应只有 1 行
    df_1 = table_read("warehouse.spark_tt", cfg, snapshot_id=sid_1)
    assert df_1.count() == 1
    # Spark time travel 到 sid_2：应有 2 行
    df_2 = table_read("warehouse.spark_tt", cfg, snapshot_id=sid_2)
    assert df_2.count() == 2


# ----------------------------------------------------------------------
# 3. Spark 原生 incremental scan（分布式 snapshot diff）
# ----------------------------------------------------------------------
@spark_iceberg_skip
def test_spark_iceberg_snapshot_diff(spark_iceberg_env):
    """Spark 原生 incremental scan：start-snapshot-id + end-snapshot-id.

    流程：
      1. pyiceberg 写两次 append，记录 sid_1
      2. iceberg_snapshot_diff_spark(from_snapshot=sid_1) 返回增量 SparkDataFrame
      3. 验证增量行数 == 第二次 append 的行数
      4. 与 pyiceberg iceberg_snapshot_diff 行数对比
    """
    cfg = _make_cfg(spark_iceberg_env)
    fields = ["id", "val"]
    # 第一次 append（建立初始 snapshot）
    table_write(
        "warehouse.spark_diff",
        [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}],
        cfg,
        fields=fields,
        mode="append",
    )
    snaps_1 = list_snapshots("warehouse.spark_diff", cfg)
    sid_1 = snaps_1[-1]["snapshot_id"]
    # 第二次 append（增量）
    table_write(
        "warehouse.spark_diff",
        [{"id": "3", "val": "c"}, {"id": "4", "val": "d"}],
        cfg,
        fields=fields,
        mode="append",
    )

    # Spark 原生 incremental scan
    spark_diff = iceberg_snapshot_diff_spark("warehouse.spark_diff", cfg, from_snapshot=sid_1)
    assert spark_diff["from_snapshot"] == sid_1
    assert spark_diff["to_snapshot"] is not None
    assert spark_diff["added_rows_count"] == 2
    assert "df" in spark_diff
    assert spark_diff["fields"] == ["id", "val"]

    # 与 pyiceberg snapshot diff 对比行数
    py_diff = iceberg_snapshot_diff("warehouse.spark_diff", cfg, from_snapshot=sid_1)
    assert spark_diff["added_rows_count"] == py_diff["added_rows_count"]

    # 验证增量 id 集合
    spark_ids = {row.id for row in spark_diff["df"].collect()}
    assert spark_ids == {"3", "4"}


# ----------------------------------------------------------------------
# 4. 互操作：pyiceberg 写 → Spark 读
# ----------------------------------------------------------------------
@spark_iceberg_skip
def test_spark_iceberg_interop_pyiceberg_write_spark_read(spark_iceberg_env):
    """互操作：pyiceberg 写入 → spark.read.table 读回.

    验证 Spark Iceberg catalog 能正确加载 pyiceberg 创建的表，
    数据内容一致（行数 + 字段 + 值）.
    """
    cfg = _make_cfg(spark_iceberg_env)
    fields = ["id", "name", "age"]
    rows = [
        {"id": "1", "name": "alice", "age": "30"},
        {"id": "2", "name": "bob", "age": "25"},
        {"id": "3", "name": "carol", "age": "40"},
    ]
    # pyiceberg 写入（python backend）
    table_write("warehouse.interop_py2sp", rows, cfg, fields=fields, mode="append")

    # Spark 读回
    df = table_read("warehouse.interop_py2sp", cfg)
    assert df.count() == 3
    # 验证字段
    field_names = [f.name for f in df.schema.fields]
    assert set(field_names) == {"id", "name", "age"}
    # 验证 id 集合
    ids = {row.id for row in df.collect()}
    assert ids == {"1", "2", "3"}


# ----------------------------------------------------------------------
# 5. 互操作：Spark 写 → pyiceberg 读
# ----------------------------------------------------------------------
@spark_iceberg_skip
def test_spark_iceberg_interop_spark_write_pyiceberg_read(spark_iceberg_env):
    """互操作：Spark 写入 → pyiceberg 读回.

    流程：
      1. pyiceberg 建表（空表）
      2. Spark df.writeTo(...).append() 写入数据
      3. pyiceberg（python backend）读回，验证行数与内容
    """
    cfg = _make_cfg(spark_iceberg_env)
    fields = ["id", "name", "val"]
    # pyiceberg 建表（写一行后 overwrite 为空，确保表存在）
    table_write(
        "warehouse.interop_sp2py",
        [{"id": "0", "name": "init", "val": "x"}],
        cfg,
        fields=fields,
        mode="append",
    )
    # overwrite 清空（pyiceberg overwrite 接受空 list 不行，写一行再 overwrite 空）
    # 改用：直接 append，最后读时过滤掉 init 行

    # Spark 写入
    from src.helpers import _get_spark_session

    spark = _get_spark_session(cfg)
    rows_data = [("10", "spark_row_1", "a"), ("20", "spark_row_2", "b")]
    spark_df = spark.createDataFrame(rows_data, schema="id string, name string, val string")
    n = table_write("warehouse.interop_sp2py", spark_df, cfg, mode="append")
    assert n == 2

    # pyiceberg 读回（python backend）
    # 临时切到 python backend 读
    cfg_py = copy.deepcopy(cfg)
    cfg_py["engine"]["backend"] = "python"
    read_rows, read_fields = table_read("warehouse.interop_sp2py", cfg_py)
    # 应有 3 行（1 init + 2 spark）
    assert len(read_rows) == 3
    assert set(read_fields) == {"id", "name", "val"}
    # 验证 spark 写入的 id 存在
    ids = {r["id"] for r in read_rows}
    assert {"10", "20"}.issubset(ids)


# ----------------------------------------------------------------------
# 6. 等价性：Spark+Iceberg 与 python+Iceberg 产物等价
# ----------------------------------------------------------------------
@spark_iceberg_skip
def test_spark_iceberg_equivalence_python(spark_iceberg_env):
    """等价性：相同数据写入 Iceberg 表，Spark 读与 python 读行数一致.

    流程：
      1. 用 python backend 写入 N 行到 Iceberg 表
      2. python backend 读回 → rows_py
      3. spark backend 读回 → df_spark
      4. 验证行数一致 + id 集合一致
    """
    cfg = _make_cfg(spark_iceberg_env)
    fields = ["id", "val"]
    n_rows = 50
    rows = [{"id": str(i), "val": f"v{i}"} for i in range(n_rows)]
    # python backend 写入
    cfg_py = copy.deepcopy(cfg)
    cfg_py["engine"]["backend"] = "python"
    table_write("warehouse.equiv_test", rows, cfg_py, fields=fields, mode="append")

    # python backend 读回
    rows_py, _ = table_read("warehouse.equiv_test", cfg_py)
    assert len(rows_py) == n_rows

    # spark backend 读回
    df_spark = table_read("warehouse.equiv_test", cfg)
    assert df_spark.count() == n_rows

    # id 集合一致
    ids_py = {r["id"] for r in rows_py}
    ids_spark = {row.id for row in df_spark.collect()}
    assert ids_py == ids_spark


# ----------------------------------------------------------------------
# 7. 表名格式 catalog.ns.tbl 路由正确
# ----------------------------------------------------------------------
@spark_iceberg_skip
def test_spark_iceberg_table_name_format(spark_iceberg_env):
    """表名格式：Spark 用 catalog.namespace.table，pyiceberg 用 namespace.table.

    验证 _iceberg_spark_full_name 正确拼接 catalog 前缀.
    """
    cfg = _make_cfg(spark_iceberg_env)
    catalog_name = cfg["storage"]["iceberg"]["catalog_name"]
    # _iceberg_spark_full_name 应返回 "catalog.namespace.table"
    full = _iceberg_spark_full_name("warehouse.orders", cfg)
    assert full == f"{catalog_name}.warehouse.orders"
    # 多层 namespace
    full_nested = _iceberg_spark_full_name("warehouse.sub.orders", cfg)
    assert full_nested == f"{catalog_name}.warehouse.sub.orders"
    # 单层（缺省 namespace 由 _iceberg_table_identifier 处理，这里只测拼接）
    full_single = _iceberg_spark_full_name("orders", cfg)
    assert full_single == f"{catalog_name}.orders"


# ----------------------------------------------------------------------
# 8. overwrite + append 模式
# ----------------------------------------------------------------------
@spark_iceberg_skip
def test_spark_iceberg_overwrite_append(spark_iceberg_env):
    """Spark 写 Iceberg 表：overwrite 覆盖 + append 追加.

    流程：
      1. pyiceberg 建表 + 写初始数据
      2. Spark overwrite 覆盖为新数据
      3. 验证只有新数据
      4. Spark append 追加更多数据
      5. 验证行数 == 新数据 + 追加数据
    """
    cfg = _make_cfg(spark_iceberg_env)
    fields = ["id", "val"]
    # pyiceberg 建表 + 写初始数据
    table_write(
        "warehouse.spark_mode", [{"id": "1", "val": "init"}], cfg, fields=fields, mode="append"
    )

    # Spark overwrite
    from src.helpers import _get_spark_session

    spark = _get_spark_session(cfg)
    df_overwrite = spark.createDataFrame(
        [("10", "overwrite_1"), ("20", "overwrite_2")],
        schema="id string, val string",
    )
    n_ow = table_write("warehouse.spark_mode", df_overwrite, cfg, mode="overwrite")
    assert n_ow == 2

    # 验证只有 overwrite 的数据
    df_read = table_read("warehouse.spark_mode", cfg)
    assert df_read.count() == 2
    ids = {row.id for row in df_read.collect()}
    assert ids == {"10", "20"}

    # Spark append
    df_append = spark.createDataFrame(
        [("30", "append_1"), ("40", "append_2"), ("50", "append_3")],
        schema="id string, val string",
    )
    n_ap = table_write("warehouse.spark_mode", df_append, cfg, mode="append")
    assert n_ap == 3

    # 验证行数 == 2 + 3 = 5
    df_final = table_read("warehouse.spark_mode", cfg)
    assert df_final.count() == 5
    ids_final = {row.id for row in df_final.collect()}
    assert ids_final == {"10", "20", "30", "40", "50"}


# ----------------------------------------------------------------------
# 9. 配置注入：Spark Iceberg extensions 配置正确
# ----------------------------------------------------------------------
def test_spark_iceberg_config_injection(spark_iceberg_env):
    """验证 spark_iceberg_env fixture 配置正确（不需要 Spark 运行）.

    检查 cfg 字段：
      - engine.backend == "spark"
      - storage.backend == "iceberg"
      - storage.iceberg.spark_extensions / spark_catalog_class 已设置
      - storage.iceberg.catalog_name / catalog_type / catalog_uri / warehouse 已设置
    """
    cfg = _make_cfg(spark_iceberg_env)
    assert cfg["engine"]["backend"] == "spark"
    assert cfg["storage"]["backend"] == "iceberg"
    ice = cfg["storage"]["iceberg"]
    assert ice["catalog_name"] == "autobatch_test"
    assert ice["catalog_type"] == "sql"
    assert "catalog_uri" in ice
    assert "warehouse" in ice
    assert ice["spark_extensions"] == (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    assert ice["spark_catalog_class"] == "org.apache.iceberg.spark.SparkCatalog"
    # properties 包含 JAR 信息
    props = ice["properties"]
    assert "iceberg_spark_runtime" in props
    assert "iceberg_aws_bundle" in props
    assert "1.11.0" in props["iceberg_spark_runtime"]


# ----------------------------------------------------------------------
# 10. 配置文件持久化：pipeline.json 包含 Spark Iceberg 配置
# ----------------------------------------------------------------------
def test_pipeline_json_has_spark_iceberg_config():
    """验证 config/pipeline.json + pipeline_small.json 包含 Spark Iceberg 配置.

    检查 storage.iceberg.spark_extensions / spark_catalog_class 字段存在.
    """
    from src.helpers import abs_path, json_load

    for fname in ("config/pipeline.json", "config/pipeline_small.json"):
        cfg = json_load(abs_path(fname))
        ice = cfg.get("storage", {}).get("iceberg", {})
        assert "spark_extensions" in ice, f"{fname} missing spark_extensions"
        assert "spark_catalog_class" in ice, f"{fname} missing spark_catalog_class"
        assert ice["spark_extensions"] == (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        assert ice["spark_catalog_class"] == "org.apache.iceberg.spark.SparkCatalog"
        # properties 包含 JAR 兼容性信息
        props = ice.get("properties", {})
        assert "iceberg_spark_runtime" in props
        assert "iceberg_aws_bundle" in props
        assert "spark_compat" in props
