"""RuleEngine 单元测试：8 类规则正例/反例 + referential 性能回归
+ 三引擎一致性回归（uniqueness null-key 豁免 / 正则前缀锚定 / 秒级日期边界）。"""

from __future__ import annotations

import os
import sys
import time

import pytest

from src.quality import RuleEngine


def test_completeness_pass(orders_rules, ref_data, good_order):
    rows = [good_order()]
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert len(good) == 1 and len(bad) == 0


def test_completeness_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["order_id"] = ""
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(good) == 0 and len(bad) == 1
    assert "missing_required:order_id" in bad[0]["_reasons"]


def test_uniqueness_pass(orders_rules, ref_data, good_order):
    rows = [good_order("ORD-00000001"), good_order("ORD-00000002")]
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert len(good) == 2 and len(bad) == 0


def test_uniqueness_fail(orders_rules, ref_data, good_order):
    rows = [good_order("ORD-00000001"), good_order("ORD-00000001")]
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert len(bad) == 1
    assert "duplicate_key:order_id" in bad[0]["_reasons"]


def test_range_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_range_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["quantity"] = "0"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "range_violation:quantity" in bad[0]["_reasons"]


def test_allowed_values_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_allowed_values_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["status"] = "shipped"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "invalid_value:status" in bad[0]["_reasons"]


def test_format_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_format_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["order_id"] = "ORD-XXX"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "format_violation:order_id" in bad[0]["_reasons"]


def test_date_valid_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_date_valid_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["order_date"] = "2019-06-30"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "invalid_date:order_date" in bad[0]["_reasons"]


def test_referential_pass(orders_rules, ref_data, good_order):
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([good_order()])
    assert len(good) == 1


def test_referential_fail(orders_rules, ref_data, good_order):
    row = good_order()
    row["customer_id"] = "CUS-999999"
    good, bad, _, _ = RuleEngine("orders", orders_rules, ref_data).check([row])
    assert len(bad) == 1
    assert "orphan_reference:customer_id" in bad[0]["_reasons"]


def test_outlier_pass(orders_rules, ref_data, good_order):
    rows = []
    for i in range(100):
        r = good_order(f"ORD-{i + 1:08d}")
        r["total_amount"] = "500.00"
        rows.append(r)
    _, _, _, outlier_indices = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert len(outlier_indices) == 0


def test_outlier_fail(orders_rules, ref_data, good_order):
    rows = []
    for i in range(100):
        r = good_order(f"ORD-{i + 1:08d}")
        r["total_amount"] = "500.00"
        rows.append(r)
    outlier_row = good_order("ORD-00000101")
    outlier_row["total_amount"] = "999999.00"
    rows.append(outlier_row)
    _, _, _, outlier_indices = RuleEngine("orders", orders_rules, ref_data).check(rows)
    assert 100 in outlier_indices


def test_referential_performance(orders_rules):
    customers = [
        {"customer_id": f"CUS-{i:06d}", "tier": "silver", "city": "上海", "join_date": "2022-01-01"}
        for i in range(1, 1001)
    ]
    products = [
        {"product_id": f"PRD-{i:06d}", "name": "p", "category": "数码", "cost": "10"}
        for i in range(1, 201)
    ]
    ref = {"customers": customers, "products": products}
    rows = []
    for i in range(5000):
        rows.append(
            {
                "order_id": f"ORD-{i + 1:08d}",
                "customer_id": f"CUS-{(i % 1000) + 1:06d}",
                "product_id": f"PRD-{(i % 200) + 1:06d}",
                "order_date": "2026-01-15",
                "created_ts": "2026-01-15T10:00:00",
                "region": "华东",
                "channel": "web",
                "quantity": "5",
                "unit_price": "100.00",
                "status": "completed",
            }
        )
    engine = RuleEngine("orders", orders_rules, ref)
    start = time.monotonic()
    engine.check(rows)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0, f"referential check should be < 1s, got {elapsed:.2f}s"


# ----------------------------------------------------------------------
# 三引擎语义一致性回归（任务76）：
#   1) uniqueness：null/空串 key 不参与唯一性判定（缺失由 completeness 负责），
#      python/polars/spark 三路径结果一致
#   2) format：正则前缀锚定——python 用 re.match（前缀锚定）；spark rlike 原为
#      子串匹配已统一补 ^；polars 走同一逐行 re.match。中间匹配必须失败、
#      前缀匹配+尾部多余字符必须通过
#   3) date_valid：min/max 边界按完整 timestamp 秒级比较——纯日期边界值等于
#      当日 00:00:00 通过；同日 T23:59:59 超过边界被拦截（旧 spark to_date
#      日粒度截断会放行，已修复）
# ----------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_session():
    """本机 SparkSession（模块级复用）；JVM/环境不可用时自动 skip（不算错误）.

    Windows 本地环境要点（探测结论，2026-08）：
    - pyspark 的 ``_find_spark_home()`` 默认把 SPARK_HOME 解析到 pip pyspark
      包目录；python 安装路径含空格/括号（如 ``Program Files (x86)``）时，
      ``spark-submit.cmd`` 内部 cmd 展开报 "was unexpected at this time"，
      JVM 网关无法启动。故优先使用显式的无空格 SPARK_HOME（detect_spark_paths
      探测结果，本机为 ``F:\\spark_home``）。
    - JAVA_HOME 值含括号时被批处理脚本错误展开，优先无括号链接路径。
    - PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON 强制指向当前解释器，避免 worker
      选中 PATH 上带空格/未加引号的 python。
    所有环境变量改动都在 teardown 还原；创建失败一律 skip。
    """
    pytest.importorskip("pyspark")

    from src.helpers import apply_spark_env, detect_spark_paths

    old_env = dict(os.environ)

    apply_spark_env(detect_spark_paths())
    # worker/driver python 指向当前解释器（detect 结果可能指向带空格的路径）
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # JAVA_HOME 含括号时批处理展开失败，回退到无括号链接路径（若存在）
    jh = os.environ.get("JAVA_HOME", "")
    if "(" in jh and os.path.isfile(r"F:\jdk17\bin\java.exe"):
        os.environ["JAVA_HOME"] = r"F:\jdk17"
    # HADOOP_HOME 指向不存在目录会干扰 winutils 查找，临时移除
    if os.environ.get("HADOOP_HOME") and not os.path.isdir(os.environ["HADOOP_HOME"]):
        del os.environ["HADOOP_HOME"]

    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("autobatch-rule-engine-consistency")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 - JVM/hadoop 不可用 → skip，不算错误
        os.environ.clear()
        os.environ.update(old_env)
        pytest.skip(f"SparkSession 无法启动（本机无可用 JVM 环境）: {exc}")
    yield session
    session.stop()
    os.environ.clear()
    os.environ.update(old_env)


def test_uniqueness_null_key_exempt_python(ref_data, good_order):
    """python 路径：null/空串 key 不参与唯一性判定（缺失由 completeness 负责）。"""
    rules = {"uniqueness": {"columns": ["order_id"]}}
    rows = [
        good_order(""),  # 空串 key——豁免
        good_order(""),  # 第二个空串——仍豁免（不判重复）
        good_order(None),  # null key——豁免
        good_order("ORD-00000001"),
        good_order("ORD-00000001"),  # 仅此行为真重复
    ]
    good, bad, stats, _ = RuleEngine("orders", rules, ref_data).check(rows)
    assert len(good) == 4
    assert len(bad) == 1
    assert "duplicate_key:order_id" in bad[0]["_reasons"]
    uniq = next(s for s in stats if s["rule"] == "uniqueness")
    assert (uniq["checked"], uniq["failed"]) == (5, 1)


def test_uniqueness_null_key_exempt_polars(ref_data):
    """polars 路径与 python 一致：null/空串 key 豁免，只标记真重复的后续行。"""
    pl = pytest.importorskip("polars")
    rules = {"uniqueness": {"columns": ["order_id"]}}
    df = pl.DataFrame({"order_id": ["", "", None, "ORD-00000001", "ORD-00000001"]})
    good_df, bad_df, stats, _ = RuleEngine("orders", rules, ref_data).check(df=df)
    assert good_df.height == 4
    assert bad_df.height == 1
    assert "duplicate_key:order_id" in bad_df.to_dicts()[0]["_reasons"]
    uniq = next(s for s in stats if s["rule"] == "uniqueness")
    assert (uniq["checked"], uniq["failed"]) == (5, 1)


def test_uniqueness_null_key_exempt_spark(ref_data, spark_session):
    """spark 路径与 python/polars 一致；且输出不残留 _row_idx 等辅助列。"""
    rules = {"uniqueness": {"columns": ["order_id"]}}
    df = spark_session.createDataFrame(
        [("",), ("",), (None,), ("ORD-00000001",), ("ORD-00000001",)], "order_id string"
    )
    good_df, bad_df, stats, _ = RuleEngine("orders", rules, ref_data).check(
        df=df, spark=spark_session
    )
    assert good_df.count() == 4
    assert bad_df.count() == 1
    assert "duplicate_key:order_id" in bad_df.select("_reasons").collect()[0]["_reasons"]
    uniq = next(s for s in stats if s["rule"] == "uniqueness")
    assert (uniq["checked"], uniq["failed"]) == (5, 1)
    # 物化行号列与 mask 列不得泄漏到 good/bad 输出（下游按原始 schema 消费）
    assert "_row_idx" not in good_df.columns and "_row_idx" not in bad_df.columns
    assert "__bad" not in bad_df.columns


def test_format_prefix_anchor_python(ref_data, good_order):
    """python 路径 re.match 前缀锚定：中间匹配失败，前缀匹配+尾部多余通过。"""
    rules = {"format": {"order_id": "ORD-\\d{8}"}}
    mid_match = good_order("ORD-00000001")
    mid_match["order_id"] = "XORD-12345678"  # 中间匹配、前缀不匹配 → 必须失败
    suffix_ok = good_order("ORD-00000002")
    suffix_ok["order_id"] = "ORD-12345678-suffix"  # 前缀匹配、尾部多余 → 通过
    good, bad, _, _ = RuleEngine("orders", rules, ref_data).check([mid_match, suffix_ok])
    assert len(good) == 1 and good[0]["order_id"] == "ORD-12345678-suffix"
    assert len(bad) == 1 and "format_violation:order_id" in bad[0]["_reasons"]


def test_format_prefix_anchor_polars(ref_data):
    """polars 路径（逐行 re.match）与 python 锚定语义一致。"""
    pl = pytest.importorskip("polars")
    rules = {"format": {"order_id": "ORD-\\d{8}"}}
    df = pl.DataFrame({"order_id": ["XORD-12345678", "ORD-12345678-suffix"]})
    good_df, bad_df, _, _ = RuleEngine("orders", rules, ref_data).check(df=df)
    assert good_df.height == 1 and good_df.to_dicts()[0]["order_id"] == "ORD-12345678-suffix"
    assert bad_df.height == 1
    assert "format_violation:order_id" in bad_df.to_dicts()[0]["_reasons"]


def test_format_prefix_anchor_spark(ref_data, spark_session):
    """spark 路径 rlike 已补前缀 ^ 锚定：中间匹配失败，前缀匹配通过。"""
    rules = {"format": {"order_id": "ORD-\\d{8}"}}
    df = spark_session.createDataFrame(
        [("XORD-12345678",), ("ORD-12345678-suffix",)], "order_id string"
    )
    good_df, bad_df, _, _ = RuleEngine("orders", rules, ref_data).check(df=df, spark=spark_session)
    assert good_df.count() == 1
    assert good_df.select("order_id").collect()[0]["order_id"] == "ORD-12345678-suffix"
    assert bad_df.count() == 1
    assert bad_df.select("_reasons").collect()[0]["_reasons"] == "format_violation:order_id"


def test_date_valid_second_precision_max_python(ref_data, good_order):
    """python 路径：max 边界秒级比较——纯日期边界值通过，同日 T23:59:59 拦截。"""
    rules = {"date_valid": {"columns": ["order_date"], "max": "2026-01-15"}}
    boundary_ok = good_order("ORD-00000001")
    boundary_ok["order_date"] = "2026-01-15"
    same_day_late = good_order("ORD-00000002")
    same_day_late["order_date"] = "2026-01-15T23:59:59"
    good, bad, _, _ = RuleEngine("orders", rules, ref_data).check([boundary_ok, same_day_late])
    assert len(good) == 1 and good[0]["order_id"] == "ORD-00000001"
    assert len(bad) == 1 and "invalid_date:order_date" in bad[0]["_reasons"]


def test_date_valid_second_precision_min_python(ref_data, good_order):
    """python 路径：min 边界秒级比较——纯日期边界值通过，前日 T23:59:59 拦截。"""
    rules = {"date_valid": {"columns": ["order_date"], "min": "2026-01-10"}}
    boundary_ok = good_order("ORD-00000001")
    boundary_ok["order_date"] = "2026-01-10"
    before_min = good_order("ORD-00000002")
    before_min["order_date"] = "2026-01-09T23:59:59"
    good, bad, _, _ = RuleEngine("orders", rules, ref_data).check([boundary_ok, before_min])
    assert len(good) == 1 and good[0]["order_id"] == "ORD-00000001"
    assert len(bad) == 1 and "invalid_date:order_date" in bad[0]["_reasons"]


def test_date_valid_second_precision_polars(ref_data):
    """polars 路径与 python 秒级边界语义一致（max 场景）。"""
    pl = pytest.importorskip("polars")
    rules = {"date_valid": {"columns": ["order_date"], "max": "2026-01-15"}}
    df = pl.DataFrame({"order_date": ["2026-01-15", "2026-01-15T23:59:59"]})
    good_df, bad_df, _, _ = RuleEngine("orders", rules, ref_data).check(df=df)
    assert good_df.height == 1 and good_df.to_dicts()[0]["order_date"] == "2026-01-15"
    assert bad_df.height == 1
    assert "invalid_date:order_date" in bad_df.to_dicts()[0]["_reasons"]


def test_date_valid_second_precision_spark(ref_data, spark_session):
    """spark 路径 to_timestamp 秒级边界：同日 23:59:59 被拦截（旧 to_date 会放行）。"""
    rules = {"date_valid": {"columns": ["order_date"], "max": "2026-01-15"}}
    df = spark_session.createDataFrame(
        [("2026-01-15",), ("2026-01-15T23:59:59",)], "order_date string"
    )
    good_df, bad_df, _, _ = RuleEngine("orders", rules, ref_data).check(df=df, spark=spark_session)
    assert good_df.count() == 1
    assert bad_df.count() == 1
    assert bad_df.select("_reasons").collect()[0]["_reasons"] == "invalid_date:order_date"


def test_spark_empty_dataframe_no_crash(ref_data, spark_session):
    """spark 路径空表不崩溃：增量批次中某表零新增行时 validate 拿到空 DataFrame。

    回归 cluster 增量测试第二批场景：customers 无新增 → 空增量表校验。
    修复前 _spark_indexed 对空 RDD 做 ``zipWithIndex().toDF()``，schema 推断
    调 ``rdd.first()`` 抛 ValueError: RDD is empty。空表须短路为零违规：
    good/bad 均空、checked=0、pass_rate=1.0（与 python/polars 空表语义一致）。
    """
    rules = {
        "completeness": {"required_columns": ["order_id"]},
        "uniqueness": {"columns": ["order_id"]},
    }
    df = spark_session.createDataFrame([], "order_id string, customer_id string")
    good_df, bad_df, stats, _ = RuleEngine("orders", rules, ref_data).check(
        df=df, spark=spark_session
    )
    assert good_df.count() == 0
    assert bad_df.count() == 0
    assert "_row_idx" not in good_df.columns and "_row_idx" not in bad_df.columns
    by_rule = {s["rule"]: s for s in stats}
    for rule in ("completeness", "uniqueness"):
        assert by_rule[rule]["checked"] == 0
        assert by_rule[rule]["passed"] == 0
        assert by_rule[rule]["pass_rate"] == 1.0
