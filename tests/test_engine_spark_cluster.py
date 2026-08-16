"""Spark 多机模式测试（cluster + S3 Parquet）。

验证 ``engine.backend="spark"`` + ``master="spark://localhost:7077"`` +
``cluster.enabled=true`` + ``storage.backend="parquet"`` + S3（MinIO）时，
五阶段产物与 ``local_csv`` 一致，多 executor 并行正确，增量+多机+S3 组合
行为正确。

前置条件：
    - Docker Spark 集群已启动（Master + 2 Worker，各 2c/2G）
    - Spark Master: spark://localhost:7077
    - MinIO: localhost:9000, bucket=autobatch


场景:
1. test_cluster_spark_s3_equivalence    — 多机+S3 全量产物与 local_csv 一致
2. test_cluster_multi_executor          — 多 executor 并行验证（Spark UI API）
3. test_cluster_incremental_spark_s3    — 增量+多机+S3 组合
4. test_cluster_worker_count            — Worker 数量 ≥ 2
"""
from __future__ import annotations

import copy
import os
import socket
import uuid
from datetime import datetime, timedelta
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

import pytest

from src.helpers import _table_exists, abs_path, csv_read, csv_write, json_load, table_read
from src.pipeline import run_pipeline


# ----------------------------------------------------------------------
# skipif 条件：Spark Master 不可达 或 pyspark 未安装
# （多机模式 Worker 在 Docker Linux 容器中运行，不需要 Windows hadoop.dll）
# ----------------------------------------------------------------------
def _spark_master_reachable(host: str = "localhost", port: int = 7077,
                            timeout: float = 3.0) -> bool:
    try:
        s = socket.socket()
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except Exception:  # noqa: BLE001
        return False


try:
    import pyspark  # noqa: F401
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

CLUSTER_SKIP = (
    not _spark_master_reachable()
    or not _PYSPARK_AVAILABLE
)

_CLUSTER_SKIP_REASON = (
    "Spark Master not reachable at localhost:7077, or pyspark not installed"
    " - cluster mode tests require Docker Spark cluster running"
)

cluster_skip = pytest.mark.skipif(CLUSTER_SKIP, reason=_CLUSTER_SKIP_REASON)


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def _run(cfg: dict[str, Any], batch_id: str, fail_at: str = "") -> tuple[int, str]:
    """跑 pipeline，返回 (rc, run_dir)。"""
    rc = run_pipeline(cfg, batch_id, fail_at)
    run_root = abs_path(cfg["pipeline"].get("run_dir", "run"))
    run_dir = os.path.join(run_root, batch_id)
    return rc, run_dir


def _new_bid(tag: str) -> str:
    """生成唯一 batch_id，前缀 test-cluster- 便于 fixture cleanup 统一清理。"""
    return f"test-cluster-{tag}-{uuid.uuid4().hex[:6]}"


def _table_rows(path: str, cfg: dict[str, Any]) -> list[dict[str, str]]:
    """读 table 为 List[Dict]；文件不存在返回空列表.

    用 table_read 路由（兼容 local_csv 和 parquet storage）。
    """
    if not _table_exists(path, cfg):
        return []
    result = table_read(path, cfg)
    # python backend 返回 (rows, fields)
    if isinstance(result, tuple):
        return result[0]
    if result is None:
        return []
    # polars DataFrame：有 height 属性
    if hasattr(result, "height"):
        return result.to_dicts() if result.height > 0 else []
    # SparkDataFrame：有 count/collect 方法，无 height
    if hasattr(result, "count") and hasattr(result, "collect"):
        return [row.asDict() for row in result.collect()]
    return []


def _table_count(path: str, cfg: dict[str, Any]) -> int:
    """table 行数；文件不存在返回 -1。"""
    if not _table_exists(path, cfg):
        return -1
    return len(_table_rows(path, cfg))


def _csv_rows(path: str) -> list[dict[str, str]]:
    """读 CSV 为 List[Dict]；文件不存在返回空列表。"""
    if not os.path.exists(path):
        return []
    rows, _ = csv_read(path)
    return rows


def _next_date(date_str: str, n: int = 1) -> str:
    """date_str + n 天，返回 'YYYY-MM-DD'。"""
    d = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=n)
    return d.strftime("%Y-%m-%d")


def _append_orders(orders_path: str, new_rows: list[dict[str, str]]) -> None:
    """向 orders.csv 追加行（保留原 header）。"""
    existing, fields = csv_read(orders_path)
    csv_write(orders_path, fields, existing + new_rows)


def _make_new_orders(n: int, start_id: int, cid: str, pid: str,
                     base_date: str, unit_price: str = "100000.00") -> list[dict[str, str]]:
    """生成 n 个合法新订单，order_date 从 base_date 开始每日递增。"""
    rows = []
    for i in range(n):
        date_str = _next_date(base_date, i)
        rows.append({
            "order_id": f"ORD-{start_id + i:08d}",
            "customer_id": cid,
            "product_id": pid,
            "order_date": date_str,
            "created_ts": date_str + "T10:00:00",
            "region": "华东",
            "channel": "web",
            "quantity": "5",
            "unit_price": unit_price,
            "status": "completed",
        })
    return rows


def _normalize_rows(rows: list[dict[str, str]], keys: list[str]) -> list[tuple]:
    """把 rows 投影到 keys 列并排序，用于无序比较。"""
    return sorted(tuple(str(r.get(k, "")) for k in keys) for r in rows)


def _make_local_csv_env(env) -> dict[str, Any]:
    """从 spark_cluster_env 派生 local_csv 配置（相同数据，storage.backend="local_csv"）."""
    cfg = copy.deepcopy(env["cfg"])
    cfg["engine"]["backend"] = "python"
    cfg["engine"]["format"] = "csv"
    cfg["storage"]["backend"] = "local_csv"
    cfg["storage"]["endpoint"] = ""
    cfg["storage"]["bucket"] = ""
    return cfg


def _get_spark_workers() -> list[dict[str, Any]]:
    """通过 Spark Master REST API 获取 Worker 列表.

    GET http://localhost:8080/json/
    返回 Master 状态 JSON，其中 workers 数组含每个 Worker 的 id / host /
    cores / memory / state 等信息。API 不可达时返回空列表。
    """
    try:
        resp = urlopen("http://localhost:8080/json/", timeout=5)
        import json
        data = json.loads(resp.read().decode("utf-8"))
        return data.get("workers", []) if isinstance(data, dict) else []
    except (URLError, OSError, Exception):  # noqa: BLE001
        return []


# ----------------------------------------------------------------------
# 场景 1: Spark 多机 + S3 Parquet 等价性测试
# ----------------------------------------------------------------------
@cluster_skip
def test_cluster_spark_s3_equivalence(spark_cluster_env):
    """Spark 多机模式 + S3 Parquet 全量 pipeline 产物应与 local_csv 完全一致.

    验证：status=success、orders_final 行数一致、daily_sales/category_stats/
    region_channel_stats/customer_value 内容一致、DQ Score 一致。

    对比方法：先跑 local_csv 获取基线，再跑 cluster 模式，对比关键产物。
    """
    env = spark_cluster_env
    cfg_cluster = env["cfg"]

    # 跑 cluster 模式全量
    bid_c = _new_bid("eq-c")
    rc_c, run_dir_c = _run(cfg_cluster, bid_c)
    assert rc_c == 0, "cluster 模式全量运行应成功"

    # 跑 local_csv 全量（相同数据，获取基线）
    cfg_csv = _make_local_csv_env(env)
    bid_l = _new_bid("eq-l")
    rc_l, run_dir_l = _run(cfg_csv, bid_l)
    assert rc_l == 0, "local_csv 全量运行应成功"

    # status 都成功
    status_c = json_load(os.path.join(run_dir_c, "status.json"))
    status_l = json_load(os.path.join(run_dir_l, "status.json"))
    assert status_c["status"] == "success", "cluster 模式 status 应为 success"
    assert status_l["status"] == "success", "local_csv 模式 status 应为 success"

    # orders_final 行数一致
    c_final = _table_count(
        os.path.join(run_dir_c, "05_output", "orders_final.csv"), cfg_cluster)
    l_final = _table_count(
        os.path.join(run_dir_l, "05_output", "orders_final.csv"), cfg_csv)
    assert c_final == l_final, \
        f"orders_final 行数 cluster={c_final} 应等于 local_csv={l_final}"

    # daily_sales 内容一致
    c_daily = _table_rows(os.path.join(run_dir_c, "04_aggregates", "daily_sales.csv"), cfg_cluster)
    l_daily = _table_rows(os.path.join(run_dir_l, "04_aggregates", "daily_sales.csv"), cfg_csv)
    daily_keys = ["order_date", "orders", "units", "revenue", "avg_order_value"]
    assert _normalize_rows(c_daily, daily_keys) == _normalize_rows(l_daily, daily_keys), \
        "daily_sales 内容 cluster 与 local_csv 不一致"

    # category_stats 内容一致
    c_cat = _table_rows(os.path.join(run_dir_c, "04_aggregates", "category_stats.csv"), cfg_cluster)
    l_cat = _table_rows(os.path.join(run_dir_l, "04_aggregates", "category_stats.csv"), cfg_csv)
    cat_keys = ["category", "orders", "units", "revenue", "revenue_share"]
    assert _normalize_rows(c_cat, cat_keys) == _normalize_rows(l_cat, cat_keys), \
        "category_stats 内容 cluster 与 local_csv 不一致"

    # region_channel_stats 内容一致
    c_rc = _table_rows(os.path.join(run_dir_c, "04_aggregates", "region_channel_stats.csv"), cfg_cluster)
    l_rc = _table_rows(os.path.join(run_dir_l, "04_aggregates", "region_channel_stats.csv"), cfg_csv)
    rc_keys = ["region", "channel", "orders", "revenue"]
    assert _normalize_rows(c_rc, rc_keys) == _normalize_rows(l_rc, rc_keys), \
        "region_channel_stats 内容 cluster 与 local_csv 不一致"

    # customer_value 内容一致
    c_cv = _table_rows(os.path.join(run_dir_c, "04_aggregates", "customer_value.csv"), cfg_cluster)
    l_cv = _table_rows(os.path.join(run_dir_l, "04_aggregates", "customer_value.csv"), cfg_csv)
    cv_keys = ["customer_id", "tier", "city", "orders", "revenue", "rank"]
    assert _normalize_rows(c_cv, cv_keys) == _normalize_rows(l_cv, cv_keys), \
        "customer_value 内容 cluster 与 local_csv 不一致"

    # DQ Score 一致
    manifest_c = json_load(os.path.join(run_dir_c, "manifest.json"))
    manifest_l = json_load(os.path.join(run_dir_l, "manifest.json"))
    dq_c = manifest_c["quality"]["dq_score"]
    dq_l = manifest_l["quality"]["dq_score"]
    assert dq_c == dq_l, \
        f"DQ Score cluster={dq_c} 应等于 local_csv={dq_l}"


# ----------------------------------------------------------------------
# 场景 2: 多 executor 并行验证
# ----------------------------------------------------------------------
@cluster_skip
def test_cluster_multi_executor(spark_cluster_env):
    """验证 Spark 多机模式下 2+ executor 在线.

    通过 Spark Master REST API（http://localhost:8080/api/v1/workers）
    获取 Worker 列表，断言至少 2 个 Worker 处于 ALIVE 状态。
    """
    workers = _get_spark_workers()
    assert len(workers) >= 2, \
        f"应有至少 2 个 Worker，实际 {len(workers)}"

    # 检查至少 2 个 Worker 状态为 ALIVE（JSON API 字段名为 "state"）
    alive_count = sum(1 for w in workers if w.get("state") == "ALIVE")
    assert alive_count >= 2, \
        f"应有至少 2 个 ALIVE Worker，实际 {alive_count}"

    # 打印 Worker 信息（信息性）
    for w in workers:
        print("Worker: id={}, host={}, cores={}, memory={}, state={}".format(
            w.get("id", "?"), w.get("host", "?"), w.get("cores", "?"),
            w.get("memory", "?"), w.get("state", "?")))


# ----------------------------------------------------------------------
# 场景 3: 增量 + Spark 多机 + S3 Parquet 组合测试
# ----------------------------------------------------------------------
@cluster_skip
def test_cluster_incremental_spark_s3(spark_cluster_env):
    """incremental.enabled=true + engine.backend="spark" + cluster.enabled=true +
    storage.backend="parquet" 组合行为正确.

    验证：
    - 首次运行成功，state.json 生成且水位正确
    - 追加新数据后二次运行成功，只处理新增行（orders_incremental 行数 = 新增数）
    - 水位推进到新 max
    """
    env = spark_cluster_env
    cfg = env["cfg"]
    work_dir = env["work_dir"]

    # 开启增量，state_dir 放在 work_dir/state（同盘隔离）
    state_dir = os.path.join(work_dir, "state")
    cfg["incremental"]["enabled"] = True
    cfg["incremental"]["state_dir"] = state_dir

    # --- 首次运行（全量建立水位）---
    bid1 = _new_bid("inc-1")
    rc1, run_dir1 = _run(cfg, bid1)
    assert rc1 == 0, "首次增量+cluster 运行应成功"

    # state.json 生成
    state_path = os.path.join(state_dir, "state.json")
    assert os.path.exists(state_path), "state.json 应生成"
    state1 = json_load(state_path)
    assert "tables" in state1
    assert "orders" in state1["tables"]

    # 首次水位 = max(order_date)
    expected_orders_wm = max(r["order_date"] for r in _csv_rows(env["orders_path"]))
    assert state1["tables"]["orders"]["watermark_value"] == expected_orders_wm, \
        "首次 orders 水位应为 max(order_date)"

    # --- 追加新数据后二次运行 ---
    cust_rows = _csv_rows(env["customers_path"])
    prod_rows = _csv_rows(env["products_path"])
    cid = cust_rows[0]["customer_id"]
    pid = prod_rows[0]["product_id"]

    n_new = 10
    base_date = _next_date(expected_orders_wm)
    new_orders = _make_new_orders(n_new, start_id=100001, cid=cid, pid=pid,
                                  base_date=base_date, unit_price="100000.00")
    _append_orders(env["orders_path"], new_orders)

    bid2 = _new_bid("inc-2")
    rc2, run_dir2 = _run(cfg, bid2)
    assert rc2 == 0, "追加新数据后增量+cluster 运行应成功"

    # orders_incremental.csv 只含新增行
    inc_csv2 = os.path.join(run_dir2, "01_raw", "orders_incremental.csv")
    # Spark 写出的是目录（多分区 part-00000-* 文件），需要汇总。
    # cluster+S3 模式下数据在 S3（Parquet），用 _table_count 统一 IO 层
    # （兼容 local_csv 和 parquet storage），_csv_count_spark_dir 只能读本地。
    inc_count = _table_count(inc_csv2, cfg)
    assert inc_count == n_new, \
        f"orders_incremental.csv 应只含新增 {n_new} 行，实际 {inc_count}"

    # 水位推进到新 max
    state2 = json_load(state_path)
    expected_new_wm = max(o["order_date"] for o in new_orders)
    assert state2["tables"]["orders"]["watermark_value"] == expected_new_wm, \
        "水位应推进到新 max"


def _csv_count_spark_dir(path: str) -> int:
    """CSV 数据行数（兼容 Spark 写出的目录格式）.

    Spark 写出的是目录（多分区 part-00000-* 文件），若 path 是目录则
    汇总目录下所有 part-*.csv 的行数；否则按普通 CSV 文件处理。
    """
    if os.path.isdir(path):
        total = 0
        for fn in os.listdir(path):
            if fn.startswith("part-") and fn.endswith(".csv"):
                rows, _ = csv_read(os.path.join(path, fn))
                total += len(rows)
        return total
    if not os.path.exists(path):
        return -1
    rows, _ = csv_read(path)
    return len(rows)


# ----------------------------------------------------------------------
# 场景 4: Worker 数量验证
# ----------------------------------------------------------------------
@cluster_skip
def test_cluster_worker_count(spark_cluster_env):
    """验证 Spark 集群 Worker 数量 ≥ 2.

    通过 Spark Master REST API 获取 Worker 列表并计数。
    备选方案：通过 SparkSession 的 executor 数量验证。
    """
    workers = _get_spark_workers()
    alive_workers = [w for w in workers if w.get("state") == "ALIVE"]
    assert len(alive_workers) >= 2, \
        f"ALIVE Worker 数量应 ≥ 2，实际 {len(alive_workers)}"

    # 验证每个 Worker 的核心数和内存（信息性断言）
    for w in alive_workers:
        cores = w.get("cores", 0)
        memory = w.get("memory", 0)
        assert cores >= 2, \
            "Worker {} 核心数应 ≥ 2，实际 {}".format(w.get("id", "?"), cores)
        # memory 单位为 MB，2G = 2048MB
        assert memory >= 2048, \
            "Worker {} 内存应 ≥ 2048MB，实际 {}MB".format(w.get("id", "?"), memory)
