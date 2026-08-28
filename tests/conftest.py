"""共享 fixture：端到端批次、固定小数据、链式 ctx。

注意：pytest 默认 tmp_path 位于系统临时目录，而项目根 ROOT 可能在不同盘符
（Windows）或不同挂载点（WSL/macOS）。src/stages/ingest.py 使用
os.path.relpath(dst, ROOT) 计算相对路径，跨盘时会抛 ValueError。为绕过该
src 限制，本 conftest 将测试临时目录强制创建在项目所在驱动器/挂载点上，
确保 dst 与 ROOT 同盘/同挂载点。
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import tempfile
import uuid
from typing import Any

import pytest

from src.helpers import (
    ROOT,
    PipelineContext,
    StageLog,
    abs_path,
    apply_spark_env,
    csv_write,
    detect_spark_paths,
    json_load,
    json_save,
    rmtree_retry,
)
from src.lineage import Manifest
from src.pipeline import config_digest, run_pipeline

# ----------------------------------------------------------------------
# JVM 启动参数：必须在任何 JVM 启动之前设置（模块级）
# ----------------------------------------------------------------------
# WSL2 + Docker Desktop 反向连接修复：Driver JVM 默认把 RPC 端口绑到 IPv6
# dual-stack socket（ss 显示 *:port），而 WSL2 localhost forwarding 只把
# IPv4 listener 转发到 Windows 宿主——容器内 executor 经 host.docker.internal
# 回连 Driver 被 refused → exit(1) 崩溃循环 → "Initial job has not accepted any
# resources"（2026-08-28 实测对照：IPv6 dual-stack listener refused，同机制
# IPv4 listener OK）。preferIPv4Stack=true 使 Driver 全部 socket 绑 IPv4。
# executor 侧 JVM 在容器内、不受本变量影响（其自带 preferIPv6Addresses=false，
# host.docker.internal 优先解析 IPv4 192.168.65.254，连接方向无需改动）。
#
# 为什么必须放模块级而不是 fixture 内：PySpark 的 JVM gateway 是进程级单例
# （SparkContext._gateway 只 launch 一次），JVM -D 参数只在启动时生效。
# tests/test_engine_spark_cluster.py 的收集期门禁 _jvm_available() 会在模块
# import 时启动 local[1] SparkContext——那是 pytest 进程最早的 JVM 启动点；
# 在 fixture 内才设置环境变量时 JVM 已启动、参数不生效（2026-08-28 实测：
# fixture 内设置导致连续两个 app 的 executor 全部 exit(1) 崩溃循环）。
# conftest.py 先于所有测试模块 import，放这里保证对任何 JVM 启动点生效。
_jto = os.environ.get("JAVA_TOOL_OPTIONS", "")
if "preferIPv4Stack" not in _jto:
    os.environ["JAVA_TOOL_OPTIONS"] = (_jto + " -Djava.net.preferIPv4Stack=true").strip()


# ----------------------------------------------------------------------
# 命令行选项：--runslow（任务73 / #69审查H3 修复）
# ----------------------------------------------------------------------
def pytest_addoption(parser):
    """注册 --runslow 命令行选项，用于显式启用基准/慢速测试.

    选项必须定义在 conftest.py（会话级插件入口）才能对任意 pytest 调用生效。
    此前该选项定义在 tests/test_benchmark.py 模块内，只有该模块被收集时才注册，
    导致单文件运行（如 pytest tests/test_stages.py --runslow）报
    "unrecognized arguments: --runslow"。
    """
    parser.addoption(
        "--runslow",
        action="store_true",
        default=False,
        help="显式启用基准测试（tests/test_benchmark.py，默认跳过，因为耗时长）",
    )


def pytest_collection_modifyitems(config, items):
    """基准测试默认跳过的统一控制点（任务73 / #69审查H3 修复）.

    tests/test_benchmark.py 内的用例不再挂无条件 @pytest.mark.skip 装饰器，
    收集阶段在此统一打 skip 标记，保持单一机制：
      - 未传 --runslow：跳过全部基准用例（默认回归计数 18 skipped 含此 6 项）
      - 传了 --runslow：用例真正执行（环境缺失时用例内部自行按 skipif 兜底）
    """
    if config.getoption("--runslow", default=False):
        return
    skip_benchmark = pytest.mark.skip(reason="基准测试默认跳过，显式传 --runslow 启用")
    for item in items:
        if item.path.name == "test_benchmark.py":
            item.add_marker(skip_benchmark)


# 会话内创建过的沙箱根目录（_same_drive_tmp_root），sessionfinish 时二次清扫.
# teardown 单次 rmtree 可能因 Windows 句柄延迟释放失败，这里兜底再删一遍，
# 避免盘根积累 autobatch_test_* 残留。
_SANDBOX_ROOTS: list[str] = []


def _sweep_stale_sandboxes() -> None:
    """清扫上次会话残留的 autobatch_test_* 沙箱目录.

    只删 mtime 超过 1 小时的：活跃会话的沙箱在持续写入，近期 mtime 视为
    在用（防止并发 pytest 会话互删）；超龄的一定是崩溃/句柄残留，此时
    持有进程早已退出，rmtree_retry 必然成功——自愈闭环。
    """
    import contextlib
    import time

    if os.name == "nt":
        base = os.path.splitdrive(ROOT)[0] + os.sep
    else:
        base = os.path.dirname(ROOT)
    with contextlib.suppress(Exception):
        for name in os.listdir(base):
            if not name.startswith("autobatch_test_"):
                continue
            path = os.path.join(base, name)
            with contextlib.suppress(Exception):
                if time.time() - os.path.getmtime(path) < 3600:
                    continue
                rmtree_retry(path, attempts=2, base_delay=0.2)


def pytest_sessionstart(session):
    _sweep_stale_sandboxes()


def pytest_sessionfinish(session, exitstatus):
    import contextlib

    for d in _SANDBOX_ROOTS:
        with contextlib.suppress(Exception):
            rmtree_retry(d, attempts=6, base_delay=0.5)
    _SANDBOX_ROOTS.clear()


# ----------------------------------------------------------------------
# 跨平台 Spark / Hadoop 路径自动探测
# （实现已下沉到 src.helpers.detect_spark_paths/apply_spark_env，
#   与 tools/bench_scale_cluster.py 共用同一份；此处仅保留 fixture
#   需要的"设置并快照旧环境"包装）
# ----------------------------------------------------------------------
def _build_spark_env(spark_paths: dict[str, str]) -> dict[str, str]:
    """按探测结果设置环境变量，返回旧环境快照供 fixture teardown 还原."""
    old_env = dict(os.environ)
    apply_spark_env(spark_paths)
    return old_env


# 精确 batch_id cleanup 机制
# ----------------------------------------------------------------------
# 设计：每个 env fixture 维护一个 created_batch_ids 列表，通过包装 run_pipeline
# 自动捕获测试创建的 batch_id。cleanup 优先按精确列表清理，列表为空时 fallback
# 到 prefix 过滤（向后兼容，避免遗漏未捕获的批次）.
#
# 与原 prefix 过滤（name.startswith("test-xxx-")）的区别：
# - 原方案会清理所有以 prefix 开头的目录，包括其他并发 pytest 进程的同 prefix 目录
# - 新方案只清理本 fixture 实际创建的 batch_id，不误删其他进程的临时目录
# - fallback 仅在列表为空时启用（向后兼容，不应在正常路径触发）
_BATCH_ID_PREFIXES = (
    "test-inc-",
    "test-polars-",
    "test-cluster-",
    "test-parquet-",
    "test-s3-",
    "test-iceberg-",
    "test-spark-",
    "test-spark-iceberg-",
    "test-e2e-",
    "test-errhand-",
)


def _cleanup_run_dir(
    run_root: str, created_batch_ids: list[str], prefix: str | None = None
) -> None:
    """按精确 batch_id 列表清理 run_dir.

    优先清理 created_batch_ids 中的批次（精确清理，不误删其他进程目录）.
    若列表为空且 prefix 提供，fallback 到 prefix 过滤（向后兼容）.
    """
    if not os.path.isdir(run_root):
        return
    if created_batch_ids:
        # 精确清理：只删列表中的 batch_id
        for bid in created_batch_ids:
            shutil.rmtree(os.path.join(run_root, bid), ignore_errors=True)
        return
    # fallback：prefix 过滤（向后兼容，仅在未捕获到 batch_id 时启用）
    if prefix:
        for name in os.listdir(run_root):
            if name.startswith(prefix):
                shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)


def _make_run_wrapper(real_run_pipeline, created_batch_ids: list[str]):
    """包装 run_pipeline，自动捕获 batch_id 到 created_batch_ids 列表.

    返回的 wrapper 签名与 run_pipeline 完全一致，仅在调用前把 batch_id
    append 到列表.测试通过 env["run"] 调用即可享受精确清理.
    """

    def _wrapped(cfg, batch_id, fail_at="", **kwargs):
        created_batch_ids.append(batch_id)
        return real_run_pipeline(cfg, batch_id, fail_at, **kwargs)

    return _wrapped


SAMPLE_ORDERS: list[dict[str, str]] = [
    {
        "order_id": "ORD-00000001",
        "customer_id": "CUS-000001",
        "product_id": "PRD-000001",
        "order_date": "2026-01-15",
        "created_ts": "2026-01-15T10:00:00",
        "region": "华东",
        "channel": "web",
        "quantity": "5",
        "unit_price": "100.00",
        "status": "completed",
    },
    {
        "order_id": "ORD-00000002",
        "customer_id": "CUS-000002",
        "product_id": "PRD-000002",
        "order_date": "2026-02-20",
        "created_ts": "2026-02-20T11:30:00",
        "region": "华北",
        "channel": "app",
        "quantity": "3",
        "unit_price": "50.50",
        "status": "pending",
    },
    {
        "order_id": "ORD-00000003",
        "customer_id": "CUS-000001",
        "product_id": "PRD-000001",
        "order_date": "2026-03-10",
        "created_ts": "2026-03-10T09:15:00",
        "region": "华南",
        "channel": "store",
        "quantity": "10",
        "unit_price": "25.00",
        "status": "cancelled",
    },
]
SAMPLE_CUSTOMERS: list[dict[str, str]] = [
    {"customer_id": "CUS-000001", "tier": "gold", "city": "上海", "join_date": "2022-06-19"},
    {"customer_id": "CUS-000002", "tier": "silver", "city": "北京", "join_date": "2023-01-15"},
]
SAMPLE_PRODUCTS: list[dict[str, str]] = [
    {"product_id": "PRD-000001", "name": "数码-商品001", "category": "数码", "cost": "100.00"},
    {"product_id": "PRD-000002", "name": "服饰-商品002", "category": "服饰", "cost": "50.00"},
]
ORDER_FIELDS = list(SAMPLE_ORDERS[0].keys())
CUSTOMER_FIELDS = list(SAMPLE_CUSTOMERS[0].keys())
PRODUCT_FIELDS = list(SAMPLE_PRODUCTS[0].keys())


def _good_order(oid="ORD-00000001", cid="CUS-000001", pid="PRD-000001"):
    return {
        "order_id": oid,
        "customer_id": cid,
        "product_id": pid,
        "order_date": "2026-01-15",
        "created_ts": "2026-01-15T10:00:00",
        "region": "华东",
        "channel": "web",
        "quantity": "5",
        "unit_price": "100.00",
        "status": "completed",
    }


def _load_small_config():
    return json_load(abs_path("config/pipeline_small.json"))


def _make_log(run_dir, name):
    return StageLog(os.path.join(run_dir, "logs", name + ".jsonl"))


@pytest.fixture(scope="session")
def _same_drive_tmp_root():
    """在项目所在驱动器/挂载点上创建临时目录基，避免跨盘 os.path.relpath 失败。

    Windows: 确保 tmp 与 ROOT 同盘（如都在 F:）。
    POSIX (Linux/macOS/WSL): 用项目父目录——与 ROOT 同一挂载点，且运行测试的
    用户必然可写（CI 的非 root runner 对 "/" 无写权限，不能用 splitdrive
    退化出的 "/" 作为 mkdtemp 目录）；父目录不可写时回退系统默认 tmp。
    """
    if os.name == "nt":
        base = os.path.splitdrive(ROOT)[0] + os.sep
    else:
        base = os.path.dirname(ROOT)
        if not os.access(base, os.W_OK):
            base = None  # 回退 tempfile 默认目录
    d = tempfile.mkdtemp(prefix="autobatch_test_", dir=base)
    _SANDBOX_ROOTS.append(d)
    yield d
    # 重试删除：SQLite/JVM 句柄延迟释放常让单次 rmtree 失败（残留根因）.
    rmtree_retry(d)


@pytest.fixture(scope="session")
def small_batch_dir(_same_drive_tmp_root):
    tmp = tempfile.mkdtemp(prefix="autobatch_e2e_", dir=_same_drive_tmp_root)
    cfg = _load_small_config()
    data_dir = os.path.join(tmp, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    from src.generator import main as gen_main

    gen_main(cfg)
    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    # src/output.py 的 _register_edges 硬编码 prefix="run/<batch_id>/"，要求
    # run_dir 必须位于 ROOT/run/<batch_id> 下，否则 os.path.relpath(fp, ROOT)
    # 生成的 artifact key 与 prefix 不匹配，lineage edges 全部被跳过。因此将
    # run_root 固定到 ROOT/run，并使用唯一 batch_id 避免与真实批次冲突。
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root
    cfg["generator"]["enabled"] = False
    batch_id = "test-e2e-" + uuid.uuid4().hex[:6]
    rc = run_pipeline(cfg, batch_id, "")
    assert rc == 0, "端到端流水线应成功"
    run_dir = os.path.join(run_root, batch_id)
    yield run_dir
    shutil.rmtree(run_dir, ignore_errors=True)


@pytest.fixture
def small_config():
    return _load_small_config()


@pytest.fixture
def orders_rules(small_config):
    return small_config["quality"]["rules"]["orders"]


@pytest.fixture
def sample_customers():
    return [dict(r) for r in SAMPLE_CUSTOMERS]


@pytest.fixture
def sample_products():
    return [dict(r) for r in SAMPLE_PRODUCTS]


@pytest.fixture
def ref_data(sample_customers, sample_products):
    return {"customers": sample_customers, "products": sample_products}


@pytest.fixture
def good_order():
    return _good_order


@pytest.fixture
def base_ctx(_same_drive_tmp_root):
    tmp = tempfile.mkdtemp(prefix="fixed_batch_", dir=_same_drive_tmp_root)
    data_dir = os.path.join(tmp, "data", "raw")
    run_root = os.path.join(tmp, "run")
    run_dir = os.path.join(run_root, "fixed-batch")
    os.makedirs(run_dir, exist_ok=True)
    csv_write(os.path.join(data_dir, "orders.csv"), ORDER_FIELDS, SAMPLE_ORDERS)
    csv_write(os.path.join(data_dir, "customers.csv"), CUSTOMER_FIELDS, SAMPLE_CUSTOMERS)
    csv_write(os.path.join(data_dir, "products.csv"), PRODUCT_FIELDS, SAMPLE_PRODUCTS)
    cfg = _load_small_config()
    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["pipeline"]["run_dir"] = run_root
    cfg["generator"]["enabled"] = False
    manifest = Manifest("fixed-batch", config_digest(cfg), run_dir)
    return PipelineContext(config=cfg, run_dir=run_dir, batch_id="fixed-batch", manifest=manifest)


@pytest.fixture
def ingested_ctx(base_ctx):
    from src.stages import ingest

    with _make_log(base_ctx.run_dir, "ingest") as log:
        ingest.run(base_ctx, log)
    return base_ctx


@pytest.fixture
def validated_ctx(ingested_ctx):
    from src.stages import validate

    with _make_log(ingested_ctx.run_dir, "validate") as log:
        validate.run(ingested_ctx, log)
    return ingested_ctx


@pytest.fixture
def cleaned_ctx(validated_ctx):
    from src.stages import clean

    with _make_log(validated_ctx.run_dir, "clean") as log:
        clean.run(validated_ctx, log)
    return validated_ctx


@pytest.fixture
def computed_ctx(cleaned_ctx):
    from src.stages import compute

    with _make_log(cleaned_ctx.run_dir, "compute") as log:
        compute.run(cleaned_ctx, log)
    return cleaned_ctx


# ----------------------------------------------------------------------
# 增量模式 fixture（Phase 1，见 docs/evolution.md §3.3.1~§3.3.6）
# ----------------------------------------------------------------------
@pytest.fixture
def inc_config_path(_same_drive_tmp_root):
    """增量模式配置文件路径。

    复制 pipeline_small.json 到同盘临时目录，把 incremental.enabled 改为 true，
    state_dir 指向 <work_dir>/state。返回配置文件路径。

    用同盘临时目录而非 pytest 默认 tmp_path，因为 src/stages/ingest.py 使用
    os.path.relpath(dst, ROOT) 计算相对路径，跨盘会抛 ValueError（见本文件
    顶部注释）。state_dir 也放在同盘，避免任何潜在问题。
    """
    work_dir = tempfile.mkdtemp(prefix="autobatch_inc_", dir=_same_drive_tmp_root)
    state_dir = os.path.join(work_dir, "state")

    cfg = _load_small_config()
    cfg["incremental"]["enabled"] = True
    cfg["incremental"]["state_dir"] = state_dir

    cfg_path = os.path.join(work_dir, "pipeline_inc.json")
    json_save(cfg_path, cfg)

    yield cfg_path

    shutil.rmtree(work_dir, ignore_errors=True)


@pytest.fixture
def inc_env(inc_config_path, request):
    """增量测试环境：生成小规模数据 + 设置 source.files + run_dir。

    在 inc_config_path 基础上调用 generator 生成 data/raw/{orders,customers,products}.csv，
    把 source.files 指向生成文件，run_dir 指向 ROOT/run（output.py 的 _register_edges
    硬编码 prefix="run/<batch_id>/"，要求 run_dir 位于 ROOT/run 下）。

    返回 dict:
        cfg            — 已配置好的配置 dict（可直接传给 run_pipeline）
        cfg_path       — 配置文件路径
        state_dir      — state 目录（绝对路径）
        work_dir       — 工作目录（绝对路径，同盘）
        data_dir       — data/raw 目录
        run_root       — run 根目录（ROOT/run）
        orders_path    — orders.csv 路径
        customers_path — customers.csv 路径
        products_path  — products.csv 路径

    cleanup: 测试结束后清理本 fixture 创建的 test-inc-* run_dir。
    """
    cfg_path = inc_config_path
    cfg = json_load(cfg_path)
    state_dir = cfg["incremental"]["state_dir"]
    work_dir = os.path.dirname(state_dir)

    # 生成小规模数据（隔离在 work_dir/data/raw 下，避免污染项目 data/）
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    # 增量测试需要水位是合法日期：ingest._compute_watermark 用字符串比较
    # max(order_date)，若含坏日期（如 'not-a-date'）会污染水位，导致后续增量
    # 无法追加合法新订单（合法日期 'YYYY-MM-DD' < 'not-a-date'）。关闭 bad_date
    # 缺陷，保留其他缺陷（missing/duplicate/negative_qty/...）以维持 DQ 校验。
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main

    gen_main(cfg)

    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root

    # cleanup: 清理本测试创建的 test-inc-* run_dir（pytest 默认串行，安全）
    def _cleanup():
        if os.path.isdir(run_root):
            for name in os.listdir(run_root):
                if name.startswith("test-inc-"):
                    shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)

    request.addfinalizer(_cleanup)

    return {
        "cfg": cfg,
        "cfg_path": cfg_path,
        "state_dir": state_dir,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "run_root": run_root,
        "orders_path": os.path.join(data_dir, "orders.csv"),
        "customers_path": os.path.join(data_dir, "customers.csv"),
        "products_path": os.path.join(data_dir, "products.csv"),
    }


# ----------------------------------------------------------------------
# Polars backend fixture（Phase 2a，见 docs/evolution.md §4.7.1）
# ----------------------------------------------------------------------
@pytest.fixture
def polars_env(_same_drive_tmp_root, request):
    """Polars backend 测试环境：生成数据 + config backend=polars.

    在同盘临时目录下复制 pipeline_small.json，把 engine.backend 改为 "polars"，
    调用 generator 生成 data/raw/{orders,customers,products}.csv，把 source.files
    指向生成文件，run_dir 指向 ROOT/run（output.py 的 _register_edges 硬编码
    prefix="run/<batch_id>/"，要求 run_dir 位于 ROOT/run 下）。

    返回 dict:
        cfg            — 已配置好的配置 dict（engine.backend="polars"，可直接传给 run_pipeline）
        cfg_path       — 配置文件路径
        work_dir       — 工作目录（绝对路径，同盘）
        data_dir       — data/raw 目录
        run_root       — run 根目录（ROOT/run）
        orders_path    — orders.csv 路径
        customers_path — customers.csv 路径
        products_path  — products.csv 路径

    cleanup: 测试结束后清理本 fixture 创建的 test-polars-* run_dir。
    """
    work_dir = tempfile.mkdtemp(prefix="autobatch_polars_", dir=_same_drive_tmp_root)

    cfg = _load_small_config()
    # 关键：开启 Polars backend
    cfg["engine"]["backend"] = "polars"
    # format 保持 csv（与 python backend 产物路径一致，便于 _advance_and_merge 用 load_csv 读）
    cfg["engine"]["format"] = "csv"

    # 生成小规模数据（隔离在 work_dir/data/raw 下，避免污染项目 data/）
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    # 与 inc_env 一致：关闭 bad_date 缺陷，避免污染水位（增量测试复用本 fixture 时需要）
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main

    gen_main(cfg)

    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root

    # 持久化配置文件（部分测试需要从文件加载）
    cfg_path = os.path.join(work_dir, "pipeline_polars.json")
    json_save(cfg_path, cfg)

    # cleanup: 清理本测试创建的 test-polars-* run_dir（pytest 默认串行，安全）
    def _cleanup():
        if os.path.isdir(run_root):
            for name in os.listdir(run_root):
                if name.startswith("test-polars-"):
                    shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)

    request.addfinalizer(_cleanup)

    return {
        "cfg": cfg,
        "cfg_path": cfg_path,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "run_root": run_root,
        "orders_path": os.path.join(data_dir, "orders.csv"),
        "customers_path": os.path.join(data_dir, "customers.csv"),
        "products_path": os.path.join(data_dir, "products.csv"),
    }


# ----------------------------------------------------------------------
# Spark 多机模式 fixture（cluster + S3 Parquet）
# ----------------------------------------------------------------------
def _spark_master_reachable(
    host: str = "localhost", port: int = 15077, timeout: float = 3.0
) -> bool:
    """检查 Spark Master 是否可达（socket 连接测试）."""
    import socket

    try:
        s = socket.socket()
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except Exception:  # noqa: BLE001
        return False


SPARK_MASTER_REACHABLE = _spark_master_reachable()


@pytest.fixture
def spark_cluster_env(_same_drive_tmp_root, request):
    """Spark 多机模式 + S3 Parquet 测试环境.

    前置条件：Docker Spark 集群已启动（spark://localhost:15077），
    MinIO 在 localhost:9000 可用。

    配置要点：
    - engine.backend="spark" + master="spark://localhost:15077"
    - engine.spark.cluster.enabled=true + driver_host="host.docker.internal"
    - engine.spark.cluster.s3_endpoint="localhost:9000"（Worker 通过容器内 socat 代理访问 MinIO）
    - storage.backend="parquet" + endpoint="localhost:9000"（Driver 端 pyarrow 用此地址）
    - storage.bucket="autobatch" + access_key/secret_key="minioadmin"

    返回 dict:
        cfg            — 已配置好的配置 dict（可直接传给 run_pipeline）
        cfg_path       — 配置文件路径
        work_dir       — 工作目录（绝对路径，同盘）
        data_dir       — data/raw 目录
        run_root       — run 根目录（ROOT/run）
        s3_prefix      — S3 测试前缀（用于 cleanup）
        orders_path    — orders.csv 路径
        customers_path — customers.csv 路径
        products_path  — products.csv 路径

    cleanup: 测试结束后清理 test-cluster-* run_dir + MinIO 对应前缀数据 + 还原环境变量。
    """
    if not _spark_master_reachable():
        pytest.skip("Spark Master not reachable at localhost:15077")

    # 多机模式：PYSPARK_PYTHON 是 Worker 端 Python 路径（Docker 容器内为
    # python3）。必须在 detect_spark_paths/_build_spark_env 之前占位——否则
    # 探测到的宿主机 Windows 路径会被 apply_spark_env 抢先注入 os.environ，
    # 并在 SparkContext 创建时快照为 ctx.pythonExec（pyspark context.py:
    # "self.pythonExec = os.environ.get('PYSPARK_PYTHON', 'python3')"）打包进
    # 每个 PythonFunction 分发到容器内 executor；该 Windows 路径在 Linux 容器
    # 中不存在 → "Cannot run program ... error=2" 任务级失败。
    os.environ.setdefault("PYSPARK_PYTHON", "python3")
    # 跨平台探测 Spark/Hadoop 路径（环境变量优先，系统路径次之，Windows 回退）
    spark_paths = detect_spark_paths()
    old_env = _build_spark_env(spark_paths)
    # JAVA_TOOL_OPTIONS（preferIPv4Stack）已在模块级设置——必须早于收集期
    # JVM 启动，fixture 内再设为时已晚，见文件头部注释。
    # PYSPARK_DRIVER_PYTHON 是 Driver 端 Python 路径（宿主机路径）。detect 链条
    # 会把 DRIVER 缺省为 worker 侧的 "python3" 占位值，因此以进入本 fixture 前
    # 的外部配置（old_env 快照）为准：外部显式设置则尊重，未设置则回指宿主机
    # 解释器（Windows 上无 python3 命令，不能沿用占位值）。
    # 兜底值须过滤 cmd 不安全字符（空格/括号等）：PySpark bin\*.cmd 批处理展开
    # %PYSPARK_DRIVER_PYTHON% 时会被 "(x86)" 截断 if/for 块直接崩溃；此时回退
    # 裸命令名交由 PATH 解析。
    if not old_env.get("PYSPARK_DRIVER_PYTHON"):
        _driver_py = shutil.which("python") or ""
        if any(_c in _driver_py for _c in " ()&^%!"):
            _driver_py = "python"
        os.environ["PYSPARK_DRIVER_PYTHON"] = _driver_py

    work_dir = tempfile.mkdtemp(prefix="autobatch_cluster_", dir=_same_drive_tmp_root)

    cfg = _load_small_config()
    # 关键：Spark backend + 多机模式
    cfg["engine"]["backend"] = "spark"
    cfg["engine"]["format"] = "csv"
    cfg["engine"]["spark"]["master"] = "spark://localhost:15077"
    cfg["engine"]["spark"]["shuffle_partitions"] = 4
    # executor_memory 必须适配 docker-compose.yml 的 SPARK_WORKER_MEMORY（2g）：
    # Spark 调度按 executor 内存 + overhead（缺省 max(384MB, 10%)）占用 worker，
    # 2g executor 需 ~2.4g > 2g worker → 永远无法调度（"Initial job has not
    # accepted any resources" 直到超时）。1g + 384MB = 1.4g ≤ 2g 可放下。
    cfg["engine"]["spark"]["executor_memory"] = "1g"
    cfg["engine"]["spark"]["executor_cores"] = 2
    cfg["engine"]["spark"]["num_executors"] = 2
    cfg["engine"]["spark"]["driver_memory"] = "4g"
    # 多机模式：Driver ↔ Worker 反向连接
    # driver_host 是容器内 executor 回连 Driver 用的地址。Docker Desktop
    # （Windows/macOS）与 WSL2 场景容器内均有 host.docker.internal DNS：
    # WSL2 localhost forwarding 只把 IPv4 listener 转发到 Windows 宿主，
    # host.docker.internal 指向 Windows 宿主侧（192.168.65.254），链路为
    # 容器 → Windows 宿主 → 转发到 WSL IPv4 listener。原生 Linux 保留
    # localhost 旧行为（Driver 在容器外时实际需宿主桥接 IP，用
    # SPARK_CLUSTER_DRIVER_HOST 覆盖）。2026-08-28 实测：WSL2 走 "localhost"
    # 缺省时 executor 在容器内连自己的 loopback → refused → exit(1) 崩溃循环。
    _is_wsl = "microsoft" in platform.release().lower()
    _driver_host = os.environ.get("SPARK_CLUSTER_DRIVER_HOST") or (
        "host.docker.internal"
        if platform.system() in ("Windows", "Darwin") or _is_wsl
        else "localhost"
    )
    cfg["engine"]["spark"]["cluster"] = {
        "enabled": True,
        "driver_host": _driver_host,
        "s3_endpoint": "localhost:9000",  # Worker 通过容器内 socat 代理访问 MinIO
    }

    # 关键：Parquet storage + S3（MinIO）
    cfg["storage"]["backend"] = "parquet"
    s3_prefix = "test-cluster-" + uuid.uuid4().hex[:8]
    cfg["storage"]["prefix"] = s3_prefix
    cfg["storage"]["bucket"] = "autobatch"
    cfg["storage"]["endpoint"] = "localhost:9000"  # Driver 端 pyarrow 用此地址
    cfg["storage"]["access_key"] = "minioadmin"
    cfg["storage"]["secret_key"] = "minioadmin"
    cfg["storage"]["secure"] = False
    cfg["storage"]["region"] = "us-east-1"
    cfg["storage"]["warehouse"] = "warehouse"
    cfg["storage"]["compression"] = "zstd"

    # 生成小规模数据（隔离在 work_dir/data/raw 下，避免污染项目 data/）
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    # 关闭 bad_date 缺陷，避免污染水位（增量测试复用本 fixture 时需要）
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main

    gen_main(cfg)

    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root

    # 持久化配置文件
    cfg_path = os.path.join(work_dir, "pipeline_cluster.json")
    json_save(cfg_path, cfg)

    # cleanup: 清理 test-cluster-* run_dir + MinIO 数据 + 还原环境变量
    def _cleanup():
        if os.path.isdir(run_root):
            for name in os.listdir(run_root):
                if name.startswith("test-cluster-"):
                    shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)
        # 清理 MinIO 中本测试前缀的数据
        try:
            from minio import Minio

            client = Minio(
                "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
            )
            prefix_to_clean = s3_prefix + "/"
            objects = list(client.list_objects("autobatch", prefix=prefix_to_clean, recursive=True))
            for obj in objects:
                client.remove_object("autobatch", obj.object_name)
        except Exception:  # noqa: BLE001
            pass  # MinIO 不可用时忽略清理错误
        # 还原环境变量
        os.environ.clear()
        os.environ.update(old_env)

    request.addfinalizer(_cleanup)

    return {
        "cfg": cfg,
        "cfg_path": cfg_path,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "run_root": run_root,
        "s3_prefix": s3_prefix,
        "orders_path": os.path.join(data_dir, "orders.csv"),
        "customers_path": os.path.join(data_dir, "customers.csv"),
        "products_path": os.path.join(data_dir, "products.csv"),
    }


# ----------------------------------------------------------------------
# Phase 3 Parquet storage fixture（见 docs/evolution.md §5.5 / §5.8.1）
# ----------------------------------------------------------------------
@pytest.fixture
def parquet_env(_same_drive_tmp_root, request):
    """本地 Parquet storage 测试环境：生成数据 + config storage.backend="parquet".

    在同盘临时目录下复制 pipeline_small.json，把 storage.backend 改为 "parquet"，
    warehouse 指向本地临时目录（确保五阶段产物写到本地 .parquet 文件）。
    调用 generator 生成 data/raw/{orders,customers,products}.csv，把 source.files
    指向生成文件，run_dir 指向 ROOT/run（output.py 的 _register_edges 硬编码
    prefix="run/<batch_id>/"，要求 run_dir 位于 ROOT/run 下）。

    返回 dict:
        cfg            — 已配置好的配置 dict（storage.backend="parquet"，可直接传给 run_pipeline）
        cfg_path       — 配置文件路径
        work_dir       — 工作目录（绝对路径，同盘）
        data_dir       — data/raw 目录
        run_root       — run 根目录（ROOT/run）
        warehouse_dir  — 本地 parquet warehouse 目录
        orders_path    — orders.csv 路径
        customers_path — customers.csv 路径
        products_path  — products.csv 路径

    cleanup: 测试结束后清理本 fixture 创建的 test-parquet-* run_dir。
    """
    work_dir = tempfile.mkdtemp(prefix="autobatch_parquet_", dir=_same_drive_tmp_root)

    cfg = _load_small_config()
    # 关键：开启 Parquet storage backend（本地）
    cfg["storage"]["backend"] = "parquet"
    # warehouse 指向本地临时目录（_is_s3_target 会因 bucket+endpoint 配置而判
    # 定为 S3，所以清空 endpoint 强制走本地 parquet 路径）
    warehouse_dir = os.path.join(work_dir, "warehouse")
    cfg["storage"]["warehouse"] = warehouse_dir
    cfg["storage"]["endpoint"] = ""  # 清空 endpoint → _is_s3_target 返回 False → 走本地 parquet
    cfg["storage"]["bucket"] = ""
    cfg["storage"]["compression"] = "zstd"

    # 生成小规模数据（隔离在 work_dir/data/raw 下，避免污染项目 data/）
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    # 与 inc_env / polars_env 一致：关闭 bad_date 缺陷，避免污染水位
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main

    gen_main(cfg)

    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root

    # 持久化配置文件（部分测试需要从文件加载）
    cfg_path = os.path.join(work_dir, "pipeline_parquet.json")
    json_save(cfg_path, cfg)

    # cleanup: 清理本测试创建的 test-parquet-* run_dir
    def _cleanup():
        if os.path.isdir(run_root):
            for name in os.listdir(run_root):
                if name.startswith("test-parquet-"):
                    shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)

    request.addfinalizer(_cleanup)

    return {
        "cfg": cfg,
        "cfg_path": cfg_path,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "run_root": run_root,
        "warehouse_dir": warehouse_dir,
        "orders_path": os.path.join(data_dir, "orders.csv"),
        "customers_path": os.path.join(data_dir, "customers.csv"),
        "products_path": os.path.join(data_dir, "products.csv"),
    }


@pytest.fixture
def s3_env(_same_drive_tmp_root, request):
    """S3 Parquet storage 测试环境：生成数据 + config storage.backend="parquet" + S3（MinIO）.

    在同盘临时目录下复制 pipeline_small.json，把 storage.backend 改为 "parquet"，
    配 S3（endpoint=localhost:9000, bucket=autobatch, access_key=minioadmin,
    secret_key=minioadmin）。用 MinIO 做存储。测试前清理 MinIO 中对应前缀的
    测试数据（避免残留影响）。

    返回 dict:
        cfg            — 已配置好的配置 dict（storage.backend="parquet" + S3）
        cfg_path       — 配置文件路径
        work_dir       — 工作目录（绝对路径，同盘）
        data_dir       — data/raw 目录
        run_root       — run 根目录（ROOT/run）
        orders_path    — orders.csv 路径
        customers_path — customers.csv 路径
        products_path  — products.csv 路径
        s3_prefix      — S3 测试前缀（用于 cleanup）

    cleanup: 测试结束后清理本 fixture 创建的 test-s3-* run_dir + MinIO 中对应前缀数据。
    """
    work_dir = tempfile.mkdtemp(prefix="autobatch_s3_", dir=_same_drive_tmp_root)

    cfg = _load_small_config()
    # 关键：开启 Parquet storage backend + S3（MinIO）
    cfg["storage"]["backend"] = "parquet"
    # 用唯一前缀隔离不同测试的数据（避免 MinIO 残留影响）
    s3_prefix = "test-s3-" + uuid.uuid4().hex[:8]
    cfg["storage"]["prefix"] = s3_prefix
    cfg["storage"]["bucket"] = "autobatch"
    cfg["storage"]["endpoint"] = "localhost:9000"
    cfg["storage"]["access_key"] = "minioadmin"
    cfg["storage"]["secret_key"] = "minioadmin"
    cfg["storage"]["secure"] = False
    cfg["storage"]["region"] = "us-east-1"
    cfg["storage"]["warehouse"] = "warehouse"
    cfg["storage"]["compression"] = "zstd"

    # 生成小规模数据（隔离在 work_dir/data/raw 下，避免污染项目 data/）
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main

    gen_main(cfg)

    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root

    # 持久化配置文件
    cfg_path = os.path.join(work_dir, "pipeline_s3.json")
    json_save(cfg_path, cfg)

    # cleanup: 清理本测试创建的 test-s3-* run_dir + MinIO 中对应前缀数据
    def _cleanup():
        if os.path.isdir(run_root):
            for name in os.listdir(run_root):
                if name.startswith("test-s3-"):
                    shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)
        # 清理 MinIO 中本测试前缀的数据
        try:
            from minio import Minio

            client = Minio(
                "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
            )
            prefix_to_clean = s3_prefix + "/"
            objects = list(client.list_objects("autobatch", prefix=prefix_to_clean, recursive=True))
            for obj in objects:
                client.remove_object("autobatch", obj.object_name)
        except Exception:  # noqa: BLE001
            pass  # MinIO 不可用时忽略清理错误

    request.addfinalizer(_cleanup)

    return {
        "cfg": cfg,
        "cfg_path": cfg_path,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "run_root": run_root,
        "s3_prefix": s3_prefix,
        "orders_path": os.path.join(data_dir, "orders.csv"),
        "customers_path": os.path.join(data_dir, "customers.csv"),
        "products_path": os.path.join(data_dir, "products.csv"),
    }


# MinIO 可用性检查（用于 S3 测试 skipif）
def _minio_available() -> bool:
    """检查 MinIO 是否可用（localhost:9000, bucket=autobatch）."""
    try:
        from minio import Minio

        client = Minio(
            "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
        )
        # 确保 bucket 存在
        if not client.bucket_exists("autobatch"):
            client.make_bucket("autobatch")
        return True
    except Exception:  # noqa: BLE001
        return False


MINIO_AVAILABLE = _minio_available()


# PyIceberg 可用性检查（用于 Iceberg 测试 skipif）
def _pyiceberg_available() -> bool:
    """检查 PyIceberg 是否可用（导入测试 + 基础功能）。"""
    try:
        import pyarrow  # noqa: F401
        import pyiceberg  # noqa: F401

        return True
    except Exception:  # noqa: BLE001
        return False


PYICEBERG_AVAILABLE = _pyiceberg_available()


# ----------------------------------------------------------------------
# Phase 4 Iceberg storage fixture（见 docs/evolution.md §6.x）
# ----------------------------------------------------------------------
@pytest.fixture
def iceberg_env(_same_drive_tmp_root, request):
    """Iceberg storage 测试环境：SQL catalog + SQLite + 本地 warehouse.

    在同盘临时目录下复制 pipeline_small.json，把 storage.backend 改为 "iceberg"，
    配 SQL catalog（SQLite，零额外服务），warehouse 指向本地临时目录.

    返回 dict:
        cfg            — 已配置好的配置 dict（storage.backend="iceberg"）
        cfg_path       — 配置文件路径
        work_dir       — 工作目录（绝对路径，同盘）
        data_dir       — data/raw 目录
        run_root       — run 根目录（ROOT/run）
        warehouse_dir  — Iceberg warehouse 目录
        catalog_db     — SQLite catalog 数据库路径
        orders_path    — orders.csv 路径
        customers_path — customers.csv 路径
        products_path  — products.csv 路径

    cleanup: 测试结束后清理本 fixture 创建的 test-iceberg-* run_dir.
    """
    if not PYICEBERG_AVAILABLE:
        pytest.skip("pyiceberg not installed or unavailable")

    # 仅创建一次 work_dir；此前此处出现过 mkdtemp + 重复 docstring 的粘贴残留，
    # 第一个临时目录没有任何变量/finalizer 引用，每跑一个用例就泄漏一个目录（任务73 修复）。
    work_dir = tempfile.mkdtemp(prefix="autobatch_iceberg_", dir=_same_drive_tmp_root)

    cfg = _load_small_config()
    # 关键：开启 Iceberg storage backend
    cfg["storage"]["backend"] = "iceberg"
    # SQL catalog + SQLite（零额外服务，开发测试用）
    warehouse_dir = os.path.join(work_dir, "warehouse")
    catalog_db = os.path.join(work_dir, "iceberg_catalog.db")
    # Windows 路径用正斜杠（pyiceberg pyarrow FileIO 不接受 file:/// + 盘符）
    warehouse_uri = warehouse_dir.replace("\\", "/")
    catalog_uri = "sqlite:///" + catalog_db.replace("\\", "/")
    cfg["storage"]["iceberg"] = {
        "enabled": True,
        "catalog_name": "autobatch_test",
        "catalog_type": "sql",
        "catalog_uri": catalog_uri,
        "warehouse": warehouse_uri,
        "default_partition_spec": [],
        "properties": {"pyiceberg": "0.12.0rc1"},
    }
    # 清空 S3 配置（iceberg 模式不用 S3）
    cfg["storage"]["endpoint"] = ""
    cfg["storage"]["bucket"] = ""

    # 生成小规模数据（隔离在 work_dir/data/raw 下，避免污染项目 data/）
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    # 与 inc_env / polars_env 一致：关闭 bad_date 缺陷，避免污染水位
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main

    gen_main(cfg)

    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root

    # state_dir 指向 work_dir/state（隔离 Iceberg catalog 与 watermark state）
    cfg["incremental"]["state_dir"] = os.path.join(work_dir, "state")

    # 持久化配置文件（部分测试需要从文件加载）
    cfg_path = os.path.join(work_dir, "pipeline_iceberg.json")
    json_save(cfg_path, cfg)

    # cleanup: 清理本测试创建的 test-iceberg-* run_dir
    def _cleanup():
        if os.path.isdir(run_root):
            for name in os.listdir(run_root):
                if name.startswith("test-iceberg-"):
                    shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)

    request.addfinalizer(_cleanup)

    return {
        "cfg": cfg,
        "cfg_path": cfg_path,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "run_root": run_root,
        "warehouse_dir": warehouse_dir,
        "catalog_db": catalog_db,
        "orders_path": os.path.join(data_dir, "orders.csv"),
        "customers_path": os.path.join(data_dir, "customers.csv"),
        "products_path": os.path.join(data_dir, "products.csv"),
    }


# ----------------------------------------------------------------------
# Spark backend fixture（Phase 2b，见 docs/evolution.md §4.7.2）
# ----------------------------------------------------------------------
@pytest.fixture
def spark_env(_same_drive_tmp_root, request):
    """Spark backend 测试环境：设置环境变量 + 生成数据 + config backend=spark.

    在同盘临时目录下复制 pipeline_small.json，把 engine.backend 改为 "spark"，
    设置 SPARK_HOME / JAVA_HOME / PYSPARK_PYTHON / HADOOP_HOME 环境变量（用
    junction 路径 F:\\spark_home, F:\\jdk17, F:\\Py314\\python.exe, F:\\hadoop），
    调用 generator 生成 data/raw/{orders,customers,products}.csv，把 source.files
    指向生成文件，run_dir 指向 ROOT/run（output.py 的 _register_edges 硬编码
    prefix="run/<batch_id>/"，要求 run_dir 位于 ROOT/run 下）。

    环境变量在 fixture 退出时恢复原值（yield + finally 还原），避免污染其他测试。

    返回 dict:
        cfg            — 已配置好的配置 dict（engine.backend="spark"，可直接传给 run_pipeline）
        cfg_path       — 配置文件路径
        work_dir       — 工作目录（绝对路径，同盘）
        data_dir       — data/raw 目录
        run_root       — run 根目录（ROOT/run）
        orders_path    — orders.csv 路径
        customers_path — customers.csv 路径
        products_path  — products.csv 路径

    cleanup: 测试结束后清理本 fixture 创建的 test-spark-* run_dir + 还原环境变量。
    """
    # 跨平台探测 Spark/Hadoop 路径（环境变量优先，系统路径次之，Windows 回退）
    spark_paths = detect_spark_paths()
    old_env = _build_spark_env(spark_paths)

    work_dir = tempfile.mkdtemp(prefix="autobatch_spark_", dir=_same_drive_tmp_root)

    cfg = _load_small_config()
    # 关键：开启 Spark backend
    cfg["engine"]["backend"] = "spark"
    # format 保持 csv（与 python backend 产物路径一致，便于 _advance_and_merge 读）
    cfg["engine"]["format"] = "csv"
    # local[*] 用所有核；shuffle_partitions 小以加速本地测试
    cfg["engine"]["spark"]["master"] = "local[*]"
    cfg["engine"]["spark"]["shuffle_partitions"] = 4
    cfg["engine"]["spark"]["executor_memory"] = "1g"
    cfg["engine"]["spark"]["driver_memory"] = "512m"

    # 生成小规模数据（隔离在 work_dir/data/raw 下，避免污染项目 data/）
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    # 与 inc_env / polars_env 一致：关闭 bad_date 缺陷，避免污染水位
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main

    gen_main(cfg)

    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root

    # 持久化配置文件（部分测试需要从文件加载）
    cfg_path = os.path.join(work_dir, "pipeline_spark.json")
    json_save(cfg_path, cfg)

    # cleanup: 清理本测试创建的 test-spark-* run_dir + 还原环境变量
    def _cleanup():
        if os.path.isdir(run_root):
            for name in os.listdir(run_root):
                if name.startswith("test-spark-"):
                    shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)
        os.environ.clear()
        os.environ.update(old_env)

    request.addfinalizer(_cleanup)

    return {
        "cfg": cfg,
        "cfg_path": cfg_path,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "run_root": run_root,
        "orders_path": os.path.join(data_dir, "orders.csv"),
        "customers_path": os.path.join(data_dir, "customers.csv"),
        "products_path": os.path.join(data_dir, "products.csv"),
    }


# ----------------------------------------------------------------------
# Phase 5 Spark + Iceberg 三合一 fixture（见 docs/evolution.md §6.x）
# ----------------------------------------------------------------------
@pytest.fixture
def spark_iceberg_env(_same_drive_tmp_root, request):
    """Spark + Iceberg 测试环境：engine.backend="spark" + storage.backend="iceberg".

    合并 spark_env 与 iceberg_env 的配置：
      - 设置 SPARK_HOME / JAVA_HOME / PYSPARK_PYTHON / HADOOP_HOME 环境变量
      - storage.backend="iceberg"，配 SQL catalog（SQLite）+ 本地 warehouse
      - engine.backend="spark"，local[*] 模式
      - 注入 IcebergSparkSessionExtensions + SparkCatalog 配置

    本 fixture **不**预检 iceberg-spark-runtime JAR 是否在 SPARK_HOME/jars/，
    由测试模块用 ``pytest.mark.skipif`` 在收集时检测，缺 JAR 时跳过测试，
    不阻塞全量回归.

    返回 dict:
        cfg            — 已配置好的配置 dict（engine.backend="spark" +
                         storage.backend="iceberg"，可直接传给 run_pipeline）
        cfg_path       — 配置文件路径
        work_dir       — 工作目录（绝对路径，同盘）
        data_dir       — data/raw 目录
        run_root       — run 根目录（ROOT/run）
        warehouse_dir  — Iceberg warehouse 目录
        catalog_db     — SQLite catalog 数据库路径
        orders_path    — orders.csv 路径
        customers_path — customers.csv 路径
        products_path  — products.csv 路径

    cleanup: 测试结束后清理 test-spark-iceberg-* run_dir + 还原环境变量.
    """
    if not PYICEBERG_AVAILABLE:
        pytest.skip("pyiceberg not installed or unavailable")
    # 跨平台探测 Spark/Hadoop 路径（环境变量优先，系统路径次之，Windows 回退）
    spark_paths = detect_spark_paths()
    old_env = _build_spark_env(spark_paths)

    work_dir = tempfile.mkdtemp(prefix="autobatch_spark_iceberg_", dir=_same_drive_tmp_root)

    cfg = _load_small_config()
    # 关键：同时开启 Spark backend + Iceberg storage backend
    cfg["engine"]["backend"] = "spark"
    cfg["engine"]["format"] = "csv"
    cfg["engine"]["spark"]["master"] = "local[*]"
    cfg["engine"]["spark"]["shuffle_partitions"] = 4
    cfg["engine"]["spark"]["executor_memory"] = "1g"
    cfg["engine"]["spark"]["driver_memory"] = "512m"

    cfg["storage"]["backend"] = "iceberg"
    warehouse_dir = os.path.join(work_dir, "warehouse")
    catalog_db = os.path.join(work_dir, "iceberg_catalog.db")
    warehouse_uri = warehouse_dir.replace("\\", "/")
    catalog_uri = "sqlite:///" + catalog_db.replace("\\", "/")
    cfg["storage"]["iceberg"] = {
        "enabled": True,
        "catalog_name": "autobatch_test",
        "catalog_type": "sql",
        "catalog_uri": catalog_uri,
        "warehouse": warehouse_uri,
        "default_partition_spec": [],
        "spark_extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark_catalog_class": "org.apache.iceberg.spark.SparkCatalog",
        "properties": {
            "pyiceberg": "0.12.0rc1",
            "iceberg_spark_runtime": "iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
            "iceberg_aws_bundle": "iceberg-aws-bundle-1.11.0.jar",
            "sqlite_jdbc": "sqlite-jdbc-3.50.3.0.jar",
            "spark_compat": "spark-4.1.0+iceberg-1.11.0+scala-2.13",
        },
    }
    # 清空 S3 配置（本地 SQL catalog 模式不用 S3）
    cfg["storage"]["endpoint"] = ""
    cfg["storage"]["bucket"] = ""

    # 生成小规模数据（隔离在 work_dir/data/raw 下）
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main

    gen_main(cfg)

    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root

    # state_dir 指向 work_dir/state（隔离 Iceberg catalog 与 watermark state）
    cfg["incremental"]["state_dir"] = os.path.join(work_dir, "state")

    # 持久化配置文件
    cfg_path = os.path.join(work_dir, "pipeline_spark_iceberg.json")
    json_save(cfg_path, cfg)

    # cleanup: 清理本测试创建的 test-spark-iceberg-* run_dir + 还原环境变量
    def _cleanup():
        # 停止活跃 SparkSession：每个测试的 catalog_uri/warehouse 指向各自
        # work_dir 的 SQLite/目录，getOrCreate 复用旧 session 会让后续测试的
        # Spark catalog 仍指向第一个测试的库（表跨测试串库）。
        try:
            from pyspark.sql import SparkSession as _SparkSess

            _active = _SparkSess.getActiveSession()
            if _active is not None:
                _active.stop()
        except Exception:  # noqa: BLE001 - pyspark 未安装或 JVM 已关闭
            pass
        if os.path.isdir(run_root):
            for name in os.listdir(run_root):
                if name.startswith("test-spark-iceberg-"):
                    shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)
        os.environ.clear()
        os.environ.update(old_env)

    request.addfinalizer(_cleanup)

    return {
        "cfg": cfg,
        "cfg_path": cfg_path,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "run_root": run_root,
        "warehouse_dir": warehouse_dir,
        "catalog_db": catalog_db,
        "orders_path": os.path.join(data_dir, "orders.csv"),
        "customers_path": os.path.join(data_dir, "customers.csv"),
        "products_path": os.path.join(data_dir, "products.csv"),
    }
