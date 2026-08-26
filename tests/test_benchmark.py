"""基准测试 pytest 包装。

基准测试耗时长（跑 4 个 engine × storage 组合，每个完整 pipeline），
默认 skip，不进入常规回归。跳过控制为单一机制（任务73 修复）：

- ``--runslow`` 选项在 tests/conftest.py 的 ``pytest_addoption`` 注册，
  对任意 pytest 调用生效（单文件运行也可传，不再报 unrecognized arguments）。
- 未传 ``--runslow`` 时，tests/conftest.py 的 ``pytest_collection_modifyitems``
  在收集阶段给本模块全部用例打 skip 标记；传了则真正执行。
- 环境兜底：spark 组合在本机 Spark 写环境不可用（复用
  tests/test_engine_spark.py 的 SPARK_WRITE_DISABLED 判定口径）时在用例内
  跳过，确保 --runslow + 无环境 = skip，--runslow + 有环境 = 执行。

手动运行方式：

    # 跑全部基准组合（默认 skip，需 --runslow 启用）
    F:\\Py314\\python.exe -m pytest tests/test_benchmark.py -v --runslow

    # 仅跑 python/local_csv 组合（快速验证脚本可运行）
    F:\\Py314\\python.exe -m pytest tests/test_benchmark.py -v --runslow \
        -k test_benchmark_python_local_csv

    # 直接跑脚本（不走 pytest）
    F:\\Py314\\python.exe benchmarks/run_benchmark.py

注意：基准测试会创建 run/bench-*/ 目录，跑完自动清理（除非 --no-cleanup）。
"""

from __future__ import annotations

import os
import sys

import pytest

# 把项目根加入 sys.path，使 benchmarks.run_benchmark 可导入
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Spark 写环境可用性：复用 tests/test_engine_spark.py 的模块级判定
# （SPARK_WRITE_DISABLED = 缺 hadoop native lib / 缺 pyspark / JVM 不可启动）。
# pytest 收集时会把 tests/ 目录加入 sys.path，故可直接按模块名导入。
from test_engine_spark import SPARK_WRITE_DISABLED  # noqa: E402

_SPARK_SKIP_REASON = (
    "Spark 写环境不可用（缺 hadoop native lib / pyspark / JVM，"
    "判定口径同 tests/test_engine_spark.SPARK_WRITE_DISABLED）"
)

# ----------------------------------------------------------------------
# 基准组合定义（与 run_benchmark.DEFAULT_COMBINATIONS 一致）
# ----------------------------------------------------------------------
_BENCH_COMBINATIONS = [
    ("python", "local_csv"),
    ("polars", "local_csv"),
    ("polars", "parquet"),
    ("spark", "local_csv"),
]


@pytest.mark.parametrize(
    "engine,storage", _BENCH_COMBINATIONS, ids=[f"{e}/{s}" for e, s in _BENCH_COMBINATIONS]
)
def test_benchmark_combination(engine, storage):
    """跑单个 engine × storage 组合的基准测试。

    默认 skip 由 conftest.py 统一控制（未传 --runslow 时收集阶段打 skip）。
    手动运行：

        F:\\Py314\\python.exe -m pytest tests/test_benchmark.py -v --runslow \
            -k test_benchmark_combination

    本测试调用 benchmarks.run_benchmark.main，只跑指定组合，
    报告写到 benchmarks/report.{md,json}。spark 组合在写环境不可用时跳过。
    """
    if engine == "spark" and SPARK_WRITE_DISABLED:
        pytest.skip(_SPARK_SKIP_REASON)

    from benchmarks.run_benchmark import main as bench_main

    rc = bench_main(["--combinations", f"{engine}/{storage}"])
    assert rc == 0, f"基准组合 {engine}/{storage} 失败（rc={rc}）"


def test_benchmark_python_local_csv():
    """快速冒烟：仅跑 python/local_csv 组合，验证基准脚本能运行。

    默认 skip 由 conftest.py 统一控制。手动运行：

        F:\\Py314\\python.exe -m pytest tests/test_benchmark.py::test_benchmark_python_local_csv \
            -v --runslow
    """
    from benchmarks.run_benchmark import main as bench_main

    rc = bench_main(["--combinations", "python/local_csv"])
    assert rc == 0, f"python/local_csv 基准组合失败（rc={rc}）"


def test_benchmark_all_combinations():
    """跑全部 4 个基准组合，生成完整报告。

    默认 skip 由 conftest.py 统一控制。手动运行：

        F:\\Py314\\python.exe -m pytest tests/test_benchmark.py::test_benchmark_all_combinations \
            -v --runslow

    等价于直接运行 `benchmarks/run_benchmark.py`。
    spark 写环境不可用时整体跳过（全部组合含 spark，无法全部成功）。
    """
    if SPARK_WRITE_DISABLED:
        pytest.skip(_SPARK_SKIP_REASON)

    from benchmarks.run_benchmark import main as bench_main

    rc = bench_main([])
    assert rc == 0, f"部分基准组合失败（rc={rc}）"
