"""基准测试 pytest 包装。

基准测试耗时长（跑 4 个 engine × storage 组合，每个完整 pipeline），
默认 skip，不进入常规回归。手动运行方式：

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


def pytest_addoption(parser):
    """注册 --runslow 命令行选项，用于启用基准测试。"""
    parser.addoption(
        "--runslow", action="store_true", default=False,
        help="运行基准测试（默认跳过，因为耗时长）"
    )


def pytest_collection_modifyitems(config, items):
    """未传 --runslow 时，跳过本模块所有测试。"""
    if config.getoption("--runslow"):
        return
    skip_slow = pytest.mark.skip(reason="benchmark, run manually with --runslow")
    for item in items:
        if "test_benchmark" in item.keywords or item.fspath.basename == "test_benchmark.py":
            item.add_marker(skip_slow)


# ----------------------------------------------------------------------
# 基准组合定义（与 run_benchmark.DEFAULT_COMBINATIONS 一致）
# ----------------------------------------------------------------------
_BENCH_COMBINATIONS = [
    ("python", "local_csv"),
    ("polars", "local_csv"),
    ("polars", "parquet"),
    ("spark", "local_csv"),
]


@pytest.mark.skip(reason="benchmark, run manually")
@pytest.mark.parametrize("engine,storage", _BENCH_COMBINATIONS,
                         ids=[f"{e}/{s}" for e, s in _BENCH_COMBINATIONS])
def test_benchmark_combination(engine, storage):
    """跑单个 engine × storage 组合的基准测试。

    默认 skip（@pytest.mark.skip）。手动运行：

        F:\\Py314\\python.exe -m pytest tests/test_benchmark.py -v --runslow \
            -k test_benchmark_combination

    本测试调用 benchmarks.run_benchmark.main，只跑指定组合，
    报告写到 benchmarks/report.{md,json}。
    """
    from benchmarks.run_benchmark import main as bench_main

    rc = bench_main(["--combinations", f"{engine}/{storage}"])
    assert rc == 0, f"基准组合 {engine}/{storage} 失败（rc={rc}）"


@pytest.mark.skip(reason="benchmark, run manually")
def test_benchmark_python_local_csv():
    """快速冒烟：仅跑 python/local_csv 组合，验证基准脚本能运行。

    默认 skip。手动运行：

        F:\\Py314\\python.exe -m pytest tests/test_benchmark.py::test_benchmark_python_local_csv \
            -v --runslow
    """
    from benchmarks.run_benchmark import main as bench_main

    rc = bench_main(["--combinations", "python/local_csv"])
    assert rc == 0, f"python/local_csv 基准组合失败（rc={rc}）"


@pytest.mark.skip(reason="benchmark, run manually")
def test_benchmark_all_combinations():
    """跑全部 4 个基准组合，生成完整报告。

    默认 skip。手动运行：

        F:\\Py314\\python.exe -m pytest tests/test_benchmark.py::test_benchmark_all_combinations \
            -v --runslow

    等价于直接运行 `benchmarks/run_benchmark.py`。
    """
    from benchmarks.run_benchmark import main as bench_main

    rc = bench_main([])
    assert rc == 0, f"部分基准组合失败（rc={rc}）"
