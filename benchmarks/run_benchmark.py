"""AutoBatch 性能基准测试脚本。

跑各 engine × storage 组合，记录每阶段耗时 / 吞吐量 / 内存峰值，
生成 Markdown 表格（benchmarks/report.md）+ JSON 原始数据（benchmarks/report.json）。

覆盖组合（按任务要求）：
    python/local_csv   — 纯 Python 后端 + 本地 CSV 存储（基线）
    polars/local_csv   — Polars 列式引擎 + 本地 CSV 存储
    polars/parquet     — Polars 列式引擎 + 本地 Parquet 列式存储
    spark/local_csv    — Spark 分布式引擎 + 本地 CSV 存储

设计要点：
    1. 以 config/pipeline_small.json 作为基础配置，按组合修改 engine.backend
       和 storage.backend，其余配置保持不变。
    2. 数据生成只跑一次（共享给所有组合），避免重复生成影响计时。
    3. 每个组合用独立 batch_id（前缀 bench-），run_dir 位于 ROOT/run 下
       （output.py 的 _register_edges 硬编码 prefix="run/<batch_id>/"）。
    4. 用 tracemalloc 采 Python 堆内存峰值（注意：仅跟踪 Python 对象分配，
       不跟踪 polars/pyspark 的 native 内存；对 python 后端最准确）。
    5. 每个组合跑一次完整 pipeline，从 <run_dir>/metrics.json 读每阶段耗时
       和行数，计算吞吐量 = rows_out / duration_s。
    6. 失败的组合也记录（status=failed + error），不中断后续组合。
    7. 跑完清理本脚本创建的 bench-* run_dir，避免占用磁盘。

用法：
    F:\\Py314\\python.exe benchmarks/run_benchmark.py
    F:\\Py314\\python.exe benchmarks/run_benchmark.py --rows 20000
    F:\\Py314\\python.exe benchmarks/run_benchmark.py --scales 10000,50000,200000
        # 多规模扫描：每个规模独立生成数据 × 全部组合，观察吞吐随规模的变化
    F:\\Py314\\python.exe benchmarks/run_benchmark.py --combinations python/local_csv polars/local_csv
    F:\\Py314\\python.exe benchmarks/run_benchmark.py --no-cleanup   # 保留 run_dir/work_dir 用于排查

退出码：0 = 全部组合成功；1 = 至少一个组合失败。
"""

from __future__ import annotations

import argparse
import copy
import os
import shutil
import sys
import tempfile
import time
import tracemalloc
import uuid
from datetime import datetime
from typing import Any, Optional

# 把项目根加入 sys.path，使 src.* 可导入
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.helpers import ROOT, abs_path, json_load, json_save, rmtree_retry  # noqa: E402
from src.pipeline import run_pipeline  # noqa: E402

# 基础配置文件（小规模，5k 行）
_BASE_CONFIG = "config/pipeline_small.json"

# 默认基准组合：engine.backend / storage.backend
DEFAULT_COMBINATIONS: list[tuple[str, str]] = [
    ("python", "local_csv"),
    ("polars", "local_csv"),
    ("polars", "parquet"),
    ("spark", "local_csv"),
]

# 基准结果产物
REPORT_MD = os.path.join(_ROOT, "benchmarks", "report.md")
REPORT_JSON = os.path.join(_ROOT, "benchmarks", "report.json")


# ----------------------------------------------------------------------
# 配置构造
# ----------------------------------------------------------------------
def _load_base_config() -> dict[str, Any]:
    """加载基础配置（pipeline_small.json）。"""
    return json_load(abs_path(_BASE_CONFIG))


def _make_combination_cfg(base: dict[str, Any], engine: str, storage: str,
                          data_dir: str, run_root: str,
                          warehouse_dir: str) -> dict[str, Any]:
    """从基础配置派生指定组合的配置。

    Args:
        base: 基础配置 dict（pipeline_small.json）。
        engine: engine.backend 值（python/polars/spark）。
        storage: storage.backend 值（local_csv/parquet）。
        data_dir: 已生成数据的 data/raw 目录（含 orders/customers/products.csv）。
        run_root: run 根目录（ROOT/run）。
        warehouse_dir: 本地 parquet warehouse 目录（仅 storage=parquet 用）。

    Returns:
        深拷贝的、按组合调整后的配置 dict。
    """
    cfg = copy.deepcopy(base)
    cfg["engine"]["backend"] = engine
    cfg["storage"]["backend"] = storage

    # 指向已生成的共享数据
    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False  # 数据已生成，跳过
    cfg["generator"]["output_dir"] = data_dir

    # run_dir 必须在 ROOT/run 下（output.py 的 _register_edges 硬编码 prefix）
    cfg["pipeline"]["run_dir"] = run_root

    # storage=parquet 走本地 parquet（清空 endpoint/bucket 避免 _is_s3_target 误判 S3）
    if storage == "parquet":
        cfg["storage"]["warehouse"] = warehouse_dir
        cfg["storage"]["endpoint"] = ""
        cfg["storage"]["bucket"] = ""
        cfg["storage"]["compression"] = "zstd"

    # 关闭增量模式（基准测试跑全量，避免 state 跨批次干扰）
    cfg["incremental"]["enabled"] = False

    return cfg


# ----------------------------------------------------------------------
# 数据生成（共享）
# ----------------------------------------------------------------------
def _generate_data(base: dict[str, Any], work_dir: str, scale: int) -> str:
    """生成指定规模的数据到 work_dir/data_<scale>/raw，返回 data_dir。

    多规模扫描时每个规模独立一份目录，互不覆盖；单规模模式等价于旧路径。
    只生成一次、同规模所有组合共享同一份数据，避免重复生成影响计时。
    """
    data_dir = os.path.join(work_dir, f"data_{scale}", "raw")
    cfg = copy.deepcopy(base)
    cfg["generator"]["output_dir"] = data_dir
    # 关闭 bad_date 缺陷，避免污染水位（与 conftest.parquet_env 一致）
    cfg["generator"]["defect_rates"]["bad_date"] = 0.0
    from src.generator import main as gen_main
    meta = gen_main(cfg)
    print("[benchmark] 生成数据(scale={}): orders={} customers={} products={}".format(
        scale, meta["rows"]["orders"], meta["rows"]["customers"], meta["rows"]["products"]))
    return data_dir


# ----------------------------------------------------------------------
# 单组合运行
# ----------------------------------------------------------------------
def _run_one_combination(engine: str, storage: str,
                         cfg: dict[str, Any], batch_id: str
                         ) -> tuple[int, str, Optional[dict[str, Any]]]:
    """跑一个组合的完整 pipeline。

    Returns:
        (rc, run_dir, metrics)：rc=0 成功；run_dir；metrics.json 内容（失败时可能为 None）。
    """
    rc = run_pipeline(cfg, batch_id, "")
    run_root = abs_path(cfg["pipeline"].get("run_dir", "run"))
    run_dir = os.path.join(run_root, batch_id)
    metrics_path = os.path.join(run_dir, "metrics.json")
    metrics = json_load(metrics_path) if os.path.exists(metrics_path) else None
    return rc, run_dir, metrics


def _measure_combination(engine: str, storage: str, cfg: dict[str, Any]
                         ) -> dict[str, Any]:
    """跑一个组合并采集耗时 / 内存峰值 / metrics。

    返回结构：
        {
            "engine": "python",
            "storage": "local_csv",
            "status": "success" | "failed",
            "error": None | "错误信息",
            "batch_id": "bench-...",
            "wall_time_ms": 12345,        # 外层挂钟耗时
            "peak_memory_mb": 12.34,      # tracemalloc Python 堆峰值（MB）
            "pipeline_duration_ms": ...,  # 从 metrics.json 读
            "total_rows_in": ...,
            "total_rows_out": ...,
            "dq_score": ...,
            "stages": [                   # 每阶段明细
                {
                    "name": "ingest",
                    "duration_ms": ...,
                    "rows_in": ...,
                    "rows_out": ...,
                    "throughput_rows_per_sec": ...,  # rows_out / (duration_ms/1000)
                    "status": "success"
                }, ...
            ],
            "overall_throughput_rows_per_sec": ...  # total_rows_out / wall_time_s
        }
    """
    batch_id = f"bench-{engine}-{storage}-{uuid.uuid4().hex[:6]}"
    print(f"\n[benchmark] === 组合: engine={engine}/storage={storage} batch={batch_id} ===")

    result: dict[str, Any] = {
        "engine": engine,
        "storage": storage,
        "batch_id": batch_id,
    }

    # tracemalloc 采 Python 堆内存峰值
    tracemalloc.start()
    wall_start = time.monotonic()
    try:
        rc, run_dir, metrics = _run_one_combination(engine, storage, cfg, batch_id)
    except Exception as exc:  # noqa: BLE001
        rc, run_dir, metrics = 1, None, None
        result["error"] = f"{type(exc).__name__}: {str(exc)}"
        print("[benchmark] 组合异常: {}".format(result["error"]))
    wall_ms = int((time.monotonic() - wall_start) * 1000)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    result["wall_time_ms"] = wall_ms
    result["peak_memory_mb"] = round(peak / (1024 * 1024), 3)
    result["run_dir"] = run_dir
    result["status"] = "success" if rc == 0 else "failed"
    result["error"] = result.get("error") if rc != 0 else None

    # 从 metrics.json 提取每阶段明细
    total_rows_in = 0
    total_rows_out = 0
    stages: list[dict[str, Any]] = []
    if metrics is not None:
        result["pipeline_duration_ms"] = metrics.get("total_duration_ms")
        result["dq_score"] = metrics.get("dq_score")
        for s in metrics.get("stages", []):
            dur_ms = s.get("duration_ms", 0)
            rows_in = s.get("rows_in", 0)
            rows_out = s.get("rows_out", 0)
            total_rows_in += rows_in
            total_rows_out += rows_out
            # 吞吐量：rows_out / duration_s；duration=0 时记为 0
            throughput = round(rows_out / (dur_ms / 1000), 1) if dur_ms > 0 else 0.0
            stages.append({
                "name": s.get("name"),
                "status": s.get("status"),
                "duration_ms": dur_ms,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "throughput_rows_per_sec": throughput,
            })
    result["total_rows_in"] = total_rows_in
    result["total_rows_out"] = total_rows_out
    result["stages"] = stages
    # 整体吞吐量：total_rows_out / wall_time_s
    wall_s = wall_ms / 1000
    result["overall_throughput_rows_per_sec"] = round(
        total_rows_out / wall_s, 1) if wall_s > 0 else 0.0

    print("[benchmark] 结果: status={} wall={}ms peak_mem={}MB total_rows_out={}".format(
        result["status"], wall_ms, result["peak_memory_mb"], total_rows_out))
    return result


# ----------------------------------------------------------------------
# 报告生成
# ----------------------------------------------------------------------
def _generate_reports(results: list[dict[str, Any]], scales: list[int],
                      started_at: str, finished_at: str) -> None:
    """生成 Markdown 报告（report.md）和 JSON 原始数据（report.json）."""
    # --- JSON 原始数据 ---
    json_doc = {
        "generated_at": finished_at,
        "started_at": started_at,
        "base_config": _BASE_CONFIG,
        "rows_per_run": scales[0] if len(scales) == 1 else None,
        "scales": scales,
        "combinations": results,
    }
    os.makedirs(os.path.dirname(REPORT_JSON), exist_ok=True)
    json_save(REPORT_JSON, json_doc)

    # --- Markdown 报告 ---
    lines: list[str] = []
    lines.append("# AutoBatch 性能基准报告")
    lines.append("")
    lines.append(f"- **生成时间**: {finished_at}")
    lines.append(f"- **基础配置**: `{_BASE_CONFIG}`")
    if len(scales) == 1:
        lines.append(f"- **每组合行数**: {scales[0]}")
    else:
        lines.append(f"- **规模扫描**: {', '.join(str(s) for s in scales)} 行")
    lines.append(f"- **组合数**: {len(results)}")
    lines.append("- **内存说明**: `peak_memory_mb` 为 tracemalloc 采集的 Python 堆峰值，"
                 "不包含 polars/pyspark 的 native（Rust/JVM）内存；对 python 后端最准确。")
    lines.append("")

    # 概览表
    lines.append("## 1. 组合概览")
    lines.append("")
    lines.append("| engine | storage | rows | status | wall(ms) | pipeline(ms) | "
                 "peak_mem(MB) | total_rows_out | throughput(rows/s) | dq_score |")
    lines.append("|--------|---------|------|--------|----------|-------------|"
                 "--------------|----------------|--------------------|----------|")
    for r in results:
        lines.append(
            "| {engine} | {storage} | {rows} | {status} | {wall} | {pdur} | "
            "{mem} | {rows_out} | {tput} | {dq} |".format(
                engine=r["engine"],
                storage=r["storage"],
                rows=r.get("rows", "N/A"),
                status=r["status"],
                wall=r["wall_time_ms"],
                pdur=r.get("pipeline_duration_ms", "N/A"),
                mem=r["peak_memory_mb"],
                rows_out=r["total_rows_out"],
                tput=r["overall_throughput_rows_per_sec"],
                dq=r.get("dq_score", "N/A"),
            )
        )
    lines.append("")

    # 每阶段明细表
    lines.append("## 2. 每阶段耗时与吞吐量")
    lines.append("")
    lines.append("| engine | storage | rows | stage | status | duration(ms) | "
                 "rows_in | rows_out | throughput(rows/s) |")
    lines.append("|--------|---------|------|-------|--------|-------------|"
                 "---------|---------|--------------------|")
    for r in results:
        for s in r.get("stages", []):
            lines.append(
                "| {eng} | {stor} | {rows} | {name} | {st} | {dur} | "
                "{ri} | {ro} | {tp} |".format(
                    eng=r["engine"], stor=r["storage"], rows=r.get("rows", "N/A"),
                    name=s["name"], st=s["status"],
                    dur=s["duration_ms"], ri=s["rows_in"],
                    ro=s["rows_out"], tp=s["throughput_rows_per_sec"],
                )
            )
    lines.append("")

    # 内存峰值对比
    lines.append("## 3. 内存峰值对比")
    lines.append("")
    lines.append("| engine | storage | rows | peak_memory(MB) | wall(ms) |")
    lines.append("|--------|---------|------|-----------------|----------|")
    for r in results:
        lines.append("| {} | {} | {} | {} | {} |".format(
            r["engine"], r["storage"], r.get("rows", "N/A"),
            r["peak_memory_mb"], r["wall_time_ms"]))
    lines.append("")

    # 失败组合
    failed = [r for r in results if r["status"] != "success"]
    if failed:
        lines.append("## 4. 失败组合")
        lines.append("")
        for r in failed:
            lines.append("- **{}/{}**: {}".format(
                r["engine"], r["storage"], r.get("error", "unknown")))
        lines.append("")

    lines.append("## 5. 原始数据")
    lines.append("")
    lines.append("完整 JSON 原始数据见 `benchmarks/report.json`。")
    lines.append("")

    os.makedirs(os.path.dirname(REPORT_MD), exist_ok=True)
    with open(REPORT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ----------------------------------------------------------------------
# 清理
# ----------------------------------------------------------------------
def _cleanup_run_dirs(run_root: str) -> None:
    """清理本脚本创建的 bench-* run_dir。"""
    if not os.path.isdir(run_root):
        return
    for name in os.listdir(run_root):
        if name.startswith("bench-"):
            shutil.rmtree(os.path.join(run_root, name), ignore_errors=True)


# ----------------------------------------------------------------------
# 主入口
# ----------------------------------------------------------------------
def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="AutoBatch 性能基准测试")
    parser.add_argument("--rows", type=int, default=None,
                        help="覆盖 generator.rows（默认用 pipeline_small.json 的 5000）")
    parser.add_argument("--scales", type=str, default=None,
                        help="逗号分隔的多规模扫描（如 10000,50000,200000），"
                             "每个规模独立生成数据并跑全部组合；与 --rows 互斥")
    parser.add_argument("--combinations", nargs="+", default=None,
                        help="指定组合（形如 python/local_csv），默认跑全部 4 个")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="保留 bench-* run_dir 与自动创建的 work_dir 用于排查")
    parser.add_argument("--work-dir", default=None,
                        help="工作目录（默认自动创建临时目录，跑完自动清理）")
    args = parser.parse_args(argv)

    # 解析规模列表
    if args.scales and args.rows is not None:
        print("[benchmark] --rows 与 --scales 互斥，只能二选一")
        return 1
    if args.scales:
        try:
            scales = [int(x) for x in args.scales.split(",") if x.strip()]
        except ValueError:
            print("[benchmark] 非法 --scales，应为逗号分隔整数，如 10000,50000")
            return 1
        if not scales or any(s <= 0 for s in scales):
            print("[benchmark] --scales 必须为正整数列表")
            return 1
    else:
        scales = [args.rows] if args.rows is not None else None  # None = 用基础配置

    # 解析组合
    if args.combinations:
        combos: list[tuple[str, str]] = []
        for c in args.combinations:
            if "/" not in c:
                print(f"[benchmark] 非法组合 '{c}', 应形如 engine/storage")
                return 1
            eng, stor = c.split("/", 1)
            combos.append((eng, stor))
    else:
        combos = list(DEFAULT_COMBINATIONS)

    print("[benchmark] 计划组合: {}".format([f"{e}/{s}" for e, s in combos]))

    base = _load_base_config()

    # 同盘临时工作目录（避免跨盘 os.path.relpath 失败，与 conftest 一致）
    drive = os.path.splitdrive(ROOT)[0] + os.sep
    auto_work_dir = args.work_dir is None
    if not auto_work_dir:
        work_dir = args.work_dir
        os.makedirs(work_dir, exist_ok=True)
    else:
        work_dir = tempfile.mkdtemp(prefix="autobatch_bench_", dir=drive)

    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    warehouse_dir = os.path.join(work_dir, "warehouse")

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[benchmark] work_dir={work_dir}")
    print(f"[benchmark] run_root={run_root}")
    print(f"[benchmark] 开始时间={started_at}")

    results: list[dict[str, Any]] = []
    try:
        for scale in (scales or [int(base["generator"]["rows"])]):
            base["generator"]["rows"] = scale

            # 1. 生成本规模的共享数据
            try:
                data_dir = _generate_data(base, work_dir, scale)
            except Exception as exc:  # noqa: BLE001
                print(f"[benchmark] 数据生成失败(scale={scale}): {exc}")
                return 1

            # 2. 跑每个组合
            for engine, storage in combos:
                cfg = _make_combination_cfg(
                    base, engine, storage, data_dir, run_root, warehouse_dir)
                try:
                    r = _measure_combination(engine, storage, cfg)
                except Exception as exc:  # noqa: BLE001
                    r = {
                        "engine": engine, "storage": storage,
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {str(exc)}",
                        "wall_time_ms": 0, "peak_memory_mb": 0,
                        "total_rows_in": 0, "total_rows_out": 0,
                        "stages": [], "overall_throughput_rows_per_sec": 0.0,
                        "batch_id": "n/a",
                    }
                    print(f"[benchmark] 组合 {engine}/{storage} 异常: {exc}")
                r["rows"] = scale
                results.append(r)
    finally:
        # 3. 清理：bench-* run_dir 总是尝试清（--no-cleanup 除外）；
        #    自动创建的 work_dir 也一并清，避免盘根积累 autobatch_bench_* 残留.
        if not args.no_cleanup:
            _cleanup_run_dirs(run_root)
            print("[benchmark] 已清理 bench-* run_dir（用 --no-cleanup 保留）")
            if auto_work_dir:
                ok = rmtree_retry(work_dir, attempts=6, base_delay=0.5)
                print("[benchmark] 清理临时 work_dir: {}".format("ok" if ok else "FAILED"))
        else:
            print("[benchmark] 保留产物（--no-cleanup）")

    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 4. 生成报告
    used_scales = sorted({r.get("rows", 0) for r in results})
    _generate_reports(results, used_scales, started_at, finished_at)
    print("\n[benchmark] 报告已生成:")
    print(f"  - Markdown: {REPORT_MD}")
    print(f"  - JSON:     {REPORT_JSON}")

    # 5. 汇总
    n_ok = sum(1 for r in results if r["status"] == "success")
    n_fail = len(results) - n_ok
    print(f"\n[benchmark] 汇总: {n_ok}/{len(results)} 组合成功, {n_fail} 失败")
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
