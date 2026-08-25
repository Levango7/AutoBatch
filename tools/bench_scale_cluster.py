"""Spark 集群大规模基准工具：polars 向量合成 → MinIO 预置 → 集群 pipeline 实测.

与 benchmarks/run_benchmark.py 的差异：run_benchmark 用项目生成器（纯 Python，
百万行级即分钟级且 Driver 内存受限）；本工具面向亿行级验证——

1. polars 向量化合成 orders（列名/格式与 src/generator.py 完全一致，零缺陷率，
   保证 DQ Score 满分、全部行流入计算），参考表复用 gen_customers/gen_products；
2. 源 CSV 经 pyarrow 流式上传到 MinIO 的 raw_src/ 前缀（executor 经 socat 代理
   直读，绕开 cluster ingest 的 Driver 单点，见 stages/ingest.py S3 分支）；
3. 以 Spark 多机 + Parquet/S3 配置调用 run_pipeline，逐阶段计时入
   benchmarks/runs/<batch>.json 作为可提交证据。

用法：
    python tools/bench_scale_cluster.py --rows 10000000          # 千万级验证
    python tools/bench_scale_cluster.py --rows 100000000         # 亿级冲击
    python tools/bench_scale_cluster.py --rows 10000000 --keep-source

前置条件：Docker Spark 集群（localhost:15077）+ MinIO（localhost:9000）已启动；
凭证经环境变量 MINIO_ROOT_USER/MINIO_ROOT_PASSWORD 注入（缺省 minioadmin）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Any

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.helpers import (  # noqa: E402
    ROOT,
    abs_path,
    apply_spark_env,
    detect_spark_paths,
    json_load,
    rmtree_retry,
)


def _win_short_path(path: str) -> str:
    """Windows 下把含空格/括号的路径转 8.3 短路径.

    Spark 的 bin/spark-submit.cmd 对未引号展开的 %PATH% 片段会因
    'Program Files (x86)' 这类空格+括号路径直接解析崩溃
    （'... was unexpected at this time'）。
    """
    if os.name != "nt" or not path or not os.path.exists(path):
        return path
    try:
        import ctypes

        buf = ctypes.create_unicode_buffer(512)
        if ctypes.windll.kernel32.GetShortPathNameW(path, buf, 512):
            return buf.value or path
    except Exception:  # noqa: BLE001 - 短路径不可得时原样返回
        pass
    return path


def _bootstrap_driver_env() -> None:
    """Driver 端 Spark/JVM 环境引导（与 conftest fixture 同源探测逻辑）.

    PYSPARK_PYTHON 必须锁定为**当前解释器**（装有 pyspark 的那个）：
    shutil.which("python3") 在 Windows 上可能命中无关应用自带的同名解释器
    （实测命中 JoyClaw 的 python3.EXE 导致 Java gateway 启动即崩）。
    """
    os.environ["PYSPARK_PYTHON"] = _win_short_path(sys.executable)
    os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
    apply_spark_env(detect_spark_paths())
    # 空格路径会让 spark-submit.cmd 的未引号展开崩溃（'was unexpected at this
    # time'）——优先采用盘根的无空格 junction 别名（F:\py314 / F:\jdk17c）.
    if os.name == "nt":
        py_alias = "F:\\py314\\python.exe"
        jdk_alias = "F:\\jdk17c"
        if os.path.isfile(py_alias):
            os.environ["PYSPARK_PYTHON"] = py_alias
            os.environ["PYSPARK_DRIVER_PYTHON"] = py_alias
        if os.path.isdir(jdk_alias):
            os.environ["JAVA_HOME"] = jdk_alias


# ---------------------------------------------------------------------------
# 数据合成（polars 向量化）
# ---------------------------------------------------------------------------
def synth_orders_polars(
    n_rows: int,
    n_customers: int,
    n_products: int,
    out_path: str,
    seed: int = 42,
    date_days: int = 365,
) -> None:
    """向量合成 orders.csv——列名/值域/格式与 src/generator.py 零缺陷路径一致."""
    import numpy as np
    import polars as pl

    rng = np.random.default_rng(seed)
    base_ns = int(datetime(2026, 8, 15).timestamp() * 1_000_000_000)

    ci = rng.integers(1, n_customers + 1, n_rows)
    pi = rng.integers(1, n_products + 1, n_rows)
    day_off = rng.integers(0, date_days, n_rows)
    min_off = day_off.astype(np.int64) * 1440 + rng.integers(0, 1440, n_rows)

    regions = np.array(["华东", "华北", "华南", "西南", "西北", "东北", "华中"])
    channels = np.array(["web", "app", "store"])
    statuses = np.array(["completed", "pending", "cancelled", "refunded"])

    base = pl.DataFrame(
        {
            "ci": pl.Series(ci, dtype=pl.Int64),
            "pi": pl.Series(pi, dtype=pl.Int64),
            "order_date": pl.Series(
                (base_ns - day_off.astype(np.int64) * 86_400_000_000_000).astype("datetime64[ns]")
            ).dt.to_string("%Y-%m-%d"),
            "created_ts": pl.Series(
                (base_ns - min_off.astype(np.int64) * 60_000_000_000).astype("datetime64[ns]")
            ).dt.to_string("%Y-%m-%dT%H:%M:%S"),
            "region": pl.Series(regions).gather(rng.integers(0, len(regions), n_rows)),
            "channel": pl.Series(channels).gather(rng.integers(0, len(channels), n_rows)),
            "quantity": pl.Series(rng.integers(1, 21, n_rows), dtype=pl.Int64),
            "unit_price": (np.round(rng.uniform(5, 1500, n_rows) * 100) / 100),
            "status": pl.Series(statuses).gather(rng.integers(0, len(statuses), n_rows)),
        }
    )
    df = base.select(
        pl.format(
            "ORD-{}",
            pl.int_range(1, n_rows + 1, dtype=pl.Int64).cast(pl.Utf8).str.pad_start(9, "0"),
        ).alias("order_id"),
        pl.format("CUS-{}", pl.col("ci").cast(pl.Utf8).str.pad_start(6, "0")).alias("customer_id"),
        pl.format("PRD-{}", pl.col("pi").cast(pl.Utf8).str.pad_start(6, "0")).alias("product_id"),
        pl.col("order_date"),
        pl.col("created_ts"),
        pl.col("region"),
        pl.col("channel"),
        pl.col("quantity"),
        pl.col("unit_price"),
        pl.col("status"),
    )
    df.lazy().sink_csv(out_path)


def synth_reference_tables(n_customers: int, n_products: int, out_dir: str) -> None:
    """参考表直接复用项目生成器（规模小，纯 Python 循环可接受）."""
    import random
    from datetime import datetime

    from src.generator import gen_customers, gen_products
    from src.helpers import csv_write

    rng = random.Random(42)
    base_date = datetime(2026, 8, 15)
    customers = gen_customers(rng, n_customers, base_date)
    products = gen_products(rng, n_products)
    csv_write(os.path.join(out_dir, "customers.csv"), list(customers[0].keys()), customers)
    csv_write(os.path.join(out_dir, "products.csv"), list(products[0].keys()), products)


# ---------------------------------------------------------------------------
# 上传到 MinIO
# ---------------------------------------------------------------------------
def upload_to_s3(
    local_path: str, bucket: str, key: str, endpoint: str, access_key: str, secret_key: str
) -> None:
    """pyarrow 流式单文件上传（8GB 级不占额外内存）."""
    import pyarrow.fs as fs

    s3 = fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=f"http://{endpoint}",
        region="us-east-1",
    )
    with open(local_path, "rb") as f:
        with s3.open_output_stream(f"{bucket}/{key}") as out:
            while chunk := f.read(64 << 20):
                out.write(chunk)


def ensure_bucket(bucket: str, endpoint: str, access_key: str, secret_key: str) -> None:
    import pyarrow.fs as fs

    s3 = fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=f"http://{endpoint}",
        region="us-east-1",
        allow_bucket_creation=True,
        allow_bucket_deletion=False,
    )
    try:
        s3.get_file_info(fs.FileSelector(f"{bucket}/", recursive=False))
    except Exception:
        s3.create_bucket(bucket)


# ---------------------------------------------------------------------------
# 基准配置构造（镜像 conftest.spark_cluster_env 要点）
# ---------------------------------------------------------------------------
def build_cluster_cfg(
    data_src_prefix: str,
    ref_dir: str,
    shuffle_partitions: int,
    s3_endpoint: str,
    access_key: str,
    secret_key: str,
    executor_s3_endpoint: str = "",
    master: str = "spark://localhost:15077",
    driver_memory: str = "4g",
    local_mode: bool = False,
    jvm_opts: str = "",
) -> dict[str, Any]:
    cfg = json_load(abs_path("config/pipeline.json"))
    cfg["engine"]["backend"] = "spark"
    # JVM 附加参数（如堆转储/GC 日志）经 driver extraJavaOptions 注入，
    # local 模式下 executor 与 driver 同 JVM，同样生效
    if jvm_opts:
        cfg["engine"]["spark"]["driver_extra_java_options"] = jvm_opts
    if local_mode:
        # 单 JVM local[*]：Driver 与 Executor 同进程，无容器反连问题；
        # 在 Linux 容器内运行时这是绕开 Windows winutils 门槛的稳态路径.
        cfg["engine"]["spark"].update(
            {
                "master": master,
                "shuffle_partitions": shuffle_partitions,
                "driver_memory": driver_memory,
                "max_result_size": "6g",
                "cluster": {"enabled": False, "driver_host": "", "s3_endpoint": ""},
            }
        )
    else:
        platform = sys.platform
        driver_host = "host.docker.internal" if platform == "win32" else "localhost"
        cfg["engine"]["spark"].update(
            {
                "master": master,
                "shuffle_partitions": shuffle_partitions,
                "executor_memory": "2g",
                "executor_cores": 2,
                "num_executors": 2,
                "driver_memory": driver_memory,
                # fs.s3a.endpoint（Driver+Executor 共用）：Executor 在容器内，须经
                # 宿主机映射端口访问 MinIO——镜像宣称的 socat localhost:9000 代理
                # 实际不存在（2026-08 实测），host.docker.internal 才可达.
                "cluster": {
                    "enabled": True,
                    "driver_host": driver_host,
                    "s3_endpoint": executor_s3_endpoint or s3_endpoint,
                },
            }
        )
    cfg["storage"].update(
        {
            "backend": "parquet",
            "bucket": "autobatch",
            "endpoint": s3_endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "secure": False,
            "region": "us-east-1",
            "warehouse": "warehouse",
            "compression": "zstd",
            "prefix": "",
        }
    )
    # 亿行编号为 9 位，放宽默认 8 位格式规则（其余规则保持默认严格度）
    cfg["quality"]["rules"]["orders"]["format"]["order_id"] = r"^ORD-\d{9}$"
    cfg["source"]["files"] = {
        "orders": f"s3a://autobatch/{data_src_prefix}/orders.csv",
        "customers": os.path.join(ref_dir, "customers.csv"),
        "products": os.path.join(ref_dir, "products.csv"),
    }
    cfg["generator"]["enabled"] = False
    cfg["incremental"]["enabled"] = False
    cfg["error_handling"]["stage_timeouts"] = {
        "ingest": 7200,
        "validate": 7200,
        "clean": 7200,
        "compute": 10800,
        "output": 1800,
    }
    return cfg


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Spark 集群大规模基准")
    parser.add_argument("--rows", type=int, required=True, help="orders 行数")
    parser.add_argument("--customers", type=int, default=200000)
    parser.add_argument("--products", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--s3-endpoint", default="localhost:9000")
    parser.add_argument(
        "--executor-s3-endpoint",
        default="host.docker.internal:9000",
        help="Executor(容器内)访问 MinIO 的地址；缺省经宿主机映射端口",
    )
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="单 JVM local[*] 模式（容器内运行时推荐），忽略 cluster 配置",
    )
    parser.add_argument("--master", default="spark://localhost:15077")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument(
        "--jvm-opts",
        default="",
        help="附加 JVM 参数（spark.driver.extraJavaOptions），如堆转储/GC 日志",
    )
    parser.add_argument("--keep-source", action="store_true", help="保留本地源 CSV")
    parser.add_argument("--skip-upload", action="store_true", help="跳过上传（源已在 MinIO 时）")
    args = parser.parse_args(argv)

    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    drive = os.path.splitdrive(ROOT)[0] + os.sep if os.name == "nt" else tempfile_dir()
    work_dir = os.path.join(drive, f"autobatch_bench_{args.rows}")
    data_dir = os.path.join(work_dir, "data", "raw")
    os.makedirs(data_dir, exist_ok=True)

    orders_path = os.path.join(data_dir, "orders.csv")
    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[scale-bench] work_dir={work_dir}")

    t0 = time.monotonic()
    if not (os.path.isfile(orders_path) and os.path.getsize(orders_path) > 0):
        print(f"[scale-bench] 合成 orders x{args.rows} ...")
        synth_orders_polars(args.rows, args.customers, args.products, orders_path, seed=args.seed)
    synth_reference_tables(args.customers, args.products, data_dir)
    size_gb = os.path.getsize(orders_path) / (1 << 30)
    print(f"[scale-bench] 合成完成 {time.monotonic() - t0:.0f}s, orders.csv={size_gb:.2f}GB")

    src_prefix = f"raw_src/{args.rows}"
    if not args.skip_upload:
        t1 = time.monotonic()
        ensure_bucket("autobatch", args.s3_endpoint, access_key, secret_key)
        print("[scale-bench] 上传 orders.csv → MinIO ...")
        upload_to_s3(
            orders_path,
            "autobatch",
            f"{src_prefix}/orders.csv",
            args.s3_endpoint,
            access_key,
            secret_key,
        )
        print(f"[scale-bench] 上传完成 {time.monotonic() - t1:.0f}s")

    cfg = build_cluster_cfg(
        src_prefix,
        data_dir,
        args.shuffle_partitions,
        args.s3_endpoint,
        access_key,
        secret_key,
        executor_s3_endpoint=args.executor_s3_endpoint,
        master=args.master,
        driver_memory=args.driver_memory,
        local_mode=args.local_mode,
        jvm_opts=args.jvm_opts,
    )
    batch_id = (
        f"bench-{'local' if args.local_mode else 'cluster'}-{args.rows}-{uuid.uuid4().hex[:6]}"
    )
    print(
        f"[scale-bench] 启动 pipeline batch={batch_id} mode={'local' if args.local_mode else 'cluster'}"
    )

    _bootstrap_driver_env()
    from src.pipeline import run_pipeline

    wall_ms = int((time.monotonic() - t0) * 1000)
    rc = run_pipeline(cfg, batch_id, "")
    total_ms = int((time.monotonic() - t0) * 1000)

    run_root = abs_path(cfg["pipeline"].get("run_dir", "run"))
    run_dir = os.path.join(run_root, batch_id)
    metrics = json_load(os.path.join(run_dir, "metrics.json")) if rc == 0 else {}

    record = {
        "tool": "tools/bench_scale_cluster.py",
        "started_at": started,
        "rows_requested": args.rows,
        "source_size_gb": round(size_gb, 2),
        "engine": "spark",
        "mode": (
            f"local({args.master}, {args.driver_memory})"
            if args.local_mode
            else "cluster(2 workers x 2 cores x 2g)"
        ),
        "storage": "parquet/s3(minio)",
        "shuffle_partitions": args.shuffle_partitions,
        "status": "success" if rc == 0 else "failed",
        "synth_upload_ms": wall_ms,
        "total_wall_ms": total_ms,
        "pipeline_duration_ms": metrics.get("total_duration_ms"),
        "dq_score": metrics.get("dq_score"),
        "stages": [
            {
                "name": s.get("name"),
                "duration_ms": s.get("duration_ms"),
                "rows_in": s.get("rows_in"),
                "rows_out": s.get("rows_out"),
            }
            for s in metrics.get("stages", [])
        ],
    }
    runs_dir = os.path.join(_ROOT, "benchmarks", "runs")
    os.makedirs(runs_dir, exist_ok=True)
    out_json = os.path.join(runs_dir, f"{batch_id}.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    print(
        f"[scale-bench] status={record['status']} "
        f"pipeline={metrics.get('total_duration_ms')}ms dq={metrics.get('dq_score')}"
    )
    print(f"[scale-bench] 证据已写入 {out_json}")

    if not args.keep_source:
        rmtree_retry(work_dir, attempts=6, base_delay=0.5)
        print("[scale-bench] 已清理本地工作目录")
    return 0 if rc == 0 else 1


def tempfile_dir() -> str:
    import tempfile

    return tempfile.gettempdir()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
