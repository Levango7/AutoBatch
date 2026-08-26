# AutoBatch 性能基准测试

本目录包含 AutoBatch 项目的性能基准测试脚本，用于对比不同 `engine.backend` ×
`storage.backend` 组合的耗时、吞吐量和内存峰值。

## 目录结构

```
benchmarks/
├── README.md          # 本说明文档
├── run_benchmark.py   # 基准测试主脚本
├── report.md          # 最近一次基准报告（Markdown 表格，运行后生成）
├── report.json        # 最近一次基准原始数据（JSON，运行后生成）
└── runs/              # 大规模实证证据（tools/bench_scale_cluster.py 产出）
```

## 大规模 Docker 实证（Spark + MinIO S3）

`tools/bench_scale_cluster.py` 面向大规模验证：polars 向量合成数据（秒级千万行）→
上传 MinIO → Spark 引擎经 S3 直读跑完整五阶段 → 每次运行的阶段耗时/行数/DQ
落盘 `runs/<batch>.json` 作为可提交证据。

**已实测（Docker Desktop 单机 VM：26GB 内存 / 32 vCPU，Spark local[8]，Parquet/S3）：**

| 规模 | pipeline 耗时 | DQ Score | 证据 |
|---|---|---|---|
| 100 万行 | 151 s | 0.9994 | `runs/bench-local-1000000-d29e27.json` |
| 500 万行 | 227.7 s | 0.9994 | `runs/bench-local-5000000-1817bb.json` |
| **1000 万行** | **251.6 s** | 0.9994 | `runs/bench-local-10000000-f2e765.json` |

10M 分阶段：ingest 51.9s / validate 97.6s / clean 41.2s / compute 43.9s / output 15.5s。

**规模边界（实测结论）**：1000 万行此前在 15GB VM 下 clean 阶段触顶 OOM，
根因是 clean 阶段 `df.toPandas().to_dict()` 把全量数据收集进 driver Python
堆（10M 行 ≈ 9.8GB RSS）——代码缺陷而非环境边界。修复（改从磁盘读，
见 `src/stages/clean.py`）并升级 VM 到 26GB 后 10M 全链路通过且 DQ 保真。
亿行级仍需更大内存或真集群横向扩容；命令已就绪：

```bash
# ≥48GB 内存环境（或 Docker VM 内存调大后）直接执行：
docker run --rm --network autobatch-net --entrypoint bash \
  -v "$(pwd):/work" -w /work autobatch-bench:latest -c \
  "python3 tools/bench_scale_cluster.py --rows 100000000 \
   --local-mode --master 'local[8]' --driver-memory 32g \
   --s3-endpoint minio:9000"
```

该工具链在开发过程中实际暴露并驱动修复了四个产品级扩展性缺陷：
集群模式 ingest 的 Driver 全量 collect（新增 S3 源直读分支）、outlier 规则
两处全表 collect（改分布式 approxQuantile + 仅收集越界 id）、clean 阶段
isin 大列表表达式树（改 broadcast join）、abs_path 破坏 s3a:// URI。

### 前置条件

- Docker Desktop 已启动；Spark 集群与 MinIO 容器运行中（MinIO 需接入 autobatch-net）
- 基准镜像：`docker build -t autobatch-bench:latest -f docker/spark-cluster/Dockerfile.bench docker/spark-cluster`
- 凭证经环境变量 `MINIO_ROOT_USER/MINIO_ROOT_PASSWORD` 注入（缺省 minioadmin）

## 覆盖组合

| engine.backend | storage.backend | 说明 |
|----------------|-----------------|------|
| `python`       | `local_csv`     | 纯 Python 后端 + 本地 CSV 存储（基线） |
| `polars`       | `local_csv`     | Polars 列式引擎 + 本地 CSV 存储 |
| `polars`       | `parquet`       | Polars 列式引擎 + 本地 Parquet 列式存储 |
| `spark`        | `local_csv`     | Spark 分布式引擎 + 本地 CSV 存储 |

> `polars/parquet` 走**本地** Parquet（清空 `endpoint`/`bucket`），无需启动 MinIO。
> 如需测 S3 Parquet，可参考 `tests/test_storage_parquet.py` 的 `s3_env` fixture。

## 运行方式

### 前置条件

- Python 解释器：`F:\Py314\python.exe`
- 依赖：`polars`、`pyspark`（仅对应组合需要；缺失组合会失败但不中断其他组合）
- 项目根：`F:\Nexus\Workflow\AutoBatch`

### 跑全部组合

```bash
F:\Py314\python.exe benchmarks/run_benchmark.py
```

### 跑指定组合

```bash
F:\Py314\python.exe benchmarks/run_benchmark.py --combinations python/local_csv polars/local_csv
```

### 调整数据规模

```bash
F:\Py314\python.exe benchmarks/run_benchmark.py --rows 20000
```

默认用 `config/pipeline_small.json` 的 `generator.rows = 5000`。

### 多规模扫描（观察吞吐随规模的变化）

```bash
F:\Py314\python.exe benchmarks/run_benchmark.py --scales 10000,30000,100000
```

`--scales` 接逗号分隔的正整数列表，每个规模**独立生成一份数据**再跑全部组合，
报告中所有表格带 `rows` 列，可直接对比同一组合在不同规模下的 wall time 与
吞吐量（判断引擎是线性退化还是近恒定）。与 `--rows` 互斥，同时给出会报错退出。
典型结论参考 report.md：python 路径耗时随规模近似线性增长，polars 在万行级后
反超并保持近恒定。

### 保留 run_dir 用于排查

```bash
F:\Py314\python.exe benchmarks/run_benchmark.py --no-cleanup
```

默认会清理 `run/bench-*/` 目录和自动创建的临时 work_dir（用 `rmtree_retry`
应对 Windows 句柄延迟释放）；加 `--no-cleanup` 保留产物和 metrics.json。
通过 `--work-dir` 显式指定的目录不会被清理。

### 指定工作目录

```bash
F:\Py314\python.exe benchmarks/run_benchmark.py --work-dir F:\tmp\bench
```

默认在项目所在驱动器（F:）上创建临时目录，避免跨盘 `os.path.relpath` 失败。

## 输出报告

运行完成后生成两个文件：

### `benchmarks/report.md` — Markdown 表格报告

包含 5 个章节：

1. **组合概览**：每组合的 wall time、pipeline duration、内存峰值、总行数、整体吞吐量、DQ score
2. **每阶段耗时与吞吐量**：ingest/validate/clean/compute/output 五阶段的明细
3. **内存峰值对比**：tracemalloc 采集的 Python 堆峰值
4. **失败组合**（如有）：错误信息
5. **原始数据**：指向 `report.json`

### `benchmarks/report.json` — JSON 原始数据

结构：

```json
{
  "generated_at": "2026-08-16 11:00:00",
  "started_at": "2026-08-16 10:55:00",
  "base_config": "config/pipeline_small.json",
  "rows_per_run": 5000,
  "combinations": [
    {
      "engine": "python",
      "storage": "local_csv",
      "status": "success",
      "batch_id": "bench-python-local_csv-abc123",
      "wall_time_ms": 12345,
      "peak_memory_mb": 12.34,
      "pipeline_duration_ms": 12300,
      "total_rows_in": 5000,
      "total_rows_out": 4900,
      "dq_score": 0.98,
      "overall_throughput_rows_per_sec": 396.8,
      "stages": [
        {
          "name": "ingest",
          "status": "success",
          "duration_ms": 1200,
          "rows_in": 5000,
          "rows_out": 5000,
          "throughput_rows_per_sec": 4166.7
        }
      ]
    }
  ]
}
```

## 指标说明

| 指标 | 含义 | 采集方式 |
|------|------|----------|
| `wall_time_ms` | 外层挂钟耗时（含 pipeline 启动/停止开销） | `time.monotonic()` |
| `pipeline_duration_ms` | pipeline 内部记录的总耗时（从 metrics.json 读） | `MetricsRecorder` |
| `peak_memory_mb` | Python 堆内存峰值（MB） | `tracemalloc` |
| `throughput_rows_per_sec` | 每秒输出行数 = `rows_out / (duration_ms / 1000)` | 计算 |
| `overall_throughput_rows_per_sec` | 整体吞吐量 = `total_rows_out / (wall_time_ms / 1000)` | 计算 |

### 内存指标注意事项

`peak_memory_mb` 由 `tracemalloc` 采集，**仅跟踪 Python 对象分配**，不包含：

- `polars` 的 Rust 内存（Arrow 列式缓冲区）
- `pyspark` 的 JVM 堆内存（executor/driver）
- `pyarrow` 的 native 缓冲区

因此：

- 对 `python/local_csv` 后端最准确（几乎纯 Python）
- 对 `polars/*` 和 `spark/*` 后端**显著低估**实际内存占用
- 如需测 native 内存，请用操作系统级工具（如 `psutil.Process().memory_info().rss`）

## pytest 集成

`tests/test_benchmark.py` 用 `@pytest.mark.skip` 包装本脚本，默认跳过
（基准测试耗时长，不应在常规回归中运行）：

```bash
# 默认跳过
F:\Py314\python.exe -m pytest tests/test_benchmark.py -v

# 手动运行（去掉 skip）
F:\Py314\python.exe -m pytest tests/test_benchmark.py -v --runslow
```

## 设计要点

1. **数据共享**：数据生成只跑一次，所有组合共享同一份数据，避免重复生成影响计时。
2. **独立 batch_id**：每组合用 `bench-<engine>-<storage>-<uuid>` 前缀，避免冲突。
3. **run_dir 位置**：`run_dir` 必须在 `ROOT/run` 下（`output.py` 的 `_register_edges`
   硬编码 `prefix="run/<batch_id>/"`）。
4. **同盘临时目录**：工作目录在项目所在驱动器（F:）上，避免跨盘
   `os.path.relpath` 失败（与 `tests/conftest.py` 一致）。
5. **失败不中断**：单个组合失败会记录错误并继续跑下一个组合，最终汇总。
6. **自动清理**：默认清理 `run/bench-*/`，加 `--no-cleanup` 保留。
7. **不修改 src/**：基准测试仅调用 `run_pipeline` 公共接口，不修改项目源码。