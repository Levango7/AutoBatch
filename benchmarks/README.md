# AutoBatch 性能基准测试

本目录包含 AutoBatch 项目的性能基准测试脚本，用于对比不同 `engine.backend` ×
`storage.backend` 组合的耗时、吞吐量和内存峰值。

## 目录结构

```
benchmarks/
├── README.md          # 本说明文档
├── run_benchmark.py   # 基准测试主脚本
├── report.md          # 最近一次基准报告（Markdown 表格，运行后生成）
└── report.json        # 最近一次基准原始数据（JSON，运行后生成）
```

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

### 保留 run_dir 用于排查

```bash
F:\Py314\python.exe benchmarks/run_benchmark.py --no-cleanup
```

默认会清理 `run/bench-*/` 目录；加 `--no-cleanup` 保留产物和 metrics.json。

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