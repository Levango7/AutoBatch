# AutoBatch 性能基准报告

- **生成时间**: 2026-08-16 11:09:42
- **基础配置**: `config/pipeline_small.json`
- **每组合行数**: 5000
- **组合数**: 1
- **内存说明**: `peak_memory_mb` 为 tracemalloc 采集的 Python 堆峰值，不包含 polars/pyspark 的 native（Rust/JVM）内存；对 python 后端最准确。

## 1. 组合概览

| engine | storage | status | wall(ms) | pipeline(ms) | peak_mem(MB) | total_rows_out | throughput(rows/s) | dq_score |
|--------|---------|--------|----------|-------------|--------------|----------------|--------------------|----------|
| python | local_csv | success | 2353 | 2270 | 12.527 | 29090 | 12362.9 | 0.9967 |

## 2. 每阶段耗时与吞吐量

| engine | storage | stage | status | duration(ms) | rows_in | rows_out | throughput(rows/s) |
|--------|---------|-------|--------|-------------|---------|---------|--------------------|
| python | local_csv | ingest | success | 91 | 0 | 8250 | 90659.3 |
| python | local_csv | validate | success | 1019 | 8250 | 7983 | 7834.2 |
| python | local_csv | clean | success | 346 | 4783 | 7983 | 23072.3 |
| python | local_csv | compute | success | 342 | 4783 | 91 | 266.1 |
| python | local_csv | output | success | 411 | 4783 | 4783 | 11637.5 |

## 3. 内存峰值对比

| engine | storage | peak_memory(MB) | wall(ms) |
|--------|---------|-----------------|----------|
| python | local_csv | 12.527 | 2353 |

## 5. 原始数据

完整 JSON 原始数据见 `benchmarks/report.json`。
