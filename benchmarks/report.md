# AutoBatch 性能基准报告

- **生成时间**: 2026-08-24 19:05:54
- **基础配置**: `config/pipeline_small.json`
- **规模扫描**: 10000, 30000 行
- **组合数**: 4
- **内存说明**: `peak_memory_mb` 为 tracemalloc 采集的 Python 堆峰值，不包含 polars/pyspark 的 native（Rust/JVM）内存；对 python 后端最准确。

## 1. 组合概览

| engine | storage | rows | status | wall(ms) | pipeline(ms) | peak_mem(MB) | total_rows_out | throughput(rows/s) | dq_score |
|--------|---------|------|--------|----------|-------------|--------------|----------------|--------------------|----------|
| python | local_csv | 10000 | success | 2979 | 2922 | 22.641 | 48534 | 16292.0 | 0.9966 |
| polars | local_csv | 10000 | success | 3484 | 3433 | 25.731 | 48534 | 13930.5 | 0.9966 |
| python | local_csv | 30000 | success | 7488 | 7340 | 57.769 | 126295 | 16866.3 | 0.9962 |
| polars | local_csv | 30000 | success | 3444 | 3316 | 31.703 | 126295 | 36671.0 | 0.9962 |

## 2. 每阶段耗时与吞吐量

| engine | storage | rows | stage | status | duration(ms) | rows_in | rows_out | throughput(rows/s) |
|--------|---------|------|-------|--------|-------------|---------|---------|--------------------|
| python | local_csv | 10000 | ingest | success | 102 | 0 | 13300 | 130392.2 |
| python | local_csv | 10000 | validate | success | 1391 | 13300 | 12781 | 9188.4 |
| python | local_csv | 10000 | clean | success | 413 | 9581 | 12781 | 30946.7 |
| python | local_csv | 10000 | compute | success | 387 | 9581 | 91 | 235.1 |
| python | local_csv | 10000 | output | success | 596 | 9581 | 9581 | 16075.5 |
| polars | local_csv | 10000 | ingest | success | 103 | 0 | 13300 | 129126.2 |
| polars | local_csv | 10000 | validate | success | 2493 | 13300 | 12781 | 5126.8 |
| polars | local_csv | 10000 | clean | success | 248 | 9581 | 12781 | 51536.3 |
| polars | local_csv | 10000 | compute | success | 102 | 9581 | 91 | 892.2 |
| polars | local_csv | 10000 | output | success | 471 | 9581 | 9581 | 20341.8 |
| python | local_csv | 30000 | ingest | success | 221 | 0 | 33500 | 151583.7 |
| python | local_csv | 30000 | validate | success | 3619 | 33500 | 31968 | 8833.4 |
| python | local_csv | 30000 | clean | success | 1059 | 28768 | 31968 | 30187.0 |
| python | local_csv | 30000 | compute | success | 984 | 28768 | 91 | 92.5 |
| python | local_csv | 30000 | output | success | 1441 | 28768 | 28768 | 19963.9 |
| polars | local_csv | 30000 | ingest | success | 189 | 0 | 33500 | 177248.7 |
| polars | local_csv | 30000 | validate | success | 1695 | 33500 | 31968 | 18860.2 |
| polars | local_csv | 30000 | clean | success | 414 | 28768 | 31968 | 77217.4 |
| polars | local_csv | 30000 | compute | success | 107 | 28768 | 91 | 850.5 |
| polars | local_csv | 30000 | output | success | 895 | 28768 | 28768 | 32143.0 |

## 3. 内存峰值对比

| engine | storage | rows | peak_memory(MB) | wall(ms) |
|--------|---------|------|-----------------|----------|
| python | local_csv | 10000 | 22.641 | 2979 |
| polars | local_csv | 10000 | 25.731 | 3484 |
| python | local_csv | 30000 | 57.769 | 7488 |
| polars | local_csv | 30000 | 31.703 | 3444 |

## 5. 原始数据

完整 JSON 原始数据见 `benchmarks/report.json`。
