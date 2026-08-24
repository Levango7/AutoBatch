# AutoBatch 性能基准报告

- **生成时间**: 2026-08-25 02:48:01
- **基础配置**: `config/pipeline_small.json`
- **规模扫描**: 100000, 1000000 行
- **组合数**: 4
- **内存说明**: `peak_memory_mb` 为 tracemalloc 采集的 Python 堆峰值，不包含 polars/pyspark 的 native（Rust/JVM）内存；对 python 后端最准确。

## 1. 组合概览

| engine | storage | rows | status | wall(ms) | pipeline(ms) | peak_mem(MB) | total_rows_out | throughput(rows/s) | dq_score |
|--------|---------|------|--------|----------|-------------|--------------|----------------|--------------------|----------|
| python | local_csv | 100000 | success | 28890 | 28239 | 187.541 | 398163 | 13782.0 | 0.996 |
| polars | local_csv | 100000 | success | 16349 | 15624 | 119.429 | 398163 | 24354.0 | 0.996 |
| python | local_csv | 1000000 | success | 350062 | 345543 | 1833.601 | 3892521 | 11119.5 | 0.9959 |
| polars | local_csv | 1000000 | success | 79898 | 76830 | 1050.315 | 3892521 | 48718.6 | 0.9959 |

## 2. 每阶段耗时与吞吐量

| engine | storage | rows | stage | status | duration(ms) | rows_in | rows_out | throughput(rows/s) |
|--------|---------|------|-------|--------|-------------|---------|---------|--------------------|
| python | local_csv | 100000 | ingest | success | 563 | 0 | 104200 | 185079.9 |
| python | local_csv | 100000 | validate | success | 13606 | 104200 | 99024 | 7278.0 |
| python | local_csv | 100000 | clean | success | 4209 | 95824 | 99024 | 23526.7 |
| python | local_csv | 100000 | compute | success | 4017 | 95824 | 91 | 22.7 |
| python | local_csv | 100000 | output | success | 5795 | 95824 | 95824 | 16535.6 |
| polars | local_csv | 100000 | ingest | success | 625 | 0 | 104200 | 166720.0 |
| polars | local_csv | 100000 | validate | success | 10140 | 104200 | 99024 | 9765.7 |
| polars | local_csv | 100000 | clean | success | 1658 | 95824 | 99024 | 59725.0 |
| polars | local_csv | 100000 | compute | success | 150 | 95824 | 91 | 606.7 |
| polars | local_csv | 100000 | output | success | 3031 | 95824 | 95824 | 31614.6 |
| python | local_csv | 1000000 | ingest | success | 5615 | 0 | 1013200 | 180445.2 |
| python | local_csv | 1000000 | validate | success | 147817 | 1013200 | 960810 | 6500.0 |
| python | local_csv | 1000000 | clean | success | 64178 | 957610 | 960810 | 14971.0 |
| python | local_csv | 1000000 | compute | success | 60485 | 957610 | 91 | 1.5 |
| python | local_csv | 1000000 | output | success | 67423 | 957610 | 957610 | 14203.0 |
| polars | local_csv | 1000000 | ingest | success | 4499 | 0 | 1013200 | 225205.6 |
| polars | local_csv | 1000000 | validate | success | 42115 | 1013200 | 960810 | 22814.0 |
| polars | local_csv | 1000000 | clean | success | 9604 | 957610 | 960810 | 100042.7 |
| polars | local_csv | 1000000 | compute | success | 387 | 957610 | 91 | 235.1 |
| polars | local_csv | 1000000 | output | success | 20209 | 957610 | 957610 | 47385.3 |

## 3. 内存峰值对比

| engine | storage | rows | peak_memory(MB) | wall(ms) |
|--------|---------|------|-----------------|----------|
| python | local_csv | 100000 | 187.541 | 28890 |
| polars | local_csv | 100000 | 119.429 | 16349 |
| python | local_csv | 1000000 | 1833.601 | 350062 |
| polars | local_csv | 1000000 | 1050.315 | 79898 |

## 5. 原始数据

完整 JSON 原始数据见 `benchmarks/report.json`。
