# 运行与扩展手册（RUNBOOK）

## 1. 环境要求

- Python 3.9+（Windows / Linux / macOS 均可）
- 零第三方依赖（仅标准库），无需安装任何包
- 可选：Polars 1.43+（`pip install polars`，启用 `engine.backend="polars"` 列式加速路径）
- 可选：PySpark 4.x + JDK 11+ 或 17（`pip install pyspark`，启用 `engine.backend="spark"` 分布式加速路径；Windows 额外需 `winutils.exe` + `hadoop.dll` 并设 `HADOOP_HOME`，详见第 21 节；多机模式需 Docker Desktop + MinIO，详见第 21.3 节）
- 可选：pyarrow 14+ + minio 7+（`pip install pyarrow minio`，启用 `storage.backend="parquet"` 湖存储路径，详见第 22 节）
- 可选：pyiceberg 0.12.0rc1+（`pip install pyiceberg>=0.12.0rc1`，启用 `storage.backend="iceberg"` 湖表路径，详见第 23 节）
- 可选：Docker 20+（容器化运行）
- 磁盘：每批 2 万行订单约占用 15~20 MB（含全部中间产物；`storage.backend="parquet"` 时因列式压缩降至 3~6 MB）

## 2. 启动方式

| 方式 | 命令 | 说明 |
|---|---|---|
| Windows 原生 | `run.bat` 或 `python main.py --config config\pipeline.json` | 自动生成数据并执行五阶段 |
| Linux / macOS | `./run.sh` 或 `python main.py --config config/pipeline.json` | 同上 |
| Docker | `docker compose up --build` | 容器内运行，结果经 volume 持久化到宿主机 |
| 指定配置 | `python main.py --config config\pipeline_small.json` | 用 5000 行小规模配置 |
| 增量模式 | 把 `incremental.enabled` 改为 `true` 后 `python main.py --config config\pipeline_small.json` | 首次=全量 + 建水位；后续只处理水位后新增行，详见第 13 节 |
| Polars 列式加速 | 把 `engine.backend` 改为 `"polars"` 后 `python main.py --config config\pipeline_small.json` | 五阶段走 Polars 列式路径，产物与 python 一致，详见第 18 节 |
| 增量 + Polars | 同时设 `incremental.enabled=true` 与 `engine.backend="polars"` | 两能力正交叠加，ingest 流式过滤 + compute 列式 merge |
| Spark 分布式加速 | 把 `engine.backend` 改为 `"spark"` 后 `python main.py --config config\pipeline_small.json` | 五阶段走 Spark DataFrame API 路径，产物与 python 一致，详见第 21 节 |
| 增量 + Spark | 同时设 `incremental.enabled=true` 与 `engine.backend="spark"` | 两能力正交叠加，ingest 分区并行过滤 + compute 分布式 merge |
| Spark 多机分布式 | 启动集群 `pwsh docker/spark-cluster/up.ps1` + `engine.spark.master="spark://localhost:15077"` + `engine.spark.cluster.enabled=true` + `storage.backend="parquet"` | Docker Compose Standalone 集群（Master + 2 Worker）+ MinIO 共享存储，详见第 21.3 节 |
| 本地 Parquet 湖存储 | 把 `storage.backend` 改为 `"parquet"`（不配 bucket/endpoint）后 `python main.py --config config\pipeline_small.json` | 五阶段产物改写本地 `.parquet`，列式压缩 3-6 倍，产物与 local_csv 一致，详见第 22 节 |
| S3/MinIO Parquet 湖存储 | 把 `storage.backend` 改为 `"parquet"` + 配 `bucket`/`endpoint` 后 `python main.py --config config\pipeline_small.json` | 产物写到 `s3://bucket/warehouse/.../*.parquet`，远端共享存储，详见第 22 节 |
| 增量 + Parquet | 同时设 `incremental.enabled=true` 与 `storage.backend="parquet"` | 两能力正交叠加，ingest 增量过滤 + Parquet 谓词下推，compute 增量 merge + 列式聚合 |
| Iceberg 湖表模式 | 把 `storage.backend` 改为 `"iceberg"` + 配 `iceberg` 子段后 `python main.py --config config\pipeline_small.json` | 五阶段产物改写 Iceberg 表，获得 ACID + time travel + snapshot diff，详见第 23 节 |
| 增量 + Iceberg snapshot diff | 同时设 `incremental.mode="iceberg_snapshot_diff"` 与 `storage.backend="iceberg"` | 用 Iceberg 原生 snapshot diff 替代自建水位，详见第 23 节 |
| Spark + Iceberg 三合一 | 同时设 `engine.backend="spark"` 与 `storage.backend="iceberg"` | Spark 原生读写 Iceberg 表，分布式 + 湖表终态，详见第 23 节 |
| 指定批次号 | `python main.py --batch-id B-MY-001` | 批次号可固定，便于对照 |
| 失败演示 | `python main.py --fail-at clean` | 在指定阶段注入失败，验证失败定位 |

## 3. 配置说明（config/pipeline.json）

| 配置项 | 含义 | 示例/取值 |
|---|---|---|
| pipeline.name / version | 流水线标识 | autobatch / 1.0.0 |
| pipeline.batch_id | 批次号（auto=自动生成） | auto 或 B-YYYYMMDD-HHMMSS-XXXXXX |
| pipeline.run_dir / data_dir | 运行产物与数据目录 | run / data |
| source.name | 数据源名称（写入台账） | ecommerce-demo |
| source.files | 各表文件路径（相对项目根） | orders/customers/products → data/raw/*.csv |
| generator.enabled | 是否自动生成示例数据 | true/false |
| generator.rows | 订单行数（规模开关） | 20000 / 5000 / 100000 |
| generator.seed | 随机种子（固定可复现） | 42 |
| generator.customer_count / product_count / date_range_days | 参考表规模与时间跨度 | 3000 / 200 / 90 |
| generator.defect_rates | 8 类缺陷注入率 | missing 0.02 / duplicate 0.01 / negative_qty 0.005 / invalid_status 0.01 / bad_date 0.005 / orphan_fk 0.01 / outlier 0.002 / bad_channel 0.005 |
| quality.rules.<表>.completeness | 必填列完整性（max_null_ratio=0 即硬校验） | required_columns + max_null_ratio |
| quality.rules.<表>.uniqueness | 唯一性（重复键隔离） | columns: [order_id] |
| quality.rules.<表>.range | 数值范围 | quantity 1~1000、unit_price 0.01~100000 |
| quality.rules.<表>.allowed_values | 枚举白名单 | status/channel/tier |
| quality.rules.<表>.format | 正则格式 | order_id ^ORD-\\d{8}$ |
| quality.rules.<表>.date_valid | 日期范围 | order_date 2020-01-01 ~ 2026-12-31 |
| quality.rules.<表>.referential | 引用完整性（外键在参考表存在） | customer_id → customers.customer_id |
| quality.rules.<表>.outlier | 异常值（IQR/zscore，action=flag 仅标记） | total_amount, method=iqr, factor=1.5 |
| clean.dedup_columns | 去重键 | order_id |
| clean.fill_missing | 缺失值填充 | region/channel → unknown |
| clean.flag_column | 异常标记列名 | is_anomaly |
| compute.aggregations / top_n_customers / currency | 聚合开关、Top N、币种 | daily_sales / category_stats / region_channel_stats / customer_value |
| output.formats / aggregates_dir / report_dir | 输出格式与目录 | csv |
| monitoring.log_level | 日志级别 | INFO |
| demo.fail_at | 演示用失败阶段（null=关闭） | null 或 ingest/validate/clean/compute/output |
| engine.backend | 执行引擎后端 | `python`（缺省，零依赖）/ `polars`（列式加速，需 `pip install polars`）/ `spark`（分布式加速，需 `pip install pyspark` + JDK 11+/17，Windows 额外需 `hadoop.dll`，详见第 21 节） |
| engine.format | 产物存储格式 | `csv`（缺省）/ `parquet`（列式压缩，配合 polars/spark 后端） |
| engine.polars.streaming | Polars streaming 模式（spill 到磁盘） | true / false（缺省） |
| engine.polars.parquet_compression | Parquet 压缩算法 | `zstd`（缺省）/ `snappy` / `gzip` |
| engine.polars.read_options | 传给 `pl.read_csv` 的参数 | `{"try_parse_dates": true}` 等 |
| engine.spark.master | Spark 集群 master URL | `local[*]`（本地模式，缺省）/ `spark://localhost:15077`（Docker Compose Standalone 集群，多机模式已实现）/ `k8s://https://...`（K8s） |
| engine.spark.app_name | Spark 应用名（Spark UI 显示） | `autobatch-small` |
| engine.spark.executor_memory | 每个 executor 内存 | `1g`（缺省）/ `4g` 等 |
| engine.spark.executor_cores | 每个 executor CPU 核数 | 1（缺省）/ 2 / 4 |
| engine.spark.num_executors | executor 实例数（多机模式） | 1（缺省）/ 4 / 8 |
| engine.spark.driver_memory | driver 进程内存 | `512m`（缺省）/ `2g` 等 |
| engine.spark.shuffle_partitions | shuffle 分区数（影响并行度） | 4（缺省，本地）/ 200（多机） |
| engine.spark.max_result_size | driver 端 action 结果序列化上限（千万行级 join 需放大） | "1g"（Spark 默认）/ 大规模 "4g"+ |
| engine.spark.adaptive_query_execution | AQE 开关（自动合并小分区、处理倾斜） | true（缺省）/ false |
| engine.spark.write_single_file | 写出时合并为单文件（coalesce(1)） | false（缺省）/ true |
| engine.spark.read_options | 传给 `spark.read.csv` 的参数 | `{}`（缺省） |
| storage.backend | 存储介质后端 | `local_csv`（缺省，本地 CSV，向后兼容）/ `parquet`（Parquet 列式存储，本地或 S3/MinIO，需 `pip install pyarrow minio`，详见第 22 节）/ `iceberg`（Iceberg 湖表，ACID + time travel + snapshot diff，需 `pip install pyiceberg>=0.12.0rc1`，详见第 23 节） |
| storage.bucket | S3/MinIO bucket 名 | `autobatch`（缺省） |
| storage.endpoint | S3/MinIO endpoint（host:port） | `localhost:9000`（缺省，本地 MinIO） |
| storage.access_key / secret_key | S3 凭证 | `minioadmin` / `minioadmin`（缺省，MinIO 默认凭证） |
| storage.secure | 是否用 HTTPS | false（缺省）/ true |
| storage.region | S3 region | `us-east-1`（缺省） |
| storage.warehouse | warehouse 子路径（bucket 下的逻辑仓库根） | `warehouse`（缺省） |
| storage.prefix | bucket 下额外前缀（多租户隔离用） | `""`（缺省） |
| storage.compression | Parquet 压缩算法 | `zstd`（缺省）/ `snappy` / `gzip` / `none` |
| storage.iceberg.catalog_type | Iceberg catalog 类型 | `sql`（开发/测试，SQLite 零额外服务）/ `rest`（生产，REST catalog 服务） |
| storage.iceberg.catalog_uri | catalog URI（与 config 实际字段名对齐） | `sqlite:///state/iceberg_catalog.db`（SQL catalog，开发）/ `http://localhost:8181`（REST catalog，生产） |
| storage.iceberg.warehouse | Iceberg warehouse 路径（与 `storage.warehouse` 解耦，Iceberg catalog 独立寻址） | `state/warehouse`（缺省，本地路径）/ `s3://autobatch/warehouse`（S3/MinIO 生产） |
| incremental.mode | 增量模式 | `high_watermark`（缺省，Phase 1 自建水位）/ `iceberg_snapshot_diff`（Phase 4，Iceberg 原生 snapshot diff，详见第 23 节） |

## 4. 调整数据规模

- 修改 `generator.rows` 即可（如 5000 / 20000 / 100000），其余逻辑不变。
- 参考：2 万行完整运行约 4~5 秒（本机，其中校验阶段约 1.5 秒——已对 referential 检查做外键集合预计算优化，原逐行查找约 21 秒，降幅 90%+），5 千行约 1~2 秒。
- 单机原型建议百万行以内；更大规模按第 10 节演进到 Spark/Dask 等分布式引擎（同一套阶段抽象）。

## 5. 接入新数据源

1. 把数据文件放入 `data/raw/`（CSV，UTF-8 或 UTF-8-BOM；首行为表头）。
2. 在 `config/pipeline.json` 的 `source.files` 增加条目（如 `"payments": "data/raw/payments.csv"`）。
3. 在 `quality.rules` 增加该表的规则（至少 completeness 与 uniqueness；其余按需）。
4. 如需参与计算，在 `src/stages/compute.py` 增加聚合（见第 6 节）。血缘无需手工维护：各 stage 通过 `ctx.lineage_decls` 声明产物 ← 上游，output 阶段自动拼接写入 manifest。
5. 运行 `python main.py`，在 `run\<batch>\manifest.json` 确认新表已登记。

## 6. 扩展计算逻辑

以「新增一个聚合」为例：

1. 在 `src/stages/compute.py` 写一个函数：输入为 clean 阶段的订单/客户/商品行（list[dict]），输出 list[dict]。
2. 在 `run()` 中调用，用 `csv_write(os.path.join(agg_dir, "新表.csv"), 字段, 结果)` 落盘。
3. 把结果挂进 `ctx.aggregates["新键"]`，这样 `output.py` 会把该键并入 dashboard_data.json（如想在看板展示，还需更新看板模板）。
4. 在 `compute.py` 末尾用 `ctx.lineage_decls["04_aggregates/新表.csv"] = ["03_clean/orders_clean.csv", ...]` 声明血缘上游，output 阶段会自动并入 manifest.lineage（无需改 output.py）。

## 7. 扩展质量规则

- 已有 8 类规则全部由配置驱动，直接改 `quality.rules` 即可（阈值、字段、白名单）。
- 如需新规则类型：在 `src/quality.py` 的 `RuleEngine.check()` 中仿照现有检查块新增一段（bump 计数 + reasons 追加原因码），原因码加入 `quality.quarantine_reasons` 便于阅读。

## 8. 故障定位

1. `run\latest.json` → 最新批次目录。
2. `status.json` → 批次级状态 + 每个阶段的 status / error / 耗时。
3. `logs\<阶段>.jsonl` → 该阶段 JSON Lines 日志（失败含 trace 尾部）。
4. `manifest.json` → 完整台账（含已登记产物）。
5. 失败演示：`python main.py --fail-at clean` 会生成一个 failed 批次，可对照观察。

常见问题：

| 现象 | 原因与处理 |
|---|---|
| 找不到模块 src | 必须从项目根目录运行（main.py 会自动把根目录加入 sys.path） |
| 中文乱码 | CSV 读取兼容 UTF-8-BOM；输出 UTF-8 无 BOM，Excel 打开可用「数据→自文本导入」 |
| validate 隔离行很多 | 检查 defect_rates 是否调大；隔离行在 quarantine\ 目录，原因码在 _reasons 列 |
| 想用真实数据 | generator.enabled=false，并把文件放好、配置 source.files 指向真实文件 |

### 8.1 断点续跑（resume）

失败批次可从断点继续，不必整批重跑。启用方式（`config/pipeline.json`）：

```json
"error_handling": { "resume": true, "...": "其余字段不变" }
```

触发条件（全部满足才生效，否则静默走全量路径）：

1. 显式指定 batch_id 重跑（`python main.py --batch-id <上次失败的ID>` 或配置固定 `pipeline.batch_id`），`auto` 不续跑；
2. `run/<batch_id>/manifest.json` 存在且 `status=="failed"`（成功批次重跑视为全新执行）；
3. pipeline 版本与 config 摘要（config_digest）与上次一致——改了配置就禁止续跑；
4. 各已成功 stage 的**主输出目录**非空（validate 另要求 `02_valid/quality_summary.json` 在位；quarantine/report 等终端产物目录不参与判据，干净数据下 quarantine 为空是正常态）。

跳过的 stage 在新 manifest 中带 `"resumed": true` 标记；output 永不跳过。

### 8.2 OpenLineage 血缘事件

每次批次与各 stage 的执行以 OpenLineage v1 RunEvent 发射：

```json
"openlineage": {
  "enabled": true,
  "namespace": "autobatch",
  "endpoint": ""
}
```

- `enabled=true` 后事件追加写入 `run/<batch_id>/openlineage.ndjson`（每行一个 JSON）；
- `endpoint` 填 Marquez 等 OpenLineage 兼容服务地址（如 `http://localhost:5000/api/v1/lineage`）即同步 HTTP POST，上报失败仅记 warning 不影响主流程；
- pipeline 整批为一个父 Run，stage 为子 Run（parent facet 关联）；runId 由 batch_id/stage 经 uuid5 确定性派生，同批次重跑产生相同 runId，下游可幂等去重；终态 COMPLETE/FAILED 与 START 配对完整。

## 9. 测试运行方式

项目内置 pytest 测试套件（344 个用例），位于 `tests/`：

```
python -m pytest tests/ -v
```

测试覆盖范围（22 个测试模块，Windows 本地 Python 3.14 基线：322 passed + 22 skipped——skip 为环境相关用例）：

| 文件 | 覆盖内容 | 用例数 |
|---|---|---|
| tests/test_benchmark.py | 基准测试：4 个 engine × storage 组合完整 pipeline 耗时对比，默认 skip 需 `--runslow` 启用 | 6 |
| tests/test_config_schema.py | 配置 schema 校验：合法/非法 backend、fail_at、多余键、最小配置等 12 个场景 | 12 |
| tests/test_edge_cases.py | 边界条件：零行/单行数据、空值/混合空值 CSV、空 state/manifest/metrics、聚合单行与空串数值、rmtree_retry 删除重试、s3_credentials 环境变量回退等 39 个场景 | 39 |
| tests/test_engine_polars.py | 4 个 Polars 等价性场景：全量产物与 python 路径一致、DQ Score in [0.95, 1.0] 且 lineage/metrics 正确、增量 + Polars 组合、Parquet 格式条件 skip | 4 |
| tests/test_engine_spark.py | 3 个 Spark 等价性场景：全量产物与 python 路径一致、DQ Score in [0.95, 1.0] 且 lineage/metrics 正确、增量 + Spark 组合；Windows 缺 `hadoop.dll` 时 `skipif` 跳过本地模式 | 3 |
| tests/test_engine_spark_cluster.py | 4 个 Spark 多机模式场景：多机+S3 全量产物与 local_csv 一致、多 executor 并行、增量+多机+S3 组合、Worker 数量；Docker 集群不可用时跳过 | 4 |
| tests/test_error_handling.py | 28 个错误处理加固场景：正常执行不受影响、可配置重试次数、重试成功、超时控制、幂等性、StageExecutionError/StageTimeoutError 上下文、幂等清理不触碰 state/ | 28 |
| tests/test_generator.py | 数据生成器：customers/products/orders 行数、字段完整性、ID 格式、值域、缺陷注入（缺失/负数/非法状态/坏日期/孤儿外键）、同 seed 可复现等 26 个场景 | 26 |
| tests/test_incremental.py | 5 个增量场景：首次增量=全量 + 建水位、无新数据二跑零增量、追加新数据后只处理新增行且聚合 merge 正确、失败重跑幂等水位不推进、`enabled:false` 全量回归行为不变 | 5 |
| tests/test_lineage.py | 血缘 manifest：默认值、set_source、add_stage/artifact/edge、finish、to_dict JSON 往返、lineage_view 图视图等 23 个场景 | 23 |
| tests/test_logging_setup.py | 日志：BatchLogFilter 默认注入、JSON Formatter 序列化/异常/时间戳、级别解析回退、handler 幂等创建与关闭等 22 个场景 | 22 |
| tests/test_metrics.py | 指标：recorder 默认值、record_stage 追加与 extra、finish、to_dict 扁平化（pipeline/stage 级）、save 往返等 14 个场景 | 14 |
| tests/test_monitoring.py | 28 个监控告警场景：MetricsSampler.sample()、AlertChecker.check() DQ Score 低于阈值、stage duration 超阈值、无超阈值返回空、check_alerts() 多批次、HealthServer start/stop、monitoring.enabled=false 不启用 | 28 |
| tests/test_pipeline_e2e.py | 端到端冒烟：pipeline success、DQ Score 落区间、manifest 血缘非空、metrics.json 存在、各表行数、KPI 一致性（daily_sales/category_stats 收入对账）等 14 个场景 | 14 |
| tests/test_quality.py | 8 类质量规则的正例与反例（completeness / uniqueness / range / allowed_values / format / date_valid / referential / outlier）+ referential 性能回归（2 万行外键检查秒级完成） | 17 |
| tests/test_spark_iceberg.py | 10 个 Spark+Iceberg 三合一场景：8 个 `skipif` 环境守护（pyiceberg/pyspark/Docker/MinIO/Iceberg JAR 不可用）+ 2 个 config 验证 Spark Iceberg connector 注入 | 10 |
| tests/test_stages.py | ingest / validate / clean / compute / output 五个 stage 单测 | 5 |
| tests/test_state.py | StateStore：水位/snapshot id 两阶段提交、失败不推进、聚合 merge 累加与派生列重算、原子写等 26 个场景 | 26 |
| tests/test_storage_iceberg.py | 13 个 Iceberg 湖表场景：等价性 / ACID / time travel / schema evolution / snapshot diff 增量 / 增量+Iceberg / SQL catalog / REST catalog；pyiceberg 未安装时 `skipif` 跳过 | 13 |
| tests/test_storage_parquet.py | 4 个 Parquet 湖存储场景：本地 Parquet 全量产物与 local_csv 一致、S3 MinIO Parquet 全量产物与 local_csv 一致、Parquet 压缩比基准、增量 + Parquet 组合；MinIO 不可用时 `skipif` 跳过 | 4 |
| tests/test_openlineage.py | 20 个 OpenLineage 血缘事件测试：event 结构 / parent facet / NDJSON 写出 / HTTP 上报容错 / 确定性 runId；纯单元级，零外部依赖 | 20 |
| tests/test_resume.py          | 21 个断点续跑测试：resume 触发条件（disabled/auto batch/no manifest/success status/version drift/digest drift）/ 产物完整性检查（主输出目录判据 + validate 需批次根 quality_summary.json）/ lineage_decl 持久化 / 干净数据 e2e 续跑回归锁；纯单元级，零外部依赖 | 21 |

合计 344 个用例。

`pytest.ini` 已配置 `testpaths = tests` 与 `pythonpath = .`，从项目根直接运行即可，无需额外参数。增量测试用 `conftest.py` 的 `inc_env` 夹具隔离 state 目录与数据目录，互不污染。Spark 本地模式测试用 `pytest.mark.skipif` 在 Windows 缺 `hadoop.dll` 或未装 `pyspark` 时跳过，代码逻辑完整，装齐环境后可直接运行。Spark 多机模式测试在 Docker Desktop / MinIO 不可用时跳过。Parquet S3 测试用 `pytest.mark.skipif` 在 MinIO 不可达时跳过，本地 Parquet 测试无需 MinIO 即可运行。OpenLineage 与断点续跑测试为纯单元级，不依赖任何外部服务。

## 10. CI（GitHub Actions）

`.github/workflows/ci.yml` 与 `.github/workflows/quality.yml` 配置了持续集成：

- **触发**：ci.yml 在 push（main / master / release/*）或提交 PR 时运行；quality.yml 同。
- **矩阵**：ubuntu-latest × Python 3.10 / 3.11 / 3.12 + macos-latest × 3.12 单腿（POSIX 兼容验证；fail-fast 关闭，各节点独立报告）。
- **步骤**：Checkout → 安装依赖（runtime + pyspark/polars/pyarrow/pyiceberg 可选引擎）→ `python -m pytest tests/ -v -k "not cluster"`（全量测试 + 覆盖率；Spark 集群用例需本地 Docker 集群，CI 中显式排除）→ `python main.py --config config/pipeline_small.json`（流水线冒烟）；另有独立 pip-audit 依赖安全扫描 job。
- **quality.yml**：ruff lint + ruff format 校验 + mypy 类型检查（Python 3.12，依赖与 CI 绿腿对齐）+ pytest 覆盖率收集（经 `tools/quality_collect.py`，失败用例自动转 ::error:: 注解）+ `coverage report --fail-under=60` 门禁。已知事项：覆盖率收集步骤在 GHA runner 上存在启动层平台故障（2026-08 连续多轮未定位），已显式降级为非阻塞并在 yml 内注释恢复条件；pytest 断言职责由 ci.yml 承担。

任一矩阵节点失败即 CI 标红，保证主干始终可运行且测试通过。

## 11. Docker 部署

```
docker compose up --build
```

- 镜像 python:3.13.1-slim，无任何第三方依赖。
- ./run、./data、./config 以 volume 挂载，结果持久化在宿主机。
- 重复执行会生成新批次目录；日志输出到容器 stdout。

## 12. 演进到生产集群（摘要）

| 原型组件 | 生产对应物 |
|---|---|
| main.py 串行五阶段 | Airflow / Dagster / DolphinScheduler 的 DAG |
| 单机 Python 计算 | Spark（批）/ Dask / Polars（单机加速） |
| 本地目录 + CSV | MinIO/S3 + Parquet，再上 Delta/Iceberg/Hudi 湖表格式 |
| 自研规则引擎 | Great Expectations / Soda Core（规则库可平移），Spark 阶段用 Deequ |
| manifest 台账 | OpenLineage 事件（job/run/dataset + facets）+ Marquez/DataHub |
| metrics.json 扁平指标 | Prometheus（pushgateway / node_exporter textfile）+ Grafana 看板 |

仅在集群阶段可获得的能力：分布式调度与并行、ACID 湖表语义、集群资源管理、平台级血缘/元数据、高可用容错、指标集中采集与告警。详见 docs/design.html 第 6 章。

## 13. 增量配置（incremental 段）

Phase 1 增量处理通过 `config/pipeline.json` 的 `incremental` 段开启，零新增依赖。`config/pipeline_small.json` 提供完整示例。各字段说明如下：

表：incremental 段字段说明表

| 字段 | 取值 | 含义 |
|---|---|---|
| `enabled` | `true` / `false`（缺省） | 增量模式开关。`false` 时走全量路径，行为与 Phase 0 完全一致（向后兼容） |
| `mode` | `high_watermark` / `iceberg_snapshot_diff` | 增量模式类型。`high_watermark` 为 Phase 1 自建水位模式；`iceberg_snapshot_diff` 已支持，详见第 23 节 |
| `state_dir` | 路径（缺省 `state`） | 跨批次状态目录，与 `run/`、`data/` 平级。`state/state.json` 存水位，`state/aggregates/` 存历史聚合 |
| `tables.<表>.watermark_column` | 列名或 `null` | 水位列。`orders` 用 `order_date`，`customers` 用 `join_date`，`products` 设 `null` 表示无递增列 |
| `tables.<表>.watermark_type` | `date` / `timestamp` / `int_offset` / `full_load` | 水位列类型。`full_load` 表示该表每次全量加载（适用于 products 这种小静态表） |
| `tables.<表>.init_mode` | `full_load` | 首次运行（state.json 不存在）时的初始化模式，全量加载并建立初始水位 |
| `aggregates_persist` | `true` / `false` | 是否持久化历史聚合结果到 `state/aggregates/`。`true` 时 compute 增量 merge，`false` 时每次全量重算聚合 |
| `fail_safe` | `backup_before_merge` | 失败安全策略。当前实现为两阶段提交（merge 仅在全部阶段成功后执行），不污染 state |

配置示例（摘自 `config/pipeline_small.json`）：

```json
"incremental": {
  "enabled": false,
  "mode": "high_watermark",
  "state_dir": "state",
  "tables": {
    "orders":    {"watermark_column": "order_date", "watermark_type": "date", "init_mode": "full_load"},
    "customers": {"watermark_column": "join_date",  "watermark_type": "date", "init_mode": "full_load"},
    "products":  {"watermark_column": null,         "watermark_type": "full_load"}
  },
  "aggregates_persist": true,
  "fail_safe": "backup_before_merge"
}
```

开启增量模式：把 `enabled` 改为 `true` 即可，其余字段保持默认。首次运行等价于全量（`init_mode=full_load`）并建立水位，后续运行只处理水位后新增行。

## 14. 水位管理

### 14.1 state.json 结构

`state/state.json` 由 `src/state.py` 的 `StateStore` 类管理，跨批次共享，独立于 `run/<batch_id>/` 批次目录。结构如下：

```json
{
  "version": "1.0",
  "updated_at": "2026-08-15T13:45:48Z",
  "last_batch_id": "B-20260815-134548-E85420",
  "tables": {
    "orders": {
      "watermark_column": "order_date",
      "watermark_value": "2026-05-15",
      "watermark_type": "date",
      "last_seen_row_count": 23400,
      "cumulative_row_count": 23400,
      "last_batch_id": "B-20260815-134548-E85420",
      "last_processed_at": "2026-08-15T13:45:48Z"
    }
  },
  "aggregates": {
    "daily_sales": {"path": "state/aggregates/daily_sales.csv", "row_count": 91, "last_batch_id": "B-..."}
  }
}
```

### 14.2 watermark_value 语义

`tables.<表>.watermark_value` 记录上次成功处理到的水位列最大值。下次 ingest 阶段只读取 `watermark_column > watermark_value` 的新增行，写入 `01_raw/<表>_incremental.csv`。

- 首次运行（state.json 不存在）：全量加载，水位初始化为 `max(watermark_column)`
- 后续运行：流式扫描源文件，过滤出 `> watermark_value` 的行
- 水位推进：全部阶段成功后，`pipeline._advance_and_merge` 调用 `StateStore.commit_watermark`，把 `new_watermark` 提升为 `watermark_value`，更新 `last_seen_row_count` / `cumulative_row_count` / `last_batch_id` / `last_processed_at`

### 14.3 两阶段提交（失败不推进）

水位推进采用两阶段提交思想，保证失败时水位不前进，下次重跑幂等：

1. **prepare 阶段**：ingest 计算本批次 `new_watermark` 后，通过 `StateStore.set_new_watermark` 暂存到 `ctx.state`（内存），**不落盘 state.json**
2. **commit 阶段**：`pipeline.py` 在五阶段全部 `success` 后，调用 `_advance_and_merge` → `commit_watermark`，把 `new_watermark` 提升为 `watermark_value` 并原子写回 state.json（`os.replace` 保证崩溃安全）
3. **abort 阶段**：任一阶段抛异常或 `fail_at` 注入失败时，`overall != "success"`，跳过 commit 块，state.json 保持旧水位

重跑场景：失败后 state.json 未推进，下次运行 ingest 重读同一水位后的新增行，validate / clean / compute 重新处理同一批增量，**幂等**。`tests/test_incremental.py::test_incremental_failure_idempotent` 覆盖此场景。

## 15. 聚合 merge

### 15.1 state/aggregates/ 目录

增量模式下，历史聚合结果持久化在 `state/aggregates/` 目录，与批次目录 `run/<batch_id>/04_aggregates/` 分离：

```
state/aggregates/
├── daily_sales.csv          # 按 order_date 分桶
├── category_stats.csv       # 按 category 分桶
├── region_channel_stats.csv # 按 (region, channel) 分桶
├── customer_value.csv       # 按 customer_id 分桶，含 rank
└── customer_tier.csv        # 按 tier 分桶
```

每批次 compute 阶段只产出本批次增量聚合到 `run/<batch_id>/04_aggregates/`；pipeline 在全部阶段成功后调用 `_advance_and_merge` 把批次聚合 merge 到 `state/aggregates/`。这样任一阶段失败都不污染 state，重跑完全幂等。

### 15.2 merge_aggregate 累加语义

`StateStore.merge_aggregate(name, fields, new_rows, key_cols)` 按 `key_cols` 做 upsert 合并：

- **key 列**（如 `order_date` / `customer_id`）：作为分桶标识，不累加
- **数值列**（`orders` / `units` / `revenue` / `customers` 等）：历史值 + 新值累加
- **派生列**（`avg_order_value` / `revenue_share` / `rank`）：merge 后重算，不累加
  - `avg_order_value = revenue / orders`
  - `revenue_share = revenue / total_revenue`（total_revenue 为全表求和）
  - `rank` = 按 `revenue` 降序的 dense rank
- **其他非 key 列**（`tier` / `city` / `category` / `region` / `channel`）：新值优先，无新值保留历史值

`src/pipeline.py` 的 `_AGGREGATE_SPECS` 声明每个聚合的 `(name, fields, key_cols)`，pipeline 按此规格逐个 merge。

### 15.3 customer_value rank 重算

`customer_value` 是最 tricky 的聚合：merge 时按 `customer_id` 累加 `orders` / `revenue`，然后对全量分桶按 `revenue` 降序重新排序，重算 `rank` 列。`tier` / `city` 取最新值。这意味着每次增量 merge 后，所有客户的 rank 都可能变化（因为新数据可能改变排序）。这是正确语义——rank 反映的是当前全量视图下的排名。

`tests/test_incremental.py::test_incremental_append_new_data` 验证：追加 10 个高金额新订单后，目标 `customer_id` 的 `orders` 累加正确且进入 `customer_value` top。

## 16. 增量故障排查

| 现象 | 原因与处理 |
|---|---|
| 水位被脏值污染（如 `bad_date` 缺陷注入的非法日期） | `generator.defect_rates.bad_date` 会注入格式错误的 `order_date`。ingest 计算 `max(order_date)` 时若遇到脏值，水位可能错误推进。处理：① 脏行在 validate 阶段会被隔离（`invalid_date` 原因码），不影响下游；② 若怀疑水位污染，检查 `state/state.json` 的 `watermark_value` 是否在合理区间（如 `2020-01-01 ~ 2026-12-31`）；③ 必要时删除 `state/` 目录，下次运行重新全量建水位 |
| 零增量（二跑无新数据，`orders_incremental.csv` 行数为 0） | 这是正常现象。ingest 过滤 `order_date > watermark_value` 无新行，产出 0 行增量。validate / clean / compute 处理空集，DQ Score=1.0（无新增检查项），水位不变。`test_incremental_no_new_data_second_run` 覆盖此场景。若业务上期望有新数据却零增量，检查源文件是否真的追加了 `order_date > 水位` 的行 |
| 聚合不一致（`state/aggregates/` 数值与全量重算不符） | 可能原因：① 源数据有更新/删除（高水位模式只捕获 append，不捕获 update/delete）；② 水位曾被污染导致漏处理某批数据；③ 手动修改过 `state/aggregates/` 文件。处理：删除 `state/` 目录，下次运行全量重建水位与聚合，对比数值。若源数据存在 update/delete，已支持，切换 `incremental.mode="iceberg_snapshot_diff"`（详见第 23 节） |
| 失败重跑后聚合重复累加 | 不应发生。当前实现 compute 只写批次目录 `04_aggregates/`，pipeline 在全部成功后才 merge 到 `state/aggregates/`。失败时 `04_aggregates/` 在批次目录中隔离，不污染 state。`test_incremental_failure_idempotent` 验证此幂等性。若观察到重复累加，检查是否手动执行了 merge 或 `aggregates_persist` 配置错误 |
| `state.json` 不生成 | 检查 `incremental.enabled` 是否为 `true`。`false` 时 pipeline 不构造 `StateStore`，不碰 state 目录（`test_full_mode_regression` 验证全量模式不生成 state.json） |

## 17. FAQ

- **为什么零依赖？** 保证一键复现；引擎层已抽象为五阶段接口，替换 pandas/Spark 不影响流水线骨架。
- **数据每次运行都重新生成？** 是（seed 固定可复现）；关闭 generator.enabled 即可改用自有数据。
- **怎么找回上一次结果？** run\latest.json 指针；每个批次目录独立保存，不会互相覆盖。
- **异常值为什么只标记不拒收？** 设计为 action=flag（可改 reject）；统计方法只适合标记，硬性拒收应由业务规则承担。
- **DQ Score 口径？** 全部规则检查项的简单平均通过率（checks_passed / checks_total），口径写死在质量报告里。

## 18. engine 配置（engine 段）

Phase 2a 列式加速 + Phase 2b 分布式加速能力通过 `config/pipeline.json` 的 `engine` 段开启，与 `incremental` 段平级。`config/pipeline_small.json` 提供完整示例。各字段说明如下：

表：engine 段字段说明表

| 字段 | 取值 | 含义 |
|---|---|---|
| `backend` | `python`（缺省）/ `polars` / `spark` | 执行引擎后端。`python` 走现有 `csv_read` / `csv_write` 路径，零依赖；`polars` 走 `table_read` / `table_write` 统一 IO 接口的列式路径，需 `pip install polars`；`spark` 走 Spark DataFrame API 分布式路径，需 `pip install pyspark` + JDK 11+/17（详见第 21 节） |
| `format` | `csv`（缺省）/ `parquet` | 产物存储格式。`csv` 与现有行为一致；`parquet` 列式压缩，配合 `backend="polars"` / `"spark"` 获得 3-6 倍压缩 + 谓词下推 |
| `polars.streaming` | `true` / `false`（缺省） | Polars streaming 模式。`true` 时允许超内存数据集 spill 到磁盘，支持千万行以上 |
| `polars.parquet_compression` | `zstd`（缺省）/ `snappy` / `gzip` | Parquet 压缩算法。`zstd` 压缩比与速度综合最优 |
| `polars.read_options` | dict | 传给 `pl.read_csv` 的参数。`try_parse_dates=true` 自动把日期列解析为 Date/Datetime 类型 |
| `spark.master` | URL | Spark 集群 master。`local[*]`（本地模式，缺省）/ `spark://localhost:15077`（Docker Compose Standalone 集群，多机模式已实现）/ `k8s://https://...`（K8s）。详见第 21 节 |
| `spark.app_name` | str | Spark 应用名（Spark UI 显示） |
| `spark.executor_memory` | str | 每个 executor 内存，如 `1g` / `4g` |
| `spark.executor_cores` | int | 每个 executor CPU 核数 |
| `spark.num_executors` | int | executor 实例数（多机模式生效） |
| `spark.driver_memory` | str | driver 进程内存，如 `512m` / `2g` |
| `spark.shuffle_partitions` | int | shuffle 分区数，影响并行度。本地小数据建议 4，多机建议 200 |
| `spark.adaptive_query_execution` | bool | AQE 开关（自动合并小分区、处理倾斜），缺省 `true` |
| `spark.write_single_file` | bool | 写出时是否 `coalesce(1)` 合并为单文件，缺省 `false`（保留分区并行写出） |
| `spark.max_result_size` | str | driver 端 action 结果序列化上限，缺省 `"1g"`（Spark 默认）。千万行级 join/broadcast 的单 task 序列化结果会超限，需放大（如 `"4g"`） |
| `spark.read_options` | dict | 传给 `spark.read.csv` 的参数 |

配置示例（摘自 `config/pipeline_small.json`）：

```json
"engine": {
  "backend": "python",
  "format": "csv",
  "polars": {
    "streaming": false,
    "parquet_compression": "zstd",
    "read_options": {"try_parse_dates": true}
  },
  "spark": {
    "master": "local[*]",
    "app_name": "autobatch-small",
    "executor_memory": "1g",
    "executor_cores": 1,
    "num_executors": 1,
    "driver_memory": "512m",
    "shuffle_partitions": 4,
    "adaptive_query_execution": true,
    "write_single_file": false,
    "read_options": {}
  }
}
```

### 18.1 backend 路由机制

`src/helpers.py` 的 `table_read` / `table_write` 是统一 IO 收口点，按 `_get_engine_backend(cfg)` 路由：

- `backend="python"`：`table_read` 内部直接调用 `csv_read`，返回 `(List[Dict], fields)` 元组；`table_write` 调用 `csv_write`。行为与 Phase 1 完全一致
- `backend="polars"`：`table_read` 调用 `pl.read_csv` / `pl.read_parquet`，返回 `polars.DataFrame`；`table_write` 调用 `df.write_csv` / `df.write_parquet`。各 stage 通过 `ctx.engine_backend` 判断走列式分支
- `backend="spark"`：`table_read` 调用 `spark.read.csv` / `spark.read.parquet`，返回 `SparkDataFrame`（分布式跨 executor 分区）；`table_write` 调用 `df.write.csv` / `df.write.parquet`，触发 Spark action 分布式写出多分区文件。各 stage 通过 `ctx.engine_backend` + `ctx.spark_session` 判断走 Spark 分支

`polars` 与 `pyspark` 均采用 lazy import：仅 `backend="polars"` 时才 `import polars`，仅 `backend="spark"` 时才 `import pyspark`，保持 `backend="python"` 路径零额外依赖。`PipelineContext.engine_backend` 字段（缺省 `"python"`）镜像 `cfg["engine"]["backend"]`，`PipelineContext.spark_session` 字段（缺省 `None`）在 `backend="spark"` 时由 `pipeline.py` 的 `_init_spark_session` 注入，由 `pipeline.py` 在编排前同步到 ctx。

### 18.2 各 stage 的 Polars / Spark 分支

表：各 stage Polars 与 Spark 分支关键改动对照表

| 阶段 | Polars 分支关键改动 | Spark 分支关键改动 |
|---|---|---|
| ingest | 增量路径 `_copy_incremental_polars` 用 `pl.scan_csv(src).filter(pl.col(wm_col) > wm_value).collect().write_csv(dst)` 流式过滤；水位计算用 `df.select(pl.col(wm_col).max())` | 全量路径 `_ingest_full_spark` 用 `table_read(src, cfg, spark=ctx.spark_session)` 读为 SparkDataFrame；增量路径 `_copy_incremental_spark` 用 `spark.read.csv(src).filter(F.col(wm_col) > wm_value)` 分区并行过滤；水位用 `df.agg(F.max(wm_col))` |
| validate | `RuleEngine._check_polars` 走向量化路径：completeness / range / allowed_values 用列表达式；referential 用 `orders.join(ref, on="customer_id", how="anti")` 一次找孤儿行；format / date_valid 仍 Python 逐行算 mask（Polars regex / 多格式 date 解析支持有限）；outlier 用 Polars quantile 算 bounds | `RuleEngine._check_spark` 走 Spark SQL 表达式：completeness / range / allowed_values 用 `F.col` 表达式；referential 用 `orders.join(ref, on="customer_id", how="left_anti")` 跨 executor 分布式找孤儿行；outlier 用 Spark approxQuantile 算 bounds；format / date_valid 用 `F.regexp_extract` / `F.to_date` 列表达式 |
| clean | `_clean_orders_polars` 用 `df.unique()` 去重、`df.with_columns(pl.col().fill_null())` 补缺、`pl.col("quantity") * pl.col("unit_price")` 算 `total_amount` | `_clean_orders_spark` 用 `df.dropDuplicates(["order_id"])` 去重、`df.fillna()` 补缺、`F.col("quantity") * F.col("unit_price")` 算 `total_amount` |
| compute | 四个聚合函数增加 Polars 表达式分支：`daily_sales_polars` 用 `df.group_by("order_date").agg(...)`、`category_stats_polars` 用 `df.join(products).group_by("category")`、`customer_value_polars` 用 `df.group_by("customer_id").agg(...).sort("revenue", descending=True).head(top_n)` | 四个聚合函数增加 Spark 表达式分支：`daily_sales_spark` 用 `df.groupBy("order_date").agg(F.sum("total_amount"))`、`category_stats_spark` 用 `df.join(products).groupBy("category")`、`customer_value_spark` 用 `df.groupBy("customer_id").agg(...)` + 窗口函数 `F.row_number().over(Window.orderBy(F.desc("revenue")))` 取 Top N |
| output | `_write_orders_final_polars` 用 `pl.read_csv` → 加标记列 → `table_write` | `_write_orders_final_spark` 用 `spark.read.csv` → 加标记列 → `table_write`（`df.write.csv` 分布式写出多分区文件） |

### 18.3 与 Phase 1 增量的正交叠加

`incremental.enabled=true` + `engine.backend="polars"` / `"spark"` 可同时生效，互不互斥：

**Polars 叠加**：

- **ingest**：增量 + Polars 流式过滤，`pl.scan_csv().filter(wm_col > wm_value).collect()` 既只读新增行又走列式扫描
- **validate**：外键参考表（customers / products）仍全量 `table_read` 到 Polars DataFrame；增量 orders 用 Polars 表达式批量校验
- **compute**：增量分桶用 `df.group_by(key).agg(...)` 生成 delta buckets；`pipeline._advance_and_merge` 中 `store.merge_aggregate` 改用 `pl.concat([history_df, delta_df]).group_by(key).agg(...)` 一次合并
- **state 持久化**：`state/aggregates/*.csv` 可改写为 Parquet（`engine.format="parquet"`），merge 用 Polars concat + group_by，比 Python dict merge 快 10-50 倍

**Spark 叠加**：

- **ingest**：增量 + Spark 分区并行过滤，`spark.read.csv(src).filter(F.col(wm_col) > wm_value)` 既只读新增行又跨 executor 并行扫描
- **validate**：外键参考表（customers / products）仍全量 `table_read` 到 SparkDataFrame；增量 orders 用 Spark SQL 表达式批量校验
- **compute**：增量分桶用 `df.groupBy(key).agg(...)` 生成 delta；`pipeline._advance_and_merge` 调用 `_merge_aggregate_spark`，用 `history_df.unionByName(delta_df).groupBy(key).agg(...)` 分布式合并
- **state 持久化**：`state/aggregates/*.csv` 由 Spark 读写，merge 用 union + groupBy，跨机分布式合并；多机模式待 Phase 3 MinIO 共享存储就位后推进

## 19. Python vs Polars vs Spark 路径选择建议

### 19.1 按数据量选择

表：路径选择对照表（按数据量）

| 数据量（orders 行数） | 推荐路径 | 推荐格式 | 理由 |
|---|---|---|---|
| < 5 万 | `python` | `csv` | 零依赖，启动快，Python 循环开销可忽略 |
| 5 万 ~ 100 万 | `polars` | `csv` 或 `parquet` | validate / compute 阶段 Python 循环成为瓶颈，Polars 向量化 + 多线程收益明显 |
| 100 万 ~ 1000 万 | `polars` | `parquet` | 列式压缩节省 IO + 谓词下推 + streaming spill 到磁盘 |
| 1000 万 ~ 1 亿（单机内存足够） | `polars`（streaming=true） | `parquet` | 单机内存上限附近，需 streaming |
| > 1 亿 或 超单机内存 | `spark` | `parquet` | 分布式 shuffle 突破单机内存上限；多机模式需共享存储（MinIO/HDFS，Phase 3） |

### 19.2 按环境选择

| 环境 | 推荐路径 | 理由 |
|---|---|---|
| CI / 教学 / 演示 | `python` | 零依赖，无需 `pip install`，矩阵测试简单 |
| 生产单机批处理 | `polars` | 单机加速，零运维，pip 安装即可 |
| 容器化部署 | `polars` | Dockerfile 加 `pip install polars` 一行，镜像增约 30MB |
| 已部署 Spark 集群 | `spark` | 复用现有集群资源，分布式 shuffle 突破单机上限 |
| 超大数据 + 多机需求 | `spark` | 千万行以上或超内存，需多 executor 并行；配合 Phase 3 MinIO 共享存储（多机模式已实现，见第 21.3 节） |
| 无外网隔离环境 | `python` | 无法 pip 安装时退回零依赖路径 |

### 19.3 切换步骤

**切换到 Polars**：

1. 安装依赖：`pip install polars`（一次性，Polars 1.43+）
2. 修改配置：`config/pipeline.json` 或 `pipeline_small.json` 的 `engine.backend` 改为 `"polars"`
3. 运行：`python main.py --config config/pipeline_small.json`
4. 验证产物：与 `backend="python"` 路径产物完全一致（行数、聚合值、DQ Score、manifest lineage、metrics）。`tests/test_engine_polars.py::test_polars_full_run_equals_python` 自动覆盖此等价性
5. 回退：`engine.backend` 改回 `"python"` 即恢复零依赖路径

**切换到 Spark**：

1. 安装依赖：`pip install pyspark`（一次性，PySpark 4.x）+ JDK 11+ 或 17
2. Windows 额外配置：下载 `winutils.exe` + `hadoop.dll` 放到 `%HADOOP_HOME%\bin\`，设环境变量 `HADOOP_HOME`（详见第 21.4 节）。**多机模式不需要 hadoop.dll**（Worker 在 Linux 容器内执行写文件，driver 不直接写）
3. 修改配置：`config/pipeline.json` 或 `pipeline_small.json` 的 `engine.backend` 改为 `"spark"`，按需调 `engine.spark.master` / `executor_memory` / `shuffle_partitions`
4. 运行：`python main.py --config config/pipeline_small.json`
5. 验证产物：与 `backend="python"` 路径产物完全一致（行数、聚合值、DQ Score、manifest lineage、metrics）。`tests/test_engine_spark.py::test_spark_full_equivalence` 自动覆盖此等价性（Windows 需装齐 `hadoop.dll` 才不 skip）
6. 回退：`engine.backend` 改回 `"python"` 或 `"polars"` 即恢复路径

**切换到 Spark 多机模式**：

1. 安装 driver 端依赖：`pip install pyspark==4.2.0` + JDK 17（建议 junction 路径 `F:\jdk17` / `F:\spark_home` / `F:\Py314` 避免空格）
2. 启动 MinIO + 创建 bucket `autobatch`（见第 22.2 节）
3. 启动 Spark 集群：`pwsh docker/spark-cluster/up.ps1`（自动 build 镜像 + 启动 Master/Worker + 连接 MinIO 网络）
4. 修改配置：`engine.backend="spark"` + `engine.spark.master="spark://localhost:15077"` + `engine.spark.cluster.enabled=true` + `storage.backend="parquet"` + `storage.endpoint="localhost:9000"`
5. 运行：`python main.py --config config/pipeline_small.json`
6. 验证产物：与 `backend="python"` 路径产物完全一致。`tests/test_engine_spark.py::test_cluster_spark_s3_equivalence` 自动覆盖多机模式等价性
7. 停止集群：`pwsh docker/spark-cluster/down.ps1`
8. 回退：`engine.spark.master` 改回 `"local[*]"` + `engine.spark.cluster.enabled=false` 即回退本地模式

## 20. Polars 故障排查

表：Polars 路径常见问题与处理

| 现象 | 原因与处理 |
|---|---|
| `ModuleNotFoundError: No module named 'polars'` | 未安装 polars。处理：`pip install polars`（1.43+）。`backend="python"` 路径不受影响，无需安装 |
| `AttributeError: 'Expr' object has no attribute 'is_first'` | Polars 1.43+ 移除了 `Expr.is_first()`，改用 `is_first_distinct()`。当前代码已用 `is_first_distinct()`，若报此错说明 polars 版本过旧，升级到 1.43+ |
| Polars 路径产物与 python 路径不一致 | 不应发生。`tests/test_engine_polars.py::test_polars_full_run_equals_python` 覆盖此等价性。若观察到差异，检查：① polars 版本是否 1.43+；② config 是否还残留其他改动；③ 上游数据是否在运行间被修改 |
| `pl.read_csv` 日期解析失败（日期列变成 Utf8） | `engine.polars.read_options.try_parse_dates` 缺省 `true` 会自动解析。若日期格式非标准（如 `2026/08/15`），Polars 可能不识别，该列保留 Utf8，下游 format / date_valid 规则仍用 Python 逐行算 mask，不影响正确性 |
| `engine.format="parquet"` 时产物路径多了 `.parquet` 后缀 | 设计如此。`table_write` 在 parquet 格式下自动补 `.parquet` 后缀，`table_read` 同样自动尝试 `.parquet`。CSV 与 Parquet 产物不会混存 |
| Polars 路径比 python 路径慢 | 不应发生。若观察到，可能：① 数据量太小（< 1000 行），Polars 启动开销大于收益；② polars 版本过旧；③ 单线程机器（Polars 多线程收益为 0）。处理：小数据量用 `backend="python"` |
| `engine.backend="polars"` + `incremental.enabled=true` 行为异常 | 两能力正交叠加，`tests/test_engine_polars.py::test_polars_incremental_combination` 覆盖。若观察到异常，检查 state 目录是否被污染（删除 `state/` 重建），或 polars 版本是否 1.43+ |
| Parquet 写出报 `ComputeError` 类型推断失败 | 某列含混合类型或全 null。处理：在 `engine.polars.read_options` 加 `infer_schema_length=10000` 增加推断样本，或显式声明 schema（当前未暴露 schema 配置，可临时改 `backend="python"` 跑通后对账） |

## 21. Spark 配置与运行（Phase 2b）

Phase 2b 分布式加速能力通过 `config/pipeline.json` 的 `engine.spark` 段开启，`engine.backend="spark"` 时生效。本地模式 `master="local[*]"` 与多机模式 `master="spark://localhost:15077"`（Docker Compose Standalone 集群）均已实现并上线（2026-08-16）。多机模式通过 S3A connector 连接 MinIO 共享存储、Worker 内 socat 代理解决 `localhost:9000` → `minio:9000` 网络寻址问题。

### 21.1 engine.spark 段配置说明

表：engine.spark 段字段说明表

| 字段 | 取值 | 含义 |
|---|---|---|
| `master` | URL | Spark 集群 master。`local[*]`（本地模式，缺省，用本机所有 CPU 核）/ `local[N]`（限 N 核）/ `spark://master:7077`（Standalone 集群）/ `yarn`（Hadoop YARN）/ `k8s://https://...`（K8s 集群） |
| `app_name` | str | Spark 应用名，Spark UI（默认 `http://driver:4040`）显示 |
| `executor_memory` | str | 每个 executor 内存，如 `1g` / `4g`。本地模式建议 `1g`，多机建议 `4g`+ |
| `executor_cores` | int | 每个 executor CPU 核数。本地模式建议 1，多机建议 2-4 |
| `num_executors` | int | executor 实例数。本地模式固定 1，多机模式按集群规模设 4/8/16 |
| `driver_memory` | str | driver 进程内存，如 `512m` / `2g`。driver 负责任务调度与结果收集，无需太大 |
| `shuffle_partitions` | int | shuffle 分区数，影响并行度。本地小数据建议 4，多机建议 200（约等于 executor_cores × num_executors × 2-3） |
| `adaptive_query_execution` | bool | AQE 开关（Spark 3.x+），自动合并小分区、处理 shuffle 倾斜，缺省 `true` |
| `write_single_file` | bool | 写出时是否 `coalesce(1)` 合并为单文件。`false`（缺省）保留分区并行写出（性能优）；`true` 合并为单文件（便于下游单文件消费，但触发 driver 单点收集） |
| `read_options` | dict | 传给 `spark.read.csv` 的参数，如 `{"header": true, "inferSchema": true}` |

### 21.2 Spark 本地模式运行

本地模式 `master="local[*]"` 在单机内模拟 Spark 集群，driver + executor 同进程，用于开发、调试、CI 验证 Spark 分支逻辑。无需部署 Spark 集群，仅需 `pip install pyspark` + JDK。

```json
"engine": {
  "backend": "spark",
  "format": "csv",
  "spark": {
    "master": "local[*]",
    "app_name": "autobatch-local",
    "executor_memory": "1g",
    "executor_cores": 1,
    "num_executors": 1,
    "driver_memory": "512m",
    "shuffle_partitions": 4,
    "adaptive_query_execution": true
  }
}
```

运行：`python main.py --config config/pipeline_small.json`。Spark UI 在 `http://localhost:4040`（仅运行期间可用）。`pipeline.py` 在 `finally` 块中 `spark.stop()` 确保 SparkSession 总是释放。

### 21.3 Spark 多机模式运行（Docker Compose Standalone 集群，已实现 2026-08-16）

多机模式通过 Docker Compose 部署 Spark Standalone 集群（Master + 2 Worker）+ MinIO 共享存储 + S3A connector + socat 代理实现。**S3 存储（MinIO）是多机模式的必要条件**——多 executor 无法共享 driver 本地 FS 路径，必须通过 S3 协议访问共享存储。

#### 21.3.1 集群架构

图：Spark Standalone 集群架构示意图

```
                ┌─────────────────── 宿主机 (Windows / Docker Desktop) ───────────────────┐
                │                                                                       │
   driver       │  ┌───────────────── autobatch-net (bridge network) ─────────────────┐  │
   (python  ────┼─▶│ spark-master:7077 (RPC) / :8080 (Web UI)                          │  │
   main.py)     │  │     │                                                              │
                │  │     ├── spark-worker-1:8081 (Web UI), 2 cores / 2g memory         │  │
                │  │     │     └─ socat localhost:9000 → minio:9000                    │  │
                │  │     └── spark-worker-2:8082 (Web UI), 2 cores / 2g memory         │  │
                │  │           └─ socat localhost:9000 → minio:9000                    │  │
                │  │                                                                       │  │
                │  │  minio:9000 (S3 API) / :9001 (Web Console)  ← connect-minio.ps1   │  │
                │  └───────────────────────────────────────────────────────────────────┘  │
                │     端口映射: 15077/8080/8081/8082/9000/9001 → localhost                │
                └───────────────────────────────────────────────────────────────────────┘
```

各组件端口与用途：

表：Spark 集群组件端口说明表

| 组件 | 容器内端口 | 宿主机端口 | 用途 |
|---|---|---|---|
| Spark Master | 15077 | 15077 | Spark RPC（driver 与 executor 通信） |
| Spark Master Web UI | 8080 | 8080 | Master 状态 / Worker 列表 / 应用列表 |
| Spark Worker-1 Web UI | 8081 | 8081 | Worker-1 资源 / 执行的 executor |
| Spark Worker-2 Web UI | 8081 | 8082 | Worker-2 资源 / 执行的 executor |
| MinIO S3 API | 9000 | 9000 | S3 协议端点（`storage.endpoint=localhost:9000`） |
| MinIO Web Console | 9001 | 9001 | 浏览器管理 bucket / 凭证 |

#### 21.3.2 部署步骤

**前置条件**：

- Docker Desktop 4.x+（Windows 下建议 WSL2 后端，分配 ≥ 4 GB 内存给 Docker）
- MinIO 容器已启动（见第 22.2 节），bucket `autobatch` 已创建
- JDK 17（driver 端，宿主机；Worker 内容器自带 JRE 17）
- PySpark 4.2.0（driver 端，宿主机 `pip install pyspark==4.2.0`）
- Windows junction 路径建议：`F:\spark_home` → Spark 安装目录、`F:\jdk17` → JDK 17、`F:\Py314` → Python 3.14（避免路径含空格，见第 21.5 节故障排查）

**步骤 1：启动 Spark 集群**

命令示例：一键启动 Spark Standalone 集群

```bash
pwsh docker/spark-cluster/up.ps1
```

`up.ps1` 自动执行 4 步：

1. `docker compose up -d --build`：构建镜像（基于 `eclipse-temurin:17-jre` + Spark 4.2.0 + hadoop-aws 3.5.0 + aws-sdk-v2-bundle 2.35.4 + analyticsaccelerator-s3 1.3.1，JAR 构建时打入 `/opt/spark/jars/` 避免运行时分发 530 MB）并启动 Master + 2 Worker 容器
2. 等待 Master Web UI `http://localhost:8080` 就绪（最多 30 次重试，每次间隔 3 秒）
3. 调用 `connect-minio.ps1` 把已运行的 MinIO 容器加入 `autobatch-net` 网络（使 Worker 可经容器名 `minio:9000` 访问）
4. 调用 Master REST API `http://localhost:8080/api/v1/workers` 验证 Worker 已注册并 ALIVE

启动成功后输出：

```
==============================================
  集群启动完成！
  Master Web UI:  http://localhost:8080
  Master RPC:     spark://localhost:15077
  Worker-1 UI:    http://localhost:8081
  Worker-2 UI:    http://localhost:8082
==============================================
```

**步骤 2：把 MinIO 加入集群网络（如未自动执行）**

命令示例：把 MinIO 加入 autobatch-net 网络

```bash
pwsh docker/spark-cluster/connect-minio.ps1
```

- 自动查找运行中的 MinIO 容器（按 `ancestor=minio/minio` 或容器名 `minio`）
- 检查是否已在 `autobatch-net` 网络中，已在则跳过；否则 `docker network connect autobatch-net <minio_container>`
- 验证 MinIO 在 `autobatch-net` 中的 IP

**步骤 3：Worker 内 socat 代理（已内置于 entrypoint.sh）**

Worker 容器 `entrypoint.sh` 启动时自动执行：

```bash
socat TCP-LISTEN:9000,fork,reuseaddr TCP:minio:9000 &
```

作用：让 Worker 用 `localhost:9000` 访问 MinIO（与 driver 一致）。driver 在宿主机用 `localhost:9000` 直连 MinIO 端口映射；Worker 在容器内 `localhost:9000` 经 socat 代理转到 `minio:9000`（容器互连）。这样 driver 与 Worker 用统一的 `storage.endpoint="localhost:9000"` 配置，无需 Worker 单独配 MinIO 容器名。

**步骤 4：配置 AutoBatch 多机模式**

配置示例（摘自 `config/pipeline_small.json` 的 `engine` + `storage` 段）

```json
"engine": {
  "backend": "spark",
  "format": "parquet",
  "spark": {
    "master": "spark://localhost:15077",
    "app_name": "autobatch-cluster",
    "executor_memory": "2g",
    "executor_cores": 2,
    "num_executors": 2,
    "driver_memory": "1g",
    "shuffle_partitions": 8,
    "adaptive_query_execution": true,
    "write_single_file": false,
    "read_options": {},
    "cluster": {
      "enabled": true,
      "driver_host": "host.docker.internal",
      "s3_endpoint": ""
    }
  }
},
"storage": {
  "backend": "parquet",
  "bucket": "autobatch",
  "endpoint": "localhost:9000",
  "access_key": "minioadmin",
  "secret_key": "minioadmin",
  "secure": false,
  "region": "us-east-1",
  "warehouse": "warehouse",
  "prefix": "",
  "compression": "zstd"
}
```

`engine.spark.cluster` 子段说明：

表：engine.spark.cluster 子段字段说明表

| 字段 | 取值 | 含义 |
|---|---|---|
| `enabled` | `true` / `false`（缺省） | 多机模式开关。`true` 时启用 S3A connector + driver_host 配置；`false` 走本地模式路径 |
| `driver_host` | str | driver 主机名（容器内访问宿主机用 `host.docker.internal`，Linux 主机用宿主机 IP 或 `host.docker.internal` 若 Docker 支持） |
| `s3_endpoint` | str | Worker 端 S3 endpoint 覆盖（缺省 `""` 用 `storage.endpoint`；如需 Worker 用不同 endpoint 可在此指定） |

**步骤 5：运行流水线**

命令示例：多机模式运行

```bash
python main.py --config config/pipeline_small.json
```

driver 在宿主机创建 SparkSession 连接 `spark://localhost:15077`，五阶段跨 Master/Worker 分布式执行，executor 通过 S3A connector 读写 `s3a://autobatch/warehouse/.../*.parquet`。

**步骤 6：停止集群**

命令示例：停止并移除 Spark 集群容器

```bash
pwsh docker/spark-cluster/down.ps1
```

执行 `docker compose down`，移除 Master + 2 Worker 容器（MinIO 容器不动，数据卷持久化）。

#### 21.3.3 部署要求

表：Spark 多机模式环境要求清单

| 组件 | 版本 | 用途 | 安装方式 |
|---|---|---|---|
| Docker Desktop | 4.x+（WSL2 后端） | 容器化部署 Master/Worker | Windows 安装包 |
| JDK | 17（driver 端） | driver JVM | 系统包管理器或官方 tarball，设 `JAVA_HOME` |
| PySpark | 4.2.0（driver 端） | Python 调用 Spark API | `pip install pyspark==4.2.0` |
| Python | 3.9+（driver 端） | driver 进程 | 系统自带或 pyenv |
| MinIO | latest | S3 共享存储 | `docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"` |
| Spark 集群 | 4.2.0（容器内） | 分布式调度 | `docker/spark-cluster/up.ps1` 一键部署 |
| hadoop-aws | 3.5.0（容器内） | S3A connector | Dockerfile 构建时打入 `/opt/spark/jars/` |
| aws-sdk-v2-bundle | 2.35.4（容器内） | hadoop-aws 依赖 | Dockerfile 构建时打入 `/opt/spark/jars/` |
| analyticsaccelerator-s3 | 1.3.1（容器内） | hadoop-aws 传递依赖 | Dockerfile 构建时打入 `/opt/spark/jars/` |
| socat | 容器内自带 | Worker→MinIO 代理 | Dockerfile `apt-get install socat` |

**Windows 特殊处理**：

- **junction 路径**：Windows 下 JDK / Spark / Python 路径含空格（如 `C:\Program Files\Java\...`）会导致 Spark 启动命令行被错误分词。建议用 `mklink /J` 创建无空格 junction 路径：`F:\spark_home` → Spark 安装目录、`F:\jdk17` → JDK 17、`F:\Py314` → Python 3.14，并设 `JAVA_HOME=F:\jdk17` / `SPARK_HOME=F:\spark_home` / `PYSPARK_PYTHON=F:\Py314\python.exe`
- **hadoop.dll 不需要**：多机模式下 Worker 在 Linux 容器内执行 `df.write.csv` / `df.write.parquet`，不需要 Windows 的 `hadoop.dll` Native IO 库。driver 端仅负责调度与结果收集，不直接写文件。这避免了 Windows 环境下 `Py4JJavaError: ... Hadoop NativeIO$Windows.access0` 问题
- **Docker Desktop WSL2 后端**：建议启用 WSL2 后端（默认），分配 ≥ 4 GB 内存给 Docker（Settings → Resources → Memory），否则 2 Worker × 2g memory 可能超出默认 2 GB 限制

#### 21.3.4 多机模式故障排查

表：Spark 多机模式常见问题与处理

| 现象 | 原因与处理 |
|---|---|
| `up.ps1` 卡在 "等待 Spark Master 就绪" | Master 容器启动失败。处理：① `docker logs spark-master` 看 Master 日志；② 确认 Docker Desktop 已启动且 WSL2 后端可用；③ 确认 15077/8080 端口未被占用（`netstat -an \| findstr "15077 8080"`）；④ 内存不足时调大 Docker 内存限制或减小 Worker `SPARK_WORKER_MEMORY` |
| Worker 不注册到 Master（Master Web UI Workers=0） | Worker 无法连接 Master。处理：① `docker logs spark-worker-1` 看 Worker 日志；② 确认 Worker 与 Master 在同一 `autobatch-net` 网络（`docker network inspect autobatch-net`）；③ 确认 `SPARK_MASTER_URL=spark://spark-master:7077`（容器名 `spark-master` 而非 `localhost`） |
| Worker 报 `Connection refused: minio:9000` | MinIO 未加入 `autobatch-net` 网络。处理：① 确认 MinIO 容器已启动（`docker ps \| findstr minio`）；② 重新执行 `pwsh docker/spark-cluster/connect-minio.ps1`；③ `docker network inspect autobatch-net` 确认 MinIO 在网络成员列表中 |
| Worker 报 `Connection refused: localhost:9000` | socat 代理未启动。处理：① `docker exec spark-worker-1 ps aux \| findstr socat` 确认 socat 进程存在；② 若不存在，`docker exec spark-worker-1 socat TCP-LISTEN:9000,fork,reuseaddr TCP:minio:9000 &` 手动启动；③ 检查 `entrypoint.sh` 是否被正确复制（Dockerfile `COPY entrypoint.sh /opt/entrypoint.sh`） |
| driver 报 `FileNotFoundException: s3a://autobatch/...` | bucket 不存在或 S3A 配置错误。处理：① 浏览器打开 `http://localhost:9001` 确认 bucket `autobatch` 已创建；② 确认 `storage.backend="parquet"` + `storage.bucket="autobatch"` + `storage.endpoint="localhost:9000"`；③ 确认 `engine.spark.cluster.enabled=true`；④ `docker logs spark-worker-1` 看 S3A 连接日志 |
| driver 报 `Connection refused: localhost:7077` | Master 未启动或端口未映射。处理：① `docker ps \| findstr spark-master` 确认容器运行；② `curl http://localhost:8080` 确认 Web UI 可达；③ 确认 `engine.spark.master="spark://localhost:15077"`（不是 `spark://master:7077`，driver 在宿主机用 `localhost`） |
| Docker Desktop 不稳定 / 容器频繁退出 | Docker Desktop 资源不足。处理：① Settings → Resources 调大 Memory ≥ 4 GB、CPU ≥ 2；② 启用 WSL2 后端；③ `docker system prune -a` 清理无用镜像/容器释放磁盘；④ 重启 Docker Desktop |
| 多机模式产物与 python 路径不一致 | 不应发生。`tests/test_engine_spark.py::test_cluster_spark_s3_equivalence` 覆盖此等价性。若观察到差异，检查：① MinIO bucket 是否被其他数据污染；② config 是否还残留其他改动；③ `docker logs spark-master` + `spark-worker-1` + `spark-worker-2` 看是否有 task 失败重试 |
| 多机模式比本地模式慢 | 不应发生（除非数据量极小）。可能原因：① 数据量 < 10 万行，分布式调度 + 网络开销大于并行收益；② Docker Desktop 网络性能差（WSL2 后端建议）；③ MinIO IO 瓶颈（单机 MinIO 磁盘）；④ `shuffle_partitions` 过大（小数据建议 8，非 200）。处理：小数据量用 `master="local[*]"` 本地模式 |

### 21.4 环境要求

表：Spark 路径环境要求清单

| 组件 | 版本 | 用途 | 安装方式 |
|---|---|---|---|
| JDK | 11+ 或 17（Spark 4.x 要求，JDK 8 不再支持） | Spark 内核 JVM | 系统包管理器或官方 tarball，设 `JAVA_HOME` |
| PySpark | 4.x（含 Spark 内核，约 200MB） | Python 调用 Spark API | `pip install pyspark` |
| Python | 3.9+ | driver / executor Python 进程 | 系统自带或 pyenv |
| winutils.exe + hadoop.dll | 对应 Hadoop 3.x | **仅 Windows**：Spark 写文件需 Hadoop Native IO | 下载放到 `%HADOOP_HOME%\bin\`，设 `HADOOP_HOME` 环境变量 |
| Spark 集群 | 4.2.0 | **仅多机模式**：分布式调度 | Standalone（`docker/spark-cluster/up.ps1` 一键部署）/ YARN / K8s |
| 共享存储 | MinIO / HDFS / S3 | **仅多机模式**：executor 共享数据 | MinIO Docker 启动（见第 22.2 节），多机模式已实现 |

**Windows 额外配置（重要）**：

Spark 在 Windows 上写文件（`df.write.csv` / `df.write.parquet`）需要 Hadoop Native IO 库（`hadoop.dll`），否则抛 `Py4JJavaError: ... Hadoop NativeIO$Windows.access0`。配置步骤：

1. 下载 `winutils.exe` + `hadoop.dll`（对应 Hadoop 3.x，可从 https://github.com/cdarlint/winutils 获取）
2. 创建目录如 `F:\hadoop\bin\`，把两个文件放进去
3. 设环境变量 `HADOOP_HOME=F:\hadoop`（`PATH` 会自动含 `%HADOOP_HOME%\bin`）
4. 确认 `JAVA_HOME` 已设且指向 JDK 11+ 或 17（路径不含空格，见第 21.5 节故障排查）

缺 `hadoop.dll` 时，`tests/test_engine_spark.py` 的 3 个用例会通过 `pytest.mark.skipif` 跳过（不是失败），代码逻辑完整正确，装齐后可直接运行。

### 21.5 Spark 故障排查

表：Spark 路径常见问题与处理

| 现象 | 原因与处理 |
|---|---|
| `ModuleNotFoundError: No module named 'pyspark'` | 未安装 pyspark。处理：`pip install pyspark`（4.x）。`backend="python"` / `"polars"` 路径不受影响，无需安装 |
| `Py4JJavaError: ... Hadoop NativeIO$Windows.access0` | **Windows 缺 hadoop.dll**。Spark 写文件需 Hadoop Native IO 库。处理：下载 `winutils.exe` + `hadoop.dll` 放到 `%HADOOP_HOME%\bin\`，设 `HADOOP_HOME` 环境变量。详见第 21.4 节。`tests/test_engine_spark.py` 在此场景下 `skipif` 跳过 |
| `Java Home not set` 或 `JAVA_HOME is not set` | 未设 `JAVA_HOME` 环境变量。处理：设 `JAVA_HOME` 指向 JDK 11+ 或 17 安装目录（如 `C:\Program Files\Java\jdk-17`，但路径含空格可能有问题，见下条） |
| `JAVA_HOME` 路径含空格导致 Spark 启动失败 | Spark 内部用 `JAVA_HOME` 拼命令行，路径含空格（如 `C:\Program Files\Java\...`）可能被错误分词。处理：把 JDK 装到无空格路径（如 `C:\jdk-17\`），或用 `Progra~1` 短路径名（`C:\Progra~1\Java\...`） |
| `UnsupportedClassVersionError` 或 `Java version mismatch` | JDK 版本与 Spark 不兼容。Spark 4.x 要求 JDK 11 或 17，不支持 JDK 8。处理：升级 JDK 到 11+ 或 17，确认 `java -version` 与 `JAVA_HOME` 一致 |
| `Python version mismatch` 在 executor 端 | driver 与 executor Python 版本不一致。处理：确保所有节点 Python 版本一致（3.9+），多机模式用 `PYSPARK_PYTHON` 环境变量显式指定 python 路径 |
| Spark 路径产物与 python 路径不一致 | 不应发生。`tests/test_engine_spark.py::test_spark_full_equivalence` 覆盖此等价性（Windows 需装齐 hadoop.dll 才不 skip）。若观察到差异，检查：① pyspark 版本是否 4.x；② config 是否还残留其他改动；③ 上游数据是否在运行间被修改 |
| Spark 路径比 python / polars 路径慢 | 小数据量（< 10 万行）Spark 启动 + JVM 预热开销大于并行收益。处理：小数据量用 `backend="python"` 或 `"polars"`，Spark 适合千万行以上或超内存场景 |
| `engine.backend="spark"` + `incremental.enabled=true` 行为异常 | 两能力正交叠加，`tests/test_engine_spark.py::test_spark_incremental` 覆盖。若观察到异常，检查 state 目录是否被污染（删除 `state/` 重建），或 pyspark 版本是否 4.x |
| Spark 写出产物是目录而非单文件 | 设计如此。`df.write.csv(path)` 写出 `path/` 目录下多个分区文件（part-00000-...csv 等）。如需单文件，设 `engine.spark.write_single_file=true`（触发 `coalesce(1)`，性能下降）。`table_read` 自动识别目录并读取所有分区文件 |
| 多机模式 executor 报 `FileNotFoundException` | executor 无法访问 driver 本地路径。多机模式必须用共享存储（MinIO/HDFS/S3），所有路径指向共享存储 URL。Phase 3 MinIO 已就位，多机模式已实现（见第 21.3 节） |
| `OutOfMemoryError: Java heap space` | executor 内存不足。处理：调大 `engine.spark.executor_memory`（如 `4g` → `8g`），或减小 `shuffle_partitions` 减少 shuffle 缓存压力 |

## 22. storage 配置与运行（Phase 3）

Phase 3 湖存储能力通过 `config/pipeline.json` 的 `storage` 段开启，与 `engine` 段平级正交。`storage.backend` 决定**存储介质**，`engine.backend` 决定**计算引擎**，两者解耦任意组合。`config/pipeline.json` + `pipeline_small.json` 已包含完整 `storage` 段示例。

### 22.1 storage 段配置说明

表：storage 段字段说明表

| 字段 | 取值 | 含义 |
|---|---|---|
| `backend` | `local_csv`（缺省）/ `parquet` | 存储介质后端。`local_csv` 走现有 CSV 路径，零依赖，向后兼容；`parquet` 走 Parquet 列式存储路径（本地 `.parquet` 文件或 S3/MinIO 远端存储），需 `pip install pyarrow minio` |
| `bucket` | str | S3/MinIO bucket 名，如 `autobatch`。`backend="parquet"` 且配了 `bucket` + `endpoint` 时走 S3 路径；不配或清空 `endpoint` 时降级为本地 `.parquet` 文件 |
| `endpoint` | host:port | S3/MinIO endpoint，如 `localhost:9000`（本地 MinIO）/ `minio:9000`（Docker 内）/ `s3.amazonaws.com`（AWS S3） |
| `access_key` | str | S3 access key。MinIO 默认 `minioadmin` |
| `secret_key` | str | S3 secret key。MinIO 默认 `minioadmin` |
| `secure` | bool | 是否用 HTTPS。本地 MinIO 缺省 `false`，云上 S3 缺省 `true` |
| `region` | str | S3 region。缺省 `us-east-1`，MinIO 不强制 region |
| `warehouse` | str | warehouse 子路径（bucket 下的逻辑仓库根）。缺省 `warehouse`。逻辑路径 `orders/orders_clean` 解析为 `s3://bucket/warehouse/orders/orders_clean.parquet` |
| `prefix` | str | bucket 下额外前缀（多租户隔离用）。缺省 `""`。如 `tenant_a/` 让所有产物落到 `s3://bucket/tenant_a/warehouse/...` |
| `compression` | str | Parquet 压缩算法。`zstd`（缺省，压缩比与速度综合最优）/ `snappy`（速度优先）/ `gzip`（兼容性最好）/ `none`（不压缩） |

配置示例（摘自 `config/pipeline.json`）：

```json
"storage": {
  "backend": "local_csv",
  "bucket": "autobatch",
  "endpoint": "localhost:9000",
  "access_key": "minioadmin",
  "secret_key": "minioadmin",
  "secure": false,
  "region": "us-east-1",
  "warehouse": "warehouse",
  "prefix": "",
  "compression": "zstd"
}
```

### 22.2 MinIO 部署

MinIO 是 S3 兼容的对象存储，单机一行命令启动，适合开发/测试/小规模生产。

**Docker 启动（推荐）**：

命令示例：本地启动 MinIO（开发环境）

```
docker run -d --name minio -p 9000:9000 -p 9001:9001 -v minio-data:/data minio/minio server /data --console-address ":9001"
```

- `9000`：S3 API 端口（`storage.endpoint=localhost:9000` 指向此）
- `9001`：Web 控制台端口（浏览器打开 `http://localhost:9001`）
- 默认凭证：access_key=`minioadmin`，secret_key=`minioadmin`（生产环境务必修改）
- 数据卷 `minio-data` 持久化到宿主机，重启不丢数据

**bucket 创建**：

1. 浏览器打开 `http://localhost:9001`，用 `minioadmin` / `minioadmin` 登录
2. 左侧 Buckets → Create Bucket → 名字填 `autobatch` → Create
3. 或用 mc 客户端：`mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/autobatch`
4. 或用 Python minio SDK：`from minio import Minio; c = Minio("localhost:9000", "minioadmin", "minioadmin", secure=False); c.make_bucket("autobatch")`

**默认凭证（开发环境）**：

| 项 | 值 |
|---|---|
| endpoint | `localhost:9000` |
| access_key | `minioadmin` |
| secret_key | `minioadmin` |
| bucket | `autobatch` |
| secure | `false` |

生产环境务必修改默认凭证、启用 HTTPS（`secure=true`）、配置多副本/纠删码。

### 22.3 S3 路径说明

`storage.backend="parquet"` 时，`table_read` / `table_write` 的 `path` 参数支持三种形式：

**逻辑路径**（推荐）：如 `orders/orders_clean`，由 `_resolve_s3_path` 解析为完整 S3 URI。解析规则：

```
s3://<bucket>/<prefix>/<warehouse>/<path>.parquet
```

示例：`bucket=autobatch` / `prefix=""` / `warehouse=warehouse` / `path=orders/orders_clean` → `s3://autobatch/warehouse/orders/orders_clean.parquet`

**完整 S3 URI**：如 `s3://autobatch/warehouse/orders/orders_clean.parquet`，原样使用不再拼接。

**本地路径**：如 `run/<batch>/03_clean/orders_clean.csv.parquet`，当 `os.path.exists(path)` 为真时走本地 pyarrow 读写（`_is_s3_target` 判定规则：本地文件存在优先走本地）。

`_is_s3_target` 判断规则（按优先级）：

1. path 以 `s3://` 开头 → S3
2. path 指向本地已存在的文件 → 本地（允许 `storage.backend="parquet"` 时读写本地 `.parquet`，便于单测与无 MinIO 环境的降级）
3. `cfg["storage"]` 配了 `bucket` + `endpoint` → S3（path 是逻辑路径，用 `_resolve_s3_path` 解析为 `s3://` URI）
4. 其余 → 本地

### 22.4 本地 Parquet 模式

不配 `bucket` / `endpoint`（或清空 `endpoint`）时，`storage.backend="parquet"` 降级为本地 `.parquet` 文件模式：产物写到本地路径 + `.parquet` 后缀，用 pyarrow 读写。无需 MinIO 实例，适合单机列式压缩场景。

配置示例（本地 Parquet 模式）：

```json
"storage": {
  "backend": "parquet",
  "compression": "zstd"
}
```

运行：`python main.py --config config/pipeline_small.json`。五阶段产物从 `.csv` 改为 `.csv.parquet`（如 `03_clean/orders_clean.csv.parquet`），列式压缩 3-6 倍，与 `local_csv` 路径产物内容完全一致（行数、聚合值、DQ Score、manifest lineage、metrics）。`tests/test_storage_parquet.py::test_local_parquet_equivalence` 自动覆盖此等价性。

### 22.5 S3/MinIO Parquet 模式

配了 `bucket` + `endpoint` 时，`storage.backend="parquet"` 走 S3/MinIO 远端存储：产物写到 `s3://bucket/warehouse/.../*.parquet`，任意节点通过 S3 协议访问。

配置示例（S3/MinIO Parquet 模式，摘自 `config/pipeline.json` 的 `storage` 段）：

```json
"storage": {
  "backend": "parquet",
  "bucket": "autobatch",
  "endpoint": "localhost:9000",
  "access_key": "minioadmin",
  "secret_key": "minioadmin",
  "secure": false,
  "region": "us-east-1",
  "warehouse": "warehouse",
  "prefix": "",
  "compression": "zstd"
}
```

运行：先启动 MinIO + 创建 bucket（见第 22.2 节），再 `python main.py --config config/pipeline_small.json`。`tests/test_storage_parquet.py::test_s3_parquet_equivalence` 自动覆盖此等价性（MinIO 不可用时 `skipif` 跳过）。

### 22.6 与 Phase 1 增量的集成

`incremental.enabled=true` + `storage.backend="parquet"` 同时生效，正交叠加：

- **ingest 增量读取**：Phase 1 的 `_copy_incremental` 改用 `table_read` + 谓词下推。`storage.backend="parquet"` 时，Parquet footer 的 min/max 统计让 `WHERE order_date > wm_value` 跳过不匹配的 row group，增量 IO 量与增量行数成正比（而非全表扫描）。水位计算 `max(order_date)` 用列裁剪，只读水位列
- **compute 增量 merge**：Phase 1 的 `_advance_and_merge` 读历史聚合 + 增量分桶合并。`storage.backend="parquet"` 时，历史聚合从 `s3://bucket/state/aggregates/daily_sales.parquet` 读，合并用 pyarrow concat + group_by，写出回 S3 Parquet。比 Phase 1 的 Python dict merge 快 10-50 倍
- **配置组合**：`incremental.enabled=true` + `storage.backend="parquet"` 同时生效，ingest 走增量 + 谓词下推，compute 走增量 merge + 列式聚合。Phase 1 的水位逻辑（`state.json` 的 `watermark` 字段、两阶段提交、幂等性）完全保留，只是底层 IO 从本地 CSV 改为 S3 Parquet

### 22.7 与 Phase 2a / 2b 的集成

`storage.backend` 与 `engine.backend` 正交组合形成多种执行路径：

- **Polars + S3 Parquet**（`engine.backend="polars"` + `storage.backend="parquet"`）：Polars 原生支持 S3 协议，`pl.read_parquet("s3://bucket/...", storage_options=...)` 直接读 S3 Parquet 到 Arrow 列式内存，零拷贝。`pl.scan_parquet` 谓词下推到 S3 IO 层。**这是 Phase 3 的推荐组合**——单机列式加速 + 对象存储压缩 + 谓词下推，无需 Spark
- **Spark + S3 Parquet**（`engine.backend="spark"` + `storage.backend="parquet"`）：Spark 通过 `s3a://` connector 读写 S3 Parquet，`spark.read.parquet("s3a://bucket/...")` 分布式跨 executor 扫描。**这解锁 Phase 2b 多机模式**——多 executor 通过 S3 共享存储，本地 FS 不再是瓶颈。多机模式已实现（2026-08-16），通过 Docker Compose Standalone 集群 + S3A connector + socat 代理部署，详见第 21.3 节。需配置 `engine.spark.cluster.enabled=true` + `engine.spark.cluster.driver_host="host.docker.internal"` + S3A endpoint/凭证（自动从 `storage` 段注入 Spark conf）

### 22.8 storage 故障排查

表：storage 路径常见问题与处理

| 现象 | 原因与处理 |
|---|---|
| `ModuleNotFoundError: No module named 'pyarrow'` | 未安装 pyarrow。处理：`pip install pyarrow`（14+）。`storage.backend="local_csv"` 路径不受影响，无需安装 |
| `ModuleNotFoundError: No module named 'minio'` | 未安装 minio SDK。处理：`pip install minio`（7+）。仅 S3 模式 bucket 初始化与迁移脚本需要，本地 Parquet 模式不需要 |
| MinIO 连接失败（`ConnectionRefusedError` / `EndpointConnectionError`） | MinIO 未启动或 endpoint 配置错误。处理：① 确认 MinIO 已启动（`docker ps` 看容器状态）；② 确认 `storage.endpoint` 正确（本地 `localhost:9000`，Docker 内 `minio:9000`）；③ 确认端口未被占用（`netstat -an \| findstr 9000`） |
| `S3Error: The specified bucket does not exist` | bucket 未创建。处理：在 MinIO 控制台或用 mc / minio SDK 创建 bucket（见第 22.2 节）。`tests/test_storage_parquet.py` 的 `_minio_available` 会自动尝试 `make_bucket`，但生产环境应预先创建 |
| `S3Error: Access Denied` / `InvalidAccessKeyId` | 凭证错误。处理：确认 `storage.access_key` / `secret_key` 与 MinIO 实例配置一致。MinIO 默认 `minioadmin` / `minioadmin`，生产环境可能已修改 |
| `OSError: [WinError 123]` 路径含非法字符 | Windows 下 S3 URI 被误当本地路径。处理：确认 `storage.backend="parquet"` 已生效（`_get_storage_backend` 读 `cfg["storage"]["backend"]`），且 `bucket` + `endpoint` 已配（否则 `_is_s3_target` 可能误判为本地） |
| `ArrowIOError: Failed to open Parquet file` | S3 上文件不存在或路径解析错误。处理：① 用 MinIO 控制台或 mc 检查 `s3://bucket/warehouse/...` 路径是否存在；② 确认 `storage.warehouse` / `prefix` 配置与产物路径拼接一致（见第 22.3 节） |
| `pyarrow.lib.ArrowInvalid: Unsupported compression type` | pyarrow 版本过旧，不支持 `zstd`。处理：升级 pyarrow 到 14+（`pip install --upgrade pyarrow`）。或改用 `snappy` / `gzip`（`storage.compression="snappy"`） |
| Parquet 路径产物与 local_csv 路径不一致 | 不应发生。`tests/test_storage_parquet.py::test_local_parquet_equivalence` + `test_s3_parquet_equivalence` 覆盖此等价性。若观察到差异，检查：① pyarrow 版本是否 14+；② config 是否还残留其他改动；③ 上游数据是否在运行间被修改 |
| `storage.backend="parquet"` + `incremental.enabled=true` 行为异常 | 两能力正交叠加，`tests/test_storage_parquet.py::test_incremental_parquet` 覆盖。若观察到异常，检查 state 目录是否被污染（删除 `state/` 重建），或 pyarrow 版本是否 14+ |
| Parquet 文件比 CSV 还大 | 不应发生（除非数据量极小或列过多）。处理：① 确认 `storage.compression` 不是 `none`；② 极小数据集（< 100 行）Parquet 元数据开销可能超过压缩收益，改用 `local_csv`；③ 检查数据是否已高度压缩（如全是重复值），此时列式压缩收益有限 |

## 23. Iceberg 湖表运维（Phase 4）

Phase 4 引入 Apache Iceberg 湖表格式，通过 `config/pipeline.json` 的 `storage.backend="iceberg"` 开启，与 `engine` 段、`incremental` 段平级正交。Iceberg 在 Parquet 列式存储之上提供 ACID 事务语义、time travel（快照回溯）、snapshot diff（增量原生 diff）与 schema evolution（schema 演化不重写数据）。底层 catalog 由 `storage.iceberg` 子段配置，`config/pipeline.json` + `pipeline_small.json` 已包含完整 `iceberg` 子段示例。

Phase 4 关键实现事实：

- **pyiceberg 0.12.0rc1**：Python 3.14 兼容（稳定版 0.11.1 仅支持 Python 3.10-3.13，因此 Phase 4 必须用 rc1+）
- **SQL catalog + SQLite**：开发/测试用，`uri="sqlite:///autobatch.db"`，零额外 Docker 服务
- **REST catalog**：生产部署用，`docker/iceberg/docker-compose.yml` 编排
- **snapshot diff API**：`Table.incremental_append_scan(from_snapshot_id_exclusive=..., to_snapshot_id_inclusive=...)`
- **表名解析**：pyiceberg 用 `warehouse.orders`，Spark 用 `autobatch.warehouse.orders`（多了一层 namespace）
- **Iceberg JAR**：最高支持 Spark 4.1（`iceberg-spark-runtime-4.1_2.13-1.11.0.jar`），Docker 用 Spark 4.2 + `ENABLE_ICEBERG` ARG 开关（缺省 `false`）
- **测试**：`tests/test_storage_iceberg.py`（13 测试，pyiceberg 未安装时 `skipif`）+ `tests/test_spark_iceberg.py`（10 测试：8 个环境守护 `skipif` + 2 个纯 config 验证）

### 23.1 Iceberg catalog 部署

Iceberg catalog 负责表的元数据寻址（snapshot pointer、schema、partition spec）。Phase 4 支持两种 catalog：SQL catalog（开发/测试）与 REST catalog（生产）。

#### 23.1.1 SQL catalog + SQLite（开发/测试）

SQL catalog 把元数据存到关系库，开发/测试用 SQLite 零额外服务。`uri="sqlite:///autobatch.db"` 在项目根生成 `autobatch.db` 文件，首次访问自动建表。

配置示例（SQL catalog，开发/测试）

```json
"storage": {
  "backend": "iceberg",
  "iceberg": {
    "catalog_type": "sql",
    "catalog_uri": "sqlite:///autobatch.db",
    "warehouse": "s3://autobatch/warehouse"
  }
}
```

特点：

- 零额外 Docker 服务，`sqlite:///autobatch.db` 自动建库
- 单机使用，不支持多客户端并发写 catalog（SQLite 文件锁）
- 适合 CI、本地开发、单元测试（`tests/test_storage_iceberg.py` 全部用 SQL catalog）
- warehouse 仍可指向 S3/MinIO（数据文件远端，元数据本地 SQLite）

#### 23.1.2 REST catalog（生产）

REST catalog 把元数据寻址逻辑放到独立服务（tabulario/iceberg-rest 等），客户端只发 HTTP 请求。生产部署用 `docker/iceberg/docker-compose.yml` 编排。

命令示例：启动 REST catalog（生产）

```bash
docker compose -f docker/iceberg/docker-compose.yml up -d
docker compose -f docker/iceberg/docker-compose.yml up -d
```

配置示例（REST catalog，生产）

```json
"storage": {
  "backend": "iceberg",
  "iceberg": {
    "catalog_type": "rest",
    "catalog_uri": "http://localhost:8181",
    "warehouse": "s3://autobatch/warehouse"
  }
}
```

REST catalog 服务端口与用途：

表：REST catalog 组件端口说明表

| 组件 | 端口 | 用途 |
|---|---|---|
| iceberg-rest | 8181 | REST catalog API（`storage.iceberg.uri` 指向此） |
| postgres（catalog 后端） | 5432 | catalog 元数据持久化（REST 服务后端） |
| minio（数据存储） | 9000 / 9001 | S3 API / Web Console（数据文件存储） |

停止：

命令示例：停止 REST catalog

```bash
docker compose -f docker/iceberg/docker-compose.yml down
```

#### 23.1.3 catalog 选项对比

表：Iceberg catalog 选项对照表

| catalog | 适用场景 | 额外服务 | 元数据存储 | 多客户端并发 | Phase 4 支持 |
|---|---|---|---|---|---|
| SQL + SQLite | 开发 / 测试 / CI | 无 | 本地 `autobatch.db` 文件 | 不支持（文件锁） | ✅（`catalog_type="sql"`） |
| REST | 生产 | iceberg-rest + postgres | postgres | 支持 | ✅（`catalog_type="rest"`） |
| Nessie | 多分支 / 多租户 | nessie server | nessie 后端 | 支持 | 未集成（可后续扩展） |
| Hive | 兼容现有 Hive Metastore | hive-metastore + thrift | RDBMS | 支持 | 未集成（可后续扩展） |

### 23.2 表创建与迁移

#### 23.2.1 自动创建

`storage.backend="iceberg"` 时，首次写入某张表会自动建表：`table_write` 检测到 catalog 中不存在该表，按写入数据的 schema + 默认 partition spec（按 `order_date` 日分区，参考表无分区）创建 Iceberg 表，然后 append 写入。后续写入复用已有表，每次 append 生成一个新 snapshot。

无需手工执行 DDL，零额外建表脚本。`tests/test_storage_iceberg.py::test_iceberg_table_write_read` 覆盖此路径。

#### 23.2.2 零拷贝迁移（tools/parquet_to_iceberg_migrate.py）

`tools/parquet_to_iceberg_migrate.py` 把现有 Parquet 文件注册为 Iceberg 表，**不移动/拷贝数据文件**，仅写 Iceberg metadata（manifest list、manifest、snapshot）。底层用 pyiceberg 的 `Table.add_files()` API，把已有 Parquet data file 登记到 Iceberg manifest，秒级完成。

命令示例：零拷贝迁移 Parquet → Iceberg

```bash
python tools/parquet_to_iceberg_migrate.py \
  --parquet-path s3://autobatch/warehouse/orders/orders_clean.parquet \
  --iceberg-table warehouse.orders
```

迁移后 Iceberg 表的 snapshot 0 指向原 Parquet 文件，可立即用 `table_read` 读取或 time travel。原 Parquet 文件不动，迁移可重复执行（幂等，已注册的文件会跳过）。

代码示例：迁移脚本核心逻辑（Python）

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default", **{
    "type": "sql",
    "catalog_uri": "sqlite:///autobatch.db",
    "warehouse": "s3://autobatch/warehouse",
})
# 自动建表（如不存在）+ 注册已有 Parquet 文件，零拷贝
table = catalog.create_table("warehouse.orders", schema=schema, exist_ok=True)
table.add_files(["s3://autobatch/warehouse/orders/orders_clean.parquet"])
```

适用场景：从 Phase 3（`storage.backend="parquet"`）平滑升级到 Phase 4，不重写历史数据。

### 23.3 Time Travel 操作

Iceberg 每次写操作（append / overwrite / delete）生成一个不可变 snapshot，snapshot 链按 parent_id 串联。Phase 4 暴露两个 API 读取历史快照：

- `list_snapshots(table_name)`：返回该表所有 snapshot 的 `(snapshot_id, parent_id, timestamp, summary)` 列表，按时间倒序
- `read_history_snapshot(table_name, snapshot_id)`：读取指定 snapshot 的数据，等价于 `AT SNAPSHOT` 语义

代码示例：time travel 读取历史快照（Python）

```python
from src.iceberg import list_snapshots, read_history_snapshot

# 1. 列出所有快照
snapshots = list_snapshots("warehouse.orders", cfg)
for snap in snapshots:
    print(snap.snapshot_id, snap.timestamp, snap.parent_id, snap.summary)
# 9123456789 2026-08-16T10:00:00Z 9123456788 {"added-data-files":"1"}
# 9123456788 2026-08-16T09:00:00Z 9123456787 {"added-data-files":"1"}
# ...

# 2. 读取昨天 09:00 的快照（回溯到某个 snapshot_id）
rows, fields = read_history_snapshot("warehouse.orders", cfg, snapshot_id=9123456788)
print(f"历史快照行数: {len(rows)}")
```

应用场景：

- **数据回溯**：发现今天数据有问题，回读昨天快照对账
- **审计**：按 snapshot_id 查任意历史时刻的全量视图
- **可重复读**：固定 snapshot_id 做分析，不受后续写操作影响（snapshot 不可变）

`tests/test_storage_iceberg.py::test_iceberg_time_travel` 覆盖此路径：写入 3 批数据后，`read_history_snapshot` 各 snapshot 返回对应批次的行数。

### 23.4 Snapshot Diff 增量

#### 23.4.1 配置

`incremental.mode="iceberg_snapshot_diff"` + `storage.backend="iceberg"` 启用 Iceberg 原生 snapshot diff 替代 Phase 1 自建水位。`incremental.enabled=true` 仍需打开（开关复用），但 `incremental.tables.<表>.watermark_column` 不再生效（Iceberg 用 snapshot_id 寻址，不依赖业务列水位）。

配置示例（snapshot diff 增量）

```json
"incremental": {
  "enabled": true,
  "mode": "iceberg_snapshot_diff",
  "state_dir": "state",
  "tables": {
    "orders":    {"watermark_column": "order_date", "watermark_type": "date", "init_mode": "full_load"},
    "customers": {"watermark_column": "join_date",  "watermark_type": "date", "init_mode": "full_load"},
    "products":  {"watermark_column": null,         "watermark_type": "full_load"}
  },
  "aggregates_persist": true,
  "fail_safe": "backup_before_merge"
},
"storage": {
  "backend": "iceberg",
  "iceberg": {
    "catalog_type": "sql",
    "catalog_uri": "sqlite:///autobatch.db",
    "warehouse": "s3://autobatch/warehouse"
  }
}
```

#### 23.4.2 工作原理

`state/state.json` 新增 `tables.<表>.last_snapshot_id` 字段，记录上次成功处理到的 Iceberg snapshot_id。ingest 阶段调用 pyiceberg 的 `Table.incremental_append_scan()` 获取两个 snapshot 之间的 added_data_files：

代码示例：snapshot diff 核心逻辑（Python）

```python
table = catalog.load_table("warehouse.orders")
new_scan = table.incremental_append_scan(
    from_snapshot_id_exclusive=last_snapshot_id,  # 上次处理的 snapshot
    to_snapshot_id_inclusive=table.current_snapshot_id,  # 当前 snapshot
)
# new_scan 只含两个 snapshot 之间 append 的 data files，不扫全表
rows = list(new_scan.to_arrow().to_pylist())
```

只读增量 data files，不扫全表，IO 量与增量行数成正比。水位推进：全部阶段成功后，`last_snapshot_id` 更新为 `table.current_snapshot_id`，两阶段提交保证失败不推进。

#### 23.4.3 与 Phase 1 高水位对比

表：snapshot diff vs 高水位模式对照表

| 维度 | `high_watermark`（Phase 1） | `iceberg_snapshot_diff`（Phase 4） |
|---|---|---|
| 寻址机制 | 业务列水位（`max(order_date)`） | Iceberg snapshot_id |
| 捕获 append | ✅ | ✅ |
| 捕获 update | ❌（只看水位列 > 旧值） | ✅（snapshot diff 含 update 的 before/after data file） |
| 捕获 delete | ❌ | ✅（snapshot diff 含 deleted data file） |
| 水位污染风险 | 脏值（如 `bad_date`）可能污染水位 | 无业务列水位，snapshot_id 由 Iceberg 保证单调 |
| 依赖 | 零（标准库） | pyiceberg 0.12.0rc1+ + Iceberg catalog |
| 适用 | 源数据只 append、无 update/delete | 源数据有 update/delete、需精确增量 |

#### 23.4.4 从 high_watermark 切换到 iceberg_snapshot_diff

1. 确认 `storage.backend="iceberg"` 已生效（见第 23.1 节部署 catalog）
2. 把 `incremental.mode` 从 `"high_watermark"` 改为 `"iceberg_snapshot_diff"`
3. 删除 `state/state.json`（旧 `watermark_value` 字段不再用，需重建 `last_snapshot_id`）
4. 运行 `python main.py --config config/pipeline_small.json`：首次等价全量（`init_mode=full_load`），建立 `last_snapshot_id` 为当前 snapshot
5. 后续运行：snapshot diff 只处理新增/更新/删除的 data files
6. 回退：`incremental.mode` 改回 `"high_watermark"` + 删除 `state/` 重建水位

`tests/test_storage_iceberg.py::test_iceberg_snapshot_diff` 覆盖：写入 3 批数据，每次只处理本批 snapshot diff，聚合 merge 正确。

### 23.5 Spark + Iceberg 集成

`engine.backend="spark"` + `storage.backend="iceberg"` 让 Spark 原生读写 Iceberg 表，分布式 + 湖表终态。Spark 通过 `iceberg-spark-runtime` JAR 集成 Iceberg，`spark.read.table("autobatch.warehouse.orders")` 直接读 Iceberg 表（注意 Spark 表名带 catalog 前缀 `autobatch.warehouse.orders`，pyiceberg 用 `warehouse.orders`）。

#### 23.5.1 Docker ENABLE_ICEBERG 开关

Docker Spark 集群（`docker/spark-cluster/`）通过构建 ARG `ENABLE_ICEBERG` 控制是否打入 Iceberg JAR：

- `ENABLE_ICEBERG=false`（缺省）：不装 Iceberg JAR，镜像小，纯 Spark + Parquet
- `ENABLE_ICEBERG=true`：构建时打入 `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` + 对应 extensions 配置

命令示例：构建带 Iceberg 的 Spark 镜像

```bash
docker build -t autobatch-spark:iceberg \
  --build-arg ENABLE_ICEBERG=true \
  -f docker/spark-cluster/Dockerfile .
```

#### 23.5.2 Spark 4.1 降级说明

**重要**：Iceberg 官方 JAR 最高支持 Spark 4.1（`iceberg-spark-runtime-4.1_2.13-1.11.0.jar`），而当前 Docker Spark 集群用 Spark 4.2.0。启用 `ENABLE_ICEBERG=true` 时需把 Spark 降级到 4.1，否则 JAR 版本不匹配会抛 `ClassNotFoundException` / `NoSuchMethodError`。

降级方式：`docker/spark-cluster/Dockerfile` 中 `ARG SPARK_VERSION=4.1.0`（缺省 4.2.0），构建时 `--build-arg SPARK_VERSION=4.1.0`。driver 端 PySpark 也需对应降到 `pip install pyspark==4.1.0`。

若坚持用 Spark 4.2，需等 Iceberg 社区发布 `iceberg-spark-runtime-4.2` JAR（截至 2026-08 未发布），或自建 Iceberg 源码编译 4.2 分支（不推荐，维护成本高）。

#### 23.5.3 配置示例

配置示例（Spark + Iceberg 三合一）

```json
"engine": {
  "backend": "spark",
  "format": "parquet",
  "spark": {
    "master": "spark://localhost:15077",
    "app_name": "autobatch-iceberg",
    "executor_memory": "2g",
    "executor_cores": 2,
    "num_executors": 2,
    "adaptive_query_execution": true,
    "extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "catalog": {
      "autobatch": {
        "type": "rest",
        "catalog_uri": "http://localhost:8181",
        "warehouse": "s3://autobatch/warehouse"
      }
    }
  }
},
"storage": {
  "backend": "iceberg",
  "iceberg": {
    "catalog_type": "rest",
    "catalog_uri": "http://localhost:8181",
    "warehouse": "s3://autobatch/warehouse"
  }
}
```

`engine.spark.extensions` 注册 Iceberg Spark extensions（解锁 `MERGE INTO` / `UPDATE` / `DELETE` 等 Iceberg SQL 语法）；`engine.spark.catalog.autobatch` 把 Iceberg catalog 注册为 Spark catalog 名 `autobatch`，故 Spark 表名是 `autobatch.warehouse.orders`。

#### 23.5.4 Spark 原生读写 Iceberg

代码示例：Spark 原生读写 Iceberg 表（Python）

```python
# 读 Iceberg 表（Spark 表名带 catalog 前缀）
df = spark.read.table("autobatch.warehouse.orders")
df.show()

# 写 Iceberg 表（append，生成新 snapshot）
df.writeTo("autobatch.warehouse.orders").append()

# time travel（Spark AT SNAPSHOT 语法）
df_hist = spark.read.option("snapshot-id", "9123456788") \
    .table("autobatch.warehouse.orders")

# Iceberg MERGE INTO（需 extensions）
spark.sql("""
MERGE INTO autobatch.warehouse.orders t
USING updates u ON t.order_id = u.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```

`tests/test_spark_iceberg.py`（10 测试）覆盖 Spark + Iceberg 等价性：全量产物与 python 路径一致、time travel、snapshot diff、MERGE INTO 等。Windows 缺 `hadoop.dll` 或未装 Iceberg JAR 时 `skipif` 跳过。

### 23.6 Iceberg 故障排查

表：Iceberg 路径常见问题与处理

| 现象 | 原因与处理 |
|---|---|
| `ModuleNotFoundError: No module named 'pyiceberg'` | 未安装 pyiceberg。处理：`pip install pyiceberg>=0.12.0rc1`。`storage.backend="local_csv"` / `"parquet"` 路径不受影响，无需安装 |
| `ValueError: Unsupported Python version` 或 pyiceberg 安装失败 | pyiceberg 0.11.1 稳定版仅支持 Python 3.10-3.13。Python 3.14 需装 rc1+：`pip install pyiceberg>=0.12.0rc1`。确认 `python --version` 后选对应版本 |
| catalog 连接失败（`ConnectionRefusedError` / `CatalogNotFound`） | catalog 服务未启动或 uri 配置错误。处理：① SQL catalog：确认 `uri` 路径可写（`sqlite:///autobatch.db` 在项目根生成）；② REST catalog：`docker ps` 确认 iceberg-rest 容器运行，`curl http://localhost:8181` 确认可达；③ 确认 `storage.iceberg.catalog_type` 与 `uri` 匹配（sql→sqlite，rest→http） |
| `NoSuchTableError: warehouse.orders` | 表不存在。处理：① 首次写入会自动建表，确认 `storage.backend="iceberg"` 已生效；② 若手工删过 catalog 元数据，用 `tools/parquet_to_iceberg_migrate.py` 重新注册；③ 确认表名 namespace 正确（pyiceberg 用 `warehouse.orders`，Spark 用 `autobatch.warehouse.orders`） |
| `NoSuchSnapshotError` 或 snapshot id 无效 | `read_history_snapshot(table_name, snapshot_id)` 的 snapshot_id 不存在。处理：先用 `list_snapshots(table_name)` 查有效 snapshot_id 列表，确认传入的 id 在列表中。snapshot id 是 long 型，注意不要传成字符串 |
| Spark + Iceberg `ClassNotFoundException: org.apache.iceberg.spark...` | Iceberg JAR 未装入 Spark。处理：① 确认 Docker 构建时 `--build-arg ENABLE_ICEBERG=true`；② 确认 `engine.spark.extensions` 已注册 IcebergSparkSessionExtensions；③ 确认 JAR 在 `/opt/spark/jars/`（`docker exec spark-master ls /opt/spark/jars/ \| findstr iceberg`） |
| Spark + Iceberg `NoSuchMethodError` / 版本不匹配 | Iceberg JAR 与 Spark 版本不兼容。Iceberg JAR 最高支持 Spark 4.1，当前 Docker 用 Spark 4.2。处理：把 Spark 降级到 4.1（`--build-arg SPARK_VERSION=4.1.0` + `pip install pyspark==4.1.0`），详见第 24.5.2 节 |
| Spark 表名 `autobatch.warehouse.orders` 找不到但 pyiceberg `warehouse.orders` 能读 | Spark 与 pyiceberg 表名解析差异。Spark 表名格式 `<catalog>.<namespace>.<table>`，pyiceberg 是 `<namespace>.<table>`。处理：Spark 调用加 catalog 前缀 `autobatch.warehouse.orders`，pyiceberg 调用用 `warehouse.orders` |
| `incremental.mode="iceberg_snapshot_diff"` 不生效 | 检查：① `incremental.enabled` 是否为 `true`；② `storage.backend` 是否为 `"iceberg"`（snapshot diff 依赖 Iceberg 表）；③ `state/state.json` 是否还残留旧 `watermark_value`（从 `high_watermark` 切换时需删除 `state/` 重建） |
| Iceberg 路径产物与 local_csv / parquet 路径不一致 | 不应发生。`tests/test_storage_iceberg.py` 覆盖等价性。若观察到差异，检查：① pyiceberg 版本是否 0.12.0rc1+；② catalog 元数据是否被污染（删除 `autobatch.db` 重建）；③ config 是否还残留其他改动 |
| snapshot diff 漏处理 update / delete | 不应发生。`incremental_append_scan` 捕获 append，update/delete 用 `incremental_changelog_scan`（如需 CDC 语义）。当前 Phase 4 ingest 用 `incremental_append_scan`，若源数据有 update/delete 需切到 changelog scan（后续扩展） |
| `pyiceberg.catalog.load_catalog` 报 `TypeError` 参数不匹配 | pyiceberg API 在 0.11 → 0.12 有 breaking change。处理：确认用 0.12.0rc1+ 的 API（`load_catalog(name, **kwargs)`），参考 `src/storage_iceberg.py` 的调用方式 |

## 24. 路径选择指南

### 24.1 何时用 local_csv / parquet(本地) / parquet(S3)

表：存储路径选择对照表

| 场景 | 推荐路径 | 理由 |
|---|---|---|
| 单机演示 / 教学 / CI | `local_csv` | 零依赖，产物可直接用文本编辑器 / Excel 查看 |
| 单机生产 + 节省磁盘 | `parquet`（本地） | 列式压缩 3-6 倍，节省存储 + IO 带宽，无需运维 MinIO |
| 多机协同 / 远端访问 | `parquet`（S3/MinIO） | 共享存储，任意节点可访问，解锁 Phase 2b 多机模式 |
| 已有 S3/MinIO 基础设施 | `parquet`（S3/MinIO） | 复用现有对象存储，无需额外部署 |
| 数据量极小（< 1000 行） | `local_csv` | Parquet 元数据开销可能超过压缩收益 |

### 24.2 何时用 polars+parquet / spark+parquet

表：engine × storage 组合选择对照表

| 场景 | 推荐组合 | 理由 |
|---|---|---|
| 单机加速 + 远端存储 | `engine.backend="polars"` + `storage.backend="parquet"`（S3） | Polars 原生读 S3 Parquet 零拷贝 + 谓词下推，单机列式加速 + 对象存储压缩，无需 Spark |
| 单机加速 + 本地压缩 | `engine.backend="polars"` + `storage.backend="parquet"`（本地） | Polars 原生读写本地 Parquet，列式 + 压缩 + 谓词下推，零运维 |
| 分布式 + 共享存储 | `engine.backend="spark"` + `storage.backend="parquet"`（S3） | Spark + S3A connector + Docker Compose Standalone 集群，多 executor 分布式 + MinIO 共享存储，突破单机内存上限（多机模式已实现，见第 21.3 节） |
| 分布式 + 本地（仅本地模式） | `engine.backend="spark"` + `storage.backend="local_csv"` | Spark 本地模式 `master="local[*]"`，单机模拟分布式，无需 MinIO |
| 零依赖 / CI 轻量 | `engine.backend="python"` + `storage.backend="local_csv"` | 零第三方依赖，一键复现 |

### 24.3 切换步骤

**切换到本地 Parquet**：

1. 安装依赖：`pip install pyarrow`（一次性，pyarrow 14+）
2. 修改配置：`config/pipeline.json` 或 `pipeline_small.json` 的 `storage.backend` 改为 `"parquet"`，不配 `bucket` / `endpoint`（或清空 `endpoint`）
3. 运行：`python main.py --config config/pipeline_small.json`
4. 验证产物：与 `storage.backend="local_csv"` 路径产物内容完全一致（行数、聚合值、DQ Score、manifest lineage、metrics）。`tests/test_storage_parquet.py::test_local_parquet_equivalence` 自动覆盖此等价性
5. 回退：`storage.backend` 改回 `"local_csv"` 即恢复 CSV 路径

**切换到 S3/MinIO Parquet**：

1. 安装依赖：`pip install pyarrow minio`（一次性，pyarrow 14+ + minio 7+）
2. 启动 MinIO + 创建 bucket（见第 22.2 节）
3. 修改配置：`config/pipeline.json` 或 `pipeline_small.json` 的 `storage.backend` 改为 `"parquet"`，配 `bucket="autobatch"` + `endpoint="localhost:9000"` + 凭证
4. 运行：`python main.py --config config/pipeline_small.json`
5. 验证产物：与 `storage.backend="local_csv"` 路径产物内容完全一致。`tests/test_storage_parquet.py::test_s3_parquet_equivalence` 自动覆盖此等价性（MinIO 不可用时 `skipif` 跳过）
6. 回退：`storage.backend` 改回 `"local_csv"` 或 `"parquet"`（本地）即恢复路径
