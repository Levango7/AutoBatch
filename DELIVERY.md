# AutoBatch 交付清单

**最新提交**: 见 `git log`（P0/P1/P2 修复 `74660f1`，交付清单 `185b228`，P3 收尾见后续提交）
**时间**: 2026-08-28
**状态**: ✅ 可交付（P0/P1/P2/P3 全部闭环）

---

## 一、交付判定

| 维度 | 结论 | 证据 |
|------|------|------|
| 单元测试 | ✅ 全绿 | 415 passed / 22 skipped / 0 failed |
| Lint | ✅ 全绿 | `ruff check src tests tools scripts dashboard` pass |
| Format | ✅ 全绿 | `ruff format --check` 52 files already formatted |
| Type check | ✅ 全绿 | `mypy src` pass (23 files) |
| 分支覆盖 | ✅ 74.35% | `[tool.coverage.run] branch=true`，门禁 60% 通过 |
| 集成测试 | ⚠️ 部分 skip | Windows 缺 hadoop native DLL（环境限制，非代码问题） |
| Spark 集群真值 | ✅ B1/B2/B3 通过 | `B1_BROADCAST_JOIN_OK`、`B2_B3_SPARK_VERIFY_ALL_OK` |
| Iceberg 假单测 | ✅ 5 passed | `test_spark_overwrite_*` 全绿 |

**综合判定**: 可交付。P0/P1/P2/P3 全部闭环，核心修复经真实 Spark 集群验证。跳过测试为环境限制（Windows 无 hadoop.dll），在 Linux/macOS 或装齐 Hadoop native lib 的 Windows 上可全跑。

---

## 二、P0/P1/P2 修复清单（提交 74660f1）

### P0（交付阻断项）

| 编号 | 问题 | 修复 | 文件 |
|------|------|------|------|
| A1 | Python 版本声明三处 "3.9+" 与 pyproject `>=3.10` 矛盾 | 统一改为 "3.10+" | readme.md；docs/runbook.md；docs/evolution.md |
| D1 | 缺少 LICENSE 文件 | 新增 Apache-2.0 | LICENSE（新建） |
| C1 | Dockerfile `python:3.13.1-slim` 与 CI 验证版本不一致 | 改为 `python:3.12-slim` | Dockerfile；ci.yml；requirements.txt |

### P1（生产宣称阻断项）

| 编号 | 问题 | 修复 | 文件 | 验证 |
|------|------|------|------|------|
| B1 | Spark referential 检查 `isin(收集孤儿键)` OOM 风险 | 新增 `_spark_referential_markers` 广播 join，内存 O(\|ref\|) | src/quality.py | Spark 集群真值 `B1_BROADCAST_JOIN_OK` |
| B5 | Spark+Iceberg overwrite 用 `createOrReplace` 销毁历史 snapshot | 表已存在时用 `INSERT OVERWRITE` SQL | src/iceberg.py | 假单测 5 passed |
| B8 | Spark 目录型输出导致看板缺图 | `_load_daily_sales` 兼容目录产物 | dashboard/build_data.py | — |
| C2 | 无最小 CD 流程 | 新增 release.yml | .github/workflows/release.yml | — |

### P2

| 编号 | 问题 | 修复 | 文件 |
|------|------|------|------|
| A2 | 死配置键无代码引用 | 删除死键 + `mode` 枚举校验 | config/pipeline*.json；src/config_schema.py |
| A3 | 看板需手动刷新 | run.bat/run.sh 流水线结束后自动执行 build_data.py | run.bat；run.sh |
| A4 | Docker 限制未说明 | README + Dockerfile 注明容器内无 JDK | readme.md；Dockerfile |
| A5 | `incremental.mode` 拼错静默回落 | config_schema 新增 validator | src/config_schema.py |
| B6 | state.json 损坏无明确报错 | StateStore.load() 抛 RuntimeError 带恢复指引 | src/state.py |
| B9 | generator 空表兜底列序错位 | 修正 orders/products fallback 字段 | src/generator.py |
| C6 | 未知 engine.backend 静默兜底 | pipeline.py 添加 warning | src/pipeline.py |
| D3 | .zcode/ 被 git 跟踪 | 加入 .gitignore 并移出版本控制 | .gitignore |

---

## 三、P3 收尾修复清单（本轮）

> 本轮对 DELIVERY.md 原列的 8 个 P3 项逐一**取证复核**：4 项为真实缺口已修复，
> 3 项经证据核实为**此前文档误判**（实际已实现/已有/属有意设计），1 项属有意设计保留。

### 真实缺口 → 已修复

| 编号 | 问题（证据） | 修复 | 验证 |
|------|------|------|------|
| B2 | Spark outlier 缺 `n<100` 样本门槛（python quality.py:1029 / polars:492 均有），且 `method=zscore` 被静默当 iqr 跑；裸 `cast` 在 ANSI 下对非法值抛异常 | `_spark_outlier` 补样本门槛 + zscore 分支（mean/stddev_pop 对齐 pstdev/std(ddof=0)）+ `try_cast` 容错 | Spark 集群真值 `B2a/B2b/B2c OK` |
| B3 | Spark clean 的 qty/price 用裸 `cast`（clean.py:287-288），null/非法时 total_amount 为 null，与 python `as_int/as_float or 0` / polars `fill_null(0)` 的 0.0 语义分歧 | 改 `try_cast + coalesce(0.0)`，与 discount 列同口径 | Spark 集群真值 `B3_TRY_CAST_COALESCE_OK` |
| B10 | `csv_write`（helpers.py:148）直接 `open(path,"w")` 流式写，非原子——崩溃留半截 CSV、并发双写互撕；`json_save` tmp 名固定易碰撞 | csv_write/json_save 均改「同目录唯一 tmp（pid+uuid）+ os.replace」原子写，失败清理 tmp | 新增 3 个原子性单测 pass |
| — | 分支覆盖 0%（coverage.xml branch-rate=0） | pyproject 新增 `[tool.coverage.run] branch=true` + `[tool.coverage.report] fail_under=60` | 本地实测分支口径 74.35% > 60% 门禁 |
| — | CI 重复执行：quality.yml「Collect pytest log」步骤经 quality_collect.py 再跑一遍全量 suite，仅归档日志，且 GHA runner 平台故障恒失败 | 移除该步骤及 Upload pytest log 步骤（原注释已自认可移除）；quality_collect.py 脚本与其单测保留 | YAML 语法校验 OK |

### 此前文档误判 → 已纠正（无需代码改动）

| 编号 | DELIVERY.md 原说法 | 证据核实结论 |
|------|------|------|
| B7 | 「Iceberg SnapshotDiff 模式未实现」 | **已实现**：`iceberg_snapshot_diff`（iceberg.py:304）+ `iceberg_snapshot_diff_spark`（iceberg.py:357），ingest.py:286/664 已接入 `incremental.mode=iceberg_snapshot_diff`，tests/test_incremental.py 有覆盖 |
| B11 | 「OpenLineage async POST 无 timeout」 | **已有 timeout**：openlineage.py:176 `urlopen(req, timeout=3)`，且 `_post` 全程被 `_dispatch` 的 try/except 吞异常，不影响主流程 |
| B12 | 「硬编码 F:\ 路径」 | **属有意设计**：helpers.py:494-513 是 `detect_spark_paths` 的兜底候选，环境变量优先、`os.path.isdir` 守卫，非功能缺陷；本机 `F:\spark_home` 真实存在且被探测使用，删除会破坏本地 Spark 探测，故保留 |

---

## 四、环境限制说明

以下测试在当前 Windows 环境**必然 skip**，非代码问题：

| 测试模块 | Skip 原因 | 可在什么环境跑通 |
|----------|-----------|------------------|
| test_engine_spark.py | 缺 hadoop.native.dll（Windows） | Linux / macOS / 装齐 Hadoop native 的 Windows |
| test_spark_iceberg.py | 同上 + 需 iceberg-spark-runtime JAR | 同上 + 配置 Iceberg JAR |
| test_engine_spark_cluster.py | 同上 + 需 docker compose 集群 | Linux Docker 环境 |

**已验证**:
- Spark 集群连通性：`spark://spark-master:7077` ✅
- B1 广播 join 真值：`B1_BROADCAST_JOIN_OK` ✅
- B2/B3 Spark 逻辑真值：`B2_B3_SPARK_VERIFY_ALL_OK` ✅
- B5 INSERT OVERWRITE 假单测：5 passed ✅
- 全量回归：415 passed / 22 skipped / 0 failed ✅
- 分支覆盖：74.35%（门禁 60%）✅

---

## 五、交付后操作建议

1. **Linux 环境补跑**: 在 Linux 机器或 CI runner 上跑全量测试（含 Spark 集成测试）
2. **Hadoop native 补齐**: Windows 开发机安装 hadoop.dll 后可跑全部 Spark 测试
3. **Codecov 观察**: 开启分支覆盖后 coverage.xml 含 branch 数据，首次上传 Codecov 基线会变化属正常

---

**交付结论**: ✅ **可以交付**。核心功能完整，测试全绿，P0/P1/P2/P3 全部闭环，关键修复经真实 Spark 集群验证。
