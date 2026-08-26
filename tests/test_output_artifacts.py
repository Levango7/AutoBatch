"""output 阶段产物/血缘登记与相关快速修复的回归测试.

覆盖项（对应深度代码审查修复编号）：
- 【3/M5】validate 阶段血缘声明的 relpath 归一为 "/" 分隔：Windows 原生
  反斜杠场景 + monkeypatch 强制反斜杠场景（POSIX 上也回归）。
- 【4/M14】spark local_csv 风格目录产物（目录名以 .csv 结尾、内含
  part-00000-* 分区文件）被登记进 manifest，行数聚合正确、digest 带
  "dir:" 前缀，且血缘边仍匹配。
- 【4/minor】跨盘符 os.path.relpath ValueError 回退：artifact key 与
  _register_edges 前缀使用同一回退逻辑，回退后血缘边仍匹配（仅 Windows
  存在跨盘 ValueError，用例带 skipif）。
- 【1/C6】_table_write_iceberg spark 分支：表存在时 append 失败绝不回退
  createOrReplace（用 fake SparkSession/DataFrame 纯逻辑单测，不依赖
  真实 Spark）；表不存在时保留建表兜底；overwrite 模式行为不变。
- 【2/minor】catalog_uri 相对 sqlite URI 归一为相对 ROOT 的绝对路径。
- 【5/minor】StageLog close 幂等 + close 后 emit 安全 no-op。

目录产物用例不依赖真实 Spark session：直接构造 spark 写出风格的目录并
调用 output._register_artifacts（纯 Python 路径）。
"""

from __future__ import annotations

import os

import pytest

from src.helpers import ROOT, PipelineContext, StageLog
from src.lineage import Manifest
from src.stages import output, validate


# ----------------------------------------------------------------------
# 公共小工具
# ----------------------------------------------------------------------
def _make_ctx(run_dir: str, small_config: dict, batch_id: str = "B-UNIT") -> PipelineContext:
    manifest = Manifest(batch_id, "digest-unit", run_dir)
    return PipelineContext(
        config=small_config, run_dir=run_dir, batch_id=batch_id, manifest=manifest
    )


def _find_key(artifacts: dict, suffix: str) -> str:
    """在 artifact key（可能是 ../ 相对路径或跨盘回退的绝对路径）中按后缀查找."""
    hits = [k for k in artifacts if k.endswith(suffix)]
    assert hits, f"应存在以 {suffix} 结尾的 artifact key，实际 keys={list(artifacts)}"
    return hits[0]


# ----------------------------------------------------------------------
# 【3/M5】validate 血缘边的 "/" 归一
# ----------------------------------------------------------------------
def test_validate_lineage_edges_survive_registration(ingested_ctx):
    """Windows 原生反斜杠 relpath 场景：validate 声明 → 归一 → 边保留.

    复现 M5 缺陷路径：validate 用 os.path.relpath(path, run_dir) 计算上游
    （Windows 下带反斜杠），output._register_edges 用 "/" 规范化的
    manifest artifact key 匹配——修复前全部 validate 边被静默丢弃。
    """
    ctx = ingested_ctx
    with StageLog(os.path.join(ctx.run_dir, "logs", "validate.jsonl")) as log:
        summary = validate.run(ctx, log)

    # validate 声明本身必须已归一（不含分隔符以外的反斜杠）
    assert "02_valid/valid_orders.csv" in summary["lineage"]
    for target, ups in summary["lineage"].items():
        assert "\\" not in target
        for u in ups:
            assert "\\" not in u, f"血缘声明未归一: {target} <- {u}"
        ctx.lineage_decls.setdefault(target, list(ups))

    # 模拟 pipeline 编排：output 阶段登记产物 + 拼接血缘边
    output._register_artifacts(ctx)
    edge_count = output._register_edges(ctx)
    assert edge_count >= 1, "至少应注册 validate 声明的血缘边"

    target = _find_key(ctx.manifest.lineage, "02_valid/valid_orders.csv")
    assert "\\" not in target
    upstreams = ctx.manifest.lineage[target]
    assert any(u.endswith("01_raw/orders.csv") for u in upstreams), (
        f"validate 边 02_valid/valid_orders.csv 的上游应含 01_raw/orders.csv，实际 {upstreams}"
    )


def test_validate_lineage_survives_forced_backslash_relpath(ingested_ctx, monkeypatch):
    """强制 relpath 返回反斜杠（POSIX 上也回归 M5），声明值仍被归一."""
    ctx = ingested_ctx
    real_relpath = os.path.relpath

    def backslash_relpath(path, start=os.curdir):
        # 模拟 Windows 行为：分隔符全部为反斜杠
        return real_relpath(path, start).replace("/", "\\")

    with monkeypatch.context() as m:
        m.setattr(os.path, "relpath", backslash_relpath)
        with StageLog(os.path.join(ctx.run_dir, "logs", "validate_bs.jsonl")) as log:
            summary = validate.run(ctx, log)

    ups = summary["lineage"]["02_valid/valid_orders.csv"]
    assert all("\\" not in u for u in ups), f"归一失效: {ups}"

    for target, ups_list in summary["lineage"].items():
        ctx.lineage_decls.setdefault(target, list(ups_list))
    output._register_artifacts(ctx)
    assert output._register_edges(ctx) >= 1
    assert _find_key(ctx.manifest.lineage, "02_valid/valid_orders.csv")


# ----------------------------------------------------------------------
# 【4/M14】目录型产物登记（spark local_csv 风格，不依赖真实 Spark）
# ----------------------------------------------------------------------
def test_directory_artifact_registered(tmp_path, small_config):
    """spark 风格目录产物（.csv 目录 + part 文件）应登记，行数聚合正确."""
    run_dir = str(tmp_path / "B-DIR")
    part_dir = os.path.join(run_dir, "03_clean", "orders_clean.csv")
    os.makedirs(part_dir)
    # Spark 以 header=True 写 CSV：每个分区文件各自带一行 header
    with open(os.path.join(part_dir, "part-00000.csv"), "w", encoding="utf-8") as f:
        f.write("order_id,quantity\nO-1,5\nO-2,3\n")
    with open(os.path.join(part_dir, "part-00001.csv"), "w", encoding="utf-8") as f:
        f.write("order_id,quantity\nO-3,7\n")
    # Spark 目录内的元数据文件不应计入
    with open(os.path.join(part_dir, "_SUCCESS"), "w", encoding="utf-8") as f:
        f.write("")

    ctx = _make_ctx(run_dir, small_config, "B-DIR")
    output._register_artifacts(ctx)

    key = _find_key(ctx.manifest.artifacts, "03_clean/orders_clean.csv")
    art = ctx.manifest.artifacts[key]
    assert art["kind"] == "clean"
    # 两个分片各 2/1 条数据行（csv_lines 逐分片去 header 后求和）
    assert art["rows"] == 3
    assert art["sha256"].startswith("dir:")
    assert art["batch_id"] == "B-DIR"


def test_directory_artifact_digest_stable(tmp_path, small_config):
    """目录 digest 对排序文件清单确定：同内容两次登记结果一致."""
    run_dir = str(tmp_path / "B-DIG")
    part_dir = os.path.join(run_dir, "05_output", "orders_final.csv")
    os.makedirs(part_dir)
    for name, body in (("part-00000.csv", "a,b\n1,2\n"), ("part-00001.csv", "a,b\n3,4\n")):
        with open(os.path.join(part_dir, name), "w", encoding="utf-8") as f:
            f.write(body)

    ctx1 = _make_ctx(run_dir, small_config, "B-DIG")
    output._register_artifacts(ctx1)
    sha1 = ctx1.manifest.artifacts[_find_key(ctx1.manifest.artifacts, "orders_final.csv")]["sha256"]

    ctx2 = _make_ctx(run_dir, small_config, "B-DIG")
    output._register_artifacts(ctx2)
    sha2 = ctx2.manifest.artifacts[_find_key(ctx2.manifest.artifacts, "orders_final.csv")]["sha256"]
    assert sha1 == sha2 and sha1.startswith("dir:")


def test_directory_artifact_keeps_lineage_edge(tmp_path, small_config):
    """目录产物登记后，指向它的血缘边仍被 _register_edges 匹配."""
    run_dir = str(tmp_path / "B-EDGE")
    raw_dir = os.path.join(run_dir, "01_raw")
    os.makedirs(raw_dir)
    with open(os.path.join(raw_dir, "orders.csv"), "w", encoding="utf-8") as f:
        f.write("order_id\nO-1\n")
    part_dir = os.path.join(run_dir, "03_clean", "orders_clean.csv")
    os.makedirs(part_dir)
    with open(os.path.join(part_dir, "part-00000.csv"), "w", encoding="utf-8") as f:
        f.write("order_id\nO-1\n")

    ctx = _make_ctx(run_dir, small_config, "B-EDGE")
    ctx.lineage_decls["03_clean/orders_clean.csv"] = ["01_raw/orders.csv"]
    output._register_artifacts(ctx)
    edge_count = output._register_edges(ctx)
    assert edge_count == 1
    target = _find_key(ctx.manifest.lineage, "03_clean/orders_clean.csv")
    ups = ctx.manifest.lineage[target]
    assert len(ups) == 1 and ups[0].endswith("01_raw/orders.csv")


# ----------------------------------------------------------------------
# 【4/minor】跨盘符 relpath ValueError 回退
# ----------------------------------------------------------------------
def test_rel_to_root_same_drive_normalized():
    """ROOT 之下路径：相对 key + "/" 归一（纯字符串计算，不落盘）."""
    sub = os.path.join(ROOT, "run", "B-UNIT", "03_clean", "x.csv")
    assert output._rel_to_root(sub) == "run/B-UNIT/03_clean/x.csv"


@pytest.mark.skipif(os.name != "nt", reason="跨盘 ValueError 仅 Windows 存在")
def test_rel_to_root_cross_drive_fallback(tmp_path, monkeypatch):
    """跨盘时回退为 "/" 归一的绝对路径，不抛 ValueError."""
    drive = os.path.splitdrive(str(tmp_path))[0]
    other = "Z" if drive.lower() != "z:" else "Y"
    monkeypatch.setattr(output, "ROOT", other + ":\\fake_root")
    p = os.path.join(str(tmp_path), "a", "b.csv")
    assert output._rel_to_root(p) == os.path.abspath(p).replace("\\", "/")


@pytest.mark.skipif(os.name != "nt", reason="跨盘 ValueError 仅 Windows 存在")
def test_cross_drive_fallback_keeps_edge_match(tmp_path, small_config, monkeypatch):
    """跨盘回退后 artifact key 与边前缀仍一致（同一回退逻辑）."""
    drive = os.path.splitdrive(str(tmp_path))[0]
    other = "Z" if drive.lower() != "z:" else "Y"
    monkeypatch.setattr(output, "ROOT", other + ":\\fake_root")

    run_dir = str(tmp_path / "B-XD")
    for sub in ("01_raw", "02_valid"):
        os.makedirs(os.path.join(run_dir, sub))
    with open(os.path.join(run_dir, "01_raw", "orders.csv"), "w", encoding="utf-8") as f:
        f.write("order_id\nO-1\n")
    with open(os.path.join(run_dir, "02_valid", "valid_orders.csv"), "w", encoding="utf-8") as f:
        f.write("order_id\nO-1\n")

    ctx = _make_ctx(run_dir, small_config, "B-XD")
    ctx.lineage_decls["02_valid/valid_orders.csv"] = ["01_raw/orders.csv"]
    output._register_artifacts(ctx)
    assert len(ctx.manifest.artifacts) == 2
    # 回退后 key 为绝对路径（"/" 归一）
    assert all(":" in k for k in ctx.manifest.artifacts)
    assert output._register_edges(ctx) == 1
    target = _find_key(ctx.manifest.lineage, "02_valid/valid_orders.csv")
    assert ctx.manifest.lineage[target][0].endswith("01_raw/orders.csv")


# ----------------------------------------------------------------------
# 【1/C6】_table_write_iceberg spark 分支 append 安全化（纯逻辑单测）
# ----------------------------------------------------------------------
class _FakeTableWriter:
    """记录 append / createOrReplace 调用次数；可注入 append 瞬态失败."""

    def __init__(self, fail_append: bool = False):
        self.append_calls = 0
        self.create_or_replace_calls = 0
        self.fail_append = fail_append

    def append(self) -> None:
        self.append_calls += 1
        if self.fail_append:
            raise RuntimeError("S3 transient timeout")

    def createOrReplace(self) -> None:
        self.create_or_replace_calls += 1


class _FakeCatalog:
    def __init__(self, exists: bool):
        self.exists = exists

    def tableExists(self, name: str) -> bool:  # noqa: N802 - Spark API 原名
        return self.exists


class _FakeSpark:
    def __init__(self, exists: bool, fail_append: bool = False):
        self.catalog = _FakeCatalog(exists)
        self.writer = _FakeTableWriter(fail_append)


class _FakeDF:
    """最小 SparkDataFrame 替身：仅需 count() 与 writeTo()."""

    def __init__(self, writer: _FakeTableWriter, rows: int = 42):
        self._writer = writer
        self._rows = rows

    def count(self) -> int:
        return self._rows

    def writeTo(self, name: str):  # noqa: N802 - Spark API 原名
        return self._writer


_ICE_CFG: dict = {"storage": {"iceberg": {"catalog_name": "autobatch"}}}


def test_spark_append_to_existing_table_succeeds():
    from src.iceberg import _table_write_iceberg

    spark = _FakeSpark(exists=True)
    n = _table_write_iceberg("ns.orders", _FakeDF(spark.writer), _ICE_CFG, "spark", spark=spark)
    assert n == 42
    assert spark.writer.append_calls == 1
    assert spark.writer.create_or_replace_calls == 0


def test_spark_append_failure_never_overwrites_existing_table():
    """C6 回归：已存在表的 append 瞬态失败必须原样报错，绝不 createOrReplace."""
    from src.iceberg import _table_write_iceberg

    spark = _FakeSpark(exists=True, fail_append=True)
    with pytest.raises(RuntimeError, match="failed to append"):
        _table_write_iceberg("ns.orders", _FakeDF(spark.writer), _ICE_CFG, "spark", spark=spark)
    assert spark.writer.append_calls == 1
    # 关键断言：任何情况下都不得用 createOrReplace 覆盖已存在表
    assert spark.writer.create_or_replace_calls == 0


def test_spark_missing_table_falls_back_to_create():
    """合法兜底路径保留：表不存在时建表（createOrReplace）."""
    from src.iceberg import _table_write_iceberg

    spark = _FakeSpark(exists=False)
    n = _table_write_iceberg("ns.orders", _FakeDF(spark.writer), _ICE_CFG, "spark", spark=spark)
    assert n == 42
    assert spark.writer.append_calls == 0
    assert spark.writer.create_or_replace_calls == 1


def test_spark_overwrite_mode_uses_create_or_replace():
    """overwrite 模式行为不变：无条件 createOrReplace."""
    from src.iceberg import _table_write_iceberg

    spark = _FakeSpark(exists=True)
    n = _table_write_iceberg(
        "ns.orders", _FakeDF(spark.writer), _ICE_CFG, "spark", spark=spark, mode="overwrite"
    )
    assert n == 42
    assert spark.writer.create_or_replace_calls == 1
    assert spark.writer.append_calls == 0


# ----------------------------------------------------------------------
# 【2/minor】catalog_uri 相对路径归一到 ROOT
# ----------------------------------------------------------------------
def test_catalog_uri_relative_resolved_to_root():
    from src.iceberg import _normalize_catalog_uri

    expected = "sqlite:///" + os.path.join(ROOT, "state", "iceberg_catalog.db").replace(os.sep, "/")
    assert _normalize_catalog_uri("sqlite:///state/iceberg_catalog.db") == expected


def test_catalog_uri_absolute_and_other_schemes_untouched():
    from src.iceberg import _normalize_catalog_uri

    # 显式绝对路径（测试常用 tmp 路径形态）不受影响
    abs_uri = "sqlite:///" + os.path.join(ROOT, "state", "custom.db").replace(os.sep, "/")
    assert _normalize_catalog_uri(abs_uri) == abs_uri
    # 其他 scheme 一律原样返回
    assert _normalize_catalog_uri("http://localhost:8181") == "http://localhost:8181"
    assert _normalize_catalog_uri("s3://bucket/catalog.db") == "s3://bucket/catalog.db"
    assert _normalize_catalog_uri("") == ""


# ----------------------------------------------------------------------
# 【5/minor】StageLog close 幂等 + close 后 emit 安全
# ----------------------------------------------------------------------
def test_stagelog_close_idempotent_and_emit_after_close_safe(tmp_path):
    p = str(tmp_path / "logs" / "guarded.jsonl")
    log = StageLog(p)
    log.info("first")
    log.close()
    log.close()  # 双重 close 幂等，不抛异常
    log.info("dropped")  # close 后 emit 安全 no-op（原行为抛 ValueError）
    log.warn("dropped-too")
    with open(p, encoding="utf-8") as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip()]
    assert len(lines) == 1, "close 后的 emit 不应再写入"
    assert '"first"' in lines[0]
