"""ingest 阶段边缘用例测试：短行 None 防护 / 水位归一 / polars+parquet delta.

覆盖任务 #75 的三项修复：

1. M1  — 短行缺失字段必须是 None 而非字面量 "None"（_rows_to_str_rows）；
2. C7  — polars 增量 delta 必须经统一 table_write 路由：storage.backend="parquet"
         时产出 ``dst + ".parquet"`` 且下游 ``table_read`` 可读、内容是新 delta；
3. 水位格式跨引擎归一 ISO-8601（_normalize_watermark）与数值型水位列的
   一次性启发式告警（_maybe_warn_numeric_watermark）。

自包含设计：不依赖 conftest 的业务 fixture。临时目录创建在与 ROOT 同盘的
驱动器上——ingest 构造 entry 时使用 ``os.path.relpath(dst, ROOT)``，Windows
跨盘会抛 ValueError（与 conftest 顶部注释同一约束）。
"""

from __future__ import annotations

import csv
import logging
import os
import shutil
import tempfile
from datetime import date, datetime
from typing import Any

import pytest

from src.helpers import ROOT, PipelineContext, abs_path, csv_write, json_load, table_read
from src.lineage import Manifest
from src.pipeline import config_digest
from src.stages import ingest


class _FakeLog:
    """满足 ingest 阶段调用面的最小日志槽（仅 info/warning）."""

    def info(self, msg: str, **kwargs: Any) -> None:
        pass

    def warning(self, msg: str, **kwargs: Any) -> None:
        pass


@pytest.fixture()
def work_dir():
    """与 ROOT 同盘的临时目录（避免 os.path.relpath 跨盘 ValueError）."""
    if os.name == "nt":
        base = os.path.splitdrive(ROOT)[0] + os.sep
    else:
        base = os.path.dirname(ROOT)
    d = tempfile.mkdtemp(prefix="autobatch_ingest_edge_", dir=base)
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _make_ctx(
    cfg: dict[str, Any], run_dir: str, batch_id: str, state: dict[str, Any]
) -> PipelineContext:
    """构造支撑 _ingest_incremental 的最小 PipelineContext."""
    os.makedirs(run_dir, exist_ok=True)
    manifest = Manifest(batch_id, config_digest(cfg), run_dir)
    return PipelineContext(
        config=cfg,
        run_dir=run_dir,
        batch_id=batch_id,
        manifest=manifest,
        state=state,
        incremental_enabled=True,
        engine_backend=cfg.get("engine", {}).get("backend", "python"),
    )


def _load_polars_parquet_cfg(work_dir: str) -> dict[str, Any]:
    """polars 引擎 + 本地 parquet storage 的配置（参考 conftest.parquet_env 构造）."""
    cfg = json_load(abs_path("config/pipeline_small.json"))
    cfg["engine"]["backend"] = "polars"
    cfg["storage"]["backend"] = "parquet"
    # 清空 endpoint/bucket → _is_s3_target 返回 False → 走本地 parquet 文件
    cfg["storage"]["endpoint"] = ""
    cfg["storage"]["bucket"] = ""
    cfg["storage"]["warehouse"] = os.path.join(work_dir, "warehouse")
    # 水位列改用 created_ts（timestamp 列），验证跨引擎 datetime 归一
    cfg["incremental"]["tables"]["orders"]["watermark_column"] = "created_ts"
    return cfg


# ----------------------------------------------------------------------
# 【1】M1：短行缺失字段 → None（而非字面量 "None"）
# ----------------------------------------------------------------------
def test_rows_to_str_rows_short_row_missing_field_is_none(tmp_path):
    """M1：csv.DictReader 对短行缺失字段填 restval=None，str_rows 结果必须是 None."""
    src = tmp_path / "short.csv"
    # header 3 列，第一行只有 2 个字段
    src.write_text("a,b,c\n1,0\n4,5,6\n", encoding="utf-8", newline="")
    with open(src, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames or [])
        rows = list(reader)
    # 前置确认：DictReader 确实把短行缺失字段填为 None（键存在值为 None）
    assert fields == ["a", "b", "c"]
    assert rows[0]["c"] is None

    out = ingest._rows_to_str_rows(rows, fields)
    # 缺失字段是 None 而非字面量 "None"；"0" 保留
    assert out[0] == {"a": "1", "b": "0", "c": None}
    assert out[1] == {"a": "4", "b": "5", "c": "6"}


def test_rows_to_str_rows_preserves_zero_and_maps_empty_to_none():
    """M1 口径回归：falsy 值 "0" 保留；空串 / None → None；真实字符串 "None" 原样保留."""
    rows = [{"f1": "0", "f2": "", "f3": None, "f4": "None"}]
    out = ingest._rows_to_str_rows(rows, ["f1", "f2", "f3", "f4"])
    assert out[0]["f1"] == "0"  # falsy 字符串必须保留
    assert out[0]["f2"] is None  # 空串 → None（与 spark.read.csv inferSchema 对齐）
    assert out[0]["f3"] is None  # 短行 None 不得变成字面量 "None"
    assert out[0]["f4"] == "None"  # 源数据真实 "None" 字符串原样保留


# ----------------------------------------------------------------------
# 【3】水位格式跨引擎归一：_normalize_watermark
# ----------------------------------------------------------------------
def test_normalize_watermark_date_to_iso():
    assert ingest._normalize_watermark(date(2026, 8, 15)) == "2026-08-15"


def test_normalize_watermark_datetime_to_iso_t_separator():
    # 旧行为 str(datetime) = "2026-08-15 10:30:45"（空格分隔）；归一后必须是 "T"
    assert ingest._normalize_watermark(datetime(2026, 8, 15, 10, 30, 45)) == "2026-08-15T10:30:45"


def test_normalize_watermark_string_passthrough_and_none():
    # python 路径的源 CSV 字符串原样返回（本身就是 ISO-8601 口径）
    assert ingest._normalize_watermark("2026-08-15T10:00:00") == "2026-08-15T10:00:00"
    assert ingest._normalize_watermark("2026-08-15") == "2026-08-15"
    assert ingest._normalize_watermark(None) is None


# ----------------------------------------------------------------------
# 【3-附】数值型水位列一次性启发式 warning
# ----------------------------------------------------------------------
@pytest.fixture()
def numeric_wm_cleanup():
    """测试前后清理模块级告警去重集合，避免跨用例串扰."""
    yield
    ingest._NUMERIC_WM_WARNED.discard("orders_num")
    ingest._NUMERIC_WM_WARNED.discard("orders_seq")


def test_numeric_watermark_warns_once(numeric_wm_cleanup, caplog):
    """纯数值水位告警一次；同表第二次调用不再重复告警（一次性语义）."""
    ingest._NUMERIC_WM_WARNED.discard("orders_num")
    with caplog.at_level(logging.WARNING, logger="src.stages.ingest"):
        ingest._maybe_warn_numeric_watermark("orders_num", "12345")
        ingest._maybe_warn_numeric_watermark("orders_num", "67890")
    assert len(caplog.records) == 1
    assert "numeric" in caplog.records[0].message


def test_time_watermark_does_not_warn(numeric_wm_cleanup, caplog):
    """含 '-' / ':' 的日期时间水位不得触发告警."""
    with caplog.at_level(logging.WARNING, logger="src.stages.ingest"):
        ingest._maybe_warn_numeric_watermark("orders_iso_ts", "2026-08-15T10:00:00")
        ingest._maybe_warn_numeric_watermark("orders_date", "2026-08-15")
    assert caplog.records == []


def test_numeric_watermark_column_warns_on_first_ingest(work_dir, numeric_wm_cleanup, caplog):
    """端到端：数值 ID 列做水位时，首次 ingest 建立水位应触发一次性 warning."""
    ingest._NUMERIC_WM_WARNED.discard("orders_seq")
    cfg = json_load(abs_path("config/pipeline_small.json"))
    # python 引擎 + local_csv（缺省），不需要 polars/pyarrow
    data_dir = os.path.join(work_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    src_csv = os.path.join(data_dir, "orders.csv")
    fields = ["order_id", "seq", "created_ts"]
    rows = [
        {"order_id": "ORD-1", "seq": "10", "created_ts": "2026-08-01T10:00:00"},
        {"order_id": "ORD-2", "seq": "9", "created_ts": "2026-08-01T11:00:00"},
    ]
    csv_write(src_csv, fields, rows)
    table_cfg = {"watermark_column": "seq", "watermark_type": "date"}
    run_dir = os.path.join(work_dir, "run", "B3")
    ctx = _make_ctx(cfg, run_dir, "B-ingest-edge-B3", state={})
    with caplog.at_level(logging.WARNING, logger="src.stages.ingest"):
        ingest._ingest_incremental(
            ctx,
            "orders_seq",
            "data/raw/orders.csv",
            src_csv,
            os.path.join(run_dir, "01_raw"),
            table_cfg,
            _FakeLog(),
        )
    warned = [r for r in caplog.records if "numeric" in r.message]
    assert len(warned) == 1
    # 副产物：字符串字典序 max("10", "9") == "9"，正是数值水位列的口径缺陷
    assert ctx.state["tables"]["orders_seq"]["new_watermark"] == "9"


# ----------------------------------------------------------------------
# 【2】C7：polars + parquet storage 增量 delta 走 table_write 路由
# ----------------------------------------------------------------------
def test_polars_parquet_delta_written_via_table_write(work_dir):
    """C7：polars 引擎 + parquet storage 增量 delta 必须产出 dst+".parquet".

    首跑全量建立水位 → 源文件追加新行 → 二跑 delta 只含新增行；
    断言 ``dst + ".parquet"`` 存在、无 CSV 残留、下游 table_read 可读且
    内容恰为新 delta；新水位归一为 ISO-8601（不含空格分隔符）。
    """
    pytest.importorskip("polars")
    pytest.importorskip("pyarrow")

    cfg = _load_polars_parquet_cfg(work_dir)
    fields = [
        "order_id",
        "customer_id",
        "order_date",
        "created_ts",
        "quantity",
        "unit_price",
        "status",
    ]
    batch1 = [
        {
            "order_id": "ORD-1",
            "customer_id": "CUS-1",
            "order_date": "2026-08-01",
            "created_ts": "2026-08-01T10:00:00",
            "quantity": "1",
            "unit_price": "10.00",
            "status": "completed",
        },
        {
            "order_id": "ORD-2",
            "customer_id": "CUS-1",
            "order_date": "2026-08-02",
            "created_ts": "2026-08-02T10:00:00",
            "quantity": "2",
            "unit_price": "20.00",
            "status": "completed",
        },
        {
            "order_id": "ORD-3",
            "customer_id": "CUS-2",
            "order_date": "2026-08-03",
            "created_ts": "2026-08-03T10:00:00",
            "quantity": "3",
            "unit_price": "30.00",
            "status": "completed",
        },
    ]
    data_dir = os.path.join(work_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    src_csv = os.path.join(data_dir, "orders.csv")
    csv_write(src_csv, fields, batch1)
    table_cfg = cfg["incremental"]["tables"]["orders"]

    # ---- 首跑：全量 + 建立水位（wm 为 None 的 parquet 分支，行为不动）----
    run_root = os.path.join(work_dir, "run")
    run_dir1 = os.path.join(run_root, "B1")
    ctx1 = _make_ctx(cfg, run_dir1, "B-ingest-edge-B1", state={})
    entry1 = ingest._ingest_incremental(
        ctx1,
        "orders",
        "data/raw/orders.csv",
        src_csv,
        os.path.join(run_dir1, "01_raw"),
        table_cfg,
        _FakeLog(),
    )
    assert entry1["incremental_mode"] == "init_full_load"
    assert entry1["rows"] == 3
    first_wm = ctx1.state["tables"]["orders"]["new_watermark"]
    # 首跑水位取源 CSV 字符串 max（python 路径），本身即 ISO-8601 "T" 口径
    assert first_wm == "2026-08-03T10:00:00"

    # ---- 追加 2 行新数据（created_ts 晚于首跑水位）----
    batch2 = [
        {
            "order_id": "ORD-4",
            "customer_id": "CUS-2",
            "order_date": "2026-08-04",
            "created_ts": "2026-08-04T10:00:00",
            "quantity": "4",
            "unit_price": "40.00",
            "status": "completed",
        },
        {
            "order_id": "ORD-5",
            "customer_id": "CUS-1",
            "order_date": "2026-08-05",
            "created_ts": "2026-08-05T10:00:00",
            "quantity": "5",
            "unit_price": "50.00",
            "status": "completed",
        },
    ]
    csv_write(src_csv, fields, batch1 + batch2)

    # 模拟 pipeline 成功后的水位推进：new_watermark → watermark_value
    state2 = {"tables": {"orders": {"watermark_value": first_wm}}}
    run_dir2 = os.path.join(run_root, "B2")
    ctx2 = _make_ctx(cfg, run_dir2, "B-ingest-edge-B2", state=state2)
    entry2 = ingest._ingest_incremental(
        ctx2,
        "orders",
        "data/raw/orders.csv",
        src_csv,
        os.path.join(run_dir2, "01_raw"),
        table_cfg,
        _FakeLog(),
    )
    assert entry2["incremental_mode"] == "delta"
    assert entry2["rows"] == 2

    dst = os.path.join(run_dir2, "01_raw", "orders_incremental.csv")
    # C7 核心断言：parquet 文件必须存在，且不得有 CSV 残留（旧 bug 形态）
    assert os.path.exists(dst + ".parquet")
    assert not os.path.exists(dst)
    # 下游 table_read 可读（storage=parquet → _table_read_parquet 强制读 .parquet）
    df = table_read(dst, cfg)
    assert df.height == 2
    got_ids = sorted(str(r["order_id"]) for r in df.to_dicts())
    assert got_ids == ["ORD-4", "ORD-5"]  # 内容恰为新 delta，非陈旧数据
    # 新水位归一为 ISO-8601（无论 polars 是否推断出 datetime，均无空格分隔符）
    new_wm2 = ctx2.state["tables"]["orders"]["new_watermark"]
    assert new_wm2 == "2026-08-05T10:00:00"
    assert " " not in new_wm2


def test_polars_parquet_delta_empty_still_writes_readable_parquet(work_dir):
    """C7 边界：无新数据时 polars+parquet delta 仍写出可读的空 parquet（保留列 schema）."""
    pytest.importorskip("polars")
    pytest.importorskip("pyarrow")

    cfg = _load_polars_parquet_cfg(work_dir)
    fields = ["order_id", "customer_id", "created_ts"]
    batch1 = [
        {
            "order_id": "ORD-1",
            "customer_id": "CUS-1",
            "created_ts": "2026-08-01T10:00:00",
        },
    ]
    data_dir = os.path.join(work_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    src_csv = os.path.join(data_dir, "orders.csv")
    csv_write(src_csv, fields, batch1)
    table_cfg = cfg["incremental"]["tables"]["orders"]

    run_root = os.path.join(work_dir, "run")
    run_dir1 = os.path.join(run_root, "B1")
    ctx1 = _make_ctx(cfg, run_dir1, "B-ingest-edge-empty-B1", state={})
    entry1 = ingest._ingest_incremental(
        ctx1,
        "orders",
        "data/raw/orders.csv",
        src_csv,
        os.path.join(run_dir1, "01_raw"),
        table_cfg,
        _FakeLog(),
    )
    first_wm = ctx1.state["tables"]["orders"]["new_watermark"]
    assert entry1["incremental_mode"] == "init_full_load"

    # 二跑：源数据未变 → 零增量；水位保持不动
    state2 = {"tables": {"orders": {"watermark_value": first_wm}}}
    run_dir2 = os.path.join(run_root, "B2")
    ctx2 = _make_ctx(cfg, run_dir2, "B-ingest-edge-empty-B2", state=state2)
    entry2 = ingest._ingest_incremental(
        ctx2,
        "orders",
        "data/raw/orders.csv",
        src_csv,
        os.path.join(run_dir2, "01_raw"),
        table_cfg,
        _FakeLog(),
    )
    assert entry2["incremental_mode"] == "delta"
    assert entry2["rows"] == 0
    assert entry2["new_watermark"] == first_wm

    dst = os.path.join(run_dir2, "01_raw", "orders_incremental.csv")
    # 空 delta 也必须经 table_write 产出 .parquet（下游 table_read 才不会崩溃）
    assert os.path.exists(dst + ".parquet")
    df = table_read(dst, cfg)
    assert df.height == 0
    assert list(df.columns) == fields
