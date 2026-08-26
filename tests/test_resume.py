"""断点续跑（resume）功能单元测试.

覆盖 _load_resume_plan 的各种分支：
- resume=false 不加载
- auto batch_id 不加载
- 无 manifest.json 不加载
- status != failed 不加载（成功批次重跑视为全新）
- version/digest 漂移不加载
- 正常可续跑返回 prev dict

_stage_outputs_intact 分支：
- validate 要求 02_valid 非空 + quality_summary.json（主产物判据）
- quarantine/report 等终端目录为空/缺失不影响判定（干净数据正常态）
- ingest/clean/compute/output 主目录非空即可

端到端：真实失败 → 同 batch_id 续跑（干净数据 + OpenLineage 启用），
锁定 P0 回归：quarantine 为空不得阻断续跑；血缘边不得重复叠加；
OL 批次级 START→FAILED / START→COMPLETE 终态配对完整.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import uuid
from typing import Any

import pytest

from src.helpers import ROOT, VERSION, abs_path, csv_read, json_load, json_save
from src.lineage import Manifest
from src.pipeline import (
    _load_resume_plan,
    _stage_outputs_intact,
    run_pipeline,
)
from src.state import StateStore


# ----------------------------------------------------------------------
# helpers / fixtures
# ----------------------------------------------------------------------
def _mkcfg(resume: bool = False, **overrides) -> dict[str, Any]:
    cfg = {
        "error_handling": {"resume": resume, "max_retries": 0},
        "pipeline": {"version": VERSION},
        **overrides,
    }
    return cfg


def _write_manifest(
    run_dir: str,
    status: str = "failed",
    stages: list | None = None,
    pipeline_version: str | None = None,
    **extra,
) -> str:
    m = Manifest("b-resume", "digest-ok", run_dir)
    if pipeline_version is not None:
        m.pipeline_version = pipeline_version
    m.finish(status, extra.get("error"))
    path = os.path.join(run_dir, "manifest.json")
    json_save(path, m.to_dict())
    return path


@pytest.fixture
def workdir():
    d = tempfile.mkdtemp(prefix="autobatch_resume_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ----------------------------------------------------------------------
# _load_resume_plan 分支
# ----------------------------------------------------------------------
def test_resume_disabled_skips(workdir):
    cfg = _mkcfg(resume=False)
    _write_manifest(workdir, status="failed")
    result = _load_resume_plan(cfg, "b-resume", workdir, "digest-ok", logger=None)
    assert result is None


def test_auto_batch_id_skips(workdir):
    cfg = _mkcfg(resume=True)
    _write_manifest(workdir, status="failed")
    result = _load_resume_plan(cfg, "auto", workdir, "digest-ok", logger=None)
    assert result is None


def test_empty_batch_id_skips(workdir):
    cfg = _mkcfg(resume=True)
    _write_manifest(workdir, status="failed")
    result = _load_resume_plan(cfg, "", workdir, "digest-ok", logger=None)
    assert result is None


def test_no_manifest_skips(workdir):
    cfg = _mkcfg(resume=True)
    result = _load_resume_plan(cfg, "b-resume", workdir, "digest-ok", logger=None)
    assert result is None


def test_success_status_skips(workdir):
    cfg = _mkcfg(resume=True)
    _write_manifest(workdir, status="success")
    result = _load_resume_plan(cfg, "b-resume", workdir, "digest-ok", logger=None)
    assert result is None


def test_version_drift_skips(workdir):
    cfg = _mkcfg(resume=True)
    _write_manifest(workdir, status="failed", pipeline_version="9.9.9")
    result = _load_resume_plan(cfg, "b-resume", workdir, "digest-ok", logger=None)
    assert result is None


def test_digest_drift_skips(workdir):
    cfg = _mkcfg(resume=True)
    _write_manifest(workdir, status="failed")
    result = _load_resume_plan(cfg, "b-resume", workdir, "digest-different", logger=None)
    assert result is None


def test_valid_resume_plan_returns_prev(workdir):
    cfg = _mkcfg(resume=True)
    _write_manifest(workdir, status="failed")
    result = _load_resume_plan(cfg, "b-resume", workdir, "digest-ok", logger=None)
    assert result is not None
    assert result["batch_id"] == "b-resume"
    assert result["status"] == "failed"


# ----------------------------------------------------------------------
# _stage_outputs_intact
# ----------------------------------------------------------------------
def test_validate_intact_when_quality_exists(workdir):
    # 主判据 = 02_valid 非空 + 批次根目录的 quality_summary.json 在位；
    # quarantine/report 是终端产物，不参与判定（干净数据下可为空）
    for sub in ["02_valid", "quarantine", "report"]:
        d = os.path.join(workdir, sub)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "placeholder"), "w") as f:
            f.write("x")
    json_save(os.path.join(workdir, "quality_summary.json"), {"dq_score": 95})
    assert _stage_outputs_intact("validate", workdir) is True


def test_validate_missing_quality_summary_fails(workdir):
    # 02_valid 非空但批次根目录缺 quality_summary.json → 判产物不完整
    for sub in ["02_valid", "quarantine", "report"]:
        os.makedirs(os.path.join(workdir, sub), exist_ok=True)
        with open(os.path.join(workdir, sub, "placeholder"), "w"):
            pass
    assert _stage_outputs_intact("validate", workdir) is False


def test_ingest_intact_when_dir_nonempty(workdir):
    d = os.path.join(workdir, "01_raw")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "orders.csv"), "w") as f:
        f.write("a,b\n1,2\n")
    assert _stage_outputs_intact("ingest", workdir) is True


def test_ingest_empty_dir_fails(workdir):
    os.makedirs(os.path.join(workdir, "01_raw"), exist_ok=True)
    assert _stage_outputs_intact("ingest", workdir) is False


def test_clean_intact_when_dir_nonempty(workdir):
    d = os.path.join(workdir, "03_clean")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "cleaned.csv"), "w") as f:
        f.write("x\n1\n")
    assert _stage_outputs_intact("clean", workdir) is True


def test_compute_intact_when_dir_nonempty(workdir):
    d = os.path.join(workdir, "04_aggregates")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "daily_sales.csv"), "w") as f:
        f.write("x\n1\n")
    assert _stage_outputs_intact("compute", workdir) is True


def test_output_intact_when_dir_nonempty(workdir):
    d = os.path.join(workdir, "05_output")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "dashboard_data.json"), "w") as f:
        f.write("{}")
    assert _stage_outputs_intact("output", workdir) is True


def test_unknown_stage_always_true(workdir):
    """未知 stage（不在 _STAGE_OUTPUT_DIRS）返回 True（不校验输出目录）."""
    assert _stage_outputs_intact("unknown_stage", workdir) is True


def test_missing_dir_returns_false(workdir):
    assert _stage_outputs_intact("ingest", workdir) is False


# ----------------------------------------------------------------------
# lineage_decls 持久化与恢复
# ----------------------------------------------------------------------
def test_lineage_decl_stored_in_stage_extra(workdir):
    """manifest.add_stage(extra={"lineage_decl": {...}}) 正确写入磁盘（平铺到顶层）."""
    m = Manifest("b-lineage", "d", workdir)
    m.add_stage(
        "validate", "success", 100, 90, 50, "", extra={"lineage_decl": {"orders": ["02_valid"]}}
    )
    m.finish("success")
    m.save()
    saved = json_load(os.path.join(workdir, "manifest.json"))
    stage_entry = next(s for s in saved["stages"] if s["name"] == "validate")
    # add_stage 把 extra 平铺到 entry 顶层，pipeline resume 同时兼容顶层和嵌套两种写法
    assert stage_entry.get("lineage_decl") == {"orders": ["02_valid"]}


# ----------------------------------------------------------------------
# 主产物判据：终端目录（quarantine/report）不参与续跑判定（P0 回归锁）
# ----------------------------------------------------------------------
def test_validate_intact_allows_empty_quarantine(workdir):
    """干净数据下 quarantine 为空目录不应阻断续跑判定."""
    for sub, fname in [("02_valid", "orders_valid.csv"), ("report", "quality_report.md")]:
        d = os.path.join(workdir, sub)
        os.makedirs(d)
        with open(os.path.join(d, fname), "w", encoding="utf-8") as f:
            f.write("x")
    os.makedirs(os.path.join(workdir, "quarantine"))  # 故意为空
    json_save(os.path.join(workdir, "quality_summary.json"), {"dq_score": 100})
    assert _stage_outputs_intact("validate", workdir) is True


def test_validate_intact_ignores_missing_aux_dirs(workdir):
    """quarantine/report 目录整个不存在也不影响主产物判据."""
    d = os.path.join(workdir, "02_valid")
    os.makedirs(d)
    with open(os.path.join(d, "orders_valid.csv"), "w", encoding="utf-8") as f:
        f.write("x")
    json_save(os.path.join(workdir, "quality_summary.json"), {"dq_score": 100})
    assert _stage_outputs_intact("validate", workdir) is True


# ----------------------------------------------------------------------
# 端到端：真实失败 → 同 batch_id 续跑（干净数据 + OpenLineage）
# ----------------------------------------------------------------------
def test_e2e_resume_clean_data_with_openlineage(_same_drive_tmp_root, request):
    """P0 回归锁：compute 真实失败后续跑成功.

    - 干净数据（缺陷率全 0）→ quarantine/ 为空目录，不得阻断续跑
    - 续跑 stage 带 resumed 标记；血缘边无重复；恢复 stage 计入 metrics
    - OL 批次级 START→FAILED / START→COMPLETE 配对完整
    """
    import src.stages.compute as compute_mod
    from src.generator import main as gen_main

    work_dir = tempfile.mkdtemp(prefix="resume_e2e_", dir=_same_drive_tmp_root)
    cfg = json_load(abs_path("config/pipeline_small.json"))
    data_dir = os.path.join(work_dir, "data", "raw")
    cfg["generator"]["output_dir"] = data_dir
    for k in cfg["generator"]["defect_rates"]:
        cfg["generator"]["defect_rates"][k] = 0.0
    gen_main(cfg)
    cfg["generator"]["enabled"] = False
    cfg["source"]["files"] = {
        "orders": os.path.join(data_dir, "orders.csv"),
        "customers": os.path.join(data_dir, "customers.csv"),
        "products": os.path.join(data_dir, "products.csv"),
    }
    run_root = os.path.join(ROOT, "run")
    os.makedirs(run_root, exist_ok=True)
    cfg["pipeline"]["run_dir"] = run_root
    cfg["error_handling"]["resume"] = True
    cfg["openlineage"] = {"enabled": True, "namespace": "testns", "endpoint": ""}
    batch_id = "test-resume-e2e-" + uuid.uuid4().hex[:6]
    run_dir = os.path.join(run_root, batch_id)
    request.addfinalizer(lambda: shutil.rmtree(run_dir, ignore_errors=True))

    # 第一次：compute 模块函数注入一次性失败（不改配置 → config_digest 保持一致）
    orig_run = compute_mod.run

    def _boom(ctx, log):
        raise RuntimeError("boom-for-resume")

    compute_mod.run = _boom
    try:
        rc1 = run_pipeline(cfg, batch_id, "")
    finally:
        compute_mod.run = orig_run
    assert rc1 == 1
    status1 = json_load(os.path.join(run_dir, "status.json"))
    assert status1["status"] == "failed"
    # 前置确认：干净数据下 quarantine 目录存在但为空（旧实现据此回退全量）
    qu_dir = os.path.join(run_dir, "quarantine")
    assert os.path.isdir(qu_dir) and not os.listdir(qu_dir)

    # 第二次：同 cfg 同 batch_id → ingest/validate/clean 应被跳过
    rc2 = run_pipeline(cfg, batch_id, "")
    assert rc2 == 0
    status2 = json_load(os.path.join(run_dir, "status.json"))
    assert status2["status"] == "success"
    assert len(status2["stages"]) == 5
    by_name = {s["name"]: s for s in status2["stages"]}
    for skipped in ("ingest", "validate", "clean"):
        assert by_name[skipped].get("resumed") is True, skipped
    assert "resumed" not in by_name["compute"]
    assert "resumed" not in by_name["output"]

    # 血缘边无重复叠加（旧实现 extend 会把上游重复 N 次）
    manifest2 = json_load(os.path.join(run_dir, "manifest.json"))
    for target, ups in manifest2["lineage"].items():
        assert len(ups) == len(set(ups)), f"duplicated upstreams for {target}: {ups}"

    # 恢复的 stage 也计入本轮 metrics（5 个阶段齐全）
    metrics2 = json_load(os.path.join(run_dir, "metrics.json"))
    assert len(metrics2.get("stages", [])) == 5

    # OpenLineage 事件流：批次级与 compute 级生命周期配对完整
    ol_path = os.path.join(run_dir, "openlineage.ndjson")
    with open(ol_path, encoding="utf-8") as f:
        events = [json.loads(line) for line in f if line.strip()]
    pipe_events = [e for e in events if e["job"]["name"] == "testns.pipeline"]
    assert [e["eventType"] for e in pipe_events] == ["START", "FAILED", "START", "COMPLETE"]
    compute_events = [e for e in events if e["job"]["name"] == "testns.compute"]
    assert [e["eventType"] for e in compute_events] == ["START", "FAILED", "START", "COMPLETE"]
    # validate：第一次运行真实执行（START→COMPLETE），续跑批次跳过时补发 COMPLETE
    validate_events = [e for e in events if e["job"]["name"] == "testns.validate"]
    assert [e["eventType"] for e in validate_events] == ["START", "COMPLETE", "COMPLETE"]


# ----------------------------------------------------------------------
# 任务 #74 C1+C2+C4：续跑恢复内存态与增量提交（端到端）
# ----------------------------------------------------------------------
def _max_order_date(orders_csv: str) -> str:
    """源文件中的最大 order_date（bad_date 缺陷在增量 fixture 中已关闭）."""
    rows, _ = csv_read(orders_csv)
    return max(r["order_date"] for r in rows if r.get("order_date"))


def test_resume_after_validate_failure_incremental(inc_env):
    """C1+C2+C4：增量批次 validate 失败 → 同 batch_id 续跑.

    修复前缺陷：
      - C1：续跑跳过 ingest 后 ctx.ingested 为空 → validate 重跑零校验
        （DQ 虚报，quality checks_total=0）.
      - C2：staged 水位随崩溃进程丢失 → 提交阶段无水位可提升，
        下一批次重复聚合翻倍.
    本测试锁定：validate 真实执行（checks_total>0）、staged 水位从失败
    manifest 恢复并经续跑提交恰好提升一次、台账登记、聚合落正式路径.
    """
    env = inc_env
    cfg = env["cfg"]
    cfg["error_handling"]["resume"] = True
    state_dir = env["state_dir"]
    batch_id = "test-inc-" + uuid.uuid4().hex[:8]
    run_dir = os.path.join(env["run_root"], batch_id)

    # --- run1：validate 注入失败 ---
    rc1 = run_pipeline(cfg, batch_id, fail_at="validate")
    assert rc1 == 1

    # 失败 manifest 必须携带续跑所需内存态（C1+C2 写侧）
    manifest1 = json_load(os.path.join(run_dir, "manifest.json"))
    ingest1 = next(s for s in manifest1["stages"] if s["name"] == "ingest")
    assert ingest1["status"] == "success"
    assert ingest1.get("ingested"), "ingest 条目应持久化 ingested 文件列表（C1）"
    assert ingest1.get("staged_state"), "ingest 条目应持久化 staged 水位（C2）"
    assert ingest1["staged_state"].get("tables"), "staged_state 应含 tables 段"

    # 失败不推进水位（两阶段提交）
    store = StateStore(state_dir)
    assert not store.is_batch_merged(store.load(), batch_id)

    # --- run2：同 cfg 同 batch_id 续跑 ---
    rc2 = run_pipeline(cfg, batch_id, fail_at="")
    assert rc2 == 0

    manifest2 = json_load(os.path.join(run_dir, "manifest.json"))
    by_name = {s["name"]: s for s in manifest2["stages"]}
    assert by_name["ingest"].get("resumed") is True
    assert by_name["validate"].get("resumed") is not True, "validate 必须重跑"

    # C1：validate 真实执行（非空转）——质量检查数 > 0，valid 产物重建
    quality = json_load(os.path.join(run_dir, "quality_summary.json"))
    assert quality["checks_total"] > 0, "续跑 validate 不得零校验空转（C1）"
    assert os.listdir(os.path.join(run_dir, "02_valid"))

    # C2+C4：staged 水位恢复后提交恰好一次
    state = store.load()
    info = state["tables"]["orders"]
    assert info["watermark_value"] == _max_order_date(env["orders_path"]), (
        "续跑成功后 staged 水位应恢复并正式提升（修复前：staged 丢失、水位不推进）"
    )
    assert info["cumulative_row_count"] == info["last_seen_row_count"] > 0, (
        "水位必须恰好推进一次（cumulative == 本批 seen，无双提交）"
    )
    assert "new_watermark" not in info
    assert batch_id in state.get("merged_batches", []), "台账应登记批次"
    # C4：聚合从暂存替换进正式路径
    assert os.path.isfile(os.path.join(state_dir, "aggregates", "daily_sales.csv"))


def test_resume_after_clean_failure_restores_outlier_keys(inc_env, monkeypatch):
    """C1：clean 失败续跑 → clean 重跑时拿到 run1 validate 持久化的 outlier_keys.

    用 spy 捕获续跑 clean 收到的 ctx.outlier_keys，与失败 manifest 中
    validate 条目持久化的列表对比（修复前续跑 ctx 全新 → clean 缺键）。
    """
    import src.stages.clean as clean_mod

    env = inc_env
    cfg = env["cfg"]
    cfg["error_handling"]["resume"] = True
    batch_id = "test-inc-" + uuid.uuid4().hex[:8]
    run_dir = os.path.join(env["run_root"], batch_id)

    # --- run1：clean 注入失败（ingest/validate 已成功）---
    rc1 = run_pipeline(cfg, batch_id, fail_at="clean")
    assert rc1 == 1

    manifest1 = json_load(os.path.join(run_dir, "manifest.json"))
    validate1 = next(s for s in manifest1["stages"] if s["name"] == "validate")
    assert validate1["status"] == "success"
    # validate 条目持久化了 outlier_keys（可能为空集 → 键缺省；空集时续跑
    # 行为与缺省一致，round-trip 语义仍然成立）
    persisted_keys = validate1.get("outlier_keys")

    # --- run2：续跑，spy 捕获 clean 收到的 ctx ---
    captured: dict[str, Any] = {}
    orig_run = clean_mod.run

    def _spy(ctx, log):
        captured["outlier_keys"] = set(ctx.outlier_keys)
        return orig_run(ctx, log)

    monkeypatch.setattr(clean_mod, "run", _spy)
    rc2 = run_pipeline(cfg, batch_id, fail_at="")
    assert rc2 == 0

    manifest2 = json_load(os.path.join(run_dir, "manifest.json"))
    by_name = {s["name"]: s for s in manifest2["stages"]}
    assert by_name["ingest"].get("resumed") is True
    assert by_name["validate"].get("resumed") is True
    assert by_name["clean"].get("resumed") is not True, "clean 必须重跑"

    # clean 真实执行并消费了恢复的 outlier_keys（C1 round-trip）
    expected = set(persisted_keys or [])
    assert captured.get("outlier_keys") == expected, (
        f"续跑 clean 的 outlier_keys 应与 run1 validate 持久化值一致: "
        f"captured={captured.get('outlier_keys')}, persisted={persisted_keys}"
    )
    # 续跑 manifest 的 validate 条目继续携带该键（二次失败仍可恢复）
    if persisted_keys is not None:
        assert by_name["validate"].get("outlier_keys") == persisted_keys


def test_resume_after_validate_failure_full_mode_dq_equivalence(_same_drive_tmp_root, request):
    """C1 全量模式回归：validate 失败续跑后的 DQ 必须等价于一次成功批次.

    同配置同种子生成两份确定性数据：控制批次一次跑成功，实验批次
    fail_at=validate 后续跑成功。修复前后者 validate 空转（DQ=1.0/零校验），
    与控制批次 DQ 明显不等；修复后两者检查数与 DQ 完全一致。
    """
    from src.generator import main as gen_main

    def _make_env(tag: str) -> tuple[dict[str, Any], str]:
        work_dir = tempfile.mkdtemp(prefix=f"resume_full_{tag}_", dir=_same_drive_tmp_root)
        cfg = json_load(abs_path("config/pipeline_small.json"))
        data_dir = os.path.join(work_dir, "data", "raw")
        cfg["generator"]["output_dir"] = data_dir
        gen_main(cfg)
        cfg["generator"]["enabled"] = False
        cfg["source"]["files"] = {
            "orders": os.path.join(data_dir, "orders.csv"),
            "customers": os.path.join(data_dir, "customers.csv"),
            "products": os.path.join(data_dir, "products.csv"),
        }
        run_root = os.path.join(ROOT, "run")
        os.makedirs(run_root, exist_ok=True)
        cfg["pipeline"]["run_dir"] = run_root
        cfg["error_handling"]["resume"] = True
        return cfg, run_root

    # --- 控制批次：一次成功 ---
    cfg_ctrl, run_root_ctrl = _make_env("ctrl")
    bid_ctrl = "test-resume-full-ctrl-" + uuid.uuid4().hex[:6]
    run_dir_ctrl = os.path.join(run_root_ctrl, bid_ctrl)
    request.addfinalizer(lambda: shutil.rmtree(run_dir_ctrl, ignore_errors=True))
    rc_ctrl = run_pipeline(cfg_ctrl, bid_ctrl, fail_at="")
    assert rc_ctrl == 0
    quality_ctrl = json_load(os.path.join(run_dir_ctrl, "quality_summary.json"))

    # --- 实验批次：validate 失败 → 续跑 ---
    cfg_exp, run_root_exp = _make_env("exp")
    bid_exp = "test-resume-full-exp-" + uuid.uuid4().hex[:6]
    run_dir_exp = os.path.join(run_root_exp, bid_exp)
    request.addfinalizer(lambda: shutil.rmtree(run_dir_exp, ignore_errors=True))
    rc1 = run_pipeline(cfg_exp, bid_exp, fail_at="validate")
    assert rc1 == 1
    rc2 = run_pipeline(cfg_exp, bid_exp, fail_at="")
    assert rc2 == 0

    manifest2 = json_load(os.path.join(run_dir_exp, "manifest.json"))
    by_name = {s["name"]: s for s in manifest2["stages"]}
    assert by_name["ingest"].get("resumed") is True
    quality_exp = json_load(os.path.join(run_dir_exp, "quality_summary.json"))

    # 续跑 validate 真实执行：检查数非零，且与控制批次一致（确定性数据）
    assert quality_exp["checks_total"] > 0, "续跑 validate 不得零校验（C1）"
    assert quality_exp["checks_total"] == quality_ctrl["checks_total"]
    assert quality_exp["dq_score"] == quality_ctrl["dq_score"], (
        f"续跑 DQ {quality_exp['dq_score']} 应等价于一次成功批次 "
        f"{quality_ctrl['dq_score']}（修复前 validate 空转会虚报 1.0）"
    )
