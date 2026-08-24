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

from src.helpers import ROOT, VERSION, abs_path, json_load, json_save
from src.lineage import Manifest
from src.pipeline import (
    _load_resume_plan,
    _stage_outputs_intact,
    run_pipeline,
)


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


def _write_manifest(run_dir: str, status: str = "failed", stages: list | None = None, pipeline_version: str | None = None, **extra) -> str:
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
    m.add_stage("validate", "success", 100, 90, 50, "", extra={"lineage_decl": {"orders": ["02_valid"]}})
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
