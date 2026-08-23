"""断点续跑（resume）功能单元测试.

覆盖 _load_resume_plan 的各种分支：
- resume=false 不加载
- auto batch_id 不加载
- 无 manifest.json 不加载
- status != failed 不加载（成功批次重跑视为全新）
- version/digest 漂移不加载
- 正常可续跑返回 prev dict

_stage_outputs_intact 分支：
- validate 要求 quality_summary.json
- ingest 要求 01_raw/ 非空
- 目录不存在 / 为空 → False
- 其他 stage（无额外文件要求）→ dirs 非空即可

集成：pipeline 在 resume 模式下正确跳过 stage 并恢复 lineage_decls.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from typing import Any

import pytest

from src.helpers import VERSION, abs_path, json_load, json_save
from src.lineage import Manifest
from src.pipeline import (
    _load_resume_plan,
    _stage_outputs_intact,
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
    # validate 写 02_valid/ + quarantine/ + report/，三者都必须非空
    for sub in ["02_valid", "quarantine", "report"]:
        d = os.path.join(workdir, sub)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "placeholder"), "w") as f:
            f.write("x")
    json_save(os.path.join(workdir, "02_valid", "quality_summary.json"), {"dq_score": 95})
    assert _stage_outputs_intact("validate", workdir) is True


def test_validate_missing_quality_summary_fails(workdir):
    # 创建必要目录但缺少 quality_summary.json
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
