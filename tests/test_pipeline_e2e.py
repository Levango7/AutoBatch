"""端到端冒烟：跑完整流水线，断言 status/DQ Score/manifest/metrics。"""
from __future__ import annotations

import os

from src.helpers import json_load


def test_pipeline_success(small_batch_dir):
    status = json_load(os.path.join(small_batch_dir, "status.json"))
    assert status["status"] == "success"


def test_dq_score_in_range(small_batch_dir):
    manifest = json_load(os.path.join(small_batch_dir, "manifest.json"))
    dq = manifest["quality"]["dq_score"]
    assert 0.95 <= dq <= 1.0, f"DQ Score {dq} 不在 [0.95, 1.0]"


def test_manifest_lineage_nonempty(small_batch_dir):
    manifest = json_load(os.path.join(small_batch_dir, "manifest.json"))
    assert len(manifest["lineage"]) > 0


def test_metrics_json_exists(small_batch_dir):
    metrics = json_load(os.path.join(small_batch_dir, "metrics.json"))
    assert "stages" in metrics
    assert len(metrics["stages"]) == 5
    assert metrics["status"] == "success"
