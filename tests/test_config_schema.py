"""Tests for src.config_schema — pipeline config validation."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.config_schema import ConfigValidationError, validate_config

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def _load(name: str) -> dict:
    return json.loads((CONFIG_DIR / name).read_text(encoding="utf-8-sig"))


# ---------------------------------------------------------------------------
# Happy path: every shipped config must validate
# ---------------------------------------------------------------------------


class TestLoadedConfigs:
    def test_pipeline_json_valid(self) -> None:
        validate_config(_load("pipeline.json"))

    def test_pipeline_small_json_valid(self) -> None:
        validate_config(_load("pipeline_small.json"))


# ---------------------------------------------------------------------------
# Known invalid values raise ConfigValidationError
# ---------------------------------------------------------------------------


class TestInvalidEngineBackend:
    def test_unknown_backend(self) -> None:
        cfg = _load("pipeline_small.json")
        cfg["engine"]["backend"] = "dask"
        with pytest.raises(ConfigValidationError, match="engine.backend"):
            validate_config(cfg)

    def test_valid_backends(self) -> None:
        for backend in ("python", "polars", "spark"):
            cfg = _load("pipeline_small.json")
            cfg["engine"]["backend"] = backend
            validate_config(cfg)  # must not raise


class TestInvalidStorageBackend:
    def test_unknown_backend(self) -> None:
        cfg = _load("pipeline_small.json")
        cfg["storage"]["backend"] = "hdfs"
        with pytest.raises(ConfigValidationError, match="storage.backend"):
            validate_config(cfg)

    def test_valid_backends(self) -> None:
        for backend in ("local_csv", "parquet", "iceberg"):
            cfg = _load("pipeline_small.json")
            cfg["storage"]["backend"] = backend
            validate_config(cfg)


class TestInvalidFailAt:
    def test_bad_fail_at(self) -> None:
        cfg = _load("pipeline_small.json")
        cfg["demo"]["fail_at"] = "nonexistent_stage"
        with pytest.raises(ConfigValidationError, match="fail_at"):
            validate_config(cfg)

    def test_empty_fail_at_ok(self) -> None:
        cfg = _load("pipeline_small.json")
        cfg["demo"]["fail_at"] = ""
        validate_config(cfg)

    def test_valid_fail_at_stages(self) -> None:
        from src.pipeline import STAGES

        for stage in STAGES:
            cfg = _load("pipeline_small.json")
            cfg["demo"]["fail_at"] = stage
            validate_config(cfg)


# ---------------------------------------------------------------------------
# Extra top-level keys are tolerated (forward-compatibility)
# ---------------------------------------------------------------------------


class TestExtraKeys:
    def test_unknown_top_level_key_allowed(self) -> None:
        cfg = _load("pipeline_small.json")
        cfg["some_future_section"] = {"x": 1}
        validate_config(cfg)  # must not raise


# ---------------------------------------------------------------------------
# Missing required sub-sections use defaults (no error)
# ---------------------------------------------------------------------------


class TestMinimalConfig:
    def test_empty_dict_valid(self) -> None:
        validate_config({})

    def test_only_pipeline_name(self) -> None:
        validate_config({"pipeline": {"name": "my-pipeline"}})
