"""Pipeline configuration validation via pydantic v2.

Usage::

    from src.config_schema import validate_config, ConfigValidationError
    try:
        cfg = validate_config(cfg)   # returns normalized dict (defaults filled)
    except ConfigValidationError as e:
        print(f"config error: {e}")

Covers the fields actually read by pipeline.py / helpers.py / iceberg.py.
Unknown keys — top-level or inside known sections — are preserved
(``extra="allow"`` everywhere) so the returned dict is a drop-in
replacement for the input. Type coercion is lenient (``strict=False``)
to match the runtime, which freely does ``int(...)`` / ``float(...)``
on config values.

All annotations use ``Optional[...]`` instead of PEP 604 ``X | Y``:
pydantic evaluates annotations at class-definition time, and on
Python 3.9 (the minimum supported version) ``eval("int | None")``
raises TypeError even with ``from __future__ import annotations``.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class _AllowExtra(BaseModel):
    """Base: keep unknown keys so model_dump() round-trips the input."""

    model_config = {"extra": "allow", "strict": False}


class PipelineSection(_AllowExtra):
    name: str = "autobatch"
    version: str = "1.0.0"
    batch_id: str = "auto"
    run_dir: str = "run"
    data_dir: str = "data"


class EngineSparkSection(_AllowExtra):
    master: str = "local[*]"
    app_name: str = "autobatch"
    executor_memory: str = ""
    executor_cores: Optional[int] = None
    num_executors: Optional[int] = None
    driver_memory: str = ""
    shuffle_partitions: Optional[int] = None
    adaptive_query_execution: bool = True
    write_single_file: bool = False
    read_options: dict[str, Any] = Field(default_factory=dict)
    cluster: dict[str, Any] = Field(default_factory=dict)


class EnginePolarsSection(_AllowExtra):
    streaming: bool = False
    parquet_compression: str = "zstd"
    read_options: dict[str, Any] = Field(default_factory=dict)


class EngineSection(_AllowExtra):
    backend: str = "python"
    format: str = "csv"
    polars: EnginePolarsSection = Field(default_factory=EnginePolarsSection)
    spark: EngineSparkSection = Field(default_factory=EngineSparkSection)

    @field_validator("backend")
    @classmethod
    def _backend_must_be_known(cls, v: str) -> str:
        if v not in ("python", "polars", "spark"):
            raise ValueError(f"engine.backend must be one of python/polars/spark, got {v!r}")
        return v


class StorageSection(_AllowExtra):
    backend: str = "local_csv"
    bucket: str = ""
    endpoint: str = ""
    access_key: str = ""
    secret_key: str = ""
    secure: bool = False
    region: str = "us-east-1"
    warehouse: str = "state/warehouse"
    prefix: str = ""
    compression: str = "zstd"
    iceberg: dict[str, Any] = Field(default_factory=dict)

    @field_validator("backend")
    @classmethod
    def _storage_backend_must_be_known(cls, v: str) -> str:
        if v not in ("local_csv", "parquet", "iceberg"):
            raise ValueError(f"storage.backend must be one of local_csv/parquet/iceberg, got {v!r}")
        return v


class GeneratorSection(_AllowExtra):
    enabled: bool = False
    rows: int = 1000
    seed: int = 42
    output_dir: str = "data/raw"
    customer_count: int = 1000
    product_count: int = 100
    date_range_days: int = 90
    defect_rates: dict[str, float] = Field(default_factory=dict)


class DemoSection(_AllowExtra):
    fail_at: Optional[str] = None

    @field_validator("fail_at")
    @classmethod
    def _fail_at_must_be_valid(cls, v: Optional[str]) -> str:
        from .pipeline import STAGES  # lazy: pipeline imports this module too

        if v is None or v == "":
            return ""
        if v not in STAGES:
            raise ValueError(f"demo.fail_at must be one of {STAGES} or empty, got {v!r}")
        return v


class IncrementalSection(_AllowExtra):
    enabled: bool = False
    state_dir: str = "state"
    mode: str = ""  # "" / "watermark" / "snapshot"（运行时按缺省处理）
    tables: dict[str, Any] = Field(default_factory=dict)


class LoggingSection(_AllowExtra):
    format: str = "text"
    level: str = "INFO"


class MonitoringSection(_AllowExtra):
    enabled: bool = False
    log_level: str = "INFO"
    health_check: dict[str, Any] = Field(default_factory=dict)


class Config(_AllowExtra):
    pipeline: PipelineSection = Field(default_factory=PipelineSection)
    engine: EngineSection = Field(default_factory=EngineSection)
    storage: StorageSection = Field(default_factory=StorageSection)
    generator: GeneratorSection = Field(default_factory=GeneratorSection)
    demo: DemoSection = Field(default_factory=DemoSection)
    incremental: IncrementalSection = Field(default_factory=IncrementalSection)
    logging: LoggingSection = Field(default_factory=LoggingSection)
    monitoring: MonitoringSection = Field(default_factory=MonitoringSection)

    # Sections consumed verbatim by stages — passthrough maps.
    source: dict[str, Any] = Field(default_factory=dict)
    quality: dict[str, Any] = Field(default_factory=dict)
    clean: dict[str, Any] = Field(default_factory=dict)
    compute: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] = Field(default_factory=dict)
    error_handling: dict[str, Any] = Field(default_factory=dict)
    monitoring_config: str = "config/monitoring.json"


class ConfigValidationError(Exception):
    """Raised when the pipeline config fails validation."""


def validate_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Validate a pipeline config dict and return the normalized version.

    The returned dict has schema defaults filled in and unknown keys
    preserved, so callers can use it in place of the input and never hit
    KeyError on sections the schema guarantees (e.g. ``cfg["pipeline"]``).

    Raises ConfigValidationError with a human-readable message on the
    first issue encountered.
    """
    try:
        model = Config.model_validate(cfg)
    except Exception as e:  # noqa: BLE001
        raise ConfigValidationError(str(e)) from e
    return model_dump_preserving_extras(model)


def model_dump_preserving_extras(model: BaseModel) -> dict[str, Any]:
    """model_dump() that also recurses into extra (non-field) attributes.

    pydantic v2's model_dump() includes extras only when extra="allow",
    but nested extras inside dict-typed fields need no handling; extras
    stored on nested models are dumped by their own model_dump call.
    """
    data = model.model_dump()
    extras = model.__pydantic_extra__ or {}
    for key, value in extras.items():
        data.setdefault(key, value)
    return data
