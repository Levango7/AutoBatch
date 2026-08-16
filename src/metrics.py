"""Pipeline metrics recorder.

Collects per-stage timings, row counts, DQ score and quarantine totals, then
emits ``metrics.json`` into the run directory. The on-disk layout is designed
for easy forwarding to Prometheus / other metric sinks:

* ``stages``  - labelled records (one per stage), suitable for iteration.
* ``metrics`` - flat key/value map (e.g. ``stage_ingest_duration_ms``), suitable
  for direct gauge exposition with ``batch_id`` as an external label.

Stdlib only; no third-party dependencies.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from .helpers import json_save, utc_ts


class MetricsRecorder:
    """Accumulate pipeline metrics and persist them as metrics.json."""

    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        self.started_at = utc_ts()
        self.finished_at: Optional[str] = None
        self.status: str = "running"
        self.stages: list[dict[str, Any]] = []
        self.total_duration_ms: int = 0
        self.dq_score: Optional[float] = None
        self.quarantined_rows: dict[str, int] = {}

    def record_stage(self, name: str, status: str, duration_ms: int,
                     rows_in: int, rows_out: int,
                     extra: Optional[dict[str, Any]] = None) -> None:
        """Record one stage execution result."""
        rec: dict[str, Any] = {
            "name": name,
            "status": status,
            "duration_ms": duration_ms,
            "rows_in": rows_in,
            "rows_out": rows_out,
        }
        if extra:
            rec.update(extra)
        self.stages.append(rec)

    def finish(self, status: str, total_duration_ms: int,
               dq_score: Optional[float] = None,
               quarantined_rows: Optional[dict[str, int]] = None) -> None:
        """Finalise the pipeline-level metrics."""
        self.status = status
        self.finished_at = utc_ts()
        self.total_duration_ms = total_duration_ms
        if dq_score is not None:
            self.dq_score = dq_score
        if quarantined_rows is not None:
            self.quarantined_rows = quarantined_rows

    def to_dict(self) -> dict[str, Any]:
        """Return the full metrics document (labelled + flat views)."""
        quarantined_total = sum(self.quarantined_rows.values())
        flat: dict[str, Any] = {
            "pipeline_duration_ms": self.total_duration_ms,
            "pipeline_status_success": 1 if self.status == "success" else 0,
            "pipeline_quarantined_total": quarantined_total,
        }
        if self.dq_score is not None:
            flat["pipeline_dq_score"] = self.dq_score
        for s in self.stages:
            prefix = "stage_{}_".format(s["name"])
            flat[prefix + "duration_ms"] = s["duration_ms"]
            flat[prefix + "rows_in"] = s["rows_in"]
            flat[prefix + "rows_out"] = s["rows_out"]
            flat[prefix + "status_success"] = 1 if s["status"] == "success" else 0
        return {
            "batch_id": self.batch_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "total_duration_ms": self.total_duration_ms,
            "dq_score": self.dq_score,
            "quarantined_rows": self.quarantined_rows,
            "quarantined_total": quarantined_total,
            "stages": self.stages,
            "metrics": flat,
        }

    def save(self, run_dir: str) -> str:
        """Write metrics.json into the run directory and return its path."""
        path = os.path.join(run_dir, "metrics.json")
        json_save(path, self.to_dict())
        return path
