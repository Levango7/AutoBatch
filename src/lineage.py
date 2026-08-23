"""Run manifest and lineage ledger."""

from __future__ import annotations

import os
from typing import Any, Optional

from .helpers import VERSION, json_save, utc_ts


class Manifest:
    """Run ledger: batch id, sources, stage statuses, artifacts, lineage edges."""

    def __init__(self, batch_id: str, config_digest: str, run_dir: str):
        self.batch_id = batch_id
        self.pipeline_version = VERSION
        self.config_digest = config_digest
        self.run_dir = run_dir
        self.started_at = utc_ts()
        self.finished_at: Optional[str] = None
        self.status = "running"
        self.source: dict[str, Any] = {"name": "", "files": []}
        self.stages: list[dict[str, Any]] = []
        self.artifacts: dict[str, dict[str, Any]] = {}
        self.lineage: dict[str, list[str]] = {}
        self.quality: Optional[dict[str, Any]] = None
        self.error: Optional[str] = None

    def set_source(self, name: str, files: list[dict[str, Any]]) -> None:
        self.source = {"name": name, "files": files}

    def add_stage(
        self,
        name: str,
        status: str,
        rows_in: int,
        rows_out: int,
        duration_ms: int,
        log_path: str,
        error: Optional[str] = None,
    ) -> None:
        self.stages.append(
            {
                "name": name,
                "status": status,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "duration_ms": duration_ms,
                "log": log_path,
                "error": error,
            }
        )

    def add_artifact(
        self,
        relpath: str,
        kind: str,
        rows: Optional[int],
        sha256: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        entry = {
            "path": relpath,
            "kind": kind,
            "rows": rows,
            "sha256": sha256,
            "batch_id": self.batch_id,
        }
        if extra:
            entry.update(extra)
        self.artifacts[relpath] = entry

    def add_edge(self, target: str, upstreams: list[str]) -> None:
        self.lineage[target] = list(upstreams)

    def set_quality(self, summary: dict[str, Any]) -> None:
        self.quality = summary

    def finish(self, status: str, error: Optional[str] = None) -> None:
        self.status = status
        self.finished_at = utc_ts()
        self.error = error

    def save(self) -> str:
        path = os.path.join(self.run_dir, "manifest.json")
        json_save(path, self.to_dict())
        return path

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "pipeline_version": self.pipeline_version,
            "config_digest": self.config_digest,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "run_dir": self.run_dir,
            "source": self.source,
            "stages": self.stages,
            "artifacts": self.artifacts,
            "lineage": self.lineage,
            "quality": self.quality,
            "error": self.error,
        }


def save_latest_pointer(run_root: str, batch_id: str, run_dir: str) -> None:
    json_save(
        os.path.join(run_root, "latest.json"),
        {
            "batch_id": batch_id,
            "run_dir": run_dir,
            "updated_at": utc_ts(),
        },
    )


def lineage_view(manifest: Manifest) -> dict[str, Any]:
    """User-facing lineage: nodes (artifacts) and edges."""
    nodes = []
    for rel, info in manifest.artifacts.items():
        nodes.append(
            {
                "id": rel,
                "kind": info.get("kind", "file"),
                "rows": info.get("rows"),
                "sha256": (info.get("sha256") or "")[:12],
                "batch_id": info.get("batch_id"),
            }
        )
    edges = [{"from": up, "to": target} for target, ups in manifest.lineage.items() for up in ups]
    return {"nodes": nodes, "edges": edges}
