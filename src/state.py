"""State store: cross-batch watermark + aggregate persistence for incremental mode.

Manages ``state/state.json`` (watermarks + per-table metadata) and
``state/aggregates/*.csv`` (historical aggregation results). Lives at project
root, independent of any ``run/<batch_id>/`` directory, shared across batches.

Design (see docs/evolution.md §3.3.1 / §3.3.5):
- Watermark is staged in memory by ``set_new_watermark`` and only persisted by
  ``commit_watermark`` after every pipeline stage succeeds (two-phase commit).
- On failure the caller simply never commits, so state.json keeps the old
  watermark and the next run re-reads the same delta — idempotent.
- ``merge_aggregate`` accumulates numeric columns over key columns and
  recomputes derived columns (avg_order_value / revenue_share / rank).

Zero dependencies: only stdlib (json / csv / os).
"""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Sequence
from typing import Any, Optional

from .helpers import utc_ts

# Derived columns that must be recomputed after merge rather than accumulated.
_DERIVED_COLS = {"avg_order_value", "revenue_share", "rank"}


class StateStore:
    """Persistent cross-batch state for incremental processing.

    Parameters
    ----------
    state_dir:
        Absolute (or project-relative) path to the state directory. The store
        reads/writes ``<state_dir>/state.json`` and ``<state_dir>/aggregates/``.
    """

    def __init__(self, state_dir: str) -> None:
        self.state_dir = os.path.abspath(state_dir)
        self.state_path = os.path.join(self.state_dir, "state.json")

    # ------------------------------------------------------------------
    # state.json load / save
    # ------------------------------------------------------------------
    def load(self) -> dict[str, Any]:
        """Load state.json; return an empty skeleton if the file is absent."""
        if os.path.exists(self.state_path):
            with open(self.state_path, encoding="utf-8-sig") as f:
                state = json.load(f)
            state.setdefault("version", "1.0")
            state.setdefault("tables", {})
            state.setdefault("aggregates", {})
            # Phase 4: Iceberg snapshot id 持久化（与 watermark_value 并存）
            state.setdefault("iceberg_snapshots", {})
            return state
        return {
            "version": "1.0",
            "tables": {},
            "aggregates": {},
            "iceberg_snapshots": {},
        }

    def save(self, state: dict[str, Any]) -> None:
        """Persist state.json atomically (write then os.replace for crash safety)."""
        os.makedirs(self.state_dir, exist_ok=True)
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.state_path)

    # ------------------------------------------------------------------
    # watermark accessors
    # ------------------------------------------------------------------
    def get_watermark(self, table: str) -> Optional[str]:
        """Return ``tables[table][watermark_value]`` or ``None`` if not set."""
        if not os.path.exists(self.state_path):
            return None
        state = self.load()
        return state.get("tables", {}).get(table, {}).get("watermark_value")

    def set_new_watermark(self, state: dict[str, Any], table: str,
                          value: Optional[str], row_count: int,
                          batch_id: str) -> None:
        """Stage a new watermark in memory (does NOT persist).

        Called by the ingest stage for each table. The value is promoted to
        ``watermark_value`` only when ``commit_watermark`` runs after every
        stage succeeds.
        """
        tables = state.setdefault("tables", {})
        info = tables.setdefault(table, {})
        info["new_watermark"] = value
        info["new_seen_row_count"] = row_count
        info["new_batch_id"] = batch_id

    def commit_watermark(self, state: dict[str, Any], batch_id: str) -> None:
        """Promote every staged ``new_watermark`` to ``watermark_value`` and persist.

        Updates ``last_batch_id`` / ``last_processed_at`` /
        ``cumulative_row_count`` for each committed table. Tables without a
        staged new watermark are left untouched.
        """
        now = utc_ts()
        for _name, info in state.get("tables", {}).items():
            if "new_watermark" in info:
                info["watermark_value"] = info.pop("new_watermark")
                seen = info.pop("new_seen_row_count", 0)
                info["last_seen_row_count"] = seen
                info["cumulative_row_count"] = (
                    info.get("cumulative_row_count", 0) + seen
                )
                info.pop("new_batch_id", None)
                info["last_batch_id"] = batch_id
                info["last_processed_at"] = now
        state["updated_at"] = now
        state["last_batch_id"] = batch_id
        self.save(state)

    # ------------------------------------------------------------------
    # Iceberg snapshot id accessors (Phase 4)
    # ------------------------------------------------------------------
    # 与 watermark 两阶段提交并行：set_new_snapshot_id 暂存新 snapshot id，
    # commit_snapshot_id 在所有 stage 成功后提升为已提交 snapshot_id。
    # state["iceberg_snapshots"][table] = {
    #     "snapshot_id": <committed>,        # 已提交的 last snapshot id
    #     "new_snapshot_id": <staged>,       # 暂存的 new snapshot id（commit 前存在）
    #     "last_batch_id": <batch_id>,
    #     "last_processed_at": <ts>,
    # }
    def get_snapshot_id(self, table: str) -> Optional[int]:
        """Return committed ``iceberg_snapshots[table][snapshot_id]`` or None."""
        if not os.path.exists(self.state_path):
            return None
        state = self.load()
        val = state.get("iceberg_snapshots", {}).get(table, {}).get("snapshot_id")
        return int(val) if val is not None else None

    def set_new_snapshot_id(
        self,
        state: dict[str, Any],
        table: str,
        snapshot_id: Optional[int],
        batch_id: str,
    ) -> None:
        """Stage a new Iceberg snapshot id in memory (does NOT persist).

        Called by the ingest stage (iceberg_snapshot_diff mode) for each table
        after appending new rows. The value is promoted to ``snapshot_id`` only
        when ``commit_snapshot_id`` runs after every stage succeeds.
        """
        snaps = state.setdefault("iceberg_snapshots", {})
        info = snaps.setdefault(table, {})
        info["new_snapshot_id"] = snapshot_id
        info["new_batch_id"] = batch_id

    def commit_snapshot_id(self, state: dict[str, Any], batch_id: str) -> None:
        """Promote every staged ``new_snapshot_id`` to ``snapshot_id`` and persist.

        Called by pipeline._advance_and_merge after every stage succeeded.
        Tables without a staged new snapshot id are left untouched. Persists
        state.json (combined with watermark commit in the same call site).
        """
        now = utc_ts()
        for _name, info in state.get("iceberg_snapshots", {}).items():
            if "new_snapshot_id" in info:
                info["snapshot_id"] = info.pop("new_snapshot_id")
                info.pop("new_batch_id", None)
                info["last_batch_id"] = batch_id
                info["last_processed_at"] = now
        state["updated_at"] = now
        state["last_batch_id"] = batch_id
        self.save(state)

    # ------------------------------------------------------------------
    # aggregate persistence
    # ------------------------------------------------------------------
    def get_aggregate_path(self, name: str) -> str:
        """Return the absolute path of ``<state_dir>/aggregates/{name}.csv``."""
        return os.path.join(self.state_dir, "aggregates", name + ".csv")

    def load_aggregate(self, name: str) -> tuple[list[dict[str, str]], list[str]]:
        """Load historical aggregate csv. Returns ``([], [])`` if absent."""
        path = self.get_aggregate_path(name)
        if not os.path.exists(path):
            return [], []
        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fields = list(reader.fieldnames or [])
            data = list(reader)
        return data, fields

    def save_aggregate(self, name: str, fields: Sequence[str],
                       rows: Sequence[dict[str, Any]]) -> None:
        """Write historical aggregate to ``<state_dir>/aggregates/{name}.csv``."""
        path = self.get_aggregate_path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(fields),
                                    extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def merge_aggregate(self, name: str, fields: Sequence[str],
                        new_rows: Sequence[dict[str, Any]],
                        key_cols: Sequence[str]) -> int:
        """Read history, merge ``new_rows`` by ``key_cols``, write back.

        Merge rules
        -----------
        - ``key_cols``: identity columns (e.g. ``order_date``, ``customer_id``).
          A new row whose key already exists in history is merged into that
          bucket; otherwise it is appended as a new bucket.
        - Numeric non-key, non-derived columns (``orders`` / ``units`` /
          ``revenue`` / ``customers`` ...): accumulated.
        - Derived columns recomputed after the merge:
          * ``avg_order_value`` = ``revenue`` / ``orders``
          * ``revenue_share``  = ``revenue`` / ``total_revenue``
          * ``rank``           = dense rank by ``revenue`` desc
        - Other non-key columns (``tier`` / ``city`` / ``category`` /
          ``region`` / ``channel``): keep the new value if present, else the
          historical value.

        Returns the row count of the merged result.
        """
        history, _ = self.load_aggregate(name)
        key_set = set(key_cols)
        buckets: dict[tuple, dict[str, Any]] = {}
        order: list[tuple] = []

        def _key(row: dict[str, Any]) -> tuple:
            return tuple(row.get(k, "") for k in key_cols)

        for h in history:
            k = _key(h)
            if k not in buckets:
                buckets[k] = dict(h)
                order.append(k)
            else:
                self._merge_into(buckets[k], h, fields, key_set)

        for nr in new_rows:
            k = _key(nr)
            if k in buckets:
                self._merge_into(buckets[k], nr, fields, key_set)
            else:
                buckets[k] = dict(nr)
                order.append(k)

        merged = [buckets[k] for k in order]
        self._recompute_derived(merged, fields)
        self.save_aggregate(name, fields, merged)
        return len(merged)

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _is_numeric(v: Any) -> bool:
        if v is None:
            return False
        s = str(v).strip()
        if s == "" or s.lower() in {"null", "none", "nan"}:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False

    def _merge_into(self, base: dict[str, Any], new: dict[str, Any],
                    fields: Sequence[str], key_set: set) -> None:
        """Accumulate numeric cols of ``new`` into ``base``; keep non-numeric new value."""
        for f in fields:
            if f in key_set or f in _DERIVED_COLS:
                continue
            nv = new.get(f)
            bv = base.get(f)
            if self._is_numeric(nv) and self._is_numeric(bv):
                assert bv is not None and nv is not None
                total = float(bv) + float(nv)
                base[f] = int(total) if total.is_integer() else round(total, 4)
            elif nv is not None and str(nv).strip() != "":
                base[f] = nv

    @staticmethod
    def _recompute_derived(rows: list[dict[str, Any]],
                           fields: Sequence[str]) -> None:
        """Recompute avg_order_value / revenue_share / rank in place."""
        field_set = set(fields)
        if "avg_order_value" in field_set:
            for r in rows:
                orders = float(r.get("orders", 0) or 0)
                revenue = float(r.get("revenue", 0) or 0)
                r["avg_order_value"] = round(revenue / orders, 2) if orders else 0.0
        if "revenue_share" in field_set:
            total = sum(float(r.get("revenue", 0) or 0) for r in rows) or 1.0
            for r in rows:
                r["revenue_share"] = round(float(r.get("revenue", 0) or 0) / total, 4)
        if "rank" in field_set:
            indexed = sorted(range(len(rows)),
                             key=lambda i: -float(rows[i].get("revenue", 0) or 0))
            for rank, idx in enumerate(indexed, 1):
                rows[idx]["rank"] = rank
