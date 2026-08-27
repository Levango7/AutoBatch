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

Crash-safe commit protocol (task #74 C4, "staging ledger"):
- ``merge_aggregate_staged`` computes the full merged result and writes it to
  ``state/aggregates_pending/{name}.csv`` WITHOUT touching official files.
- ``commit_batch`` is the single atomic commit point: one state.json save that
  promotes staged watermarks + Iceberg snapshot ids, registers the batch_id in
  the ``merged_batches`` ledger, and records the ``aggregates_pending`` marker.
- ``complete_pending_aggregates`` then os.replaces the staged files into
  ``state/aggregates/``. If the process crashes between the commit point and
  the replacement, the next startup sees the pending marker and finishes the
  replacement (the staged file is the complete merged result, so replacing is
  idempotent). A batch_id already in the ledger is never merged twice.

Concurrency (task #74 M16):
- All mutating operations run inside a cross-process file lock
  (``<state_dir>/.state.lock``; msvcrt.locking on Windows, fcntl.flock on
  POSIX; stdlib only). The lock is reentrant per thread, so nested calls
  (commit_* → save) do not self-deadlock. Acquisition failure after a timeout
  raises ``TimeoutError`` with a diagnostic message.

Zero dependencies: only stdlib (json / csv / os / time / threading / shutil).
"""

from __future__ import annotations

import csv
import json
import logging
import math
import os
import shutil
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Optional

from .helpers import utc_ts

# Derived columns that must be recomputed after merge rather than accumulated.
_DERIVED_COLS = {"avg_order_value", "revenue_share", "rank"}

# Dimension (non-numeric, non-derived, non-key) columns: string overwrite is
# allowed here; numeric columns reject non-numeric overwrites (task #74 minor).
_DIMENSION_COLS = {"tier", "city", "category", "region", "channel"}

# Directory (under state_dir) holding staged merge results awaiting commit.
_PENDING_DIR_NAME = "aggregates_pending"

# merged_batches ledger is bounded to the most recent N ids to keep state.json
# small; dedup only needs to cover re-arrivals of the same batch (resume),
# which always happens shortly after the original commit.
_MAX_MERGED_BATCH_RECORDS = 200

# Default cross-process lock acquisition timeout (seconds).
_DEFAULT_LOCK_TIMEOUT = 10.0

# Per-thread registry of currently held locks (lock_path -> depth). Makes the
# file lock reentrant within a single thread (e.g. commit_watermark -> save),
# while distinct threads/processes still serialize via the OS lock.
_THREAD_HELD = threading.local()


def _try_float(v: Any) -> Optional[float]:
    """Parse ``v`` as float; return None (instead of raising) on bad input.

    ``None`` / empty / 'null' / 'none' / 'nan' strings are treated as missing
    (None) so callers can distinguish "no value" from "literal 0".
    """
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"null", "none", "nan"}:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def recompute_derived(rows: list[dict[str, Any]], fields: Sequence[str]) -> None:
    """Recompute avg_order_value / revenue_share / rank in place.

    Engine-agnostic (task #74 M12): shared by the python merge path
    (``StateStore.merge_aggregate``) and the Spark merge path
    (``pipeline._merge_aggregate_spark`` collects merged rows to the driver
    and calls this function so both engines produce identical derived values).

    Defensive behaviour (task #74 minor): instead of crashing with ValueError
    on non-numeric ``orders``/``revenue``, log a warning and keep the affected
    row's original derived value; the global revenue total ignores bad rows
    (falls back to 1.0 when nothing numeric remains); rank treats bad revenue
    as 0.
    """
    log = logging.getLogger(__name__)
    field_set = set(fields)
    if "avg_order_value" in field_set:
        for r in rows:
            orders = _try_float(r.get("orders"))
            revenue = _try_float(r.get("revenue"))
            if orders is None or revenue is None:
                log.warning(
                    "non-numeric orders/revenue (%r/%r) in aggregate row; "
                    "keeping original avg_order_value %r",
                    r.get("orders"),
                    r.get("revenue"),
                    r.get("avg_order_value"),
                )
                continue
            r["avg_order_value"] = round(revenue / orders, 2) if orders else 0.0
    if "revenue_share" in field_set:
        parsed: list[Optional[float]] = []
        for r in rows:
            v = _try_float(r.get("revenue"))
            if v is None:
                log.warning(
                    "non-numeric revenue %r in aggregate row; keeping original revenue_share %r",
                    r.get("revenue"),
                    r.get("revenue_share"),
                )
            parsed.append(v)
        total = sum(v for v in parsed if v is not None) or 1.0
        # parsed 与 rows 一一对应构建，长度恒等（strict=True 显式声明该不变量）
        for r, v in zip(rows, parsed, strict=True):
            if v is None:
                continue  # keep original value
            r["revenue_share"] = round(v / total, 4)
    if "rank" in field_set:

        def _rank_key(i: int) -> float:
            v = _try_float(rows[i].get("revenue"))
            if v is None:
                log.warning(
                    "non-numeric revenue %r while ranking; treated as 0", rows[i].get("revenue")
                )
                return 0.0
            return -v

        indexed = sorted(range(len(rows)), key=_rank_key)
        for rank, idx in enumerate(indexed, 1):
            rows[idx]["rank"] = rank


def _write_csv_atomic(path: str, fields: Sequence[str], rows: Sequence[dict[str, Any]]) -> None:
    """Write CSV via tmp file + os.replace so a crash never leaves a torn target.

    Task #74 M15: the previous direct-write could leave a half-written CSV that
    the next run would happily parse.
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp, path)


def _acquire_file_lock(fh: Any, lock_path: str, timeout: float) -> None:
    """Non-blocking retry loop acquiring the cross-process lock on ``fh``.

    Windows: msvcrt.locking (byte-range lock at offset 0); POSIX: fcntl.flock.
    Raises TimeoutError with a diagnostic message after ``timeout`` seconds.
    ``sys.platform`` guards (vs os.name) are recognized by mypy so each
    platform only type-checks its own branch.
    """
    deadline = time.monotonic() + max(timeout, 0.0)
    while True:
        try:
            if sys.platform == "win32":
                import msvcrt

                # msvcrt.locking locks nbytes from the CURRENT file offset.
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except OSError as exc:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"could not acquire state lock {lock_path!r} within {timeout:.1f}s; "
                    "another pipeline process is likely writing the same state_dir "
                    f"(last OS error: {exc})"
                ) from exc
            time.sleep(0.05)


def _release_file_lock(fh: Any) -> None:
    try:
        if sys.platform == "win32":
            import msvcrt

            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except OSError:
        # Closing the handle also releases the lock; swallow unlock errors on
        # the teardown path so they never mask the original exception.
        pass


class StateStore:
    """Persistent cross-batch state for incremental processing.

    Parameters
    ----------
    state_dir:
        Absolute (or project-relative) path to the state directory. The store
        reads/writes ``<state_dir>/state.json`` and ``<state_dir>/aggregates/``.
    lock_timeout:
        Seconds to wait for the cross-process state lock before raising
        TimeoutError (default 10).
    """

    def __init__(self, state_dir: str, lock_timeout: float = _DEFAULT_LOCK_TIMEOUT) -> None:
        self.state_dir = os.path.abspath(state_dir)
        self.state_path = os.path.join(self.state_dir, "state.json")
        self.lock_path = os.path.join(self.state_dir, ".state.lock")
        self.lock_timeout = lock_timeout

    # ------------------------------------------------------------------
    # cross-process file lock (task #74 M16)
    # ------------------------------------------------------------------
    @contextmanager
    def locked(self, timeout: Optional[float] = None) -> Iterator[None]:
        """Cross-process lock guarding every mutation under ``state_dir``.

        Reentrant within the same thread (depth-counted per lock path), so
        nested calls such as ``commit_watermark`` -> ``save`` do not
        deadlock. Distinct threads (same process) and distinct processes
        serialize through the OS lock; acquisition failure after the timeout
        raises ``TimeoutError`` with a clear message.
        """
        held: dict[str, int] = getattr(_THREAD_HELD, "held", None) or {}
        _THREAD_HELD.held = held
        if self.lock_path in held:
            held[self.lock_path] += 1
            try:
                yield
            finally:
                held[self.lock_path] -= 1
            return

        os.makedirs(self.state_dir, exist_ok=True)
        fh = open(self.lock_path, "a+b")
        try:
            _acquire_file_lock(
                fh, self.lock_path, self.lock_timeout if timeout is None else timeout
            )
            held[self.lock_path] = 1
            try:
                yield
            finally:
                held[self.lock_path] -= 1
                _release_file_lock(fh)
        finally:
            fh.close()

    # ------------------------------------------------------------------
    # state.json load / save
    # ------------------------------------------------------------------
    def load(self) -> dict[str, Any]:
        """Load state.json; return an empty skeleton if the file is absent.

        Raises RuntimeError with recovery guidance if the file exists but is
        corrupted — silently starting from an empty state would make the next
        run a full re-read AND re-accumulate aggregates (double counting), so
        this is deliberately a loud failure requiring manual recovery.
        """
        if os.path.exists(self.state_path):
            with open(self.state_path, encoding="utf-8-sig") as f:
                try:
                    state = json.load(f)
                except json.JSONDecodeError as e:
                    raise RuntimeError(
                        f"state file {self.state_path!r} is corrupted ({e}). "
                        "增量水位/聚合状态不可读。请人工修复该文件或从备份恢复；"
                        "注意：直接删除会令下一批次按全量重读并对 state/aggregates "
                        "重复累加（跨批翻倍），删除前必须同时清空 state/aggregates/ "
                        "并接受聚合从零重建。"
                    ) from e
            state.setdefault("version", "1.0")
            state.setdefault("tables", {})
            state.setdefault("aggregates", {})
            # Phase 4: Iceberg snapshot id 持久化（与 watermark_value 并存）
            state.setdefault("iceberg_snapshots", {})
            # Task #74 C4: batch merge ledger (ids of committed batches).
            state.setdefault("merged_batches", [])
            return state
        return {
            "version": "1.0",
            "tables": {},
            "aggregates": {},
            "iceberg_snapshots": {},
            "merged_batches": [],
        }

    def save(self, state: dict[str, Any]) -> None:
        """Persist state.json atomically (write then os.replace for crash safety)."""
        with self.locked():
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

    def set_new_watermark(
        self, state: dict[str, Any], table: str, value: Optional[str], row_count: int, batch_id: str
    ) -> None:
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
                info["cumulative_row_count"] = info.get("cumulative_row_count", 0) + seen
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

    def commit_all(self, state: dict[str, Any], batch_id: str) -> None:
        """Atomically promote staged watermarks **and** Iceberg snapshot ids.

        把 ``commit_watermark`` 与 ``commit_snapshot_id`` 合并为单次 ``state.save``
        调用，避免原两步提交中间失败导致 watermark 已提升但 snapshot id 未提升
        （或反之）的不一致。下游增量读以 (watermark, snapshot_id) 联合定位，
        任一者领先另一者都会导致漏读或重读。

        行为等价于 ``commit_snapshot_id(state, batch_id)`` +
        ``commit_watermark(state, batch_id)``，但只持久化一次。

        Args:
            state:    in-memory state dict（由调用方持有，本方法就地修改）。
            batch_id: 当前批次 ID，写入 ``last_batch_id`` /
                      ``last_processed_at`` 用于追溯。
        """
        now = utc_ts()
        # 1. 提升 Iceberg snapshot id（若有 staged）
        for _name, info in state.get("iceberg_snapshots", {}).items():
            if "new_snapshot_id" in info:
                info["snapshot_id"] = info.pop("new_snapshot_id")
                info.pop("new_batch_id", None)
                info["last_batch_id"] = batch_id
                info["last_processed_at"] = now
        # 2. 提升 watermark（若有 staged）
        for _name, info in state.get("tables", {}).items():
            if "new_watermark" in info:
                info["watermark_value"] = info.pop("new_watermark")
                seen = info.pop("new_seen_row_count", 0)
                info["last_seen_row_count"] = seen
                info["cumulative_row_count"] = info.get("cumulative_row_count", 0) + seen
                info.pop("new_batch_id", None)
                info["last_batch_id"] = batch_id
                info["last_processed_at"] = now
        state["updated_at"] = now
        state["last_batch_id"] = batch_id
        # 3. 单次原子持久化
        self.save(state)

    # ------------------------------------------------------------------
    # batch commit point + merge ledger (task #74 C4)
    # ------------------------------------------------------------------
    def is_batch_merged(self, state: dict[str, Any], batch_id: str) -> bool:
        """True if ``batch_id`` is already registered in the merged_batches ledger.

        A registered id means the batch's aggregates and watermarks were fully
        committed; re-arrivals (resume after a crash past the commit point)
        must skip merging entirely to stay idempotent.
        """
        return batch_id in (state.get("merged_batches") or [])

    def commit_batch(
        self,
        state: dict[str, Any],
        batch_id: str,
        pending_aggregates: Optional[dict[str, str]] = None,
    ) -> None:
        """Single atomic commit point (task #74 C4).

        One atomic state.json save performs ALL of:
        1. promote staged watermarks (``new_watermark`` -> ``watermark_value``,
           accumulating ``cumulative_row_count``);
        2. promote staged Iceberg snapshot ids (``new_snapshot_id`` ->
           ``snapshot_id``) — superset of ``commit_all``;
        3. register ``batch_id`` in the ``merged_batches`` ledger (idempotent,
           capped at the most recent ``_MAX_MERGED_BATCH_RECORDS`` ids);
        4. record the ``aggregates_pending`` marker (name -> staged file path)
           when staged merge results await replacement.

        Crash semantics: crash BEFORE this point -> watermarks/ledger untouched,
        re-run recomputes and overwrites the staged files; crash AFTER this
        point -> the next startup's ``complete_pending_aggregates`` finishes the
        replacement based on the marker, and the ledger prevents double-merging
        the same batch.
        """
        now = utc_ts()
        for _name, info in state.get("iceberg_snapshots", {}).items():
            if "new_snapshot_id" in info:
                info["snapshot_id"] = info.pop("new_snapshot_id")
                info.pop("new_batch_id", None)
                info["last_batch_id"] = batch_id
                info["last_processed_at"] = now
        for _name, info in state.get("tables", {}).items():
            if "new_watermark" in info:
                info["watermark_value"] = info.pop("new_watermark")
                seen = info.pop("new_seen_row_count", 0)
                info["last_seen_row_count"] = seen
                info["cumulative_row_count"] = info.get("cumulative_row_count", 0) + seen
                info.pop("new_batch_id", None)
                info["last_batch_id"] = batch_id
                info["last_processed_at"] = now
        state["updated_at"] = now
        state["last_batch_id"] = batch_id
        ledger = state.setdefault("merged_batches", [])
        if batch_id not in ledger:
            ledger.append(batch_id)
        overflow = len(ledger) - _MAX_MERGED_BATCH_RECORDS
        if overflow > 0:
            del ledger[:overflow]
        if pending_aggregates:
            state["aggregates_pending"] = {
                name: os.path.abspath(path) for name, path in pending_aggregates.items()
            }
        else:
            # Defensive: a stale marker (already completed at startup) must not
            # survive a fresh commit point.
            state.pop("aggregates_pending", None)
        self.save(state)

    def get_pending_dir(self) -> str:
        """Absolute path of ``<state_dir>/aggregates_pending/``."""
        return os.path.join(self.state_dir, _PENDING_DIR_NAME)

    def get_pending_aggregate_path(self, name: str) -> str:
        """Absolute path of the staged (pending) aggregate ``{name}.csv``."""
        return os.path.join(self.get_pending_dir(), name + ".csv")

    def write_pending_aggregate(
        self, name: str, fields: Sequence[str], rows: Sequence[dict[str, Any]]
    ) -> str:
        """Write merged rows to the pending staged file (atomic); return its path.

        Used by the Spark merge path, which computes the merged result itself
        (distributed groupBy, then driver-side ``recompute_derived``) and hands
        the final rows to the same staging protocol as the python path. Only
        valid for local filesystem storage — S3 targets cannot be atomically
        replaced and keep the legacy direct write (see pipeline comments).
        """
        with self.locked():
            path = self.get_pending_aggregate_path(name)
            _write_csv_atomic(path, fields, rows)
            return path

    def complete_pending_aggregates(self, state: dict[str, Any]) -> list[str]:
        """Crash-recovery: finish replacing staged aggregates into official paths.

        Called at pipeline startup (after ``load``) and right after
        ``commit_batch``. Idempotent: a missing staged file means the
        replacement already happened during an earlier recovery. Returns the
        list of aggregate names processed.
        """
        pending = state.get("aggregates_pending") or {}
        if not pending:
            return []
        with self.locked():
            for name, staged_path in list(pending.items()):
                target = self.get_aggregate_path(name)
                if os.path.isdir(staged_path):
                    # Spark CSV output is a directory of part files.
                    if os.path.isdir(target):
                        shutil.rmtree(target, ignore_errors=True)
                    elif os.path.exists(target):
                        os.remove(target)
                    shutil.move(staged_path, target)
                elif os.path.exists(staged_path):
                    parent = os.path.dirname(target)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    os.replace(staged_path, target)
                # else: staged file absent -> replacement already completed.
            state.pop("aggregates_pending", None)
            state["updated_at"] = utc_ts()
            self.save(state)
            return list(pending.keys())

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

    def save_aggregate(
        self, name: str, fields: Sequence[str], rows: Sequence[dict[str, Any]]
    ) -> None:
        """Write historical aggregate to ``<state_dir>/aggregates/{name}.csv``.

        Atomic (tmp + os.replace, task #74 M15) and serialized by the state
        lock (task #74 M16): a crash mid-write can never leave a torn CSV that
        the next run would parse.
        """
        with self.locked():
            _write_csv_atomic(self.get_aggregate_path(name), fields, rows)

    def merge_aggregate(
        self,
        name: str,
        fields: Sequence[str],
        new_rows: Sequence[dict[str, Any]],
        key_cols: Sequence[str],
    ) -> int:
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
          * ``rank``           = 顺序排名（revenue desc；并列值按排序稳定性取不同名次）
        - Other non-key columns (``tier`` / ``city`` / ``category`` /
          ``region`` / ``channel``): keep the new value if present, else the
          historical value. Non-numeric values are refused for numeric columns
          (warning logged, historical value kept).

        Returns the row count of the merged result.
        """
        with self.locked():
            history, _ = self.load_aggregate(name)
            merged = self._compute_merged_rows(history, new_rows, fields, key_cols)
            self.save_aggregate(name, fields, merged)
            return len(merged)

    def merge_aggregate_staged(
        self,
        name: str,
        fields: Sequence[str],
        new_rows: Sequence[dict[str, Any]],
        key_cols: Sequence[str],
    ) -> tuple[int, str]:
        """Compute the merge and write it to the pending staged file only.

        Task #74 C4: the official aggregate is NOT touched here. The caller
        passes the returned staged path to ``commit_batch`` and then finishes
        with ``complete_pending_aggregates``. Returns ``(row_count, staged_path)``.
        """
        with self.locked():
            history, _ = self.load_aggregate(name)
            merged = self._compute_merged_rows(history, new_rows, fields, key_cols)
            path = self.get_pending_aggregate_path(name)
            _write_csv_atomic(path, fields, merged)
            return len(merged), path

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------
    def _compute_merged_rows(
        self,
        history: Sequence[dict[str, Any]],
        new_rows: Sequence[dict[str, Any]],
        fields: Sequence[str],
        key_cols: Sequence[str],
    ) -> list[dict[str, Any]]:
        """Pure merge computation: bucket by key, accumulate, recompute derived."""
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
        recompute_derived(merged, fields)
        return merged

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

    def _merge_into(
        self, base: dict[str, Any], new: dict[str, Any], fields: Sequence[str], key_set: set
    ) -> None:
        """Accumulate numeric cols of ``new`` into ``base``; keep non-numeric new value.

        Task #74 (minor) type protection: dimension columns may be overwritten
        by strings, but a NON-numeric value is refused for a column whose
        historical value is numeric (warning logged, historical value kept),
        so a single dirty delta row cannot destroy accumulated numbers.
        """
        log = logging.getLogger(__name__)
        for f in fields:
            if f in key_set or f in _DERIVED_COLS:
                continue
            nv = new.get(f)
            bv = base.get(f)
            if self._is_numeric(nv) and self._is_numeric(bv):
                # _is_numeric 已排除 None/非数值；显式窄化让静态检查器可见
                if bv is None or nv is None:  # pragma: no cover
                    continue
                total = float(bv) + float(nv)
                # inf/nan 等非有限值会让 round() 抛 OverflowError——跳过并告警，
                # 不让单列脏数据炸掉整个增量合并
                if not math.isfinite(total):
                    log.warning(
                        "skipping non-finite accumulation for column %r: %s + %s", f, bv, nv
                    )
                    continue
                base[f] = int(total) if total.is_integer() else round(total, 4)
            elif nv is not None and str(nv).strip() != "":
                if f in _DIMENSION_COLS or not self._is_numeric(bv):
                    base[f] = nv
                else:
                    log.warning(
                        "refusing non-numeric overwrite for numeric column %r: new=%r, keeping %r",
                        f,
                        nv,
                        bv,
                    )

    @staticmethod
    def _recompute_derived(rows: list[dict[str, Any]], fields: Sequence[str]) -> None:
        """Backward-compatible wrapper around module-level ``recompute_derived``."""
        recompute_derived(rows, fields)
