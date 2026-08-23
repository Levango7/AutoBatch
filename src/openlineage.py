"""OpenLineage 标准血缘事件发射（零依赖实现）.

将每次 pipeline 批次与各 stage 的执行以 OpenLineage v1 RunEvent 格式输出：

1. NDJSON 文件：``run/<batch_id>/openlineage.ndjson``，每行一个事件；
2. 可选 HTTP 端点（如 Marquez）：``openlineage.endpoint`` 配置后 POST JSON，
   失败仅记 warning，不影响主流程。

设计约定：
- pipeline 整批为一个父 Run（job 名 ``<namespace>.pipeline``），每个 stage 为子
  Run（job 名 ``<namespace>.<stage>``），子事件通过 ``parent`` runFacet 关联父 Run；
- runId 用 ``uuid5`` 从 batch_id/stage 确定性派生——同一批次重跑产生相同 runId，
  便于下游按幂等去重；
- 默认关闭（``openlineage.enabled=false``），启用后也不改变 pipeline 的任何
  计算行为；发射路径上的任何异常都被吞掉并记日志。

事件结构遵循 https://openlineage.io/spec/ 的 RunEvent 定义（必需字段：
eventTime/eventType/run/job/producer/schemaVersion，inputs/outputs 为数据集引用）。
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

PRODUCER = "https://github.com/Levango7/AutoBatch"
SCHEMA_VERSION = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"

# 各 stage 的逻辑输入/输出数据集名（与 run/<batch>/ 下目录对应）
STAGE_IO: dict[str, tuple[list[str], list[str]]] = {
    "ingest": (["source"], ["01_raw"]),
    "validate": (["01_raw"], ["02_valid"]),
    "clean": (["02_valid"], ["03_clean"]),
    "compute": (["03_clean"], ["04_aggregates"]),
    "output": (["03_clean", "04_aggregates"], ["05_output"]),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _stable_runid(batch_id: str, key: str = "") -> str:
    """确定性 runId：同批次同 stage 重跑得到相同 UUID（下游可幂等去重）。"""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCER}:{batch_id}:{key}"))


def _dataset(namespace: str, name: str, batch_id: str) -> dict[str, Any]:
    return {"namespace": namespace, "name": f"{batch_id}/{name}"}


def _event(
    event_type: str,
    namespace: str,
    job_name: str,
    run_id: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    parent: Optional[dict[str, Any]] = None,
    error_message: Optional[str] = None,
) -> dict[str, Any]:
    ev: dict[str, Any] = {
        "eventTime": _now_iso(),
        "eventType": event_type,
        "schemaVersion": SCHEMA_VERSION,
        "producer": PRODUCER,
        "job": {"namespace": namespace, "name": job_name},
        "run": {"runId": run_id},
        "inputs": inputs,
        "outputs": outputs,
    }
    facets: dict[str, Any] = {}
    if parent is not None:
        facets["parent"] = {
            "_producer": PRODUCER,
            "_schemaURL": SCHEMA_VERSION,
            **parent,
        }
    if error_message:
        facets["error"] = {
            "_producer": PRODUCER,
            "_schemaURL": SCHEMA_VERSION,
            "message": error_message[:500],
        }
    if facets:
        ev["runFacets"] = facets
    return ev


class OpenLineageEmitter:
    """批次级 OpenLineage 事件收集器：写 NDJSON + 可选 HTTP 上报."""

    def __init__(
        self,
        batch_id: str,
        namespace: str = "autobatch",
        endpoint: str = "",
        out_path: Optional[str] = None,
        logger: Optional[Any] = None,
    ):
        self.batch_id = batch_id
        self.namespace = namespace
        self.endpoint = endpoint.strip()
        self.out_path = out_path
        self.logger = logger
        # 父 Run：整条 pipeline 一个 runId，stage 事件的 parent facet 指向它
        self.pipeline_run_id = _stable_runid(batch_id, "pipeline")

    # ------------------------------------------------------------------
    # 公开 API（全部吞异常，绝不影响 pipeline 主流程）
    # ------------------------------------------------------------------
    def pipeline_event(self, event_type: str, error_message: Optional[str] = None) -> None:
        ev = _event(
            event_type,
            self.namespace,
            f"{self.namespace}.pipeline",
            self.pipeline_run_id,
            [],
            [],
            error_message=error_message,
        )
        self._dispatch(ev)

    def stage_event(
        self,
        stage: str,
        event_type: str,
        error_message: Optional[str] = None,
    ) -> None:
        ins, outs = STAGE_IO.get(stage, ([], []))
        ev = _event(
            event_type,
            self.namespace,
            f"{self.namespace}.{stage}",
            _stable_runid(self.batch_id, stage),
            [_dataset(self.namespace, n, self.batch_id) for n in ins],
            [_dataset(self.namespace, n, self.batch_id) for n in outs],
            parent={
                "run": {"runId": self.pipeline_run_id},
                "job": {"namespace": self.namespace, "name": f"{self.namespace}.pipeline"},
            },
            error_message=error_message,
        )
        self._dispatch(ev)

    # ------------------------------------------------------------------
    def _dispatch(self, ev: dict[str, Any]) -> None:
        try:
            line = json.dumps(ev, ensure_ascii=False)
            if self.out_path:
                os.makedirs(os.path.dirname(self.out_path), exist_ok=True)
                with open(self.out_path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            if self.endpoint:
                self._post(ev)
        except Exception as exc:  # noqa: BLE001
            if self.logger is not None:
                self.logger.warning(
                    "openlineage emit failed, ignoring",
                    extra={"stage": "pipeline", "error": f"{type(exc).__name__}: {exc}"},
                )

    def _post(self, ev: dict[str, Any]) -> None:
        req = urllib.request.Request(
            self.endpoint,
            data=json.dumps(ev).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:  # noqa: S310
                resp.read()
        except urllib.error.HTTPError as exc:
            exc.close()
            raise
