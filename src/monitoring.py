"""运行时监控与告警（任务41）.

本模块为 AutoBatch 批处理流水线增加运行时监控与告警能力，向后兼容：
当 ``config/monitoring.json`` 的 ``enabled=false``（缺省）时，所有函数
均不启用，pipeline 行为 100% 不变.

提供三类能力：

1. **资源采样** — ``MetricsSampler.sample()`` 采集当前进程的 CPU% 和内存 MB.
   优先用 ``psutil``（若已安装）；不可用时降级到 ``resource.getrusage``
   （Unix）；Windows 上两者都不可用时返回 ``None`` 值.**psutil 不是硬依赖**.

2. **告警检查** — ``AlertChecker.check(metrics)`` 检查 metrics 是否超阈值，
   返回告警列表. ``check_alerts(run_dir, monitoring_cfg)`` 扫描最近 N 个批次
   的 ``metrics.json``，聚合告警.

3. **健康检查端点** — ``HealthServer`` 用 ``http.server`` 启动后台线程，
   ``GET /health`` 返回最近批次状态的 JSON.**仅用标准库**，不引入
   Flask/FastAPI.

设计参见 docs/evolution.md §6（监控告警）.Stdlib + 可选 psutil，无其他依赖.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional

from .helpers import json_load, utc_ts


# ---------------------------------------------------------------------------
# 资源采样
# ---------------------------------------------------------------------------
def _try_psutil() -> Optional[Any]:
    """lazy import psutil；不可用返回 None."""
    try:
        import psutil  # noqa: WPS433
        return psutil
    except ImportError:
        return None


def _try_resource_module() -> Optional[Any]:
    """lazy import resource（Unix-only）；不可用返回 None."""
    try:
        import resource  # noqa: WPS433
        return resource
    except ImportError:
        return None


class MetricsSampler:
    """采样当前进程的 CPU 和内存使用.

    优先用 ``psutil``（若已安装），可获取 CPU% 和 RSS 内存 MB；
    不可用时降级到 ``resource.getrusage(resource.RUSAGE_SELF)``（仅 Unix），
    可获取 RSS 内存 MB（CPU% 无法获取，返回 None）；
    Windows 上两者都不可用时返回 ``{"cpu_percent": None, "memory_mb": None}``.

    ``sample()`` 总是返回 dict（值可能为 None），调用方无需处理异常.
    """

    def __init__(self) -> None:
        self._psutil = _try_psutil()
        self._resource = _try_resource_module()

    def sample(self) -> dict[str, Optional[float]]:
        """返回当前进程的 CPU% 和内存 MB.

        Returns:
            dict with keys:
                cpu_percent: float | None — CPU 占用百分比（0-100）
                memory_mb:   float | None — RSS 内存占用 MB
        """
        # 优先 psutil
        if self._psutil is not None:
            try:
                proc = self._psutil.Process()
                cpu = proc.cpu_percent(interval=None)
                mem = proc.memory_info().rss / (1024.0 * 1024.0)
                return {"cpu_percent": float(cpu), "memory_mb": float(mem)}
            except Exception:  # noqa: BLE001
                # psutil 调用失败时降级
                pass

        # 降级到 resource 模块（Unix-only）
        if self._resource is not None:
            try:
                # ru_maxrss 单位：KB（Linux）/ bytes（macOS）.取 Linux 行为.
                usage = self._resource.getrusage(self._resource.RUSAGE_SELF)
                # macOS 上 ru_maxrss 是 bytes，Linux 是 KB.统一按 KB 处理
                # （近似值，仅用于告警阈值检查，精度不关键）.
                mem_mb = usage.ru_maxrss / 1024.0
                return {"cpu_percent": None, "memory_mb": float(mem_mb)}
            except Exception:  # noqa: BLE001
                pass

        # 完全降级：Windows 无 psutil 时返回 None 值
        return {"cpu_percent": None, "memory_mb": None}


# ---------------------------------------------------------------------------
# 告警检查
# ---------------------------------------------------------------------------
@dataclass
class Alert:
    """告警数据类.

    Attributes:
        severity:    "warning" / "critical"（当前统一 warning）.
        rule:        触发的规则名（如 "dq_score_min"）.
        message:     人类可读的告警描述.
        value:       实际观测值.
        threshold:   阈值.
        batch_id:    触发告警的批次 ID（可能为 None）.
        stage:       触发告警的 stage 名（仅 stage 级告警）.
    """

    severity: str
    rule: str
    message: str
    value: Any
    threshold: Any
    batch_id: Optional[str] = None
    stage: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "rule": self.rule,
            "message": self.message,
            "value": self.value,
            "threshold": self.threshold,
            "batch_id": self.batch_id,
            "stage": self.stage,
        }


class AlertChecker:
    """检查指标是否超阈值，返回告警列表.

    阈值 dict 字段（缺省值见 config/monitoring.json）：
        dq_score_min:              DQ Score 下限（0-100），低于则告警
        stage_duration_max_seconds: 单阶段耗时上限（秒），超过则告警
        memory_usage_max_mb:       内存占用上限（MB），超过则告警
        failure_rate_max_percent:  失败率上限（%），超过则告警
    """

    def __init__(self, thresholds: dict[str, Any]):
        self.thresholds = thresholds or {}

    def check(self, metrics: dict[str, Any]) -> list[Alert]:
        """检查单个批次的 metrics 是否超阈值，返回告警列表.

        Args:
            metrics: ``metrics.json`` 解析后的 dict，字段见 src/metrics.py.

        Returns:
            Alert 列表（可能为空）.
        """
        alerts: list[Alert] = []
        batch_id = metrics.get("batch_id")

        # 1. DQ Score 低于阈值
        dq_min = self.thresholds.get("dq_score_min")
        if dq_min is not None:
            dq_score = metrics.get("dq_score")
            if dq_score is not None:
                # metrics.json 中 dq_score 是 0-1 小数（如 0.9966），
                # 也可能是 0-100（如 99.66）.统一规范化为 0-100.
                dq_normalized = self._normalize_dq(dq_score)
                if dq_normalized < dq_min:
                    alerts.append(Alert(
                        severity="warning",
                        rule="dq_score_min",
                        message=f"DQ Score {dq_normalized} 低于阈值 {dq_min}",
                        value=dq_normalized,
                        threshold=dq_min,
                        batch_id=batch_id,
                    ))

        # 2. 单阶段耗时超阈值
        dur_max = self.thresholds.get("stage_duration_max_seconds")
        if dur_max is not None:
            for stage in metrics.get("stages", []) or []:
                dur_ms = stage.get("duration_ms")
                if dur_ms is None:
                    continue
                dur_sec = dur_ms / 1000.0
                if dur_sec > dur_max:
                    alerts.append(Alert(
                        severity="warning",
                        rule="stage_duration_max_seconds",
                        message="stage {} 耗时 {:.1f}s 超过阈值 {}s".format(
                            stage.get("name", "?"), dur_sec, dur_max),
                        value=dur_sec,
                        threshold=dur_max,
                        batch_id=batch_id,
                        stage=stage.get("name"),
                    ))

        # 3. 内存占用超阈值
        mem_max = self.thresholds.get("memory_usage_max_mb")
        if mem_max is not None:
            # 优先读 metrics.json 中追加的 resource_sample.memory_mb
            rs = metrics.get("resource_sample") or {}
            mem_mb = rs.get("memory_mb")
            if mem_mb is not None and mem_mb > mem_max:
                alerts.append(Alert(
                    severity="warning",
                    rule="memory_usage_max_mb",
                    message=f"内存占用 {mem_mb:.1f}MB 超过阈值 {mem_max}MB",
                    value=mem_mb,
                    threshold=mem_max,
                    batch_id=batch_id,
                ))

        # 4. 失败率超阈值（基于单批次 status）
        fail_max = self.thresholds.get("failure_rate_max_percent")
        if fail_max is not None:
            status = metrics.get("status")
            # 单批次失败率：failed=100%，success=0%.
            # 多批次聚合失败率由 check_alerts 在扫描多个批次时计算.
            if status == "failed":
                alerts.append(Alert(
                    severity="warning",
                    rule="failure_rate_max_percent",
                    message=f"批次状态为 failed（失败率 100% 超过阈值 {fail_max}%）",
                    value=100.0,
                    threshold=fail_max,
                    batch_id=batch_id,
                ))

        return alerts

    @staticmethod
    def _normalize_dq(dq_score: Any) -> float:
        """把 DQ Score 规范化为 0-100.

        metrics.json 中 dq_score 是 0-1 小数（如 0.9966），
        但阈值配置 dq_score_min 是 0-100（如 80.0）.
        若 dq_score <= 1.0，认为是 0-1 小数，乘 100.
        """
        v = float(dq_score)
        if 0.0 <= v <= 1.0:
            return v * 100.0
        return v


# ---------------------------------------------------------------------------
# 批次扫描与告警聚合
# ---------------------------------------------------------------------------
def _list_batch_dirs(run_dir: str) -> list[str]:
    """列出 run_dir 下所有批次目录（按 mtime 排序，最新在后）.

    批次目录判定：含 metrics.json 的子目录.
    """
    if not os.path.isdir(run_dir):
        return []
    candidates: list[tuple] = []  # (mtime, path)
    for name in os.listdir(run_dir):
        full = os.path.join(run_dir, name)
        if not os.path.isdir(full):
            continue
        if not os.path.isfile(os.path.join(full, "metrics.json")):
            continue
        try:
            mtime = os.path.getmtime(full)
        except OSError:
            mtime = 0.0
        candidates.append((mtime, full))
    candidates.sort(key=lambda x: x[0])
    return [p for _, p in candidates]


def _load_metrics(batch_dir: str) -> Optional[dict[str, Any]]:
    """加载 batch_dir/metrics.json；失败返回 None."""
    path = os.path.join(batch_dir, "metrics.json")
    try:
        return json_load(path)
    except (OSError, json.JSONDecodeError):
        return None


def check_alerts(run_dir: str, monitoring_cfg: dict[str, Any]) -> list[Alert]:
    """扫描最近 N 个批次的 metrics.json，检查告警.

    Args:
        run_dir:        run 根目录（含多个批次子目录）.
        monitoring_cfg: ``config/monitoring.json`` 解析后的 dict.

    Returns:
        Alert 列表（按批次顺序，可能为空）.
    """
    if not monitoring_cfg.get("enabled", False):
        return []

    window = int(monitoring_cfg.get("history_window", 10) or 10)
    thresholds = monitoring_cfg.get("alerts", {}) or {}
    checker = AlertChecker(thresholds)

    batch_dirs = _list_batch_dirs(run_dir)
    if not batch_dirs:
        return []

    # 取最近 N 个批次
    recent = batch_dirs[-window:] if window > 0 else batch_dirs

    all_alerts: list[Alert] = []
    for bd in recent:
        metrics = _load_metrics(bd)
        if metrics is None:
            continue
        all_alerts.extend(checker.check(metrics))

    # 多批次聚合：失败率超阈值
    fail_max = thresholds.get("failure_rate_max_percent")
    if fail_max is not None and recent:
        statuses: list[str] = []
        for bd in recent:
            m = _load_metrics(bd)
            if m is not None:
                statuses.append(m.get("status", ""))
        if statuses:
            failed = sum(1 for s in statuses if s == "failed")
            rate = failed / len(statuses) * 100.0
            if rate > fail_max and failed > 0:
                # 仅当聚合失败率超阈值时追加一条聚合告警；
                # 单批次失败告警已由 AlertChecker.check 产生.
                all_alerts.append(Alert(
                    severity="warning",
                    rule="failure_rate_max_percent",
                    message=f"最近 {len(statuses)} 个批次失败率 {rate:.1f}% 超过阈值 {fail_max}%",
                    value=rate,
                    threshold=fail_max,
                ))

    return all_alerts


# ---------------------------------------------------------------------------
# 健康检查端点
# ---------------------------------------------------------------------------
def _build_health_response(run_dir: str) -> dict[str, Any]:
    """构造 /health 响应：最近批次状态 + 聚合统计.

    Args:
        run_dir: run 根目录.

    Returns:
        dict with keys:
            status:     "ok" / "degraded"
            timestamp:  UTC ISO8601
            run_dir:    run 根目录
            batches:    最近批次列表（每项含 batch_id/status/dq_score/...）
            summary:    聚合统计（total/success/failed/failure_rate）
    """
    batch_dirs = _list_batch_dirs(run_dir)
    recent = batch_dirs[-10:] if batch_dirs else []
    batches: list[dict[str, Any]] = []
    for bd in recent:
        m = _load_metrics(bd)
        if m is None:
            continue
        batches.append({
            "batch_id": m.get("batch_id"),
            "status": m.get("status"),
            "started_at": m.get("started_at"),
            "finished_at": m.get("finished_at"),
            "total_duration_ms": m.get("total_duration_ms"),
            "dq_score": m.get("dq_score"),
        })

    total = len(batches)
    success = sum(1 for b in batches if b.get("status") == "success")
    failed = sum(1 for b in batches if b.get("status") == "failed")
    rate = (failed / total * 100.0) if total else 0.0

    return {
        "status": "ok" if failed == 0 else "degraded",
        "timestamp": utc_ts(),
        "run_dir": run_dir,
        "batches": batches,
        "summary": {
            "total": total,
            "success": success,
            "failed": failed,
            "failure_rate_percent": round(rate, 2),
        },
    }


class HealthServer:
    """简易 HTTP 健康检查服务器（后台线程）.

    用 ``http.server.ThreadingHTTPServer`` 启动后台线程，``GET /health``
    返回最近批次状态的 JSON.**仅用标准库**，不引入 Flask/FastAPI.

    Usage:
        server = HealthServer(host="0.0.0.0", port=8086, run_dir="/path/to/run")
        server.start()   # 后台线程启动
        ...
        server.stop()    # 停止服务器

    Attributes:
        host:    监听地址.
        port:    监听端口.
        run_dir: run 根目录（用于扫描批次状态）.
    """

    def __init__(self, host: str, port: int, run_dir: str) -> None:
        self.host = host
        self.port = int(port)
        self.run_dir = run_dir
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """启动后台线程运行 HTTP 服务器.

        幂等：若已启动则不重复启动.
        """
        if self._server is not None:
            return

        server_ref = self  # 闭包引用

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                # 静默访问日志，避免污染 stdout
                pass

            def do_GET(self):  # noqa: N802
                if self.path == "/health":
                    try:
                        body = _build_health_response(server_ref.run_dir)
                        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json; charset=utf-8")
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                    except Exception:  # noqa: BLE001
                        self.send_response(500)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(b'{"status":"error"}')
                else:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"error":"not found"}')

        try:
            self._server = ThreadingHTTPServer((self.host, self.port), _Handler)
            self._server.daemon_threads = True
        except OSError:
            # 端口被占用等：标记未启动，start 失败但不抛
            self._server = None
            return

        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="autobatch-health-server",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """停止服务器并清理后台线程.

        幂等：若未启动则 no-op.
        """
        if self._server is not None:
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception:  # noqa: BLE001
                pass
            self._server = None
        if self._thread is not None:
            # daemon 线程，join 短超时避免阻塞
            try:
                self._thread.join(timeout=2.0)
            except Exception:  # noqa: BLE001
                pass
            self._thread = None

    def is_running(self) -> bool:
        """服务器是否在运行."""
        return self._server is not None and self._thread is not None and self._thread.is_alive()


# ---------------------------------------------------------------------------
# 配置加载辅助
# ---------------------------------------------------------------------------
def load_monitoring_config(path: str) -> dict[str, Any]:
    """加载 monitoring.json；文件不存在时返回 disabled 缺省配置.

    Args:
        path: monitoring.json 路径.

    Returns:
        配置 dict（至少含 enabled=False）.
    """
    default = {
        "enabled": False,
        "alerts": {},
        "health_check": {"enabled": False, "port": 8086, "host": "0.0.0.0"},
        "history_window": 10,
    }
    if not os.path.isfile(path):
        return default
    try:
        cfg = json_load(path)
        if not isinstance(cfg, dict):
            return default
        return cfg
    except (OSError, json.JSONDecodeError):
        return default
