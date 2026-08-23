"""任务41 监控告警测试.

覆盖：
  1. MetricsSampler.sample() 返回 dict（值可能为 None）
  2. AlertChecker.check() 正确检测 DQ Score 低于阈值
  3. AlertChecker.check() 正确检测 stage duration 超阈值
  4. AlertChecker.check() 无超阈值时返回空列表
  5. check_alerts() 扫描多个批次返回正确告警
  6. HealthServer start/stop（启动后可访问 /health，返回 JSON）
  7. monitoring.enabled=false 时不启用任何监控
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
import urllib.request
import uuid

import pytest

from src.helpers import ROOT, abs_path, json_save
from src.monitoring import (
    Alert,
    AlertChecker,
    HealthServer,
    MetricsSampler,
    check_alerts,
    load_monitoring_config,
)


# ---------------------------------------------------------------------------
# 1. MetricsSampler
# ---------------------------------------------------------------------------
class TestMetricsSampler:
    def test_sample_returns_dict(self):
        """sample() 总是返回 dict，含 cpu_percent / memory_mb 键（值可能 None）."""
        sampler = MetricsSampler()
        result = sampler.sample()
        assert isinstance(result, dict)
        assert "cpu_percent" in result
        assert "memory_mb" in result

    def test_sample_values_are_float_or_none(self):
        """sample() 的值是 float 或 None."""
        sampler = MetricsSampler()
        result = sampler.sample()
        for key in ("cpu_percent", "memory_mb"):
            v = result[key]
            assert v is None or isinstance(v, float), (
                f"{key} 应为 float 或 None，实际 {type(v)}: {v}"
            )

    def test_sample_does_not_raise(self):
        """sample() 不应抛异常（即使 psutil 不可用）."""
        sampler = MetricsSampler()
        # 多次调用确保稳定
        for _ in range(5):
            result = sampler.sample()
            assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 2/3/4. AlertChecker
# ---------------------------------------------------------------------------
class TestAlertChecker:
    def _base_metrics(self, **overrides):
        m = {
            "batch_id": "B-test-001",
            "status": "success",
            "dq_score": 0.95,  # 0-1 小数，规范化后 95.0
            "total_duration_ms": 1000,
            "stages": [
                {
                    "name": "ingest",
                    "status": "success",
                    "duration_ms": 100,
                    "rows_in": 0,
                    "rows_out": 100,
                },
                {
                    "name": "validate",
                    "status": "success",
                    "duration_ms": 200,
                    "rows_in": 100,
                    "rows_out": 95,
                },
            ],
        }
        m.update(overrides)
        return m

    def test_dq_score_below_threshold(self):
        """DQ Score 低于阈值时产生告警."""
        thresholds = {"dq_score_min": 80.0}
        checker = AlertChecker(thresholds)
        # dq_score=0.75 → 规范化 75.0 < 80.0
        metrics = self._base_metrics(dq_score=0.75)
        alerts = checker.check(metrics)
        dq_alerts = [a for a in alerts if a.rule == "dq_score_min"]
        assert len(dq_alerts) == 1
        assert dq_alerts[0].value == 75.0
        assert dq_alerts[0].threshold == 80.0

    def test_dq_score_above_threshold(self):
        """DQ Score 高于阈值时不产生告警."""
        thresholds = {"dq_score_min": 80.0}
        checker = AlertChecker(thresholds)
        metrics = self._base_metrics(dq_score=0.95)  # 95.0 > 80.0
        alerts = checker.check(metrics)
        assert all(a.rule != "dq_score_min" for a in alerts)

    def test_dq_score_already_0_100_scale(self):
        """DQ Score 已是 0-100 范围（如 99.0）时直接比较."""
        thresholds = {"dq_score_min": 80.0}
        checker = AlertChecker(thresholds)
        metrics = self._base_metrics(dq_score=99.0)
        alerts = checker.check(metrics)
        assert all(a.rule != "dq_score_min" for a in alerts)

        metrics_low = self._base_metrics(dq_score=70.0)
        alerts = checker.check(metrics_low)
        dq_alerts = [a for a in alerts if a.rule == "dq_score_min"]
        assert len(dq_alerts) == 1

    def test_stage_duration_exceeds_threshold(self):
        """stage duration 超阈值时产生告警."""
        thresholds = {"stage_duration_max_seconds": 0.5}  # 500ms
        checker = AlertChecker(thresholds)
        # validate stage duration_ms=2000 → 2.0s > 0.5s
        metrics = self._base_metrics(
            stages=[
                {
                    "name": "ingest",
                    "status": "success",
                    "duration_ms": 100,
                    "rows_in": 0,
                    "rows_out": 100,
                },
                {
                    "name": "validate",
                    "status": "success",
                    "duration_ms": 2000,
                    "rows_in": 100,
                    "rows_out": 95,
                },
            ]
        )
        alerts = checker.check(metrics)
        dur_alerts = [a for a in alerts if a.rule == "stage_duration_max_seconds"]
        assert len(dur_alerts) == 1
        assert dur_alerts[0].stage == "validate"
        assert dur_alerts[0].value == 2.0

    def test_stage_duration_within_threshold(self):
        """stage duration 未超阈值时不产生告警."""
        thresholds = {"stage_duration_max_seconds": 10.0}
        checker = AlertChecker(thresholds)
        metrics = self._base_metrics()  # 最大 200ms
        alerts = checker.check(metrics)
        assert all(a.rule != "stage_duration_max_seconds" for a in alerts)

    def test_memory_usage_exceeds_threshold(self):
        """内存占用超阈值时产生告警."""
        thresholds = {"memory_usage_max_mb": 100.0}
        checker = AlertChecker(thresholds)
        metrics = self._base_metrics(resource_sample={"memory_mb": 500.0})
        alerts = checker.check(metrics)
        mem_alerts = [a for a in alerts if a.rule == "memory_usage_max_mb"]
        assert len(mem_alerts) == 1
        assert mem_alerts[0].value == 500.0

    def test_failure_status_triggers_alert(self):
        """批次 status=failed 时产生失败率告警."""
        thresholds = {"failure_rate_max_percent": 20.0}
        checker = AlertChecker(thresholds)
        metrics = self._base_metrics(status="failed")
        alerts = checker.check(metrics)
        fail_alerts = [a for a in alerts if a.rule == "failure_rate_max_percent"]
        assert len(fail_alerts) == 1
        assert fail_alerts[0].value == 100.0

    def test_no_alerts_when_all_within_thresholds(self):
        """所有指标都在阈值内时返回空列表."""
        thresholds = {
            "dq_score_min": 80.0,
            "stage_duration_max_seconds": 600,
            "memory_usage_max_mb": 4096,
            "failure_rate_max_percent": 20.0,
        }
        checker = AlertChecker(thresholds)
        metrics = self._base_metrics(
            dq_score=0.99,
            status="success",
            resource_sample={"memory_mb": 100.0},
        )
        alerts = checker.check(metrics)
        assert alerts == []

    def test_empty_thresholds_returns_no_alerts(self):
        """空阈值 dict 不产生告警."""
        checker = AlertChecker({})
        metrics = self._base_metrics()
        alerts = checker.check(metrics)
        assert alerts == []


# ---------------------------------------------------------------------------
# 5. check_alerts
# ---------------------------------------------------------------------------
class TestCheckAlerts:
    @pytest.fixture
    def run_dir_with_batches(self, tmp_path):
        """创建含多个批次 metrics.json 的 run 目录."""
        run_dir = str(tmp_path / "run")
        os.makedirs(run_dir, exist_ok=True)
        # 批次 1：DQ 低
        b1 = os.path.join(run_dir, "B-001")
        os.makedirs(b1, exist_ok=True)
        json_save(
            os.path.join(b1, "metrics.json"),
            {
                "batch_id": "B-001",
                "status": "success",
                "dq_score": 0.50,  # 50.0 < 80.0
                "stages": [
                    {
                        "name": "ingest",
                        "status": "success",
                        "duration_ms": 100,
                        "rows_in": 0,
                        "rows_out": 100,
                    }
                ],
            },
        )
        # 批次 2：DQ 高
        b2 = os.path.join(run_dir, "B-002")
        os.makedirs(b2, exist_ok=True)
        json_save(
            os.path.join(b2, "metrics.json"),
            {
                "batch_id": "B-002",
                "status": "success",
                "dq_score": 0.95,
                "stages": [
                    {
                        "name": "ingest",
                        "status": "success",
                        "duration_ms": 100,
                        "rows_in": 0,
                        "rows_out": 100,
                    }
                ],
            },
        )
        # 批次 3：失败
        b3 = os.path.join(run_dir, "B-003")
        os.makedirs(b3, exist_ok=True)
        json_save(
            os.path.join(b3, "metrics.json"),
            {
                "batch_id": "B-003",
                "status": "failed",
                "dq_score": 0.90,
                "stages": [
                    {
                        "name": "ingest",
                        "status": "failed",
                        "duration_ms": 100,
                        "rows_in": 0,
                        "rows_out": 0,
                    }
                ],
            },
        )
        return run_dir

    def test_scan_multiple_batches(self, run_dir_with_batches):
        """check_alerts 扫描多个批次返回正确告警."""
        cfg = {
            "enabled": True,
            "alerts": {"dq_score_min": 80.0, "failure_rate_max_percent": 20.0},
            "history_window": 10,
        }
        alerts = check_alerts(run_dir_with_batches, cfg)
        # 批次 1 DQ 低 → 1 条 dq 告警
        # 批次 3 failed → 1 条 failure 告警
        # 3 批次中 1 失败 → 失败率 33.3% > 20% → 1 条聚合 failure 告警
        rules = [a.rule for a in alerts]
        assert "dq_score_min" in rules
        assert rules.count("failure_rate_max_percent") >= 1

    def test_disabled_returns_empty(self, run_dir_with_batches):
        """enabled=false 时返回空列表."""
        cfg = {"enabled": False, "alerts": {"dq_score_min": 80.0}}
        alerts = check_alerts(run_dir_with_batches, cfg)
        assert alerts == []

    def test_empty_run_dir(self, tmp_path):
        """空 run 目录返回空列表."""
        empty_run = str(tmp_path / "empty_run")
        os.makedirs(empty_run, exist_ok=True)
        cfg = {"enabled": True, "alerts": {"dq_score_min": 80.0}}
        alerts = check_alerts(empty_run, cfg)
        assert alerts == []

    def test_history_window_limits_scan(self, run_dir_with_batches):
        """history_window 限制扫描批次数."""
        cfg = {
            "enabled": True,
            "alerts": {"dq_score_min": 80.0},
            "history_window": 1,  # 只看最近 1 个批次（B-003，DQ 90.0 > 80.0）
        }
        alerts = check_alerts(run_dir_with_batches, cfg)
        # 最近 1 个批次是 B-003，DQ=0.90 → 90.0 > 80.0，无 DQ 告警
        assert all(a.rule != "dq_score_min" for a in alerts)

    def test_nonexistent_run_dir(self, tmp_path):
        """不存在的 run 目录返回空列表."""
        cfg = {"enabled": True, "alerts": {"dq_score_min": 80.0}}
        alerts = check_alerts(str(tmp_path / "nonexistent"), cfg)
        assert alerts == []


# ---------------------------------------------------------------------------
# 6. HealthServer
# ---------------------------------------------------------------------------
class TestHealthServer:
    def _find_free_port(self):
        import socket

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    @pytest.fixture
    def health_run_dir(self, tmp_path):
        """创建含 1 个成功批次的 run 目录."""
        run_dir = str(tmp_path / "run")
        os.makedirs(run_dir, exist_ok=True)
        b = os.path.join(run_dir, "B-health-001")
        os.makedirs(b, exist_ok=True)
        json_save(
            os.path.join(b, "metrics.json"),
            {
                "batch_id": "B-health-001",
                "status": "success",
                "started_at": "2026-08-16T10:00:00Z",
                "finished_at": "2026-08-16T10:00:01Z",
                "total_duration_ms": 1000,
                "dq_score": 0.95,
                "stages": [],
            },
        )
        return run_dir

    def test_start_stop(self, health_run_dir):
        """HealthServer start/stop 不抛异常."""
        port = self._find_free_port()
        server = HealthServer(host="127.0.0.1", port=port, run_dir=health_run_dir)
        server.start()
        try:
            assert server.is_running()
        finally:
            server.stop()
        # stop 后不再运行
        assert not server.is_running()

    def test_health_endpoint_returns_json(self, health_run_dir):
        """启动后 GET /health 返回 JSON，含 batches/summary 字段."""
        port = self._find_free_port()
        server = HealthServer(host="127.0.0.1", port=port, run_dir=health_run_dir)
        server.start()
        try:
            assert server.is_running()
            # 等服务器就绪
            time.sleep(0.2)
            url = f"http://127.0.0.1:{port}/health"
            with urllib.request.urlopen(url, timeout=5) as resp:
                assert resp.status == 200
                body = json.loads(resp.read().decode("utf-8"))
            assert "status" in body
            assert "batches" in body
            assert "summary" in body
            assert body["summary"]["total"] == 1
            assert body["summary"]["success"] == 1
            assert body["batches"][0]["batch_id"] == "B-health-001"
        finally:
            server.stop()

    def test_unknown_path_returns_404(self, health_run_dir):
        """未知路径返回 404."""
        port = self._find_free_port()
        server = HealthServer(host="127.0.0.1", port=port, run_dir=health_run_dir)
        server.start()
        try:
            time.sleep(0.2)
            url = f"http://127.0.0.1:{port}/unknown"
            try:
                urllib.request.urlopen(url, timeout=5)
                raise AssertionError("应返回 404")
            except urllib.error.HTTPError as e:
                assert e.code == 404
        finally:
            server.stop()

    def test_stop_is_idempotent(self, health_run_dir):
        """多次 stop 不抛异常."""
        port = self._find_free_port()
        server = HealthServer(host="127.0.0.1", port=port, run_dir=health_run_dir)
        server.start()
        server.stop()
        server.stop()  # 不抛
        assert not server.is_running()

    def test_start_is_idempotent(self, health_run_dir):
        """多次 start 不重复启动."""
        port = self._find_free_port()
        server = HealthServer(host="127.0.0.1", port=port, run_dir=health_run_dir)
        server.start()
        server.start()  # 不抛，不重复
        try:
            assert server.is_running()
        finally:
            server.stop()


# ---------------------------------------------------------------------------
# 7. monitoring.enabled=false 向后兼容
# ---------------------------------------------------------------------------
class TestBackwardCompat:
    def test_load_default_config_disabled(self):
        """加载 config/monitoring.json 缺省 enabled=false."""
        cfg = load_monitoring_config(abs_path("config/monitoring.json"))
        assert cfg.get("enabled") is False

    def test_check_alerts_disabled_no_alerts(self, tmp_path):
        """enabled=false 时 check_alerts 返回空."""
        run_dir = str(tmp_path / "run")
        os.makedirs(run_dir, exist_ok=True)
        b = os.path.join(run_dir, "B-001")
        os.makedirs(b, exist_ok=True)
        json_save(
            os.path.join(b, "metrics.json"),
            {
                "batch_id": "B-001",
                "status": "failed",
                "dq_score": 0.1,
                "stages": [],
            },
        )
        cfg = {"enabled": False, "alerts": {"dq_score_min": 80.0}}
        assert check_alerts(run_dir, cfg) == []

    def test_load_nonexistent_config_returns_default(self):
        """加载不存在的配置文件返回 disabled 缺省."""
        cfg = load_monitoring_config("/nonexistent/monitoring.json")
        assert cfg.get("enabled") is False

    def test_load_malformed_config_returns_default(self, tmp_path):
        """加载损坏的配置文件返回 disabled 缺省."""
        bad_path = str(tmp_path / "bad.json")
        with open(bad_path, "w", encoding="utf-8") as f:
            f.write("{not valid json")
        cfg = load_monitoring_config(bad_path)
        assert cfg.get("enabled") is False


# ---------------------------------------------------------------------------
# 8. 端到端：pipeline 集成监控
# ---------------------------------------------------------------------------
class TestPipelineIntegration:
    """端到端验证 pipeline 集成监控（monitoring.enabled=true）."""

    def _make_run_dir(self, tmp_path):
        """在项目所在盘创建临时 run 目录（避免跨盘 relpath 问题）."""
        drive = os.path.splitdrive(ROOT)[0] + os.sep
        d = tempfile.mkdtemp(prefix="autobatch_mon_", dir=drive)
        return d

    def test_pipeline_with_monitoring_enabled(self, tmp_path):
        """monitoring.enabled=true 时 pipeline 完成后 metrics.json 含 resource_sample."""
        from src.helpers import json_load
        from src.pipeline import run_pipeline

        work_dir = self._make_run_dir(tmp_path)
        try:
            # 准备小规模数据
            from src.generator import main as gen_main

            cfg = json_load(abs_path("config/pipeline_small.json"))
            data_dir = os.path.join(work_dir, "data", "raw")
            cfg["generator"]["output_dir"] = data_dir
            cfg["generator"]["enabled"] = True
            gen_main(cfg)
            cfg["source"]["files"] = {
                "orders": os.path.join(data_dir, "orders.csv"),
                "customers": os.path.join(data_dir, "customers.csv"),
                "products": os.path.join(data_dir, "products.csv"),
            }
            cfg["generator"]["enabled"] = False

            # run_dir 必须在 ROOT/run 下（output.py 硬编码 prefix）
            run_root = os.path.join(ROOT, "run")
            os.makedirs(run_root, exist_ok=True)
            cfg["pipeline"]["run_dir"] = run_root

            # 写临时 monitoring.json，enabled=true
            mon_cfg = {
                "enabled": True,
                "alerts": {
                    "dq_score_min": 80.0,
                    "stage_duration_max_seconds": 600,
                    "memory_usage_max_mb": 4096,
                    "failure_rate_max_percent": 20.0,
                },
                "health_check": {"enabled": False, "port": 8086, "host": "0.0.0.0"},
                "history_window": 10,
            }
            mon_path = os.path.join(work_dir, "monitoring.json")
            json_save(mon_path, mon_cfg)
            cfg["monitoring_config"] = mon_path

            batch_id = "test-mon-" + uuid.uuid4().hex[:6]
            rc = run_pipeline(cfg, batch_id, "")
            assert rc == 0, "pipeline 应成功"

            # 验证 metrics.json 含 resource_sample
            metrics_path = os.path.join(run_root, batch_id, "metrics.json")
            metrics = json_load(metrics_path)
            assert "resource_sample" in metrics
            assert "cpu_percent" in metrics["resource_sample"]
            assert "memory_mb" in metrics["resource_sample"]
        finally:
            if "batch_id" in dir() and batch_id:
                shutil.rmtree(
                    os.path.join(ROOT, "run", batch_id),
                    ignore_errors=True,
                )
            shutil.rmtree(work_dir, ignore_errors=True)

    def test_pipeline_with_monitoring_disabled_no_resource_sample(self, tmp_path):
        """monitoring.enabled=false 时 metrics.json 不含 resource_sample（向后兼容）."""
        from src.helpers import json_load
        from src.pipeline import run_pipeline

        work_dir = self._make_run_dir(tmp_path)
        try:
            from src.generator import main as gen_main

            cfg = json_load(abs_path("config/pipeline_small.json"))
            data_dir = os.path.join(work_dir, "data", "raw")
            cfg["generator"]["output_dir"] = data_dir
            cfg["generator"]["enabled"] = True
            gen_main(cfg)
            cfg["source"]["files"] = {
                "orders": os.path.join(data_dir, "orders.csv"),
                "customers": os.path.join(data_dir, "customers.csv"),
                "products": os.path.join(data_dir, "products.csv"),
            }
            cfg["generator"]["enabled"] = False

            run_root = os.path.join(ROOT, "run")
            os.makedirs(run_root, exist_ok=True)
            cfg["pipeline"]["run_dir"] = run_root

            # 用缺省 config/monitoring.json（enabled=false）
            # 不设 cfg["monitoring_config"]，pipeline 用缺省路径

            batch_id = "test-mon-off-" + uuid.uuid4().hex[:6]
            rc = run_pipeline(cfg, batch_id, "")
            assert rc == 0

            metrics_path = os.path.join(run_root, batch_id, "metrics.json")
            metrics = json_load(metrics_path)
            # enabled=false 时不追加 resource_sample
            assert "resource_sample" not in metrics
        finally:
            if "batch_id" in dir() and batch_id:
                shutil.rmtree(
                    os.path.join(ROOT, "run", batch_id),
                    ignore_errors=True,
                )
            shutil.rmtree(work_dir, ignore_errors=True)
