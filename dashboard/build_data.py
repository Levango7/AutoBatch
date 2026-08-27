"""数据质量仪表盘数据生成器.

扫描 run/ 目录下所有批次，读取 status.json / quality_summary.json /
04_aggregates/kpi.json / 04_aggregates/daily_sales.csv，生成自包含的
dashboard/data.js，供 dashboard.html 离线打开使用。

用法：
    python dashboard/build_data.py            # 默认扫描 ../run，输出 ./data.js
    python dashboard/build_data.py --run-dir F:\\...\\run --out data.js

设计要点：
- 纯标准库，不依赖 src/ 包，可独立运行。
- 容错：单个批次缺文件时跳过该批次，不影响整体生成。
- 排序：按 started_at 升序，便于折线图/时间线按时间绘制。
- 输出：window.AutoBatchData = {...}; 形式，HTML 用 <script src="data.js"> 加载。
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    """解析 ISO8601 时间戳（容忍尾随 Z）。"""
    if not ts:
        return None
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _load_json(path: Path) -> Optional[dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _load_daily_sales(path: Path) -> list[dict[str, Any]]:
    """读 daily_sales.csv，返回 [{date, orders, units, revenue, avgOrderValue}, ...].

    兼容 Spark 后端的目录型产物：`df.write.csv(path)` 写出的是目录
    （内含 part-00000-* 分片文件，每个分片自带 header）。此时逐分片
    DictReader 解析（自动跳过各分片 header）后合并。跳过 "_" / "." 开头的
    元数据文件（_SUCCESS、.crc 等），分片按文件名排序保证结果确定。
    """
    files: list[Path] = []
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = sorted(
            p for p in path.iterdir() if p.is_file() and not p.name.startswith(("_", "."))
        )
    out: list[dict[str, Any]] = []
    for fp in files:
        try:
            with fp.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        out.append(
                            {
                                "date": row.get("order_date", ""),
                                "orders": int(row.get("orders", 0) or 0),
                                "units": int(row.get("units", 0) or 0),
                                "revenue": float(row.get("revenue", 0.0) or 0.0),
                                "avgOrderValue": float(row.get("avg_order_value", 0.0) or 0.0),
                            }
                        )
                    except (ValueError, TypeError):
                        continue
        except OSError:
            continue
    out.sort(key=lambda r: r.get("date", ""))
    return out


def _to_int(v: Any, default: int = 0) -> int:
    """宽松转 int：脏数据（字符串/None/浮点串）返回 default 而非抛 ValueError."""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _to_float(v: Any, default: float = 0.0) -> float:
    """宽松转 float：脏数据返回 default 而非抛 ValueError."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _extract_batch(batch_dir: Path) -> Optional[dict[str, Any]]:
    """从单个批次目录提取数据。缺 status.json 或 quality_summary.json 则返回 None."""
    status = _load_json(batch_dir / "status.json")
    quality = _load_json(batch_dir / "quality_summary.json")
    if status is None and quality is None:
        return None

    batch_id = (status or {}).get("batch_id") or batch_dir.name
    started_at = (status or {}).get("started_at")
    finished_at = (status or {}).get("finished_at")

    # 阶段信息（status.stages）
    stages_raw = (status or {}).get("stages") or []
    stages = []
    for s in stages_raw:
        stages.append(
            {
                "name": s.get("name", ""),
                "status": s.get("status", ""),
                "durationMs": int(s.get("duration_ms", 0) or 0),
                "rowsIn": int(s.get("rows_in", 0) or 0),
                "rowsOut": int(s.get("rows_out", 0) or 0),
            }
        )

    # 总耗时：优先用 finished - started，否则累加 stage duration
    duration_ms = 0
    start_dt = _parse_iso(started_at)
    finish_dt = _parse_iso(finished_at)
    if start_dt and finish_dt:
        duration_ms = int((finish_dt - start_dt).total_seconds() * 1000)
    if duration_ms <= 0:
        duration_ms = sum(s["durationMs"] for s in stages)

    # 质量摘要
    q: dict[str, Any] = {}
    if quality:
        q = {
            "dqScore": _to_float(quality.get("dq_score")),
            "rulesTotal": _to_int(quality.get("rules_total")),
            "checksTotal": _to_int(quality.get("checks_total")),
            "checksPassed": _to_int(quality.get("checks_passed")),
            "checksFailed": _to_int(quality.get("checks_failed")),
            "quarantinedRows": quality.get("quarantined_rows", {}) or {},
        }

    # KPI
    kpi_raw = _load_json(batch_dir / "04_aggregates" / "kpi.json")
    kpi: dict[str, Any] = {}
    if kpi_raw:
        kpi = {
            "orders": _to_int(kpi_raw.get("orders")),
            "units": _to_int(kpi_raw.get("units")),
            "totalRevenue": _to_float(kpi_raw.get("total_revenue")),
            "avgOrderValue": _to_float(kpi_raw.get("avg_order_value")),
            "days": _to_int(kpi_raw.get("days")),
            "currency": kpi_raw.get("currency", "CNY"),
        }

    daily_sales = _load_daily_sales(batch_dir / "04_aggregates" / "daily_sales.csv")

    return {
        "batchId": batch_id,
        "batchDir": batch_dir.name,
        "status": (status or {}).get("status", "unknown"),
        "startedAt": started_at,
        "finishedAt": finished_at,
        "durationMs": duration_ms,
        "stages": stages,
        "quality": q,
        "kpi": kpi,
        "dailySales": daily_sales,
    }


def scan_runs(run_dir: Path) -> list[dict[str, Any]]:
    """扫描 run/ 目录，返回按 startedAt 升序排列的批次列表."""
    batches: list[dict[str, Any]] = []
    if not run_dir.is_dir():
        return batches
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        # 跳过非批次目录（无 status.json 且无 quality_summary.json）
        rec = _extract_batch(child)
        if rec is not None:
            batches.append(rec)

    # 排序：按 startedAt 升序；缺时间戳的排到最后
    def sort_key(b: dict[str, Any]) -> Any:
        dt = _parse_iso(b.get("startedAt"))
        # 用 (0, timestamp) 让有时间的在前，(1, name) 让无时间的在后
        if dt is None:
            return (1, b.get("batchDir", ""))
        return (0, dt)

    batches.sort(key=sort_key)
    return batches


def build_data_object(run_dir: Path) -> dict[str, Any]:
    batches = scan_runs(run_dir)
    # 统计摘要
    total = len(batches)
    success = sum(1 for b in batches if b.get("status") == "success")
    failed = sum(1 for b in batches if b.get("status") not in ("success", "unknown"))
    dq_scores = [
        b["quality"]["dqScore"]
        for b in batches
        if b.get("quality") and b["quality"].get("dqScore") is not None
    ]
    dq_avg = sum(dq_scores) / len(dq_scores) if dq_scores else 0.0
    return {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        # 相对路径：避免把本机绝对路径写进对外分发的 data.js（信息泄漏）
        "runDir": "run",
        "summary": {
            "totalBatches": total,
            "successBatches": success,
            "failedBatches": failed,
            "dqScoreAvg": round(dq_avg, 6),
            "dqScoreMin": round(min(dq_scores), 6) if dq_scores else 0.0,
            "dqScoreMax": round(max(dq_scores), 6) if dq_scores else 0.0,
        },
        "batches": batches,
    }


def write_data_js(data: dict[str, Any], out_path: Path) -> None:
    """写入 data.js：window.AutoBatchData = {...};"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    # 防 </script> 提前终止 <script> 块（存储型 XSS 通道）：
    # batchId 等字段来自目录名/JSON，未做格式校验，含 "</script>" 时会
    # 截断 JS 解析并注入任意 HTML。"<\/" 在 JS 字符串里等价于 "</"。
    payload = payload.replace("</", "<\\/")
    header = (
        "// 自动生成，请勿手工编辑。\n"
        "// 由 dashboard/build_data.py 扫描 run/ 目录生成。\n"
        "// 重新生成：python dashboard/build_data.py\n"
    )
    content = f"{header}window.AutoBatchData = {payload};\n"
    with out_path.open("w", encoding="utf-8") as f:
        f.write(content)


def main(argv: Optional[list[str]] = None) -> int:
    here = Path(__file__).resolve().parent
    default_run = here.parent / "run"
    default_out = here / "data.js"
    parser = argparse.ArgumentParser(description="生成 dashboard/data.js")
    parser.add_argument(
        "--run-dir", default=str(default_run), help=f"run 目录路径（默认 {default_run}）"
    )
    parser.add_argument(
        "--out", default=str(default_out), help=f"输出 data.js 路径（默认 {default_out}）"
    )
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir).resolve()
    out_path = Path(args.out).resolve()
    if not run_dir.is_dir():
        print(f"[build_data] run 目录不存在: {run_dir}", file=sys.stderr)
        return 2

    data = build_data_object(run_dir)
    write_data_js(data, out_path)
    n = len(data["batches"])
    print(f"[build_data] 扫描 {n} 个批次，写入 {out_path}")
    print(f"[build_data] 摘要: {data['summary']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
