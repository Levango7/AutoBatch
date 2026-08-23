"""Run 目录清理工具。

用法：
    python scripts/clean_runs.py                    # 预览模式（默认 dry-run）
    python scripts/clean_runs.py --keep 5           # 保留最近 5 个批次
    python scripts/clean_runs.py --older-than 30d   # 删除 30 天前的批次
    python scripts/clean_runs.py --keep 5 --older-than 30d --yes  # 实际删除

保护规则：
    - 始终保留 latest.json 指针指向的批次
    - 始终保留最近 1 个批次（即使失败）
    - 默认 dry-run：真正删除必须显式传 --yes
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _parse_days(value: str) -> int:
    """解析 '30d' 或 '30' 为天数。"""
    value = value.strip().lower()
    if value.endswith("d"):
        return int(value[:-1])
    return int(value)


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    """解析 ISO8601 时间戳；统一返回 aware UTC（naive 视为 UTC）。

    统一时区的原因：run/ 里可能混存带 Z 与不带 Z 的时间戳，
    aware/naive 直接比较会 TypeError。
    """
    if not ts:
        return None
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def scan_batches(run_dir: Path) -> list[dict]:
    """扫描 run/ 目录，返回每个批次的元信息。"""
    batches = []
    if not run_dir.is_dir():
        return batches
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        status_path = child / "status.json"
        if not status_path.is_file():
            continue
        try:
            with status_path.open("r", encoding="utf-8") as f:
                status = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        batches.append(
            {
                "path": child,
                "batch_id": status.get("batch_id", child.name),
                "status": status.get("status", "unknown"),
                "started_at": status.get("started_at"),
                "finished_at": status.get("finished_at"),
            }
        )
    return batches


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="清理 AutoBatch run/ 目录中的旧批次",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--run-dir",
        default="run",
        help="run 目录路径（默认当前工作目录的 run/）",
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=0,
        help="始终保留最近 N 个批次（含失败），默认为 0（不过滤）",
    )
    parser.add_argument(
        "--older-than",
        dest="older_than",
        default="",
        help="删除此时间之前的批次，格式：30d / 7d / 30",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅预览将删除的批次，不实际删除（默认行为）",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="实际执行删除（危险操作；缺省时等价于 dry-run 预览）",
    )
    args = parser.parse_args(argv)
    # 删除必须显式 --yes；--dry-run 与 --yes 同传时以保守的 dry-run 为准
    do_delete = args.yes and not args.dry_run

    run_dir = Path(args.run_dir).resolve()
    if not run_dir.is_dir():
        print(f"[clean_runs] run 目录不存在: {run_dir}", file=sys.stderr)
        return 2

    batches = scan_batches(run_dir)
    if not batches:
        print(f"[clean_runs] run/ 目录中无有效批次（{len(batches)} 个）")
        return 0

    # 找到 latest.json 指针
    latest_id = None
    latest_path = run_dir / "latest.json"
    if latest_path.is_file():
        try:
            with latest_path.open("r", encoding="utf-8") as f:
                latest_id = (json.load(f) or {}).get("batch_id")
        except (OSError, json.JSONDecodeError):
            pass

    # 按 started_at 排序（无时间戳的排最后）
    def sort_key(b):
        dt = _parse_iso(b["started_at"])
        return (0, dt) if dt else (1, b["batch_id"])

    batches.sort(key=sort_key, reverse=True)  # 最新的在前

    protected = {latest_id} if latest_id else set()
    # 始终保留最近 1 个（无论成功失败）
    if batches:
        protected.add(batches[0]["batch_id"])

    to_delete: list[dict] = []
    to_keep: list[dict] = []

    cutoff_dt = None
    if args.older_than:
        days = _parse_days(args.older_than)
        cutoff_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        from datetime import timedelta

        cutoff_dt = cutoff_dt - timedelta(days=days)

    for b in batches:
        bid = b["batch_id"]
        if bid in protected:
            to_keep.append(b)
            continue
        if args.keep > 0 and len(to_keep) < args.keep:
            to_keep.append(b)
            continue
        if cutoff_dt:
            started = _parse_iso(b["started_at"])
            # _parse_iso 已统一返回 aware UTC，可直接比较
            if started and started >= cutoff_dt:
                to_keep.append(b)
                continue
        to_delete.append(b)

    if not to_delete:
        print("[clean_runs] 无需清理（所有批次均受保护）")
        return 0

    print(
        f"[clean_runs] 共 {len(batches)} 个批次，保留 {len(to_keep)} 个，待删除 {len(to_delete)} 个"
    )
    if not do_delete:
        mode = "dry-run" if args.dry_run else "preview（加 --yes 实际删除）"
        print(f"[{mode}] 以下批次将被删除：")
        for b in to_delete:
            print(f"  - {b['batch_id']} ({b['status']})")
        return 0

    total_freed = 0
    for b in to_delete:
        size = sum(p.stat().st_size for p in b["path"].rglob("*") if p.is_file())
        try:
            shutil.rmtree(b["path"])
            total_freed += size
            print(f"  已删除: {b['batch_id']} ({size / 1024 / 1024:.1f} MB)")
        except OSError as e:
            print(f"  删除失败: {b['batch_id']}: {e}", file=sys.stderr)

    print(f"[clean_runs] 清理完成，释放 {total_freed / 1024 / 1024:.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
