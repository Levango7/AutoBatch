"""查看最新 bench-cluster 批次的进度（各 stage 日志最后事件）."""

import json
import os


def main() -> int:
    root = "run"
    if not os.path.isdir(root):
        print("no run/")
        return 1
    batches = sorted(
        (d for d in os.listdir(root) if d.startswith("bench-")),
        key=lambda d: os.path.getmtime(os.path.join(root, d)),
    )
    if not batches:
        print("no bench batches")
        return 1
    b = batches[-1]
    print("latest:", b)
    log_dir = os.path.join(root, b, "logs")
    if not os.path.isdir(log_dir):
        print("(logs not yet created)")
        return 0
    for f in sorted(os.listdir(log_dir)):
        p = os.path.join(log_dir, f)
        last = ""
        n = 0
        with open(p, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.strip():
                    last = line.strip()
                    n += 1
        try:
            d = json.loads(last)
            brief = f"{d.get('level')} {d.get('msg', '')[:70]}"
        except Exception:
            brief = last[:90]
        print(f"  {f}: {n} lines | {brief}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
