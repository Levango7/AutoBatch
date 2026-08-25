"""打印指定批次/阶段的错误与 traceback 尾部（合并诊断工具）."""

import json
import os
import sys


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: _bench_err.py <batch> <stage>")
        return 1
    batch, stage = sys.argv[1], sys.argv[2].strip(";")
    path = os.path.join("run", batch, "logs", f"{stage}.jsonl")
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("level") == "ERROR":
                print("error:", str(d.get("error", ""))[:300])
                for t in (d.get("trace") or [])[-6:]:
                    print("  ", t)
    return 0


if __name__ == "__main__":
    sys.exit(main())
