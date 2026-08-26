"""Quality 工作流的测试收集器：跑 pytest 并把结果转成 GHA 注解.

为什么存在：quality.yml 曾用 `set -eo pipefail; pytest | tee` 收集，
在 runner 上出现过"零输出秒退且 tee 文件缺失"的 shell 层异常（2026-08
连续数轮远程排查未定位到根因）。本脚本以 subprocess 直跑并自行落盘/
打注解，绕开 bash 管道与 YAML 转义层；退出码 = pytest 退出码，保住
pipefail 语义（测试失败即步骤失败）。

注解经 check-runs 公开 API 可读（job 日志需鉴权），是远程排障的生命线。
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

LOG_PATH = os.path.join("benchmarks", "quality_pytest.log")


def main(argv: list[str]) -> int:
    # 首行即打 START 标记：若 runner 平台层在解释器启动前拦截，job 日志里
    # 连 START 都不会出现→判定为平台故障；若 START 在而 DONE 缺→pytest 层问题。
    t0 = time.time()
    print(f"[qcollect] START t={t0:.3f} pid={os.getpid()}", flush=True)
    cmd = [sys.executable, "-m", "pytest", *argv]
    print("[qcollect] run:", " ".join(cmd), flush=True)
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    out = (proc.stdout or "") + (proc.stderr or "")
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w", encoding="utf-8") as f:
        f.write(out)
    print(f"[qcollect] exit={proc.returncode} log={LOG_PATH} ({len(out)} chars)", flush=True)

    if proc.returncode != 0:
        lines = out.splitlines()
        failed = [x for x in lines if x.startswith(("FAILED", "ERROR"))]
        if failed:
            for x in failed[:30]:
                print(f"::error::{x[:180]}", flush=True)
        else:
            # 崩溃在摘要前：转储尾部帮助判层
            print("::error::no FAILED lines — dumping tail", flush=True)
            for x in lines[-40:]:
                if x.strip():
                    print(f"::error::TAIL|{x[:170]}", flush=True)
    print(f"[qcollect] DONE t={time.time():.3f} elapsed={time.time() - t0:.1f}s", flush=True)
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
