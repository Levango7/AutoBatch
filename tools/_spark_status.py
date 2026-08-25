"""查询 Spark Master / Worker 状态（JSON API），确认集群基准是否在跑."""

import json
import sys
import urllib.request


def get(url: str):
    with urllib.request.urlopen(url, timeout=10) as r:  # noqa: S310
        return json.load(r)


def main() -> int:
    try:
        apps = get("http://localhost:8080/api/v1/applications").get("apps", [])
        print("== Master applications ==")
        for a in apps:
            print(a.get("id"), a.get("name", "")[:50], a.get("state"), "cores=", a.get("cores"))
        if not apps:
            print("(none)")
    except Exception as e:  # noqa: BLE001
        print("master query failed:", e)
    for port, name in ((8081, "worker-1"), (8082, "worker-2")):
        try:
            w = get(f"http://localhost:{port}/api/v1/applications")
            lst = w.get("apps", [])
            print(f"== {name} ==")
            for a in lst:
                print(" ", a.get("id"), a.get("state"))
            if not lst:
                print("  (idle)")
        except Exception as e:  # noqa: BLE001
            print(name, "query failed:", e)
    return 0


if __name__ == "__main__":
    sys.exit(main())
