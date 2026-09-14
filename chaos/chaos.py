from __future__ import annotations

import argparse
import json
import sys
import time

import httpx


def main() -> int:
    parser = argparse.ArgumentParser(description="Chaos engine client for the fault-tolerant task queue")
    parser.add_argument("--api", default="http://localhost:8010")
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--kill-interval", type=int, default=15)
    parser.add_argument("--workers", default="random")
    parser.add_argument("--restart-delay", type=int, default=8)
    parser.add_argument("--submit", type=int, default=0, help="submit this many jobs before starting chaos")
    args = parser.parse_args()

    client = httpx.Client(base_url=args.api, timeout=30.0)
    if args.submit:
        submitted = client.post("/jobs/batch", json={"count": args.submit})
        submitted.raise_for_status()
        print(json.dumps(submitted.json() | {"phase": "submitted"}, indent=2))

    start = client.post(
        "/chaos/start",
        json={
            "duration": args.duration,
            "kill_interval": args.kill_interval,
            "workers": args.workers,
            "restart_delay": args.restart_delay,
        },
    )
    start.raise_for_status()
    print(json.dumps({"phase": "chaos_started", **start.json()}, indent=2))

    deadline = time.time() + args.duration + args.restart_delay + 15
    while time.time() < deadline:
        status = client.get("/chaos").json()
        print(json.dumps({"phase": "status", "status": status.get("status"), "killed": status.get("workers_killed")}))
        if status.get("status") in {"complete", "stopped"}:
            summary = client.get("/experiment").json()
            print(json.dumps({"phase": "summary", **summary}, indent=2))
            lost = int(summary.get("jobs_lost") or 0)
            return 1 if lost else 0
        time.sleep(3)
    print("chaos did not finish before timeout", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
