import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

from . import quota_adapter
from .scheduler import load_task, paths, recover, tick
from .state_store import Busy, read


def main(argv=None):
    parser = argparse.ArgumentParser(description="Schedule Codex task trees using account quota")
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("tick", "watch", "quota", "status", "stop", "recover"):
        p = sub.add_parser(name)
        p.add_argument("task")
        p.add_argument("--state-dir", default=str(Path.home() / ".reqflow-scheduler"))
        if name == "recover":
            p.add_argument("--inspected", action="store_true")
        if name == "watch":
            p.add_argument("--interval-seconds", type=int, default=60)
    args = parser.parse_args(argv)
    try:
        task = load_task(args.task)
        folder, state_path, stop_path = paths(task, args.state_dir)
        if args.action == "watch" and args.interval_seconds < 1:
            raise ValueError("interval must be positive")
        while True:
            if args.action in {"tick", "watch"}:
                result = tick(task, args.state_dir)
            elif args.action == "quota":
                result = quota_adapter.query(task, datetime.now(timezone.utc))
            elif args.action == "recover":
                result = recover(task, args.state_dir, args.inspected)
            elif args.action == "stop":
                folder.mkdir(parents=True, exist_ok=True)
                stop_path.touch()
                result = {"id": task["id"], "stop_requested": True}
            else:
                result = read(state_path, {"id": task["id"], "status": "queued"})
            print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
            if args.action != "watch" or result.get("status") in {"completed", "interrupted", "running", "stopped"}:
                return 0 if result.get("status") not in {"blocked", "interrupted", "running"} else 2
            time.sleep(args.interval_seconds)
    except Busy as exc:
        print(json.dumps({"status": "busy", "reason": str(exc)}))
        return 3
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "error", "reason": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
