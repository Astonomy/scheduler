import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

from . import quota_adapter
from .config import load_config
from .view import render_tree
from .scheduler import load_task, paths, recover, tick
from .state_store import Busy, read
from .task_list import DEFAULT_TASK_DIR, render_tasks, resolve_task


def main(argv=None):
    parser = argparse.ArgumentParser(description="Schedule Codex task trees using account quota")
    sub = parser.add_subparsers(dest="action", required=True)
    config_parser = sub.add_parser("config", help="validate and display effective TOML policy")
    config_parser.add_argument("--config", metavar="FILE")
    list_parser = sub.add_parser("list", help="list task definitions and saved progress; never dispatch")
    list_parser.add_argument("--task-dir", default=str(DEFAULT_TASK_DIR))
    list_parser.add_argument("--state-dir", default=str(Path.home() / ".reqflow-scheduler"))
    for name in ("tick", "watch", "quota", "status", "stop", "recover", "view"):
        p = sub.add_parser(name)
        p.add_argument("task")
        p.add_argument("--config", metavar="FILE")
        p.add_argument("--state-dir", default=str(Path.home() / ".reqflow-scheduler"))
        if name == "recover":
            p.add_argument("--inspected", action="store_true")
        if name in {"watch", "view"}:
            p.add_argument("--interval-seconds", type=int)
        if name == "view":
            p.add_argument("--task-dir", default=str(DEFAULT_TASK_DIR), help="directory to search for task IDs")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--watch", dest="watch", action="store_true", default=True,
                              help="refresh until Ctrl+C (default); never dispatch")
            mode.add_argument("--once", dest="watch", action="store_false", help="print one snapshot and exit")
    args = parser.parse_args(argv)
    try:
        if args.action == "list":
            print(render_tasks(args.task_dir, args.state_dir))
            return 0
        config = load_config(args.config)
        if args.action == "config":
            print(json.dumps(config, ensure_ascii=False, indent=2))
            return 0
        task_path = resolve_task(args.task, args.task_dir) if args.action == "view" else args.task
        task = load_task(task_path)
        if args.config:
            task.update({key: value for key, value in config.items() if key != "interval_seconds"})
        if args.action in {"watch", "view"}:
            default_interval = 2 if args.action == "view" and not args.config else config["interval_seconds"]
            args.interval_seconds = args.interval_seconds if args.interval_seconds is not None else default_interval
        folder, state_path, stop_path = paths(task, args.state_dir)
        if args.action in {"watch", "view"} and args.interval_seconds < 1:
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
            if args.action == "view":
                if args.watch and sys.stdout.isatty():
                    print("\033[2J\033[H", end="")
                print(render_tree(task, result), flush=True)
                if not args.watch:
                    return 0
                time.sleep(min(args.interval_seconds, 0.1) if sys.stdout.isatty() else args.interval_seconds)
                continue
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
