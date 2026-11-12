import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from . import quota_adapter, routing, stages
from .config import load_config
from .view import render_tree
from .scheduler import continue_task, fingerprint, load_task, paths, process_running, recover, tick, validate_extension
from .state_store import Busy, lock, read, write
from .task_list import DEFAULT_TASK_DIR, remove_task, render_tasks, request_stop, resolve_task


def main(argv=None):
    parser = argparse.ArgumentParser(description="Schedule Codex task trees using account quota")
    sub = parser.add_subparsers(dest="action", required=True)
    config_parser = sub.add_parser("config", help="validate and display effective TOML policy")
    config_parser.add_argument("--config", metavar="FILE")
    list_parser = sub.add_parser("list", help="list task definitions and saved progress; never dispatch")
    list_parser.add_argument("--task-dir", default=str(DEFAULT_TASK_DIR))
    list_parser.add_argument("--state-dir", default=str(Path.home() / ".reqflow-scheduler"))
    list_parser.add_argument("--all", action="store_true", help="include archived completed and removed task trees")
    rm_parser = sub.add_parser("rm", help="permanently delete matching task definitions, checkpoints and logs")
    rm_parser.add_argument("task_id", metavar="PATTERN", help="task ID or glob pattern (*, ?, [abc]); quote wildcards")
    rm_parser.add_argument("--regex", action="store_true", help="match the entire ID using a regular expression instead of glob")
    rm_parser.add_argument("--task-dir", default=str(DEFAULT_TASK_DIR))
    rm_parser.add_argument("--state-dir", default=str(Path.home() / ".reqflow-scheduler"))
    for name in ("tick", "watch", "submit", "continue", "quota", "status", "stop", "recover", "view", "models", "usage", "feedback"):
        p = sub.add_parser(name)
        p.add_argument("task")
        p.add_argument("--config", metavar="FILE")
        p.add_argument("--state-dir", default=str(Path.home() / ".reqflow-scheduler"))
        if name == "recover":
            p.add_argument("--inspected", action="store_true")
        if name == "feedback":
            source = p.add_mutually_exclusive_group(required=True)
            source.add_argument("--event", help="version 1 Scheduler feedback JSON")
            source.add_argument("--completion", help="existing Agenthook completion.json and sibling artifacts")
            p.add_argument("--node", help="target node for Agenthook completion (default: root)")
        if name in {"watch", "submit", "continue", "view"}:
            p.add_argument("--interval-seconds", type=int)
        if name == "watch":
            p.add_argument("--background-ready", help=argparse.SUPPRESS)
        if name in {"view", "continue", "stop"}:
            p.add_argument("--task-dir", default=str(DEFAULT_TASK_DIR), help="directory to search for task IDs")
        if name == "view":
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--watch", dest="watch", action="store_true", default=True,
                              help="refresh until Ctrl+C (default); never dispatch")
            mode.add_argument("--once", dest="watch", action="store_false", help="print one snapshot and exit")
    args = parser.parse_args(argv)
    try:
        if args.action == "list":
            print(render_tasks(args.task_dir, args.state_dir, include_completed=args.all))
            return 0
        if args.action == "rm":
            result = remove_task(args.task_id, args.task_dir, args.state_dir, regex=args.regex)
            print(json.dumps(result, ensure_ascii=False))
            return 3 if result.get("skipped") else 0
        if args.action == "stop":
            print(json.dumps(request_stop(args.task, args.task_dir, args.state_dir), ensure_ascii=False))
            return 0
        config = load_config(args.config)
        if args.action == "config":
            print(json.dumps(config, ensure_ascii=False, indent=2))
            return 0
        task_path = resolve_task(args.task, args.task_dir, args.state_dir) if args.action in {"view", "continue"} else args.task
        task = load_task(task_path)
        if args.config:
            task.update({key: value for key, value in config.items() if key != "interval_seconds"})
        if args.action in {"watch", "submit", "continue", "view"}:
            default_interval = 2 if args.action == "view" and not args.config else config["interval_seconds"]
            args.interval_seconds = args.interval_seconds if args.interval_seconds is not None else default_interval
        folder, state_path, stop_path = paths(task, args.state_dir)
        if args.action == "models":
            routes, errors = {}, {}
            for stage in routing.STAGES:
                try:
                    routes[stage] = routing.resolve(task, stage)
                except (ValueError, OSError) as exc:
                    errors[stage] = str(exc)
            print(json.dumps(dict(status="blocked" if errors else "validated", routes=routes, errors=errors), ensure_ascii=False, indent=2))
            return 2 if errors else 0
        if args.action == "usage":
            print(json.dumps(stages.usage_summary(read(state_path, {})), ensure_ascii=False, indent=2))
            return 0
        if args.action == "feedback":
            event = read(Path(args.event)) if args.event else stages.agenthook_event(task, args.completion, args.node)
            result = stages.feedback(task, args.state_dir, event)
            print(json.dumps(dict(id=task["id"], status=result["status"], reason=result.get("reason")), ensure_ascii=False))
            return 0
        if args.action in {"watch", "submit", "continue", "view"} and args.interval_seconds < 1:
            raise ValueError("interval must be positive")
        if args.action in {"submit", "continue"}:
            with lock("background:" + str(state_path)):
                record_path = folder / "submit.json"
                if args.action == "submit":
                    state = read(state_path, {})
                    changed = bool(state.get("fingerprint") and state["fingerprint"] != fingerprint(task))
                    if changed:
                        validate_extension(task, state)
                    if state.get("status") in {"interrupted", "stopped"} or stop_path.exists():
                        raise ValueError("task requires recovery; inspect it and use continue")
                    background = read(record_path, {})
                    if background.get("pid") and process_running(background["pid"]):
                        if changed:
                            raise Busy("wait for the current background scheduler to exit before appending")
                        print(json.dumps({"id": task["id"], "status": state.get("status", "starting"),
                            "scheduler_pid": background["pid"], "already_running": True}), flush=True)
                        return 0
                    if state.get("status") == "running":
                        raise ValueError("task has a running checkpoint; inspect it and use continue")
                    if state.get("status") == "completed" and not changed:
                        print(json.dumps({"id": task["id"], "status": "completed"}), flush=True)
                        return 0
                else:
                    state = read(state_path, {})
                    if state.get("fingerprint") and state["fingerprint"] != fingerprint(task):
                        raise ValueError("task changed after dispatch")
                    background = read(record_path, {})
                    if background.get("pid") and process_running(background["pid"]) and not stop_path.exists():
                        print(json.dumps({"id": task["id"], "status": state.get("status", "starting"),
                            "scheduler_pid": background["pid"], "already_running": True}), flush=True)
                        return 2 if state.get("status") in {"interrupted", "stopped"} else 0
                    if state.get("status") == "completed":
                        print(json.dumps({"id": task["id"], "status": "completed"}), flush=True)
                        return 0
                    continue_task(task, args.state_dir)
                folder.mkdir(parents=True, exist_ok=True)
                ready = folder / ("." + args.action + "-" + uuid.uuid4().hex + ".json")
                log_path = folder / (args.action + ".log")
                command = [sys.executable, "-m", "reqflow_scheduler", "watch", str(Path(task_path).resolve()),
                           "--state-dir", str(Path(args.state_dir).resolve()),
                           "--interval-seconds", str(args.interval_seconds), "--background-ready", str(ready)]
                if args.config:
                    command += ["--config", str(Path(args.config).resolve())]
                options = {"creationflags": subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
                with log_path.open("ab") as log:
                    process = subprocess.Popen(command, cwd=Path(__file__).resolve().parent.parent,
                        stdin=subprocess.DEVNULL, stdout=log, stderr=log, **options)
                write(record_path, {"pid": process.pid, "task": str(Path(task_path).resolve())})
                try:
                    while True:
                        result = read(ready)
                        if result:
                            result.update(scheduler_pid=process.pid, log=str(log_path))
                            print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
                            failed = {"interrupted", "stopped"} | ({"blocked"} if args.action == "continue" else set())
                            return 2 if result["status"] in failed else 0
                        if process.poll() is not None:
                            raise ValueError(f"background scheduler exited ({process.returncode}); see {log_path}")
                        time.sleep(0.1)
                finally:
                    ready.unlink(missing_ok=True)
        ready_path = getattr(args, "background_ready", None)

        def acknowledge(result):
            nonlocal ready_path
            if ready_path:
                write(Path(ready_path), {"id": task["id"], **result})
                ready_path = None

        while True:
            if args.action in {"tick", "watch"}:
                if ready_path:
                    result = tick(task, args.state_dir, on_running=lambda nodes: acknowledge({"status": "running", "running_nodes": nodes}))
                    acknowledge({"status": result.get("status"), "reason": result.get("reason"), "running_nodes": []})
                else:
                    result = tick(task, args.state_dir)
            elif args.action == "quota":
                result = quota_adapter.query(task, datetime.now(timezone.utc))
            elif args.action == "recover":
                result = recover(task, args.state_dir, args.inspected)
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
