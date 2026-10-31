from concurrent.futures import ThreadPoolExecutor
import threading
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re

from . import dispatch, quota_adapter
from .state_store import lock, read, write


# A fixed UTC+8 default works on Windows without a third-party timezone database.
DEFAULT_TIMEZONE = timezone(timedelta(hours=8))


def load_task(path):
    path = Path(path).resolve()
    task = read(path)
    if not isinstance(task, dict) or task.get("version") != 1:
        raise ValueError("task version must be 1")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", task.get("id", "")):
        raise ValueError("invalid task id")
    if not isinstance(task.get("workspace"), str) or not task["workspace"].strip():
        raise ValueError("workspace must be a nonempty string")
    task["workspace"] = str((path.parent / task["workspace"]).resolve())
    if "tree" not in task:
        task["tree"] = dict(id="root", prompt=task.get("prompt"))
    seen = set()
    for node in nodes(task["tree"]):
        identifier = node.get("id", "")
        if not isinstance(identifier, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", identifier) or identifier in seen:
            raise ValueError("node ids must be unique, simple identifiers")
        seen.add(identifier)
        if not isinstance(node.get("prompt"), str) or not node["prompt"].strip():
            raise ValueError("node prompt must be a nonempty string")
    timeout = task.get("timeout_seconds", 3600)
    if isinstance(timeout, bool) or not isinstance(timeout, (float, int)) or not math.isfinite(timeout) or not 1 <= timeout <= 86400:
        raise ValueError("invalid timeout_seconds")
    if "codex" in task and (not isinstance(task["codex"], list) or not task["codex"] or not all(isinstance(s, str) and s for s in task["codex"])):
        raise ValueError("codex must be a nonempty argv array")
    if task.get("not_before"):
        quota_adapter.timestamp(task["not_before"])
    if task.get("schedule_policy", "windowed") not in {"windowed", "continuous"}:
        raise ValueError("schedule_policy must be windowed or continuous")
    if not isinstance(task.get("allow_append", False), bool):
        raise ValueError("allow_append must be a boolean")
    return task


def nodes(node):
    for level in levels(node):
        yield from level


def levels(root):
    level = [root]
    while level:
        following = []
        for node in level:
            if not isinstance(node, dict) or not isinstance(node.get("children", []), list):
                raise ValueError("tree nodes must be objects with a children array")
            following.extend(node.get("children", []))
        yield level
        level = following


def fingerprint(task):
    # Only execution content matters; schedule_policy is a mutable dispatch setting.
    content = {key: task.get(key) for key in ("workspace", "tree", "codex", "timeout_seconds")}
    return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


def validate_extension(task, state):
    """Allow opted-in completed trees to grow only below their deepest level."""
    previous = state.get("execution_task")
    if not task.get("allow_append") or not previous or not previous.get("allow_append"):
        raise ValueError("task tree changed after dispatch; use a new task id")
    if state.get("status") != "completed":
        raise ValueError("finish the current tree before appending levels")
    for key in ("workspace", "codex", "timeout_seconds"):
        if task.get(key) != previous.get(key):
            raise ValueError(f"cannot change dispatched execution setting: {key}")
    old_levels, new_levels = list(levels(previous["tree"])), list(levels(task["tree"]))
    if len(new_levels) <= len(old_levels):
        raise ValueError("append must add a deeper level")
    for depth, old_level in enumerate(old_levels):
        if len(old_level) != len(new_levels[depth]):
            raise ValueError("cannot add nodes to existing levels")
        for old, new in zip(old_level, new_levels[depth]):
            if state.get("nodes", {}).get(old["id"], {}).get("status") != "completed":
                raise ValueError("finish every existing node before appending levels")
            if {k: v for k, v in old.items() if k != "children"} != {k: v for k, v in new.items() if k != "children"}:
                raise ValueError("cannot change dispatched node content or order")
            if depth < len(old_levels) - 1 and [n["id"] for n in old.get("children", [])] != [n["id"] for n in new.get("children", [])]:
                raise ValueError("cannot reparent nodes or extend shallower leaves")


def execution_snapshot(task):
    """Detach the accepted execution definition from mutable caller objects."""
    return json.loads(json.dumps({key: task.get(key) for key in
        ("workspace", "tree", "codex", "timeout_seconds", "allow_append")}))


def paths(task, root):
    folder = Path(root).resolve() / task["id"]
    return folder, folder / "state.json", folder / "stop"


def sleeping(now, task=None):
    task = task or {}
    start, end = task.get("night_start_hour", 1), task.get("night_end_hour", 9)
    hour = now.astimezone(DEFAULT_TIMEZONE).hour
    return start <= hour < end if start < end else hour >= start or hour < end


def tick(task, root, now=None):
    # Injected time is fixed for tests; real runs recheck time after each node.
    clock = (lambda: now) if now is not None else (lambda: datetime.now(timezone.utc))
    folder, state_path, stop_path = paths(task, root)
    with lock("task:" + str(state_path)), lock("workspace:" + os.path.normcase(task["workspace"])):
        state = read(state_path, {"id": task["id"], "status": "queued", "nodes": {}})
        if state.get("fingerprint") and state["fingerprint"] != fingerprint(task):
            validate_extension(task, state)
            state.update(fingerprint=fingerprint(task), execution_task=execution_snapshot(task))
        if "nodes" not in state:
            raise ValueError("legacy execution state: use a new task id for the task tree")
        save_lock = threading.Lock()

        def save():
            with save_lock:
                snapshot = {**state, "nodes": {key: value.copy() for key, value in state["nodes"].items()}}
                write(state_path, snapshot)

        def wait(reason, until=None, status="waiting"):
            state.update(status=status, reason=reason, next_check_at=until, checked_at=clock().isoformat())
            save()
            return state

        def wait_quota(quota):
            if state.get("quota_wait_until") != quota["blocked_until"]:
                state["quota_reached_at"] = clock().isoformat()
            state["quota_wait_until"] = quota["blocked_until"]
            return wait("quota exhausted", quota["blocked_until"])

        while True:
            current_time = clock()
            if stop_path.exists():
                return wait("stop requested", status="stopped")
            batch = next(([n for n in level if state["nodes"].get(n["id"], {}).get("status") != "completed"]
                          for level in levels(task["tree"])
                          if any(state["nodes"].get(n["id"], {}).get("status") != "completed" for n in level)), [])
            if not batch:
                return wait("all nodes completed", status="completed")
            state["current_node"] = batch[0]["id"]
            state["current_nodes"] = [node["id"] for node in batch]
            for node in batch:
                ns = state["nodes"].setdefault(node["id"], dict(status="queued", attempts=0))
                if ns["status"] in {"running", "interrupted", "stopped"}:
                    return wait(ns.get("reason") or "inspect interrupted node before recovery", status=ns["status"])
            if task.get("not_before") and current_time < quota_adapter.timestamp(task["not_before"]):
                return wait("not due", task["not_before"])
            due = state.get("next_check_at")
            if due and current_time < quota_adapter.timestamp(due):
                # Entering sleep overrides a daytime window delay, never a quota reset delay.
                if not (sleeping(current_time, task) or task.get("schedule_policy") == "continuous") or state.get("reason") not in {"outside daytime reset window", "current node already submitted in this daytime window"}:
                    return state
            try:
                quota = quota_adapter.query(task, current_time)
            except (ValueError, OSError, KeyError, TypeError) as exc:
                return wait(str(exc), (current_time + timedelta(minutes=1)).isoformat(), "blocked")
            state["quota"] = quota
            if quota["blocked_until"]:
                return wait_quota(quota)
            state.pop("quota_wait_until", None)
            reset = quota_adapter.timestamp(quota["resets_at"])
            night = sleeping(current_time, task) or task.get("schedule_policy") == "continuous"
            if not night:
                window = reset - timedelta(minutes=task.get("reset_lead_minutes", 30))
                if current_time < window:
                    return wait("outside daytime reset window", window.isoformat())
                if state.get("daytime_window") == quota["resets_at"]:
                    return wait("current node already submitted in this daytime window", quota["resets_at"])
            state.update(status="running", fingerprint=fingerprint(task), next_check_at=None, reason=None)
            if task.get("allow_append"):
                state["execution_task"] = execution_snapshot(task)
            if not night:
                state["daytime_window"] = quota["resets_at"]
            for node in batch:
                ns = state["nodes"][node["id"]]
                ns.update(status="running", attempts=ns["attempts"] + 1, started_at=current_time.isoformat())
            save()

            def run_node(node):
                ns = state["nodes"][node["id"]]
                run_task = {**task, "prompt": "继续" if ns.get("session_id") else node["prompt"]}
                if not ns.get("session_id") and node.get("session_id"):
                    ns["session_id"] = node["session_id"]
                try:
                    dispatch.run(run_task, ns, lambda value: save(), folder / "runs" / node["id"] / str(ns["attempts"]), stop_path)
                except (Exception, KeyboardInterrupt) as exc:
                    ns.update(status="interrupted", reason=str(exc) or type(exc).__name__, pid=None)
                    save()

            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                try:
                    list(executor.map(run_node, batch))
                except KeyboardInterrupt:
                    stop_path.touch()
                    raise
            failed = [state["nodes"][node["id"]] for node in batch
                      if state["nodes"][node["id"]]["status"] != "completed"]
            if failed:
                if all(ns.get("reason") not in {"timeout", "stopped"} for ns in failed):
                    try:
                        quota = quota_adapter.query(task, clock())
                        state["quota"] = quota
                        if quota["blocked_until"] and all(ns.get("session_id") for ns in failed):
                            for ns in failed:
                                ns.update(status="queued", reason="quota exhausted; resume this session after reset")
                            return wait_quota(quota)
                    except (ValueError, OSError, KeyError, TypeError):
                        pass
                return wait(failed[0].get("reason") or "node interrupted", status="interrupted")
            save()
            # Each level is a barrier; recheck quota before starting the next.


def recover(task, root, inspected=False):
    folder, state_path, stop_path = paths(task, root)
    with lock("task:" + str(state_path)), lock("workspace:" + os.path.normcase(task["workspace"])):
        state = read(state_path)
        if not state or state["status"] not in {"running", "interrupted", "stopped"}:
            raise ValueError("task has no interrupted run")
        if state.get("fingerprint") and state["fingerprint"] != fingerprint(task):
            raise ValueError("task changed after dispatch")
        if not inspected:
            raise ValueError("inspect child processes, then pass --inspected")
        if not state.get("current_node"):
            state["current_node"] = task["tree"]["id"]
        pending = [ns for ns in state["nodes"].values() if ns["status"] != "completed"]
        if any(not ns.get("session_id") and ns.get("attempts", 0) for ns in pending):
            raise ValueError("no durable session id; inspect logs before creating a new task")
        for ns in pending:
            ns.update(status="queued", reason=None, pid=None)
        stop_path.unlink(missing_ok=True)
        state.update(status="queued", reason="operator inspected recovery", next_check_at=None)
        write(state_path, state)
        return state
