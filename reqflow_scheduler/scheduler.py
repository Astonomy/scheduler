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
    return task


def nodes(node):
    if not isinstance(node, dict) or not isinstance(node.get("children", []), list):
        raise ValueError("tree nodes must be objects with a children array")
    yield node
    for child in node.get("children", []):
        yield from nodes(child)


def fingerprint(task):
    # Only execution content matters; legacy readiness fields are ignored.
    content = {key: task.get(key) for key in ("workspace", "tree", "codex", "timeout_seconds")}
    return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


def paths(task, root):
    folder = Path(root).resolve() / task["id"]
    return folder, folder / "state.json", folder / "stop"


def sleeping(now):
    return 1 <= now.astimezone(DEFAULT_TIMEZONE).hour < 9


def tick(task, root, now=None):
    # Injected time is fixed for tests; real runs recheck time after each node.
    clock = (lambda: now) if now is not None else (lambda: datetime.now(timezone.utc))
    folder, state_path, stop_path = paths(task, root)
    with lock("task:" + str(state_path)), lock("workspace:" + os.path.normcase(task["workspace"])):
        state = read(state_path, {"id": task["id"], "status": "queued", "nodes": {}})
        if state.get("fingerprint") and state["fingerprint"] != fingerprint(task):
            raise ValueError("task tree changed after dispatch; use a new task id")
        if "nodes" not in state:
            raise ValueError("legacy execution state: use a new task id for the task tree")
        save = lambda: write(state_path, state)

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
            node = next((n for n in nodes(task["tree"]) if state["nodes"].get(n["id"], {}).get("status") != "completed"), None)
            if node is None:
                return wait("all nodes completed", status="completed")
            state["current_node"] = node["id"]
            ns = state["nodes"].setdefault(node["id"], dict(status="queued", attempts=0))
            if ns["status"] in {"running", "interrupted", "stopped"}:
                return wait(ns.get("reason") or "inspect interrupted node before recovery", status=ns["status"])
            if task.get("not_before") and current_time < quota_adapter.timestamp(task["not_before"]):
                return wait("not due", task["not_before"])
            due = state.get("next_check_at")
            if due and current_time < quota_adapter.timestamp(due):
                # Entering sleep overrides a daytime window delay, never a quota reset delay.
                if not sleeping(current_time) or state.get("reason") not in {"outside daytime reset window", "current node already submitted in this daytime window"}:
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
            night = sleeping(current_time)
            if not night:
                window = reset - timedelta(minutes=30)
                if current_time < window:
                    return wait("outside daytime reset window", window.isoformat())
                if state.get("daytime_window") == quota["resets_at"]:
                    return wait("current node already submitted in this daytime window", quota["resets_at"])
            ns.update(attempts=ns["attempts"] + 1, started_at=current_time.isoformat())
            state.update(status="running", fingerprint=fingerprint(task), next_check_at=None, reason=None)
            if not night:
                state["daytime_window"] = quota["resets_at"]
            run_task = {**task, "prompt": "继续" if ns.get("session_id") else node["prompt"]}
            if not ns.get("session_id") and node.get("session_id"):
                ns["session_id"] = node["session_id"]
            save()
            try:
                dispatch.run(run_task, ns, lambda value: save(), folder / "runs" / node["id"] / str(ns["attempts"]), stop_path)
            except (Exception, KeyboardInterrupt) as exc:
                ns.update(status="interrupted", reason=str(exc) or type(exc).__name__, pid=None)
                save()
            if ns["status"] != "completed":
                if ns.get("reason") not in {"timeout", "stopped"}:
                    try:
                        quota = quota_adapter.query(task, clock())
                        state["quota"] = quota
                        if quota["blocked_until"] and ns.get("session_id"):
                            ns.update(status="queued", reason="quota exhausted; resume this session after reset")
                            return wait_quota(quota)
                    except (ValueError, OSError, KeyError, TypeError):
                        pass
                return wait(ns.get("reason") or "node interrupted", status="interrupted")
            save()
            # Re-evaluate time and quota before dispatching the next child/sibling.


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
        ns = state["nodes"].setdefault(state["current_node"], dict(status="queued", attempts=0))
        if ns["status"] != "completed":
            if not ns.get("session_id") and ns.get("attempts", 0):
                raise ValueError("no durable session id; inspect logs before creating a new task")
            ns.update(status="queued", reason=None, pid=None)
        stop_path.unlink(missing_ok=True)
        state.update(status="queued", reason="operator inspected recovery", next_check_at=None)
        write(state_path, state)
        return state
