"""List and remove task definitions and saved execution records."""
from contextlib import ExitStack
from fnmatch import fnmatchcase
from pathlib import Path
import re
import shutil
import sys

from .scheduler import load_task, nodes, process_running
from .state_store import Busy, lock, read


DEFAULT_TASK_DIR = Path(__file__).resolve().parent.parent / "config"


def submitted_task(reference, state_dir):
    """Find an external definition recorded by submit/continue."""
    try:
        record = read(Path(state_dir) / reference / "submit.json", {})
        path = Path(record["task"])
        definition = read(path, {})
        if definition.get("id") == reference:
            return path
    except (ValueError, OSError, KeyError, TypeError, AttributeError):
        pass
    return None


def resolve_task(reference, task_dir, state_dir=None):
    path = Path(reference)
    if path.is_file():
        return path
    if path.name != reference or path.suffix == ".json" or reference in {".", ".."}:
        raise ValueError(f"Task file not found: {reference}")
    matches = []
    for candidate in sorted(Path(task_dir).glob("*.json")):
        try:
            definition = read(candidate)
        except (ValueError, OSError):
            continue
        if isinstance(definition, dict) and definition.get("id") == reference:
            matches.append(candidate)
    if len(matches) > 1:
        raise ValueError(f"Multiple definitions for task {reference}; pass an explicit task file path")
    if not matches:
        if state_dir is not None:
            submitted = submitted_task(reference, state_dir)
            if submitted:
                return submitted
        raise ValueError(f"Task ID not found: {reference} in {task_dir}; use --task-dir or pass a task file path")
    return matches[0]


def remove_task(pattern, task_dir, state_dir, *, regex=False):
    """Permanently delete matching task definitions and their state folders."""
    root = Path(state_dir).resolve()
    try:
        matches = re.compile(pattern).fullmatch if regex else lambda identifier: fnmatchcase(identifier, pattern)
    except re.error as exc:
        raise ValueError(f"Invalid ID regex: {exc}") from exc
    definitions = {}
    for candidate in Path(task_dir).glob("*.json"):
        if candidate.name.endswith(".example.json"):
            continue
        try:
            definition = read(candidate)
        except (ValueError, OSError):
            continue
        identifier = definition.get("id") if isinstance(definition, dict) else None
        if isinstance(identifier, str):
            definitions.setdefault(identifier, set()).add(candidate.resolve())
    known = set(definitions)
    for name in ("state.json", "submit.json", "removed"):
        known.update(path.parent.name for path in root.glob("*/" + name))
    identifiers = sorted(identifier for identifier in known
                         if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", identifier)
                         and matches(identifier))
    if not identifiers:
        raise ValueError(f"No task IDs match: {pattern}; use --task-dir or --state-dir")
    removed, skipped = [], []
    for identifier in identifiers:
        try:
            folder = root / identifier
            if folder.resolve().parent != root or folder.is_symlink():
                raise ValueError(f"Task state directory escapes state root: {folder}")
            with ExitStack() as locks:
                locks.enter_context(lock("background:" + str(folder / "state.json")))
                locks.enter_context(lock("task:" + str(folder / "state.json")))
                state = read(folder / "state.json", {})
                background = read(folder / "submit.json", {})
                pids = [background.get("pid"), *(node.get("pid") for node in state.get("nodes", {}).values())]
                if any(pid and process_running(pid) for pid in pids):
                    raise Busy(f"Task {identifier} is still running; stop it and wait for exit before rm")
                if any(node.get("status") == "running" and not node.get("pid")
                       for node in state.get("nodes", {}).values()):
                    raise Busy(f"Task {identifier} has a running node without a PID; inspect child processes before rm")
                submitted = submitted_task(identifier, root)
                if submitted:
                    definitions.setdefault(identifier, set()).add(submitted.resolve())
                for definition in definitions.get(identifier, ()):
                    definition.unlink(missing_ok=True)
                if folder.exists():
                    shutil.rmtree(folder)
                removed.append(identifier)
        except (Busy, ValueError, OSError, KeyError, TypeError, AttributeError) as exc:
            skipped.append(dict(id=identifier, reason=str(exc)))
    result = dict(status="removed", ids=removed)
    if skipped:
        result.update(status="partial" if removed else "skipped", skipped=skipped)
    return result


def request_stop(reference, task_dir, state_dir):
    """Request a stop even when only a checkpoint, not a definition, survives."""
    path = Path(reference)
    from_file = path.is_file()
    identifier = read(path).get("id") if from_file else reference
    if not isinstance(identifier, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", identifier):
        raise ValueError("stop requires a task ID or task definition file")
    root = Path(state_dir).resolve()
    folder = root / identifier
    if folder.resolve().parent != root or folder.is_symlink():
        raise ValueError(f"Task state directory escapes state root: {folder}")
    if not from_file and not folder.is_dir():
        resolve_task(identifier, task_dir, state_dir)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "stop").touch()
    return dict(id=identifier, stop_requested=True)


def render_tasks(task_dir, state_dir, *, include_completed=False):
    tasks = {}
    for path in sorted(Path(task_dir).glob("*.json")):
        if path.name.endswith(".example.json"):
            continue
        try:
            task = load_task(path)
            tasks[task["id"]] = (task, path.resolve())
        except (ValueError, OSError, KeyError, TypeError) as exc:
            print(f"Skipping {path}: {exc}", file=sys.stderr)
    checkpoints = {path.parent.name: path for path in Path(state_dir).glob("*/state.json")}
    lines = []
    for identifier in sorted(tasks.keys() | checkpoints.keys()):
        removed = (Path(state_dir) / identifier / "removed").is_file()
        if removed and not include_completed:
            continue
        task, path = tasks.get(identifier, (None, None))
        try:
            if task is None:
                path = submitted_task(identifier, state_dir)
                if path:
                    task = load_task(path)
            state = read(checkpoints[identifier]) if identifier in checkpoints else {}
            status = state.get("status", "queued")
            # Completion archives the tree from the active list. Appended levels
            # make it active again before its next scheduling pass.
            if status == "completed":
                pending_append = task and task.get("allow_append") and any(
                    state.get("nodes", {}).get(node["id"], {}).get("status") != "completed"
                    for node in nodes(task["tree"]))
                if pending_append:
                    status = "queued"
                elif not include_completed:
                    continue
            completed = sum(node.get("status") == "completed" for node in state.get("nodes", {}).values())
            total = str(sum(1 for _ in nodes(task["tree"]))) if task else "?"
            lines.append(f"{identifier} | {status} | "
                         f"{completed}/{total} completed | current={state.get('current_node') or '-'}"
                         + (" | removed" if removed else ""))
            lines.append(f"  task: {path or '(definition not found in task directory)'}")
            if identifier in checkpoints:
                lines.append(f"  state: {checkpoints[identifier].resolve()}")
        except (ValueError, OSError, KeyError, TypeError, AttributeError) as exc:
            lines.append(f"{identifier} | error: {exc}")
    return "\n".join(lines) or "No tasks found."
