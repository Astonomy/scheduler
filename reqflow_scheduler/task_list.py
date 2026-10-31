"""Read task definitions and checkpoints without dispatching work."""
from pathlib import Path
import sys

from .scheduler import load_task, nodes
from .state_store import read


DEFAULT_TASK_DIR = Path(__file__).resolve().parent.parent / "config"


def resolve_task(reference, task_dir):
    path = Path(reference)
    if path.is_file():
        return path
    if path.name != reference or path.suffix == ".json":
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
        raise ValueError(f"Task ID not found: {reference} in {task_dir}; use --task-dir or pass a task file path")
    return matches[0]


def render_tasks(task_dir, state_dir):
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
        task, path = tasks.get(identifier, (None, None))
        try:
            state = read(checkpoints[identifier]) if identifier in checkpoints else {}
            completed = sum(node.get("status") == "completed" for node in state.get("nodes", {}).values())
            total = str(sum(1 for _ in nodes(task["tree"]))) if task else "?"
            lines.append(f"{identifier} | {state.get('status', 'queued')} | "
                         f"{completed}/{total} completed | current={state.get('current_node') or '-'}")
            lines.append(f"  task: {path or '(definition not found in task directory)'}")
            if identifier in checkpoints:
                lines.append(f"  state: {checkpoints[identifier].resolve()}")
        except (ValueError, OSError, KeyError, TypeError, AttributeError) as exc:
            lines.append(f"{identifier} | error: {exc}")
    return "\n".join(lines) or "No tasks found."
