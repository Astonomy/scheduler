"""Read-only terminal task tree rendering."""
import time

from .scheduler import process_running


def render_tree(task, state):
    spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"[int(time.monotonic() * 10) % 10]
    active = set()
    unknown = set()
    for identifier, node in state.get("nodes", {}).items():
        if node.get("status") != "running":
            continue
        if node.get("pid"):
            try:
                if process_running(node["pid"]):
                    active.add(identifier)
            except OSError:
                unknown.add(identifier)
        else:
            unknown.add(identifier)
    status = state.get("status", "queued")
    if status == "running" and not active:
        status = "unknown" if unknown else "interrupted"

    def line(value):
        return " ".join(str(value).split())

    lines = [f"Task {task['id']} | {status}",
             f"Workspace: {line(task['workspace'])}"]
    if state.get("status") == "running" and not active:
        lines.append("Process status unverified" if unknown else "No running child process; checkpoint is stale")
    for key in ("reason", "next_check_at"):
        if state.get(key):
            lines.append(f"{key}: {line(state[key])}")
    lines.append("")

    def visit(node, prefix="", branch="", following=""):
        status = state.get("nodes", {}).get(node["id"], {}).get("status", "queued")
        marker = "✓" if status == "completed" else spinner if node["id"] in active else "-"
        lines.append(f"{prefix}{branch}{node['id']} {marker}")
        children = node.get("children", [])
        for index, child in enumerate(children):
            last = index == len(children) - 1
            visit(child, prefix + following, "`-- " if last else "|-- ", "    " if last else "|   ")

    visit(task["tree"])
    return "\n".join(lines)
