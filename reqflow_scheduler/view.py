"""Read-only terminal task tree rendering."""
import time


def render_tree(task, state):
    spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"[int(time.monotonic() * 10) % 10]

    def line(value):
        return " ".join(str(value).split())

    lines = [f"Task {task['id']} | {state.get('status', 'queued')}",
             f"Workspace: {line(task['workspace'])}"]
    for key in ("reason", "next_check_at"):
        if state.get(key):
            lines.append(f"{key}: {line(state[key])}")
    lines.append("")

    def visit(node, prefix="", branch="", following=""):
        status = state.get("nodes", {}).get(node["id"], {}).get("status", "queued")
        marker = "✓" if status == "completed" else spinner if status == "running" else "-"
        lines.append(f"{prefix}{branch}{node['id']} {marker}")
        children = node.get("children", [])
        for index, child in enumerate(children):
            last = index == len(children) - 1
            visit(child, prefix + following, "`-- " if last else "|-- ", "    " if last else "|   ")

    visit(task["tree"])
    return "\n".join(lines)
