"""Read-only terminal task tree rendering, without terminal dependencies."""


def render_tree(task, state):
    def line(value):
        return " ".join(str(value).split())

    lines = [f"Task {task['id']} | {state.get('status', 'queued')}",
             f"Workspace: {line(task['workspace'])}"]
    for key in ("reason", "next_check_at"):
        if state.get(key):
            lines.append(f"{key}: {line(state[key])}")
    lines.append("")

    def visit(node, prefix="", branch="", following=""):
        checkpoint = state.get("nodes", {}).get(node["id"], {})
        current = ">" if state.get("current_node") == node["id"] else " "
        lines.append(f"{prefix}{branch}{current} [{checkpoint.get('status', 'queued')}] "
                     f"{node['id']} (attempts={checkpoint.get('attempts', 0)}) {line(node['prompt'])}")
        children = node.get("children", [])
        for index, child in enumerate(children):
            last = index == len(children) - 1
            visit(child, prefix + following, "`-- " if last else "|-- ", "    " if last else "|   ")

    visit(task["tree"])
    return "\n".join(lines)
