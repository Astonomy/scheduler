"""Run Codex without a shell and checkpoint its native session identifier."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time


def executable(task):
    executable = list(task.get("codex", ["codex"]))
    # Windows npm shims are .cmd files. Resolve the installed JS entrypoint so
    # prompts never cross cmd.exe's command-string parser.
    resolved = shutil.which(executable[0]) or executable[0]
    if os.name == "nt" and Path(resolved).suffix.lower() in {".cmd", ".bat"}:
        sibling = Path(resolved).parent / "node_modules" / "@openai" / "codex" / "bin" / "codex.js"
        if not sibling.is_file():
            volta = Path(os.environ.get("VOLTA_HOME", str(Path(resolved).parent.parent)))
            sibling = volta / "tools/image/packages/@openai/codex/node_modules/@openai/codex/bin/codex.js"
        if not sibling.is_file():
            raise ValueError("set codex to [node.exe, absolute/path/to/codex.js]; .cmd shims are unsupported")
        executable = [shutil.which("node") or "node", str(sibling), *executable[1:]]
    else:
        executable[0] = resolved
    return executable


def command(task, session=None):
    # Profile is a top-level option: exec resume does not expose --profile.
    from .routing import arguments
    route_args = arguments(task["route"]) if task.get("route") else []
    args = [*executable(task), *route_args, "exec", "--json", "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check"]
    if task.get("schema_path"):
        args += ["--output-schema", str(task["schema_path"])]
    if session:
        args += ["resume", session, "-"]
    else:
        args += ["-"]
    return args


def terminate(process):
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True, check=False,
                       creationflags=subprocess.CREATE_NO_WINDOW)
    else:
        import signal
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        if os.name != "nt":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait()


def run(task, state, save, run_dir, stop_file):
    started = time.monotonic()
    args = command(task, state.get("session_id"))
    run_dir.mkdir(parents=True, exist_ok=True)
    state.update(status="running", reason=None, command=args, usage=None, final_message=None,
                 actual_model=None, tool_results=[], codex_error=None)
    save(state)  # If killed during launch, recover requires explicit operator inspection.
    completed = False
    stream_error = []
    with (run_dir / "stderr.log").open("ab") as errors:
        process = subprocess.Popen(args, cwd=task["workspace"], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=errors,
                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                                   start_new_session=os.name != "nt")
        state["pid"] = process.pid
        save(state)

        def consume():
            nonlocal completed
            try:
                with (run_dir / "events.jsonl").open("ab", buffering=0) as log:
                    for line in iter(process.stdout.readline, b""):
                        log.write(line)
                        os.fsync(log.fileno())
                        try:
                            event = json.loads(line)
                        except (ValueError, UnicodeDecodeError):
                            continue
                        if event.get("type") == "thread.started" and event.get("thread_id"):
                            state["session_id"] = event["thread_id"]
                            save(state)
                        if event.get("type") == "turn.completed":
                            completed = True
                            state["usage"] = event.get("usage")
                        item = event.get("item", {})
                        if event.get("type") == "item.completed" and item.get("type") == "agent_message":
                            state["final_message"] = item.get("text")
                        if event.get("type") == "item.completed" and item.get("type") == "command_execution":
                            state["tool_results"].append({k: item.get(k) for k in ("command", "exit_code", "aggregated_output")})
                        if event.get("model"):
                            state["actual_model"] = event["model"]
                        if event.get("type") in {"error", "turn.failed"}:
                            error = str(event.get("error", event.get("message", "")))
                            if re.search(r"unknown model|model.{0,80}(not found|unavailable|not supported|does not exist)", error, re.I):
                                state["codex_error"] = "Codex rejected the configured model; no fallback (see events.jsonl and stderr.log)"
                            elif re.search(r"credential|api.key|environment variable|authentication|unauthorized", error, re.I):
                                state["codex_error"] = "Codex provider authentication failed (see events.jsonl and stderr.log)"
            except Exception as exc:
                stream_error.append(str(exc))

        reader = threading.Thread(target=consume, daemon=True)
        reader.start()
        def feed():
            try:
                process.stdin.write(task["prompt"].encode("utf-8"))
            except (BrokenPipeError, OSError):
                pass
            finally:
                process.stdin.close()

        writer = threading.Thread(target=feed, daemon=True)
        writer.start()
        try:
            deadline = time.monotonic() + task.get("timeout_seconds", 3600)
            while process.poll() is None:
                if stop_file.exists() or time.monotonic() >= deadline:
                    terminate(process)
                    state["reason"] = "stopped" if stop_file.exists() else "timeout"
                    break
                time.sleep(0.1)
        except BaseException:
            terminate(process)
            raise
        finally:
            writer.join(timeout=10)
            reader.join(timeout=10)
            if reader.is_alive():
                stream_error.append("event stream did not close")
            else:
                process.stdout.close()
            state["exit_code"] = process.poll()
            state["pid"] = None
    state["status"] = "completed" if process.returncode == 0 and completed and not stream_error and not state.get("reason") else "interrupted"
    state["elapsed_seconds"] = time.monotonic() - started
    if state["status"] != "completed" and not state.get("reason"):
        state["reason"] = "; ".join(stream_error) or state.get("codex_error") or "Codex failed or no turn.completed event; see events.jsonl and stderr.log"
    save(state)
    return state
