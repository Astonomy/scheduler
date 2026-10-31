---
name: scheduler
description: Schedule and resume Codex task trees for any project using sleep hours and automatically fetched account quota. Use for task-tree scheduling, dispatch, quota, stop and recovery; do not gate ordinary development requests.
---

# Codex task-tree scheduler

Read `D:/code/scheduler/README.md` (WSL: `/mnt/d/code/scheduler/README.md`) for the current task format and commands.

This is a general task submission scheduler, not a CCE/ARC requirement gate. Do not require scope approval, readiness files, requirement hashes, a clean Git checkout or manual quota confirmation. Do not intercept unrelated development requests.

Use native Windows Python `D:/code/scheduler/.venv/Scripts/python.exe` or the existing WSL environment `/home/ssh/.cache/reqflow-arc-venv/bin/python`, from the scheduler repository. Use only one OS/runtime per workspace.

1. Create a version 1 task JSON with a unique id, workspace and tree (node id, prompt, children). Reuse an existing matching task rather than duplicate its execution. User submission authorizes executing that task tree; do not invent prompts or run the example against a real project.
2. `quota TASK` automatically reads real account quota using Codex app-server, without starting a model turn. Never estimate or fabricate quota. `status TASK` displays node checkpoints and next check time.
   `scheduler list` lists task definitions in the installation's `config` directory and checkpoints in the default state directory, without dispatching. Override locations with `--task-dir` and `--state-dir`. For global CLI access, use `uv tool install --editable D:/code/scheduler` and `uv tool update-shell`, then open a new terminal. `view` accepts task file paths or task IDs; IDs are resolved against JSON definitions in `config` (override with `--task-dir`).
   Optional skill actions: `scheduler view TASK` displays a read-only TUI task tree (all nodes with ✓ completed, - unfinished, animated spinner running), refreshing until Ctrl+C; `--once` prints one snapshot. `--watch` remains supported. It never dispatches tasks or queries quota. Use this action when the user asks to view the task tree.
   `config --config FILE.toml` validates and displays execution policy; `config` shows defaults. Pass `--config FILE.toml` to task commands to apply it. See `config/scheduler.example.toml`: `[scheduler]` supports `schedule_policy` (`windowed`/`continuous`), Beijing `night_start_hour`/`night_end_hour` (including overnight ranges), `reset_lead_minutes`, and `interval_seconds`. Explicit TOML overrides task policy; `--interval-seconds` overrides TOML polling. Without `--config`, existing task policy and defaults apply. Offer these options when configuring execution strategy.
3. `watch TASK` polls every 60 seconds. `tick TASK` performs one scheduling pass. Beijing 01:00 inclusive to 09:00 exclusive: run from root level by level, all same-depth nodes in parallel, waiting for the full level before the next while quota remains. Otherwise submit only the current level in the 30 minutes before the five-hour window reset, at most once per window.
4. At a quota limit, retain the current session and node, wait for the reported reset and requery. Resume that exact node with `继续` only when actual quota permits. A longer exhausted window can delay recovery beyond five hours.
5. `stop TASK` stops dispatch and terminates the running child on its next poll. Ordinary failures and timeouts require checking child processes, then `recover TASK --inspected`. Never resume with `--last` or discard a missing session checkpoint.

State and logs belong outside the target workspace. Completion only means Codex finished a turn successfully, not business acceptance. Legacy single-prompt task files still load; legacy execution checkpoints require a new task id. Changed dispatched tree content also requires a new id.

Child Codex sessions (including resumes) run with full access, without sandboxing or approval prompts, inheriting the scheduler process's OS permissions and credentials.

Register OS scheduling only when requested. Plugin installation alone does not start an example task or alter hook trust.
