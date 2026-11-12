# 外置完成 hook

外置组件负责等待远程任务、读取结果并生成版本 1 的任务树。Scheduler 负责在正常额度与时间策略下执行这棵树。

```powershell
scheduler submit D:/code/481/agenthook/artifacts/<run-id>/scheduler-task.json
```

该命令启动独立后台 watcher，收到实际启动/等待回执后返回，不占用 Codex 对话等待。回执包含 `status`、`scheduler_pid`、`log`；等待中的任务不会被报告为已启动模型节点。新任务无需预建检查点。后台初始化失败时命令返回非零，错误指向 `submit.log`。

重复提交同一任务会复用存活的已提交 watcher；完成状态不会触发重跑。已派发内容仍受既有不可变规则约束。停止、中断或只留下 running 检查点的任务不会由 submit 自动恢复，需要检查进程后使用 continue。任务 ID 必须唯一。

`submit` 支持与 watch 相同的 `--config`、`--state-dir`、`--interval-seconds`，不绕过正常额度和时段策略。未指定 TOML 时不自动加载本机配置。已有 continue/watch/tick 行为保留。

## ARC-Bench 接入位置

- 外置源码与配置：`D:/code/481/agenthook`，不放进 scheduler 的受跟踪源码或 config 样例。
- 网站会话：agenthook 的 `.state`；API key 由外置组件读取 `ARCBENCH-KEY`。
- 完成证据与任务树：agenthook 的 `artifacts/<run-id>`。
- Scheduler 状态、回执与后台日志：默认 `~/.reqflow-scheduler/<task-id>/`。

agenthook 调用 `python -m reqflow_scheduler submit TASK` 并检查返回的 JSON 回执。优化节点修改 agent，提交节点只打包、上传并重新挂接外置 hook，随后结束该 Codex turn。等待、网页取证与触发下一轮任务由外置进程完成。

## 分阶段任务的反馈

显式启用三阶段流程的任务可使用 `scheduler feedback TASK --completion artifacts/RUN_ID/completion.json --node NODE_ID`。Scheduler 会保存测试、日志、原始 run/task ID 和上下文，并在 Verification 中独立判断 PASS/FAIL。运行期间的回调先写入持久收件箱；重复事件去重，停止或中断仍需原有恢复流程。随后 `submit TASK` 继续遵循时段和额度策略。

本机 Agenthook 新增可选 `scheduler_task` / `scheduler_node` / `scheduler_config` / `scheduler_state_dir` 配置；关联后会自动先反馈再提交原任务，通过和失败结果均送往 Verification。未关联时维持原有生成优化任务树的行为。模型路由只配置在 Scheduler。完整配置、预算、恢复和外置源码补丁见[分阶段调度说明](stage-orchestration.md)。

外置 Agenthook 的 `agenthook recover` 现可在机器重启后统一恢复 hook 与已派发的 Scheduler 任务：优先读取本地完成证据，再调用 `continue` 原任务并更新派发回执。`continue` 保存最新 watcher PID，重复恢复复用存活的 watcher，完成任务不重跑。已记录的本地文件缺失启动失败，仅在退出码非零、事件日志为空且没有会话时允许重试原阶段；其他缺失会话仍拒绝推测恢复。已有分析结果、失败调用和预算保留。

## 同步与保存

Scheduler 的通用 submit 入口及测试应作为 Git 提交保留；外置组件和私有状态保留在仓库外。更新远程代码时合并或 rebase 该功能分支。若切换到尚未包含该提交的 main，需先合入功能提交再启动外置回调。

不使用 assume-unchanged/skip-worktree 隐藏代码改动，不修改远程同步设置，也不提交网站会话或真实凭证。仓库外还可保存 `git format-patch` 备份，以便重新检出后通过 `git am` 恢复。
