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

## 同步与保存

Scheduler 的通用 submit 入口及测试应作为 Git 提交保留；外置组件和私有状态保留在仓库外。更新远程代码时合并或 rebase 该功能分支。若切换到尚未包含该提交的 main，需先合入功能提交再启动外置回调。

不使用 assume-unchanged/skip-worktree 隐藏代码改动，不修改远程同步设置，也不提交网站会话或真实凭证。仓库外还可保存 `git format-patch` 备份，以便重新检出后通过 `git am` 恢复。
