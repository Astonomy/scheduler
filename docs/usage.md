# Reqflow Codex Scheduler

通用 Codex 任务树定时提交器，适用于任意项目。只维护节点顺序、会话与提交时机，不检查需求审批、ARC/CCE 工作流、文件摘要或 Git 工作区是否干净。`PLAN.md` 为旧版设计记录，不再代表当前行为。

## 调度规则

默认使用北京时间（UTC+8）：

- 睡眠时段为 **01:00（含）至 09:00（不含）**。从根节点开始，逐层执行，同层节点并行，本层全部完成后才开始下一层，每层提交前重新读取额度。子节点数量不设上限。
- 节点触限时保留当前节点及精确会话 ID，等待服务端返回的重置时间。重置后重新查询真实额度，额度恢复后在原会话发送“继续”。若周额度也耗尽，等待所有已耗尽窗口解除。
- 其他时间，只在五小时窗口**重置前 30 分钟内**提交当前节点，每个窗口最多提交一次。完成后不在同一白天窗口继续提交子节点；进入睡眠时段后自动继续。
- “提前 30 分钟”按额度窗口重置时间计算。未来何时耗尽取决于后续用量，无法事先准确预测。
- 额度来自 `codex app-server` 的 `account/rateLimits/read`，不启动模型 turn，不通过提示词询问额度。记录五小时剩余比例、重置时间和首次观测触限时间（非精确耗尽瞬间）。等待期间不重复查询，重置后不假定额度已恢复。
- 无法获取真实额度时保留任务并稍后重试。普通执行错误、超时和主动停止保留会话，需检查进程后恢复。

完成指 Codex 正常退出并产生 `turn.completed`，不代表业务验收通过。每个节点独立保存会话；可用节点的可选 `session_id` 指定已有会话，首次提交发送该节点提示词，后续续跑发送“继续”。

## 安装与使用

外置完成 hook 可用 `scheduler submit 任务文件` 提交新任务：命令在收到后台 watcher 的实际运行/等待回执后返回。
重复调用复用存活 watcher；停止/中断的任务仍通过 `continue` 恢复。`submit` 支持 `--config`、`--state-dir`、`--interval-seconds`，
日志和进程记录为任务状态目录的 `submit.log` 与 `submit.json`，不改变额度或时段策略。参见 [外置 hook](external-hooks.md)。

需要 Python 3.11+、已登录的 Codex CLI，无第三方运行依赖。

```powershell
cd D:/code/scheduler
.venv/Scripts/python.exe -m pip install -e .
.venv/Scripts/python.exe -m unittest discover -s tests -v
.venv/Scripts/python.exe -m reqflow_scheduler quota config/task.example.json
.venv/Scripts/python.exe -m reqflow_scheduler watch config/task.json
```

若虚拟环境未安装 pip，可用 `uv pip install --python .venv/Scripts/python.exe -e .`。

从 `config/task.example.json` 复制任务，设置工作区和任务树：

```json
{
  "version": 1,
  "id": "my-project-001",
  "workspace": "D:/code/my-project",
  "tree": {
    "id": "root",
    "prompt": "执行第一阶段任务",
    "children": [
      {"id": "step-2", "prompt": "执行第二阶段任务"},
      {"id": "step-3", "prompt": "执行第三阶段任务"}
    ]
  }
}
```

`workspace` 相对任务文件；节点 id 在树内唯一。旧版单个 `prompt` 自动视为根节点；旧 `requirements`、`readiness`、`quota_file`、`require_clean` 等字段不再参与检查。旧执行状态与新任务树状态不兼容，已有派发记录应使用新任务 id。已开始的树内容固定，修改执行内容应使用新 id。

可选字段：`not_before`（带时区 ISO 时间）、`timeout_seconds`（默认 3600）、`codex`（可执行文件与固定参数数组）、`quota_limit_id`（默认 `codex`）。自动识别标准 npm 和 Volta Windows shim；其他环境可显式指定 `["node", "完整路径/codex.js"]`。

```powershell
# 一次检查，睡眠时段可连续执行整棵树
.venv/Scripts/python.exe -m reqflow_scheduler tick config/task.json
# 常驻轮询，默认每 60 秒；停止/完成/普通错误后退出
.venv/Scripts/python.exe -m reqflow_scheduler watch config/task.json --interval-seconds 60
.venv/Scripts/python.exe -m reqflow_scheduler status config/task.json
.venv/Scripts/python.exe -m reqflow_scheduler stop config/task.json
# 确认原进程已退出后，恢复精确会话
.venv/Scripts/python.exe -m reqflow_scheduler recover config/task.json --inspected
# 从已有检查点恢复并持续调度（也支持任务 ID，默认查找安装目录 config）
scheduler continue config/task.json
scheduler continue my-project-001 --task-dir D:/tasks
```

`continue` 在后台续跑，确认节点启动并保存 PID 后输出运行节点并返回终端；等待额度或时间窗口时输出实际状态。后台日志写入任务状态目录的 `continue.log`，使用 `stop 任务文件` 停止。它自动检查保存的子进程 PID；原进程退出后恢复精确会话，跳过已完成节点，恢复后清除停止标记。
它要求已有任务树检查点，支持与 `watch` 相同的 `--config`、`--interval-seconds`、`--state-dir`，保留额度等待与时间策略。
原进程仍在运行、执行过的节点缺少会话 ID 时拒绝恢复；运行节点缺少 PID 时需先人工检查进程，再执行 `recover --inspected`。

`view ID` 和 `continue ID` 优先在 `--task-dir`（默认安装目录 `config`）查找定义；未找到时，
读取所选 `--state-dir` 内该任务的 `submit.json`，使用其中记录的定义路径。外部项目提交的任务因此可直接用 ID 查找，
`list` 也会显示其定义路径和节点总数。定义文件须仍存在且 ID 匹配；缺少记录的旧任务仍需提供文件路径或 `--task-dir`。

`watch` 在无人登录的后台运行需由操作系统服务或任务计划程序托管。`scripts/print-windows-task.ps1` 输出每分钟调用 `tick` 的任务 XML，可导入 Windows 任务计划程序；脚本本身不注册任务。只更新插件不会自动启动没有配置工作区和提示词的示例任务。

状态默认保存在用户目录 `.reqflow-scheduler/<id>`，可通过 `--state-dir` 指定。每个节点保留 JSONL、stderr、退出码及精确会话 ID。同任务和工作区使用操作系统锁互斥；同一工作区仅使用一个 OS/runtime，不同时在 Windows 与 WSL 中调度。

任务树全部完成后，按完成状态自动归档，默认 `scheduler list` 不再显示；`scheduler list --all` 包含归档任务。
`scheduler rm 任务ID` 彻底删除任务定义、检查点和日志，支持 `--task-dir`、`--state-dir`。只有检查点、找不到定义的任务也可删除；提交记录指向的外部定义一并删除，项目源码保留。删除后 `list --all` 也不会显示。
按 ID 批量匹配可用 `scheduler rm 'arcbench-*'`（支持 `*`、`?`、`[abc]`），或 `scheduler rm --regex '^arcbench-.*$'`（完整正则，匹配整个 ID）。模式加引号，匹配区分大小写；无匹配返回错误。逐项检查任务及状态目录边界；锁被占用、watcher/子进程仍在运行、运行节点缺少 PID、目录越界或删除失败时，跳过该任务，继续处理其他匹配项。返回 `ids` 和带原因的 `skipped`，状态为 `removed`、`partial` 或 `skipped`；存在跳过项时退出码为 3。
`scheduler stop 任务ID` 支持 `--task-dir`、`--state-dir`，直接对已有状态目录写入停止标记，即使任务定义已丢失也可停止；仍支持定义文件路径。该命令不派发任务或查询额度，watcher/子进程在后续轮询处理停止标记后退出，再重试删除。
已有完成记录也会自动隐藏。任务定义、检查点和日志保留原位，`status`、`view` 仍可查询，重复提交不会重新执行。
启用 `allow_append` 的任务追加未完成节点后，会重新出现在活动列表并显示为 `queued`，直到新增层级完成。

退出码：0 为检查完成/等待/完成/停止；1 为配置或额度查询错误；2 为 blocked/interrupted/running；3 为互斥 busy。

## 插件

`plugin.json` 分发 `skills/scheduler/SKILL.md`。已安装的 skill 只负责使用这个通用调度器，不接管普通开发请求，不要求 ARC 或 CCE 审批。不修改 hooks 或信任设置。

额度接口依据：[官方 Codex app-server 文档](https://learn.chatgpt.com/docs/app-server#6-rate-limits-chatgpt)。
