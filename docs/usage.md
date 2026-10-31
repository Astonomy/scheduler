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
```

`watch` 在无人登录的后台运行需由操作系统服务或任务计划程序托管。`scripts/print-windows-task.ps1` 输出每分钟调用 `tick` 的任务 XML，可导入 Windows 任务计划程序；脚本本身不注册任务。只更新插件不会自动启动没有配置工作区和提示词的示例任务。

状态默认保存在用户目录 `.reqflow-scheduler/<id>`，可通过 `--state-dir` 指定。每个节点保留 JSONL、stderr、退出码及精确会话 ID。同任务和工作区使用操作系统锁互斥；同一工作区仅使用一个 OS/runtime，不同时在 Windows 与 WSL 中调度。

退出码：0 为检查完成/等待/完成/停止；1 为配置或额度查询错误；2 为 blocked/interrupted/running；3 为互斥 busy。

## 插件

`plugin.json` 分发 `skills/scheduler/SKILL.md`。已安装的 skill 只负责使用这个通用调度器，不接管普通开发请求，不要求 ARC 或 CCE 审批。不修改 hooks 或信任设置。

额度接口依据：[官方 Codex app-server 文档](https://learn.chatgpt.com/docs/app-server#6-rate-limits-chatgpt)。
