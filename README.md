# AITeams

一个按 `selfdoc/python-agent-collab-platform-design.md` 重构的 Python Agent 协同平台。

当前版本聚焦于 V1 核心闭环：

- `Blueprint` 配置与模板化
- `TaskRelease -> Run -> Step -> Checkpoint -> Artifact -> Approval` 元数据闭环
- 基于 `asyncio` 的本地编排运行时
- `aimemory` 作为底层记忆库，通过 `MemoryAdapter` 接入
- 本地工作区与 Artifact 输出
- 轻量控制台，支持 Blueprint 编辑、任务发布、Run 查看与审批恢复

## 架构

`aiteams/` 现已按运行时重构为以下主干：

- `app/`: 配置与入口
- `api/`: HTTP 控制面
- `domain/`: Blueprint/Flow/Agent 领域模型
- `runtime/`: 编译器与执行引擎
- `agent/`: Agent Kernel
- `memory/`: `aimemory` 适配层
- `storage/`: SQLite 元数据存储
- `workspace/`: 本地工作区与 Artifact 管理

`aimemory/` 未被改动，仍只负责记忆存储与检索。

## 启动

```powershell
.\.venv\Scripts\python.exe -m aiteams
```

默认地址：

```text
http://127.0.0.1:8000
```

## 控制面能力

- 载入内置模板并编辑 YAML/JSON Blueprint
- 校验并保存 Blueprint
- 发布任务并自动执行 Run
- 查看 Step、Event、Artifact、Workspace 文件
- 对等待审批的 Run 进行批准并恢复执行

## 测试

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
```
