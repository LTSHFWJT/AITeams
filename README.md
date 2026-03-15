# AI Teams

一个基于 `FastAPI + SQLite + aimemory` 的多智能体协同平台最小实现。

当前版本重点完成：

- 平台层独立 SQLite 存储：Provider、Agent、协作会话、协作消息
- `aimemory` 作为 Agent 记忆库，而不是平台自身存储
- AI API 接入模块：支持自定义 URL / API Key / headers / extra config
- 主流 AI API 适配：
  - OpenAI
  - Azure OpenAI
  - Anthropic
  - Google Gemini
  - DeepSeek
  - OpenRouter
  - Ollama
  - Custom OpenAI-compatible
  - Mock provider（本地验证）
- 前后端一体：FastAPI 提供 API 和静态前端页面

## 目录

- `aiteams/`: 平台代码
- `aimemory/`: Agent 记忆存储库
- `data/platform.db`: 平台 SQLite
- `data/aimemory/aimemory.db`: aimemory SQLite

## 启动

当前实现不依赖第三方 Web 框架，直接运行即可：

```powershell
.\.venv\Scripts\python.exe -m aiteams
```

打开：

```text
http://127.0.0.1:8000
```

## 开发验证

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
```

## 说明

- Provider API Key 目前直接存 SQLite，适合本地开发和内网原型，不适合生产密钥治理。
- 前端可直接用 `Mock Provider` 先跑通平台闭环，再切换到真实模型。
- Agent 协作记录存在平台 SQLite；每个 Agent 的 brief、回答与长期记忆写入 `aimemory`。
