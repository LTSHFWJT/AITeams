from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from aimemory.core.scope import CollaborationScope, apply_scope_to_payload, scope_schema

if TYPE_CHECKING:
    from aimemory.core.facade import AIMemory


class AIMemoryMCPAdapter:
    def __init__(self, memory: "AIMemory", scope: CollaborationScope | dict[str, Any] | None = None):
        self.memory = memory
        raw_scope = CollaborationScope.from_value(scope).as_dict(include_none=True)
        self.scope = CollaborationScope.from_value(memory._resolve_scope(**raw_scope))

    def scoped(self, **scope_overrides: Any) -> "AIMemoryMCPAdapter":
        return AIMemoryMCPAdapter(self.memory, scope=self.scope.merge(scope_overrides))

    def litellm_config(self) -> dict[str, Any]:
        return self.memory.litellm_config()

    def _schema(self, properties: dict[str, Any] | None = None, *, required: list[str] | None = None) -> dict[str, Any]:
        scope_properties = dict(scope_schema()["properties"])
        return {
            "type": "object",
            "properties": {
                **scope_properties,
                **dict(properties or {}),
                "context_scope": scope_schema(),
            },
            "required": required or [],
        }

    def _tool(self, name: str, description: str, properties: dict[str, Any] | None = None, *, required: list[str] | None = None) -> dict[str, Any]:
        return {
            "name": name,
            "description": description,
            "inputSchema": self._schema(properties, required=required),
        }

    def manifest(self) -> dict[str, Any]:
        return {
            "name": "aimemory",
            "transport": "in-process",
            "tools": self.tool_specs(),
            "litellm": self.litellm_config(),
            "embeddings": self.memory.config.embeddings.as_provider_kwargs(),
            "default_scope": self.scope.as_metadata(),
            "storage": {
                "root_dir": str(self.memory.config.root_dir),
                "sqlite_path": str(self.memory.config.sqlite_path),
                "relational_backend": "sqlite",
                "index_backend": self.memory._resolve_vector_backend_name(),
                "graph_backend": self.memory._resolve_graph_backend_name(),
                "layout": self.memory.storage_layout(**self.scope.as_metadata()),
            },
        }

    def tool_specs(self) -> list[dict[str, Any]]:
        memory_write = {
            "text": {"type": "string"},
            "session_id": {"type": "string"},
            "memory_type": {"type": "string"},
            "importance": {"type": "number"},
            "metadata": {"type": "object"},
        }
        memory_list = {
            "session_id": {"type": "string"},
            "limit": {"type": "integer", "default": 200},
            "offset": {"type": "integer", "default": 0},
            "include_generated": {"type": "boolean", "default": False},
            "include_inactive": {"type": "boolean", "default": False},
            "filters": {"type": "object"},
        }
        memory_search = {
            "query": {"type": "string"},
            "session_id": {"type": "string"},
            "limit": {"type": "integer", "default": 10},
            "threshold": {"type": "number", "default": 0.0},
            "include_generated": {"type": "boolean", "default": True},
        }
        compression_input = {
            "session_id": {"type": "string"},
            "force": {"type": "boolean", "default": False},
            "limit": {"type": "integer", "default": 400},
        }
        knowledge_write = {
            "title": {"type": "string"},
            "text": {"type": "string"},
            "source_name": {"type": "string"},
            "source_type": {"type": "string"},
            "uri": {"type": "string"},
            "kb_namespace": {"type": "string"},
            "global_scope": {"type": "boolean", "default": False},
            "metadata": {"type": "object"},
        }
        knowledge_list = {
            "limit": {"type": "integer", "default": 100},
            "offset": {"type": "integer", "default": 0},
            "status": {"type": "string", "default": "active"},
            "include_global": {"type": "boolean", "default": True},
        }
        knowledge_search = {
            "query": {"type": "string"},
            "limit": {"type": "integer", "default": 10},
            "threshold": {"type": "number", "default": 0.0},
            "include_global": {"type": "boolean", "default": True},
        }
        archive_write = {
            "summary": {"type": "string"},
            "content": {"type": "string"},
            "source_type": {"type": "string"},
            "domain": {"type": "string"},
            "session_id": {"type": "string"},
            "global_scope": {"type": "boolean", "default": False},
            "metadata": {"type": "object"},
        }
        archive_list = {
            "session_id": {"type": "string"},
            "limit": {"type": "integer", "default": 100},
            "offset": {"type": "integer", "default": 0},
            "include_global": {"type": "boolean", "default": True},
            "include_generated": {"type": "boolean", "default": False},
        }
        archive_search = {
            "query": {"type": "string"},
            "session_id": {"type": "string"},
            "limit": {"type": "integer", "default": 10},
            "threshold": {"type": "number", "default": 0.0},
            "include_global": {"type": "boolean", "default": True},
        }
        skill_write = {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "prompt_template": {"type": "string"},
            "workflow": {},
            "schema": {"type": "object"},
            "tools": {"type": "array", "items": {"type": "string"}},
            "tests": {"type": "array", "items": {}},
            "topics": {"type": "array", "items": {"type": "string"}},
            "skill_markdown": {"type": "string"},
            "files": {"type": "array", "items": {"type": "object"}},
            "references": {},
            "scripts": {},
            "assets": {},
            "metadata": {"type": "object"},
            "status": {"type": "string"},
        }
        skill_list = {
            "limit": {"type": "integer", "default": 100},
            "offset": {"type": "integer", "default": 0},
            "status": {"type": "string", "default": "active"},
        }
        skill_search = {
            "query": {"type": "string"},
            "limit": {"type": "integer", "default": 10},
            "threshold": {"type": "number", "default": 0.0},
        }
        skill_reference_search = {
            "query": {"type": "string"},
            "skill_id": {"type": "string"},
            "path_prefix": {"type": "string"},
            "limit": {"type": "integer", "default": 10},
            "threshold": {"type": "number", "default": 0.0},
        }
        text_compress = {
            "text": {"type": "string"},
            "query": {"type": "string"},
            "domain_hint": {"type": "string"},
            "budget_chars": {"type": "integer", "default": 600},
            "max_sentences": {"type": "integer", "default": 8},
            "max_highlights": {"type": "integer", "default": 12},
            "metadata": {"type": "object"},
        }
        return [
            self._tool("aimemory_manifest", "返回 AIMemory 的能力、存储布局与 LiteLLM 兼容配置。"),
            self._tool(
                "recall_query",
                "跨长期记忆、短期记忆、知识库、技能和归档做统一查询。",
                {
                    "query": {"type": "string"},
                    "domains": {"type": "array", "items": {"type": "string"}},
                    "session_id": {"type": "string"},
                    "limit": {"type": "integer", "default": 8},
                },
                required=["query"],
            ),
            self._tool("text_compress", "使用本地纯算法压缩长文本。", text_compress, required=["text"]),
            self._tool("long_term_memory_add", "写入指定 agent 与交互主体之间的长期记忆。", memory_write, required=["text"]),
            self._tool("long_term_memory_get", "获取一条长期记忆。", {"memory_id": {"type": "string"}}, required=["memory_id"]),
            self._tool("long_term_memory_list", "列出指定 agent 与主体之间的完整长期记忆。", memory_list),
            self._tool("long_term_memory_search", "按关键字或短查询快速检索长期记忆。", memory_search, required=["query"]),
            self._tool(
                "long_term_memory_update",
                "更新长期记忆内容、权重或元数据。",
                {**memory_write, "memory_id": {"type": "string"}, "status": {"type": "string"}},
                required=["memory_id"],
            ),
            self._tool("long_term_memory_delete", "删除一条长期记忆。", {"memory_id": {"type": "string"}}, required=["memory_id"]),
            self._tool("long_term_memory_compress", "压缩指定 agent 与主体之间的长期记忆，降低 token 负担。", compression_input),
            self._tool("short_term_memory_add", "写入短期记忆。适合当前会话窗口中的重要上下文。", memory_write, required=["text"]),
            self._tool("short_term_memory_get", "获取一条短期记忆。", {"memory_id": {"type": "string"}}, required=["memory_id"]),
            self._tool("short_term_memory_list", "列出指定 agent 与主体之间的完整短期记忆。", memory_list),
            self._tool("short_term_memory_search", "按关键字快速检索短期记忆。", memory_search, required=["query"]),
            self._tool(
                "short_term_memory_update",
                "更新短期记忆内容、权重或元数据。",
                {**memory_write, "memory_id": {"type": "string"}, "status": {"type": "string"}},
                required=["memory_id"],
            ),
            self._tool("short_term_memory_delete", "删除一条短期记忆。", {"memory_id": {"type": "string"}}, required=["memory_id"]),
            self._tool("short_term_memory_compress", "压缩短期记忆；有会话时优先写入 working memory snapshot。", compression_input),
            self._tool("archive_memory_add", "新增归档记忆；支持全局归档。", archive_write, required=["summary"]),
            self._tool("archive_memory_get", "获取一条归档记忆。", {"archive_unit_id": {"type": "string"}}, required=["archive_unit_id"]),
            self._tool("archive_memory_list", "列出归档记忆；可选择包含全局归档。", archive_list),
            self._tool("archive_memory_search", "按关键字检索归档记忆。", archive_search, required=["query"]),
            self._tool(
                "archive_memory_update",
                "更新归档记忆内容或元数据。",
                {**archive_write, "archive_unit_id": {"type": "string"}},
                required=["archive_unit_id"],
            ),
            self._tool("archive_memory_delete", "删除一条归档记忆。", {"archive_unit_id": {"type": "string"}}, required=["archive_unit_id"]),
            self._tool("archive_memory_compress", "压缩一组归档记忆并生成低成本摘要。", {**archive_list, "force": {"type": "boolean", "default": False}}),
            self._tool("knowledge_document_add", "写入知识库文档；支持 agent 私有和全局知识库。", knowledge_write, required=["title", "text"]),
            self._tool("knowledge_document_get", "获取知识库文档完整内容。", {"document_id": {"type": "string"}}, required=["document_id"]),
            self._tool("knowledge_document_list", "列出知识库文档。", knowledge_list),
            self._tool("knowledge_document_search", "检索知识库文档与切块。", knowledge_search, required=["query"]),
            self._tool(
                "knowledge_document_compress",
                "对知识文档执行本地纯算法压缩。",
                {
                    "document_id": {"type": "string"},
                    "query": {"type": "string"},
                    "budget_chars": {"type": "integer", "default": 800},
                    "max_sentences": {"type": "integer", "default": 8},
                    "max_highlights": {"type": "integer", "default": 12},
                },
                required=["document_id"],
            ),
            self._tool(
                "knowledge_document_update",
                "更新知识库文档标题、正文、作用域或元数据。",
                {**knowledge_write, "document_id": {"type": "string"}, "status": {"type": "string"}},
                required=["document_id"],
            ),
            self._tool("knowledge_document_delete", "删除一份知识库文档。", {"document_id": {"type": "string"}}, required=["document_id"]),
            self._tool("skill_add", "保存或新增一个 agent skill。", skill_write, required=["name", "description"]),
            self._tool("skill_get", "通过 skill ID 获取完整 skill 内容。", {"skill_id": {"type": "string"}}, required=["skill_id"]),
            self._tool("skill_list", "列出当前 agent 的所有 skill metadata。", skill_list),
            self._tool("skill_search", "检索 skill 主体与执行上下文。", skill_search, required=["query"]),
            self._tool("skill_search_references", "检索 skill references 内的命中文本分块。", skill_reference_search, required=["query"]),
            self._tool(
                "skill_refresh_execution_context",
                "根据 reference 文件刷新 skill 的常用执行上下文。",
                {
                    "skill_id": {"type": "string"},
                    "path_prefix": {"type": "string"},
                    "budget_chars": {"type": "integer", "default": 900},
                    "max_sentences": {"type": "integer", "default": 8},
                    "max_highlights": {"type": "integer", "default": 12},
                },
                required=["skill_id"],
            ),
            self._tool(
                "skill_reference_compress",
                "对 skill reference 文件执行本地纯算法压缩，不修改已保存的执行上下文。",
                {
                    "skill_id": {"type": "string"},
                    "path_prefix": {"type": "string"},
                    "query": {"type": "string"},
                    "budget_chars": {"type": "integer", "default": 800},
                    "max_sentences": {"type": "integer", "default": 8},
                    "max_highlights": {"type": "integer", "default": 12},
                },
                required=["skill_id"],
            ),
            self._tool(
                "skill_update",
                "更新 skill 元信息，必要时写入新版本。",
                {**skill_write, "skill_id": {"type": "string"}},
                required=["skill_id"],
            ),
            self._tool("skill_delete", "删除一个 skill。", {"skill_id": {"type": "string"}}, required=["skill_id"]),
            self._tool(
                "session_create",
                "创建会话上下文。",
                {
                    "user_id": {"type": "string"},
                    "title": {"type": "string"},
                    "metadata": {"type": "object"},
                },
            ),
            self._tool(
                "session_append_turn",
                "追加一条人-agent 或 agent-agent 交互轮次。",
                {
                    "session_id": {"type": "string"},
                    "role": {"type": "string"},
                    "content": {"type": "string"},
                    "speaker_participant_id": {"type": "string"},
                    "target_participant_id": {"type": "string"},
                    "speaker_type": {"type": "string"},
                    "speaker_external_id": {"type": "string"},
                    "target_type": {"type": "string"},
                    "target_external_id": {"type": "string"},
                    "turn_type": {"type": "string"},
                    "metadata": {"type": "object"},
                },
                required=["session_id", "role", "content"],
            ),
            self._tool(
                "session_compress",
                "压缩会话上下文。",
                {
                    "session_id": {"type": "string"},
                    "budget_chars": {"type": "integer", "default": 600},
                },
                required=["session_id"],
            ),
            self._tool(
                "session_archive",
                "归档整个会话。",
                {
                    "session_id": {"type": "string"},
                    "metadata": {"type": "object"},
                },
                required=["session_id"],
            ),
        ]

    def _handlers(self, payload: dict[str, Any]) -> dict[str, Callable[[], Any]]:
        return {
            "aimemory_manifest": lambda: self.manifest(),
            "recall_query": lambda: self.memory.api.recall.query(**payload),
            "text_compress": lambda: self.memory.api.recall.compress_text(**payload),
            "long_term_memory_add": lambda: self.memory.api.long_term.add(**payload),
            "long_term_memory_get": lambda: self.memory.api.long_term.get(payload["memory_id"]),
            "long_term_memory_list": lambda: self.memory.api.long_term.list(**payload),
            "long_term_memory_search": lambda: self.memory.api.long_term.search(**payload),
            "long_term_memory_update": lambda: self.memory.api.long_term.update(payload["memory_id"], **{key: value for key, value in payload.items() if key != "memory_id"}),
            "long_term_memory_delete": lambda: self.memory.api.long_term.delete(payload["memory_id"]),
            "long_term_memory_compress": lambda: self.memory.api.long_term.compress(**payload),
            "short_term_memory_add": lambda: self.memory.api.short_term.add(**payload),
            "short_term_memory_get": lambda: self.memory.api.short_term.get(payload["memory_id"]),
            "short_term_memory_list": lambda: self.memory.api.short_term.list(**payload),
            "short_term_memory_search": lambda: self.memory.api.short_term.search(**payload),
            "short_term_memory_update": lambda: self.memory.api.short_term.update(payload["memory_id"], **{key: value for key, value in payload.items() if key != "memory_id"}),
            "short_term_memory_delete": lambda: self.memory.api.short_term.delete(payload["memory_id"]),
            "short_term_memory_compress": lambda: self.memory.api.short_term.compress(**payload),
            "archive_memory_add": lambda: self.memory.api.archive.add(**payload),
            "archive_memory_get": lambda: self.memory.api.archive.get(payload["archive_unit_id"]),
            "archive_memory_list": lambda: self.memory.api.archive.list(**payload),
            "archive_memory_search": lambda: self.memory.api.archive.search(**payload),
            "archive_memory_update": lambda: self.memory.api.archive.update(payload["archive_unit_id"], **{key: value for key, value in payload.items() if key != "archive_unit_id"}),
            "archive_memory_delete": lambda: self.memory.api.archive.delete(payload["archive_unit_id"]),
            "archive_memory_compress": lambda: self.memory.api.archive.compress(**payload),
            "knowledge_document_add": lambda: self.memory.api.knowledge.add(**payload),
            "knowledge_document_get": lambda: self.memory.api.knowledge.get(payload["document_id"]),
            "knowledge_document_list": lambda: self.memory.api.knowledge.list(**payload),
            "knowledge_document_search": lambda: self.memory.api.knowledge.search(**payload),
            "knowledge_document_compress": lambda: self.memory.api.knowledge.compress(payload["document_id"], **{key: value for key, value in payload.items() if key != "document_id"}),
            "knowledge_document_update": lambda: self.memory.api.knowledge.update(payload["document_id"], **{key: value for key, value in payload.items() if key != "document_id"}),
            "knowledge_document_delete": lambda: self.memory.api.knowledge.delete(payload["document_id"]),
            "skill_add": lambda: self.memory.api.skill.add(**payload),
            "skill_get": lambda: self.memory.api.skill.get(payload["skill_id"]),
            "skill_list": lambda: self.memory.api.skill.list(**payload),
            "skill_search": lambda: self.memory.api.skill.search(**payload),
            "skill_search_references": lambda: self.memory.api.skill.search_references(**payload),
            "skill_refresh_execution_context": lambda: self.memory.api.skill.refresh_execution_context(payload["skill_id"], **{key: value for key, value in payload.items() if key != "skill_id"}),
            "skill_reference_compress": lambda: self.memory.api.skill.compress_references(payload["skill_id"], **{key: value for key, value in payload.items() if key != "skill_id"}),
            "skill_update": lambda: self.memory.api.skill.update(payload["skill_id"], **{key: value for key, value in payload.items() if key != "skill_id"}),
            "skill_delete": lambda: self.memory.api.skill.delete(payload["skill_id"]),
            "session_create": lambda: self.memory.api.session.create(**payload),
            "session_append_turn": lambda: self.memory.api.session.append(**payload),
            "session_compress": lambda: self.memory.api.session.compress(**payload),
            "session_archive": lambda: self.memory.api.session.archive(**payload),
        }

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        payload = apply_scope_to_payload(arguments, default_scope=self.scope)
        handlers = self._handlers(payload)
        if name not in handlers:
            raise ValueError(f"Unknown MCP tool `{name}`")
        return handlers[name]()

    def bind_fastmcp(self, server=None):
        try:
            from mcp.server.fastmcp import FastMCP  # type: ignore
        except ImportError as exc:
            raise RuntimeError("`mcp` package is not installed.") from exc

        fastmcp = server or FastMCP("aimemory")
        for spec in self.tool_specs():
            tool_name = spec["name"]
            description = spec["description"]

            def make_handler(name: str):
                def handler(**kwargs):
                    return self.call_tool(name, kwargs)

                return handler

            fastmcp.tool(name=tool_name, description=description)(make_handler(tool_name))
        return fastmcp
