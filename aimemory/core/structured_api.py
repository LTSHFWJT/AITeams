from __future__ import annotations

import asyncio
from functools import cached_property
from typing import TYPE_CHECKING, Any

from aimemory.core.scope import CollaborationScope

if TYPE_CHECKING:
    from aimemory.core.facade import AIMemory


class _NamespaceBase:
    namespace_name = ""

    def __init__(self, root: "_StructuredAPIBase"):
        self._root = root

    def scoped(self, **scope_overrides: Any):
        rebound = self._root.scoped(**scope_overrides)
        return getattr(rebound, self.namespace_name)


class LongTermMemoryNamespace(_NamespaceBase):
    namespace_name = "long_term"

    def add(self, text: str, **kwargs: Any):
        return self._root._call("store_long_term_memory", text, **kwargs)

    def get(self, memory_id: str):
        return self._root._call("get_long_term_memory", memory_id)

    def list(self, **kwargs: Any):
        return self._root._call("list_long_term_memories", **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_long_term_memories", query, **kwargs)

    def update(self, memory_id: str, **kwargs: Any):
        return self._root._call("update_long_term_memory", memory_id, **kwargs)

    def delete(self, memory_id: str):
        return self._root._call("delete_long_term_memory", memory_id)

    def compress(self, **kwargs: Any):
        return self._root._call("compress_long_term_memories", **kwargs)


class ShortTermMemoryNamespace(_NamespaceBase):
    namespace_name = "short_term"

    def add(self, text: str, **kwargs: Any):
        return self._root._call("store_short_term_memory", text, **kwargs)

    def get(self, memory_id: str):
        return self._root._call("get_short_term_memory", memory_id)

    def list(self, **kwargs: Any):
        return self._root._call("list_short_term_memories", **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_short_term_memories", query, **kwargs)

    def update(self, memory_id: str, **kwargs: Any):
        return self._root._call("update_short_term_memory", memory_id, **kwargs)

    def delete(self, memory_id: str):
        return self._root._call("delete_short_term_memory", memory_id)

    def compress(self, **kwargs: Any):
        return self._root._call("compress_short_term_memories", **kwargs)


class KnowledgeNamespace(_NamespaceBase):
    namespace_name = "knowledge"

    def add(self, title: str, text: str, **kwargs: Any):
        return self._root._call("save_knowledge_document", title=title, text=text, **kwargs)

    def get(self, document_id: str):
        return self._root._call("get_knowledge_document", document_id)

    def list(self, **kwargs: Any):
        return self._root._call("list_knowledge_documents", **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_knowledge_documents", query, **kwargs)

    def update(self, document_id: str, **kwargs: Any):
        return self._root._call("update_knowledge_document", document_id, **kwargs)

    def delete(self, document_id: str):
        return self._root._call("delete_knowledge_document", document_id)


class SkillNamespace(_NamespaceBase):
    namespace_name = "skill"

    def add(self, name: str, description: str, **kwargs: Any):
        return self._root._call("save_skill", name=name, description=description, **kwargs)

    def get(self, skill_id: str):
        return self._root._call("get_skill_content", skill_id)

    def list(self, **kwargs: Any):
        return self._root._call("list_skill_metadata", **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_skill_keywords", query, **kwargs)

    def update(self, skill_id: str, **kwargs: Any):
        return self._root._call("update_skill", skill_id, **kwargs)

    def delete(self, skill_id: str):
        return self._root._call("delete_skill", skill_id)


class ArchiveNamespace(_NamespaceBase):
    namespace_name = "archive"

    def add(self, summary: str, **kwargs: Any):
        return self._root._call("save_archive_memory", summary, **kwargs)

    def get(self, archive_unit_id: str):
        return self._root._call("get_archive_memory", archive_unit_id)

    def list(self, **kwargs: Any):
        return self._root._call("list_archive_memories", **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_archive_memories", query, **kwargs)

    def update(self, archive_unit_id: str, **kwargs: Any):
        return self._root._call("update_archive_memory", archive_unit_id, **kwargs)

    def delete(self, archive_unit_id: str):
        return self._root._call("delete_archive_memory", archive_unit_id)

    def compress(self, **kwargs: Any):
        return self._root._call("compress_archive_memories", **kwargs)


class SessionNamespace(_NamespaceBase):
    namespace_name = "session"

    def create(self, **kwargs: Any):
        return self._root._call("create_session", **kwargs)

    def get(self, session_id: str):
        return self._root._call("get_session", session_id)

    def append(self, session_id: str, role: str, content: str, **kwargs: Any):
        return self._root._call("append_turn", session_id, role, content, **kwargs)

    def compress(self, session_id: str, **kwargs: Any):
        return self._root._call("compress_session_context", session_id, **kwargs)

    def promote(self, session_id: str, **kwargs: Any):
        return self._root._call("promote_session_memories", session_id, **kwargs)

    def health(self, session_id: str):
        return self._root._call("session_health", session_id)

    def prune(self, session_id: str):
        return self._root._call("prune_session_snapshots", session_id)

    def archive(self, session_id: str, **kwargs: Any):
        return self._root._call("archive_session", session_id, **kwargs)

    def govern(self, session_id: str, **kwargs: Any):
        return self._root._call("govern_session", session_id, **kwargs)


class RecallNamespace(_NamespaceBase):
    namespace_name = "recall"

    def query(self, query: str, **kwargs: Any):
        return self._root._call("query", query, **kwargs)

    def explain(self, query: str, **kwargs: Any):
        return self._root._call("explain_recall", query, **kwargs)


class ExecutionNamespace(_NamespaceBase):
    namespace_name = "execution"

    def start_run(self, user_id: str | None = None, goal: str = "", **kwargs: Any):
        return self._root._call("start_run", user_id=user_id, goal=goal, **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_execution", query, **kwargs)


class _StructuredAPIBase:
    def __init__(self, memory: "AIMemory", scope: CollaborationScope | dict[str, Any] | None = None):
        self.memory = memory
        self.scope = CollaborationScope.from_value(scope)

    def scoped(self, **scope_overrides: Any):
        return self.__class__(self.memory, scope=self.scope.merge(scope_overrides))

    def scope_dict(self) -> dict[str, str]:
        return self.scope.as_metadata()

    @cached_property
    def long_term(self) -> LongTermMemoryNamespace:
        return LongTermMemoryNamespace(self)

    @cached_property
    def short_term(self) -> ShortTermMemoryNamespace:
        return ShortTermMemoryNamespace(self)

    @cached_property
    def knowledge(self) -> KnowledgeNamespace:
        return KnowledgeNamespace(self)

    @cached_property
    def skill(self) -> SkillNamespace:
        return SkillNamespace(self)

    @cached_property
    def archive(self) -> ArchiveNamespace:
        return ArchiveNamespace(self)

    @cached_property
    def session(self) -> SessionNamespace:
        return SessionNamespace(self)

    @cached_property
    def recall(self) -> RecallNamespace:
        return RecallNamespace(self)

    @cached_property
    def execution(self) -> ExecutionNamespace:
        return ExecutionNamespace(self)


class StructuredAIMemoryAPI(_StructuredAPIBase):
    def _call(self, method_name: str, *args: Any, **kwargs: Any):
        payload = self.scope.apply_to_kwargs(kwargs)
        return self.memory._call_api_method(method_name, *args, **payload)


class AsyncStructuredAIMemoryAPI(_StructuredAPIBase):
    async def _call(self, method_name: str, *args: Any, **kwargs: Any):
        payload = self.scope.apply_to_kwargs(kwargs)
        return await asyncio.to_thread(self.memory._call_api_method, method_name, *args, **payload)
