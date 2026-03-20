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

    def get(self, memory_id: str, **kwargs: Any):
        return self._root._call("get_long_term_memory", memory_id, **kwargs)

    def list(self, **kwargs: Any):
        return self._root._call("list_long_term_memories", **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_long_term_memories", query, **kwargs)

    def update(self, memory_id: str, **kwargs: Any):
        return self._root._call("update_long_term_memory", memory_id, **kwargs)

    def supersede(self, memory_id: str, **kwargs: Any):
        return self._root._call("supersede_long_term_memory", memory_id, **kwargs)

    def history(self, memory_id: str, **kwargs: Any):
        return self._root._call("history_long_term_memory", memory_id, **kwargs)

    def link(self, memory_id: str, target_memory_ids: str | list[str], **kwargs: Any):
        return self._root._call("link_long_term_memory", memory_id, target_memory_ids, **kwargs)

    def delete(self, memory_id: str, **kwargs: Any):
        return self._root._call("delete_long_term_memory", memory_id, **kwargs)

    def compress(self, **kwargs: Any):
        return self._root._call("compress_long_term_memories", **kwargs)


class ShortTermMemoryNamespace(_NamespaceBase):
    namespace_name = "short_term"

    def add(self, text: str, **kwargs: Any):
        return self._root._call("store_short_term_memory", text, **kwargs)

    def get(self, memory_id: str, **kwargs: Any):
        return self._root._call("get_short_term_memory", memory_id, **kwargs)

    def list(self, **kwargs: Any):
        return self._root._call("list_short_term_memories", **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_short_term_memories", query, **kwargs)

    def update(self, memory_id: str, **kwargs: Any):
        return self._root._call("update_short_term_memory", memory_id, **kwargs)

    def supersede(self, memory_id: str, **kwargs: Any):
        return self._root._call("supersede_short_term_memory", memory_id, **kwargs)

    def history(self, memory_id: str, **kwargs: Any):
        return self._root._call("history_short_term_memory", memory_id, **kwargs)

    def link(self, memory_id: str, target_memory_ids: str | list[str], **kwargs: Any):
        return self._root._call("link_short_term_memory", memory_id, target_memory_ids, **kwargs)

    def delete(self, memory_id: str, **kwargs: Any):
        return self._root._call("delete_short_term_memory", memory_id, **kwargs)

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

    def compress(self, document_id: str, **kwargs: Any):
        return self._root._call("compress_knowledge_document", document_id, **kwargs)


class SkillNamespace(_NamespaceBase):
    namespace_name = "skill"

    def add(self, name: str, description: str, **kwargs: Any):
        return self._root._call("save_skill", name=name, description=description, **kwargs)

    def get(self, skill_id: str):
        return self._root._call("get_skill_content", skill_id)

    def list(self, **kwargs: Any):
        return self._root._call("list_skill_metadata", **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_skills", query, **kwargs)

    def search_references(self, query: str, **kwargs: Any):
        return self._root._call("search_skill_references", query, **kwargs)

    def refresh_execution_context(self, skill_id: str, **kwargs: Any):
        return self._root._call("refresh_skill_execution_context", skill_id, **kwargs)

    def compress_references(self, skill_id: str, **kwargs: Any):
        return self._root._call("compress_skill_reference_bundle", skill_id, **kwargs)

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

    def reflect(self, session_id: str, **kwargs: Any):
        return self._root._call("reflect_session", session_id, **kwargs)


class RecallNamespace(_NamespaceBase):
    namespace_name = "recall"

    def query(self, query: str, **kwargs: Any):
        return self._root._call("query", query, **kwargs)

    def plan(self, query: str, **kwargs: Any):
        return self._root._call("plan_recall", query, **kwargs)

    def explain(self, query: str, **kwargs: Any):
        return self._root._call("explain_recall", query, **kwargs)

    def compress_text(self, text: str, **kwargs: Any):
        return self._root._call("compress_text_payload", text, **kwargs)

    def context(self, query: str, **kwargs: Any):
        return self._root._call("build_context", query, **kwargs)


class ExecutionNamespace(_NamespaceBase):
    namespace_name = "execution"

    def start_run(self, user_id: str | None = None, goal: str = "", **kwargs: Any):
        return self._root._call("start_run", user_id=user_id, goal=goal, **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_execution", query, **kwargs)


class ContextNamespace(_NamespaceBase):
    namespace_name = "context"

    def build(self, query: str, **kwargs: Any):
        return self._root._call("build_context", query, **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_context_artifacts", query, **kwargs)

    def get(self, artifact_id: str, **kwargs: Any):
        return self._root._call("get_context_artifact", artifact_id, **kwargs)

    def list(self, **kwargs: Any):
        return self._root._call("list_context_artifacts", **kwargs)


class HandoffNamespace(_NamespaceBase):
    namespace_name = "handoff"

    def build(self, target_agent_id: str, **kwargs: Any):
        return self._root._call("build_handoff_pack", target_agent_id, **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_handoff_packs", query, **kwargs)

    def get(self, handoff_id: str, **kwargs: Any):
        return self._root._call("get_handoff_pack", handoff_id, **kwargs)

    def list(self, **kwargs: Any):
        return self._root._call("list_handoff_packs", **kwargs)


class ReflectionNamespace(_NamespaceBase):
    namespace_name = "reflection"

    def session(self, session_id: str, **kwargs: Any):
        return self._root._call("reflect_session", session_id, **kwargs)

    def run(self, run_id: str, **kwargs: Any):
        return self._root._call("reflect_run", run_id, **kwargs)

    def search(self, query: str, **kwargs: Any):
        return self._root._call("search_reflection_memories", query, **kwargs)

    def get(self, reflection_id: str, **kwargs: Any):
        return self._root._call("get_reflection_memory", reflection_id, **kwargs)

    def list(self, **kwargs: Any):
        return self._root._call("list_reflection_memories", **kwargs)


class ACLNamespace(_NamespaceBase):
    namespace_name = "acl"

    def get(self, rule_id: str, **kwargs: Any):
        return self._root._call("get_scope_acl_rule", rule_id, **kwargs)

    def list(self, **kwargs: Any):
        return self._root._call("list_scope_acl_rules", **kwargs)

    def grant(self, **kwargs: Any):
        return self._root._call("grant_scope_acl_rule", **kwargs)

    def revoke(self, rule_id: str | None = None, **kwargs: Any):
        if rule_id is None:
            return self._root._call("revoke_scope_acl_rule", **kwargs)
        return self._root._call("revoke_scope_acl_rule", rule_id, **kwargs)


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

    @cached_property
    def context(self) -> ContextNamespace:
        return ContextNamespace(self)

    @cached_property
    def handoff(self) -> HandoffNamespace:
        return HandoffNamespace(self)

    @cached_property
    def reflection(self) -> ReflectionNamespace:
        return ReflectionNamespace(self)

    @cached_property
    def acl(self) -> ACLNamespace:
        return ACLNamespace(self)


class StructuredAIMemoryAPI(_StructuredAPIBase):
    def _call(self, method_name: str, *args: Any, **kwargs: Any):
        payload = self.scope.apply_to_kwargs(kwargs)
        return self.memory._call_api_method(method_name, *args, **payload)


class AsyncStructuredAIMemoryAPI(_StructuredAPIBase):
    async def _call(self, method_name: str, *args: Any, **kwargs: Any):
        payload = self.scope.apply_to_kwargs(kwargs)
        return await asyncio.to_thread(self.memory._call_api_method, method_name, *args, **payload)
