from __future__ import annotations

from typing import Any

from aimemory.core.governance import evaluate_memory_value
from aimemory.core.utils import make_id, utcnow_iso
from aimemory.domains.memory.models import MemoryScope, MemoryType
from aimemory.memory_intelligence.models import MemoryScopeContext, NormalizedMessage
from aimemory.querying.filters import filter_records
from aimemory.services.base import ServiceBase


class MemoryService(ServiceBase):
    def __init__(self, db, projection, config, interaction_service, intelligence_pipeline=None):
        super().__init__(db=db, projection=projection, config=config)
        self.interaction_service = interaction_service
        self.intelligence_pipeline = intelligence_pipeline

    def set_intelligence_pipeline(self, pipeline) -> None:
        self.intelligence_pipeline = pipeline

    def add(self, messages, **kwargs) -> dict[str, Any]:
        if "agent_id" in kwargs and "owner_agent_id" not in kwargs:
            kwargs["owner_agent_id"] = kwargs["agent_id"]
        return self._kernel().add(messages, **kwargs)

    def remember(
        self,
        text: str,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        memory_type: str = str(MemoryType.SEMANTIC),
        importance: float = 0.5,
        long_term: bool = True,
        source: str = "manual",
    ) -> dict[str, Any]:
        return self._kernel().memory_store(
            text,
            user_id=user_id,
            session_id=session_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            run_id=run_id,
            metadata=metadata,
            memory_type=memory_type,
            importance=importance,
            long_term=long_term,
            source=source,
        )

    def get(self, memory_id: str) -> dict[str, Any] | None:
        return self._kernel().get(memory_id)

    def get_all(
        self,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        actor_id: str | None = None,
        role: str | None = None,
        strategy_scope: str | None = None,
        scope: str = MemoryScope.LONG_TERM,
        limit: int = 100,
        offset: int = 0,
        include_deleted: bool = False,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result = self._kernel().get_all(
            user_id=user_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            scope=scope,
            limit=limit,
            offset=offset,
            filters=filters,
        )
        items = list(result["results"])
        if run_id:
            items = [item for item in items if item.get("run_id") == run_id or item.get("source_run_id") == run_id]
        if actor_id:
            items = [item for item in items if item.get("actor_id") == actor_id or dict(item.get("metadata") or {}).get("actor_id") == actor_id]
        if role:
            items = [item for item in items if item.get("role") == role or dict(item.get("metadata") or {}).get("role") == role]
        if strategy_scope:
            items = [item for item in items if dict(item.get("metadata") or {}).get("strategy_scope") == strategy_scope]
        if not include_deleted:
            items = [item for item in items if item.get("status") != "deleted"]
        if filters:
            items = filter_records(items, filters)
        return {"results": items, "count": len(items), "limit": limit, "offset": offset}

    def promote_session_memories(
        self,
        session_id: str,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        run_id: str | None = None,
        limit: int = 50,
        min_importance: float = 0.55,
        include_memory_types: list[str] | None = None,
        force: bool = False,
        archive_after_promotion: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result = self._kernel().promote_session_memories(
            session_id,
            memory_type=(include_memory_types or [str(MemoryType.SEMANTIC)])[0] if include_memory_types and len(include_memory_types) == 1 else str(MemoryType.SEMANTIC),
            threshold=min_importance if not force else 0.0,
        )
        created = result.get("results", [])
        if archive_after_promotion:
            session_memories = self.get_all(
                user_id=user_id,
                owner_agent_id=owner_agent_id or agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                interaction_type=interaction_type,
                session_id=session_id,
                scope=str(MemoryScope.SESSION),
                limit=limit,
            )["results"]
            now = utcnow_iso()
            for item in session_memories:
                self.db.execute("UPDATE memories SET status = 'archived', archived_at = ?, updated_at = ? WHERE id = ?", (now, now, item["id"]))
        return {
            "source_count": len(created),
            "promoted_count": len(created),
            "results": created,
            "facts": [item.get("text") for item in created],
            "source_ids": [item.get("id") for item in created],
            "skipped": [],
            "metadata": metadata or {},
            "run_id": run_id,
        }

    def plan_low_value_cleanup(
        self,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        run_id: str | None = None,
        scope: str = MemoryScope.LONG_TERM,
        limit: int = 100,
        threshold: float | None = None,
    ) -> dict[str, Any]:
        effective_threshold = float(
            threshold if threshold is not None else getattr(self.config.memory_policy, "cleanup_importance_threshold", 0.34)
        )
        items = self.get_all(
            user_id=user_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            run_id=run_id,
            scope=scope,
            limit=limit,
        )["results"]
        candidates: list[dict[str, Any]] = []
        for item in items:
            if item.get("status") != "active":
                continue
            evaluation = evaluate_memory_value(item)
            if float(evaluation["recency_score"]) > float(getattr(self.config.memory_policy, "cleanup_recent_score_ceiling", 0.08)):
                continue
            item_threshold = float(threshold if threshold is not None else evaluation["cleanup_threshold"])
            if float(evaluation["value_score"]) <= item_threshold:
                candidates.append(
                    {
                        "id": item["id"],
                        "text": item["text"],
                        "memory_type": item.get("memory_type"),
                        "strategy_scope": evaluation["strategy_scope"],
                        "value_score": evaluation["value_score"],
                        "cleanup_threshold": round(item_threshold, 6),
                        "suggested_action": evaluation["cleanup_action"],
                        "evaluation": evaluation,
                        "owner_agent_id": item.get("owner_agent_id") or item.get("agent_id"),
                        "subject_type": item.get("subject_type"),
                        "subject_id": item.get("subject_id"),
                    }
                )
        candidates.sort(key=lambda item: (float(item["value_score"]), len(item["text"])), reverse=False)
        return {"threshold": effective_threshold, "results": candidates}

    def update(
        self,
        memory_id: str,
        text: str | None = None,
        metadata: dict[str, Any] | None = None,
        importance: float | None = None,
        status: str | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        result = self._kernel().update(memory_id, text=text, metadata=metadata, importance=importance, status=status)
        if timestamp and result is not None:
            self.db.execute("UPDATE memories SET updated_at = ? WHERE id = ?", (timestamp, memory_id))
            refreshed = self.get(memory_id)
            assert refreshed is not None
            return refreshed
        return result

    def delete(self, memory_id: str) -> dict[str, Any]:
        return self._kernel().delete(memory_id)

    def delete_by_query(
        self,
        query: str,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        limit: int = 10,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().memory_forget(
            query=query,
            user_id=user_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            scope=scope,
            limit=limit,
            filters=filters,
        )

    def history(self, memory_id: str) -> list[dict[str, Any]]:
        return self._kernel().history(memory_id)

    def _scope_from_bool(self, long_term: bool) -> str:
        return str(MemoryScope.LONG_TERM if long_term else MemoryScope.SESSION)

    def build_scope_context(
        self,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        actor_id: str | None = None,
        role: str | None = None,
    ) -> MemoryScopeContext:
        return MemoryScopeContext(
            user_id=user_id or self.config.default_user_id,
            agent_id=agent_id or owner_agent_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            run_id=run_id,
            actor_id=actor_id,
            role=role,
        )

    def _normalize_messages(self, messages) -> list[NormalizedMessage]:
        return self._kernel()._normalize_messages(messages)

    def _message_content(self, content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, dict):
            return str(content.get("text") or content.get("content") or "")
        if isinstance(content, list):
            return "\n".join(self._message_content(item) for item in content)
        return str(content or "")

    def _resolve_memory_type(self, memory_type: str | None = None, text: str | None = None) -> str:
        if memory_type:
            return str(memory_type)
        return str(MemoryType.SEMANTIC)

    def _infer_memory_type(self, text: str) -> str:
        return str(MemoryType.SEMANTIC)

    def _extract_candidates(self, messages: list[NormalizedMessage]) -> list[str]:
        return [item.content for item in messages if item.content.strip()]

    def _promote_memory_row(self, item: dict[str, Any] | None) -> dict[str, Any] | None:
        return item
