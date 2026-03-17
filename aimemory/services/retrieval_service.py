from __future__ import annotations

from typing import Any

from aimemory.core.router import DEFAULT_RETRIEVAL_DOMAINS
from aimemory.memory_intelligence.models import MemoryScopeContext
from aimemory.querying.filters import filter_records


class RetrievalService:
    def __init__(self, db, config, router=None, reranker=None, index_backend=None, graph_backend=None, recall_planner=None):
        self.db = db
        self.config = config
        self.router = router
        self.reranker = reranker
        self.index_backend = index_backend
        self.graph_backend = graph_backend
        self.recall_planner = recall_planner
        self._kernel_instance = None

    def _kernel(self):
        if self._kernel_instance is None:
            from aimemory.core.facade import AIMemory

            self._kernel_instance = AIMemory(self.config)
        return self._kernel_instance

    def search_memory(
        self,
        query: str,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        run_id: str | None = None,
        actor_id: str | None = None,
        role: str | None = None,
        scope: str = "all",
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().memory_search(
            query,
            user_id=user_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            scope=scope,
            limit=limit,
            threshold=threshold,
            filters=filters,
        )

    def retrieve(
        self,
        query: str,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        run_id: str | None = None,
        actor_id: str | None = None,
        role: str | None = None,
        domains: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 10,
        threshold: float = 0.0,
    ) -> dict[str, Any]:
        selected_domains = list(dict.fromkeys(domains or DEFAULT_RETRIEVAL_DOMAINS))
        result = self._kernel().query(
            query,
            user_id=user_id,
            session_id=session_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            run_id=run_id,
            actor_id=actor_id,
            role=role,
            domains=selected_domains,
            filters=filters,
            limit=limit,
            threshold=threshold,
        )
        if filters:
            result["results"] = filter_records(result["results"], filters)
        return result

    def search_interaction(
        self,
        query: str,
        session_id: str | None = None,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        actor_id: str | None = None,
        role: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().search_interaction(
            query,
            user_id=user_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            limit=limit,
            threshold=threshold,
            filters=filters,
        )

    def search_knowledge(
        self,
        query: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().search_knowledge(
            query,
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            limit=limit,
            threshold=threshold,
            filters=filters,
        )

    def search_skills(
        self,
        query: str,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().search_skills(
            query,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            limit=limit,
            threshold=threshold,
            filters=filters,
        )

    def search_archive(
        self,
        query: str,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().search_archive(
            query,
            user_id=user_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            limit=limit,
            threshold=threshold,
            filters=filters,
        )

    def search_execution(
        self,
        query: str,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().search_execution(
            query,
            user_id=user_id,
            owner_agent_id=owner_agent_id or agent_id,
            session_id=session_id,
            limit=limit,
            threshold=threshold,
            filters=filters,
        )

    def _relations_for_ref(self, ref_id: str) -> list[dict[str, Any]]:
        if self.graph_backend is None:
            return []
        return self.graph_backend.relations_for_ref(ref_id, limit=12)

    def _graph_context(self, query: str, relations: list[dict[str, Any]]) -> dict[str, Any]:
        return {"query": query, "relation_count": len(relations), "relations": relations}

    def _build_context(
        self,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
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

    def _match_context(self, item: dict[str, Any], context: MemoryScopeContext) -> bool:
        if context.owner_agent_id and (item.get("owner_agent_id") or item.get("agent_id")) not in {None, context.owner_agent_id}:
            return False
        if context.subject_type and item.get("subject_type") not in {None, context.subject_type}:
            return False
        if context.subject_id and item.get("subject_id") not in {None, context.subject_id}:
            return False
        if context.session_id and item.get("session_id") not in {None, context.session_id}:
            return False
        return True

    def _annotate_domain(self, domain: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{**row, "domain": domain} for row in rows]

    def _promote_memory(self, item: dict[str, Any] | None) -> dict[str, Any] | None:
        return item

    def _rerank(self, query: str, rows: list[dict[str, Any]], *, domain: str, context: MemoryScopeContext) -> list[dict[str, Any]]:
        return rows

    def plan_memory_recall(
        self,
        query: str,
        *,
        context: MemoryScopeContext | None = None,
        preferred_scope: str | None = None,
        limit: int = 10,
        auxiliary_limit: int | None = None,
    ) -> dict[str, Any]:
        scope = preferred_scope or ("session" if context and context.session_id else "all")
        domains = ["memory", "interaction"]
        if scope == "all":
            domains.extend(["knowledge", "skill", "archive"])
        return {
            "query": query,
            "scope": scope,
            "limit": limit,
            "auxiliary_limit": auxiliary_limit or self.config.memory_policy.auxiliary_search_limit,
            "handoff_domains": domains,
        }

    def explain_memory_recall(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        return self._kernel().explain_recall(
            query,
            user_id=user_id,
            session_id=session_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            run_id=run_id,
        )

    def _execute_memory_recall_plan(self, query: str, *, context: MemoryScopeContext, plan: dict[str, Any], threshold: float = 0.0):
        result = self.search_memory(
            query,
            user_id=context.user_id,
            session_id=context.session_id,
            agent_id=context.agent_id,
            owner_agent_id=context.owner_agent_id,
            subject_type=context.subject_type,
            subject_id=context.subject_id,
            interaction_type=context.interaction_type,
            run_id=context.run_id,
            actor_id=context.actor_id,
            role=context.role,
            scope=plan.get("scope", "all"),
            limit=plan.get("limit", 10),
            threshold=threshold,
        )
        relations_map = {item["id"]: self._relations_for_ref(item["id"]) for item in result["results"]}
        return result["results"], relations_map

    def _deserialize(self, row: dict[str, Any] | None, json_fields: tuple[str, ...] = ("metadata",)) -> dict[str, Any] | None:
        if row is None:
            return None
        item = dict(row)
        for field in json_fields:
            if field in item and isinstance(item[field], str):
                from aimemory.core.utils import json_loads

                item[field] = json_loads(item[field], {})
        return item
