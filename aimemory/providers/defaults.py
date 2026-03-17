from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from aimemory.algorithms.affinity import (
    MEMORY_TYPE_PROTOTYPES,
    best_label,
    blend_score_maps,
    choose_labels,
    coverage_ratio,
    normalize_score_map,
    prototype_affinities,
    tokens_density,
)
from aimemory.algorithms.distill import AdaptiveDistiller
from aimemory.algorithms.retrieval import mmr_rerank, score_record
from aimemory.core.capabilities import capability_dict
from aimemory.core.governance import governance_scope_rules, resolve_strategy_scope
from aimemory.core.text import normalize_text
from aimemory.domains.memory.models import MemoryType
from aimemory.memory_intelligence.models import (
    FactCandidate,
    MemoryAction,
    MemoryActionType,
    MemoryScopeContext,
    MessagePart,
    NeighborMemory,
    NormalizedMessage,
)
from aimemory.memory_intelligence.policies import MemoryPolicy


TYPE_TO_QUERY_MODE = {
    str(MemoryType.PREFERENCE): "preference",
    str(MemoryType.RELATIONSHIP_SUMMARY): "relationship",
    str(MemoryType.PROFILE): "profile",
    str(MemoryType.PROCEDURAL): "procedural",
    str(MemoryType.EPISODIC): "episodic",
    str(MemoryType.SEMANTIC): "semantic",
}


def _contextual_type_affinity(
    text: str,
    *,
    context: MemoryScopeContext,
    role: str | None = None,
) -> dict[str, float]:
    base = prototype_affinities(text, MEMORY_TYPE_PROTOTYPES, dense_weight=0.76, sparse_weight=0.2, containment_weight=0.04)
    bias = {key: 0.0 for key in MEMORY_TYPE_PROTOTYPES}
    active_role = str(role or context.role or "").lower()
    if context.run_id:
        bias[str(MemoryType.EPISODIC)] += 0.1
    if context.agent_id:
        bias[str(MemoryType.PROCEDURAL)] += 0.08
    if active_role == "assistant":
        bias[str(MemoryType.PROCEDURAL)] += 0.06
    if active_role == "user":
        bias[str(MemoryType.PREFERENCE)] += 0.03
        bias[str(MemoryType.PROFILE)] += 0.02
    return normalize_score_map(blend_score_maps(base, bias))


def infer_query_profile(query: str, *, context: MemoryScopeContext, policy: MemoryPolicy) -> dict[str, Any]:
    del policy
    type_scores = _contextual_type_affinity(query, context=context)
    focus_memory_types = choose_labels(type_scores, preferred=[str(MemoryType.SEMANTIC)], top_n=4)
    primary_scope = "session" if context.session_id else "long-term"
    strategy_scope = resolve_strategy_scope(
        best_label(type_scores, default=str(MemoryType.SEMANTIC)),
        agent_id=context.agent_id,
        run_id=context.run_id,
        role=context.role,
        metadata=context.as_metadata(),
        text=query,
    )
    dominant_type = best_label(type_scores, default=str(MemoryType.SEMANTIC))
    query_mode = TYPE_TO_QUERY_MODE.get(dominant_type, "semantic")
    return {
        "query_mode": query_mode,
        "focus_memory_types": focus_memory_types,
        "preferred_scope": primary_scope,
        "strategy_scope": strategy_scope,
        "needs_interaction": False,
        "needs_execution": False,
        "needs_archive": False,
        "handoff_domains": [],
        "domain_scores": {},
        "type_scores": type_scores,
    }


class NoopLLMProvider:
    def generate(self, messages: list[dict[str, Any]], *, response_format: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"messages": messages, "response_format": response_format}

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="llm",
            provider="noop",
            features={
                "generation": False,
                "structured_output": False,
                "remote_model": False,
            },
            notes=["placeholder provider for lightweight local mode"],
        )


class TextOnlyVisionProcessor:
    def normalize(self, messages: Any) -> list[NormalizedMessage]:
        if isinstance(messages, str):
            return [NormalizedMessage(role="user", content=messages, parts=[MessagePart(kind="text", text=messages)])]
        if isinstance(messages, dict):
            return [self._normalize_message(messages)]
        if not isinstance(messages, list):
            raise TypeError("messages must be str, dict, or list[dict]")
        return [self._normalize_message(message) for message in messages]

    def _normalize_message(self, message: dict[str, Any]) -> NormalizedMessage:
        role = message.get("role", "user")
        metadata = dict(message.get("metadata") or {})
        actor_id = message.get("name") or metadata.get("actor_id")
        content = message.get("content")
        if isinstance(content, str):
            return NormalizedMessage(role=role, content=content, actor_id=actor_id, metadata=metadata, parts=[MessagePart(kind="text", text=content)])

        parts: list[MessagePart] = []
        texts: list[str] = []
        omitted_parts: list[str] = []

        if isinstance(content, dict):
            extracted = str(content.get("text") or content.get("content") or "")
            if extracted:
                texts.append(extracted)
                parts.append(MessagePart(kind=content.get("type", "text"), text=extracted, payload=content))
            else:
                omitted_parts.append(content.get("type", "object"))
                parts.append(MessagePart(kind=content.get("type", "object"), payload=content))
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, str):
                    texts.append(item)
                    parts.append(MessagePart(kind="text", text=item, payload=item))
                    continue
                if isinstance(item, dict):
                    text = str(item.get("text") or item.get("content") or "")
                    kind = str(item.get("type", "object"))
                    if text:
                        texts.append(text)
                        parts.append(MessagePart(kind=kind, text=text, payload=item))
                    else:
                        omitted_parts.append(kind)
                        parts.append(MessagePart(kind=kind, payload=item))
        else:
            texts.append(str(content or ""))
            parts.append(MessagePart(kind="text", text=str(content or ""), payload=content))

        if omitted_parts:
            metadata["omitted_modalities"] = omitted_parts
        return NormalizedMessage(role=role, content=" ".join(part for part in texts if part).strip(), actor_id=actor_id, metadata=metadata, parts=parts)

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="vision",
            provider="text-only",
            features={
                "text_normalization": True,
                "multimodal_placeholder": True,
                "image_understanding": False,
            },
            notes=["non-text modalities are preserved as metadata placeholders"],
        )


class SemanticFactExtractor:
    def extract(
        self,
        messages: list[NormalizedMessage],
        *,
        context: MemoryScopeContext,
        policy: MemoryPolicy,
        memory_type: str | None = None,
    ) -> list[FactCandidate]:
        distiller = AdaptiveDistiller(policy)
        distilled = distiller.distill(messages, background_texts=[], memory_type=memory_type or str(MemoryType.SEMANTIC))
        candidates: list[FactCandidate] = []
        seen: set[str] = set()
        for item in distilled:
            source_role = str(item.metadata.get("source_role") or "user")
            if source_role == "tool":
                continue
            normalized = normalize_text(item.text)
            if normalized in seen:
                continue
            seen.add(normalized)
            type_scores = _contextual_type_affinity(item.text, context=context, role=source_role)
            chosen_type = str(memory_type or best_label(type_scores, default=str(MemoryType.SEMANTIC)))
            strategy_scope = resolve_strategy_scope(
                chosen_type,
                agent_id=context.agent_id,
                run_id=context.run_id,
                role=source_role,
                metadata={**context.as_metadata(), **dict(item.metadata)},
                text=item.text,
            )
            candidates.append(
                FactCandidate(
                    text=item.text,
                    memory_type=chosen_type,
                    confidence=round(max(0.22, (0.6 * item.score) + (0.4 * type_scores.get(chosen_type, 0.0))), 6),
                    importance=round(min(1.0, 0.28 + (0.52 * item.score) + (0.08 if strategy_scope == "user" else 0.04)), 6),
                    metadata={
                        **context.as_metadata(),
                        **dict(item.metadata),
                        "keywords": [],
                        "source_role": source_role,
                        "strategy_scope": strategy_scope,
                        "classification_score": type_scores.get(chosen_type, 0.0),
                        "novelty": item.novelty,
                        "informativeness": item.informativeness,
                        "density": item.density,
                    },
                )
            )
        candidates.sort(key=lambda candidate: (candidate.importance, candidate.confidence, len(candidate.text)), reverse=True)
        return candidates[: policy.max_candidates]

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="extractor",
            provider="semantic",
            features={
                "local_algorithmic_extraction": True,
                "adaptive_distillation": True,
                "typed_memory_projection": True,
                "llm_required": False,
            },
        )


class AdaptiveMemoryPlanner:
    def plan(
        self,
        candidate: FactCandidate,
        neighbors: list[NeighborMemory],
        *,
        context: MemoryScopeContext,
        policy: MemoryPolicy,
    ) -> list[MemoryAction]:
        if not neighbors:
            return [MemoryAction(MemoryActionType.ADD, candidate=candidate, reason="novel-memory", confidence=candidate.confidence)]

        ranked = sorted(neighbors, key=lambda item: self._neighbor_score(candidate, item), reverse=True)
        best = ranked[0]
        similarity = self._neighbor_score(candidate, best)
        improvement = self._improvement_score(candidate, best)

        if similarity >= policy.duplicate_threshold:
            if improvement >= 0.08:
                return [
                    MemoryAction(
                        MemoryActionType.UPDATE,
                        candidate=candidate,
                        reason="higher-information-duplicate",
                        target_id=best.id,
                        previous_text=best.text,
                        confidence=min(1.0, similarity + improvement),
                    )
                ]
            return [MemoryAction(MemoryActionType.NONE, candidate=candidate, reason="semantic-duplicate", target_id=best.id, previous_text=best.text, confidence=similarity)]

        if similarity >= policy.merge_threshold:
            return [
                MemoryAction(
                    MemoryActionType.UPDATE,
                    candidate=candidate,
                    reason="semantic-merge",
                    target_id=best.id,
                    previous_text=best.text,
                    confidence=min(1.0, similarity + max(0.0, improvement)),
                )
            ]

        return [MemoryAction(MemoryActionType.ADD, candidate=candidate, reason="novel-memory", confidence=max(candidate.confidence, similarity * 0.5))]

    def _neighbor_score(self, candidate: FactCandidate, neighbor: NeighborMemory) -> float:
        from aimemory.algorithms.dedupe import semantic_similarity

        similarity = semantic_similarity(candidate.text, neighbor.text)
        if neighbor.memory_type == candidate.memory_type:
            similarity += 0.06
        similarity += min(0.06, float(neighbor.importance) * 0.08)
        return round(min(1.0, similarity), 6)

    def _improvement_score(self, candidate: FactCandidate, neighbor: NeighborMemory) -> float:
        candidate_density = tokens_density(candidate.text)
        neighbor_density = tokens_density(neighbor.text)
        candidate_coverage = coverage_ratio(candidate.text, neighbor.text)
        neighbor_coverage = coverage_ratio(neighbor.text, candidate.text)
        return round(
            (0.42 * (float(candidate.confidence) - float(neighbor.metadata.get("confidence", 0.5) or 0.5)))
            + (0.28 * (float(candidate.importance) - float(neighbor.importance)))
            + (0.2 * (candidate_density - neighbor_density))
            + (0.1 * (candidate_coverage - neighbor_coverage)),
            6,
        )

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="planner",
            provider="adaptive",
            features={
                "duplicate_detection": True,
                "semantic_merge": True,
                "local_algorithmic_planning": True,
                "llm_required": False,
            },
        )


class HybridReranker:
    def rerank(
        self,
        query: str,
        records: list[dict[str, Any]],
        *,
        domain: str,
        context: MemoryScopeContext,
        policy: MemoryPolicy | None = None,
    ) -> list[dict[str, Any]]:
        del domain
        query_profile = infer_query_profile(query, context=context, policy=policy or MemoryPolicy())
        focus_types = set(query_profile["focus_memory_types"])
        ranked: list[dict[str, Any]] = []
        for record in records:
            updated = dict(record)
            text = str(updated.get("text") or updated.get("content") or updated.get("summary") or "")
            score, breakdown = score_record(
                query,
                text=text,
                keywords=updated.get("keywords"),
                updated_at=updated.get("updated_at") or updated.get("created_at"),
                importance=float(updated.get("importance", 0.5) or 0.0),
            )
            metadata = dict(updated.get("metadata") or {})
            if focus_types and str(updated.get("memory_type")) in focus_types:
                score += 0.08
            if context.session_id and updated.get("session_id") == context.session_id:
                score += 0.05
            if context.user_id and updated.get("user_id") == context.user_id:
                score += 0.03
            if context.actor_id and metadata.get("actor_id") == context.actor_id:
                score += 0.03
            graph_context = dict(updated.get("graph_context") or {})
            score += min(0.06, float(graph_context.get("matched_relation_count", 0)) * 0.02)
            updated["score"] = round(score, 6)
            updated["score_breakdown"] = {**breakdown, "domain_bias": 0.0}
            ranked.append(updated)
        ranked.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        return mmr_rerank(ranked, lambda_value=(policy or MemoryPolicy()).diversity_lambda)

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="reranker",
            provider="hybrid",
            features={
                "hybrid_scoring": True,
                "graph_aware": True,
                "context_aware": True,
                "llm_required": False,
            },
        )


class AdaptiveRecallPlanner:
    def plan(
        self,
        query: str,
        *,
        context: MemoryScopeContext,
        policy: MemoryPolicy,
        preferred_scope: str | None = None,
        limit: int | None = None,
        auxiliary_limit: int | None = None,
        graph_enabled: bool = True,
    ) -> dict[str, Any]:
        profile = infer_query_profile(query, context=context, policy=policy)
        strategy_scope = str(profile["strategy_scope"])
        primary_scope = preferred_scope or str(profile["preferred_scope"])
        secondary_scope = "long-term" if primary_scope == "session" else "session"
        primary_limit = int(limit or policy.search_limit)
        secondary_limit = int(policy.auxiliary_search_limit if auxiliary_limit is None else auxiliary_limit)
        scope_policy = governance_scope_rules(strategy_scope)
        focus_memory_types = list(dict.fromkeys(profile["focus_memory_types"]))
        stages = [
            {
                "name": "primary",
                "scope": primary_scope,
                "limit": primary_limit,
                "targetable": True,
                "score_bias": 0.08 if primary_scope == "session" else 0.05,
                "memory_types": focus_memory_types,
                "strategy_scopes": [strategy_scope],
            }
        ]
        if secondary_limit > 0:
            stages.append(
                {
                    "name": "auxiliary",
                    "scope": secondary_scope,
                    "limit": secondary_limit,
                    "targetable": False,
                    "score_bias": 0.03 if secondary_scope == "long-term" else 0.01,
                    "memory_types": focus_memory_types[:2],
                    "strategy_scopes": ["user", "agent", "run"],
                }
            )
        return {
            "strategy_scope": strategy_scope,
            "strategy_name": f"{strategy_scope}-{profile['query_mode']}-{primary_scope}-adaptive",
            "query_profile": profile,
            "graph_enrichment": bool(graph_enabled),
            "handoff_domains": list(dict.fromkeys(profile["handoff_domains"])),
            "policy_notes": list(scope_policy.get("notes", [])),
            "stages": stages,
        }

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="recall_planner",
            provider="adaptive",
            features={
                "multi_stage_recall": True,
                "scope_aware": True,
                "typed_recall": True,
                "domain_handoff_hints": True,
                "graph_hinting": True,
                "llm_required": False,
            },
        )
