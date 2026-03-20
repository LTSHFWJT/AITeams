from __future__ import annotations

import re
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
from aimemory.core.text import normalize_text, split_sentences
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
from aimemory.memory_intelligence.semantic_categories import (
    ALWAYS_MERGE_CATEGORIES,
    APPEND_ONLY_CATEGORIES,
    MERGE_SUPPORTED_CATEGORIES,
    TEMPORAL_VERSIONED_CATEGORIES,
    build_semantic_abstract,
    build_semantic_overview,
    category_to_memory_type,
    default_confidence_for_category,
    default_importance_for_category,
    default_tier_for_category,
    derive_fact_key,
    derive_topic_key,
    extract_semantic_keywords,
    infer_context_label,
    infer_semantic_category,
    memory_type_to_category,
    normalize_semantic_category,
    semantic_key_text,
)


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
    def _resolved_category(
        self,
        text: str,
        *,
        source_role: str,
        memory_type: str | None,
        metadata: dict[str, Any],
    ) -> str:
        explicit = memory_type_to_category(memory_type)
        if explicit is not None and memory_type != str(MemoryType.SEMANTIC):
            return explicit
        return infer_semantic_category(
            text,
            role=source_role,
            memory_type=memory_type,
            metadata=metadata,
        )

    def _base_confidence(self, item_score: float, category: str, type_score: float) -> float:
        category_floor = default_confidence_for_category(category, fallback=0.68)
        return round(min(1.0, max(0.28, (0.48 * item_score) + (0.22 * type_score) + (0.3 * category_floor))), 6)

    def _base_importance(self, item_score: float, category: str, *, strategy_scope: str) -> float:
        category_bias = default_importance_for_category(category, fallback=0.5)
        scope_bonus = 0.08 if strategy_scope == "user" else 0.04
        return round(min(1.0, max(category_bias, (0.55 * category_bias) + (0.35 * item_score) + scope_bonus)), 6)

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
            candidate_category = self._resolved_category(
                item.text,
                source_role=source_role,
                memory_type=memory_type or best_label(type_scores, default=str(MemoryType.SEMANTIC)),
                metadata=dict(item.metadata),
            )
            chosen_type = category_to_memory_type(candidate_category, default=str(memory_type or best_label(type_scores, default=str(MemoryType.SEMANTIC))))
            strategy_scope = resolve_strategy_scope(
                chosen_type,
                agent_id=context.agent_id,
                run_id=context.run_id,
                role=source_role,
                metadata={**context.as_metadata(), **dict(item.metadata)},
                text=item.text,
            )
            keywords = extract_semantic_keywords(item.text, limit=8)
            abstract = build_semantic_abstract(item.text, candidate_category, keywords=keywords)
            overview = build_semantic_overview(item.text, candidate_category, keywords=keywords)
            content = item.text.strip()
            topic_key = derive_topic_key(candidate_category, abstract)
            fact_key = derive_fact_key(candidate_category, abstract)
            context_label = infer_context_label(content)
            confidence = self._base_confidence(item.score, candidate_category, type_scores.get(chosen_type, 0.0))
            importance = self._base_importance(item.score, candidate_category, strategy_scope=strategy_scope)
            candidates.append(
                FactCandidate(
                    text=abstract or item.text,
                    memory_type=chosen_type,
                    confidence=confidence,
                    importance=importance,
                    semantic_category=candidate_category,
                    abstract=abstract,
                    overview=overview,
                    content=content,
                    fact_key=fact_key,
                    topic_key=topic_key,
                    context_label=context_label,
                    metadata={
                        **context.as_metadata(),
                        **dict(item.metadata),
                        "keywords": keywords,
                        "source_role": source_role,
                        "strategy_scope": strategy_scope,
                        "classification_score": type_scores.get(chosen_type, 0.0),
                        "novelty": item.novelty,
                        "informativeness": item.informativeness,
                        "density": item.density,
                        "semantic_category": candidate_category,
                        "memory_category": candidate_category,
                        "summary_l0": abstract,
                        "summary_l1": overview,
                        "l0_abstract": abstract,
                        "l1_overview": overview,
                        "l2_content": content,
                        "topic_key": topic_key,
                        "fact_key": fact_key,
                        "confidence": confidence,
                        "tier": default_tier_for_category(candidate_category),
                        "valid_from": int(datetime.now(timezone.utc).timestamp() * 1000),
                        "contexts": [context_label] if context_label != "general" else [],
                    },
                )
            )
        candidates.sort(
            key=lambda candidate: (
                candidate.importance,
                candidate.confidence,
                len(candidate.storage_text() or candidate.text),
            ),
            reverse=True,
        )
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
    def _resolved_category(self, candidate: FactCandidate) -> str | None:
        category = candidate.normalized_category()
        if category is not None:
            return category
        inferred = memory_type_to_category(candidate.memory_type)
        if inferred is not None:
            return inferred
        return infer_semantic_category(candidate.storage_text() or candidate.text, memory_type=candidate.memory_type, metadata=candidate.metadata)

    def _neighbor_category(self, neighbor: NeighborMemory) -> str | None:
        category = neighbor.normalized_category()
        if category is not None:
            return category
        inferred = memory_type_to_category(neighbor.memory_type)
        if inferred is not None:
            return inferred
        return infer_semantic_category(neighbor.storage_text() or neighbor.text, memory_type=neighbor.memory_type, metadata=neighbor.metadata)

    def plan(
        self,
        candidate: FactCandidate,
        neighbors: list[NeighborMemory],
        *,
        context: MemoryScopeContext,
        policy: MemoryPolicy,
    ) -> list[MemoryAction]:
        del context
        if not neighbors:
            return [MemoryAction(MemoryActionType.ADD, candidate=candidate, reason="novel-memory", confidence=candidate.confidence)]

        category = self._resolved_category(candidate)
        active_neighbors = [item for item in neighbors if str(item.status or "active") == "active"] or list(neighbors)
        ranked = sorted(active_neighbors, key=lambda item: self._neighbor_score(candidate, item), reverse=True)
        relation_targets = self._relation_targets(candidate, ranked, policy=policy)
        same_category_ranked = [item for item in ranked if self._neighbor_category(item) == category and bool(item.metadata.get("targetable", True))]
        best = same_category_ranked[0] if same_category_ranked else None
        context_label = candidate.context_label or infer_context_label(candidate.storage_text() or candidate.text)

        if best is None:
            if relation_targets:
                return [
                    MemoryAction(
                        MemoryActionType.LINK,
                        candidate=candidate,
                        reason="related-memory-link",
                        confidence=max(candidate.confidence, self._neighbor_score(candidate, ranked[0])),
                        link_target_ids=relation_targets,
                        link_type="related",
                        evidence={"relation": "semantic_related", "target_count": len(relation_targets), "category": category},
                    )
                ]
            return [MemoryAction(MemoryActionType.ADD, candidate=candidate, reason="novel-memory", confidence=candidate.confidence)]

        similarity = self._neighbor_score(candidate, best)
        improvement = self._improvement_score(candidate, best)
        same_topic = self._same_topic(candidate, best)

        if category in ALWAYS_MERGE_CATEGORIES:
            return [
                MemoryAction(
                    MemoryActionType.MERGE,
                    candidate=candidate,
                    reason="profile-always-merge",
                    target_id=best.id,
                    previous_text=best.storage_text(),
                    confidence=min(1.0, max(similarity, candidate.confidence)),
                    evidence={"category": category, "topic_match": same_topic},
                )
            ]

        if category in APPEND_ONLY_CATEGORIES:
            if same_topic and similarity >= policy.duplicate_threshold:
                return [
                    MemoryAction(
                        MemoryActionType.NONE,
                        candidate=candidate,
                        reason="append-only-duplicate",
                        target_id=best.id,
                        previous_text=best.storage_text(),
                        confidence=similarity,
                        evidence={"category": category},
                    )
                ]
            if relation_targets:
                return [
                    MemoryAction(
                        MemoryActionType.LINK,
                        candidate=candidate,
                        reason="append-only-linked",
                        confidence=max(candidate.confidence, similarity),
                        link_target_ids=relation_targets,
                        link_type="related",
                        evidence={"category": category, "append_only": True},
                    )
                ]
            return [MemoryAction(MemoryActionType.ADD, candidate=candidate, reason="append-only-create", confidence=max(candidate.confidence, similarity * 0.5))]

        if same_topic and category in TEMPORAL_VERSIONED_CATEGORIES and self._is_version_shift(candidate, best):
            return [
                MemoryAction(
                    MemoryActionType.SUPERSEDE,
                    candidate=candidate,
                    reason="temporal-version-shift",
                    target_id=best.id,
                    previous_text=best.storage_text(),
                    confidence=min(1.0, max(similarity, similarity + max(0.0, improvement))),
                    context_label=context_label,
                    evidence={"relation": "version_shift", "target_scope": best.scope, "category": category},
                )
            ]

        if same_topic and self._is_contradiction(candidate, best):
            action_type = (
                MemoryActionType.SUPERSEDE
                if category in TEMPORAL_VERSIONED_CATEGORIES and context_label == "general"
                else MemoryActionType.CONTRADICT
            )
            return [
                MemoryAction(
                    action_type,
                    candidate=candidate,
                    reason="semantic-contradiction",
                    target_id=best.id,
                    previous_text=best.storage_text(),
                    confidence=min(1.0, max(similarity, candidate.confidence)),
                    context_label=context_label,
                    evidence={"relation": "contradiction", "category": category},
                )
            ]

        if same_topic and self._is_support_signal(candidate) and similarity >= policy.support_threshold:
            return [
                MemoryAction(
                    MemoryActionType.SUPPORT,
                    candidate=candidate,
                    reason="support-existing-memory",
                    target_id=best.id,
                    previous_text=best.storage_text(),
                    confidence=min(1.0, max(similarity, candidate.confidence)),
                    context_label=context_label,
                    evidence={"relation": "support", "category": category},
                )
            ]

        if same_topic and context_label != "general" and similarity >= policy.contextualize_threshold and self._is_contextual_variant(candidate, best):
            return [
                MemoryAction(
                    MemoryActionType.CONTEXTUALIZE,
                    candidate=candidate,
                    reason="contextualize-existing-memory",
                    target_id=best.id,
                    previous_text=best.storage_text(),
                    confidence=min(1.0, max(similarity, candidate.confidence)),
                    context_label=context_label,
                    link_type="contextualizes",
                    evidence={"relation": "contextualize", "category": category},
                )
            ]

        if same_topic and similarity >= policy.duplicate_threshold:
            if improvement >= 0.08 and category in MERGE_SUPPORTED_CATEGORIES:
                return [
                    MemoryAction(
                        MemoryActionType.MERGE,
                        candidate=candidate,
                        reason="higher-information-merge",
                        target_id=best.id,
                        previous_text=best.storage_text(),
                        confidence=min(1.0, similarity + improvement),
                        evidence={"category": category},
                    )
                ]
            return [
                MemoryAction(
                    MemoryActionType.NONE,
                    candidate=candidate,
                    reason="semantic-duplicate",
                    target_id=best.id,
                    previous_text=best.storage_text(),
                    confidence=similarity,
                    evidence={"category": category},
                )
            ]

        if same_topic and similarity >= policy.merge_threshold and category in MERGE_SUPPORTED_CATEGORIES:
            return [
                MemoryAction(
                    MemoryActionType.MERGE,
                    candidate=candidate,
                    reason="semantic-merge",
                    target_id=best.id,
                    previous_text=best.storage_text(),
                    confidence=min(1.0, similarity + max(0.0, improvement)),
                    evidence={"category": category},
                )
            ]

        if relation_targets:
            return [
                MemoryAction(
                    MemoryActionType.LINK,
                    candidate=candidate,
                    reason="related-memory-link",
                    confidence=max(candidate.confidence, similarity),
                    link_target_ids=relation_targets,
                    link_type="related",
                    evidence={"relation": "semantic_related", "target_count": len(relation_targets), "category": category},
                )
            ]

        if same_topic and improvement >= 0.08 and category in MERGE_SUPPORTED_CATEGORIES:
            return [
                MemoryAction(
                    MemoryActionType.MERGE,
                    candidate=candidate,
                    reason="topic-refinement-merge",
                    target_id=best.id,
                    previous_text=best.storage_text(),
                    confidence=min(1.0, max(candidate.confidence, similarity)),
                    evidence={"category": category},
                )
            ]

        return [MemoryAction(MemoryActionType.ADD, candidate=candidate, reason="novel-memory", confidence=max(candidate.confidence, similarity * 0.5))]

    def _relation_targets(self, candidate: FactCandidate, ranked: list[NeighborMemory], *, policy: MemoryPolicy) -> list[str]:
        category = self._resolved_category(candidate)
        targets: list[str] = []
        for item in ranked:
            if not bool(item.metadata.get("targetable", True)):
                continue
            if self._neighbor_category(item) == category and self._same_topic(candidate, item):
                continue
            score = self._neighbor_score(candidate, item)
            if score < policy.relation_threshold:
                continue
            if item.id not in targets:
                targets.append(item.id)
            if len(targets) >= 3:
                break
        return targets

    def _neighbor_score(self, candidate: FactCandidate, neighbor: NeighborMemory) -> float:
        from aimemory.algorithms.dedupe import semantic_similarity

        candidate_text = candidate.index_text() or candidate.storage_text() or candidate.text
        neighbor_text = neighbor.index_text() or neighbor.storage_text() or neighbor.text
        similarity = semantic_similarity(candidate_text, neighbor_text)
        if neighbor.memory_type == candidate.memory_type:
            similarity += 0.06
        if self._neighbor_category(neighbor) == self._resolved_category(candidate):
            similarity += 0.06
        if self._same_topic(candidate, neighbor):
            similarity += 0.1
        similarity += min(0.06, float(neighbor.importance) * 0.08)
        return round(min(1.0, similarity), 6)

    def _improvement_score(self, candidate: FactCandidate, neighbor: NeighborMemory) -> float:
        candidate_text = candidate.storage_text() or candidate.index_text() or candidate.text
        neighbor_text = neighbor.storage_text() or neighbor.index_text() or neighbor.text
        candidate_density = tokens_density(candidate_text)
        neighbor_density = tokens_density(neighbor_text)
        candidate_coverage = coverage_ratio(candidate_text, neighbor_text)
        neighbor_coverage = coverage_ratio(neighbor_text, candidate_text)
        return round(
            (0.42 * (float(candidate.confidence) - float(neighbor.confidence or neighbor.metadata.get("confidence", 0.5) or 0.5)))
            + (0.28 * (float(candidate.importance) - float(neighbor.importance)))
            + (0.2 * (candidate_density - neighbor_density))
            + (0.1 * (candidate_coverage - neighbor_coverage)),
            6,
        )

    def _same_topic(self, candidate: FactCandidate, neighbor: NeighborMemory) -> bool:
        candidate_fact_key = str(candidate.fact_key or candidate.metadata.get("fact_key") or "").strip()
        neighbor_fact_key = str(neighbor.metadata.get("fact_key") or "").strip()
        if candidate_fact_key and candidate_fact_key == neighbor_fact_key:
            return True
        candidate_topic = str(candidate.topic_key or candidate.metadata.get("topic_key") or derive_topic_key(self._resolved_category(candidate), candidate.index_text()) or "").strip()
        neighbor_topic = str(neighbor.metadata.get("topic_key") or derive_topic_key(self._neighbor_category(neighbor), neighbor.index_text()) or "").strip()
        if candidate_topic and candidate_topic == neighbor_topic:
            return True

        candidate_keywords = {semantic_key_text(item) for item in candidate.metadata.get("keywords", []) if semantic_key_text(item)}
        neighbor_keywords = {semantic_key_text(item) for item in neighbor.metadata.get("keywords", []) if semantic_key_text(item)}
        if candidate_keywords and neighbor_keywords:
            overlap = len(candidate_keywords & neighbor_keywords) / max(1, len(candidate_keywords | neighbor_keywords))
            if overlap >= 0.5:
                return True

        candidate_text = candidate.index_text() or candidate.storage_text() or candidate.text
        neighbor_text = neighbor.index_text() or neighbor.storage_text() or neighbor.text
        if coverage_ratio(candidate_text, neighbor_text) >= 0.82 or coverage_ratio(neighbor_text, candidate_text) >= 0.82:
            return True
        return False

    def _is_support_signal(self, candidate: FactCandidate) -> bool:
        text = normalize_text(candidate.storage_text() or candidate.text)
        return any(marker in text for marker in ("still", "again", "continues to", "仍然", "依旧", "还是", "继续"))

    def _is_contextual_variant(self, candidate: FactCandidate, neighbor: NeighborMemory) -> bool:
        candidate_context = candidate.context_label or infer_context_label(candidate.storage_text() or candidate.text)
        neighbor_contexts = {
            normalize_text(str(item))
            for item in (neighbor.metadata.get("contexts") or [])
            if str(item).strip()
        }
        if candidate_context != "general" and normalize_text(candidate_context) not in neighbor_contexts:
            return True
        candidate_text = normalize_text(candidate.storage_text() or candidate.text)
        neighbor_text = normalize_text(neighbor.storage_text() or neighbor.text)
        return candidate_text != neighbor_text and self._same_topic(candidate, neighbor)

    def _is_contradiction(self, candidate: FactCandidate, neighbor: NeighborMemory) -> bool:
        candidate_text = normalize_text(candidate.storage_text() or candidate.text)
        neighbor_text = normalize_text(neighbor.storage_text() or neighbor.text)
        if not candidate_text or not neighbor_text or not self._same_topic(candidate, neighbor):
            return False
        if self._is_version_shift(candidate, neighbor):
            return True

        negations = ("not", "no longer", "never", "停止", "不再", "不是", "不用", "取消")
        candidate_negated = any(marker in candidate_text for marker in negations)
        neighbor_negated = any(marker in neighbor_text for marker in negations)
        if candidate_negated != neighbor_negated:
            return True

        candidate_choices = set(re.findall(r"\b[a-z][a-z0-9_.-]{1,32}\b|[\u4e00-\u9fff]{2,12}", candidate_text))
        neighbor_choices = set(re.findall(r"\b[a-z][a-z0-9_.-]{1,32}\b|[\u4e00-\u9fff]{2,12}", neighbor_text))
        if candidate_choices and neighbor_choices:
            overlap = len(candidate_choices & neighbor_choices) / max(1, len(candidate_choices | neighbor_choices))
            if overlap < 0.45 and any(marker in candidate_text for marker in ("prefer", "preferred", "改为", "切换到", "选择", "使用")):
                return True
        return False

    def _is_version_shift(self, candidate: FactCandidate, neighbor: NeighborMemory) -> bool:
        candidate_text = normalize_text(candidate.storage_text() or candidate.text)
        neighbor_text = normalize_text(neighbor.storage_text() or neighbor.text)
        if not candidate_text or not neighbor_text:
            return False
        if bool(candidate.metadata.get("force_supersede")):
            return True

        candidate_numbers = set(re.findall(r"\d+(?:\.\d+)*", candidate_text))
        neighbor_numbers = set(re.findall(r"\d+(?:\.\d+)*", neighbor_text))
        numeric_shift = bool(candidate_numbers and neighbor_numbers and candidate_numbers != neighbor_numbers)

        candidate_markers = (
            "改为",
            "改成",
            "变更为",
            "更新为",
            "切换到",
            "升级到",
            "现在",
            "当前",
            "不再",
            "instead of",
            "replaced by",
            "replace with",
            "migrated to",
        )
        neighbor_markers = ("旧版", "之前", "曾经", "legacy", "deprecated")
        marker_shift = any(marker in candidate_text for marker in candidate_markers) or any(marker in neighbor_text for marker in neighbor_markers)

        candidate_version = candidate.metadata.get("version")
        neighbor_version = neighbor.metadata.get("version")
        metadata_shift = candidate_version is not None and neighbor_version is not None and candidate_version != neighbor_version

        return bool(numeric_shift or marker_shift or metadata_shift)

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="planner",
            provider="adaptive",
            features={
                "duplicate_detection": True,
                "semantic_merge": True,
                "category_strategy": True,
                "support_context_contradiction": True,
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
