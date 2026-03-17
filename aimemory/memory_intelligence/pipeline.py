from __future__ import annotations

from typing import Any

from aimemory.algorithms.affinity import coverage_ratio, tokens_density
from aimemory.algorithms.dedupe import semantic_similarity
from aimemory.core.text import character_ngrams, normalize_text, tokenize
from aimemory.memory_intelligence.models import (
    FactCandidate,
    MemoryAction,
    MemoryActionType,
    MemoryMutationResult,
    MemoryScopeContext,
    NeighborMemory,
    NormalizedMessage,
)
from aimemory.memory_intelligence.policies import MemoryPolicy


class MemoryIntelligencePipeline:
    def __init__(self, *, vision_processor, extractor, planner, memory_service, retrieval_service, policy: MemoryPolicy):
        self.vision_processor = vision_processor
        self.extractor = extractor
        self.planner = planner
        self.memory_service = memory_service
        self.retrieval_service = retrieval_service
        self.policy = policy

    def add(
        self,
        messages: Any,
        *,
        context: MemoryScopeContext,
        metadata: dict[str, Any] | None = None,
        long_term: bool = True,
        memory_type: str | None = None,
        source: str = "conversation",
        infer: bool = True,
    ) -> dict[str, Any]:
        if messages and isinstance(messages, list) and isinstance(messages[0], NormalizedMessage):
            normalized_messages = messages
        else:
            normalized_messages = self.vision_processor.normalize(messages)
        if not infer:
            return self._store_raw_messages(normalized_messages, context=context, metadata=metadata, long_term=long_term, memory_type=memory_type, source=source)

        candidates = self.extractor.extract(
            normalized_messages,
            context=context,
            policy=self.policy,
            memory_type=memory_type,
        )
        candidates = self._consolidate_candidates(candidates)
        if not candidates:
            return self._store_raw_messages(normalized_messages, context=context, metadata=metadata, long_term=long_term, memory_type=memory_type, source=source)

        planned_actions: list[MemoryAction] = []
        for candidate in candidates:
            neighbors = self._retrieve_neighbors(candidate.text, context=context, long_term=long_term)
            planned_actions.extend(self.planner.plan(candidate, neighbors, context=context, policy=self.policy))

        results: list[dict[str, Any]] = []
        for action in self._arbitrate_actions(planned_actions):
            result = self._apply_action(action, context=context, metadata=metadata, long_term=long_term, source=source)
            if result:
                results.append(result)
        return {"results": results, "facts": [candidate.text for candidate in candidates]}

    def _store_raw_messages(
        self,
        normalized_messages,
        *,
        context: MemoryScopeContext,
        metadata: dict[str, Any] | None,
        long_term: bool,
        memory_type: str | None,
        source: str,
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for message in normalized_messages:
            if not message.content.strip() or message.role == "system":
                continue
            entry_metadata = dict(metadata or {})
            entry_metadata.update(context.clone_with(role=message.role, actor_id=message.actor_id).as_metadata())
            entry_metadata.update(message.metadata)
            created = self.memory_service.remember(
                text=message.content,
                user_id=context.user_id,
                agent_id=context.agent_id,
                session_id=context.session_id,
                run_id=context.run_id,
                metadata=entry_metadata,
                memory_type=memory_type or "semantic",
                importance=0.6,
                long_term=long_term,
                source=source,
            )
            results.append(MemoryMutationResult(id=created["id"], memory=created["text"], event="ADD", reason="raw-store").as_dict())
        return {"results": results, "facts": [item["memory"] for item in results]}

    def _retrieve_neighbors(self, query: str, *, context: MemoryScopeContext, long_term: bool) -> list[NeighborMemory]:
        primary_scope = "long-term" if long_term else "session"
        plan = self.retrieval_service.plan_memory_recall(
            query,
            context=context,
            preferred_scope=primary_scope,
            limit=self.policy.search_limit,
            auxiliary_limit=self.policy.auxiliary_search_limit,
        )
        found, _relations = self.retrieval_service._execute_memory_recall_plan(query, context=context, plan=plan, threshold=0.0)

        neighbors: dict[str, NeighborMemory] = {}
        for item in found:
            metadata = dict(item.get("metadata", {}))
            metadata["recall_source"] = str(item.get("recall_stage") or "primary")
            metadata["targetable"] = bool(item.get("targetable", item.get("recall_scope") == primary_scope))
            neighbors[item["id"]] = NeighborMemory(
                id=item["id"],
                text=item["text"],
                score=float(item.get("score", 0.0)),
                scope=item.get("scope"),
                memory_type=item.get("memory_type"),
                importance=float(item.get("importance", 0.5)),
                metadata=metadata,
            )
        return list(neighbors.values())

    def _consolidate_candidates(self, candidates: list[FactCandidate]) -> list[FactCandidate]:
        consolidated: list[FactCandidate] = []
        for candidate in candidates:
            match_index = self._find_similar_candidate(candidate, consolidated)
            if match_index is None:
                consolidated.append(candidate)
                continue
            consolidated[match_index] = self._merge_candidates(consolidated[match_index], candidate)
        consolidated.sort(key=lambda item: (item.importance, item.confidence, len(item.text)), reverse=True)
        return consolidated[: self.policy.max_candidates]

    def _find_similar_candidate(self, candidate: FactCandidate, consolidated: list[FactCandidate]) -> int | None:
        for index, existing in enumerate(consolidated):
            if existing.memory_type != candidate.memory_type:
                continue
            if self._candidate_similarity(existing, candidate) >= self.policy.candidate_merge_threshold:
                return index
        return None

    def _candidate_similarity(self, left: FactCandidate, right: FactCandidate) -> float:
        normalized_left = self._canonical_text(left.text)
        normalized_right = self._canonical_text(right.text)
        if normalized_left == normalized_right:
            return 1.0
        base_similarity = semantic_similarity(normalized_left, normalized_right)
        left_tokens = set(tokenize(normalized_left))
        right_tokens = set(tokenize(normalized_right))
        left_ngrams = set(character_ngrams(normalized_left, min_n=2, max_n=4))
        right_ngrams = set(character_ngrams(normalized_right, min_n=2, max_n=4))
        char_overlap = 0.0
        if left_ngrams and right_ngrams:
            char_overlap = len(left_ngrams & right_ngrams) / max(1, len(left_ngrams | right_ngrams))
        token_overlap = 0.0
        if left_tokens and right_tokens:
            token_overlap = len(left_tokens & right_tokens) / max(1, len(left_tokens | right_tokens))
        coverage = max(coverage_ratio(normalized_left, normalized_right), coverage_ratio(normalized_right, normalized_left))
        containment = 1.0 if normalized_left in normalized_right or normalized_right in normalized_left else 0.0
        score = (
            (0.72 * base_similarity)
            + (0.08 * char_overlap)
            + (0.1 * coverage)
            + (0.1 * token_overlap)
            + (0.05 * containment)
        )
        if base_similarity >= 0.52 and coverage >= 0.62 and (char_overlap >= 0.3 or token_overlap >= 0.46):
            score += 0.22
        elif base_similarity >= 0.46 and coverage >= 0.58 and char_overlap >= 0.24 and token_overlap >= 0.42:
            score += 0.12
        return min(1.0, score)

    def _canonical_text(self, text: str) -> str:
        return normalize_text(text).strip(" .,!?:;，。！？；：")

    def _merge_candidates(self, existing: FactCandidate, incoming: FactCandidate) -> FactCandidate:
        prefer_incoming = self._prefer_incoming(existing, incoming)
        chosen = incoming if prefer_incoming else existing
        merged_metadata = dict(existing.metadata)
        merged_metadata.update(incoming.metadata)
        existing_keywords = existing.metadata.get("keywords")
        incoming_keywords = incoming.metadata.get("keywords")
        if isinstance(existing_keywords, list) or isinstance(incoming_keywords, list):
            merged_metadata["keywords"] = list(
                dict.fromkeys(
                    [
                        *([str(item) for item in existing_keywords] if isinstance(existing_keywords, list) else []),
                        *([str(item) for item in incoming_keywords] if isinstance(incoming_keywords, list) else []),
                    ]
                )
            )
        return FactCandidate(
            text=chosen.text,
            memory_type=chosen.memory_type,
            confidence=max(existing.confidence, incoming.confidence),
            importance=max(existing.importance, incoming.importance),
            source=chosen.source,
            metadata=merged_metadata,
        )

    def _prefer_incoming(self, existing: FactCandidate, incoming: FactCandidate) -> bool:
        existing_score = (
            (0.4 * float(existing.confidence))
            + (0.35 * float(existing.importance))
            + (0.15 * tokens_density(existing.text))
            + (0.1 * coverage_ratio(existing.text, incoming.text))
        )
        incoming_score = (
            (0.4 * float(incoming.confidence))
            + (0.35 * float(incoming.importance))
            + (0.15 * tokens_density(incoming.text))
            + (0.1 * coverage_ratio(incoming.text, existing.text))
        )
        if incoming.memory_type != existing.memory_type and incoming.memory_type != "semantic":
            incoming_score += 0.06
        return incoming_score >= existing_score

    def _arbitrate_actions(self, actions: list[MemoryAction]) -> list[MemoryAction]:
        if not actions:
            return []
        priority = {
            MemoryActionType.DELETE: 4,
            MemoryActionType.UPDATE: 3,
            MemoryActionType.NONE: 2,
            MemoryActionType.ADD: 1,
        }
        winners: dict[str, MemoryAction] = {}
        for action in actions:
            key = action.target_id or f"candidate:{normalize_text(action.candidate.text)}"
            current = winners.get(key)
            if current is None:
                winners[key] = action
                continue
            current_rank = (
                priority[current.action_type],
                float(current.confidence),
                float(current.candidate.importance),
                len(current.candidate.text),
            )
            candidate_rank = (
                priority[action.action_type],
                float(action.confidence),
                float(action.candidate.importance),
                len(action.candidate.text),
            )
            if candidate_rank > current_rank:
                winners[key] = action
        return sorted(
            winners.values(),
            key=lambda action: (
                priority[action.action_type],
                float(action.confidence),
                float(action.candidate.importance),
                len(action.candidate.text),
            ),
            reverse=True,
        )

    def _apply_action(
        self,
        action: MemoryAction,
        *,
        context: MemoryScopeContext,
        metadata: dict[str, Any] | None,
        long_term: bool,
        source: str,
    ) -> dict[str, Any] | None:
        action_metadata = dict(metadata or {})
        action_metadata.update(context.as_metadata())
        action_metadata.update(action.candidate.metadata)
        if action.action_type == MemoryActionType.ADD:
            created = self.memory_service.remember(
                text=action.candidate.text,
                user_id=context.user_id,
                agent_id=context.agent_id,
                session_id=context.session_id,
                run_id=context.run_id,
                metadata=action_metadata,
                memory_type=action.candidate.memory_type,
                importance=action.candidate.importance,
                long_term=long_term,
                source=source,
            )
            return MemoryMutationResult(
                id=created["id"],
                memory=created["text"],
                event="ADD",
                reason=action.reason,
                confidence=action.confidence,
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.UPDATE and action.target_id:
            updated = self.memory_service.update(
                action.target_id,
                text=action.candidate.text,
                metadata=action_metadata,
                importance=action.candidate.importance,
            )
            return MemoryMutationResult(
                id=updated["id"],
                memory=updated["text"],
                event="UPDATE",
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.DELETE and action.target_id:
            self.memory_service.delete(action.target_id)
            return MemoryMutationResult(
                id=action.target_id,
                memory=action.previous_text or action.candidate.text,
                event="DELETE",
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.NONE:
            return MemoryMutationResult(
                id=action.target_id,
                memory=action.previous_text or action.candidate.text,
                event="NONE",
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                evidence=action.evidence,
            ).as_dict()
        return None
