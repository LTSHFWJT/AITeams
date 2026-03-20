from __future__ import annotations

import time
from typing import Any

from aimemory.algorithms.affinity import coverage_ratio, tokens_density
from aimemory.algorithms.dedupe import merge_text_fragments, semantic_similarity
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
from aimemory.memory_intelligence.semantic_categories import (
    append_relation,
    infer_context_label,
    normalize_context_label,
    update_support_info,
)


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
            neighbors = self._retrieve_neighbors(candidate.index_text() or candidate.storage_text() or candidate.text, context=context, long_term=long_term)
            planned_actions.extend(self.planner.plan(candidate, neighbors, context=context, policy=self.policy))

        results: list[dict[str, Any]] = []
        for action in self._arbitrate_actions(planned_actions):
            result = self._apply_action(action, context=context, metadata=metadata, long_term=long_term, source=source)
            if result:
                results.append(result)
        return {"results": results, "facts": [candidate.storage_text() or candidate.text for candidate in candidates]}

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
                confidence=float(item.get("confidence", metadata.get("confidence", 0.5)) or 0.5),
                semantic_category=str(metadata.get("semantic_category") or metadata.get("memory_category") or "").strip() or None,
                summary_l0=str(item.get("summary_l0") or metadata.get("summary_l0") or metadata.get("l0_abstract") or "").strip() or None,
                summary_l1=str(item.get("summary_l1") or metadata.get("summary_l1") or metadata.get("l1_overview") or "").strip() or None,
                tier=str(item.get("tier") or metadata.get("tier") or "").strip() or None,
                version=int(item.get("version", 0) or 0) or None,
                status=str(item.get("status") or metadata.get("status") or "active"),
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
        consolidated.sort(key=lambda item: (item.importance, item.confidence, len(item.storage_text() or item.text)), reverse=True)
        return consolidated[: self.policy.max_candidates]

    def _find_similar_candidate(self, candidate: FactCandidate, consolidated: list[FactCandidate]) -> int | None:
        for index, existing in enumerate(consolidated):
            if existing.memory_type != candidate.memory_type:
                continue
            if existing.normalized_category() != candidate.normalized_category():
                continue
            if existing.fact_key and candidate.fact_key and existing.fact_key == candidate.fact_key:
                return index
            if existing.topic_key and candidate.topic_key and existing.topic_key == candidate.topic_key:
                return index
            if self._candidate_similarity(existing, candidate) >= self.policy.candidate_merge_threshold:
                return index
        return None

    def _candidate_similarity(self, left: FactCandidate, right: FactCandidate) -> float:
        normalized_left = self._canonical_text(left.index_text() or left.storage_text() or left.text)
        normalized_right = self._canonical_text(right.index_text() or right.storage_text() or right.text)
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
        existing_contexts = existing.metadata.get("contexts")
        incoming_contexts = incoming.metadata.get("contexts")
        if isinstance(existing_contexts, list) or isinstance(incoming_contexts, list):
            merged_metadata["contexts"] = list(
                dict.fromkeys(
                    [
                        *([str(item) for item in existing_contexts] if isinstance(existing_contexts, list) else []),
                        *([str(item) for item in incoming_contexts] if isinstance(incoming_contexts, list) else []),
                    ]
                )
            )
        merged_content = merge_text_fragments(
            [existing.storage_text() or existing.text, incoming.storage_text() or incoming.text],
            max_sentences=8,
            max_chars=720,
        )
        merged_overview = merge_text_fragments(
            [existing.overview or existing.metadata.get("summary_l1") or "", incoming.overview or incoming.metadata.get("summary_l1") or ""],
            max_sentences=8,
            max_chars=420,
        )
        return FactCandidate(
            text=chosen.text,
            memory_type=chosen.memory_type,
            confidence=max(existing.confidence, incoming.confidence),
            importance=max(existing.importance, incoming.importance),
            semantic_category=chosen.semantic_category or existing.semantic_category or incoming.semantic_category,
            abstract=chosen.abstract or chosen.text,
            overview=merged_overview or chosen.overview,
            content=merged_content or chosen.content or chosen.text,
            fact_key=chosen.fact_key or existing.fact_key or incoming.fact_key,
            topic_key=chosen.topic_key or existing.topic_key or incoming.topic_key,
            context_label=chosen.context_label or existing.context_label or incoming.context_label,
            source=chosen.source,
            metadata=merged_metadata,
        )

    def _prefer_incoming(self, existing: FactCandidate, incoming: FactCandidate) -> bool:
        existing_score = (
            (0.4 * float(existing.confidence))
            + (0.35 * float(existing.importance))
            + (0.15 * tokens_density(existing.storage_text() or existing.text))
            + (0.1 * coverage_ratio(existing.storage_text() or existing.text, incoming.storage_text() or incoming.text))
        )
        incoming_score = (
            (0.4 * float(incoming.confidence))
            + (0.35 * float(incoming.importance))
            + (0.15 * tokens_density(incoming.storage_text() or incoming.text))
            + (0.1 * coverage_ratio(incoming.storage_text() or incoming.text, existing.storage_text() or existing.text))
        )
        if incoming.memory_type != existing.memory_type and incoming.memory_type != "semantic":
            incoming_score += 0.06
        return incoming_score >= existing_score

    def _arbitrate_actions(self, actions: list[MemoryAction]) -> list[MemoryAction]:
        if not actions:
            return []
        priority = {
            MemoryActionType.DELETE: 9,
            MemoryActionType.SUPERSEDE: 8,
            MemoryActionType.CONTRADICT: 7,
            MemoryActionType.CONTEXTUALIZE: 6,
            MemoryActionType.UPDATE: 5,
            MemoryActionType.MERGE: 4,
            MemoryActionType.SUPPORT: 3,
            MemoryActionType.LINK: 2,
            MemoryActionType.NONE: 1,
            MemoryActionType.ADD: 0,
        }
        winners: dict[str, MemoryAction] = {}
        for action in actions:
            key = action.target_id or f"candidate:{normalize_text(action.candidate.index_text() or action.candidate.text)}"
            current = winners.get(key)
            if current is None:
                winners[key] = action
                continue
            current_rank = (
                priority[current.action_type],
                float(current.confidence),
                float(current.candidate.importance),
                len(current.candidate.storage_text() or current.candidate.text),
            )
            candidate_rank = (
                priority[action.action_type],
                float(action.confidence),
                float(action.candidate.importance),
                len(action.candidate.storage_text() or action.candidate.text),
            )
            if candidate_rank > current_rank:
                winners[key] = action
        return sorted(
            winners.values(),
            key=lambda action: (
                priority[action.action_type],
                float(action.confidence),
                float(action.candidate.importance),
                len(action.candidate.storage_text() or action.candidate.text),
            ),
            reverse=True,
        )

    def _candidate_storage_text(self, candidate: FactCandidate) -> str:
        return candidate.storage_text() or candidate.text

    def _candidate_summary_l0(self, candidate: FactCandidate) -> str:
        return str(candidate.abstract or candidate.metadata.get("summary_l0") or candidate.metadata.get("l0_abstract") or candidate.text).strip()

    def _candidate_summary_l1(self, candidate: FactCandidate) -> str:
        return str(candidate.overview or candidate.metadata.get("summary_l1") or candidate.metadata.get("l1_overview") or self._candidate_summary_l0(candidate)).strip()

    def _candidate_metadata(self, action: MemoryAction, base_metadata: dict[str, Any]) -> dict[str, Any]:
        payload = dict(base_metadata)
        payload.update(action.candidate.metadata)
        category = action.candidate.normalized_category()
        if category is not None:
            payload["semantic_category"] = category
            payload["memory_category"] = category
        summary_l0 = self._candidate_summary_l0(action.candidate)
        summary_l1 = self._candidate_summary_l1(action.candidate)
        storage_text = self._candidate_storage_text(action.candidate)
        payload["summary_l0"] = summary_l0
        payload["summary_l1"] = summary_l1
        payload["l0_abstract"] = summary_l0
        payload["l1_overview"] = summary_l1
        payload["l2_content"] = storage_text
        payload["confidence"] = float(action.candidate.confidence)
        payload["tier"] = str(payload.get("tier") or "working")
        if action.candidate.fact_key:
            payload["fact_key"] = action.candidate.fact_key
        if action.candidate.topic_key:
            payload["topic_key"] = action.candidate.topic_key
        context_label = normalize_context_label(action.context_label or action.candidate.context_label or infer_context_label(storage_text))
        if context_label != "general":
            contexts = [str(item) for item in payload.get("contexts", [])] if isinstance(payload.get("contexts"), list) else []
            payload["contexts"] = list(dict.fromkeys([*contexts, context_label]))
        return payload

    def _link_created_memory(self, created_id: str, related_ids: list[str], *, action: MemoryAction) -> None:
        if not related_ids:
            return
        self.memory_service.link(
            created_id,
            related_ids,
            link_type=action.link_type or "related",
            weight=max(0.5, action.confidence),
            confidence=max(0.5, action.confidence),
            metadata={"evidence": action.evidence, "reason": action.reason, "context_label": action.context_label},
            reason_code=action.reason,
        )

    def _merged_text(self, action: MemoryAction) -> str:
        return merge_text_fragments(
            [action.previous_text or "", self._candidate_storage_text(action.candidate)],
            max_sentences=8,
            max_chars=720,
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
        action_metadata = self._candidate_metadata(action, action_metadata)
        storage_text = self._candidate_storage_text(action.candidate)
        summary_l0 = self._candidate_summary_l0(action.candidate)
        summary_l1 = self._candidate_summary_l1(action.candidate)
        related_ids = list(dict.fromkeys([*action.link_target_ids, *([action.target_id] if action.target_id else [])]))
        if action.action_type == MemoryActionType.ADD:
            created = self.memory_service.remember(
                text=storage_text,
                user_id=context.user_id,
                agent_id=context.agent_id,
                session_id=context.session_id,
                run_id=context.run_id,
                metadata=action_metadata,
                memory_type=action.candidate.memory_type,
                importance=action.candidate.importance,
                confidence=action.candidate.confidence,
                tier=str(action_metadata.get("tier") or "working"),
                summary_l0=summary_l0,
                summary_l1=summary_l1,
                long_term=long_term,
                source=source,
                skip_existing_lookup=True,
                event_type="ADD",
                reason_code=action.reason,
            )
            self._link_created_memory(created["id"], related_ids, action=action)
            return MemoryMutationResult(
                id=created["id"],
                memory=created["text"],
                event="ADD",
                reason=action.reason,
                confidence=action.confidence,
                related_ids=related_ids,
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.UPDATE and action.target_id:
            updated = self.memory_service.update(
                action.target_id,
                text=storage_text,
                metadata=action_metadata,
                importance=action.candidate.importance,
                confidence=action.candidate.confidence,
                tier=str(action_metadata.get("tier") or "working"),
                summary_l0=summary_l0,
                summary_l1=summary_l1,
                mode="update",
                event_type="UPDATE",
                reason_code=action.reason,
                audit_payload={"previous_text": action.previous_text, "evidence": action.evidence},
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

        if action.action_type == MemoryActionType.MERGE and action.target_id:
            merged_text = self._merged_text(action)
            updated = self.memory_service.update(
                action.target_id,
                text=merged_text,
                metadata=action_metadata,
                importance=action.candidate.importance,
                confidence=max(action.candidate.confidence, float(action_metadata.get("confidence", 0.5) or 0.5)),
                tier=str(action_metadata.get("tier") or "working"),
                summary_l0=summary_l0,
                summary_l1=summary_l1,
                mode="merge",
                event_type="MERGE",
                reason_code=action.reason,
                audit_payload={"previous_text": action.previous_text, "evidence": action.evidence},
            )
            return MemoryMutationResult(
                id=updated["id"],
                memory=updated["text"],
                event="MERGE",
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                version=int(updated.get("version", 1) or 1),
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.SUPERSEDE and action.target_id:
            created = self.memory_service.supersede(
                action.target_id,
                text=storage_text,
                metadata=action_metadata,
                importance=action.candidate.importance,
                confidence=action.candidate.confidence,
                tier=str(action_metadata.get("tier") or "working"),
                summary_l0=summary_l0,
                summary_l1=summary_l1,
                reason_code=action.reason,
                audit_payload={"previous_text": action.previous_text, "evidence": action.evidence},
            )
            return MemoryMutationResult(
                id=created["id"],
                memory=created["text"],
                event="SUPERSEDE",
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                version=int(created.get("version", 1) or 1),
                supersedes_memory_id=created.get("supersedes_memory_id"),
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.SUPPORT and action.target_id:
            current = self.memory_service.get(action.target_id)
            current_metadata = dict((current or {}).get("metadata") or {})
            observed_at = int(time.time() * 1000)
            support_info = update_support_info(
                current_metadata.get("support_info"),
                action.context_label or infer_context_label(storage_text),
                "support",
                observed_at=observed_at,
            )
            action_metadata["support_info"] = support_info
            updated = self.memory_service.update(
                action.target_id,
                metadata=action_metadata,
                importance=max(float((current or {}).get("importance", 0.5) or 0.5), action.candidate.importance),
                confidence=max(float((current or {}).get("confidence", 0.5) or 0.5), action.candidate.confidence),
                tier=str(action_metadata.get("tier") or (current or {}).get("tier") or "working"),
                mode="update",
                event_type="SUPPORT",
                reason_code=action.reason,
                audit_payload={"previous_text": action.previous_text, "context_label": action.context_label, "evidence": action.evidence},
            )
            return MemoryMutationResult(
                id=updated["id"],
                memory=updated["text"],
                event="SUPPORT",
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                evidence={**action.evidence, "support_info": support_info},
            ).as_dict()

        if action.action_type in {MemoryActionType.CONTEXTUALIZE, MemoryActionType.CONTRADICT} and action.target_id:
            relation_type = "contextualizes" if action.action_type == MemoryActionType.CONTEXTUALIZE else "contradicts"
            if action.action_type == MemoryActionType.CONTRADICT:
                current = self.memory_service.get(action.target_id)
                current_metadata = dict((current or {}).get("metadata") or {})
                observed_at = int(time.time() * 1000)
                contradiction_info = update_support_info(
                    current_metadata.get("support_info"),
                    action.context_label or infer_context_label(storage_text),
                    "contradict",
                    observed_at=observed_at,
                )
                current_metadata["support_info"] = contradiction_info
                self.memory_service.update(
                    action.target_id,
                    metadata=current_metadata,
                    importance=float((current or {}).get("importance", 0.5) or 0.5),
                    confidence=float((current or {}).get("confidence", 0.5) or 0.5),
                    tier=str((current or {}).get("tier") or current_metadata.get("tier") or "working"),
                    mode="update",
                    event_type="UPDATE",
                    reason_code=f"{action.reason}-observed",
                    audit_payload={"context_label": action.context_label, "evidence": action.evidence, "event": "contradiction_observed"},
                )
                action_metadata["support_info_target"] = contradiction_info
            action_metadata["relations"] = append_relation(action_metadata.get("relations"), relation_type, action.target_id)
            created = self.memory_service.remember(
                text=storage_text,
                user_id=context.user_id,
                agent_id=context.agent_id,
                session_id=context.session_id,
                run_id=context.run_id,
                metadata=action_metadata,
                memory_type=action.candidate.memory_type,
                importance=action.candidate.importance,
                confidence=action.candidate.confidence,
                tier=str(action_metadata.get("tier") or "working"),
                summary_l0=summary_l0,
                summary_l1=summary_l1,
                long_term=long_term,
                source=source,
                skip_existing_lookup=True,
                event_type=action.action_type.value,
                event_payload={"evidence": action.evidence, "target_memory_id": action.target_id, "context_label": action.context_label},
                reason_code=action.reason,
            )
            self._link_created_memory(created["id"], [action.target_id], action=MemoryAction(
                action.action_type,
                candidate=action.candidate,
                reason=action.reason,
                target_id=action.target_id,
                previous_text=action.previous_text,
                confidence=action.confidence,
                link_target_ids=[action.target_id],
                link_type=relation_type,
                context_label=action.context_label,
                evidence=action.evidence,
            ))
            return MemoryMutationResult(
                id=created["id"],
                memory=created["text"],
                event=action.action_type.value,
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                related_ids=[action.target_id],
                version=int(created.get("version", 1) or 1),
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.DELETE and action.target_id:
            self.memory_service.delete(action.target_id)
            return MemoryMutationResult(
                id=action.target_id,
                memory=action.previous_text or storage_text,
                event="DELETE",
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.LINK:
            created = self.memory_service.remember(
                text=storage_text,
                user_id=context.user_id,
                agent_id=context.agent_id,
                session_id=context.session_id,
                run_id=context.run_id,
                metadata=action_metadata,
                memory_type=action.candidate.memory_type,
                importance=action.candidate.importance,
                confidence=action.candidate.confidence,
                tier=str(action_metadata.get("tier") or "working"),
                summary_l0=summary_l0,
                summary_l1=summary_l1,
                long_term=long_term,
                source=source,
                skip_existing_lookup=True,
                event_type="LINK",
                event_payload={"evidence": action.evidence, "link_target_ids": list(action.link_target_ids)},
                reason_code=action.reason,
            )
            self._link_created_memory(created["id"], related_ids, action=action)
            return MemoryMutationResult(
                id=created["id"],
                memory=created["text"],
                event="LINK",
                reason=action.reason,
                confidence=action.confidence,
                related_ids=related_ids,
                version=int(created.get("version", 1) or 1),
                evidence=action.evidence,
            ).as_dict()

        if action.action_type == MemoryActionType.NONE:
            return MemoryMutationResult(
                id=action.target_id,
                memory=action.previous_text or storage_text,
                event="NONE",
                reason=action.reason,
                confidence=action.confidence,
                previous_memory=action.previous_text,
                evidence=action.evidence,
            ).as_dict()
        return None
