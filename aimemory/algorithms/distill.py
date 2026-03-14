from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Sequence

from aimemory.algorithms.dedupe import fingerprint, semantic_similarity
from aimemory.algorithms.retrieval import mmr_rerank
from aimemory.core.text import hash_embedding, normalize_text, split_sentences, tokenize
from aimemory.memory_intelligence.models import NormalizedMessage
from aimemory.memory_intelligence.policies import MemoryPolicy


@dataclass(slots=True)
class DistilledCandidate:
    text: str
    score: float
    novelty: float
    informativeness: float
    density: float
    length_score: float
    fingerprint: str
    embedding: list[float]
    memory_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


class AdaptiveDistiller:
    def __init__(self, policy: MemoryPolicy):
        self.policy = policy

    def distill(
        self,
        messages: list[NormalizedMessage],
        *,
        background_texts: Sequence[str] | None = None,
        memory_type: str = "semantic",
    ) -> list[DistilledCandidate]:
        background = [str(item).strip() for item in (background_texts or []) if str(item).strip()]
        background = background[: self.policy.background_sample_limit]

        raw_candidates: list[tuple[str, str, dict[str, Any]]] = []
        seen_normalized: set[str] = set()
        for message in messages:
            if not message.content.strip():
                continue
            for sentence in split_sentences(message.content):
                cleaned = sentence.strip()
                if not cleaned:
                    continue
                if len(cleaned) < self.policy.min_candidate_chars or len(cleaned) > self.policy.max_candidate_chars:
                    continue
                normalized = normalize_text(cleaned)
                if normalized in seen_normalized:
                    continue
                seen_normalized.add(normalized)
                raw_candidates.append(
                    (
                        cleaned,
                        message.role,
                        {
                            "source_role": message.role,
                            "actor_id": message.actor_id,
                            **dict(message.metadata),
                        },
                    )
                )

        if not raw_candidates:
            return []

        doc_sets = [set(tokenize(text)) for text, _, _ in raw_candidates]
        background_sets = [set(tokenize(text)) for text in background]
        document_frequency: Counter[str] = Counter()
        for token_set in doc_sets + background_sets:
            document_frequency.update(token_set)
        corpus_size = max(1, len(doc_sets) + len(background_sets))
        background_for_similarity = background[: self.policy.background_similarity_limit]
        role_weights = {
            "user": 1.0,
            "assistant": 0.82,
            "tool": 0.25,
            "system": 0.15,
        }

        prepared: list[DistilledCandidate] = []
        for text, role, metadata in raw_candidates:
            tokens = tokenize(text)
            unique_tokens = set(tokens)
            if not unique_tokens:
                continue
            idf_values = [math.log(1.0 + ((corpus_size + 1.0) / (1.0 + document_frequency[token]))) for token in unique_tokens]
            informativeness = sum(idf_values) / max(1, len(idf_values))
            max_idf = math.log(1.0 + corpus_size)
            informativeness = min(1.0, informativeness / max(max_idf, 1e-6))
            density = len(unique_tokens) / max(1, len(tokens))
            length_score = min(1.0, len(tokens) / 18.0)
            novelty = 1.0
            if background_for_similarity:
                novelty = 1.0 - max(semantic_similarity(text, item) for item in background_for_similarity)
                novelty = max(0.0, novelty)
            role_weight = role_weights.get(role, 0.9)
            score = role_weight * (
                (self.policy.candidate_information_weight * informativeness)
                + (self.policy.candidate_novelty_weight * novelty)
                + (self.policy.candidate_density_weight * density)
                + (self.policy.candidate_length_weight * length_score)
            )
            prepared.append(
                DistilledCandidate(
                    text=text,
                    score=round(score, 6),
                    novelty=round(novelty, 6),
                    informativeness=round(informativeness, 6),
                    density=round(density, 6),
                    length_score=round(length_score, 6),
                    fingerprint=fingerprint(text),
                    embedding=hash_embedding(text),
                    memory_type=memory_type,
                    metadata=metadata,
                )
            )

        deduped: list[DistilledCandidate] = []
        for candidate in sorted(prepared, key=lambda item: item.score, reverse=True):
            if any(semantic_similarity(candidate.text, existing.text) >= self.policy.candidate_merge_threshold for existing in deduped):
                continue
            deduped.append(candidate)

        rerank_payload = [{"id": item.fingerprint, "text": item.text, "score": item.score} for item in deduped]
        reranked = mmr_rerank(
            rerank_payload,
            lambda_value=self.policy.diversity_lambda,
            limit=self.policy.max_candidates,
        )
        order = [item["id"] for item in reranked]
        by_id = {item.fingerprint: item for item in deduped}
        return [by_id[item_id] for item_id in order if item_id in by_id]
