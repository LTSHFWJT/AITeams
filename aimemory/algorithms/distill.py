from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Sequence

from aimemory.algorithms.dedupe import fingerprint, semantic_similarity
from aimemory.algorithms.retrieval import mmr_rerank, score_record
from aimemory.algorithms.segmentation import TextUnit
from aimemory.core.text import hash_embedding, normalize_text, split_sentences, tokenize
from aimemory.memory_intelligence.models import NormalizedMessage
from aimemory.memory_intelligence.policies import MemoryPolicy


PATHLIKE_RE = re.compile(r"(?:/[\w./-]+)|(?:\b[\w.-]+\.[A-Za-z0-9]{1,8}\b)")
CODELIKE_RE = re.compile(r"(?:->|=>|==|!=|<=|>=|::|\(\)|\[\]|{})")
VERSIONLIKE_RE = re.compile(r"\bv?\d+(?:\.\d+){1,4}\b", re.IGNORECASE)


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


@dataclass(slots=True)
class DistilledUnitCandidate:
    id: str
    text: str
    level: str
    title_path: list[str]
    score: float
    structure_score: float
    information_score: float
    density_score: float
    query_score: float
    actionability_score: float
    constraint_score: float
    fingerprint: str
    embedding: list[float]
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

    def distill_units(
        self,
        units: list[TextUnit],
        *,
        query: str | None = None,
        domain_hint: str | None = None,
        limit: int | None = None,
    ) -> list[DistilledUnitCandidate]:
        if not units:
            return []

        prepared_units = [unit for unit in units if str(unit.text or "").strip()]
        if not prepared_units:
            return []

        token_sets = [set(tokenize(unit.text)) for unit in prepared_units]
        document_frequency: Counter[str] = Counter()
        for token_set in token_sets:
            document_frequency.update(token_set)
        corpus_size = max(1, len(token_sets))
        max_idf = math.log(1.0 + corpus_size)
        query_text = str(query or "").strip()
        query_present = bool(query_text)
        profile = self._unit_score_profile(domain_hint)

        prepared: list[DistilledUnitCandidate] = []
        for unit, token_set in zip(prepared_units, token_sets):
            text = str(unit.text or "").strip()
            if not text:
                continue
            if unit.level == "paragraph" and len(text) > max(self.policy.max_candidate_chars * 4, 960):
                continue

            structure_score = self._unit_structure_score(unit)
            density_score = self._unit_density_score(text, token_set)
            information_score = self._unit_information_score(token_set, document_frequency, corpus_size, max_idf)
            actionability_score = self._unit_actionability_score(unit, text)
            constraint_score = self._unit_constraint_score(unit, text)
            query_score = 0.0
            if query_present:
                query_score, _breakdown = score_record(
                    query_text,
                    text=text,
                    keywords=list(token_set),
                    importance=0.5,
                    lexical_score=0.0,
                    half_life_days=3650.0,
                )
            score = (
                (profile["structure"] * structure_score)
                + (profile["information"] * information_score)
                + (profile["density"] * density_score)
                + (profile["query"] * query_score)
                + (profile["actionability"] * actionability_score)
                + (profile["constraint"] * constraint_score)
            )
            prepared.append(
                DistilledUnitCandidate(
                    id=unit.id,
                    text=text,
                    level=unit.level,
                    title_path=list(unit.title_path),
                    score=round(min(1.0, score), 6),
                    structure_score=round(structure_score, 6),
                    information_score=round(information_score, 6),
                    density_score=round(density_score, 6),
                    query_score=round(query_score, 6),
                    actionability_score=round(actionability_score, 6),
                    constraint_score=round(constraint_score, 6),
                    fingerprint=fingerprint(text),
                    embedding=hash_embedding(text),
                    metadata={
                        "section_index": unit.section_index,
                        "paragraph_index": unit.paragraph_index,
                        "sentence_index": unit.sentence_index,
                        "start_offset": unit.start_offset,
                        "end_offset": unit.end_offset,
                        **dict(unit.metadata),
                    },
                )
            )

        deduped: list[DistilledUnitCandidate] = []
        for candidate in sorted(prepared, key=lambda item: item.score, reverse=True):
            if any(semantic_similarity(candidate.text, existing.text) >= self.policy.candidate_merge_threshold for existing in deduped):
                continue
            deduped.append(candidate)

        reranked = mmr_rerank(
            [{"id": item.id, "text": item.text, "score": item.score} for item in deduped],
            lambda_value=self.policy.diversity_lambda,
            limit=limit or max(self.policy.max_candidates * 2, 12),
        )
        order = [item["id"] for item in reranked]
        by_id = {item.id: item for item in deduped}
        return [by_id[item_id] for item_id in order if item_id in by_id]

    def _unit_score_profile(self, domain_hint: str | None) -> dict[str, float]:
        domain = str(domain_hint or "").strip().lower()
        base = {
            "structure": 0.19,
            "information": 0.26,
            "density": 0.14,
            "query": 0.19,
            "actionability": 0.11,
            "constraint": 0.11,
        }
        if domain in {"skill", "skill_reference", "execution"}:
            return {
                "structure": 0.16,
                "information": 0.18,
                "density": 0.12,
                "query": 0.2,
                "actionability": 0.19,
                "constraint": 0.15,
            }
        if domain in {"knowledge", "archive"}:
            return {
                "structure": 0.18,
                "information": 0.28,
                "density": 0.15,
                "query": 0.2,
                "actionability": 0.08,
                "constraint": 0.11,
            }
        return base

    def _unit_structure_score(self, unit: TextUnit) -> float:
        title_bonus = min(0.12, len(unit.title_path) * 0.03)
        position_bonus = 0.08 if unit.sentence_index in {0, None} else max(0.0, 0.06 - (unit.sentence_index * 0.02))
        level_base = {
            "heading": 0.78,
            "list_item": 0.72,
            "paragraph": 0.64,
            "sentence": 0.56,
            "table_block": 0.68,
            "code_block": 0.7,
        }.get(unit.level, 0.48)
        return min(1.0, level_base + title_bonus + position_bonus)

    def _unit_information_score(self, token_set: set[str], document_frequency: Counter[str], corpus_size: int, max_idf: float) -> float:
        if not token_set:
            return 0.0
        idf_values = [
            math.log(1.0 + ((corpus_size + 1.0) / (1.0 + document_frequency[token])))
            for token in token_set
        ]
        average_idf = sum(idf_values) / max(1, len(idf_values))
        normalized = average_idf / max(max_idf, 1e-6)
        return max(0.0, min(1.0, normalized))

    def _unit_density_score(self, text: str, token_set: set[str]) -> float:
        tokens = tokenize(text)
        if not tokens:
            return 0.0
        unique_ratio = len(token_set) / max(1, len(tokens))
        symbol_bonus = 0.04 if len(text) <= 220 else 0.0
        return min(1.0, unique_ratio + symbol_bonus)

    def _unit_actionability_score(self, unit: TextUnit, text: str) -> float:
        code_like = bool(CODELIKE_RE.search(text) or PATHLIKE_RE.search(text))
        punctuation_density = sum(text.count(symbol) for symbol in (":", "-", ">", "/", "=", "(", ")"))
        score = 0.18 if unit.level == "sentence" else 0.28
        if unit.level == "list_item":
            score += 0.26
        if unit.level == "code_block":
            score += 0.34
        if code_like:
            score += 0.18
        if punctuation_density >= 3:
            score += 0.08
        return min(1.0, score)

    def _unit_constraint_score(self, unit: TextUnit, text: str) -> float:
        digit_count = sum(char.isdigit() for char in text)
        score = min(0.42, digit_count * 0.03)
        if "%" in text:
            score += 0.1
        if VERSIONLIKE_RE.search(text):
            score += 0.12
        if any(symbol in text for symbol in ("<", ">", "=", "!")):
            score += 0.08
        if unit.level in {"table_block", "code_block"}:
            score += 0.08
        return min(1.0, score)
