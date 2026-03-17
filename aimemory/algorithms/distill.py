from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Sequence

from aimemory.algorithms.dedupe import fingerprint, semantic_similarity
from aimemory.algorithms.retrieval import mmr_rerank
from aimemory.algorithms.segmentation import TextUnit
from aimemory.core.text import cosine_similarity, hash_embedding, normalize_text, split_sentences, tokenize
from aimemory.memory_intelligence.models import NormalizedMessage
from aimemory.memory_intelligence.policies import MemoryPolicy


PATHLIKE_RE = re.compile(r"(?:/[\w./-]+)|(?:\b[\w.-]+\.[A-Za-z0-9]{1,8}\b)")
CODELIKE_RE = re.compile(r"(?:->|=>|==|!=|<=|>=|::|\(\)|\[\]|{})")
VERSIONLIKE_RE = re.compile(r"\bv?\d+(?:\.\d+){1,4}\b", re.IGNORECASE)
CLAUSE_SPLIT_RE = re.compile(r"[，,;；:：]+")

PROCEDURE_PROTOTYPE = (
    "procedure workflow ordered step execution command checklist operating sequence "
    "步骤 流程 顺序 执行 命令 检查 验证 操作 发布"
)
CONSTRAINT_PROTOTYPE = (
    "constraint requirement limit threshold budget boundary quota tolerance requirement "
    "约束 限制 阈值 上限 下限 不得 不能 超过 窗口 配额"
)
RISK_PROTOTYPE = (
    "risk recovery rollback failure caution incident warning exception fallback "
    "风险 回滚 恢复 失败 异常 故障 注意 告警"
)


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
    title_affinity_score: float
    actionability_score: float
    constraint_score: float
    risk_score: float
    centrality_score: float
    coverage_score: float
    fingerprint: str
    embedding: list[float]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class UnitLocalSignals:
    local_cohesion: float = 0.0
    local_contrast: float = 0.0
    section_cohesion: float = 0.0
    section_contrast: float = 0.0
    numeric_signal: float = 0.0
    numeric_contrast: float = 0.0
    operator_signal: float = 0.0
    operator_contrast: float = 0.0
    sequence_signal: float = 0.0
    boundary_signal: float = 0.0
    terminal_signal: float = 0.0
    block_signal: float = 0.0
    compactness: float = 0.0
    clause_complexity: float = 0.0


@lru_cache(maxsize=16)
def _prototype_embedding(label: str) -> list[float]:
    payload = {
        "procedure": PROCEDURE_PROTOTYPE,
        "constraint": CONSTRAINT_PROTOTYPE,
        "risk": RISK_PROTOTYPE,
    }.get(label, label)
    return hash_embedding(payload)


def _average_embedding(vectors: Sequence[list[float]]) -> list[float]:
    if not vectors:
        return []
    dims = len(vectors[0])
    if dims <= 0:
        return []
    totals = [0.0] * dims
    for vector in vectors:
        if len(vector) != dims:
            continue
        for index, value in enumerate(vector):
            totals[index] += value
    size = max(1, len(vectors))
    averaged = [value / size for value in totals]
    norm = math.sqrt(sum(value * value for value in averaged))
    if norm <= 0:
        return averaged
    return [value / norm for value in averaged]


def _embedding_affinity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return max(0.0, cosine_similarity(left, right))


def _topk_mean(values: Sequence[float], *, k: int) -> float:
    if not values:
        return 0.0
    ordered = sorted((value for value in values if value > 0.0), reverse=True)
    if not ordered:
        return 0.0
    head = ordered[: max(1, k)]
    return sum(head) / max(1, len(head))


def _graph_centrality_scores(embeddings: Sequence[list[float]], *, threshold: float = 0.16, iterations: int = 12) -> list[float]:
    size = len(embeddings)
    if size <= 0:
        return []
    if size == 1:
        return [1.0]

    adjacency: list[list[tuple[int, float]]] = [[] for _ in range(size)]
    for left in range(size):
        for right in range(left + 1, size):
            similarity = _embedding_affinity(embeddings[left], embeddings[right])
            if similarity < threshold:
                continue
            adjacency[left].append((right, similarity))
            adjacency[right].append((left, similarity))

    scores = [1.0 / size] * size
    damping = 0.85
    for _ in range(iterations):
        updated = [(1.0 - damping) / size] * size
        for index, neighbors in enumerate(adjacency):
            if not neighbors:
                spill = damping * scores[index] / size
                for target in range(size):
                    updated[target] += spill
                continue
            normalizer = sum(weight for _target, weight in neighbors) or 1.0
            for target, weight in neighbors:
                updated[target] += damping * scores[index] * (weight / normalizer)
        scores = updated

    maximum = max(scores) or 1.0
    return [round(value / maximum, 6) for value in scores]


def _normalized_information_score(
    token_set: set[str],
    document_frequency: Counter[str],
    corpus_size: int,
    max_idf: float,
) -> float:
    if not token_set or max_idf <= 0:
        return 0.0
    idf_values = [
        math.log(1.0 + ((corpus_size + 1.0) / (1.0 + document_frequency[token])))
        for token in token_set
    ]
    average_idf = sum(idf_values) / max(1, len(idf_values))
    return max(0.0, min(1.0, average_idf / max(max_idf, 1e-6)))


def _numeric_signal(text: str) -> float:
    raw = str(text or "")
    if not raw:
        return 0.0
    digits = sum(char.isdigit() for char in raw)
    operators = sum(raw.count(symbol) for symbol in ("<", ">", "=", "%", ":", "/", "-", "+"))
    version_bonus = 0.16 if VERSIONLIKE_RE.search(raw) else 0.0
    return min(1.0, (digits / max(1, len(raw))) * 8.0 + min(0.42, operators * 0.05) + version_bonus)


def _operator_density(text: str) -> float:
    raw = str(text or "")
    if not raw:
        return 0.0
    operators = sum(raw.count(symbol) for symbol in (":", "-", ">", "/", "=", "(", ")", "[", "]"))
    return min(1.0, operators / 10.0)


def _prototype_affinity(text: str, label: str) -> float:
    normalized = normalize_text(text)
    if not normalized:
        return 0.0
    return round(_embedding_affinity(hash_embedding(normalized), _prototype_embedding(label)), 6)


def _mean(values: Sequence[float]) -> float:
    usable = [float(value) for value in values]
    if not usable:
        return 0.0
    return sum(usable) / max(1, len(usable))


def _compactness_score(text: str, *, level: str) -> float:
    cleaned = str(text or "").strip()
    if not cleaned:
        return 0.0
    target = 84 if level in {"sentence", "list_item"} else 132
    tolerance = max(48, int(target * 1.15))
    distance = abs(len(cleaned) - target)
    return max(0.0, 1.0 - min(1.0, distance / tolerance))


def _clause_complexity(text: str) -> float:
    raw = str(text or "").strip()
    if not raw:
        return 0.0
    clauses = [part.strip() for part in CLAUSE_SPLIT_RE.split(raw) if part.strip()]
    clause_bonus = min(0.54, max(0, len(clauses) - 1) * 0.18)
    punctuation_bonus = min(0.32, sum(raw.count(symbol) for symbol in ("(", ")", "[", "]", "/", "-", ">")) * 0.04)
    newline_bonus = 0.12 if "\n" in raw else 0.0
    return min(1.0, clause_bonus + punctuation_bonus + newline_bonus)


def _title_text(unit: TextUnit) -> str:
    return " / ".join(str(item).strip() for item in unit.title_path if str(item).strip())


def _unit_section_key(unit: TextUnit) -> tuple[str, int, tuple[str, ...]]:
    return (
        str(unit.metadata.get("source_id") or ""),
        int(unit.section_index),
        tuple(str(item).strip() for item in unit.title_path if str(item).strip()),
    )


def _same_section(left: TextUnit | None, right: TextUnit | None) -> bool:
    if left is None or right is None:
        return False
    return _unit_section_key(left) == _unit_section_key(right)


def _same_span(left: TextUnit | None, right: TextUnit | None) -> bool:
    if left is None or right is None:
        return False
    return (
        str(left.metadata.get("source_id") or "") == str(right.metadata.get("source_id") or "")
        and int(left.paragraph_index) == int(right.paragraph_index)
        and int(left.start_offset) == int(right.start_offset)
        and int(left.end_offset) == int(right.end_offset)
    )


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

        doc_sets = [set(tokenize(text)) for text, _role, _metadata in raw_candidates]
        background_sets = [set(tokenize(text)) for text in background]
        document_frequency: Counter[str] = Counter()
        for token_set in doc_sets + background_sets:
            document_frequency.update(token_set)
        corpus_size = max(1, len(doc_sets) + len(background_sets))
        max_idf = math.log(1.0 + corpus_size)

        embeddings = [hash_embedding(text) for text, _role, _metadata in raw_candidates]
        background_embeddings = [hash_embedding(text) for text in background[: self.policy.background_similarity_limit]]
        centrality_scores = _graph_centrality_scores(embeddings)

        role_weights = {
            "user": 1.0,
            "assistant": 0.82,
            "tool": 0.25,
            "system": 0.15,
        }

        prepared: list[DistilledCandidate] = []
        for index, (text, role, metadata) in enumerate(raw_candidates):
            tokens = tokenize(text)
            unique_tokens = set(tokens)
            if not unique_tokens:
                continue
            informativeness = _normalized_information_score(unique_tokens, document_frequency, corpus_size, max_idf)
            density = len(unique_tokens) / max(1, len(tokens))
            length_score = min(1.0, len(tokens) / 18.0)
            novelty = 1.0
            if background_embeddings:
                novelty = 1.0 - max(_embedding_affinity(embeddings[index], item) for item in background_embeddings)
                novelty = max(0.0, novelty)
            role_weight = role_weights.get(role, 0.9)
            score = role_weight * (
                (0.28 * informativeness)
                + (0.22 * centrality_scores[index])
                + (0.2 * novelty)
                + (0.18 * density)
                + (0.12 * length_score)
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
                    embedding=embeddings[index],
                    memory_type=memory_type,
                    metadata=metadata,
                )
            )

        deduped: list[DistilledCandidate] = []
        for candidate in sorted(prepared, key=lambda item: item.score, reverse=True):
            if any(semantic_similarity(candidate.text, existing.text) >= self.policy.candidate_merge_threshold for existing in deduped):
                continue
            deduped.append(candidate)

        reranked = mmr_rerank(
            [{"id": item.fingerprint, "text": item.text, "score": item.score} for item in deduped],
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
        prepared_units = [unit for unit in units if str(unit.text or "").strip()]
        if not prepared_units:
            return []

        token_sets = [set(tokenize(unit.text)) for unit in prepared_units]
        document_frequency: Counter[str] = Counter()
        for token_set in token_sets:
            document_frequency.update(token_set)
        corpus_size = max(1, len(token_sets))
        max_idf = math.log(1.0 + corpus_size)

        embeddings = [hash_embedding(unit.text) for unit in prepared_units]
        title_texts = [_title_text(unit) for unit in prepared_units]
        title_embeddings = [hash_embedding(text) if text else [] for text in title_texts]
        centrality_scores = _graph_centrality_scores(embeddings)
        global_centroid = _average_embedding(embeddings)
        local_signals = self._unit_local_signals(prepared_units, embeddings)

        query_text = str(query or "").strip()
        query_embedding = hash_embedding(query_text) if query_text else []
        profile = self._unit_score_profile(domain_hint)

        prepared: list[DistilledUnitCandidate] = []
        for index, (unit, token_set) in enumerate(zip(prepared_units, token_sets)):
            text = str(unit.text or "").strip()
            if not text:
                continue
            if unit.level == "paragraph" and len(text) <= 16 and text.endswith((":", "：")):
                continue
            if unit.level == "paragraph" and len(split_sentences(text)) > 1:
                continue
            if unit.level == "paragraph" and len(text) > max(self.policy.max_candidate_chars * 4, 960):
                continue

            structure_score = self._unit_structure_score(unit)
            information_score = _normalized_information_score(token_set, document_frequency, corpus_size, max_idf)
            density_score = self._unit_density_score(text, token_set)
            centrality_score = centrality_scores[index]
            coverage_score = round(_embedding_affinity(embeddings[index], global_centroid), 6)
            query_score = self._query_score(
                unit_embedding=embeddings[index],
                query_embedding=query_embedding,
                title_embedding=title_embeddings[index],
            )
            title_affinity_score = self._title_affinity_score(
                unit=unit,
                unit_embedding=embeddings[index],
                title_embedding=title_embeddings[index],
                query_embedding=query_embedding,
            )
            actionability_score = self._unit_actionability_score(
                unit,
                text,
                title_texts[index],
                local_signals[index],
                centrality_score,
            )
            constraint_score = self._unit_constraint_score(
                unit,
                text,
                title_texts[index],
                local_signals[index],
                centrality_score,
            )
            risk_score = self._unit_risk_score(
                unit,
                text,
                title_texts[index],
                local_signals[index],
                centrality_score,
            )

            score = (
                (profile["structure"] * structure_score)
                + (profile["information"] * information_score)
                + (profile["density"] * density_score)
                + (profile["centrality"] * centrality_score)
                + (profile["coverage"] * coverage_score)
                + (profile["query"] * query_score)
                + (profile["title_affinity"] * title_affinity_score)
                + (profile["actionability"] * actionability_score)
                + (profile["constraint"] * constraint_score)
                + (profile["risk"] * risk_score)
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
                    title_affinity_score=round(title_affinity_score, 6),
                    actionability_score=round(actionability_score, 6),
                    constraint_score=round(constraint_score, 6),
                    risk_score=round(risk_score, 6),
                    centrality_score=round(centrality_score, 6),
                    coverage_score=round(coverage_score, 6),
                    fingerprint=fingerprint(text),
                    embedding=embeddings[index],
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
            limit=limit or max(self.policy.max_candidates * 3, 18),
        )
        order = [item["id"] for item in reranked]
        by_id = {item.id: item for item in deduped}
        return [by_id[item_id] for item_id in order if item_id in by_id]

    def _unit_score_profile(self, domain_hint: str | None) -> dict[str, float]:
        domain = str(domain_hint or "").strip().lower()
        base = {
            "structure": 0.12,
            "information": 0.17,
            "density": 0.08,
            "centrality": 0.23,
            "coverage": 0.11,
            "query": 0.14,
            "title_affinity": 0.07,
            "actionability": 0.05,
            "constraint": 0.02,
            "risk": 0.01,
        }
        if domain in {"skill", "skill_reference", "execution"}:
            return {
                "structure": 0.12,
                "information": 0.14,
                "density": 0.08,
                "centrality": 0.19,
                "coverage": 0.09,
                "query": 0.13,
                "title_affinity": 0.08,
                "actionability": 0.09,
                "constraint": 0.05,
                "risk": 0.03,
            }
        if domain in {"knowledge", "archive"}:
            return {
                "structure": 0.13,
                "information": 0.18,
                "density": 0.09,
                "centrality": 0.24,
                "coverage": 0.12,
                "query": 0.14,
                "title_affinity": 0.07,
                "actionability": 0.02,
                "constraint": 0.008,
                "risk": 0.002,
            }
        return base

    def _query_score(
        self,
        *,
        unit_embedding: list[float],
        query_embedding: list[float],
        title_embedding: list[float],
    ) -> float:
        if not query_embedding:
            return 0.0
        dense = _embedding_affinity(unit_embedding, query_embedding)
        title = _embedding_affinity(title_embedding, query_embedding) if title_embedding else 0.0
        return min(1.0, (0.76 * dense) + (0.24 * title))

    def _title_affinity_score(
        self,
        *,
        unit: TextUnit,
        unit_embedding: list[float],
        title_embedding: list[float],
        query_embedding: list[float],
    ) -> float:
        if not title_embedding:
            return 0.0
        title_support = _embedding_affinity(unit_embedding, title_embedding)
        query_support = _embedding_affinity(title_embedding, query_embedding) if query_embedding else 0.0
        depth_bonus = min(0.12, len(unit.title_path) * 0.03)
        return min(1.0, title_support + (0.2 * query_support) + depth_bonus)

    def _unit_structure_score(self, unit: TextUnit) -> float:
        title_bonus = min(0.12, len(unit.title_path) * 0.03)
        position_bonus = 0.08 if unit.sentence_index in {0, None} else max(0.0, 0.06 - (unit.sentence_index * 0.02))
        level_base = {
            "heading": 0.76,
            "list_item": 0.74,
            "paragraph": 0.62,
            "sentence": 0.52,
            "table_block": 0.68,
            "code_block": 0.72,
        }.get(unit.level, 0.46)
        return min(1.0, level_base + title_bonus + position_bonus)

    def _unit_density_score(self, text: str, token_set: set[str]) -> float:
        tokens = tokenize(text)
        if not tokens:
            return 0.0
        unique_ratio = len(token_set) / max(1, len(tokens))
        length_balance = 1.0 - min(1.0, abs(len(text) - 160) / 320.0)
        return min(1.0, (0.72 * unique_ratio) + (0.28 * length_balance))

    def _unit_actionability_score(
        self,
        unit: TextUnit,
        text: str,
        title_text: str,
        local: UnitLocalSignals,
        centrality_score: float,
    ) -> float:
        procedural_affinity = _prototype_affinity(" ".join(part for part in [title_text, text] if part), "procedure")
        code_signal = 1.0 if CODELIKE_RE.search(text) or PATHLIKE_RE.search(text) else 0.0
        return min(
            1.0,
            (0.38 * local.sequence_signal)
            + (0.16 * local.block_signal)
            + (0.12 * local.compactness)
            + (0.1 * centrality_score)
            + (0.08 * local.local_cohesion)
            + (0.08 * code_signal)
            + (0.06 * local.boundary_signal)
            + (0.1 * procedural_affinity),
        )

    def _unit_constraint_score(
        self,
        unit: TextUnit,
        text: str,
        title_text: str,
        local: UnitLocalSignals,
        centrality_score: float,
    ) -> float:
        constraint_affinity = _prototype_affinity(" ".join(part for part in [title_text, text] if part), "constraint")
        list_kind = str(unit.metadata.get("list_kind") or "")
        structure_bonus = 0.16 if unit.level == "list_item" and list_kind == "unordered" else 0.0
        structure_bonus += 0.1 if unit.level in {"table_block", "code_block"} else 0.0
        version_penalty = 0.14 if VERSIONLIKE_RE.search(text) and local.sequence_signal >= 0.5 else 0.0
        score = (
            (0.26 * local.numeric_signal)
            + (0.18 * local.numeric_contrast)
            + (0.14 * local.operator_signal)
            + (0.08 * local.operator_contrast)
            + (0.08 * local.compactness)
            + (0.06 * local.boundary_signal)
            + (0.06 * centrality_score)
            + (0.08 * constraint_affinity)
            + structure_bonus
            - (0.12 * local.sequence_signal)
            - version_penalty
        )
        return min(1.0, max(0.0, score))

    def _unit_risk_score(
        self,
        unit: TextUnit,
        text: str,
        title_text: str,
        local: UnitLocalSignals,
        centrality_score: float,
    ) -> float:
        risk_affinity = _prototype_affinity(" ".join(part for part in [title_text, text] if part), "risk")
        structure_bonus = 0.06 if unit.level == "paragraph" else 0.0
        transition_penalty = 0.1 * max(0.0, local.sequence_signal - local.terminal_signal)
        procedural_penalty = 0.12 if local.sequence_signal >= 0.6 and risk_affinity < 0.12 and local.clause_complexity < 0.08 else 0.0
        score = (
            (0.1 * local.local_contrast)
            + (0.06 * local.section_contrast)
            + (0.06 * local.boundary_signal)
            + (0.18 * local.terminal_signal)
            + (0.12 * local.clause_complexity)
            + (0.06 * centrality_score)
            + (0.24 * risk_affinity)
            + structure_bonus
            - transition_penalty
            - procedural_penalty
        )
        return min(1.0, max(0.0, score))

    def _unit_local_signals(self, units: list[TextUnit], embeddings: list[list[float]]) -> list[UnitLocalSignals]:
        numeric_values = [_numeric_signal(unit.text) for unit in units]
        operator_values = [_operator_density(unit.text) for unit in units]
        clause_values = [_clause_complexity(unit.text) for unit in units]
        compactness_values = [_compactness_score(unit.text, level=unit.level) for unit in units]

        section_embeddings: dict[tuple[str, int, tuple[str, ...]], list[list[float]]] = {}
        section_numeric: dict[tuple[str, int, tuple[str, ...]], list[float]] = {}
        section_operator: dict[tuple[str, int, tuple[str, ...]], list[float]] = {}
        for index, unit in enumerate(units):
            key = _unit_section_key(unit)
            section_embeddings.setdefault(key, []).append(embeddings[index])
            section_numeric.setdefault(key, []).append(numeric_values[index])
            section_operator.setdefault(key, []).append(operator_values[index])

        section_centroids = {key: _average_embedding(value) for key, value in section_embeddings.items()}
        section_numeric_mean = {key: _mean(value) for key, value in section_numeric.items()}
        section_operator_mean = {key: _mean(value) for key, value in section_operator.items()}

        signals: list[UnitLocalSignals] = []
        for index, unit in enumerate(units):
            key = _unit_section_key(unit)
            neighbor_indices = self._neighbor_indices(index, units)
            similarities = [_embedding_affinity(embeddings[index], embeddings[item]) for item in neighbor_indices]
            local_cohesion = _topk_mean(similarities, k=2)
            section_cohesion = _embedding_affinity(embeddings[index], section_centroids.get(key, []))
            feature_contrast = min(
                1.0,
                1.4
                * _mean(
                    [
                        abs(numeric_values[index] - numeric_values[item])
                        + abs(operator_values[index] - operator_values[item])
                        + abs(clause_values[index] - clause_values[item])
                        for item in neighbor_indices
                    ]
                ),
            )
            numeric_baseline = max(
                _mean([numeric_values[item] for item in neighbor_indices]),
                section_numeric_mean.get(key, 0.0),
            )
            operator_baseline = max(
                _mean([operator_values[item] for item in neighbor_indices]),
                section_operator_mean.get(key, 0.0),
            )
            signals.append(
                UnitLocalSignals(
                    local_cohesion=round(local_cohesion, 6),
                    local_contrast=round(min(1.0, (0.35 * max(0.0, 1.0 - local_cohesion)) + (0.65 * feature_contrast)), 6),
                    section_cohesion=round(section_cohesion, 6),
                    section_contrast=round(max(0.0, 1.0 - section_cohesion), 6),
                    numeric_signal=round(numeric_values[index], 6),
                    numeric_contrast=round(max(0.0, numeric_values[index] - numeric_baseline), 6),
                    operator_signal=round(operator_values[index], 6),
                    operator_contrast=round(max(0.0, operator_values[index] - operator_baseline), 6),
                    sequence_signal=round(self._sequence_signal(index, units), 6),
                    boundary_signal=round(self._boundary_signal(index, units), 6),
                    terminal_signal=round(self._terminal_signal(index, units), 6),
                    block_signal=round(self._block_signal(unit), 6),
                    compactness=round(compactness_values[index], 6),
                    clause_complexity=round(clause_values[index], 6),
                )
            )
        return signals

    def _neighbor_indices(self, index: int, units: list[TextUnit]) -> list[int]:
        target = units[index]
        neighbors: list[int] = []
        previous = self._context_neighbor(index, units, direction=-1, same_section_only=True)
        following = self._context_neighbor(index, units, direction=1, same_section_only=True)
        if previous is not None:
            neighbors.append(previous)
        if following is not None:
            neighbors.append(following)
        if neighbors:
            return neighbors
        previous = self._context_neighbor(index, units, direction=-1, same_section_only=False)
        following = self._context_neighbor(index, units, direction=1, same_section_only=False)
        if previous is not None:
            neighbors.append(previous)
        if following is not None:
            neighbors.append(following)
        return neighbors

    def _context_neighbor(
        self,
        index: int,
        units: list[TextUnit],
        *,
        direction: int,
        same_section_only: bool,
    ) -> int | None:
        target = units[index]
        cursor = index + direction
        while 0 <= cursor < len(units):
            candidate = units[cursor]
            if _same_span(target, candidate):
                cursor += direction
                continue
            if same_section_only and not _same_section(target, candidate):
                cursor += direction
                continue
            return cursor
        return None

    def _sequence_signal(self, index: int, units: list[TextUnit]) -> float:
        unit = units[index]
        ordinal = unit.metadata.get("list_ordinal")
        previous_index = self._context_neighbor(index, units, direction=-1, same_section_only=True)
        following_index = self._context_neighbor(index, units, direction=1, same_section_only=True)
        previous = units[previous_index] if previous_index is not None else None
        following = units[following_index] if following_index is not None else None
        signal = 0.0
        if isinstance(ordinal, int):
            signal = 0.52
            previous_ordinal = previous.metadata.get("list_ordinal") if previous and _same_section(previous, unit) else None
            following_ordinal = following.metadata.get("list_ordinal") if following and _same_section(following, unit) else None
            if isinstance(previous_ordinal, int) and previous_ordinal == ordinal - 1:
                signal += 0.24
            if isinstance(following_ordinal, int) and following_ordinal == ordinal + 1:
                signal += 0.24
        elif unit.level == "list_item":
            if previous and previous.level == "list_item" and _same_section(previous, unit):
                signal += 0.08
            if following and following.level == "list_item" and _same_section(following, unit):
                signal += 0.08
        continued_lines = int(unit.metadata.get("continued_lines", 0) or 0)
        if continued_lines > 0:
            signal += min(0.12, continued_lines * 0.06)
        return min(1.0, signal)

    def _boundary_signal(self, index: int, units: list[TextUnit]) -> float:
        unit = units[index]
        previous_index = self._context_neighbor(index, units, direction=-1, same_section_only=False)
        following_index = self._context_neighbor(index, units, direction=1, same_section_only=False)
        previous = units[previous_index] if previous_index is not None else None
        following = units[following_index] if following_index is not None else None
        signal = 0.0
        if previous is None or not _same_section(previous, unit):
            signal += 0.34
        elif previous.level != unit.level:
            signal += 0.14
        if previous and previous.level == "list_item" and unit.level == "paragraph" and _same_section(previous, unit):
            signal += 0.22
        if following is None or not _same_section(following, unit):
            signal += 0.16
        return min(1.0, signal)

    def _terminal_signal(self, index: int, units: list[TextUnit]) -> float:
        unit = units[index]
        ordinal = unit.metadata.get("list_ordinal")
        previous_index = self._context_neighbor(index, units, direction=-1, same_section_only=False)
        following_index = self._context_neighbor(index, units, direction=1, same_section_only=False)
        previous = units[previous_index] if previous_index is not None else None
        following = units[following_index] if following_index is not None else None
        signal = 0.0
        if isinstance(ordinal, int):
            following_ordinal = following.metadata.get("list_ordinal") if following and _same_section(following, unit) else None
            if not isinstance(following_ordinal, int) or following_ordinal != ordinal + 1:
                signal += 0.48
        elif unit.level == "paragraph" and previous and previous.level == "list_item" and _same_section(previous, unit):
            signal += 0.34
        if following is None or not _same_section(following, unit):
            signal += 0.26
        if index == len(units) - 1:
            signal += 0.12
        return min(1.0, signal)

    def _block_signal(self, unit: TextUnit) -> float:
        if unit.level == "code_block":
            return 1.0
        if unit.level == "table_block":
            return 0.84
        continued_lines = int(unit.metadata.get("continued_lines", 0) or 0)
        if continued_lines > 0:
            return min(0.48, 0.18 + (continued_lines * 0.12))
        return 0.0
