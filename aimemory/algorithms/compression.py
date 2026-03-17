from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from aimemory.algorithms.dedupe import semantic_similarity
from aimemory.algorithms.distill import AdaptiveDistiller, DistilledUnitCandidate
from aimemory.algorithms.retrieval import estimate_tokens, mmr_rerank
from aimemory.algorithms.segmentation import segment_text
from aimemory.core.text import build_summary, split_sentences
from aimemory.memory_intelligence.policies import MemoryPolicy


@dataclass(slots=True)
class CompressionResult:
    summary: str
    highlights: list[str]
    kept_ids: list[str]
    estimated_tokens: int
    source_count: int
    facts: list[str] = field(default_factory=list)
    constraints: list[str] = field(default_factory=list)
    steps: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    selected_unit_ids: list[str] = field(default_factory=list)
    coverage_score: float = 0.0
    redundancy_score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)
    evidence_spans: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "highlights": list(self.highlights),
            "kept_ids": list(self.kept_ids),
            "estimated_tokens": int(self.estimated_tokens),
            "source_count": int(self.source_count),
            "facts": list(self.facts),
            "constraints": list(self.constraints),
            "steps": list(self.steps),
            "risks": list(self.risks),
            "selected_unit_ids": list(self.selected_unit_ids),
            "coverage_score": round(float(self.coverage_score), 6),
            "redundancy_score": round(float(self.redundancy_score), 6),
            "metadata": dict(self.metadata),
            "evidence_spans": [dict(item) for item in self.evidence_spans],
        }


def compress_text(
    text: str,
    *,
    query: str | None = None,
    domain_hint: str | None = None,
    budget_chars: int = 600,
    max_sentences: int = 8,
    diversity_lambda: float = 0.72,
    max_highlights: int = 12,
    policy: MemoryPolicy | None = None,
    source_id: str = "text",
    metadata: dict[str, Any] | None = None,
) -> CompressionResult:
    return compress_records(
        [
            {
                "id": source_id,
                "text": text,
                "score": 1.0,
                "metadata": metadata or {},
            }
        ],
        query=query,
        domain_hint=domain_hint,
        budget_chars=budget_chars,
        max_sentences=max_sentences,
        diversity_lambda=diversity_lambda,
        max_highlights=max_highlights,
        policy=policy,
    )


def compress_records(
    records: list[dict[str, Any]],
    *,
    budget_chars: int = 600,
    max_sentences: int = 8,
    diversity_lambda: float = 0.72,
    query: str | None = None,
    domain_hint: str | None = None,
    max_highlights: int = 12,
    policy: MemoryPolicy | None = None,
) -> CompressionResult:
    if not records:
        return CompressionResult(summary="", highlights=[], kept_ids=[], estimated_tokens=0, source_count=0)

    effective_policy = policy or MemoryPolicy()
    units, source_count = _record_units(records)
    if not units:
        return CompressionResult(summary="", highlights=[], kept_ids=[], estimated_tokens=0, source_count=source_count)

    distiller = AdaptiveDistiller(effective_policy)
    candidates = distiller.distill_units(
        units,
        query=query,
        domain_hint=domain_hint,
        limit=max(max_highlights * 6, effective_policy.max_candidates * 6, min(len(units), 48), 24),
    )
    if not candidates:
        return _fallback_result(records, budget_chars=budget_chars, max_sentences=max_sentences)

    selected = _select_candidates(
        candidates,
        budget_chars=max(budget_chars, 80),
        max_highlights=max(max_highlights, 1),
        diversity_lambda=diversity_lambda,
    )
    if not selected:
        return _fallback_result(records, budget_chars=budget_chars, max_sentences=max_sentences)

    selected_ordered = sorted(
        selected,
        key=lambda item: (
            int(item.metadata.get("section_index", 0) or 0),
            int(item.metadata.get("paragraph_index", 0) or 0),
            int(item.metadata.get("sentence_index", -1) or -1),
            str(item.id),
        ),
    )
    highlights = _build_highlights(selected_ordered, budget_chars=budget_chars, max_highlights=max_highlights)
    kept_ids = list(
        dict.fromkeys(
            str(item.metadata.get("source_id") or item.id)
            for item in selected_ordered
            if str(item.metadata.get("source_id") or item.id).strip()
        )
    )
    selected_unit_ids = [item.id for item in selected_ordered]
    facts, constraints, steps, risks = _build_structured_slots(selected_ordered, budget_chars=budget_chars)
    summary = _build_summary_text(
        selected_ordered,
        highlights=highlights,
        budget_chars=budget_chars,
        max_sentences=max_sentences,
    )

    return CompressionResult(
        summary=summary,
        highlights=highlights,
        kept_ids=kept_ids,
        estimated_tokens=estimate_tokens(summary),
        source_count=source_count,
        facts=facts,
        constraints=constraints,
        steps=steps,
        risks=risks,
        selected_unit_ids=selected_unit_ids,
        coverage_score=_coverage_score(candidates, selected_ordered),
        redundancy_score=_redundancy_score(selected_ordered),
        metadata={
            "query": str(query or ""),
            "domain_hint": str(domain_hint or ""),
            "unit_count": len(units),
            "candidate_count": len(candidates),
        },
        evidence_spans=[
            {
                "unit_id": item.id,
                "source_id": item.metadata.get("source_id"),
                "title_path": list(item.title_path),
                "level": item.level,
                "start_offset": item.metadata.get("start_offset"),
                "end_offset": item.metadata.get("end_offset"),
                "score": item.score,
            }
            for item in selected_ordered
        ],
    )


def _record_units(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    units: list[dict[str, Any]] = []
    valid_sources = 0
    for index, record in enumerate(records):
        text = str(record.get("text") or record.get("content") or record.get("summary") or "").strip()
        if not text:
            continue
        valid_sources += 1
        source_id = str(record.get("id") or record.get("record_id") or index)
        source_score = float(record.get("score", 0.35 + min(0.45, len(text) / 320.0)))
        metadata = dict(record.get("metadata") or {})
        for unit in segment_text(text, source_id=source_id):
            units.append(
                {
                    "unit": unit,
                    "source_id": source_id,
                    "source_score": source_score,
                    "source_metadata": metadata,
                }
            )
    normalized_units: list[dict[str, Any]] = []
    for item in units:
        unit = item["unit"]
        unit.metadata = {
            **dict(unit.metadata),
            "source_id": item["source_id"],
            "source_score": item["source_score"],
            **dict(item["source_metadata"]),
            "start_offset": unit.start_offset,
            "end_offset": unit.end_offset,
        }
        normalized_units.append(item)
    return [item["unit"] for item in normalized_units], valid_sources


def _select_candidates(
    candidates: list[DistilledUnitCandidate],
    *,
    budget_chars: int,
    max_highlights: int,
    diversity_lambda: float,
) -> list[DistilledUnitCandidate]:
    if not candidates:
        return []

    grouped: dict[tuple[Any, ...], list[DistilledUnitCandidate]] = {}
    for candidate in candidates:
        key = (
            candidate.metadata.get("source_id"),
            candidate.metadata.get("section_index", 0),
            tuple(candidate.title_path),
        )
        grouped.setdefault(key, []).append(candidate)

    seeded: list[DistilledUnitCandidate] = []
    for _key, group in sorted(
        grouped.items(),
        key=lambda item: max(candidate.score for candidate in item[1]),
        reverse=True,
    )[: max(1, min(4, len(grouped)))]:
        seeded.append(max(group, key=lambda candidate: candidate.score))
    seeded.extend(_slot_seed_candidates(candidates))

    selected: list[DistilledUnitCandidate] = []
    seen_ids: set[str] = set()
    used_chars = 0
    for candidate in seeded:
        if candidate.id in seen_ids:
            continue
        if selected and used_chars + len(candidate.text) > budget_chars:
            continue
        selected.append(candidate)
        seen_ids.add(candidate.id)
        used_chars += len(candidate.text)

    selected, used_chars = _complete_numbered_sequences(
        selected,
        candidates,
        budget_chars=budget_chars,
        max_highlights=max_highlights,
        used_chars=used_chars,
    )
    seen_ids = {item.id for item in selected}

    reranked = mmr_rerank(
        [{"id": candidate.id, "text": candidate.text, "score": candidate.score} for candidate in candidates],
        lambda_value=diversity_lambda,
        limit=max(len(candidates), max_highlights),
    )
    by_id = {candidate.id: candidate for candidate in candidates}
    for item in reranked:
        candidate = by_id[item["id"]]
        if candidate.id in seen_ids:
            continue
        if len(selected) >= max_highlights:
            break
        if any(semantic_similarity(candidate.text, existing.text) >= 0.94 for existing in selected):
            continue
        next_chars = used_chars + len(candidate.text)
        if selected and next_chars > budget_chars and used_chars >= int(budget_chars * 0.72):
            continue
        selected.append(candidate)
        seen_ids.add(candidate.id)
        used_chars = next_chars
        if used_chars >= budget_chars:
            break

    selected, used_chars = _ensure_slot_coverage(
        selected,
        candidates,
        budget_chars=budget_chars,
        max_highlights=max_highlights,
        used_chars=used_chars,
    )

    if not selected and candidates:
        selected.append(candidates[0])
    return selected


def _build_highlights(
    selected: list[DistilledUnitCandidate],
    *,
    budget_chars: int,
    max_highlights: int,
) -> list[str]:
    highlights: list[str] = []
    used_chars = 0
    max_highlight_chars = max(120, budget_chars // max(1, min(max_highlights, 4)))
    for candidate in selected:
        text = _contextualized_text(candidate, max_chars=max_highlight_chars)
        if not text:
            continue
        if highlights and (used_chars + len(text)) > budget_chars:
            continue
        highlights.append(text)
        used_chars += len(text)
        if len(highlights) >= max_highlights or used_chars >= budget_chars:
            break
    if not highlights and selected:
        highlights.append(str(selected[0].text)[:budget_chars])
    return highlights


def _build_summary_text(
    selected: list[DistilledUnitCandidate],
    *,
    highlights: list[str],
    budget_chars: int,
    max_sentences: int,
) -> str:
    summary_parts: list[str] = []
    seen: set[str] = set()
    max_part_chars = max(48, min(96, budget_chars // 2))
    for predicate in (_is_step_candidate, _is_constraint_candidate, _is_risk_candidate):
        for candidate in selected:
            if not predicate(candidate):
                continue
            text = _contextualized_text(candidate, max_chars=max_part_chars)
            normalized = text.strip()
            if not normalized or normalized in seen:
                continue
            summary_parts.append(normalized)
            seen.add(normalized)
    for item in highlights:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        summary_parts.append(normalized)
        seen.add(normalized)
    if not summary_parts:
        summary_parts = [item for item in highlights if str(item or "").strip()]
    return build_summary(summary_parts, max_sentences=max_sentences, max_chars=budget_chars)


def _build_structured_slots(
    selected: list[DistilledUnitCandidate],
    *,
    budget_chars: int,
) -> tuple[list[str], list[str], list[str], list[str]]:
    unique_texts: dict[str, DistilledUnitCandidate] = {}
    for item in selected:
        unique_texts.setdefault(item.text, item)

    ranked = list(unique_texts.values())
    facts = _slot_texts(
        sorted(
            ranked,
            key=lambda item: (
                item.information_score + (0.3 * item.structure_score),
                item.score,
            ),
            reverse=True,
        ),
        limit_chars=max(120, budget_chars // 2),
        max_items=6,
    )
    constraints = _slot_texts(
        [item for item in ranked if _is_constraint_candidate(item)],
        limit_chars=max(90, budget_chars // 3),
        max_items=5,
    )
    steps = _slot_texts(
        [
            item
            for item in sorted(
                ranked,
                key=lambda item: (item.actionability_score + (0.08 if item.metadata.get("list_ordinal") else 0.0), item.score),
                reverse=True,
            )
            if _is_step_candidate(item)
        ],
        limit_chars=max(100, budget_chars // 2),
        max_items=6,
    )
    risks = _slot_texts(
        [
            item
            for item in sorted(
                ranked,
                key=lambda item: (item.risk_score, item.constraint_score, item.score),
                reverse=True,
            )
            if _is_risk_candidate(item)
        ],
        limit_chars=max(90, budget_chars // 3),
        max_items=4,
    )
    return facts, constraints, steps, risks


def _slot_texts(items: list[DistilledUnitCandidate], *, limit_chars: int, max_items: int) -> list[str]:
    results: list[str] = []
    used_chars = 0
    seen: set[str] = set()
    for item in items:
        text = str(item.text or "").strip()
        if not text or text in seen:
            continue
        if results and used_chars + len(text) > limit_chars:
            continue
        results.append(text)
        seen.add(text)
        used_chars += len(text)
        if len(results) >= max_items or used_chars >= limit_chars:
            break
    return results


def _coverage_score(candidates: list[DistilledUnitCandidate], selected: list[DistilledUnitCandidate]) -> float:
    sections = {
        (candidate.metadata.get("source_id"), candidate.metadata.get("section_index", 0))
        for candidate in candidates
    }
    if not sections:
        return 1.0
    covered = {
        (candidate.metadata.get("source_id"), candidate.metadata.get("section_index", 0))
        for candidate in selected
    }
    return round(len(covered) / max(1, len(sections)), 6)


def _redundancy_score(selected: list[DistilledUnitCandidate]) -> float:
    if len(selected) <= 1:
        return 0.0
    total = 0.0
    count = 0
    for index, left in enumerate(selected):
        for right in selected[index + 1 :]:
            total += semantic_similarity(left.text, right.text)
            count += 1
    if count <= 0:
        return 0.0
    return round(total / count, 6)


def _section_label(candidate: DistilledUnitCandidate) -> str:
    if not candidate.title_path:
        return ""
    return str(candidate.title_path[-1]).strip()


def _contextualized_text(candidate: DistilledUnitCandidate, *, max_chars: int) -> str:
    text = str(candidate.text or "").strip()
    if not text:
        return ""
    section_label = _section_label(candidate)
    prefix = f"[{section_label}] " if section_label else ""
    max_text_chars = max_chars - len(prefix)
    if max_text_chars <= 24:
        max_text_chars = max_chars
        prefix = ""
    text = _trim_highlight_text(text, max_chars=max_text_chars)
    if prefix and not text.startswith(prefix):
        return f"{prefix}{text}"
    return text


def _trim_highlight_text(text: str, *, max_chars: int) -> str:
    cleaned = str(text or "").strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return build_summary(split_sentences(cleaned), max_sentences=3, max_chars=max_chars)


def _is_step_candidate(candidate: DistilledUnitCandidate) -> bool:
    ordered = isinstance(candidate.metadata.get("list_ordinal"), int)
    imperative = candidate.actionability_score >= 0.34
    heavy_constraint = candidate.constraint_score >= 0.42 and not ordered
    return (ordered or imperative or candidate.level == "code_block") and not heavy_constraint


def _is_constraint_candidate(candidate: DistilledUnitCandidate) -> bool:
    return candidate.constraint_score >= 0.24


def _is_risk_candidate(candidate: DistilledUnitCandidate) -> bool:
    return candidate.risk_score >= 0.24


def _slot_seed_candidates(candidates: list[DistilledUnitCandidate]) -> list[DistilledUnitCandidate]:
    seeds: list[DistilledUnitCandidate] = []
    pools = (
        [item for item in candidates if _is_step_candidate(item)],
        [item for item in candidates if _is_constraint_candidate(item)],
        [item for item in candidates if _is_risk_candidate(item)],
    )
    key_functions = (
        lambda item: (item.actionability_score + (0.08 if item.metadata.get("list_ordinal") else 0.0), item.score),
        lambda item: (item.constraint_score, item.score),
        lambda item: (item.risk_score, item.score),
    )
    for pool, key_function in zip(pools, key_functions):
        if pool:
            seeds.append(max(pool, key=key_function))
    deduped: list[DistilledUnitCandidate] = []
    seen: set[str] = set()
    for candidate in seeds:
        if candidate.id in seen:
            continue
        deduped.append(candidate)
        seen.add(candidate.id)
    return deduped


def _complete_numbered_sequences(
    selected: list[DistilledUnitCandidate],
    candidates: list[DistilledUnitCandidate],
    *,
    budget_chars: int,
    max_highlights: int,
    used_chars: int,
) -> tuple[list[DistilledUnitCandidate], int]:
    selected_ids = {item.id for item in selected}
    groups: dict[tuple[Any, ...], dict[int, DistilledUnitCandidate]] = {}
    for candidate in candidates:
        ordinal = candidate.metadata.get("list_ordinal")
        if not isinstance(ordinal, int):
            continue
        key = (
            candidate.metadata.get("source_id"),
            candidate.metadata.get("section_index", 0),
            tuple(candidate.title_path),
        )
        groups.setdefault(key, {})[ordinal] = candidate

    queue: list[DistilledUnitCandidate] = []
    queued_ids: set[str] = set()
    for ordinal_map in groups.values():
        selected_ordinals = sorted(ordinal for ordinal, candidate in ordinal_map.items() if candidate.id in selected_ids)
        if not selected_ordinals:
            continue
        missing: list[int] = []
        lower = min(selected_ordinals)
        upper = max(selected_ordinals)
        if lower > 1 and (lower - 1) in ordinal_map:
            missing.append(lower - 1)
        for ordinal in range(lower, upper + 1):
            if ordinal not in selected_ordinals and ordinal in ordinal_map:
                missing.append(ordinal)
        if len(selected_ordinals) == 1 and (upper + 1) in ordinal_map:
            missing.append(upper + 1)
        for ordinal in missing:
            candidate = ordinal_map.get(ordinal)
            if candidate is None or candidate.id in selected_ids or candidate.id in queued_ids:
                continue
            queue.append(candidate)
            queued_ids.add(candidate.id)

    for candidate in sorted(
        queue,
        key=lambda item: (
            item.metadata.get("section_index", 0),
            item.metadata.get("paragraph_index", 0),
            item.metadata.get("list_ordinal", 0),
        ),
    ):
        if len(selected) >= max_highlights:
            break
        next_chars = used_chars + len(candidate.text)
        if selected and next_chars > budget_chars and used_chars >= int(budget_chars * 0.72):
            selected, used_chars, fitted = _rebalance_for_candidate(
                selected,
                candidate,
                used_chars=used_chars,
                budget_chars=budget_chars,
            )
            if not fitted:
                continue
            next_chars = used_chars + len(candidate.text)
        selected.append(candidate)
        selected_ids.add(candidate.id)
        used_chars = next_chars
        if used_chars >= budget_chars:
            break
    return selected, used_chars


def _ensure_slot_coverage(
    selected: list[DistilledUnitCandidate],
    candidates: list[DistilledUnitCandidate],
    *,
    budget_chars: int,
    max_highlights: int,
    used_chars: int,
) -> tuple[list[DistilledUnitCandidate], int]:
    selected_ids = {item.id for item in selected}
    checks = (
        (_is_step_candidate, lambda item: (item.actionability_score, item.score)),
        (_is_constraint_candidate, lambda item: (item.constraint_score, item.score)),
        (_is_risk_candidate, lambda item: (item.risk_score, item.score)),
    )
    for predicate, key_function in checks:
        if any(predicate(item) for item in selected):
            continue
        pool = [item for item in candidates if predicate(item) and item.id not in selected_ids]
        if not pool or len(selected) >= max_highlights:
            continue
        candidate = max(pool, key=key_function)
        next_chars = used_chars + len(candidate.text)
        if selected and next_chars > budget_chars and used_chars >= int(budget_chars * 0.72):
            continue
        selected.append(candidate)
        selected_ids.add(candidate.id)
        used_chars = next_chars
    return selected, used_chars


def _rebalance_for_candidate(
    selected: list[DistilledUnitCandidate],
    candidate: DistilledUnitCandidate,
    *,
    used_chars: int,
    budget_chars: int,
) -> tuple[list[DistilledUnitCandidate], int, bool]:
    if used_chars + len(candidate.text) <= budget_chars:
        return selected, used_chars, True
    removable = sorted(selected, key=_removal_priority)
    removed_ids: set[str] = set()
    adjusted_chars = used_chars
    for item in removable:
        if _removal_priority(item) >= _removal_priority(candidate):
            continue
        removed_ids.add(item.id)
        adjusted_chars -= len(item.text)
        if adjusted_chars + len(candidate.text) <= budget_chars:
            retained = [item for item in selected if item.id not in removed_ids]
            return retained, adjusted_chars, True
    return selected, used_chars, False


def _removal_priority(candidate: DistilledUnitCandidate) -> float:
    priority = float(candidate.score)
    if _is_step_candidate(candidate):
        priority += 1.0
    if _is_constraint_candidate(candidate):
        priority += 0.35
    if _is_risk_candidate(candidate):
        priority += 0.45
    if candidate.metadata.get("list_ordinal"):
        priority += 0.24
    if candidate.level == "code_block":
        priority += 0.2
    return round(priority, 6)


def _fallback_result(
    records: list[dict[str, Any]],
    *,
    budget_chars: int,
    max_sentences: int,
) -> CompressionResult:
    texts = [
        str(record.get("text") or record.get("content") or record.get("summary") or "").strip()
        for record in records
        if str(record.get("text") or record.get("content") or record.get("summary") or "").strip()
    ]
    highlights = []
    used_chars = 0
    kept_ids: list[str] = []
    for index, text in enumerate(texts):
        if highlights and used_chars + len(text) > budget_chars:
            continue
        highlights.append(text)
        used_chars += len(text)
        kept_ids.append(str(records[index].get("id") or records[index].get("record_id") or index))
    if not highlights and texts:
        highlights.append(texts[0][:budget_chars])
        kept_ids.append(str(records[0].get("id") or records[0].get("record_id") or 0))
    summary = build_summary(highlights, max_sentences=max_sentences, max_chars=budget_chars)
    return CompressionResult(
        summary=summary,
        highlights=highlights,
        kept_ids=kept_ids,
        estimated_tokens=estimate_tokens(summary),
        source_count=len(texts),
    )
