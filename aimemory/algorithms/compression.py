from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aimemory.algorithms.retrieval import estimate_tokens, mmr_rerank
from aimemory.core.text import build_summary


@dataclass(slots=True)
class CompressionResult:
    summary: str
    highlights: list[str]
    kept_ids: list[str]
    estimated_tokens: int
    source_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "highlights": list(self.highlights),
            "kept_ids": list(self.kept_ids),
            "estimated_tokens": int(self.estimated_tokens),
            "source_count": int(self.source_count),
        }


def compress_records(
    records: list[dict[str, Any]],
    *,
    budget_chars: int = 600,
    max_sentences: int = 8,
    diversity_lambda: float = 0.72,
) -> CompressionResult:
    if not records:
        return CompressionResult(summary="", highlights=[], kept_ids=[], estimated_tokens=0, source_count=0)

    prepared: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        text = str(record.get("text") or record.get("content") or record.get("summary") or "").strip()
        if not text:
            continue
        prepared.append(
            {
                "id": str(record.get("id") or record.get("record_id") or index),
                "text": text,
                "score": float(record.get("score", 0.35 + min(0.45, len(text) / 320.0))),
            }
        )

    reranked = mmr_rerank(prepared, lambda_value=diversity_lambda)
    highlights: list[str] = []
    kept_ids: list[str] = []
    used_chars = 0
    for item in reranked:
        text = item["text"]
        if used_chars and (used_chars + len(text)) > budget_chars:
            continue
        highlights.append(text)
        kept_ids.append(item["id"])
        used_chars += len(text)
        if used_chars >= budget_chars:
            break
    if not highlights and reranked:
        highlights.append(reranked[0]["text"][:budget_chars])
        kept_ids.append(reranked[0]["id"])

    summary = build_summary(highlights, max_sentences=max_sentences, max_chars=budget_chars)
    return CompressionResult(
        summary=summary,
        highlights=highlights,
        kept_ids=kept_ids,
        estimated_tokens=estimate_tokens(summary),
        source_count=len(records),
    )
