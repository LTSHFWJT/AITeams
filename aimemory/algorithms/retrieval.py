from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from aimemory.core.text import character_ngrams, cosine_similarity, hash_embedding, normalize_text, tokenize
from aimemory.core.utils import json_loads


def estimate_tokens(text: str | None) -> int:
    cleaned = str(text or "").strip()
    if not cleaned:
        return 0
    return max(1, math.ceil(len(cleaned) / 4))


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def recency_multiplier(updated_at: str | None, half_life_days: float, *, now: datetime | None = None) -> float:
    if half_life_days <= 0:
        return 1.0
    parsed = parse_timestamp(updated_at)
    if parsed is None:
        return 1.0
    current = now or datetime.now(timezone.utc)
    age_seconds = max(0.0, (current - parsed).total_seconds())
    age_days = age_seconds / 86400.0
    decay_lambda = math.log(2) / max(half_life_days, 1e-6)
    return math.exp(-decay_lambda * age_days)


def _overlap_score(query_tokens: set[str], candidate_tokens: set[str]) -> float:
    if not query_tokens or not candidate_tokens:
        return 0.0
    intersection = len(query_tokens & candidate_tokens)
    return intersection / max(1, len(query_tokens))


def _token_weight(token: str) -> float:
    cleaned = str(token or "").strip()
    if not cleaned:
        return 0.0
    if cleaned.isdigit():
        return 0.8
    return 1.0 + min(1.4, len(cleaned) / 6.0)


def _weighted_overlap_score(query_tokens: set[str], candidate_tokens: set[str]) -> float:
    if not query_tokens or not candidate_tokens:
        return 0.0
    denominator = sum(_token_weight(token) for token in query_tokens)
    if denominator <= 0:
        return 0.0
    numerator = sum(_token_weight(token) for token in query_tokens if token in candidate_tokens)
    return numerator / denominator


def _weighted_jaccard_score(left_tokens: list[str], right_tokens: list[str]) -> float:
    if not left_tokens or not right_tokens:
        return 0.0
    left_counts = Counter(left_tokens)
    right_counts = Counter(right_tokens)
    keys = set(left_counts) | set(right_counts)
    if not keys:
        return 0.0
    intersection = 0.0
    union = 0.0
    for key in keys:
        weight = _token_weight(key)
        intersection += min(left_counts.get(key, 0), right_counts.get(key, 0)) * weight
        union += max(left_counts.get(key, 0), right_counts.get(key, 0)) * weight
    return intersection / union if union else 0.0


def _char_ngram_score(left_text: str, right_text: str) -> float:
    left = set(character_ngrams(left_text[:320], min_n=2, max_n=4))
    right = set(character_ngrams(right_text[:960], min_n=2, max_n=4))
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def _coerce_keywords(value: str | list[str] | None) -> list[str]:
    if isinstance(value, list):
        return tokenize(" ".join(str(item).strip() for item in value if str(item).strip()))
    if isinstance(value, str):
        loaded = json_loads(value)
        if isinstance(loaded, list):
            return tokenize(" ".join(str(item).strip() for item in loaded if str(item).strip()))
        return tokenize(value)
    return []


def score_record(
    query: str,
    *,
    text: str,
    keywords: str | list[str] | None = None,
    embedding: str | list[float] | None = None,
    updated_at: str | None = None,
    importance: float = 0.5,
    lexical_score: float = 0.0,
    boost: float = 0.0,
    half_life_days: float = 30.0,
) -> tuple[float, dict[str, float]]:
    query_normalized = normalize_text(query)
    text_normalized = normalize_text(text)
    query_token_list = tokenize(query_normalized)
    text_token_list = tokenize(text_normalized)
    query_tokens = set(query_token_list)
    text_tokens = set(text_token_list)
    keyword_tokens = set(_coerce_keywords(keywords))

    dense_vector = json_loads(embedding) if isinstance(embedding, str) else embedding
    query_embedding = hash_embedding(query_normalized)
    dense_score = max(
        0.0,
        cosine_similarity(
            query_embedding,
            dense_vector if isinstance(dense_vector, list) and dense_vector else hash_embedding(text_normalized),
        ),
    )
    sparse_score = _weighted_overlap_score(query_tokens, text_tokens)
    keyword_score = _weighted_overlap_score(query_tokens, keyword_tokens)
    weighted_sparse_score = _weighted_jaccard_score(query_token_list, text_token_list)
    char_score = _char_ngram_score(query_normalized, text_normalized)
    exact_score = 1.0 if query_normalized and query_normalized in text_normalized else 0.0
    recency_score = recency_multiplier(updated_at, half_life_days)
    importance_score = max(0.0, min(1.0, float(importance or 0.0)))
    lexical_score = max(0.0, min(1.0, float(lexical_score or 0.0)))
    lexical_signal = max(lexical_score, (0.72 * lexical_score) + (0.28 * max(sparse_score, keyword_score, char_score)))

    score = (
        (0.31 * dense_score)
        + (0.17 * sparse_score)
        + (0.12 * weighted_sparse_score)
        + (0.08 * keyword_score)
        + (0.14 * lexical_signal)
        + (0.08 * char_score)
        + (0.04 * exact_score)
        + (0.04 * recency_score)
        + (0.02 * importance_score)
        + boost
    )
    breakdown = {
        "dense": round(dense_score, 6),
        "sparse": round(sparse_score, 6),
        "weighted_sparse": round(weighted_sparse_score, 6),
        "keyword": round(keyword_score, 6),
        "lexical": round(lexical_signal, 6),
        "char_ngram": round(char_score, 6),
        "exact": round(exact_score, 6),
        "recency": round(recency_score, 6),
        "importance": round(importance_score, 6),
        "boost": round(boost, 6),
    }
    return round(score, 6), breakdown


def mmr_rerank(
    records: list[dict[str, Any]],
    *,
    lambda_value: float = 0.72,
    limit: int | None = None,
    text_key: str = "text",
) -> list[dict[str, Any]]:
    if len(records) <= 1:
        return records[: limit or len(records)]

    clamped = max(0.0, min(1.0, lambda_value))
    pending = [dict(item) for item in records]
    pending.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
    selected: list[dict[str, Any]] = []
    token_cache: dict[str, set[str]] = {}

    def token_set(item: dict[str, Any]) -> set[str]:
        item_id = str(item.get("id") or item.get("record_id") or len(token_cache))
        if item_id not in token_cache:
            token_cache[item_id] = set(tokenize(str(item.get(text_key) or "")))
        return token_cache[item_id]

    while pending and (limit is None or len(selected) < limit):
        if not selected:
            selected.append(pending.pop(0))
            continue
        best_index = 0
        best_score = -float("inf")
        for index, candidate in enumerate(pending):
            candidate_tokens = token_set(candidate)
            max_similarity = 0.0
            for item in selected:
                item_tokens = token_set(item)
                union = candidate_tokens | item_tokens
                similarity = (len(candidate_tokens & item_tokens) / len(union)) if union else 0.0
                if similarity > max_similarity:
                    max_similarity = similarity
            score = (clamped * float(candidate.get("score", 0.0))) - ((1.0 - clamped) * max_similarity)
            if score > best_score:
                best_score = score
                best_index = index
        selected.append(pending.pop(best_index))

    return selected
