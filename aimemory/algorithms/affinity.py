from __future__ import annotations

from typing import Iterable

from aimemory.core.text import cosine_similarity, hash_embedding, normalize_text, tokenize


DOMAIN_PROTOTYPES: dict[str, str] = {
    "memory": "personal memory long-term fact preference profile relationship episodic procedural reusable context",
    "knowledge": "reference knowledge document source citation manual specification article guide documentation factual material",
    "skill": "skill workflow reusable capability tool binding prompt template procedure operating method agent action",
    "archive": "archive historical summary compressed digest older context prior conversation retired memory preserved snapshot",
    "execution": "execution run task checkpoint observation progress tool call plan step process trace runtime state",
    "interaction": "interaction session conversation recent dialog latest turns working context short-term exchange current thread",
}

MEMORY_TYPE_PROTOTYPES: dict[str, str] = {
    "semantic": "general fact decision constraint concept information stable memory",
    "episodic": "recent event transient observation happened progress current activity checkpoint",
    "procedural": "reusable method workflow procedure skill operating sequence tool usage",
    "profile": "identity background role name biography user profile stable personal descriptor",
    "preference": "personal preference favored style choice habit like dislike format option",
    "relationship_summary": "relationship between people roles stakeholder teammate owner manager collaborator contact",
}

STRATEGY_SCOPE_PROTOTYPES: dict[str, str] = {
    "user": "stable user-centric preference profile relationship long-term personalization",
    "agent": "reusable agent-centric operating skill workflow tool strategy procedural memory",
    "run": "transient run-centric execution step observation checkpoint episodic continuity",
}


def prototype_affinities(
    text: str | None,
    prototypes: dict[str, str],
    *,
    dense_weight: float = 0.72,
    sparse_weight: float = 0.24,
    containment_weight: float = 0.04,
) -> dict[str, float]:
    normalized = normalize_text(text)
    query_embedding = hash_embedding(normalized)
    query_tokens = set(tokenize(normalized))
    scores: dict[str, float] = {}
    for label, prototype in prototypes.items():
        prototype_normalized = normalize_text(prototype)
        dense = max(0.0, cosine_similarity(query_embedding, hash_embedding(prototype_normalized)))
        prototype_tokens = set(tokenize(prototype_normalized))
        sparse = 0.0
        if query_tokens and prototype_tokens:
            sparse = len(query_tokens & prototype_tokens) / max(1, len(query_tokens))
        containment = 1.0 if normalized and (normalized in prototype_normalized or prototype_normalized in normalized) else 0.0
        scores[label] = round((dense_weight * dense) + (sparse_weight * sparse) + (containment_weight * containment), 6)
    return scores


def ranked_labels(
    scores: dict[str, float],
    *,
    top_n: int = 3,
    min_score: float = 0.12,
    relative_threshold: float = 0.82,
) -> list[str]:
    if not scores:
        return []
    ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    max_score = ordered[0][1]
    threshold = max(min_score, max_score * relative_threshold)
    labels = [label for label, score in ordered if score >= threshold][:top_n]
    return labels or [ordered[0][0]]


def best_label(scores: dict[str, float], *, default: str) -> str:
    if not scores:
        return default
    return max(scores.items(), key=lambda item: item[1])[0]


def normalize_score_map(scores: dict[str, float]) -> dict[str, float]:
    if not scores:
        return {}
    maximum = max(scores.values())
    if maximum <= 0:
        return {key: 0.0 for key in scores}
    return {key: round(value / maximum, 6) for key, value in scores.items()}


def blend_score_maps(*maps: dict[str, float]) -> dict[str, float]:
    keys: set[str] = set()
    for item in maps:
        keys.update(item.keys())
    return {key: round(sum(item.get(key, 0.0) for item in maps), 6) for key in keys}


def tokens_density(text: str | None) -> float:
    tokens = tokenize(text)
    if not tokens:
        return 0.0
    return min(1.0, len(set(tokens)) / max(1, len(tokens)))


def coverage_ratio(text: str | None, reference: str | None) -> float:
    left = set(tokenize(text))
    right = set(tokenize(reference))
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, len(left))


def choose_labels(scores: dict[str, float], preferred: Iterable[str] | None = None, *, top_n: int = 3) -> list[str]:
    preferred = list(preferred or [])
    ordered = ranked_labels(scores, top_n=top_n)
    merged: list[str] = []
    for label in [*preferred, *ordered]:
        if label not in merged and label in scores:
            merged.append(label)
    return merged
