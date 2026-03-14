from __future__ import annotations

import hashlib
from collections import Counter

from aimemory.core.text import build_summary, cosine_similarity, hash_embedding, normalize_text, tokenize


def simhash64(text: str | None) -> int:
    tokens = Counter(tokenize(text))
    if not tokens:
        return 0
    weights = [0] * 64
    for token, count in tokens.items():
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        bits = int.from_bytes(digest, "big")
        for index in range(64):
            mask = 1 << index
            weights[index] += count if bits & mask else -count
    value = 0
    for index, weight in enumerate(weights):
        if weight >= 0:
            value |= 1 << index
    return value


def fingerprint(text: str | None) -> str:
    return f"{simhash64(text):016x}"


def hamming_distance(left: int | str, right: int | str) -> int:
    left_value = int(left, 16) if isinstance(left, str) else int(left)
    right_value = int(right, 16) if isinstance(right, str) else int(right)
    return (left_value ^ right_value).bit_count()


def hamming_similarity(left: int | str, right: int | str) -> float:
    return max(0.0, 1.0 - (hamming_distance(left, right) / 64.0))


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    intersection = len(left & right)
    union = len(left | right)
    return intersection / union if union else 0.0


def semantic_similarity(left_text: str | None, right_text: str | None) -> float:
    left_normalized = normalize_text(left_text)
    right_normalized = normalize_text(right_text)
    if not left_normalized or not right_normalized:
        return 0.0
    if left_normalized == right_normalized:
        return 1.0
    dense = max(0.0, cosine_similarity(hash_embedding(left_normalized), hash_embedding(right_normalized)))
    sparse = jaccard_similarity(set(tokenize(left_normalized)), set(tokenize(right_normalized)))
    containment = min(
        1.0,
        len(set(tokenize(left_normalized)) & set(tokenize(right_normalized))) / max(1, min(len(set(tokenize(left_normalized))), len(set(tokenize(right_normalized))))),
    )
    return round((0.52 * dense) + (0.32 * sparse) + (0.16 * containment), 6)


def merge_text_fragments(parts: list[str], *, max_sentences: int = 6, max_chars: int = 420) -> str:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    return build_summary(cleaned, max_sentences=max_sentences, max_chars=max_chars)
