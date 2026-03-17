from __future__ import annotations

import hashlib
from collections import Counter

from aimemory.core.text import build_summary, character_ngrams, cosine_similarity, hash_embedding, normalize_text, tokenize


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


def _token_weight(token: str) -> float:
    cleaned = str(token or "").strip()
    if not cleaned:
        return 0.0
    if cleaned.isdigit():
        return 0.82
    return 1.0 + min(1.25, len(cleaned) / 6.0)


def _weighted_jaccard_similarity(left_tokens: list[str], right_tokens: list[str]) -> float:
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


def _containment_similarity(left_normalized: str, right_normalized: str, left_tokens: set[str], right_tokens: set[str]) -> float:
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens) / max(1, min(len(left_tokens), len(right_tokens)))
    if left_normalized in right_normalized or right_normalized in left_normalized:
        return max(overlap, 1.0)
    return overlap


def _char_ngram_similarity(left_normalized: str, right_normalized: str) -> float:
    left = set(character_ngrams(left_normalized[:320], min_n=2, max_n=4))
    right = set(character_ngrams(right_normalized[:320], min_n=2, max_n=4))
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def semantic_similarity(left_text: str | None, right_text: str | None) -> float:
    left_normalized = normalize_text(left_text)
    right_normalized = normalize_text(right_text)
    if not left_normalized or not right_normalized:
        return 0.0
    if left_normalized == right_normalized:
        return 1.0
    left_token_list = tokenize(left_normalized)
    right_token_list = tokenize(right_normalized)
    left_tokens = set(left_token_list)
    right_tokens = set(right_token_list)
    dense = max(0.0, cosine_similarity(hash_embedding(left_normalized), hash_embedding(right_normalized)))
    sparse = jaccard_similarity(left_tokens, right_tokens)
    weighted_sparse = _weighted_jaccard_similarity(left_token_list, right_token_list)
    containment = _containment_similarity(left_normalized, right_normalized, left_tokens, right_tokens)
    char_score = _char_ngram_similarity(left_normalized, right_normalized)
    simhash_score = hamming_similarity(fingerprint(left_normalized), fingerprint(right_normalized))
    return round(
        (0.34 * dense)
        + (0.18 * sparse)
        + (0.18 * weighted_sparse)
        + (0.14 * char_score)
        + (0.08 * containment)
        + (0.08 * simhash_score),
        6,
    )


def merge_text_fragments(parts: list[str], *, max_sentences: int = 6, max_chars: int = 420) -> str:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    return build_summary(cleaned, max_sentences=max_sentences, max_chars=max_chars)
