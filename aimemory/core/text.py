from __future__ import annotations

import math
import re
from collections import Counter
from typing import Iterable


WORD_PATTERN = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)
SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[。！？!?\.])\s+|\n+")


def normalize_text(text: str | None) -> str:
    if not text:
        return ""
    lowered = text.lower().strip()
    return re.sub(r"\s+", " ", lowered)


def _ngrams(token: str, min_n: int = 2, max_n: int = 3) -> list[str]:
    if len(token) < min_n:
        return [token]
    grams: list[str] = []
    for n in range(min_n, min(max_n, len(token)) + 1):
        for index in range(0, len(token) - n + 1):
            grams.append(token[index : index + n])
    return grams or [token]


def tokenize(text: str | None) -> list[str]:
    normalized = normalize_text(text)
    if not normalized:
        return []
    tokens: list[str] = []
    for raw in WORD_PATTERN.findall(normalized):
        if any("\u4e00" <= char <= "\u9fff" for char in raw):
            tokens.extend(_ngrams(raw))
        else:
            tokens.append(raw)
            if len(raw) > 4:
                tokens.extend(_ngrams(raw, min_n=3, max_n=3))
    return tokens


def extract_keywords(text: str | None, limit: int = 12) -> list[str]:
    counts = Counter(tokenize(text))
    return [item for item, _count in counts.most_common(limit)]


def split_sentences(text: str | None) -> list[str]:
    if not text:
        return []
    parts = [part.strip() for part in SENTENCE_SPLIT_PATTERN.split(text) if part.strip()]
    return parts or [text.strip()]


def build_summary(parts: Iterable[str], max_sentences: int = 4, max_chars: int = 320) -> str:
    selected: list[str] = []
    total = 0
    for part in parts:
        cleaned = part.strip()
        if not cleaned or cleaned in selected:
            continue
        selected.append(cleaned)
        total += len(cleaned)
        if len(selected) >= max_sentences or total >= max_chars:
            break
    return " ".join(selected)[:max_chars]


def chunk_text(text: str, chunk_size: int = 500, overlap: int = 80) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return []
    if len(normalized) <= chunk_size:
        return [normalized]
    chunks: list[str] = []
    start = 0
    while start < len(normalized):
        end = min(len(normalized), start + chunk_size)
        chunks.append(normalized[start:end])
        if end == len(normalized):
            break
        start = max(0, end - overlap)
    return chunks


def lexical_hash_embedding(text: str | None, dims: int = 128) -> list[float]:
    import hashlib

    vector = [0.0] * dims
    for token in tokenize(text):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=16).digest()
        index = int.from_bytes(digest[:4], "big") % dims
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        weight = 1.0 + (digest[5] / 255.0)
        vector[index] += sign * weight
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [value / norm for value in vector]


def hash_embedding(text: str | None, dims: int | None = None) -> list[float]:
    try:
        from aimemory.providers.embeddings import embed_text

        vector = embed_text(text, dims=dims)
        if vector:
            return vector
    except Exception:
        pass
    return lexical_hash_embedding(text, dims=dims or 128)


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return float(sum(a * b for a, b in zip(left, right)))


def hybrid_score(query: str, text: str, keywords: str | list[str] | None = None, boost: float = 0.0) -> float:
    query_tokens = set(tokenize(query))
    text_tokens = set(tokenize(text))
    if isinstance(keywords, str):
        keyword_tokens = set(tokenize(keywords))
    else:
        keyword_tokens = set(keywords or [])

    overlap = len(query_tokens & text_tokens)
    keyword_overlap = len(query_tokens & keyword_tokens)
    denominator = max(1, len(query_tokens))
    exact_bonus = 0.2 if normalize_text(query) and normalize_text(query) in normalize_text(text) else 0.0
    embedding_bonus = max(0.0, cosine_similarity(hash_embedding(query), hash_embedding(text)))
    return round((0.45 * (overlap / denominator)) + (0.15 * (keyword_overlap / denominator)) + (0.3 * embedding_bonus) + exact_bonus + boost, 6)
