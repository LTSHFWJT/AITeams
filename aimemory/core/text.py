from __future__ import annotations

import math
import re
from collections import Counter
from typing import Iterable


WORD_PATTERN = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)
ENGLISH_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "if",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "via",
    "with",
}
CJK_STOPWORDS = {
    "一个",
    "一些",
    "一种",
    "以及",
    "但是",
    "其中",
    "然后",
    "需要",
    "可以",
    "如果",
    "因为",
    "所以",
    "这个",
    "那个",
}

SENTENCE_ENDINGS = {"。", "！", "？", "!", "?"}
SENTENCE_TRAILERS = "\"'”’)]}】）"


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


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in text)


def _keyword_noise(term: str) -> bool:
    cleaned = str(term or "").strip("_ ")
    if not cleaned:
        return True
    if cleaned in ENGLISH_STOPWORDS or cleaned in CJK_STOPWORDS:
        return True
    if cleaned.isdigit() and len(cleaned) <= 1:
        return True
    if len(cleaned) <= 1 and not any(char.isdigit() for char in cleaned):
        return True
    return False


def _keyword_length_weight(term: str) -> float:
    if _contains_cjk(term):
        return {
            2: 1.02,
            3: 1.1,
            4: 1.16,
        }.get(len(term), 1.08)
    base = 0.94 + min(0.42, len(term) / 18.0)
    if any(char.isdigit() for char in term):
        base += 0.08
    return base


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
    normalized = normalize_text(text)
    if not normalized:
        return []

    raw_terms = [item.strip("_ ") for item in WORD_PATTERN.findall(normalized)]
    candidates: list[tuple[str, int, bool]] = []
    for index, raw in enumerate(raw_terms):
        if not raw:
            continue
        if _contains_cjk(raw):
            max_n = 4 if len(raw) >= 4 else min(3, len(raw))
            for gram in _ngrams(raw, min_n=2, max_n=max_n):
                if not _keyword_noise(gram):
                    candidates.append((gram, index, False))
            if len(raw) <= 6 and not _keyword_noise(raw):
                candidates.append((raw, index, True))
        else:
            for segment in raw.split("_"):
                cleaned = segment.strip()
                if not _keyword_noise(cleaned):
                    candidates.append((cleaned, index, True))

    if not candidates:
        return []

    counts: Counter[str] = Counter()
    first_seen: dict[str, int] = {}
    boundary_hits: Counter[str] = Counter()
    for term, index, is_boundary in candidates:
        counts[term] += 1
        first_seen.setdefault(term, index)
        if is_boundary:
            boundary_hits[term] += 1

    span = max(1, len(raw_terms) - 1)
    scored: list[tuple[str, float]] = []
    for term, count in counts.items():
        position = first_seen.get(term, span)
        position_bonus = 0.22 * (1.0 - (position / span))
        frequency_bonus = 0.18 * min(1.0, math.log1p(count))
        boundary_bonus = 0.14 if boundary_hits.get(term, 0) else 0.0
        digit_bonus = 0.06 if any(char.isdigit() for char in term) else 0.0
        score = _keyword_length_weight(term) + position_bonus + frequency_bonus + boundary_bonus + digit_bonus
        scored.append((term, round(score, 6)))

    scored.sort(key=lambda item: (item[1], len(item[0]), counts[item[0]]), reverse=True)
    selected: list[tuple[str, float]] = []
    for term, score in scored:
        if any(term != existing and term in existing and score <= (existing_score + 0.08) for existing, existing_score in selected):
            continue
        selected.append((term, score))
        if len(selected) >= limit:
            break
    return [term for term, _score in selected]


def split_sentences(text: str | None) -> list[str]:
    if not text:
        return []
    source = str(text).replace("\r\n", "\n").strip()
    if not source:
        return []

    parts: list[str] = []
    start = 0
    index = 0
    length = len(source)

    while index < length:
        char = source[index]
        if char == "\n":
            segment = source[start:index].strip()
            if segment:
                parts.append(segment)
            while index < length and source[index] == "\n":
                index += 1
            start = index
            continue

        if _is_sentence_boundary(source, index):
            end = index + 1
            while end < length and source[end] in SENTENCE_TRAILERS:
                end += 1
            segment = source[start:end].strip()
            if segment:
                parts.append(segment)
            while end < length and source[end].isspace() and source[end] != "\n":
                end += 1
            start = end
            index = end
            continue
        index += 1

    tail = source[start:].strip()
    if tail:
        parts.append(tail)
    return parts or [source]


def _is_sentence_boundary(source: str, index: int) -> bool:
    char = source[index]
    if char in SENTENCE_ENDINGS:
        return True
    if char != ".":
        return False

    previous = source[index - 1] if index > 0 else ""
    following = source[index + 1] if index + 1 < len(source) else ""
    if previous.isdigit() and following.isdigit():
        return False

    next_non_space = _next_non_whitespace_char(source, index + 1)
    if not next_non_space:
        return True
    if next_non_space in SENTENCE_TRAILERS:
        return True
    if next_non_space.isspace():
        return True
    if next_non_space.isupper():
        return True
    if "\u4e00" <= next_non_space <= "\u9fff":
        return True
    return False


def _next_non_whitespace_char(source: str, start: int) -> str:
    for char in source[start:]:
        if not char.isspace():
            return char
    return ""


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
    from aimemory.algorithms.segmentation import chunk_text_units

    return [chunk.text for chunk in chunk_text_units(text, chunk_size=chunk_size, overlap=overlap)]


def character_ngrams(text: str | None, min_n: int = 2, max_n: int = 4) -> list[str]:
    normalized = normalize_text(text)
    if not normalized:
        return []
    compact = "".join(char for char in normalized if not char.isspace())
    if not compact:
        return []
    if len(compact) < min_n:
        return [compact]
    grams: list[str] = []
    upper = min(max_n, len(compact))
    for size in range(min_n, upper + 1):
        for index in range(0, len(compact) - size + 1):
            grams.append(compact[index : index + size])
    return grams or [compact]


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
