from __future__ import annotations

import hashlib
import math
import re
import time


VERSIONED_KINDS = {"profile", "preference", "entity"}
SKIP_PATTERNS = [
    re.compile(r"^(hi|hello|hey|thanks|ok|okay|yes|no)\b", re.IGNORECASE),
    re.compile(r"^/"),
    re.compile(r"^(run|build|test|git|pip|npm|docker)\b", re.IGNORECASE),
]
FORCE_PATTERNS = [
    re.compile(r"\b(remember|memory|previous|earlier|history|preference|before)\b", re.IGNORECASE),
    re.compile(r"(\u4e0a\u6b21|\u4e4b\u524d|\u8bb0\u5f97|\u504f\u597d|\u5386\u53f2)"),
]


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_text(text: str) -> str:
    return " ".join(text.strip().split())


def summarize_text(text: str, *, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def compute_checksum(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def compute_fingerprint(scope_key: str, kind: str, checksum: str) -> str:
    return hashlib.sha1(f"{scope_key}|{kind}|{checksum}".encode("utf-8")).hexdigest()


def derive_fact_key(kind: str, text: str) -> str | None:
    if kind not in VERSIONED_KINDS:
        return None
    normalized = normalize_text(summarize_text(text.lower(), limit=96))
    return hashlib.sha1(f"{kind}:{normalized}".encode("utf-8")).hexdigest()


def split_text(text: str, *, chunk_size: int, chunk_overlap: int) -> list[dict[str, int | str]]:
    if len(text) <= chunk_size:
        return [{"chunk_no": 0, "text": text, "char_start": 0, "char_end": len(text), "token_count": len(text.split())}]
    chunks: list[dict[str, int | str]] = []
    start = 0
    chunk_no = 0
    while start < len(text):
        end = min(len(text), start + chunk_size)
        segment = text[start:end]
        chunks.append(
            {
                "chunk_no": chunk_no,
                "text": segment,
                "char_start": start,
                "char_end": end,
                "token_count": len(segment.split()),
            }
        )
        if end >= len(text):
            break
        start = max(0, end - chunk_overlap)
        chunk_no += 1
    return chunks


def should_skip_vector_search(query: str) -> bool:
    normalized = normalize_text(query)
    if any(pattern.search(normalized) for pattern in FORCE_PATTERNS):
        return False
    return len(normalized) < 5 or any(pattern.search(normalized) for pattern in SKIP_PATTERNS)


def lexical_score(rank: float, text: str, query: str) -> float:
    base = 1.0 / (1.0 + abs(rank))
    if query.lower() in text.lower():
        base = min(1.0, base + 0.15)
    return base


def vector_score(distance: float) -> float:
    return 1.0 / (1.0 + max(distance, 0.0))


def freshness_multiplier(updated_at: int, *, now: int) -> float:
    age_days = max(0.0, (now - updated_at) / 86_400_000)
    return 0.55 + 0.45 * math.exp(-age_days / 30.0)


def tier_multiplier(tier: str) -> float:
    return {"core": 1.08, "active": 1.0, "cold": 0.92}.get(tier, 1.0)


def access_bonus(access_count: int) -> float:
    return min(0.05, math.log1p(max(access_count, 0)) * 0.01)


def clamp_unit(value: float) -> float:
    return max(0.0, min(1.0, value))


def frequency_score(access_count: int) -> float:
    if access_count <= 0:
        return 0.0
    return min(1.0, math.log1p(access_count) / math.log1p(32))


def intrinsic_value(importance: float, confidence: float) -> float:
    return clamp_unit(importance) * clamp_unit(confidence)


def freshness_score(reference_at: int | None, *, now: int, window_ms: int) -> float:
    if not reference_at or reference_at <= 0:
        return 0.0
    age_ms = max(0, now - reference_at)
    return math.exp(-age_ms / max(window_ms, 1))


def lifecycle_score(
    *,
    importance: float,
    confidence: float,
    access_count: int,
    updated_at: int,
    last_accessed_at: int | None,
    now: int,
    freshness_window_ms: int,
) -> float:
    reference_at = max(updated_at, last_accessed_at or 0)
    freshness = freshness_score(reference_at, now=now, window_ms=freshness_window_ms)
    frequency = frequency_score(access_count)
    intrinsic = intrinsic_value(importance, confidence)
    return (0.40 * freshness) + (0.30 * frequency) + (0.30 * intrinsic)


def suggest_tier(
    current_tier: str,
    *,
    importance: float,
    confidence: float,
    access_count: int,
    updated_at: int,
    last_accessed_at: int | None,
    now: int,
    freshness_window_ms: int,
    cold_after_ms: int,
    core_promote_importance: float,
    core_promote_access_count: int,
    core_promote_score: float,
    core_demote_score: float,
    cold_demote_score: float,
    cold_reactivate_score: float,
) -> str:
    composite = lifecycle_score(
        importance=importance,
        confidence=confidence,
        access_count=access_count,
        updated_at=updated_at,
        last_accessed_at=last_accessed_at,
        now=now,
        freshness_window_ms=freshness_window_ms,
    )
    reference_at = max(updated_at, last_accessed_at or 0)
    stale = reference_at > 0 and (now - reference_at) >= cold_after_ms
    promote_to_core = (
        importance >= core_promote_importance
        and access_count >= core_promote_access_count
        and composite >= core_promote_score
    )

    if current_tier == "core":
        if not promote_to_core and composite < core_demote_score:
            return "active"
        return "core"
    if current_tier == "cold":
        if promote_to_core:
            return "core"
        if composite >= cold_reactivate_score or access_count >= max(1, core_promote_access_count // 2):
            return "active"
        return "cold"
    if promote_to_core:
        return "core"
    if composite < cold_demote_score or stale:
        return "cold"
    return "active"
