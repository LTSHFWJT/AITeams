from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypedDict


class MemoryDraft(TypedDict, total=False):
    text: str
    kind: str
    layer: str
    tier: str
    importance: float
    confidence: float
    vector: list[float]
    fact_key: str
    metadata: dict[str, Any]
    source_type: str
    source_ref: str


@dataclass(slots=True)
class MemoryRecord:
    head_id: str
    version_id: str
    scope_key: str
    kind: str
    layer: str
    tier: str
    state: str
    text: str
    abstract: str
    overview: str
    fact_key: str | None
    importance: float
    confidence: float
    access_count: int
    created_at: int
    updated_at: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SearchHit:
    head_id: str
    version_id: str
    chunk_id: str
    kind: str
    layer: str
    tier: str
    text: str
    abstract: str
    overview: str
    score: float
    lexical_score: float
    vector_score: float
    access_count: int
    valid_from: int
    valid_to: int | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SearchQuery:
    query: str
    top_k: int = 10
    filters: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SearchResult:
    query: str
    hits: list[SearchHit] = field(default_factory=list)
    used_working_memory: bool = False
    used_longterm_memory: bool = False


@dataclass(slots=True)
class HistoryEntry:
    event_type: str
    created_at: int
    payload: dict[str, Any] = field(default_factory=dict)
