from __future__ import annotations

from typing import Any, Protocol

from aimemory.scope import Scope
from aimemory.types import MemoryDraft


class Extractor(Protocol):
    def extract(self, messages: list[dict[str, Any]], scope: Scope) -> list[MemoryDraft]: ...


class Reranker(Protocol):
    def rerank(self, query: str, docs: list[str], top_k: int) -> list[tuple[int, float]]: ...


class RetrievalGate(Protocol):
    def should_retrieve(self, query: str, scope: Scope) -> bool: ...
