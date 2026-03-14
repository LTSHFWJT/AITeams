from __future__ import annotations

from typing import Any, Protocol


class MessageNormalizer(Protocol):
    def normalize(self, messages: Any) -> list[Any]: ...


class LLMProvider(Protocol):
    def generate(self, messages: list[dict[str, Any]], *, response_format: dict[str, Any] | None = None) -> Any: ...


class EmbedderProvider(Protocol):
    def embed(self, text: str, purpose: str = "search") -> list[float]: ...

    def embed_many(self, texts: list[str], purpose: str = "search") -> list[list[float]]: ...


class VisionProcessor(MessageNormalizer, Protocol): ...


class FactExtractor(Protocol):
    def extract(self, messages: list[Any], *, context: Any, policy: Any, memory_type: str | None = None) -> list[Any]: ...


class MemoryPlanner(Protocol):
    def plan(self, candidate: Any, neighbors: list[Any], *, context: Any, policy: Any) -> list[Any]: ...


class Reranker(Protocol):
    def rerank(self, query: str, records: list[dict[str, Any]], *, domain: str, context: Any, policy: Any | None = None) -> list[dict[str, Any]]: ...


class ContextCompressor(Protocol):
    def compress(self, records: list[dict[str, Any]], *, budget_chars: int) -> dict[str, Any]: ...


class MCPBridge(Protocol):
    def tool_specs(self) -> list[dict[str, Any]]: ...

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any: ...
