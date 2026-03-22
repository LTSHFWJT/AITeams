from __future__ import annotations

from typing import Any, Protocol

from aimemory import AIMemory, MemoryConfig, Scope


class MemoryAdapter(Protocol):
    async def load_working(self, scope: Scope) -> list[dict[str, Any]]: ...
    async def append_working(self, scope: Scope, role: str, content: str, metadata: dict[str, Any] | None = None) -> None: ...
    async def recall(self, scope: Scope, query: str, top_k: int = 8, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...
    async def remember(self, scope: Scope, records: list[dict[str, Any]]) -> list[dict[str, Any]]: ...
    async def feedback(self, scope: Scope, head_id: str, text: str) -> dict[str, Any]: ...


class AIMemoryAdapter:
    def __init__(self, root_dir: str):
        self._memory = AIMemory(MemoryConfig(root_dir=root_dir, worker_mode="library_only", vector_dim=32))

    async def load_working(self, scope: Scope) -> list[dict[str, Any]]:
        return self._memory.working_snapshot(scope=scope, limit=32)

    async def append_working(self, scope: Scope, role: str, content: str, metadata: dict[str, Any] | None = None) -> None:
        self._memory.working_append(scope=scope, role=role, content=content, metadata=metadata or {})

    async def recall(self, scope: Scope, query: str, top_k: int = 8, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return self._memory.search(scope=scope, query=query, top_k=top_k, filters=filters or {})

    async def remember(self, scope: Scope, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self._memory.ingest_records(scope=scope, records=records)

    async def feedback(self, scope: Scope, head_id: str, text: str) -> dict[str, Any]:
        return self._memory.feedback(scope=scope, head_id=head_id, text=text)

    def close(self) -> None:
        self._memory.close()
