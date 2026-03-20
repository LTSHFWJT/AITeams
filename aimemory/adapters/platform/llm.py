from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class PlatformLLMAdapter(Protocol):
    provider: str | None
    model: str | None

    def compress(
        self,
        *,
        task_type: str,
        records: list[dict[str, Any]],
        budget_chars: int,
        scope: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> Any | dict[str, Any] | str: ...
