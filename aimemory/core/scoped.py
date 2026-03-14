from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aimemory.core.scope import CollaborationScope

if TYPE_CHECKING:
    from aimemory.core.facade import AIMemory


class ScopedAIMemory:
    def __init__(self, memory: "AIMemory", scope: CollaborationScope | dict[str, Any] | None = None):
        self.memory = memory
        self.scope = CollaborationScope.from_value(scope)
        self._structured_api = None

    def using(self, **scope_overrides: Any) -> "ScopedAIMemory":
        return ScopedAIMemory(self.memory, self.scope.merge(scope_overrides))

    def scope_dict(self) -> dict[str, str]:
        return self.scope.as_metadata()

    @property
    def api(self):
        if self._structured_api is None:
            from aimemory.core.structured_api import StructuredAIMemoryAPI

            self._structured_api = StructuredAIMemoryAPI(self.memory, scope=self.scope.as_metadata())
        return self._structured_api

    def storage_layout(self) -> dict[str, Any]:
        return self.memory.storage_layout(**self.scope.as_metadata())

    def create_mcp_adapter(self):
        return self.memory.create_mcp_adapter(scope=self.scope.as_metadata())

    def __dir__(self):
        names = set(super().__dir__())
        names.update({"api", "create_mcp_adapter", "scope_dict", "storage_layout", "using"})
        return sorted(names)
