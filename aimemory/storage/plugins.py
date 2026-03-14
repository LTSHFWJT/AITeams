from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from aimemory.backends.registry import BACKEND_REGISTRY
from aimemory.storage.sqlite.database import SQLiteDatabase


class RelationalStore(Protocol):
    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any: ...

    def executemany(self, sql: str, items: list[tuple[Any, ...]]) -> None: ...

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None: ...

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]: ...

    def ensure_schema(self, statements: list[str]) -> None: ...

    def ensure_columns(self, table_name: str, columns: dict[str, str]) -> None: ...

    def close(self) -> None: ...


RelationalFactory = Callable[[Any], RelationalStore]


@dataclass(slots=True)
class StoragePluginRegistry:
    relational_factories: dict[str, RelationalFactory] = field(default_factory=dict)
    _bootstrapped: bool = False

    def bootstrap_defaults(self) -> None:
        if self._bootstrapped:
            return
        self.register_relational("sqlite", lambda config: SQLiteDatabase(config.sqlite_path))
        self._bootstrapped = True

    def register_relational(self, name: str, factory: RelationalFactory) -> None:
        self.relational_factories[str(name)] = factory

    def create_relational(self, name: str, config: Any) -> RelationalStore:
        self.bootstrap_defaults()
        backend_name = str(name or "sqlite")
        if backend_name not in self.relational_factories:
            raise ValueError(f"Unknown relational backend `{backend_name}`")
        return self.relational_factories[backend_name](config)


STORAGE_PLUGIN_REGISTRY = StoragePluginRegistry()


def register_relational_backend(name: str, factory: RelationalFactory) -> None:
    STORAGE_PLUGIN_REGISTRY.register_relational(name, factory)


def register_vector_backend(name: str, factory: Callable[..., Any]) -> None:
    BACKEND_REGISTRY.register_vector(name, factory)


def register_graph_backend(name: str, factory: Callable[..., Any]) -> None:
    BACKEND_REGISTRY.register_graph(name, factory)
