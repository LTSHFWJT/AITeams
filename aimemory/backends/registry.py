from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from aimemory.core.capabilities import capability_dict
from aimemory.storage.lancedb.index_store import LanceIndexStore


class VectorIndex(Protocol):
    name: str
    available: bool

    def upsert(self, collection: str, record_id: str, text: str, payload: dict[str, Any] | None = None) -> bool: ...

    def delete(self, collection: str, record_id: str) -> bool: ...

    def search(self, collection: str, query: str, *, limit: int = 10) -> list[dict[str, Any]]: ...

    def describe_capabilities(self) -> dict[str, Any]: ...


class GraphStore(Protocol):
    backend_name: str
    active_backend: str
    available: bool

    def upsert_node(self, node_type: str, ref_id: str, label: str, metadata: dict[str, Any] | None = None) -> str: ...

    def upsert_edge(
        self,
        source_type: str,
        source_ref_id: str,
        edge_type: str,
        target_type: str,
        target_ref_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> str: ...

    def delete_reference(self, ref_id: str) -> None: ...

    def relations_for_ref(self, ref_id: str, *, limit: int = 12) -> list[dict[str, Any]]: ...

    def describe_capabilities(self) -> dict[str, Any]: ...


class LanceDBVectorIndex:
    name = "lancedb"
    MEMORY_COLLECTIONS = {"memory_index", "archive_summary_index"}
    COMPETENCY_COLLECTIONS = {"knowledge_chunk_index", "skill_index", "skill_reference_index"}

    def __init__(self, config):
        base_path = Path(config.lancedb_path).expanduser().resolve()
        self.memory_path = base_path / "memory" / "lancedb"
        self.competency_path = base_path / "competency" / "lancedb"
        self.memory_store = LanceIndexStore(self.memory_path)
        self.competency_store = LanceIndexStore(self.competency_path)
        self.available = True

    def upsert(self, collection: str, record_id: str, text: str, payload: dict[str, Any] | None = None) -> bool:
        return bool(self._store_for_collection(collection).upsert(collection, record_id, text, payload))

    def delete(self, collection: str, record_id: str) -> bool:
        return bool(self._store_for_collection(collection).delete(collection, record_id))

    def search(self, collection: str, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        return self._store_for_collection(collection).search(collection, query, limit=limit)

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="vector_index",
            provider=self.name,
            active_provider=self.name,
            features={"semantic_search": True, "persistent_index": True, "extra_service": False},
        )

    def _store_for_collection(self, collection: str) -> LanceIndexStore:
        if collection in self.MEMORY_COLLECTIONS:
            return self.memory_store
        if collection in self.COMPETENCY_COLLECTIONS:
            return self.competency_store
        raise ValueError(f"Unknown vector collection `{collection}`")


class NullGraphStore:
    backend_name = "disabled"
    active_backend = "disabled"
    available = False

    def upsert_node(
        self,
        node_type: str,
        ref_id: str,
        label: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        return f"{node_type}:{ref_id}"

    def upsert_edge(
        self,
        source_type: str,
        source_ref_id: str,
        edge_type: str,
        target_type: str,
        target_ref_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        return f"{source_type}:{source_ref_id}:{edge_type}:{target_type}:{target_ref_id}"

    def delete_reference(self, ref_id: str) -> None:
        return None

    def relations_for_ref(self, ref_id: str, *, limit: int = 12) -> list[dict[str, Any]]:
        return []

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="graph_store",
            provider=self.backend_name,
            active_provider=self.active_backend,
            features={
                "relations": False,
                "persistent_graph": False,
                "native_graph_queries": False,
            },
            notes=["graph storage disabled"],
        )
