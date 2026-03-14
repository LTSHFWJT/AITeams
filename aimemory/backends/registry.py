from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from aimemory.backends.defaults import KuzuGraphBackend, NoopGraphBackend, SQLiteGraphBackend
from aimemory.core.capabilities import capability_dict
from aimemory.storage.faiss.index_store import FaissIndexStore
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


@dataclass(slots=True)
class BackendRegistry:
    vector_factories: dict[str, Callable[..., VectorIndex]]
    graph_factories: dict[str, Callable[..., GraphStore]]
    _bootstrapped: bool = False

    def register_vector(self, name: str, factory: Callable[..., VectorIndex]) -> None:
        self.vector_factories[name] = factory

    def register_graph(self, name: str, factory: Callable[..., GraphStore]) -> None:
        self.graph_factories[name] = factory

    def create_vector(self, name: str, **kwargs: Any) -> VectorIndex:
        self.bootstrap_defaults()
        if name not in self.vector_factories:
            raise ValueError(f"Unknown vector backend `{name}`")
        return self.vector_factories[name](**kwargs)

    def create_graph(self, name: str, **kwargs: Any) -> GraphStore:
        self.bootstrap_defaults()
        if name not in self.graph_factories:
            raise ValueError(f"Unknown graph backend `{name}`")
        return self.graph_factories[name](**kwargs)

    def bootstrap_defaults(self) -> None:
        if self._bootstrapped:
            return
        self.register_vector("sqlite", lambda db, **_: SQLiteVectorIndex(db))
        self.register_vector("lancedb", lambda config, **_: LanceDBVectorIndex(config))
        self.register_vector("faiss", lambda config, **_: FaissVectorIndex(config))
        self.register_vector("none", lambda **_: NoopVectorIndex())
        self.register_graph("sqlite", lambda db, config=None, **_: SQLiteGraphBackend(db=db, config=config))
        self.register_graph("kuzu", lambda db, config=None, **_: KuzuGraphBackend(db=db, config=config))
        self.register_graph("none", lambda **_: NoopGraphBackend())
        self._bootstrapped = True


class NoopVectorIndex:
    name = "none"
    available = False

    def upsert(self, collection: str, record_id: str, text: str, payload: dict[str, Any] | None = None) -> bool:
        return False

    def delete(self, collection: str, record_id: str) -> bool:
        return False

    def search(self, collection: str, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        return []

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="vector_index",
            provider=self.name,
            features={"semantic_search": False, "persistent_index": False},
            notes=["vector backend disabled"],
        )


class SQLiteVectorIndex:
    name = "sqlite"
    available = True

    def __init__(self, db):
        self.db = db

    def upsert(self, collection: str, record_id: str, text: str, payload: dict[str, Any] | None = None) -> bool:
        payload = dict(payload or {})
        self.db.execute(
            """
            INSERT INTO semantic_index_cache(record_id, domain, collection, text, embedding, fingerprint, quality, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                domain = excluded.domain,
                collection = excluded.collection,
                text = excluded.text,
                embedding = excluded.embedding,
                fingerprint = excluded.fingerprint,
                quality = excluded.quality,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                record_id,
                str(payload.get("domain") or _domain_from_collection(collection)),
                collection,
                text or "",
                str(payload.get("embedding") or "[]"),
                str(payload.get("fingerprint") or ""),
                float(payload.get("quality", 0.0) or 0.0),
                str(payload.get("updated_at") or ""),
                str(payload.get("metadata") or "{}"),
            ),
        )
        return True

    def delete(self, collection: str, record_id: str) -> bool:
        self.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (record_id,))
        return True

    def search(self, collection: str, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        from aimemory.algorithms.retrieval import score_record

        rows = self.db.fetch_all(
            """
            SELECT record_id, text, embedding, updated_at, metadata
            FROM semantic_index_cache
            WHERE collection = ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (collection, max(limit * 12, 60)),
        )
        ranked: list[dict[str, Any]] = []
        for row in rows:
            score, _breakdown = score_record(
                query,
                text=row.get("text", ""),
                embedding=row.get("embedding"),
                updated_at=row.get("updated_at"),
            )
            ranked.append(
                {
                    "id": row["record_id"],
                    "record_id": row["record_id"],
                    "text": row.get("text", ""),
                    "_distance": round(max(0.0, 1.0 - score), 6),
                    "metadata": row.get("metadata", "{}"),
                }
            )
        ranked.sort(key=lambda item: item.get("_distance", 1.0))
        return ranked[:limit]

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="vector_index",
            provider=self.name,
            features={"semantic_search": True, "persistent_index": True, "extra_service": False},
            notes=["uses sqlite semantic cache as local vector fallback"],
        )


class LanceDBVectorIndex:
    name = "lancedb"

    def __init__(self, config):
        self.store = LanceIndexStore(config.lancedb_path)
        self.available = bool(self.store.available)

    def upsert(self, collection: str, record_id: str, text: str, payload: dict[str, Any] | None = None) -> bool:
        return bool(self.store.upsert(collection, record_id, text, payload))

    def delete(self, collection: str, record_id: str) -> bool:
        return bool(self.store.delete(collection, record_id))

    def search(self, collection: str, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        return self.store.search(collection, query, limit=limit) if self.available else []

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="vector_index",
            provider=self.name,
            active_provider=self.name if self.available else "sqlite",
            features={"semantic_search": True, "persistent_index": True, "extra_service": False},
            notes=["falls back to sqlite cache when LanceDB is unavailable"] if not self.available else [],
        )


class FaissVectorIndex:
    name = "faiss"

    def __init__(self, config):
        self.store = FaissIndexStore(config.faiss_path, dims=int(config.embeddings.dimensions))
        self.available = bool(self.store.available)

    def upsert(self, collection: str, record_id: str, text: str, payload: dict[str, Any] | None = None) -> bool:
        return bool(self.store.upsert(collection, record_id, text, payload))

    def delete(self, collection: str, record_id: str) -> bool:
        return bool(self.store.delete(collection, record_id))

    def search(self, collection: str, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        return self.store.search(collection, query, limit=limit) if self.available else []

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="vector_index",
            provider=self.name,
            active_provider=self.name if self.available else "sqlite",
            features={"semantic_search": True, "persistent_index": True, "extra_service": False},
            notes=["falls back to sqlite cache when FAISS is unavailable"] if not self.available else [],
        )


def _domain_from_collection(collection: str) -> str:
    if collection == "knowledge_chunk_index":
        return "knowledge"
    if collection == "skill_index":
        return "skill"
    if collection == "archive_summary_index":
        return "archive"
    return "memory"


BACKEND_REGISTRY = BackendRegistry(vector_factories={}, graph_factories={})
