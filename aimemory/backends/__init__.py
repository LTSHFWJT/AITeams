from aimemory.backends.base import GraphBackend, IndexBackend
from aimemory.backends.defaults import KuzuGraphBackend, LanceDBIndexBackend, NoopGraphBackend, SQLiteGraphBackend, SQLiteIndexBackend
from aimemory.backends.registry import BACKEND_REGISTRY, BackendRegistry, GraphStore, VectorIndex


def register_vector_backend(name, factory) -> None:
    BACKEND_REGISTRY.register_vector(name, factory)


def register_graph_backend(name, factory) -> None:
    BACKEND_REGISTRY.register_graph(name, factory)


__all__ = [
    "BACKEND_REGISTRY",
    "BackendRegistry",
    "register_vector_backend",
    "register_graph_backend",
    "IndexBackend",
    "GraphBackend",
    "VectorIndex",
    "GraphStore",
    "SQLiteIndexBackend",
    "LanceDBIndexBackend",
    "SQLiteGraphBackend",
    "KuzuGraphBackend",
    "NoopGraphBackend",
]
