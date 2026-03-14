from aimemory.core.facade import AIMemory, AsyncAIMemory
from aimemory.core.scope import CollaborationScope
from aimemory.core.scoped import ScopedAIMemory
from aimemory.core.settings import AIMemoryConfig, EmbeddingLiteConfig, ProviderLiteConfig
from aimemory.mcp.adapter import AIMemoryMCPAdapter
from aimemory.storage.plugins import register_graph_backend, register_relational_backend, register_vector_backend

__all__ = [
    "AIMemory",
    "AIMemoryConfig",
    "AIMemoryMCPAdapter",
    "AsyncAIMemory",
    "CollaborationScope",
    "ScopedAIMemory",
    "EmbeddingLiteConfig",
    "ProviderLiteConfig",
    "register_relational_backend",
    "register_vector_backend",
    "register_graph_backend",
]
__version__ = "0.4.0"
