from aimemory.core.facade import AIMemory, AsyncAIMemory
from aimemory.core.scope import CollaborationScope
from aimemory.core.scoped import ScopedAIMemory
from aimemory.core.settings import AIMemoryConfig, EmbeddingLiteConfig, ProviderLiteConfig
from aimemory.mcp.adapter import AIMemoryMCPAdapter

__all__ = [
    "AIMemory",
    "AIMemoryConfig",
    "AIMemoryMCPAdapter",
    "AsyncAIMemory",
    "CollaborationScope",
    "ScopedAIMemory",
    "EmbeddingLiteConfig",
    "ProviderLiteConfig",
]
__version__ = "0.4.0"
