from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any

from aimemory.core.utils import ensure_dir
from aimemory.memory_intelligence.policies import MemoryPolicy


@dataclass(slots=True)
class ProviderLiteConfig:
    provider: str = "openai"
    model: str | None = None
    api_base: str | None = None
    api_key_env: str | None = None
    organization: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def as_litellm_kwargs(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "provider": self.provider,
            "model": self.model,
            "api_base": self.api_base,
            "api_key_env": self.api_key_env,
            "organization": self.organization,
            "headers": dict(self.headers),
            **dict(self.extra),
        }
        return {key: value for key, value in payload.items() if value not in (None, {}, "")}


@dataclass(slots=True)
class EmbeddingLiteConfig:
    provider: str = "sentence-transformers"
    model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    dimensions: int = 384
    device: str | None = None
    normalize: bool = True
    batch_size: int = 32
    cache_size: int = 4096
    cache_dir: str | None = None
    local_files_only: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def as_provider_kwargs(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "provider": self.provider,
            "model": self.model,
            "dimensions": self.dimensions,
            "device": self.device,
            "normalize": self.normalize,
            "batch_size": self.batch_size,
            "cache_size": self.cache_size,
            "cache_dir": self.cache_dir,
            "local_files_only": self.local_files_only,
            **dict(self.extra),
        }
        return {key: value for key, value in payload.items() if value not in (None, {}, "")}


@dataclass(slots=True)
class AIMemoryConfig:
    root_dir: str | Path = ".aimemory"
    sqlite_path: str | Path | None = None
    object_store_path: str | Path | None = None
    relational_backend: str = "sqlite"
    default_user_id: str = "default"
    platform_id: str | None = None
    workspace_id: str | None = None
    team_id: str | None = None
    project_id: str | None = None
    auto_project: bool = True
    session_ttl_seconds: int = 60 * 60 * 24
    projection_batch_size: int = 100
    index_backend: str = "sqlite"
    graph_backend: str = "sqlite"
    enable_lancedb: bool = False
    enable_faiss: bool = False
    enable_kuzu: bool = False
    lancedb_path: str | Path | None = None
    faiss_path: str | Path | None = None
    kuzu_path: str | Path | None = None
    intelligence_enabled: bool = True
    providers: ProviderLiteConfig = field(default_factory=ProviderLiteConfig)
    embeddings: EmbeddingLiteConfig = field(default_factory=EmbeddingLiteConfig)
    memory_policy: MemoryPolicy = field(default_factory=MemoryPolicy)
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_value(cls, value: "AIMemoryConfig | dict[str, Any] | None") -> "AIMemoryConfig":
        if value is None:
            return cls().resolved()
        if isinstance(value, cls):
            return value.resolved()
        if isinstance(value, dict):
            payload = dict(value)
            if "providers" in payload and isinstance(payload["providers"], dict):
                provider_keys = {item.name for item in fields(ProviderLiteConfig)}
                filtered = {key: item for key, item in payload["providers"].items() if key in provider_keys}
                payload["providers"] = ProviderLiteConfig(**filtered)
            if "embeddings" in payload and isinstance(payload["embeddings"], dict):
                embedding_keys = {item.name for item in fields(EmbeddingLiteConfig)}
                filtered = {key: item for key, item in payload["embeddings"].items() if key in embedding_keys}
                payload["embeddings"] = EmbeddingLiteConfig(**filtered)
            if "memory_policy" in payload and isinstance(payload["memory_policy"], dict):
                policy_keys = {item.name for item in fields(MemoryPolicy)}
                filtered = {key: item for key, item in payload["memory_policy"].items() if key in policy_keys}
                payload["memory_policy"] = MemoryPolicy(**filtered)
            config_keys = {item.name for item in fields(cls)}
            filtered_payload = {key: item for key, item in payload.items() if key in config_keys}
            return cls(**filtered_payload).resolved()
        raise TypeError("config must be AIMemoryConfig, dict, or None")

    def resolved(self) -> "AIMemoryConfig":
        root_dir = ensure_dir(self.root_dir)
        sqlite_path = Path(self.sqlite_path) if self.sqlite_path else root_dir / "data" / "aimemory.db"
        object_store_path = Path(self.object_store_path) if self.object_store_path else root_dir / "objects"
        lancedb_path = Path(self.lancedb_path) if self.lancedb_path else root_dir / "lancedb"
        faiss_path = Path(self.faiss_path) if self.faiss_path else root_dir / "faiss"
        kuzu_path = Path(self.kuzu_path) if self.kuzu_path else root_dir / "kuzu"

        ensure_dir(sqlite_path.parent)
        ensure_dir(object_store_path)
        ensure_dir(lancedb_path)
        ensure_dir(faiss_path)
        if self.kuzu_path and Path(self.kuzu_path).suffix:
            ensure_dir(kuzu_path.parent)
        else:
            ensure_dir(kuzu_path)

        return replace(
            self,
            root_dir=root_dir,
            sqlite_path=sqlite_path.resolve(),
            object_store_path=object_store_path.resolve(),
            lancedb_path=lancedb_path.resolve(),
            faiss_path=faiss_path.resolve(),
            kuzu_path=kuzu_path.resolve(),
        )
