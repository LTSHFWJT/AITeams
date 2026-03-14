from __future__ import annotations

import math
from collections import OrderedDict
from dataclasses import asdict
from typing import Any

from aimemory.core.capabilities import capability_dict
from aimemory.core.settings import EmbeddingLiteConfig
from aimemory.core.text import lexical_hash_embedding, normalize_text


class HashFallbackEmbedder:
    provider_name = "hash"
    model_name = "lexical-hash"

    def __init__(self, *, dimensions: int = 384):
        self.available = True
        self.dimensions = int(dimensions)
        self.load_error: str | None = None

    def embed(self, text: str, purpose: str = "search", dims: int | None = None) -> list[float]:
        return lexical_hash_embedding(text, dims=dims or self.dimensions)

    def embed_many(self, texts: list[str], purpose: str = "search", dims: int | None = None) -> list[list[float]]:
        return [self.embed(text, purpose=purpose, dims=dims) for text in texts]


class SentenceTransformerEmbedder:
    provider_name = "sentence-transformers"

    def __init__(
        self,
        *,
        model: str,
        dimensions: int | None = None,
        device: str | None = None,
        normalize: bool = True,
        batch_size: int = 32,
        cache_dir: str | None = None,
        local_files_only: bool = False,
    ):
        self.model_name = model
        self.requested_dimensions = dimensions
        self.device = device
        self.normalize = normalize
        self.batch_size = batch_size
        self.cache_dir = cache_dir
        self.local_files_only = local_files_only
        self.available = False
        self.load_error: str | None = None
        self.dimensions = int(dimensions) if dimensions else 384
        self._model = None
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            kwargs: dict[str, Any] = {}
            if device:
                kwargs["device"] = device
            if cache_dir:
                kwargs["cache_folder"] = cache_dir
            kwargs["local_files_only"] = local_files_only
            self._model = SentenceTransformer(model, **kwargs)
            inferred = getattr(self._model, "get_sentence_embedding_dimension", lambda: None)()
            if inferred:
                self.dimensions = int(dimensions or inferred)
            self.available = True
        except Exception as exc:
            self.load_error = str(exc)

    def embed(self, text: str, purpose: str = "search", dims: int | None = None) -> list[float]:
        return self.embed_many([text], purpose=purpose, dims=dims)[0]

    def embed_many(self, texts: list[str], purpose: str = "search", dims: int | None = None) -> list[list[float]]:
        if not self.available or self._model is None:
            fallback = HashFallbackEmbedder(dimensions=dims or self.dimensions)
            return fallback.embed_many(texts, purpose=purpose, dims=dims)

        normalized = [normalize_text(text) for text in texts]
        vectors = self._model.encode(
            normalized,
            batch_size=self.batch_size,
            show_progress_bar=False,
            normalize_embeddings=self.normalize,
        )
        result: list[list[float]] = []
        target_dims = dims or self.requested_dimensions or self.dimensions
        for vector in vectors:
            current = vector.tolist() if hasattr(vector, "tolist") else list(vector)
            result.append(resize_vector(current, target_dims))
        return result


def resize_vector(vector: list[float], dims: int) -> list[float]:
    if dims <= 0:
        raise ValueError("dims must be positive")
    if len(vector) == dims:
        return normalize_vector(vector)
    if not vector:
        return [0.0] * dims
    if len(vector) > dims:
        folded = [0.0] * dims
        for index, value in enumerate(vector):
            folded[index % dims] += float(value)
        return normalize_vector(folded)
    padded = list(vector) + ([0.0] * (dims - len(vector)))
    return normalize_vector(padded)


def normalize_vector(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(float(value) * float(value) for value in vector))
    if norm == 0:
        return [float(value) for value in vector]
    return [float(value) / norm for value in vector]


class EmbeddingRuntime:
    def __init__(self):
        self.config: Any = None
        self.provider: Any = HashFallbackEmbedder()
        self.fallback = HashFallbackEmbedder()
        self._cache: OrderedDict[tuple[str, int], list[float]] = OrderedDict()
        self._cache_size = 4096

    def configure(self, config: Any) -> None:
        self.config = config
        self._cache_size = int(getattr(config, "cache_size", 4096) or 4096)
        requested_provider = str(getattr(config, "provider", "sentence-transformers"))
        dimensions = int(getattr(config, "dimensions", 384) or 384)
        self.fallback = HashFallbackEmbedder(dimensions=dimensions)
        if requested_provider == "sentence-transformers":
            provider = SentenceTransformerEmbedder(
                model=str(getattr(config, "model", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")),
                dimensions=dimensions,
                device=getattr(config, "device", None),
                normalize=bool(getattr(config, "normalize", True)),
                batch_size=int(getattr(config, "batch_size", 32) or 32),
                cache_dir=getattr(config, "cache_dir", None),
                local_files_only=bool(getattr(config, "local_files_only", False)),
            )
            self.provider = provider if provider.available else self.fallback
        else:
            self.provider = self.fallback
        self._cache.clear()

    def embed(self, text: str | None, *, purpose: str = "search", dims: int | None = None) -> list[float]:
        normalized = normalize_text(text)
        target_dims = int(dims or getattr(self.provider, "dimensions", 384) or 384)
        key = (normalized, target_dims)
        if key in self._cache:
            self._cache.move_to_end(key)
            return list(self._cache[key])
        vector = self.provider.embed(normalized, purpose=purpose, dims=target_dims)
        self._cache[key] = list(vector)
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return list(vector)

    def embed_many(self, texts: list[str], *, purpose: str = "search", dims: int | None = None) -> list[list[float]]:
        target_dims = int(dims or getattr(self.provider, "dimensions", 384) or 384)
        return [self.embed(text, purpose=purpose, dims=target_dims) for text in texts]

    def describe(self) -> dict[str, Any]:
        provider = self.provider
        requested = getattr(self.config, "provider", "sentence-transformers") if self.config is not None else "sentence-transformers"
        model = getattr(self.config, "model", getattr(provider, "model_name", "lexical-hash")) if self.config is not None else getattr(provider, "model_name", "lexical-hash")
        fallback = provider.provider_name == "hash" and requested != "hash"
        payload = capability_dict(
            category="embeddings",
            provider=requested,
            active_provider=provider.provider_name,
            features={
                "local_embeddings": True,
                "sentence_transformers": requested == "sentence-transformers",
                "fallback_hash_embeddings": fallback,
                "batch_embedding": True,
            },
            items={
                "model": model,
                "dimensions": getattr(provider, "dimensions", 384),
                "config": asdict(self.config) if self.config is not None else {},
            },
            notes=[getattr(provider, "load_error", "")] if fallback and getattr(provider, "load_error", None) else [],
        )
        return payload


_RUNTIME = EmbeddingRuntime()
_RUNTIME.configure(EmbeddingLiteConfig())


def configure_embedding_runtime(config: Any) -> None:
    _RUNTIME.configure(config)


def embed_text(text: str | None, *, purpose: str = "search", dims: int | None = None) -> list[float]:
    return _RUNTIME.embed(text, purpose=purpose, dims=dims)


def embed_many(texts: list[str], *, purpose: str = "search", dims: int | None = None) -> list[list[float]]:
    return _RUNTIME.embed_many(texts, purpose=purpose, dims=dims)


def describe_embedding_runtime() -> dict[str, Any]:
    return _RUNTIME.describe()
