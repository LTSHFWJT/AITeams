from __future__ import annotations

import asyncio
import hashlib
import math
import re
import sqlite3
import threading
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import lmdb
import pyarrow as pa
from langgraph.store.base import (
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    MatchCondition,
    Op,
    PutOp,
    SearchItem,
    SearchOp,
    TTLConfig,
)

from aiteams.ai_gateway import AIGateway, ProviderRequestError
from aiteams.utils import json_dumps, json_loads

DEFAULT_LOCAL_EMBEDDING_MODEL = "BAAI/bge-m3"

try:  # pragma: no cover - optional runtime dependency
    from llama_index.embeddings.huggingface import HuggingFaceEmbedding
    HUGGINGFACE_EMBED_IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - optional runtime dependency
    HuggingFaceEmbedding = None
    HUGGINGFACE_EMBED_IMPORT_ERROR = exc


TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]")
NS_SEP = "\x1f"
ITEM_SEP = "\x1e"
ITEM_CACHE_DB = b"item_cache"
EMBED_CACHE_DB = b"embedding_cache"
PRIMARY_ITEM_DB = b"primary_items"
PRIMARY_EXPIRY_DB = b"primary_expiry"

STORE_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS store_items (
        namespace_path TEXT NOT NULL,
        namespace_json TEXT NOT NULL,
        item_key TEXT NOT NULL,
        value_json TEXT NOT NULL,
        index_json TEXT NOT NULL DEFAULT '[]',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        accessed_at TEXT NOT NULL,
        expires_at TEXT,
        PRIMARY KEY(namespace_path, item_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_store_items_namespace ON store_items(namespace_path, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_store_items_expiry ON store_items(expires_at)",
]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _parse_datetime(value: str | None) -> datetime:
    if not value:
        return _utcnow()
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return _utcnow()
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def namespace_path(namespace: tuple[str, ...]) -> str:
    return NS_SEP.join(str(part) for part in namespace)


def namespace_tuple(path: str) -> tuple[str, ...]:
    if not path:
        return ()
    return tuple(part for part in path.split(NS_SEP) if part)


def namespace_prefix_sql(namespace_prefix: tuple[str, ...]) -> tuple[str, tuple[Any, ...]]:
    path = namespace_path(namespace_prefix)
    if not path:
        return "1 = 1", ()
    return "(namespace_path = ? OR namespace_path LIKE ?)", (path, f"{path}{NS_SEP}%")


def namespace_matches(namespace: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    return namespace[: len(prefix)] == prefix if len(namespace) >= len(prefix) else False


def item_storage_key(namespace_path_value: str, item_key: str) -> bytes:
    return f"{namespace_path_value}{ITEM_SEP}{item_key}".encode("utf-8")


def parse_item_storage_key(raw_key: bytes) -> tuple[str, str]:
    text = raw_key.decode("utf-8")
    namespace_path_value, _, item_key = text.rpartition(ITEM_SEP)
    return namespace_path_value, item_key


def expiry_storage_key(expires_at: str, namespace_path_value: str, item_key: str) -> bytes:
    return f"{expires_at}{ITEM_SEP}{namespace_path_value}{ITEM_SEP}{item_key}".encode("utf-8")


def parse_expiry_storage_key(raw_key: bytes) -> tuple[str, str, str]:
    text = raw_key.decode("utf-8")
    expires_at, _, remainder = text.partition(ITEM_SEP)
    namespace_path_value, _, item_key = remainder.rpartition(ITEM_SEP)
    return expires_at, namespace_path_value, item_key


def match_condition(namespace: tuple[str, ...], condition: MatchCondition) -> bool:
    path = tuple(str(item) for item in condition.path)
    if condition.match_type == "prefix":
        if len(namespace) < len(path):
            return False
        return all(expected == "*" or actual == expected for actual, expected in zip(namespace[: len(path)], path, strict=False))
    if condition.match_type == "suffix":
        if len(namespace) < len(path):
            return False
        return all(expected == "*" or actual == expected for actual, expected in zip(namespace[-len(path) :], path, strict=False))
    return False


def compare_filter(candidate: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        if not isinstance(candidate, dict):
            return False
        return all(compare_filter(candidate.get(key), value) for key, value in expected.items())
    if isinstance(expected, (list, tuple)):
        return candidate in expected
    return candidate == expected


def extract_text(value: Any, path: str) -> list[str]:
    if path == "$":
        if isinstance(value, str):
            return [value]
        return [json_dumps(value)]
    current: list[Any] = [value]
    for token in path.split("."):
        next_items: list[Any] = []
        if token.endswith("[*]"):
            key = token[:-3]
            for item in current:
                if isinstance(item, dict):
                    selected = item.get(key)
                    if isinstance(selected, list):
                        next_items.extend(selected)
        else:
            for item in current:
                if isinstance(item, dict) and token in item:
                    next_items.append(item[token])
        current = next_items
        if not current:
            break
    results: list[str] = []
    for item in current:
        if isinstance(item, str) and item.strip():
            results.append(item)
        elif isinstance(item, (int, float, bool)):
            results.append(str(item))
        elif isinstance(item, dict):
            results.append(json_dumps(item))
    return results


def normalize_vector(values: list[float], *, dimension: int | None = None) -> list[float]:
    target = dimension if isinstance(dimension, int) and dimension > 0 else len(values)
    if target <= 0:
        return []
    if len(values) < target:
        vector = [float(value or 0.0) for value in values] + [0.0] * (target - len(values))
    else:
        vector = [float(value or 0.0) for value in values[:target]]
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [value / norm for value in vector]


def _missing_local_embedding_dependency_message() -> str:
    detail = f" ({HUGGINGFACE_EMBED_IMPORT_ERROR})" if HUGGINGFACE_EMBED_IMPORT_ERROR is not None else ""
    return (
        "本地 HuggingFaceEmbedding 依赖未安装，请安装 `llama-index-embeddings-huggingface` 及其 SentenceTransformer 依赖。"
        f"{detail}"
    )


class HashEmbedder:
    def __init__(self, dimension: int = 32):
        self.dimension = dimension
        self.model_name = f"hash-{dimension}"

    def embed_text(self, text: str) -> list[float]:
        vector = [0.0] * self.dimension
        tokens = TOKEN_RE.findall(text.lower())
        if not tokens:
            return vector
        features: list[tuple[str, float]] = []
        for token in tokens:
            features.append((token, 1.0))
            if len(token) >= 3:
                for index in range(len(token) - 2):
                    features.append((token[index : index + 3], 0.35))
        for token, base_weight in features:
            digest = hashlib.sha1(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], "big") % self.dimension
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            weight = base_weight * (1.0 + (digest[5] / 255.0))
            vector[index] += sign * weight
        return normalize_vector(vector)


class GatewayEmbedder:
    def __init__(self, gateway: AIGateway, provider: dict[str, Any], model_name: str):
        self._gateway = gateway
        self._provider = dict(provider)
        self._model = str(model_name)
        provider_id = str(provider.get("id") or provider.get("name") or provider.get("provider_type") or "provider")
        self.dimension = self._probe_dimension()
        self.model_name = f"provider:{provider_id}:{self._model}:{self.dimension}"

    def embed_text(self, text: str) -> list[float]:
        result = self._gateway.embed(self._provider, [text], model=self._model)
        source = result.vectors[0] if result.vectors else []
        if not source:
            return [0.0] * self.dimension
        if len(source) != self.dimension:
            raise ProviderRequestError(
                f"Embedding dimension changed unexpectedly for model `{self._model}`: expected {self.dimension}, got {len(source)}."
            )
        return normalize_vector([float(value or 0.0) for value in source], dimension=self.dimension)

    def _probe_dimension(self) -> int:
        result = self._gateway.embed(self._provider, ["provider embedding probe"], model=self._model)
        source = result.vectors[0] if result.vectors else []
        if not source:
            raise ProviderRequestError(f"Provider embedding model `{self._model}` returned an empty vector.")
        return len(source)


class HuggingFaceLocalEmbedder:
    def __init__(self, model_name: str, *, cache_folder: str | Path):
        if HuggingFaceEmbedding is None:
            raise RuntimeError(_missing_local_embedding_dependency_message())
        resolved_cache = Path(cache_folder).expanduser().resolve()
        resolved_cache.mkdir(parents=True, exist_ok=True)
        self._model = HuggingFaceEmbedding(
            model_name=model_name,
            cache_folder=str(resolved_cache),
            show_progress_bar=False,
        )
        self.dimension = self._probe_dimension()
        self.model_name = f"local:huggingface:{model_name}:{self.dimension}"

    def embed_text(self, text: str) -> list[float]:
        source = [float(value or 0.0) for value in list(self._model.get_text_embedding(text) or [])]
        if not source:
            return [0.0] * self.dimension
        if len(source) != self.dimension:
            raise ProviderRequestError(
                f"Local HuggingFace embedding dimension changed unexpectedly: expected {self.dimension}, got {len(source)}."
            )
        return normalize_vector(source, dimension=self.dimension)

    def _probe_dimension(self) -> int:
        source = [float(value or 0.0) for value in list(self._model.get_query_embedding("local embedding probe") or [])]
        if not source:
            raise ProviderRequestError("Local HuggingFace embedding model returned an empty vector.")
        return len(source)


class GatewayReranker:
    def __init__(self, gateway: AIGateway, provider: dict[str, Any], model_name: str):
        self._gateway = gateway
        self._provider = dict(provider)
        self._model = str(model_name)
        provider_id = str(provider.get("id") or provider.get("name") or provider.get("provider_type") or "provider")
        self.name = f"provider:{provider_id}:{self._model}"

    def rerank(self, *, query: str, documents: list[str], top_n: int | None = None) -> list[dict[str, Any]]:
        result = self._gateway.rerank(self._provider, query=query, documents=documents, model=self._model, top_n=top_n)
        return [dict(item) for item in result.items]


class LMDBHotCache:
    def __init__(self, root_dir: str | Path):
        path = Path(root_dir).expanduser().resolve()
        path.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._env = lmdb.open(
            str(path),
            map_size=128 * 1024 * 1024,
            max_dbs=8,
            subdir=True,
            create=True,
            lock=True,
        )
        self._item_cache = self._env.open_db(ITEM_CACHE_DB)
        self._embed_cache = self._env.open_db(EMBED_CACHE_DB)

    def close(self) -> None:
        with self._lock:
            self._env.close()

    def get_item(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            with self._env.begin(db=self._item_cache) as txn:
                raw = txn.get(key.encode("utf-8"))
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json_loads(raw, None)

    def put_item(self, key: str, item: dict[str, Any]) -> None:
        with self._lock:
            with self._env.begin(write=True, db=self._item_cache) as txn:
                txn.put(key.encode("utf-8"), json_dumps(item).encode("utf-8"))

    def delete_item(self, key: str) -> None:
        with self._lock:
            with self._env.begin(write=True, db=self._item_cache) as txn:
                txn.delete(key.encode("utf-8"))

    def get_embedding(self, key: str) -> list[float] | None:
        with self._lock:
            with self._env.begin(db=self._embed_cache) as txn:
                raw = txn.get(key.encode("utf-8"))
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json_loads(raw, None)

    def put_embedding(self, key: str, vector: list[float]) -> None:
        with self._lock:
            with self._env.begin(write=True, db=self._embed_cache) as txn:
                txn.put(key.encode("utf-8"), json_dumps(vector).encode("utf-8"))

    def clear_embeddings(self) -> None:
        with self._lock:
            with self._env.begin(write=True) as txn:
                txn.drop(self._embed_cache, delete=False)


class LanceIndex:
    def __init__(self, root_dir: str | Path, *, vector_dim: int):
        try:
            import lancedb
        except ImportError:
            self._available = False
            self._table = None
            return
        self._available = True
        self.vector_dim = int(vector_dim)
        root = Path(root_dir).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        self._db = lancedb.connect(root)
        self._schema = pa.schema(
            [
                pa.field("row_id", pa.string()),
                pa.field("namespace_path", pa.string()),
                pa.field("item_key", pa.string()),
                pa.field("index_path", pa.string()),
                pa.field("text", pa.string()),
                pa.field("vector", pa.list_(pa.float32(), self.vector_dim)),
                pa.field("updated_at", pa.string()),
            ]
        )
        try:
            table = self._db.open_table("store_index")
            if not table.schema.equals(self._schema, check_metadata=False):
                self._table = self._db.create_table("store_index", schema=self._schema, mode="overwrite")
            else:
                self._table = table
        except Exception:
            self._table = self._db.create_table("store_index", schema=self._schema, mode="overwrite")

    def close(self) -> None:
        return None

    def replace_item(self, namespace_path_value: str, item_key: str, rows: list[dict[str, Any]]) -> None:
        if not self._available:
            return
        self._table.delete(
            f"namespace_path = '{self._quote(namespace_path_value)}' AND item_key = '{self._quote(item_key)}'"
        )
        if rows:
            self._table.add(rows)

    def replace_all(self, rows: list[dict[str, Any]]) -> None:
        if not self._available:
            return
        self._table = self._db.create_table("store_index", schema=self._schema, mode="overwrite")
        if rows:
            self._table.add(rows)

    def search(self, *, namespace_prefix: tuple[str, ...], vector: list[float], limit: int) -> list[dict[str, Any]]:
        if not self._available:
            return []
        candidates = self._table.search(vector).limit(max(limit * 4, limit)).to_list()
        if not namespace_prefix:
            return candidates[:limit]
        filtered = [
            item
            for item in candidates
            if namespace_matches(namespace_tuple(str(item.get("namespace_path") or "")), namespace_prefix)
        ]
        return filtered[:limit]

    @staticmethod
    def _quote(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")


class NullVectorIndex:
    def __init__(self, root_dir: str | Path):
        self.root = Path(root_dir).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._available = False

    def close(self) -> None:
        return None

    def replace_item(self, namespace_path_value: str, item_key: str, rows: list[dict[str, Any]]) -> None:
        del namespace_path_value, item_key, rows
        return None

    def replace_all(self, rows: list[dict[str, Any]]) -> None:
        del rows
        return None

    def search(self, *, namespace_prefix: tuple[str, ...], vector: list[float], limit: int) -> list[dict[str, Any]]:
        del namespace_prefix, vector, limit
        return []


class SQLiteLanceDBStore(BaseStore):
    supports_ttl = True

    def __init__(
        self,
        root_dir: str | Path,
        *,
        vector_dim: int = 32,
        default_ttl_minutes: float | None = None,
        sweep_interval_minutes: int = 10,
        default_index_fields: list[str] | None = None,
        gateway: AIGateway | None = None,
    ) -> None:
        self.root = Path(root_dir).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._hash_vector_dim = int(vector_dim)
        self._vector_dim: int | None = None
        self.ttl_config: TTLConfig = {
            "refresh_on_read": True,
            "default_ttl": default_ttl_minutes,
            "sweep_interval_minutes": sweep_interval_minutes,
        }
        self._lock = threading.RLock()
        self._catalog = sqlite3.connect(str(self.root / "store.sqlite3"), check_same_thread=False)
        self._catalog.row_factory = sqlite3.Row
        self._catalog.execute("PRAGMA journal_mode=WAL")
        self._catalog.execute("PRAGMA foreign_keys=ON")
        for statement in STORE_SCHEMA:
            self._catalog.execute(statement)
        self._catalog.commit()
        self._hot = LMDBHotCache(self.root / "lmdb")
        self._gateway = gateway or AIGateway()
        self._embedder: GatewayEmbedder | HashEmbedder | HuggingFaceLocalEmbedder | None = None
        self._reranker: GatewayReranker | None = None
        self._index: LanceIndex | NullVectorIndex = NullVectorIndex(self.root / "lancedb")
        self._default_index_fields = list(default_index_fields or ["content"])
        self._last_sweep = _utcnow()
        self._retrieval = {
            "embedding": {"mode": "disabled", "vector_enabled": False, "vector_dim": None},
            "rerank": {"mode": "disabled"},
        }

    def close(self) -> None:
        self._index.close()
        self._hot.close()
        with self._lock:
            self._catalog.close()

    def configure_retrieval(self, settings: dict[str, Any] | None = None) -> dict[str, Any]:
        config = dict(settings or {})
        embedding = dict(config.get("embedding") or {})
        rerank = dict(config.get("rerank") or {})
        with self._lock:
            next_embedder = self._build_embedder(embedding)
            next_reranker = self._build_reranker(rerank)
            previous_model_name = self._embedder.model_name if self._embedder is not None else ""
            previous_vector_dim = int(self._vector_dim or 0)
            next_model_name = next_embedder.model_name if next_embedder is not None else ""
            next_vector_dim = int(next_embedder.dimension) if next_embedder is not None else 0
            rebuild_index = previous_model_name != next_model_name or previous_vector_dim != next_vector_dim
            if rebuild_index:
                self._hot.clear_embeddings()
            if next_embedder is None:
                self._index.close()
                self._vector_dim = None
                self._index = NullVectorIndex(self.root / "lancedb")
            elif previous_vector_dim != next_vector_dim or isinstance(self._index, NullVectorIndex):
                self._index.close()
                self._vector_dim = next_vector_dim
                self._index = LanceIndex(self.root / "lancedb", vector_dim=next_vector_dim)
            self._embedder = next_embedder
            self._reranker = next_reranker
            self._retrieval = {
                "embedding": self._retrieval_embedding_payload(
                    embedding,
                    model_name=self._embedder.model_name if self._embedder is not None else "",
                    vector_dim=self._vector_dim,
                ),
                "rerank": self._retrieval_rerank_payload(rerank),
            }
            reindexed_items = self._rebuild_index_locked() if rebuild_index else 0
        return {
            "embedding_reindexed": rebuild_index,
            "reindexed_items": reindexed_items,
            "retrieval": self.retrieval_info(),
        }

    def retrieval_info(self) -> dict[str, Any]:
        return {
            "embedding": dict(self._retrieval.get("embedding") or {}),
            "rerank": dict(self._retrieval.get("rerank") or {}),
            "vector_dim": self._vector_dim,
        }

    def _build_embedder(self, config: dict[str, Any]) -> GatewayEmbedder | HashEmbedder | HuggingFaceLocalEmbedder | None:
        mode = str(config.get("mode") or "disabled").strip().lower()
        if mode == "hash":
            dimension = max(8, int(config.get("dimension") or self._hash_vector_dim or 32))
            return HashEmbedder(dimension=dimension)
        if mode == "local":
            model_name = str(config.get("model") or config.get("model_name") or DEFAULT_LOCAL_EMBEDDING_MODEL).strip()
            if not model_name:
                return None
            return HuggingFaceLocalEmbedder(model_name, cache_folder=self.root / "huggingface-cache")
        if mode != "provider":
            return None
        provider = dict(config.get("provider") or {})
        model_name = str(config.get("model") or config.get("model_name") or "").strip()
        if not provider or not model_name:
            return None
        return GatewayEmbedder(self._gateway, provider, model_name)

    def _build_reranker(self, config: dict[str, Any]) -> GatewayReranker | None:
        mode = str(config.get("mode") or "disabled").strip().lower()
        if mode != "provider":
            return None
        provider = dict(config.get("provider") or {})
        model_name = str(config.get("model") or config.get("model_name") or "").strip()
        if not provider or not model_name:
            return None
        return GatewayReranker(self._gateway, provider, model_name)

    def _retrieval_embedding_payload(self, config: dict[str, Any], *, model_name: str, vector_dim: int | None) -> dict[str, Any]:
        mode = str(config.get("mode") or "disabled").strip().lower()
        if mode == "hash":
            return {
                "mode": "hash",
                "model_name": str(config.get("model_name") or model_name or f"hash-{vector_dim or self._hash_vector_dim}"),
                "vector_enabled": True,
                "vector_dim": vector_dim,
            }
        if mode == "local":
            return {
                "mode": "local",
                "backend": "huggingface",
                "model_name": str(config.get("model_name") or config.get("model") or DEFAULT_LOCAL_EMBEDDING_MODEL),
                "vector_enabled": True,
                "vector_dim": vector_dim,
            }
        if mode != "provider":
            return {"mode": "disabled", "model_name": None, "vector_enabled": False, "vector_dim": None}
        return {
            "mode": "provider",
            "provider_id": config.get("provider_id"),
            "provider_name": config.get("provider_name"),
            "provider_type": config.get("provider_type"),
            "model_name": str(config.get("model_name") or config.get("model") or model_name),
            "vector_enabled": True,
            "vector_dim": vector_dim,
        }

    def _retrieval_rerank_payload(self, config: dict[str, Any]) -> dict[str, Any]:
        mode = str(config.get("mode") or "disabled").strip().lower()
        if mode != "provider":
            return {"mode": "disabled"}
        return {
            "mode": "provider",
            "provider_id": config.get("provider_id"),
            "provider_name": config.get("provider_name"),
            "provider_type": config.get("provider_type"),
            "model_name": str(config.get("model_name") or config.get("model") or ""),
        }

    def batch(self, ops: Iterable[Op]) -> list[Any]:
        with self._lock:
            self._sweep_expired_locked()
            results: list[Any] = []
            for op in ops:
                if isinstance(op, GetOp):
                    results.append(self._get_locked(op))
                elif isinstance(op, SearchOp):
                    results.append(self._search_locked(op))
                elif isinstance(op, ListNamespacesOp):
                    results.append(self._list_namespaces_locked(op))
                elif isinstance(op, PutOp):
                    self._put_locked(op)
                    results.append(None)
                else:
                    raise ValueError(f"Unknown operation type: {type(op)}")
            return results

    async def abatch(self, ops: Iterable[Op]) -> list[Any]:
        return await asyncio.to_thread(self.batch, list(ops))

    def _cache_key(self, namespace: tuple[str, ...], key: str) -> str:
        return f"{namespace_path(namespace)}::{key}"

    def _embed(self, text: str) -> list[float]:
        if self._embedder is None or self._vector_dim is None:
            return []
        digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
        cache_key = f"{self._embedder.model_name}:{digest}"
        cached = self._hot.get_embedding(cache_key)
        if cached is not None:
            return [float(value) for value in cached]
        vector = self._embedder.embed_text(text)
        if len(vector) != self._vector_dim:
            raise ValueError(
                f"Embedding vector dimension mismatch: expected {self._vector_dim}, received {len(vector)} from `{self._embedder.model_name}`."
            )
        self._hot.put_embedding(cache_key, vector)
        return vector

    def _expires_at(self, ttl_minutes: float | None) -> str | None:
        if ttl_minutes is None:
            return None
        return _isoformat(_utcnow() + timedelta(minutes=float(ttl_minutes)))

    def _serialize_item(
        self,
        *,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        created_at: str,
        updated_at: str,
        accessed_at: str,
        expires_at: str | None,
        index_fields: list[str],
    ) -> dict[str, Any]:
        return {
            "namespace": list(namespace),
            "key": key,
            "value": value,
            "created_at": created_at,
            "updated_at": updated_at,
            "accessed_at": accessed_at,
            "expires_at": expires_at,
            "index_fields": list(index_fields),
        }

    def _row_to_item(self, row: sqlite3.Row) -> Item:
        return Item(
            namespace=tuple(json_loads(row["namespace_json"], [])),
            key=str(row["item_key"]),
            value=dict(json_loads(row["value_json"], {})),
            created_at=_parse_datetime(str(row["created_at"])),
            updated_at=_parse_datetime(str(row["updated_at"])),
        )

    def _row_to_search_item(self, row: sqlite3.Row, *, score: float | None = None) -> SearchItem:
        return SearchItem(
            namespace=tuple(json_loads(row["namespace_json"], [])),
            key=str(row["item_key"]),
            value=dict(json_loads(row["value_json"], {})),
            created_at=_parse_datetime(str(row["created_at"])),
            updated_at=_parse_datetime(str(row["updated_at"])),
            score=score,
        )

    def _refresh_ttl_locked(self, namespace_path_value: str, item_key: str, expires_at: str | None) -> None:
        now = _isoformat(_utcnow())
        self._catalog.execute(
            "UPDATE store_items SET accessed_at = ?, expires_at = ? WHERE namespace_path = ? AND item_key = ?",
            (now, expires_at, namespace_path_value, item_key),
        )
        self._catalog.commit()

    def _delete_locked(self, namespace_path_value: str, item_key: str) -> None:
        self._catalog.execute(
            "DELETE FROM store_items WHERE namespace_path = ? AND item_key = ?",
            (namespace_path_value, item_key),
        )
        self._catalog.commit()
        self._index.replace_item(namespace_path_value, item_key, [])
        self._hot.delete_item(f"{namespace_path_value}::{item_key}")

    def _index_rows_for_value(
        self,
        *,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        index_fields: list[str],
        updated_at: str,
    ) -> list[dict[str, Any]]:
        if self._embedder is None or self._vector_dim is None:
            return []
        ns_path = namespace_path(namespace)
        rows: list[dict[str, Any]] = []
        for index_path in index_fields:
            for idx, text in enumerate(extract_text(value, index_path)):
                if not text.strip():
                    continue
                rows.append(
                    {
                        "row_id": hashlib.sha1(f"{ns_path}:{key}:{index_path}:{idx}".encode("utf-8")).hexdigest(),
                        "namespace_path": ns_path,
                        "item_key": key,
                        "index_path": index_path,
                        "text": text,
                        "vector": self._embed(text),
                        "updated_at": updated_at,
                    }
                )
        return rows

    def _rebuild_index_locked(self) -> int:
        rows = self._catalog.execute(
            "SELECT namespace_json, item_key, value_json, index_json, updated_at FROM store_items ORDER BY updated_at ASC"
        ).fetchall()
        if self._embedder is None or self._vector_dim is None:
            self._index.replace_all([])
            return len(rows)
        index_rows: list[dict[str, Any]] = []
        for row in rows:
            namespace = tuple(json_loads(row["namespace_json"], []))
            value = dict(json_loads(row["value_json"], {}))
            index_fields = list(json_loads(row["index_json"], []))
            index_rows.extend(
                self._index_rows_for_value(
                    namespace=namespace,
                    key=str(row["item_key"]),
                    value=value,
                    index_fields=index_fields,
                    updated_at=str(row["updated_at"]),
                )
            )
        self._index.replace_all(index_rows)
        return len(rows)

    def _put_locked(self, op: PutOp) -> None:
        ns_path = namespace_path(op.namespace)
        if op.value is None:
            self._delete_locked(ns_path, op.key)
            return
        existing = self._catalog.execute(
            "SELECT created_at FROM store_items WHERE namespace_path = ? AND item_key = ?",
            (ns_path, op.key),
        ).fetchone()
        now = _isoformat(_utcnow())
        created_at = str(existing["created_at"]) if existing is not None else now
        expires_at = self._expires_at(op.ttl)
        index_fields = list(self._default_index_fields if op.index is None else ([] if op.index is False else op.index))
        self._catalog.execute(
            """
            INSERT INTO store_items(namespace_path, namespace_json, item_key, value_json, index_json, created_at, updated_at, accessed_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(namespace_path, item_key) DO UPDATE SET
                namespace_json = excluded.namespace_json,
                value_json = excluded.value_json,
                index_json = excluded.index_json,
                updated_at = excluded.updated_at,
                accessed_at = excluded.accessed_at,
                expires_at = excluded.expires_at
            """,
            (
                ns_path,
                json_dumps(list(op.namespace)),
                op.key,
                json_dumps(op.value),
                json_dumps(index_fields),
                created_at,
                now,
                now,
                expires_at,
            ),
        )
        self._catalog.commit()
        self._hot.put_item(
            self._cache_key(op.namespace, op.key),
            self._serialize_item(
                namespace=op.namespace,
                key=op.key,
                value=op.value,
                created_at=created_at,
                updated_at=now,
                accessed_at=now,
                expires_at=expires_at,
                index_fields=index_fields,
            ),
        )
        rows = self._index_rows_for_value(
            namespace=op.namespace,
            key=op.key,
            value=op.value,
            index_fields=index_fields,
            updated_at=now,
        )
        self._index.replace_item(ns_path, op.key, rows)

    def _get_locked(self, op: GetOp) -> Item | None:
        cache = self._hot.get_item(self._cache_key(op.namespace, op.key))
        if cache:
            expires_at = _parse_datetime(cache.get("expires_at")) if cache.get("expires_at") else None
            if expires_at and expires_at <= _utcnow():
                self._delete_locked(namespace_path(op.namespace), op.key)
                return None
            if op.refresh_ttl and cache.get("expires_at"):
                refreshed = self._expires_at(self.ttl_config.get("default_ttl"))
                self._refresh_ttl_locked(namespace_path(op.namespace), op.key, refreshed)
            return Item(
                namespace=tuple(cache.get("namespace") or []),
                key=str(cache.get("key") or op.key),
                value=dict(cache.get("value") or {}),
                created_at=_parse_datetime(str(cache.get("created_at"))),
                updated_at=_parse_datetime(str(cache.get("updated_at"))),
            )
        row = self._catalog.execute(
            "SELECT * FROM store_items WHERE namespace_path = ? AND item_key = ?",
            (namespace_path(op.namespace), op.key),
        ).fetchone()
        if row is None:
            return None
        expires_at = _parse_datetime(str(row["expires_at"])) if row["expires_at"] else None
        if expires_at and expires_at <= _utcnow():
            self._delete_locked(namespace_path(op.namespace), op.key)
            return None
        if op.refresh_ttl and row["expires_at"]:
            refreshed = self._expires_at(self.ttl_config.get("default_ttl"))
            self._refresh_ttl_locked(namespace_path(op.namespace), op.key, refreshed)
        item = self._row_to_item(row)
        self._hot.put_item(
            self._cache_key(op.namespace, op.key),
            self._serialize_item(
                namespace=item.namespace,
                key=item.key,
                value=item.value,
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
                accessed_at=str(row["accessed_at"]),
                expires_at=str(row["expires_at"]) if row["expires_at"] else None,
                index_fields=list(json_loads(row["index_json"], [])),
            ),
        )
        return item

    def _search_locked(self, op: SearchOp) -> list[SearchItem]:
        where, params = namespace_prefix_sql(op.namespace_prefix)
        sql = f"SELECT * FROM store_items WHERE {where}"
        rows = self._catalog.execute(sql, params).fetchall()
        active_rows: list[sqlite3.Row] = []
        now = _utcnow()
        for row in rows:
            expires_at = _parse_datetime(str(row["expires_at"])) if row["expires_at"] else None
            if expires_at and expires_at <= now:
                self._delete_locked(str(row["namespace_path"]), str(row["item_key"]))
                continue
            active_rows.append(row)
        if op.filter:
            filtered_rows = []
            for row in active_rows:
                value = dict(json_loads(row["value_json"], {}))
                if all(compare_filter(value.get(key), expected) for key, expected in op.filter.items()):
                    filtered_rows.append(row)
            active_rows = filtered_rows
        scored: list[tuple[float | None, sqlite3.Row]] = []
        query_value = (op.query or "").strip()
        if query_value:
            vector_score_map: dict[tuple[str, str], float] = {}
            if self._embedder is not None and self._vector_dim is not None:
                vector_hits = self._index.search(namespace_prefix=op.namespace_prefix, vector=self._embed(query_value), limit=max(op.limit + op.offset, 1))
                for hit in vector_hits:
                    score = 1.0 / (1.0 + max(float(hit.get("_distance") or 0.0), 0.0))
                    identity = (str(hit.get("namespace_path") or ""), str(hit.get("item_key") or ""))
                    if score > vector_score_map.get(identity, float("-inf")):
                        vector_score_map[identity] = score
            query_lower = query_value.lower()
            query_terms = [term.lower() for term in TOKEN_RE.findall(query_lower)]
            for row in active_rows:
                value = dict(json_loads(row["value_json"], {}))
                content = json_dumps(value).lower()
                lexical = float(content.count(query_lower)) * 5.0 if query_lower in content else 0.0
                for term in query_terms:
                    lexical += float(content.count(term))
                semantic = vector_score_map.get((str(row["namespace_path"]), str(row["item_key"])), 0.0)
                score = lexical + semantic
                if score > 0:
                    scored.append((score, row))
            scored.sort(key=lambda item: item[0], reverse=True)
            reranked = self._rerank_scored_rows(query_value, scored, limit=max(op.limit + op.offset, 1))
            if reranked:
                scored = reranked
        else:
            active_rows.sort(key=lambda row: str(row["updated_at"]), reverse=True)
            scored = [(None, row) for row in active_rows]
        selected = scored[op.offset : op.offset + op.limit]
        if op.refresh_ttl:
            refreshed = self._expires_at(self.ttl_config.get("default_ttl"))
            for _, row in selected:
                if row["expires_at"]:
                    self._refresh_ttl_locked(str(row["namespace_path"]), str(row["item_key"]), refreshed)
        return [self._row_to_search_item(row, score=score) for score, row in selected]

    def _rerank_scored_rows(
        self,
        query: str,
        scored: list[tuple[float | None, sqlite3.Row]],
        *,
        limit: int,
    ) -> list[tuple[float | None, sqlite3.Row]] | None:
        if self._reranker is None or not scored:
            return None
        head_size = min(len(scored), max(limit * 4, 12))
        head = scored[:head_size]
        documents = [self._document_text_for_rerank(dict(json_loads(row["value_json"], {}))) for _, row in head]
        try:
            reranked = self._reranker.rerank(query=query, documents=documents, top_n=head_size)
        except ProviderRequestError:
            return None
        rerank_scores = {int(item.get("index", -1)): float(item.get("relevance_score", 0.0) or 0.0) for item in reranked}
        if not rerank_scores:
            return None
        rescored = [
            (((rerank_scores.get(index, 0.0) * 100.0) + float(base_score or 0.0)), row)
            for index, (base_score, row) in enumerate(head)
        ]
        rescored.sort(key=lambda item: item[0], reverse=True)
        return rescored + scored[head_size:]

    def _document_text_for_rerank(self, value: dict[str, Any]) -> str:
        parts: list[str] = []
        summary = str(value.get("summary") or "").strip()
        content = value.get("content")
        if summary:
            parts.append(summary)
        if isinstance(content, str) and content.strip():
            parts.append(content.strip())
        elif isinstance(content, dict):
            for candidate in ("summary", "content", "text"):
                text = str(content.get(candidate) or "").strip()
                if text:
                    parts.append(text)
        if parts:
            return "\n".join(parts[:2])
        return json_dumps(value)

    def _list_namespaces_locked(self, op: ListNamespacesOp) -> list[tuple[str, ...]]:
        rows = self._catalog.execute("SELECT DISTINCT namespace_path FROM store_items ORDER BY namespace_path ASC").fetchall()
        namespaces = [namespace_tuple(str(row["namespace_path"])) for row in rows]
        if op.match_conditions:
            namespaces = [ns for ns in namespaces if all(match_condition(ns, condition) for condition in op.match_conditions)]
        if op.max_depth is not None:
            namespaces = sorted({ns[: op.max_depth] for ns in namespaces})
        else:
            namespaces = sorted(namespaces)
        return namespaces[op.offset : op.offset + op.limit]

    def sweep_expired(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            deleted = self._sweep_expired_locked(force=force)
        return {
            "deleted": deleted,
            "checked_at": _isoformat(_utcnow()),
            "force": force,
        }

    def maintenance_interval_seconds(self) -> float:
        interval = int(self.ttl_config.get("sweep_interval_minutes") or 0)
        if interval <= 0:
            return 60.0
        return max(float(interval) * 60.0, 5.0)

    def _sweep_expired_locked(self, *, force: bool = False) -> int:
        interval = int(self.ttl_config.get("sweep_interval_minutes") or 0)
        if interval <= 0 and not force:
            return 0
        now = _utcnow()
        if not force and (now - self._last_sweep) < timedelta(minutes=interval):
            return 0
        expired = self._catalog.execute(
            "SELECT namespace_path, item_key FROM store_items WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (_isoformat(now),),
        ).fetchall()
        for row in expired:
            self._delete_locked(str(row["namespace_path"]), str(row["item_key"]))
        self._last_sweep = now
        return len(expired)


class LMDBLanceDBStore(BaseStore):
    supports_ttl = True

    def __init__(
        self,
        root_dir: str | Path,
        *,
        vector_dim: int = 32,
        default_ttl_minutes: float | None = None,
        sweep_interval_minutes: int = 10,
        default_index_fields: list[str] | None = None,
        gateway: AIGateway | None = None,
        lmdb_map_size: int = 512 * 1024 * 1024,
    ) -> None:
        self.root = Path(root_dir).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.primary_path = self.root / "lmdb"
        self.primary_path.mkdir(parents=True, exist_ok=True)
        self._hash_vector_dim = int(vector_dim)
        self._vector_dim: int | None = None
        self.ttl_config: TTLConfig = {
            "refresh_on_read": True,
            "default_ttl": default_ttl_minutes,
            "sweep_interval_minutes": sweep_interval_minutes,
        }
        self._lock = threading.RLock()
        self._env = lmdb.open(
            str(self.primary_path),
            map_size=max(int(lmdb_map_size or 0), 64 * 1024 * 1024),
            max_dbs=16,
            subdir=True,
            create=True,
            lock=True,
        )
        self._items = self._env.open_db(PRIMARY_ITEM_DB)
        self._expiry = self._env.open_db(PRIMARY_EXPIRY_DB)
        self._embed_cache = self._env.open_db(EMBED_CACHE_DB)
        self._gateway = gateway or AIGateway()
        self._embedder: GatewayEmbedder | HashEmbedder | HuggingFaceLocalEmbedder | None = None
        self._reranker: GatewayReranker | None = None
        self._index: LanceIndex | NullVectorIndex = NullVectorIndex(self.root / "lancedb")
        self._default_index_fields = list(default_index_fields or ["content"])
        self._last_sweep = _utcnow()
        self._retrieval = {
            "embedding": {"mode": "disabled", "vector_enabled": False, "vector_dim": None},
            "rerank": {"mode": "disabled"},
        }

    def close(self) -> None:
        self._index.close()
        with self._lock:
            self._env.close()

    def configure_retrieval(self, settings: dict[str, Any] | None = None) -> dict[str, Any]:
        config = dict(settings or {})
        embedding = dict(config.get("embedding") or {})
        rerank = dict(config.get("rerank") or {})
        with self._lock:
            next_embedder = self._build_embedder(embedding)
            next_reranker = self._build_reranker(rerank)
            previous_model_name = self._embedder.model_name if self._embedder is not None else ""
            previous_vector_dim = int(self._vector_dim or 0)
            next_model_name = next_embedder.model_name if next_embedder is not None else ""
            next_vector_dim = int(next_embedder.dimension) if next_embedder is not None else 0
            rebuild_index = previous_model_name != next_model_name or previous_vector_dim != next_vector_dim
            if rebuild_index:
                self._clear_embedding_cache_locked()
            if next_embedder is None:
                self._index.close()
                self._vector_dim = None
                self._index = NullVectorIndex(self.root / "lancedb")
            elif previous_vector_dim != next_vector_dim or isinstance(self._index, NullVectorIndex):
                self._index.close()
                self._vector_dim = next_vector_dim
                self._index = LanceIndex(self.root / "lancedb", vector_dim=next_vector_dim)
            self._embedder = next_embedder
            self._reranker = next_reranker
            self._retrieval = {
                "embedding": self._retrieval_embedding_payload(
                    embedding,
                    model_name=self._embedder.model_name if self._embedder is not None else "",
                    vector_dim=self._vector_dim,
                ),
                "rerank": self._retrieval_rerank_payload(rerank),
            }
            reindexed_items = self._rebuild_index_locked() if rebuild_index else 0
        return {
            "embedding_reindexed": rebuild_index,
            "reindexed_items": reindexed_items,
            "retrieval": self.retrieval_info(),
        }

    def retrieval_info(self) -> dict[str, Any]:
        return {
            "embedding": dict(self._retrieval.get("embedding") or {}),
            "rerank": dict(self._retrieval.get("rerank") or {}),
            "vector_dim": self._vector_dim,
        }

    def batch(self, ops: Iterable[Op]) -> list[Any]:
        with self._lock:
            self._sweep_expired_locked()
            results: list[Any] = []
            for op in ops:
                if isinstance(op, GetOp):
                    results.append(self._get_locked(op))
                elif isinstance(op, SearchOp):
                    results.append(self._search_locked(op))
                elif isinstance(op, ListNamespacesOp):
                    results.append(self._list_namespaces_locked(op))
                elif isinstance(op, PutOp):
                    self._put_locked(op)
                    results.append(None)
                else:
                    raise ValueError(f"Unknown operation type: {type(op)}")
            return results

    async def abatch(self, ops: Iterable[Op]) -> list[Any]:
        return await asyncio.to_thread(self.batch, list(ops))

    def item_count(self) -> int:
        with self._lock:
            with self._env.begin() as txn:
                return int(txn.stat(db=self._items).get("entries", 0) or 0)

    def sweep_expired(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            deleted = self._sweep_expired_locked(force=force)
        return {
            "deleted": deleted,
            "checked_at": _isoformat(_utcnow()),
            "force": force,
        }

    def maintenance_interval_seconds(self) -> float:
        interval = int(self.ttl_config.get("sweep_interval_minutes") or 0)
        if interval <= 0:
            return 60.0
        return max(float(interval) * 60.0, 5.0)

    def _build_embedder(self, config: dict[str, Any]) -> GatewayEmbedder | HashEmbedder | HuggingFaceLocalEmbedder | None:
        mode = str(config.get("mode") or "disabled").strip().lower()
        if mode == "hash":
            dimension = max(8, int(config.get("dimension") or self._hash_vector_dim or 32))
            return HashEmbedder(dimension=dimension)
        if mode == "local":
            model_name = str(config.get("model") or config.get("model_name") or DEFAULT_LOCAL_EMBEDDING_MODEL).strip()
            if not model_name:
                return None
            return HuggingFaceLocalEmbedder(model_name, cache_folder=self.root / "huggingface-cache")
        if mode != "provider":
            return None
        provider = dict(config.get("provider") or {})
        model_name = str(config.get("model") or config.get("model_name") or "").strip()
        if not provider or not model_name:
            return None
        return GatewayEmbedder(self._gateway, provider, model_name)

    def _build_reranker(self, config: dict[str, Any]) -> GatewayReranker | None:
        mode = str(config.get("mode") or "disabled").strip().lower()
        if mode != "provider":
            return None
        provider = dict(config.get("provider") or {})
        model_name = str(config.get("model") or config.get("model_name") or "").strip()
        if not provider or not model_name:
            return None
        return GatewayReranker(self._gateway, provider, model_name)

    def _retrieval_embedding_payload(self, config: dict[str, Any], *, model_name: str, vector_dim: int | None) -> dict[str, Any]:
        mode = str(config.get("mode") or "disabled").strip().lower()
        if mode == "hash":
            return {
                "mode": "hash",
                "model_name": str(config.get("model_name") or model_name or f"hash-{vector_dim or self._hash_vector_dim}"),
                "vector_enabled": True,
                "vector_dim": vector_dim,
            }
        if mode == "local":
            return {
                "mode": "local",
                "backend": "huggingface",
                "model_name": str(config.get("model_name") or config.get("model") or DEFAULT_LOCAL_EMBEDDING_MODEL),
                "vector_enabled": True,
                "vector_dim": vector_dim,
            }
        if mode != "provider":
            return {"mode": "disabled", "model_name": None, "vector_enabled": False, "vector_dim": None}
        return {
            "mode": "provider",
            "provider_id": config.get("provider_id"),
            "provider_name": config.get("provider_name"),
            "provider_type": config.get("provider_type"),
            "model_name": str(config.get("model_name") or config.get("model") or model_name),
            "vector_enabled": True,
            "vector_dim": vector_dim,
        }

    def _retrieval_rerank_payload(self, config: dict[str, Any]) -> dict[str, Any]:
        mode = str(config.get("mode") or "disabled").strip().lower()
        if mode != "provider":
            return {"mode": "disabled"}
        return {
            "mode": "provider",
            "provider_id": config.get("provider_id"),
            "provider_name": config.get("provider_name"),
            "provider_type": config.get("provider_type"),
            "model_name": str(config.get("model_name") or config.get("model") or ""),
        }

    def _embed(self, text: str) -> list[float]:
        if self._embedder is None or self._vector_dim is None:
            return []
        digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
        cache_key = f"{self._embedder.model_name}:{digest}"
        cached = self._get_cached_embedding(cache_key)
        if cached is not None:
            return [float(value) for value in cached]
        vector = self._embedder.embed_text(text)
        if len(vector) != self._vector_dim:
            raise ValueError(
                f"Embedding vector dimension mismatch: expected {self._vector_dim}, received {len(vector)} from `{self._embedder.model_name}`."
            )
        self._put_cached_embedding(cache_key, vector)
        return vector

    def _get_cached_embedding(self, key: str) -> list[float] | None:
        with self._env.begin() as txn:
            raw = txn.get(key.encode("utf-8"), db=self._embed_cache)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json_loads(raw, None)

    def _put_cached_embedding(self, key: str, vector: list[float]) -> None:
        with self._env.begin(write=True) as txn:
            txn.put(key.encode("utf-8"), json_dumps(vector).encode("utf-8"), db=self._embed_cache)

    def _clear_embedding_cache_locked(self) -> None:
        with self._env.begin(write=True) as txn:
            txn.drop(self._embed_cache, delete=False)

    def _serialize_record(
        self,
        *,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        created_at: str,
        updated_at: str,
        accessed_at: str,
        expires_at: str | None,
        index_fields: list[str],
    ) -> dict[str, Any]:
        return {
            "namespace": list(namespace),
            "key": key,
            "value": value,
            "created_at": created_at,
            "updated_at": updated_at,
            "accessed_at": accessed_at,
            "expires_at": expires_at,
            "index_fields": list(index_fields),
        }

    def _deserialize_record(self, raw: bytes | str | None) -> dict[str, Any] | None:
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json_loads(raw, None)
        return dict(payload or {}) if isinstance(payload, dict) else None

    def _record_to_item(self, record: dict[str, Any]) -> Item:
        return Item(
            namespace=tuple(record.get("namespace") or []),
            key=str(record.get("key") or ""),
            value=dict(record.get("value") or {}),
            created_at=_parse_datetime(str(record.get("created_at") or "")),
            updated_at=_parse_datetime(str(record.get("updated_at") or "")),
        )

    def _record_to_search_item(self, record: dict[str, Any], *, score: float | None = None) -> SearchItem:
        return SearchItem(
            namespace=tuple(record.get("namespace") or []),
            key=str(record.get("key") or ""),
            value=dict(record.get("value") or {}),
            created_at=_parse_datetime(str(record.get("created_at") or "")),
            updated_at=_parse_datetime(str(record.get("updated_at") or "")),
            score=score,
        )

    def _expires_at(self, ttl_minutes: float | None) -> str | None:
        if ttl_minutes is None:
            return None
        return _isoformat(_utcnow() + timedelta(minutes=float(ttl_minutes)))

    def _load_record(self, namespace_path_value: str, item_key: str) -> dict[str, Any] | None:
        with self._env.begin() as txn:
            raw = txn.get(item_storage_key(namespace_path_value, item_key), db=self._items)
        return self._deserialize_record(raw)

    def _delete_records_locked(self, identities: list[tuple[str, str]]) -> int:
        if not identities:
            return 0
        cleared: list[tuple[str, str]] = []
        with self._env.begin(write=True) as txn:
            for namespace_path_value, item_key in identities:
                item_key_bytes = item_storage_key(namespace_path_value, item_key)
                record = self._deserialize_record(txn.get(item_key_bytes, db=self._items))
                if record is None:
                    continue
                txn.delete(item_key_bytes, db=self._items)
                expires_at = str(record.get("expires_at") or "").strip()
                if expires_at:
                    txn.delete(expiry_storage_key(expires_at, namespace_path_value, item_key), db=self._expiry)
                cleared.append((namespace_path_value, item_key))
        for namespace_path_value, item_key in cleared:
            self._index.replace_item(namespace_path_value, item_key, [])
        return len(cleared)

    def _refresh_ttl_locked(self, namespace_path_value: str, item_key: str, expires_at: str | None) -> None:
        with self._env.begin(write=True) as txn:
            item_key_bytes = item_storage_key(namespace_path_value, item_key)
            record = self._deserialize_record(txn.get(item_key_bytes, db=self._items))
            if record is None:
                return
            previous_expires_at = str(record.get("expires_at") or "").strip()
            record["accessed_at"] = _isoformat(_utcnow())
            record["expires_at"] = expires_at
            txn.put(item_key_bytes, json_dumps(record).encode("utf-8"), db=self._items)
            if previous_expires_at:
                txn.delete(expiry_storage_key(previous_expires_at, namespace_path_value, item_key), db=self._expiry)
            if expires_at:
                txn.put(expiry_storage_key(expires_at, namespace_path_value, item_key), b"", db=self._expiry)

    def _index_rows_for_value(
        self,
        *,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        index_fields: list[str],
        updated_at: str,
    ) -> list[dict[str, Any]]:
        if self._embedder is None or self._vector_dim is None:
            return []
        namespace_path_value = namespace_path(namespace)
        rows: list[dict[str, Any]] = []
        for index_path in index_fields:
            for idx, text in enumerate(extract_text(value, index_path)):
                if not text.strip():
                    continue
                rows.append(
                    {
                        "row_id": hashlib.sha1(f"{namespace_path_value}:{key}:{index_path}:{idx}".encode("utf-8")).hexdigest(),
                        "namespace_path": namespace_path_value,
                        "item_key": key,
                        "index_path": index_path,
                        "text": text,
                        "vector": self._embed(text),
                        "updated_at": updated_at,
                    }
                )
        return rows

    def _rebuild_index_locked(self) -> int:
        if self._embedder is None or self._vector_dim is None:
            self._index.replace_all([])
            return self.item_count()
        index_rows: list[dict[str, Any]] = []
        record_count = 0
        with self._env.begin() as txn:
            cursor = txn.cursor(db=self._items)
            for raw_key, raw_value in cursor:
                namespace_path_value, item_key = parse_item_storage_key(bytes(raw_key))
                record = self._deserialize_record(raw_value)
                if record is None:
                    continue
                expires_at = str(record.get("expires_at") or "").strip()
                if expires_at and _parse_datetime(expires_at) <= _utcnow():
                    continue
                record_count += 1
                index_rows.extend(
                    self._index_rows_for_value(
                        namespace=tuple(record.get("namespace") or namespace_tuple(namespace_path_value)),
                        key=str(record.get("key") or item_key),
                        value=dict(record.get("value") or {}),
                        index_fields=list(record.get("index_fields") or []),
                        updated_at=str(record.get("updated_at") or _isoformat(_utcnow())),
                    )
                )
        self._index.replace_all(index_rows)
        return record_count

    def _iter_namespace_records(self, namespace_prefix: tuple[str, ...]) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
        prefix_path = namespace_path(namespace_prefix)
        prefix_bytes = prefix_path.encode("utf-8")
        active: list[dict[str, Any]] = []
        expired: list[tuple[str, str]] = []
        now = _utcnow()
        with self._env.begin() as txn:
            cursor = txn.cursor(db=self._items)
            if prefix_path:
                found = cursor.set_range(prefix_bytes)
                while found:
                    raw_key = bytes(cursor.key())
                    if not raw_key.startswith(prefix_bytes):
                        break
                    namespace_path_value, item_key = parse_item_storage_key(raw_key)
                    if not namespace_matches(namespace_tuple(namespace_path_value), namespace_prefix):
                        found = cursor.next()
                        continue
                    record = self._deserialize_record(cursor.value())
                    if record is not None:
                        expires_at = str(record.get("expires_at") or "").strip()
                        if expires_at and _parse_datetime(expires_at) <= now:
                            expired.append((namespace_path_value, item_key))
                        else:
                            active.append(
                                {
                                    "namespace_path": namespace_path_value,
                                    "item_key": item_key,
                                    "record": record,
                                }
                            )
                    found = cursor.next()
            else:
                for raw_key, raw_value in cursor:
                    namespace_path_value, item_key = parse_item_storage_key(bytes(raw_key))
                    record = self._deserialize_record(raw_value)
                    if record is None:
                        continue
                    expires_at = str(record.get("expires_at") or "").strip()
                    if expires_at and _parse_datetime(expires_at) <= now:
                        expired.append((namespace_path_value, item_key))
                        continue
                    active.append(
                        {
                            "namespace_path": namespace_path_value,
                            "item_key": item_key,
                            "record": record,
                        }
                    )
        return active, expired

    def _put_locked(self, op: PutOp) -> None:
        namespace_path_value = namespace_path(op.namespace)
        item_key_value = str(op.key)
        if op.value is None:
            self._delete_records_locked([(namespace_path_value, item_key_value)])
            return
        now = _isoformat(_utcnow())
        expires_at = self._expires_at(op.ttl)
        index_fields = list(self._default_index_fields if op.index is None else ([] if op.index is False else op.index))
        item_key_bytes = item_storage_key(namespace_path_value, item_key_value)
        with self._env.begin(write=True) as txn:
            existing = self._deserialize_record(txn.get(item_key_bytes, db=self._items))
            created_at = str(existing.get("created_at") or now) if existing is not None else now
            previous_expires_at = str(existing.get("expires_at") or "").strip() if existing is not None else ""
            payload = self._serialize_record(
                namespace=op.namespace,
                key=item_key_value,
                value=dict(op.value or {}),
                created_at=created_at,
                updated_at=now,
                accessed_at=now,
                expires_at=expires_at,
                index_fields=index_fields,
            )
            txn.put(item_key_bytes, json_dumps(payload).encode("utf-8"), db=self._items)
            if previous_expires_at:
                txn.delete(expiry_storage_key(previous_expires_at, namespace_path_value, item_key_value), db=self._expiry)
            if expires_at:
                txn.put(expiry_storage_key(expires_at, namespace_path_value, item_key_value), b"", db=self._expiry)
        rows = self._index_rows_for_value(
            namespace=op.namespace,
            key=item_key_value,
            value=dict(op.value or {}),
            index_fields=index_fields,
            updated_at=now,
        )
        self._index.replace_item(namespace_path_value, item_key_value, rows)

    def _get_locked(self, op: GetOp) -> Item | None:
        namespace_path_value = namespace_path(op.namespace)
        record = self._load_record(namespace_path_value, op.key)
        if record is None:
            return None
        expires_at = str(record.get("expires_at") or "").strip()
        if expires_at and _parse_datetime(expires_at) <= _utcnow():
            self._delete_records_locked([(namespace_path_value, op.key)])
            return None
        if op.refresh_ttl and expires_at:
            self._refresh_ttl_locked(namespace_path_value, op.key, self._expires_at(self.ttl_config.get("default_ttl")))
            record["expires_at"] = self._expires_at(self.ttl_config.get("default_ttl"))
        return self._record_to_item(record)

    def _search_locked(self, op: SearchOp) -> list[SearchItem]:
        active, expired = self._iter_namespace_records(op.namespace_prefix)
        if expired:
            self._delete_records_locked(expired)
        if op.filter:
            filtered: list[dict[str, Any]] = []
            for entry in active:
                value = dict((entry.get("record") or {}).get("value") or {})
                if all(compare_filter(value.get(key), expected) for key, expected in op.filter.items()):
                    filtered.append(entry)
            active = filtered
        scored: list[tuple[float | None, dict[str, Any]]] = []
        query_value = (op.query or "").strip()
        if query_value:
            vector_score_map: dict[tuple[str, str], float] = {}
            if self._embedder is not None and self._vector_dim is not None:
                vector_hits = self._index.search(
                    namespace_prefix=op.namespace_prefix,
                    vector=self._embed(query_value),
                    limit=max(op.limit + op.offset, 1),
                )
                for hit in vector_hits:
                    score = 1.0 / (1.0 + max(float(hit.get("_distance") or 0.0), 0.0))
                    identity = (str(hit.get("namespace_path") or ""), str(hit.get("item_key") or ""))
                    if score > vector_score_map.get(identity, float("-inf")):
                        vector_score_map[identity] = score
            query_lower = query_value.lower()
            query_terms = [term.lower() for term in TOKEN_RE.findall(query_lower)]
            for entry in active:
                value = dict((entry.get("record") or {}).get("value") or {})
                content = json_dumps(value).lower()
                lexical = float(content.count(query_lower)) * 5.0 if query_lower in content else 0.0
                for term in query_terms:
                    lexical += float(content.count(term))
                semantic = vector_score_map.get((str(entry.get("namespace_path") or ""), str(entry.get("item_key") or "")), 0.0)
                score = lexical + semantic
                if score > 0:
                    scored.append((score, entry))
            scored.sort(key=lambda item: item[0], reverse=True)
            reranked = self._rerank_scored_entries(query_value, scored, limit=max(op.limit + op.offset, 1))
            if reranked is not None:
                scored = reranked
        else:
            active.sort(key=lambda entry: str((entry.get("record") or {}).get("updated_at") or ""), reverse=True)
            scored = [(None, entry) for entry in active]
        selected = scored[op.offset : op.offset + op.limit]
        if op.refresh_ttl:
            refreshed = self._expires_at(self.ttl_config.get("default_ttl"))
            for _, entry in selected:
                record = dict(entry.get("record") or {})
                if record.get("expires_at"):
                    self._refresh_ttl_locked(str(entry.get("namespace_path") or ""), str(entry.get("item_key") or ""), refreshed)
        return [self._record_to_search_item(dict(entry.get("record") or {}), score=score) for score, entry in selected]

    def _rerank_scored_entries(
        self,
        query: str,
        scored: list[tuple[float | None, dict[str, Any]]],
        *,
        limit: int,
    ) -> list[tuple[float | None, dict[str, Any]]] | None:
        if self._reranker is None or not scored:
            return None
        head_size = min(len(scored), max(limit * 4, 12))
        head = scored[:head_size]
        documents = [self._document_text_for_rerank(dict((entry.get("record") or {}).get("value") or {})) for _, entry in head]
        try:
            reranked = self._reranker.rerank(query=query, documents=documents, top_n=head_size)
        except ProviderRequestError:
            return None
        rerank_scores = {int(item.get("index", -1)): float(item.get("relevance_score", 0.0) or 0.0) for item in reranked}
        if not rerank_scores:
            return None
        rescored = [
            (((rerank_scores.get(index, 0.0) * 100.0) + float(base_score or 0.0)), entry)
            for index, (base_score, entry) in enumerate(head)
        ]
        rescored.sort(key=lambda item: item[0], reverse=True)
        return rescored + scored[head_size:]

    def _document_text_for_rerank(self, value: dict[str, Any]) -> str:
        parts: list[str] = []
        summary = str(value.get("summary") or "").strip()
        content = value.get("content")
        if summary:
            parts.append(summary)
        if isinstance(content, str) and content.strip():
            parts.append(content.strip())
        elif isinstance(content, dict):
            for candidate in ("summary", "content", "text"):
                text = str(content.get(candidate) or "").strip()
                if text:
                    parts.append(text)
        if parts:
            return "\n".join(parts[:2])
        return json_dumps(value)

    def _list_namespaces_locked(self, op: ListNamespacesOp) -> list[tuple[str, ...]]:
        namespaces: list[tuple[str, ...]] = []
        expired: list[tuple[str, str]] = []
        seen: set[tuple[str, ...]] = set()
        with self._env.begin() as txn:
            cursor = txn.cursor(db=self._items)
            for raw_key, raw_value in cursor:
                namespace_path_value, item_key = parse_item_storage_key(bytes(raw_key))
                record = self._deserialize_record(raw_value)
                if record is None:
                    continue
                expires_at = str(record.get("expires_at") or "").strip()
                if expires_at and _parse_datetime(expires_at) <= _utcnow():
                    expired.append((namespace_path_value, item_key))
                    continue
                namespace_value = tuple(record.get("namespace") or namespace_tuple(namespace_path_value))
                if namespace_value in seen:
                    continue
                seen.add(namespace_value)
                namespaces.append(namespace_value)
        if expired:
            self._delete_records_locked(expired)
        if op.match_conditions:
            namespaces = [ns for ns in namespaces if all(match_condition(ns, condition) for condition in op.match_conditions)]
        if op.max_depth is not None:
            namespaces = sorted({ns[: op.max_depth] for ns in namespaces})
        else:
            namespaces = sorted(namespaces)
        return namespaces[op.offset : op.offset + op.limit]

    def _sweep_expired_locked(self, *, force: bool = False) -> int:
        interval = int(self.ttl_config.get("sweep_interval_minutes") or 0)
        if interval <= 0 and not force:
            return 0
        now = _utcnow()
        if not force and (now - self._last_sweep) < timedelta(minutes=interval):
            return 0
        cutoff = _isoformat(now)
        expired: list[tuple[str, str]] = []
        with self._env.begin() as txn:
            cursor = txn.cursor(db=self._expiry)
            for raw_key, _ in cursor:
                expires_at, namespace_path_value, item_key = parse_expiry_storage_key(bytes(raw_key))
                if expires_at > cutoff:
                    break
                expired.append((namespace_path_value, item_key))
        deleted = self._delete_records_locked(expired)
        self._last_sweep = now
        return deleted
