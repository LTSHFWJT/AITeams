from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol

from langchain_core.messages import AIMessage, BaseMessage, ChatMessage, HumanMessage, SystemMessage, messages_from_dict, messages_to_dict
from langchain_core.runnables import RunnableLambda
from langgraph.store.sqlite import SqliteStore
from langmem import create_manage_memory_tool, create_memory_store_manager, create_search_memory_tool
from langmem.short_term import RunningSummary, summarize_messages

from aiteams.ai_gateway import AIGateway
from aiteams.memory.scope import Scope
from aiteams.memory.store import GatewayEmbedder, GatewayReranker, TOKEN_RE
from aiteams.utils import json_dumps, json_loads, make_id, trim_text, utcnow_iso


LOGGER = logging.getLogger("aiteams.memory")

FEEDBACK_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS memory_feedback (
        feedback_id TEXT PRIMARY KEY,
        scope_key TEXT NOT NULL,
        memory_id TEXT NOT NULL,
        text TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_memory_feedback_scope ON memory_feedback(scope_key, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_memory_feedback_memory ON memory_feedback(memory_id, created_at DESC)",
]

NEGATIVE_FEEDBACK_RE = re.compile(r"(过期|失效|错误|不准确|obsolete|expired|wrong|incorrect)", re.IGNORECASE)
WORKING_MEMORY_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS working_memory_state (
        scope_key TEXT NOT NULL,
        bucket TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL,
        PRIMARY KEY(scope_key, bucket)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_working_memory_scope ON working_memory_state(scope_key, updated_at DESC)",
]


class MemoryAdapter(Protocol):
    async def load_working(self, scope: Scope) -> list[dict[str, Any]]: ...
    async def append_working(
        self,
        scope: Scope,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        runtime: dict[str, Any] | None = None,
    ) -> None: ...
    async def recall(self, scope: Scope, query: str, top_k: int = 8, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...
    async def remember(
        self,
        scope: Scope,
        records: list[dict[str, Any]],
        runtime: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...
    async def feedback(self, scope: Scope, head_id: str, text: str) -> dict[str, Any]: ...
    def configure_retrieval(self, settings: dict[str, Any] | None = None) -> dict[str, Any]: ...


class WorkingMemoryStore:
    def __init__(self, database_path: str | Path):
        path = Path(database_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        for statement in WORKING_MEMORY_SCHEMA:
            self._conn.execute(statement)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def working_snapshot(self, scope_key: str) -> list[dict[str, Any]]:
        return list(self._bucket_snapshot(scope_key, "working", []))

    def turn_snapshot(self, scope_key: str) -> list[dict[str, Any]]:
        return list(self._bucket_snapshot(scope_key, "turns", []))

    def state_snapshot(self, scope_key: str) -> dict[str, Any]:
        return dict(self._bucket_snapshot(scope_key, "state", {}))

    def save_working(self, scope_key: str, items: list[dict[str, Any]]) -> None:
        self._save_bucket(scope_key, "working", items)

    def save_turns(self, scope_key: str, items: list[dict[str, Any]]) -> None:
        self._save_bucket(scope_key, "turns", items)

    def save_state(self, scope_key: str, payload: dict[str, Any]) -> None:
        self._save_bucket(scope_key, "state", payload)

    def _bucket_snapshot(self, scope_key: str, bucket: str, default: Any) -> Any:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload_json FROM working_memory_state WHERE scope_key = ? AND bucket = ?",
                (scope_key, bucket),
            ).fetchone()
        return json_loads(row["payload_json"] if row is not None else None, default)

    def _save_bucket(self, scope_key: str, bucket: str, payload: Any) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO working_memory_state(scope_key, bucket, payload_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(scope_key, bucket) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (scope_key, bucket, json_dumps(payload), utcnow_iso()),
            )
            self._conn.commit()


class LangMemAdapter:
    def __init__(
        self,
        root_dir: str,
        *,
        vector_dim: int = 32,
        working_limit: int = 32,
        turn_limit: int = 128,
        long_term_ttl_minutes: float | None = 60.0 * 24 * 30,
        gateway: AIGateway | None = None,
        retrieval_settings: dict[str, Any] | None = None,
    ):
        self.root = Path(root_dir).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._store_path = self.root / "langgraph-store.sqlite3"
        self._working_path = self.root / "working-memory.sqlite3"
        self._working_limit = max(8, working_limit)
        self._turn_limit = max(self._working_limit * 2, turn_limit)
        self._default_vector_dim = max(int(vector_dim or 0), 8)
        self._default_index_fields = ["content", "summary", "content.content", "content.summary"]
        self._long_term_ttl = long_term_ttl_minutes
        self._gateway = gateway or AIGateway()
        self._store_lock = threading.RLock()
        self._retrieval_settings = dict(retrieval_settings or {})
        self._retrieval = {
            "embedding": {"mode": "disabled", "vector_enabled": False, "vector_dim": None},
            "rerank": {"mode": "disabled"},
        }
        self._embedder: GatewayEmbedder | None = None
        self._reranker: GatewayReranker | None = None
        self._store = self._create_store(index=None)
        self._working = WorkingMemoryStore(self._working_path)
        self._feedback_lock = threading.RLock()
        self._feedback = sqlite3.connect(str(self.root / "feedback.sqlite3"), check_same_thread=False)
        self._feedback.row_factory = sqlite3.Row
        self._feedback.execute("PRAGMA journal_mode=WAL")
        for statement in FEEDBACK_SCHEMA:
            self._feedback.execute(statement)
        self._feedback.commit()
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._maintenance_stop = threading.Event()
        self._maintenance_thread: threading.Thread | None = None
        self._local_summary_model = RunnableLambda(self._local_summary_response)
        self.configure_retrieval(retrieval_settings)

    async def load_working(self, scope: Scope) -> list[dict[str, Any]]:
        return self._working.working_snapshot(scope.key)

    async def append_working(
        self,
        scope: Scope,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        runtime: dict[str, Any] | None = None,
    ) -> None:
        item = {
            "role": role,
            "content": content,
            "metadata": dict(metadata or {}),
            "created_at": utcnow_iso(),
        }
        turns = self._working.turn_snapshot(scope.key)
        turns.append(item)
        turns = turns[-self._turn_limit :]
        self._working.save_turns(scope.key, turns)

        state = self._working.state_snapshot(scope.key)
        messages = self._deserialize_messages(state.get("messages"))
        messages.append(self._turn_to_message(item))
        short_term = self._resolve_short_term_config(runtime)
        running_summary = self._running_summary_from_dict(state.get("running_summary"))
        compacted_messages, next_summary = self._summarize_working_messages(
            messages,
            running_summary=running_summary,
            runtime=runtime,
            short_term=short_term,
        )
        self._working.save_state(
            scope.key,
            {
                "messages": messages_to_dict(compacted_messages),
                "running_summary": self._running_summary_to_dict(next_summary),
                "short_term": short_term,
                "updated_at": utcnow_iso(),
            },
        )
        self._working.save_working(scope.key, self._messages_to_working_items(compacted_messages))

    async def recall(self, scope: Scope, query: str, top_k: int = 8, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return self._search_scope_records(scope, query.strip(), top_k=top_k, filters=filters)

    async def remember(
        self,
        scope: Scope,
        records: list[dict[str, Any]],
        runtime: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        namespace = self._namespace(scope)
        created: list[dict[str, Any]] = []
        for payload in records:
            text = trim_text(str(payload.get("text") or ""), limit=4000)
            if not text:
                continue
            memory_id = self._resolve_memory_key(scope, payload)
            value = {
                "content": text,
                "summary": trim_text(str(payload.get("summary") or text), limit=240),
                "role": payload.get("role"),
                "memory_type": str(payload.get("layer") or payload.get("memory_type") or "semantic"),
                "importance": float(payload.get("importance", 0.5) or 0.5),
                "confidence": float(payload.get("confidence", 0.7) or 0.7),
                "fact_key": payload.get("fact_key"),
                "metadata": dict(payload.get("metadata") or {}),
                "source_run_id": scope.run_id,
                "source_agent_id": scope.agent_id,
                "expires_at": payload.get("expires_at"),
                "updated_by": "langmem-adapter",
            }
            ttl = self._ttl_from_payload(payload)
            self._store.put(
                namespace,
                memory_id,
                value,
                index=list(self._default_index_fields),
                ttl=ttl,
            )
            created.append(
                {
                    "head_id": memory_id,
                    "memory_id": memory_id,
                    "text": text,
                    "metadata": dict(value["metadata"]),
                    "layer": "longterm",
                    "namespace": scope.namespace,
                }
            )
        self._maintain_long_term(scope)
        for item in created[-2:]:
            await self.append_working(
                scope,
                "memory",
                item["text"],
                metadata={"memory_id": item["memory_id"], "memory_layer": "longterm"},
                runtime=runtime,
            )
        self._schedule_background_reflection(scope, runtime)
        return created

    async def feedback(self, scope: Scope, head_id: str, text: str) -> dict[str, Any]:
        payload = {
            "feedback_id": make_id("fb"),
            "memory_id": head_id,
            "scope_key": scope.key,
            "text": text,
            "created_at": utcnow_iso(),
        }
        with self._feedback_lock:
            self._feedback.execute(
                """
                INSERT INTO memory_feedback(feedback_id, scope_key, memory_id, text, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (payload["feedback_id"], payload["scope_key"], payload["memory_id"], payload["text"], payload["created_at"]),
            )
            self._feedback.commit()
        if NEGATIVE_FEEDBACK_RE.search(text):
            item = self._store.get(self._namespace(scope), head_id)
            if item is not None:
                value = self._normalize_store_value(item.value)
                value["confidence"] = max(0.1, float(value.get("confidence", 0.7)) - 0.2)
                value["importance"] = max(0.1, float(value.get("importance", 0.5)) - 0.1)
                self._store.put(
                    self._namespace(scope),
                    head_id,
                    value,
                    index=list(self._default_index_fields),
                    ttl=self._long_term_ttl,
                )
        return payload

    def builtin_search(
        self,
        scopes: list[Scope],
        *,
        query: str,
        top_k: int = 8,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_query = query.strip()
        limit = max(int(top_k or 8), 1)
        merged: list[dict[str, Any]] = []
        seen: set[str] = set()
        for scope in scopes:
            for item in self._search_scope_records(scope, normalized_query, top_k=limit, filters=filters):
                identity = str(item.get("memory_id") or item.get("head_id") or "")
                if identity and identity in seen:
                    continue
                if identity:
                    seen.add(identity)
                merged.append(item)
        merged.sort(
            key=lambda item: (
                float(item.get("score") or 0.0),
                float(item.get("importance") or 0.0),
                str(item.get("updated_at") or ""),
            ),
            reverse=True,
        )
        return {
            "query": normalized_query,
            "count": len(merged[:limit]),
            "items": merged[:limit],
            "scopes": [self._scope_payload(scope) for scope in scopes],
        }

    def builtin_manage(
        self,
        scope: Scope,
        *,
        operation: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_operation = operation.strip().lower()
        content = dict(payload or {})
        namespace = self._namespace(scope)
        items: list[dict[str, Any]] = []
        if normalized_operation in {"create", "update", "upsert"}:
            records = [dict(item) for item in list(content.get("records") or []) if isinstance(item, dict)]
            record = content.get("record")
            if isinstance(record, dict):
                records.insert(0, dict(record))
            for record_payload in records:
                memory_id = (
                    str(
                        record_payload.get("memory_id")
                        or record_payload.get("id")
                        or record_payload.get("key")
                        or content.get("memory_id")
                        or ""
                    ).strip()
                    or self._resolve_memory_key(scope, record_payload)
                )
                text = trim_text(str(record_payload.get("text") or record_payload.get("content") or ""), limit=4000)
                if not text:
                    continue
                value = {
                    "content": text,
                    "summary": trim_text(str(record_payload.get("summary") or text), limit=240),
                    "role": record_payload.get("role"),
                    "memory_type": str(record_payload.get("layer") or record_payload.get("memory_type") or "semantic"),
                    "importance": float(record_payload.get("importance", 0.5) or 0.5),
                    "confidence": float(record_payload.get("confidence", 0.7) or 0.7),
                    "fact_key": record_payload.get("fact_key"),
                    "metadata": dict(record_payload.get("metadata") or {}),
                    "source_run_id": scope.run_id,
                    "source_agent_id": scope.agent_id,
                    "expires_at": record_payload.get("expires_at"),
                    "updated_by": "builtin.memory.manage",
                }
                self._store.put(
                    namespace,
                    memory_id,
                    value,
                    index=list(self._default_index_fields),
                    ttl=self._ttl_from_payload(record_payload),
                )
                stored = self._store.get(namespace, memory_id)
                if stored is not None:
                    items.append(self._store_item_to_memory_record(scope, stored))
            self._maintain_long_term(scope)
            return {
                "operation": "upsert" if normalized_operation == "upsert" else normalized_operation,
                "count": len(items),
                "items": items,
                "scope": self._scope_payload(scope),
            }
        if normalized_operation == "delete":
            candidate_ids = [
                str(item).strip()
                for item in [
                    content.get("memory_id"),
                    *(content.get("memory_ids") or []),
                ]
                if str(item).strip()
            ]
            records = [dict(item) for item in list(content.get("records") or []) if isinstance(item, dict)]
            for record_payload in records:
                identifier = str(record_payload.get("memory_id") or record_payload.get("id") or record_payload.get("key") or "").strip()
                if identifier:
                    candidate_ids.append(identifier)
            deleted = 0
            for memory_id in sorted(set(candidate_ids)):
                if not memory_id:
                    continue
                self._store.delete(namespace, memory_id)
                deleted += 1
            return {
                "operation": "delete",
                "count": deleted,
                "items": [{"memory_id": memory_id} for memory_id in sorted(set(candidate_ids)) if memory_id],
                "scope": self._scope_payload(scope),
            }
        raise ValueError(f"Unsupported memory manage operation `{operation}`.")

    def builtin_background_reflection(
        self,
        scope: Scope,
        *,
        runtime: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        model = self._langmem_model(runtime)
        messages = self._reflection_messages(scope)
        if model is None or not messages:
            return {
                "status": "skipped",
                "reason": "model_unavailable" if model is None else "no_messages",
                "scope": self._scope_payload(scope),
            }
        try:
            self._schedule_background_reflection(scope, runtime)
        except RuntimeError:
            return {
                "status": "skipped",
                "reason": "event_loop_unavailable",
                "scope": self._scope_payload(scope),
            }
        return {
            "status": "scheduled",
            "message_count": len(messages),
            "scope": self._scope_payload(scope),
        }

    def langmem_tools(self, scope: Scope) -> dict[str, Any]:
        namespace = self._namespace(scope)
        suffix = str(abs(hash(scope.key)))
        return {
            "search": create_search_memory_tool(namespace, store=self._store, name=f"search_memory_{suffix}"),
            "manage": create_manage_memory_tool(namespace, store=self._store, name=f"manage_memory_{suffix}"),
        }

    def configure_retrieval(self, settings: dict[str, Any] | None = None) -> dict[str, Any]:
        config = dict(settings or {})
        embedding = dict(config.get("embedding") or {})
        rerank = dict(config.get("rerank") or {})
        next_embedder = self._build_embedder(embedding)
        next_reranker = self._build_reranker(rerank)
        next_embedding_payload = self._retrieval_embedding_payload(embedding, embedder=next_embedder)
        next_rerank_payload = self._retrieval_rerank_payload(rerank, reranker=next_reranker)
        previous_embedding = dict(self._retrieval.get("embedding") or {})
        embedding_reindexed = (
            bool(previous_embedding.get("vector_enabled")) != bool(next_embedding_payload.get("vector_enabled"))
            or str(previous_embedding.get("model_name") or "") != str(next_embedding_payload.get("model_name") or "")
            or int(previous_embedding.get("vector_dim") or 0) != int(next_embedding_payload.get("vector_dim") or 0)
        )
        reindexed_items = 0
        with self._store_lock:
            self._embedder = next_embedder
            self._reranker = next_reranker
            self._retrieval_settings = config
            self._retrieval = {
                "embedding": next_embedding_payload,
                "rerank": next_rerank_payload,
            }
            if embedding_reindexed:
                reindexed_items = int(self._store.conn.execute("SELECT COUNT(*) FROM store").fetchone()[0])
        return {
            "embedding_reindexed": embedding_reindexed,
            "reindexed_items": reindexed_items,
            "retrieval": self._retrieval,
        }

    def run_maintenance_once(self, *, force: bool = False) -> dict[str, Any]:
        deleted = self._store.sweep_ttl() if force else self._store.sweep_ttl()
        return {
            "deleted": deleted,
            "checked_at": utcnow_iso(),
            "force": force,
        }

    def start_background_maintenance(self, *, interval_seconds: float | None = None) -> None:
        if self._maintenance_thread and self._maintenance_thread.is_alive():
            return
        self._maintenance_stop.clear()
        interval = max(float(interval_seconds or self._maintenance_interval_seconds()), 1.0)
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            args=(interval,),
            name="aiteams-memory-maintenance",
            daemon=True,
        )
        self._maintenance_thread.start()

    def stop_background_maintenance(self) -> None:
        self._maintenance_stop.set()
        if self._maintenance_thread and self._maintenance_thread.is_alive():
            self._maintenance_thread.join(timeout=2.0)
        self._maintenance_thread = None

    def storage_info(self) -> dict[str, Any]:
        return {
            "runtime": "langgraph-official",
            "memory_runtime": "langmem-official",
            "store": {"driver": "sqlite", "path": str(self._store_path)},
            "kv": {"driver": "sqlite", "path": str(self._working_path)},
            "vector": {"driver": "sqlite", "path": str(self._store_path)},
            "feedback": {"driver": "sqlite", "path": str(self.root / "feedback.sqlite3")},
            "retrieval": self._retrieval,
        }

    def close(self) -> None:
        self.stop_background_maintenance()
        for task in list(self._background_tasks):
            task.cancel()
        self._background_tasks.clear()
        self._working.close()
        with self._store_lock:
            self._close_store(self._store)
        with self._feedback_lock:
            self._feedback.close()

    def _maintenance_loop(self, interval_seconds: float) -> None:
        while not self._maintenance_stop.wait(interval_seconds):
            try:
                self._store.sweep_ttl()
            except Exception:
                LOGGER.exception("Background memory maintenance failed")

    def _create_store(self, *, index: dict[str, Any] | None) -> SqliteStore:
        connection = sqlite3.connect(
            str(self._store_path),
            check_same_thread=False,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        store = SqliteStore(
            connection,
            index=index,
            ttl={
                "refresh_on_read": True,
                "default_ttl": self._long_term_ttl,
                "sweep_interval_minutes": 10,
            },
        )
        store.setup()
        return store

    def _close_store(self, store: SqliteStore) -> None:
        stopper = getattr(store, "stop_ttl_sweeper", None)
        if callable(stopper):
            stopper(timeout=0.2)
        connection = getattr(store, "conn", None)
        if connection is not None:
            connection.close()

    def _maintenance_interval_seconds(self) -> float:
        return 600.0

    def _build_embedder(self, settings: dict[str, Any]) -> GatewayEmbedder | None:
        mode = str(settings.get("mode") or "disabled").strip().lower()
        if mode != "provider":
            return None
        provider = dict(settings.get("provider") or {})
        model_name = str(settings.get("model") or settings.get("model_name") or "").strip()
        if not provider or not model_name:
            return None
        return GatewayEmbedder(self._gateway, provider, model_name)

    def _build_reranker(self, settings: dict[str, Any]) -> GatewayReranker | None:
        mode = str(settings.get("mode") or "disabled").strip().lower()
        if mode != "provider":
            return None
        provider = dict(settings.get("provider") or {})
        model_name = str(settings.get("model") or settings.get("model_name") or "").strip()
        if not provider or not model_name:
            return None
        return GatewayReranker(self._gateway, provider, model_name)

    def _retrieval_embedding_payload(self, settings: dict[str, Any], *, embedder: GatewayEmbedder | None) -> dict[str, Any]:
        if embedder is None:
            return {"mode": "disabled", "vector_enabled": False, "vector_dim": None}
        return {
            "mode": "provider",
            "provider_id": settings.get("provider_id"),
            "provider_name": settings.get("provider_name"),
            "provider_type": settings.get("provider_type"),
            "model_name": settings.get("model_name") or settings.get("model"),
            "vector_enabled": True,
            "vector_dim": int(embedder.dimension),
        }

    def _retrieval_rerank_payload(self, settings: dict[str, Any], *, reranker: GatewayReranker | None) -> dict[str, Any]:
        if reranker is None:
            return {"mode": "disabled"}
        return {
            "mode": "provider",
            "provider_id": settings.get("provider_id"),
            "provider_name": settings.get("provider_name"),
            "provider_type": settings.get("provider_type"),
            "model_name": settings.get("model_name") or settings.get("model"),
        }

    def _namespace(self, scope: Scope) -> tuple[str, ...]:
        parts = ["workspace", scope.workspace_id, "project", scope.project_id, "memory", scope.namespace]
        if scope.team_id:
            parts.extend(["team", scope.team_id])
        if scope.agent_id:
            parts.extend(["agent", scope.agent_id])
        if scope.run_id:
            parts.extend(["run", scope.run_id])
        if scope.user_id:
            parts.extend(["user", scope.user_id])
        if scope.session_id:
            parts.extend(["session", scope.session_id])
        return tuple(parts)

    def _scope_payload(self, scope: Scope) -> dict[str, Any]:
        return {
            "workspace_id": scope.workspace_id,
            "project_id": scope.project_id,
            "namespace": scope.namespace,
            "agent_id": scope.agent_id,
            "run_id": scope.run_id,
            "team_id": scope.team_id,
            "user_id": scope.user_id,
            "session_id": scope.session_id,
            "scope_key": scope.key,
        }

    def _search_working(self, scope: Scope, query: str, *, top_k: int) -> list[dict[str, Any]]:
        items = self._working.working_snapshot(scope.key)
        query_lower = query.lower().strip()
        scored: list[tuple[int, dict[str, Any]]] = []
        for item in items:
            content = str(item.get("content") or "")
            if not query_lower or query_lower in content.lower():
                score = 100 if not query_lower else max(1, content.lower().count(query_lower))
                scored.append(
                    (
                        score,
                        {
                            "head_id": str((item.get("metadata") or {}).get("memory_id") or ""),
                            "text": content,
                            "metadata": dict(item.get("metadata") or {}) | {"working_memory": True},
                            "layer": "working",
                            "role": item.get("role"),
                        },
                    )
                )
        scored.sort(key=lambda entry: entry[0], reverse=True)
        return [item for _, item in scored[:top_k]]

    def _search_scope_records(
        self,
        scope: Scope,
        query: str,
        *,
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        namespace = self._namespace(scope)
        results = self._search_store_items(
            namespace,
            query=query,
            top_k=max(top_k * 2, top_k),
            filters=filters,
        )
        records: list[dict[str, Any]] = []
        for item in results:
            records.append(self._store_item_to_memory_record(scope, item))
            if len(records) >= top_k:
                break
        if records:
            return records
        return self._search_working(scope, query, top_k=top_k)

    def _search_store_items(
        self,
        namespace: tuple[str, ...],
        *,
        query: str,
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[Any]:
        normalized_query = query.strip()
        limit = max(top_k, 1)
        items = self._list_store_items(namespace, filters=filters, limit=max(limit * 4, 64))
        if normalized_query and self._supports_vector_search():
            items = self._semantically_rank_items(items, query=normalized_query, limit=limit)
        elif normalized_query:
            items = self._lexically_rank_items(items, query=normalized_query, limit=limit)
        if normalized_query and self._reranker is not None and items:
            items = self._rerank_items(query=normalized_query, items=items, limit=limit)
        return items[:limit]

    def _list_store_items(
        self,
        namespace: tuple[str, ...],
        *,
        filters: dict[str, Any] | None = None,
        limit: int,
    ) -> list[Any]:
        page_size = max(min(limit, 128), 32)
        offset = 0
        items: list[Any] = []
        while len(items) < limit:
            page = list(
                self._store.search(
                    namespace,
                    filter=dict(filters or {}) or None,
                    limit=page_size,
                    offset=offset,
                )
            )
            if not page:
                break
            items.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return items[:limit]

    def _supports_vector_search(self) -> bool:
        return self._embedder is not None

    def _lexically_rank_items(self, items: list[Any], *, query: str, limit: int) -> list[Any]:
        query_lower = query.lower().strip()
        if not query_lower:
            return items[:limit]
        query_terms = [term.lower() for term in TOKEN_RE.findall(query_lower)]
        scored: list[tuple[float, Any]] = []
        for item in items:
            value = self._normalize_store_value(item.value)
            haystack = "\n".join(
                part
                for part in [
                    str(value.get("summary") or "").strip(),
                    str(value.get("content") or "").strip(),
                    json_dumps(value.get("metadata") or {}),
                ]
                if part
            ).lower()
            score = float(haystack.count(query_lower)) * 5.0 if query_lower in haystack else 0.0
            for term in query_terms:
                score += float(haystack.count(term))
            if score <= 0:
                continue
            item.score = score
            scored.append((score, item))
        scored.sort(key=lambda entry: entry[0], reverse=True)
        return [item for _, item in scored[:limit]]

    def _semantically_rank_items(self, items: list[Any], *, query: str, limit: int) -> list[Any]:
        if self._embedder is None:
            return self._lexically_rank_items(items, query=query, limit=limit)
        query_vector = self._embedder.embed_text(query)
        query_terms = [term.lower() for term in TOKEN_RE.findall(query.lower().strip())]
        scored: list[tuple[float, Any]] = []
        for item in items:
            value = self._normalize_store_value(item.value)
            document = self._document_text_for_rerank(value)
            semantic = sum(left * right for left, right in zip(query_vector, self._embedder.embed_text(document), strict=False))
            lexical = 0.0
            document_lower = document.lower()
            if query.lower().strip() in document_lower:
                lexical += float(document_lower.count(query.lower().strip())) * 5.0
            for term in query_terms:
                lexical += float(document_lower.count(term))
            score = (semantic * 100.0) + lexical
            if score <= 0:
                continue
            item.score = score
            scored.append((score, item))
        scored.sort(key=lambda entry: entry[0], reverse=True)
        return [item for _, item in scored[:limit]]

    def _rerank_items(self, *, query: str, items: list[Any], limit: int) -> list[Any]:
        head_size = min(len(items), max(limit * 2, 8))
        head = list(items[:head_size])
        documents = [self._document_text_for_rerank(self._normalize_store_value(item.value)) for item in head]
        try:
            reranked = self._reranker.rerank(query=query, documents=documents, top_n=head_size)
        except Exception:
            return items
        relevance = {int(item.get("index", -1)): float(item.get("relevance_score", 0.0) or 0.0) for item in reranked}
        rescored: list[tuple[float, Any]] = []
        for index, item in enumerate(head):
            score = (relevance.get(index, 0.0) * 100.0) + float(getattr(item, "score", 0.0) or 0.0)
            item.score = score
            rescored.append((score, item))
        rescored.sort(key=lambda entry: entry[0], reverse=True)
        return [item for _, item in rescored] + list(items[head_size:])

    def _document_text_for_rerank(self, value: dict[str, Any]) -> str:
        summary = str(value.get("summary") or "").strip()
        content = str(value.get("content") or "").strip()
        if summary and content and summary != content:
            return f"{summary}\n{content}"
        return summary or content or json_dumps(value)

    def _store_item_to_memory_record(self, scope: Scope, item: Any) -> dict[str, Any]:
        value = self._normalize_store_value(item.value)
        updated_at = getattr(item, "updated_at", None)
        created_at = getattr(item, "created_at", None)
        return {
            "head_id": str(item.key),
            "memory_id": str(item.key),
            "text": str(value.get("content") or ""),
            "summary": str(value.get("summary") or ""),
            "metadata": dict(value.get("metadata") or {}),
            "layer": "longterm",
            "memory_type": str(value.get("memory_type") or "semantic"),
            "importance": float(value.get("importance", 0.0) or 0.0),
            "confidence": float(value.get("confidence", 0.0) or 0.0),
            "namespace": scope.namespace,
            "scope_key": scope.key,
            "updated_at": updated_at.isoformat() if updated_at is not None else None,
            "created_at": created_at.isoformat() if created_at is not None else None,
            "score": getattr(item, "score", None),
        }

    def _ttl_from_payload(self, payload: dict[str, Any]) -> float | None:
        expires_at = payload.get("expires_at")
        if expires_at in (None, ""):
            return self._long_term_ttl
        if isinstance(expires_at, (int, float)):
            return max(float(expires_at), 1.0)
        return self._long_term_ttl

    def _resolve_memory_key(self, scope: Scope, payload: dict[str, Any]) -> str:
        fact_key = str(payload.get("fact_key") or "").strip()
        namespace = self._namespace(scope)
        if fact_key:
            existing = list(self._store.search(namespace, filter={"fact_key": fact_key}, limit=1))
            if existing:
                return str(existing[0].key)
        text = trim_text(str(payload.get("text") or ""), limit=240)
        if text:
            existing = self._search_store_items(namespace, query=text, top_k=3)
            for item in existing:
                value = self._normalize_store_value(item.value)
                if trim_text(str(value.get("content") or ""), limit=240) == text:
                    return str(item.key)
        return make_id("mem")

    def _maintain_long_term(self, scope: Scope) -> None:
        namespace = self._namespace(scope)
        items = self._store.search(namespace, limit=128)
        by_fact_key: dict[str, list[Any]] = {}
        by_text: dict[str, list[Any]] = {}
        for item in items:
            value = self._normalize_store_value(item.value)
            fact_key = str(value.get("fact_key") or "").strip()
            content_key = trim_text(str(value.get("content") or ""), limit=320)
            if fact_key:
                by_fact_key.setdefault(fact_key, []).append(item)
            elif content_key:
                by_text.setdefault(content_key, []).append(item)
        for group in list(by_fact_key.values()) + [matching for matching in by_text.values() if len(matching) > 1]:
            if len(group) <= 1:
                continue
            ordered = sorted(
                group,
                key=lambda item: (
                    float(self._normalize_store_value(item.value).get("importance", 0.0) or 0.0),
                    item.updated_at,
                ),
                reverse=True,
            )
            survivor = ordered[0]
            merged_value = self._normalize_store_value(survivor.value)
            merged_value["confidence"] = max(
                float(self._normalize_store_value(item.value).get("confidence", 0.0) or 0.0) for item in ordered
            )
            merged_value["importance"] = max(
                float(self._normalize_store_value(item.value).get("importance", 0.0) or 0.0) for item in ordered
            )
            self._store.put(
                namespace,
                str(survivor.key),
                merged_value,
                index=list(self._default_index_fields),
                ttl=self._long_term_ttl,
            )
            for duplicate in ordered[1:]:
                self._store.delete(namespace, str(duplicate.key))

    def _deserialize_messages(self, payload: Any) -> list[BaseMessage]:
        if not payload:
            return []
        try:
            messages = messages_from_dict(list(payload))
        except Exception:
            return []
        return [message for message in messages if isinstance(message, BaseMessage)]

    def _turn_to_message(self, item: dict[str, Any]) -> BaseMessage:
        role = str(item.get("role") or "message")
        content = trim_text(str(item.get("content") or ""), limit=8000)
        created_at = str(item.get("created_at") or utcnow_iso())
        metadata = dict(item.get("metadata") or {})
        kwargs = {
            "id": make_id("msg"),
            "additional_kwargs": {
                "aiteams_metadata": metadata,
                "aiteams_created_at": created_at,
            },
        }
        if role in {"user", "human"}:
            return HumanMessage(content=content, **kwargs)
        if role in {"assistant", "ai"}:
            return AIMessage(content=content, **kwargs)
        if role == "system":
            return SystemMessage(content=content, **kwargs)
        return ChatMessage(role=role, content=content, **kwargs)

    def _message_role(self, message: BaseMessage) -> str:
        if isinstance(message, HumanMessage):
            return "user"
        if isinstance(message, AIMessage):
            return "assistant"
        if isinstance(message, SystemMessage):
            return "system"
        if isinstance(message, ChatMessage):
            return str(message.role or "message")
        return str(getattr(message, "type", "message") or "message")

    def _message_text(self, message: BaseMessage) -> str:
        content = getattr(message, "content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, Iterable):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    value = item.get("text") or item.get("content") or ""
                else:
                    value = item
                text = str(value or "").strip()
                if text:
                    parts.append(text)
            return "\n".join(parts)
        return str(content)

    def _messages_to_working_items(self, messages: list[BaseMessage]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for message in messages:
            additional_kwargs = dict(getattr(message, "additional_kwargs", {}) or {})
            metadata = dict(additional_kwargs.get("aiteams_metadata") or {})
            content = trim_text(self._message_text(message), limit=1600)
            if not content:
                continue
            items.append(
                {
                    "role": self._message_role(message),
                    "content": content,
                    "metadata": metadata,
                    "created_at": str(additional_kwargs.get("aiteams_created_at") or utcnow_iso()),
                }
            )
        return items[-self._working_limit :]

    def _resolve_short_term_config(self, runtime: dict[str, Any] | None) -> dict[str, Any]:
        runtime = dict(runtime or {})
        memory_profile = dict(runtime.get("memory_profile") or {})
        memory_profile_config = dict(memory_profile.get("config") or memory_profile)
        short_term = dict(memory_profile_config.get("short_term") or runtime.get("short_term") or {})
        return {
            "enabled": bool(short_term.get("enabled", True)),
            "summary_trigger_tokens": max(int(short_term.get("summary_trigger_tokens", 1800) or 1800), 256),
            "summary_max_tokens": max(int(short_term.get("summary_max_tokens", 320) or 320), 96),
        }

    def _summarize_working_messages(
        self,
        messages: list[BaseMessage],
        *,
        running_summary: RunningSummary | None,
        runtime: dict[str, Any] | None,
        short_term: dict[str, Any],
    ) -> tuple[list[BaseMessage], RunningSummary | None]:
        if not short_term.get("enabled", True):
            return messages[-self._working_limit :], None
        trigger_tokens = int(short_term.get("summary_trigger_tokens", 1800) or 1800)
        summary_tokens = min(int(short_term.get("summary_max_tokens", 320) or 320), max(trigger_tokens - 64, 96))
        model = self._langmem_model(runtime) or self._local_summary_model
        try:
            result = summarize_messages(
                list(messages),
                running_summary=running_summary,
                model=model,
                max_tokens=trigger_tokens,
                max_tokens_before_summary=trigger_tokens,
                max_summary_tokens=summary_tokens,
            )
            next_summary = result.running_summary or running_summary
            next_messages = self._decorate_running_summary_messages(list(result.messages), next_summary)
        except Exception:
            summary_message, next_summary = self._fallback_summary_message(messages, running_summary)
            tail_count = max(4, self._working_limit - 1)
            next_messages = ([summary_message] if summary_message is not None else []) + list(messages[-tail_count:])
        return next_messages[-self._working_limit :], next_summary

    def _fallback_summary_message(
        self,
        messages: list[BaseMessage],
        running_summary: RunningSummary | None,
    ) -> tuple[SystemMessage | None, RunningSummary | None]:
        lines: list[str] = []
        for message in messages[-8:]:
            content = trim_text(self._message_text(message), limit=96)
            if content:
                lines.append(f"{self._message_role(message)}: {content}")
        summary = "; ".join(lines)
        if not summary:
            return None, running_summary
        text = f"Running summary ({len(lines)} messages): {summary}"
        summarized_ids = set(running_summary.summarized_message_ids) if running_summary else set()
        last_id = running_summary.last_summarized_message_id if running_summary else None
        for message in messages[:-max(4, self._working_limit - 1)]:
            if message.id:
                summarized_ids.add(str(message.id))
                last_id = str(message.id)
        return (
            SystemMessage(
                content=text,
                id=make_id("msg"),
                additional_kwargs={
                    "aiteams_metadata": {"running_summary": True, "source": "fallback"},
                    "aiteams_created_at": utcnow_iso(),
                },
            ),
            RunningSummary(
                summary=text,
                summarized_message_ids=summarized_ids,
                last_summarized_message_id=last_id,
            ),
        )

    def _running_summary_from_dict(self, payload: Any) -> RunningSummary | None:
        if not isinstance(payload, dict):
            return None
        summary = str(payload.get("summary") or "").strip()
        if not summary:
            return None
        return RunningSummary(
            summary=summary,
            summarized_message_ids={str(item) for item in list(payload.get("summarized_message_ids") or []) if str(item).strip()},
            last_summarized_message_id=str(payload.get("last_summarized_message_id") or "") or None,
        )

    def _running_summary_to_dict(self, summary: RunningSummary | None) -> dict[str, Any] | None:
        if summary is None:
            return None
        return {
            "summary": summary.summary,
            "summarized_message_ids": sorted(str(item) for item in summary.summarized_message_ids),
            "last_summarized_message_id": summary.last_summarized_message_id,
        }

    def _decorate_running_summary_messages(
        self,
        messages: list[BaseMessage],
        summary: RunningSummary | None,
    ) -> list[BaseMessage]:
        if summary is None or not messages:
            return messages
        first = messages[0]
        if not isinstance(first, SystemMessage):
            return messages
        additional_kwargs = dict(getattr(first, "additional_kwargs", {}) or {})
        metadata = dict(additional_kwargs.get("aiteams_metadata") or {})
        metadata["running_summary"] = True
        additional_kwargs["aiteams_metadata"] = metadata
        additional_kwargs.setdefault("aiteams_created_at", utcnow_iso())
        messages[0] = SystemMessage(
            content=self._message_text(first),
            id=str(first.id or make_id("msg")),
            additional_kwargs=additional_kwargs,
        )
        return messages

    def _local_summary_response(self, prompt: Any) -> AIMessage:
        messages = prompt.messages if hasattr(prompt, "messages") else list(prompt if isinstance(prompt, list) else [prompt])
        lines: list[str] = []
        for message in messages:
            if not isinstance(message, BaseMessage):
                continue
            content = trim_text(self._message_text(message), limit=96)
            if not content:
                continue
            if getattr(message, "id", None) is None and (
                content.startswith("Create a summary of the conversation above:")
                or content.startswith("This is summary of the conversation so far:")
            ):
                continue
            lines.append(f"{self._message_role(message)}: {content}")
        summary = "; ".join(lines[-8:])
        content = f"Running summary ({len(lines)} messages): {summary}" if summary else "Running summary: no details."
        return AIMessage(content=content)

    def _langmem_model(self, runtime: dict[str, Any] | None) -> Any | None:
        runtime = dict(runtime or {})
        provider = dict(runtime.get("provider") or runtime.get("provider_profile") or {})
        provider_type = str(provider.get("provider_type") or runtime.get("provider_type") or "").strip()
        model = str(provider.get("model") or runtime.get("model") or "").strip()
        if not provider_type or provider_type == "mock" or not model:
            return None
        api_key = provider.get("api_key") or runtime.get("api_key")
        api_key_env = provider.get("api_key_env") or runtime.get("api_key_env")
        if not api_key and api_key_env:
            api_key = os.getenv(str(api_key_env))
        provider_alias = str(provider.get("custom_llm_provider") or provider.get("provider_alias") or "").strip()
        if not provider_alias:
            provider_alias = {
                "openai": "openai",
                "custom_openai": "openai",
                "azure_openai": "azure",
                "anthropic": "anthropic",
                "gemini": "gemini",
                "cohere": "cohere",
            }.get(provider_type, provider_type)
        if provider_alias in {"openai", "azure", "anthropic", "gemini", "cohere"} and not api_key:
            return None
        try:
            from langchain_litellm import ChatLiteLLM

            extra_config = dict(provider.get("extra_config") or runtime.get("extra_config") or {})
            return ChatLiteLLM(
                model=model,
                api_key=api_key,
                api_base=provider.get("base_url") or runtime.get("base_url"),
                organization=provider.get("organization") or runtime.get("organization"),
                custom_llm_provider=provider_alias or None,
                extra_headers=dict(provider.get("extra_headers") or runtime.get("extra_headers") or {}),
                max_tokens=provider.get("max_tokens") or runtime.get("max_tokens"),
                temperature=provider.get("temperature") or runtime.get("temperature"),
                model_kwargs={key: value for key, value in extra_config.items() if key != "custom_llm_provider"},
            )
        except Exception:
            return None

    def _schedule_background_reflection(self, scope: Scope, runtime: dict[str, Any] | None) -> None:
        model = self._langmem_model(runtime)
        if model is None:
            return
        messages = self._reflection_messages(scope)
        if not messages:
            return
        task = asyncio.create_task(self._run_background_reflection(scope, model, messages))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _reflection_messages(self, scope: Scope) -> list[BaseMessage]:
        messages: list[BaseMessage] = []
        for item in self._working.turn_snapshot(scope.key):
            if str(item.get("role") or "") == "memory":
                continue
            messages.append(self._turn_to_message(item))
        return messages[-24:]

    async def _run_background_reflection(self, scope: Scope, model: Any, messages: list[BaseMessage]) -> None:
        try:
            manager = create_memory_store_manager(
                model,
                namespace=self._namespace(scope),
                store=self._store,
            )
            await manager.ainvoke({"messages": messages, "max_steps": 1})
            self._maintain_long_term(scope)
        except Exception:
            return

    def _normalize_store_value(self, raw_value: Any) -> dict[str, Any]:
        value = dict(raw_value or {})
        if "kind" in value and isinstance(value.get("content"), dict):
            nested = dict(value.get("content") or {})
            metadata = dict(nested.get("metadata") or {})
            if value.get("kind"):
                metadata.setdefault("langmem_kind", value.get("kind"))
            content = trim_text(str(nested.get("content") or nested.get("text") or ""), limit=4000)
            return {
                "content": content,
                "summary": trim_text(str(nested.get("summary") or content), limit=240),
                "memory_type": str(nested.get("memory_type") or "semantic"),
                "importance": float(nested.get("importance", 0.55) or 0.55),
                "confidence": float(nested.get("confidence", 0.72) or 0.72),
                "fact_key": nested.get("fact_key"),
                "metadata": metadata,
            }
        return {
            "content": trim_text(str(value.get("content") or ""), limit=4000),
            "summary": trim_text(str(value.get("summary") or value.get("content") or ""), limit=240),
            "memory_type": str(value.get("memory_type") or value.get("layer") or "semantic"),
            "importance": float(value.get("importance", 0.5) or 0.5),
            "confidence": float(value.get("confidence", 0.7) or 0.7),
            "fact_key": value.get("fact_key"),
            "metadata": dict(value.get("metadata") or {}),
        }


AIMemoryAdapter = LangMemAdapter
