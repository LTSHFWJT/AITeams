from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Any, Callable

from aimemory.algorithms.compression import CompressionResult, compress_records, compress_text as compress_text_content
from aimemory.algorithms.dedupe import fingerprint, hamming_similarity, merge_text_fragments, semantic_similarity
from aimemory.algorithms.distill import AdaptiveDistiller, DistilledCandidate
from aimemory.algorithms.retrieval import estimate_tokens, mmr_rerank, score_record
from aimemory.algorithms.segmentation import chunk_text_units
from aimemory.backends.registry import LanceDBVectorIndex, NullGraphStore
from aimemory.core.capabilities import capability_dict
from aimemory.core.scope import CollaborationScope
from aimemory.core.settings import AIMemoryConfig
from aimemory.core.text import build_summary, extract_keywords, normalize_text, split_sentences, tokenize
from aimemory.core.utils import json_dumps, json_loads, make_id, make_uuid7, merge_metadata, utcnow_iso
from aimemory.domains.memory.models import MemoryScope, MemoryType
from aimemory.domains.skill.package import (
    default_skill_markdown,
    is_textual_skill_file,
    looks_like_skill_file_mapping,
    normalize_skill_package_inputs,
)
from aimemory.memory_intelligence.models import MemoryScopeContext, NormalizedMessage
from aimemory.providers.defaults import TextOnlyVisionProcessor
from aimemory.providers.embeddings import configure_embedding_runtime, describe_embedding_runtime, embed_text
from aimemory.querying.filters import filter_records
from aimemory.storage.lmdb.store import LMDBMemoryStore
from aimemory.storage.object_store.local import LocalObjectStore
from aimemory.storage.sqlite.database import SQLiteDatabase
from aimemory.storage.sqlite.runtime_schema import ADDITIONAL_COLUMNS, EXTRA_SCHEMA_STATEMENTS, POST_MIGRATION_SCHEMA_STATEMENTS


def _loads(value: Any, default: Any) -> Any:
    loaded = json_loads(value, default)
    return default if loaded is None else loaded


def _deserialize_row(
    row: dict[str, Any] | None,
    json_fields: tuple[str, ...] = (
        "metadata",
        "constraints",
        "resolved_items",
        "unresolved_items",
        "next_actions",
        "salience_vector",
        "capability_tags",
        "tool_affinity",
    ),
) -> dict[str, Any] | None:
    if row is None:
        return None
    item = dict(row)
    for field in json_fields:
        if field in item:
            fallback: Any = [] if field.endswith("s") or field in {"keywords", "highlights"} else {}
            item[field] = _loads(item.get(field), fallback)
    return item


def _deserialize_rows(
    rows: list[dict[str, Any]],
    json_fields: tuple[str, ...] = (
        "metadata",
        "constraints",
        "resolved_items",
        "unresolved_items",
        "next_actions",
        "salience_vector",
        "capability_tags",
        "tool_affinity",
    ),
) -> list[dict[str, Any]]:
    return [item for item in (_deserialize_row(row, json_fields) for row in rows) if item is not None]


def _domain_priority(domain: str) -> float:
    return {
        "memory": 0.06,
        "interaction": 0.04,
        "knowledge": 0.05,
        "skill": 0.05,
        "archive": -0.02,
        "execution": 0.01,
    }.get(domain, 0.0)


def _chunk_title(base_title: str | None, metadata: dict[str, Any] | None = None) -> str:
    title = str(base_title or "").strip()
    section_label = str((metadata or {}).get("section_label") or "").strip()
    if not section_label:
        return title
    if not title:
        return section_label
    return f"{title} | {section_label}"


MEMORY_TABLE_MAP = {
    str(MemoryScope.SESSION): "short_term_memories",
    str(MemoryScope.LONG_TERM): "long_term_memories",
}

MEMORY_BUCKET_MAP = {
    str(MemoryScope.SESSION): "short_term",
    str(MemoryScope.LONG_TERM): "long_term",
    "archive": "archive",
}

MEMORY_TABLE_SELECT = """
    SELECT
        id,
        bundle_id,
        content_id,
        user_id,
        agent_id,
        owner_agent_id,
        session_id,
        run_id,
        source_session_id,
        source_run_id,
        subject_type,
        subject_id,
        interaction_type,
        namespace_key,
        memory_type,
        summary,
        importance,
        status,
        source,
        metadata,
        content_format,
        created_at,
        updated_at,
        archived_at
    FROM {table}
"""


class AIMemory:
    def __init__(self, config: AIMemoryConfig | dict[str, Any] | None = None):
        self.config = AIMemoryConfig.from_value(config)
        configure_embedding_runtime(self.config.embeddings)
        self.memory_content_store = LMDBMemoryStore(self.config.lmdb_path)
        self.object_store = LocalObjectStore(self.config.object_store_path)
        self.db = SQLiteDatabase(self.config.sqlite_path)
        self._ensure_runtime_schema()
        self.vector_index = LanceDBVectorIndex(self.config)
        self.graph_store = NullGraphStore()
        self.normalizer = TextOnlyVisionProcessor()
        self.distiller = AdaptiveDistiller(self.config.memory_policy)
        self._domain_compressors: dict[str, Callable[..., Any]] = {}
        self._agent_store_api = None
        self._structured_api = None
        self._closed = False

    def _ensure_runtime_schema(self) -> None:
        self.db.ensure_schema(EXTRA_SCHEMA_STATEMENTS)
        for table_name, columns in ADDITIONAL_COLUMNS.items():
            self.db.ensure_columns(table_name, columns)
        self.db.ensure_schema(POST_MIGRATION_SCHEMA_STATEMENTS)
        self.db.execute("UPDATE sessions SET owner_agent_id = COALESCE(owner_agent_id, agent_id) WHERE owner_agent_id IS NULL")
        self.db.execute(
            """
            UPDATE sessions
            SET subject_type = COALESCE(subject_type, CASE WHEN user_id IS NOT NULL AND user_id != '' THEN 'human' ELSE 'agent' END)
            WHERE subject_type IS NULL
            """
        )
        self.db.execute(
            """
            UPDATE sessions
            SET subject_id = COALESCE(
                subject_id,
                CASE
                    WHEN subject_type = 'human' THEN user_id
                    WHEN subject_type = 'agent' THEN NULL
                    ELSE user_id
                END
            )
            WHERE subject_id IS NULL
            """
        )
        self.db.execute(
            """
            UPDATE sessions
            SET interaction_type = COALESCE(interaction_type, CASE WHEN subject_type = 'agent' THEN 'agent_agent' ELSE 'human_agent' END)
            WHERE interaction_type IS NULL
            """
        )
        self.db.execute("UPDATE runs SET owner_agent_id = COALESCE(owner_agent_id, agent_id) WHERE owner_agent_id IS NULL")
        self._normalize_memory_management_tables()
        self.db.execute("UPDATE skills SET owner_agent_id = COALESCE(owner_agent_id, owner_id) WHERE owner_agent_id IS NULL")
        self._normalize_archive_management_table()
        self._backfill_memory_bundles()

    def _normalize_memory_management_tables(self) -> None:
        for table_name in MEMORY_TABLE_MAP.values():
            self.db.execute(f"UPDATE {table_name} SET owner_agent_id = COALESCE(owner_agent_id, agent_id) WHERE owner_agent_id IS NULL")
            self.db.execute(f"UPDATE {table_name} SET source_session_id = COALESCE(source_session_id, session_id) WHERE source_session_id IS NULL")
            self.db.execute(f"UPDATE {table_name} SET source_run_id = COALESCE(source_run_id, run_id) WHERE source_run_id IS NULL")
            self.db.execute(
                f"""
                UPDATE {table_name}
                SET subject_type = COALESCE(subject_type, (SELECT s.subject_type FROM sessions s WHERE s.id = {table_name}.session_id), 'human')
                WHERE subject_type IS NULL
                """
            )
            self.db.execute(
                f"""
                UPDATE {table_name}
                SET subject_id = COALESCE(subject_id, (SELECT s.subject_id FROM sessions s WHERE s.id = {table_name}.session_id), user_id)
                WHERE subject_id IS NULL
                """
            )
            self.db.execute(
                f"""
                UPDATE {table_name}
                SET interaction_type = COALESCE(interaction_type, (SELECT s.interaction_type FROM sessions s WHERE s.id = {table_name}.session_id), 'human_agent')
                WHERE interaction_type IS NULL
                """
            )

    def _normalize_archive_management_table(self) -> None:
        self.db.execute(
            """
            UPDATE archive_memories
            SET owner_agent_id = COALESCE(owner_agent_id, (SELECT s.owner_agent_id FROM sessions s WHERE s.id = archive_memories.session_id))
            WHERE owner_agent_id IS NULL
            """
        )
        self.db.execute(
            """
            UPDATE archive_memories
            SET subject_type = COALESCE(subject_type, (SELECT s.subject_type FROM sessions s WHERE s.id = archive_memories.session_id))
            WHERE subject_type IS NULL
            """
        )
        self.db.execute(
            """
            UPDATE archive_memories
            SET subject_id = COALESCE(subject_id, (SELECT s.subject_id FROM sessions s WHERE s.id = archive_memories.session_id))
            WHERE subject_id IS NULL
            """
        )
        self.db.execute(
            """
            UPDATE archive_memories
            SET interaction_type = COALESCE(interaction_type, (SELECT s.interaction_type FROM sessions s WHERE s.id = archive_memories.session_id))
            WHERE interaction_type IS NULL
            """
        )
        self.db.execute("UPDATE archive_memories SET source_type = COALESCE(source_type, domain) WHERE source_type IS NULL")
        self._sync_text_search_index()

    def _memory_table_for_scope(self, scope: str) -> str:
        return MEMORY_TABLE_MAP.get(scope, MEMORY_TABLE_MAP[str(MemoryScope.LONG_TERM)])

    def _memory_bucket_for_scope(self, scope: str) -> str:
        return MEMORY_BUCKET_MAP.get(scope, MEMORY_BUCKET_MAP[str(MemoryScope.LONG_TERM)])

    def _memory_scope_from_table(self, table_name: str) -> str:
        for scope, candidate in MEMORY_TABLE_MAP.items():
            if candidate == table_name:
                return scope
        return str(MemoryScope.LONG_TERM)

    def _memory_bundle_scope_key(
        self,
        *,
        scope: str,
        user_id: str | None,
        owner_agent_id: str | None,
        subject_type: str | None,
        subject_id: str | None,
        interaction_type: str | None,
        namespace_key: str | None,
    ) -> str:
        return "|".join(
            [
                f"scope={scope}",
                f"user={user_id or ''}",
                f"owner={owner_agent_id or ''}",
                f"subject_type={subject_type or ''}",
                f"subject_id={subject_id or ''}",
                f"interaction={interaction_type or ''}",
                f"namespace={namespace_key or ''}",
            ]
        )

    def _bundle_payload(self, bundle_row: dict[str, Any]) -> dict[str, Any]:
        return {
            "bundle_id": bundle_row["id"],
            "scope": bundle_row["scope"],
            "scope_key": bundle_row["scope_key"],
            "user_id": bundle_row.get("user_id"),
            "owner_agent_id": bundle_row.get("owner_agent_id"),
            "subject_type": bundle_row.get("subject_type"),
            "subject_id": bundle_row.get("subject_id"),
            "interaction_type": bundle_row.get("interaction_type"),
            "namespace_key": bundle_row.get("namespace_key"),
            "items": [],
            "metadata": _loads(bundle_row.get("metadata"), {}),
            "updated_at": bundle_row.get("updated_at"),
        }

    def _ensure_memory_bundle(
        self,
        *,
        scope: str,
        user_id: str | None,
        owner_agent_id: str | None,
        subject_type: str | None,
        subject_id: str | None,
        interaction_type: str | None,
        namespace_key: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scope_key = self._memory_bundle_scope_key(
            scope=scope,
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            namespace_key=namespace_key,
        )
        existing = self.db.fetch_one("SELECT * FROM memory_bundles WHERE scope_key = ?", (scope_key,))
        now = utcnow_iso()
        if existing is not None:
            merged_metadata = merge_metadata(_loads(existing.get("metadata"), {}), metadata or {})
            self.db.execute(
                """
                UPDATE memory_bundles
                SET user_id = ?, owner_agent_id = ?, subject_type = ?, subject_id = ?, interaction_type = ?, namespace_key = ?, metadata = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    user_id,
                    owner_agent_id,
                    subject_type,
                    subject_id,
                    interaction_type,
                    namespace_key,
                    json_dumps(merged_metadata),
                    now,
                    existing["id"],
                ),
            )
            updated = self.db.fetch_one("SELECT * FROM memory_bundles WHERE id = ?", (existing["id"],))
            assert updated is not None
            return updated
        bundle_id = make_uuid7()
        self.db.execute(
            """
            INSERT INTO memory_bundles(id, scope, scope_key, user_id, owner_agent_id, subject_type, subject_id, interaction_type, namespace_key, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bundle_id,
                scope,
                scope_key,
                user_id,
                owner_agent_id,
                subject_type,
                subject_id,
                interaction_type,
                namespace_key,
                json_dumps(metadata or {}),
                now,
                now,
            ),
        )
        bundle = self.db.fetch_one("SELECT * FROM memory_bundles WHERE id = ?", (bundle_id,))
        assert bundle is not None
        self.memory_content_store.put_json(self._memory_bucket_for_scope(scope), self._bundle_payload(bundle), key=bundle_id)
        return bundle

    def _get_memory_bundle(self, scope: str, bundle_id: str, *, bundle_row: dict[str, Any] | None = None) -> dict[str, Any]:
        bundle = self.memory_content_store.get_json(self._memory_bucket_for_scope(scope), bundle_id, None)
        if isinstance(bundle, dict):
            bundle.setdefault("items", [])
            bundle.setdefault("scope", scope)
            bundle.setdefault("bundle_id", bundle_id)
            return bundle
        row = bundle_row or self.db.fetch_one("SELECT * FROM memory_bundles WHERE id = ?", (bundle_id,))
        if row is None:
            return {"bundle_id": bundle_id, "scope": scope, "items": [], "metadata": {}, "updated_at": utcnow_iso()}
        payload = self._bundle_payload(row)
        self.memory_content_store.put_json(self._memory_bucket_for_scope(scope), payload, key=bundle_id)
        return payload

    def _put_memory_bundle(self, scope: str, bundle_id: str, payload: dict[str, Any]) -> None:
        now = utcnow_iso()
        normalized = dict(payload)
        normalized["bundle_id"] = bundle_id
        normalized["scope"] = scope
        normalized["items"] = list(normalized.get("items") or [])
        normalized["updated_at"] = now
        self.memory_content_store.put_json(self._memory_bucket_for_scope(scope), normalized, key=bundle_id)
        self.db.execute("UPDATE memory_bundles SET updated_at = ? WHERE id = ?", (now, bundle_id))

    def _find_bundle_item(
        self,
        bundle: dict[str, Any] | None,
        *,
        record_id: str | None = None,
        content_id: str | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(bundle, dict):
            return None
        for item in list(bundle.get("items") or []):
            if record_id and item.get("record_id") == record_id:
                return item
            if content_id and item.get("content_id") == content_id:
                return item
        return None

    def _upsert_bundle_item(self, scope: str, bundle_id: str, item_payload: dict[str, Any]) -> dict[str, Any]:
        bundle = self._get_memory_bundle(scope, bundle_id)
        items = list(bundle.get("items") or [])
        replaced = False
        for index, item in enumerate(items):
            if item.get("record_id") == item_payload.get("record_id") or item.get("content_id") == item_payload.get("content_id"):
                items[index] = dict(item_payload)
                replaced = True
                break
        if not replaced:
            items.append(dict(item_payload))
        bundle["items"] = items
        self._put_memory_bundle(scope, bundle_id, bundle)
        return dict(item_payload)

    def _bundle_memory_item_payload(self, row: dict[str, Any], *, text: str) -> dict[str, Any]:
        return {
            "record_id": row["id"],
            "content_id": row["content_id"],
            "text": text,
            "summary": row.get("summary"),
            "importance": float(row.get("importance", 0.5) or 0.0),
            "status": row.get("status", "active"),
            "source": row.get("source"),
            "memory_type": row.get("memory_type"),
            "session_id": row.get("session_id"),
            "run_id": row.get("run_id"),
            "source_session_id": row.get("source_session_id"),
            "source_run_id": row.get("source_run_id"),
            "metadata": _loads(row.get("metadata"), {}),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "archived_at": row.get("archived_at"),
        }

    def _bundle_archive_item_payload(self, row: dict[str, Any], *, summary: str, content: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "record_id": row["id"],
            "content_id": row["content_id"],
            "summary": summary,
            "content": content,
            "status": "active",
            "domain": row.get("domain"),
            "source_id": row.get("source_id"),
            "source_type": row.get("source_type"),
            "session_id": row.get("session_id"),
            "metadata": metadata or _loads(row.get("metadata"), {}),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _backfill_memory_bundles(self) -> None:
        for table_name in MEMORY_TABLE_MAP.values():
            scope = self._memory_scope_from_table(table_name)
            rows = self.db.fetch_all(f"{self._memory_row_sql(table_name)} WHERE COALESCE(bundle_id, '') = ''")
            for row in rows:
                bundle = self._ensure_memory_bundle(
                    scope=scope,
                    user_id=row.get("user_id"),
                    owner_agent_id=row.get("owner_agent_id") or row.get("agent_id"),
                    subject_type=row.get("subject_type"),
                    subject_id=row.get("subject_id"),
                    interaction_type=row.get("interaction_type"),
                    namespace_key=row.get("namespace_key"),
                    metadata={},
                )
                text = self.memory_content_store.get_text(self._memory_bucket_for_scope(scope), row["content_id"]) or ""
                self._upsert_bundle_item(scope, bundle["id"], self._bundle_memory_item_payload({**row, "bundle_id": bundle["id"]}, text=text))
                self.db.execute(f"UPDATE {table_name} SET bundle_id = ? WHERE id = ?", (bundle["id"], row["id"]))

        archive_rows = self.db.fetch_all("SELECT * FROM archive_memories WHERE COALESCE(bundle_id, '') = ''")
        for row in archive_rows:
            bundle = self._ensure_memory_bundle(
                scope="archive",
                user_id=row.get("user_id"),
                owner_agent_id=row.get("owner_agent_id"),
                subject_type=row.get("subject_type"),
                subject_id=row.get("subject_id"),
                interaction_type=row.get("interaction_type"),
                namespace_key=row.get("namespace_key"),
                metadata={},
            )
            archive_payload = self.memory_content_store.get_json("archive", row["content_id"], {}) or {}
            self._upsert_bundle_item(
                "archive",
                bundle["id"],
                self._bundle_archive_item_payload(
                    {**row, "bundle_id": bundle["id"]},
                    summary=str(archive_payload.get("summary") or row.get("summary") or ""),
                    content=str(archive_payload.get("content") or ""),
                    metadata=archive_payload.get("metadata") or _loads(row.get("metadata"), {}),
                ),
            )
            self.db.execute("UPDATE archive_memories SET bundle_id = ? WHERE id = ?", (bundle["id"], row["id"]))

    def _memory_table_for_id(self, memory_id: str) -> str | None:
        for table_name in MEMORY_TABLE_MAP.values():
            if self.db.fetch_one(f"SELECT id FROM {table_name} WHERE id = ?", (memory_id,)):
                return table_name
        return None

    def _memory_row_sql(self, table_name: str) -> str:
        return MEMORY_TABLE_SELECT.format(table=table_name)

    def _memory_row_with_scope(self, table_name: str, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        item = dict(row)
        item["scope"] = self._memory_scope_from_table(table_name)
        return item

    def _hydrate_memory_row(
        self,
        table_name: str,
        row: dict[str, Any] | None,
        *,
        bundle_cache: dict[tuple[str, str], dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        item = _deserialize_row(self._memory_row_with_scope(table_name, row))
        if item is None:
            return None
        bucket = self._memory_bucket_for_scope(item["scope"])
        bundle_id = str(item.get("bundle_id") or "").strip()
        content_id = item.get("content_id")
        text = ""
        if bundle_id:
            cache_key = (bucket, bundle_id)
            bundle = (bundle_cache or {}).get(cache_key)
            if bundle is None:
                bundle = self._get_memory_bundle(item["scope"], bundle_id)
                if bundle_cache is not None:
                    bundle_cache[cache_key] = bundle
            bundle_item = self._find_bundle_item(bundle, record_id=item["id"], content_id=content_id)
            if bundle_item is not None:
                text = str(bundle_item.get("text") or "")
        if not text and content_id:
            text = self.memory_content_store.get_text(bucket, content_id) or ""
        item["text"] = text
        return item

    def _hydrate_memory_rows(self, table_name: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        bundle_cache: dict[tuple[str, str], dict[str, Any]] = {}
        return [item for item in (self._hydrate_memory_row(table_name, row, bundle_cache=bundle_cache) for row in rows) if item is not None]

    def _list_memory_rows(
        self,
        *,
        scope: str = "all",
        filters: list[str] | None = None,
        params: list[Any] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        sql_filters = filters or ["1 = 1"]
        sql_params = list(params or [])
        target_tables = (
            [(self._memory_table_for_scope(scope), scope)]
            if scope in MEMORY_TABLE_MAP
            else [(table_name, table_scope) for table_scope, table_name in MEMORY_TABLE_MAP.items()]
        )
        rows: list[dict[str, Any]] = []
        fetch_size = max(limit + offset, 1)
        for table_name, _table_scope in target_tables:
            rows.extend(
                self._hydrate_memory_rows(
                    table_name,
                    self.db.fetch_all(
                        f"{self._memory_row_sql(table_name)} WHERE {' AND '.join(sql_filters)} ORDER BY updated_at DESC LIMIT ?",
                        tuple(sql_params + [fetch_size]),
                    ),
                )
            )
        rows.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return rows[offset : offset + limit]

    def _memory_index_payloads(self, memory_ids: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        if not memory_ids:
            return {}, {}
        placeholders = ",".join("?" for _ in memory_ids)
        index_rows = self.db.fetch_all(
            f"SELECT * FROM memory_index WHERE record_id IN ({placeholders})",
            tuple(memory_ids),
        )
        semantic_rows = self.db.fetch_all(
            f"SELECT * FROM semantic_index_cache WHERE collection = 'memory_index' AND record_id IN ({placeholders})",
            tuple(memory_ids),
        )
        return (
            {str(row["record_id"]): row for row in index_rows},
            {str(row["record_id"]): row for row in semantic_rows},
        )

    def _resolve_vector_backend_name(self) -> str:
        return "lancedb"

    def _resolve_graph_backend_name(self) -> str:
        return "disabled"

    def add(self, messages, **kwargs) -> dict[str, Any]:
        kwargs = self._normalize_add_kwargs(kwargs)
        normalized = self._normalize_messages(messages)
        context = self._build_context(
            user_id=kwargs.pop("user_id", None),
            session_id=kwargs.pop("session_id", None),
            agent_id=kwargs.pop("agent_id", None),
            owner_agent_id=kwargs.pop("owner_agent_id", None),
            subject_type=kwargs.pop("subject_type", None),
            subject_id=kwargs.pop("subject_id", None),
            interaction_type=kwargs.pop("interaction_type", None),
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
            run_id=kwargs.pop("run_id", None),
            actor_id=kwargs.pop("actor_id", None),
            role=kwargs.pop("role", None),
        )
        metadata = dict(kwargs.pop("metadata", {}) or {})
        long_term = bool(kwargs.pop("long_term", True))
        infer = bool(kwargs.pop("infer", self.config.memory_policy.infer_by_default))
        memory_type = str(kwargs.pop("memory_type", str(MemoryType.SEMANTIC)))
        source = str(kwargs.pop("source", "conversation"))
        background = self._background_texts(context=context, long_term=long_term)

        if infer:
            candidates = self.distiller.distill(normalized, background_texts=background, memory_type=memory_type)
        else:
            candidates = self._raw_candidates(normalized, memory_type=memory_type)

        results: list[dict[str, Any]] = []
        for candidate in candidates:
            if candidate.score < self.config.memory_policy.short_term_capture_threshold and not long_term:
                continue
            stored = self._remember(
                candidate.text,
                user_id=context.user_id,
                agent_id=context.agent_id,
                owner_agent_id=context.owner_agent_id,
                subject_type=context.subject_type,
                subject_id=context.subject_id,
                interaction_type=context.interaction_type,
                platform_id=context.platform_id,
                workspace_id=context.workspace_id,
                team_id=context.team_id,
                project_id=context.project_id,
                namespace_key=context.namespace_key,
                session_id=context.session_id,
                run_id=context.run_id,
                metadata=merge_metadata(metadata, merge_metadata(candidate.metadata, context.as_metadata())),
                memory_type=memory_type,
                importance=max(0.25, candidate.score),
                long_term=long_term,
                source=source,
            )
            results.append(
                {
                    "event": stored.pop("_event", "ADD"),
                    "id": stored["id"],
                    "memory": stored["text"],
                    "score": round(candidate.score, 6),
                    "memory_type": stored["memory_type"],
                }
            )
        return {"results": results, "facts": [item["memory"] for item in results]}

    def get(self, memory_id: str) -> dict[str, Any] | None:
        table_name = self._memory_table_for_id(memory_id)
        if table_name is None:
            return None
        row = self.db.fetch_one(f"{self._memory_row_sql(table_name)} WHERE id = ?", (memory_id,))
        return self._hydrate_memory_row(table_name, row)

    def get_all(
        self,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        scope: str = "all",
        limit: int = 100,
        offset: int = 0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sql_filters = ["status != 'deleted'"]
        params: list[Any] = []
        if user_id:
            sql_filters.append("user_id = ?")
            params.append(user_id)
        if owner_agent_id:
            sql_filters.append("(owner_agent_id = ? OR (owner_agent_id IS NULL AND agent_id = ?))")
            params.extend([owner_agent_id, owner_agent_id])
        if session_id:
            sql_filters.append("(session_id = ? OR session_id IS NULL)")
            params.append(session_id)
        if subject_type:
            sql_filters.append("(subject_type = ? OR subject_type IS NULL)")
            params.append(subject_type)
        if subject_id:
            sql_filters.append("(subject_id = ? OR subject_id IS NULL)")
            params.append(subject_id)
        if interaction_type:
            sql_filters.append("(interaction_type = ? OR interaction_type IS NULL)")
            params.append(interaction_type)
        namespace_filter = self._namespace_filter_value(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            sql_filters.append("namespace_key = ?")
            params.append(namespace_filter)
        if scope == str(MemoryScope.SESSION):
            rows = self._list_memory_rows(
                scope=str(MemoryScope.SESSION),
                filters=sql_filters,
                params=params,
                limit=limit,
                offset=offset,
            )
        elif scope == str(MemoryScope.LONG_TERM):
            rows = self._list_memory_rows(
                scope=str(MemoryScope.LONG_TERM),
                filters=sql_filters,
                params=params,
                limit=limit,
                offset=offset,
            )
        else:
            rows = self._list_memory_rows(filters=sql_filters, params=params, limit=limit, offset=offset)
        if filters:
            rows = filter_records(rows, filters)
        return {"results": rows, "count": len(rows), "limit": limit, "offset": offset}

    def search(self, query: str, **kwargs) -> dict[str, Any]:
        kwargs = self._normalize_search_kwargs(kwargs)
        return self.memory_search(query, **kwargs)

    def update(self, memory_id: str, **kwargs) -> dict[str, Any]:
        row = self.get(memory_id)
        if row is None:
            raise ValueError(f"Memory `{memory_id}` does not exist.")
        table_name = self._memory_table_for_id(memory_id)
        assert table_name is not None
        metadata = merge_metadata(row.get("metadata"), kwargs.pop("metadata", None))
        text = str(kwargs.pop("text", row["text"]))
        summary = str(kwargs.pop("summary", build_summary(split_sentences(text), max_sentences=3, max_chars=200)))
        importance = float(kwargs.pop("importance", row.get("importance", 0.5)))
        status = str(kwargs.pop("status", row.get("status", "active")))
        now = utcnow_iso()
        self.db.execute(
            f"""
            UPDATE {table_name}
            SET summary = ?, importance = ?, status = ?, metadata = ?, updated_at = ?
            WHERE id = ?
            """,
            (summary, importance, status, json_dumps(metadata), now, memory_id),
        )
        bundle_id = str(row.get("bundle_id") or "").strip()
        if bundle_id:
            self._upsert_bundle_item(
                row["scope"],
                bundle_id,
                self._bundle_memory_item_payload(
                    {
                        **row,
                        "summary": summary,
                        "importance": importance,
                        "status": status,
                        "metadata": metadata,
                        "updated_at": now,
                    },
                    text=text,
                ),
            )
        self._record_memory_event(memory_id, "UPDATE", {"text": text, "metadata": metadata, "status": status})
        updated = self.get(memory_id)
        assert updated is not None
        if status == "active":
            self._index_memory(updated)
        else:
            self._delete_memory_index(memory_id)
        updated["_event"] = "UPDATE"
        warning = self._maybe_compact_memory_scope(
            scope=row["scope"],
            bundle_id=bundle_id,
            user_id=updated.get("user_id"),
            owner_agent_id=updated.get("owner_agent_id"),
            subject_type=updated.get("subject_type"),
            subject_id=updated.get("subject_id"),
            interaction_type=updated.get("interaction_type"),
            namespace_key=updated.get("namespace_key"),
            session_id=updated.get("session_id"),
            source=updated.get("source"),
        )
        if warning is not None:
            updated["memory_overflow_warning"] = warning
        return updated

    def delete(self, memory_id: str) -> dict[str, Any]:
        row = self.get(memory_id)
        if row is None:
            raise ValueError(f"Memory `{memory_id}` does not exist.")
        table_name = self._memory_table_for_id(memory_id)
        assert table_name is not None
        now = utcnow_iso()
        self.db.execute(f"UPDATE {table_name} SET status = ?, updated_at = ? WHERE id = ?", ("deleted", now, memory_id))
        bundle_id = str(row.get("bundle_id") or "").strip()
        if bundle_id:
            self._upsert_bundle_item(
                row["scope"],
                bundle_id,
                self._bundle_memory_item_payload({**row, "status": "deleted", "updated_at": now}, text=row["text"]),
            )
        self._delete_memory_index(memory_id)
        self._record_memory_event(memory_id, "DELETE", {"deleted_at": now})
        result = dict(row)
        result["status"] = "deleted"
        result["_event"] = "DELETE"
        return result

    def history(self, memory_id: str) -> list[dict[str, Any]]:
        rows = self.db.fetch_all("SELECT * FROM memory_events WHERE memory_id = ? ORDER BY created_at ASC", (memory_id,))
        return _deserialize_rows(rows, ("payload",))

    def memory_store(
        self,
        text: str,
        user_id: str | None = None,
        session_id: str | None = None,
        long_term: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        if "longTerm" in kwargs:
            long_term = bool(kwargs.pop("longTerm"))
        return self._remember(text, user_id=user_id, session_id=session_id, long_term=long_term, **kwargs)

    def remember_long_term(self, text: str, **kwargs) -> dict[str, Any]:
        return self._remember(text, long_term=True, **kwargs)

    def remember_short_term(self, text: str, **kwargs) -> dict[str, Any]:
        return self._remember(text, long_term=False, **kwargs)

    def memory_search(
        self,
        query: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        top_k: int = 8,
        search_threshold: float = 0.0,
        **kwargs,
    ) -> dict[str, Any]:
        limit = int(kwargs.pop("limit", kwargs.pop("topK", top_k)))
        threshold = float(kwargs.pop("threshold", kwargs.pop("searchThreshold", search_threshold)))
        filters = kwargs.pop("filters", None)
        namespace_filter = self._namespace_filter_value(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
        )
        sql_filters = ["status = 'active'"]
        params: list[Any] = []
        if user_id:
            sql_filters.append("user_id = ?")
            params.append(user_id)
        if owner_agent_id:
            sql_filters.append("(owner_agent_id = ? OR (owner_agent_id IS NULL AND agent_id = ?))")
            params.extend([owner_agent_id, owner_agent_id])
        if subject_type:
            sql_filters.append("(subject_type = ? OR subject_type IS NULL)")
            params.append(subject_type)
        if subject_id:
            sql_filters.append("(subject_id = ? OR subject_id IS NULL)")
            params.append(subject_id)
        if interaction_type:
            sql_filters.append("(interaction_type = ? OR interaction_type IS NULL)")
            params.append(interaction_type)
        if session_id and scope == str(MemoryScope.SESSION):
            sql_filters.append("session_id = ?")
            params.append(session_id)
        if namespace_filter:
            sql_filters.append("namespace_key = ?")
            params.append(namespace_filter)
        scan_limit = self.config.memory_policy.search_scan_limit
        if scope == str(MemoryScope.SESSION):
            rows = self._list_memory_rows(scope=str(MemoryScope.SESSION), filters=sql_filters, params=params, limit=scan_limit)
        elif scope == str(MemoryScope.LONG_TERM):
            rows = self._list_memory_rows(scope=str(MemoryScope.LONG_TERM), filters=sql_filters, params=params, limit=scan_limit)
        else:
            rows = self._list_memory_rows(filters=sql_filters, params=params, limit=scan_limit)
            if session_id:
                rows = [
                    row
                    for row in rows
                    if row.get("scope") == str(MemoryScope.LONG_TERM) or row.get("session_id") == session_id
                ]
        index_rows, semantic_rows = self._memory_index_payloads([row["id"] for row in rows])
        enriched_rows: list[dict[str, Any]] = []
        for row in rows:
            enriched = dict(row)
            record_id = row["id"]
            enriched["index_keywords"] = index_rows.get(record_id, {}).get("keywords")
            enriched["embedding"] = semantic_rows.get(record_id, {}).get("embedding")
            enriched_rows.append(enriched)
        vector_hits = self._vector_hit_map("memory_index", query, limit=max(limit * 4, 24))
        fts_hits = self._fts_hit_map(["memory_index"], query, limit=max(limit * 6, 36))
        results = self._rank_memory_rows(
            query,
            enriched_rows,
            half_life_days=self.config.memory_policy.short_term_half_life_days if scope == str(MemoryScope.SESSION) else self.config.memory_policy.long_term_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity={
                "owner_agent_id": owner_agent_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "interaction_type": interaction_type,
            },
        )
        return {"results": results[:limit]}

    def memory_list(
        self,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        limit: int = 100,
        offset: int = 0,
        **kwargs,
    ) -> dict[str, Any]:
        page = int(kwargs.pop("page", 1))
        page_size = int(kwargs.pop("page_size", kwargs.pop("pageSize", limit)))
        if page > 1 and offset == 0:
            offset = (page - 1) * page_size
        return self.get_all(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            scope=scope,
            limit=page_size,
            offset=offset,
            filters=kwargs.pop("filters", None),
        )

    def memory_get(self, memory_id: str) -> dict[str, Any] | None:
        return self.get(memory_id)

    def memory_forget(
        self,
        memory_id: str | None = None,
        query: str | None = None,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        limit: int = 10,
        **kwargs,
    ) -> dict[str, Any]:
        if memory_id:
            deleted = self.delete(memory_id)
            return {"results": [deleted]}
        if not query:
            raise ValueError("Either memory_id or query must be provided.")
        found = self.memory_search(
            query,
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            scope=scope,
            limit=limit,
            filters=kwargs.pop("filters", None),
        )
        deleted: list[dict[str, Any]] = []
        for item in found["results"]:
            deleted.append(self.delete(item["id"]))
        return {"results": deleted}

    def query(
        self,
        query: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        run_id: str | None = None,
        actor_id: str | None = None,
        role: str | None = None,
        domains: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 10,
        threshold: float = 0.0,
    ) -> dict[str, Any]:
        searchers: dict[str, Callable[[], dict[str, Any]]] = {
            "memory": lambda: self.memory_search(
                query,
                user_id=user_id,
                owner_agent_id=owner_agent_id or agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                interaction_type=interaction_type,
                session_id=session_id,
                platform_id=platform_id,
                workspace_id=workspace_id,
                team_id=team_id,
                project_id=project_id,
                namespace_key=namespace_key,
                scope="all",
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
            "interaction": lambda: self.search_interaction(
                query,
                user_id=user_id,
                owner_agent_id=owner_agent_id or agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                interaction_type=interaction_type,
                session_id=session_id,
                platform_id=platform_id,
                workspace_id=workspace_id,
                team_id=team_id,
                project_id=project_id,
                namespace_key=namespace_key,
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
            "knowledge": lambda: self.search_knowledge(
                query,
                user_id=user_id,
                owner_agent_id=owner_agent_id or agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                platform_id=platform_id,
                workspace_id=workspace_id,
                team_id=team_id,
                project_id=project_id,
                namespace_key=namespace_key,
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
            "skill": lambda: self.search_skills(
                query,
                owner_agent_id=owner_agent_id or agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                platform_id=platform_id,
                workspace_id=workspace_id,
                team_id=team_id,
                project_id=project_id,
                namespace_key=namespace_key,
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
            "archive": lambda: self.search_archive(
                query,
                user_id=user_id,
                owner_agent_id=owner_agent_id or agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                interaction_type=interaction_type,
                session_id=session_id,
                platform_id=platform_id,
                workspace_id=workspace_id,
                team_id=team_id,
                project_id=project_id,
                namespace_key=namespace_key,
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
            "execution": lambda: self.search_execution(
                query,
                user_id=user_id,
                owner_agent_id=owner_agent_id or agent_id,
                session_id=session_id,
                platform_id=platform_id,
                workspace_id=workspace_id,
                team_id=team_id,
                project_id=project_id,
                namespace_key=namespace_key,
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
        }
        selected_domains = domains or ["memory", "interaction", "knowledge", "skill", "archive"]
        merged: list[dict[str, Any]] = []
        for domain in selected_domains:
            if domain not in searchers:
                continue
            payload = searchers[domain]()
            for item in payload.get("results", []):
                merged_item = dict(item)
                merged_item["domain"] = domain
                merged_item["score"] = round(float(merged_item.get("score", 0.0)) + _domain_priority(domain), 6)
                merged.append(merged_item)
        if filters:
            merged = filter_records(merged, filters)
        merged.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        merged = mmr_rerank(merged, lambda_value=self.config.memory_policy.diversity_lambda, limit=limit)
        return {"results": merged[:limit]}

    def explain_recall(self, query: str, **kwargs) -> dict[str, Any]:
        result = self.query(query, **kwargs)
        return {
            "query": query,
            "policy": {
                "vector_backend": getattr(self.vector_index, "name", "unknown"),
                "graph_backend": getattr(self.graph_store, "active_backend", "disabled"),
                "diversity_lambda": self.config.memory_policy.diversity_lambda,
                "scan_limit": self.config.memory_policy.search_scan_limit,
            },
            "results": result["results"],
        }

    def create_session(self, user_id: str | None = None, session_id: str | None = None, **kwargs) -> dict[str, Any]:
        session_id = session_id or make_id("sess")
        now = utcnow_iso()
        metadata = dict(kwargs.pop("metadata", {}) or {})
        scope = self._resolve_scope(
            user_id=user_id,
            agent_id=kwargs.pop("agent_id", None),
            owner_agent_id=kwargs.pop("owner_agent_id", None),
            subject_type=kwargs.pop("subject_type", None),
            subject_id=kwargs.pop("subject_id", None),
            interaction_type=kwargs.pop("interaction_type", None),
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
        )
        title = kwargs.pop("title", None)
        self.db.execute(
            """
            INSERT INTO sessions(id, user_id, agent_id, owner_agent_id, interaction_type, subject_type, subject_id, namespace_key, title, status, metadata, active_window, ttl_seconds, expires_at, last_accessed_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                scope["user_id"],
                scope["owner_agent_id"],
                scope["owner_agent_id"],
                scope["interaction_type"],
                scope["subject_type"],
                scope["subject_id"],
                scope.get("namespace_key"),
                title,
                "active",
                json_dumps(merge_metadata(metadata, self._scope_metadata(scope))),
                "",
                int(kwargs.pop("ttl_seconds", self.config.session_ttl_seconds)),
                None,
                now,
                now,
                now,
            ),
        )
        return self.get_session(session_id)

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        row = self.db.fetch_one("SELECT * FROM sessions WHERE id = ?", (session_id,))
        session = _deserialize_row(row)
        if session is None:
            return None
        session["participants"] = self._ensure_session_participants(session)
        return session

    def append_turn(self, session_id: str, role: str, content: str, **kwargs) -> dict[str, Any]:
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        turn_id = make_id("turn")
        now = utcnow_iso()
        metadata = dict(kwargs.pop("metadata", {}) or {})
        run_id = kwargs.pop("run_id", None)
        name = kwargs.pop("name", None)
        speaker_participant_id = kwargs.pop("speaker_participant_id", None)
        target_participant_id = kwargs.pop("target_participant_id", None)
        speaker_type = kwargs.pop("speaker_type", None)
        speaker_external_id = kwargs.pop("speaker_external_id", None)
        target_type = kwargs.pop("target_type", None)
        target_external_id = kwargs.pop("target_external_id", None)
        turn_type = str(kwargs.pop("turn_type", "message"))
        salience_score = float(kwargs.pop("salience_score", round(min(0.95, 0.34 + (estimate_tokens(content) / 220.0)), 6)))
        if speaker_participant_id is None:
            if speaker_type is None:
                if role in {"user", "human"}:
                    speaker_type = "human"
                    speaker_external_id = speaker_external_id or (session.get("subject_id") if session.get("subject_type") == "human" else session.get("user_id"))
                elif role in {"peer_agent"}:
                    speaker_type = "agent"
                    speaker_external_id = speaker_external_id or (session.get("subject_id") if session.get("subject_type") == "agent" else None)
                else:
                    speaker_type = "agent"
                    speaker_external_id = speaker_external_id or session.get("owner_agent_id") or session.get("agent_id")
            speaker = self._ensure_participant(speaker_type, speaker_external_id, metadata={"session_id": session_id})
            if speaker is not None:
                speaker_participant_id = speaker["id"]
                participant_role = "owner_agent" if speaker_type == "agent" and speaker_external_id == (session.get("owner_agent_id") or session.get("agent_id")) else ("human_subject" if speaker_type == "human" else "peer_agent")
                self._bind_session_participant(session_id, speaker_participant_id, participant_role)
        if target_participant_id is None:
            if target_type is None:
                if speaker_type == "human":
                    target_type = "agent"
                    target_external_id = target_external_id or session.get("owner_agent_id") or session.get("agent_id")
                elif speaker_type == "agent" and session.get("subject_type") == "human":
                    target_type = "human"
                    target_external_id = target_external_id or session.get("subject_id") or session.get("user_id")
                elif speaker_type == "agent" and session.get("subject_type") == "agent":
                    target_type = "agent"
                    target_external_id = target_external_id or session.get("subject_id")
            target = self._ensure_participant(target_type or "", target_external_id, metadata={"session_id": session_id}) if target_type else None
            if target is not None:
                target_participant_id = target["id"]
                target_role = "owner_agent" if target_type == "agent" and target_external_id == (session.get("owner_agent_id") or session.get("agent_id")) else ("human_subject" if target_type == "human" else "peer_agent")
                self._bind_session_participant(session_id, target_participant_id, target_role)
        self.db.execute(
            """
            INSERT INTO conversation_turns(id, session_id, run_id, role, content, name, metadata, tokens_in, tokens_out, speaker_participant_id, target_participant_id, turn_type, salience_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                turn_id,
                session_id,
                run_id,
                role,
                content,
                name,
                json_dumps(merge_metadata(metadata, self._scope_metadata(session))),
                kwargs.pop("tokens_in", None),
                kwargs.pop("tokens_out", None),
                speaker_participant_id,
                target_participant_id,
                turn_type,
                salience_score,
                now,
            ),
        )
        self.db.execute("UPDATE sessions SET updated_at = ?, last_accessed_at = ? WHERE id = ?", (now, now, session_id))
        self._index_interaction_turn(
            session,
            turn_id=turn_id,
            role=role,
            content=content,
            turn_type=turn_type,
            updated_at=now,
        )
        auto_capture = bool(kwargs.pop("auto_capture", True))
        auto_compress = bool(kwargs.pop("auto_compress", True))
        captured = None
        if auto_capture:
            capture_result = self.add(
                [{"role": role, "content": content, "name": name, "metadata": metadata}],
                user_id=session["user_id"],
                session_id=session_id,
                agent_id=session.get("agent_id"),
                owner_agent_id=session.get("owner_agent_id"),
                subject_type=session.get("subject_type"),
                subject_id=session.get("subject_id"),
                interaction_type=session.get("interaction_type"),
                platform_id=session.get("platform_id"),
                workspace_id=session.get("workspace_id"),
                team_id=session.get("team_id"),
                project_id=session.get("project_id"),
                namespace_key=session.get("namespace_key"),
                run_id=run_id,
                long_term=False,
                source="conversation_turn",
                infer=True,
            )
            captured = capture_result["results"]
        health = self.session_health(session_id)
        compressed = None
        if auto_compress and health["turn_count"] >= self.config.memory_policy.compression_turn_threshold:
            compressed = self.compress_session_context(session_id)
        return {
            "id": turn_id,
            "session_id": session_id,
            "role": role,
            "content": content,
            "speaker_participant_id": speaker_participant_id,
            "target_participant_id": target_participant_id,
            "turn_type": turn_type,
            "salience_score": salience_score,
            "captured": captured or [],
            "compressed": compressed,
        }

    def start_run(self, user_id: str | None = None, goal: str = "", **kwargs) -> dict[str, Any]:
        run_id = kwargs.pop("run_id", make_id("run"))
        now = utcnow_iso()
        scope = self._resolve_scope(
            user_id=user_id,
            agent_id=kwargs.pop("agent_id", None),
            owner_agent_id=kwargs.pop("owner_agent_id", None),
            subject_type=kwargs.pop("subject_type", None),
            subject_id=kwargs.pop("subject_id", None),
            interaction_type=kwargs.pop("interaction_type", None),
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
        )
        self.db.execute(
            """
            INSERT INTO runs(id, session_id, user_id, agent_id, owner_agent_id, interaction_type, subject_type, subject_id, namespace_key, goal, status, metadata, started_at, ended_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                kwargs.pop("session_id", None),
                scope["user_id"],
                scope["owner_agent_id"],
                scope["owner_agent_id"],
                scope["interaction_type"],
                scope["subject_type"],
                scope["subject_id"],
                scope.get("namespace_key"),
                goal,
                kwargs.pop("status", "running"),
                json_dumps(merge_metadata(kwargs.pop("metadata", {}) or {}, self._scope_metadata(scope))),
                now,
                None,
                now,
            ),
        )
        run = _deserialize_row(self.db.fetch_one("SELECT * FROM runs WHERE id = ?", (run_id,)))
        if run is not None:
            self._index_execution_run(run)
        return run

    def ingest_document(self, title: str, text: str, **kwargs) -> dict[str, Any]:
        source_name = kwargs.pop("source_name", title)
        source_type = kwargs.pop("source_type", "inline")
        uri = kwargs.pop("uri", None)
        global_scope = bool(kwargs.pop("global_scope", False))
        scope = self._resolve_scope(
            user_id=kwargs.pop("user_id", None),
            agent_id=kwargs.pop("agent_id", None),
            owner_agent_id=kwargs.pop("owner_agent_id", None),
            subject_type=kwargs.pop("source_subject_type", kwargs.pop("subject_type", None)),
            subject_id=kwargs.pop("source_subject_id", kwargs.pop("subject_id", None)),
            interaction_type=kwargs.pop("interaction_type", None),
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
            global_scope=global_scope,
        )
        user_id = scope["user_id"]
        owner_agent_id = scope["owner_agent_id"]
        source_subject_type = scope["subject_type"]
        source_subject_id = scope["subject_id"]
        external_id = kwargs.pop("external_id", None)
        metadata = dict(kwargs.pop("metadata", {}) or {})
        chunk_size = int(kwargs.pop("chunk_size", self.config.memory_policy.chunk_size))
        chunk_overlap = int(kwargs.pop("chunk_overlap", self.config.memory_policy.chunk_overlap))
        kb_namespace = kwargs.pop("kb_namespace", scope.get("namespace_key") or owner_agent_id or "default")
        now = utcnow_iso()

        source = self.db.fetch_one("SELECT * FROM knowledge_sources WHERE name = ? AND source_type = ?", (source_name, source_type))
        if source is None:
            source_id = make_id("source")
            self.db.execute(
                """
                INSERT INTO knowledge_sources(id, name, source_type, uri, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (source_id, source_name, source_type, uri, json_dumps(metadata), now, now),
            )
        else:
            source_id = source["id"]

        document_id = make_id("doc")
        self.db.execute(
            """
            INSERT INTO documents(id, source_id, title, user_id, owner_agent_id, kb_namespace, source_subject_type, source_subject_id, namespace_key, external_id, status, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_id,
                source_id,
                title,
                user_id,
                owner_agent_id,
                kb_namespace,
                source_subject_type,
                source_subject_id,
                scope.get("namespace_key"),
                external_id,
                "active",
                json_dumps(merge_metadata(metadata, self._scope_metadata(scope))),
                now,
                now,
            ),
        )
        stored = self.object_store.put_text(text, object_type="knowledge", suffix=".txt", prefix=self._object_store_prefix(scope, "knowledge"))
        object_row = self._persist_object(stored, mime_type="text/plain", metadata={"document_id": document_id, **self._scope_metadata(scope), **metadata})
        version_id = make_id("docver")
        self.db.execute(
            """
            INSERT INTO document_versions(id, document_id, version_label, object_id, checksum, size_bytes, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (version_id, document_id, "v1", object_row["id"], object_row["checksum"], object_row["size_bytes"], json_dumps(metadata), now),
        )
        chunks = chunk_text_units(text, source_id=document_id, chunk_size=chunk_size, overlap=chunk_overlap)
        for index, chunk in enumerate(chunks):
            chunk_id = make_id("chunk")
            chunk_metadata = {
                "chunk_index": index,
                **chunk.metadata,
                **self._scope_metadata(scope),
                **metadata,
            }
            self.db.execute(
                """
                INSERT INTO document_chunks(id, document_id, version_id, chunk_index, content, tokens, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (chunk_id, document_id, version_id, index, chunk.text, estimate_tokens(chunk.text), json_dumps(chunk_metadata), now),
            )
            self._index_knowledge_chunk(
                {
                    "id": chunk_id,
                    "document_id": document_id,
                    "source_id": source_id,
                    "owner_agent_id": owner_agent_id,
                    "source_subject_type": source_subject_type,
                    "source_subject_id": source_subject_id,
                    "namespace_key": scope.get("namespace_key"),
                    "title": _chunk_title(title, chunk_metadata),
                    "content": chunk.text,
                    "metadata": chunk_metadata,
                    "updated_at": now,
                }
            )
        return self.get_document(document_id)

    def ingest_knowledge(self, title: str, text: str, **kwargs) -> dict[str, Any]:
        return self.ingest_document(title, text, **kwargs)

    def get_document(self, document_id: str) -> dict[str, Any] | None:
        row = self.db.fetch_one("SELECT * FROM documents WHERE id = ?", (document_id,))
        document = _deserialize_row(row)
        if document is None:
            return None
        versions = _deserialize_rows(self.db.fetch_all("SELECT * FROM document_versions WHERE document_id = ? ORDER BY created_at DESC", (document_id,)))
        chunks = _deserialize_rows(self.db.fetch_all("SELECT * FROM document_chunks WHERE document_id = ? ORDER BY chunk_index ASC", (document_id,)))
        document["versions"] = versions
        document["chunks"] = chunks
        return document

    def search_knowledge(
        self,
        query: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        include_global: bool = True,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sql_filters = ["d.status = 'active'"]
        params: list[Any] = []
        if user_id:
            if include_global:
                sql_filters.append("(d.user_id = ? OR d.user_id IS NULL)")
            else:
                sql_filters.append("d.user_id = ?")
            params.append(user_id)
        if owner_agent_id:
            if include_global:
                sql_filters.append("(d.owner_agent_id = ? OR d.owner_agent_id IS NULL)")
            else:
                sql_filters.append("d.owner_agent_id = ?")
            params.append(owner_agent_id)
        if subject_type:
            sql_filters.append("(d.source_subject_type = ? OR d.source_subject_type IS NULL)")
            params.append(subject_type)
        if subject_id:
            sql_filters.append("(d.source_subject_id = ? OR d.source_subject_id IS NULL)")
            params.append(subject_id)
        namespace_filter = self._namespace_filter_value(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            if include_global:
                sql_filters.append("(d.namespace_key = ? OR d.namespace_key = 'global')")
            else:
                sql_filters.append("d.namespace_key = ?")
            params.append(namespace_filter)
        rows = self.db.fetch_all(
            f"""
            SELECT dc.id, dc.document_id, dc.content AS text, dc.metadata, d.title, d.user_id, d.owner_agent_id, d.source_subject_type, d.source_subject_id, d.updated_at, sic.embedding
            FROM document_chunks dc
            JOIN documents d ON d.id = dc.document_id
            LEFT JOIN semantic_index_cache sic ON sic.record_id = dc.id
            WHERE {' AND '.join(sql_filters)}
            ORDER BY d.updated_at DESC, dc.chunk_index ASC
            LIMIT ?
            """,
            tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
        )
        vector_hits = self._vector_hit_map("knowledge_chunk_index", query, limit=max(limit * 4, 24))
        fts_hits = self._fts_hit_map(["knowledge_chunk_index"], query, limit=max(limit * 6, 36))
        ranked = self._rank_rows(
            query,
            rows,
            domain="knowledge",
            text_key="text",
            keywords_getter=lambda row: extract_keywords(" ".join(part for part in [row.get("title"), row.get("text")] if part)),
            updated_at_key="updated_at",
            importance_getter=lambda row: 0.72,
            half_life_days=self.config.memory_policy.knowledge_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity={"owner_agent_id": owner_agent_id, "subject_type": subject_type, "subject_id": subject_id},
        )
        return {"results": ranked[:limit]}

    def save_skill(self, name: str, description: str, **kwargs) -> dict[str, Any]:
        now = utcnow_iso()
        owner_id = kwargs.pop("owner_id", None)
        scope = self._resolve_scope(
            agent_id=kwargs.pop("agent_id", owner_id),
            owner_agent_id=kwargs.pop("owner_agent_id", owner_id),
            subject_type=kwargs.pop("source_subject_type", kwargs.pop("subject_type", None)),
            subject_id=kwargs.pop("source_subject_id", kwargs.pop("subject_id", None)),
            interaction_type=kwargs.pop("interaction_type", None),
            user_id=kwargs.pop("user_id", None),
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
        )
        owner_agent_id = scope["owner_agent_id"]
        source_subject_type = scope["subject_type"]
        source_subject_id = scope["subject_id"]
        if owner_agent_id:
            skill = self.db.fetch_one("SELECT * FROM skills WHERE name = ? AND COALESCE(owner_agent_id, owner_id) = ?", (name, owner_agent_id))
        else:
            skill = self.db.fetch_one("SELECT * FROM skills WHERE name = ?", (name,))
        metadata = dict(kwargs.pop("metadata", {}) or {})
        raw_assets = kwargs.pop("assets", None)
        skill_markdown = kwargs.pop("skill_markdown", None)
        files = kwargs.pop("files", None)
        references = kwargs.pop("references", None)
        scripts = kwargs.pop("scripts", None)
        asset_files = raw_assets if looks_like_skill_file_mapping(raw_assets) else None
        if raw_assets is not None and asset_files is None:
            metadata["assets"] = raw_assets
        if skill is None:
            skill_id = make_id("skill")
            self.db.execute(
                """
                INSERT INTO skills(id, name, description, owner_id, owner_agent_id, source_subject_type, source_subject_id, namespace_key, status, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    skill_id,
                    name,
                    description,
                    owner_agent_id,
                    owner_agent_id,
                    source_subject_type,
                    source_subject_id,
                    scope.get("namespace_key"),
                    "active",
                    json_dumps(merge_metadata(metadata, self._scope_metadata(scope))),
                    now,
                    now,
                ),
            )
        else:
            skill_id = skill["id"]
            merged_metadata = merge_metadata(_loads(skill.get("metadata"), {}), metadata)
            self.db.execute(
                """
                UPDATE skills
                SET description = ?, owner_id = ?, owner_agent_id = ?, source_subject_type = ?, source_subject_id = ?, namespace_key = ?, metadata = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    description,
                    owner_agent_id or skill.get("owner_id"),
                    owner_agent_id or skill.get("owner_agent_id") or skill.get("owner_id"),
                    source_subject_type or skill.get("source_subject_type"),
                    source_subject_id or skill.get("source_subject_id"),
                    scope.get("namespace_key") or skill.get("namespace_key"),
                    json_dumps(merge_metadata(merged_metadata, self._scope_metadata(scope))),
                    now,
                    skill_id,
                ),
            )

        version = kwargs.pop("version", f"v{len(self.db.fetch_all('SELECT id FROM skill_versions WHERE skill_id = ?', (skill_id,))) + 1}")
        prompt_template = kwargs.pop("prompt_template", None)
        workflow = kwargs.pop("workflow", None)
        schema = kwargs.pop("schema", None)
        tools = list(kwargs.pop("tools", []) or [])
        tests = list(kwargs.pop("tests", []) or [])
        topics = list(kwargs.pop("topics", []) or [])
        self._write_skill_version_record(
            skill_id=skill_id,
            name=name,
            description=description,
            scope=scope,
            metadata=metadata,
            version=version,
            prompt_template=prompt_template,
            workflow=workflow,
            schema=schema,
            tools=tools,
            tests=tests,
            topics=topics,
            skill_markdown=skill_markdown,
            base_files=None,
            files=list(files or []),
            references=references,
            scripts=scripts,
            assets=asset_files,
            now=now,
        )
        return self.get_skill(skill_id)

    def register_skill(self, name: str, description: str, **kwargs) -> dict[str, Any]:
        return self.save_skill(name, description, **kwargs)

    def _load_skill_version_asset(self, version_or_id: dict[str, Any] | str | None) -> dict[str, Any]:
        if version_or_id is None:
            return {}
        object_id = version_or_id.get("object_id") if isinstance(version_or_id, dict) else None
        if object_id is None and not isinstance(version_or_id, dict):
            row = self.db.fetch_one("SELECT object_id FROM skill_versions WHERE id = ?", (str(version_or_id),))
            object_id = row.get("object_id") if row else None
        if not object_id:
            return {}
        object_row = self.db.fetch_one("SELECT object_key FROM objects WHERE id = ?", (object_id,))
        if object_row is None:
            return {}
        try:
            return json_loads(self.object_store.get_text(object_row["object_key"]), {}) or {}
        except (FileNotFoundError, UnicodeDecodeError):
            return {}

    def _skill_version_file_rows(self, skill_version_id: str) -> list[dict[str, Any]]:
        return _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT sf.*, o.object_key, o.object_type, o.metadata AS object_metadata
                FROM skill_files sf
                JOIN objects o ON o.id = sf.object_id
                WHERE sf.skill_version_id = ?
                ORDER BY
                    CASE sf.role
                        WHEN 'skill_md' THEN 0
                        WHEN 'reference' THEN 1
                        WHEN 'script' THEN 2
                        ELSE 3
                    END,
                    sf.relative_path ASC
                """,
                (skill_version_id,),
            ),
            ("metadata", "object_metadata"),
        )

    def _skill_version_files(self, skill_version_id: str, *, inline_contents: bool = True) -> list[dict[str, Any]]:
        files: list[dict[str, Any]] = []
        for row in self._skill_version_file_rows(skill_version_id):
            item = {
                "id": row["id"],
                "skill_id": row["skill_id"],
                "skill_version_id": row["skill_version_id"],
                "object_id": row["object_id"],
                "object_key": row.get("object_key"),
                "relative_path": row["relative_path"],
                "role": row["role"],
                "mime_type": row.get("mime_type"),
                "size_bytes": row.get("size_bytes"),
                "checksum": row.get("checksum"),
                "metadata": row.get("metadata", {}),
                "textual": is_textual_skill_file(
                    relative_path=row["relative_path"],
                    mime_type=row.get("mime_type"),
                    role=row.get("role"),
                ),
            }
            if inline_contents and item["textual"] and row.get("object_key"):
                try:
                    item["content"] = self.object_store.get_text(row["object_key"])
                except (FileNotFoundError, UnicodeDecodeError):
                    item["content"] = None
            files.append(item)
        return files

    def _clone_skill_version_file_inputs(
        self,
        *,
        skill_version_id: str,
        name: str,
        description: str,
        prompt_template: str | None,
        workflow: Any,
        tools: list[str],
        topics: list[str],
    ) -> list[dict[str, Any]]:
        cloned: list[dict[str, Any]] = []
        for row in self._skill_version_file_rows(skill_version_id):
            role = str(row.get("role") or "")
            metadata = dict(row.get("metadata", {}) or {})
            if role == "skill_md" and metadata.get("generated"):
                content: str | bytes = default_skill_markdown(
                    name=name,
                    description=description,
                    prompt_template=prompt_template,
                    workflow=workflow,
                    tools=tools,
                    topics=topics,
                )
            elif is_textual_skill_file(
                relative_path=row["relative_path"],
                mime_type=row.get("mime_type"),
                role=role,
            ):
                content = self.object_store.get_text(row["object_key"])
            else:
                content = self.object_store.get_bytes(row["object_key"])
            cloned.append(
                {
                    "path": row["relative_path"],
                    "role": role,
                    "mime_type": row.get("mime_type"),
                    "content": content,
                    "metadata": metadata,
                    "index": role == "reference",
                }
            )
        return cloned

    def _delete_skill_version_artifacts(self, skill_version_id: str) -> None:
        rows = self.db.fetch_all("SELECT id FROM skill_reference_chunks WHERE skill_version_id = ?", (skill_version_id,))
        for row in rows:
            record_id = row["id"]
            self.db.execute("DELETE FROM skill_reference_index WHERE record_id = ?", (record_id,))
            self.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (record_id,))
            self._delete_text_search_record(record_id)
            self.vector_index.delete("skill_reference_index", record_id)
        self.db.execute("DELETE FROM skill_reference_chunks WHERE skill_version_id = ?", (skill_version_id,))
        self.db.execute("DELETE FROM skill_files WHERE skill_version_id = ?", (skill_version_id,))

    def _write_skill_version_record(
        self,
        *,
        skill_id: str,
        name: str,
        description: str,
        scope: dict[str, Any],
        metadata: dict[str, Any],
        version: str | None,
        prompt_template: str | None,
        workflow: Any,
        schema: dict[str, Any] | None,
        tools: list[str],
        tests: list[dict[str, Any]],
        topics: list[str],
        skill_markdown: str | None,
        base_files: list[dict[str, Any]] | None,
        files: list[dict[str, Any]] | None,
        references: dict[str, Any] | list[dict[str, Any]] | None,
        scripts: dict[str, Any] | list[dict[str, Any]] | None,
        assets: dict[str, Any] | list[dict[str, Any]] | None,
        now: str,
    ) -> dict[str, Any]:
        asset_payload = {
            "name": name,
            "description": description,
            "workflow": workflow,
            "schema": schema,
            "tools": tools,
            "topics": topics,
            "tests": tests,
            "metadata": metadata,
        }
        stored = self.object_store.put_text(
            json_dumps(asset_payload),
            object_type="skills",
            suffix=".json",
            prefix=self._object_store_prefix(scope, "skill"),
        )
        object_row = self._persist_object(
            stored,
            mime_type="application/json",
            metadata={"skill_id": skill_id, **self._scope_metadata(scope), **metadata},
        )
        version_id = make_id("skillver")
        existing_count = int(
            self.db.fetch_one("SELECT COUNT(*) AS count FROM skill_versions WHERE skill_id = ?", (skill_id,)).get("count", 0)
        )
        resolved_version = str(version or f"v{existing_count + 1}")
        workflow_text = workflow if isinstance(workflow, str) else json_dumps(workflow or {})
        self.db.execute(
            """
            INSERT INTO skill_versions(id, skill_id, version, prompt_template, workflow, schema_json, object_id, changelog, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_id,
                skill_id,
                resolved_version,
                prompt_template,
                workflow_text,
                json_dumps(schema or {}),
                object_row["id"],
                None,
                json_dumps(metadata or {}),
                now,
            ),
        )
        for tool_name in tools:
            self.db.execute(
                """
                INSERT INTO skill_bindings(id, skill_version_id, tool_name, binding_type, config, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (make_id("bind"), version_id, tool_name, "tool", json_dumps({}), now),
            )
        for test_case in tests:
            self.db.execute(
                """
                INSERT INTO skill_tests(id, skill_version_id, input_payload, expected_output, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    make_id("stest"),
                    version_id,
                    json_dumps(test_case.get("input", {})),
                    json_dumps(test_case.get("expected")),
                    json_dumps(test_case.get("metadata", {})),
                    now,
                ),
            )

        normalized_files = normalize_skill_package_inputs(
            name=name,
            description=description,
            prompt_template=prompt_template,
            workflow=workflow,
            tools=tools,
            topics=topics,
            skill_markdown=skill_markdown,
            base_files=base_files,
            files=list(files or []),
            references=references,
            scripts=scripts,
            assets=assets,
        )
        file_rows: list[dict[str, Any]] = []
        for entry in normalized_files:
            relative_path = entry["relative_path"]
            suffix = "." + relative_path.rsplit(".", 1)[1] if "." in relative_path.rsplit("/", 1)[-1] else ".bin"
            if entry.get("text_content") is not None:
                stored_file = self.object_store.put_text(
                    entry["text_content"],
                    object_type="skill-files",
                    suffix=suffix,
                    prefix=self._object_store_prefix(scope, "skill"),
                )
            else:
                stored_file = self.object_store.put_bytes(
                    entry["content_bytes"],
                    object_type="skill-files",
                    suffix=suffix,
                    prefix=self._object_store_prefix(scope, "skill"),
                )
            object_metadata = {
                "skill_id": skill_id,
                "skill_version_id": version_id,
                "relative_path": relative_path,
                "role": entry["role"],
                **self._scope_metadata(scope),
                **dict(metadata or {}),
                **dict(entry.get("metadata") or {}),
            }
            file_object = self._persist_object(
                stored_file,
                mime_type=entry.get("mime_type") or "application/octet-stream",
                metadata=object_metadata,
            )
            file_id = make_id("sfile")
            file_metadata = {**dict(entry.get("metadata") or {}), "indexable": bool(entry.get("indexable"))}
            self.db.execute(
                """
                INSERT INTO skill_files(id, skill_id, skill_version_id, object_id, relative_path, role, mime_type, size_bytes, checksum, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_id,
                    skill_id,
                    version_id,
                    file_object["id"],
                    relative_path,
                    entry["role"],
                    entry.get("mime_type"),
                    file_object["size_bytes"],
                    file_object["checksum"],
                    json_dumps(file_metadata),
                    now,
                ),
            )
            file_rows.append(
                {
                    "id": file_id,
                    "object_id": file_object["id"],
                    "relative_path": relative_path,
                    "role": entry["role"],
                    "mime_type": entry.get("mime_type"),
                    "metadata": file_metadata,
                    "text_content": entry.get("text_content"),
                    "indexable": bool(entry.get("indexable")),
                }
            )

        for file_row in file_rows:
            if not (file_row["role"] == "reference" and file_row["indexable"] and file_row.get("text_content")):
                continue
            title = f"{name}:{file_row['relative_path']}"
            for index, chunk in enumerate(
                chunk_text_units(
                    str(file_row["text_content"]),
                    source_id=str(file_row["id"]),
                    chunk_size=int(self.config.memory_policy.chunk_size),
                    overlap=int(self.config.memory_policy.chunk_overlap),
                )
            ):
                chunk_id = make_id("skref")
                chunk_metadata = {
                    "chunk_index": index,
                    "relative_path": file_row["relative_path"],
                    "role": file_row["role"],
                    **chunk.metadata,
                    **self._scope_metadata(scope),
                    **dict(metadata or {}),
                    **dict(file_row.get("metadata") or {}),
                }
                self.db.execute(
                    """
                    INSERT INTO skill_reference_chunks(id, skill_id, skill_version_id, file_id, object_id, relative_path, chunk_index, title, content, metadata, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chunk_id,
                        skill_id,
                        version_id,
                        file_row["id"],
                        file_row["object_id"],
                        file_row["relative_path"],
                        index,
                        _chunk_title(title, chunk_metadata),
                        chunk.text,
                        json_dumps(chunk_metadata),
                        now,
                    ),
                )
                self._index_skill_reference_chunk(
                    {
                        "record_id": chunk_id,
                        "skill_id": skill_id,
                        "skill_version_id": version_id,
                        "file_id": file_row["id"],
                        "object_id": file_row["object_id"],
                        "owner_agent_id": scope.get("owner_agent_id"),
                        "source_subject_type": scope.get("subject_type"),
                        "source_subject_id": scope.get("subject_id"),
                        "namespace_key": scope.get("namespace_key"),
                        "relative_path": file_row["relative_path"],
                        "title": _chunk_title(title, chunk_metadata),
                        "text": chunk.text,
                        "metadata": chunk_metadata,
                        "updated_at": now,
                    }
                )

        skill_markdown_text = next(
            (str(file_row.get("text_content") or "") for file_row in file_rows if file_row["role"] == "skill_md"),
            "",
        )
        self._index_skill(
            {
                "record_id": version_id,
                "skill_id": skill_id,
                "version": resolved_version,
                "name": name,
                "description": description,
                "text": "\n".join(
                    part
                    for part in [
                        name,
                        description,
                        skill_markdown_text,
                        prompt_template or "",
                        workflow_text,
                        " ".join(topics),
                        " ".join(tools),
                    ]
                    if part
                ),
                "tools": tools,
                "topics": topics,
                "owner_agent_id": scope.get("owner_agent_id"),
                "source_subject_type": scope.get("subject_type"),
                "source_subject_id": scope.get("subject_id"),
                "namespace_key": scope.get("namespace_key"),
                "metadata": metadata,
                "updated_at": now,
            }
        )
        return {"id": version_id, "version": resolved_version}

    def get_skill(self, skill_id: str) -> dict[str, Any] | None:
        skill = _deserialize_row(self.db.fetch_one("SELECT * FROM skills WHERE id = ?", (skill_id,)))
        if skill is None:
            return None
        skill["versions"] = _deserialize_rows(self.db.fetch_all("SELECT * FROM skill_versions WHERE skill_id = ? ORDER BY created_at DESC", (skill_id,)), ("metadata", "schema_json"))
        for version in skill["versions"]:
            payload = self._load_skill_version_asset(version)
            version["files"] = self._skill_version_files(version["id"])
            version["payload"] = payload
            version["tools"] = payload.get("tools", [])
            version["topics"] = payload.get("topics", [])
            version["tests_payload"] = payload.get("tests", [])
            version["skill_markdown"] = next(
                (item.get("content") for item in version["files"] if item.get("role") == "skill_md"),
                None,
            )
            version["references"] = [item for item in version["files"] if item.get("role") == "reference"]
            version["scripts"] = [item for item in version["files"] if item.get("role") == "script"]
            version["assets"] = [item for item in version["files"] if item.get("role") == "asset"]
        skill["bindings"] = _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT sb.* FROM skill_bindings sb
                JOIN skill_versions sv ON sv.id = sb.skill_version_id
                WHERE sv.skill_id = ?
                ORDER BY sb.created_at ASC
                """,
                (skill_id,),
            ),
            ("config",),
        )
        skill["tests"] = _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT st.* FROM skill_tests st
                JOIN skill_versions sv ON sv.id = st.skill_version_id
                WHERE sv.skill_id = ?
                ORDER BY st.created_at ASC
                """,
                (skill_id,),
            ),
            ("input_payload", "expected_output", "metadata"),
        )
        if skill["versions"]:
            latest = skill["versions"][0]
            skill["files"] = latest.get("files", [])
            skill["skill_markdown"] = latest.get("skill_markdown")
            skill["references"] = latest.get("references", [])
            skill["scripts"] = latest.get("scripts", [])
            skill["assets"] = latest.get("assets", [])
        return skill

    def list_skills(self, status: str | None = None) -> dict[str, Any]:
        if status:
            rows = self.db.fetch_all("SELECT * FROM skills WHERE status = ? ORDER BY updated_at DESC", (status,))
        else:
            rows = self.db.fetch_all("SELECT * FROM skills ORDER BY updated_at DESC")
        return {"results": _deserialize_rows(rows)}

    def search_skills(
        self,
        query: str,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sql_filters = ["s.status = 'active'"]
        params: list[Any] = []
        if owner_agent_id:
            sql_filters.append("(COALESCE(s.owner_agent_id, s.owner_id) = ? OR (s.owner_agent_id IS NULL AND s.owner_id IS NULL))")
            params.append(owner_agent_id)
        if subject_type:
            sql_filters.append("(si.source_subject_type = ? OR si.source_subject_type IS NULL)")
            params.append(subject_type)
        if subject_id:
            sql_filters.append("(si.source_subject_id = ? OR si.source_subject_id IS NULL)")
            params.append(subject_id)
        namespace_filter = self._namespace_filter_value(
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            sql_filters.append("si.namespace_key = ?")
            params.append(namespace_filter)
        rows = self.db.fetch_all(
            f"""
            SELECT si.*, s.status, sic.embedding
            FROM skill_index si
            JOIN skills s ON s.id = si.skill_id
            LEFT JOIN semantic_index_cache sic ON sic.record_id = si.record_id
            WHERE {' AND '.join(sql_filters)}
            ORDER BY si.updated_at DESC
            LIMIT ?
            """,
            tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
        )
        vector_hits = self._vector_hit_map("skill_index", query, limit=max(limit * 4, 24))
        fts_hits = self._fts_hit_map(["skill_index"], query, limit=max(limit * 6, 36))
        for version_id, score in self._skill_reference_version_hit_map(query, limit=max(limit * 4, 24)).items():
            vector_hits[version_id] = max(vector_hits.get(version_id, 0.0), score)
        ranked = self._rank_rows(
            query,
            rows,
            domain="skill",
            text_key="text",
            keywords_getter=lambda row: extract_keywords(" ".join(part for part in [row.get("name"), row.get("description"), row.get("text")] if part)),
            updated_at_key="updated_at",
            importance_getter=lambda row: 0.78,
            half_life_days=self.config.memory_policy.knowledge_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity={"owner_agent_id": owner_agent_id, "subject_type": subject_type, "subject_id": subject_id},
        )
        return {"results": ranked[:limit]}

    def search_skill_references(
        self,
        query: str,
        *,
        skill_id: str | None = None,
        skill_version_id: str | None = None,
        path_prefix: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sql_filters = ["s.status = 'active'"]
        params: list[Any] = []
        if skill_id:
            sql_filters.append("sri.skill_id = ?")
            params.append(skill_id)
        if skill_version_id:
            sql_filters.append("sri.skill_version_id = ?")
            params.append(skill_version_id)
        if owner_agent_id:
            sql_filters.append("(sri.owner_agent_id = ? OR sri.owner_agent_id IS NULL)")
            params.append(owner_agent_id)
        if subject_type:
            sql_filters.append("(sri.source_subject_type = ? OR sri.source_subject_type IS NULL)")
            params.append(subject_type)
        if subject_id:
            sql_filters.append("(sri.source_subject_id = ? OR sri.source_subject_id IS NULL)")
            params.append(subject_id)
        if path_prefix:
            sql_filters.append("sri.relative_path LIKE ?")
            params.append(f"{path_prefix}%")
        namespace_filter = self._namespace_filter_value(
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            sql_filters.append("sri.namespace_key = ?")
            params.append(namespace_filter)
        rows = self.db.fetch_all(
            f"""
            SELECT
                sri.*,
                sri.source_subject_type AS subject_type,
                sri.source_subject_id AS subject_id,
                src.chunk_index,
                s.name AS skill_name,
                s.description AS skill_description,
                sv.version,
                sic.embedding
            FROM skill_reference_index sri
            JOIN skills s ON s.id = sri.skill_id
            JOIN skill_versions sv ON sv.id = sri.skill_version_id
            LEFT JOIN skill_reference_chunks src ON src.id = sri.record_id
            LEFT JOIN semantic_index_cache sic ON sic.record_id = sri.record_id
            WHERE {' AND '.join(sql_filters)}
            ORDER BY sri.updated_at DESC
            LIMIT ?
            """,
            tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
        )
        vector_hits = self._vector_hit_map("skill_reference_index", query, limit=max(limit * 4, 24))
        fts_hits = self._fts_hit_map(["skill_reference_index"], query, limit=max(limit * 6, 36))
        ranked = self._rank_rows(
            query,
            rows,
            domain="skill_reference",
            text_key="text",
            keywords_getter=lambda row: _loads(row.get("keywords"), []),
            updated_at_key="updated_at",
            importance_getter=lambda row: 0.7,
            half_life_days=self.config.memory_policy.knowledge_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity={"owner_agent_id": owner_agent_id, "subject_type": subject_type, "subject_id": subject_id},
        )
        return {"results": ranked[:limit]}

    def archive_memory(self, memory_id: str, **kwargs) -> dict[str, Any]:
        memory = self.get(memory_id)
        if memory is None:
            raise ValueError(f"Memory `{memory_id}` does not exist.")
        if memory.get("status") == "archived":
            return {"memory": memory, "archive": None}
        now = utcnow_iso()
        metadata = dict(kwargs.pop("metadata", {}) or {})
        payload = {"memory": memory, "metadata": metadata}
        archive_id = make_id("arch")
        content_id = make_uuid7()
        summary_text = memory.get("summary") or build_summary(split_sentences(memory["text"]), max_sentences=3, max_chars=240)
        archive_bundle = self._ensure_memory_bundle(
            scope="archive",
            user_id=memory.get("user_id"),
            owner_agent_id=memory.get("owner_agent_id") or memory.get("agent_id"),
            subject_type=memory.get("subject_type"),
            subject_id=memory.get("subject_id"),
            interaction_type=memory.get("interaction_type"),
            namespace_key=memory.get("namespace_key"),
            metadata=self._scope_metadata(memory),
        )
        self.db.execute(
            """
            INSERT INTO archive_memories(id, bundle_id, content_id, domain, source_id, user_id, owner_agent_id, subject_type, subject_id, interaction_type, namespace_key, source_type, session_id, summary, metadata, content_format, created_at, updated_at, last_rehydrated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                archive_id,
                archive_bundle["id"],
                content_id,
                "memory",
                memory_id,
                memory.get("user_id"),
                memory.get("owner_agent_id") or memory.get("agent_id"),
                memory.get("subject_type"),
                memory.get("subject_id"),
                memory.get("interaction_type"),
                memory.get("namespace_key"),
                "memory",
                memory.get("session_id"),
                summary_text,
                json_dumps(merge_metadata(metadata, self._scope_metadata(memory))),
                "application/json",
                now,
                now,
                None,
            ),
        )
        self._upsert_bundle_item(
            "archive",
            archive_bundle["id"],
            self._bundle_archive_item_payload(
                {
                    "id": archive_id,
                    "bundle_id": archive_bundle["id"],
                    "content_id": content_id,
                    "domain": "memory",
                    "source_id": memory_id,
                    "source_type": "memory",
                    "session_id": memory.get("session_id"),
                    "summary": summary_text,
                    "metadata": merge_metadata(metadata, self._scope_metadata(memory)),
                    "created_at": now,
                    "updated_at": now,
                },
                summary=summary_text,
                content=json_dumps(payload),
                metadata=merge_metadata(metadata, self._scope_metadata(memory)),
            ),
        )
        summary_id = make_id("archsum")
        highlights = [memory["text"], summary_text]
        self.db.execute(
            """
            INSERT INTO archive_summaries(id, archive_unit_id, summary, highlights, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (summary_id, archive_id, summary_text, json_dumps(highlights), json_dumps(metadata), now),
        )
        table_name = self._memory_table_for_id(memory_id)
        assert table_name is not None
        self.db.execute(f"UPDATE {table_name} SET status = ?, archived_at = ?, updated_at = ? WHERE id = ?", ("archived", now, now, memory_id))
        if memory.get("bundle_id"):
            self._upsert_bundle_item(
                memory["scope"],
                str(memory["bundle_id"]),
                self._bundle_memory_item_payload({**memory, "status": "archived", "updated_at": now, "archived_at": now}, text=memory["text"]),
            )
        self._delete_memory_index(memory_id)
        self._index_archive_summary(
            {
                "record_id": summary_id,
                "archive_unit_id": archive_id,
                "domain": "memory",
                "user_id": memory.get("user_id"),
                "owner_agent_id": memory.get("owner_agent_id") or memory.get("agent_id"),
                "subject_type": memory.get("subject_type"),
                "subject_id": memory.get("subject_id"),
                "interaction_type": memory.get("interaction_type"),
                "namespace_key": memory.get("namespace_key"),
                "source_type": "memory",
                "session_id": memory.get("session_id"),
                "text": summary_text,
                "metadata": {"source_memory_id": memory_id, **metadata},
                "updated_at": now,
            }
        )
        self._record_memory_event(memory_id, "ARCHIVE", {"archive_unit_id": archive_id})
        return {"memory": self.get(memory_id), "archive": self.get_archive_unit(archive_id)}

    def archive_session(self, session_id: str, **kwargs) -> dict[str, Any]:
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        turns = self._session_turns(session_id)
        snapshots = _deserialize_rows(self.db.fetch_all("SELECT * FROM working_memory_snapshots WHERE session_id = ? ORDER BY updated_at DESC", (session_id,)))
        memories = self.memory_list(
            user_id=session["user_id"],
            owner_agent_id=session.get("owner_agent_id"),
            subject_type=session.get("subject_type"),
            subject_id=session.get("subject_id"),
            interaction_type=session.get("interaction_type"),
            session_id=session_id,
            scope=str(MemoryScope.SESSION),
            limit=200,
        )["results"]
        compression = compress_records(
            [
                *[{"id": item["id"], "text": item["content"], "score": 0.6} for item in turns],
                *[{"id": item["id"], "text": item.get("summary", ""), "score": 0.7} for item in snapshots],
                *[{"id": item["id"], "text": item["text"], "score": float(item.get("importance", 0.5))} for item in memories],
            ],
            budget_chars=int(kwargs.pop("budget_chars", self.config.memory_policy.compression_budget_chars)),
            diversity_lambda=self.config.memory_policy.diversity_lambda,
        )
        now = utcnow_iso()
        metadata = dict(kwargs.pop("metadata", {}) or {})
        payload = {
            "session": session,
            "turns": turns,
            "snapshots": snapshots,
            "memories": memories,
            "compression": compression.as_dict(),
        }
        archive_id = make_id("arch")
        content_id = make_uuid7()
        archive_bundle = self._ensure_memory_bundle(
            scope="archive",
            user_id=session.get("user_id"),
            owner_agent_id=session.get("owner_agent_id") or session.get("agent_id"),
            subject_type=session.get("subject_type"),
            subject_id=session.get("subject_id"),
            interaction_type=session.get("interaction_type"),
            namespace_key=session.get("namespace_key"),
            metadata=self._scope_metadata(session),
        )
        self.db.execute(
            """
            INSERT INTO archive_memories(id, bundle_id, content_id, domain, source_id, user_id, owner_agent_id, subject_type, subject_id, interaction_type, namespace_key, source_type, session_id, summary, metadata, content_format, created_at, updated_at, last_rehydrated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                archive_id,
                archive_bundle["id"],
                content_id,
                "session",
                session_id,
                session.get("user_id"),
                session.get("owner_agent_id") or session.get("agent_id"),
                session.get("subject_type"),
                session.get("subject_id"),
                session.get("interaction_type"),
                session.get("namespace_key"),
                "session",
                session_id,
                compression.summary,
                json_dumps(merge_metadata(metadata, self._scope_metadata(session))),
                "application/json",
                now,
                now,
                None,
            ),
        )
        self._upsert_bundle_item(
            "archive",
            archive_bundle["id"],
            self._bundle_archive_item_payload(
                {
                    "id": archive_id,
                    "bundle_id": archive_bundle["id"],
                    "content_id": content_id,
                    "domain": "session",
                    "source_id": session_id,
                    "source_type": "session",
                    "session_id": session_id,
                    "summary": compression.summary,
                    "metadata": merge_metadata(metadata, self._scope_metadata(session)),
                    "created_at": now,
                    "updated_at": now,
                },
                summary=compression.summary,
                content=json_dumps(payload),
                metadata=merge_metadata(metadata, self._scope_metadata(session)),
            ),
        )
        summary_id = make_id("archsum")
        self.db.execute(
            """
            INSERT INTO archive_summaries(id, archive_unit_id, summary, highlights, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (summary_id, archive_id, compression.summary, json_dumps(compression.highlights), json_dumps(metadata), now),
        )
        self._index_archive_summary(
            {
                "record_id": summary_id,
                "archive_unit_id": archive_id,
                "domain": "session",
                "user_id": session.get("user_id"),
                "owner_agent_id": session.get("owner_agent_id") or session.get("agent_id"),
                "subject_type": session.get("subject_type"),
                "subject_id": session.get("subject_id"),
                "interaction_type": session.get("interaction_type"),
                "namespace_key": session.get("namespace_key"),
                "source_type": "session",
                "session_id": session_id,
                "text": compression.summary,
                "metadata": {"highlights": compression.highlights, **metadata},
                "updated_at": now,
            }
        )
        self.db.execute("UPDATE sessions SET status = ?, updated_at = ? WHERE id = ?", ("archived", now, session_id))
        for item in memories:
            if item.get("status") == "active":
                table_name = self._memory_table_for_id(item["id"])
                if table_name:
                    self.db.execute(f"UPDATE {table_name} SET status = ?, archived_at = ?, updated_at = ? WHERE id = ?", ("archived", now, now, item["id"]))
                if item.get("bundle_id"):
                    self._upsert_bundle_item(
                        item["scope"],
                        str(item["bundle_id"]),
                        self._bundle_memory_item_payload({**item, "status": "archived", "updated_at": now, "archived_at": now}, text=item["text"]),
                    )
                self._delete_memory_index(item["id"])
        return {
            "archive": self.get_archive_unit(archive_id),
            "compression": compression.as_dict(),
        }

    def get_archive_unit(self, archive_unit_id: str) -> dict[str, Any] | None:
        archive = _deserialize_row(self.db.fetch_one("SELECT * FROM archive_memories WHERE id = ?", (archive_unit_id,)))
        if archive is None:
            return None
        bundle_id = str(archive.get("bundle_id") or "").strip()
        if bundle_id:
            bundle = self._get_memory_bundle("archive", bundle_id)
            bundle_item = self._find_bundle_item(bundle, record_id=archive_unit_id, content_id=archive.get("content_id"))
            if bundle_item is not None:
                archive["content"] = bundle_item.get("content")
        archive["summaries"] = _deserialize_rows(self.db.fetch_all("SELECT * FROM archive_summaries WHERE archive_unit_id = ? ORDER BY created_at DESC", (archive_unit_id,)), ("highlights", "metadata"))
        return archive

    def search_archive(
        self,
        query: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        include_global: bool = True,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sql_filters = ["1 = 1"]
        params: list[Any] = []
        if user_id:
            sql_filters.append("(asi.user_id = ? OR (? AND asi.user_id IS NULL))")
            params.extend([user_id, 1 if include_global else 0])
        if owner_agent_id:
            if include_global:
                sql_filters.append("(asi.owner_agent_id = ? OR asi.owner_agent_id IS NULL)")
            else:
                sql_filters.append("asi.owner_agent_id = ?")
            params.append(owner_agent_id)
        if subject_type:
            sql_filters.append("(asi.subject_type = ? OR asi.subject_type IS NULL)")
            params.append(subject_type)
        if subject_id:
            sql_filters.append("(asi.subject_id = ? OR asi.subject_id IS NULL)")
            params.append(subject_id)
        if interaction_type:
            sql_filters.append("(asi.interaction_type = ? OR asi.interaction_type IS NULL)")
            params.append(interaction_type)
        if session_id:
            sql_filters.append("asi.session_id = ?")
            params.append(session_id)
        namespace_filter = self._namespace_filter_value(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            if include_global:
                sql_filters.append("(asi.namespace_key = ? OR asi.namespace_key = 'global')")
            else:
                sql_filters.append("asi.namespace_key = ?")
            params.append(namespace_filter)
        rows = self.db.fetch_all(
            f"""
            SELECT asi.*, sic.embedding
            FROM archive_summary_index asi
            LEFT JOIN semantic_index_cache sic ON sic.record_id = asi.record_id
            WHERE {' AND '.join(sql_filters)}
            ORDER BY asi.updated_at DESC
            LIMIT ?
            """,
            tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
        )
        vector_hits = self._vector_hit_map("archive_summary_index", query, limit=max(limit * 4, 24))
        fts_hits = self._fts_hit_map(["archive_summary_index"], query, limit=max(limit * 6, 36))
        ranked = self._rank_rows(
            query,
            rows,
            domain="archive",
            text_key="text",
            keywords_getter=lambda row: row.get("keywords") or [],
            updated_at_key="updated_at",
            importance_getter=lambda row: 0.6,
            half_life_days=self.config.memory_policy.archive_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity={
                "owner_agent_id": owner_agent_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "interaction_type": interaction_type,
            },
        )
        return {"results": ranked[:limit]}

    def search_interaction(
        self,
        query: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sql_filters = ["1 = 1"]
        params: list[Any] = []
        if session_id:
            sql_filters.append("ct.session_id = ?")
            params.append(session_id)
        if user_id:
            sql_filters.append("s.user_id = ?")
            params.append(user_id)
        if owner_agent_id:
            sql_filters.append("(s.owner_agent_id = ? OR (s.owner_agent_id IS NULL AND s.agent_id = ?))")
            params.extend([owner_agent_id, owner_agent_id])
        if subject_type:
            sql_filters.append("(s.subject_type = ? OR s.subject_type IS NULL)")
            params.append(subject_type)
        if subject_id:
            sql_filters.append("(s.subject_id = ? OR s.subject_id IS NULL)")
            params.append(subject_id)
        if interaction_type:
            sql_filters.append("(s.interaction_type = ? OR s.interaction_type IS NULL)")
            params.append(interaction_type)
        namespace_filter = self._namespace_filter_value(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            sql_filters.append("s.namespace_key = ?")
            params.append(namespace_filter)
        fts_rows = self._search_text_records(["interaction_turn", "interaction_snapshot"], query, limit=max(limit * 6, 24))
        rows = self.db.fetch_all(
            f"""
            SELECT ct.id, ct.session_id, ct.role, ct.turn_type, ct.speaker_participant_id, ct.target_participant_id, ct.salience_score, s.owner_agent_id, s.subject_type, s.subject_id, s.interaction_type, ct.content AS text, ct.metadata, ct.created_at AS updated_at
            FROM conversation_turns ct
            JOIN sessions s ON s.id = ct.session_id
            WHERE {' AND '.join(sql_filters)}
            ORDER BY ct.created_at DESC
            LIMIT ?
            """,
            tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
        )
        turn_ids = [
            row["record_id"]
            for row in fts_rows
            if row.get("collection") == "interaction_turn"
        ]
        if turn_ids:
            placeholders = ", ".join("?" for _ in turn_ids)
            rows = self._merge_rows_by_id(
                rows,
                self.db.fetch_all(
                    f"""
                    SELECT ct.id, ct.session_id, ct.role, ct.turn_type, ct.speaker_participant_id, ct.target_participant_id, ct.salience_score, s.owner_agent_id, s.subject_type, s.subject_id, s.interaction_type, ct.content AS text, ct.metadata, ct.created_at AS updated_at
                    FROM conversation_turns ct
                    JOIN sessions s ON s.id = ct.session_id
                    WHERE {' AND '.join(sql_filters)} AND ct.id IN ({placeholders})
                    """,
                    tuple(params + turn_ids),
                ),
            )
        fts_hits = self._fts_hit_map(["interaction_turn", "interaction_snapshot"], query, limit=max(limit * 6, 24))
        ranked = self._rank_rows(
            query,
            rows,
            domain="interaction",
            text_key="text",
            keywords_getter=lambda row: extract_keywords(str(row.get("text") or "")),
            updated_at_key="updated_at",
            importance_getter=lambda row: 0.68,
            half_life_days=self.config.memory_policy.short_term_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits={},
            fts_hits=fts_hits,
            affinity={
                "owner_agent_id": owner_agent_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "interaction_type": interaction_type,
            },
        )
        snapshot_filters = ["1 = 1"]
        snapshot_params: list[Any] = []
        if session_id:
            snapshot_filters.append("wms.session_id = ?")
            snapshot_params.append(session_id)
        if user_id:
            snapshot_filters.append("s.user_id = ?")
            snapshot_params.append(user_id)
        if owner_agent_id:
            snapshot_filters.append("(wms.owner_agent_id = ? OR wms.owner_agent_id IS NULL)")
            snapshot_params.append(owner_agent_id)
        if subject_type:
            snapshot_filters.append("(wms.subject_type = ? OR wms.subject_type IS NULL)")
            snapshot_params.append(subject_type)
        if subject_id:
            snapshot_filters.append("(wms.subject_id = ? OR wms.subject_id IS NULL)")
            snapshot_params.append(subject_id)
        if interaction_type:
            snapshot_filters.append("(wms.interaction_type = ? OR wms.interaction_type IS NULL)")
            snapshot_params.append(interaction_type)
        if namespace_filter:
            snapshot_filters.append("wms.namespace_key = ?")
            snapshot_params.append(namespace_filter)
        snapshot_rows = self.db.fetch_all(
            f"""
            SELECT wms.id, wms.session_id, wms.owner_agent_id, wms.subject_type, wms.subject_id, wms.interaction_type, wms.summary AS text, wms.metadata, wms.updated_at
            FROM working_memory_snapshots wms
            JOIN sessions s ON s.id = wms.session_id
            WHERE {' AND '.join(snapshot_filters)}
            ORDER BY wms.updated_at DESC
            LIMIT ?
            """,
            tuple(snapshot_params + [max(limit * 6, 12)]),
        )
        snapshot_ids = [
            row["record_id"]
            for row in fts_rows
            if row.get("collection") == "interaction_snapshot"
        ]
        if snapshot_ids:
            placeholders = ", ".join("?" for _ in snapshot_ids)
            snapshot_rows = self._merge_rows_by_id(
                snapshot_rows,
                self.db.fetch_all(
                    f"""
                    SELECT wms.id, wms.session_id, wms.owner_agent_id, wms.subject_type, wms.subject_id, wms.interaction_type, wms.summary AS text, wms.metadata, wms.updated_at
                    FROM working_memory_snapshots wms
                    JOIN sessions s ON s.id = wms.session_id
                    WHERE {' AND '.join(snapshot_filters)} AND wms.id IN ({placeholders})
                    """,
                    tuple(snapshot_params + snapshot_ids),
                ),
            )
        ranked.extend(
            self._rank_rows(
                query,
                snapshot_rows,
                domain="interaction",
                text_key="text",
                keywords_getter=lambda row: extract_keywords(str(row.get("text") or "")),
                updated_at_key="updated_at",
                importance_getter=lambda row: 0.78,
                half_life_days=self.config.memory_policy.short_term_half_life_days,
                threshold=threshold,
                filters=filters,
                vector_hits={},
                fts_hits=fts_hits,
                affinity={
                    "owner_agent_id": owner_agent_id,
                    "subject_type": subject_type,
                    "subject_id": subject_id,
                    "interaction_type": interaction_type,
                },
            )
        )
        ranked.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        ranked = mmr_rerank(ranked, lambda_value=self.config.memory_policy.diversity_lambda, limit=limit)
        return {"results": ranked[:limit]}

    def search_execution(
        self,
        query: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        session_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sql_filters = ["1 = 1"]
        params: list[Any] = []
        if user_id:
            sql_filters.append("user_id = ?")
            params.append(user_id)
        if owner_agent_id:
            sql_filters.append("(owner_agent_id = ? OR (owner_agent_id IS NULL AND agent_id = ?))")
            params.extend([owner_agent_id, owner_agent_id])
        if session_id:
            sql_filters.append("session_id = ?")
            params.append(session_id)
        namespace_filter = self._namespace_filter_value(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            sql_filters.append("namespace_key = ?")
            params.append(namespace_filter)
        fts_rows = self._search_text_records(["execution_run", "execution_observation"], query, limit=max(limit * 6, 24))
        runs = self.db.fetch_all(f"SELECT * FROM runs WHERE {' AND '.join(sql_filters)} ORDER BY updated_at DESC LIMIT 80", tuple(params))
        run_ids = [row["record_id"] for row in fts_rows if row.get("collection") == "execution_run"]
        if run_ids:
            placeholders = ", ".join("?" for _ in run_ids)
            runs = self._merge_rows_by_id(
                runs,
                self.db.fetch_all(
                    f"SELECT * FROM runs WHERE {' AND '.join(sql_filters)} AND id IN ({placeholders})",
                    tuple(params + run_ids),
                ),
            )
        fts_hits = self._fts_hit_map(["execution_run", "execution_observation"], query, limit=max(limit * 6, 24))
        prepared: list[dict[str, Any]] = []
        prepared_ids: set[str] = set()
        for run in runs:
            item = _deserialize_row(run) or dict(run)
            item["text"] = " ".join(part for part in [str(item.get("goal") or ""), str(item.get("status") or "")] if part)
            prepared.append(item)
            prepared_ids.add(str(item.get("id") or ""))
            observations = self.db.fetch_all("SELECT * FROM observations WHERE run_id = ? ORDER BY created_at DESC LIMIT 12", (run["id"],))
            for observation in observations:
                obs_item = _deserialize_row(observation) or dict(observation)
                obs_item["text"] = str(obs_item.get("content") or "")
                obs_item["updated_at"] = obs_item.get("created_at")
                prepared.append(obs_item)
                prepared_ids.add(str(obs_item.get("id") or ""))
        observation_ids = [row["record_id"] for row in fts_rows if row.get("collection") == "execution_observation"]
        if observation_ids:
            observation_filters = ["1 = 1"]
            observation_params: list[Any] = []
            if user_id:
                observation_filters.append("r.user_id = ?")
                observation_params.append(user_id)
            if owner_agent_id:
                observation_filters.append("(r.owner_agent_id = ? OR (r.owner_agent_id IS NULL AND r.agent_id = ?))")
                observation_params.extend([owner_agent_id, owner_agent_id])
            if session_id:
                observation_filters.append("r.session_id = ?")
                observation_params.append(session_id)
            if namespace_filter:
                observation_filters.append("r.namespace_key = ?")
                observation_params.append(namespace_filter)
            placeholders = ", ".join("?" for _ in observation_ids)
            extra_observations = self.db.fetch_all(
                f"""
                SELECT o.*, r.user_id, r.owner_agent_id, r.agent_id, r.namespace_key
                FROM observations o
                JOIN runs r ON r.id = o.run_id
                WHERE {' AND '.join(observation_filters)} AND o.id IN ({placeholders})
                """,
                tuple(observation_params + observation_ids),
            )
            for observation in extra_observations:
                obs_item = _deserialize_row(observation) or dict(observation)
                obs_id = str(obs_item.get("id") or "")
                if not obs_id or obs_id in prepared_ids:
                    continue
                obs_item["text"] = str(obs_item.get("content") or "")
                obs_item["updated_at"] = obs_item.get("created_at")
                prepared.append(obs_item)
                prepared_ids.add(obs_id)
        ranked = self._rank_rows(
            query,
            prepared,
            domain="execution",
            text_key="text",
            keywords_getter=lambda row: extract_keywords(str(row.get("text") or "")),
            updated_at_key="updated_at",
            importance_getter=lambda row: 0.66,
            half_life_days=self.config.memory_policy.archive_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits={},
            fts_hits=fts_hits,
            affinity={"owner_agent_id": owner_agent_id},
        )
        return {"results": ranked[:limit]}

    def promote_session_memories(self, session_id: str, **kwargs) -> dict[str, Any]:
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        turns = self._session_turns(session_id)
        recent_long_term = self.memory_list(
            user_id=session["user_id"],
            owner_agent_id=session.get("owner_agent_id"),
            subject_type=session.get("subject_type"),
            subject_id=session.get("subject_id"),
            interaction_type=session.get("interaction_type"),
            scope=str(MemoryScope.LONG_TERM),
            limit=60,
        )["results"]
        background = [item["text"] for item in recent_long_term]
        messages = self._normalize_messages([{"role": item["role"], "content": item["content"], "metadata": item.get("metadata", {})} for item in turns])
        candidates = self.distiller.distill(messages, background_texts=background, memory_type=str(kwargs.pop("memory_type", str(MemoryType.SEMANTIC))))
        threshold = float(kwargs.pop("threshold", self.config.memory_policy.short_term_capture_threshold))
        created: list[dict[str, Any]] = []
        for candidate in candidates:
            if candidate.score < threshold:
                continue
            stored = self._remember(
                candidate.text,
                user_id=session["user_id"],
                agent_id=session.get("agent_id"),
                owner_agent_id=session.get("owner_agent_id"),
                subject_type=session.get("subject_type"),
                subject_id=session.get("subject_id"),
                interaction_type=session.get("interaction_type"),
                platform_id=session.get("platform_id"),
                workspace_id=session.get("workspace_id"),
                team_id=session.get("team_id"),
                project_id=session.get("project_id"),
                namespace_key=session.get("namespace_key"),
                session_id=session_id,
                metadata={"promoted_from_session": session_id, **candidate.metadata},
                memory_type=candidate.memory_type,
                importance=max(0.35, candidate.score),
                long_term=True,
                source="session_promotion",
            )
            created.append(stored)
        return {"results": created}

    def compress_session_context(self, session_id: str, **kwargs) -> dict[str, Any]:
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        turns = self._session_turns(session_id)
        if len(turns) < self.config.memory_policy.compression_turn_threshold:
            return {"snapshot": None, "compressed": False, "turn_count": len(turns)}
        preserve_recent = int(kwargs.pop("preserve_recent_turns", self.config.memory_policy.compression_preserve_recent_turns))
        budget_chars = int(kwargs.pop("budget_chars", self.config.memory_policy.compression_budget_chars))
        older_turns = turns[:-preserve_recent] if preserve_recent < len(turns) else []
        compression = compress_records(
            [{"id": item["id"], "text": item["content"], "score": 0.42 + min(0.36, len(item["content"]) / 520.0)} for item in older_turns],
            budget_chars=budget_chars,
            diversity_lambda=self.config.memory_policy.diversity_lambda,
        )
        snapshot_id = make_id("snap")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO working_memory_snapshots(id, session_id, run_id, owner_agent_id, interaction_type, subject_type, subject_id, namespace_key, summary, plan, scratchpad, window_size, constraints, resolved_items, unresolved_items, next_actions, budget_tokens, salience_vector, compression_revision, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                session_id,
                kwargs.pop("run_id", None),
                session.get("owner_agent_id") or session.get("agent_id"),
                session.get("interaction_type"),
                session.get("subject_type"),
                session.get("subject_id"),
                session.get("namespace_key"),
                compression.summary,
                None,
                None,
                len(turns),
                json_dumps([]),
                json_dumps([]),
                json_dumps(compression.highlights),
                json_dumps([]),
                estimate_tokens(compression.summary),
                json_dumps([item.get("score", 0.0) for item in older_turns[-12:]]),
                1,
                json_dumps({"highlights": compression.highlights, "kept_ids": compression.kept_ids}),
                now,
                now,
            ),
        )
        snapshot = self.get_snapshot(snapshot_id)
        if snapshot is not None:
            self._index_interaction_snapshot(snapshot)
        return {"snapshot": snapshot, "compressed": True, "turn_count": len(turns)}

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        return _deserialize_row(self.db.fetch_one("SELECT * FROM working_memory_snapshots WHERE id = ?", (snapshot_id,)))

    def session_health(self, session_id: str) -> dict[str, Any]:
        turns = self._session_turns(session_id)
        snapshots = _deserialize_rows(self.db.fetch_all("SELECT * FROM working_memory_snapshots WHERE session_id = ? ORDER BY updated_at DESC", (session_id,)))
        session = self.get_session(session_id)
        memories = self.memory_list(
            user_id=session.get("user_id") if session else None,
            owner_agent_id=session.get("owner_agent_id") if session else None,
            subject_type=session.get("subject_type") if session else None,
            subject_id=session.get("subject_id") if session else None,
            interaction_type=session.get("interaction_type") if session else None,
            session_id=session_id,
            scope=str(MemoryScope.SESSION),
            limit=200,
        )["results"]
        latest_snapshot = snapshots[0] if snapshots else None
        recent_text = "\n".join(item["content"] for item in turns[-self.config.memory_policy.compression_preserve_recent_turns :])
        return {
            "session_id": session_id,
            "turn_count": len(turns),
            "snapshot_count": len(snapshots),
            "session_memory_count": len(memories),
            "estimated_recent_tokens": estimate_tokens(recent_text),
            "needs_compaction": len(turns) >= self.config.memory_policy.compression_turn_threshold,
            "latest_snapshot": latest_snapshot,
        }

    def prune_session_snapshots(self, session_id: str, **kwargs) -> dict[str, Any]:
        keep_recent = int(kwargs.pop("keep_recent", self.config.memory_policy.snapshot_keep_recent))
        snapshots = self.db.fetch_all("SELECT id FROM working_memory_snapshots WHERE session_id = ? ORDER BY updated_at DESC", (session_id,))
        removed = 0
        for item in snapshots[keep_recent:]:
            self.db.execute("DELETE FROM working_memory_snapshots WHERE id = ?", (item["id"],))
            self._delete_text_search_record(item["id"])
            removed += 1
        return {"removed": removed, "kept": min(keep_recent, len(snapshots))}

    def cleanup_low_value_memories(self, **kwargs) -> dict[str, Any]:
        limit = int(kwargs.pop("limit", 20))
        rows = list(
            reversed(
                self._list_memory_rows(
                    scope=str(MemoryScope.LONG_TERM),
                    filters=["status = 'active'"],
                    limit=max(limit * 4, 80),
                )
            )
        )
        _index_rows, semantic_rows = self._memory_index_payloads([row["id"] for row in rows])
        for row in rows:
            row["fingerprint"] = semantic_rows.get(row["id"], {}).get("fingerprint")
        archived: list[dict[str, Any]] = []
        seen: list[dict[str, Any]] = []
        for row in rows:
            importance = float(row.get("importance", 0.5) or 0.0)
            is_weak = importance < self.config.memory_policy.cleanup_importance_threshold
            is_duplicate = any(
                semantic_similarity(row.get("text"), existing.get("text")) >= self.config.memory_policy.merge_threshold
                or hamming_similarity(row.get("fingerprint") or fingerprint(row.get("text")), existing.get("fingerprint") or fingerprint(existing.get("text"))) >= self.config.memory_policy.duplicate_threshold
                for existing in seen
            )
            if is_weak or is_duplicate:
                archived.append(self.archive_memory(row["id"])["memory"])
                if len(archived) >= limit:
                    break
            else:
                seen.append(row)
        return {"results": archived}

    def govern_session(self, session_id: str, **kwargs) -> dict[str, Any]:
        return {
            "session": self.get_session(session_id),
            "health": self.session_health(session_id),
            "compression": self.compress_session_context(session_id, **kwargs),
            "promotion": self.promote_session_memories(session_id, **kwargs),
            "snapshot_prune": self.prune_session_snapshots(session_id, **kwargs),
        }

    def project(self, limit: int | None = None) -> dict[str, Any]:
        projected = {"memory": 0, "knowledge": 0, "skill": 0, "archive": 0}
        rows = self._list_memory_rows(
            filters=["status = 'active'"],
            limit=(limit or self.config.memory_policy.search_scan_limit),
        )
        for row in rows:
            self._index_memory(row)
            projected["memory"] += 1
        knowledge_rows = _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT dc.id, dc.document_id, d.source_id, d.owner_agent_id, d.source_subject_type, d.source_subject_id, d.namespace_key, d.title, dc.content, dc.metadata, d.updated_at
                FROM document_chunks dc
                JOIN documents d ON d.id = dc.document_id
                WHERE d.status = 'active'
                ORDER BY d.updated_at DESC
                LIMIT ?
                """,
                (limit or self.config.memory_policy.search_scan_limit,),
            )
        )
        for row in knowledge_rows:
            self._index_knowledge_chunk(
                {
                    "id": row["id"],
                    "document_id": row["document_id"],
                    "source_id": row["source_id"],
                    "owner_agent_id": row.get("owner_agent_id"),
                    "source_subject_type": row.get("source_subject_type"),
                    "source_subject_id": row.get("source_subject_id"),
                    "namespace_key": row.get("namespace_key"),
                    "title": row["title"],
                    "content": row["content"],
                    "metadata": row.get("metadata", {}),
                    "updated_at": row["updated_at"],
                }
            )
            projected["knowledge"] += 1
        skill_rows = _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT sv.id AS record_id, sv.skill_id, sv.version, s.owner_agent_id, s.source_subject_type, s.source_subject_id, s.namespace_key, s.name, s.description, sv.workflow, sv.prompt_template, s.metadata, s.updated_at
                FROM skill_versions sv
                JOIN skills s ON s.id = sv.skill_id
                WHERE s.status = 'active'
                ORDER BY s.updated_at DESC
                LIMIT ?
                """,
                (limit or self.config.memory_policy.search_scan_limit,),
            )
        )
        for row in skill_rows:
            combined = "\n".join(part for part in [row["name"], row["description"], row.get("prompt_template"), row.get("workflow")] if part)
            self._index_skill({**row, "text": combined, "tools": [], "topics": []})
            projected["skill"] += 1
        reference_rows = _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT src.id AS record_id, src.skill_id, src.skill_version_id, src.file_id, src.object_id, sri.owner_agent_id, sri.source_subject_type,
                       sri.source_subject_id, sri.namespace_key, src.relative_path, src.title, src.content AS text, src.metadata, sv.created_at AS updated_at
                FROM skill_reference_chunks src
                JOIN skill_versions sv ON sv.id = src.skill_version_id
                LEFT JOIN skill_reference_index sri ON sri.record_id = src.id
                ORDER BY sv.created_at DESC
                LIMIT ?
                """,
                (limit or self.config.memory_policy.search_scan_limit,),
            )
        )
        for row in reference_rows:
            self._index_skill_reference_chunk(row)
        archive_rows = _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT asi.*
                FROM archive_summary_index asi
                ORDER BY asi.updated_at DESC
                LIMIT ?
                """,
                (limit or self.config.memory_policy.search_scan_limit,),
            )
        )
        for row in archive_rows:
            self._index_archive_summary(row)
            projected["archive"] += 1
        return {"projected": projected, "vector_backend": getattr(self.vector_index, "name", "sqlite")}

    def describe_capabilities(self) -> dict[str, Any]:
        return {
            "core": capability_dict(
                category="core",
                provider="aimemory",
                features={
                    "local_only": True,
                    "llm_required": False,
                    "portable_storage": True,
                    "fixed_storage_stack": True,
                    "multi_domain_memory": True,
                    "agent_facing_mcp": True,
                    "team_scoped_namespace": True,
                },
            ),
            "embeddings": describe_embedding_runtime(),
            "vector_index": getattr(self.vector_index, "describe_capabilities", lambda: {})(),
            "graph_store": getattr(self.graph_store, "describe_capabilities", lambda: {})(),
            "algorithms": capability_dict(
                category="algorithms",
                provider="local-hybrid",
                features={
                    "adaptive_distillation": True,
                    "semantic_deduplication": True,
                    "hybrid_retrieval": True,
                    "temporal_decay": True,
                    "mmr_reranking": True,
                    "local_context_compression": True,
                },
                items={
                    "policy": asdict(self.config.memory_policy),
                    "litellm_bridge": self.config.providers.as_litellm_kwargs(),
                    "embedding_model": self.config.embeddings.as_provider_kwargs(),
                },
            ),
            "mcp": capability_dict(
                category="mcp",
                provider="local-adapter",
                features={
                    "tool_schema_export": True,
                    "fastmcp_binding": True,
                    "service_required": False,
                    "scoped_tool_defaults": True,
                },
                items={
                    "tools": [item["name"] for item in self.create_mcp_adapter().tool_specs()],
                    "litellm": self.config.providers.as_litellm_kwargs(),
                    "storage_layout": self.storage_layout(),
                    "relational_backend": "sqlite",
                },
            ),
        }

    def scoped(self, **scope_kwargs: Any):
        from aimemory.core.scoped import ScopedAIMemory

        return ScopedAIMemory(self, scope_kwargs)

    def create_mcp_adapter(self, scope: dict[str, Any] | None = None):
        from aimemory.mcp.adapter import AIMemoryMCPAdapter

        return AIMemoryMCPAdapter(self, scope=scope)

    def _domain_api(self):
        if self._agent_store_api is None:
            from aimemory.core.domain_api import AgentStoreAPI

            self._agent_store_api = AgentStoreAPI(self)
        return self._agent_store_api

    def _call_api_method(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        try:
            target = object.__getattribute__(self, method_name)
        except AttributeError:
            target = getattr(self._domain_api(), method_name)
        return target(*args, **kwargs)

    @property
    def api(self):
        if self._structured_api is None:
            from aimemory.core.structured_api import StructuredAIMemoryAPI

            self._structured_api = StructuredAIMemoryAPI(self)
        return self._structured_api

    def litellm_config(self) -> dict[str, Any]:
        return self.config.providers.as_litellm_kwargs()

    def compress_text(
        self,
        text: str,
        *,
        query: str | None = None,
        domain_hint: str | None = None,
        budget_chars: int = 600,
        max_sentences: int = 8,
        max_highlights: int = 12,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result = compress_text_content(
            text,
            query=query,
            domain_hint=domain_hint,
            budget_chars=budget_chars,
            max_sentences=max_sentences,
            diversity_lambda=self.config.memory_policy.diversity_lambda,
            max_highlights=max_highlights,
            policy=self.config.memory_policy,
            metadata=metadata,
        )
        return result.as_dict()

    def compress_document(
        self,
        document_id: str,
        *,
        query: str | None = None,
        budget_chars: int | None = None,
        max_sentences: int = 8,
        max_highlights: int = 12,
    ) -> dict[str, Any]:
        document = self.get_document(document_id)
        if document is None:
            raise ValueError(f"Document `{document_id}` does not exist.")
        chunk_records = [
            {
                "id": chunk["id"],
                "text": str(chunk.get("content") or chunk.get("text") or ""),
                "score": 0.72,
                "metadata": {
                    "document_id": document_id,
                    "title": document.get("title"),
                    "chunk_index": chunk.get("chunk_index"),
                },
            }
            for chunk in document.get("chunks", [])
            if str(chunk.get("content") or chunk.get("text") or "").strip()
        ]
        compression = compress_records(
            chunk_records,
            query=query,
            domain_hint="knowledge",
            budget_chars=int(budget_chars or max(self.config.memory_policy.long_term_compression_budget_chars, 800)),
            max_sentences=max_sentences,
            diversity_lambda=self.config.memory_policy.diversity_lambda,
            max_highlights=max_highlights,
            policy=self.config.memory_policy,
        ).as_dict()
        return {
            "document_id": document_id,
            "title": document.get("title"),
            **compression,
        }

    def compress_skill_references(
        self,
        skill_id: str,
        *,
        skill_version_id: str | None = None,
        path_prefix: str | None = None,
        query: str | None = None,
        budget_chars: int | None = None,
        max_sentences: int = 8,
        max_highlights: int = 12,
    ) -> dict[str, Any]:
        skill = self.get_skill(skill_id)
        if skill is None:
            raise ValueError(f"Skill `{skill_id}` does not exist.")
        versions = list(skill.get("versions", []))
        if skill_version_id:
            versions = [version for version in versions if version.get("id") == skill_version_id]
            if not versions:
                raise ValueError(f"Skill version `{skill_version_id}` does not exist for skill `{skill_id}`.")
        elif versions:
            versions = [versions[0]]

        reference_records: list[dict[str, Any]] = []
        for version in versions:
            for reference in version.get("references", []):
                relative_path = str(reference.get("relative_path") or "")
                if path_prefix and not relative_path.startswith(path_prefix):
                    continue
                text = str(reference.get("content") or "")
                if not text.strip():
                    continue
                reference_records.append(
                    {
                        "id": reference.get("id") or relative_path,
                        "text": text,
                        "score": 0.76,
                        "metadata": {
                            "skill_id": skill_id,
                            "skill_version_id": version.get("id"),
                            "relative_path": relative_path,
                        },
                    }
                )
        if not reference_records:
            raise ValueError(f"Skill `{skill_id}` does not have matching reference files.")
        compression = compress_records(
            reference_records,
            query=query,
            domain_hint="skill_reference",
            budget_chars=int(budget_chars or max(self.config.memory_policy.long_term_compression_budget_chars, 800)),
            max_sentences=max_sentences,
            diversity_lambda=self.config.memory_policy.diversity_lambda,
            max_highlights=max_highlights,
            policy=self.config.memory_policy,
        ).as_dict()
        return {
            "skill_id": skill_id,
            "skill_version_id": versions[0].get("id") if versions else skill_version_id,
            "name": skill.get("name"),
            **compression,
        }

    def register_domain_compressor(self, domain: str, compressor: Callable[..., Any]) -> None:
        self._domain_compressors[str(domain)] = compressor

    def unregister_domain_compressor(self, domain: str) -> None:
        self._domain_compressors.pop(str(domain), None)

    def compress_domain_records(
        self,
        domain: str,
        records: list[dict[str, Any]],
        *,
        scope: dict[str, Any] | None = None,
        threshold_chars: int,
        budget_chars: int,
        force: bool = False,
    ) -> dict[str, Any]:
        total_chars = sum(len(str(item.get("text") or item.get("content") or item.get("summary") or "")) for item in records)
        payload = {
            "domain": str(domain),
            "total_chars": int(total_chars),
            "threshold_chars": int(threshold_chars),
            "budget_chars": int(budget_chars),
            "source_count": len(records),
            "triggered": bool(force or total_chars > threshold_chars),
        }
        if not payload["triggered"] or not records:
            return {
                **payload,
                "summary": "",
                "highlights": [],
                "kept_ids": [],
                "estimated_tokens": 0,
                "facts": [],
                "constraints": [],
                "steps": [],
                "risks": [],
                "selected_unit_ids": [],
                "coverage_score": 0.0,
                "redundancy_score": 0.0,
                "metadata": {},
                "evidence_spans": [],
                "provider": "skipped",
            }
        compressor = self._domain_compressors.get(str(domain))
        if compressor is None:
            result = compress_records(
                records,
                domain_hint=str(domain),
                budget_chars=budget_chars,
                diversity_lambda=self.config.memory_policy.diversity_lambda,
                policy=self.config.memory_policy,
            )
            normalized = result.as_dict()
            provider = "local"
        else:
            external = compressor(
                domain=str(domain),
                records=list(records),
                scope=dict(scope or {}),
                threshold_chars=int(threshold_chars),
                budget_chars=int(budget_chars),
                memory=self,
            )
            if isinstance(external, CompressionResult):
                normalized = external.as_dict()
            elif isinstance(external, str):
                normalized = compress_records(
                    [{"id": item.get("id"), "text": external, "score": 1.0}],
                    domain_hint=str(domain),
                    budget_chars=budget_chars,
                    diversity_lambda=self.config.memory_policy.diversity_lambda,
                    policy=self.config.memory_policy,
                ).as_dict()
            elif isinstance(external, dict):
                normalized = {
                    "summary": str(external.get("summary") or ""),
                    "highlights": list(external.get("highlights") or []),
                    "kept_ids": list(external.get("kept_ids") or []),
                    "estimated_tokens": int(external.get("estimated_tokens") or estimate_tokens(str(external.get("summary") or ""))),
                    "source_count": int(external.get("source_count") or len(records)),
                    "facts": list(external.get("facts") or []),
                    "constraints": list(external.get("constraints") or []),
                    "steps": list(external.get("steps") or []),
                    "risks": list(external.get("risks") or []),
                    "selected_unit_ids": list(external.get("selected_unit_ids") or []),
                    "coverage_score": float(external.get("coverage_score") or 0.0),
                    "redundancy_score": float(external.get("redundancy_score") or 0.0),
                    "metadata": dict(external.get("metadata") or {}),
                    "evidence_spans": list(external.get("evidence_spans") or []),
                }
            else:
                raise TypeError("compressor must return CompressionResult, dict, or str")
            provider = "external"
        return {
            **payload,
            **normalized,
            "provider": provider,
        }

    def __dir__(self):
        names = set(super().__dir__())
        names.update({"api"})
        return sorted(names)

    def close(self) -> None:
        if self._closed:
            return
        self.memory_content_store.close()
        self.db.close()
        self._closed = True

    def __enter__(self) -> "AIMemory":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _normalize_add_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(kwargs)
        if "longTerm" in normalized and "long_term" not in normalized:
            normalized["long_term"] = normalized.pop("longTerm")
        if "ownerAgentId" in normalized and "owner_agent_id" not in normalized:
            normalized["owner_agent_id"] = normalized.pop("ownerAgentId")
        if "subjectType" in normalized and "subject_type" not in normalized:
            normalized["subject_type"] = normalized.pop("subjectType")
        if "subjectId" in normalized and "subject_id" not in normalized:
            normalized["subject_id"] = normalized.pop("subjectId")
        if "interactionType" in normalized and "interaction_type" not in normalized:
            normalized["interaction_type"] = normalized.pop("interactionType")
        if "infer" not in normalized:
            normalized["infer"] = self.config.memory_policy.infer_by_default
        return normalized

    def _normalize_search_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(kwargs)
        if "ownerAgentId" in normalized and "owner_agent_id" not in normalized:
            normalized["owner_agent_id"] = normalized.pop("ownerAgentId")
        if "subjectType" in normalized and "subject_type" not in normalized:
            normalized["subject_type"] = normalized.pop("subjectType")
        if "subjectId" in normalized and "subject_id" not in normalized:
            normalized["subject_id"] = normalized.pop("subjectId")
        if "interactionType" in normalized and "interaction_type" not in normalized:
            normalized["interaction_type"] = normalized.pop("interactionType")
        if "top_k" in normalized and "limit" not in normalized:
            normalized["limit"] = normalized.pop("top_k")
        if "topK" in normalized and "limit" not in normalized:
            normalized["limit"] = normalized.pop("topK")
        if "search_threshold" in normalized and "threshold" not in normalized:
            normalized["threshold"] = normalized.pop("search_threshold")
        if "searchThreshold" in normalized and "threshold" not in normalized:
            normalized["threshold"] = normalized.pop("searchThreshold")
        return normalized

    def _normalize_list_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(kwargs)
        if "page_size" in normalized and "limit" not in normalized:
            normalized["limit"] = normalized.pop("page_size")
        if "pageSize" in normalized and "limit" not in normalized:
            normalized["limit"] = normalized.pop("pageSize")
        if "page" in normalized and "offset" not in normalized:
            page = int(normalized.pop("page"))
            limit = int(normalized.get("limit", 100))
            normalized["offset"] = max(0, page - 1) * limit
        return normalized

    def _resolve_scope(
        self,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        session: dict[str, Any] | None = None,
        global_scope: bool = False,
    ) -> dict[str, str | None]:
        if global_scope:
            return {
                "user_id": None,
                "owner_agent_id": None,
                "subject_type": None,
                "subject_id": None,
                "interaction_type": None,
                "platform_id": None,
                "workspace_id": None,
                "team_id": None,
                "project_id": None,
                "namespace_key": namespace_key or "global",
            }
        resolved_user_id = user_id or (session.get("user_id") if session else None) or self.config.default_user_id
        resolved_owner_agent_id = owner_agent_id or agent_id or (session.get("owner_agent_id") if session else None) or (session.get("agent_id") if session else None)
        resolved_subject_type = subject_type or (session.get("subject_type") if session else None)
        resolved_subject_id = subject_id or (session.get("subject_id") if session else None)
        resolved_platform_id = platform_id or (session.get("platform_id") if session else None) or self.config.platform_id
        resolved_workspace_id = workspace_id or (session.get("workspace_id") if session else None) or self.config.workspace_id
        resolved_team_id = team_id or (session.get("team_id") if session else None) or self.config.team_id
        resolved_project_id = project_id or (session.get("project_id") if session else None) or self.config.project_id
        if resolved_subject_type is None:
            if resolved_subject_id is not None:
                resolved_subject_type = "human" if resolved_subject_id == resolved_user_id else "agent"
            elif resolved_user_id:
                resolved_subject_type = "human"
        if resolved_subject_id is None and resolved_subject_type == "human":
            resolved_subject_id = resolved_user_id
        resolved_interaction_type = interaction_type or (session.get("interaction_type") if session else None)
        if resolved_interaction_type is None:
            resolved_interaction_type = "agent_agent" if resolved_subject_type == "agent" else "human_agent"
        scope = CollaborationScope(
            user_id=resolved_user_id,
            agent_id=agent_id,
            owner_agent_id=resolved_owner_agent_id,
            subject_type=resolved_subject_type,
            subject_id=resolved_subject_id,
            interaction_type=resolved_interaction_type,
            platform_id=resolved_platform_id,
            workspace_id=resolved_workspace_id,
            team_id=resolved_team_id,
            project_id=resolved_project_id,
            namespace_key=namespace_key or (session.get("namespace_key") if session else None),
        )
        return {
            **scope.as_metadata(),
        }

    def _scope_metadata(self, scope: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "user_id": scope.get("user_id"),
                "owner_agent_id": scope.get("owner_agent_id"),
                "subject_type": scope.get("subject_type"),
                "subject_id": scope.get("subject_id"),
                "interaction_type": scope.get("interaction_type"),
                "platform_id": scope.get("platform_id"),
                "workspace_id": scope.get("workspace_id"),
                "team_id": scope.get("team_id"),
                "project_id": scope.get("project_id"),
                "namespace_key": scope.get("namespace_key"),
            }.items()
            if value is not None
        }

    def _namespace_filter_value(
        self,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        session: dict[str, Any] | None = None,
        global_scope: bool = False,
    ) -> str | None:
        scope = self._resolve_scope(
            user_id=user_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
            session=session,
            global_scope=global_scope,
        )
        if namespace_key:
            return namespace_key
        if any(scope.get(key) for key in ("platform_id", "workspace_id", "team_id", "project_id")):
            return scope.get("namespace_key")
        if global_scope:
            return "global"
        return None

    def _object_store_prefix(self, scope: dict[str, Any], domain: str) -> str:
        return CollaborationScope.from_value(scope).storage_prefix(domain)

    def storage_layout(self, **scope_kwargs: Any) -> dict[str, Any]:
        scope = self._resolve_scope(**scope_kwargs)
        memory_vector_path = str(getattr(self.vector_index, "memory_path", self.config.memory_path))
        competency_vector_path = str(getattr(self.vector_index, "competency_path", self.config.competency_path))
        return {
            "scope": scope,
            "root_dir": str(self.config.root_dir),
            "memory_dir": str(self.config.memory_path),
            "competency_dir": str(self.config.competency_path),
            "sqlite_path": str(self.config.sqlite_path),
            "relational_backend": "sqlite",
            "content_backend": "lmdb",
            "lmdb_path": str(self.config.lmdb_path),
            "object_store_path": str(self.config.object_store_path),
            "vector_backend": self._resolve_vector_backend_name(),
            "vector_paths": {
                "memory": memory_vector_path,
                "competency": competency_vector_path,
            },
            "graph_backend": self._resolve_graph_backend_name(),
            "domains": {
                "long_term_memory": {
                    "tables": ["long_term_memories", "memory_index"],
                    "lmdb_bucket": "long_term",
                    "object_prefix": self._object_store_prefix(scope, "memory"),
                    "vector_path": memory_vector_path,
                    "scope": str(MemoryScope.LONG_TERM),
                    "strategy": "dedupe + semantic distill + hybrid retrieval",
                    "compression_threshold_chars": self.config.memory_policy.long_term_char_threshold,
                },
                "short_term_memory": {
                    "tables": ["conversation_turns", "working_memory_snapshots", "short_term_memories"],
                    "lmdb_bucket": "short_term",
                    "object_prefix": self._object_store_prefix(scope, "interaction"),
                    "vector_path": memory_vector_path,
                    "scope": str(MemoryScope.SESSION),
                    "strategy": "session turns + salience compression + promotion",
                    "compression_threshold_chars": self.config.memory_policy.short_term_char_threshold,
                },
                "knowledge": {
                    "tables": ["documents", "document_chunks", "knowledge_chunk_index"],
                    "object_prefix": self._object_store_prefix(scope, "knowledge"),
                    "vector_path": competency_vector_path,
                    "strategy": "chunk + semantic index + lightweight rerank",
                },
                "skill": {
                    "tables": ["skills", "skill_versions", "skill_files", "skill_reference_chunks", "skill_index", "skill_reference_index"],
                    "object_prefix": self._object_store_prefix(scope, "skill"),
                    "vector_path": competency_vector_path,
                    "strategy": "versioned procedural memory + semantic search",
                },
                "archive": {
                    "tables": ["archive_memories", "archive_summaries", "archive_summary_index"],
                    "lmdb_bucket": "archive",
                    "object_prefix": self._object_store_prefix(scope, "archive"),
                    "vector_path": memory_vector_path,
                    "strategy": "budget compression + low-cost summary retrieval",
                    "compression_threshold_chars": self.config.memory_policy.archive_char_threshold,
                },
            },
        }

    def _ensure_participant(
        self,
        participant_type: str,
        external_id: str | None,
        *,
        display_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not external_id:
            return None
        row = self.db.fetch_one(
            "SELECT * FROM participants WHERE participant_type = ? AND external_id = ?",
            (participant_type, external_id),
        )
        now = utcnow_iso()
        payload = dict(metadata or {})
        if row is None:
            participant_id = make_id("part")
            self.db.execute(
                """
                INSERT INTO participants(id, participant_type, external_id, display_name, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (participant_id, participant_type, external_id, display_name or external_id, json_dumps(payload), now, now),
            )
            row = self.db.fetch_one("SELECT * FROM participants WHERE id = ?", (participant_id,))
        else:
            merged_metadata = merge_metadata(_loads(row.get("metadata"), {}), payload)
            self.db.execute(
                "UPDATE participants SET display_name = ?, metadata = ?, updated_at = ? WHERE id = ?",
                (display_name or row.get("display_name") or external_id, json_dumps(merged_metadata), now, row["id"]),
            )
            row = self.db.fetch_one("SELECT * FROM participants WHERE id = ?", (row["id"],))
        return _deserialize_row(row)

    def _bind_session_participant(
        self,
        session_id: str,
        participant_id: str | None,
        participant_role: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not participant_id:
            return
        now = utcnow_iso()
        existing = self.db.fetch_one(
            "SELECT * FROM session_participants WHERE session_id = ? AND participant_id = ?",
            (session_id, participant_id),
        )
        payload = dict(metadata or {})
        if existing is None:
            self.db.execute(
                """
                INSERT INTO session_participants(id, session_id, participant_id, participant_role, joined_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (make_id("spart"), session_id, participant_id, participant_role, now, json_dumps(payload)),
            )
            return
        merged_metadata = merge_metadata(_loads(existing.get("metadata"), {}), payload)
        self.db.execute(
            "UPDATE session_participants SET participant_role = ?, metadata = ? WHERE id = ?",
            (participant_role, json_dumps(merged_metadata), existing["id"]),
        )

    def _ensure_session_participants(self, session: dict[str, Any]) -> list[dict[str, Any]]:
        participants: list[dict[str, Any]] = []
        owner = self._ensure_participant("agent", session.get("owner_agent_id") or session.get("agent_id"), metadata={"session_id": session["id"]})
        if owner:
            self._bind_session_participant(session["id"], owner["id"], "owner_agent")
            participants.append({**owner, "participant_role": "owner_agent"})
        if session.get("subject_type") == "human":
            subject = self._ensure_participant("human", session.get("subject_id") or session.get("user_id"), metadata={"session_id": session["id"]})
            if subject:
                self._bind_session_participant(session["id"], subject["id"], "human_subject")
                participants.append({**subject, "participant_role": "human_subject"})
        elif session.get("subject_type") == "agent":
            subject = self._ensure_participant("agent", session.get("subject_id"), metadata={"session_id": session["id"]})
            if subject:
                self._bind_session_participant(session["id"], subject["id"], "peer_agent")
                participants.append({**subject, "participant_role": "peer_agent"})
        rows = _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT p.*, sp.participant_role, sp.joined_at
                FROM session_participants sp
                JOIN participants p ON p.id = sp.participant_id
                WHERE sp.session_id = ?
                ORDER BY sp.joined_at ASC
                """,
                (session["id"],),
            )
        )
        return rows or participants

    def _build_context(
        self,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        run_id: str | None = None,
        actor_id: str | None = None,
        role: str | None = None,
    ) -> MemoryScopeContext:
        scope = self._resolve_scope(
            user_id=user_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        return MemoryScopeContext(
            user_id=scope["user_id"],
            session_id=session_id,
            agent_id=agent_id or scope["owner_agent_id"],
            owner_agent_id=scope["owner_agent_id"],
            subject_type=scope["subject_type"],
            subject_id=scope["subject_id"],
            interaction_type=scope["interaction_type"],
            platform_id=scope.get("platform_id"),
            workspace_id=scope.get("workspace_id"),
            team_id=scope.get("team_id"),
            project_id=scope.get("project_id"),
            namespace_key=scope.get("namespace_key"),
            run_id=run_id,
            actor_id=actor_id,
            role=role,
        )

    def _normalize_messages(self, messages: Any) -> list[NormalizedMessage]:
        normalized = self.normalizer.normalize(messages)
        return [item for item in normalized if item.content.strip()]

    def _raw_candidates(self, messages: list[NormalizedMessage], *, memory_type: str) -> list[DistilledCandidate]:
        results: list[DistilledCandidate] = []
        for item in messages:
            text = item.content.strip()
            if not text:
                continue
            results.append(
                DistilledCandidate(
                    text=text,
                    score=0.62,
                    novelty=1.0,
                    informativeness=0.6,
                    density=0.6,
                    length_score=0.6,
                    fingerprint=fingerprint(text),
                    embedding=[],
                    memory_type=memory_type,
                    metadata={"source_role": item.role, **item.metadata},
                )
            )
        return results

    def _background_texts(self, *, context: MemoryScopeContext, long_term: bool) -> list[str]:
        scope = str(MemoryScope.LONG_TERM if long_term else MemoryScope.SESSION)
        filters = ["status = 'active'"]
        params: list[Any] = []
        if context.user_id:
            filters.append("user_id = ?")
            params.append(context.user_id)
        if context.owner_agent_id:
            filters.append("(owner_agent_id = ? OR (owner_agent_id IS NULL AND agent_id = ?))")
            params.extend([context.owner_agent_id, context.owner_agent_id])
        if context.subject_type:
            filters.append("(subject_type = ? OR subject_type IS NULL)")
            params.append(context.subject_type)
        if context.subject_id:
            filters.append("(subject_id = ? OR subject_id IS NULL)")
            params.append(context.subject_id)
        if context.interaction_type:
            filters.append("(interaction_type = ? OR interaction_type IS NULL)")
            params.append(context.interaction_type)
        if context.namespace_key:
            filters.append("namespace_key = ?")
            params.append(context.namespace_key)
        if not long_term and context.session_id:
            filters.append("session_id = ?")
            params.append(context.session_id)
        rows = self._list_memory_rows(
            scope=scope,
            filters=filters,
            params=params,
            limit=self.config.memory_policy.background_sample_limit,
        )
        return [str(item.get("text") or "") for item in rows if str(item.get("text") or "").strip()]

    def _remember(
        self,
        text: str,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        memory_type: str = str(MemoryType.SEMANTIC),
        importance: float = 0.5,
        long_term: bool = True,
        source: str = "manual",
    ) -> dict[str, Any]:
        cleaned = text.strip()
        if not cleaned:
            raise ValueError("text must not be empty")
        scope = str(MemoryScope.LONG_TERM if long_term else MemoryScope.SESSION)
        resolved_scope = self._resolve_scope(
            user_id=user_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        user_id = resolved_scope["user_id"] or self.config.default_user_id
        owner_agent_id = resolved_scope["owner_agent_id"]
        subject_type = resolved_scope["subject_type"]
        subject_id = resolved_scope["subject_id"]
        interaction_type = resolved_scope["interaction_type"]
        namespace_key = resolved_scope.get("namespace_key")
        metadata = merge_metadata(metadata or {}, self._scope_metadata(resolved_scope))
        now = utcnow_iso()
        table_name = self._memory_table_for_scope(scope)
        bundle = self._ensure_memory_bundle(
            scope=scope,
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            namespace_key=namespace_key,
            metadata=self._scope_metadata(resolved_scope),
        )
        existing, relation = (None, None)
        if source != "auto_compression":
            existing, relation = self._find_existing_memory(
                cleaned,
                user_id=user_id,
                owner_agent_id=owner_agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                interaction_type=interaction_type,
                namespace_key=namespace_key,
                session_id=session_id,
                scope=scope,
            )
        if existing is not None and relation == "duplicate":
            merged_metadata = merge_metadata(existing.get("metadata"), metadata)
            new_importance = max(float(existing.get("importance", 0.5) or 0.0), float(importance))
            existing_table = self._memory_table_for_id(existing["id"])
            assert existing_table is not None
            self.db.execute(
                f"""
                UPDATE {existing_table}
                SET importance = ?, namespace_key = ?, metadata = ?, updated_at = ?, bundle_id = ?
                WHERE id = ?
                """,
                (new_importance, namespace_key or existing.get("namespace_key"), json_dumps(merged_metadata), now, bundle["id"], existing["id"]),
            )
            self._upsert_bundle_item(
                existing["scope"],
                bundle["id"],
                self._bundle_memory_item_payload(
                    {
                        **existing,
                        "bundle_id": bundle["id"],
                        "importance": new_importance,
                        "metadata": merged_metadata,
                        "updated_at": now,
                    },
                    text=existing["text"],
                ),
            )
            self._record_memory_event(existing["id"], "DUPLICATE_TOUCH", {"incoming_text": cleaned})
            touched = self.get(existing["id"])
            assert touched is not None
            self._index_memory(touched)
            touched["_event"] = "DUPLICATE"
            warning = self._maybe_compact_memory_scope(
                scope=scope,
                bundle_id=bundle["id"],
                user_id=user_id,
                owner_agent_id=owner_agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                interaction_type=interaction_type,
                namespace_key=namespace_key,
                session_id=session_id,
                source=source,
            )
            if warning is not None:
                touched["memory_overflow_warning"] = warning
            return touched
        if existing is not None and relation == "merge":
            merged_text = merge_text_fragments([existing["text"], cleaned], max_sentences=6, max_chars=480)
            merged_summary = build_summary(split_sentences(merged_text), max_sentences=3, max_chars=220)
            merged_metadata = merge_metadata(existing.get("metadata"), metadata)
            new_importance = max(float(existing.get("importance", 0.5) or 0.0), float(importance))
            existing_table = self._memory_table_for_id(existing["id"])
            assert existing_table is not None
            self.db.execute(
                f"""
                UPDATE {existing_table}
                SET summary = ?, importance = ?, namespace_key = ?, metadata = ?, updated_at = ?, bundle_id = ?
                WHERE id = ?
                """,
                (merged_summary, new_importance, namespace_key or existing.get("namespace_key"), json_dumps(merged_metadata), now, bundle["id"], existing["id"]),
            )
            self._upsert_bundle_item(
                existing["scope"],
                bundle["id"],
                self._bundle_memory_item_payload(
                    {
                        **existing,
                        "bundle_id": bundle["id"],
                        "summary": merged_summary,
                        "importance": new_importance,
                        "metadata": merged_metadata,
                        "updated_at": now,
                    },
                    text=merged_text,
                ),
            )
            self._record_memory_event(existing["id"], "MERGE", {"incoming_text": cleaned})
            merged = self.get(existing["id"])
            assert merged is not None
            self._index_memory(merged)
            merged["_event"] = "MERGE"
            warning = self._maybe_compact_memory_scope(
                scope=scope,
                bundle_id=bundle["id"],
                user_id=user_id,
                owner_agent_id=owner_agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                interaction_type=interaction_type,
                namespace_key=namespace_key,
                session_id=session_id,
                source=source,
            )
            if warning is not None:
                merged["memory_overflow_warning"] = warning
            return merged

        memory_id = make_id("mem")
        content_id = make_uuid7()
        summary = build_summary(split_sentences(cleaned), max_sentences=3, max_chars=220)
        self.db.execute(
            f"""
            INSERT INTO {table_name}(id, bundle_id, content_id, user_id, agent_id, owner_agent_id, subject_type, subject_id, interaction_type, namespace_key, session_id, run_id, source_session_id, source_run_id, memory_type, summary, importance, status, source, metadata, content_format, created_at, updated_at, archived_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                memory_id,
                bundle["id"],
                content_id,
                user_id,
                owner_agent_id,
                owner_agent_id,
                subject_type,
                subject_id,
                interaction_type,
                namespace_key,
                session_id,
                run_id,
                session_id,
                run_id,
                memory_type,
                summary,
                float(importance),
                "active",
                source,
                json_dumps(metadata),
                "text/plain",
                now,
                now,
                None,
            ),
        )
        self._upsert_bundle_item(
            scope,
            bundle["id"],
            self._bundle_memory_item_payload(
                {
                    "id": memory_id,
                    "bundle_id": bundle["id"],
                    "content_id": content_id,
                    "summary": summary,
                    "importance": float(importance),
                    "status": "active",
                    "source": source,
                    "memory_type": memory_type,
                    "session_id": session_id,
                    "run_id": run_id,
                    "source_session_id": session_id,
                    "source_run_id": run_id,
                    "metadata": metadata,
                    "created_at": now,
                    "updated_at": now,
                    "archived_at": None,
                },
                text=cleaned,
            ),
        )
        self._record_memory_event(memory_id, "ADD", {"text": cleaned, "scope": scope})
        created = self.get(memory_id)
        assert created is not None
        self._index_memory(created)
        created["_event"] = "ADD"
        warning = self._maybe_compact_memory_scope(
            scope=scope,
            bundle_id=bundle["id"],
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            namespace_key=namespace_key,
            session_id=session_id,
            source=source,
        )
        if warning is not None:
            created["memory_overflow_warning"] = warning
        return created

    def _find_existing_memory(
        self,
        text: str,
        *,
        user_id: str,
        owner_agent_id: str | None,
        subject_type: str | None,
        subject_id: str | None,
        interaction_type: str | None,
        namespace_key: str | None,
        session_id: str | None,
        scope: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        filters = ["user_id = ?", "status = 'active'", "(source IS NULL OR source != 'auto_compression')"]
        params: list[Any] = [user_id]
        if owner_agent_id:
            filters.append("(owner_agent_id = ? OR (owner_agent_id IS NULL AND agent_id = ?))")
            params.extend([owner_agent_id, owner_agent_id])
        if subject_type:
            filters.append("(subject_type = ? OR subject_type IS NULL)")
            params.append(subject_type)
        if subject_id:
            filters.append("(subject_id = ? OR subject_id IS NULL)")
            params.append(subject_id)
        if interaction_type:
            filters.append("(interaction_type = ? OR interaction_type IS NULL)")
            params.append(interaction_type)
        if namespace_key:
            filters.append("namespace_key = ?")
            params.append(namespace_key)
        if scope == str(MemoryScope.SESSION) and session_id:
            filters.append("session_id = ?")
            params.append(session_id)
        rows = self._list_memory_rows(scope=scope, filters=filters, params=params, limit=48)
        _index_rows, semantic_rows = self._memory_index_payloads([row["id"] for row in rows])
        for row in rows:
            row["fingerprint"] = semantic_rows.get(row["id"], {}).get("fingerprint")
        current_fingerprint = fingerprint(text)
        best_duplicate = None
        best_merge = None
        duplicate_score = 0.0
        merge_score = 0.0
        for row in rows:
            semantic = semantic_similarity(text, row.get("text"))
            fp_similarity = hamming_similarity(current_fingerprint, row.get("fingerprint") or fingerprint(row.get("text")))
            combined = max(semantic, (0.55 * semantic) + (0.45 * fp_similarity))
            if combined >= self.config.memory_policy.duplicate_threshold and combined > duplicate_score:
                best_duplicate = row
                duplicate_score = combined
            elif combined >= self.config.memory_policy.merge_threshold and combined > merge_score:
                best_merge = row
                merge_score = combined
        if best_duplicate is not None:
            return best_duplicate, "duplicate"
        if best_merge is not None:
            return best_merge, "merge"
        return None, None

    def _find_generated_memory_by_bundle(self, scope: str, bundle_id: str) -> dict[str, Any] | None:
        if not bundle_id:
            return None
        rows = self._list_memory_rows(
            scope=scope,
            filters=["bundle_id = ?", "status = 'active'", "source = 'auto_compression'"],
            params=[bundle_id],
            limit=1,
        )
        return rows[0] if rows else None

    def _scope_rows_for_bundle(
        self,
        scope: str,
        bundle_id: str,
        *,
        active_only: bool = True,
        include_generated: bool = True,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        filters = ["bundle_id = ?"]
        params: list[Any] = [bundle_id]
        if active_only:
            filters.append("status = 'active'")
        if not include_generated:
            filters.append("(source IS NULL OR source != 'auto_compression')")
        return self._list_memory_rows(scope=scope, filters=filters, params=params, limit=limit)

    def _maybe_compact_memory_scope(
        self,
        *,
        scope: str,
        bundle_id: str | None,
        user_id: str | None,
        owner_agent_id: str | None,
        subject_type: str | None,
        subject_id: str | None,
        interaction_type: str | None,
        namespace_key: str | None,
        session_id: str | None,
        source: str | None,
    ) -> dict[str, Any] | None:
        if scope not in {str(MemoryScope.SESSION), str(MemoryScope.LONG_TERM)}:
            return None
        if not bundle_id or source == "auto_compression":
            return None

        policy = self.config.memory_policy
        threshold_chars = (
            int(policy.short_term_char_threshold)
            if scope == str(MemoryScope.SESSION)
            else int(policy.long_term_char_threshold)
        )
        budget_chars = (
            int(policy.short_term_compression_budget_chars)
            if scope == str(MemoryScope.SESSION)
            else int(policy.long_term_compression_budget_chars)
        )
        active_rows = self._scope_rows_for_bundle(scope, bundle_id, active_only=True, include_generated=False)
        total_chars = sum(len(str(item.get("text") or "")) for item in active_rows)
        if total_chars <= threshold_chars or not active_rows:
            return None

        compression = compress_records(
            [
                {
                    "id": item["id"],
                    "text": str(item.get("summary") or item.get("text") or ""),
                    "score": float(item.get("importance", 0.5) or 0.5),
                    "metadata": {"bundle_id": bundle_id},
                }
                for item in active_rows
                if str(item.get("summary") or item.get("text") or "").strip()
            ],
            domain_hint="short_term" if scope == str(MemoryScope.SESSION) else "long_term",
            budget_chars=budget_chars,
            diversity_lambda=policy.diversity_lambda,
            policy=policy,
        )
        kept_ids = set(compression.kept_ids or ([active_rows[0]["id"]] if active_rows else []))
        table_name = self._memory_table_for_scope(scope)
        now = utcnow_iso()

        for item in active_rows:
            next_status = "active" if item["id"] in kept_ids else "compressed"
            if next_status == item.get("status"):
                continue
            self.db.execute(
                f"UPDATE {table_name} SET status = ?, updated_at = ?, archived_at = ? WHERE id = ?",
                (next_status, now, now if next_status == "compressed" else item.get("archived_at"), item["id"]),
            )
            self._upsert_bundle_item(
                scope,
                bundle_id,
                self._bundle_memory_item_payload({**item, "status": next_status, "updated_at": now, "archived_at": now}, text=item["text"]),
            )
            if next_status == "compressed":
                self._delete_memory_index(item["id"])

        generated_metadata = merge_metadata(
            self._scope_metadata(
                {
                    "user_id": user_id,
                    "owner_agent_id": owner_agent_id,
                    "subject_type": subject_type,
                    "subject_id": subject_id,
                    "interaction_type": interaction_type,
                    "namespace_key": namespace_key,
                }
            ),
            {
                "generated": True,
                "compression": {
                    "scope": scope,
                    "source_count": len(active_rows),
                    "source_ids": [item["id"] for item in active_rows],
                    "kept_ids": list(kept_ids),
                    "threshold_chars": threshold_chars,
                    "total_chars": total_chars,
                },
            },
        )
        generated = self._find_generated_memory_by_bundle(scope, bundle_id)
        if generated is not None:
            generated_row = self.update(
                generated["id"],
                text=compression.summary,
                metadata=generated_metadata,
                importance=max(0.78, float(generated.get("importance", 0.78) or 0.0)),
                status="active",
            )
        else:
            generated_row = self._remember(
                compression.summary,
                user_id=user_id,
                owner_agent_id=owner_agent_id,
                subject_type=subject_type,
                subject_id=subject_id,
                interaction_type=interaction_type,
                namespace_key=namespace_key,
                session_id=session_id,
                long_term=scope == str(MemoryScope.LONG_TERM),
                source="auto_compression",
                metadata=generated_metadata,
                memory_type=str(MemoryType.RELATIONSHIP_SUMMARY),
                importance=0.82,
            )
        return {
            "scope": scope,
            "bundle_id": bundle_id,
            "threshold_chars": threshold_chars,
            "total_chars": total_chars,
            "triggered": True,
            "generated_memory_id": generated_row.get("id"),
            "compression": compression.as_dict(),
        }

    def _record_memory_event(self, memory_id: str, event_type: str, payload: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO memory_events(id, memory_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            (make_id("mevt"), memory_id, event_type, json_dumps(payload), utcnow_iso()),
        )

    def _persist_object(self, stored, *, mime_type: str, metadata: dict[str, Any]) -> dict[str, Any]:
        row = self.db.fetch_one("SELECT * FROM objects WHERE object_key = ?", (stored.object_key,))
        if row is not None:
            return _deserialize_row(row) or dict(row)
        object_id = make_id("obj")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO objects(id, object_key, object_type, mime_type, size_bytes, checksum, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (object_id, stored.object_key, stored.object_type, mime_type, stored.size_bytes, stored.checksum, json_dumps(metadata), now),
        )
        row = self.db.fetch_one("SELECT * FROM objects WHERE id = ?", (object_id,))
        return _deserialize_row(row) or dict(row or {})

    def _index_memory(self, memory: dict[str, Any]) -> None:
        keywords = extract_keywords(memory["text"])
        index_text = self._memory_index_text(memory)
        self.db.execute(
            """
            INSERT INTO memory_index(record_id, domain, scope, user_id, owner_agent_id, subject_type, subject_id, interaction_type, namespace_key, session_id, text, keywords, score_boost, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                scope = excluded.scope,
                user_id = excluded.user_id,
                owner_agent_id = excluded.owner_agent_id,
                subject_type = excluded.subject_type,
                subject_id = excluded.subject_id,
                interaction_type = excluded.interaction_type,
                namespace_key = excluded.namespace_key,
                session_id = excluded.session_id,
                text = excluded.text,
                keywords = excluded.keywords,
                score_boost = excluded.score_boost,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                memory["id"],
                "memory",
                memory["scope"],
                memory.get("user_id"),
                memory.get("owner_agent_id") or memory.get("agent_id"),
                memory.get("subject_type"),
                memory.get("subject_id"),
                memory.get("interaction_type"),
                memory.get("namespace_key"),
                memory.get("session_id"),
                index_text,
                json_dumps(keywords),
                round(float(memory.get("importance", 0.5) or 0.0) * 0.08, 6),
                memory["updated_at"],
                json_dumps({"bundle_id": memory.get("bundle_id"), "content_id": memory.get("content_id"), **dict(memory.get("metadata", {}))}),
            ),
        )
        self._index_memory_search_record(memory, text=index_text, keywords=keywords)
        self._index_semantic_record(
            collection="memory_index",
            record_id=memory["id"],
            domain="memory",
            text=index_text,
            semantic_source_text=memory["text"],
            updated_at=memory["updated_at"],
            quality=float(memory.get("importance", 0.5) or 0.0),
            metadata={"bundle_id": memory.get("bundle_id"), "content_id": memory.get("content_id"), "memory_type": memory.get("memory_type"), "scope": memory.get("scope"), **dict(memory.get("metadata", {}))},
            keywords=keywords,
        )
        self.graph_store.upsert_node("memory", memory["id"], memory.get("summary") or memory["text"][:120], memory.get("metadata"))
        self._link_memory_relations(memory)

    def _delete_memory_index(self, memory_id: str) -> None:
        self.db.execute("DELETE FROM memory_index WHERE record_id = ?", (memory_id,))
        self.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (memory_id,))
        self.db.execute("DELETE FROM memory_links WHERE source_memory_id = ? OR target_memory_id = ?", (memory_id, memory_id))
        self._delete_text_search_record(memory_id)
        self.vector_index.delete("memory_index", memory_id)
        self.graph_store.delete_reference(memory_id)

    def _index_knowledge_chunk(self, payload: dict[str, Any]) -> None:
        keywords = extract_keywords(" ".join(part for part in [payload.get("title"), payload.get("content")] if part))
        self.db.execute(
            """
            INSERT INTO knowledge_chunk_index(record_id, document_id, source_id, owner_agent_id, source_subject_type, source_subject_id, namespace_key, title, text, keywords, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                document_id = excluded.document_id,
                source_id = excluded.source_id,
                owner_agent_id = excluded.owner_agent_id,
                source_subject_type = excluded.source_subject_type,
                source_subject_id = excluded.source_subject_id,
                namespace_key = excluded.namespace_key,
                title = excluded.title,
                text = excluded.text,
                keywords = excluded.keywords,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                payload["id"],
                payload["document_id"],
                payload.get("source_id"),
                payload.get("owner_agent_id"),
                payload.get("source_subject_type"),
                payload.get("source_subject_id"),
                payload.get("namespace_key"),
                payload.get("title"),
                payload["content"],
                json_dumps(keywords),
                payload["updated_at"],
                json_dumps(payload.get("metadata", {})),
            ),
        )
        self._upsert_text_search_record(
            record_id=payload["id"],
            domain="knowledge",
            collection="knowledge_chunk_index",
            title=payload.get("title"),
            text=payload["content"],
            keywords=keywords,
            updated_at=payload.get("updated_at"),
            owner_agent_id=payload.get("owner_agent_id"),
            subject_type=payload.get("source_subject_type"),
            subject_id=payload.get("source_subject_id"),
            namespace_key=payload.get("namespace_key"),
        )
        self._index_semantic_record(
            collection="knowledge_chunk_index",
            record_id=payload["id"],
            domain="knowledge",
            text=payload["content"],
            updated_at=payload["updated_at"],
            quality=0.74,
            metadata={"document_id": payload["document_id"], "title": payload.get("title"), **dict(payload.get("metadata", {}))},
            keywords=keywords,
        )
        self.graph_store.upsert_node("knowledge", payload["id"], payload.get("title") or payload["content"][:120], payload.get("metadata"))

    def _index_skill(self, payload: dict[str, Any]) -> None:
        keywords = extract_keywords(payload["text"])
        self.db.execute(
            """
            INSERT INTO skill_index(record_id, skill_id, version, owner_agent_id, source_subject_type, source_subject_id, namespace_key, name, description, text, keywords, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                skill_id = excluded.skill_id,
                version = excluded.version,
                owner_agent_id = excluded.owner_agent_id,
                source_subject_type = excluded.source_subject_type,
                source_subject_id = excluded.source_subject_id,
                namespace_key = excluded.namespace_key,
                name = excluded.name,
                description = excluded.description,
                text = excluded.text,
                keywords = excluded.keywords,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                payload["record_id"],
                payload["skill_id"],
                payload["version"],
                payload.get("owner_agent_id"),
                payload.get("source_subject_type"),
                payload.get("source_subject_id"),
                payload.get("namespace_key"),
                payload["name"],
                payload.get("description"),
                payload["text"],
                json_dumps(keywords),
                payload["updated_at"],
                json_dumps(payload.get("metadata", {})),
            ),
        )
        self._upsert_text_search_record(
            record_id=payload["record_id"],
            domain="skill",
            collection="skill_index",
            title=payload.get("name"),
            text=" ".join(part for part in [payload.get("description"), payload["text"]] if part),
            keywords=keywords,
            updated_at=payload.get("updated_at"),
            owner_agent_id=payload.get("owner_agent_id"),
            subject_type=payload.get("source_subject_type"),
            subject_id=payload.get("source_subject_id"),
            namespace_key=payload.get("namespace_key"),
        )
        self._index_semantic_record(
            collection="skill_index",
            record_id=payload["record_id"],
            domain="skill",
            text=payload["text"],
            updated_at=payload["updated_at"],
            quality=0.8,
            metadata={"skill_id": payload["skill_id"], "version": payload["version"], **dict(payload.get("metadata", {}))},
            keywords=keywords,
        )
        self.graph_store.upsert_node("skill", payload["skill_id"], payload["name"], payload.get("metadata"))

    def _index_skill_reference_chunk(self, payload: dict[str, Any]) -> None:
        keywords = extract_keywords(" ".join(part for part in [payload.get("title"), payload.get("text")] if part))
        self.db.execute(
            """
            INSERT INTO skill_reference_index(
                record_id, skill_id, skill_version_id, file_id, object_id, owner_agent_id, source_subject_type, source_subject_id,
                namespace_key, relative_path, title, text, keywords, updated_at, metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                skill_id = excluded.skill_id,
                skill_version_id = excluded.skill_version_id,
                file_id = excluded.file_id,
                object_id = excluded.object_id,
                owner_agent_id = excluded.owner_agent_id,
                source_subject_type = excluded.source_subject_type,
                source_subject_id = excluded.source_subject_id,
                namespace_key = excluded.namespace_key,
                relative_path = excluded.relative_path,
                title = excluded.title,
                text = excluded.text,
                keywords = excluded.keywords,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                payload["record_id"],
                payload["skill_id"],
                payload["skill_version_id"],
                payload["file_id"],
                payload["object_id"],
                payload.get("owner_agent_id"),
                payload.get("source_subject_type"),
                payload.get("source_subject_id"),
                payload.get("namespace_key"),
                payload["relative_path"],
                payload.get("title"),
                payload["text"],
                json_dumps(keywords),
                payload["updated_at"],
                json_dumps(payload.get("metadata", {})),
            ),
        )
        self._upsert_text_search_record(
            record_id=payload["record_id"],
            domain="skill",
            collection="skill_reference_index",
            title=payload.get("title"),
            text=payload["text"],
            keywords=keywords,
            path=payload.get("relative_path"),
            updated_at=payload.get("updated_at"),
            owner_agent_id=payload.get("owner_agent_id"),
            subject_type=payload.get("source_subject_type"),
            subject_id=payload.get("source_subject_id"),
            namespace_key=payload.get("namespace_key"),
        )
        self._index_semantic_record(
            collection="skill_reference_index",
            record_id=payload["record_id"],
            domain="skill",
            text=payload["text"],
            updated_at=payload["updated_at"],
            quality=0.76,
            metadata={
                "skill_id": payload["skill_id"],
                "skill_version_id": payload["skill_version_id"],
                "file_id": payload["file_id"],
                "relative_path": payload["relative_path"],
                **dict(payload.get("metadata", {})),
            },
            keywords=keywords,
        )

    def _index_archive_summary(self, payload: dict[str, Any]) -> None:
        keywords = payload.get("keywords") or extract_keywords(payload["text"])
        self.db.execute(
            """
            INSERT INTO archive_summary_index(record_id, archive_unit_id, domain, user_id, owner_agent_id, subject_type, subject_id, interaction_type, namespace_key, source_type, session_id, text, keywords, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                archive_unit_id = excluded.archive_unit_id,
                domain = excluded.domain,
                user_id = excluded.user_id,
                owner_agent_id = excluded.owner_agent_id,
                subject_type = excluded.subject_type,
                subject_id = excluded.subject_id,
                interaction_type = excluded.interaction_type,
                namespace_key = excluded.namespace_key,
                source_type = excluded.source_type,
                session_id = excluded.session_id,
                text = excluded.text,
                keywords = excluded.keywords,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                payload["record_id"],
                payload["archive_unit_id"],
                payload["domain"],
                payload.get("user_id"),
                payload.get("owner_agent_id"),
                payload.get("subject_type"),
                payload.get("subject_id"),
                payload.get("interaction_type"),
                payload.get("namespace_key"),
                payload.get("source_type"),
                payload.get("session_id"),
                payload["text"],
                json_dumps(keywords),
                payload["updated_at"],
                json_dumps(payload.get("metadata", {})),
            ),
        )
        self._upsert_text_search_record(
            record_id=payload["record_id"],
            domain="archive",
            collection="archive_summary_index",
            title=payload.get("domain"),
            text=payload["text"],
            keywords=list(keywords),
            updated_at=payload.get("updated_at"),
            user_id=payload.get("user_id"),
            owner_agent_id=payload.get("owner_agent_id"),
            subject_type=payload.get("subject_type"),
            subject_id=payload.get("subject_id"),
            interaction_type=payload.get("interaction_type"),
            session_id=payload.get("session_id"),
            namespace_key=payload.get("namespace_key"),
        )
        self._index_semantic_record(
            collection="archive_summary_index",
            record_id=payload["record_id"],
            domain="archive",
            text=payload["text"],
            updated_at=payload["updated_at"],
            quality=0.62,
            metadata={"archive_unit_id": payload["archive_unit_id"], **dict(payload.get("metadata", {}))},
            keywords=list(keywords),
        )
        self.graph_store.upsert_node("archive", payload["archive_unit_id"], payload["text"][:120], payload.get("metadata"))

    def _index_semantic_record(
        self,
        *,
        collection: str,
        record_id: str,
        domain: str,
        text: str,
        semantic_source_text: str | None = None,
        updated_at: str,
        quality: float,
        metadata: dict[str, Any],
        keywords: list[str],
    ) -> None:
        source_text = semantic_source_text if semantic_source_text is not None else text
        payload = {
            "domain": domain,
            "embedding": json_dumps([] if not source_text else self._embedding_for_text(source_text)),
            "fingerprint": fingerprint(source_text),
            "quality": quality,
            "updated_at": updated_at,
            "metadata": json_dumps(metadata),
            "keywords": keywords,
        }
        self.vector_index.upsert(collection, record_id, text, payload)
        if getattr(self.vector_index, "name", "") != "sqlite":
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
                    domain,
                    collection,
                    text,
                    payload["embedding"],
                    payload["fingerprint"],
                    quality,
                    updated_at,
                    payload["metadata"],
                ),
            )

    def _embedding_for_text(self, text: str) -> list[float]:
        return embed_text(text, dims=int(self.config.embeddings.dimensions))

    def _lexical_index_text(self, *parts: Any, limit: int = 256) -> str:
        tokens: list[str] = []
        for part in parts:
            if part in (None, ""):
                continue
            tokens.extend(tokenize(str(part)))
        unique = list(dict.fromkeys(token for token in tokens if token))
        return " ".join(unique[:limit])

    def _text_search_query(self, query: str, *, max_terms: int = 14) -> str:
        normalized = normalize_text(query)
        if not normalized:
            return ""
        clauses: list[str] = []
        phrase = normalized.replace('"', " ").strip()
        if phrase:
            clauses.append(f'"{phrase}"')
        for token in list(dict.fromkeys(tokenize(normalized)))[:max_terms]:
            cleaned = token.replace('"', " ").strip()
            if cleaned:
                clauses.append(f'"{cleaned}"')
        return " OR ".join(dict.fromkeys(clauses))

    def _upsert_text_search_record(
        self,
        *,
        record_id: str,
        domain: str,
        collection: str,
        text: str,
        title: str | None = None,
        keywords: list[str] | None = None,
        path: str | None = None,
        updated_at: str | None = None,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        namespace_key: str | None = None,
    ) -> None:
        title_text = str(title or "")
        body_text = str(text or "")
        path_text = str(path or "")
        keyword_list = [str(item).strip() for item in (keywords or []) if str(item).strip()]
        keyword_text = " ".join(dict.fromkeys(keyword_list))
        lexical = self._lexical_index_text(title_text, body_text, keyword_text, path_text)
        if not any(item.strip() for item in (title_text, body_text, keyword_text, lexical, path_text)):
            self._delete_text_search_record(record_id)
            return
        self._delete_text_search_record(record_id)
        self.db.execute(
            """
            INSERT INTO text_search_index(
                record_id, domain, collection, title, text, keywords, lexical, path, updated_at,
                user_id, owner_agent_id, subject_type, subject_id, interaction_type, session_id, run_id, namespace_key
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                domain,
                collection,
                title_text,
                body_text,
                keyword_text,
                lexical,
                path_text,
                str(updated_at or ""),
                user_id,
                owner_agent_id,
                subject_type,
                subject_id,
                interaction_type,
                session_id,
                run_id,
                namespace_key,
            ),
        )

    def _delete_text_search_record(self, record_id: str) -> None:
        self.db.execute("DELETE FROM text_search_index WHERE record_id = ?", (record_id,))

    def _search_text_records(self, collections: list[str] | tuple[str, ...], query: str, *, limit: int) -> list[dict[str, Any]]:
        match_query = self._text_search_query(query)
        if not match_query or not collections:
            return []
        placeholders = ", ".join("?" for _ in collections)
        try:
            rows = self.db.fetch_all(
                f"""
                SELECT
                    record_id,
                    collection,
                    bm25(text_search_index, 3.2, 1.2, 1.8, 4.4, 2.1) AS bm25_score
                FROM text_search_index
                WHERE text_search_index MATCH ?
                  AND collection IN ({placeholders})
                ORDER BY bm25_score ASC
                LIMIT ?
                """,
                tuple([match_query, *collections, max(1, limit)]),
            )
        except Exception:
            return []
        results: list[dict[str, Any]] = []
        window = max(1, limit)
        for index, row in enumerate(rows):
            raw_score = float(row.get("bm25_score", 1.0) or 1.0)
            base = 1.0 / (1.0 + abs(raw_score))
            rank_bonus = max(0.0, 1.0 - (index / window))
            results.append(
                {
                    "record_id": str(row.get("record_id") or ""),
                    "collection": str(row.get("collection") or ""),
                    "score": round((0.68 * base) + (0.32 * rank_bonus), 6),
                }
            )
        return results

    def _fts_hit_map(self, collections: list[str] | tuple[str, ...], query: str, *, limit: int) -> dict[str, float]:
        hits: dict[str, float] = {}
        for row in self._search_text_records(collections, query, limit=limit):
            record_id = str(row.get("record_id") or "")
            if not record_id:
                continue
            hits[record_id] = max(hits.get(record_id, 0.0), float(row.get("score", 0.0) or 0.0))
        return hits

    def _merge_rows_by_id(self, *groups: list[dict[str, Any]], id_key: str = "id") -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for group in groups:
            for row in group:
                record_id = str(row.get(id_key) or row.get("record_id") or "")
                if record_id and record_id not in merged:
                    merged[record_id] = row
        return list(merged.values())

    def _index_memory_search_record(self, memory: dict[str, Any], *, text: str, keywords: list[str]) -> None:
        self._upsert_text_search_record(
            record_id=memory["id"],
            domain="memory",
            collection="memory_index",
            title=memory.get("summary"),
            text=text,
            keywords=keywords,
            updated_at=memory.get("updated_at"),
            user_id=memory.get("user_id"),
            owner_agent_id=memory.get("owner_agent_id") or memory.get("agent_id"),
            subject_type=memory.get("subject_type"),
            subject_id=memory.get("subject_id"),
            interaction_type=memory.get("interaction_type"),
            session_id=memory.get("session_id"),
            run_id=memory.get("run_id"),
            namespace_key=memory.get("namespace_key"),
        )

    def _index_interaction_turn(self, session: dict[str, Any], *, turn_id: str, role: str, content: str, turn_type: str | None, updated_at: str) -> None:
        self._upsert_text_search_record(
            record_id=turn_id,
            domain="interaction",
            collection="interaction_turn",
            title=" ".join(part for part in [session.get("title"), role, turn_type] if part),
            text=content,
            keywords=extract_keywords(content),
            updated_at=updated_at,
            user_id=session.get("user_id"),
            owner_agent_id=session.get("owner_agent_id") or session.get("agent_id"),
            subject_type=session.get("subject_type"),
            subject_id=session.get("subject_id"),
            interaction_type=session.get("interaction_type"),
            session_id=session.get("id"),
            namespace_key=session.get("namespace_key"),
        )

    def _index_interaction_snapshot(self, snapshot: dict[str, Any]) -> None:
        self._upsert_text_search_record(
            record_id=snapshot["id"],
            domain="interaction",
            collection="interaction_snapshot",
            title=f"snapshot {snapshot.get('session_id') or ''}".strip(),
            text=str(snapshot.get("summary") or ""),
            keywords=extract_keywords(str(snapshot.get("summary") or "")),
            updated_at=snapshot.get("updated_at"),
            owner_agent_id=snapshot.get("owner_agent_id"),
            subject_type=snapshot.get("subject_type"),
            subject_id=snapshot.get("subject_id"),
            interaction_type=snapshot.get("interaction_type"),
            session_id=snapshot.get("session_id"),
            run_id=snapshot.get("run_id"),
            namespace_key=snapshot.get("namespace_key"),
        )

    def _index_execution_run(self, run: dict[str, Any]) -> None:
        text = " ".join(part for part in [str(run.get("goal") or ""), str(run.get("status") or "")] if part).strip()
        self._upsert_text_search_record(
            record_id=run["id"],
            domain="execution",
            collection="execution_run",
            title=run.get("goal"),
            text=text,
            keywords=extract_keywords(text),
            updated_at=run.get("updated_at"),
            user_id=run.get("user_id"),
            owner_agent_id=run.get("owner_agent_id") or run.get("agent_id"),
            subject_type=run.get("subject_type"),
            subject_id=run.get("subject_id"),
            interaction_type=run.get("interaction_type"),
            session_id=run.get("session_id"),
            run_id=run.get("id"),
            namespace_key=run.get("namespace_key"),
        )

    def _index_execution_observation(self, observation: dict[str, Any], *, run: dict[str, Any] | None = None) -> None:
        run_row = dict(run or {})
        text = str(observation.get("content") or observation.get("text") or "")
        title = str(observation.get("kind") or "observation")
        self._upsert_text_search_record(
            record_id=observation["id"],
            domain="execution",
            collection="execution_observation",
            title=title,
            text=text,
            keywords=extract_keywords(" ".join(part for part in [title, text] if part)),
            updated_at=observation.get("created_at") or observation.get("updated_at"),
            user_id=run_row.get("user_id"),
            owner_agent_id=run_row.get("owner_agent_id") or run_row.get("agent_id"),
            subject_type=run_row.get("subject_type"),
            subject_id=run_row.get("subject_id"),
            interaction_type=run_row.get("interaction_type"),
            session_id=observation.get("session_id") or run_row.get("session_id"),
            run_id=observation.get("run_id"),
            namespace_key=run_row.get("namespace_key"),
        )

    def _memory_index_text(self, memory: dict[str, Any]) -> str:
        summary_text = str(memory.get("summary") or "").strip()
        body_text = str(memory.get("text") or "")
        if len(body_text) <= 720:
            return body_text
        return "\n".join(part for part in [summary_text, body_text[:720]] if part)

    def _expected_text_search_count(self) -> int:
        statements = [
            "SELECT COUNT(*) AS count FROM memory_index",
            "SELECT COUNT(*) AS count FROM knowledge_chunk_index",
            "SELECT COUNT(*) AS count FROM skill_index",
            "SELECT COUNT(*) AS count FROM skill_reference_index",
            "SELECT COUNT(*) AS count FROM archive_summary_index",
            "SELECT COUNT(*) AS count FROM conversation_turns",
            "SELECT COUNT(*) AS count FROM working_memory_snapshots",
            "SELECT COUNT(*) AS count FROM runs",
            "SELECT COUNT(*) AS count FROM observations",
        ]
        total = 0
        for statement in statements:
            row = self.db.fetch_one(statement)
            total += int((row or {}).get("count", 0) or 0)
        return total

    def _sync_text_search_index(self) -> None:
        try:
            current = int((self.db.fetch_one("SELECT COUNT(*) AS count FROM text_search_index") or {}).get("count", 0) or 0)
        except Exception:
            return
        expected = self._expected_text_search_count()
        if current == expected:
            return
        self.db.execute("DELETE FROM text_search_index")
        self._rebuild_text_search_index()

    def _rebuild_text_search_index(self) -> None:
        memory_limit = max(
            1,
            int((self.db.fetch_one("SELECT COUNT(*) AS count FROM short_term_memories WHERE status = 'active'") or {}).get("count", 0) or 0)
            + int((self.db.fetch_one("SELECT COUNT(*) AS count FROM long_term_memories WHERE status = 'active'") or {}).get("count", 0) or 0),
        )
        for memory in self._list_memory_rows(scope="all", filters=["status = 'active'"], limit=memory_limit):
            self._index_memory_search_record(memory, text=self._memory_index_text(memory), keywords=extract_keywords(memory.get("text")))

        knowledge_rows = self.db.fetch_all(
            """
            SELECT dc.id, dc.content AS text, d.title, d.updated_at, d.owner_agent_id, d.source_subject_type, d.source_subject_id, d.namespace_key
            FROM document_chunks dc
            JOIN documents d ON d.id = dc.document_id
            WHERE d.status = 'active'
            """
        )
        for row in knowledge_rows:
            text = str(row.get("text") or "")
            self._upsert_text_search_record(
                record_id=row["id"],
                domain="knowledge",
                collection="knowledge_chunk_index",
                title=row.get("title"),
                text=text,
                keywords=extract_keywords(" ".join(part for part in [row.get("title"), text] if part)),
                updated_at=row.get("updated_at"),
                owner_agent_id=row.get("owner_agent_id"),
                subject_type=row.get("source_subject_type"),
                subject_id=row.get("source_subject_id"),
                namespace_key=row.get("namespace_key"),
            )

        for row in self.db.fetch_all("SELECT * FROM skill_index"):
            text = " ".join(part for part in [row.get("description"), row.get("text")] if part)
            self._upsert_text_search_record(
                record_id=row["record_id"],
                domain="skill",
                collection="skill_index",
                title=row.get("name"),
                text=text,
                keywords=_loads(row.get("keywords"), []),
                updated_at=row.get("updated_at"),
                owner_agent_id=row.get("owner_agent_id"),
                subject_type=row.get("source_subject_type"),
                subject_id=row.get("source_subject_id"),
                namespace_key=row.get("namespace_key"),
            )

        for row in self.db.fetch_all("SELECT * FROM skill_reference_index"):
            self._upsert_text_search_record(
                record_id=row["record_id"],
                domain="skill",
                collection="skill_reference_index",
                title=row.get("title"),
                text=str(row.get("text") or ""),
                keywords=_loads(row.get("keywords"), []),
                path=row.get("relative_path"),
                updated_at=row.get("updated_at"),
                owner_agent_id=row.get("owner_agent_id"),
                subject_type=row.get("source_subject_type"),
                subject_id=row.get("source_subject_id"),
                namespace_key=row.get("namespace_key"),
            )

        for row in self.db.fetch_all("SELECT * FROM archive_summary_index"):
            self._upsert_text_search_record(
                record_id=row["record_id"],
                domain="archive",
                collection="archive_summary_index",
                title=row.get("domain"),
                text=str(row.get("text") or ""),
                keywords=_loads(row.get("keywords"), []),
                updated_at=row.get("updated_at"),
                user_id=row.get("user_id"),
                owner_agent_id=row.get("owner_agent_id"),
                subject_type=row.get("subject_type"),
                subject_id=row.get("subject_id"),
                interaction_type=row.get("interaction_type"),
                session_id=row.get("session_id"),
                namespace_key=row.get("namespace_key"),
            )

        interaction_turns = self.db.fetch_all(
            """
            SELECT ct.id, ct.role, ct.turn_type, ct.content, ct.created_at, s.id AS session_id, s.title, s.user_id, s.owner_agent_id, s.agent_id, s.subject_type, s.subject_id, s.interaction_type, s.namespace_key
            FROM conversation_turns ct
            JOIN sessions s ON s.id = ct.session_id
            """
        )
        for row in interaction_turns:
            self._index_interaction_turn(
                {
                    "id": row.get("session_id"),
                    "title": row.get("title"),
                    "user_id": row.get("user_id"),
                    "owner_agent_id": row.get("owner_agent_id"),
                    "agent_id": row.get("agent_id"),
                    "subject_type": row.get("subject_type"),
                    "subject_id": row.get("subject_id"),
                    "interaction_type": row.get("interaction_type"),
                    "namespace_key": row.get("namespace_key"),
                },
                turn_id=row["id"],
                role=str(row.get("role") or ""),
                content=str(row.get("content") or ""),
                turn_type=row.get("turn_type"),
                updated_at=str(row.get("created_at") or ""),
            )

        for snapshot in _deserialize_rows(self.db.fetch_all("SELECT * FROM working_memory_snapshots")):
            self._index_interaction_snapshot(snapshot)

        for run in _deserialize_rows(self.db.fetch_all("SELECT * FROM runs")):
            self._index_execution_run(run)
        observation_rows = self.db.fetch_all(
            """
            SELECT o.*, r.user_id, r.owner_agent_id, r.agent_id, r.subject_type, r.subject_id, r.interaction_type, r.namespace_key
            FROM observations o
            JOIN runs r ON r.id = o.run_id
            """
        )
        for row in observation_rows:
            self._index_execution_observation(
                _deserialize_row(row) or dict(row),
                run={
                    "user_id": row.get("user_id"),
                    "owner_agent_id": row.get("owner_agent_id"),
                    "agent_id": row.get("agent_id"),
                    "subject_type": row.get("subject_type"),
                    "subject_id": row.get("subject_id"),
                    "interaction_type": row.get("interaction_type"),
                    "session_id": row.get("session_id"),
                    "namespace_key": row.get("namespace_key"),
                },
            )

    def _link_memory_relations(self, memory: dict[str, Any]) -> None:
        self.db.execute("DELETE FROM memory_links WHERE source_memory_id = ?", (memory["id"],))
        filters = ["id != ?", "status = 'active'", "user_id = ?"]
        params: list[Any] = [memory["id"], memory.get("user_id")]
        owner_agent_id = memory.get("owner_agent_id") or memory.get("agent_id")
        if owner_agent_id:
            filters.append("(owner_agent_id = ? OR (owner_agent_id IS NULL AND agent_id = ?))")
            params.extend([owner_agent_id, owner_agent_id])
        if memory.get("subject_type"):
            filters.append("(subject_type = ? OR subject_type IS NULL)")
            params.append(memory.get("subject_type"))
        if memory.get("subject_id"):
            filters.append("(subject_id = ? OR subject_id IS NULL)")
            params.append(memory.get("subject_id"))
        candidates = self._list_memory_rows(scope=memory.get("scope") or "all", filters=filters, params=params, limit=24)
        _index_rows, semantic_rows = self._memory_index_payloads([row["id"] for row in candidates])
        for candidate in candidates:
            candidate["fingerprint"] = semantic_rows.get(candidate["id"], {}).get("fingerprint")
        relation_count = 0
        for candidate in candidates:
            similarity = semantic_similarity(memory.get("text"), candidate.get("text"))
            if similarity < self.config.memory_policy.relation_threshold:
                continue
            self.db.execute(
                """
                INSERT INTO memory_links(id, source_memory_id, target_memory_id, link_type, weight, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (make_id("mlink"), memory["id"], candidate["id"], "semantic", similarity, json_dumps({"score": similarity}), utcnow_iso()),
            )
            self.graph_store.upsert_edge("memory", memory["id"], "related_to", "memory", candidate["id"], {"score": similarity})
            relation_count += 1
            if relation_count >= 4:
                break

    def _vector_hit_map(self, collection: str, query: str, *, limit: int) -> dict[str, float]:
        hits: dict[str, float] = {}
        for row in self.vector_index.search(collection, query, limit=limit):
            record_id = str(row.get("record_id") or row.get("id") or "")
            if not record_id:
                continue
            distance = float(row.get("_distance", 1.0) or 1.0)
            hits[record_id] = max(hits.get(record_id, 0.0), max(0.0, 1.0 - distance))
        return hits

    def _skill_reference_version_hit_map(self, query: str, *, limit: int) -> dict[str, float]:
        hits: dict[str, float] = {}
        for row in self.vector_index.search("skill_reference_index", query, limit=limit):
            metadata = _loads(row.get("metadata"), {})
            version_id = str(
                row.get("skill_version_id")
                or metadata.get("skill_version_id")
                or ""
            )
            if not version_id:
                continue
            distance = float(row.get("_distance", 1.0) or 1.0)
            score = max(0.0, 1.0 - distance)
            hits[version_id] = max(hits.get(version_id, 0.0), round(score * 0.92, 6))
        return hits

    def _rank_memory_rows(
        self,
        query: str,
        rows: list[dict[str, Any]],
        *,
        half_life_days: float,
        threshold: float,
        filters: dict[str, Any] | None,
        vector_hits: dict[str, float],
        fts_hits: dict[str, float],
        affinity: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        ranked = self._rank_rows(
            query,
            rows,
            domain="memory",
            text_key="text",
            keywords_getter=lambda row: _loads(row.get("index_keywords"), []),
            updated_at_key="updated_at",
            importance_getter=lambda row: float(row.get("importance", 0.5) or 0.0),
            half_life_days=half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity=affinity,
        )
        for item in ranked:
            item["relations"] = self.graph_store.relations_for_ref(item["id"], limit=6)
        return ranked

    def _rank_rows(
        self,
        query: str,
        rows: list[dict[str, Any]],
        *,
        domain: str,
        text_key: str,
        keywords_getter: Callable[[dict[str, Any]], list[str] | str],
        updated_at_key: str,
        importance_getter: Callable[[dict[str, Any]], float],
        half_life_days: float,
        threshold: float,
        filters: dict[str, Any] | None,
        vector_hits: dict[str, float],
        fts_hits: dict[str, float],
        affinity: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        for row in rows:
            item = _deserialize_row(row) or dict(row)
            text = str(item.get(text_key) or "")
            if not text.strip():
                continue
            record_id = str(item.get("record_id") or item.get("id") or "")
            score, breakdown = score_record(
                query,
                text=text,
                keywords=keywords_getter(item),
                embedding=item.get("embedding"),
                updated_at=item.get(updated_at_key),
                importance=importance_getter(item),
                lexical_score=fts_hits.get(record_id, 0.0),
                boost=vector_hits.get(record_id, 0.0) * 0.12,
                half_life_days=half_life_days,
            )
            if affinity:
                affinity_boost = 0.0
                owner_agent_id = affinity.get("owner_agent_id")
                row_owner = item.get("owner_agent_id") or item.get("agent_id")
                if owner_agent_id and row_owner == owner_agent_id:
                    affinity_boost += 0.06
                subject_type = affinity.get("subject_type")
                if subject_type and item.get("subject_type") == subject_type:
                    affinity_boost += 0.03
                subject_id = affinity.get("subject_id")
                if subject_id and item.get("subject_id") == subject_id:
                    affinity_boost += 0.06
                interaction_type = affinity.get("interaction_type")
                if interaction_type and item.get("interaction_type") == interaction_type:
                    affinity_boost += 0.03
                if affinity_boost:
                    score = round(score + affinity_boost, 6)
                    breakdown["scope_affinity"] = round(affinity_boost, 6)
            if score < threshold:
                continue
            item["id"] = record_id or str(item.get("id") or "")
            item["text"] = text
            item["score"] = score
            item["score_breakdown"] = breakdown
            item["domain"] = domain
            ranked.append(item)
        if filters:
            ranked = filter_records(ranked, filters)
        ranked.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        return mmr_rerank(ranked, lambda_value=self.config.memory_policy.diversity_lambda)

    def _session_turns(self, session_id: str) -> list[dict[str, Any]]:
        return _deserialize_rows(self.db.fetch_all("SELECT * FROM conversation_turns WHERE session_id = ? ORDER BY created_at ASC", (session_id,)))


class AsyncAIMemory:
    def __init__(self, config: AIMemoryConfig | dict[str, Any] | None = None):
        self._sync = AIMemory(config)
        self._structured_api = None

    @property
    def api(self):
        if self._structured_api is None:
            from aimemory.core.structured_api import AsyncStructuredAIMemoryAPI

            self._structured_api = AsyncStructuredAIMemoryAPI(self._sync)
        return self._structured_api

    def __getattr__(self, name: str):
        target = getattr(self._sync, name)
        if not callable(target):
            return target

        async def wrapper(*args, **kwargs):
            return await asyncio.to_thread(target, *args, **kwargs)

        return wrapper

    async def close(self) -> None:
        await asyncio.to_thread(self._sync.close)

    def __dir__(self):
        names = set(super().__dir__())
        names.update({"api"})
        return sorted(names)
