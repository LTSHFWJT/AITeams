from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Any, Callable

from aimemory.algorithms.compression import CompressionResult, compress_records, compress_text as compress_text_content
from aimemory.adapters.platform.registry import create_platform_llm_plugin, list_platform_llm_plugins
from aimemory.algorithms.dedupe import fingerprint, hamming_similarity, merge_text_fragments, semantic_similarity
from aimemory.algorithms.distill import AdaptiveDistiller, DistilledCandidate
from aimemory.algorithms.retrieval import estimate_tokens, mmr_rerank, score_record
from aimemory.algorithms.segmentation import chunk_text_units
from aimemory.backends.registry import LanceDBVectorIndex, NullGraphStore
from aimemory.core.capabilities import capability_dict
from aimemory.core.router import RetrievalRouter
from aimemory.core.scope import CollaborationScope
from aimemory.core.settings import AIMemoryConfig, PlatformLLMPluginConfig
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
from aimemory.providers.defaults import AdaptiveRecallPlanner, TextOnlyVisionProcessor
from aimemory.providers.embeddings import configure_embedding_runtime, describe_embedding_runtime, embed_text
from aimemory.querying.filters import filter_records
from aimemory.storage.lmdb.store import LMDBMemoryStore
from aimemory.storage.object_store.local import LocalObjectStore
from aimemory.storage.policy import build_inline_excerpt, normalize_raw_store_policy, payload_size_bytes, should_externalize_text
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
        "participants",
        "hot_refs",
        "source_memory_ids",
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
        "participants",
        "hot_refs",
        "source_memory_ids",
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
        "context": 0.045,
        "handoff": 0.05,
        "reflection": 0.035,
    }.get(domain, 0.0)


def _chunk_title(base_title: str | None, metadata: dict[str, Any] | None = None) -> str:
    title = str(base_title or "").strip()
    section_label = str((metadata or {}).get("section_label") or "").strip()
    if not section_label:
        return title
    if not title:
        return section_label
    return f"{title} | {section_label}"


def _format_skill_execution_context(context: dict[str, Any] | None) -> str:
    payload = dict(context or {})
    if not payload:
        return ""
    lines: list[str] = []
    summary = str(payload.get("summary") or "").strip()
    if summary:
        lines.extend(["## Execution Context", summary])
    sections = (
        ("steps", "Recommended Steps"),
        ("constraints", "Constraints"),
        ("risks", "Risks"),
        ("highlights", "Highlights"),
        ("supporting_passages", "Supporting Passages"),
    )
    for key, title in sections:
        values = [str(item).strip() for item in list(payload.get(key) or []) if str(item).strip()]
        if not values:
            continue
        lines.extend(["", f"## {title}"])
        lines.extend([f"- {item}" for item in values])
    return "\n".join(lines).strip()


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
        text,
        text_hash,
        storage_policy,
        storage_ref,
        payload_bytes,
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
        summary_l0,
        summary_l1,
        importance,
        confidence,
        visibility,
        tier,
        version,
        supersedes_memory_id,
        superseded_by_memory_id,
        access_count,
        last_accessed_at,
        expires_at,
        actor_id,
        participants,
        compression_state,
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
    def __init__(
        self,
        config: AIMemoryConfig | dict[str, Any] | None = None,
        *,
        platform_llm: Any | None = None,
        platform_events: Any | None = None,
    ):
        self.config = AIMemoryConfig.from_value(config)
        configure_embedding_runtime(self.config.embeddings)
        self.platform_llm = self._resolve_platform_llm_adapter(platform_llm)
        if platform_events is None:
            try:
                from aimemory.adapters.platform.events import AIMemoryPlatformEventAdapter

                self.platform_events = AIMemoryPlatformEventAdapter(self)
            except Exception:
                self.platform_events = None
        else:
            self.platform_events = platform_events
        self.memory_content_store = LMDBMemoryStore(self.config.lmdb_path)
        self.object_store = LocalObjectStore(self.config.object_store_path)
        self.db = SQLiteDatabase(self.config.sqlite_path)
        self._ensure_runtime_schema()
        self.vector_index = LanceDBVectorIndex(self.config)
        self.graph_store = NullGraphStore()
        self.normalizer = TextOnlyVisionProcessor()
        self.recall_router = RetrievalRouter()
        self.recall_planner = AdaptiveRecallPlanner()
        self.distiller = AdaptiveDistiller(self.config.memory_policy)
        self._domain_compressors: dict[str, Callable[..., Any]] = {}
        self._agent_store_api = None
        self._structured_api = None
        self._closed = False

    def _resolve_platform_llm_adapter(self, adapter: Any | None) -> Any | None:
        if adapter is not None:
            return adapter
        plugin = PlatformLLMPluginConfig.from_value(self.config.platform_llm_plugin)
        if plugin is None or not plugin.enabled:
            return None
        plugin_name = str(plugin.name or "").strip()
        if not plugin_name:
            return None
        return create_platform_llm_plugin(plugin_name, plugin.settings)

    def bind_platform_llm(
        self,
        adapter: Any | None = None,
        *,
        plugin_name: str | None = None,
        settings: dict[str, Any] | None = None,
    ) -> Any | None:
        if adapter is not None and plugin_name is not None:
            raise ValueError("pass either `adapter` or `plugin_name`, not both")
        if plugin_name is not None:
            plugin_settings = dict(settings or {})
            adapter = create_platform_llm_plugin(plugin_name, plugin_settings)
            self.config.platform_llm_plugin = PlatformLLMPluginConfig(
                name=plugin_name,
                settings=plugin_settings,
                enabled=True,
            )
        else:
            self.config.platform_llm_plugin = None
        self.platform_llm = adapter
        return self.platform_llm

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
        self._backfill_memory_inline_texts()
        self._backfill_document_storage_fields()
        self._backfill_memory_bundles()
        self._sync_text_search_index()
        self._sync_contextual_semantic_indexes()

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
        self._sync_contextual_semantic_indexes()

    def _backfill_memory_inline_texts(self) -> None:
        for scope, table_name in MEMORY_TABLE_MAP.items():
            rows = self.db.fetch_all(f"{self._memory_row_sql(table_name)} WHERE COALESCE(text, '') = ''")
            for row in rows:
                legacy_text = ""
                bundle_id = str(row.get("bundle_id") or "").strip()
                if bundle_id:
                    bundle = self.memory_content_store.get_json(self._memory_bucket_for_scope(scope), bundle_id, None)
                    if isinstance(bundle, dict):
                        bundle_item = self._find_bundle_item(bundle, record_id=row["id"], content_id=row.get("content_id"))
                        if bundle_item is not None:
                            legacy_text = str(bundle_item.get("text") or "")
                if not legacy_text and row.get("content_id"):
                    legacy_text = self.memory_content_store.get_text(self._memory_bucket_for_scope(scope), row["content_id"]) or ""
                if not legacy_text:
                    continue
                payload = self._memory_text_payload(legacy_text)
                self.db.execute(
                    f"""
                    UPDATE {table_name}
                    SET text = ?, text_hash = ?, storage_policy = ?, storage_ref = ?, payload_bytes = ?, summary_l0 = ?, summary_l1 = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        payload["text"],
                        payload["text_hash"],
                        payload["storage_policy"],
                        payload["storage_ref"],
                        payload["payload_bytes"],
                        payload["summary_l0"],
                        payload["summary_l1"],
                        row.get("updated_at") or utcnow_iso(),
                        row["id"],
                    ),
                )

    def _backfill_document_storage_fields(self) -> None:
        rows = self.db.fetch_all("SELECT id, inline_text, inline_excerpt, storage_policy, storage_ref, payload_bytes, updated_at FROM documents")
        for row in rows:
            if row.get("storage_policy") and (row.get("inline_text") or row.get("storage_ref") or row.get("payload_bytes")):
                continue
            document = self.get_document(row["id"])
            if document is None:
                continue
            text = self._document_text(document)
            if not text:
                continue
            payload_bytes = payload_size_bytes(text)
            self.db.execute(
                """
                UPDATE documents
                SET inline_excerpt = COALESCE(inline_excerpt, ?),
                    payload_bytes = COALESCE(payload_bytes, ?),
                    storage_policy = COALESCE(storage_policy, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    build_inline_excerpt(text),
                    payload_bytes,
                    normalize_raw_store_policy(row.get("storage_policy"), default=self.config.knowledge_raw_store_policy),
                    row.get("updated_at") or utcnow_iso(),
                    row["id"],
                ),
            )

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

    def _memory_text_payload(self, text: str, *, summary_l0: str | None = None, summary_l1: str | None = None) -> dict[str, Any]:
        cleaned = str(text or "")
        generated_l0 = build_summary(split_sentences(cleaned), max_sentences=1, max_chars=120)
        generated_l1 = build_summary(split_sentences(cleaned), max_sentences=3, max_chars=220)
        resolved_l0 = str(summary_l0 or "").strip() or generated_l0
        resolved_l1 = str(summary_l1 or "").strip() or generated_l1 or resolved_l0
        return {
            "text": cleaned,
            "text_hash": fingerprint(cleaned),
            "storage_policy": "inline",
            "storage_ref": None,
            "payload_bytes": payload_size_bytes(cleaned),
            "summary_l0": resolved_l0,
            "summary_l1": resolved_l1,
        }

    def _memory_structured_fields(
        self,
        metadata: dict[str, Any] | None,
        *,
        confidence: float | None = None,
        tier: str | None = None,
        summary_l0: str | None = None,
        summary_l1: str | None = None,
    ) -> dict[str, Any]:
        payload = dict(metadata or {})
        resolved_summary_l0 = (
            str(summary_l0 or payload.get("summary_l0") or payload.get("l0_abstract") or "").strip() or None
        )
        resolved_summary_l1 = (
            str(summary_l1 or payload.get("summary_l1") or payload.get("l1_overview") or "").strip() or None
        )
        resolved_confidence = confidence
        if resolved_confidence is None:
            try:
                resolved_confidence = float(payload.get("confidence", 0.5) or 0.5)
            except (TypeError, ValueError):
                resolved_confidence = 0.5
        resolved_confidence = max(0.0, min(1.0, float(resolved_confidence)))
        resolved_tier = str(tier or payload.get("tier") or "working").strip() or "working"
        return {
            "summary_l0": resolved_summary_l0,
            "summary_l1": resolved_summary_l1,
            "confidence": resolved_confidence,
            "tier": resolved_tier,
        }

    def _bundle_summary_from_items(self, items: list[dict[str, Any]]) -> str:
        parts = [
            str(item.get("summary_l0") or item.get("summary_l1") or item.get("summary") or "").strip()
            for item in items
            if str(item.get("summary_l0") or item.get("summary_l1") or item.get("summary") or "").strip()
        ]
        if not parts:
            return ""
        return build_summary(parts[:12], max_sentences=4, max_chars=240)

    def _normalize_bundle_items(self, items: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in list(items or []):
            payload = {
                "record_id": item.get("record_id"),
                "content_id": item.get("content_id"),
                "memory_type": item.get("memory_type"),
                "summary": item.get("summary"),
                "summary_l0": item.get("summary_l0") or build_inline_excerpt(item.get("text"), max_chars=120),
                "summary_l1": item.get("summary_l1") or item.get("summary") or build_inline_excerpt(item.get("text"), max_chars=220),
                "importance": float(item.get("importance", 0.5) or 0.0),
                "status": item.get("status", "active"),
                "source": item.get("source"),
                "storage_policy": item.get("storage_policy"),
                "storage_ref": item.get("storage_ref"),
                "payload_bytes": item.get("payload_bytes"),
                "session_id": item.get("session_id"),
                "run_id": item.get("run_id"),
                "source_session_id": item.get("source_session_id"),
                "source_run_id": item.get("source_run_id"),
                "metadata": _loads(item.get("metadata"), {}),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "archived_at": item.get("archived_at"),
            }
            if payload["record_id"] or payload["content_id"]:
                normalized.append(payload)
        return normalized

    def _bundle_payload(self, bundle_row: dict[str, Any]) -> dict[str, Any]:
        items = self._normalize_bundle_items(_loads(bundle_row.get("hot_refs"), []))
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
            "bundle_summary": bundle_row.get("bundle_summary") or self._bundle_summary_from_items(items),
            "item_count": int(bundle_row.get("item_count", len(items)) or len(items)),
            "compression_state": bundle_row.get("compression_state"),
            "items": items,
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
            INSERT INTO memory_bundles(
                id, scope, scope_key, user_id, owner_agent_id, subject_type, subject_id, interaction_type,
                namespace_key, bundle_summary, item_count, compression_state, hot_refs, metadata, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                None,
                0,
                None,
                json_dumps([]),
                json_dumps(metadata or {}),
                now,
                now,
            ),
        )
        bundle = self.db.fetch_one("SELECT * FROM memory_bundles WHERE id = ?", (bundle_id,))
        assert bundle is not None
        return bundle

    def _get_memory_bundle(self, scope: str, bundle_id: str, *, bundle_row: dict[str, Any] | None = None) -> dict[str, Any]:
        row = bundle_row or self.db.fetch_one("SELECT * FROM memory_bundles WHERE id = ?", (bundle_id,))
        if row is None:
            return {"bundle_id": bundle_id, "scope": scope, "items": [], "metadata": {}, "updated_at": utcnow_iso()}
        payload = self._bundle_payload(row)
        if payload.get("items"):
            return payload
        legacy = self.memory_content_store.get_json(self._memory_bucket_for_scope(scope), bundle_id, None)
        if isinstance(legacy, dict):
            migrated = dict(legacy)
            migrated["items"] = self._normalize_bundle_items(legacy.get("items"))
            self._put_memory_bundle(scope, bundle_id, migrated)
            return self._bundle_payload(self.db.fetch_one("SELECT * FROM memory_bundles WHERE id = ?", (bundle_id,)) or row)
        return payload

    def _put_memory_bundle(self, scope: str, bundle_id: str, payload: dict[str, Any]) -> None:
        now = utcnow_iso()
        normalized = dict(payload)
        normalized["bundle_id"] = bundle_id
        normalized["scope"] = scope
        normalized["items"] = self._normalize_bundle_items(normalized.get("items"))
        normalized["updated_at"] = now
        bundle_summary = str(normalized.get("bundle_summary") or self._bundle_summary_from_items(normalized["items"]) or "").strip()
        compression_state = normalized.get("compression_state")
        self.db.execute(
            """
            UPDATE memory_bundles
            SET bundle_summary = ?, item_count = ?, compression_state = ?, hot_refs = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                bundle_summary or None,
                len(normalized["items"]),
                compression_state,
                json_dumps(normalized["items"]),
                now,
                bundle_id,
            ),
        )

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

    def _bundle_memory_item_payload(self, row: dict[str, Any], *, text: str | None = None) -> dict[str, Any]:
        payload = self._memory_text_payload(str(text if text is not None else row.get("text") or ""))
        return {
            "record_id": row["id"],
            "content_id": row.get("content_id"),
            "summary": row.get("summary"),
            "summary_l0": row.get("summary_l0") or payload["summary_l0"],
            "summary_l1": row.get("summary_l1") or row.get("summary") or payload["summary_l1"],
            "importance": float(row.get("importance", 0.5) or 0.0),
            "version": int(row.get("version", 1) or 1),
            "supersedes_memory_id": row.get("supersedes_memory_id"),
            "superseded_by_memory_id": row.get("superseded_by_memory_id"),
            "status": row.get("status", "active"),
            "source": row.get("source"),
            "memory_type": row.get("memory_type"),
            "storage_policy": row.get("storage_policy") or payload["storage_policy"],
            "storage_ref": row.get("storage_ref"),
            "payload_bytes": row.get("payload_bytes") or payload["payload_bytes"],
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
            "summary_l0": row.get("summary_l0") or build_inline_excerpt(summary, max_chars=120),
            "summary_l1": row.get("summary_l1") or build_inline_excerpt(content or summary, max_chars=220),
            "status": "active",
            "domain": row.get("domain"),
            "source_id": row.get("source_id"),
            "source_type": row.get("source_type"),
            "storage_policy": row.get("storage_policy"),
            "storage_ref": row.get("storage_ref"),
            "payload_bytes": row.get("payload_bytes"),
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
                text = str(row.get("text") or "")
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
        text = str(item.get("text") or "")
        bucket = self._memory_bucket_for_scope(item["scope"])
        bundle_id = str(item.get("bundle_id") or "").strip()
        content_id = item.get("content_id")
        if not text and bundle_id:
            cache_key = (bucket, bundle_id)
            bundle = (bundle_cache or {}).get(cache_key)
            if bundle is None:
                bundle = self._get_memory_bundle(item["scope"], bundle_id)
                if bundle_cache is not None:
                    bundle_cache[cache_key] = bundle
            bundle_item = self._find_bundle_item(bundle, record_id=item["id"], content_id=content_id)
            if bundle_item is not None:
                text = str(bundle_item.get("summary_l1") or bundle_item.get("summary") or bundle_item.get("summary_l0") or "")
        if not text and content_id:
            text = self.memory_content_store.get_text(self._memory_bucket_for_scope(item["scope"]), content_id) or ""
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

    def _merge_hit_maps(self, *maps: dict[str, float]) -> dict[str, float]:
        merged: dict[str, float] = {}
        for item in maps:
            for key, value in item.items():
                merged[key] = max(merged.get(key, 0.0), float(value or 0.0))
        return merged

    def _delete_auxiliary_index_record(self, record_id: str, *, collection: str) -> None:
        self.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (record_id,))
        self._delete_text_search_record(record_id)
        self.vector_index.delete(collection, record_id)

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

    def get(
        self,
        memory_id: str,
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
    ) -> dict[str, Any] | None:
        table_name = self._memory_table_for_id(memory_id)
        if table_name is None:
            return None
        row = self.db.fetch_one(f"{self._memory_row_sql(table_name)} WHERE id = ?", (memory_id,))
        item = self._hydrate_memory_row(table_name, row)
        if item is None:
            return None
        requester_scope = self._request_access_scope(
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
        if any(value is not None for value in requester_scope.values()):
            return item if self._is_resource_visible(item, resource_type="memory", requester_scope=requester_scope) else None
        return item

    def get_all(
        self,
        user_id: str | None = None,
        agent_id: str | None = None,
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
        rows = self._filter_accessible_rows(
            rows,
            resource_type="memory",
            requester_scope=self._request_access_scope(
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
                namespace_key=namespace_filter,
            ),
        )
        if filters:
            rows = filter_records(rows, filters)
        return {"results": rows, "count": len(rows), "limit": limit, "offset": offset}

    def search(self, query: str, **kwargs) -> dict[str, Any]:
        kwargs = self._normalize_search_kwargs(kwargs)
        return self.memory_search(query, **kwargs)

    def update(self, memory_id: str, **kwargs) -> dict[str, Any]:
        requester_scope = self._pop_request_access_scope(kwargs)
        requester_kwargs = {key: value for key, value in requester_scope.items() if value is not None}
        row = self.get(memory_id)
        if row is None:
            raise ValueError(f"Memory `{memory_id}` does not exist.")
        self._assert_resource_permission(
            row,
            resource_type="memory",
            requester_scope=requester_scope,
            permission="write",
            action_label="update memory",
        )
        mode = str(kwargs.pop("mode", "update") or "update").strip().lower()
        if mode == "supersede":
            return self.supersede_memory(
                memory_id,
                text=str(kwargs.pop("text", row["text"])),
                metadata=kwargs.pop("metadata", None),
                importance=kwargs.pop("importance", None),
                confidence=kwargs.pop("confidence", None),
                tier=kwargs.pop("tier", None),
                summary_l0=kwargs.pop("summary_l0", None),
                summary_l1=kwargs.pop("summary_l1", None),
                source=kwargs.pop("source", None),
                reason_code=kwargs.pop("reason_code", None),
                audit_payload=kwargs.pop("audit_payload", None),
                **requester_kwargs,
            )
        if mode not in {"update", "merge"}:
            raise ValueError(f"Unsupported update mode `{mode}`.")
        table_name = self._memory_table_for_id(memory_id)
        assert table_name is not None
        event_type = str(kwargs.pop("event_type", "MERGE" if mode == "merge" else "UPDATE") or ("MERGE" if mode == "merge" else "UPDATE")).upper()
        reason_code = str(kwargs.pop("reason_code", mode)).strip() or mode
        audit_payload = dict(kwargs.pop("audit_payload", {}) or {})
        metadata = merge_metadata(row.get("metadata"), kwargs.pop("metadata", None))
        text = str(kwargs.pop("text", row["text"]))
        structured = self._memory_structured_fields(
            metadata,
            confidence=kwargs.pop("confidence", None),
            tier=kwargs.pop("tier", None),
            summary_l0=kwargs.pop("summary_l0", None),
            summary_l1=kwargs.pop("summary_l1", None),
        )
        text_payload = self._memory_text_payload(text, summary_l0=structured["summary_l0"], summary_l1=structured["summary_l1"])
        summary = str(kwargs.pop("summary", text_payload["summary_l1"]))
        importance = float(kwargs.pop("importance", row.get("importance", 0.5)))
        confidence = float(structured["confidence"])
        tier = str(structured["tier"])
        status = str(kwargs.pop("status", row.get("status", "active")))
        now = utcnow_iso()
        self.db.execute(
            f"""
            UPDATE {table_name}
            SET text = ?, text_hash = ?, storage_policy = ?, storage_ref = ?, payload_bytes = ?, summary = ?, summary_l0 = ?, summary_l1 = ?,
                importance = ?, confidence = ?, tier = ?, status = ?, metadata = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                text_payload["text"],
                text_payload["text_hash"],
                text_payload["storage_policy"],
                text_payload["storage_ref"],
                text_payload["payload_bytes"],
                summary,
                text_payload["summary_l0"],
                text_payload["summary_l1"],
                importance,
                confidence,
                tier,
                status,
                json_dumps(metadata),
                now,
                memory_id,
            ),
        )
        bundle_id = str(row.get("bundle_id") or "").strip()
        if bundle_id:
            self._upsert_bundle_item(
                row["scope"],
                bundle_id,
                self._bundle_memory_item_payload(
                    {
                        **row,
                        "text": text_payload["text"],
                        "text_hash": text_payload["text_hash"],
                        "storage_policy": text_payload["storage_policy"],
                        "storage_ref": text_payload["storage_ref"],
                        "payload_bytes": text_payload["payload_bytes"],
                        "summary": summary,
                        "summary_l0": text_payload["summary_l0"],
                        "summary_l1": text_payload["summary_l1"],
                        "importance": importance,
                        "confidence": confidence,
                        "tier": tier,
                        "status": status,
                        "metadata": metadata,
                        "updated_at": now,
                    }
                ),
            )
        self._record_memory_event(
            memory_id,
            event_type,
            {
                "text": text,
                "previous_text": row.get("text"),
                "metadata": metadata,
                "status": status,
                **audit_payload,
            },
            reason_code=reason_code,
            source_row={
                **row,
                "text": text_payload["text"],
                "metadata": metadata,
                "status": status,
                "importance": importance,
                "confidence": confidence,
                "tier": tier,
                "updated_at": now,
            },
            source_table=table_name,
            version=int(row.get("version", 1) or 1),
        )
        updated = self.get(memory_id)
        assert updated is not None
        if status == "active":
            self._index_memory(updated)
        else:
            self._delete_memory_index(memory_id)
        updated["_event"] = event_type
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

    def delete(self, memory_id: str, **kwargs) -> dict[str, Any]:
        requester_scope = self._pop_request_access_scope(kwargs)
        row = self.get(memory_id)
        if row is None:
            raise ValueError(f"Memory `{memory_id}` does not exist.")
        self._assert_resource_permission(
            row,
            resource_type="memory",
            requester_scope=requester_scope,
            permission="write",
            action_label="delete memory",
        )
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
        rows = self.db.fetch_all("SELECT * FROM memory_events WHERE memory_id = ? ORDER BY created_at ASC, id ASC", (memory_id,))
        return _deserialize_rows(rows, ("payload",))

    def supersede_memory(
        self,
        memory_id: str,
        *,
        text: str,
        metadata: dict[str, Any] | None = None,
        importance: float | None = None,
        confidence: float | None = None,
        tier: str | None = None,
        summary_l0: str | None = None,
        summary_l1: str | None = None,
        source: str | None = None,
        reason_code: str | None = None,
        audit_payload: dict[str, Any] | None = None,
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
    ) -> dict[str, Any]:
        requester_scope = self._request_access_scope(
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
        row = self.get(memory_id)
        if row is None:
            raise ValueError(f"Memory `{memory_id}` does not exist.")
        self._assert_resource_permission(
            row,
            resource_type="memory",
            requester_scope=requester_scope,
            permission="write",
            action_label="supersede memory",
        )
        table_name = self._memory_table_for_id(memory_id)
        assert table_name is not None
        now = utcnow_iso()
        merged_metadata = merge_metadata(row.get("metadata"), metadata)
        created = self._remember(
            text,
            user_id=row.get("user_id"),
            agent_id=row.get("agent_id"),
            owner_agent_id=row.get("owner_agent_id") or row.get("agent_id"),
            subject_type=row.get("subject_type"),
            subject_id=row.get("subject_id"),
            interaction_type=row.get("interaction_type"),
            namespace_key=row.get("namespace_key"),
            session_id=row.get("session_id"),
            run_id=row.get("run_id"),
            metadata=merged_metadata,
            memory_type=str(row.get("memory_type") or MemoryType.SEMANTIC),
            importance=float(importance if importance is not None else row.get("importance", 0.5) or 0.5),
            confidence=float(confidence if confidence is not None else row.get("confidence", 0.5) or 0.5),
            tier=str(tier or row.get("tier") or merged_metadata.get("tier") or "working"),
            summary_l0=summary_l0,
            summary_l1=summary_l1,
            long_term=row.get("scope") == str(MemoryScope.LONG_TERM),
            source=str(source or row.get("source") or "manual"),
            version=int(row.get("version", 1) or 1) + 1,
            supersedes_memory_id=memory_id,
            skip_existing_lookup=True,
            event_type="SUPERSEDE",
            event_payload={
                "supersedes_memory_id": memory_id,
                "previous_text": row.get("text"),
                "previous_version": int(row.get("version", 1) or 1),
                **dict(audit_payload or {}),
            },
            reason_code=reason_code or "supersede",
            **{key: value for key, value in requester_scope.items() if value is not None},
        )
        self.db.execute(
            f"""
            UPDATE {table_name}
            SET status = ?, superseded_by_memory_id = ?, updated_at = ?
            WHERE id = ?
            """,
            ("superseded", created["id"], now, memory_id),
        )
        bundle_id = str(row.get("bundle_id") or "").strip()
        if bundle_id:
            self._upsert_bundle_item(
                row["scope"],
                bundle_id,
                self._bundle_memory_item_payload(
                    {
                        **row,
                        "status": "superseded",
                        "superseded_by_memory_id": created["id"],
                        "updated_at": now,
                    }
                ),
            )
        self._delete_memory_index(memory_id)
        self.link_memory(
            created["id"],
            memory_id,
            link_type="supersedes",
            weight=1.0,
            confidence=1.0,
            metadata={"version_chain": True, "supersedes_memory_id": memory_id},
            reason_code=reason_code or "supersede",
            emit_event=False,
        )
        self._record_memory_event(
            memory_id,
            "SUPERSEDED",
            {
                "superseded_by_memory_id": created["id"],
                "replacement_text": created.get("text"),
                "replacement_version": int(created.get("version", 1) or 1),
                **dict(audit_payload or {}),
            },
            reason_code=reason_code or "supersede",
            source_row={
                **row,
                "status": "superseded",
                "superseded_by_memory_id": created["id"],
                "updated_at": now,
            },
            source_table=table_name,
            version=int(row.get("version", 1) or 1),
        )
        created["_event"] = "SUPERSEDE"
        return created

    def link_memory(
        self,
        source_memory_id: str,
        target_memory_ids: str | list[str],
        *,
        link_type: str = "related",
        weight: float = 1.0,
        confidence: float = 0.5,
        metadata: dict[str, Any] | None = None,
        reason_code: str | None = None,
        emit_event: bool = True,
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
    ) -> dict[str, Any]:
        requester_scope = self._request_access_scope(
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
        source = self.get(source_memory_id)
        if source is None:
            raise ValueError(f"Memory `{source_memory_id}` does not exist.")
        self._assert_resource_permission(
            source,
            resource_type="memory",
            requester_scope=requester_scope,
            permission="write",
            action_label="link memory",
        )
        target_ids = [target_memory_ids] if isinstance(target_memory_ids, str) else [item for item in target_memory_ids if item]
        links: list[dict[str, Any]] = []
        for target_memory_id in target_ids:
            target = self.get(target_memory_id)
            if target is None:
                raise ValueError(f"Memory `{target_memory_id}` does not exist.")
            self._assert_resource_permission(
                target,
                resource_type="memory",
                requester_scope=requester_scope,
                permission="write",
                action_label="link target memory",
            )
            links.append(
                self._upsert_memory_link(
                    source_memory_id,
                    target_memory_id,
                    link_type=link_type,
                    weight=weight,
                    confidence=confidence,
                    metadata=metadata,
                    bundle_id=source.get("bundle_id"),
                    source_domain="memory",
                    target_domain="memory",
                )
            )
        if emit_event and links:
            self._record_memory_event(
                source_memory_id,
                "LINK",
                {
                    "link_type": link_type,
                    "linked_memory_ids": [item["target_memory_id"] for item in links],
                    "weight": float(weight),
                    "confidence": float(confidence),
                    "metadata": dict(metadata or {}),
                },
                reason_code=reason_code or "link",
                source_row=source,
                source_table=self._memory_table_for_id(source_memory_id),
                version=int(source.get("version", 1) or 1),
            )
        return {
            "source_memory_id": source_memory_id,
            "target_memory_ids": [item["target_memory_id"] for item in links],
            "link_type": link_type,
            "links": links,
        }

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
        agent_id: str | None = None,
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
        platform_id = kwargs.pop("platform_id", None)
        workspace_id = kwargs.pop("workspace_id", None)
        team_id = kwargs.pop("team_id", None)
        project_id = kwargs.pop("project_id", None)
        namespace_key = kwargs.pop("namespace_key", None)
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
        sql_filters = ["status = 'active'"]
        params: list[Any] = []
        if user_id:
            sql_filters.append("user_id = ?")
            params.append(user_id)
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
        rows = self._filter_accessible_rows(
            rows,
            resource_type="memory",
            requester_scope=self._request_access_scope(
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
                namespace_key=namespace_filter,
            ),
        )
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
        recall_plan = self.plan_recall(
            query,
            user_id=user_id,
            session_id=session_id,
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
            run_id=run_id,
            actor_id=actor_id,
            role=role,
            limit=limit,
            domains=domains,
        )
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
            "context": lambda: self.search_context_artifacts(
                query,
                owner_agent_id=owner_agent_id or agent_id,
                session_id=session_id,
                run_id=run_id,
                platform_id=platform_id,
                workspace_id=workspace_id,
                team_id=team_id,
                project_id=project_id,
                namespace_key=namespace_key,
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
            "handoff": lambda: self.search_handoff_packs(
                query,
                owner_agent_id=owner_agent_id or agent_id,
                source_agent_id=owner_agent_id or agent_id,
                source_session_id=session_id,
                source_run_id=run_id,
                platform_id=platform_id,
                workspace_id=workspace_id,
                team_id=team_id,
                project_id=project_id,
                namespace_key=namespace_key,
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
            "reflection": lambda: self.search_reflection_memories(
                query,
                owner_agent_id=owner_agent_id or agent_id,
                session_id=session_id,
                run_id=run_id,
                limit=max(limit, self.config.memory_policy.auxiliary_search_limit),
                threshold=threshold,
            ),
        }
        selected_domains = list(dict.fromkeys(domains or recall_plan.get("selected_domains") or ["memory", "interaction", "knowledge", "skill", "archive"]))
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
        return {"results": merged[:limit], "plan": recall_plan}

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
            "plan": result.get("plan"),
            "results": result["results"],
        }

    def _deserialize_handoff_pack(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        item = _deserialize_row(row, ("metadata", "highlights", "open_tasks", "constraints", "source_refs"))
        if item is None:
            return None
        item["text"] = "\n".join(
            part
            for part in [
                str(item.get("summary") or "").strip(),
                "; ".join(str(value).strip() for value in list(item.get("highlights") or []) if str(value).strip()),
                "; ".join(str(value).strip() for value in list(item.get("open_tasks") or []) if str(value).strip()),
                "; ".join(str(value).strip() for value in list(item.get("constraints") or []) if str(value).strip()),
            ]
            if part
        ).strip()
        return item

    def _deserialize_context_artifact(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        return _deserialize_row(row, ("metadata", "source_refs"))

    def _deserialize_reflection_memory(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        item = _deserialize_row(row, ("metadata", "source_refs"))
        if item is None:
            return None
        item["text"] = "\n".join(part for part in [str(item.get("summary") or "").strip(), str(item.get("details") or "").strip()] if part).strip()
        return item

    def _deserialize_compression_job(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        return _deserialize_row(row, ("request_payload",))

    def _compression_records_from_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for item in rows:
            text = str(item.get("text") or item.get("content") or item.get("summary") or item.get("details") or "").strip()
            if not text:
                continue
            record_id = str(item.get("id") or item.get("record_id") or fingerprint(text)).strip()
            metadata = {
                key: item.get(key)
                for key in (
                    "domain",
                    "title",
                    "name",
                    "session_id",
                    "run_id",
                    "owner_agent_id",
                    "subject_type",
                    "subject_id",
                    "interaction_type",
                    "namespace_key",
                    "relative_path",
                )
                if item.get(key) is not None
            }
            if item.get("metadata"):
                metadata["source_metadata"] = item.get("metadata")
            records.append(
                {
                    "id": record_id,
                    "text": text,
                    "score": float(item.get("score", 0.55) or 0.0),
                    "metadata": metadata,
                }
            )
        return records

    def _source_refs_from_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in rows:
            ref_id = str(item.get("id") or item.get("record_id") or "").strip()
            if not ref_id or ref_id in seen:
                continue
            seen.add(ref_id)
            refs.append(
                {
                    "id": ref_id,
                    "domain": item.get("domain"),
                    "title": item.get("title") or item.get("name"),
                    "score": round(float(item.get("score", 0.0) or 0.0), 6),
                    "session_id": item.get("session_id"),
                    "run_id": item.get("run_id"),
                }
            )
        return refs

    def _dedupe_compression_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in rows:
            key = str(item.get("id") or item.get("record_id") or "").strip()
            if not key:
                text = str(item.get("text") or item.get("content") or item.get("summary") or "").strip()
                if not text:
                    continue
                key = fingerprint(text)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _snapshot_text(self, snapshot: dict[str, Any] | None) -> str:
        if snapshot is None:
            return ""
        lines: list[str] = []
        summary = str(snapshot.get("summary") or "").strip()
        if summary:
            lines.append(summary)
        for label, values in (
            ("Constraints", list(snapshot.get("constraints") or [])),
            ("Resolved", list(snapshot.get("resolved_items") or [])),
            ("Unresolved", list(snapshot.get("unresolved_items") or [])),
            ("Next Actions", list(snapshot.get("next_actions") or [])),
        ):
            cleaned = [str(item).strip() for item in values if str(item).strip()]
            if cleaned:
                lines.append(f"{label}: " + "; ".join(cleaned))
        return "\n".join(lines).strip()

    def _format_compression_content(self, title: str, payload: dict[str, Any]) -> str:
        lines: list[str] = []
        summary = str(payload.get("summary") or "").strip()
        if summary:
            lines.extend([f"## {title}", summary])
        sections = (
            ("facts", "Facts"),
            ("constraints", "Constraints"),
            ("steps", "Recommended Steps"),
            ("risks", "Risks"),
            ("highlights", "Highlights"),
            ("supporting_passages", "Supporting Passages"),
        )
        for key, heading in sections:
            values = [str(item).strip() for item in list(payload.get(key) or []) if str(item).strip()]
            if not values:
                continue
            if lines:
                lines.append("")
            lines.append(f"## {heading}")
            lines.extend([f"- {item}" for item in values])
        return "\n".join(lines).strip() or summary

    def _normalize_external_compression(
        self,
        value: CompressionResult | dict[str, Any] | str,
        *,
        records: list[dict[str, Any]],
        budget_chars: int,
        domain_hint: str,
    ) -> dict[str, Any]:
        if isinstance(value, CompressionResult):
            return value.as_dict()
        if isinstance(value, str):
            return compress_records(
                [{"id": "external", "text": value, "score": 1.0}],
                domain_hint=domain_hint,
                budget_chars=budget_chars,
                diversity_lambda=self.config.memory_policy.diversity_lambda,
                policy=self.config.memory_policy,
            ).as_dict()
        if not isinstance(value, dict):
            raise TypeError("platform compression result must be CompressionResult, dict, or str")
        return {
            "summary": str(value.get("summary") or ""),
            "highlights": [str(item).strip() for item in list(value.get("highlights") or []) if str(item).strip()],
            "kept_ids": [str(item).strip() for item in list(value.get("kept_ids") or []) if str(item).strip()],
            "estimated_tokens": int(value.get("estimated_tokens") or estimate_tokens(str(value.get("summary") or ""))),
            "source_count": int(value.get("source_count") or len(records)),
            "facts": [str(item).strip() for item in list(value.get("facts") or []) if str(item).strip()],
            "constraints": [str(item).strip() for item in list(value.get("constraints") or []) if str(item).strip()],
            "steps": [str(item).strip() for item in list(value.get("steps") or []) if str(item).strip()],
            "risks": [str(item).strip() for item in list(value.get("risks") or []) if str(item).strip()],
            "selected_unit_ids": [str(item).strip() for item in list(value.get("selected_unit_ids") or []) if str(item).strip()],
            "coverage_score": float(value.get("coverage_score") or 0.0),
            "redundancy_score": float(value.get("redundancy_score") or 0.0),
            "metadata": dict(value.get("metadata") or {}),
            "evidence_spans": [dict(item) for item in list(value.get("evidence_spans") or []) if isinstance(item, dict)],
            "supporting_passages": [str(item).strip() for item in list(value.get("supporting_passages") or []) if str(item).strip()],
        }

    def _start_compression_job(
        self,
        *,
        job_type: str,
        session_id: str | None,
        run_id: str | None,
        owner_agent_id: str | None,
        budget_chars: int,
        scope: dict[str, Any],
        records: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        job_id = make_id("cmpjob")
        now = utcnow_iso()
        request_payload = {
            "job_type": job_type,
            "budget_chars": int(budget_chars),
            "scope": dict(scope),
            "source_count": len(records),
            "metadata": dict(metadata or {}),
        }
        self.db.execute(
            """
            INSERT INTO compression_jobs(
                id, job_type, session_id, run_id, owner_agent_id, status, provider, model, request_payload,
                result_artifact_id, error, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                job_type,
                session_id,
                run_id,
                owner_agent_id,
                "running",
                None,
                None,
                json_dumps(request_payload),
                None,
                None,
                now,
                now,
            ),
        )
        job = self.get_compression_job(job_id)
        assert job is not None
        return job

    def _finish_compression_job(
        self,
        job_id: str,
        *,
        status: str,
        provider: str | None,
        model: str | None,
        result_artifact_id: str | None,
        error: str | None,
    ) -> dict[str, Any]:
        now = utcnow_iso()
        self.db.execute(
            """
            UPDATE compression_jobs
            SET status = ?, provider = ?, model = ?, result_artifact_id = ?, error = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, provider, model, result_artifact_id, error, now, job_id),
        )
        job = self.get_compression_job(job_id)
        assert job is not None
        return job

    def _run_contextual_compression(
        self,
        *,
        job_type: str,
        records: list[dict[str, Any]],
        scope: dict[str, Any],
        budget_chars: int,
        use_platform_llm: bool,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], str, str | None, str | None, str | None]:
        if use_platform_llm and self.platform_llm is not None:
            provider = getattr(self.platform_llm, "provider", None)
            model = getattr(self.platform_llm, "model", None)
            try:
                external = self.platform_llm.compress(
                    task_type=job_type,
                    records=list(records),
                    budget_chars=int(budget_chars),
                    scope=dict(scope),
                    metadata=dict(metadata or {}),
                )
                normalized = self._normalize_external_compression(
                    external,
                    records=records,
                    budget_chars=budget_chars,
                    domain_hint=job_type,
                )
                if isinstance(external, dict):
                    provider = str(external.get("provider") or provider or "platform").strip() or "platform"
                    model = str(external.get("model") or model or "").strip() or model
                return (
                    {
                        "triggered": True,
                        "domain": job_type,
                        "threshold_chars": 0,
                        "budget_chars": int(budget_chars),
                        "total_chars": sum(len(str(item.get("text") or "")) for item in records),
                        **normalized,
                        "provider": provider or "platform",
                    },
                    "completed",
                    provider or "platform",
                    model,
                    None,
                )
            except Exception as exc:
                local = self.compress_domain_records(
                    job_type,
                    records,
                    scope=scope,
                    threshold_chars=0,
                    budget_chars=budget_chars,
                    force=True,
                )
                return local, "degraded", provider or "platform", model, str(exc)
        local = self.compress_domain_records(
            job_type,
            records,
            scope=scope,
            threshold_chars=0,
            budget_chars=budget_chars,
            force=True,
        )
        status = "completed" if not use_platform_llm else "degraded"
        error = None if not use_platform_llm else "platform_llm_unavailable"
        return local, status, local.get("provider"), None, error

    def _create_context_artifact(
        self,
        *,
        artifact_type: str,
        session_id: str | None,
        run_id: str | None,
        owner_agent_id: str | None,
        target_agent_id: str | None,
        namespace_key: str | None,
        provider: str | None,
        model: str | None,
        budget_chars: int | None,
        source_refs: list[dict[str, Any]] | None,
        content: str,
        scope_metadata: dict[str, Any] | None = None,
        visibility: str | None = None,
        metadata: dict[str, Any] | None = None,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        artifact_id = make_id("ctxart")
        now = utcnow_iso()
        merged_metadata = merge_metadata(scope_metadata, metadata)
        if visibility:
            merged_metadata["visibility"] = visibility
        self.db.execute(
            """
            INSERT INTO context_artifacts(
                id, artifact_type, session_id, run_id, owner_agent_id, target_agent_id, namespace_key,
                provider, model, budget_chars, source_refs, content, metadata, created_at, expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                artifact_type,
                session_id,
                run_id,
                owner_agent_id,
                target_agent_id,
                namespace_key,
                provider,
                model,
                budget_chars,
                json_dumps(source_refs or []),
                content,
                json_dumps(merged_metadata),
                now,
                expires_at,
            ),
        )
        artifact = self.get_context_artifact(artifact_id)
        assert artifact is not None
        self._index_context_artifact(artifact)
        return artifact

    def _create_handoff_pack_row(
        self,
        *,
        source_run_id: str | None,
        source_session_id: str | None,
        source_agent_id: str | None,
        target_agent_id: str,
        namespace_key: str | None,
        visibility: str,
        summary: str,
        highlights: list[str],
        open_tasks: list[str],
        constraints: list[str],
        source_refs: list[dict[str, Any]],
        scope_metadata: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        handoff_id = make_id("handoff")
        now = utcnow_iso()
        merged_metadata = merge_metadata(scope_metadata, metadata)
        merged_metadata["visibility"] = visibility
        self.db.execute(
            """
            INSERT INTO handoff_packs(
                id, source_run_id, source_session_id, source_agent_id, target_agent_id, namespace_key, visibility,
                summary, highlights, open_tasks, constraints, source_refs, metadata, created_at, expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                handoff_id,
                source_run_id,
                source_session_id,
                source_agent_id,
                target_agent_id,
                namespace_key,
                visibility,
                summary,
                json_dumps(highlights),
                json_dumps(open_tasks),
                json_dumps(constraints),
                json_dumps(source_refs),
                json_dumps(merged_metadata),
                now,
                expires_at,
            ),
        )
        handoff = self.get_handoff_pack(handoff_id)
        assert handoff is not None
        self._index_handoff_pack(handoff)
        return handoff

    def _create_reflection_memory_row(
        self,
        *,
        owner_agent_id: str | None,
        session_id: str | None,
        run_id: str | None,
        reflection_type: str,
        summary: str,
        details: str,
        confidence: float,
        decay_half_life_days: float | None,
        source_refs: list[dict[str, Any]],
        scope_metadata: dict[str, Any] | None = None,
        visibility: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        reflection_id = make_id("refl")
        now = utcnow_iso()
        merged_metadata = merge_metadata(scope_metadata, metadata)
        if visibility:
            merged_metadata["visibility"] = visibility
        self.db.execute(
            """
            INSERT INTO reflection_memories(
                id, owner_agent_id, session_id, run_id, reflection_type, summary, details, confidence,
                decay_half_life_days, source_refs, metadata, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                reflection_id,
                owner_agent_id,
                session_id,
                run_id,
                reflection_type,
                summary,
                details,
                confidence,
                decay_half_life_days,
                json_dumps(source_refs),
                json_dumps(merged_metadata),
                now,
                now,
            ),
        )
        reflection = self.get_reflection_memory(reflection_id)
        assert reflection is not None
        self._index_reflection_memory(reflection)
        return reflection

    def _reflection_types(self, mode: str | None) -> list[str]:
        raw = str(mode or "derived+invariant").replace(",", "+").replace("|", "+")
        items = [part.strip() for part in raw.split("+") if part.strip()]
        if not items:
            items = ["derived", "invariant"]
        if "all" in items:
            items = ["derived", "invariant", "failure_pattern", "tool_lesson"]
        allowed = {"derived", "invariant", "failure_pattern", "tool_lesson"}
        return [item for item in dict.fromkeys(items) if item in allowed]

    def _reflection_payload_for_type(
        self,
        reflection_type: str,
        compression: dict[str, Any],
    ) -> tuple[str, str, float]:
        summary = str(compression.get("summary") or "").strip()
        facts = [str(item).strip() for item in list(compression.get("facts") or []) if str(item).strip()]
        constraints = [str(item).strip() for item in list(compression.get("constraints") or []) if str(item).strip()]
        steps = [str(item).strip() for item in list(compression.get("steps") or []) if str(item).strip()]
        risks = [str(item).strip() for item in list(compression.get("risks") or []) if str(item).strip()]
        highlights = [str(item).strip() for item in list(compression.get("highlights") or []) if str(item).strip()]
        if reflection_type == "invariant":
            chosen_summary = facts[0] if facts else (constraints[0] if constraints else summary)
            details = self._format_compression_content(
                "Invariant Reflection",
                {"summary": chosen_summary, "facts": facts, "constraints": constraints, "highlights": highlights},
            )
            return chosen_summary or summary, details, 0.78
        if reflection_type == "failure_pattern":
            chosen_summary = risks[0] if risks else (constraints[0] if constraints else summary)
            details = self._format_compression_content(
                "Failure Pattern",
                {"summary": chosen_summary, "risks": risks, "constraints": constraints, "supporting_passages": compression.get("supporting_passages")},
            )
            return chosen_summary or summary, details, 0.66
        if reflection_type == "tool_lesson":
            chosen_summary = steps[0] if steps else (highlights[0] if highlights else summary)
            details = self._format_compression_content(
                "Tool Lesson",
                {"summary": chosen_summary, "steps": steps, "highlights": highlights, "supporting_passages": compression.get("supporting_passages")},
            )
            return chosen_summary or summary, details, 0.69
        details = self._format_compression_content(
            "Derived Reflection",
            {"summary": summary, "facts": facts, "steps": steps, "highlights": highlights, "risks": risks},
        )
        return summary, details, 0.72

    def get_context_artifact(
        self,
        artifact_id: str,
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
    ) -> dict[str, Any] | None:
        item = self._deserialize_context_artifact(self.db.fetch_one("SELECT * FROM context_artifacts WHERE id = ?", (artifact_id,)))
        if item is None:
            return None
        requester_scope = self._request_access_scope(
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
        if any(value is not None for value in requester_scope.values()):
            return item if self._is_resource_visible(item, resource_type="context", requester_scope=requester_scope) else None
        return item

    def list_context_artifacts(
        self,
        *,
        artifact_type: str | None = None,
        user_id: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        target_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 50,
        offset: int = 0,
        **_unused: Any,
    ) -> dict[str, Any]:
        filters = ["1 = 1"]
        params: list[Any] = []
        namespace_filter, _ = self._context_scope_filters(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            run_id=run_id,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        for field, value in (
            ("artifact_type", artifact_type),
            ("session_id", session_id),
            ("run_id", run_id),
            ("namespace_key", namespace_filter),
        ):
            if value:
                filters.append(f"{field} = ?")
                params.append(value)
        if target_agent_id:
            filters.append("(target_agent_id = ? OR target_agent_id IS NULL)")
            params.append(target_agent_id)
        rows = [
            item
            for item in (
                self._deserialize_context_artifact(row)
                for row in self.db.fetch_all(
                    f"SELECT * FROM context_artifacts WHERE {' AND '.join(filters)} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    tuple(params + [limit, offset]),
                )
            )
            if item is not None
        ]
        rows = self._filter_accessible_rows(
            rows,
            resource_type="context",
            requester_scope=self._request_access_scope(
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
                namespace_key=namespace_key or namespace_filter,
            ),
        )
        return {"results": rows, "count": len(rows), "limit": limit, "offset": offset}

    def get_handoff_pack(
        self,
        handoff_id: str,
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
    ) -> dict[str, Any] | None:
        item = self._deserialize_handoff_pack(self.db.fetch_one("SELECT * FROM handoff_packs WHERE id = ?", (handoff_id,)))
        if item is None:
            return None
        requester_scope = self._request_access_scope(
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
        if any(value is not None for value in requester_scope.values()):
            return item if self._is_resource_visible(item, resource_type="handoff", requester_scope=requester_scope) else None
        return item

    def list_handoff_packs(
        self,
        *,
        user_id: str | None = None,
        source_run_id: str | None = None,
        source_session_id: str | None = None,
        source_agent_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        target_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 50,
        offset: int = 0,
        **_unused: Any,
    ) -> dict[str, Any]:
        filters = ["1 = 1"]
        params: list[Any] = []
        namespace_filter, _ = self._context_scope_filters(
            user_id=user_id,
            owner_agent_id=owner_agent_id or source_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=source_session_id,
            run_id=source_run_id,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        for field, value in (
            ("source_run_id", source_run_id),
            ("source_session_id", source_session_id),
            ("source_agent_id", source_agent_id),
            ("namespace_key", namespace_filter),
        ):
            if value:
                filters.append(f"{field} = ?")
                params.append(value)
        if target_agent_id:
            filters.append("(target_agent_id = ? OR target_agent_id IS NULL)")
            params.append(target_agent_id)
        rows = [
            item
            for item in (
                self._deserialize_handoff_pack(row)
                for row in self.db.fetch_all(
                    f"SELECT * FROM handoff_packs WHERE {' AND '.join(filters)} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    tuple(params + [limit, offset]),
                )
            )
            if item is not None
        ]
        rows = self._filter_accessible_rows(
            rows,
            resource_type="handoff",
            requester_scope=self._request_access_scope(
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
                namespace_key=namespace_key or namespace_filter,
            ),
        )
        return {"results": rows, "count": len(rows), "limit": limit, "offset": offset}

    def get_reflection_memory(
        self,
        reflection_id: str,
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
    ) -> dict[str, Any] | None:
        item = self._deserialize_reflection_memory(self.db.fetch_one("SELECT * FROM reflection_memories WHERE id = ?", (reflection_id,)))
        if item is None:
            return None
        requester_scope = self._request_access_scope(
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
        if any(value is not None for value in requester_scope.values()):
            return item if self._is_resource_visible(item, resource_type="reflection", requester_scope=requester_scope) else None
        return item

    def list_reflection_memories(
        self,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        reflection_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 50,
        offset: int = 0,
        **_unused: Any,
    ) -> dict[str, Any]:
        filters = ["1 = 1"]
        params: list[Any] = []
        for field, value in (
            ("session_id", session_id),
            ("run_id", run_id),
            ("reflection_type", reflection_type),
        ):
            if value:
                filters.append(f"{field} = ?")
                params.append(value)
        rows = [
            item
            for item in (
                self._deserialize_reflection_memory(row)
                for row in self.db.fetch_all(
                    f"SELECT * FROM reflection_memories WHERE {' AND '.join(filters)} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    tuple(params + [limit, offset]),
                )
            )
            if item is not None
        ]
        rows = self._filter_accessible_rows(
            rows,
            resource_type="reflection",
            requester_scope=self._request_access_scope(
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
            ),
        )
        return {"results": rows, "count": len(rows), "limit": limit, "offset": offset}

    def get_compression_job(self, job_id: str) -> dict[str, Any] | None:
        return self._deserialize_compression_job(self.db.fetch_one("SELECT * FROM compression_jobs WHERE id = ?", (job_id,)))

    def get_scope_acl_rule(self, rule_id: str, **kwargs: Any) -> dict[str, Any] | None:
        rule = _deserialize_row(self.db.fetch_one("SELECT * FROM scope_acl_rules WHERE id = ?", (rule_id,)), ("metadata",))
        if rule is None:
            return None
        requester_scope = self._pop_request_access_scope(kwargs)
        self._assert_namespace_permission(
            namespace_key=rule.get("namespace_key"),
            resource_type=str(rule.get("resource_type") or "all"),
            resource_scope=str(rule.get("resource_scope") or "all"),
            requester_scope=requester_scope,
            permission="manage",
            action_label="read ACL rule",
        )
        return rule

    def list_scope_acl_rules(
        self,
        *,
        namespace_key: str | None = None,
        resource_type: str | None = None,
        resource_scope: str | None = None,
        principal_type: str | None = None,
        principal_id: str | None = None,
        permission: str | None = None,
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
        limit: int = 100,
        offset: int = 0,
        **_unused: Any,
    ) -> dict[str, Any]:
        requester_scope = self._request_access_scope(
            user_id=user_id,
            agent_id=agent_id or owner_agent_id,
            owner_agent_id=agent_id or owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        resolved_namespace = namespace_key or self._namespace_filter_value(
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
        )
        self._assert_namespace_permission(
            namespace_key=resolved_namespace,
            resource_type=resource_type or "all",
            resource_scope=resource_scope or "all",
            requester_scope=requester_scope,
            permission="manage",
            action_label="list ACL rules",
        )
        filters = ["1 = 1"]
        params: list[Any] = []
        for field, value in (
            ("namespace_key", resolved_namespace),
            ("resource_type", resource_type),
            ("resource_scope", resource_scope),
            ("principal_type", principal_type),
            ("principal_id", principal_id),
            ("permission", permission),
        ):
            if value:
                filters.append(f"{field} = ?")
                params.append(value)
        rows = _deserialize_rows(
            self.db.fetch_all(
                f"SELECT * FROM scope_acl_rules WHERE {' AND '.join(filters)} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                tuple(params + [limit, offset]),
            ),
            ("metadata",),
        )
        return {"results": rows, "count": len(rows), "limit": limit, "offset": offset}

    def grant_scope_acl_rule(
        self,
        *,
        namespace_key: str | None = None,
        resource_type: str = "all",
        resource_scope: str = "all",
        principal_type: str,
        principal_id: str,
        permission: str = "read",
        metadata: dict[str, Any] | None = None,
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
    ) -> dict[str, Any]:
        requester_scope = self._request_access_scope(
            user_id=user_id,
            agent_id=agent_id or owner_agent_id,
            owner_agent_id=agent_id or owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        resolved_namespace = namespace_key or self._namespace_filter_value(
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
        )
        if not resolved_namespace:
            raise ValueError("`namespace_key` is required when scope cannot be resolved.")
        self._assert_namespace_permission(
            namespace_key=resolved_namespace,
            resource_type=resource_type,
            resource_scope=resource_scope,
            requester_scope=requester_scope,
            permission="manage",
            action_label="grant ACL rule",
        )
        existing = self.db.fetch_one(
            """
            SELECT * FROM scope_acl_rules
            WHERE namespace_key = ? AND resource_type = ? AND resource_scope = ?
              AND principal_type = ? AND principal_id = ? AND permission = ?
            LIMIT 1
            """,
            (resolved_namespace, resource_type, resource_scope, principal_type, principal_id, permission),
        )
        now = utcnow_iso()
        if existing is None:
            rule_id = make_id("acl")
            self.db.execute(
                """
                INSERT INTO scope_acl_rules(
                    id, namespace_key, resource_type, resource_scope, principal_type, principal_id, permission, metadata, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rule_id,
                    resolved_namespace,
                    resource_type,
                    resource_scope,
                    principal_type,
                    principal_id,
                    permission,
                    json_dumps(metadata or {}),
                    now,
                    now,
                ),
            )
            return self.get_scope_acl_rule(rule_id) or {}
        merged_metadata = merge_metadata(_loads(existing.get("metadata"), {}), metadata)
        self.db.execute(
            "UPDATE scope_acl_rules SET metadata = ?, updated_at = ? WHERE id = ?",
            (json_dumps(merged_metadata), now, existing["id"]),
        )
        return self.get_scope_acl_rule(existing["id"]) or {}

    def revoke_scope_acl_rule(
        self,
        rule_id: str | None = None,
        *,
        namespace_key: str | None = None,
        resource_type: str | None = None,
        resource_scope: str | None = None,
        principal_type: str | None = None,
        principal_id: str | None = None,
        permission: str | None = None,
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
        **_unused: Any,
    ) -> dict[str, Any]:
        requester_scope = self._request_access_scope(
            user_id=user_id,
            agent_id=agent_id or owner_agent_id,
            owner_agent_id=agent_id or owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if rule_id:
            existing = _deserialize_row(self.db.fetch_one("SELECT * FROM scope_acl_rules WHERE id = ?", (rule_id,)), ("metadata",))
            if existing is not None:
                self._assert_namespace_permission(
                    namespace_key=existing.get("namespace_key"),
                    resource_type=str(existing.get("resource_type") or "all"),
                    resource_scope=str(existing.get("resource_scope") or "all"),
                    requester_scope=requester_scope,
                    permission="manage",
                    action_label="revoke ACL rule",
                )
            if existing is not None:
                self.db.execute("DELETE FROM scope_acl_rules WHERE id = ?", (rule_id,))
            return {"deleted": existing is not None, "rule_id": rule_id}
        resolved_namespace = namespace_key or self._namespace_filter_value(
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
        )
        self._assert_namespace_permission(
            namespace_key=resolved_namespace,
            resource_type=resource_type or "all",
            resource_scope=resource_scope or "all",
            requester_scope=requester_scope,
            permission="manage",
            action_label="revoke ACL rules",
        )
        filters = ["1 = 1"]
        params: list[Any] = []
        for field, value in (
            ("namespace_key", resolved_namespace),
            ("resource_type", resource_type),
            ("resource_scope", resource_scope),
            ("principal_type", principal_type),
            ("principal_id", principal_id),
            ("permission", permission),
        ):
            if value:
                filters.append(f"{field} = ?")
                params.append(value)
        rows = self.db.fetch_all(f"SELECT id FROM scope_acl_rules WHERE {' AND '.join(filters)}", tuple(params))
        for row in rows:
            self.db.execute("DELETE FROM scope_acl_rules WHERE id = ?", (row["id"],))
        return {"deleted": True, "count": len(rows)}

    def _planned_retrieval_domains(
        self,
        query: str,
        *,
        session_id: str | None,
        run_id: str | None,
        owner_agent_id: str | None,
        subject_type: str | None,
        subject_id: str | None,
        interaction_type: str | None,
    ) -> list[str]:
        selected = self.recall_router.route(query, session_id=session_id)
        lowered = str(query or "").lower()
        if run_id and "execution" not in selected:
            selected.append("execution")
        if owner_agent_id and any(token in lowered for token in ("handoff", "交接", "接手")):
            for domain in ("handoff", "context", "reflection"):
                if domain not in selected:
                    selected.append(domain)
        if session_id and any(token in lowered for token in ("context", "上下文", "summary", "摘要", "brief")):
            if "context" not in selected:
                selected.append("context")
        if owner_agent_id and any(token in lowered for token in ("lesson", "经验", "反思", "reflect", "reflection")):
            if "reflection" not in selected:
                selected.append("reflection")
        if subject_type == "agent" and subject_id and "handoff" not in selected and any(token in lowered for token in ("agent", "代理", "协同")):
            selected.append("handoff")
        return list(dict.fromkeys(selected))

    def plan_recall(
        self,
        query: str,
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
        preferred_scope: str | None = None,
        limit: int = 10,
        auxiliary_limit: int | None = None,
        domains: list[str] | None = None,
    ) -> dict[str, Any]:
        context = self._build_context(
            user_id=user_id,
            session_id=session_id,
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
            run_id=run_id,
            actor_id=actor_id,
            role=role,
        )
        plan = self.recall_planner.plan(
            query,
            context=context,
            policy=self.config.memory_policy,
            preferred_scope=preferred_scope,
            limit=limit,
            auxiliary_limit=auxiliary_limit,
            graph_enabled=False,
        )
        selected_domains = list(
            dict.fromkeys(
                domains
                or plan.get("handoff_domains")
                or self._planned_retrieval_domains(
                    query,
                    session_id=session_id,
                    run_id=run_id,
                    owner_agent_id=context.owner_agent_id,
                    subject_type=context.subject_type,
                    subject_id=context.subject_id,
                    interaction_type=context.interaction_type,
                )
            )
        )
        if not selected_domains:
            selected_domains = self._planned_retrieval_domains(
                query,
                session_id=session_id,
                run_id=run_id,
                owner_agent_id=context.owner_agent_id,
                subject_type=context.subject_type,
                subject_id=context.subject_id,
                interaction_type=context.interaction_type,
            )
        normalized_stages: list[dict[str, Any]] = []
        for stage in list(plan.get("stages") or []):
            stage_scope = str(stage.get("scope") or "")
            normalized_scope = "long-term" if stage_scope in {"all", "", "long_term"} else stage_scope
            normalized_stages.append({**dict(stage), "scope": normalized_scope})
        return {
            **plan,
            "query": query,
            "scope": preferred_scope or str(plan.get("query_profile", {}).get("preferred_scope") or ("session" if session_id else "long-term")),
            "selected_domains": selected_domains,
            "stages": normalized_stages,
            "scope_context": context.as_metadata(),
            "router_weights": self.recall_router.explain(query, session_id=session_id),
            "limit": int(limit),
            "auxiliary_limit": int(auxiliary_limit or self.config.memory_policy.auxiliary_search_limit),
        }

    def _context_scope_filters(
        self,
        *,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
    ) -> tuple[str | None, dict[str, Any]]:
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
        affinity = {
            "owner_agent_id": owner_agent_id,
            "subject_type": subject_type,
            "subject_id": subject_id,
            "interaction_type": interaction_type,
        }
        return namespace_filter, affinity

    def search_context_artifacts(
        self,
        query: str,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        target_agent_id: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        artifact_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
        **_unused: Any,
    ) -> dict[str, Any]:
        sql_filters = ["1 = 1"]
        params: list[Any] = []
        if target_agent_id:
            sql_filters.append("(target_agent_id = ? OR target_agent_id IS NULL)")
            params.append(target_agent_id)
        if session_id:
            sql_filters.append("(session_id = ? OR session_id IS NULL)")
            params.append(session_id)
        if run_id:
            sql_filters.append("(run_id = ? OR run_id IS NULL)")
            params.append(run_id)
        if artifact_type:
            sql_filters.append("artifact_type = ?")
            params.append(artifact_type)
        namespace_filter, affinity = self._context_scope_filters(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=session_id,
            run_id=run_id,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            sql_filters.append("(namespace_key = ? OR namespace_key IS NULL)")
            params.append(namespace_filter)
        rows = [
            item
            for item in (
                self._deserialize_context_artifact(row)
                for row in self.db.fetch_all(
                    f"""
                    SELECT ca.*, sic.embedding
                    FROM context_artifacts ca
                    LEFT JOIN semantic_index_cache sic ON sic.record_id = ca.id AND sic.collection = 'context_artifact_index'
                    WHERE {' AND '.join(sql_filters)}
                    ORDER BY ca.created_at DESC
                    LIMIT ?
                    """,
                    tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
                )
            )
            if item is not None
        ]
        requester_scope = self._request_access_scope(
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
            namespace_key=namespace_key or namespace_filter,
        )
        rows = self._filter_accessible_rows(rows, resource_type="context", requester_scope=requester_scope)
        fts_hits = self._fts_hit_map(["context_artifact_index"], query, limit=max(limit * 6, 24))
        vector_hits = self._vector_hit_map("context_artifact_index", query, limit=max(limit * 6, 24))
        ranked = self._rank_rows(
            query,
            rows,
            domain="context",
            text_key="content",
            keywords_getter=lambda row: extract_keywords(str(row.get("content") or "")),
            updated_at_key="created_at",
            importance_getter=lambda row: 0.74,
            half_life_days=self.config.memory_policy.short_term_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity=affinity,
        )
        return {"results": ranked[:limit]}

    def search_handoff_packs(
        self,
        query: str,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        source_agent_id: str | None = None,
        target_agent_id: str | None = None,
        source_session_id: str | None = None,
        source_run_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
        **_unused: Any,
    ) -> dict[str, Any]:
        sql_filters = ["1 = 1"]
        params: list[Any] = []
        if source_session_id:
            sql_filters.append("(source_session_id = ? OR source_session_id IS NULL)")
            params.append(source_session_id)
        if source_run_id:
            sql_filters.append("(source_run_id = ? OR source_run_id IS NULL)")
            params.append(source_run_id)
        if source_agent_id:
            sql_filters.append("(source_agent_id = ? OR source_agent_id IS NULL)")
            params.append(source_agent_id)
        if target_agent_id:
            sql_filters.append("(target_agent_id = ? OR target_agent_id IS NULL)")
            params.append(target_agent_id)
        namespace_filter, affinity = self._context_scope_filters(
            user_id=user_id,
            owner_agent_id=owner_agent_id or source_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            session_id=source_session_id,
            run_id=source_run_id,
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
        )
        if namespace_filter:
            sql_filters.append("(namespace_key = ? OR namespace_key IS NULL)")
            params.append(namespace_filter)
        rows = [
            item
            for item in (
                self._deserialize_handoff_pack(row)
                for row in self.db.fetch_all(
                    f"""
                    SELECT hp.*, sic.embedding
                    FROM handoff_packs hp
                    LEFT JOIN semantic_index_cache sic ON sic.record_id = hp.id AND sic.collection = 'handoff_pack_index'
                    WHERE {' AND '.join(sql_filters)}
                    ORDER BY hp.created_at DESC
                    LIMIT ?
                    """,
                    tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
                )
            )
            if item is not None
        ]
        requester_scope = self._request_access_scope(
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
            namespace_key=namespace_key or namespace_filter,
        )
        rows = self._filter_accessible_rows(rows, resource_type="handoff", requester_scope=requester_scope)
        fts_hits = self._fts_hit_map(["handoff_pack_index"], query, limit=max(limit * 6, 24))
        vector_hits = self._vector_hit_map("handoff_pack_index", query, limit=max(limit * 6, 24))
        ranked = self._rank_rows(
            query,
            rows,
            domain="handoff",
            text_key="text",
            keywords_getter=lambda row: extract_keywords(str(row.get("text") or "")),
            updated_at_key="created_at",
            importance_getter=lambda row: 0.76,
            half_life_days=self.config.memory_policy.short_term_half_life_days,
            threshold=threshold,
            filters=filters,
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity=affinity,
        )
        return {"results": ranked[:limit]}

    def search_reflection_memories(
        self,
        query: str,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        reflection_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
        limit: int = 10,
        threshold: float = 0.0,
        filters: dict[str, Any] | None = None,
        **_unused: Any,
    ) -> dict[str, Any]:
        sql_filters = ["1 = 1"]
        params: list[Any] = []
        if session_id:
            sql_filters.append("(session_id = ? OR session_id IS NULL)")
            params.append(session_id)
        if run_id:
            sql_filters.append("(run_id = ? OR run_id IS NULL)")
            params.append(run_id)
        if reflection_type:
            sql_filters.append("reflection_type = ?")
            params.append(reflection_type)
        rows = [
            item
            for item in (
                self._deserialize_reflection_memory(row)
                for row in self.db.fetch_all(
                    f"""
                    SELECT rm.*, sic.embedding
                    FROM reflection_memories rm
                    LEFT JOIN semantic_index_cache sic ON sic.record_id = rm.id AND sic.collection = 'reflection_memory_index'
                    WHERE {' AND '.join(sql_filters)}
                    ORDER BY rm.updated_at DESC
                    LIMIT ?
                    """,
                    tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
                )
            )
            if item is not None
        ]
        requester_scope = self._request_access_scope(
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
        rows = self._filter_accessible_rows(rows, resource_type="reflection", requester_scope=requester_scope)
        fts_hits = self._fts_hit_map(["reflection_memory_index"], query, limit=max(limit * 6, 24))
        vector_hits = self._vector_hit_map("reflection_memory_index", query, limit=max(limit * 6, 24))
        ranked = self._rank_rows(
            query,
            rows,
            domain="reflection",
            text_key="text",
            keywords_getter=lambda row: extract_keywords(str(row.get("text") or "")),
            updated_at_key="updated_at",
            importance_getter=lambda row: float(row.get("confidence", 0.5) or 0.0),
            half_life_days=self.config.memory_policy.long_term_half_life_days,
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

    def build_context(
        self,
        query: str,
        *,
        include_domains: list[str] | None = None,
        budget_chars: int | None = None,
        target_agent_id: str | None = None,
        use_platform_llm: bool = True,
        metadata: dict[str, Any] | None = None,
        limit: int = 12,
        threshold: float = 0.0,
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
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        actor_agent_id = agent_id or owner_agent_id
        planned = None if include_domains else self.plan_recall(
            query,
            user_id=user_id,
            session_id=session_id,
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
            run_id=run_id,
            actor_id=actor_id,
            role=role,
            limit=limit,
        )
        selected_domains = list(dict.fromkeys(include_domains or list((planned or {}).get("selected_domains") or ["memory", "interaction", "knowledge", "skill", "archive", "execution"])))
        budget = int(budget_chars or self.config.memory_policy.compression_budget_chars)
        run = _deserialize_row(self.db.fetch_one("SELECT * FROM runs WHERE id = ?", (run_id,))) if run_id else None
        session = self.get_session(session_id) if session_id else None
        scope = self._resolve_scope(
            user_id=user_id or (run.get("user_id") if run else None),
            agent_id=agent_id,
            owner_agent_id=owner_agent_id or (run.get("owner_agent_id") if run else None),
            subject_type=subject_type or (run.get("subject_type") if run else None),
            subject_id=subject_id or (run.get("subject_id") if run else None),
            interaction_type=interaction_type or (run.get("interaction_type") if run else None),
            platform_id=platform_id,
            workspace_id=workspace_id,
            team_id=team_id,
            project_id=project_id,
            namespace_key=namespace_key,
            session=session,
        )
        requester_scope = self._request_access_scope_for_scope(
            scope,
            actor_user_id=user_id,
            actor_agent_id=actor_agent_id,
        )
        self._assert_namespace_permission(
            namespace_key=scope.get("namespace_key"),
            resource_type="context",
            resource_scope="prompt_context",
            requester_scope=requester_scope,
            permission="write",
            action_label="build context artifact",
        )
        recall = self.query(
            query,
            user_id=scope.get("user_id"),
            owner_agent_id=scope.get("owner_agent_id"),
            subject_type=scope.get("subject_type"),
            subject_id=scope.get("subject_id"),
            interaction_type=scope.get("interaction_type"),
            session_id=session_id,
            agent_id=agent_id or scope.get("owner_agent_id"),
            platform_id=scope.get("platform_id"),
            workspace_id=scope.get("workspace_id"),
            team_id=scope.get("team_id"),
            project_id=scope.get("project_id"),
            namespace_key=scope.get("namespace_key"),
            run_id=run_id,
            actor_id=actor_id,
            role=role,
            domains=selected_domains,
            limit=max(limit, 8),
            threshold=threshold,
        )
        source_rows = self._dedupe_compression_rows(recall.get("results", []))
        records = self._compression_records_from_rows(source_rows)
        job = self._start_compression_job(
            job_type="context_build",
            session_id=session_id,
            run_id=run_id,
            owner_agent_id=scope.get("owner_agent_id"),
            budget_chars=budget,
            scope=scope,
            records=records,
            metadata={"query": query, "include_domains": selected_domains, **dict(metadata or {})},
        )
        compression, status, provider, model, error = self._run_contextual_compression(
            job_type="context_build",
            records=records,
            scope=scope,
            budget_chars=budget,
            use_platform_llm=use_platform_llm,
            metadata={"query": query, "include_domains": selected_domains, **dict(metadata or {})},
        )
        content = self._format_compression_content("Context Brief", compression) or f"## Context Brief\n{query}"
        source_refs = self._source_refs_from_rows(source_rows)
        artifact = self._create_context_artifact(
            artifact_type="prompt_context",
            session_id=session_id,
            run_id=run_id,
            owner_agent_id=scope.get("owner_agent_id"),
            target_agent_id=target_agent_id,
            namespace_key=scope.get("namespace_key"),
            provider=provider,
            model=model,
            budget_chars=budget,
            source_refs=source_refs,
            content=content,
            scope_metadata=self._scope_metadata(scope),
            visibility="target_agent" if target_agent_id else "private_agent",
            metadata={
                "query": query,
                "include_domains": selected_domains,
                "compression": compression,
                **dict(metadata or {}),
            },
            expires_at=expires_at,
        )
        job = self._finish_compression_job(
            job["id"],
            status=status,
            provider=provider,
            model=model,
            result_artifact_id=artifact["id"],
            error=error,
        )
        return {
            "query": query,
            "artifact": artifact,
            "job": job,
            "compression": compression,
            "results": recall.get("results", []),
            "source_count": len(source_rows),
            "domains": selected_domains,
        }

    def build_handoff_pack(
        self,
        target_agent_id: str,
        *,
        source_run_id: str | None = None,
        source_session_id: str | None = None,
        run_id: str | None = None,
        source_agent_id: str | None = None,
        budget_chars: int | None = None,
        visibility: str = "target_agent",
        use_platform_llm: bool = True,
        metadata: dict[str, Any] | None = None,
        query: str | None = None,
        expires_at: str | None = None,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        agent_id: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
    ) -> dict[str, Any]:
        actor_agent_id = agent_id or owner_agent_id or source_agent_id
        resolved_run_id = source_run_id or run_id
        run = _deserialize_row(self.db.fetch_one("SELECT * FROM runs WHERE id = ?", (resolved_run_id,))) if resolved_run_id else None
        resolved_session_id = source_session_id or (run.get("session_id") if run else None)
        session = self.get_session(resolved_session_id) if resolved_session_id else None
        scope = self._resolve_scope(
            user_id=user_id or (run.get("user_id") if run else None),
            agent_id=agent_id,
            owner_agent_id=owner_agent_id or (run.get("owner_agent_id") if run else None) or (session.get("owner_agent_id") if session else None),
            subject_type=subject_type or (run.get("subject_type") if run else None) or (session.get("subject_type") if session else None),
            subject_id=subject_id or (run.get("subject_id") if run else None) or (session.get("subject_id") if session else None),
            interaction_type=interaction_type or (run.get("interaction_type") if run else None) or (session.get("interaction_type") if session else None),
            platform_id=platform_id or (run.get("platform_id") if run else None) or (session.get("platform_id") if session else None),
            workspace_id=workspace_id or (run.get("workspace_id") if run else None) or (session.get("workspace_id") if session else None),
            team_id=team_id or (run.get("team_id") if run else None) or (session.get("team_id") if session else None),
            project_id=project_id or (run.get("project_id") if run else None) or (session.get("project_id") if session else None),
            namespace_key=namespace_key or (run.get("namespace_key") if run else None) or (session.get("namespace_key") if session else None),
        )
        requester_scope = self._request_access_scope_for_scope(
            scope,
            actor_user_id=user_id,
            actor_agent_id=actor_agent_id,
        )
        self._assert_namespace_permission(
            namespace_key=scope.get("namespace_key"),
            resource_type="handoff",
            resource_scope="handoff",
            requester_scope=requester_scope,
            permission="write",
            action_label="build handoff pack",
        )
        budget = int(budget_chars or max(self.config.memory_policy.compression_budget_chars, 900))
        rows: list[dict[str, Any]] = []
        if run is not None:
            run_text = "\n".join(
                part for part in [str(run.get("goal") or "").strip(), f"Run status: {run.get('status')}" if run.get("status") else ""] if part
            ).strip()
            if run_text:
                rows.append(
                    {
                        "id": run["id"],
                        "domain": "execution",
                        "text": run_text,
                        "score": 0.82,
                        "run_id": run["id"],
                        "metadata": {"kind": "run"},
                    }
                )
        if session is not None:
            for turn in self._session_turns(session["id"]):
                rows.append(
                    {
                        "id": turn["id"],
                        "domain": "interaction",
                        "text": str(turn.get("content") or ""),
                        "score": 0.52 + min(0.22, float(turn.get("salience_score", 0.0) or 0.0) * 0.2),
                        "session_id": session["id"],
                        "run_id": turn.get("run_id"),
                        "metadata": {"role": turn.get("role"), "turn_type": turn.get("turn_type")},
                    }
                )
            snapshot = self.session_health(session["id"]).get("latest_snapshot")
            snapshot_text = self._snapshot_text(snapshot)
            if snapshot_text:
                rows.append(
                    {
                        "id": snapshot["id"],
                        "domain": "interaction",
                        "text": snapshot_text,
                        "score": 0.78,
                        "session_id": session["id"],
                        "run_id": snapshot.get("run_id"),
                        "metadata": {"kind": "working_memory_snapshot"},
                    }
                )
        seed_query = str(query or (run.get("goal") if run else None) or (session.get("title") if session else None) or f"handoff to {target_agent_id}").strip()
        related = self.query(
            seed_query,
            user_id=scope.get("user_id"),
            owner_agent_id=scope.get("owner_agent_id"),
            subject_type=scope.get("subject_type"),
            subject_id=scope.get("subject_id"),
            interaction_type=scope.get("interaction_type"),
            session_id=resolved_session_id,
            agent_id=agent_id or scope.get("owner_agent_id"),
            platform_id=scope.get("platform_id"),
            workspace_id=scope.get("workspace_id"),
            team_id=scope.get("team_id"),
            project_id=scope.get("project_id"),
            namespace_key=scope.get("namespace_key"),
            run_id=resolved_run_id,
            domains=["memory", "knowledge", "execution", "archive"],
            limit=12,
        )
        rows.extend(related.get("results", []))
        source_rows = self._dedupe_compression_rows(rows)
        records = self._compression_records_from_rows(source_rows)
        job = self._start_compression_job(
            job_type="handoff_build",
            session_id=resolved_session_id,
            run_id=resolved_run_id,
            owner_agent_id=scope.get("owner_agent_id"),
            budget_chars=budget,
            scope=scope,
            records=records,
            metadata={"target_agent_id": target_agent_id, **dict(metadata or {})},
        )
        compression, status, provider, model, error = self._run_contextual_compression(
            job_type="handoff_build",
            records=records,
            scope=scope,
            budget_chars=budget,
            use_platform_llm=use_platform_llm,
            metadata={"target_agent_id": target_agent_id, **dict(metadata or {})},
        )
        source_refs = self._source_refs_from_rows(source_rows)
        snapshot = self.session_health(resolved_session_id).get("latest_snapshot") if resolved_session_id else None
        snapshot_open_tasks = [str(item).strip() for item in list((snapshot or {}).get("next_actions") or []) if str(item).strip()]
        snapshot_constraints = [str(item).strip() for item in list((snapshot or {}).get("constraints") or []) if str(item).strip()]
        open_tasks = list(dict.fromkeys(snapshot_open_tasks + [str(item).strip() for item in list(compression.get("steps") or []) if str(item).strip()]))
        constraints = list(dict.fromkeys(snapshot_constraints + [str(item).strip() for item in list(compression.get("constraints") or []) if str(item).strip()]))
        highlights = [str(item).strip() for item in list(compression.get("highlights") or []) if str(item).strip()]
        summary = str(compression.get("summary") or seed_query or f"Handoff for {target_agent_id}").strip()
        handoff = self._create_handoff_pack_row(
            source_run_id=resolved_run_id,
            source_session_id=resolved_session_id,
            source_agent_id=source_agent_id or scope.get("owner_agent_id"),
            target_agent_id=target_agent_id,
            namespace_key=scope.get("namespace_key"),
            visibility=visibility,
            summary=summary,
            highlights=highlights,
            open_tasks=open_tasks,
            constraints=constraints,
            source_refs=source_refs,
            scope_metadata=self._scope_metadata(scope),
            metadata={"compression": compression, **dict(metadata or {})},
            expires_at=expires_at,
        )
        artifact = self._create_context_artifact(
            artifact_type="handoff_pack",
            session_id=resolved_session_id,
            run_id=resolved_run_id,
            owner_agent_id=scope.get("owner_agent_id"),
            target_agent_id=target_agent_id,
            namespace_key=scope.get("namespace_key"),
            provider=provider,
            model=model,
            budget_chars=budget,
            source_refs=source_refs,
            content=self._format_compression_content(
                "Handoff Brief",
                {"summary": summary, "steps": open_tasks, "constraints": constraints, "highlights": highlights, "supporting_passages": compression.get("supporting_passages")},
            ),
            scope_metadata=self._scope_metadata(scope),
            visibility="target_agent",
            metadata={"handoff_id": handoff["id"], "compression": compression, **dict(metadata or {})},
            expires_at=expires_at,
        )
        job = self._finish_compression_job(
            job["id"],
            status=status,
            provider=provider,
            model=model,
            result_artifact_id=artifact["id"],
            error=error,
        )
        return {
            **handoff,
            "artifact": artifact,
            "job": job,
            "compression": compression,
            "source_count": len(source_rows),
        }

    def reflect_session(
        self,
        session_id: str,
        *,
        run_id: str | None = None,
        mode: str | None = None,
        budget_chars: int | None = None,
        use_platform_llm: bool = True,
        metadata: dict[str, Any] | None = None,
        expires_at: str | None = None,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        platform_id: str | None = None,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        namespace_key: str | None = None,
    ) -> dict[str, Any]:
        actor_agent_id = owner_agent_id
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        budget = int(budget_chars or max(self.config.memory_policy.compression_budget_chars, 900))
        rows: list[dict[str, Any]] = []
        for turn in self._session_turns(session_id):
            rows.append(
                {
                    "id": turn["id"],
                    "domain": "interaction",
                    "text": str(turn.get("content") or ""),
                    "score": 0.5 + min(0.25, float(turn.get("salience_score", 0.0) or 0.0) * 0.25),
                    "session_id": session_id,
                    "run_id": turn.get("run_id"),
                    "metadata": {"role": turn.get("role"), "turn_type": turn.get("turn_type")},
                }
            )
        snapshot = self.session_health(session_id).get("latest_snapshot")
        snapshot_text = self._snapshot_text(snapshot)
        if snapshot_text:
            rows.append(
                {
                    "id": snapshot["id"],
                    "domain": "interaction",
                    "text": snapshot_text,
                    "score": 0.8,
                    "session_id": session_id,
                    "run_id": snapshot.get("run_id"),
                    "metadata": {"kind": "working_memory_snapshot"},
                }
            )
        for memory in self.memory_list(
            user_id=session.get("user_id"),
            owner_agent_id=session.get("owner_agent_id"),
            subject_type=session.get("subject_type"),
            subject_id=session.get("subject_id"),
            interaction_type=session.get("interaction_type"),
            session_id=session_id,
            scope=str(MemoryScope.SESSION),
            limit=24,
        )["results"]:
            rows.append(
                {
                    "id": memory["id"],
                    "domain": "memory",
                    "text": str(memory.get("text") or ""),
                    "score": float(memory.get("importance", 0.55) or 0.0),
                    "session_id": session_id,
                    "run_id": memory.get("run_id"),
                    "metadata": {"memory_type": memory.get("memory_type")},
                }
            )
        source_rows = self._dedupe_compression_rows(rows)
        records = self._compression_records_from_rows(source_rows)
        scope = self._resolve_scope(
            user_id=user_id or session.get("user_id"),
            owner_agent_id=owner_agent_id or session.get("owner_agent_id"),
            subject_type=subject_type or session.get("subject_type"),
            subject_id=subject_id or session.get("subject_id"),
            interaction_type=interaction_type or session.get("interaction_type"),
            platform_id=platform_id or session.get("platform_id"),
            workspace_id=workspace_id or session.get("workspace_id"),
            team_id=team_id or session.get("team_id"),
            project_id=project_id or session.get("project_id"),
            namespace_key=namespace_key or session.get("namespace_key"),
        )
        requester_scope = self._request_access_scope_for_scope(
            scope,
            actor_user_id=user_id,
            actor_agent_id=actor_agent_id,
        )
        self._assert_namespace_permission(
            namespace_key=scope.get("namespace_key"),
            resource_type="reflection",
            resource_scope="reflection",
            requester_scope=requester_scope,
            permission="write",
            action_label="create reflection artifacts",
        )
        job = self._start_compression_job(
            job_type="reflection_session",
            session_id=session_id,
            run_id=run_id,
            owner_agent_id=scope.get("owner_agent_id"),
            budget_chars=budget,
            scope=scope,
            records=records,
            metadata={"mode": mode or "derived+invariant", **dict(metadata or {})},
        )
        compression, status, provider, model, error = self._run_contextual_compression(
            job_type="reflection_session",
            records=records,
            scope=scope,
            budget_chars=budget,
            use_platform_llm=use_platform_llm,
            metadata={"mode": mode or "derived+invariant", **dict(metadata or {})},
        )
        source_refs = self._source_refs_from_rows(source_rows)
        reflections: list[dict[str, Any]] = []
        for reflection_type in self._reflection_types(mode):
            summary, details, confidence = self._reflection_payload_for_type(reflection_type, compression)
            if not summary:
                continue
            reflections.append(
                self._create_reflection_memory_row(
                    owner_agent_id=scope.get("owner_agent_id"),
                    session_id=session_id,
                    run_id=run_id,
                    reflection_type=reflection_type,
                    summary=summary,
                    details=details,
                    confidence=confidence,
                    decay_half_life_days=self.config.memory_policy.long_term_half_life_days,
                    source_refs=source_refs,
                    scope_metadata=self._scope_metadata(scope),
                    visibility="private_agent",
                    metadata={
                        "compression": compression,
                        "namespace_key": scope.get("namespace_key"),
                        **dict(metadata or {}),
                    },
                )
            )
        artifact = self._create_context_artifact(
            artifact_type="reflection_pack",
            session_id=session_id,
            run_id=run_id,
            owner_agent_id=scope.get("owner_agent_id"),
            target_agent_id=None,
            namespace_key=scope.get("namespace_key"),
            provider=provider,
            model=model,
            budget_chars=budget,
            source_refs=source_refs,
            content=self._format_compression_content("Reflection Brief", compression),
            scope_metadata=self._scope_metadata(scope),
            visibility="private_agent",
            metadata={
                "mode": mode or "derived+invariant",
                "reflection_ids": [item["id"] for item in reflections],
                "compression": compression,
                **dict(metadata or {}),
            },
            expires_at=expires_at,
        )
        job = self._finish_compression_job(
            job["id"],
            status=status,
            provider=provider,
            model=model,
            result_artifact_id=artifact["id"],
            error=error,
        )
        return {
            "session_id": session_id,
            "run_id": run_id,
            "mode": mode or "derived+invariant",
            "reflections": reflections,
            "artifact": artifact,
            "job": job,
            "compression": compression,
        }

    def reflect_run(self, run_id: str, **kwargs) -> dict[str, Any]:
        run = _deserialize_row(self.db.fetch_one("SELECT * FROM runs WHERE id = ?", (run_id,)))
        if run is None:
            raise ValueError(f"Run `{run_id}` does not exist.")
        if run.get("session_id"):
            return self.reflect_session(str(run["session_id"]), run_id=run_id, **kwargs)
        requester_scope = self._pop_request_access_scope(kwargs)
        rows = [
            {
                "id": run["id"],
                "domain": "execution",
                "text": "\n".join(
                    part for part in [str(run.get("goal") or "").strip(), f"Run status: {run.get('status')}" if run.get("status") else ""] if part
                ).strip(),
                "score": 0.82,
                "run_id": run_id,
                "metadata": {"kind": "run"},
            }
        ]
        for row in self.db.fetch_all("SELECT * FROM observations WHERE run_id = ? ORDER BY created_at ASC", (run_id,)):
            text = str(row.get("content") or "").strip()
            if not text:
                continue
            rows.append(
                {
                    "id": row["id"],
                    "domain": "execution",
                    "text": text,
                    "score": 0.64,
                    "run_id": run_id,
                    "metadata": {"kind": row.get("kind")},
                }
            )
        scope = self._resolve_scope(
            user_id=run.get("user_id"),
            owner_agent_id=run.get("owner_agent_id"),
            subject_type=run.get("subject_type"),
            subject_id=run.get("subject_id"),
            interaction_type=run.get("interaction_type"),
            platform_id=run.get("platform_id"),
            workspace_id=run.get("workspace_id"),
            team_id=run.get("team_id"),
            project_id=run.get("project_id"),
            namespace_key=run.get("namespace_key"),
        )
        self._assert_namespace_permission(
            namespace_key=scope.get("namespace_key"),
            resource_type="reflection",
            resource_scope="reflection",
            requester_scope=requester_scope,
            permission="write",
            action_label="create run reflections",
        )
        budget = int(kwargs.pop("budget_chars", max(self.config.memory_policy.compression_budget_chars, 900)))
        mode = kwargs.pop("mode", None)
        metadata = dict(kwargs.pop("metadata", {}) or {})
        use_platform_llm = bool(kwargs.pop("use_platform_llm", True))
        source_rows = self._dedupe_compression_rows(rows)
        records = self._compression_records_from_rows(source_rows)
        job = self._start_compression_job(
            job_type="reflection_run",
            session_id=None,
            run_id=run_id,
            owner_agent_id=scope.get("owner_agent_id"),
            budget_chars=budget,
            scope=scope,
            records=records,
            metadata={"mode": mode or "derived+invariant", **metadata},
        )
        compression, status, provider, model, error = self._run_contextual_compression(
            job_type="reflection_run",
            records=records,
            scope=scope,
            budget_chars=budget,
            use_platform_llm=use_platform_llm,
            metadata={"mode": mode or "derived+invariant", **metadata},
        )
        source_refs = self._source_refs_from_rows(source_rows)
        reflections: list[dict[str, Any]] = []
        for reflection_type in self._reflection_types(mode):
            summary, details, confidence = self._reflection_payload_for_type(reflection_type, compression)
            if not summary:
                continue
            reflections.append(
                self._create_reflection_memory_row(
                    owner_agent_id=scope.get("owner_agent_id"),
                    session_id=None,
                    run_id=run_id,
                    reflection_type=reflection_type,
                    summary=summary,
                    details=details,
                    confidence=confidence,
                    decay_half_life_days=self.config.memory_policy.long_term_half_life_days,
                    source_refs=source_refs,
                    scope_metadata=self._scope_metadata(scope),
                    visibility="private_agent",
                    metadata={"namespace_key": scope.get("namespace_key"), "compression": compression, **metadata},
                )
            )
        artifact = self._create_context_artifact(
            artifact_type="reflection_pack",
            session_id=None,
            run_id=run_id,
            owner_agent_id=scope.get("owner_agent_id"),
            target_agent_id=None,
            namespace_key=scope.get("namespace_key"),
            provider=provider,
            model=model,
            budget_chars=budget,
            source_refs=source_refs,
            content=self._format_compression_content("Run Reflection", compression),
            scope_metadata=self._scope_metadata(scope),
            visibility="private_agent",
            metadata={"mode": mode or "derived+invariant", "reflection_ids": [item["id"] for item in reflections], "compression": compression, **metadata},
            expires_at=kwargs.pop("expires_at", None),
        )
        job = self._finish_compression_job(
            job["id"],
            status=status,
            provider=provider,
            model=model,
            result_artifact_id=artifact["id"],
            error=error,
        )
        return {
            "run_id": run_id,
            "mode": mode or "derived+invariant",
            "reflections": reflections,
            "artifact": artifact,
            "job": job,
            "compression": compression,
        }

    def create_session(self, user_id: str | None = None, session_id: str | None = None, **kwargs) -> dict[str, Any]:
        session_id = session_id or make_id("sess")
        now = utcnow_iso()
        metadata = dict(kwargs.pop("metadata", {}) or {})
        raw_agent_id = kwargs.pop("agent_id", None)
        raw_owner_agent_id = kwargs.pop("owner_agent_id", None)
        actor_agent_id = raw_agent_id or raw_owner_agent_id
        scope = self._resolve_scope(
            user_id=user_id,
            agent_id=raw_agent_id,
            owner_agent_id=raw_owner_agent_id,
            subject_type=kwargs.pop("subject_type", None),
            subject_id=kwargs.pop("subject_id", None),
            interaction_type=kwargs.pop("interaction_type", None),
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
        )
        requester_scope = self._request_access_scope_for_scope(
            scope,
            actor_user_id=user_id,
            actor_agent_id=actor_agent_id,
        )
        self._assert_namespace_permission(
            namespace_key=scope.get("namespace_key"),
            resource_type="session",
            resource_scope="session",
            requester_scope=requester_scope,
            permission="write",
            action_label="create session",
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
        requester_scope = self._pop_request_access_scope(kwargs)
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        self._assert_resource_permission(
            session,
            resource_type="session",
            requester_scope=requester_scope,
            permission="write",
            action_label="append session turn",
        )
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

    def _store_document_payload(
        self,
        *,
        document_id: str,
        text: str,
        scope: dict[str, Any],
        metadata: dict[str, Any],
        raw_store_policy: str | None,
    ) -> dict[str, Any]:
        policy = normalize_raw_store_policy(raw_store_policy, default=self.config.knowledge_raw_store_policy)
        externalize = should_externalize_text(
            text,
            policy=policy,
            inline_char_limit=int(self.config.knowledge_inline_char_limit),
        )
        payload_bytes = payload_size_bytes(text)
        inline_text = None if externalize else text
        storage_ref = None
        object_row = None
        if externalize:
            stored = self.object_store.put_text(
                text,
                object_type="knowledge",
                suffix=".txt",
                prefix=self._object_store_prefix(scope, "knowledge"),
            )
            object_row = self._persist_object(
                stored,
                mime_type="text/plain",
                metadata={"document_id": document_id, **self._scope_metadata(scope), **metadata},
            )
            storage_ref = object_row["object_key"]
        return {
            "storage_policy": policy,
            "inline_text": inline_text,
            "inline_excerpt": build_inline_excerpt(text),
            "storage_ref": storage_ref,
            "payload_bytes": payload_bytes,
            "object_row": object_row,
        }

    def _document_text(self, document: dict[str, Any]) -> str:
        inline_text = str(document.get("inline_text") or "")
        if inline_text:
            return inline_text
        storage_ref = str(document.get("storage_ref") or "")
        if storage_ref:
            try:
                return self.object_store.get_text(storage_ref)
            except (FileNotFoundError, UnicodeDecodeError):
                pass
        latest_version = document["versions"][0] if document.get("versions") else None
        if latest_version is not None and latest_version.get("object_id"):
            object_row = self.db.fetch_one("SELECT * FROM objects WHERE id = ?", (latest_version["object_id"],))
            if object_row is not None:
                try:
                    return self.object_store.get_text(object_row["object_key"])
                except (FileNotFoundError, UnicodeDecodeError):
                    pass
        return "\n".join(str(chunk.get("content") or "") for chunk in document.get("chunks", []))

    def ingest_document(self, title: str, text: str, **kwargs) -> dict[str, Any]:
        source_name = kwargs.pop("source_name", title)
        source_type = kwargs.pop("source_type", "inline")
        uri = kwargs.pop("uri", None)
        global_scope = bool(kwargs.pop("global_scope", False))
        raw_user_id = kwargs.pop("user_id", None)
        raw_agent_id = kwargs.pop("agent_id", None)
        raw_owner_agent_id = kwargs.pop("owner_agent_id", None)
        actor_agent_id = raw_agent_id or raw_owner_agent_id
        raw_subject_type = kwargs.pop("source_subject_type", kwargs.pop("subject_type", None))
        raw_subject_id = kwargs.pop("source_subject_id", kwargs.pop("subject_id", None))
        scope = self._resolve_scope(
            user_id=raw_user_id,
            agent_id=raw_agent_id,
            owner_agent_id=raw_owner_agent_id,
            subject_type=raw_subject_type,
            subject_id=raw_subject_id,
            interaction_type=kwargs.pop("interaction_type", None),
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
            global_scope=global_scope,
        )
        requester_scope = self._request_access_scope_for_scope(
            scope,
            actor_user_id=raw_user_id,
            actor_agent_id=actor_agent_id,
        )
        self._assert_namespace_permission(
            namespace_key=scope.get("namespace_key"),
            resource_type="knowledge",
            resource_scope="document",
            requester_scope=requester_scope,
            permission="write",
            action_label="create knowledge document",
        )
        user_id = scope["user_id"]
        owner_agent_id = scope["owner_agent_id"]
        source_subject_type = scope["subject_type"]
        source_subject_id = scope["subject_id"]
        external_id = kwargs.pop("external_id", None)
        metadata = dict(kwargs.pop("metadata", {}) or {})
        raw_store_policy = kwargs.pop("raw_store_policy", kwargs.pop("knowledge_raw_store_policy", None))
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
        payload = self._store_document_payload(
            document_id=document_id,
            text=text,
            scope=scope,
            metadata=metadata,
            raw_store_policy=raw_store_policy,
        )
        self.db.execute(
            """
            INSERT INTO documents(
                id, source_id, title, user_id, owner_agent_id, kb_namespace, source_subject_type, source_subject_id,
                namespace_key, inline_text, inline_excerpt, storage_policy, storage_ref, payload_bytes, external_id,
                status, metadata, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                payload["inline_text"],
                payload["inline_excerpt"],
                payload["storage_policy"],
                payload["storage_ref"],
                payload["payload_bytes"],
                external_id,
                "active",
                json_dumps(merge_metadata(metadata, self._scope_metadata(scope))),
                now,
                now,
            ),
        )
        version_id = make_id("docver")
        object_row = payload.get("object_row")
        self.db.execute(
            """
            INSERT INTO document_versions(id, document_id, version_label, object_id, checksum, size_bytes, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_id,
                document_id,
                "v1",
                object_row["id"] if object_row is not None else None,
                object_row["checksum"] if object_row is not None else fingerprint(text),
                payload["payload_bytes"],
                json_dumps(metadata),
                now,
            ),
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
        document["text"] = self._document_text(document)
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
        raw_user_id = kwargs.pop("user_id", None)
        raw_agent_id = kwargs.pop("agent_id", owner_id)
        raw_owner_agent_id = kwargs.pop("owner_agent_id", owner_id)
        actor_agent_id = raw_agent_id or raw_owner_agent_id
        raw_subject_type = kwargs.pop("source_subject_type", kwargs.pop("subject_type", None))
        raw_subject_id = kwargs.pop("source_subject_id", kwargs.pop("subject_id", None))
        scope = self._resolve_scope(
            agent_id=raw_agent_id,
            owner_agent_id=raw_owner_agent_id,
            subject_type=raw_subject_type,
            subject_id=raw_subject_id,
            interaction_type=kwargs.pop("interaction_type", None),
            user_id=raw_user_id,
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
        )
        requester_scope = self._request_access_scope_for_scope(
            scope,
            actor_user_id=raw_user_id,
            actor_agent_id=actor_agent_id,
        )
        self._assert_namespace_permission(
            namespace_key=scope.get("namespace_key"),
            resource_type="skill",
            resource_scope="skill",
            requester_scope=requester_scope,
            permission="write",
            action_label="save skill",
        )
        owner_agent_id = scope["owner_agent_id"]
        source_subject_type = scope["subject_type"]
        source_subject_id = scope["subject_id"]
        skill = self._find_skill_by_name(
            name=name,
            owner_agent_id=owner_agent_id,
            namespace_key=scope.get("namespace_key"),
        )
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

        previous_snapshot = self._current_skill_snapshot_row(skill_id)
        if previous_snapshot is not None:
            self._delete_skill_snapshot(previous_snapshot["id"])
        prompt_template = kwargs.pop("prompt_template", None)
        workflow = kwargs.pop("workflow", None)
        schema = kwargs.pop("schema", None)
        tools = list(kwargs.pop("tools", []) or [])
        tests = list(kwargs.pop("tests", []) or [])
        topics = list(kwargs.pop("topics", []) or [])
        self._write_skill_snapshot_record(
            skill_id=skill_id,
            name=name,
            description=description,
            scope=scope,
            metadata=metadata,
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

    def _load_skill_snapshot_asset(self, snapshot_or_id: dict[str, Any] | str | None) -> dict[str, Any]:
        if snapshot_or_id is None:
            return {}
        object_id = snapshot_or_id.get("object_id") if isinstance(snapshot_or_id, dict) else None
        if object_id is None and not isinstance(snapshot_or_id, dict):
            row = self.db.fetch_one("SELECT object_id FROM skill_snapshots WHERE id = ?", (str(snapshot_or_id),))
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

    def _skill_snapshot_file_rows(self, skill_snapshot_id: str) -> list[dict[str, Any]]:
        return _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT sf.*, o.object_key, o.object_type, o.metadata AS object_metadata
                FROM skill_files sf
                JOIN objects o ON o.id = sf.object_id
                WHERE sf.skill_snapshot_id = ?
                ORDER BY
                    CASE sf.role
                        WHEN 'skill_md' THEN 0
                        WHEN 'reference' THEN 1
                        WHEN 'script' THEN 2
                        ELSE 3
                    END,
                    sf.relative_path ASC
                """,
                (skill_snapshot_id,),
            ),
            ("metadata", "object_metadata"),
        )

    def _skill_snapshot_files(self, skill_snapshot_id: str, *, inline_contents: bool = True) -> list[dict[str, Any]]:
        files: list[dict[str, Any]] = []
        for row in self._skill_snapshot_file_rows(skill_snapshot_id):
            item = {
                "id": row["id"],
                "skill_id": row["skill_id"],
                "skill_snapshot_id": row["skill_snapshot_id"],
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

    def _clone_skill_snapshot_file_inputs(
        self,
        *,
        skill_snapshot_id: str,
        name: str,
        description: str,
        prompt_template: str | None,
        workflow: Any,
        tools: list[str],
        topics: list[str],
    ) -> list[dict[str, Any]]:
        cloned: list[dict[str, Any]] = []
        for row in self._skill_snapshot_file_rows(skill_snapshot_id):
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

    def _find_skill_by_name(
        self,
        *,
        name: str,
        owner_agent_id: str | None,
        namespace_key: str | None,
    ) -> dict[str, Any] | None:
        filters = ["name = ?"]
        params: list[Any] = [name]
        if owner_agent_id:
            filters.append("COALESCE(owner_agent_id, owner_id) = ?")
            params.append(owner_agent_id)
        else:
            filters.append("COALESCE(owner_agent_id, owner_id) IS NULL")
        if namespace_key:
            filters.append("namespace_key = ?")
            params.append(namespace_key)
        else:
            filters.append("namespace_key IS NULL")
        return self.db.fetch_one(
            f"SELECT * FROM skills WHERE {' AND '.join(filters)} ORDER BY updated_at DESC LIMIT 1",
            tuple(params),
        )

    def _current_skill_snapshot_row(self, skill_id: str) -> dict[str, Any] | None:
        return _deserialize_row(
            self.db.fetch_one(
                """
                SELECT ss.*
                FROM skill_snapshots ss
                JOIN skills s ON s.current_snapshot_id = ss.id
                WHERE s.id = ?
                LIMIT 1
                """,
                (skill_id,),
            ),
            ("metadata", "schema_json"),
        )

    def _build_skill_execution_context(
        self,
        reference_records: list[dict[str, Any]],
        *,
        query: str | None = None,
        budget_chars: int | None = None,
        max_sentences: int = 8,
        max_highlights: int = 12,
        generated_at: str | None = None,
    ) -> dict[str, Any]:
        if not reference_records:
            return {}
        compression = compress_records(
            reference_records,
            query=query,
            domain_hint="skill_reference",
            budget_chars=int(budget_chars or max(self.config.memory_policy.short_term_compression_budget_chars, 800)),
            max_sentences=max_sentences,
            diversity_lambda=self.config.memory_policy.diversity_lambda,
            max_highlights=max_highlights,
            policy=self.config.memory_policy,
        ).as_dict()
        reference_paths = list(
            dict.fromkeys(
                str(item.get("metadata", {}).get("relative_path") or item.get("id") or "").strip()
                for item in reference_records
                if str(item.get("metadata", {}).get("relative_path") or item.get("id") or "").strip()
            )
        )
        context = {
            **compression,
            "reference_paths": reference_paths,
            "generated_at": generated_at or utcnow_iso(),
            "query": str(query or ""),
        }
        context["text"] = _format_skill_execution_context(context)
        return context

    def _persist_skill_execution_context(self, skill_snapshot_id: str, execution_context: dict[str, Any]) -> dict[str, Any]:
        snapshot_row = self.db.fetch_one("SELECT * FROM skill_snapshots WHERE id = ?", (skill_snapshot_id,))
        if snapshot_row is None:
            raise ValueError(f"Skill snapshot `{skill_snapshot_id}` does not exist.")
        skill_row = self.db.fetch_one("SELECT * FROM skills WHERE id = ?", (snapshot_row["skill_id"],))
        if skill_row is None:
            raise ValueError(f"Skill `{snapshot_row['skill_id']}` does not exist.")
        payload = self._load_skill_snapshot_asset(snapshot_row)
        payload["execution_context"] = dict(execution_context or {})
        scope = {
            "owner_agent_id": skill_row.get("owner_agent_id") or skill_row.get("owner_id"),
            "subject_type": skill_row.get("source_subject_type"),
            "subject_id": skill_row.get("source_subject_id"),
            "namespace_key": skill_row.get("namespace_key"),
        }
        stored = self.object_store.put_text(
            json_dumps(payload),
            object_type="skills",
            suffix=".json",
            prefix=self._object_store_prefix(scope, "skill"),
        )
        object_row = self._persist_object(
            stored,
            mime_type="application/json",
            metadata={
                "skill_id": snapshot_row["skill_id"],
                "skill_snapshot_id": skill_snapshot_id,
                **self._scope_metadata(scope),
                **dict(payload.get("metadata") or {}),
            },
        )
        now = utcnow_iso()
        self.db.execute(
            "UPDATE skill_snapshots SET object_id = ?, metadata = ?, updated_at = ? WHERE id = ?",
            (
                object_row["id"],
                snapshot_row.get("metadata"),
                now,
                skill_snapshot_id,
            ),
        )
        self.db.execute("UPDATE skills SET updated_at = ? WHERE id = ?", (now, snapshot_row["skill_id"]))
        return payload

    def _delete_skill_snapshot_artifacts(self, skill_snapshot_id: str) -> None:
        rows = self.db.fetch_all("SELECT id FROM skill_reference_chunks WHERE skill_snapshot_id = ?", (skill_snapshot_id,))
        for row in rows:
            record_id = row["id"]
            self.db.execute("DELETE FROM skill_reference_index WHERE record_id = ?", (record_id,))
            self.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (record_id,))
            self._delete_text_search_record(record_id)
            self.vector_index.delete("skill_reference_index", record_id)
        self.db.execute("DELETE FROM skill_reference_chunks WHERE skill_snapshot_id = ?", (skill_snapshot_id,))
        self.db.execute("DELETE FROM skill_files WHERE skill_snapshot_id = ?", (skill_snapshot_id,))

    def _delete_skill_snapshot(self, skill_snapshot_id: str) -> None:
        self.db.execute("UPDATE skills SET current_snapshot_id = NULL WHERE current_snapshot_id = ?", (skill_snapshot_id,))
        self.db.execute("DELETE FROM skill_index WHERE record_id = ?", (skill_snapshot_id,))
        self.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (skill_snapshot_id,))
        self._delete_text_search_record(skill_snapshot_id)
        self.vector_index.delete("skill_index", skill_snapshot_id)
        self._delete_skill_snapshot_artifacts(skill_snapshot_id)
        self.db.execute("DELETE FROM skill_tests WHERE skill_snapshot_id = ?", (skill_snapshot_id,))
        self.db.execute("DELETE FROM skill_bindings WHERE skill_snapshot_id = ?", (skill_snapshot_id,))
        self.db.execute("DELETE FROM skill_snapshots WHERE id = ?", (skill_snapshot_id,))

    def _write_skill_snapshot_record(
        self,
        *,
        skill_id: str,
        name: str,
        description: str,
        scope: dict[str, Any],
        metadata: dict[str, Any],
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
        snapshot_id = make_id("skillsnap")
        workflow_text = workflow if isinstance(workflow, str) else json_dumps(workflow or {})
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
        reference_records = [
            {
                "id": entry["relative_path"],
                "text": str(entry.get("text_content") or ""),
                "score": 0.76,
                "metadata": {
                    "skill_id": skill_id,
                    "skill_snapshot_id": snapshot_id,
                    "relative_path": entry["relative_path"],
                },
            }
            for entry in normalized_files
            if entry["role"] == "reference" and entry.get("indexable") and entry.get("text_content")
        ]
        execution_context = self._build_skill_execution_context(reference_records, generated_at=now)
        asset_payload = {
            "name": name,
            "description": description,
            "workflow": workflow,
            "schema": schema,
            "tools": tools,
            "topics": topics,
            "tests": tests,
            "metadata": metadata,
            "execution_context": execution_context,
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
            metadata={"skill_id": skill_id, "skill_snapshot_id": snapshot_id, **self._scope_metadata(scope), **metadata},
        )
        self.db.execute(
            """
            INSERT INTO skill_snapshots(id, skill_id, prompt_template, workflow, schema_json, object_id, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                skill_id,
                prompt_template,
                workflow_text,
                json_dumps(schema or {}),
                object_row["id"],
                json_dumps(metadata or {}),
                now,
                now,
            ),
        )
        for tool_name in tools:
            self.db.execute(
                """
                INSERT INTO skill_bindings(id, skill_snapshot_id, tool_name, binding_type, config, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (make_id("bind"), snapshot_id, tool_name, "tool", json_dumps({}), now),
            )
        for test_case in tests:
            self.db.execute(
                """
                INSERT INTO skill_tests(id, skill_snapshot_id, input_payload, expected_output, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    make_id("stest"),
                    snapshot_id,
                    json_dumps(test_case.get("input", {})),
                    json_dumps(test_case.get("expected")),
                    json_dumps(test_case.get("metadata", {})),
                    now,
                ),
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
                "skill_snapshot_id": snapshot_id,
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
                INSERT INTO skill_files(id, skill_id, skill_snapshot_id, object_id, relative_path, role, mime_type, size_bytes, checksum, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_id,
                    skill_id,
                    snapshot_id,
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
                    INSERT INTO skill_reference_chunks(id, skill_id, skill_snapshot_id, file_id, object_id, relative_path, chunk_index, title, content, metadata, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chunk_id,
                        skill_id,
                        snapshot_id,
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
                        "skill_snapshot_id": snapshot_id,
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
                "record_id": snapshot_id,
                "skill_id": skill_id,
                "skill_snapshot_id": snapshot_id,
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
                        execution_context.get("text", ""),
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
        self.db.execute("UPDATE skills SET current_snapshot_id = ?, updated_at = ? WHERE id = ?", (snapshot_id, now, skill_id))
        return {"id": snapshot_id}

    def get_skill(self, skill_id: str) -> dict[str, Any] | None:
        skill = _deserialize_row(self.db.fetch_one("SELECT * FROM skills WHERE id = ?", (skill_id,)))
        if skill is None:
            return None
        snapshot_id = skill.get("current_snapshot_id")
        current_snapshot = None
        if snapshot_id:
            current_snapshot = _deserialize_row(
                self.db.fetch_one("SELECT * FROM skill_snapshots WHERE id = ?", (snapshot_id,)),
                ("metadata", "schema_json"),
            )
        skill["current_snapshot"] = None
        skill["bindings"] = []
        skill["tests"] = []
        skill["files"] = []
        skill["references"] = []
        skill["scripts"] = []
        skill["assets"] = []
        skill["execution_context"] = {}
        skill["execution_context_text"] = ""
        skill["skill_markdown"] = None
        if current_snapshot is not None:
            payload = self._load_skill_snapshot_asset(current_snapshot)
            current_snapshot["files"] = self._skill_snapshot_files(current_snapshot["id"])
            current_snapshot["payload"] = payload
            current_snapshot["tools"] = payload.get("tools", [])
            current_snapshot["topics"] = payload.get("topics", [])
            current_snapshot["tests_payload"] = payload.get("tests", [])
            current_snapshot["execution_context"] = dict(payload.get("execution_context") or {})
            current_snapshot["execution_context_text"] = str(current_snapshot["execution_context"].get("text") or "")
            current_snapshot["skill_markdown"] = next(
                (item.get("content") for item in current_snapshot["files"] if item.get("role") == "skill_md"),
                None,
            )
            current_snapshot["references"] = [item for item in current_snapshot["files"] if item.get("role") == "reference"]
            current_snapshot["scripts"] = [item for item in current_snapshot["files"] if item.get("role") == "script"]
            current_snapshot["assets"] = [item for item in current_snapshot["files"] if item.get("role") == "asset"]
            skill["bindings"] = _deserialize_rows(
                self.db.fetch_all(
                    "SELECT * FROM skill_bindings WHERE skill_snapshot_id = ? ORDER BY created_at ASC",
                    (current_snapshot["id"],),
                ),
                ("config",),
            )
            skill["tests"] = _deserialize_rows(
                self.db.fetch_all(
                    "SELECT * FROM skill_tests WHERE skill_snapshot_id = ? ORDER BY created_at ASC",
                    (current_snapshot["id"],),
                ),
                ("input_payload", "expected_output", "metadata"),
            )
            skill["current_snapshot"] = current_snapshot
            skill["files"] = current_snapshot.get("files", [])
            skill["skill_markdown"] = current_snapshot.get("skill_markdown")
            skill["references"] = current_snapshot.get("references", [])
            skill["scripts"] = current_snapshot.get("scripts", [])
            skill["assets"] = current_snapshot.get("assets", [])
            skill["execution_context"] = current_snapshot.get("execution_context", {})
            skill["execution_context_text"] = current_snapshot.get("execution_context_text", "")
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
        for snapshot_id, score in self._skill_reference_snapshot_hit_map(query, limit=max(limit * 4, 24)).items():
            vector_hits[snapshot_id] = max(vector_hits.get(snapshot_id, 0.0), score)
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
                sic.embedding
            FROM skill_reference_index sri
            JOIN skills s ON s.id = sri.skill_id
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
        requester_scope = self._pop_request_access_scope(kwargs)
        memory = self.get(memory_id)
        if memory is None:
            raise ValueError(f"Memory `{memory_id}` does not exist.")
        self._assert_resource_permission(
            memory,
            resource_type="memory",
            requester_scope=requester_scope,
            permission="write",
            action_label="archive memory",
        )
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
        requester_scope = self._pop_request_access_scope(kwargs)
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        self._assert_resource_permission(
            session,
            resource_type="session",
            requester_scope=requester_scope,
            permission="write",
            action_label="archive session",
        )
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
        turn_vector_hits = self._vector_hit_map("interaction_turn", query, limit=max(limit * 6, 24))
        snapshot_vector_hits = self._vector_hit_map("interaction_snapshot", query, limit=max(limit * 6, 24))
        vector_hits = self._merge_hit_maps(turn_vector_hits, snapshot_vector_hits)
        rows = self.db.fetch_all(
            f"""
            SELECT ct.id, ct.session_id, ct.role, ct.turn_type, ct.speaker_participant_id, ct.target_participant_id, ct.salience_score, s.owner_agent_id, s.subject_type, s.subject_id, s.interaction_type, ct.content AS text, ct.metadata, ct.created_at AS updated_at, sic.embedding
            FROM conversation_turns ct
            JOIN sessions s ON s.id = ct.session_id
            LEFT JOIN semantic_index_cache sic ON sic.record_id = ct.id AND sic.collection = 'interaction_turn'
            WHERE {' AND '.join(sql_filters)}
            ORDER BY ct.created_at DESC
            LIMIT ?
            """,
            tuple(params + [max(limit * 12, self.config.memory_policy.search_scan_limit // 2)]),
        )
        turn_ids = list(
            dict.fromkeys(
                [
                    row["record_id"]
                    for row in fts_rows
                    if row.get("collection") == "interaction_turn"
                ]
                + list(turn_vector_hits.keys())
            )
        )
        if turn_ids:
            placeholders = ", ".join("?" for _ in turn_ids)
            rows = self._merge_rows_by_id(
                rows,
                self.db.fetch_all(
                    f"""
                    SELECT ct.id, ct.session_id, ct.role, ct.turn_type, ct.speaker_participant_id, ct.target_participant_id, ct.salience_score, s.owner_agent_id, s.subject_type, s.subject_id, s.interaction_type, ct.content AS text, ct.metadata, ct.created_at AS updated_at, sic.embedding
                    FROM conversation_turns ct
                    JOIN sessions s ON s.id = ct.session_id
                    LEFT JOIN semantic_index_cache sic ON sic.record_id = ct.id AND sic.collection = 'interaction_turn'
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
            vector_hits=vector_hits,
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
            SELECT wms.id, wms.session_id, wms.owner_agent_id, wms.subject_type, wms.subject_id, wms.interaction_type, wms.summary AS text, wms.metadata, wms.updated_at, sic.embedding
            FROM working_memory_snapshots wms
            JOIN sessions s ON s.id = wms.session_id
            LEFT JOIN semantic_index_cache sic ON sic.record_id = wms.id AND sic.collection = 'interaction_snapshot'
            WHERE {' AND '.join(snapshot_filters)}
            ORDER BY wms.updated_at DESC
            LIMIT ?
            """,
            tuple(snapshot_params + [max(limit * 6, 12)]),
        )
        snapshot_ids = list(
            dict.fromkeys(
                [
                    row["record_id"]
                    for row in fts_rows
                    if row.get("collection") == "interaction_snapshot"
                ]
                + list(snapshot_vector_hits.keys())
            )
        )
        if snapshot_ids:
            placeholders = ", ".join("?" for _ in snapshot_ids)
            snapshot_rows = self._merge_rows_by_id(
                snapshot_rows,
                self.db.fetch_all(
                    f"""
                    SELECT wms.id, wms.session_id, wms.owner_agent_id, wms.subject_type, wms.subject_id, wms.interaction_type, wms.summary AS text, wms.metadata, wms.updated_at, sic.embedding
                    FROM working_memory_snapshots wms
                    JOIN sessions s ON s.id = wms.session_id
                    LEFT JOIN semantic_index_cache sic ON sic.record_id = wms.id AND sic.collection = 'interaction_snapshot'
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
                vector_hits=vector_hits,
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
        run_vector_hits = self._vector_hit_map("execution_run", query, limit=max(limit * 6, 24))
        observation_vector_hits = self._vector_hit_map("execution_observation", query, limit=max(limit * 6, 24))
        vector_hits = self._merge_hit_maps(run_vector_hits, observation_vector_hits)
        runs = self.db.fetch_all(
            f"""
            SELECT r.*, sic.embedding
            FROM runs r
            LEFT JOIN semantic_index_cache sic ON sic.record_id = r.id AND sic.collection = 'execution_run'
            WHERE {' AND '.join(sql_filters)}
            ORDER BY r.updated_at DESC
            LIMIT 80
            """,
            tuple(params),
        )
        run_ids = list(
            dict.fromkeys(
                [
                    row["record_id"]
                    for row in fts_rows
                    if row.get("collection") == "execution_run"
                ]
                + list(run_vector_hits.keys())
            )
        )
        if run_ids:
            placeholders = ", ".join("?" for _ in run_ids)
            runs = self._merge_rows_by_id(
                runs,
                self.db.fetch_all(
                    f"""
                    SELECT r.*, sic.embedding
                    FROM runs r
                    LEFT JOIN semantic_index_cache sic ON sic.record_id = r.id AND sic.collection = 'execution_run'
                    WHERE {' AND '.join(sql_filters)} AND r.id IN ({placeholders})
                    """,
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
            observations = self.db.fetch_all(
                """
                SELECT o.*, sic.embedding
                FROM observations o
                LEFT JOIN semantic_index_cache sic ON sic.record_id = o.id AND sic.collection = 'execution_observation'
                WHERE o.run_id = ?
                ORDER BY o.created_at DESC
                LIMIT 12
                """,
                (run["id"],),
            )
            for observation in observations:
                obs_item = _deserialize_row(observation) or dict(observation)
                obs_item["text"] = str(obs_item.get("content") or "")
                obs_item["updated_at"] = obs_item.get("created_at")
                obs_item.setdefault("owner_agent_id", item.get("owner_agent_id"))
                obs_item.setdefault("agent_id", item.get("agent_id"))
                obs_item.setdefault("user_id", item.get("user_id"))
                obs_item.setdefault("subject_type", item.get("subject_type"))
                obs_item.setdefault("subject_id", item.get("subject_id"))
                obs_item.setdefault("session_id", item.get("session_id"))
                prepared.append(obs_item)
                prepared_ids.add(str(obs_item.get("id") or ""))
        observation_ids = list(
            dict.fromkeys(
                [
                    row["record_id"]
                    for row in fts_rows
                    if row.get("collection") == "execution_observation"
                ]
                + list(observation_vector_hits.keys())
            )
        )
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
                SELECT o.*, r.user_id, r.owner_agent_id, r.agent_id, r.namespace_key, sic.embedding
                FROM observations o
                JOIN runs r ON r.id = o.run_id
                LEFT JOIN semantic_index_cache sic ON sic.record_id = o.id AND sic.collection = 'execution_observation'
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
            vector_hits=vector_hits,
            fts_hits=fts_hits,
            affinity={"owner_agent_id": owner_agent_id},
        )
        return {"results": ranked[:limit]}

    def promote_session_memories(self, session_id: str, **kwargs) -> dict[str, Any]:
        requester_scope = self._pop_request_access_scope(kwargs)
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        self._assert_resource_permission(
            session,
            resource_type="session",
            requester_scope=requester_scope,
            permission="write",
            action_label="promote session memories",
        )
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
        requester_scope = self._pop_request_access_scope(kwargs)
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        self._assert_resource_permission(
            session,
            resource_type="session",
            requester_scope=requester_scope,
            permission="write",
            action_label="compress session context",
        )
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
        requester_scope = self._pop_request_access_scope(kwargs)
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        self._assert_resource_permission(
            session,
            resource_type="session",
            requester_scope=requester_scope,
            permission="manage",
            action_label="prune session snapshots",
        )
        keep_recent = int(kwargs.pop("keep_recent", self.config.memory_policy.snapshot_keep_recent))
        snapshots = self.db.fetch_all("SELECT id FROM working_memory_snapshots WHERE session_id = ? ORDER BY updated_at DESC", (session_id,))
        removed = 0
        for item in snapshots[keep_recent:]:
            self.db.execute("DELETE FROM working_memory_snapshots WHERE id = ?", (item["id"],))
            self._delete_auxiliary_index_record(item["id"], collection="interaction_snapshot")
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
        requester_scope = self._pop_request_access_scope(kwargs)
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        self._assert_resource_permission(
            session,
            resource_type="session",
            requester_scope=requester_scope,
            permission="manage",
            action_label="govern session",
        )
        scoped_kwargs = {key: value for key, value in requester_scope.items() if value is not None}
        return {
            "session": session,
            "health": self.session_health(session_id),
            "compression": self.compress_session_context(session_id, **scoped_kwargs, **kwargs),
            "promotion": self.promote_session_memories(session_id, **scoped_kwargs, **kwargs),
            "snapshot_prune": self.prune_session_snapshots(session_id, **scoped_kwargs, **kwargs),
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
                SELECT s.id
                FROM skills s
                WHERE s.status = 'active'
                ORDER BY s.updated_at DESC
                LIMIT ?
                """,
                (limit or self.config.memory_policy.search_scan_limit,),
            )
        )
        for row in skill_rows:
            skill = self.get_skill(str(row["id"]))
            if skill is None or not skill.get("current_snapshot"):
                continue
            snapshot = skill["current_snapshot"]
            combined = "\n".join(
                part
                for part in [
                    skill["name"],
                    skill["description"],
                    skill.get("skill_markdown") or "",
                    snapshot.get("prompt_template") or "",
                    snapshot.get("workflow") or "",
                    skill.get("execution_context_text") or "",
                    " ".join(snapshot.get("topics", [])),
                    " ".join(snapshot.get("tools", [])),
                ]
                if part
            )
            self._index_skill(
                {
                    "record_id": snapshot["id"],
                    "skill_id": skill["id"],
                    "skill_snapshot_id": snapshot["id"],
                    "owner_agent_id": skill.get("owner_agent_id"),
                    "source_subject_type": skill.get("source_subject_type"),
                    "source_subject_id": skill.get("source_subject_id"),
                    "namespace_key": skill.get("namespace_key"),
                    "name": skill["name"],
                    "description": skill["description"],
                    "metadata": skill.get("metadata", {}),
                    "updated_at": skill["updated_at"],
                    "text": combined,
                    "tools": list(snapshot.get("tools", [])),
                    "topics": list(snapshot.get("topics", [])),
                }
            )
            projected["skill"] += 1
        reference_rows = _deserialize_rows(
            self.db.fetch_all(
                """
                SELECT src.id AS record_id, src.skill_id, src.skill_snapshot_id, src.file_id, src.object_id, sri.owner_agent_id, sri.source_subject_type,
                       sri.source_subject_id, sri.namespace_key, src.relative_path, src.title, src.content AS text, src.metadata, ss.updated_at AS updated_at
                FROM skill_reference_chunks src
                JOIN skill_snapshots ss ON ss.id = src.skill_snapshot_id
                LEFT JOIN skill_reference_index sri ON sri.record_id = src.id
                ORDER BY ss.updated_at DESC
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
            "platform": capability_dict(
                category="platform",
                provider="plugin-registry" if self.platform_llm is not None else "local-only",
                features={
                    "platform_event_adapter": self.platform_events is not None,
                    "platform_llm_plugin_registry": True,
                    "platform_llm_compression": True,
                    "mcp_required_for_platform_llm": False,
                },
                items={
                    "registered_platform_llm_plugins": list_platform_llm_plugins(),
                    "configured_platform_llm_plugin": (
                        asdict(self.config.platform_llm_plugin) if self.config.platform_llm_plugin is not None else None
                    ),
                    "active_platform_llm_provider": getattr(self.platform_llm, "provider", None),
                    "active_platform_llm_model": getattr(self.platform_llm, "model", None),
                    "platform_event_adapter": type(self.platform_events).__name__ if self.platform_events is not None else None,
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

    @property
    def events(self):
        return self.platform_events

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

    def refresh_skill_execution_context(
        self,
        skill_id: str,
        *,
        path_prefix: str | None = None,
        budget_chars: int | None = None,
        max_sentences: int = 8,
        max_highlights: int = 12,
        **kwargs: Any,
    ) -> dict[str, Any]:
        requester_scope = self._pop_request_access_scope(kwargs)
        skill = self.get_skill(skill_id)
        if skill is None:
            raise ValueError(f"Skill `{skill_id}` does not exist.")
        self._assert_resource_permission(
            {
                **skill,
                "resource_scope": "skill",
                "subject_type": skill.get("source_subject_type") or skill.get("subject_type"),
                "subject_id": skill.get("source_subject_id") or skill.get("subject_id"),
            },
            resource_type="skill",
            requester_scope=requester_scope,
            permission="write",
            action_label="refresh skill execution context",
        )
        current_snapshot = skill.get("current_snapshot")
        if current_snapshot is None:
            raise ValueError(f"Skill `{skill_id}` does not have a stored snapshot.")

        reference_records: list[dict[str, Any]] = []
        for reference in current_snapshot.get("references", []):
            relative_path = str(reference.get("relative_path") or "")
            if path_prefix and not relative_path.startswith(path_prefix):
                continue
            text = str(reference.get("content") or "")
            if not text.strip():
                continue
            reference_records.append(
                {
                    "id": relative_path,
                    "text": text,
                    "score": 0.76,
                    "metadata": {
                        "skill_id": skill_id,
                        "skill_snapshot_id": current_snapshot.get("id"),
                        "relative_path": relative_path,
                    },
                }
            )
        if not reference_records:
            raise ValueError(f"Skill `{skill_id}` does not have matching reference files.")
        execution_context = self._build_skill_execution_context(
            reference_records,
            budget_chars=budget_chars,
            max_sentences=max_sentences,
            max_highlights=max_highlights,
        )
        self._persist_skill_execution_context(current_snapshot["id"], execution_context)
        refreshed = self.get_skill(skill_id)
        assert refreshed is not None
        refreshed_snapshot = refreshed.get("current_snapshot") or {}
        self._index_skill(
            {
                "record_id": refreshed_snapshot.get("id"),
                "skill_id": skill_id,
                "skill_snapshot_id": refreshed_snapshot.get("id"),
                "name": refreshed["name"],
                "description": refreshed["description"],
                "text": "\n".join(
                    part
                    for part in [
                        refreshed["name"],
                        refreshed["description"],
                        refreshed.get("skill_markdown") or "",
                        refreshed_snapshot.get("prompt_template") or "",
                        refreshed_snapshot.get("workflow") or "",
                        refreshed.get("execution_context_text") or "",
                        " ".join(refreshed_snapshot.get("topics", [])),
                        " ".join(refreshed_snapshot.get("tools", [])),
                    ]
                    if part
                ),
                "tools": list(refreshed_snapshot.get("tools", [])),
                "topics": list(refreshed_snapshot.get("topics", [])),
                "owner_agent_id": refreshed.get("owner_agent_id") or refreshed.get("owner_id"),
                "source_subject_type": refreshed.get("source_subject_type"),
                "source_subject_id": refreshed.get("source_subject_id"),
                "namespace_key": refreshed.get("namespace_key"),
                "metadata": refreshed.get("metadata", {}),
                "updated_at": utcnow_iso(),
            }
        )
        return {
            "skill_id": skill_id,
            "skill_snapshot_id": refreshed_snapshot.get("id"),
            "name": refreshed.get("name"),
            "execution_context": refreshed.get("execution_context", {}),
            "persisted": True,
        }

    def compress_skill_references(
        self,
        skill_id: str,
        *,
        path_prefix: str | None = None,
        query: str | None = None,
        budget_chars: int | None = None,
        max_sentences: int = 8,
        max_highlights: int = 12,
    ) -> dict[str, Any]:
        skill = self.get_skill(skill_id)
        if skill is None:
            raise ValueError(f"Skill `{skill_id}` does not exist.")
        current_snapshot = skill.get("current_snapshot")
        if not current_snapshot:
            raise ValueError(f"Skill `{skill_id}` does not have a stored snapshot.")

        reference_records: list[dict[str, Any]] = []
        for reference in current_snapshot.get("references", []):
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
                        "skill_snapshot_id": current_snapshot.get("id"),
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
        compression["text"] = _format_skill_execution_context(compression)
        return {
            "skill_id": skill_id,
            "skill_snapshot_id": current_snapshot.get("id"),
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
                "supporting_passages": [],
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
                    "supporting_passages": list(external.get("supporting_passages") or []),
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

    def _request_access_scope(
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
    ) -> dict[str, Any]:
        explicit_values = (
            user_id,
            agent_id,
            owner_agent_id,
            subject_type,
            subject_id,
            interaction_type,
            platform_id,
            workspace_id,
            team_id,
            project_id,
            namespace_key,
        )
        if not any(value is not None for value in explicit_values):
            return CollaborationScope().as_dict(include_none=True)
        scope = CollaborationScope.from_value(
            {
                "user_id": user_id,
                "agent_id": agent_id,
                "owner_agent_id": owner_agent_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "interaction_type": interaction_type,
                "platform_id": platform_id or self.config.platform_id,
                "workspace_id": workspace_id or self.config.workspace_id,
                "team_id": team_id or self.config.team_id,
                "project_id": project_id or self.config.project_id,
                "namespace_key": namespace_key,
            }
        )
        payload = scope.as_dict(include_none=True)
        payload["namespace_key"] = scope.resolved_namespace_key()
        return payload

    def _request_access_scope_for_scope(
        self,
        scope: dict[str, Any] | None,
        *,
        actor_user_id: str | None = None,
        actor_agent_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_scope = dict(scope or {})
        if actor_user_id is None and actor_agent_id is None:
            return self._request_access_scope()
        resolved_actor_agent_id = actor_agent_id or resolved_scope.get("owner_agent_id") or resolved_scope.get("agent_id")
        return self._request_access_scope(
            user_id=actor_user_id if actor_user_id is not None else resolved_scope.get("user_id"),
            agent_id=resolved_actor_agent_id,
            owner_agent_id=resolved_actor_agent_id,
            subject_type=resolved_scope.get("subject_type"),
            subject_id=resolved_scope.get("subject_id"),
            interaction_type=resolved_scope.get("interaction_type"),
            platform_id=resolved_scope.get("platform_id"),
            workspace_id=resolved_scope.get("workspace_id"),
            team_id=resolved_scope.get("team_id"),
            project_id=resolved_scope.get("project_id"),
            namespace_key=resolved_scope.get("namespace_key"),
        )

    def _resource_metadata(self, row: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict((row or {}).get("metadata") or {})
        for key in (
            "user_id",
            "agent_id",
            "owner_agent_id",
            "source_agent_id",
            "target_agent_id",
            "subject_type",
            "subject_id",
            "interaction_type",
            "platform_id",
            "workspace_id",
            "team_id",
            "project_id",
            "namespace_key",
            "visibility",
            "scope",
            "artifact_type",
            "reflection_type",
            "memory_type",
        ):
            value = (row or {}).get(key)
            if value is not None and payload.get(key) is None:
                payload[key] = value
        return payload

    def _resource_scope_value(self, resource_type: str, row: dict[str, Any]) -> str:
        if resource_type == "memory":
            return str(row.get("scope") or row.get("memory_scope") or "all")
        if resource_type == "context":
            return str(row.get("artifact_type") or "artifact")
        if resource_type == "handoff":
            return "handoff"
        if resource_type == "reflection":
            return str(row.get("reflection_type") or "reflection")
        if resource_type == "archive":
            return str(row.get("domain") or row.get("resource_scope") or "archive")
        if resource_type == "knowledge":
            return str(row.get("resource_scope") or "document")
        if resource_type == "skill":
            return str(row.get("resource_scope") or "skill")
        if resource_type == "session":
            return str(row.get("resource_scope") or "session")
        return str(row.get("resource_scope") or "namespace")

    def _acl_principals(self, requester_scope: dict[str, Any]) -> list[tuple[str, str]]:
        principals: list[tuple[str, str]] = []
        for principal_type, principal_id in (
            ("agent", requester_scope.get("owner_agent_id")),
            ("agent", requester_scope.get("agent_id")),
            ("user", requester_scope.get("user_id")),
            ("workspace", requester_scope.get("workspace_id")),
            ("team", requester_scope.get("team_id")),
            ("project", requester_scope.get("project_id")),
            ("namespace", requester_scope.get("namespace_key")),
        ):
            if principal_id:
                principals.append((principal_type, str(principal_id)))
        subject_type = requester_scope.get("subject_type")
        subject_id = requester_scope.get("subject_id")
        if subject_id:
            principals.append(("subject", str(subject_id)))
            if subject_type:
                principals.append((f"subject:{subject_type}", str(subject_id)))
        deduped: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for item in principals:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    def _acl_permission_candidates(self, permission: str) -> list[str]:
        normalized = str(permission or "read").strip().lower()
        if normalized == "write":
            return ["write", "read_write", "manage", "admin", "*"]
        return ["read", "read_write", "manage", "admin", "*"]

    def _acl_resource_scope_candidates(self, resource_scope: str) -> list[str]:
        normalized = str(resource_scope or "namespace").strip() or "namespace"
        candidates = [normalized]
        for candidate in (normalized.replace("-", "_"), normalized.replace("_", "-")):
            if candidate and candidate not in candidates:
                candidates.append(candidate)
        while len(candidates) < 3:
            candidates.append(candidates[-1])
        return candidates[:3]

    def _acl_rule_allows(
        self,
        *,
        namespace_key: str | None,
        resource_type: str,
        resource_scope: str,
        requester_scope: dict[str, Any],
        permission: str = "read",
    ) -> bool:
        if not namespace_key:
            return False
        for principal_type, principal_id in self._acl_principals(requester_scope):
            scope_candidates = self._acl_resource_scope_candidates(resource_scope)
            allowed = self.db.fetch_one(
                """
                SELECT id
                FROM scope_acl_rules
                WHERE namespace_key = ?
                  AND principal_type = ?
                  AND principal_id = ?
                  AND resource_type IN (?, '*', 'all')
                  AND resource_scope IN (?, ?, ?, 'namespace', '*', 'all')
                  AND permission IN (?, ?, ?, ?, ?)
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (
                    namespace_key,
                    principal_type,
                    principal_id,
                    resource_type,
                    *scope_candidates,
                    *self._acl_permission_candidates(permission),
                ),
            )
            if allowed is not None:
                return True
        return False

    def _default_visibility(self, resource_type: str, row: dict[str, Any]) -> str:
        if resource_type == "handoff":
            return str(row.get("visibility") or "target_agent")
        if resource_type == "context":
            return "target_agent" if row.get("target_agent_id") else "private_agent"
        if resource_type == "reflection":
            return "private_agent"
        return str(row.get("visibility") or "private_agent")

    def _is_resource_visible(
        self,
        row: dict[str, Any],
        *,
        resource_type: str,
        requester_scope: dict[str, Any] | None,
        permission: str = "read",
    ) -> bool:
        if requester_scope is None or not any(value is not None for value in requester_scope.values()):
            return True
        metadata = self._resource_metadata(row)
        namespace_key = str(row.get("namespace_key") or metadata.get("namespace_key") or "").strip() or None
        resource_scope = self._resource_scope_value(resource_type, row)
        if self._acl_rule_allows(
            namespace_key=namespace_key,
            resource_type=resource_type,
            resource_scope=resource_scope,
            requester_scope=requester_scope,
            permission=permission,
        ):
            return True

        visibility = str(
            row.get("visibility")
            or metadata.get("visibility")
            or self._default_visibility(resource_type, row)
        ).strip().lower()
        requester_agents = {
            str(value)
            for value in [requester_scope.get("owner_agent_id"), requester_scope.get("agent_id")]
            if value
        }
        requester_user = str(requester_scope.get("user_id") or "").strip()
        requester_subject_id = str(requester_scope.get("subject_id") or "").strip()
        requester_subject_type = str(requester_scope.get("subject_type") or "").strip()

        row_owner_agent = str(
            row.get("owner_agent_id")
            or row.get("source_agent_id")
            or metadata.get("owner_agent_id")
            or metadata.get("source_agent_id")
            or row.get("agent_id")
            or metadata.get("agent_id")
            or ""
        ).strip()
        row_target_agent = str(row.get("target_agent_id") or metadata.get("target_agent_id") or "").strip()
        row_user = str(row.get("user_id") or metadata.get("user_id") or "").strip()
        row_subject_id = str(row.get("subject_id") or metadata.get("subject_id") or "").strip()
        row_subject_type = str(row.get("subject_type") or metadata.get("subject_type") or "").strip()

        if visibility in {"public", "global"}:
            return True
        if visibility in {"workspace", "shared_workspace"}:
            return bool(
                requester_scope.get("workspace_id")
                and metadata.get("workspace_id")
                and str(requester_scope.get("workspace_id")) == str(metadata.get("workspace_id"))
            )
        if visibility in {"team", "shared_team"}:
            return bool(
                requester_scope.get("team_id")
                and metadata.get("team_id")
                and str(requester_scope.get("team_id")) == str(metadata.get("team_id"))
            )
        if visibility in {"project", "shared_project"}:
            return bool(
                requester_scope.get("project_id")
                and metadata.get("project_id")
                and str(requester_scope.get("project_id")) == str(metadata.get("project_id"))
            )
        if visibility in {"private_user", "user_private", "user"}:
            return bool(requester_user and row_user and requester_user == row_user)
        if visibility in {"subject", "subject_private", "private_subject"}:
            return bool(
                requester_subject_id
                and row_subject_id
                and requester_subject_id == row_subject_id
                and (not requester_subject_type or not row_subject_type or requester_subject_type == row_subject_type)
            )
        if visibility in {"target_agent"}:
            if requester_agents & {row_owner_agent, row_target_agent}:
                return True
            return False
        if visibility in {"shared_agents", "agent_shared"}:
            if requester_agents & {row_owner_agent, row_target_agent}:
                return True
            if namespace_key and requester_scope.get("namespace_key") and str(requester_scope.get("namespace_key")) == namespace_key:
                return True
            return False
        if requester_agents & {row_owner_agent}:
            return True
        if not row_owner_agent and requester_user and row_user and requester_user == row_user:
            return True
        return False

    def _request_scope_active(self, requester_scope: dict[str, Any] | None) -> bool:
        return bool(requester_scope and any(value is not None for value in requester_scope.values()))

    def _resource_owner_matches(self, row: dict[str, Any], requester_scope: dict[str, Any] | None) -> bool:
        if not self._request_scope_active(requester_scope):
            return True
        metadata = self._resource_metadata(row)
        requester_agents = {
            str(value)
            for value in [requester_scope.get("owner_agent_id"), requester_scope.get("agent_id")]
            if value
        }
        requester_user = str(requester_scope.get("user_id") or "").strip()
        row_owner_agent = str(
            row.get("owner_agent_id")
            or row.get("source_agent_id")
            or metadata.get("owner_agent_id")
            or metadata.get("source_agent_id")
            or row.get("agent_id")
            or metadata.get("agent_id")
            or ""
        ).strip()
        row_user = str(row.get("user_id") or metadata.get("user_id") or "").strip()
        if requester_agents and row_owner_agent and requester_agents & {row_owner_agent}:
            return True
        if not row_owner_agent and requester_user and row_user and requester_user == row_user:
            return True
        return False

    def _requester_namespace_matches(self, requester_scope: dict[str, Any] | None, namespace_key: str | None) -> bool:
        if not self._request_scope_active(requester_scope):
            return True
        requester_namespace = str((requester_scope or {}).get("namespace_key") or "").strip()
        resolved_namespace = str(namespace_key or "").strip()
        return bool(requester_namespace and resolved_namespace and requester_namespace == resolved_namespace)

    def _has_namespace_permission(
        self,
        *,
        namespace_key: str | None,
        resource_type: str,
        resource_scope: str,
        requester_scope: dict[str, Any] | None,
        permission: str = "write",
    ) -> bool:
        if not self._request_scope_active(requester_scope):
            return True
        resolved_namespace = str(namespace_key or "").strip() or None
        if self._acl_rule_allows(
            namespace_key=resolved_namespace,
            resource_type=resource_type,
            resource_scope=resource_scope,
            requester_scope=requester_scope or {},
            permission=permission,
        ):
            return True
        if permission in {"manage", "admin"}:
            return bool(
                self._requester_namespace_matches(requester_scope, resolved_namespace)
                and ((requester_scope or {}).get("owner_agent_id") or (requester_scope or {}).get("user_id"))
            )
        return bool(
            self._requester_namespace_matches(requester_scope, resolved_namespace)
            and (
                (requester_scope or {}).get("owner_agent_id")
                or (requester_scope or {}).get("agent_id")
                or (requester_scope or {}).get("user_id")
            )
        )

    def _assert_namespace_permission(
        self,
        *,
        namespace_key: str | None,
        resource_type: str,
        resource_scope: str,
        requester_scope: dict[str, Any] | None,
        permission: str = "write",
        action_label: str | None = None,
    ) -> None:
        if self._has_namespace_permission(
            namespace_key=namespace_key,
            resource_type=resource_type,
            resource_scope=resource_scope,
            requester_scope=requester_scope,
            permission=permission,
        ):
            return
        label = action_label or f"{permission} {resource_type}"
        raise PermissionError(
            f"Requester does not have `{permission}` access for `{label}` in namespace `{namespace_key or 'default'}`."
        )

    def _has_resource_permission(
        self,
        row: dict[str, Any],
        *,
        resource_type: str,
        requester_scope: dict[str, Any] | None,
        permission: str = "write",
    ) -> bool:
        if not self._request_scope_active(requester_scope):
            return True
        if permission == "read":
            return self._is_resource_visible(row, resource_type=resource_type, requester_scope=requester_scope, permission=permission)
        if self._resource_owner_matches(row, requester_scope):
            return True
        metadata = self._resource_metadata(row)
        namespace_key = str(row.get("namespace_key") or metadata.get("namespace_key") or "").strip() or None
        resource_scope = self._resource_scope_value(resource_type, row)
        if self._acl_rule_allows(
            namespace_key=namespace_key,
            resource_type=resource_type,
            resource_scope=resource_scope,
            requester_scope=requester_scope or {},
            permission=permission,
        ):
            return True
        return False

    def _assert_resource_permission(
        self,
        row: dict[str, Any],
        *,
        resource_type: str,
        requester_scope: dict[str, Any] | None,
        permission: str = "write",
        action_label: str | None = None,
    ) -> None:
        if self._has_resource_permission(
            row,
            resource_type=resource_type,
            requester_scope=requester_scope,
            permission=permission,
        ):
            return
        label = action_label or f"{permission} {resource_type}"
        raise PermissionError(
            f"Requester does not have `{permission}` access for `{label}` on `{row.get('id') or row.get('record_id') or 'resource'}`."
        )

    def _pop_request_access_scope(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        agent_id = kwargs.pop("agent_id", None)
        owner_agent_id = kwargs.pop("owner_agent_id", None)
        actor_agent_id = agent_id or owner_agent_id
        return self._request_access_scope(
            user_id=kwargs.pop("user_id", None),
            agent_id=actor_agent_id,
            owner_agent_id=actor_agent_id,
            subject_type=kwargs.pop("subject_type", kwargs.pop("source_subject_type", None)),
            subject_id=kwargs.pop("subject_id", kwargs.pop("source_subject_id", None)),
            interaction_type=kwargs.pop("interaction_type", None),
            platform_id=kwargs.pop("platform_id", None),
            workspace_id=kwargs.pop("workspace_id", None),
            team_id=kwargs.pop("team_id", None),
            project_id=kwargs.pop("project_id", None),
            namespace_key=kwargs.pop("namespace_key", None),
        )

    def _filter_accessible_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        resource_type: str,
        requester_scope: dict[str, Any] | None,
        permission: str = "read",
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in rows
            if self._is_resource_visible(row, resource_type=resource_type, requester_scope=requester_scope, permission=permission)
        ]

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
                    "tables": ["skills", "skill_snapshots", "skill_files", "skill_reference_chunks", "skill_index", "skill_reference_index"],
                    "object_prefix": self._object_store_prefix(scope, "skill"),
                    "vector_path": competency_vector_path,
                    "strategy": "snapshot-based procedural memory + semantic search",
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
        confidence: float = 0.5,
        tier: str | None = None,
        summary_l0: str | None = None,
        summary_l1: str | None = None,
        long_term: bool = True,
        source: str = "manual",
        version: int = 1,
        supersedes_memory_id: str | None = None,
        superseded_by_memory_id: str | None = None,
        skip_existing_lookup: bool = False,
        event_type: str = "ADD",
        event_payload: dict[str, Any] | None = None,
        reason_code: str | None = None,
    ) -> dict[str, Any]:
        cleaned = text.strip()
        if not cleaned:
            raise ValueError("text must not be empty")
        scope = str(MemoryScope.LONG_TERM if long_term else MemoryScope.SESSION)
        actor_agent_id = agent_id or owner_agent_id
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
        requester_scope = self._request_access_scope_for_scope(
            resolved_scope,
            actor_user_id=user_id,
            actor_agent_id=actor_agent_id,
        )
        self._assert_namespace_permission(
            namespace_key=resolved_scope.get("namespace_key"),
            resource_type="memory",
            resource_scope=scope,
            requester_scope=requester_scope,
            permission="write",
            action_label="create memory",
        )
        user_id = resolved_scope["user_id"] or self.config.default_user_id
        owner_agent_id = resolved_scope["owner_agent_id"]
        subject_type = resolved_scope["subject_type"]
        subject_id = resolved_scope["subject_id"]
        interaction_type = resolved_scope["interaction_type"]
        namespace_key = resolved_scope.get("namespace_key")
        metadata = merge_metadata(metadata or {}, self._scope_metadata(resolved_scope))
        structured = self._memory_structured_fields(
            metadata,
            confidence=confidence,
            tier=tier,
            summary_l0=summary_l0,
            summary_l1=summary_l1,
        )
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
        if source != "auto_compression" and not skip_existing_lookup:
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
                    }
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
            merged_payload = self._memory_text_payload(
                merged_text,
                summary_l0=structured["summary_l0"],
                summary_l1=structured["summary_l1"],
            )
            merged_summary = merged_payload["summary_l1"]
            merged_metadata = merge_metadata(existing.get("metadata"), metadata)
            new_importance = max(float(existing.get("importance", 0.5) or 0.0), float(importance))
            new_confidence = max(float(existing.get("confidence", 0.5) or 0.0), float(structured["confidence"]))
            new_tier = str(merged_metadata.get("tier") or structured["tier"] or existing.get("tier") or "working")
            existing_table = self._memory_table_for_id(existing["id"])
            assert existing_table is not None
            self.db.execute(
                f"""
                UPDATE {existing_table}
                SET text = ?, text_hash = ?, storage_policy = ?, storage_ref = ?, payload_bytes = ?, summary = ?, summary_l0 = ?, summary_l1 = ?,
                    importance = ?, confidence = ?, tier = ?, namespace_key = ?, metadata = ?, updated_at = ?, bundle_id = ?
                WHERE id = ?
                """,
                (
                    merged_payload["text"],
                    merged_payload["text_hash"],
                    merged_payload["storage_policy"],
                    merged_payload["storage_ref"],
                    merged_payload["payload_bytes"],
                    merged_summary,
                    merged_payload["summary_l0"],
                    merged_payload["summary_l1"],
                    new_importance,
                    new_confidence,
                    new_tier,
                    namespace_key or existing.get("namespace_key"),
                    json_dumps(merged_metadata),
                    now,
                    bundle["id"],
                    existing["id"],
                ),
            )
            self._upsert_bundle_item(
                existing["scope"],
                bundle["id"],
                self._bundle_memory_item_payload(
                    {
                        **existing,
                        "text": merged_payload["text"],
                        "text_hash": merged_payload["text_hash"],
                        "storage_policy": merged_payload["storage_policy"],
                        "storage_ref": merged_payload["storage_ref"],
                        "payload_bytes": merged_payload["payload_bytes"],
                        "bundle_id": bundle["id"],
                        "summary": merged_summary,
                        "summary_l0": merged_payload["summary_l0"],
                        "summary_l1": merged_payload["summary_l1"],
                        "importance": new_importance,
                        "confidence": new_confidence,
                        "tier": new_tier,
                        "metadata": merged_metadata,
                        "updated_at": now,
                    }
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
        text_payload = self._memory_text_payload(
            cleaned,
            summary_l0=structured["summary_l0"],
            summary_l1=structured["summary_l1"],
        )
        summary = text_payload["summary_l1"]
        self.db.execute(
            f"""
            INSERT INTO {table_name}(
                id, bundle_id, content_id, text, text_hash, storage_policy, storage_ref, payload_bytes,
                user_id, agent_id, owner_agent_id, subject_type, subject_id, interaction_type, namespace_key,
                session_id, run_id, source_session_id, source_run_id, memory_type, summary, summary_l0, summary_l1,
                importance, confidence, visibility, tier, version, supersedes_memory_id, superseded_by_memory_id,
                status, source, metadata, content_format, created_at, updated_at, archived_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                memory_id,
                bundle["id"],
                content_id,
                text_payload["text"],
                text_payload["text_hash"],
                text_payload["storage_policy"],
                text_payload["storage_ref"],
                text_payload["payload_bytes"],
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
                text_payload["summary_l0"],
                text_payload["summary_l1"],
                float(importance),
                float(structured["confidence"]),
                "private_agent",
                str(structured["tier"]),
                int(version),
                supersedes_memory_id,
                superseded_by_memory_id,
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
                    "text": text_payload["text"],
                    "text_hash": text_payload["text_hash"],
                    "storage_policy": text_payload["storage_policy"],
                    "storage_ref": text_payload["storage_ref"],
                    "payload_bytes": text_payload["payload_bytes"],
                    "summary": summary,
                    "summary_l0": text_payload["summary_l0"],
                    "summary_l1": text_payload["summary_l1"],
                    "importance": float(importance),
                    "confidence": float(structured["confidence"]),
                    "tier": str(structured["tier"]),
                    "version": int(version),
                    "supersedes_memory_id": supersedes_memory_id,
                    "superseded_by_memory_id": superseded_by_memory_id,
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
                }
            ),
        )
        self._record_memory_event(
            memory_id,
            event_type,
            {"text": cleaned, "scope": scope, **dict(event_payload or {})},
            reason_code=reason_code or event_type.lower(),
            source_row={
                "id": memory_id,
                "scope": scope,
                "bundle_id": bundle["id"],
                "user_id": user_id,
                "agent_id": owner_agent_id,
                "owner_agent_id": owner_agent_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "interaction_type": interaction_type,
                "namespace_key": namespace_key,
                "session_id": session_id,
                "run_id": run_id,
                "version": int(version),
                "metadata": metadata,
            },
            source_table=table_name,
            version=int(version),
        )
        created = self.get(memory_id)
        assert created is not None
        self._index_memory(created)
        created["_event"] = event_type
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

    def _record_memory_event(
        self,
        memory_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        reason_code: str | None = None,
        source_row: dict[str, Any] | None = None,
        source_table: str | None = None,
        version: int | None = None,
    ) -> None:
        row = source_row or self.get(memory_id) or {}
        metadata = dict(row.get("metadata") or {})
        actor_id = row.get("actor_id") or metadata.get("actor_id")
        self.db.execute(
            """
            INSERT INTO memory_events(
                id, memory_id, event_type, session_id, run_id, actor_id, reason_code, source_table, version, payload, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                make_id("mevt"),
                memory_id,
                event_type,
                row.get("session_id"),
                row.get("run_id"),
                actor_id,
                reason_code,
                source_table or self._memory_table_for_id(memory_id),
                int(version if version is not None else row.get("version", 1) or 1),
                json_dumps(payload),
                utcnow_iso(),
            ),
        )

    def _upsert_memory_link(
        self,
        source_memory_id: str,
        target_memory_id: str,
        *,
        link_type: str,
        weight: float = 1.0,
        confidence: float = 0.5,
        metadata: dict[str, Any] | None = None,
        bundle_id: str | None = None,
        source_domain: str = "memory",
        target_domain: str = "memory",
    ) -> dict[str, Any]:
        now = utcnow_iso()
        payload = dict(metadata or {})
        existing = self.db.fetch_one(
            """
            SELECT * FROM memory_links
            WHERE source_memory_id = ? AND target_memory_id = ? AND link_type = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (source_memory_id, target_memory_id, link_type),
        )
        if existing is None:
            link_id = make_id("mlink")
            self.db.execute(
                """
                INSERT INTO memory_links(
                    id, source_memory_id, target_memory_id, link_type, bundle_id, weight, confidence,
                    source_domain, target_domain, metadata, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    link_id,
                    source_memory_id,
                    target_memory_id,
                    link_type,
                    bundle_id,
                    float(weight),
                    float(confidence),
                    source_domain,
                    target_domain,
                    json_dumps(payload),
                    now,
                ),
            )
            existing = self.db.fetch_one("SELECT * FROM memory_links WHERE id = ?", (link_id,))
        else:
            merged_metadata = merge_metadata(_loads(existing.get("metadata"), {}), payload)
            self.db.execute(
                """
                UPDATE memory_links
                SET bundle_id = ?, weight = ?, confidence = ?, source_domain = ?, target_domain = ?, metadata = ?
                WHERE id = ?
                """,
                (
                    bundle_id or existing.get("bundle_id"),
                    float(weight),
                    float(confidence),
                    source_domain,
                    target_domain,
                    json_dumps(merged_metadata),
                    existing["id"],
                ),
            )
            existing = self.db.fetch_one("SELECT * FROM memory_links WHERE id = ?", (existing["id"],))
        return _deserialize_row(existing) or dict(existing or {})

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
            INSERT INTO memory_index(
                record_id, domain, scope, user_id, owner_agent_id, subject_type, subject_id, interaction_type,
                namespace_key, visibility, tier, access_count, last_accessed_at, actor_id, session_id, text,
                keywords, score_boost, updated_at, metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                scope = excluded.scope,
                user_id = excluded.user_id,
                owner_agent_id = excluded.owner_agent_id,
                subject_type = excluded.subject_type,
                subject_id = excluded.subject_id,
                interaction_type = excluded.interaction_type,
                namespace_key = excluded.namespace_key,
                visibility = excluded.visibility,
                tier = excluded.tier,
                access_count = excluded.access_count,
                last_accessed_at = excluded.last_accessed_at,
                actor_id = excluded.actor_id,
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
                memory.get("visibility"),
                memory.get("tier"),
                int(memory.get("access_count", 0) or 0),
                memory.get("last_accessed_at"),
                memory.get("actor_id"),
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
            INSERT INTO skill_index(record_id, skill_id, skill_snapshot_id, owner_agent_id, source_subject_type, source_subject_id, namespace_key, name, description, text, keywords, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                skill_id = excluded.skill_id,
                skill_snapshot_id = excluded.skill_snapshot_id,
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
                payload["skill_snapshot_id"],
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
            metadata={"skill_id": payload["skill_id"], "skill_snapshot_id": payload["skill_snapshot_id"], **dict(payload.get("metadata", {}))},
            keywords=keywords,
        )
        self.graph_store.upsert_node("skill", payload["skill_id"], payload["name"], payload.get("metadata"))

    def _index_skill_reference_chunk(self, payload: dict[str, Any]) -> None:
        keywords = extract_keywords(" ".join(part for part in [payload.get("title"), payload.get("text")] if part))
        self.db.execute(
            """
            INSERT INTO skill_reference_index(
                record_id, skill_id, skill_snapshot_id, file_id, object_id, owner_agent_id, source_subject_type, source_subject_id,
                namespace_key, relative_path, title, text, keywords, updated_at, metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                skill_id = excluded.skill_id,
                skill_snapshot_id = excluded.skill_snapshot_id,
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
                payload["skill_snapshot_id"],
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
                "skill_snapshot_id": payload["skill_snapshot_id"],
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

    def _index_context_artifact(self, artifact: dict[str, Any]) -> None:
        text = str(artifact.get("content") or "")
        keywords = extract_keywords(" ".join(part for part in [artifact.get("artifact_type"), text] if part))
        self._upsert_text_search_record(
            record_id=artifact["id"],
            domain="context",
            collection="context_artifact_index",
            title=artifact.get("artifact_type"),
            text=text,
            keywords=keywords,
            updated_at=artifact.get("created_at"),
            owner_agent_id=artifact.get("owner_agent_id"),
            session_id=artifact.get("session_id"),
            run_id=artifact.get("run_id"),
            namespace_key=artifact.get("namespace_key"),
        )
        self._index_semantic_record(
            collection="context_artifact_index",
            record_id=artifact["id"],
            domain="context",
            text=text,
            updated_at=str(artifact.get("created_at") or utcnow_iso()),
            quality=0.74,
            metadata={
                "artifact_type": artifact.get("artifact_type"),
                "target_agent_id": artifact.get("target_agent_id"),
                **dict(artifact.get("metadata") or {}),
            },
            keywords=keywords,
        )

    def _index_handoff_pack(self, handoff: dict[str, Any]) -> None:
        text = str(handoff.get("text") or "")
        keywords = extract_keywords(" ".join(part for part in [handoff.get("source_agent_id"), handoff.get("target_agent_id"), text] if part))
        self._upsert_text_search_record(
            record_id=handoff["id"],
            domain="handoff",
            collection="handoff_pack_index",
            title=" -> ".join(part for part in [handoff.get("source_agent_id"), handoff.get("target_agent_id")] if part),
            text=text,
            keywords=keywords,
            updated_at=handoff.get("created_at"),
            owner_agent_id=handoff.get("source_agent_id"),
            session_id=handoff.get("source_session_id"),
            run_id=handoff.get("source_run_id"),
            namespace_key=handoff.get("namespace_key"),
        )
        self._index_semantic_record(
            collection="handoff_pack_index",
            record_id=handoff["id"],
            domain="handoff",
            text=text,
            updated_at=str(handoff.get("created_at") or utcnow_iso()),
            quality=0.78,
            metadata={
                "source_agent_id": handoff.get("source_agent_id"),
                "target_agent_id": handoff.get("target_agent_id"),
                "source_session_id": handoff.get("source_session_id"),
                "source_run_id": handoff.get("source_run_id"),
                **dict(handoff.get("metadata") or {}),
            },
            keywords=keywords,
        )

    def _index_reflection_memory(self, reflection: dict[str, Any]) -> None:
        text = str(reflection.get("text") or "")
        keywords = extract_keywords(" ".join(part for part in [reflection.get("reflection_type"), text] if part))
        self._upsert_text_search_record(
            record_id=reflection["id"],
            domain="reflection",
            collection="reflection_memory_index",
            title=reflection.get("reflection_type"),
            text=text,
            keywords=keywords,
            updated_at=reflection.get("updated_at"),
            owner_agent_id=reflection.get("owner_agent_id"),
            session_id=reflection.get("session_id"),
            run_id=reflection.get("run_id"),
        )
        self._index_semantic_record(
            collection="reflection_memory_index",
            record_id=reflection["id"],
            domain="reflection",
            text=text,
            updated_at=str(reflection.get("updated_at") or utcnow_iso()),
            quality=float(reflection.get("confidence", 0.5) or 0.5),
            metadata={
                "reflection_type": reflection.get("reflection_type"),
                "session_id": reflection.get("session_id"),
                "run_id": reflection.get("run_id"),
                **dict(reflection.get("metadata") or {}),
            },
            keywords=keywords,
        )

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
            "source_updated_at": updated_at,
            "metadata": json_dumps(metadata),
            "keywords": keywords,
        }
        self.vector_index.upsert(collection, record_id, text, payload)
        if getattr(self.vector_index, "name", "") != "sqlite":
            self.db.execute(
                """
                INSERT INTO semantic_index_cache(record_id, domain, collection, text, embedding, fingerprint, quality, updated_at, source_updated_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_id) DO UPDATE SET
                    domain = excluded.domain,
                    collection = excluded.collection,
                    text = excluded.text,
                    embedding = excluded.embedding,
                    fingerprint = excluded.fingerprint,
                    quality = excluded.quality,
                    updated_at = excluded.updated_at,
                    source_updated_at = excluded.source_updated_at,
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
        keywords = extract_keywords(content)
        self._upsert_text_search_record(
            record_id=turn_id,
            domain="interaction",
            collection="interaction_turn",
            title=" ".join(part for part in [session.get("title"), role, turn_type] if part),
            text=content,
            keywords=keywords,
            updated_at=updated_at,
            user_id=session.get("user_id"),
            owner_agent_id=session.get("owner_agent_id") or session.get("agent_id"),
            subject_type=session.get("subject_type"),
            subject_id=session.get("subject_id"),
            interaction_type=session.get("interaction_type"),
            session_id=session.get("id"),
            namespace_key=session.get("namespace_key"),
        )
        self._index_semantic_record(
            collection="interaction_turn",
            record_id=turn_id,
            domain="interaction",
            text=content,
            updated_at=updated_at,
            quality=0.66,
            metadata={
                "session_id": session.get("id"),
                "role": role,
                "turn_type": turn_type,
                "subject_type": session.get("subject_type"),
                "subject_id": session.get("subject_id"),
                "interaction_type": session.get("interaction_type"),
            },
            keywords=keywords,
        )

    def _index_interaction_snapshot(self, snapshot: dict[str, Any]) -> None:
        summary = str(snapshot.get("summary") or "")
        keywords = extract_keywords(summary)
        self._upsert_text_search_record(
            record_id=snapshot["id"],
            domain="interaction",
            collection="interaction_snapshot",
            title=f"snapshot {snapshot.get('session_id') or ''}".strip(),
            text=summary,
            keywords=keywords,
            updated_at=snapshot.get("updated_at"),
            owner_agent_id=snapshot.get("owner_agent_id"),
            subject_type=snapshot.get("subject_type"),
            subject_id=snapshot.get("subject_id"),
            interaction_type=snapshot.get("interaction_type"),
            session_id=snapshot.get("session_id"),
            run_id=snapshot.get("run_id"),
            namespace_key=snapshot.get("namespace_key"),
        )
        semantic_text = "\n".join(
            part
            for part in [
                summary,
                str(snapshot.get("plan") or "").strip(),
                str(snapshot.get("scratchpad") or "").strip(),
            ]
            if part
        )
        self._index_semantic_record(
            collection="interaction_snapshot",
            record_id=snapshot["id"],
            domain="interaction",
            text=summary,
            semantic_source_text=semantic_text or summary,
            updated_at=snapshot.get("updated_at"),
            quality=0.76,
            metadata={
                "session_id": snapshot.get("session_id"),
                "run_id": snapshot.get("run_id"),
                "subject_type": snapshot.get("subject_type"),
                "subject_id": snapshot.get("subject_id"),
                "interaction_type": snapshot.get("interaction_type"),
            },
            keywords=keywords,
        )

    def _index_execution_run(self, run: dict[str, Any]) -> None:
        text = " ".join(part for part in [str(run.get("goal") or ""), str(run.get("status") or "")] if part).strip()
        keywords = extract_keywords(text)
        self._upsert_text_search_record(
            record_id=run["id"],
            domain="execution",
            collection="execution_run",
            title=run.get("goal"),
            text=text,
            keywords=keywords,
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
        self._index_semantic_record(
            collection="execution_run",
            record_id=run["id"],
            domain="execution",
            text=text,
            updated_at=run.get("updated_at"),
            quality=0.68,
            metadata={
                "run_id": run.get("id"),
                "status": run.get("status"),
                "subject_type": run.get("subject_type"),
                "subject_id": run.get("subject_id"),
            },
            keywords=keywords,
        )

    def _index_execution_observation(self, observation: dict[str, Any], *, run: dict[str, Any] | None = None) -> None:
        run_row = dict(run or {})
        text = str(observation.get("content") or observation.get("text") or "")
        title = str(observation.get("kind") or "observation")
        keywords = extract_keywords(" ".join(part for part in [title, text] if part))
        self._upsert_text_search_record(
            record_id=observation["id"],
            domain="execution",
            collection="execution_observation",
            title=title,
            text=text,
            keywords=keywords,
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
        semantic_text = "\n".join(part for part in [title, text] if part).strip()
        self._index_semantic_record(
            collection="execution_observation",
            record_id=observation["id"],
            domain="execution",
            text=text,
            semantic_source_text=semantic_text or text,
            updated_at=observation.get("created_at") or observation.get("updated_at"),
            quality=0.72,
            metadata={
                "run_id": observation.get("run_id"),
                "task_id": observation.get("task_id"),
                "kind": observation.get("kind"),
                "subject_type": run_row.get("subject_type"),
                "subject_id": run_row.get("subject_id"),
            },
            keywords=keywords,
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
            "SELECT COUNT(*) AS count FROM context_artifacts",
            "SELECT COUNT(*) AS count FROM handoff_packs",
            "SELECT COUNT(*) AS count FROM reflection_memories",
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

    def _sync_contextual_semantic_indexes(self) -> None:
        try:
            expected = sum(
                int((self.db.fetch_one(statement) or {}).get("count", 0) or 0)
                for statement in (
                    "SELECT COUNT(*) AS count FROM conversation_turns",
                    "SELECT COUNT(*) AS count FROM working_memory_snapshots",
                    "SELECT COUNT(*) AS count FROM runs",
                    "SELECT COUNT(*) AS count FROM observations",
                    "SELECT COUNT(*) AS count FROM context_artifacts",
                    "SELECT COUNT(*) AS count FROM handoff_packs",
                    "SELECT COUNT(*) AS count FROM reflection_memories",
                )
            )
            current = int(
                (
                    self.db.fetch_one(
                        """
                        SELECT COUNT(*) AS count
                        FROM semantic_index_cache
                        WHERE collection IN (
                            'interaction_turn',
                            'interaction_snapshot',
                            'execution_run',
                            'execution_observation',
                            'context_artifact_index',
                            'handoff_pack_index',
                            'reflection_memory_index'
                        )
                        """
                    )
                    or {}
                ).get("count", 0)
                or 0
            )
        except Exception:
            return
        if current >= expected:
            return
        self._rebuild_contextual_semantic_indexes()

    def _rebuild_contextual_semantic_indexes(self) -> None:
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
        for artifact in self.list_context_artifacts(limit=max(1, int((self.db.fetch_one("SELECT COUNT(*) AS count FROM context_artifacts") or {}).get("count", 0) or 0)))["results"]:
            self._index_context_artifact(artifact)
        for handoff in self.list_handoff_packs(limit=max(1, int((self.db.fetch_one("SELECT COUNT(*) AS count FROM handoff_packs") or {}).get("count", 0) or 0)))["results"]:
            self._index_handoff_pack(handoff)
        for reflection in self.list_reflection_memories(limit=max(1, int((self.db.fetch_one("SELECT COUNT(*) AS count FROM reflection_memories") or {}).get("count", 0) or 0)))["results"]:
            self._index_reflection_memory(reflection)

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

        for artifact in self.list_context_artifacts(limit=max(1, int((self.db.fetch_one("SELECT COUNT(*) AS count FROM context_artifacts") or {}).get("count", 0) or 0)))["results"]:
            self._index_context_artifact(artifact)

        for handoff in self.list_handoff_packs(limit=max(1, int((self.db.fetch_one("SELECT COUNT(*) AS count FROM handoff_packs") or {}).get("count", 0) or 0)))["results"]:
            self._index_handoff_pack(handoff)

        for reflection in self.list_reflection_memories(limit=max(1, int((self.db.fetch_one("SELECT COUNT(*) AS count FROM reflection_memories") or {}).get("count", 0) or 0)))["results"]:
            self._index_reflection_memory(reflection)

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
        self.db.execute("DELETE FROM memory_links WHERE source_memory_id = ? AND link_type = 'semantic'", (memory["id"],))
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
            self._upsert_memory_link(
                memory["id"],
                candidate["id"],
                link_type="semantic",
                bundle_id=memory.get("bundle_id"),
                weight=similarity,
                confidence=similarity,
                metadata={"score": similarity, "origin": "auto_semantic"},
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

    def _skill_reference_snapshot_hit_map(self, query: str, *, limit: int) -> dict[str, float]:
        hits: dict[str, float] = {}
        for row in self.vector_index.search("skill_reference_index", query, limit=limit):
            metadata = _loads(row.get("metadata"), {})
            snapshot_id = str(
                row.get("skill_snapshot_id")
                or metadata.get("skill_snapshot_id")
                or ""
            )
            if not snapshot_id:
                continue
            distance = float(row.get("_distance", 1.0) or 1.0)
            score = max(0.0, 1.0 - distance)
            hits[snapshot_id] = max(hits.get(snapshot_id, 0.0), round(score * 0.92, 6))
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
    def __init__(
        self,
        config: AIMemoryConfig | dict[str, Any] | None = None,
        *,
        platform_llm: Any | None = None,
        platform_events: Any | None = None,
    ):
        self._sync = AIMemory(config, platform_llm=platform_llm, platform_events=platform_events)
        self._structured_api = None

    @property
    def api(self):
        if self._structured_api is None:
            from aimemory.core.structured_api import AsyncStructuredAIMemoryAPI

            self._structured_api = AsyncStructuredAIMemoryAPI(self._sync)
        return self._structured_api

    @property
    def events(self):
        return self._sync.events

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
