from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aimemory.core.text import chunk_text
from aimemory.core.utils import json_dumps, json_loads, make_id, merge_metadata, utcnow_iso
from aimemory.domains.memory.models import MemoryScope, MemoryType

if TYPE_CHECKING:
    from aimemory.core.facade import AIMemory


def _loads(value: Any, default: Any) -> Any:
    loaded = json_loads(value, default)
    return default if loaded is None else loaded


def _deserialize_row(
    row: dict[str, Any] | None,
    json_fields: tuple[str, ...] = (
        "metadata",
        "highlights",
        "schema_json",
        "config",
        "input_payload",
        "expected_output",
    ),
) -> dict[str, Any] | None:
    if row is None:
        return None
    item = dict(row)
    for field in json_fields:
        if field in item:
            fallback: Any = [] if field in {"highlights"} or field.endswith("s") else {}
            item[field] = _loads(item.get(field), fallback)
    return item


def _deserialize_rows(
    rows: list[dict[str, Any]],
    json_fields: tuple[str, ...] = (
        "metadata",
        "highlights",
        "schema_json",
        "config",
        "input_payload",
        "expected_output",
    ),
) -> list[dict[str, Any]]:
    return [item for item in (_deserialize_row(row, json_fields) for row in rows) if item is not None]


class AgentStoreAPI:
    def __init__(self, memory: "AIMemory"):
        self.memory = memory

    def store_long_term_memory(self, text: str, **kwargs: Any) -> dict[str, Any]:
        result = self.memory.remember_long_term(text, **kwargs)
        result["compression"] = self.compress_long_term_memories(
            owner_agent_id=result.get("owner_agent_id") or result.get("agent_id"),
            subject_type=result.get("subject_type"),
            subject_id=result.get("subject_id"),
            interaction_type=result.get("interaction_type"),
            user_id=result.get("user_id"),
            namespace_key=result.get("namespace_key"),
        )
        return result

    def get_long_term_memory(self, memory_id: str) -> dict[str, Any]:
        return self._require_memory_scope(memory_id, str(MemoryScope.LONG_TERM))

    def list_long_term_memories(
        self,
        *,
        include_generated: bool = False,
        include_inactive: bool = False,
        limit: int = 200,
        offset: int = 0,
        filters: dict[str, Any] | None = None,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        result = self.memory.get_all(
            scope=str(MemoryScope.LONG_TERM),
            limit=limit,
            offset=offset,
            filters=filters,
            **self._memory_scope_kwargs(scope_kwargs),
        )
        items = self._filter_memory_items(
            result["results"],
            include_generated=include_generated,
            include_inactive=include_inactive,
        )
        return {"results": items, "count": len(items), "limit": limit, "offset": offset}

    def search_long_term_memories(
        self,
        query: str,
        *,
        include_generated: bool = True,
        limit: int = 10,
        threshold: float = 0.0,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        result = self.memory.memory_search(
            query,
            scope=str(MemoryScope.LONG_TERM),
            top_k=limit,
            search_threshold=threshold,
            **self._memory_scope_kwargs(scope_kwargs),
        )
        items = self._filter_memory_items(result["results"], include_generated=include_generated, include_inactive=False)
        return {"results": items[:limit], "count": len(items[:limit]), "query": query}

    def update_long_term_memory(self, memory_id: str, **kwargs: Any) -> dict[str, Any]:
        self._require_memory_scope(memory_id, str(MemoryScope.LONG_TERM))
        result = self.memory.update(memory_id, **kwargs)
        result["compression"] = self.compress_long_term_memories(
            owner_agent_id=result.get("owner_agent_id") or result.get("agent_id"),
            subject_type=result.get("subject_type"),
            subject_id=result.get("subject_id"),
            interaction_type=result.get("interaction_type"),
            user_id=result.get("user_id"),
            namespace_key=result.get("namespace_key"),
        )
        return result

    def delete_long_term_memory(self, memory_id: str) -> dict[str, Any]:
        self._require_memory_scope(memory_id, str(MemoryScope.LONG_TERM))
        return self.memory.delete(memory_id)

    def compress_long_term_memories(self, *, force: bool = False, limit: int = 400, **scope_kwargs: Any) -> dict[str, Any]:
        scope = self._resolve_memory_scope(scope_kwargs)
        items = self.list_long_term_memories(
            include_generated=False,
            include_inactive=False,
            limit=limit,
            **scope_kwargs,
        )["results"]
        compression = self.memory.compress_domain_records(
            "long_term",
            self._memory_compression_records(items),
            scope=scope,
            threshold_chars=self.memory.config.memory_policy.long_term_char_threshold,
            budget_chars=self.memory.config.memory_policy.long_term_compression_budget_chars,
            force=force,
        )
        if compression["triggered"]:
            compression["generated_memory"] = self._upsert_generated_memory(
                memory_scope=str(MemoryScope.LONG_TERM),
                domain="long_term",
                scope=scope,
                compression=compression,
            )
        return compression

    def store_short_term_memory(self, text: str, **kwargs: Any) -> dict[str, Any]:
        result = self.memory.remember_short_term(text, **kwargs)
        result["compression"] = self.compress_short_term_memories(
            session_id=result.get("session_id"),
            owner_agent_id=result.get("owner_agent_id") or result.get("agent_id"),
            subject_type=result.get("subject_type"),
            subject_id=result.get("subject_id"),
            interaction_type=result.get("interaction_type"),
            user_id=result.get("user_id"),
            namespace_key=result.get("namespace_key"),
        )
        return result

    def get_short_term_memory(self, memory_id: str) -> dict[str, Any]:
        return self._require_memory_scope(memory_id, str(MemoryScope.SESSION))

    def list_short_term_memories(
        self,
        *,
        include_generated: bool = False,
        include_inactive: bool = False,
        limit: int = 200,
        offset: int = 0,
        filters: dict[str, Any] | None = None,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        result = self.memory.get_all(
            scope=str(MemoryScope.SESSION),
            limit=limit,
            offset=offset,
            filters=filters,
            **self._memory_scope_kwargs(scope_kwargs),
        )
        items = self._filter_memory_items(
            result["results"],
            include_generated=include_generated,
            include_inactive=include_inactive,
        )
        return {"results": items, "count": len(items), "limit": limit, "offset": offset}

    def search_short_term_memories(
        self,
        query: str,
        *,
        include_generated: bool = True,
        limit: int = 10,
        threshold: float = 0.0,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        result = self.memory.memory_search(
            query,
            scope=str(MemoryScope.SESSION),
            top_k=limit,
            search_threshold=threshold,
            **self._memory_scope_kwargs(scope_kwargs),
        )
        items = self._filter_memory_items(result["results"], include_generated=include_generated, include_inactive=False)
        return {"results": items[:limit], "count": len(items[:limit]), "query": query}

    def update_short_term_memory(self, memory_id: str, **kwargs: Any) -> dict[str, Any]:
        record = self._require_memory_scope(memory_id, str(MemoryScope.SESSION))
        result = self.memory.update(memory_id, **kwargs)
        result["compression"] = self.compress_short_term_memories(
            session_id=result.get("session_id") or record.get("session_id"),
            owner_agent_id=result.get("owner_agent_id") or result.get("agent_id"),
            subject_type=result.get("subject_type"),
            subject_id=result.get("subject_id"),
            interaction_type=result.get("interaction_type"),
            user_id=result.get("user_id"),
            namespace_key=result.get("namespace_key"),
        )
        return result

    def delete_short_term_memory(self, memory_id: str) -> dict[str, Any]:
        self._require_memory_scope(memory_id, str(MemoryScope.SESSION))
        return self.memory.delete(memory_id)

    def compress_short_term_memories(self, *, force: bool = False, limit: int = 400, **scope_kwargs: Any) -> dict[str, Any]:
        scope = self._resolve_memory_scope(scope_kwargs)
        items = self.list_short_term_memories(
            include_generated=False,
            include_inactive=False,
            limit=limit,
            **scope_kwargs,
        )["results"]
        compression = self.memory.compress_domain_records(
            "short_term",
            self._memory_compression_records(items),
            scope=scope,
            threshold_chars=self.memory.config.memory_policy.short_term_char_threshold,
            budget_chars=self.memory.config.memory_policy.short_term_compression_budget_chars,
            force=force,
        )
        if not compression["triggered"]:
            return compression
        session_id = scope_kwargs.get("session_id")
        if session_id:
            compression["snapshot"] = self._create_short_term_snapshot(session_id, compression)
        else:
            compression["generated_memory"] = self._upsert_generated_memory(
                memory_scope=str(MemoryScope.SESSION),
                domain="short_term",
                scope=scope,
                compression=compression,
            )
        return compression

    def save_archive_memory(
        self,
        summary: str,
        *,
        content: str | None = None,
        global_scope: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        metadata = dict(kwargs.pop("metadata", {}) or {})
        scope = self.memory._resolve_scope(
            user_id=kwargs.pop("user_id", None),
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
            global_scope=global_scope,
        )
        archive_id = kwargs.pop("archive_unit_id", make_id("arch"))
        source_type = str(kwargs.pop("source_type", "manual"))
        archive_domain = str(kwargs.pop("domain", "manual"))
        session_id = kwargs.pop("session_id", None)
        source_id = kwargs.pop("source_id", archive_id)
        now = utcnow_iso()
        payload = {
            "summary": summary,
            "content": content or "",
            "metadata": metadata,
            "scope": scope,
        }
        stored = self.memory.object_store.put_text(
            json_dumps(payload),
            object_type="archive",
            suffix=".json",
            prefix=self.memory._object_store_prefix(scope, "archive"),
        )
        object_row = self.memory._persist_object(
            stored,
            mime_type="application/json",
            metadata={"archive_unit_id": archive_id, **self.memory._scope_metadata(scope), **metadata},
        )
        self.memory.db.execute(
            """
            INSERT INTO archive_units(id, domain, source_id, user_id, owner_agent_id, subject_type, subject_id, interaction_type, namespace_key, source_type, session_id, object_id, summary, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                archive_id,
                archive_domain,
                source_id,
                scope.get("user_id"),
                scope.get("owner_agent_id"),
                scope.get("subject_type"),
                scope.get("subject_id"),
                scope.get("interaction_type"),
                scope.get("namespace_key"),
                source_type,
                session_id,
                object_row["id"],
                summary,
                json_dumps(merge_metadata(metadata, self.memory._scope_metadata(scope))),
                now,
            ),
        )
        summary_id = make_id("archsum")
        highlights = [item for item in [summary, content] if item]
        self.memory.db.execute(
            """
            INSERT INTO archive_summaries(id, archive_unit_id, summary, highlights, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (summary_id, archive_id, summary, json_dumps(highlights), json_dumps(metadata), now),
        )
        self.memory._index_archive_summary(
            {
                "record_id": summary_id,
                "archive_unit_id": archive_id,
                "domain": archive_domain,
                "user_id": scope.get("user_id"),
                "owner_agent_id": scope.get("owner_agent_id"),
                "subject_type": scope.get("subject_type"),
                "subject_id": scope.get("subject_id"),
                "interaction_type": scope.get("interaction_type"),
                "namespace_key": scope.get("namespace_key"),
                "source_type": source_type,
                "session_id": session_id,
                "text": summary,
                "metadata": {"highlights": highlights, **metadata},
                "updated_at": now,
            }
        )
        return self.memory.get_archive_unit(archive_id) or {"id": archive_id, "summary": summary}

    def get_archive_memory(self, archive_unit_id: str) -> dict[str, Any]:
        archive = self.memory.get_archive_unit(archive_unit_id)
        if archive is None:
            raise ValueError(f"Archive `{archive_unit_id}` does not exist.")
        return archive

    def list_archive_memories(
        self,
        *,
        include_global: bool = True,
        include_generated: bool = False,
        limit: int = 100,
        offset: int = 0,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        filters = ["1 = 1"]
        params: list[Any] = []
        user_id = scope_kwargs.get("user_id")
        owner_agent_id = scope_kwargs.get("owner_agent_id")
        subject_type = scope_kwargs.get("subject_type")
        subject_id = scope_kwargs.get("subject_id")
        interaction_type = scope_kwargs.get("interaction_type")
        session_id = scope_kwargs.get("session_id")
        namespace_key = self.memory._namespace_filter_value(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            platform_id=scope_kwargs.get("platform_id"),
            workspace_id=scope_kwargs.get("workspace_id"),
            team_id=scope_kwargs.get("team_id"),
            project_id=scope_kwargs.get("project_id"),
            namespace_key=scope_kwargs.get("namespace_key"),
        )
        self._append_optional_filter(filters, params, "user_id", user_id, include_global=include_global)
        self._append_optional_filter(filters, params, "owner_agent_id", owner_agent_id, include_global=include_global)
        self._append_optional_filter(filters, params, "subject_type", subject_type, include_global=include_global)
        self._append_optional_filter(filters, params, "subject_id", subject_id, include_global=include_global)
        self._append_optional_filter(filters, params, "interaction_type", interaction_type, include_global=include_global)
        if session_id:
            filters.append("session_id = ?")
            params.append(session_id)
        if namespace_key:
            if include_global:
                filters.append("(namespace_key = ? OR namespace_key = 'global')")
            else:
                filters.append("namespace_key = ?")
            params.append(namespace_key)
        if not include_generated:
            filters.append("(source_type IS NULL OR source_type != 'archive_compaction')")
        rows = self.memory.db.fetch_all(
            f"SELECT * FROM archive_units WHERE {' AND '.join(filters)} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            tuple(params + [limit, offset]),
        )
        results = [self.memory.get_archive_unit(row["id"]) or _deserialize_row(row) for row in rows]
        return {"results": results, "count": len(results), "limit": limit, "offset": offset}

    def search_archive_memories(
        self,
        query: str,
        *,
        include_global: bool = True,
        limit: int = 10,
        threshold: float = 0.0,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        return self.memory.search_archive(query, include_global=include_global, limit=limit, threshold=threshold, **scope_kwargs)

    def update_archive_memory(self, archive_unit_id: str, **kwargs: Any) -> dict[str, Any]:
        archive = self.get_archive_memory(archive_unit_id)
        metadata = merge_metadata(archive.get("metadata"), kwargs.pop("metadata", None))
        global_scope = bool(kwargs.pop("global_scope", archive.get("namespace_key") == "global" and not archive.get("owner_agent_id")))
        scope = self.memory._resolve_scope(
            user_id=kwargs.pop("user_id", archive.get("user_id")),
            owner_agent_id=kwargs.pop("owner_agent_id", archive.get("owner_agent_id")),
            subject_type=kwargs.pop("subject_type", archive.get("subject_type")),
            subject_id=kwargs.pop("subject_id", archive.get("subject_id")),
            interaction_type=kwargs.pop("interaction_type", archive.get("interaction_type")),
            namespace_key=kwargs.pop("namespace_key", archive.get("namespace_key")),
            global_scope=global_scope,
        )
        summary = str(kwargs.pop("summary", archive.get("summary") or ""))
        content = kwargs.pop("content", None)
        source_type = str(kwargs.pop("source_type", archive.get("source_type") or "manual"))
        now = utcnow_iso()
        object_id = archive.get("object_id")
        if content is not None:
            payload = {
                "summary": summary,
                "content": content,
                "metadata": metadata,
                "scope": scope,
            }
            stored = self.memory.object_store.put_text(
                json_dumps(payload),
                object_type="archive",
                suffix=".json",
                prefix=self.memory._object_store_prefix(scope, "archive"),
            )
            object_row = self.memory._persist_object(
                stored,
                mime_type="application/json",
                metadata={"archive_unit_id": archive_unit_id, **self.memory._scope_metadata(scope), **dict(metadata or {})},
            )
            object_id = object_row["id"]
        self.memory.db.execute(
            """
            UPDATE archive_units
            SET user_id = ?, owner_agent_id = ?, subject_type = ?, subject_id = ?, interaction_type = ?, namespace_key = ?, source_type = ?, object_id = ?, summary = ?, metadata = ?
            WHERE id = ?
            """,
            (
                scope.get("user_id"),
                scope.get("owner_agent_id"),
                scope.get("subject_type"),
                scope.get("subject_id"),
                scope.get("interaction_type"),
                scope.get("namespace_key"),
                source_type,
                object_id,
                summary,
                json_dumps(merge_metadata(metadata, self.memory._scope_metadata(scope))),
                archive_unit_id,
            ),
        )
        latest_summary = archive["summaries"][0] if archive.get("summaries") else None
        summary_id = latest_summary["id"] if latest_summary else make_id("archsum")
        highlights = [item for item in [summary, content] if item]
        if latest_summary:
            self.memory.db.execute(
                "UPDATE archive_summaries SET summary = ?, highlights = ?, metadata = ? WHERE id = ?",
                (summary, json_dumps(highlights), json_dumps(metadata or {}), summary_id),
            )
        else:
            self.memory.db.execute(
                """
                INSERT INTO archive_summaries(id, archive_unit_id, summary, highlights, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (summary_id, archive_unit_id, summary, json_dumps(highlights), json_dumps(metadata or {}), now),
            )
        self.memory._index_archive_summary(
            {
                "record_id": summary_id,
                "archive_unit_id": archive_unit_id,
                "domain": archive.get("domain") or "manual",
                "user_id": scope.get("user_id"),
                "owner_agent_id": scope.get("owner_agent_id"),
                "subject_type": scope.get("subject_type"),
                "subject_id": scope.get("subject_id"),
                "interaction_type": scope.get("interaction_type"),
                "namespace_key": scope.get("namespace_key"),
                "source_type": source_type,
                "session_id": archive.get("session_id"),
                "text": summary,
                "metadata": {"highlights": highlights, **dict(metadata or {})},
                "updated_at": now,
            }
        )
        return self.get_archive_memory(archive_unit_id)

    def delete_archive_memory(self, archive_unit_id: str) -> dict[str, Any]:
        archive = self.get_archive_memory(archive_unit_id)
        for summary in archive.get("summaries", []):
            self.memory.db.execute("DELETE FROM archive_summary_index WHERE record_id = ?", (summary["id"],))
            self.memory.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (summary["id"],))
            self.memory.vector_index.delete("archive_summary_index", summary["id"])
        self.memory.graph_store.delete_reference(archive_unit_id)
        self.memory.db.execute("DELETE FROM archive_summaries WHERE archive_unit_id = ?", (archive_unit_id,))
        self.memory.db.execute("DELETE FROM archive_units WHERE id = ?", (archive_unit_id,))
        return {"id": archive_unit_id, "deleted": True, "archive": archive}

    def compress_archive_memories(
        self,
        *,
        include_global: bool = True,
        force: bool = False,
        limit: int = 400,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        scope = self._resolve_archive_scope(scope_kwargs)
        items = self.list_archive_memories(
            include_global=include_global,
            include_generated=False,
            limit=limit,
            **scope_kwargs,
        )["results"]
        records = [
            {
                "id": item["id"],
                "text": str(item.get("summary") or ""),
                "score": 0.58,
            }
            for item in items
            if str(item.get("summary") or "").strip()
        ]
        compression = self.memory.compress_domain_records(
            "archive",
            records,
            scope=scope,
            threshold_chars=self.memory.config.memory_policy.archive_char_threshold,
            budget_chars=self.memory.config.memory_policy.archive_compression_budget_chars,
            force=force,
        )
        if compression["triggered"]:
            compression["archive"] = self._upsert_generated_archive(scope, compression)
        return compression

    def save_knowledge_document(self, title: str, text: str, **kwargs: Any) -> dict[str, Any]:
        return self.memory.ingest_document(title, text, **kwargs)

    def get_knowledge_document(self, document_id: str) -> dict[str, Any]:
        document = self.memory.get_document(document_id)
        if document is None:
            raise ValueError(f"Document `{document_id}` does not exist.")
        return document

    def list_knowledge_documents(
        self,
        *,
        include_global: bool = True,
        limit: int = 100,
        offset: int = 0,
        status: str | None = "active",
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        filters = ["1 = 1"]
        params: list[Any] = []
        user_id = scope_kwargs.get("user_id")
        owner_agent_id = scope_kwargs.get("owner_agent_id")
        subject_type = scope_kwargs.get("subject_type")
        subject_id = scope_kwargs.get("subject_id")
        namespace_key = self.memory._namespace_filter_value(
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            platform_id=scope_kwargs.get("platform_id"),
            workspace_id=scope_kwargs.get("workspace_id"),
            team_id=scope_kwargs.get("team_id"),
            project_id=scope_kwargs.get("project_id"),
            namespace_key=scope_kwargs.get("namespace_key"),
        )
        if status:
            filters.append("status = ?")
            params.append(status)
        self._append_optional_filter(filters, params, "user_id", user_id, include_global=include_global)
        self._append_optional_filter(filters, params, "owner_agent_id", owner_agent_id, include_global=include_global)
        self._append_optional_filter(filters, params, "source_subject_type", subject_type, include_global=include_global)
        self._append_optional_filter(filters, params, "source_subject_id", subject_id, include_global=include_global)
        if namespace_key:
            if include_global:
                filters.append("(namespace_key = ? OR namespace_key = 'global')")
            else:
                filters.append("namespace_key = ?")
            params.append(namespace_key)
        rows = self.memory.db.fetch_all(
            f"SELECT * FROM documents WHERE {' AND '.join(filters)} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            tuple(params + [limit, offset]),
        )
        results = [self.memory.get_document(row["id"]) or _deserialize_row(row) for row in rows]
        return {"results": results, "count": len(results), "limit": limit, "offset": offset}

    def search_knowledge_documents(
        self,
        query: str,
        *,
        include_global: bool = True,
        limit: int = 10,
        threshold: float = 0.0,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        return self.memory.search_knowledge(query, include_global=include_global, limit=limit, threshold=threshold, **scope_kwargs)

    def update_knowledge_document(self, document_id: str, **kwargs: Any) -> dict[str, Any]:
        document = self.get_knowledge_document(document_id)
        global_scope = bool(kwargs.pop("global_scope", document.get("namespace_key") == "global" and not document.get("owner_agent_id")))
        scope = self.memory._resolve_scope(
            user_id=kwargs.pop("user_id", document.get("user_id")),
            owner_agent_id=kwargs.pop("owner_agent_id", document.get("owner_agent_id")),
            subject_type=kwargs.pop("source_subject_type", kwargs.pop("subject_type", document.get("source_subject_type"))),
            subject_id=kwargs.pop("source_subject_id", kwargs.pop("subject_id", document.get("source_subject_id"))),
            namespace_key=kwargs.pop("namespace_key", document.get("namespace_key")),
            global_scope=global_scope,
        )
        title = str(kwargs.pop("title", document["title"]))
        text = kwargs.pop("text", None)
        status = str(kwargs.pop("status", document.get("status", "active")))
        metadata = merge_metadata(document.get("metadata"), kwargs.pop("metadata", None))
        kb_namespace = kwargs.pop("kb_namespace", document.get("kb_namespace") or (scope.get("namespace_key") or "default"))
        now = utcnow_iso()
        self.memory.db.execute(
            """
            UPDATE documents
            SET title = ?, user_id = ?, owner_agent_id = ?, kb_namespace = ?, source_subject_type = ?, source_subject_id = ?, namespace_key = ?, status = ?, metadata = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                title,
                scope.get("user_id"),
                scope.get("owner_agent_id"),
                kb_namespace,
                scope.get("subject_type"),
                scope.get("subject_id"),
                scope.get("namespace_key"),
                status,
                json_dumps(merge_metadata(metadata, self.memory._scope_metadata(scope))),
                now,
                document_id,
            ),
        )
        if text is not None:
            self._replace_document_chunks(document, text=text, title=title, scope=scope, metadata=metadata, now=now)
        return self.get_knowledge_document(document_id)

    def delete_knowledge_document(self, document_id: str) -> dict[str, Any]:
        document = self.get_knowledge_document(document_id)
        chunk_ids = [chunk["id"] for chunk in document.get("chunks", [])]
        for chunk_id in chunk_ids:
            self.memory.db.execute("DELETE FROM knowledge_chunk_index WHERE record_id = ?", (chunk_id,))
            self.memory.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (chunk_id,))
            self.memory.vector_index.delete("knowledge_chunk_index", chunk_id)
            self.memory.graph_store.delete_reference(chunk_id)
        self.memory.db.execute("DELETE FROM citations WHERE document_id = ?", (document_id,))
        self.memory.db.execute("DELETE FROM document_chunks WHERE document_id = ?", (document_id,))
        self.memory.db.execute("DELETE FROM document_versions WHERE document_id = ?", (document_id,))
        self.memory.db.execute("DELETE FROM documents WHERE id = ?", (document_id,))
        return {"id": document_id, "deleted": True, "document": document}

    def save_skill(self, name: str, description: str, **kwargs: Any) -> dict[str, Any]:
        return self.memory.save_skill(name, description, **kwargs)

    def get_skill_content(self, skill_id: str) -> dict[str, Any]:
        skill = self.memory.get_skill(skill_id)
        if skill is None:
            raise ValueError(f"Skill `{skill_id}` does not exist.")
        return skill

    def list_skill_metadata(
        self,
        *,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        status: str | None = "active",
        limit: int = 100,
        offset: int = 0,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        filters = ["1 = 1"]
        params: list[Any] = []
        effective_owner = owner_agent_id or scope_kwargs.get("owner_agent_id")
        effective_subject_type = subject_type or scope_kwargs.get("subject_type")
        effective_subject_id = subject_id or scope_kwargs.get("subject_id")
        namespace_key = self.memory._namespace_filter_value(
            owner_agent_id=effective_owner,
            subject_type=effective_subject_type,
            subject_id=effective_subject_id,
            platform_id=scope_kwargs.get("platform_id"),
            workspace_id=scope_kwargs.get("workspace_id"),
            team_id=scope_kwargs.get("team_id"),
            project_id=scope_kwargs.get("project_id"),
            namespace_key=scope_kwargs.get("namespace_key"),
        )
        if status:
            filters.append("status = ?")
            params.append(status)
        if effective_owner:
            filters.append("COALESCE(owner_agent_id, owner_id) = ?")
            params.append(effective_owner)
        if effective_subject_type:
            filters.append("(source_subject_type = ? OR source_subject_type IS NULL)")
            params.append(effective_subject_type)
        if effective_subject_id:
            filters.append("(source_subject_id = ? OR source_subject_id IS NULL)")
            params.append(effective_subject_id)
        if namespace_key:
            filters.append("namespace_key = ?")
            params.append(namespace_key)
        rows = _deserialize_rows(
            self.memory.db.fetch_all(
                f"SELECT * FROM skills WHERE {' AND '.join(filters)} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                tuple(params + [limit, offset]),
            )
        )
        results = []
        for row in rows:
            latest = self.memory.db.fetch_one(
                "SELECT id, version, created_at FROM skill_versions WHERE skill_id = ? ORDER BY created_at DESC LIMIT 1",
                (row["id"],),
            )
            results.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "description": row["description"],
                    "status": row.get("status"),
                    "owner_agent_id": row.get("owner_agent_id") or row.get("owner_id"),
                    "source_subject_type": row.get("source_subject_type"),
                    "source_subject_id": row.get("source_subject_id"),
                    "namespace_key": row.get("namespace_key"),
                    "metadata": row.get("metadata", {}),
                    "latest_version": _deserialize_row(latest) if latest else None,
                    "updated_at": row.get("updated_at"),
                }
            )
        return {"results": results, "count": len(results), "limit": limit, "offset": offset}

    def search_skill_keywords(
        self,
        query: str,
        *,
        limit: int = 10,
        threshold: float = 0.0,
        **scope_kwargs: Any,
    ) -> dict[str, Any]:
        return self.memory.search_skills(query, limit=limit, threshold=threshold, **scope_kwargs)

    def update_skill(self, skill_id: str, **kwargs: Any) -> dict[str, Any]:
        skill = self.get_skill_content(skill_id)
        name = str(kwargs.pop("name", skill["name"]))
        description = str(kwargs.pop("description", skill["description"]))
        status = str(kwargs.pop("status", skill.get("status", "active")))
        metadata = merge_metadata(skill.get("metadata"), kwargs.pop("metadata", None))
        global_scope = bool(kwargs.pop("global_scope", False))
        scope = self.memory._resolve_scope(
            owner_agent_id=kwargs.pop("owner_agent_id", skill.get("owner_agent_id") or skill.get("owner_id")),
            subject_type=kwargs.pop("source_subject_type", kwargs.pop("subject_type", skill.get("source_subject_type"))),
            subject_id=kwargs.pop("source_subject_id", kwargs.pop("subject_id", skill.get("source_subject_id"))),
            namespace_key=kwargs.pop("namespace_key", skill.get("namespace_key")),
            global_scope=global_scope,
        )
        now = utcnow_iso()
        self.memory.db.execute(
            """
            UPDATE skills
            SET name = ?, description = ?, owner_id = ?, owner_agent_id = ?, source_subject_type = ?, source_subject_id = ?, namespace_key = ?, status = ?, metadata = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                name,
                description,
                scope.get("owner_agent_id"),
                scope.get("owner_agent_id"),
                scope.get("subject_type"),
                scope.get("subject_id"),
                scope.get("namespace_key"),
                status,
                json_dumps(merge_metadata(metadata, self.memory._scope_metadata(scope))),
                now,
                skill_id,
            ),
        )
        version_fields = {
            "version": kwargs.pop("version", None),
            "prompt_template": kwargs.pop("prompt_template", None),
            "workflow": kwargs.pop("workflow", None),
            "schema": kwargs.pop("schema", None),
            "tools": kwargs.pop("tools", None),
            "tests": kwargs.pop("tests", None),
            "topics": kwargs.pop("topics", None),
        }
        if any(value is not None for value in version_fields.values()):
            self._write_skill_version(
                skill_id=skill_id,
                name=name,
                description=description,
                scope=scope,
                metadata=metadata or {},
                version=version_fields["version"],
                prompt_template=version_fields["prompt_template"],
                workflow=version_fields["workflow"],
                schema=version_fields["schema"],
                tools=list(version_fields["tools"] or []),
                tests=list(version_fields["tests"] or []),
                topics=list(version_fields["topics"] or []),
                now=now,
            )
        return self.get_skill_content(skill_id)

    def delete_skill(self, skill_id: str) -> dict[str, Any]:
        skill = self.get_skill_content(skill_id)
        for version in skill.get("versions", []):
            self.memory.db.execute("DELETE FROM skill_index WHERE record_id = ?", (version["id"],))
            self.memory.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (version["id"],))
            self.memory.vector_index.delete("skill_index", version["id"])
        self.memory.graph_store.delete_reference(skill_id)
        self.memory.db.execute(
            """
            DELETE FROM skill_tests
            WHERE skill_version_id IN (SELECT id FROM skill_versions WHERE skill_id = ?)
            """,
            (skill_id,),
        )
        self.memory.db.execute(
            """
            DELETE FROM skill_bindings
            WHERE skill_version_id IN (SELECT id FROM skill_versions WHERE skill_id = ?)
            """,
            (skill_id,),
        )
        self.memory.db.execute("DELETE FROM skill_versions WHERE skill_id = ?", (skill_id,))
        self.memory.db.execute("DELETE FROM skills WHERE id = ?", (skill_id,))
        return {"id": skill_id, "deleted": True, "skill": skill}

    def _append_optional_filter(
        self,
        filters: list[str],
        params: list[Any],
        column: str,
        value: Any,
        *,
        include_global: bool,
    ) -> None:
        if value in (None, ""):
            return
        if include_global:
            filters.append(f"({column} = ? OR {column} IS NULL)")
        else:
            filters.append(f"{column} = ?")
        params.append(value)

    def _create_short_term_snapshot(self, session_id: str, compression: dict[str, Any]) -> dict[str, Any]:
        session = self.memory.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        now = utcnow_iso()
        snapshot_id = make_id("snap")
        self.memory.db.execute(
            """
            INSERT INTO working_memory_snapshots(id, session_id, run_id, owner_agent_id, interaction_type, subject_type, subject_id, namespace_key, summary, plan, scratchpad, window_size, constraints, resolved_items, unresolved_items, next_actions, budget_tokens, salience_vector, compression_revision, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                session_id,
                None,
                session.get("owner_agent_id") or session.get("agent_id"),
                session.get("interaction_type"),
                session.get("subject_type"),
                session.get("subject_id"),
                session.get("namespace_key"),
                compression["summary"],
                None,
                None,
                compression["source_count"],
                json_dumps([]),
                json_dumps([]),
                json_dumps(list(compression.get("highlights", []))),
                json_dumps([]),
                int(compression.get("estimated_tokens", 0)),
                json_dumps([]),
                1,
                json_dumps(
                    {
                        "compression_domain": "short_term",
                        "kept_ids": list(compression.get("kept_ids", [])),
                        "source_count": compression["source_count"],
                    }
                ),
                now,
                now,
            ),
        )
        return self.memory.get_snapshot(snapshot_id) or {"id": snapshot_id, "summary": compression["summary"]}

    def _filter_memory_items(
        self,
        items: list[dict[str, Any]],
        *,
        include_generated: bool,
        include_inactive: bool,
    ) -> list[dict[str, Any]]:
        filtered = list(items)
        if not include_inactive:
            filtered = [item for item in filtered if item.get("status") == "active"]
        if not include_generated:
            filtered = [item for item in filtered if not self._is_generated_memory(item)]
        return filtered

    def _is_generated_memory(self, item: dict[str, Any]) -> bool:
        metadata = dict(item.get("metadata") or {})
        compression = metadata.get("compression")
        return bool(item.get("source") == "auto_compression" or metadata.get("generated") or isinstance(compression, dict))

    def _memory_scope_kwargs(self, scope_kwargs: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "user_id",
            "owner_agent_id",
            "subject_type",
            "subject_id",
            "interaction_type",
            "session_id",
            "platform_id",
            "workspace_id",
            "team_id",
            "project_id",
            "namespace_key",
        }
        return {key: value for key, value in scope_kwargs.items() if key in allowed}

    def _memory_compression_records(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": item["id"],
                "text": str(item.get("summary") or item.get("text") or ""),
                "score": float(item.get("importance", 0.5) or 0.5),
            }
            for item in items
            if str(item.get("summary") or item.get("text") or "").strip()
        ]

    def _require_memory_scope(self, memory_id: str, memory_scope: str) -> dict[str, Any]:
        memory = self.memory.get(memory_id)
        if memory is None:
            raise ValueError(f"Memory `{memory_id}` does not exist.")
        if memory.get("scope") != memory_scope:
            raise ValueError(f"Memory `{memory_id}` is not in `{memory_scope}` scope.")
        return memory

    def _resolve_memory_scope(self, scope_kwargs: dict[str, Any]) -> dict[str, Any]:
        return self.memory._resolve_scope(
            user_id=scope_kwargs.get("user_id"),
            owner_agent_id=scope_kwargs.get("owner_agent_id"),
            subject_type=scope_kwargs.get("subject_type"),
            subject_id=scope_kwargs.get("subject_id"),
            interaction_type=scope_kwargs.get("interaction_type"),
            platform_id=scope_kwargs.get("platform_id"),
            workspace_id=scope_kwargs.get("workspace_id"),
            team_id=scope_kwargs.get("team_id"),
            project_id=scope_kwargs.get("project_id"),
            namespace_key=scope_kwargs.get("namespace_key"),
        )

    def _resolve_archive_scope(self, scope_kwargs: dict[str, Any]) -> dict[str, Any]:
        return self.memory._resolve_scope(
            user_id=scope_kwargs.get("user_id"),
            owner_agent_id=scope_kwargs.get("owner_agent_id"),
            subject_type=scope_kwargs.get("subject_type"),
            subject_id=scope_kwargs.get("subject_id"),
            interaction_type=scope_kwargs.get("interaction_type"),
            platform_id=scope_kwargs.get("platform_id"),
            workspace_id=scope_kwargs.get("workspace_id"),
            team_id=scope_kwargs.get("team_id"),
            project_id=scope_kwargs.get("project_id"),
            namespace_key=scope_kwargs.get("namespace_key"),
            global_scope=bool(scope_kwargs.get("global_scope", False)),
        )

    def _find_generated_memory(self, memory_scope: str, scope: dict[str, Any]) -> dict[str, Any] | None:
        filters = ["status = 'active'", "source = 'auto_compression'", "scope = ?"]
        params: list[Any] = [memory_scope]
        for column in ("owner_agent_id", "subject_type", "subject_id", "interaction_type", "namespace_key"):
            value = scope.get(column)
            if value is None:
                filters.append(f"{column} IS NULL")
            else:
                filters.append(f"{column} = ?")
                params.append(value)
        if memory_scope == str(MemoryScope.SESSION):
            if scope.get("session_id") is None:
                filters.append("session_id IS NULL")
            else:
                filters.append("session_id = ?")
                params.append(scope["session_id"])
        row = self.memory.db.fetch_one(
            f"SELECT * FROM memories WHERE {' AND '.join(filters)} ORDER BY updated_at DESC LIMIT 1",
            tuple(params),
        )
        return _deserialize_row(row)

    def _upsert_generated_memory(
        self,
        *,
        memory_scope: str,
        domain: str,
        scope: dict[str, Any],
        compression: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = merge_metadata(
            self.memory._scope_metadata(scope),
            {
                "generated": True,
                "compression": {
                    "domain": domain,
                    "source_count": compression["source_count"],
                    "source_ids": list(compression.get("kept_ids", [])),
                    "threshold_chars": compression["threshold_chars"],
                    "total_chars": compression["total_chars"],
                    "provider": compression.get("provider"),
                },
            },
        )
        existing = self._find_generated_memory(memory_scope, scope)
        if existing is not None:
            return self.memory.update(
                existing["id"],
                text=compression["summary"],
                metadata=metadata,
                importance=max(0.72, float(existing.get("importance", 0.72))),
                status="active",
            )
        return self.memory.memory_store(
            compression["summary"],
            user_id=scope.get("user_id"),
            owner_agent_id=scope.get("owner_agent_id"),
            subject_type=scope.get("subject_type"),
            subject_id=scope.get("subject_id"),
            interaction_type=scope.get("interaction_type"),
            namespace_key=scope.get("namespace_key"),
            session_id=scope.get("session_id"),
            long_term=memory_scope == str(MemoryScope.LONG_TERM),
            source="auto_compression",
            metadata=metadata,
            memory_type=str(MemoryType.RELATIONSHIP_SUMMARY),
            importance=0.82,
        )

    def _upsert_generated_archive(self, scope: dict[str, Any], compression: dict[str, Any]) -> dict[str, Any]:
        existing = self.memory.db.fetch_one(
            """
            SELECT * FROM archive_units
            WHERE source_type = 'archive_compaction'
              AND COALESCE(owner_agent_id, '') = COALESCE(?, '')
              AND COALESCE(subject_type, '') = COALESCE(?, '')
              AND COALESCE(subject_id, '') = COALESCE(?, '')
              AND COALESCE(interaction_type, '') = COALESCE(?, '')
              AND COALESCE(namespace_key, '') = COALESCE(?, '')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                scope.get("owner_agent_id"),
                scope.get("subject_type"),
                scope.get("subject_id"),
                scope.get("interaction_type"),
                scope.get("namespace_key"),
            ),
        )
        metadata = {
            "generated": True,
            "compression": {
                "domain": "archive",
                "source_count": compression["source_count"],
                "source_ids": list(compression.get("kept_ids", [])),
                "threshold_chars": compression["threshold_chars"],
                "total_chars": compression["total_chars"],
                "provider": compression.get("provider"),
            },
            **self.memory._scope_metadata(scope),
        }
        if existing is not None:
            return self.update_archive_memory(existing["id"], summary=compression["summary"], metadata=metadata, source_type="archive_compaction")
        return self.save_archive_memory(
            compression["summary"],
            content="\n".join(compression.get("highlights", [])),
            user_id=scope.get("user_id"),
            owner_agent_id=scope.get("owner_agent_id"),
            subject_type=scope.get("subject_type"),
            subject_id=scope.get("subject_id"),
            interaction_type=scope.get("interaction_type"),
            namespace_key=scope.get("namespace_key"),
            metadata=metadata,
            source_type="archive_compaction",
            domain="archive",
        )

    def _replace_document_chunks(
        self,
        document: dict[str, Any],
        *,
        text: str,
        title: str,
        scope: dict[str, Any],
        metadata: dict[str, Any] | None,
        now: str,
    ) -> None:
        document_id = document["id"]
        source_id = document["source_id"]
        chunk_size = int(self.memory.config.memory_policy.chunk_size)
        chunk_overlap = int(self.memory.config.memory_policy.chunk_overlap)
        for chunk in document.get("chunks", []):
            self.memory.db.execute("DELETE FROM knowledge_chunk_index WHERE record_id = ?", (chunk["id"],))
            self.memory.db.execute("DELETE FROM semantic_index_cache WHERE record_id = ?", (chunk["id"],))
            self.memory.vector_index.delete("knowledge_chunk_index", chunk["id"])
            self.memory.graph_store.delete_reference(chunk["id"])
        self.memory.db.execute("DELETE FROM citations WHERE document_id = ?", (document_id,))
        self.memory.db.execute("DELETE FROM document_chunks WHERE document_id = ?", (document_id,))
        stored = self.memory.object_store.put_text(
            text,
            object_type="knowledge",
            suffix=".txt",
            prefix=self.memory._object_store_prefix(scope, "knowledge"),
        )
        object_row = self.memory._persist_object(
            stored,
            mime_type="text/plain",
            metadata={"document_id": document_id, **self.memory._scope_metadata(scope), **dict(metadata or {})},
        )
        version_count = int(
            self.memory.db.fetch_one("SELECT COUNT(*) AS count FROM document_versions WHERE document_id = ?", (document_id,)).get("count", 0)
        )
        version_id = make_id("docver")
        self.memory.db.execute(
            """
            INSERT INTO document_versions(id, document_id, version_label, object_id, checksum, size_bytes, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_id,
                document_id,
                f"v{version_count + 1}",
                object_row["id"],
                object_row["checksum"],
                object_row["size_bytes"],
                json_dumps(metadata or {}),
                now,
            ),
        )
        chunks = chunk_text(text, chunk_size=chunk_size, overlap=chunk_overlap)
        for index, chunk in enumerate(chunks):
            chunk_id = make_id("chunk")
            chunk_metadata = {"chunk_index": index, **self.memory._scope_metadata(scope), **dict(metadata or {})}
            self.memory.db.execute(
                """
                INSERT INTO document_chunks(id, document_id, version_id, chunk_index, content, tokens, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (chunk_id, document_id, version_id, index, chunk, len(chunk), json_dumps(chunk_metadata), now),
            )
            self.memory.db.execute(
                """
                INSERT INTO citations(id, document_id, version_id, chunk_id, label, location, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (make_id("cite"), document_id, version_id, chunk_id, f"{title}#{index + 1}", f"chunk:{index}", json_dumps(chunk_metadata), now),
            )
            self.memory._index_knowledge_chunk(
                {
                    "id": chunk_id,
                    "document_id": document_id,
                    "source_id": source_id,
                    "owner_agent_id": scope.get("owner_agent_id"),
                    "source_subject_type": scope.get("subject_type"),
                    "source_subject_id": scope.get("subject_id"),
                    "namespace_key": scope.get("namespace_key"),
                    "title": title,
                    "content": chunk,
                    "metadata": chunk_metadata,
                    "updated_at": now,
                }
            )

    def _write_skill_version(
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
        now: str,
    ) -> None:
        existing_count = int(
            self.memory.db.fetch_one("SELECT COUNT(*) AS count FROM skill_versions WHERE skill_id = ?", (skill_id,)).get("count", 0)
        )
        resolved_version = version or f"v{existing_count + 1}"
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
        stored = self.memory.object_store.put_text(
            json_dumps(asset_payload),
            object_type="skills",
            suffix=".json",
            prefix=self.memory._object_store_prefix(scope, "skill"),
        )
        object_row = self.memory._persist_object(
            stored,
            mime_type="application/json",
            metadata={"skill_id": skill_id, **self.memory._scope_metadata(scope), **metadata},
        )
        version_id = make_id("skillver")
        workflow_text = workflow if isinstance(workflow, str) else json_dumps(workflow or {})
        self.memory.db.execute(
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
            self.memory.db.execute(
                """
                INSERT INTO skill_bindings(id, skill_version_id, tool_name, binding_type, config, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (make_id("bind"), version_id, tool_name, "tool", json_dumps({}), now),
            )
        for test_case in tests:
            self.memory.db.execute(
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
        self.memory._index_skill(
            {
                "record_id": version_id,
                "skill_id": skill_id,
                "version": resolved_version,
                "owner_agent_id": scope.get("owner_agent_id"),
                "source_subject_type": scope.get("subject_type"),
                "source_subject_id": scope.get("subject_id"),
                "namespace_key": scope.get("namespace_key"),
                "name": name,
                "description": description,
                "text": "\n".join(part for part in [name, description, prompt_template or "", workflow_text, " ".join(topics), " ".join(tools)] if part),
                "metadata": metadata or {},
                "updated_at": now,
            }
        )
