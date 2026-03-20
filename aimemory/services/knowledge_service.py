from __future__ import annotations

import hashlib
from typing import Any

from aimemory.algorithms.retrieval import estimate_tokens
from aimemory.algorithms.segmentation import chunk_text_units
from aimemory.core.text import extract_keywords
from aimemory.core.utils import json_dumps, make_id, utcnow_iso
from aimemory.domains.knowledge.models import KnowledgeSourceType
from aimemory.services.base import ServiceBase
from aimemory.storage.policy import build_inline_excerpt, normalize_raw_store_policy, payload_size_bytes, should_externalize_text


class KnowledgeService(ServiceBase):
    def _chunk_title(self, base_title: str | None, metadata: dict[str, Any] | None = None) -> str:
        title = str(base_title or "").strip()
        section_label = str((metadata or {}).get("section_label") or "").strip()
        if not section_label:
            return title
        if not title:
            return section_label
        return f"{title} | {section_label}"

    def create_source(
        self,
        name: str,
        source_type: str = KnowledgeSourceType.MANUAL,
        uri: str | None = None,
        metadata: dict[str, Any] | None = None,
        source_id: str | None = None,
    ) -> dict[str, Any]:
        source_id = source_id or make_id("source")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO knowledge_sources(id, name, source_type, uri, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                source_type = excluded.source_type,
                uri = excluded.uri,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            (source_id, name, str(source_type), uri, json_dumps(metadata or {}), now, now),
        )
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM knowledge_sources WHERE id = ?", (source_id,)))

    def ingest_text(
        self,
        title: str,
        text: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        source_subject_type: str | None = None,
        source_subject_id: str | None = None,
        source_id: str | None = None,
        source_name: str = "manual",
        version_label: str = "v1",
        metadata: dict[str, Any] | None = None,
        chunk_size: int = 500,
        overlap: int = 80,
        document_id: str | None = None,
        kb_namespace: str | None = None,
        raw_store_policy: str | None = None,
    ) -> dict[str, Any]:
        source = self._resolve_source(source_id=source_id, source_name=source_name)
        document_id = document_id or make_id("doc")
        job_id = make_id("ingest")
        now = utcnow_iso()
        merged_metadata = {
            **dict(metadata or {}),
            "owner_agent_id": owner_agent_id,
            "source_subject_type": source_subject_type,
            "source_subject_id": source_subject_id,
        }
        storage_policy = normalize_raw_store_policy(raw_store_policy, default=self.config.knowledge_raw_store_policy)
        externalize = should_externalize_text(
            text,
            policy=storage_policy,
            inline_char_limit=int(self.config.knowledge_inline_char_limit),
        )
        inline_text = None if externalize else text
        inline_excerpt = build_inline_excerpt(text)
        raw_payload_bytes = payload_size_bytes(text)

        self.db.execute(
            """
            INSERT INTO ingestion_jobs(id, source_id, document_id, status, message, metadata, created_at, updated_at)
            VALUES (?, ?, ?, 'running', ?, ?, ?, ?)
            """,
            (job_id, source["id"], document_id, "ingesting", json_dumps(merged_metadata), now, now),
        )
        self.db.execute(
            """
            INSERT INTO documents(
                id, source_id, title, user_id, owner_agent_id, kb_namespace, source_subject_type, source_subject_id,
                inline_text, inline_excerpt, storage_policy, storage_ref, payload_bytes, external_id, status, metadata,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                source_id = excluded.source_id,
                title = excluded.title,
                user_id = excluded.user_id,
                owner_agent_id = excluded.owner_agent_id,
                kb_namespace = excluded.kb_namespace,
                source_subject_type = excluded.source_subject_type,
                source_subject_id = excluded.source_subject_id,
                inline_text = excluded.inline_text,
                inline_excerpt = excluded.inline_excerpt,
                storage_policy = excluded.storage_policy,
                storage_ref = excluded.storage_ref,
                payload_bytes = excluded.payload_bytes,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            (
                document_id,
                source["id"],
                title,
                user_id,
                owner_agent_id,
                kb_namespace or owner_agent_id,
                source_subject_type,
                source_subject_id,
                inline_text,
                inline_excerpt,
                storage_policy,
                None,
                raw_payload_bytes,
                document_id,
                json_dumps(merged_metadata),
                now,
                now,
            ),
        )

        object_row = None
        if externalize:
            stored = self.object_store.put_text(text, object_type="knowledge", suffix=".txt")
            object_row = self._persist_object(stored, mime_type="text/plain", metadata={"document_id": document_id, **merged_metadata})
            self.db.execute("UPDATE documents SET storage_ref = ? WHERE id = ?", (object_row["object_key"], document_id))
        version_id = make_id("docver")
        checksum = hashlib.sha256(text.encode("utf-8")).hexdigest()
        self.db.execute(
            """
            INSERT INTO document_versions(id, document_id, version_label, object_id, checksum, size_bytes, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_id,
                document_id,
                version_label,
                object_row["id"] if object_row is not None else None,
                checksum,
                raw_payload_bytes,
                json_dumps(merged_metadata),
                now,
            ),
        )

        for index, chunk in enumerate(chunk_text_units(text, source_id=document_id, chunk_size=chunk_size, overlap=overlap)):
            chunk_id = make_id("chunk")
            chunk_metadata = {"chunk_index": index, **chunk.metadata, **merged_metadata}
            self.db.execute(
                """
                INSERT INTO document_chunks(id, document_id, version_id, chunk_index, content, tokens, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (chunk_id, document_id, version_id, index, chunk.text, estimate_tokens(chunk.text), json_dumps(chunk_metadata), now),
            )
            citation_id = make_id("cite")
            self.db.execute(
                """
                INSERT INTO citations(id, document_id, version_id, chunk_id, label, location, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (citation_id, document_id, version_id, chunk_id, f"{title}#{index + 1}", f"chunk:{index}", json_dumps(chunk_metadata), now),
            )
            self.projection.enqueue(
                topic="knowledge.index",
                entity_type="knowledge_chunk",
                entity_id=chunk_id,
                action="upsert",
                payload={
                    "record_id": chunk_id,
                    "document_id": document_id,
                    "source_id": source["id"],
                    "owner_agent_id": owner_agent_id,
                    "source_subject_type": source_subject_type,
                    "source_subject_id": source_subject_id,
                    "title": self._chunk_title(title, chunk_metadata),
                    "text": chunk.text,
                    "keywords": extract_keywords(" ".join(part for part in [self._chunk_title(title, chunk_metadata), chunk.text] if part)),
                    "metadata": chunk_metadata,
                    "updated_at": now,
                },
            )

        self.db.execute(
            "UPDATE ingestion_jobs SET status = 'completed', message = ?, updated_at = ? WHERE id = ?",
            ("completed", now, job_id),
        )
        if self.config.auto_project and self.projection is not None:
            self.projection.project_pending()
        document = self.get_document(document_id)
        assert document is not None
        return document

    def get_document(self, document_id: str) -> dict[str, Any] | None:
        document = self._deserialize_row(self.db.fetch_one("SELECT * FROM documents WHERE id = ?", (document_id,)))
        if document is None:
            return None
        document["versions"] = self._deserialize_rows(
            self.db.fetch_all("SELECT * FROM document_versions WHERE document_id = ? ORDER BY created_at DESC", (document_id,))
        )
        document["chunks"] = self._deserialize_rows(
            self.db.fetch_all("SELECT * FROM document_chunks WHERE document_id = ? ORDER BY chunk_index ASC", (document_id,))
        )
        document["text"] = self._document_text(document)
        return document

    def list_documents(
        self,
        source_id: str | None = None,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
    ) -> dict[str, Any]:
        filters = ["1 = 1"]
        params: list[Any] = []
        if source_id:
            filters.append("source_id = ?")
            params.append(source_id)
        if user_id:
            filters.append("user_id = ?")
            params.append(user_id)
        if owner_agent_id:
            filters.append("(owner_agent_id = ? OR owner_agent_id IS NULL)")
            params.append(owner_agent_id)
        rows = self.db.fetch_all(
            f"SELECT * FROM documents WHERE {' AND '.join(filters)} ORDER BY updated_at DESC",
            tuple(params),
        )
        return {"results": self._deserialize_rows(rows)}

    def get_document_text(self, document_id: str) -> str:
        document = self.get_document(document_id)
        if document is None:
            raise ValueError(f"Document `{document_id}` does not exist.")
        return self._document_text(document)

    def _document_text(self, document: dict[str, Any]) -> str:
        inline_text = str(document.get("inline_text") or "")
        if inline_text:
            return inline_text
        storage_ref = str(document.get("storage_ref") or "")
        if storage_ref:
            return self.object_store.get_text(storage_ref)
        latest_version = document["versions"][0] if document.get("versions") else None
        if latest_version is None or not latest_version.get("object_id"):
            return "\n".join(chunk.get("content", "") for chunk in document.get("chunks", []))
        obj = self._deserialize_row(self.db.fetch_one("SELECT * FROM objects WHERE id = ?", (latest_version["object_id"],)))
        if obj is None:
            return "\n".join(chunk.get("content", "") for chunk in document.get("chunks", []))
        return self.object_store.get_text(obj["object_key"])

    def _resolve_source(self, source_id: str | None, source_name: str) -> dict[str, Any]:
        if source_id:
            source = self._deserialize_row(self.db.fetch_one("SELECT * FROM knowledge_sources WHERE id = ?", (source_id,)))
            if source is None:
                raise ValueError(f"Knowledge source `{source_id}` does not exist.")
            return source
        existing = self.db.fetch_one("SELECT * FROM knowledge_sources WHERE name = ? ORDER BY created_at ASC LIMIT 1", (source_name,))
        if existing is not None:
            source = self._deserialize_row(existing)
            assert source is not None
            return source
        created = self.create_source(source_name)
        return created
