from __future__ import annotations

from pathlib import Path
from typing import Any

from aimemory.catalog.sqlite_store import SQLiteCatalog
from aimemory.config import MemoryConfig
from aimemory.errors import InvalidScope, RecordNotFound
from aimemory.hotstore.lmdb_store import LMDBHotStore
from aimemory.ids import make_id
from aimemory.pipeline.lifecycle import (
    VERSIONED_KINDS,
    compute_checksum,
    compute_fingerprint,
    derive_fact_key,
    normalize_text,
    now_ms,
    split_text,
    summarize_text,
)
from aimemory.serialization import json_loads
from aimemory.scope import Scope


class MemoryWritePath:
    def __init__(self, *, config: MemoryConfig, catalog: SQLiteCatalog, hotstore: LMDBHotStore):
        self.config = config
        self.catalog = catalog
        self.hotstore = hotstore

    def put(
        self,
        *,
        scope: Scope,
        text: str,
        kind: str = "fact",
        layer: str = "longterm",
        tier: str = "active",
        importance: float = 0.5,
        confidence: float = 0.7,
        fact_key: str | None = None,
        metadata: dict[str, Any] | None = None,
        source_type: str | None = None,
        source_ref: str | None = None,
    ) -> dict[str, Any]:
        return self._write_many(
            scope=scope,
            items=[
                {
                    "text": text,
                    "kind": kind,
                    "layer": layer,
                    "tier": tier,
                    "importance": importance,
                    "confidence": confidence,
                    "fact_key": fact_key,
                    "metadata": metadata,
                    "source_type": source_type,
                    "source_ref": source_ref,
                }
            ],
        )[0]

    def put_many(self, *, scope: Scope, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self._write_many(scope=scope, items=items)

    def ingest_records(self, *, scope: Scope, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        items = [dict(record) for record in records]
        return self._write_many(scope=scope, items=items)

    def ingest_jsonl(self, *, scope: Scope, path: str | Path) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        file_path = Path(path)
        with file_path.open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                record = json_loads(line, None)
                if not isinstance(record, dict):
                    raise ValueError(f"Invalid JSON object at line {line_no} in {file_path}")
                records.append(record)
        return self.ingest_records(scope=scope, records=records)

    def _write_many(self, *, scope: Scope, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            return []
        prepared = [self._prepare_draft(scope, dict(item)) for item in items]
        records: list[dict[str, Any]] = []
        fingerprint_updates: dict[str, str] = {}
        mirrored_jobs: list[dict[str, Any]] = []
        working_items: list[dict[str, Any]] = []
        with self.catalog.transaction():
            for draft in prepared:
                record, effect = self._apply_prepared_draft(scope=scope, draft=draft)
                records.append(record)
                fingerprint_updates[effect["fingerprint"]] = effect["head_id"]
                mirrored_jobs.extend(effect["mirrored_jobs"])
                working_items.append(self._make_working_item(record))
        self.hotstore.put_fingerprints(fingerprint_updates)
        self.hotstore.clear_query_cache(scope.key)
        self.hotstore.mirror_jobs(mirrored_jobs)
        self.hotstore.append_working_many(scope.key, working_items, self.config.working_memory_limit)
        return records

    def _prepare_draft(self, scope: Scope, payload: dict[str, Any]) -> dict[str, Any]:
        text = payload.get("text")
        if not isinstance(text, str):
            raise ValueError("Memory draft text must be a string")
        normalized = normalize_text(text)
        if not normalized:
            raise ValueError("Memory draft text must not be empty")
        kind = str(payload.get("kind") or "fact")
        layer = str(payload.get("layer") or "longterm")
        tier = str(payload.get("tier") or "active")
        importance = float(payload.get("importance", 0.5))
        confidence = float(payload.get("confidence", 0.7))
        fact_key = payload.get("fact_key") or derive_fact_key(kind, normalized)
        metadata = dict(payload.get("metadata") or {})
        metadata.update(
            {
                "tenant_id": scope.tenant_id,
                "workspace_id": scope.workspace_id,
                "project_id": scope.project_id,
                "user_id": scope.user_id,
                "agent_id": scope.agent_id,
                "session_id": scope.session_id,
                "run_id": scope.run_id,
                "namespace": scope.namespace,
                "visibility": scope.visibility,
            }
        )
        checksum = compute_checksum(normalized)
        return {
            "text": normalized,
            "kind": kind,
            "layer": layer,
            "tier": tier,
            "importance": importance,
            "confidence": confidence,
            "fact_key": fact_key,
            "metadata": metadata,
            "source_type": payload.get("source_type"),
            "source_ref": payload.get("source_ref"),
            "checksum": checksum,
            "fingerprint": compute_fingerprint(scope.key, kind, checksum),
            "abstract": summarize_text(normalized, limit=160),
            "overview": "- " + summarize_text(normalized, limit=240),
            "chunk_strategy": "single_chunk" if len(normalized) <= self.config.chunk_size else "sliding_window",
            "chunks": split_text(normalized, chunk_size=self.config.chunk_size, chunk_overlap=self.config.chunk_overlap),
        }

    def _apply_prepared_draft(self, *, scope: Scope, draft: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        now = now_ms()
        existing_head_id = self.hotstore.get_fingerprint(draft["fingerprint"])
        existing = self.catalog.get_head(existing_head_id) if existing_head_id else None
        if existing is None:
            existing = self.catalog.find_current_by_checksum(scope.key, draft["kind"], draft["checksum"])
        if existing is not None and existing["state"] == "active":
            self.catalog.touch_head(existing["head_id"], now)
            self.catalog.add_history_event(
                scope_key=scope.key,
                head_id=existing["head_id"],
                version_id=existing["version_id"],
                event_type="deduplicated",
                payload={"checksum": draft["checksum"]},
                created_at=now,
            )
            record = self.catalog.get_head(existing["head_id"])
            return record, {
                "fingerprint": draft["fingerprint"],
                "head_id": existing["head_id"],
                "mirrored_jobs": [],
            }

        mirrored_jobs: list[dict[str, Any]] = []
        versioned_target = None
        if draft["kind"] in VERSIONED_KINDS and draft["fact_key"]:
            versioned_target = self.catalog.find_current_by_fact_key(scope.key, draft["kind"], draft["fact_key"])

        if versioned_target is None:
            provisional_version_id = make_id("verref")
            head_id = self.catalog.create_head(
                scope=scope,
                kind=draft["kind"],
                layer=draft["layer"],
                tier=draft["tier"],
                state="active",
                fact_key=draft["fact_key"],
                version_id=provisional_version_id,
                importance=draft["importance"],
                confidence=draft["confidence"],
                now=now,
                metadata=draft["metadata"],
            )
            version_id = self.catalog.create_version(
                head_id=head_id,
                version_no=1,
                text=draft["text"],
                abstract=draft["abstract"],
                overview=draft["overview"],
                checksum=draft["checksum"],
                change_type="create",
                valid_from=now,
                source_type=draft["source_type"],
                source_ref=draft["source_ref"],
                embedding_model=self.config.embedding_model,
                chunk_strategy=draft["chunk_strategy"],
                created_by=draft["source_type"],
                created_at=now,
                metadata=draft["metadata"],
            )
            self.catalog._conn.execute(
                "UPDATE memory_heads SET current_version_id = ? WHERE head_id = ?",
                (version_id, head_id),
            )
            chunk_ids = self.catalog.create_chunks(
                head_id=head_id,
                version_id=version_id,
                scope_key=scope.key,
                chunks=draft["chunks"],
                created_at=now,
            )
            mirrored_jobs.extend(self._build_upsert_jobs(scope.key, chunk_ids, now))
            self.catalog.add_history_event(
                scope_key=scope.key,
                head_id=head_id,
                version_id=version_id,
                event_type="created",
                payload={"kind": draft["kind"], "layer": draft["layer"]},
                created_at=now,
            )
        else:
            head_id = versioned_target["head_id"]
            previous_version_id = versioned_target["version_id"]
            previous_chunks = self.catalog.list_chunk_ids_for_version(previous_version_id)
            version_id = self.catalog.create_version(
                head_id=head_id,
                version_no=self.catalog.next_version_no(head_id),
                text=draft["text"],
                abstract=draft["abstract"],
                overview=draft["overview"],
                checksum=draft["checksum"],
                change_type="supersede",
                valid_from=now,
                source_type=draft["source_type"],
                source_ref=draft["source_ref"],
                embedding_model=self.config.embedding_model,
                chunk_strategy=draft["chunk_strategy"],
                created_by=draft["source_type"],
                created_at=now,
                metadata=draft["metadata"],
            )
            self.catalog.supersede_head(
                head_id=head_id,
                previous_version_id=previous_version_id,
                new_version_id=version_id,
                tier=draft["tier"],
                importance=draft["importance"],
                confidence=draft["confidence"],
                now=now,
            )
            chunk_ids = self.catalog.create_chunks(
                head_id=head_id,
                version_id=version_id,
                scope_key=scope.key,
                chunks=draft["chunks"],
                created_at=now,
            )
            mirrored_jobs.extend(self._build_upsert_jobs(scope.key, chunk_ids, now))
            if previous_chunks:
                job_id = self.catalog.enqueue_job(
                    entity_type="chunk",
                    entity_id=head_id,
                    op_type="delete_vector",
                    payload={"chunk_ids": previous_chunks, "scope_key": scope.key},
                    now=now,
                )
                mirrored_jobs.append(
                    {
                        "job_id": job_id,
                        "op_type": "delete_vector",
                        "scope_key": scope.key,
                        "chunk_ids": previous_chunks,
                    }
                )
            self.catalog.add_link(
                src_head_id=head_id,
                dst_head_id=head_id,
                relation_type="supersedes",
                created_at=now,
                metadata={"previous_version_id": previous_version_id, "new_version_id": version_id},
            )
            self.catalog.add_history_event(
                scope_key=scope.key,
                head_id=head_id,
                version_id=version_id,
                event_type="superseded",
                payload={"previous_version_id": previous_version_id},
                created_at=now,
            )

        record = self.catalog.get_head(head_id)
        return record, {
            "fingerprint": draft["fingerprint"],
            "head_id": head_id,
            "mirrored_jobs": mirrored_jobs,
        }

    def _build_upsert_jobs(self, scope_key: str, chunk_ids: list[str], now: int) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        for chunk_id in chunk_ids:
            job_id = self.catalog.enqueue_job(
                entity_type="chunk",
                entity_id=chunk_id,
                op_type="upsert_vector",
                payload={"chunk_id": chunk_id, "scope_key": scope_key},
                now=now,
            )
            jobs.append({"job_id": job_id, "op_type": "upsert_vector", "scope_key": scope_key, "chunk_id": chunk_id})
        return jobs

    def delete(self, *, scope: Scope, head_id: str) -> dict[str, Any]:
        now = now_ms()
        record = self.catalog.get_head(head_id)
        if record is None:
            raise RecordNotFound(f"Memory head not found: {head_id}")
        if record["scope_key"] != scope.key:
            raise InvalidScope(f"Head {head_id} is outside the requested scope")
        mirrored_jobs: list[dict[str, Any]] = []
        with self.catalog.transaction():
            deleted = self.catalog.soft_delete(head_id, now)
            chunk_ids = self.catalog.list_chunk_ids_for_version(record["version_id"])
            if chunk_ids:
                job_id = self.catalog.enqueue_job(
                    entity_type="chunk",
                    entity_id=head_id,
                    op_type="delete_vector",
                    payload={"chunk_ids": chunk_ids, "scope_key": record["scope_key"]},
                    now=now,
                )
                mirrored_jobs.append({"job_id": job_id, "op_type": "delete_vector", "scope_key": record["scope_key"], "chunk_ids": chunk_ids})
            self.catalog.add_history_event(
                scope_key=record["scope_key"],
                head_id=head_id,
                version_id=record["version_id"],
                event_type="deleted",
                payload={},
                created_at=now,
            )
        self.hotstore.clear_query_cache(record["scope_key"])
        for job in mirrored_jobs:
            self.hotstore.mirror_job(job["job_id"], job)
        return deleted

    def restore(self, *, scope: Scope, head_id: str) -> dict[str, Any]:
        now = now_ms()
        record = self.catalog.get_head(head_id)
        if record is None:
            raise RecordNotFound(f"Memory head not found: {head_id}")
        if record["scope_key"] != scope.key:
            raise InvalidScope(f"Head {head_id} is outside the requested scope")
        mirrored_jobs: list[dict[str, Any]] = []
        with self.catalog.transaction():
            restored = self.catalog.restore(head_id, now)
            chunk_ids = self.catalog.list_chunk_ids_for_version(record["version_id"])
            for chunk_id in chunk_ids:
                job_id = self.catalog.enqueue_job(
                    entity_type="chunk",
                    entity_id=chunk_id,
                    op_type="upsert_vector",
                    payload={"chunk_id": chunk_id, "scope_key": record["scope_key"]},
                    now=now,
                )
                mirrored_jobs.append({"job_id": job_id, "op_type": "upsert_vector", "scope_key": record["scope_key"], "chunk_id": chunk_id})
            self.catalog.add_history_event(
                scope_key=record["scope_key"],
                head_id=head_id,
                version_id=record["version_id"],
                event_type="restored",
                payload={},
                created_at=now,
            )
        self.hotstore.clear_query_cache(record["scope_key"])
        for job in mirrored_jobs:
            self.hotstore.mirror_job(job["job_id"], job)
        self._append_hot_working(scope, restored)
        return restored

    @staticmethod
    def _make_working_item(record: dict[str, Any] | None) -> dict[str, Any]:
        if record is None:
            return {}
        return {
            "head_id": record["head_id"],
            "version_id": record["version_id"],
            "kind": record["kind"],
            "tier": record["tier"],
            "content": record["text"],
            "abstract": record["abstract"],
            "metadata": record["metadata"],
            "source": "memory",
        }

    def _append_hot_working(self, scope: Scope, record: dict[str, Any] | None) -> None:
        if record is None:
            return
        self.hotstore.append_working(scope.key, self._make_working_item(record), self.config.working_memory_limit)
