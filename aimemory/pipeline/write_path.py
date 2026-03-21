from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from aimemory.catalog.sqlite_store import SQLiteCatalog
from aimemory.config import MemoryConfig
from aimemory.errors import InvalidScope, RecordNotFound
from aimemory.hotstore.lmdb_store import LMDBHotStore
from aimemory.ids import make_id
from aimemory.outbox import OUTBOX_DELETE_VECTOR, OUTBOX_REBUILD_VECTOR, OUTBOX_UPSERT_VECTOR
from aimemory.pipeline.lifecycle import (
    compute_checksum,
    compute_fingerprint,
    derive_fact_key,
    normalize_text,
    now_ms,
    split_text,
    summarize_text,
    uses_version_chain,
    vector_score,
)
from aimemory.serialization import json_loads
from aimemory.scope import Scope
from aimemory.state import HEAD_STATE_ACTIVE, HEAD_STATE_ARCHIVED, HEAD_STATE_DELETED
from aimemory.vector.embeddings import Embedder
from aimemory.vector.lancedb_store import LanceVectorStore


class MemoryWritePath:
    def __init__(
        self,
        *,
        config: MemoryConfig,
        catalog: SQLiteCatalog,
        hotstore: LMDBHotStore,
        vector_store: LanceVectorStore,
        embedder: Embedder,
    ):
        self.config = config
        self.catalog = catalog
        self.hotstore = hotstore
        self.vector_store = vector_store
        self.embedder = embedder

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
        vector: list[float] | None = None,
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
                    "vector": vector,
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
        self._hydrate_semantic_vectors(prepared)
        records: list[dict[str, Any]] = []
        fingerprint_updates: dict[str, str] = {}
        embedding_updates: dict[str, list[float]] = {}
        mirrored_jobs: list[dict[str, Any]] = []
        working_items: list[dict[str, Any]] = []
        semantic_candidates: list[dict[str, Any]] = []
        with self.catalog.transaction():
            for draft in prepared:
                record, effect = self._apply_prepared_draft(
                    scope=scope,
                    draft=draft,
                    semantic_candidates=semantic_candidates,
                )
                records.append(record)
                fingerprint_updates[effect["fingerprint"]] = effect["head_id"]
                embedding_updates.update(effect["embedding_updates"])
                mirrored_jobs.extend(effect["mirrored_jobs"])
                if effect["semantic_candidate"] is not None:
                    semantic_candidates.append(effect["semantic_candidate"])
                working_items.append(self._make_working_item(record))
        self.hotstore.put_fingerprints(fingerprint_updates)
        self.hotstore.put_embeddings(embedding_updates)
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
        vector = payload.get("vector")
        if vector is not None:
            if not isinstance(vector, list) or len(vector) != self.config.vector_dim:
                raise ValueError(f"Memory draft vector must be a list[{self.config.vector_dim}]")
            vector = [float(value) for value in vector]
        fact_key = payload.get("fact_key")
        if fact_key is None and (uses_version_chain(kind, procedure_version_mode=self.config.procedure_version_mode) or kind == "procedure"):
            fact_key = derive_fact_key(kind, normalized)
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
            "vector": vector,
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

    def _apply_prepared_draft(
        self,
        *,
        scope: Scope,
        draft: dict[str, Any],
        semantic_candidates: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
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
                "embedding_updates": {},
                "mirrored_jobs": [],
                "semantic_candidate": None,
            }

        semantic_duplicate = self._find_semantic_duplicate(
            scope=scope,
            draft=draft,
            semantic_candidates=semantic_candidates,
        )
        if semantic_duplicate is not None:
            matched = semantic_duplicate["record"]
            self.catalog.touch_head(matched["head_id"], now)
            payload = {
                "checksum": draft["checksum"],
                "mode": "semantic",
                "score": round(float(semantic_duplicate["score"]), 6),
            }
            if semantic_duplicate.get("chunk_id"):
                payload["chunk_id"] = semantic_duplicate["chunk_id"]
            self.catalog.add_history_event(
                scope_key=scope.key,
                head_id=matched["head_id"],
                version_id=matched["version_id"],
                event_type="deduplicated",
                payload=payload,
                created_at=now,
            )
            record = self.catalog.get_head(matched["head_id"])
            return record, {
                "fingerprint": draft["fingerprint"],
                "head_id": matched["head_id"],
                "embedding_updates": {},
                "mirrored_jobs": [],
                "semantic_candidate": None,
            }

        embedding_updates: dict[str, list[float]] = {}
        mirrored_jobs: list[dict[str, Any]] = []
        versioned_target = None
        if uses_version_chain(draft["kind"], procedure_version_mode=self.config.procedure_version_mode) and draft["fact_key"]:
            versioned_target = self.catalog.find_current_by_fact_key(scope.key, draft["kind"], draft["fact_key"])

        if versioned_target is None:
            provisional_version_id = make_id("verref")
            head_id = self.catalog.create_head(
                scope=scope,
                kind=draft["kind"],
                layer=draft["layer"],
                tier=draft["tier"],
                state=HEAD_STATE_ACTIVE,
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
            mirrored_jobs.extend(self._build_vector_jobs(scope.key, chunk_ids, now, op_type=OUTBOX_UPSERT_VECTOR))
            embedding_updates.update(self._build_embedding_updates(chunk_ids, draft.get("vector")))
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
            mirrored_jobs.extend(self._build_vector_jobs(scope.key, chunk_ids, now, op_type=OUTBOX_UPSERT_VECTOR))
            embedding_updates.update(self._build_embedding_updates(chunk_ids, draft.get("vector")))
            if previous_chunks:
                job_id = self.catalog.enqueue_job(
                    entity_type="chunk",
                    entity_id=head_id,
                    op_type=OUTBOX_DELETE_VECTOR,
                    payload={"chunk_ids": previous_chunks, "scope_key": scope.key},
                    now=now,
                )
                mirrored_jobs.append(
                    {
                        "job_id": job_id,
                        "op_type": OUTBOX_DELETE_VECTOR,
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
            "embedding_updates": embedding_updates,
            "mirrored_jobs": mirrored_jobs,
            "semantic_candidate": self._make_semantic_candidate(record, draft),
        }

    def _hydrate_semantic_vectors(self, drafts: list[dict[str, Any]]) -> None:
        if not self.config.semantic_dedupe_enabled:
            return
        pending: list[tuple[dict[str, Any], str]] = []
        for draft in drafts:
            vector = draft.get("vector")
            if vector is not None:
                draft["vector"] = [float(value) for value in vector]
                continue
            cache_key = self._draft_embedding_cache_key(draft["checksum"])
            cached = self.hotstore.get_embedding(cache_key)
            if cached is not None:
                draft["vector"] = [float(value) for value in cached]
                continue
            pending.append((draft, cache_key))
        if not pending:
            return
        vectors = self.embedder.embed_texts([draft["text"] for draft, _ in pending])
        cache_updates: dict[str, list[float]] = {}
        for (draft, cache_key), vector in zip(pending, vectors, strict=False):
            if len(vector) != self.config.vector_dim:
                raise ValueError(f"Embedder returned vector[{len(vector)}], expected {self.config.vector_dim}")
            normalized = [float(value) for value in vector]
            draft["vector"] = normalized
            cache_updates[cache_key] = normalized
        self.hotstore.put_embeddings(cache_updates)

    def _find_semantic_duplicate(
        self,
        *,
        scope: Scope,
        draft: dict[str, Any],
        semantic_candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if not self.config.semantic_dedupe_enabled:
            return None
        vector = draft.get("vector")
        if vector is None:
            return None

        threshold = float(self.config.semantic_dedupe_threshold)
        for candidate in reversed(semantic_candidates):
            record = candidate["record"]
            if not self._semantic_candidate_allowed(draft, record):
                continue
            score = self._semantic_similarity(vector, candidate["vector"])
            if score >= threshold:
                return {"record": record, "score": score}

        neighbors = self.vector_store.nearest_neighbors(
            scope_key=scope.key,
            vector=vector,
            limit=max(1, int(self.config.semantic_dedupe_candidates)),
            kind=draft["kind"],
        )
        for neighbor in neighbors:
            if float(neighbor["similarity"]) < threshold:
                continue
            record = self.catalog.get_head(neighbor["head_id"])
            if record is None or not self._semantic_candidate_allowed(draft, record):
                continue
            return {
                "record": record,
                "score": float(neighbor["similarity"]),
                "chunk_id": neighbor["chunk_id"],
            }
        return None

    def _make_semantic_candidate(self, record: dict[str, Any] | None, draft: dict[str, Any]) -> dict[str, Any] | None:
        if record is None or draft.get("vector") is None:
            return None
        return {"record": record, "vector": list(draft["vector"])}

    def _semantic_candidate_allowed(self, draft: dict[str, Any], record: dict[str, Any]) -> bool:
        if record["state"] != HEAD_STATE_ACTIVE:
            return False
        if record["kind"] != draft["kind"]:
            return False
        if record["layer"] != draft["layer"]:
            return False
        if draft["kind"] == "procedure":
            return False
        if uses_version_chain(draft["kind"], procedure_version_mode=self.config.procedure_version_mode):
            return record.get("fact_key") == draft.get("fact_key")
        return True

    @staticmethod
    def _semantic_similarity(left: list[float], right: list[float]) -> float:
        distance = math.sqrt(sum((lval - rval) ** 2 for lval, rval in zip(left, right, strict=False)))
        return vector_score(distance)

    def _draft_embedding_cache_key(self, checksum: str) -> str:
        return f"{self.config.embedding_model}:draft:{checksum}"

    def _build_vector_jobs(
        self,
        scope_key: str,
        chunk_ids: list[str],
        now: int,
        *,
        op_type: str,
    ) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        for chunk_id in chunk_ids:
            job_id = self.catalog.enqueue_job(
                entity_type="chunk",
                entity_id=chunk_id,
                op_type=op_type,
                payload={"chunk_id": chunk_id, "scope_key": scope_key},
                now=now,
            )
            jobs.append({"job_id": job_id, "op_type": op_type, "scope_key": scope_key, "chunk_id": chunk_id})
        return jobs

    def _build_embedding_updates(self, chunk_ids: list[str], vector: list[float] | None) -> dict[str, list[float]]:
        if vector is None:
            return {}
        return {
            f"{self.config.embedding_model}:{chunk_id}": list(vector)
            for chunk_id in chunk_ids
        }

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
                    op_type=OUTBOX_DELETE_VECTOR,
                    payload={"chunk_ids": chunk_ids, "scope_key": record["scope_key"]},
                    now=now,
                )
                mirrored_jobs.append({"job_id": job_id, "op_type": OUTBOX_DELETE_VECTOR, "scope_key": record["scope_key"], "chunk_ids": chunk_ids})
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
        if record["state"] != HEAD_STATE_DELETED:
            raise ValueError(f"restore() only applies to deleted heads: {head_id}")
        mirrored_jobs: list[dict[str, Any]] = []
        with self.catalog.transaction():
            restored = self.catalog.restore(head_id, now)
            chunk_ids = self.catalog.list_chunk_ids_for_version(record["version_id"])
            mirrored_jobs.extend(self._build_vector_jobs(record["scope_key"], chunk_ids, now, op_type=OUTBOX_REBUILD_VECTOR))
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

    def archive(self, *, scope: Scope, head_id: str) -> dict[str, Any]:
        now = now_ms()
        record = self.catalog.get_head(head_id)
        if record is None:
            raise RecordNotFound(f"Memory head not found: {head_id}")
        if record["scope_key"] != scope.key:
            raise InvalidScope(f"Head {head_id} is outside the requested scope")
        if record["state"] != HEAD_STATE_ACTIVE:
            raise ValueError(f"archive() only applies to active heads: {head_id}")
        mirrored_jobs: list[dict[str, Any]] = []
        with self.catalog.transaction():
            archived = self.catalog.archive(head_id, now)
            chunk_ids = self.catalog.list_chunk_ids_for_version(record["version_id"])
            if chunk_ids:
                job_id = self.catalog.enqueue_job(
                    entity_type="chunk",
                    entity_id=head_id,
                    op_type=OUTBOX_DELETE_VECTOR,
                    payload={"chunk_ids": chunk_ids, "scope_key": record["scope_key"]},
                    now=now,
                )
                mirrored_jobs.append({"job_id": job_id, "op_type": OUTBOX_DELETE_VECTOR, "scope_key": record["scope_key"], "chunk_ids": chunk_ids})
            self.catalog.add_history_event(
                scope_key=record["scope_key"],
                head_id=head_id,
                version_id=record["version_id"],
                event_type="archived",
                payload={},
                created_at=now,
            )
        self.hotstore.clear_query_cache(record["scope_key"])
        for job in mirrored_jobs:
            self.hotstore.mirror_job(job["job_id"], job)
        return archived

    def restore_archive(self, *, scope: Scope, head_id: str) -> dict[str, Any]:
        now = now_ms()
        record = self.catalog.get_head(head_id)
        if record is None:
            raise RecordNotFound(f"Memory head not found: {head_id}")
        if record["scope_key"] != scope.key:
            raise InvalidScope(f"Head {head_id} is outside the requested scope")
        if record["state"] != HEAD_STATE_ARCHIVED:
            raise ValueError(f"restore_archive() only applies to archived heads: {head_id}")
        mirrored_jobs: list[dict[str, Any]] = []
        with self.catalog.transaction():
            restored = self.catalog.restore_archive(head_id, now)
            chunk_ids = self.catalog.list_chunk_ids_for_version(record["version_id"])
            mirrored_jobs.extend(self._build_vector_jobs(record["scope_key"], chunk_ids, now, op_type=OUTBOX_REBUILD_VECTOR))
            self.catalog.add_history_event(
                scope_key=record["scope_key"],
                head_id=head_id,
                version_id=record["version_id"],
                event_type="archive_restored",
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
