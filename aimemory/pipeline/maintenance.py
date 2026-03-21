from __future__ import annotations

from aimemory.catalog.sqlite_store import SQLiteCatalog
from aimemory.config import MemoryConfig
from aimemory.hotstore.lmdb_store import LMDBHotStore
from aimemory.pipeline.lifecycle import lifecycle_score, now_ms, suggest_tier
from aimemory.vector.embeddings import Embedder
from aimemory.vector.lancedb_store import LanceVectorStore


class MaintenanceCoordinator:
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

    def flush_jobs(self, limit: int = 256) -> int:
        jobs = self.catalog.pull_pending_jobs(limit)
        processed = 0
        dirty_scopes: set[str] = set()
        for job in jobs:
            now = now_ms()
            try:
                if job["op_type"] == "upsert_vector":
                    chunk = self.catalog.get_chunk_for_index(job["payload"]["chunk_id"])
                    if chunk is None:
                        self.catalog.finish_job(job["job_id"], "done", now)
                        self.hotstore.drop_job(job["job_id"])
                        continue
                    cache_key = f"{self.embedder.model_name}:{job['payload']['chunk_id']}"
                    vector = self.hotstore.get_embedding(cache_key)
                    if vector is None:
                        vector = self.embedder.embed_texts([chunk["text"]])[0]
                        self.hotstore.put_embedding(cache_key, vector)
                    self.vector_store.upsert(
                        [
                            {
                                "chunk_id": chunk["chunk_id"],
                                "head_id": chunk["head_id"],
                                "version_id": chunk["version_id"],
                                "scope_key": chunk["scope_key"],
                                "kind": chunk["kind"],
                                "tier": chunk["tier"],
                                "importance": float(chunk["importance"]),
                                "confidence": float(chunk["confidence"]),
                                "created_at": int(chunk["created_at"]),
                                "valid_from": int(chunk["valid_from"]),
                                "valid_to": chunk["valid_to"],
                                "updated_at": int(chunk["updated_at"]),
                                "text": chunk["text"],
                                "abstract": chunk["abstract"],
                                "overview": chunk["overview"],
                                "vector": vector,
                            }
                        ]
                    )
                    self.catalog.mark_chunk_embedding_state(chunk["chunk_id"], "ready")
                    dirty_scopes.add(chunk["scope_key"])
                elif job["op_type"] == "delete_vector":
                    self.vector_store.delete_chunks(job["payload"].get("chunk_ids", []))
                    self.catalog.mark_chunk_embedding_states(job["payload"].get("chunk_ids", []), "deleted")
                    if job["payload"].get("scope_key"):
                        dirty_scopes.add(job["payload"]["scope_key"])
                self.catalog.finish_job(job["job_id"], "done", now)
                self.hotstore.drop_job(job["job_id"])
                processed += 1
            except Exception:
                self.catalog.finish_job(job["job_id"], "failed", now, retry_count=int(job["retry_count"]) + 1)
        for scope_key in dirty_scopes:
            self.hotstore.clear_query_cache(scope_key)
        return processed

    def flush_access(self) -> int:
        updates = self.hotstore.drain_access()
        if not updates:
            return 0
        with self.catalog.transaction():
            self.catalog.apply_access_updates(updates, now_ms())
        return len(updates)

    def flush_access_if_needed(self) -> int:
        if self.hotstore.pending_access_total() < self.config.flush_access_every:
            return 0
        flushed = self.flush_access()
        if flushed > 0:
            self.run_lifecycle(limit=self.config.lifecycle_batch_size)
        return flushed

    def flush_all(self) -> dict[str, int]:
        access_updates = self.flush_access()
        lifecycle_stats = self.run_lifecycle(limit=self.config.lifecycle_batch_size)
        return {
            "jobs": self.flush_jobs(),
            "access_updates": access_updates,
            "lifecycle_evaluated": lifecycle_stats["evaluated"],
            "lifecycle_changed": lifecycle_stats["changed"],
            "lifecycle_jobs": lifecycle_stats["jobs"],
        }

    def run_lifecycle(self, limit: int | None = None) -> dict[str, int]:
        if not self.config.lifecycle_enabled:
            return {"evaluated": 0, "changed": 0, "jobs": 0}
        now = now_ms()
        candidates = self.catalog.list_lifecycle_candidates(limit or self.config.lifecycle_batch_size)
        if not candidates:
            return {"evaluated": 0, "changed": 0, "jobs": 0}

        changed = 0
        enqueued_jobs = 0
        mirrored_jobs: list[dict[str, str]] = []
        dirty_scopes: set[str] = set()
        with self.catalog.transaction():
            for candidate in candidates:
                next_tier = suggest_tier(
                    candidate["tier"],
                    importance=float(candidate["importance"]),
                    confidence=float(candidate["confidence"]),
                    access_count=int(candidate["access_count"]),
                    updated_at=int(candidate["updated_at"]),
                    last_accessed_at=candidate["last_accessed_at"],
                    now=now,
                    freshness_window_ms=self.config.lifecycle_freshness_window_ms,
                    cold_after_ms=self.config.lifecycle_cold_after_ms,
                    core_promote_importance=self.config.lifecycle_core_promote_importance,
                    core_promote_access_count=self.config.lifecycle_core_promote_access_count,
                    core_promote_score=self.config.lifecycle_core_promote_score,
                    core_demote_score=self.config.lifecycle_core_demote_score,
                    cold_demote_score=self.config.lifecycle_cold_demote_score,
                    cold_reactivate_score=self.config.lifecycle_cold_reactivate_score,
                )
                if next_tier == candidate["tier"]:
                    continue
                composite = lifecycle_score(
                    importance=float(candidate["importance"]),
                    confidence=float(candidate["confidence"]),
                    access_count=int(candidate["access_count"]),
                    updated_at=int(candidate["updated_at"]),
                    last_accessed_at=candidate["last_accessed_at"],
                    now=now,
                    freshness_window_ms=self.config.lifecycle_freshness_window_ms,
                )
                self.catalog.update_head_tier(candidate["head_id"], next_tier)
                self.catalog.add_history_event(
                    scope_key=candidate["scope_key"],
                    head_id=candidate["head_id"],
                    version_id=candidate["current_version_id"],
                    event_type="tier_changed",
                    payload={
                        "composite_score": round(composite, 6),
                        "new_tier": next_tier,
                        "previous_tier": candidate["tier"],
                    },
                    created_at=now,
                )
                for chunk_id in self.catalog.list_chunk_ids_for_version(candidate["current_version_id"]):
                    job_id = self.catalog.enqueue_job(
                        entity_type="chunk",
                        entity_id=chunk_id,
                        op_type="upsert_vector",
                        payload={"chunk_id": chunk_id, "scope_key": candidate["scope_key"]},
                        now=now,
                    )
                    mirrored_jobs.append(
                        {
                            "job_id": job_id,
                            "op_type": "upsert_vector",
                            "scope_key": candidate["scope_key"],
                            "chunk_id": chunk_id,
                        }
                    )
                    enqueued_jobs += 1
                dirty_scopes.add(candidate["scope_key"])
                changed += 1
        for job in mirrored_jobs:
            self.hotstore.mirror_job(job["job_id"], job)
        for scope_key in dirty_scopes:
            self.hotstore.clear_query_cache(scope_key)
        return {"evaluated": len(candidates), "changed": changed, "jobs": enqueued_jobs}

    def compact(self) -> None:
        self.flush_all()
        self.catalog._conn.execute("VACUUM")

    def reindex(self) -> int:
        chunks = self.catalog.get_indexable_chunks()
        self.vector_store.delete_chunks([row["chunk_id"] for row in chunks])
        count = 0
        with self.catalog.transaction():
            for chunk in chunks:
                self.catalog.enqueue_job(
                    entity_type="chunk",
                    entity_id=chunk["chunk_id"],
                    op_type="upsert_vector",
                    payload={"chunk_id": chunk["chunk_id"], "scope_key": chunk["scope_key"]},
                    now=now_ms(),
                )
                count += 1
        self.hotstore.clear_query_cache()
        self.flush_jobs()
        return count
