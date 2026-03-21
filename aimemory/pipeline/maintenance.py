from __future__ import annotations

import threading
from uuid import uuid4

from aimemory.catalog.sqlite_store import SQLiteCatalog
from aimemory.config import MemoryConfig
from aimemory.hotstore.lmdb_store import LMDBHotStore
from aimemory.outbox import OUTBOX_DELETE_VECTOR, OUTBOX_FLUSH_ACCESS, OUTBOX_REBUILD_VECTOR, VECTOR_WRITE_OPS
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
        self._lock = threading.RLock()

    def flush_jobs(self, limit: int = 256) -> int:
        with self._lock:
            jobs = self.catalog.pull_pending_jobs(limit)
            processed = 0
            dirty_scopes: set[str] = set()
            for job in jobs:
                now = now_ms()
                try:
                    if job["op_type"] in VECTOR_WRITE_OPS:
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
                    elif job["op_type"] == OUTBOX_DELETE_VECTOR:
                        self.vector_store.delete_chunks(job["payload"].get("chunk_ids", []))
                        self.catalog.mark_chunk_embedding_states(job["payload"].get("chunk_ids", []), "deleted")
                        if job["payload"].get("scope_key"):
                            dirty_scopes.add(job["payload"]["scope_key"])
                    elif job["op_type"] == OUTBOX_FLUSH_ACCESS:
                        updates = dict(job["payload"].get("updates") or {})
                        if updates:
                            with self.catalog.transaction():
                                self.catalog.apply_access_updates(updates, int(job["payload"].get("applied_at") or now))
                    self.catalog.finish_job(job["job_id"], "done", now)
                    self.hotstore.drop_job(job["job_id"])
                    processed += 1
                except Exception:
                    self.catalog.finish_job(job["job_id"], "failed", now, retry_count=int(job["retry_count"]) + 1)
            for scope_key in dirty_scopes:
                self.hotstore.clear_query_cache(scope_key)
            return processed

    def flush_access(self, *, current_time_ms: int | None = None) -> int:
        with self._lock:
            updates = self.hotstore.drain_access()
            if not updates:
                return 0
            applied_at = current_time_ms if current_time_ms is not None else now_ms()
            with self.catalog.transaction():
                self.catalog.apply_access_updates(updates, applied_at)
            return len(updates)

    def should_flush_access(self, *, current_time_ms: int | None = None) -> bool:
        pending_total = self.hotstore.pending_access_total()
        if pending_total <= 0:
            return False
        if pending_total >= self.config.flush_access_every:
            return True
        interval_ms = int(self.config.flush_access_interval_ms)
        if interval_ms <= 0:
            return False
        pending_since = self.hotstore.pending_access_since()
        if pending_since is None:
            return False
        observed_at = current_time_ms if current_time_ms is not None else now_ms()
        return (observed_at - pending_since) >= interval_ms

    def flush_access_if_needed(self, *, current_time_ms: int | None = None) -> int:
        observed_at = current_time_ms if current_time_ms is not None else now_ms()
        if not self.should_flush_access(current_time_ms=observed_at):
            return 0
        flushed = self.flush_access(current_time_ms=observed_at)
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
        with self._lock:
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
                            op_type=OUTBOX_REBUILD_VECTOR,
                            payload={"chunk_id": chunk_id, "scope_key": candidate["scope_key"]},
                            now=now,
                        )
                        mirrored_jobs.append(
                            {
                                "job_id": job_id,
                                "op_type": OUTBOX_REBUILD_VECTOR,
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

    def tick(self, *, limit: int = 256, current_time_ms: int | None = None) -> dict[str, int]:
        jobs = self.flush_jobs(limit=limit)
        access_updates = self.flush_access_if_needed(current_time_ms=current_time_ms)
        if access_updates > 0:
            jobs += self.flush_jobs(limit=limit)
        return {"jobs": jobs, "access_updates": access_updates}

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
                    op_type=OUTBOX_REBUILD_VECTOR,
                    payload={"chunk_id": chunk["chunk_id"], "scope_key": chunk["scope_key"]},
                    now=now_ms(),
                )
                count += 1
        self.hotstore.clear_query_cache()
        self.flush_jobs()
        return count


class EmbeddedMaintenanceWorker:
    def __init__(
        self,
        *,
        config: MemoryConfig,
        maintenance: MaintenanceCoordinator,
        hotstore: LMDBHotStore,
        worker_name: str = "embedded-maintenance",
    ):
        self.config = config
        self.maintenance = maintenance
        self.hotstore = hotstore
        self.worker_name = worker_name
        self.owner_id = uuid4().hex
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._leader = False
        self._last_error: str | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run,
            name=f"aimemory-worker-{self.owner_id[:8]}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout_s: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout_s)
        self._release_if_held()

    def wake(self) -> None:
        self._wake.set()

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def is_leader(self) -> bool:
        with self._state_lock:
            return self._leader

    def status(self) -> dict[str, object]:
        lease = self.hotstore.get_lease(self.worker_name)
        with self._state_lock:
            leader = self._leader
            last_error = self._last_error
        return {
            "mode": "embedded",
            "worker_name": self.worker_name,
            "owner_id": self.owner_id,
            "alive": self.is_alive(),
            "leader": leader,
            "lease": lease,
            "last_error": last_error,
        }

    def _run(self) -> None:
        poll_interval_s = max(0.01, float(self.config.worker_poll_interval_ms) / 1000.0)
        batch_limit = max(1, int(self.config.recovery_batch_size))
        while not self._stop.is_set():
            try:
                now = now_ms()
                if self._acquire_or_renew(now):
                    self.maintenance.tick(limit=batch_limit, current_time_ms=now)
                    with self._state_lock:
                        self._last_error = None
            except Exception as exc:
                with self._state_lock:
                    self._last_error = repr(exc)
            self._wake.wait(poll_interval_s)
            self._wake.clear()
        self._release_if_held()

    def _acquire_or_renew(self, current_time_ms: int) -> bool:
        ttl_ms = max(1, int(self.config.worker_lease_ttl_ms))
        with self._state_lock:
            leader = self._leader
        if leader:
            renewed = self.hotstore.renew_lease(
                self.worker_name,
                owner_id=self.owner_id,
                now_ms=current_time_ms,
                ttl_ms=ttl_ms,
            )
            if renewed:
                return True
        acquired = self.hotstore.acquire_lease(
            self.worker_name,
            owner_id=self.owner_id,
            now_ms=current_time_ms,
            ttl_ms=ttl_ms,
        )
        with self._state_lock:
            self._leader = acquired
        return acquired

    def _release_if_held(self) -> None:
        self.hotstore.release_lease(self.worker_name, owner_id=self.owner_id)
        with self._state_lock:
            self._leader = False
