from __future__ import annotations

from aimemory.catalog.sqlite_store import SQLiteCatalog
from aimemory.hotstore.lmdb_store import LMDBHotStore
from aimemory.pipeline.lifecycle import now_ms
from aimemory.pipeline.maintenance import MaintenanceCoordinator


class RecoveryCoordinator:
    def __init__(
        self,
        *,
        catalog: SQLiteCatalog,
        hotstore: LMDBHotStore,
        maintenance: MaintenanceCoordinator,
    ):
        self.catalog = catalog
        self.hotstore = hotstore
        self.maintenance = maintenance

    def recover(self, *, batch_size: int = 512) -> dict[str, int]:
        now = now_ms()
        with self.catalog.transaction():
            reset_jobs = self.catalog.reset_recoverable_jobs(now)
            repaired_chunks = 0
            for chunk in self.catalog.list_chunks_needing_recovery(batch_size):
                self.catalog.enqueue_job(
                    entity_type="chunk",
                    entity_id=chunk["chunk_id"],
                    op_type="upsert_vector",
                    payload={"chunk_id": chunk["chunk_id"], "scope_key": chunk["scope_key"]},
                    now=now,
                )
                repaired_chunks += 1

        mirrored = self.hotstore.replace_job_mirror(self.catalog.list_recoverable_jobs(batch_size * 8))

        processed_jobs = 0
        while True:
            batch = self.maintenance.flush_jobs(limit=batch_size)
            processed_jobs += batch
            if batch == 0:
                break

        access_updates = self.maintenance.flush_access()
        return {
            "reset_jobs": reset_jobs,
            "repaired_chunks": repaired_chunks,
            "mirrored_jobs": mirrored,
            "processed_jobs": processed_jobs,
            "access_updates": access_updates,
        }
