from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from aimemory.catalog.sqlite_store import SQLiteCatalog
from aimemory.config import MemoryConfig
from aimemory.errors import InvalidScope, RecordNotFound
from aimemory.filters import match_filters
from aimemory.hotstore.lmdb_store import LMDBHotStore
from aimemory.pipeline.maintenance import MaintenanceCoordinator
from aimemory.pipeline.lifecycle import now_ms
from aimemory.pipeline.read_path import MemoryReadPath
from aimemory.pipeline.recovery import RecoveryCoordinator
from aimemory.pipeline.write_path import MemoryWritePath
from aimemory.plugins.protocols import Extractor, RetrievalGate, Reranker
from aimemory.serialization import json_dumps, json_loads
from aimemory.scope import Scope
from aimemory.vector.embeddings import Embedder, HashEmbedder
from aimemory.vector.lancedb_store import LanceVectorStore


class MemoryDB:
    def __init__(
        self,
        config: MemoryConfig | dict[str, Any] | None = None,
        *,
        embedder: Embedder | None = None,
        extractor: Extractor | None = None,
        reranker: Reranker | None = None,
        retrieval_gate: RetrievalGate | None = None,
    ):
        self.config = config if isinstance(config, MemoryConfig) else MemoryConfig(**(config or {}))
        root = self.config.resolved_root()
        root.mkdir(parents=True, exist_ok=True)
        (root / "backups").mkdir(parents=True, exist_ok=True)
        (root / "exports").mkdir(parents=True, exist_ok=True)
        (root / "lmdb").mkdir(parents=True, exist_ok=True)
        (root / "lancedb").mkdir(parents=True, exist_ok=True)
        self.embedder = embedder or HashEmbedder(self.config.vector_dim)
        self.extractor = extractor
        self.catalog = SQLiteCatalog(root / "catalog.sqlite3")
        self.hotstore = LMDBHotStore(root / "lmdb")
        self.vector_store = LanceVectorStore(root / "lancedb", self.embedder.dimension)
        self.writer = MemoryWritePath(config=self.config, catalog=self.catalog, hotstore=self.hotstore)
        self.reader = MemoryReadPath(
            config=self.config,
            catalog=self.catalog,
            hotstore=self.hotstore,
            vector_store=self.vector_store,
            embedder=self.embedder,
            reranker=reranker,
            retrieval_gate=retrieval_gate,
        )
        self.maintenance = MaintenanceCoordinator(
            config=self.config,
            catalog=self.catalog,
            hotstore=self.hotstore,
            vector_store=self.vector_store,
            embedder=self.embedder,
        )
        self.recovery = RecoveryCoordinator(
            catalog=self.catalog,
            hotstore=self.hotstore,
            maintenance=self.maintenance,
        )
        if self.config.recover_on_open:
            self.recovery.recover(batch_size=self.config.recovery_batch_size)

    @classmethod
    def open(
        cls,
        root_dir: str | Path,
        *,
        embedder: Embedder | None = None,
        extractor: Extractor | None = None,
        reranker: Reranker | None = None,
        retrieval_gate: RetrievalGate | None = None,
    ) -> "MemoryDB":
        return cls(
            {"root_dir": str(root_dir)},
            embedder=embedder,
            extractor=extractor,
            reranker=reranker,
            retrieval_gate=retrieval_gate,
        )

    def scoped(self, scope: Scope | dict[str, Any] | None = None, **scope_overrides: Any) -> "ScopedMemoryDB":
        resolved = Scope.from_value(scope).bind(**scope_overrides)
        return ScopedMemoryDB(self, resolved)

    def put(self, *, scope: Scope | dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        record = self.writer.put(scope=Scope.from_value(scope), **kwargs)
        if self.config.auto_flush:
            self.flush()
        return record

    def put_many(self, *, scope: Scope | dict[str, Any], items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = self.writer.put_many(scope=Scope.from_value(scope), items=items)
        if self.config.auto_flush:
            self.flush()
        return records

    def ingest_records(self, *, scope: Scope | dict[str, Any], records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result = self.writer.ingest_records(scope=Scope.from_value(scope), records=records)
        if self.config.auto_flush:
            self.flush()
        return result

    def ingest_jsonl(self, *, scope: Scope | dict[str, Any], path: str | Path) -> list[dict[str, Any]]:
        result = self.writer.ingest_jsonl(scope=Scope.from_value(scope), path=path)
        if self.config.auto_flush:
            self.flush()
        return result

    def ingest_messages(
        self,
        *,
        scope: Scope | dict[str, Any],
        messages: list[dict[str, Any]],
        extractor: Extractor | None = None,
    ) -> list[dict[str, Any]]:
        resolved_scope = Scope.from_value(scope)
        active_extractor = extractor or self.extractor
        if active_extractor is None:
            raise ValueError("ingest_messages requires an extractor")
        drafts = active_extractor.extract(messages, resolved_scope)
        if drafts is None:
            return []
        normalized_drafts = [dict(draft) for draft in drafts]
        result = self.writer.ingest_records(scope=resolved_scope, records=normalized_drafts)
        if self.config.auto_flush:
            self.flush()
        return result

    def get(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any] | None:
        return self.reader.get(scope=Scope.from_value(scope), head_id=head_id)

    def list(self, *, scope: Scope | dict[str, Any], filters: dict[str, Any] | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.reader.list(scope=Scope.from_value(scope), filters=filters, limit=limit)

    def search(self, *, scope: Scope | dict[str, Any], query: str, top_k: int = 10, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if self.config.auto_flush:
            self.maintenance.flush_jobs()
        hits = self.reader.search(scope=Scope.from_value(scope), query=query, top_k=top_k, filters=filters)
        if self.config.auto_flush:
            self.maintenance.flush_access_if_needed()
        return [asdict(hit) for hit in hits]

    def history(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any]:
        return self.reader.history(scope=Scope.from_value(scope), head_id=head_id)

    def delete(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any]:
        record = self.writer.delete(scope=Scope.from_value(scope), head_id=head_id)
        if self.config.auto_flush:
            self.flush()
        return record

    def restore(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any]:
        record = self.writer.restore(scope=Scope.from_value(scope), head_id=head_id)
        if self.config.auto_flush:
            self.flush()
        return record

    def feedback(self, *, scope: Scope | dict[str, Any], head_id: str, text: str) -> dict[str, Any]:
        resolved = Scope.from_value(scope)
        current = self._require_record(scope=resolved, head_id=head_id)
        return self.put(
            scope=resolved,
            text=text,
            kind=current["kind"],
            layer=current["layer"],
            tier=current["tier"],
            importance=current["importance"],
            confidence=current["confidence"],
            fact_key=current["fact_key"],
            metadata=current["metadata"],
            source_type="feedback",
            source_ref=head_id,
        )

    def working_append(self, *, scope: Scope | dict[str, Any], role: str, content: str, metadata: dict[str, Any] | None = None) -> None:
        resolved = Scope.from_value(scope)
        item = {"role": role, "content": content, "metadata": metadata or {}}
        self.hotstore.append_turn(resolved.key, item, self.config.working_memory_limit)
        self.hotstore.append_working(resolved.key, item, self.config.working_memory_limit)
        self.hotstore.clear_query_cache(resolved.key)

    def working_snapshot(self, *, scope: Scope | dict[str, Any], limit: int | None = None) -> list[dict[str, Any]]:
        resolved = Scope.from_value(scope)
        return self.hotstore.working_snapshot(resolved.key, limit or self.config.working_memory_limit)

    def export_records(
        self,
        *,
        scope: Scope | dict[str, Any],
        filters: dict[str, Any] | None = None,
        limit: int = 1000,
        state: str = "active",
    ) -> list[dict[str, Any]]:
        resolved = Scope.from_value(scope)
        rows = self.catalog.list_heads(resolved.key, state=state, limit=limit)
        return [self._export_entry(row) for row in rows if match_filters(row, filters)]

    def export_jsonl(
        self,
        *,
        scope: Scope | dict[str, Any],
        path: str | Path | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 1000,
        state: str = "active",
    ) -> dict[str, Any]:
        records = self.export_records(scope=scope, filters=filters, limit=limit, state=state)
        export_path = Path(path) if path is not None else (self.config.resolved_root() / "exports" / f"memory-export-{now_ms()}.jsonl")
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with export_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json_dumps(record))
                handle.write("\n")
        return {"path": str(export_path), "count": len(records)}

    def import_jsonl(self, *, path: str | Path, scope: Scope | dict[str, Any] | None = None) -> list[dict[str, Any]]:
        scope_override = Scope.from_value(scope) if scope is not None else None
        batches: dict[str, tuple[Scope, list[dict[str, Any]]]] = {}
        with Path(path).open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                payload = json_loads(line, None)
                if not isinstance(payload, dict):
                    raise ValueError(f"Invalid JSON object at line {line_no} in {path}")
                resolved_scope, record = self._resolve_import_payload(payload, scope_override)
                batch = batches.get(resolved_scope.key)
                if batch is None:
                    batch = (resolved_scope, [])
                    batches[resolved_scope.key] = batch
                batch[1].append(record)
        imported: list[dict[str, Any]] = []
        for resolved_scope, records in batches.values():
            imported.extend(self.writer.ingest_records(scope=resolved_scope, records=records))
        if self.config.auto_flush and imported:
            self.flush()
        return imported

    def flush(self) -> dict[str, int]:
        return self.maintenance.flush_all()

    def run_lifecycle(self) -> dict[str, int]:
        stats = self.maintenance.run_lifecycle(limit=self.config.lifecycle_batch_size)
        if stats["jobs"] > 0:
            stats["jobs_processed"] = self.maintenance.flush_jobs()
        else:
            stats["jobs_processed"] = 0
        return stats

    def compact(self) -> None:
        self.maintenance.compact()

    def reindex(self) -> int:
        return self.maintenance.reindex()

    def stats(self) -> dict[str, int]:
        return self.catalog.stats()

    def recover(self) -> dict[str, int]:
        return self.recovery.recover(batch_size=self.config.recovery_batch_size)

    @staticmethod
    def _export_entry(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "scope": Scope.from_record(record).as_dict(),
            "memory": {
                "text": record["text"],
                "kind": record["kind"],
                "layer": record["layer"],
                "tier": record["tier"],
                "importance": record["importance"],
                "confidence": record["confidence"],
                "fact_key": record["fact_key"],
                "metadata": dict(record["metadata"]),
            },
            "record": {
                "head_id": record["head_id"],
                "version_id": record["version_id"],
                "state": record["state"],
                "created_at": record["created_at"],
                "updated_at": record["updated_at"],
                "access_count": record["access_count"],
            },
        }

    @staticmethod
    def _resolve_import_payload(
        payload: dict[str, Any],
        scope_override: Scope | None,
    ) -> tuple[Scope, dict[str, Any]]:
        if "memory" in payload:
            record = payload["memory"]
            if not isinstance(record, dict):
                raise ValueError("Import payload 'memory' must be a JSON object")
            if scope_override is not None:
                scope_value = scope_override
            elif "scope" in payload:
                scope_value = Scope.from_value(payload["scope"])
            else:
                raise InvalidScope("Import payload requires an explicit scope or embedded scope")
            return scope_value, dict(record)
        if scope_override is None:
            raise InvalidScope("Import payload requires an explicit scope or embedded scope")
        return scope_override, dict(payload)

    def _require_record(self, *, scope: Scope, head_id: str) -> dict[str, Any]:
        record = self.get(scope=scope, head_id=head_id)
        if record is None:
            raise RecordNotFound(f"Memory head not found: {head_id}")
        return record

    def close(self) -> None:
        self.catalog.close()
        self.hotstore.close()

    def __enter__(self) -> "MemoryDB":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class ScopedMemoryDB:
    def __init__(self, db: MemoryDB, scope: Scope):
        self.db = db
        self.scope = scope

    def scoped(self, **overrides: Any) -> "ScopedMemoryDB":
        return ScopedMemoryDB(self.db, self.scope.bind(**overrides))

    def put(self, **kwargs: Any) -> dict[str, Any]:
        return self.db.put(scope=self.scope, **kwargs)

    def put_many(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self.db.put_many(scope=self.scope, items=items)

    def ingest_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self.db.ingest_records(scope=self.scope, records=records)

    def ingest_jsonl(self, path: str | Path) -> list[dict[str, Any]]:
        return self.db.ingest_jsonl(scope=self.scope, path=path)

    def ingest_messages(self, messages: list[dict[str, Any]], extractor: Extractor | None = None) -> list[dict[str, Any]]:
        return self.db.ingest_messages(scope=self.scope, messages=messages, extractor=extractor)

    def get(self, head_id: str) -> dict[str, Any] | None:
        record = self.db.get(scope=self.scope, head_id=head_id)
        if record is None:
            return None
        return record

    def list(self, filters: dict[str, Any] | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.db.list(scope=self.scope, filters=filters, limit=limit)

    def search(self, query: str, top_k: int = 10, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return self.db.search(scope=self.scope, query=query, top_k=top_k, filters=filters)

    def history(self, head_id: str) -> dict[str, Any]:
        self._require_scoped_record(head_id)
        return self.db.history(scope=self.scope, head_id=head_id)

    def delete(self, head_id: str) -> dict[str, Any]:
        self._require_scoped_record(head_id)
        return self.db.delete(scope=self.scope, head_id=head_id)

    def restore(self, head_id: str) -> dict[str, Any]:
        self._require_scoped_record(head_id)
        return self.db.restore(scope=self.scope, head_id=head_id)

    def feedback(self, *, head_id: str, text: str) -> dict[str, Any]:
        self._require_scoped_record(head_id)
        return self.db.feedback(scope=self.scope, head_id=head_id, text=text)

    def working_append(self, role: str, content: str, metadata: dict[str, Any] | None = None) -> None:
        self.db.working_append(scope=self.scope, role=role, content=content, metadata=metadata)

    def working_snapshot(self, limit: int | None = None) -> list[dict[str, Any]]:
        return self.db.working_snapshot(scope=self.scope, limit=limit)

    def export_records(
        self,
        filters: dict[str, Any] | None = None,
        limit: int = 1000,
        state: str = "active",
    ) -> list[dict[str, Any]]:
        return self.db.export_records(scope=self.scope, filters=filters, limit=limit, state=state)

    def export_jsonl(
        self,
        path: str | Path | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 1000,
        state: str = "active",
    ) -> dict[str, Any]:
        return self.db.export_jsonl(scope=self.scope, path=path, filters=filters, limit=limit, state=state)

    def import_jsonl(self, path: str | Path) -> list[dict[str, Any]]:
        return self.db.import_jsonl(path=path, scope=self.scope)

    def flush(self) -> dict[str, int]:
        return self.db.flush()

    def run_lifecycle(self) -> dict[str, int]:
        return self.db.run_lifecycle()

    def compact(self) -> None:
        self.db.compact()

    def reindex(self) -> int:
        return self.db.reindex()

    def stats(self) -> dict[str, int]:
        return self.db.stats()

    def recover(self) -> dict[str, int]:
        return self.db.recover()

    def _require_scoped_record(self, head_id: str) -> dict[str, Any]:
        record = self.get(head_id)
        if record is None:
            raise RecordNotFound(f"Memory head not found: {head_id}")
        return record


AIMemory = MemoryDB
ScopedAIMemory = ScopedMemoryDB
