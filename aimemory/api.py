from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from aimemory.catalog.sqlite_store import SQLiteCatalog
from aimemory.config import MemoryConfig
from aimemory.errors import InvalidScope, RecordNotFound
from aimemory.filters import match_filters
from aimemory.hotstore.lmdb_store import LMDBHotStore
from aimemory.outbox import OUTBOX_REBUILD_VECTOR
from aimemory.pipeline.maintenance import EmbeddedMaintenanceWorker, MaintenanceCoordinator
from aimemory.pipeline.lifecycle import compute_fingerprint, now_ms
from aimemory.pipeline.read_path import MemoryReadPath
from aimemory.pipeline.recovery import RecoveryCoordinator
from aimemory.pipeline.write_path import MemoryWritePath
from aimemory.plugins.protocols import Extractor, RetrievalGate, Reranker
from aimemory.serialization import json_dumps, json_loads
from aimemory.scope import Scope
from aimemory.types import SearchQuery, SearchResult
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
        self.config.worker_mode = str(self.config.worker_mode or "library_only").strip().lower().replace("-", "_")
        if self.config.worker_mode not in {"embedded", "library_only"}:
            raise ValueError("worker_mode must be 'embedded' or 'library_only'")
        root = self.config.resolved_root()
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        (root / "backups").mkdir(parents=True, exist_ok=True)
        (root / "exports").mkdir(parents=True, exist_ok=True)
        (root / "lmdb").mkdir(parents=True, exist_ok=True)
        (root / "lancedb").mkdir(parents=True, exist_ok=True)
        self.embedder = embedder or HashEmbedder(self.config.vector_dim)
        self.config.vector_dim = self.embedder.dimension
        self.config.embedding_model = self.embedder.model_name
        self.extractor = extractor
        self.catalog = SQLiteCatalog(root / "catalog.sqlite3")
        self.hotstore = LMDBHotStore(root / "lmdb")
        self.vector_store = LanceVectorStore(root / "lancedb", self.embedder.dimension)
        self.writer = MemoryWritePath(
            config=self.config,
            catalog=self.catalog,
            hotstore=self.hotstore,
            vector_store=self.vector_store,
            embedder=self.embedder,
        )
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
        self._worker: EmbeddedMaintenanceWorker | None = None
        if self.config.recover_on_open:
            self.recovery.recover(batch_size=self.config.recovery_batch_size)
        self._write_runtime_manifest()
        if self.config.worker_mode == "embedded":
            self._worker = EmbeddedMaintenanceWorker(
                config=self.config,
                maintenance=self.maintenance,
                hotstore=self.hotstore,
            )
            self._worker.start()

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
        self._after_mutation()
        return record

    def put_many(self, *, scope: Scope | dict[str, Any], items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = self.writer.put_many(scope=Scope.from_value(scope), items=items)
        self._after_mutation()
        return records

    def ingest_records(self, *, scope: Scope | dict[str, Any], records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result = self.writer.ingest_records(scope=Scope.from_value(scope), records=records)
        self._after_mutation()
        return result

    def ingest_jsonl(self, *, scope: Scope | dict[str, Any], path: str | Path) -> list[dict[str, Any]]:
        result = self.writer.ingest_jsonl(scope=Scope.from_value(scope), path=path)
        self._after_mutation()
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
        self._after_mutation()
        return result

    def get(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any] | None:
        return self.reader.get(scope=Scope.from_value(scope), head_id=head_id)

    def list(self, *, scope: Scope | dict[str, Any], filters: dict[str, Any] | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.reader.list(scope=Scope.from_value(scope), filters=filters, limit=limit)

    def search(self, *, scope: Scope | dict[str, Any], query: str, top_k: int = 10, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        result = self.query(
            scope=scope,
            search=SearchQuery(query=query, top_k=top_k, filters=filters or {}),
        )
        return [asdict(hit) for hit in result.hits]

    def query(
        self,
        *,
        scope: Scope | dict[str, Any],
        search: SearchQuery | str,
        top_k: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> SearchResult:
        if isinstance(search, str):
            search = SearchQuery(query=search, top_k=top_k or 10, filters=filters or {})
        if self.config.auto_flush and self._worker is None:
            self.maintenance.flush_jobs()
        hits = self.reader.query(scope=Scope.from_value(scope), search=search)
        if self._worker is not None:
            self._worker.wake()
        elif self.config.auto_flush:
            flushed = self.maintenance.flush_access_if_needed()
            if flushed > 0:
                self.maintenance.flush_jobs()
        return hits

    def history(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any]:
        return self.reader.history(scope=Scope.from_value(scope), head_id=head_id)

    def delete(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any]:
        record = self.writer.delete(scope=Scope.from_value(scope), head_id=head_id)
        self._after_mutation()
        return record

    def archive(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any]:
        record = self.writer.archive(scope=Scope.from_value(scope), head_id=head_id)
        self._after_mutation()
        return record

    def restore_archive(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any]:
        record = self.writer.restore_archive(scope=Scope.from_value(scope), head_id=head_id)
        self._after_mutation()
        return record

    def restore(self, *, scope: Scope | dict[str, Any], head_id: str) -> dict[str, Any]:
        record = self.writer.restore(scope=Scope.from_value(scope), head_id=head_id)
        self._after_mutation()
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
        rows = self.catalog.list_heads(
            resolved.key,
            state=self._effective_state(state=state, filters=filters),
            limit=limit,
        )
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

    def export_package(
        self,
        *,
        scope: Scope | dict[str, Any],
        path: str | Path | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 1000,
        state: str | None = None,
    ) -> dict[str, Any]:
        resolved = Scope.from_value(scope)
        heads = [row for row in self.catalog.list_heads(resolved.key, state=state, limit=limit) if match_filters(row, filters)]
        bundle = self.catalog.export_bundle([row["head_id"] for row in heads])
        package_dir = Path(path) if path is not None else (self.root / "exports" / f"memory-package-{now_ms()}")
        package_dir.mkdir(parents=True, exist_ok=True)
        counts = {name: len(rows) for name, rows in bundle.items()}
        manifest = {
            "format": "aimemory.export.v1",
            "exported_at": now_ms(),
            "scope": resolved.as_dict(),
            "filters": filters or {},
            "state": state,
            "counts": counts,
        }
        (package_dir / "manifest.json").write_text(json_dumps(manifest), encoding="utf-8")
        for name, rows in bundle.items():
            self._write_jsonl_rows(package_dir / f"{name}.jsonl", rows)
        return {"path": str(package_dir), **counts}

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
        if imported:
            self._after_mutation()
        return imported

    def import_package(self, *, path: str | Path, scope: Scope | dict[str, Any] | None = None) -> dict[str, Any]:
        package_dir = Path(path)
        manifest = self._read_package_manifest(package_dir)
        scope_override = Scope.from_value(scope) if scope is not None else None
        heads = self._read_jsonl_rows(package_dir / "heads.jsonl")
        versions = self._read_jsonl_rows(package_dir / "versions.jsonl")
        chunks = self._read_jsonl_rows(package_dir / "chunks.jsonl")
        events = self._read_jsonl_rows(package_dir / "events.jsonl")
        links = self._read_jsonl_rows(package_dir / "links.jsonl")
        versions_by_id = {row["version_id"]: dict(row) for row in versions}
        imported_heads = [self._normalize_import_head(dict(row), scope_override) for row in heads]
        imported_head_ids = {row["head_id"] for row in imported_heads}
        imported_head_map = {row["head_id"]: row for row in imported_heads}
        imported_chunks = [self._normalize_import_chunk(dict(row), imported_head_map) for row in chunks]
        imported_events = [
            self._normalize_import_event(dict(row), imported_head_map, scope_override)
            for row in events
            if row.get("head_id") in imported_head_ids
        ]
        imported_links = [
            dict(row)
            for row in links
            if row.get("src_head_id") in imported_head_ids and row.get("dst_head_id") in imported_head_ids
        ]
        chunk_ids_by_version: dict[str, list[str]] = {}
        for chunk in imported_chunks:
            chunk_ids_by_version.setdefault(chunk["version_id"], []).append(chunk["chunk_id"])

        fingerprint_updates: dict[str, str] = {}
        mirrored_jobs: list[dict[str, Any]] = []
        dirty_scopes: set[str] = set()
        with self.catalog.transaction():
            for row in imported_heads:
                self.catalog.import_head(row)
                dirty_scopes.add(row["scope_key"])
            for row in versions:
                self.catalog.import_version(dict(row))
            for row in imported_chunks:
                self.catalog.import_chunk(row)
            for row in imported_events:
                self.catalog.import_history_event(row)
            for row in imported_links:
                self.catalog.import_link(row)

            now = now_ms()
            for row in imported_heads:
                if row["state"] != "active":
                    continue
                current_version = versions_by_id.get(row["current_version_id"])
                if current_version is None:
                    raise ValueError(f"Missing current version for imported head {row['head_id']}")
                fingerprint = compute_fingerprint(row["scope_key"], row["kind"], current_version["checksum"])
                fingerprint_updates[fingerprint] = row["head_id"]
                for chunk_id in chunk_ids_by_version.get(row["current_version_id"], []):
                    job_id = self.catalog.enqueue_job(
                        entity_type="chunk",
                        entity_id=chunk_id,
                        op_type=OUTBOX_REBUILD_VECTOR,
                        payload={"chunk_id": chunk_id, "scope_key": row["scope_key"]},
                        now=now,
                    )
                    mirrored_jobs.append(
                        {
                            "job_id": job_id,
                            "op_type": OUTBOX_REBUILD_VECTOR,
                            "scope_key": row["scope_key"],
                            "chunk_id": chunk_id,
                        }
                    )
        self.hotstore.put_fingerprints(fingerprint_updates)
        self.hotstore.mirror_jobs(mirrored_jobs)
        for scope_key in dirty_scopes:
            self.hotstore.clear_query_cache(scope_key)
        if mirrored_jobs:
            self._after_mutation(lightweight=True)
        return {
            "path": str(package_dir),
            "format": manifest.get("format", "aimemory.export.v1"),
            "heads": len(imported_heads),
            "versions": len(versions),
            "chunks": len(imported_chunks),
            "events": len(imported_events),
            "links": len(imported_links),
            "jobs": len(mirrored_jobs),
        }

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

    def worker_status(self) -> dict[str, Any]:
        if self._worker is None:
            return {
                "mode": "library_only",
                "alive": False,
                "leader": False,
                "lease": self.hotstore.get_lease("embedded-maintenance"),
            }
        return self._worker.status()

    def _write_runtime_manifest(self) -> None:
        manifest_path = self.root / "manifest.json"
        existing = json_loads(manifest_path.read_text(encoding="utf-8"), {}) if manifest_path.exists() else {}
        manifest = {
            "format": "aimemory.store.v1",
            "created_at": existing.get("created_at") or now_ms(),
            "updated_at": now_ms(),
            "storage": {
                "catalog": "sqlite",
                "hotstore": "lmdb",
                "vector": "lancedb",
            },
            "embedding_model": self.config.embedding_model,
            "vector_dim": self.config.vector_dim,
        }
        manifest_path.write_text(json_dumps(manifest), encoding="utf-8")

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

    @staticmethod
    def _effective_state(*, state: str | None, filters: dict[str, Any] | None) -> str | list[str] | None:
        if not filters or "state" not in filters:
            return state
        requested = filters["state"]
        if isinstance(requested, dict):
            if "eq" in requested:
                return str(requested["eq"])
            if "in" in requested:
                return [str(item) for item in requested["in"]]
            return state if state != "active" else None
        return str(requested)

    @staticmethod
    def _write_jsonl_rows(path: Path, rows: list[dict[str, Any]]) -> None:
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json_dumps(row))
                handle.write("\n")

    @staticmethod
    def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                payload = json_loads(line, None)
                if not isinstance(payload, dict):
                    raise ValueError(f"Invalid JSON object at line {line_no} in {path}")
                rows.append(payload)
        return rows

    @staticmethod
    def _read_package_manifest(path: Path) -> dict[str, Any]:
        manifest_path = path / "manifest.json"
        if not manifest_path.exists():
            raise ValueError(f"Missing package manifest: {manifest_path}")
        manifest = json_loads(manifest_path.read_text(encoding="utf-8"), None)
        if not isinstance(manifest, dict):
            raise ValueError(f"Invalid package manifest: {manifest_path}")
        return manifest

    @staticmethod
    def _normalize_import_head(row: dict[str, Any], scope_override: Scope | None) -> dict[str, Any]:
        scope = scope_override or Scope.from_value(
            {
                "tenant_id": row.get("tenant_id") or "local",
                "workspace_id": row.get("workspace_id"),
                "project_id": row.get("project_id"),
                "user_id": row.get("user_id"),
                "agent_id": row.get("agent_id"),
                "session_id": row.get("session_id"),
                "run_id": row.get("run_id"),
                "namespace": row.get("namespace") or "default",
                "visibility": row.get("visibility") or "private",
            }
        )
        row.update(scope.as_dict())
        row["scope_key"] = scope.key
        row["metadata"] = dict(row.get("metadata") or {}) | scope.as_dict()
        return row

    @staticmethod
    def _normalize_import_chunk(row: dict[str, Any], heads: dict[str, dict[str, Any]]) -> dict[str, Any]:
        head = heads.get(row["head_id"])
        if head is None:
            raise ValueError(f"Missing imported head for chunk {row['chunk_id']}")
        row["scope_key"] = head["scope_key"]
        return row

    @staticmethod
    def _normalize_import_event(
        row: dict[str, Any],
        heads: dict[str, dict[str, Any]],
        scope_override: Scope | None,
    ) -> dict[str, Any]:
        if row.get("head_id") in heads:
            row["scope_key"] = heads[row["head_id"]]["scope_key"]
        elif scope_override is not None:
            row["scope_key"] = scope_override.key
        return row

    def close(self) -> None:
        if self._worker is not None:
            self._worker.stop()
        self.catalog.close()
        self.hotstore.close()

    def _after_mutation(self, *, lightweight: bool = False) -> None:
        if self._worker is not None:
            self._worker.wake()
            return
        if not self.config.auto_flush:
            return
        if lightweight:
            self.maintenance.flush_jobs()
            return
        self.flush()

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

    def query(self, search: SearchQuery | str, top_k: int | None = None, filters: dict[str, Any] | None = None) -> SearchResult:
        return self.db.query(scope=self.scope, search=search, top_k=top_k, filters=filters)

    def history(self, head_id: str) -> dict[str, Any]:
        self._require_scoped_record(head_id)
        return self.db.history(scope=self.scope, head_id=head_id)

    def delete(self, head_id: str) -> dict[str, Any]:
        self._require_scoped_record(head_id)
        return self.db.delete(scope=self.scope, head_id=head_id)

    def archive(self, head_id: str) -> dict[str, Any]:
        self._require_scoped_record(head_id)
        return self.db.archive(scope=self.scope, head_id=head_id)

    def restore_archive(self, head_id: str) -> dict[str, Any]:
        self._require_scoped_record(head_id)
        return self.db.restore_archive(scope=self.scope, head_id=head_id)

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

    def export_package(
        self,
        path: str | Path | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 1000,
        state: str | None = None,
    ) -> dict[str, Any]:
        return self.db.export_package(scope=self.scope, path=path, filters=filters, limit=limit, state=state)

    def import_package(self, path: str | Path) -> dict[str, Any]:
        return self.db.import_package(path=path, scope=self.scope)

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

    def worker_status(self) -> dict[str, Any]:
        return self.db.worker_status()

    def _require_scoped_record(self, head_id: str) -> dict[str, Any]:
        record = self.get(head_id)
        if record is None:
            raise RecordNotFound(f"Memory head not found: {head_id}")
        return record


AIMemory = MemoryDB
ScopedAIMemory = ScopedMemoryDB
