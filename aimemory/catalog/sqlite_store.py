from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

from aimemory.catalog.schema import SCHEMA_STATEMENTS
from aimemory.ids import make_id
from aimemory.scope import Scope
from aimemory.serialization import json_dumps, json_loads


class SQLiteCatalog:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._initialize()

    def _initialize(self) -> None:
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        with self.transaction():
            for statement in SCHEMA_STATEMENTS:
                self._conn.execute(statement)
            self._ensure_compat_columns()

    @contextmanager
    def transaction(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _ensure_compat_columns(self) -> None:
        self._ensure_column("memory_versions", "chunk_strategy", "TEXT")
        self._ensure_column("memory_versions", "created_by", "TEXT")

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        if any(row["name"] == column for row in rows):
            return
        self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def find_current_by_fact_key(self, scope_key: str, kind: str, fact_key: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT h.*, v.version_id, v.text, v.abstract, v.overview, v.checksum
            FROM memory_heads h
            JOIN memory_versions v ON v.version_id = h.current_version_id
            WHERE h.scope_key = ? AND h.kind = ? AND h.fact_key = ? AND h.state = 'active'
            LIMIT 1
            """,
            (scope_key, kind, fact_key),
        ).fetchone()
        return self._row_to_record(row)

    def find_current_by_checksum(self, scope_key: str, kind: str, checksum: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT h.*, v.version_id, v.text, v.abstract, v.overview, v.checksum
            FROM memory_heads h
            JOIN memory_versions v ON v.version_id = h.current_version_id
            WHERE h.scope_key = ? AND h.kind = ? AND v.checksum = ? AND h.state = 'active'
            LIMIT 1
            """,
            (scope_key, kind, checksum),
        ).fetchone()
        return self._row_to_record(row)

    def create_head(
        self,
        *,
        scope: Scope,
        kind: str,
        layer: str,
        tier: str,
        state: str,
        fact_key: str | None,
        version_id: str,
        importance: float,
        confidence: float,
        now: int,
        metadata: dict[str, Any],
    ) -> str:
        head_id = make_id("head")
        self._conn.execute(
            """
            INSERT INTO memory_heads (
                head_id, scope_key, tenant_id, workspace_id, project_id, user_id, agent_id, session_id, run_id,
                namespace, visibility, kind, layer, tier, state, fact_key, current_version_id, importance,
                confidence, access_count, last_accessed_at, created_at, updated_at, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
            """,
            (
                head_id,
                scope.key,
                scope.tenant_id,
                scope.workspace_id,
                scope.project_id,
                scope.user_id,
                scope.agent_id,
                scope.session_id,
                scope.run_id,
                scope.namespace,
                scope.visibility,
                kind,
                layer,
                tier,
                state,
                fact_key,
                version_id,
                importance,
                confidence,
                now,
                now,
                now,
                json_dumps(metadata),
            ),
        )
        return head_id

    def create_version(
        self,
        *,
        head_id: str,
        version_no: int,
        text: str,
        abstract: str,
        overview: str,
        checksum: str,
        change_type: str,
        valid_from: int,
        source_type: str | None,
        source_ref: str | None,
        embedding_model: str | None,
        chunk_strategy: str | None,
        created_by: str | None,
        created_at: int,
        metadata: dict[str, Any],
    ) -> str:
        version_id = make_id("ver")
        self._conn.execute(
            """
            INSERT INTO memory_versions (
                version_id, head_id, version_no, text, abstract, overview, checksum, change_type,
                valid_from, valid_to, source_type, source_ref, embedding_model, chunk_strategy, created_by,
                created_at, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_id,
                head_id,
                version_no,
                text,
                abstract,
                overview,
                checksum,
                change_type,
                valid_from,
                source_type,
                source_ref,
                embedding_model,
                chunk_strategy,
                created_by,
                created_at,
                json_dumps(metadata),
            ),
        )
        return version_id

    def next_version_no(self, head_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(version_no), 0) + 1 AS next_version_no FROM memory_versions WHERE head_id = ?",
            (head_id,),
        ).fetchone()
        return int(row["next_version_no"])

    def supersede_head(
        self,
        *,
        head_id: str,
        previous_version_id: str,
        new_version_id: str,
        tier: str,
        importance: float,
        confidence: float,
        now: int,
    ) -> None:
        self._conn.execute(
            "UPDATE memory_versions SET valid_to = ? WHERE version_id = ?",
            (now, previous_version_id),
        )
        self._conn.execute(
            """
            UPDATE memory_heads
            SET current_version_id = ?, tier = ?, importance = ?, confidence = ?, updated_at = ?
            WHERE head_id = ?
            """,
            (new_version_id, tier, importance, confidence, now, head_id),
        )

    def touch_head(self, head_id: str, now: int) -> None:
        self._conn.execute("UPDATE memory_heads SET updated_at = ? WHERE head_id = ?", (now, head_id))

    def create_chunks(
        self,
        *,
        head_id: str,
        version_id: str,
        scope_key: str,
        chunks: Iterable[dict[str, Any]],
        created_at: int,
    ) -> list[str]:
        chunk_ids: list[str] = []
        for chunk in chunks:
            chunk_id = make_id("chk")
            cursor = self._conn.execute(
                """
                INSERT INTO memory_chunks (
                    chunk_id, head_id, version_id, scope_key, chunk_no, text, token_count,
                    char_start, char_end, embedding_state, created_at, updated_at, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    chunk_id,
                    head_id,
                    version_id,
                    scope_key,
                    chunk["chunk_no"],
                    chunk["text"],
                    chunk["token_count"],
                    chunk["char_start"],
                    chunk["char_end"],
                    created_at,
                    created_at,
                    json_dumps({}),
                ),
            )
            rowid = cursor.lastrowid
            self._conn.execute(
                "INSERT INTO memory_chunks_fts(rowid, text, scope_key, head_id, version_id) VALUES (?, ?, ?, ?, ?)",
                (rowid, chunk["text"], scope_key, head_id, version_id),
            )
            chunk_ids.append(chunk_id)
        return chunk_ids

    def list_chunk_ids_for_version(self, version_id: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT chunk_id FROM memory_chunks WHERE version_id = ? ORDER BY chunk_no",
            (version_id,),
        ).fetchall()
        return [row["chunk_id"] for row in rows]

    def get_chunk_for_index(self, chunk_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT c.chunk_id, c.head_id, c.version_id, c.scope_key, c.text, h.kind, h.tier, h.importance,
                   h.confidence, v.abstract, v.overview, h.updated_at, h.state, c.created_at, v.valid_from, v.valid_to
            FROM memory_chunks c
            JOIN memory_heads h ON h.head_id = c.head_id
            JOIN memory_versions v ON v.version_id = c.version_id
            WHERE c.chunk_id = ?
            LIMIT 1
            """,
            (chunk_id,),
        ).fetchone()
        if row is None:
            return None
        head = self.get_head(row["head_id"])
        if head is None or head["state"] != "active" or row["version_id"] != head["version_id"]:
            return None
        return dict(row)

    def get_indexable_chunks(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT c.chunk_id, c.head_id, c.version_id, c.scope_key, c.text, h.kind, h.tier, h.importance,
                   h.confidence, v.abstract, v.overview, h.updated_at, c.created_at, v.valid_from, v.valid_to
            FROM memory_chunks c
            JOIN memory_heads h ON h.head_id = c.head_id
            JOIN memory_versions v ON v.version_id = c.version_id
            WHERE h.state = 'active' AND c.version_id = h.current_version_id
            ORDER BY h.updated_at DESC, c.chunk_no ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def list_lifecycle_candidates(self, limit: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT head_id, scope_key, current_version_id, tier, importance, confidence,
                   access_count, last_accessed_at, created_at, updated_at
            FROM memory_heads
            WHERE state = 'active'
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [
            {
                "head_id": row["head_id"],
                "scope_key": row["scope_key"],
                "current_version_id": row["current_version_id"],
                "tier": row["tier"],
                "importance": float(row["importance"]),
                "confidence": float(row["confidence"]),
                "access_count": int(row["access_count"]),
                "last_accessed_at": row["last_accessed_at"],
                "created_at": int(row["created_at"]),
                "updated_at": int(row["updated_at"]),
            }
            for row in rows
        ]

    def update_head_tier(self, head_id: str, tier: str) -> None:
        self._conn.execute("UPDATE memory_heads SET tier = ? WHERE head_id = ?", (tier, head_id))

    def mark_chunk_embedding_state(self, chunk_id: str, state: str) -> None:
        self._conn.execute(
            "UPDATE memory_chunks SET embedding_state = ?, updated_at = updated_at + 1 WHERE chunk_id = ?",
            (state, chunk_id),
        )

    def mark_chunk_embedding_states(self, chunk_ids: Iterable[str], state: str) -> None:
        for chunk_id in chunk_ids:
            self.mark_chunk_embedding_state(chunk_id, state)

    def enqueue_job(self, *, entity_type: str, entity_id: str, op_type: str, payload: dict[str, Any], now: int) -> str:
        job_id = make_id("job")
        self._conn.execute(
            """
            INSERT INTO outbox_jobs (
                job_id, entity_type, entity_id, op_type, payload_json, status,
                retry_count, available_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
            """,
            (job_id, entity_type, entity_id, op_type, json_dumps(payload), now, now, now),
        )
        return job_id

    def pull_pending_jobs(self, limit: int) -> list[dict[str, Any]]:
        now = int(time.time() * 1000)
        with self.transaction():
            rows = self._conn.execute(
                """
                SELECT * FROM outbox_jobs
                WHERE status IN ('pending', 'failed') AND available_at <= ?
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    "UPDATE outbox_jobs SET status = 'running', updated_at = ? WHERE job_id = ?",
                    (now, row["job_id"]),
                )
        return [dict(row) | {"payload": json_loads(row["payload_json"], {})} for row in rows]

    def list_recoverable_jobs(self, limit: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM outbox_jobs
            WHERE status IN ('pending', 'failed', 'running')
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) | {"payload": json_loads(row["payload_json"], {})} for row in rows]

    def reset_recoverable_jobs(self, now: int) -> int:
        cursor = self._conn.execute(
            """
            UPDATE outbox_jobs
            SET status = 'pending', available_at = ?, updated_at = ?
            WHERE status IN ('running', 'failed')
            """,
            (now, now),
        )
        return int(cursor.rowcount or 0)

    def list_chunks_needing_recovery(self, limit: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT c.chunk_id, c.scope_key
            FROM memory_chunks c
            JOIN memory_heads h ON h.head_id = c.head_id
            WHERE h.state = 'active'
              AND c.version_id = h.current_version_id
              AND c.embedding_state != 'ready'
              AND NOT EXISTS (
                    SELECT 1
                    FROM outbox_jobs o
                    WHERE o.entity_id = c.chunk_id
                      AND o.op_type = 'upsert_vector'
                      AND o.status IN ('pending', 'running', 'failed')
              )
            ORDER BY c.created_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def finish_job(self, job_id: str, status: str, now: int, retry_count: int | None = None) -> None:
        if retry_count is None:
            self._conn.execute("UPDATE outbox_jobs SET status = ?, updated_at = ? WHERE job_id = ?", (status, now, job_id))
        else:
            self._conn.execute(
                "UPDATE outbox_jobs SET status = ?, retry_count = ?, updated_at = ?, available_at = ? WHERE job_id = ?",
                (status, retry_count, now, now + 1000, job_id),
            )

    def add_history_event(
        self,
        *,
        scope_key: str,
        head_id: str | None,
        version_id: str | None,
        event_type: str,
        payload: dict[str, Any],
        created_at: int,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO history_events(event_id, scope_key, head_id, version_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (make_id("evt"), scope_key, head_id, version_id, event_type, json_dumps(payload), created_at),
        )

    def add_link(
        self,
        *,
        src_head_id: str,
        dst_head_id: str,
        relation_type: str,
        created_at: int,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO memory_links(link_id, src_head_id, dst_head_id, relation_type, weight, created_at, metadata_json)
            VALUES (?, ?, ?, ?, 1.0, ?, ?)
            """,
            (make_id("lnk"), src_head_id, dst_head_id, relation_type, created_at, json_dumps(metadata or {})),
        )

    def get_head(self, head_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT h.*, v.version_id, v.text, v.abstract, v.overview, v.checksum
            FROM memory_heads h
            JOIN memory_versions v ON v.version_id = h.current_version_id
            WHERE h.head_id = ?
            LIMIT 1
            """,
            (head_id,),
        ).fetchone()
        return self._row_to_record(row)

    def list_heads(self, scope_key: str, *, state: str = "active", limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT h.*, v.version_id, v.text, v.abstract, v.overview, v.checksum
            FROM memory_heads h
            JOIN memory_versions v ON v.version_id = h.current_version_id
            WHERE h.scope_key = ? AND h.state = ?
            ORDER BY h.updated_at DESC
            LIMIT ?
            """,
            (scope_key, state, limit),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def search_lexical(self, scope_key: str, query: str, limit: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT c.chunk_id, c.head_id, c.version_id, c.text AS chunk_text,
                   v.abstract, v.overview, h.kind, h.layer, h.tier, h.importance, h.confidence,
                   h.access_count, h.updated_at, h.last_accessed_at, h.metadata_json,
                   v.valid_from, v.valid_to, bm25(memory_chunks_fts) AS rank
            FROM memory_chunks_fts
            JOIN memory_chunks c ON c.chunk_pk = memory_chunks_fts.rowid
            JOIN memory_heads h ON h.head_id = c.head_id
            JOIN memory_versions v ON v.version_id = h.current_version_id
            WHERE memory_chunks_fts MATCH ? AND h.scope_key = ? AND h.state = 'active' AND c.version_id = h.current_version_id
            ORDER BY rank
            LIMIT ?
            """,
            (query, scope_key, limit),
        ).fetchall()
        return [
            {
                "chunk_id": row["chunk_id"],
                "head_id": row["head_id"],
                "version_id": row["version_id"],
                "text": row["chunk_text"],
                "abstract": row["abstract"],
                "overview": row["overview"],
                "kind": row["kind"],
                "layer": row["layer"],
                "tier": row["tier"],
                "importance": float(row["importance"]),
                "confidence": float(row["confidence"]),
                "access_count": int(row["access_count"]),
                "updated_at": int(row["updated_at"]),
                "last_accessed_at": row["last_accessed_at"],
                "valid_from": int(row["valid_from"]),
                "valid_to": row["valid_to"],
                "metadata": json_loads(row["metadata_json"], {}),
                "rank": float(row["rank"]),
            }
            for row in rows
        ]

    def soft_delete(self, head_id: str, now: int) -> dict[str, Any]:
        self._conn.execute("UPDATE memory_heads SET state = 'deleted', updated_at = ? WHERE head_id = ?", (now, head_id))
        return self.get_head(head_id)

    def restore(self, head_id: str, now: int) -> dict[str, Any]:
        self._conn.execute("UPDATE memory_heads SET state = 'active', updated_at = ? WHERE head_id = ?", (now, head_id))
        return self.get_head(head_id)

    def get_history(self, head_id: str) -> dict[str, Any]:
        versions = self._conn.execute(
            """
            SELECT version_id, version_no, text, abstract, overview, checksum, change_type, valid_from, valid_to,
                   source_type, source_ref, created_at, metadata_json
            FROM memory_versions
            WHERE head_id = ?
            ORDER BY version_no ASC
            """,
            (head_id,),
        ).fetchall()
        events = self._conn.execute(
            """
            SELECT event_type, payload_json, created_at
            FROM history_events
            WHERE head_id = ?
            ORDER BY created_at ASC
            """,
            (head_id,),
        ).fetchall()
        return {
            "versions": [
                {
                    "version_id": row["version_id"],
                    "version_no": row["version_no"],
                    "text": row["text"],
                    "abstract": row["abstract"],
                    "overview": row["overview"],
                    "checksum": row["checksum"],
                    "change_type": row["change_type"],
                    "valid_from": row["valid_from"],
                    "valid_to": row["valid_to"],
                    "source_type": row["source_type"],
                    "source_ref": row["source_ref"],
                    "created_at": row["created_at"],
                    "metadata": json_loads(row["metadata_json"], {}),
                }
                for row in versions
            ],
            "events": [
                {
                    "event_type": row["event_type"],
                    "created_at": row["created_at"],
                    "payload": json_loads(row["payload_json"], {}),
                }
                for row in events
            ],
        }

    def apply_access_updates(self, updates: dict[str, int], now: int) -> None:
        for head_id, delta in updates.items():
            self._conn.execute(
                """
                UPDATE memory_heads
                SET access_count = access_count + ?, last_accessed_at = ?, updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
                WHERE head_id = ?
                """,
                (delta, now, now, now, head_id),
            )

    def count_pending_jobs(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS c FROM outbox_jobs WHERE status IN ('pending', 'failed', 'running')"
        ).fetchone()
        return int(row["c"])

    def stats(self) -> dict[str, int]:
        tables = {
            "heads": "memory_heads",
            "versions": "memory_versions",
            "chunks": "memory_chunks",
            "pending_jobs": None,
        }
        result: dict[str, int] = {}
        for key, table in tables.items():
            if table is None:
                result[key] = self.count_pending_jobs()
                continue
            row = self._conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            result[key] = int(row["c"])
        return result

    @staticmethod
    def _row_to_record(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "head_id": row["head_id"],
            "version_id": row["version_id"],
            "scope_key": row["scope_key"],
            "tenant_id": row["tenant_id"],
            "workspace_id": row["workspace_id"],
            "project_id": row["project_id"],
            "user_id": row["user_id"],
            "agent_id": row["agent_id"],
            "session_id": row["session_id"],
            "run_id": row["run_id"],
            "namespace": row["namespace"],
            "visibility": row["visibility"],
            "kind": row["kind"],
            "layer": row["layer"],
            "tier": row["tier"],
            "state": row["state"],
            "text": row["text"],
            "abstract": row["abstract"],
            "overview": row["overview"],
            "fact_key": row["fact_key"],
            "importance": float(row["importance"]),
            "confidence": float(row["confidence"]),
            "access_count": int(row["access_count"]),
            "last_accessed_at": row["last_accessed_at"],
            "created_at": int(row["created_at"]),
            "updated_at": int(row["updated_at"]),
            "metadata": json_loads(row["metadata_json"], {}),
            "checksum": row["checksum"],
        }
