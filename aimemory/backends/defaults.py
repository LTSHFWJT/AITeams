from __future__ import annotations

from typing import Any

from aimemory.core.capabilities import capability_dict
from aimemory.core.text import extract_keywords, hybrid_score
from aimemory.core.utils import json_dumps, json_loads, make_id, stable_edge_id, utcnow_iso
from aimemory.storage.kuzu.graph_store import KuzuGraphStore
from aimemory.storage.lancedb.index_store import LanceIndexStore


class SQLiteIndexBackend:
    backend_name = "sqlite"
    active_backend = "sqlite"
    available = True

    def __init__(self, db, lancedb_store=None, config=None):
        self.db = db
        self.config = config
        self.lancedb_store = lancedb_store

    def upsert_memory(self, payload: dict[str, Any]) -> None:
        record_id = payload["record_id"]
        keywords = payload.get("keywords") or extract_keywords(payload.get("text"))
        self.db.execute(
            """
            INSERT INTO memory_index(record_id, domain, scope, user_id, owner_agent_id, subject_type, subject_id, interaction_type, session_id, text, keywords, score_boost, updated_at, metadata)
            VALUES (?, 'memory', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                scope = excluded.scope,
                user_id = excluded.user_id,
                owner_agent_id = excluded.owner_agent_id,
                subject_type = excluded.subject_type,
                subject_id = excluded.subject_id,
                interaction_type = excluded.interaction_type,
                session_id = excluded.session_id,
                text = excluded.text,
                keywords = excluded.keywords,
                score_boost = excluded.score_boost,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                record_id,
                payload["scope"],
                payload.get("user_id"),
                payload.get("owner_agent_id"),
                payload.get("subject_type"),
                payload.get("subject_id"),
                payload.get("interaction_type"),
                payload.get("session_id"),
                payload.get("text", ""),
                json_dumps(keywords),
                payload.get("score_boost", 0.0),
                payload.get("updated_at", utcnow_iso()),
                json_dumps(payload.get("metadata", {})),
            ),
        )
        if self.lancedb_store:
            self.lancedb_store.upsert("memory_index", record_id, payload.get("text", ""), payload)

    def delete_memory(self, record_id: str) -> None:
        self.db.execute("DELETE FROM memory_index WHERE record_id = ?", (record_id,))
        if self.lancedb_store:
            self.lancedb_store.delete("memory_index", record_id)

    def list_memory_candidates(
        self,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        sql_filters = ["1 = 1"]
        params: list[Any] = []
        if scope == "session":
            sql_filters.append("scope = 'session'")
            if session_id:
                sql_filters.append("session_id = ?")
                params.append(session_id)
            if user_id:
                sql_filters.append("user_id = ?")
                params.append(user_id)
        elif scope == "long-term":
            sql_filters.append("scope = 'long-term'")
            if user_id:
                sql_filters.append("user_id = ?")
                params.append(user_id)
        else:
            if session_id and user_id:
                sql_filters.append("((scope = 'long-term' AND user_id = ?) OR (scope = 'session' AND session_id = ?))")
                params.extend([user_id, session_id])
            elif user_id:
                sql_filters.append("user_id = ?")
                params.append(user_id)
            elif session_id:
                sql_filters.append("session_id = ?")
                params.append(session_id)

        sql = f"SELECT * FROM memory_index WHERE {' AND '.join(sql_filters)} ORDER BY updated_at DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return self.db.fetch_all(sql, tuple(params))

    def search_memory_candidates(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.list_memory_candidates(user_id=user_id, session_id=session_id, scope=scope, limit=limit)
        return self._rank_query_rows(query, rows, limit=limit, boost_field="score_boost")

    def upsert_knowledge_chunk(self, payload: dict[str, Any]) -> None:
        record_id = payload["record_id"]
        keywords = payload.get("keywords") or extract_keywords(payload.get("text"))
        self.db.execute(
            """
            INSERT INTO knowledge_chunk_index(record_id, document_id, source_id, owner_agent_id, source_subject_type, source_subject_id, title, text, keywords, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                document_id = excluded.document_id,
                source_id = excluded.source_id,
                owner_agent_id = excluded.owner_agent_id,
                source_subject_type = excluded.source_subject_type,
                source_subject_id = excluded.source_subject_id,
                title = excluded.title,
                text = excluded.text,
                keywords = excluded.keywords,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                record_id,
                payload["document_id"],
                payload.get("source_id"),
                payload.get("owner_agent_id"),
                payload.get("source_subject_type"),
                payload.get("source_subject_id"),
                payload.get("title"),
                payload.get("text", ""),
                json_dumps(keywords),
                payload.get("updated_at", utcnow_iso()),
                json_dumps(payload.get("metadata", {})),
            ),
        )
        if self.lancedb_store:
            self.lancedb_store.upsert("knowledge_chunk_index", record_id, payload.get("text", ""), payload)

    def delete_knowledge_chunk(self, record_id: str) -> None:
        self.db.execute("DELETE FROM knowledge_chunk_index WHERE record_id = ?", (record_id,))
        if self.lancedb_store:
            self.lancedb_store.delete("knowledge_chunk_index", record_id)

    def list_knowledge_chunks(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM knowledge_chunk_index ORDER BY updated_at DESC"
        params: list[Any] = []
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return self.db.fetch_all(sql, tuple(params))

    def search_knowledge_chunks(self, query: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        rows = self.list_knowledge_chunks(limit=limit)
        return self._rank_query_rows(query, rows, limit=limit)

    def upsert_skill(self, payload: dict[str, Any]) -> None:
        record_id = payload["record_id"]
        keywords = payload.get("keywords") or extract_keywords(payload.get("text"))
        self.db.execute(
            """
            INSERT INTO skill_index(record_id, skill_id, version, owner_agent_id, source_subject_type, source_subject_id, name, description, text, keywords, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                skill_id = excluded.skill_id,
                version = excluded.version,
                owner_agent_id = excluded.owner_agent_id,
                source_subject_type = excluded.source_subject_type,
                source_subject_id = excluded.source_subject_id,
                name = excluded.name,
                description = excluded.description,
                text = excluded.text,
                keywords = excluded.keywords,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                record_id,
                payload["skill_id"],
                payload.get("version", "1.0.0"),
                payload.get("owner_agent_id"),
                payload.get("source_subject_type"),
                payload.get("source_subject_id"),
                payload["name"],
                payload.get("description"),
                payload.get("text", ""),
                json_dumps(keywords),
                payload.get("updated_at", utcnow_iso()),
                json_dumps(payload.get("metadata", {})),
            ),
        )
        if self.lancedb_store:
            self.lancedb_store.upsert("skill_index", record_id, payload.get("text", ""), payload)

    def delete_skill(self, record_id: str) -> None:
        self.db.execute("DELETE FROM skill_index WHERE record_id = ?", (record_id,))
        if self.lancedb_store:
            self.lancedb_store.delete("skill_index", record_id)

    def list_skill_records(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM skill_index ORDER BY updated_at DESC"
        params: list[Any] = []
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return self.db.fetch_all(sql, tuple(params))

    def search_skill_records(self, query: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        rows = self.list_skill_records(limit=limit)
        return self._rank_query_rows(query, rows, limit=limit)

    def upsert_archive_summary(self, payload: dict[str, Any]) -> None:
        record_id = payload["record_id"]
        keywords = payload.get("keywords") or extract_keywords(payload.get("text"))
        self.db.execute(
            """
            INSERT INTO archive_summary_index(record_id, archive_unit_id, domain, user_id, owner_agent_id, subject_type, subject_id, interaction_type, source_type, session_id, text, keywords, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
                archive_unit_id = excluded.archive_unit_id,
                domain = excluded.domain,
                user_id = excluded.user_id,
                owner_agent_id = excluded.owner_agent_id,
                subject_type = excluded.subject_type,
                subject_id = excluded.subject_id,
                interaction_type = excluded.interaction_type,
                source_type = excluded.source_type,
                session_id = excluded.session_id,
                text = excluded.text,
                keywords = excluded.keywords,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata
            """,
            (
                record_id,
                payload["archive_unit_id"],
                payload["domain"],
                payload.get("user_id"),
                payload.get("owner_agent_id"),
                payload.get("subject_type"),
                payload.get("subject_id"),
                payload.get("interaction_type"),
                payload.get("source_type"),
                payload.get("session_id"),
                payload.get("text", ""),
                json_dumps(keywords),
                payload.get("updated_at", utcnow_iso()),
                json_dumps(payload.get("metadata", {})),
            ),
        )
        if self.lancedb_store:
            self.lancedb_store.upsert("archive_summary_index", record_id, payload.get("text", ""), payload)

    def delete_archive_summary(self, record_id: str) -> None:
        self.db.execute("DELETE FROM archive_summary_index WHERE record_id = ?", (record_id,))
        if self.lancedb_store:
            self.lancedb_store.delete("archive_summary_index", record_id)

    def list_archive_summaries(
        self,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        sql_filters = ["1 = 1"]
        params: list[Any] = []
        if user_id:
            sql_filters.append("user_id = ?")
            params.append(user_id)
        if session_id:
            sql_filters.append("session_id = ?")
            params.append(session_id)
        sql = f"SELECT * FROM archive_summary_index WHERE {' AND '.join(sql_filters)} ORDER BY updated_at DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return self.db.fetch_all(sql, tuple(params))

    def search_archive_summaries(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.list_archive_summaries(user_id=user_id, session_id=session_id, limit=limit)
        return self._rank_query_rows(query, rows, limit=limit)

    def _rank_query_rows(
        self,
        query: str,
        rows: list[dict[str, Any]],
        *,
        limit: int | None = None,
        boost_field: str | None = None,
    ) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            boost = float(item.get(boost_field, 0.0) or 0.0) if boost_field else 0.0
            item["score"] = hybrid_score(query, item.get("text", ""), json_loads(item.get("keywords"), []), boost)
            ranked.append(item)
        ranked.sort(key=lambda item: item.get("score", 0.0), reverse=True)
        return ranked[:limit] if limit is not None else ranked

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="index_backend",
            provider=self.backend_name,
            active_provider=self.active_backend,
            features={
                "native_search": False,
                "keyword_search": True,
                "hybrid_search": True,
                "batch_projection": True,
            },
            notes=["sqlite fallback index"],
        )


class LanceDBIndexBackend(SQLiteIndexBackend):
    backend_name = "lancedb"

    def __init__(self, db, lancedb_store=None, config=None):
        if lancedb_store is None and config is not None:
            lancedb_store = LanceIndexStore(config.lancedb_path)
        super().__init__(db=db, lancedb_store=lancedb_store, config=config)
        self.available = bool(getattr(self.lancedb_store, "available", False))
        self.active_backend = "lancedb" if self.available else "sqlite"

    def search_memory_candidates(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        if not self.available:
            return super().search_memory_candidates(query, user_id=user_id, session_id=session_id, scope=scope, limit=limit)
        rows = self.lancedb_store.search(
            "memory_index",
            query,
            limit=limit or 10,
            where=self._memory_where(user_id=user_id, session_id=session_id, scope=scope),
        )
        if not rows:
            return super().search_memory_candidates(query, user_id=user_id, session_id=session_id, scope=scope, limit=limit)
        return self._rank_lance_rows(query, rows, limit=limit, boost_field="score_boost")

    def search_knowledge_chunks(self, query: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        if not self.available:
            return super().search_knowledge_chunks(query, limit=limit)
        rows = self.lancedb_store.search("knowledge_chunk_index", query, limit=limit or 10)
        if not rows:
            return super().search_knowledge_chunks(query, limit=limit)
        return self._rank_lance_rows(query, rows, limit=limit)

    def search_skill_records(self, query: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        if not self.available:
            return super().search_skill_records(query, limit=limit)
        rows = self.lancedb_store.search("skill_index", query, limit=limit or 10)
        if not rows:
            return super().search_skill_records(query, limit=limit)
        return self._rank_lance_rows(query, rows, limit=limit)

    def search_archive_summaries(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        if not self.available:
            return super().search_archive_summaries(query, user_id=user_id, session_id=session_id, limit=limit)
        rows = self.lancedb_store.search(
            "archive_summary_index",
            query,
            limit=limit or 10,
            where=self._archive_where(user_id=user_id, session_id=session_id),
        )
        if not rows:
            return super().search_archive_summaries(query, user_id=user_id, session_id=session_id, limit=limit)
        return self._rank_lance_rows(query, rows, limit=limit)

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="index_backend",
            provider=self.backend_name,
            active_provider=self.active_backend,
            features={
                "native_search": bool(self.available),
                "keyword_search": True,
                "hybrid_search": True,
                "batch_projection": True,
            },
            notes=["falls back to sqlite when LanceDB is unavailable"] if not self.available else [],
        )

    def _rank_lance_rows(
        self,
        query: str,
        rows: list[dict[str, Any]],
        *,
        limit: int | None = None,
        boost_field: str | None = None,
    ) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        for row in rows:
            item = self._normalize_lance_row(row)
            boost = float(item.get(boost_field, 0.0) or 0.0) if boost_field else 0.0
            lexical_score = hybrid_score(query, item.get("text", ""), item.get("keywords"), boost)
            distance = float(item.get("_distance", 1.0) or 1.0)
            vector_score = 1.0 / (1.0 + max(distance, 0.0))
            item["score"] = round((0.58 * vector_score) + (0.42 * lexical_score), 6)
            ranked.append(item)
        ranked.sort(key=lambda item: item.get("score", 0.0), reverse=True)
        return ranked[:limit] if limit is not None else ranked

    def _normalize_lance_row(self, row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        if "id" in item and "record_id" not in item:
            item["record_id"] = item["id"]
        for field in ("user_id", "session_id", "source_id", "title", "name", "description", "domain", "scope", "memory_type", "version"):
            if field in item and item[field] == "":
                item[field] = None
        return item

    def _memory_where(self, *, user_id: str | None, session_id: str | None, scope: str) -> str | None:
        clauses: list[str] = []
        if scope == "session":
            clauses.append("scope = 'session'")
            if session_id:
                clauses.append(f"session_id = {self._quote(session_id)}")
        elif scope == "long-term":
            clauses.append("scope = 'long-term'")
            if user_id:
                clauses.append(f"user_id = {self._quote(user_id)}")
        else:
            if session_id and user_id:
                clauses.append(
                    f"((scope = 'long-term' AND user_id = {self._quote(user_id)}) OR "
                    f"(scope = 'session' AND session_id = {self._quote(session_id)}))"
                )
            elif user_id:
                clauses.append(f"user_id = {self._quote(user_id)}")
            elif session_id:
                clauses.append(f"session_id = {self._quote(session_id)}")
        return " AND ".join(clauses) if clauses else None

    def _archive_where(self, *, user_id: str | None, session_id: str | None) -> str | None:
        clauses: list[str] = []
        if user_id:
            clauses.append(f"user_id = {self._quote(user_id)}")
        if session_id:
            clauses.append(f"session_id = {self._quote(session_id)}")
        return " AND ".join(clauses) if clauses else None

    def _quote(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"


class SQLiteGraphBackend:
    backend_name = "sqlite"
    active_backend = "sqlite"
    available = True

    def __init__(self, db, kuzu_store=None, config=None):
        self.db = db
        self.config = config
        self.kuzu_store = kuzu_store

    def upsert_node(
        self,
        node_type: str,
        ref_id: str,
        label: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        existing = self.db.fetch_one("SELECT * FROM graph_nodes WHERE node_type = ? AND ref_id = ?", (node_type, ref_id))
        node_id = existing["id"] if existing else make_id("node")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO graph_nodes(id, node_type, ref_id, label, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_type, ref_id) DO UPDATE SET
                label = excluded.label,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            (node_id, node_type, ref_id, label or ref_id, json_dumps(metadata or {}), now),
        )
        if self.kuzu_store:
            self.kuzu_store.upsert_node(node_type, ref_id, label or ref_id, metadata)
        return node_id

    def upsert_edge(
        self,
        source_type: str,
        source_ref_id: str,
        edge_type: str,
        target_type: str,
        target_ref_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        source_node_id = self._get_or_create_node_id(source_type, source_ref_id)
        target_node_id = self._get_or_create_node_id(target_type, target_ref_id)
        edge_id = stable_edge_id(source_node_id, edge_type, target_node_id)
        self.db.execute(
            """
            INSERT INTO graph_edges(id, source_node_id, target_node_id, edge_type, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            (edge_id, source_node_id, target_node_id, edge_type, json_dumps(metadata or {}), utcnow_iso()),
        )
        if self.kuzu_store:
            self.kuzu_store.upsert_edge(source_type, source_ref_id, edge_type, target_type, target_ref_id, metadata)
        return edge_id

    def _get_or_create_node_id(self, node_type: str, ref_id: str) -> str:
        existing = self.db.fetch_one("SELECT id FROM graph_nodes WHERE node_type = ? AND ref_id = ?", (node_type, ref_id))
        if existing:
            return existing["id"]
        return self.upsert_node(node_type, ref_id, ref_id, None)

    def delete_reference(self, ref_id: str) -> None:
        nodes = self.db.fetch_all("SELECT id FROM graph_nodes WHERE ref_id = ?", (ref_id,))
        for node in nodes:
            self.db.execute("DELETE FROM graph_edges WHERE source_node_id = ? OR target_node_id = ?", (node["id"], node["id"]))
        self.db.execute("DELETE FROM graph_nodes WHERE ref_id = ?", (ref_id,))
        if self.kuzu_store:
            self.kuzu_store.delete_reference(ref_id)

    def relations_for_ref(self, ref_id: str, *, limit: int = 12) -> list[dict[str, Any]]:
        rows = self.db.fetch_all(
            """
            SELECT
                ge.edge_type,
                source.ref_id AS source_ref,
                source.node_type AS source_type,
                target.ref_id AS target_ref,
                target.node_type AS target_type,
                target.label AS target_label,
                ge.metadata AS metadata
            FROM graph_edges ge
            JOIN graph_nodes source ON source.id = ge.source_node_id
            JOIN graph_nodes target ON target.id = ge.target_node_id
            WHERE source.ref_id = ? OR target.ref_id = ?
            LIMIT ?
            """,
            (ref_id, ref_id, limit),
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata"] = json_loads(item.get("metadata"), {})
            result.append(item)
        return result

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="graph_backend",
            provider=self.backend_name,
            active_provider=self.active_backend,
            features={
                "relations": True,
                "persistent_graph": True,
                "native_graph_queries": False,
            },
            notes=["sqlite fallback graph"],
        )


class KuzuGraphBackend(SQLiteGraphBackend):
    backend_name = "kuzu"

    def __init__(self, db, kuzu_store=None, config=None):
        if kuzu_store is None and config is not None:
            kuzu_store = KuzuGraphStore(config.kuzu_path)
        super().__init__(db=db, kuzu_store=kuzu_store, config=config)
        self.available = bool(getattr(self.kuzu_store, "available", False))
        self.active_backend = "kuzu" if self.available else "sqlite"

    def relations_for_ref(self, ref_id: str, *, limit: int = 12) -> list[dict[str, Any]]:
        if self.available:
            rows = self.kuzu_store.relations_for_ref(ref_id, limit=limit)
            if rows:
                return rows
        return super().relations_for_ref(ref_id, limit=limit)

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="graph_backend",
            provider=self.backend_name,
            active_provider=self.active_backend,
            features={
                "relations": True,
                "persistent_graph": True,
                "native_graph_queries": bool(self.available),
            },
            notes=["falls back to sqlite graph when Kuzu is unavailable"] if not self.available else [],
        )


class NoopGraphBackend:
    backend_name = "none"
    active_backend = "none"
    available = False

    def upsert_node(
        self,
        node_type: str,
        ref_id: str,
        label: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        return f"{node_type}:{ref_id}"

    def upsert_edge(
        self,
        source_type: str,
        source_ref_id: str,
        edge_type: str,
        target_type: str,
        target_ref_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        return stable_edge_id(f"{source_type}:{source_ref_id}", edge_type, f"{target_type}:{target_ref_id}")

    def delete_reference(self, ref_id: str) -> None:
        return None

    def relations_for_ref(self, ref_id: str, *, limit: int = 12) -> list[dict[str, Any]]:
        return []

    def describe_capabilities(self) -> dict[str, Any]:
        return capability_dict(
            category="graph_backend",
            provider=self.backend_name,
            active_provider=self.active_backend,
            features={
                "relations": False,
                "persistent_graph": False,
                "native_graph_queries": False,
            },
            notes=["graph backend explicitly disabled"],
        )
