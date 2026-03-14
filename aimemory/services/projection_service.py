from __future__ import annotations

from typing import Any

from aimemory.core.text import extract_keywords
from aimemory.core.utils import json_dumps, json_loads, make_id, utcnow_iso


class ProjectionService:
    def __init__(self, db, config, index_backend, graph_backend):
        self.db = db
        self.config = config
        self.index_backend = index_backend
        self.graph_backend = graph_backend

    def enqueue(
        self,
        topic: str,
        entity_type: str,
        entity_id: str,
        action: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event_id = make_id("outbox")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO outbox_events(
                id, topic, entity_type, entity_id, action, payload, status, attempts, available_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)
            """,
            (event_id, topic, entity_type, entity_id, action, json_dumps(payload or {}), now, now),
        )
        return self.db.fetch_one("SELECT * FROM outbox_events WHERE id = ?", (event_id,))

    def project_pending(self, limit: int | None = None) -> dict[str, Any]:
        page_size = limit or self.config.projection_batch_size
        now = utcnow_iso()
        events = self.db.fetch_all(
            """
            SELECT * FROM outbox_events
            WHERE status = 'pending' AND available_at <= ?
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (now, page_size),
        )
        processed: list[str] = []
        failed: list[str] = []
        for event in events:
            try:
                payload = json_loads(event.get("payload"), {})
                handler = getattr(self, f"_handle_{event['entity_type']}", None)
                if handler is not None:
                    handler(event["action"], payload)
                self.db.execute(
                    """
                    UPDATE outbox_events
                    SET status = 'processed', processed_at = ?, attempts = attempts + 1, last_error = NULL
                    WHERE id = ?
                    """,
                    (utcnow_iso(), event["id"]),
                )
                processed.append(event["id"])
            except Exception as exc:
                self.db.execute(
                    """
                    UPDATE outbox_events
                    SET attempts = attempts + 1, last_error = ?
                    WHERE id = ?
                    """,
                    (str(exc), event["id"]),
                )
                failed.append(event["id"])
        return {"processed": len(processed), "failed": len(failed), "event_ids": processed}

    def _handle_memory(self, action: str, payload: dict[str, Any]) -> None:
        record_id = payload["record_id"]
        if action == "delete":
            self.index_backend.delete_memory(record_id)
            self.graph_backend.delete_reference(record_id)
            return

        keywords = payload.get("keywords") or extract_keywords(payload.get("text"))
        payload = dict(payload)
        payload["keywords"] = keywords
        self.index_backend.upsert_memory(payload)
        self._project_memory_graph(payload, keywords)

    def _handle_knowledge_chunk(self, action: str, payload: dict[str, Any]) -> None:
        record_id = payload["record_id"]
        if action == "delete":
            self.index_backend.delete_knowledge_chunk(record_id)
            self.graph_backend.delete_reference(record_id)
            return

        keywords = payload.get("keywords") or extract_keywords(payload.get("text"))
        payload = dict(payload)
        payload["keywords"] = keywords
        self.index_backend.upsert_knowledge_chunk(payload)
        self._project_document_graph(payload, keywords)

    def _handle_skill_version(self, action: str, payload: dict[str, Any]) -> None:
        record_id = payload["record_id"]
        if action == "delete":
            self.index_backend.delete_skill(record_id)
            self.graph_backend.delete_reference(record_id)
            return

        keywords = payload.get("keywords") or extract_keywords(payload.get("text"))
        payload = dict(payload)
        payload["keywords"] = keywords
        self.index_backend.upsert_skill(payload)
        self._project_skill_graph(payload)

    def _handle_archive_summary(self, action: str, payload: dict[str, Any]) -> None:
        record_id = payload["record_id"]
        if action == "delete":
            self.index_backend.delete_archive_summary(record_id)
            return

        keywords = payload.get("keywords") or extract_keywords(payload.get("text"))
        payload = dict(payload)
        payload["keywords"] = keywords
        self.index_backend.upsert_archive_summary(payload)

    def _first_value(self, payload: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return value
        return None

    def _scope_metadata(self, payload: dict[str, Any]) -> dict[str, Any]:
        owner_agent_id = self._first_value(payload, "owner_agent_id", "agent_id")
        subject_type = self._first_value(payload, "subject_type", "source_subject_type")
        subject_id = self._first_value(payload, "subject_id", "source_subject_id")
        interaction_type = payload.get("interaction_type")
        metadata: dict[str, Any] = {}
        if owner_agent_id is not None:
            metadata["owner_agent_id"] = owner_agent_id
        if subject_type is not None:
            metadata["subject_type"] = subject_type
        if subject_id is not None:
            metadata["subject_id"] = subject_id
        if interaction_type is not None:
            metadata["interaction_type"] = interaction_type
        return metadata

    def _subject_node_type(self, subject_type: str | None) -> str:
        normalized = str(subject_type or "subject").strip().lower()
        mapping = {
            "human": "Human",
            "user": "Human",
            "person": "Human",
            "agent": "Agent",
            "assistant": "Agent",
            "system": "System",
            "group": "Group",
            "team": "Group",
        }
        return mapping.get(normalized, normalized.title() or "Subject")

    def _scope_matches(
        self,
        payload: dict[str, Any],
        row: dict[str, Any],
        *,
        row_subject_type_keys: tuple[str, ...] = ("subject_type", "source_subject_type"),
        row_subject_id_keys: tuple[str, ...] = ("subject_id", "source_subject_id"),
    ) -> bool:
        payload_owner = self._first_value(payload, "owner_agent_id", "agent_id")
        row_owner = self._first_value(row, "owner_agent_id", "agent_id")
        if payload_owner and row_owner and payload_owner != row_owner:
            return False

        payload_subject_type = self._first_value(payload, "subject_type", "source_subject_type")
        row_subject_type = self._first_value(row, *row_subject_type_keys)
        if payload_subject_type and row_subject_type and payload_subject_type != row_subject_type:
            return False

        payload_subject_id = self._first_value(payload, "subject_id", "source_subject_id")
        row_subject_id = self._first_value(row, *row_subject_id_keys)
        if payload_subject_id and row_subject_id and payload_subject_id != row_subject_id:
            return False

        return True

    def _upsert_owner_scope(self, payload: dict[str, Any], target_type: str, target_ref_id: str) -> None:
        owner_agent_id = self._first_value(payload, "owner_agent_id", "agent_id")
        if not owner_agent_id:
            return
        scope_metadata = self._scope_metadata(payload)
        self.graph_backend.upsert_node("Agent", owner_agent_id, owner_agent_id, {"role": "owner", **scope_metadata})
        self.graph_backend.upsert_edge("Agent", owner_agent_id, "OWNS", target_type, target_ref_id, scope_metadata)

    def _upsert_subject_scope(self, payload: dict[str, Any], source_type: str, source_ref_id: str) -> None:
        subject_id = self._first_value(payload, "subject_id", "source_subject_id")
        if not subject_id:
            return
        subject_type = self._first_value(payload, "subject_type", "source_subject_type")
        node_type = self._subject_node_type(subject_type)
        scope_metadata = self._scope_metadata(payload)
        self.graph_backend.upsert_node(node_type, subject_id, subject_id, {"subject_type": subject_type, **scope_metadata})
        self.graph_backend.upsert_edge(source_type, source_ref_id, "ABOUT", node_type, subject_id, scope_metadata)

    def _project_memory_graph(self, payload: dict[str, Any], keywords: list[str]) -> None:
        scope_metadata = self._scope_metadata(payload)
        self.graph_backend.upsert_node(
            "Memory",
            payload["record_id"],
            payload.get("text", "")[:80],
            {**dict(payload.get("metadata") or {}), **scope_metadata},
        )
        self._upsert_owner_scope(payload, "Memory", payload["record_id"])
        self._upsert_subject_scope(payload, "Memory", payload["record_id"])
        user_id = payload.get("user_id")
        session_id = payload.get("session_id")
        if user_id:
            self.graph_backend.upsert_node("User", user_id, user_id, {"user_id": user_id})
            self.graph_backend.upsert_edge("User", user_id, "HAS_MEMORY", "Memory", payload["record_id"], scope_metadata)
        if session_id:
            self.graph_backend.upsert_node("Session", session_id, session_id, scope_metadata)
            self.graph_backend.upsert_edge("Session", session_id, "CONTAINS", "Memory", payload["record_id"], scope_metadata)
        for keyword in keywords[:3]:
            self.graph_backend.upsert_node("Entity", keyword, keyword, {"source": "memory"})
            self.graph_backend.upsert_edge("Memory", payload["record_id"], "MENTIONS", "Entity", keyword)
        self._link_memory_to_knowledge(payload, payload["record_id"], keywords)
        self._link_memory_to_skills(payload, payload["record_id"], keywords)

    def _project_document_graph(self, payload: dict[str, Any], keywords: list[str]) -> None:
        scope_metadata = self._scope_metadata(payload)
        self.graph_backend.upsert_node(
            "Document",
            payload["document_id"],
            payload.get("title") or payload["document_id"],
            {**dict(payload.get("metadata") or {}), **scope_metadata},
        )
        self.graph_backend.upsert_node(
            "Chunk",
            payload["record_id"],
            payload.get("text", "")[:80],
            {**dict(payload.get("metadata") or {}), **scope_metadata},
        )
        self.graph_backend.upsert_edge("Document", payload["document_id"], "HAS_CHUNK", "Chunk", payload["record_id"])
        self._upsert_owner_scope(payload, "Document", payload["document_id"])
        self._upsert_subject_scope(payload, "Document", payload["document_id"])
        for keyword in keywords[:3]:
            self.graph_backend.upsert_node("Entity", keyword, keyword, {"source": "knowledge"})
            self.graph_backend.upsert_edge("Chunk", payload["record_id"], "ABOUT", "Entity", keyword)
        self._link_chunk_to_memories(payload, payload["record_id"], keywords)

    def _project_skill_graph(self, payload: dict[str, Any]) -> None:
        scope_metadata = self._scope_metadata(payload)
        self.graph_backend.upsert_node(
            "Skill",
            payload["skill_id"],
            payload["name"],
            {**dict(payload.get("metadata") or {}), **scope_metadata},
        )
        self._upsert_owner_scope(payload, "Skill", payload["skill_id"])
        self._upsert_subject_scope(payload, "Skill", payload["skill_id"])
        for tool_name in payload.get("tools", [])[:8]:
            self.graph_backend.upsert_node("Tool", tool_name, tool_name, None)
            self.graph_backend.upsert_edge("Skill", payload["skill_id"], "CALLS_TOOL", "Tool", tool_name)
        for topic in payload.get("topics", [])[:8]:
            self.graph_backend.upsert_node("Topic", topic, topic, None)
            self.graph_backend.upsert_edge("Skill", payload["skill_id"], "ABOUT", "Topic", topic)
        self._link_skill_to_memories(payload, payload["skill_id"], payload.get("topics", []))

    def _link_memory_to_knowledge(self, payload: dict[str, Any], memory_ref_id: str, keywords: list[str]) -> None:
        if not keywords:
            return
        rows = self.index_backend.list_knowledge_chunks(limit=40)
        for row in rows:
            if not self._scope_matches(
                payload,
                row,
                row_subject_type_keys=("source_subject_type",),
                row_subject_id_keys=("source_subject_id",),
            ):
                continue
            row_keywords = set(json_loads(row.get("keywords"), []))
            overlap = row_keywords & set(keywords)
            if not overlap:
                continue
            self.graph_backend.upsert_node("Chunk", row["record_id"], row["record_id"], {"keywords": list(row_keywords)})
            self.graph_backend.upsert_node("Document", row["document_id"], row["document_id"], None)
            link_metadata = {"keywords": list(overlap), **self._scope_metadata(payload)}
            self.graph_backend.upsert_edge("Memory", memory_ref_id, "RELATED_TO", "Chunk", row["record_id"], link_metadata)
            self.graph_backend.upsert_edge("Memory", memory_ref_id, "RELATED_TO", "Document", row["document_id"], link_metadata)

    def _link_chunk_to_memories(self, payload: dict[str, Any], chunk_ref_id: str, keywords: list[str]) -> None:
        if not keywords:
            return
        rows = self.index_backend.list_memory_candidates(scope="all", limit=40)
        for row in rows:
            if not self._scope_matches(payload, row, row_subject_type_keys=("subject_type",), row_subject_id_keys=("subject_id",)):
                continue
            row_keywords = set(json_loads(row.get("keywords"), []))
            overlap = row_keywords & set(keywords)
            if not overlap:
                continue
            self.graph_backend.upsert_node("Memory", row["record_id"], row["record_id"], {"keywords": list(row_keywords)})
            self.graph_backend.upsert_edge(
                "Memory",
                row["record_id"],
                "RELATED_TO",
                "Chunk",
                chunk_ref_id,
                {"keywords": list(overlap), **self._scope_metadata(payload)},
            )

    def _link_memory_to_skills(self, payload: dict[str, Any], memory_ref_id: str, keywords: list[str]) -> None:
        if not keywords:
            return
        rows = self.index_backend.list_skill_records(limit=30)
        for row in rows:
            if not self._scope_matches(
                payload,
                row,
                row_subject_type_keys=("source_subject_type",),
                row_subject_id_keys=("source_subject_id",),
            ):
                continue
            row_keywords = set(json_loads(row.get("keywords"), []))
            overlap = row_keywords & set(keywords)
            if not overlap:
                continue
            self.graph_backend.upsert_node("Skill", row["skill_id"], row["name"], {"keywords": list(row_keywords)})
            self.graph_backend.upsert_edge(
                "Memory",
                memory_ref_id,
                "RELATED_TO",
                "Skill",
                row["skill_id"],
                {"keywords": list(overlap), **self._scope_metadata(payload)},
            )

    def _link_skill_to_memories(self, payload: dict[str, Any], skill_ref_id: str, topics: list[str]) -> None:
        if not topics:
            return
        rows = self.index_backend.list_memory_candidates(scope="all", limit=30)
        for row in rows:
            if not self._scope_matches(payload, row, row_subject_type_keys=("subject_type",), row_subject_id_keys=("subject_id",)):
                continue
            row_keywords = set(json_loads(row.get("keywords"), []))
            overlap = row_keywords & set(topics)
            if not overlap:
                continue
            self.graph_backend.upsert_node("Memory", row["record_id"], row["record_id"], {"keywords": list(row_keywords)})
            self.graph_backend.upsert_edge(
                "Memory",
                row["record_id"],
                "RELATED_TO",
                "Skill",
                skill_ref_id,
                {"keywords": list(overlap), **self._scope_metadata(payload)},
            )
