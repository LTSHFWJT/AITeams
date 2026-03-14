from __future__ import annotations

from typing import Any, Iterable

from aimemory.core.facade import AIMemory
from aimemory.core.utils import json_dumps, json_loads, make_id, utcnow_iso
from aimemory.domains.object.models import StoredObject


class ServiceBase:
    def __init__(self, db, projection, config, object_store=None):
        self.db = db
        self.projection = projection
        self.config = config
        self.object_store = object_store
        self._kernel_instance: AIMemory | None = None

    def _kernel(self) -> AIMemory:
        if self._kernel_instance is None:
            self._kernel_instance = AIMemory(self.config)
        return self._kernel_instance

    def _deserialize_row(
        self,
        row: dict[str, Any] | None,
        json_fields: Iterable[str] = (
            "metadata",
            "active_window",
            "payload",
            "snapshot",
            "arguments",
            "result",
            "config",
            "input_payload",
            "expected_output",
            "highlights",
            "constraints",
            "resolved_items",
            "unresolved_items",
            "next_actions",
            "salience_vector",
            "capability_tags",
            "tool_affinity",
        ),
    ) -> dict[str, Any] | None:
        if row is None:
            return None
        item = dict(row)
        for field in json_fields:
            if field in item:
                fallback: Any = [] if field.endswith("s") or field in {"highlights", "constraints", "resolved_items", "unresolved_items", "next_actions", "salience_vector", "capability_tags", "tool_affinity"} else {}
                item[field] = json_loads(item.get(field), fallback)
        return item

    def _deserialize_rows(
        self,
        rows: list[dict[str, Any]],
        json_fields: Iterable[str] = (
            "metadata",
            "active_window",
            "payload",
            "snapshot",
            "arguments",
            "result",
            "config",
            "input_payload",
            "expected_output",
            "highlights",
            "constraints",
            "resolved_items",
            "unresolved_items",
            "next_actions",
            "salience_vector",
            "capability_tags",
            "tool_affinity",
        ),
    ) -> list[dict[str, Any]]:
        return [self._deserialize_row(row, json_fields) for row in rows if row is not None]

    def _persist_object(self, stored: StoredObject, mime_type: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        now = utcnow_iso()
        existing = self.db.fetch_one("SELECT * FROM objects WHERE object_key = ?", (stored.object_key,))
        object_id = existing["id"] if existing else make_id("obj")
        payload = json_dumps(metadata or {})
        self.db.execute(
            """
            INSERT INTO objects(id, object_key, object_type, mime_type, size_bytes, checksum, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(object_key) DO UPDATE SET
                object_type = excluded.object_type,
                mime_type = excluded.mime_type,
                size_bytes = excluded.size_bytes,
                checksum = excluded.checksum,
                metadata = excluded.metadata
            """,
            (
                object_id,
                stored.object_key,
                stored.object_type,
                mime_type,
                stored.size_bytes,
                stored.checksum,
                payload,
                now,
            ),
        )
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM objects WHERE id = ?", (object_id,)))

    def close(self) -> None:
        if self._kernel_instance is not None:
            try:
                self._kernel_instance.close()
            except Exception:
                pass
            self._kernel_instance = None
