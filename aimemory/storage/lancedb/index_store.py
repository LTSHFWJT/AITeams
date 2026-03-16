from __future__ import annotations

from pathlib import Path
from typing import Any

from aimemory.core.text import hash_embedding
from aimemory.core.utils import json_dumps, json_loads

try:
    import lancedb  # type: ignore
except ImportError as exc:
    raise RuntimeError("AIMemory now requires the `lancedb` package. Install dependencies with `pip install -e .`.") from exc


class LanceIndexStore:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self.path.mkdir(parents=True, exist_ok=True)
        self._db = lancedb.connect(str(self.path))
        self.available = True

    def upsert(self, table_name: str, record_id: str, text: str, payload: dict[str, Any] | None = None) -> bool:
        if not self.available:
            return False
        row = self._serialize_row(table_name, record_id, text, payload or {})
        table = self._open_or_create(table_name, row)
        if table is None:
            return False
        table.delete(f"id = {self._quote(record_id)}")
        table.add([row], mode="append")
        return True

    def delete(self, table_name: str, record_id: str) -> bool:
        if not self.available:
            return False
        table = self._open_table(table_name)
        if table is None:
            return False
        table.delete(f"id = {self._quote(record_id)}")
        return True

    def search(self, table_name: str, query: str, *, limit: int = 5, where: str | None = None) -> list[dict[str, Any]]:
        if not self.available:
            return []
        table = self._open_table(table_name)
        if table is None:
            return []
        builder = table.search(hash_embedding(query))
        if where:
            builder = builder.where(where, prefilter=True)
        rows = builder.limit(limit).to_list()
        results: list[dict[str, Any]] = []
        for row in rows:
            item = {key: value for key, value in row.items() if key != "vector"}
            if "keywords" in item:
                item["keywords"] = json_loads(item.get("keywords"), [])
            if "metadata" in item:
                item["metadata"] = json_loads(item.get("metadata"), {})
            results.append(item)
        return results

    def _open_table(self, table_name: str):
        assert self._db is not None
        table_names = self._list_tables()
        if table_name not in table_names:
            return None
        return self._db.open_table(table_name)

    def _open_or_create(self, table_name: str, row: dict[str, Any]):
        assert self._db is not None
        existing = self._open_table(table_name)
        if existing is not None:
            return existing
        return self._db.create_table(table_name, data=[row], mode="overwrite")

    def _list_tables(self) -> set[str]:
        assert self._db is not None
        raw_tables = self._db.list_tables()
        if hasattr(raw_tables, "tables"):
            raw_tables = getattr(raw_tables, "tables")
        names: set[str] = set()
        for item in raw_tables:
            if isinstance(item, str):
                names.add(item)
            elif isinstance(item, (list, tuple)) and item:
                names.add(str(item[0]))
        return names

    def _serialize_row(self, table_name: str, record_id: str, text: str, payload: dict[str, Any]) -> dict[str, Any]:
        vector = json_loads(payload.get("embedding"), None)
        row: dict[str, Any] = {
            "id": record_id,
            "vector": vector if isinstance(vector, list) and vector else hash_embedding(text),
            "text": text or "",
            "keywords": json_dumps(payload.get("keywords") or []),
            "updated_at": self._string(payload.get("updated_at")),
            "metadata": json_dumps(payload.get("metadata") or {}),
        }
        if table_name == "memory_index":
            row.update(
                {
                    "scope": self._string(payload.get("scope")),
                    "user_id": self._string(payload.get("user_id")),
                    "session_id": self._string(payload.get("session_id")),
                    "memory_type": self._string(payload.get("memory_type")),
                    "score_boost": float(payload.get("score_boost", 0.0) or 0.0),
                }
            )
        elif table_name == "knowledge_chunk_index":
            row.update(
                {
                    "document_id": self._string(payload.get("document_id")),
                    "source_id": self._string(payload.get("source_id")),
                    "title": self._string(payload.get("title")),
                }
            )
        elif table_name == "skill_index":
            row.update(
                {
                    "skill_id": self._string(payload.get("skill_id")),
                    "version": self._string(payload.get("version")),
                    "name": self._string(payload.get("name")),
                    "description": self._string(payload.get("description")),
                }
            )
        elif table_name == "archive_summary_index":
            row.update(
                {
                    "archive_unit_id": self._string(payload.get("archive_unit_id")),
                    "domain": self._string(payload.get("domain")),
                    "user_id": self._string(payload.get("user_id")),
                    "session_id": self._string(payload.get("session_id")),
                }
            )
        return row

    def _string(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    def _quote(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"
