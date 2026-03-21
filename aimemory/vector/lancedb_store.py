from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa

from aimemory.errors import StorageError


class LanceVectorStore:
    def __init__(self, root_dir: str | Path, vector_dim: int):
        try:
            import lancedb
        except ImportError as exc:
            raise StorageError("lancedb is required for vector indexing") from exc
        self._lancedb = lancedb
        self._root_dir = Path(root_dir)
        self._root_dir.mkdir(parents=True, exist_ok=True)
        self._db = self._lancedb.connect(self._root_dir)
        self._table = self._open_or_create(vector_dim)

    def _schema(self, vector_dim: int) -> pa.Schema:
        return pa.schema(
            [
                pa.field("chunk_id", pa.string()),
                pa.field("head_id", pa.string()),
                pa.field("version_id", pa.string()),
                pa.field("scope_key", pa.string()),
                pa.field("kind", pa.string()),
                pa.field("tier", pa.string()),
                pa.field("importance", pa.float32()),
                pa.field("confidence", pa.float32()),
                pa.field("created_at", pa.int64()),
                pa.field("valid_from", pa.int64()),
                pa.field("valid_to", pa.int64()),
                pa.field("updated_at", pa.int64()),
                pa.field("text", pa.string()),
                pa.field("abstract", pa.string()),
                pa.field("overview", pa.string()),
                pa.field("vector", pa.list_(pa.float32(), vector_dim)),
            ]
        )

    def _open_or_create(self, vector_dim: int):
        schema = self._schema(vector_dim)
        try:
            table = self._db.open_table("memory_vectors")
            if set(table.schema.names) != set(schema.names):
                return self._db.create_table("memory_vectors", schema=schema, mode="overwrite")
            return table
        except Exception:
            return self._db.create_table("memory_vectors", schema=schema, mode="overwrite")

    def upsert(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        for row in rows:
            self._table.delete(f"chunk_id = '{row['chunk_id']}'")
        self._table.add(rows)

    def delete_chunks(self, chunk_ids: list[str]) -> None:
        for chunk_id in chunk_ids:
            self._table.delete(f"chunk_id = '{chunk_id}'")

    def search(self, *, scope_key: str, vector: list[float], limit: int) -> list[dict[str, Any]]:
        builder = self._table.search(vector).where(f"scope_key = '{scope_key}'", prefilter=True).limit(limit)
        return builder.to_list()
