from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa

from aimemory.errors import StorageError


PUSHDOWN_FIELDS = {
    "kind": "string",
    "tier": "string",
    "importance": "number",
    "confidence": "number",
    "created_at": "number",
    "updated_at": "number",
    "valid_from": "number",
}


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

    def search(
        self,
        *,
        scope_key: str,
        vector: list[float],
        limit: int,
        kind: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        return self._search_rows(scope_key=scope_key, vector=vector, limit=limit, kind=kind, filters=filters)

    def nearest_neighbors(
        self,
        *,
        scope_key: str,
        vector: list[float],
        limit: int,
        kind: str | None = None,
        exclude_head_id: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        rows = self._search_rows(scope_key=scope_key, vector=vector, limit=limit, kind=kind, filters=filters)
        neighbors: list[dict[str, Any]] = []
        for row in rows:
            if exclude_head_id and row.get("head_id") == exclude_head_id:
                continue
            distance = max(float(row.get("_distance") or 0.0), 0.0)
            neighbors.append(dict(row) | {"distance": distance, "similarity": 1.0 / (1.0 + distance)})
        return neighbors

    def _search_rows(
        self,
        *,
        scope_key: str,
        vector: list[float],
        limit: int,
        kind: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        expressions = [f"scope_key = '{self._quote(scope_key)}'"]
        if kind:
            expressions.append(f"kind = '{self._quote(kind)}'")
        pushed = self._build_pushdown_filters(filters=filters)
        if pushed:
            expressions.extend(pushed)
        builder = self._table.search(vector).where(" AND ".join(expressions), prefilter=True).limit(limit)
        return builder.to_list()

    def _build_pushdown_filters(self, *, filters: dict[str, Any] | None) -> list[str]:
        if not filters:
            return []
        expressions: list[str] = []
        for field, condition in filters.items():
            field_type = PUSHDOWN_FIELDS.get(field)
            if field_type is None:
                continue
            expressions.extend(self._build_filter_expressions(field, field_type, condition))
        return expressions

    def _build_filter_expressions(self, field: str, field_type: str, condition: Any) -> list[str]:
        if not isinstance(condition, dict):
            return [self._eq_expression(field, field_type, condition)]
        expressions: list[str] = []
        for op, expected in condition.items():
            if op == "eq":
                expressions.append(self._eq_expression(field, field_type, expected))
            elif op == "ne":
                expressions.append(f"{field} != {self._format_value(field_type, expected)}")
            elif op == "in" and isinstance(expected, list) and expected:
                formatted = ", ".join(self._format_value(field_type, item) for item in expected)
                expressions.append(f"{field} IN ({formatted})")
            elif op == "gte":
                expressions.append(f"{field} >= {self._format_value(field_type, expected)}")
            elif op == "lte":
                expressions.append(f"{field} <= {self._format_value(field_type, expected)}")
        return expressions

    def _eq_expression(self, field: str, field_type: str, value: Any) -> str:
        return f"{field} = {self._format_value(field_type, value)}"

    def _format_value(self, field_type: str, value: Any) -> str:
        if field_type == "number":
            return str(float(value) if isinstance(value, float) else int(value))
        return f"'{self._quote(str(value))}'"

    @staticmethod
    def _quote(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")
