from __future__ import annotations

from pathlib import Path
from typing import Any

from aimemory.core.utils import json_dumps, json_loads, stable_edge_id, utcnow_iso


class KuzuGraphStore:
    def __init__(self, path: str | Path):
        original_path = Path(path).expanduser().resolve()
        self.path = self._resolve_database_path(original_path)
        self.available = False
        self._db = None
        self._conn = None
        try:
            import kuzu  # type: ignore
        except ImportError:
            return

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = kuzu.Database(str(self.path))
        self._conn = kuzu.Connection(self._db)
        self._initialize()
        self.available = True

    def upsert_node(self, node_type: str, ref_id: str, label: str, metadata: dict[str, Any] | None = None) -> bool:
        if not self.available:
            return False
        self._execute(
            """
            MERGE (n:Entity {node_key: $node_key})
            SET
                n.node_type = $node_type,
                n.ref_id = $ref_id,
                n.label = $label,
                n.metadata = $metadata,
                n.updated_at = $updated_at
            """,
            {
                "node_key": self._node_key(node_type, ref_id),
                "node_type": node_type,
                "ref_id": ref_id,
                "label": label or ref_id,
                "metadata": json_dumps(metadata or {}),
                "updated_at": utcnow_iso(),
            },
        )
        return True

    def upsert_edge(
        self,
        source_type: str,
        source_ref_id: str,
        edge_type: str,
        target_type: str,
        target_ref_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        if not self.available:
            return False
        self.upsert_node(source_type, source_ref_id, source_ref_id, None)
        self.upsert_node(target_type, target_ref_id, target_ref_id, None)
        edge_key = stable_edge_id(self._node_key(source_type, source_ref_id), edge_type, self._node_key(target_type, target_ref_id))
        self._execute(
            """
            MATCH (source:Entity {node_key: $source_key}), (target:Entity {node_key: $target_key})
            MERGE (source)-[r:RELATES {edge_key: $edge_key}]->(target)
            SET
                r.edge_type = $edge_type,
                r.metadata = $metadata,
                r.updated_at = $updated_at
            """,
            {
                "source_key": self._node_key(source_type, source_ref_id),
                "target_key": self._node_key(target_type, target_ref_id),
                "edge_key": edge_key,
                "edge_type": edge_type,
                "metadata": json_dumps(metadata or {}),
                "updated_at": utcnow_iso(),
            },
        )
        return True

    def delete_reference(self, ref_id: str) -> bool:
        if not self.available:
            return False
        self._execute("MATCH (n:Entity) WHERE n.ref_id = $ref_id DETACH DELETE n", {"ref_id": ref_id})
        return True

    def relations_for_ref(self, ref_id: str, *, limit: int = 12) -> list[dict[str, Any]]:
        if not self.available:
            return []
        rows = self._records(
            f"""
            MATCH (source:Entity)-[r:RELATES]->(target:Entity)
            WHERE source.ref_id = $ref_id OR target.ref_id = $ref_id
            RETURN
                r.edge_type AS edge_type,
                source.ref_id AS source_ref,
                source.node_type AS source_type,
                target.ref_id AS target_ref,
                target.node_type AS target_type,
                target.label AS target_label,
                r.metadata AS metadata
            LIMIT {max(1, int(limit))}
            """,
            {"ref_id": ref_id},
        )
        for row in rows:
            row["metadata"] = json_loads(row.get("metadata"), {})
        return rows

    def _initialize(self) -> None:
        self._execute_ddl(
            """
            CREATE NODE TABLE Entity(
                node_key STRING PRIMARY KEY,
                node_type STRING,
                ref_id STRING,
                label STRING,
                metadata STRING,
                updated_at STRING
            )
            """
        )
        self._execute_ddl(
            """
            CREATE REL TABLE RELATES(
                FROM Entity TO Entity,
                edge_key STRING,
                edge_type STRING,
                metadata STRING,
                updated_at STRING
            )
            """
        )

    def _execute(self, query: str, parameters: dict[str, Any] | None = None) -> None:
        assert self._conn is not None
        result = self._conn.execute(query, parameters or {})
        if isinstance(result, list):
            for item in result:
                item.close()
            return
        result.close()

    def _records(self, query: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        assert self._conn is not None
        result = self._conn.execute(query, parameters or {})
        columns = result.get_column_names()
        rows = [dict(zip(columns, row)) for row in result.get_all()]
        result.close()
        return rows

    def _execute_ddl(self, query: str) -> None:
        try:
            self._execute(query)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise

    def _node_key(self, node_type: str, ref_id: str) -> str:
        return f"{node_type}:{ref_id}"

    def _resolve_database_path(self, path: Path) -> Path:
        if path.exists() and path.is_dir():
            return path / "graph.kuzu"
        if path.suffix:
            return path
        return path / "graph.kuzu"
