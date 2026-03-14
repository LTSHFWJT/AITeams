from __future__ import annotations

from pathlib import Path
from typing import Any

from aimemory.core.text import hash_embedding
from aimemory.core.utils import json_dumps, json_loads


class FaissIndexStore:
    def __init__(self, path: str | Path, *, dims: int = 384):
        self.path = Path(path).expanduser().resolve()
        self.dims = dims
        self.available = False
        self._faiss = None
        self._np = None
        self._tables: dict[str, dict[str, Any]] = {}
        try:
            import faiss  # type: ignore
            import numpy as np  # type: ignore
        except ImportError:
            return
        self._faiss = faiss
        self._np = np
        self.path.mkdir(parents=True, exist_ok=True)
        self.available = True

    def upsert(self, table_name: str, record_id: str, text: str, payload: dict[str, Any] | None = None) -> bool:
        if not self.available:
            return False
        state = self._load_state(table_name)
        row = self._serialize_row(record_id, text, payload or {})
        faiss_id = state["id_map"].get(record_id)
        if faiss_id is None:
            faiss_id = state["next_id"]
            state["next_id"] += 1
            state["id_map"][record_id] = faiss_id
        else:
            state["index"].remove_ids(self._np.array([faiss_id], dtype="int64"))
        vector = self._np.array([row["vector"]], dtype="float32")
        state["index"].add_with_ids(vector, self._np.array([faiss_id], dtype="int64"))
        state["rows"][record_id] = row
        self._persist_state(table_name, state)
        return True

    def delete(self, table_name: str, record_id: str) -> bool:
        if not self.available:
            return False
        state = self._load_state(table_name)
        faiss_id = state["id_map"].pop(record_id, None)
        state["rows"].pop(record_id, None)
        if faiss_id is not None:
            state["index"].remove_ids(self._np.array([faiss_id], dtype="int64"))
            self._persist_state(table_name, state)
            return True
        return False

    def search(self, table_name: str, query: str, *, limit: int = 5) -> list[dict[str, Any]]:
        if not self.available:
            return []
        state = self._load_state(table_name)
        if not state["rows"]:
            return []
        query_vector = self._np.array([hash_embedding(query, dims=self.dims)], dtype="float32")
        scores, ids = state["index"].search(query_vector, max(1, limit))
        reverse_id_map = {value: key for key, value in state["id_map"].items()}
        results: list[dict[str, Any]] = []
        for score, faiss_id in zip(scores[0].tolist(), ids[0].tolist()):
            if faiss_id < 0:
                continue
            record_id = reverse_id_map.get(int(faiss_id))
            if record_id is None:
                continue
            row = dict(state["rows"].get(record_id, {}))
            row["_distance"] = round(max(0.0, 1.0 - float(score)), 6)
            row.pop("vector", None)
            if "keywords" in row:
                row["keywords"] = json_loads(row.get("keywords"), [])
            if "metadata" in row:
                row["metadata"] = json_loads(row.get("metadata"), {})
            results.append(row)
        return results

    def _load_state(self, table_name: str) -> dict[str, Any]:
        if table_name in self._tables:
            return self._tables[table_name]
        assert self._faiss is not None
        metadata_path = self.path / f"{table_name}.json"
        index_path = self.path / f"{table_name}.faiss"
        if metadata_path.exists() and index_path.exists():
            payload = json_loads(metadata_path.read_text(encoding="utf-8"), {}) or {}
            index = self._faiss.read_index(str(index_path))
        else:
            index = self._faiss.IndexIDMap2(self._faiss.IndexFlatIP(self.dims))
            payload = {"rows": {}, "id_map": {}, "next_id": 1}
        state = {
            "index": index,
            "rows": dict(payload.get("rows") or {}),
            "id_map": {key: int(value) for key, value in dict(payload.get("id_map") or {}).items()},
            "next_id": int(payload.get("next_id") or 1),
        }
        self._tables[table_name] = state
        return state

    def _persist_state(self, table_name: str, state: dict[str, Any]) -> None:
        assert self._faiss is not None
        metadata_path = self.path / f"{table_name}.json"
        index_path = self.path / f"{table_name}.faiss"
        metadata_path.write_text(
            json_dumps(
                {
                    "rows": state["rows"],
                    "id_map": state["id_map"],
                    "next_id": state["next_id"],
                }
            ),
            encoding="utf-8",
        )
        self._faiss.write_index(state["index"], str(index_path))

    def _serialize_row(self, record_id: str, text: str, payload: dict[str, Any]) -> dict[str, Any]:
        vector = json_loads(payload.get("embedding"), None)
        return {
            "id": record_id,
            "record_id": record_id,
            "text": text or "",
            "vector": vector if isinstance(vector, list) and vector else hash_embedding(text, dims=self.dims),
            "keywords": json_dumps(payload.get("keywords") or []),
            "metadata": json_dumps(payload.get("metadata") or {}),
            "updated_at": str(payload.get("updated_at") or ""),
        }
