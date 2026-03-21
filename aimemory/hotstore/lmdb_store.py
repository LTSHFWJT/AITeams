from __future__ import annotations

from pathlib import Path
from typing import Any

import lmdb

from aimemory.serialization import json_dumps, json_loads


class LMDBHotStore:
    def __init__(self, root_dir: str | Path):
        path = Path(root_dir)
        path.mkdir(parents=True, exist_ok=True)
        self._env = lmdb.open(
            str(path),
            map_size=128 * 1024 * 1024,
            max_dbs=16,
            subdir=True,
            create=True,
            lock=True,
        )
        self._fingerprint = self._env.open_db(b"fingerprint")
        self._working_set = self._env.open_db(b"working_set")
        self._turn_buffer = self._env.open_db(b"turn_buffer")
        self._embedding_cache = self._env.open_db(b"embedding_cache")
        self._query_cache = self._env.open_db(b"query_cache")
        self._access_delta = self._env.open_db(b"access_delta")
        self._job_mirror = self._env.open_db(b"job_mirror")
        self._lease = self._env.open_db(b"lease")

    def close(self) -> None:
        self._env.close()

    def get_fingerprint(self, key: str) -> str | None:
        with self._env.begin(db=self._fingerprint) as txn:
            value = txn.get(key.encode("utf-8"))
        return value.decode("utf-8") if value else None

    def put_fingerprint(self, key: str, head_id: str) -> None:
        with self._env.begin(write=True, db=self._fingerprint) as txn:
            txn.put(key.encode("utf-8"), head_id.encode("utf-8"))

    def put_fingerprints(self, values: dict[str, str]) -> int:
        if not values:
            return 0
        with self._env.begin(write=True, db=self._fingerprint) as txn:
            for key, head_id in values.items():
                txn.put(key.encode("utf-8"), head_id.encode("utf-8"))
        return len(values)

    def append_working(self, scope_key: str, item: dict[str, Any], limit: int) -> None:
        items = self.working_snapshot(scope_key, limit=limit)
        items.insert(0, item)
        items = items[:limit]
        with self._env.begin(write=True, db=self._working_set) as txn:
            txn.put(scope_key.encode("utf-8"), json_dumps(items).encode("utf-8"))

    def append_working_many(self, scope_key: str, items: list[dict[str, Any]], limit: int) -> int:
        if not items:
            return 0
        current = self.working_snapshot(scope_key, limit=limit)
        merged = list(reversed(items)) + current
        merged = merged[:limit]
        with self._env.begin(write=True, db=self._working_set) as txn:
            txn.put(scope_key.encode("utf-8"), json_dumps(merged).encode("utf-8"))
        return len(items)

    def working_snapshot(self, scope_key: str, limit: int) -> list[dict[str, Any]]:
        with self._env.begin(db=self._working_set) as txn:
            value = txn.get(scope_key.encode("utf-8"))
        items = json_loads(value, [])
        return items[:limit]

    def append_turn(self, scope_key: str, item: dict[str, Any], limit: int) -> None:
        with self._env.begin(db=self._turn_buffer) as txn:
            raw = txn.get(scope_key.encode("utf-8"))
        items = json_loads(raw, [])
        items.append(item)
        items = items[-limit:]
        with self._env.begin(write=True, db=self._turn_buffer) as txn:
            txn.put(scope_key.encode("utf-8"), json_dumps(items).encode("utf-8"))

    def turn_snapshot(self, scope_key: str, limit: int) -> list[dict[str, Any]]:
        with self._env.begin(db=self._turn_buffer) as txn:
            raw = txn.get(scope_key.encode("utf-8"))
        return json_loads(raw, [])[-limit:]

    def get_embedding(self, key: str) -> list[float] | None:
        with self._env.begin(db=self._embedding_cache) as txn:
            raw = txn.get(key.encode("utf-8"))
        return json_loads(raw, None)

    def put_embedding(self, key: str, vector: list[float]) -> None:
        with self._env.begin(write=True, db=self._embedding_cache) as txn:
            txn.put(key.encode("utf-8"), json_dumps(vector).encode("utf-8"))

    def get_query_cache(self, key: str) -> Any:
        with self._env.begin(db=self._query_cache) as txn:
            raw = txn.get(key.encode("utf-8"))
        return json_loads(raw, None)

    def put_query_cache(self, key: str, value: Any) -> None:
        with self._env.begin(write=True, db=self._query_cache) as txn:
            txn.put(key.encode("utf-8"), json_dumps(value).encode("utf-8"))

    def clear_query_cache(self, scope_key: str | None = None) -> int:
        removed = 0
        prefix = f"{scope_key}:".encode("utf-8") if scope_key else None
        with self._env.begin(write=True, db=self._query_cache) as txn:
            cursor = txn.cursor()
            keys: list[bytes] = []
            for key, _ in cursor:
                if prefix is None or key.startswith(prefix):
                    keys.append(bytes(key))
            for key in keys:
                txn.delete(key)
                removed += 1
        return removed

    def bump_access(self, head_id: str, delta: int = 1) -> int:
        key = head_id.encode("utf-8")
        with self._env.begin(write=True, db=self._access_delta) as txn:
            raw = txn.get(key)
            current = int(raw.decode("utf-8")) if raw else 0
            current += delta
            txn.put(key, str(current).encode("utf-8"))
            return current

    def pending_access_total(self) -> int:
        total = 0
        with self._env.begin(db=self._access_delta) as txn:
            cursor = txn.cursor()
            for _, value in cursor:
                total += int(value.decode("utf-8"))
        return total

    def drain_access(self) -> dict[str, int]:
        updates: dict[str, int] = {}
        with self._env.begin(write=True, db=self._access_delta) as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                updates[key.decode("utf-8")] = int(value.decode("utf-8"))
            for head_id in list(updates):
                txn.delete(head_id.encode("utf-8"))
        return updates

    def mirror_job(self, job_id: str, payload: dict[str, Any]) -> None:
        with self._env.begin(write=True, db=self._job_mirror) as txn:
            txn.put(job_id.encode("utf-8"), json_dumps(payload).encode("utf-8"))

    def mirror_jobs(self, jobs: list[dict[str, Any]]) -> int:
        if not jobs:
            return 0
        with self._env.begin(write=True, db=self._job_mirror) as txn:
            for job in jobs:
                txn.put(job["job_id"].encode("utf-8"), json_dumps(job).encode("utf-8"))
        return len(jobs)

    def drop_job(self, job_id: str) -> None:
        with self._env.begin(write=True, db=self._job_mirror) as txn:
            txn.delete(job_id.encode("utf-8"))

    def replace_job_mirror(self, jobs: list[dict[str, Any]]) -> int:
        with self._env.begin(write=True, db=self._job_mirror) as txn:
            cursor = txn.cursor()
            stale = [bytes(key) for key, _ in cursor]
            for key in stale:
                txn.delete(key)
            for job in jobs:
                txn.put(job["job_id"].encode("utf-8"), json_dumps(job).encode("utf-8"))
        return len(jobs)

    def list_job_mirror(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        with self._env.begin(db=self._job_mirror) as txn:
            cursor = txn.cursor()
            for _, value in cursor:
                items.append(json_loads(value, {}))
        return items

    def acquire_lease(self, worker_name: str, *, now_ms: int, ttl_ms: int) -> bool:
        key = worker_name.encode("utf-8")
        with self._env.begin(write=True, db=self._lease) as txn:
            raw = txn.get(key)
            current_until = int(raw.decode("utf-8")) if raw else 0
            if current_until > now_ms:
                return False
            txn.put(key, str(now_ms + ttl_ms).encode("utf-8"))
            return True
