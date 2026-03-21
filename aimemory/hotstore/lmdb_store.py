from __future__ import annotations

from pathlib import Path
import time
import threading
from typing import Any

import lmdb

from aimemory.serialization import json_dumps, json_loads


class LMDBHotStore:
    _PENDING_ACCESS_SINCE_KEY = b"pending_since"

    def __init__(self, root_dir: str | Path):
        path = Path(root_dir)
        path.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
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
        self._access_meta = self._env.open_db(b"access_meta")
        self._job_mirror = self._env.open_db(b"job_mirror")
        self._lease = self._env.open_db(b"lease")

    def close(self) -> None:
        with self._lock:
            self._env.close()

    def get_fingerprint(self, key: str) -> str | None:
        with self._lock:
            with self._env.begin(db=self._fingerprint) as txn:
                value = txn.get(key.encode("utf-8"))
        return value.decode("utf-8") if value else None

    def put_fingerprint(self, key: str, head_id: str) -> None:
        with self._lock:
            with self._env.begin(write=True, db=self._fingerprint) as txn:
                txn.put(key.encode("utf-8"), head_id.encode("utf-8"))

    def put_fingerprints(self, values: dict[str, str]) -> int:
        if not values:
            return 0
        with self._lock:
            with self._env.begin(write=True, db=self._fingerprint) as txn:
                for key, head_id in values.items():
                    txn.put(key.encode("utf-8"), head_id.encode("utf-8"))
        return len(values)

    def append_working(self, scope_key: str, item: dict[str, Any], limit: int) -> None:
        items = self.working_snapshot(scope_key, limit=limit)
        items.insert(0, item)
        items = items[:limit]
        with self._lock:
            with self._env.begin(write=True, db=self._working_set) as txn:
                txn.put(scope_key.encode("utf-8"), json_dumps(items).encode("utf-8"))

    def append_working_many(self, scope_key: str, items: list[dict[str, Any]], limit: int) -> int:
        if not items:
            return 0
        current = self.working_snapshot(scope_key, limit=limit)
        merged = list(reversed(items)) + current
        merged = merged[:limit]
        with self._lock:
            with self._env.begin(write=True, db=self._working_set) as txn:
                txn.put(scope_key.encode("utf-8"), json_dumps(merged).encode("utf-8"))
        return len(items)

    def working_snapshot(self, scope_key: str, limit: int) -> list[dict[str, Any]]:
        with self._lock:
            with self._env.begin(db=self._working_set) as txn:
                value = txn.get(scope_key.encode("utf-8"))
        items = json_loads(value, [])
        return items[:limit]

    def append_turn(self, scope_key: str, item: dict[str, Any], limit: int) -> None:
        with self._lock:
            with self._env.begin(db=self._turn_buffer) as txn:
                raw = txn.get(scope_key.encode("utf-8"))
        items = json_loads(raw, [])
        items.append(item)
        items = items[-limit:]
        with self._lock:
            with self._env.begin(write=True, db=self._turn_buffer) as txn:
                txn.put(scope_key.encode("utf-8"), json_dumps(items).encode("utf-8"))

    def turn_snapshot(self, scope_key: str, limit: int) -> list[dict[str, Any]]:
        with self._lock:
            with self._env.begin(db=self._turn_buffer) as txn:
                raw = txn.get(scope_key.encode("utf-8"))
        return json_loads(raw, [])[-limit:]

    def get_embedding(self, key: str) -> list[float] | None:
        with self._lock:
            with self._env.begin(db=self._embedding_cache) as txn:
                raw = txn.get(key.encode("utf-8"))
        return json_loads(raw, None)

    def put_embedding(self, key: str, vector: list[float]) -> None:
        with self._lock:
            with self._env.begin(write=True, db=self._embedding_cache) as txn:
                txn.put(key.encode("utf-8"), json_dumps(vector).encode("utf-8"))

    def put_embeddings(self, values: dict[str, list[float]]) -> int:
        if not values:
            return 0
        with self._lock:
            with self._env.begin(write=True, db=self._embedding_cache) as txn:
                for key, vector in values.items():
                    txn.put(key.encode("utf-8"), json_dumps(vector).encode("utf-8"))
        return len(values)

    def get_query_cache(self, key: str) -> Any:
        with self._lock:
            with self._env.begin(db=self._query_cache) as txn:
                raw = txn.get(key.encode("utf-8"))
        return json_loads(raw, None)

    def put_query_cache(self, key: str, value: Any) -> None:
        with self._lock:
            with self._env.begin(write=True, db=self._query_cache) as txn:
                txn.put(key.encode("utf-8"), json_dumps(value).encode("utf-8"))

    def clear_query_cache(self, scope_key: str | None = None) -> int:
        removed = 0
        prefix = f"{scope_key}:".encode("utf-8") if scope_key else None
        with self._lock:
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

    def bump_access(self, head_id: str, delta: int = 1, *, recorded_at_ms: int | None = None) -> int:
        key = head_id.encode("utf-8")
        with self._lock:
            with self._env.begin(write=True) as txn:
                raw = txn.get(key, db=self._access_delta)
                current = int(raw.decode("utf-8")) if raw else 0
                current += delta
                txn.put(key, str(current).encode("utf-8"), db=self._access_delta)
                pending_since = txn.get(self._PENDING_ACCESS_SINCE_KEY, db=self._access_meta)
                if pending_since is None:
                    txn.put(
                        self._PENDING_ACCESS_SINCE_KEY,
                        str(recorded_at_ms or int(time.time() * 1000)).encode("utf-8"),
                        db=self._access_meta,
                    )
                return current

    def pending_access_total(self) -> int:
        total = 0
        with self._lock:
            with self._env.begin(db=self._access_delta) as txn:
                cursor = txn.cursor()
                for _, value in cursor:
                    total += int(value.decode("utf-8"))
        return total

    def pending_access_since(self) -> int | None:
        with self._lock:
            with self._env.begin(db=self._access_meta) as txn:
                raw = txn.get(self._PENDING_ACCESS_SINCE_KEY)
        if raw is None:
            return None
        return int(raw.decode("utf-8"))

    def drain_access(self) -> dict[str, int]:
        updates: dict[str, int] = {}
        with self._lock:
            with self._env.begin(write=True) as txn:
                cursor = txn.cursor(db=self._access_delta)
                for key, value in cursor:
                    updates[key.decode("utf-8")] = int(value.decode("utf-8"))
                for head_id in list(updates):
                    txn.delete(head_id.encode("utf-8"), db=self._access_delta)
                txn.delete(self._PENDING_ACCESS_SINCE_KEY, db=self._access_meta)
        return updates

    def mirror_job(self, job_id: str, payload: dict[str, Any]) -> None:
        with self._lock:
            with self._env.begin(write=True, db=self._job_mirror) as txn:
                txn.put(job_id.encode("utf-8"), json_dumps(payload).encode("utf-8"))

    def mirror_jobs(self, jobs: list[dict[str, Any]]) -> int:
        if not jobs:
            return 0
        with self._lock:
            with self._env.begin(write=True, db=self._job_mirror) as txn:
                for job in jobs:
                    txn.put(job["job_id"].encode("utf-8"), json_dumps(job).encode("utf-8"))
        return len(jobs)

    def drop_job(self, job_id: str) -> None:
        with self._lock:
            with self._env.begin(write=True, db=self._job_mirror) as txn:
                txn.delete(job_id.encode("utf-8"))

    def replace_job_mirror(self, jobs: list[dict[str, Any]]) -> int:
        with self._lock:
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
        with self._lock:
            with self._env.begin(db=self._job_mirror) as txn:
                cursor = txn.cursor()
                for _, value in cursor:
                    items.append(json_loads(value, {}))
        return items

    def acquire_lease(self, worker_name: str, *, owner_id: str, now_ms: int, ttl_ms: int) -> bool:
        key = worker_name.encode("utf-8")
        with self._lock:
            with self._env.begin(write=True, db=self._lease) as txn:
                owner, current_until = self._decode_lease_payload(txn.get(key))
                if current_until > now_ms and owner not in (None, owner_id):
                    return False
                txn.put(key, self._encode_lease_payload(owner_id, now_ms + ttl_ms))
                return True

    def renew_lease(self, worker_name: str, *, owner_id: str, now_ms: int, ttl_ms: int) -> bool:
        key = worker_name.encode("utf-8")
        with self._lock:
            with self._env.begin(write=True, db=self._lease) as txn:
                owner, current_until = self._decode_lease_payload(txn.get(key))
                if owner not in (None, owner_id) and current_until > now_ms:
                    return False
                txn.put(key, self._encode_lease_payload(owner_id, now_ms + ttl_ms))
                return True

    def release_lease(self, worker_name: str, *, owner_id: str) -> bool:
        key = worker_name.encode("utf-8")
        with self._lock:
            with self._env.begin(write=True, db=self._lease) as txn:
                owner, _ = self._decode_lease_payload(txn.get(key))
                if owner != owner_id:
                    return False
                txn.delete(key)
                return True

    def get_lease(self, worker_name: str) -> dict[str, Any] | None:
        with self._lock:
            with self._env.begin(db=self._lease) as txn:
                raw = txn.get(worker_name.encode("utf-8"))
        owner_id, expires_at = self._decode_lease_payload(raw)
        if owner_id is None and expires_at <= 0:
            return None
        return {
            "worker_name": worker_name,
            "owner_id": owner_id,
            "expires_at": expires_at,
        }

    @staticmethod
    def _encode_lease_payload(owner_id: str, expires_at: int) -> bytes:
        return json_dumps({"owner_id": owner_id, "expires_at": expires_at}).encode("utf-8")

    @staticmethod
    def _decode_lease_payload(raw: bytes | None) -> tuple[str | None, int]:
        if raw is None:
            return None, 0
        payload = json_loads(raw, None)
        if isinstance(payload, dict):
            owner_id = payload.get("owner_id")
            expires_at = int(payload.get("expires_at") or 0)
            return (str(owner_id) if owner_id is not None else None, expires_at)
        try:
            return None, int(raw.decode("utf-8"))
        except ValueError:
            return None, 0
