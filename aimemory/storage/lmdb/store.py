from __future__ import annotations

from pathlib import Path

from aimemory.core.utils import ensure_dir, json_dumps, json_loads, make_uuid7

try:
    import lmdb
except ImportError as exc:  # pragma: no cover - exercised only when dependency is absent
    raise RuntimeError("AIMemory now requires the `lmdb` package. Install dependencies with `pip install -e .`.") from exc


class LMDBMemoryStore:
    _DATABASES = {
        "short_term": b"short_term",
        "long_term": b"long_term",
        "archive": b"archive",
    }

    def __init__(self, path: str | Path, *, map_size: int = 256 * 1024 * 1024):
        self.path = ensure_dir(path)
        self._env = lmdb.open(
            str(self.path),
            map_size=map_size,
            max_dbs=len(self._DATABASES) + 2,
            subdir=True,
            create=True,
            lock=True,
            sync=True,
            writemap=False,
        )
        self._handles = {name: self._env.open_db(db_name) for name, db_name in self._DATABASES.items()}
        self._closed = False

    def put_text(self, bucket: str, text: str, *, key: str | None = None) -> str:
        content_id = key or make_uuid7()
        with self._env.begin(write=True, db=self._db(bucket)) as txn:
            txn.put(content_id.encode("utf-8"), text.encode("utf-8"))
        return content_id

    def get_text(self, bucket: str, key: str) -> str | None:
        with self._env.begin(db=self._db(bucket)) as txn:
            value = txn.get(key.encode("utf-8"))
        return value.decode("utf-8") if value is not None else None

    def put_json(self, bucket: str, payload, *, key: str | None = None) -> str:
        return self.put_text(bucket, json_dumps(payload), key=key)

    def get_json(self, bucket: str, key: str, default=None):
        value = self.get_text(bucket, key)
        if value is None:
            return default
        return json_loads(value, default)

    def delete(self, bucket: str, key: str) -> bool:
        with self._env.begin(write=True, db=self._db(bucket)) as txn:
            return bool(txn.delete(key.encode("utf-8")))

    def close(self) -> None:
        if self._closed:
            return
        self._env.close()
        self._closed = True

    def _db(self, bucket: str):
        if bucket not in self._handles:
            raise ValueError(f"Unknown LMDB bucket `{bucket}`")
        return self._handles[bucket]
