from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

from aimemory.storage.sqlite.schema import SCHEMA_STATEMENTS


class SQLiteDatabase:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(self.path), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._closed = False
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            for statement in SCHEMA_STATEMENTS:
                self._connection.execute(statement)
            current = self.fetch_one("SELECT version FROM schema_version LIMIT 1")
            if current is None:
                self.execute("INSERT INTO schema_version(version) VALUES (?)", (1,))
            self._connection.commit()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        with self._lock:
            cursor = self._connection.execute(sql, params)
            self._connection.commit()
            return cursor

    def executemany(self, sql: str, items: list[tuple[Any, ...]]) -> None:
        with self._lock:
            self._connection.executemany(sql, items)
            self._connection.commit()

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._lock:
            cursor = self._connection.execute(sql, params)
            row = cursor.fetchone()
            return dict(row) if row else None

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._connection.execute(sql, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def ensure_schema(self, statements: list[str]) -> None:
        with self._lock:
            for statement in statements:
                self._connection.execute(statement)
            self._connection.commit()

    def table_columns(self, table_name: str) -> set[str]:
        with self._lock:
            cursor = self._connection.execute(f"PRAGMA table_info({table_name})")
            return {str(row[1]) for row in cursor.fetchall()}

    def ensure_columns(self, table_name: str, columns: dict[str, str]) -> None:
        with self._lock:
            existing = self.table_columns(table_name)
            for column_name, definition in columns.items():
                if column_name in existing:
                    continue
                self._connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._connection.close()
            self._closed = True

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
