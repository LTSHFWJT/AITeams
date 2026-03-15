from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any


PLATFORM_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS provider_configs (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        provider_type TEXT NOT NULL,
        base_url TEXT,
        api_key TEXT,
        model TEXT NOT NULL,
        api_version TEXT,
        organization TEXT,
        extra_headers TEXT NOT NULL DEFAULT '{}',
        extra_config TEXT NOT NULL DEFAULT '{}',
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agents (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        role TEXT NOT NULL,
        system_prompt TEXT NOT NULL,
        provider_id TEXT NOT NULL,
        model_override TEXT,
        temperature REAL NOT NULL DEFAULT 0.2,
        max_tokens INTEGER,
        collaboration_style TEXT NOT NULL DEFAULT 'specialist',
        is_enabled INTEGER NOT NULL DEFAULT 1,
        metadata TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(provider_id) REFERENCES provider_configs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collaboration_sessions (
        id TEXT PRIMARY KEY,
        title TEXT,
        user_prompt TEXT NOT NULL,
        lead_agent_id TEXT,
        strategy TEXT NOT NULL,
        rounds INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL,
        final_summary TEXT,
        metadata TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(lead_agent_id) REFERENCES agents(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collaboration_participants (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        turn_order INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(session_id, agent_id),
        FOREIGN KEY(session_id) REFERENCES collaboration_sessions(id) ON DELETE CASCADE,
        FOREIGN KEY(agent_id) REFERENCES agents(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collaboration_messages (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        agent_id TEXT,
        role TEXT NOT NULL,
        round_index INTEGER NOT NULL DEFAULT 0,
        order_index INTEGER NOT NULL,
        content TEXT NOT NULL,
        provider_id TEXT,
        model TEXT,
        refs_json TEXT NOT NULL DEFAULT '[]',
        metadata TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        FOREIGN KEY(session_id) REFERENCES collaboration_sessions(id) ON DELETE CASCADE,
        FOREIGN KEY(agent_id) REFERENCES agents(id),
        FOREIGN KEY(provider_id) REFERENCES provider_configs(id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_provider_name ON provider_configs(name)",
    "CREATE INDEX IF NOT EXISTS idx_agent_provider ON agents(provider_id, is_enabled)",
    "CREATE INDEX IF NOT EXISTS idx_session_created ON collaboration_sessions(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_participants_order ON collaboration_participants(session_id, turn_order)",
    "CREATE INDEX IF NOT EXISTS idx_messages_session_order ON collaboration_messages(session_id, order_index)",
]


class PlatformDatabase:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
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
            for statement in PLATFORM_SCHEMA:
                self._connection.execute(statement)
            self._connection.commit()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        with self._lock:
            cursor = self._connection.execute(sql, params)
            self._connection.commit()
            return cursor

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(sql, params).fetchone()
            return dict(row) if row else None

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(sql, params).fetchall()
            return [dict(row) for row in rows]

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._connection.close()
            self._closed = True
