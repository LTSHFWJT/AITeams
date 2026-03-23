from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

from aiteams.utils import json_dumps, json_loads, make_id, make_uuid7, utcnow_iso


SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS workspaces (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        root_path TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS projects (
        id TEXT PRIMARY KEY,
        workspace_id TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS blueprints (
        id TEXT PRIMARY KEY,
        workspace_id TEXT NOT NULL,
        project_id TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        version TEXT NOT NULL,
        raw_format TEXT NOT NULL,
        raw_text TEXT NOT NULL,
        spec_json TEXT NOT NULL,
        is_template INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
        FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS task_releases (
        id TEXT PRIMARY KEY,
        blueprint_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        project_id TEXT NOT NULL,
        title TEXT,
        prompt TEXT NOT NULL,
        input_json TEXT NOT NULL DEFAULT '{}',
        approval_mode TEXT NOT NULL DEFAULT 'auto',
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(blueprint_id) REFERENCES blueprints(id) ON DELETE CASCADE,
        FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
        FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY,
        task_release_id TEXT NOT NULL,
        blueprint_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        project_id TEXT NOT NULL,
        status TEXT NOT NULL,
        summary TEXT,
        current_node_id TEXT,
        state_json TEXT NOT NULL DEFAULT '{}',
        result_json TEXT NOT NULL DEFAULT '{}',
        started_at TEXT,
        finished_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(task_release_id) REFERENCES task_releases(id) ON DELETE CASCADE,
        FOREIGN KEY(blueprint_id) REFERENCES blueprints(id) ON DELETE CASCADE,
        FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
        FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS steps (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        node_id TEXT NOT NULL,
        node_type TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        input_json TEXT NOT NULL DEFAULT '{}',
        output_json TEXT NOT NULL DEFAULT '{}',
        error_text TEXT,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_events (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        step_id TEXT,
        event_type TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS approvals (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        node_id TEXT NOT NULL,
        title TEXT NOT NULL,
        detail TEXT NOT NULL,
        status TEXT NOT NULL,
        resolution_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        resolved_at TEXT,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
        FOREIGN KEY(step_id) REFERENCES steps(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS artifacts (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        name TEXT NOT NULL,
        path TEXT NOT NULL,
        summary TEXT,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
        FOREIGN KEY(step_id) REFERENCES steps(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS checkpoints (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        step_id TEXT,
        node_id TEXT NOT NULL,
        snapshot_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS provider_profiles (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        provider_type TEXT NOT NULL,
        description TEXT,
        config_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS provider_profile_secrets (
        provider_profile_id TEXT PRIMARY KEY,
        secret_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL,
        FOREIGN KEY(provider_profile_id) REFERENCES provider_profiles(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS plugins (
        id TEXT PRIMARY KEY,
        key TEXT NOT NULL,
        name TEXT NOT NULL,
        version TEXT NOT NULL,
        plugin_type TEXT NOT NULL,
        description TEXT,
        manifest_json TEXT NOT NULL DEFAULT '{}',
        install_path TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_templates (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        description TEXT,
        version TEXT NOT NULL DEFAULT 'v1',
        spec_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS team_templates (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        version TEXT NOT NULL DEFAULT 'v1',
        spec_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS blueprint_builds (
        id TEXT PRIMARY KEY,
        team_template_id TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        spec_json TEXT NOT NULL DEFAULT '{}',
        resource_lock_json TEXT NOT NULL DEFAULT '{}',
        blueprint_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(team_template_id) REFERENCES team_templates(id) ON DELETE CASCADE,
        FOREIGN KEY(blueprint_id) REFERENCES blueprints(id) ON DELETE SET NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_blueprints_project ON blueprints(project_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_task_releases_project ON task_releases(project_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_runs_project ON runs(project_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_steps_run ON steps(run_id, created_at ASC)",
    "CREATE INDEX IF NOT EXISTS idx_events_run ON run_events(run_id, created_at ASC)",
    "CREATE INDEX IF NOT EXISTS idx_approvals_run ON approvals(run_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts(run_id, created_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_plugins_key_version ON plugins(key, version)",
    "CREATE INDEX IF NOT EXISTS idx_provider_profiles_type ON provider_profiles(provider_type, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_agent_templates_role ON agent_templates(role, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_team_templates_updated ON team_templates(updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_builds_team_template ON blueprint_builds(team_template_id, created_at DESC)",
]


class MetadataStore:
    def __init__(
        self,
        path: str | Path,
        *,
        default_workspace_id: str,
        default_workspace_name: str,
        default_project_id: str,
        default_project_name: str,
        workspace_root: str | Path,
    ):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(self.path), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._workspace_root = str(Path(workspace_root).expanduser().resolve())
        self._defaults = {
            "workspace_id": default_workspace_id,
            "workspace_name": default_workspace_name,
            "project_id": default_project_id,
            "project_name": default_project_name,
        }
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            for statement in SCHEMA:
                self._connection.execute(statement)
            self._connection.commit()
        self.ensure_defaults()

    def ensure_defaults(self) -> None:
        workspace = self.get_workspace(self._defaults["workspace_id"])
        if workspace is None:
            self.create_workspace(
                workspace_id=self._defaults["workspace_id"],
                name=self._defaults["workspace_name"],
                description="System default workspace.",
                root_path=f"{self._workspace_root}/{self._defaults['workspace_id']}",
            )
        project = self.get_project(self._defaults["project_id"])
        if project is None:
            self.create_project(
                project_id=self._defaults["project_id"],
                workspace_id=self._defaults["workspace_id"],
                name=self._defaults["project_name"],
                description="System default project.",
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()

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

    def _deserialize(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        item = dict(row)
        for field in (
            "spec_json",
            "input_json",
            "state_json",
            "result_json",
            "output_json",
            "payload_json",
            "resolution_json",
            "metadata_json",
            "snapshot_json",
            "config_json",
            "secret_json",
            "manifest_json",
            "resource_lock_json",
        ):
            if field in item:
                item[field] = json_loads(item.get(field), {})
        return item

    def _deserialize_many(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self._deserialize(row) for row in rows if row is not None]

    def create_workspace(self, *, workspace_id: str | None = None, name: str, description: str, root_path: str) -> dict[str, Any]:
        record_id = workspace_id or make_id("ws")
        now = utcnow_iso()
        self.execute(
            """
            INSERT OR REPLACE INTO workspaces(id, name, description, root_path, created_at, updated_at)
            VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM workspaces WHERE id = ?), ?), ?)
            """,
            (record_id, name, description, root_path, record_id, now, now),
        )
        workspace = self.get_workspace(record_id)
        assert workspace is not None
        return workspace

    def list_workspaces(self) -> list[dict[str, Any]]:
        return self.fetch_all("SELECT * FROM workspaces ORDER BY updated_at DESC")

    def get_workspace(self, workspace_id: str) -> dict[str, Any] | None:
        return self.fetch_one("SELECT * FROM workspaces WHERE id = ?", (workspace_id,))

    def create_project(self, *, project_id: str | None = None, workspace_id: str, name: str, description: str) -> dict[str, Any]:
        record_id = project_id or make_id("proj")
        now = utcnow_iso()
        self.execute(
            """
            INSERT OR REPLACE INTO projects(id, workspace_id, name, description, created_at, updated_at)
            VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM projects WHERE id = ?), ?), ?)
            """,
            (record_id, workspace_id, name, description, record_id, now, now),
        )
        project = self.get_project(record_id)
        assert project is not None
        return project

    def list_projects(self, *, workspace_id: str | None = None) -> list[dict[str, Any]]:
        if workspace_id:
            return self.fetch_all("SELECT * FROM projects WHERE workspace_id = ? ORDER BY updated_at DESC", (workspace_id,))
        return self.fetch_all("SELECT * FROM projects ORDER BY updated_at DESC")

    def get_project(self, project_id: str) -> dict[str, Any] | None:
        return self.fetch_one("SELECT * FROM projects WHERE id = ?", (project_id,))

    def save_blueprint(
        self,
        *,
        blueprint_id: str | None,
        workspace_id: str,
        project_id: str,
        name: str,
        description: str,
        version: str,
        raw_format: str,
        raw_text: str,
        spec: dict[str, Any],
        is_template: bool = False,
    ) -> dict[str, Any]:
        record_id = blueprint_id or make_id("bp")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO blueprints(
                id, workspace_id, project_id, name, description, version, raw_format, raw_text, spec_json, is_template, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM blueprints WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                project_id = excluded.project_id,
                name = excluded.name,
                description = excluded.description,
                version = excluded.version,
                raw_format = excluded.raw_format,
                raw_text = excluded.raw_text,
                spec_json = excluded.spec_json,
                is_template = excluded.is_template,
                updated_at = excluded.updated_at
            """,
            (
                record_id,
                workspace_id,
                project_id,
                name,
                description,
                version,
                raw_format,
                raw_text,
                json_dumps(spec),
                1 if is_template else 0,
                record_id,
                now,
                now,
            ),
        )
        blueprint = self.get_blueprint(record_id)
        assert blueprint is not None
        return blueprint

    def list_blueprints(self, *, workspace_id: str | None = None, project_id: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if workspace_id:
            clauses.append("workspace_id = ?")
            params.append(workspace_id)
        if project_id:
            clauses.append("project_id = ?")
            params.append(project_id)
        sql = "SELECT * FROM blueprints"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        sql += " ORDER BY updated_at DESC"
        return self._deserialize_many(self.fetch_all(sql, tuple(params)))

    def get_blueprint(self, blueprint_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM blueprints WHERE id = ?", (blueprint_id,)))

    def delete_blueprint(self, blueprint_id: str) -> dict[str, Any] | None:
        existing = self.get_blueprint(blueprint_id)
        if existing is None:
            return None
        self.execute("DELETE FROM blueprints WHERE id = ?", (blueprint_id,))
        return existing

    def save_provider_profile(
        self,
        *,
        provider_profile_id: str | None,
        name: str,
        provider_type: str,
        description: str,
        config: dict[str, Any],
        secret: dict[str, Any] | None = None,
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = provider_profile_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO provider_profiles(id, name, provider_type, description, config_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM provider_profiles WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                provider_type = excluded.provider_type,
                description = excluded.description,
                config_json = excluded.config_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, name, provider_type, description, json_dumps(config), status, record_id, now, now),
        )
        if secret is not None:
            self.execute(
                """
                INSERT INTO provider_profile_secrets(provider_profile_id, secret_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(provider_profile_id) DO UPDATE SET
                    secret_json = excluded.secret_json,
                    updated_at = excluded.updated_at
                """,
                (record_id, json_dumps(secret), now),
            )
        provider = self.get_provider_profile(record_id)
        assert provider is not None
        return provider

    def get_provider_profile(self, provider_profile_id: str, *, include_secret: bool = False) -> dict[str, Any] | None:
        provider = self._deserialize(self.fetch_one("SELECT * FROM provider_profiles WHERE id = ?", (provider_profile_id,)))
        return self._attach_provider_secret(provider, include_secret=include_secret)

    def list_provider_profiles(self, *, include_secret: bool = False) -> list[dict[str, Any]]:
        rows = self._deserialize_many(self.fetch_all("SELECT * FROM provider_profiles ORDER BY updated_at DESC"))
        return [self._attach_provider_secret(row, include_secret=include_secret) for row in rows]

    def list_provider_profiles_page(
        self,
        *,
        query: str | None = None,
        provider_type: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        include_secret: bool = False,
    ) -> dict[str, Any]:
        conditions: list[str] = []
        params: list[Any] = []
        keyword = str(query or "").strip().lower()
        if keyword:
            like = f"%{keyword}%"
            conditions.append("(LOWER(name) LIKE ? OR LOWER(provider_type) LIKE ? OR LOWER(description) LIKE ?)")
            params.extend([like, like, like])
        selected_type = str(provider_type or "").strip()
        if selected_type:
            conditions.append("provider_type = ?")
            params.append(selected_type)
        where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        total = int(
            (self.fetch_one(f"SELECT COUNT(*) AS count FROM provider_profiles{where_sql}", tuple(params)) or {}).get("count", 0) or 0
        )
        safe_offset = max(0, int(offset or 0))
        sql = f"SELECT * FROM provider_profiles{where_sql} ORDER BY updated_at DESC"
        query_params = list(params)
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            query_params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        rows = self._deserialize_many(self.fetch_all(sql, tuple(query_params)))
        return {
            "items": [self._attach_provider_secret(row, include_secret=include_secret) for row in rows],
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def delete_provider_profile(self, provider_profile_id: str) -> dict[str, Any] | None:
        existing = self.get_provider_profile(provider_profile_id)
        if existing is None:
            return None
        self.execute("DELETE FROM provider_profile_secrets WHERE provider_profile_id = ?", (provider_profile_id,))
        self.execute("DELETE FROM provider_profiles WHERE id = ?", (provider_profile_id,))
        return existing

    def _attach_provider_secret(self, provider: dict[str, Any] | None, *, include_secret: bool) -> dict[str, Any] | None:
        if provider is None:
            return None
        secret_row = self._deserialize(self.fetch_one("SELECT * FROM provider_profile_secrets WHERE provider_profile_id = ?", (provider["id"],)))
        secret = dict((secret_row or {}).get("secret_json") or {})
        item = dict(provider)
        item["has_secret"] = bool(secret)
        if include_secret:
            item["secret_json"] = secret
        return item

    def save_plugin(
        self,
        *,
        plugin_id: str | None,
        key: str,
        name: str,
        version: str,
        plugin_type: str,
        description: str,
        manifest: dict[str, Any],
        install_path: str | None,
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = plugin_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO plugins(id, key, name, version, plugin_type, description, manifest_json, install_path, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM plugins WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                key = excluded.key,
                name = excluded.name,
                version = excluded.version,
                plugin_type = excluded.plugin_type,
                description = excluded.description,
                manifest_json = excluded.manifest_json,
                install_path = excluded.install_path,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, key, name, version, plugin_type, description, json_dumps(manifest), install_path, status, record_id, now, now),
        )
        plugin = self.get_plugin(record_id)
        assert plugin is not None
        return plugin

    def get_plugin(self, plugin_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM plugins WHERE id = ?", (plugin_id,)))

    def get_plugin_by_key(self, key: str) -> dict[str, Any] | None:
        row = self.fetch_one("SELECT * FROM plugins WHERE key = ? ORDER BY updated_at DESC LIMIT 1", (key,))
        return self._deserialize(row)

    def list_plugins(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM plugins ORDER BY updated_at DESC"))

    def list_plugins_page(self, *, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
        total = int((self.fetch_one("SELECT COUNT(*) AS count FROM plugins") or {}).get("count", 0) or 0)
        safe_offset = max(0, int(offset or 0))
        sql = "SELECT * FROM plugins ORDER BY updated_at DESC"
        params: list[Any] = []
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        return {
            "items": self._deserialize_many(self.fetch_all(sql, tuple(params))),
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def save_agent_template(
        self,
        *,
        agent_template_id: str | None,
        name: str,
        role: str,
        description: str,
        version: str,
        spec: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = agent_template_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO agent_templates(id, name, role, description, version, spec_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM agent_templates WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                role = excluded.role,
                description = excluded.description,
                version = excluded.version,
                spec_json = excluded.spec_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, name, role, description, version, json_dumps(spec), status, record_id, now, now),
        )
        template = self.get_agent_template(record_id)
        assert template is not None
        return template

    def get_agent_template(self, agent_template_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM agent_templates WHERE id = ?", (agent_template_id,)))

    def list_agent_templates(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM agent_templates ORDER BY updated_at DESC"))

    def list_agent_templates_page(self, *, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
        total = int((self.fetch_one("SELECT COUNT(*) AS count FROM agent_templates") or {}).get("count", 0) or 0)
        safe_offset = max(0, int(offset or 0))
        sql = "SELECT * FROM agent_templates ORDER BY updated_at DESC"
        params: list[Any] = []
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        return {
            "items": self._deserialize_many(self.fetch_all(sql, tuple(params))),
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def delete_agent_template(self, agent_template_id: str) -> dict[str, Any] | None:
        existing = self.get_agent_template(agent_template_id)
        if existing is None:
            return None
        self.execute("DELETE FROM agent_templates WHERE id = ?", (agent_template_id,))
        return existing

    def save_team_template(
        self,
        *,
        team_template_id: str | None,
        name: str,
        description: str,
        version: str,
        spec: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = team_template_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO team_templates(id, name, description, version, spec_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM team_templates WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                version = excluded.version,
                spec_json = excluded.spec_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, name, description, version, json_dumps(spec), status, record_id, now, now),
        )
        template = self.get_team_template(record_id)
        assert template is not None
        return template

    def get_team_template(self, team_template_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM team_templates WHERE id = ?", (team_template_id,)))

    def list_team_templates(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM team_templates ORDER BY updated_at DESC"))

    def save_blueprint_build(
        self,
        *,
        build_id: str | None,
        team_template_id: str,
        name: str,
        description: str,
        spec: dict[str, Any],
        resource_lock: dict[str, Any],
        blueprint_id: str | None,
    ) -> dict[str, Any]:
        record_id = build_id or make_id("build")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO blueprint_builds(id, team_template_id, name, description, spec_json, resource_lock_json, blueprint_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM blueprint_builds WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                team_template_id = excluded.team_template_id,
                name = excluded.name,
                description = excluded.description,
                spec_json = excluded.spec_json,
                resource_lock_json = excluded.resource_lock_json,
                blueprint_id = excluded.blueprint_id,
                updated_at = excluded.updated_at
            """,
            (record_id, team_template_id, name, description, json_dumps(spec), json_dumps(resource_lock), blueprint_id, record_id, now, now),
        )
        build = self.get_blueprint_build(record_id)
        assert build is not None
        return build

    def get_blueprint_build(self, build_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM blueprint_builds WHERE id = ?", (build_id,)))

    def list_blueprint_builds(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM blueprint_builds ORDER BY updated_at DESC"))

    def create_task_release(
        self,
        *,
        blueprint_id: str,
        workspace_id: str,
        project_id: str,
        title: str | None,
        prompt: str,
        inputs: dict[str, Any],
        approval_mode: str,
    ) -> dict[str, Any]:
        record_id = make_id("task")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO task_releases(id, blueprint_id, workspace_id, project_id, title, prompt, input_json, approval_mode, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_id, blueprint_id, workspace_id, project_id, title, prompt, json_dumps(inputs), approval_mode, "queued", now, now),
        )
        task = self.get_task_release(record_id)
        assert task is not None
        return task

    def update_task_release(self, task_release_id: str, *, status: str) -> dict[str, Any] | None:
        task = self.get_task_release(task_release_id)
        if task is None:
            return None
        self.execute("UPDATE task_releases SET status = ?, updated_at = ? WHERE id = ?", (status, utcnow_iso(), task_release_id))
        return self.get_task_release(task_release_id)

    def list_task_releases(self, *, project_id: str | None = None) -> list[dict[str, Any]]:
        if project_id:
            rows = self.fetch_all("SELECT * FROM task_releases WHERE project_id = ? ORDER BY updated_at DESC", (project_id,))
        else:
            rows = self.fetch_all("SELECT * FROM task_releases ORDER BY updated_at DESC")
        return self._deserialize_many(rows)

    def get_task_release(self, task_release_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM task_releases WHERE id = ?", (task_release_id,)))

    def create_run(self, *, task_release_id: str, blueprint_id: str, workspace_id: str, project_id: str, state: dict[str, Any]) -> dict[str, Any]:
        record_id = make_id("run")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO runs(id, task_release_id, blueprint_id, workspace_id, project_id, status, summary, current_node_id, state_json, result_json, started_at, finished_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_id, task_release_id, blueprint_id, workspace_id, project_id, "queued", None, None, json_dumps(state), json_dumps({}), None, None, now, now),
        )
        run = self.get_run(record_id)
        assert run is not None
        return run

    def update_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        summary: str | None = None,
        current_node_id: str | None = None,
        state: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
    ) -> dict[str, Any] | None:
        run = self.get_run(run_id)
        if run is None:
            return None
        self.execute(
            """
            UPDATE runs
            SET status = ?, summary = ?, current_node_id = ?, state_json = ?, result_json = ?, started_at = ?, finished_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                status or run["status"],
                summary if summary is not None else run.get("summary"),
                current_node_id,
                json_dumps(state if state is not None else run.get("state_json", {})),
                json_dumps(result if result is not None else run.get("result_json", {})),
                started_at if started_at is not None else run.get("started_at"),
                finished_at if finished_at is not None else run.get("finished_at"),
                utcnow_iso(),
                run_id,
            ),
        )
        return self.get_run(run_id)

    def list_runs(self, *, project_id: str | None = None) -> list[dict[str, Any]]:
        if project_id:
            rows = self.fetch_all("SELECT * FROM runs WHERE project_id = ? ORDER BY updated_at DESC", (project_id,))
        else:
            rows = self.fetch_all("SELECT * FROM runs ORDER BY updated_at DESC")
        return self._deserialize_many(rows)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM runs WHERE id = ?", (run_id,)))

    def create_step(self, *, run_id: str, node_id: str, node_type: str, status: str, attempt: int, input_payload: dict[str, Any]) -> dict[str, Any]:
        record_id = make_id("step")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO steps(id, run_id, node_id, node_type, status, attempt, input_json, output_json, error_text, created_at, started_at, finished_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_id, run_id, node_id, node_type, status, attempt, json_dumps(input_payload), json_dumps({}), None, now, now if status == "running" else None, None, now),
        )
        step = self.get_step(record_id)
        assert step is not None
        return step

    def update_step(
        self,
        step_id: str,
        *,
        status: str,
        output_payload: dict[str, Any] | None = None,
        error_text: str | None = None,
        finished: bool = False,
    ) -> dict[str, Any] | None:
        step = self.get_step(step_id)
        if step is None:
            return None
        now = utcnow_iso()
        self.execute(
            """
            UPDATE steps
            SET status = ?, output_json = ?, error_text = ?, finished_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                json_dumps(output_payload if output_payload is not None else step.get("output_json", {})),
                error_text,
                now if finished else step.get("finished_at"),
                now,
                step_id,
            ),
        )
        return self.get_step(step_id)

    def get_step(self, step_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM steps WHERE id = ?", (step_id,)))

    def list_steps(self, run_id: str) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM steps WHERE run_id = ? ORDER BY created_at ASC", (run_id,)))

    def next_step_attempt(self, run_id: str, node_id: str) -> int:
        row = self.fetch_one("SELECT COALESCE(MAX(attempt), 0) AS attempt FROM steps WHERE run_id = ? AND node_id = ?", (run_id, node_id))
        return int((row or {}).get("attempt", 0) or 0) + 1

    def latest_step_for_node(self, run_id: str, node_id: str) -> dict[str, Any] | None:
        rows = self.fetch_all("SELECT * FROM steps WHERE run_id = ? AND node_id = ? ORDER BY created_at DESC LIMIT 1", (run_id, node_id))
        return self._deserialize(rows[0]) if rows else None

    def add_event(self, *, run_id: str, event_type: str, payload: dict[str, Any], step_id: str | None = None) -> dict[str, Any]:
        record_id = make_id("evt")
        now = utcnow_iso()
        self.execute(
            "INSERT INTO run_events(id, run_id, step_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (record_id, run_id, step_id, event_type, json_dumps(payload), now),
        )
        event = self.fetch_one("SELECT * FROM run_events WHERE id = ?", (record_id,))
        assert event is not None
        return self._deserialize(event) or {}

    def list_events(self, run_id: str) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM run_events WHERE run_id = ? ORDER BY created_at ASC", (run_id,)))

    def create_approval(self, *, run_id: str, step_id: str, node_id: str, title: str, detail: str) -> dict[str, Any]:
        record_id = make_id("approval")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO approvals(id, run_id, step_id, node_id, title, detail, status, resolution_json, created_at, updated_at, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_id, run_id, step_id, node_id, title, detail, "pending", json_dumps({}), now, now, None),
        )
        approval = self.get_approval(record_id)
        assert approval is not None
        return approval

    def resolve_approval(self, approval_id: str, *, approved: bool, comment: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any] | None:
        approval = self.get_approval(approval_id)
        if approval is None:
            return None
        now = utcnow_iso()
        payload = {"approved": approved, "comment": comment or "", "metadata": metadata or {}}
        self.execute(
            "UPDATE approvals SET status = ?, resolution_json = ?, updated_at = ?, resolved_at = ? WHERE id = ?",
            ("approved" if approved else "rejected", json_dumps(payload), now, now, approval_id),
        )
        return self.get_approval(approval_id)

    def get_approval(self, approval_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM approvals WHERE id = ?", (approval_id,)))

    def list_approvals(self, *, run_id: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        sql = "SELECT * FROM approvals"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        sql += " ORDER BY created_at DESC"
        return self._deserialize_many(self.fetch_all(sql, tuple(params)))

    def create_artifact(
        self,
        *,
        run_id: str,
        step_id: str,
        kind: str,
        name: str,
        path: str,
        summary: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        record_id = make_id("artifact")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO artifacts(id, run_id, step_id, kind, name, path, summary, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_id, run_id, step_id, kind, name, path, summary, json_dumps(metadata), now),
        )
        artifact = self.fetch_one("SELECT * FROM artifacts WHERE id = ?", (record_id,))
        assert artifact is not None
        return self._deserialize(artifact) or {}

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM artifacts WHERE run_id = ? ORDER BY created_at ASC", (run_id,)))

    def save_checkpoint(self, *, run_id: str, node_id: str, snapshot: dict[str, Any], step_id: str | None = None) -> dict[str, Any]:
        record_id = make_id("chk")
        now = utcnow_iso()
        self.execute(
            "INSERT INTO checkpoints(id, run_id, step_id, node_id, snapshot_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (record_id, run_id, step_id, node_id, json_dumps(snapshot), now),
        )
        checkpoint = self.fetch_one("SELECT * FROM checkpoints WHERE id = ?", (record_id,))
        assert checkpoint is not None
        return self._deserialize(checkpoint) or {}

    def latest_checkpoint(self, run_id: str) -> dict[str, Any] | None:
        rows = self.fetch_all("SELECT * FROM checkpoints WHERE run_id = ? ORDER BY created_at DESC LIMIT 1", (run_id,))
        return self._deserialize(rows[0]) if rows else None

    def dashboard_summary(self) -> dict[str, Any]:
        return {
            "workspace_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM workspaces") or {}).get("count", 0) or 0),
            "project_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM projects") or {}).get("count", 0) or 0),
            "blueprint_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM blueprints") or {}).get("count", 0) or 0),
            "task_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM task_releases") or {}).get("count", 0) or 0),
            "run_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM runs") or {}).get("count", 0) or 0),
            "pending_approval_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM approvals WHERE status = 'pending'") or {}).get("count", 0) or 0),
            "provider_profile_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM provider_profiles") or {}).get("count", 0) or 0),
            "plugin_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM plugins") or {}).get("count", 0) or 0),
            "agent_template_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM agent_templates") or {}).get("count", 0) or 0),
            "team_template_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM team_templates") or {}).get("count", 0) or 0),
            "build_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM blueprint_builds") or {}).get("count", 0) or 0),
        }

    def storage_info(self) -> dict[str, Any]:
        return {
            "metadata_driver": "sqlite",
            "metadata_path": str(self.path),
            "journal_mode": "wal",
        }

    def get_run_bundle(self, run_id: str) -> dict[str, Any] | None:
        run = self.get_run(run_id)
        if run is None:
            return None
        task = self.get_task_release(str(run["task_release_id"]))
        blueprint = self.get_blueprint(str(run["blueprint_id"]))
        return {
            "run": run,
            "task_release": task,
            "blueprint": blueprint,
            "steps": self.list_steps(run_id),
            "events": self.list_events(run_id),
            "approvals": self.list_approvals(run_id=run_id),
            "artifacts": self.list_artifacts(run_id),
            "checkpoint": self.latest_checkpoint(run_id),
        }
