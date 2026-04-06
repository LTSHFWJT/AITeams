from __future__ import annotations

import re
import shutil
import sqlite3
import threading
from pathlib import Path
from typing import Any, Sequence

from aiteams.review_policy_migration import migrate_review_policies_in_connection
from aiteams.utils import json_dumps, json_loads, make_id, make_uuid7, slugify, trim_text, utcnow_iso


SEARCH_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]")


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
        metadata_json TEXT NOT NULL DEFAULT '{}',
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
    CREATE TABLE IF NOT EXISTS local_models (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        model_type TEXT NOT NULL,
        model_path TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS plugins (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        install_path TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skills (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        storage_path TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_groups (
        id TEXT PRIMARY KEY,
        key TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_group_members (
        skill_id TEXT NOT NULL,
        skill_group_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY(skill_id, skill_group_id),
        FOREIGN KEY(skill_id) REFERENCES skills(id) ON DELETE CASCADE,
        FOREIGN KEY(skill_group_id) REFERENCES skill_groups(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS static_memories (
        id TEXT PRIMARY KEY,
        key TEXT NOT NULL,
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
    CREATE TABLE IF NOT EXISTS knowledge_bases (
        id TEXT PRIMARY KEY,
        key TEXT NOT NULL,
        name TEXT NOT NULL,
        config_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_file_blobs (
        id TEXT PRIMARY KEY,
        xxh128 TEXT NOT NULL,
        storage_name TEXT NOT NULL,
        storage_relpath TEXT NOT NULL,
        byte_size INTEGER NOT NULL DEFAULT 0,
        mime_type TEXT,
        ext_hint TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_file_aliases (
        id TEXT PRIMARY KEY,
        blob_id TEXT NOT NULL,
        filename TEXT NOT NULL,
        normalized_filename TEXT NOT NULL,
        suffix TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(blob_id) REFERENCES knowledge_file_blobs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_documents (
        id TEXT PRIMARY KEY,
        knowledge_base_id TEXT NOT NULL,
        pool_document_id TEXT,
        blob_id TEXT,
        alias_id TEXT,
        key TEXT NOT NULL,
        title TEXT NOT NULL,
        source_path TEXT,
        content_text TEXT NOT NULL DEFAULT '',
        document_status TEXT NOT NULL DEFAULT 'not_embedded',
        sync_status TEXT NOT NULL DEFAULT 'idle',
        last_error TEXT,
        embedded_at TEXT,
        removed_at TEXT,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(knowledge_base_id) REFERENCES knowledge_bases(id) ON DELETE CASCADE,
        FOREIGN KEY(pool_document_id) REFERENCES knowledge_pool_documents(id) ON DELETE SET NULL,
        FOREIGN KEY(blob_id) REFERENCES knowledge_file_blobs(id) ON DELETE SET NULL,
        FOREIGN KEY(alias_id) REFERENCES knowledge_file_aliases(id) ON DELETE SET NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_pool_documents (
        id TEXT PRIMARY KEY,
        knowledge_base_id TEXT NOT NULL,
        blob_id TEXT NOT NULL,
        alias_id TEXT NOT NULL,
        key TEXT NOT NULL,
        title TEXT NOT NULL,
        source_path TEXT,
        content_text TEXT NOT NULL DEFAULT '',
        upload_method TEXT NOT NULL DEFAULT 'http',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(knowledge_base_id) REFERENCES knowledge_bases(id) ON DELETE CASCADE,
        FOREIGN KEY(blob_id) REFERENCES knowledge_file_blobs(id) ON DELETE RESTRICT,
        FOREIGN KEY(alias_id) REFERENCES knowledge_file_aliases(id) ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_embedding_jobs (
        id TEXT PRIMARY KEY,
        knowledge_base_id TEXT NOT NULL,
        action TEXT NOT NULL DEFAULT 'save',
        status TEXT NOT NULL DEFAULT 'pending',
        stage TEXT NOT NULL DEFAULT 'queued',
        total_documents INTEGER NOT NULL DEFAULT 0,
        processed_documents INTEGER NOT NULL DEFAULT 0,
        completed_documents INTEGER NOT NULL DEFAULT 0,
        failed_documents INTEGER NOT NULL DEFAULT 0,
        total_chunks_estimated INTEGER NOT NULL DEFAULT 0,
        embedded_chunks_completed INTEGER NOT NULL DEFAULT 0,
        current_document_id TEXT,
        current_document_title TEXT,
        message TEXT,
        error_text TEXT,
        result_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(knowledge_base_id) REFERENCES knowledge_bases(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS review_policies (
        id TEXT PRIMARY KEY,
        key TEXT NOT NULL,
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
    CREATE TABLE IF NOT EXISTS agent_definitions (
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
    CREATE TABLE IF NOT EXISTS team_definitions (
        id TEXT PRIMARY KEY,
        key TEXT NOT NULL,
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
    CREATE TABLE IF NOT EXISTS platform_settings (
        setting_key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS task_threads (
        id TEXT PRIMARY KEY,
        team_definition_id TEXT,
        run_id TEXT,
        workspace_id TEXT NOT NULL,
        project_id TEXT NOT NULL,
        title TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(team_definition_id) REFERENCES team_definitions(id) ON DELETE SET NULL,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE SET NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS message_events (
        id TEXT PRIMARY KEY,
        run_id TEXT,
        thread_id TEXT,
        source_agent_id TEXT,
        target_agent_id TEXT,
        message_type TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'delivered',
        created_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE SET NULL,
        FOREIGN KEY(thread_id) REFERENCES task_threads(id) ON DELETE SET NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS team_build_snapshots (
        id TEXT PRIMARY KEY,
        team_definition_id TEXT,
        run_id TEXT NOT NULL,
        runtime_tree_snapshot_json TEXT NOT NULL DEFAULT '{}',
        resource_lock_json TEXT NOT NULL DEFAULT '{}',
        compiled_metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(team_definition_id) REFERENCES team_definitions(id) ON DELETE SET NULL,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_blueprints_project ON blueprints(project_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_task_releases_project ON task_releases(project_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_runs_project ON runs(project_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_steps_run ON steps(run_id, created_at ASC)",
    "CREATE INDEX IF NOT EXISTS idx_events_run ON run_events(run_id, created_at ASC)",
    "CREATE INDEX IF NOT EXISTS idx_approvals_run ON approvals(run_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts(run_id, created_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_skills_name ON skills(name)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_skill_groups_key ON skill_groups(key)",
    "CREATE INDEX IF NOT EXISTS idx_skill_group_members_group ON skill_group_members(skill_group_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_skill_group_members_skill ON skill_group_members(skill_id, updated_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_static_memories_key_version ON static_memories(key, version)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_bases_key ON knowledge_bases(key)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_file_blobs_xxh128 ON knowledge_file_blobs(xxh128)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_file_blobs_storage_relpath ON knowledge_file_blobs(storage_relpath)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_file_aliases_blob_filename ON knowledge_file_aliases(blob_id, normalized_filename)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_pool_documents_kb_blob ON knowledge_pool_documents(knowledge_base_id, blob_id)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_pool_documents_kb_updated ON knowledge_pool_documents(knowledge_base_id, updated_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_documents_kb_blob ON knowledge_documents(knowledge_base_id, blob_id)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_documents_kb_status ON knowledge_documents(knowledge_base_id, document_status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_embedding_jobs_kb_status ON knowledge_embedding_jobs(knowledge_base_id, status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_embedding_jobs_updated ON knowledge_embedding_jobs(updated_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_review_policies_key_version ON review_policies(key, version)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_team_definitions_key_version ON team_definitions(key, version)",
    "CREATE INDEX IF NOT EXISTS idx_provider_profiles_type ON provider_profiles(provider_type, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_local_models_type ON local_models(model_type, updated_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_local_models_path ON local_models(model_path)",
    "CREATE INDEX IF NOT EXISTS idx_skills_updated ON skills(updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_static_memories_updated ON static_memories(updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_documents_kb ON knowledge_documents(knowledge_base_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_review_policies_updated ON review_policies(updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_agent_definitions_role ON agent_definitions(role, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_team_definitions_updated ON team_definitions(updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_task_threads_team ON task_threads(team_definition_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_message_events_thread ON message_events(thread_id, created_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_team_build_snapshots_run ON team_build_snapshots(run_id)",
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
        self._skill_storage_root = (self.path.parent / "deepagents-skills").expanduser().resolve()
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
            self._apply_migrations()
            self._connection.commit()
        self.ensure_defaults()

    def _apply_migrations(self) -> None:
        self._migrate_plugins_table()
        self._ensure_column("approvals", "metadata_json", "TEXT NOT NULL DEFAULT '{}'")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS skill_groups (
                id TEXT PRIMARY KEY,
                key TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._ensure_skill_group_members_table()
        self._migrate_skills_table()
        self._normalize_skill_storage_layout()
        self._migrate_knowledge_bases_table()
        self._migrate_agent_templates_to_definitions()
        self._migrate_review_policy_specs()
        self._connection.execute("DROP INDEX IF EXISTS idx_agent_templates_role")
        self._connection.execute("DROP INDEX IF EXISTS idx_skills_key_version")
        self._connection.execute("DROP INDEX IF EXISTS idx_skill_groups_sort")
        self._connection.execute("DROP INDEX IF EXISTS idx_plugins_key_version")
        self._connection.execute("DROP TABLE IF EXISTS plugin_secrets")
        self._connection.execute("DROP TABLE IF EXISTS agent_templates")
        self._connection.execute("DROP INDEX IF EXISTS idx_memory_profiles_key_version")
        self._connection.execute("DROP INDEX IF EXISTS idx_memory_profiles_updated")
        self._connection.execute("DROP TABLE IF EXISTS memory_profiles")

    def _table_exists(self, table_name: str) -> bool:
        return self.fetch_one("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)) is not None

    def _ensure_skill_group_members_table(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS skill_group_members (
                skill_id TEXT NOT NULL,
                skill_group_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(skill_id, skill_group_id),
                FOREIGN KEY(skill_id) REFERENCES skills(id) ON DELETE CASCADE,
                FOREIGN KEY(skill_group_id) REFERENCES skill_groups(id) ON DELETE CASCADE
            )
            """
        )

    def _migrate_plugins_table(self) -> None:
        if not self._table_exists("plugins"):
            return
        columns = {str(row["name"]) for row in self.fetch_all("PRAGMA table_info(plugins)")}
        desired_columns = {"id", "name", "install_path"}
        if columns == desired_columns:
            return
        legacy_order = "created_at ASC, id ASC" if "created_at" in columns else "id ASC"
        legacy_rows = self.fetch_all(f"SELECT * FROM plugins ORDER BY {legacy_order}")
        self._connection.execute("DROP TABLE IF EXISTS plugins__new")
        self._connection.execute(
            """
            CREATE TABLE plugins__new (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                install_path TEXT
            )
            """
        )
        for row in legacy_rows:
            record_id = str(row.get("id") or "").strip() or make_uuid7()
            name = trim_text(str(row.get("name") or row.get("key") or "").strip(), limit=255) or record_id
            install_path = str(row.get("install_path") or "").strip() or None
            self._connection.execute(
                """
                INSERT INTO plugins__new(id, name, install_path)
                VALUES (?, ?, ?)
                """,
                (record_id, name, install_path),
            )
        self._connection.execute("DROP TABLE plugins")
        self._connection.execute("ALTER TABLE plugins__new RENAME TO plugins")

    def _normalize_skill_storage_path(self, raw_path: Any) -> str:
        text = str(raw_path or "").replace("\\", "/").strip()
        if not text:
            return ""
        is_absolute = bool(re.match(r"^[a-zA-Z]:/", text)) or text.startswith("/")
        if is_absolute:
            try:
                relative = Path(text).expanduser().resolve().relative_to(self._skill_storage_root)
            except Exception:
                return ""
            return relative.as_posix().strip("/")
        parts = [part for part in text.split("/") if part and part not in {".", ".."}]
        return "/".join(parts)

    def _desired_skill_storage_path(self, *, record_id: str, name: str) -> str:
        folder_name = str(name or "").strip()
        if not folder_name:
            folder_name = record_id
        if Path(folder_name).name != folder_name or any(part in {"", ".", ".."} for part in Path(folder_name).parts):
            folder_name = record_id
        return folder_name

    def _legacy_skill_group_ids(self, row: dict[str, Any]) -> list[str]:
        spec = json_loads(row.get("spec_json"), {})
        refs: list[dict[str, Any]] = []
        raw_refs = spec.get("group_refs")
        if isinstance(raw_refs, list):
            refs.extend(item for item in raw_refs if isinstance(item, dict))
        legacy = {
            "id": spec.get("group_id"),
            "key": spec.get("group_key"),
            "name": spec.get("group_name"),
        }
        if any(legacy.values()):
            refs.append(legacy)
        group_ids: list[str] = []
        seen: set[str] = set()
        for item in refs:
            group_id = str(item.get("id") or "").strip()
            group_key = str(item.get("key") or "").strip()
            group = self.get_skill_group(group_id) if group_id else None
            if group is None and group_key:
                group = self.get_skill_group_by_key(group_key)
            resolved_id = str((group or {}).get("id") or "").strip()
            if not resolved_id or resolved_id in seen:
                continue
            seen.add(resolved_id)
            group_ids.append(resolved_id)
        return group_ids

    def _legacy_skill_storage_source(self, row: dict[str, Any]) -> Path | None:
        storage_path = self._normalize_skill_storage_path(row.get("storage_path"))
        if storage_path:
            candidate = self._skill_storage_root.joinpath(*storage_path.split("/"))
            if candidate.is_dir():
                return candidate
        spec = json_loads(row.get("spec_json"), {})
        absolute_path = str(spec.get("deepagents_skill_filesystem_path") or "").strip()
        if absolute_path:
            candidate = Path(absolute_path).expanduser().resolve()
            if candidate.is_dir():
                return candidate
        legacy_directory = str(spec.get("deepagents_skill_directory") or "").strip()
        if legacy_directory:
            for base in (self._skill_storage_root / "library" / "catalog", self._skill_storage_root):
                candidate = base / legacy_directory
                if candidate.is_dir():
                    return candidate
        return None

    def _migrate_skill_storage_path(self, row: dict[str, Any], *, record_id: str, name: str) -> str:
        desired_relative = self._desired_skill_storage_path(record_id=record_id, name=name)
        normalized_current = self._normalize_skill_storage_path(row.get("storage_path"))
        source_dir = self._legacy_skill_storage_source(row)
        current_dir = self._skill_storage_root.joinpath(*normalized_current.split("/")) if normalized_current else None
        if current_dir is not None and current_dir.is_dir() and source_dir is None:
            source_dir = current_dir
        target_relative = desired_relative
        target_dir = self._skill_storage_root.joinpath(*target_relative.split("/"))
        if target_dir.is_dir():
            return target_relative
        if source_dir is not None:
            self._skill_storage_root.mkdir(parents=True, exist_ok=True)
            if target_dir.resolve() != source_dir.resolve():
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                shutil.copytree(source_dir, target_dir)
                try:
                    legacy_relative = source_dir.resolve().relative_to(self._skill_storage_root)
                    if legacy_relative.as_posix() != target_relative:
                        shutil.rmtree(source_dir, ignore_errors=True)
                except Exception:
                    pass
            return target_relative
        return target_relative

    def _migrate_skills_table(self) -> None:
        if not self._table_exists("skills"):
            return
        columns = {str(row["name"]) for row in self.fetch_all("PRAGMA table_info(skills)")}
        desired_columns = {"id", "name", "description", "storage_path", "created_at", "updated_at"}
        if columns == desired_columns:
            return
        legacy_rows = self.fetch_all("SELECT * FROM skills ORDER BY created_at ASC, id ASC")
        memberships: list[tuple[str, str]] = []
        self._connection.execute("DROP TABLE IF EXISTS skills__new")
        self._connection.execute(
            """
            CREATE TABLE skills__new (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                storage_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        seen_names: set[str] = set()
        for row in legacy_rows:
            record_id = str(row.get("id") or "").strip() or make_uuid7()
            base_name = trim_text(str(row.get("name") or "").strip(), limit=255) or record_id
            name = base_name
            suffix = 1
            while name in seen_names:
                suffix += 1
                name = trim_text(f"{base_name}-{suffix}", limit=255) or f"{record_id}-{suffix}"
            seen_names.add(name)
            storage_path = self._migrate_skill_storage_path(row, record_id=record_id, name=name)
            created_at = str(row.get("created_at") or utcnow_iso())
            updated_at = str(row.get("updated_at") or created_at)
            self._connection.execute(
                """
                INSERT INTO skills__new(id, name, description, storage_path, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record_id, name, str(row.get("description") or ""), storage_path, created_at, updated_at),
            )
            memberships.extend((record_id, group_id) for group_id in self._legacy_skill_group_ids(row))
        self._connection.execute("DROP TABLE skills")
        self._connection.execute("ALTER TABLE skills__new RENAME TO skills")
        self._connection.execute("DROP INDEX IF EXISTS idx_skills_key_version")
        self._connection.execute("DROP INDEX IF EXISTS idx_skills_name")
        self._connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_skills_name ON skills(name)")
        self._connection.execute("CREATE INDEX IF NOT EXISTS idx_skills_updated ON skills(updated_at DESC)")
        self._connection.execute("DELETE FROM skill_group_members")
        now = utcnow_iso()
        for skill_id, group_id in memberships:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO skill_group_members(skill_id, skill_group_id, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (skill_id, group_id, now, now),
            )

    def _normalize_skill_storage_layout(self) -> None:
        if not self._table_exists("skills"):
            return
        columns = {str(row["name"]) for row in self.fetch_all("PRAGMA table_info(skills)")}
        if "storage_path" not in columns or "name" not in columns:
            return
        now = utcnow_iso()
        for skill in self.fetch_all("SELECT id, name, storage_path FROM skills"):
            record_id = str(skill.get("id") or "").strip()
            name = str(skill.get("name") or "").strip()
            if not record_id or not name:
                continue
            desired_relative = self._desired_skill_storage_path(record_id=record_id, name=name)
            current_relative = self._normalize_skill_storage_path(skill.get("storage_path"))
            target_dir = self._skill_storage_root.joinpath(*desired_relative.split("/"))
            current_dir = self._skill_storage_root.joinpath(*current_relative.split("/")) if current_relative else None
            if current_relative == desired_relative and target_dir.is_dir():
                continue
            source_dir = current_dir if current_dir is not None and current_dir.is_dir() else None
            if source_dir is not None and source_dir.resolve() != target_dir.resolve():
                self._skill_storage_root.mkdir(parents=True, exist_ok=True)
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                shutil.copytree(source_dir, target_dir)
                try:
                    source_relative = source_dir.resolve().relative_to(self._skill_storage_root)
                    if source_relative.as_posix() != desired_relative:
                        shutil.rmtree(source_dir, ignore_errors=True)
                except Exception:
                    pass
            self._connection.execute(
                "UPDATE skills SET storage_path = ?, updated_at = ? WHERE id = ?",
                (desired_relative, now, record_id),
            )

    def _migrate_knowledge_bases_table(self) -> None:
        if not self._table_exists("knowledge_bases"):
            return
        columns = {str(row["name"]) for row in self.fetch_all("PRAGMA table_info(knowledge_bases)")}
        if "description" not in columns:
            return
        try:
            self._connection.execute("ALTER TABLE knowledge_bases DROP COLUMN description")
            return
        except sqlite3.OperationalError:
            pass

        now = utcnow_iso()
        self._connection.commit()
        foreign_keys = int((self.fetch_one("PRAGMA foreign_keys") or {}).get("foreign_keys", 1) or 1)
        if foreign_keys:
            self._connection.execute("PRAGMA foreign_keys=OFF")
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            self._connection.execute("DROP TABLE IF EXISTS knowledge_bases__new")
            self._connection.execute(
                """
                CREATE TABLE knowledge_bases__new (
                    id TEXT PRIMARY KEY,
                    key TEXT NOT NULL,
                    name TEXT NOT NULL,
                    config_json TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                f"""
                INSERT INTO knowledge_bases__new(id, key, name, config_json, status, created_at, updated_at)
                SELECT
                    id,
                    key,
                    name,
                    {"config_json" if "config_json" in columns else "'{}'"},
                    {"status" if "status" in columns else "'active'"},
                    {"created_at" if "created_at" in columns else f"'{now}'"},
                    {"updated_at" if "updated_at" in columns else f"'{now}'"}
                FROM knowledge_bases
                """
            )
            self._connection.execute("DROP TABLE knowledge_bases")
            self._connection.execute("ALTER TABLE knowledge_bases__new RENAME TO knowledge_bases")
            self._connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_bases_key ON knowledge_bases(key)")
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        finally:
            if foreign_keys:
                self._connection.execute("PRAGMA foreign_keys=ON")

    def _migrate_agent_templates_to_definitions(self) -> None:
        if not self._table_exists("agent_templates"):
            return
        templates = self.fetch_all("SELECT * FROM agent_templates ORDER BY created_at ASC, id ASC")
        if not templates:
            return
        for template in templates:
            template_id = str(template.get("id") or "").strip()
            if not template_id:
                continue
            existing = self.get_agent_definition(template_id)
            if existing is None:
                spec = self._migrated_agent_definition_spec_from_template(dict(template.get("spec_json") or {}))
                self.execute(
                    """
                    INSERT INTO agent_definitions(id, name, role, description, version, spec_json, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        template_id,
                        str(template.get("name") or ""),
                        trim_text(str(template.get("role") or "").strip(), limit=255) or "agent",
                        str(template.get("description") or ""),
                        str(template.get("version") or "v1"),
                        json_dumps(spec),
                        str(template.get("status") or "active"),
                        str(template.get("created_at") or utcnow_iso()),
                        str(template.get("updated_at") or utcnow_iso()),
                    ),
                )
        for team_definition in self.fetch_all("SELECT id, spec_json FROM team_definitions"):
            spec = json_loads(team_definition.get("spec_json"), {})
            if not isinstance(spec, dict):
                continue
            changed = self._migrate_team_definition_agent_template_refs(spec)
            if not changed:
                continue
            self.execute(
                "UPDATE team_definitions SET spec_json = ? WHERE id = ?",
                (json_dumps(spec), str(team_definition.get("id") or "")),
            )

    def _migrated_agent_definition_spec_from_template(self, spec: dict[str, Any]) -> dict[str, Any]:
        migrated = dict(spec or {})
        plugin_refs = [str(item).strip() for item in list(migrated.get("tool_plugin_refs") or migrated.get("plugin_refs") or []) if str(item).strip()]
        if plugin_refs:
            migrated["tool_plugin_refs"] = list(dict.fromkeys(plugin_refs))
        migrated.pop("plugin_refs", None)
        return migrated

    def _migrate_review_policy_specs(self) -> None:
        if not self._table_exists("review_policies"):
            return
        migrate_review_policies_in_connection(self._connection, commit=False)

    def _migrate_skill_groups(self) -> None:
        return

    def sync_skill_groups_from_skills(self) -> None:
        return

    def _migrate_skill_group_refs(self) -> None:
        return

    def _strip_legacy_skill_group_order(self) -> None:
        return

    def _drop_skill_group_sort_order_column(self) -> None:
        columns = {str(row["name"]) for row in self.fetch_all("PRAGMA table_info(skill_groups)")}
        if "sort_order" not in columns:
            return
        self._connection.execute("DROP INDEX IF EXISTS idx_skill_groups_sort")
        self._connection.execute("DROP TABLE IF EXISTS skill_groups__new")
        self._connection.execute(
            """
            CREATE TABLE skill_groups__new (
                id TEXT PRIMARY KEY,
                key TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            INSERT INTO skill_groups__new(id, key, name, description, created_at, updated_at)
            SELECT id, key, name, description, created_at, updated_at
            FROM skill_groups
            """
        )
        self._connection.execute("DROP TABLE skill_groups")
        self._connection.execute("ALTER TABLE skill_groups__new RENAME TO skill_groups")
        self._connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_skill_groups_key ON skill_groups(key)")

    def _migrate_team_definition_agent_template_refs(self, payload: Any) -> bool:
        changed = False
        if isinstance(payload, dict):
            template_ref = payload.pop("agent_template_ref", None)
            template_id = payload.pop("agent_template_id", None)
            reference = template_ref if template_ref is not None else template_id
            if reference is not None:
                payload["agent_definition_ref"] = reference
                payload["source_kind"] = "agent_definition"
                changed = True
            elif str(payload.get("source_kind") or "").strip() == "agent_template":
                payload["source_kind"] = "agent_definition"
                changed = True
            for value in payload.values():
                if self._migrate_team_definition_agent_template_refs(value):
                    changed = True
            return changed
        if isinstance(payload, list):
            for item in payload:
                if self._migrate_team_definition_agent_template_refs(item):
                    changed = True
        return changed

    def _ensure_column(self, table_name: str, column_name: str, definition: str) -> None:
        columns = {str(item.get("name") or "") for item in self.fetch_all(f"PRAGMA table_info({table_name})")}
        if column_name in columns:
            return
        self._connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")

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
            "runtime_tree_snapshot_json",
            "compiled_metadata_json",
            "value_json",
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

    def default_scope_ids(self) -> dict[str, str]:
        return {
            "workspace_id": str(self._defaults["workspace_id"]),
            "project_id": str(self._defaults["project_id"]),
        }

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

    def save_local_model(
        self,
        *,
        local_model_id: str | None,
        name: str,
        model_type: str,
        model_path: str,
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = local_model_id or make_uuid7()
        now = utcnow_iso()
        normalized_name = trim_text(name, limit=255)
        normalized_type = trim_text(model_type, limit=32)
        normalized_path = trim_text(model_path, limit=2048)
        if not normalized_name:
            raise ValueError("Local model name is required.")
        if not normalized_type:
            raise ValueError("Local model type is required.")
        if not normalized_path:
            raise ValueError("Local model path is required.")
        self.execute(
            """
            INSERT INTO local_models(id, name, model_type, model_path, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM local_models WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                model_type = excluded.model_type,
                model_path = excluded.model_path,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, normalized_name, normalized_type, normalized_path, status, record_id, now, now),
        )
        saved = self.get_local_model(record_id)
        assert saved is not None
        return saved

    def get_local_model(self, local_model_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM local_models WHERE id = ?", (local_model_id,)))

    def get_local_model_by_path(self, model_path: str) -> dict[str, Any] | None:
        normalized_path = trim_text(model_path, limit=2048)
        if not normalized_path:
            return None
        return self._deserialize(self.fetch_one("SELECT * FROM local_models WHERE model_path = ? ORDER BY updated_at DESC LIMIT 1", (normalized_path,)))

    def list_local_models(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM local_models ORDER BY updated_at DESC, created_at DESC"))

    def list_local_models_page(
        self,
        *,
        query: str | None = None,
        model_type: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        conditions: list[str] = []
        params: list[Any] = []
        keyword = str(query or "").strip().lower()
        if keyword:
            like = f"%{keyword}%"
            conditions.append("(LOWER(name) LIKE ? OR LOWER(model_type) LIKE ? OR LOWER(model_path) LIKE ?)")
            params.extend([like, like, like])
        selected_type = str(model_type or "").strip()
        if selected_type:
            conditions.append("model_type = ?")
            params.append(selected_type)
        where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        total = int(
            (self.fetch_one(f"SELECT COUNT(*) AS count FROM local_models{where_sql}", tuple(params)) or {}).get("count", 0) or 0
        )
        safe_offset = max(0, int(offset or 0))
        sql = f"SELECT * FROM local_models{where_sql} ORDER BY updated_at DESC, created_at DESC"
        query_params = list(params)
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            query_params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        return {
            "items": self._deserialize_many(self.fetch_all(sql, tuple(query_params))),
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def delete_local_model(self, local_model_id: str) -> dict[str, Any] | None:
        existing = self.get_local_model(local_model_id)
        if existing is None:
            return None
        self.execute("DELETE FROM local_models WHERE id = ?", (local_model_id,))
        return existing

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
        config: dict[str, Any] | None,
        install_path: str | None,
        secret: dict[str, Any] | None = None,
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = plugin_id or make_uuid7()
        normalized_name = trim_text(name, limit=255)
        normalized_install_path = str(install_path or "").strip() or None
        if not normalized_name:
            raise ValueError("Plugin name is required.")
        self.execute(
            """
            INSERT INTO plugins(id, name, install_path)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                install_path = excluded.install_path
            """,
            (
                record_id,
                normalized_name,
                normalized_install_path,
            ),
        )
        plugin = self.get_plugin(record_id)
        assert plugin is not None
        return plugin

    def get_plugin(self, plugin_id: str, *, include_secret: bool = False) -> dict[str, Any] | None:
        plugin = self._deserialize(self.fetch_one("SELECT * FROM plugins WHERE id = ?", (plugin_id,)))
        if plugin is None and str(plugin_id or "").startswith("catalog:"):
            plugin = self._catalog_plugin_record(key=str(plugin_id).split(":", 1)[-1], record_id=str(plugin_id))
        return self._attach_plugin_runtime_metadata(plugin, include_secret=include_secret)

    def get_plugin_by_key(self, key: str, *, include_secret: bool = False) -> dict[str, Any] | None:
        normalized_key = str(key or "").strip()
        if not normalized_key:
            return None
        for item in self.list_plugins(include_secret=include_secret):
            if str(item.get("key") or "").strip() == normalized_key:
                return item
        catalog = self._catalog_plugin_record(key=normalized_key)
        if catalog is not None:
            return self._attach_plugin_runtime_metadata(catalog, include_secret=include_secret)
        return None

    def list_plugins(self, *, include_secret: bool = False) -> list[dict[str, Any]]:
        return [
            self._attach_plugin_runtime_metadata(item, include_secret=include_secret)
            for item in self._deserialize_many(self.fetch_all("SELECT * FROM plugins ORDER BY id DESC"))
        ]

    def list_plugins_page(self, *, limit: int | None = None, offset: int = 0, include_secret: bool = False) -> dict[str, Any]:
        total = int((self.fetch_one("SELECT COUNT(*) AS count FROM plugins") or {}).get("count", 0) or 0)
        safe_offset = max(0, int(offset or 0))
        sql = "SELECT * FROM plugins ORDER BY id DESC"
        params: list[Any] = []
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        return {
            "items": [
                self._attach_plugin_runtime_metadata(item, include_secret=include_secret)
                for item in self._deserialize_many(self.fetch_all(sql, tuple(params)))
            ],
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def delete_plugin(self, plugin_id: str) -> dict[str, Any] | None:
        existing = self._deserialize(self.fetch_one("SELECT * FROM plugins WHERE id = ?", (plugin_id,)))
        if existing is None:
            return None
        self.execute("DELETE FROM plugins WHERE id = ?", (plugin_id,))
        return existing

    def _plugin_manifest_from_install_path(self, install_path: str | None) -> dict[str, Any]:
        raw_path = str(install_path or "").strip()
        if not raw_path or "://" in raw_path:
            return {}
        try:
            from aiteams.plugins.manifest import load_plugin_manifest

            root = Path(raw_path).expanduser().resolve()
            manifest_path = root / "plugin.yaml"
            if not manifest_path.exists():
                return {}
            return dict(load_plugin_manifest(root) or {})
        except Exception:
            return {}

    def _catalog_plugin_definition(self, *, key: str = "", name: str = "") -> dict[str, Any] | None:
        normalized_key = str(key or "").strip()
        normalized_name = str(name or "").strip()
        try:
            from aiteams.agent_center.defaults import default_plugins
        except Exception:
            return None
        for plugin in default_plugins():
            plugin_key = str(plugin.get("key") or "").strip()
            plugin_name = str(plugin.get("name") or "").strip()
            if normalized_key and plugin_key == normalized_key:
                return dict(plugin)
            if normalized_name and normalized_name in {plugin_name, plugin_key}:
                return dict(plugin)
        return None

    def _catalog_plugin_record(self, *, key: str = "", name: str = "", record_id: str | None = None) -> dict[str, Any] | None:
        definition = self._catalog_plugin_definition(key=key, name=name)
        if definition is None:
            return None
        plugin_key = str(definition.get("key") or "").strip()
        manifest = dict(definition.get("manifest") or {})
        description = str(definition.get("description") or manifest.get("description") or "").strip()
        return {
            "id": str(record_id or f"catalog:{plugin_key}"),
            "name": str(definition.get("name") or plugin_key),
            "install_path": None,
            "key": plugin_key,
            "version": str(definition.get("version") or "v1"),
            "plugin_type": str(definition.get("plugin_type") or "toolset"),
            "description": description,
            "manifest_json": {
                **manifest,
                "key": plugin_key,
                "name": str(definition.get("name") or plugin_key),
                "version": str(definition.get("version") or "v1"),
                "plugin_type": str(definition.get("plugin_type") or "toolset"),
                "description": str(manifest.get("description") or description),
            },
            "config_json": {},
            "status": "catalog",
        }

    def _attach_plugin_runtime_metadata(self, plugin: dict[str, Any] | None, *, include_secret: bool) -> dict[str, Any] | None:
        if plugin is None:
            return None
        item = dict(plugin)
        install_path = str(item.get("install_path") or "").strip()
        manifest = self._plugin_manifest_from_install_path(install_path)
        if not manifest:
            catalog_key = install_path.split("://", 1)[-1] if install_path.startswith("catalog://") else ""
            catalog = self._catalog_plugin_record(
                key=catalog_key,
                name=str(item.get("name") or ""),
                record_id=str(item.get("id") or ""),
            )
            if catalog is not None:
                manifest = dict(catalog.get("manifest_json") or {})
                item.setdefault("key", catalog.get("key"))
                item.setdefault("version", catalog.get("version"))
                item.setdefault("plugin_type", catalog.get("plugin_type"))
                item.setdefault("description", catalog.get("description"))
        key = str(manifest.get("key") or item.get("key") or slugify(item.get("name") or "", fallback="plugin")).strip()
        version = str(manifest.get("version") or item.get("version") or "v1").strip() or "v1"
        plugin_type = str(manifest.get("plugin_type") or item.get("plugin_type") or "toolset").strip() or "toolset"
        description = str(manifest.get("description") or item.get("description") or "").strip()
        item["key"] = key
        item["version"] = version
        item["plugin_type"] = plugin_type
        item["description"] = description
        item["manifest_json"] = manifest
        item["config_json"] = {}
        item["status"] = str(item.get("status") or ("installed" if install_path else "active"))
        item["has_secret"] = False
        item["secret_field_keys"] = []
        item["secret_field_paths"] = []
        if include_secret:
            item["secret_json"] = {}
        return item

    def _flatten_object_paths(self, payload: dict[str, Any], prefix: str = "") -> list[str]:
        paths: list[str] = []
        for key, value in dict(payload or {}).items():
            name = str(key).strip()
            if not name:
                continue
            path = f"{prefix}.{name}" if prefix else name
            if isinstance(value, dict):
                nested = self._flatten_object_paths(value, prefix=path)
                if nested:
                    paths.extend(nested)
                    continue
            paths.append(path)
        return paths

    def save_skill_group(
        self,
        *,
        skill_group_id: str | None,
        key: str,
        name: str,
        description: str,
    ) -> dict[str, Any]:
        record_id = skill_group_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO skill_groups(id, key, name, description, created_at, updated_at)
            VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM skill_groups WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                key = excluded.key,
                name = excluded.name,
                description = excluded.description,
                updated_at = excluded.updated_at
            """,
            (record_id, key, name, description, record_id, now, now),
        )
        saved = self.get_skill_group(record_id)
        assert saved is not None
        return saved

    def get_skill_group(self, skill_group_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM skill_groups WHERE id = ?", (skill_group_id,)))

    def get_skill_group_by_key(self, key: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM skill_groups WHERE key = ? ORDER BY updated_at DESC LIMIT 1", (key,)))

    def _skill_group_sort_key(self, item: dict[str, Any]) -> tuple[int, str, str]:
        return (
            1 if item.get("is_ungrouped") else 0,
            str(item.get("name") or "").lower(),
            str(item.get("key") or "").lower(),
        )

    def resolve_skill_groups(self, skill: dict[str, Any]) -> list[dict[str, Any]]:
        skill_id = str(skill.get("id") or "").strip()
        if not skill_id:
            return []
        rows = self.fetch_all(
            """
            SELECT sg.*
            FROM skill_groups AS sg
            INNER JOIN skill_group_members AS sgm
                ON sgm.skill_group_id = sg.id
            WHERE sgm.skill_id = ?
            ORDER BY LOWER(sg.name) ASC, LOWER(sg.key) ASC
            """,
            (skill_id,),
        )
        return [
            {
                **row,
                "is_ungrouped": False,
            }
            for row in self._deserialize_many(rows)
        ]

    def resolve_skill_group(self, skill: dict[str, Any]) -> dict[str, Any] | None:
        groups = self.resolve_skill_groups(skill)
        return groups[0] if groups else None

    def _skill_group_usage_counts(self) -> dict[str, int]:
        counts = {
            str(item.get("group_key") or ""): int(item.get("count") or 0)
            for item in self.fetch_all(
                """
                SELECT sg.key AS group_key, COUNT(sgm.skill_id) AS count
                FROM skill_groups AS sg
                LEFT JOIN skill_group_members AS sgm
                    ON sgm.skill_group_id = sg.id
                GROUP BY sg.id, sg.key
                """
            )
            if str(item.get("group_key") or "").strip()
        }
        counts["__ungrouped__"] = int(
            (
                self.fetch_one(
                    """
                    SELECT COUNT(*) AS count
                    FROM skills AS s
                    LEFT JOIN skill_group_members AS sgm
                        ON sgm.skill_id = s.id
                    WHERE sgm.skill_id IS NULL
                    """
                )
                or {}
            ).get("count", 0)
            or 0
        )
        return counts

    def save_skill(
        self,
        *,
        skill_id: str | None,
        name: str,
        description: str,
        storage_path: str,
    ) -> dict[str, Any]:
        record_id = skill_id or make_uuid7()
        now = utcnow_iso()
        normalized_name = trim_text(name, limit=255)
        normalized_storage_path = self._normalize_skill_storage_path(storage_path)
        if not normalized_name:
            raise ValueError("Skill name is required.")
        if not normalized_storage_path:
            raise ValueError("Skill storage_path is required.")
        existing_by_name = self.get_skill_by_name(normalized_name)
        if existing_by_name is not None and str(existing_by_name.get("id") or "") != record_id:
            raise ValueError(f"Skill name `{normalized_name}` already exists.")
        self.execute(
            """
            INSERT INTO skills(id, name, description, storage_path, created_at, updated_at)
            VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM skills WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                storage_path = excluded.storage_path,
                updated_at = excluded.updated_at
            """,
            (record_id, normalized_name, description, normalized_storage_path, record_id, now, now),
        )
        saved = self.get_skill(record_id)
        assert saved is not None
        return saved

    def get_skill(self, skill_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM skills WHERE id = ?", (skill_id,)))

    def get_skill_by_name(self, name: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM skills WHERE name = ? ORDER BY updated_at DESC LIMIT 1", (name,)))

    def get_skill_by_key(self, key: str) -> dict[str, Any] | None:
        return self.get_skill_by_name(key)

    def list_skills(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM skills ORDER BY updated_at DESC, created_at DESC"))

    def _skill_group_fields(self, skill: dict[str, Any]) -> tuple[str, str, None]:
        group = self.resolve_skill_group(skill) or {}
        return (
            str(group.get("key") or ""),
            str(group.get("name") or ""),
            None,
        )

    def list_skill_groups(self, *, include_ungrouped: bool = False) -> list[dict[str, Any]]:
        counts = self._skill_group_usage_counts()
        groups = [
            {
                **row,
                "count": int(counts.get(str(row.get("key") or ""), 0) or 0),
                "is_ungrouped": False,
            }
            for row in self._deserialize_many(
                self.fetch_all(
                    """
                    SELECT * FROM skill_groups
                    ORDER BY
                        LOWER(name) ASC,
                        LOWER(key) ASC
                    """
                )
            )
        ]
        if include_ungrouped:
            groups.append(
                {
                    "id": "",
                    "key": "__ungrouped__",
                    "name": "未分组",
                    "description": "",
                    "count": int(counts.get("__ungrouped__", 0) or 0),
                    "is_ungrouped": True,
                }
            )
        return sorted(groups, key=self._skill_group_sort_key)

    def list_skill_groups_page(self, *, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
        rows = self.list_skill_groups()
        total = len(rows)
        safe_offset = max(0, int(offset or 0))
        if limit is not None:
            safe_limit = max(1, int(limit))
            items = rows[safe_offset : safe_offset + safe_limit]
        else:
            safe_limit = total or 0
            items = rows[safe_offset:]
        return {
            "items": items,
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def sync_skill_group_assignments(self, *, skill_group: dict[str, Any], previous_key: str | None = None) -> None:
        del skill_group, previous_key
        return

    def replace_skill_group_memberships(self, skill_id: str, skill_group_ids: Sequence[str]) -> None:
        normalized_skill_id = str(skill_id or "").strip()
        if not normalized_skill_id:
            return
        normalized_group_ids: list[str] = []
        seen: set[str] = set()
        for item in skill_group_ids:
            group_id = str(item or "").strip()
            if not group_id or group_id in seen:
                continue
            seen.add(group_id)
            normalized_group_ids.append(group_id)
        now = utcnow_iso()
        self.execute("DELETE FROM skill_group_members WHERE skill_id = ?", (normalized_skill_id,))
        for group_id in normalized_group_ids:
            self.execute(
                """
                INSERT OR IGNORE INTO skill_group_members(skill_id, skill_group_id, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (normalized_skill_id, group_id, now, now),
            )

    def set_skill_group_members(self, *, skill_group: dict[str, Any], skill_ids: Sequence[str]) -> None:
        group_id = str(skill_group.get("id") or "").strip()
        if not group_id:
            return
        normalized_skill_ids: list[str] = []
        seen: set[str] = set()
        for item in skill_ids:
            skill_id = str(item or "").strip()
            if not skill_id or skill_id in seen:
                continue
            seen.add(skill_id)
            normalized_skill_ids.append(skill_id)
        now = utcnow_iso()
        self.execute("DELETE FROM skill_group_members WHERE skill_group_id = ?", (group_id,))
        for skill_id in normalized_skill_ids:
            self.execute(
                """
                INSERT OR IGNORE INTO skill_group_members(skill_id, skill_group_id, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (skill_id, group_id, now, now),
            )

    def delete_skill_group(self, skill_group_id: str) -> dict[str, Any] | None:
        existing = self.get_skill_group(skill_group_id)
        if existing is None:
            return None
        self.execute("DELETE FROM skill_groups WHERE id = ?", (skill_group_id,))
        return existing

    def list_skills_page(
        self,
        *,
        query: str | None = None,
        group_key: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        rows = self.list_skills()
        groups = self.list_skill_groups(include_ungrouped=True)
        keyword = str(query or "").strip().lower()
        selected_group = str(group_key or "").strip()

        filtered: list[dict[str, Any]] = []
        for skill in rows:
            current_groups = self.resolve_skill_groups(skill)
            current_group_keys = [str(item.get("key") or "").strip() for item in current_groups if str(item.get("key") or "").strip()]
            current_group_names = [str(item.get("name") or "").strip() for item in current_groups if str(item.get("name") or "").strip()]
            if selected_group:
                if selected_group == "__ungrouped__":
                    if current_group_keys:
                        continue
                elif selected_group not in current_group_keys:
                    continue
            if keyword:
                haystack = "\n".join(
                    [
                        str(skill.get("name") or ""),
                        str(skill.get("description") or ""),
                        str(skill.get("storage_path") or ""),
                        "\n".join(current_group_keys),
                        "\n".join(current_group_names),
                    ]
                ).lower()
                if keyword not in haystack:
                    continue
            filtered.append(skill)

        total = len(filtered)
        safe_offset = max(0, int(offset or 0))
        if limit is not None:
            safe_limit = max(1, int(limit))
            items = filtered[safe_offset : safe_offset + safe_limit]
        else:
            safe_limit = total or 0
            items = filtered[safe_offset:]
        return {
            "items": items,
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
            "groups": groups,
        }

    def delete_skill(self, skill_id: str) -> dict[str, Any] | None:
        existing = self.get_skill(skill_id)
        if existing is None:
            return None
        self.execute("DELETE FROM skill_group_members WHERE skill_id = ?", (skill_id,))
        self.execute("DELETE FROM skills WHERE id = ?", (skill_id,))
        return existing

    def save_static_memory(
        self,
        *,
        static_memory_id: str | None,
        key: str,
        name: str,
        description: str,
        version: str,
        spec: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = static_memory_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO static_memories(id, key, name, description, version, spec_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM static_memories WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                key = excluded.key,
                name = excluded.name,
                description = excluded.description,
                version = excluded.version,
                spec_json = excluded.spec_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, key, name, description, version, json_dumps(spec), status, record_id, now, now),
        )
        saved = self.get_static_memory(record_id)
        assert saved is not None
        return saved

    def get_static_memory(self, static_memory_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM static_memories WHERE id = ?", (static_memory_id,)))

    def get_static_memory_by_key(self, key: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM static_memories WHERE key = ? ORDER BY updated_at DESC LIMIT 1", (key,)))

    def list_static_memories(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM static_memories ORDER BY updated_at DESC"))

    def list_static_memories_page(self, *, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
        total = int((self.fetch_one("SELECT COUNT(*) AS count FROM static_memories") or {}).get("count", 0) or 0)
        safe_offset = max(0, int(offset or 0))
        sql = "SELECT * FROM static_memories ORDER BY updated_at DESC"
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

    def delete_static_memory(self, static_memory_id: str) -> dict[str, Any] | None:
        existing = self.get_static_memory(static_memory_id)
        if existing is None:
            return None
        self.execute("DELETE FROM static_memories WHERE id = ?", (static_memory_id,))
        return existing

    def save_knowledge_base(
        self,
        *,
        knowledge_base_id: str | None,
        key: str,
        name: str,
        config: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = knowledge_base_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO knowledge_bases(id, key, name, config_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM knowledge_bases WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                key = excluded.key,
                name = excluded.name,
                config_json = excluded.config_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, key, name, json_dumps(config), status, record_id, now, now),
        )
        saved = self.get_knowledge_base(record_id)
        assert saved is not None
        return saved

    def get_knowledge_base(self, knowledge_base_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM knowledge_bases WHERE id = ?", (knowledge_base_id,)))

    def get_knowledge_base_by_key(self, key: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM knowledge_bases WHERE key = ? ORDER BY updated_at DESC LIMIT 1", (key,)))

    def list_knowledge_bases(self) -> list[dict[str, Any]]:
        return self.list_knowledge_bases_page()["items"]

    def list_knowledge_bases_page(self, *, limit: int | None = None, offset: int = 0, query: str | None = None) -> dict[str, Any]:
        safe_offset = max(0, int(offset or 0))
        query_text = str(query or "").strip()
        where = ["1 = 1"]
        params: list[Any] = []
        if query_text:
            like = f"%{query_text}%"
            where.append("(kb.name LIKE ? OR kb.id LIKE ?)")
            params.extend([like, like])
        where_sql = " AND ".join(where)
        total = int(
            (
                self.fetch_one(
                    f"""
                    SELECT COUNT(*) AS count
                    FROM knowledge_bases kb
                    WHERE {where_sql}
                    """,
                    tuple(params),
                )
                or {}
            ).get("count", 0)
            or 0
        )
        sql = f"""
            SELECT
                kb.*,
                COUNT(d.id) AS document_count
            FROM knowledge_bases kb
            LEFT JOIN knowledge_documents d
                ON d.knowledge_base_id = kb.id
                AND d.status = 'active'
                AND COALESCE(d.document_status, 'not_embedded') != 'removed'
            WHERE {where_sql}
            GROUP BY kb.id
            ORDER BY kb.updated_at DESC, kb.created_at DESC
        """
        page_params = list(params)
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            page_params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        return {
            "items": self._deserialize_many(self.fetch_all(sql, tuple(page_params))),
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def touch_knowledge_base(self, knowledge_base_id: str) -> dict[str, Any] | None:
        existing = self.get_knowledge_base(knowledge_base_id)
        if existing is None:
            return None
        self.execute("UPDATE knowledge_bases SET updated_at = ? WHERE id = ?", (utcnow_iso(), knowledge_base_id))
        return self.get_knowledge_base(knowledge_base_id)

    def delete_knowledge_base(self, knowledge_base_id: str) -> dict[str, Any] | None:
        existing = self.get_knowledge_base(knowledge_base_id)
        if existing is None:
            return None
        self.execute("DELETE FROM knowledge_bases WHERE id = ?", (knowledge_base_id,))
        return existing

    def save_knowledge_embedding_job(
        self,
        *,
        job_id: str | None,
        knowledge_base_id: str,
        action: str,
        status: str = "pending",
        stage: str = "queued",
        total_documents: int = 0,
        processed_documents: int = 0,
        completed_documents: int = 0,
        failed_documents: int = 0,
        total_chunks_estimated: int = 0,
        embedded_chunks_completed: int = 0,
        current_document_id: str | None = None,
        current_document_title: str | None = None,
        message: str | None = None,
        error_text: str | None = None,
        result: dict[str, Any] | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
    ) -> dict[str, Any]:
        record_id = job_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO knowledge_embedding_jobs(
                id,
                knowledge_base_id,
                action,
                status,
                stage,
                total_documents,
                processed_documents,
                completed_documents,
                failed_documents,
                total_chunks_estimated,
                embedded_chunks_completed,
                current_document_id,
                current_document_title,
                message,
                error_text,
                result_json,
                created_at,
                started_at,
                finished_at,
                updated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                COALESCE((SELECT created_at FROM knowledge_embedding_jobs WHERE id = ?), ?),
                COALESCE(?, (SELECT started_at FROM knowledge_embedding_jobs WHERE id = ?)),
                ?,
                ?
            )
            ON CONFLICT(id) DO UPDATE SET
                knowledge_base_id = excluded.knowledge_base_id,
                action = excluded.action,
                status = excluded.status,
                stage = excluded.stage,
                total_documents = excluded.total_documents,
                processed_documents = excluded.processed_documents,
                completed_documents = excluded.completed_documents,
                failed_documents = excluded.failed_documents,
                total_chunks_estimated = excluded.total_chunks_estimated,
                embedded_chunks_completed = excluded.embedded_chunks_completed,
                current_document_id = excluded.current_document_id,
                current_document_title = excluded.current_document_title,
                message = excluded.message,
                error_text = excluded.error_text,
                result_json = excluded.result_json,
                started_at = COALESCE(excluded.started_at, knowledge_embedding_jobs.started_at),
                finished_at = excluded.finished_at,
                updated_at = excluded.updated_at
            """,
            (
                record_id,
                knowledge_base_id,
                action,
                status,
                stage,
                int(total_documents or 0),
                int(processed_documents or 0),
                int(completed_documents or 0),
                int(failed_documents or 0),
                int(total_chunks_estimated or 0),
                int(embedded_chunks_completed or 0),
                current_document_id,
                current_document_title,
                message,
                error_text,
                json_dumps(result or {}),
                record_id,
                now,
                started_at,
                record_id,
                finished_at,
                now,
            ),
        )
        saved = self.get_knowledge_embedding_job(record_id)
        assert saved is not None
        return saved

    def get_knowledge_embedding_job(self, job_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM knowledge_embedding_jobs WHERE id = ?", (job_id,)))

    def get_active_knowledge_embedding_job(self, knowledge_base_id: str) -> dict[str, Any] | None:
        return self._deserialize(
            self.fetch_one(
                """
                SELECT *
                FROM knowledge_embedding_jobs
                WHERE knowledge_base_id = ?
                  AND status IN ('pending', 'running')
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """,
                (knowledge_base_id,),
            )
        )

    def get_latest_knowledge_embedding_job(self, knowledge_base_id: str) -> dict[str, Any] | None:
        return self._deserialize(
            self.fetch_one(
                """
                SELECT *
                FROM knowledge_embedding_jobs
                WHERE knowledge_base_id = ?
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """,
                (knowledge_base_id,),
            )
        )

    def fail_active_knowledge_embedding_jobs(self, message: str) -> None:
        now = utcnow_iso()
        self.execute(
            """
            UPDATE knowledge_embedding_jobs
            SET
                status = 'error',
                stage = 'interrupted',
                message = ?,
                error_text = ?,
                finished_at = COALESCE(finished_at, ?),
                updated_at = ?
            WHERE status IN ('pending', 'running')
            """,
            (message, message, now, now),
        )

    def save_knowledge_file_blob(
        self,
        *,
        blob_id: str | None,
        xxh128: str,
        storage_name: str,
        storage_relpath: str,
        byte_size: int,
        mime_type: str | None = None,
        ext_hint: str | None = None,
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = blob_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO knowledge_file_blobs(
                id, xxh128, storage_name, storage_relpath, byte_size, mime_type, ext_hint, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM knowledge_file_blobs WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                xxh128 = excluded.xxh128,
                storage_name = excluded.storage_name,
                storage_relpath = excluded.storage_relpath,
                byte_size = excluded.byte_size,
                mime_type = excluded.mime_type,
                ext_hint = excluded.ext_hint,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, xxh128, storage_name, storage_relpath, int(byte_size or 0), mime_type, ext_hint, status, record_id, now, now),
        )
        saved = self.get_knowledge_file_blob(record_id)
        assert saved is not None
        return saved

    def get_knowledge_file_blob(self, blob_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM knowledge_file_blobs WHERE id = ?", (blob_id,)))

    def get_knowledge_file_blob_by_xxh128(self, xxh128: str) -> dict[str, Any] | None:
        return self._deserialize(
            self.fetch_one(
                "SELECT * FROM knowledge_file_blobs WHERE xxh128 = ? ORDER BY updated_at DESC LIMIT 1",
                (str(xxh128 or "").strip(),),
            )
        )

    def list_knowledge_file_blobs(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM knowledge_file_blobs ORDER BY updated_at DESC"))

    def delete_knowledge_file_blob(self, blob_id: str) -> dict[str, Any] | None:
        existing = self.get_knowledge_file_blob(blob_id)
        if existing is None:
            return None
        self.execute("DELETE FROM knowledge_file_blobs WHERE id = ?", (blob_id,))
        return existing

    def save_knowledge_file_alias(
        self,
        *,
        alias_id: str | None,
        blob_id: str,
        filename: str,
        suffix: str,
    ) -> dict[str, Any]:
        record_id = alias_id or make_uuid7()
        now = utcnow_iso()
        normalized_filename = str(filename or "").strip().replace("\\", "/").lower()
        self.execute(
            """
            INSERT INTO knowledge_file_aliases(id, blob_id, filename, normalized_filename, suffix, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM knowledge_file_aliases WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                blob_id = excluded.blob_id,
                filename = excluded.filename,
                normalized_filename = excluded.normalized_filename,
                suffix = excluded.suffix,
                updated_at = excluded.updated_at
            """,
            (record_id, blob_id, filename, normalized_filename, suffix, record_id, now, now),
        )
        saved = self.get_knowledge_file_alias(record_id)
        assert saved is not None
        return saved

    def get_knowledge_file_alias(self, alias_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM knowledge_file_aliases WHERE id = ?", (alias_id,)))

    def get_knowledge_file_alias_by_blob_filename(self, *, blob_id: str, filename: str) -> dict[str, Any] | None:
        normalized_filename = str(filename or "").strip().replace("\\", "/").lower()
        return self._deserialize(
            self.fetch_one(
                """
                SELECT *
                FROM knowledge_file_aliases
                WHERE blob_id = ? AND normalized_filename = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (blob_id, normalized_filename),
            )
        )

    def list_knowledge_file_aliases(self, *, blob_id: str | None = None) -> list[dict[str, Any]]:
        if blob_id:
            return self._deserialize_many(
                self.fetch_all(
                    "SELECT * FROM knowledge_file_aliases WHERE blob_id = ? ORDER BY updated_at DESC, created_at DESC",
                    (blob_id,),
                )
            )
        return self._deserialize_many(self.fetch_all("SELECT * FROM knowledge_file_aliases ORDER BY updated_at DESC, created_at DESC"))

    def save_knowledge_document(
        self,
        *,
        knowledge_document_id: str | None,
        knowledge_base_id: str,
        pool_document_id: str | None = None,
        blob_id: str | None = None,
        alias_id: str | None = None,
        key: str,
        title: str,
        source_path: str | None,
        content_text: str,
        document_status: str | None = None,
        sync_status: str = "idle",
        last_error: str | None = None,
        embedded_at: str | None = None,
        removed_at: str | None = None,
        metadata: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = knowledge_document_id or make_uuid7()
        now = utcnow_iso()
        normalized_document_status = str(document_status or metadata.get("embedding_status") or "not_embedded").strip().lower() or "not_embedded"
        self.execute(
            """
            INSERT INTO knowledge_documents(
                id, knowledge_base_id, pool_document_id, blob_id, alias_id, key, title, source_path, content_text,
                document_status, sync_status, last_error, embedded_at, removed_at, metadata_json, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM knowledge_documents WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                knowledge_base_id = excluded.knowledge_base_id,
                pool_document_id = excluded.pool_document_id,
                blob_id = excluded.blob_id,
                alias_id = excluded.alias_id,
                key = excluded.key,
                title = excluded.title,
                source_path = excluded.source_path,
                content_text = excluded.content_text,
                document_status = excluded.document_status,
                sync_status = excluded.sync_status,
                last_error = excluded.last_error,
                embedded_at = excluded.embedded_at,
                removed_at = excluded.removed_at,
                metadata_json = excluded.metadata_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (
                record_id,
                knowledge_base_id,
                pool_document_id,
                blob_id,
                alias_id,
                key,
                title,
                source_path,
                content_text,
                normalized_document_status,
                sync_status,
                last_error,
                embedded_at,
                removed_at,
                json_dumps(metadata),
                status,
                record_id,
                now,
                now,
            ),
        )
        saved = self.get_knowledge_document(record_id)
        assert saved is not None
        return saved

    def get_knowledge_document(self, knowledge_document_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM knowledge_documents WHERE id = ?", (knowledge_document_id,)))

    def get_knowledge_document_by_key(self, *, knowledge_base_id: str, key: str) -> dict[str, Any] | None:
        return self._deserialize(
            self.fetch_one(
                """
                SELECT *
                FROM knowledge_documents
                WHERE knowledge_base_id = ? AND key = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (knowledge_base_id, key),
            )
        )

    def get_knowledge_document_by_blob(self, *, knowledge_base_id: str, blob_id: str) -> dict[str, Any] | None:
        return self._deserialize(
            self.fetch_one(
                """
                SELECT *
                FROM knowledge_documents
                WHERE knowledge_base_id = ? AND blob_id = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (knowledge_base_id, blob_id),
            )
        )

    def list_knowledge_documents(
        self,
        *,
        knowledge_base_id: str | None = None,
        include_removed: bool = False,
        document_statuses: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if knowledge_base_id:
            clauses.append("knowledge_base_id = ?")
            params.append(knowledge_base_id)
        if document_statuses:
            normalized = [str(item).strip().lower() for item in document_statuses if str(item).strip()]
            if normalized:
                placeholders = ", ".join("?" for _ in normalized)
                clauses.append(f"COALESCE(document_status, 'not_embedded') IN ({placeholders})")
                params.extend(normalized)
        elif not include_removed:
            clauses.append("COALESCE(document_status, 'not_embedded') != 'removed'")
        sql = "SELECT * FROM knowledge_documents"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        sql += " ORDER BY updated_at DESC, created_at DESC"
        return self._deserialize_many(self.fetch_all(sql, tuple(params)))

    def list_knowledge_documents_page(
        self,
        *,
        knowledge_base_id: str,
        limit: int | None = None,
        offset: int = 0,
        query: str | None = None,
        embedding_status: str | None = None,
        include_removed: bool = False,
    ) -> dict[str, Any]:
        safe_offset = max(0, int(offset or 0))
        keyword = str(query or "").strip().lower()
        selected_status = str(embedding_status or "").strip().lower()
        clauses = ["knowledge_base_id = ?"]
        params: list[Any] = [knowledge_base_id]
        if selected_status and selected_status != "all":
            clauses.append("COALESCE(document_status, 'not_embedded') = ?")
            params.append(selected_status)
        elif not include_removed:
            clauses.append("COALESCE(document_status, 'not_embedded') != 'removed'")
        if keyword:
            like = f"%{keyword}%"
            clauses.append("(LOWER(title) LIKE ? OR LOWER(COALESCE(source_path, '')) LIKE ? OR LOWER(COALESCE(content_text, '')) LIKE ?)")
            params.extend([like, like, like])
        where_sql = " AND ".join(clauses)
        total = int(
            (
                self.fetch_one(
                    f"SELECT COUNT(*) AS count FROM knowledge_documents WHERE {where_sql}",
                    tuple(params),
                )
                or {}
            ).get("count", 0)
            or 0
        )
        sql = f"SELECT * FROM knowledge_documents WHERE {where_sql} ORDER BY updated_at DESC, created_at DESC"
        page_params = list(params)
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            page_params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        return {
            "items": self._deserialize_many(self.fetch_all(sql, tuple(page_params))),
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def delete_knowledge_document(self, knowledge_document_id: str) -> dict[str, Any] | None:
        existing = self.get_knowledge_document(knowledge_document_id)
        if existing is None:
            return None
        self.execute("DELETE FROM knowledge_documents WHERE id = ?", (knowledge_document_id,))
        return existing

    def save_knowledge_pool_document(
        self,
        *,
        knowledge_pool_document_id: str | None,
        knowledge_base_id: str,
        blob_id: str,
        alias_id: str,
        key: str,
        title: str,
        source_path: str | None,
        content_text: str,
        upload_method: str = "http",
        metadata: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = knowledge_pool_document_id or make_uuid7()
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO knowledge_pool_documents(
                id, knowledge_base_id, blob_id, alias_id, key, title, source_path, content_text, upload_method, metadata_json, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM knowledge_pool_documents WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                knowledge_base_id = excluded.knowledge_base_id,
                blob_id = excluded.blob_id,
                alias_id = excluded.alias_id,
                key = excluded.key,
                title = excluded.title,
                source_path = excluded.source_path,
                content_text = excluded.content_text,
                upload_method = excluded.upload_method,
                metadata_json = excluded.metadata_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (
                record_id,
                knowledge_base_id,
                blob_id,
                alias_id,
                key,
                title,
                source_path,
                content_text,
                upload_method,
                json_dumps(metadata),
                status,
                record_id,
                now,
                now,
            ),
        )
        saved = self.get_knowledge_pool_document(record_id)
        assert saved is not None
        return saved

    def get_knowledge_pool_document(self, knowledge_pool_document_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM knowledge_pool_documents WHERE id = ?", (knowledge_pool_document_id,)))

    def get_knowledge_pool_document_by_key(self, key: str, *, knowledge_base_id: str | None = None) -> dict[str, Any] | None:
        clauses = ["key = ?"]
        params: list[Any] = [key]
        if knowledge_base_id:
            clauses.append("knowledge_base_id = ?")
            params.append(knowledge_base_id)
        return self._deserialize(
            self.fetch_one(
                """
                SELECT *
                FROM knowledge_pool_documents
                WHERE """ + " AND ".join(clauses) + """
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                tuple(params),
            )
        )

    def get_knowledge_pool_document_by_blob(self, *, knowledge_base_id: str, blob_id: str) -> dict[str, Any] | None:
        return self._deserialize(
            self.fetch_one(
                """
                SELECT *
                FROM knowledge_pool_documents
                WHERE knowledge_base_id = ? AND blob_id = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (knowledge_base_id, blob_id),
            )
        )

    def list_knowledge_pool_documents(self, *, knowledge_base_id: str | None = None) -> list[dict[str, Any]]:
        if knowledge_base_id:
            return self._deserialize_many(
                self.fetch_all(
                    "SELECT * FROM knowledge_pool_documents WHERE knowledge_base_id = ? ORDER BY updated_at DESC, created_at DESC",
                    (knowledge_base_id,),
                )
            )
        return self._deserialize_many(self.fetch_all("SELECT * FROM knowledge_pool_documents ORDER BY updated_at DESC, created_at DESC"))

    def delete_knowledge_pool_document(self, knowledge_pool_document_id: str) -> dict[str, Any] | None:
        existing = self.get_knowledge_pool_document(knowledge_pool_document_id)
        if existing is None:
            return None
        self.execute("DELETE FROM knowledge_pool_documents WHERE id = ?", (knowledge_pool_document_id,))
        return existing

    def search_knowledge_documents(
        self,
        *,
        query: str,
        knowledge_base_ids: list[str] | None = None,
        knowledge_base_keys: list[str] | None = None,
        limit: int = 8,
    ) -> list[dict[str, Any]]:
        kb_ids = {str(item).strip() for item in list(knowledge_base_ids or []) if str(item).strip()}
        for key in list(knowledge_base_keys or []):
            record = self.get_knowledge_base_by_key(str(key))
            if record is not None:
                kb_ids.add(str(record["id"]))
        clauses = ["d.status = ?", "kb.status = ?", "COALESCE(d.document_status, 'not_embedded') = ?"]
        params: list[Any] = ["active", "active"]
        params.append("embedded")
        if kb_ids:
            placeholders = ", ".join("?" for _ in kb_ids)
            clauses.append(f"d.knowledge_base_id IN ({placeholders})")
            params.extend(sorted(kb_ids))
        sql = """
        SELECT
            d.*,
            kb.key AS knowledge_base_key,
            kb.name AS knowledge_base_name
        FROM knowledge_documents d
        JOIN knowledge_bases kb ON kb.id = d.knowledge_base_id
        """
        sql += f" WHERE {' AND '.join(clauses)} ORDER BY d.updated_at DESC LIMIT 256"
        rows = self._deserialize_many(self.fetch_all(sql, tuple(params)))
        query_value = str(query or "").strip().lower()
        query_terms = [term.lower() for term in SEARCH_TOKEN_RE.findall(query_value)]
        scored: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            title = str(row.get("title") or "")
            body = str(row.get("content_text") or "")
            title_lower = title.lower()
            body_lower = body.lower()
            score = 0.0
            if query_value:
                if query_value in title_lower:
                    score += 12.0
                if query_value in body_lower:
                    score += 6.0
                for term in query_terms:
                    score += 4.0 * title_lower.count(term)
                    score += 1.0 * body_lower.count(term)
                if score <= 0:
                    continue
            else:
                score = 0.1
            snippet = self._knowledge_snippet(body or title, query_terms or ([query_value] if query_value else []))
            scored.append(
                (
                    score,
                    {
                        "id": row.get("id"),
                        "knowledge_base_id": row.get("knowledge_base_id"),
                        "knowledge_base_key": row.get("knowledge_base_key"),
                        "knowledge_base_name": row.get("knowledge_base_name"),
                        "key": row.get("key"),
                        "title": title,
                        "source_path": row.get("source_path"),
                        "metadata_json": dict(row.get("metadata_json") or {}),
                        "score": score,
                        "snippet": snippet,
                        "content_text": trim_text(body, limit=1200),
                    },
                )
            )
        scored.sort(key=lambda item: (item[0], str(item[1].get("title") or "")), reverse=True)
        return [item for _, item in scored[: max(1, limit)]]

    def _knowledge_snippet(self, content: str, terms: list[str]) -> str:
        text = " ".join(str(content or "").split())
        if not text:
            return ""
        lower = text.lower()
        start = 0
        for term in terms:
            term_value = str(term).strip().lower()
            if not term_value:
                continue
            index = lower.find(term_value)
            if index >= 0:
                start = max(0, index - 72)
                break
        snippet = text[start : start + 320]
        return trim_text(snippet, limit=320)

    def save_review_policy(
        self,
        *,
        review_policy_id: str | None,
        key: str | None,
        name: str,
        description: str | None,
        version: str,
        spec: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        existing = self.get_review_policy(review_policy_id) if review_policy_id else None
        record_id = review_policy_id or make_uuid7()
        stored_key = str(key or "").strip() or str((existing or {}).get("key") or "").strip() or record_id
        stored_description = description if description is not None else str((existing or {}).get("description") or "")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO review_policies(id, key, name, description, version, spec_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM review_policies WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                key = excluded.key,
                name = excluded.name,
                description = excluded.description,
                version = excluded.version,
                spec_json = excluded.spec_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, stored_key, name, stored_description, version, json_dumps(spec), status, record_id, now, now),
        )
        saved = self.get_review_policy(record_id)
        assert saved is not None
        return saved

    def get_review_policy(self, review_policy_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM review_policies WHERE id = ?", (review_policy_id,)))

    def get_review_policy_by_key(self, key: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM review_policies WHERE key = ? ORDER BY updated_at DESC LIMIT 1", (key,)))

    def list_review_policies(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM review_policies ORDER BY updated_at DESC"))

    def list_review_policies_page(self, *, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
        total = int((self.fetch_one("SELECT COUNT(*) AS count FROM review_policies") or {}).get("count", 0) or 0)
        page_limit = max(1, limit or 10)
        page_offset = max(0, offset)
        items = self._deserialize_many(
            self.fetch_all(
                "SELECT * FROM review_policies ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (page_limit, page_offset),
            )
        )
        return {
            "items": items,
            "total": total,
            "limit": page_limit,
            "offset": page_offset,
        }

    def delete_review_policy(self, review_policy_id: str) -> dict[str, Any] | None:
        existing = self.get_review_policy(review_policy_id)
        if existing is None:
            return None
        self.execute("DELETE FROM review_policies WHERE id = ?", (review_policy_id,))
        return existing

    def save_agent_definition(
        self,
        *,
        agent_definition_id: str | None,
        name: str,
        role: str | None,
        description: str,
        version: str,
        spec: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = agent_definition_id or make_uuid7()
        role_value = trim_text(str(role or "").strip(), limit=255) or "agent"
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO agent_definitions(id, name, role, description, version, spec_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM agent_definitions WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                role = excluded.role,
                description = excluded.description,
                version = excluded.version,
                spec_json = excluded.spec_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, name, role_value, description, version, json_dumps(spec), status, record_id, now, now),
        )
        saved = self.get_agent_definition(record_id)
        assert saved is not None
        return saved

    def get_agent_definition(self, agent_definition_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM agent_definitions WHERE id = ?", (agent_definition_id,)))

    def list_agent_definitions_page(self, *, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
        total = int((self.fetch_one("SELECT COUNT(*) AS count FROM agent_definitions") or {}).get("count", 0) or 0)
        safe_offset = max(0, int(offset or 0))
        sql = "SELECT * FROM agent_definitions ORDER BY updated_at DESC"
        params: list[Any] = []
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        rows = self._deserialize_many(self.fetch_all(sql, tuple(params)))
        return {
            "items": rows,
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def list_agent_definitions(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM agent_definitions ORDER BY updated_at DESC"))

    def delete_agent_definition(self, agent_definition_id: str) -> dict[str, Any] | None:
        existing = self.get_agent_definition(agent_definition_id)
        if existing is None:
            return None
        self.execute("DELETE FROM agent_definitions WHERE id = ?", (agent_definition_id,))
        return existing

    def save_team_definition(
        self,
        *,
        team_definition_id: str | None,
        key: str | None,
        name: str,
        description: str,
        version: str,
        spec: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = team_definition_id or make_uuid7()
        record_key = str(key or f"team.{record_id}")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO team_definitions(id, key, name, description, version, spec_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM team_definitions WHERE id = ?), ?), ?)
            ON CONFLICT(id) DO UPDATE SET
                key = excluded.key,
                name = excluded.name,
                description = excluded.description,
                version = excluded.version,
                spec_json = excluded.spec_json,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (record_id, record_key, name, description, version, json_dumps(spec), status, record_id, now, now),
        )
        saved = self.get_team_definition(record_id)
        assert saved is not None
        return saved

    def get_team_definition(self, team_definition_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM team_definitions WHERE id = ?", (team_definition_id,)))

    def get_team_definition_by_key(self, key: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM team_definitions WHERE key = ? ORDER BY updated_at DESC LIMIT 1", (key,)))

    def list_team_definitions(self) -> list[dict[str, Any]]:
        return self._deserialize_many(self.fetch_all("SELECT * FROM team_definitions ORDER BY updated_at DESC"))

    def list_team_definitions_page(self, *, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
        total = int((self.fetch_one("SELECT COUNT(*) AS count FROM team_definitions") or {}).get("count", 0) or 0)
        safe_offset = max(0, int(offset or 0))
        sql = "SELECT * FROM team_definitions ORDER BY updated_at DESC"
        params: list[Any] = []
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        rows = self._deserialize_many(self.fetch_all(sql, tuple(params)))
        return {
            "items": rows,
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def delete_team_definition(self, team_definition_id: str) -> dict[str, Any] | None:
        existing = self.get_team_definition(team_definition_id)
        if existing is None:
            return None
        self.execute("DELETE FROM team_definitions WHERE id = ?", (team_definition_id,))
        return existing

    def save_platform_setting(self, setting_key: str, value: dict[str, Any]) -> dict[str, Any]:
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO platform_settings(setting_key, value_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET
                value_json = excluded.value_json,
                updated_at = excluded.updated_at
            """,
            (setting_key, json_dumps(value), now),
        )
        record = self.get_platform_setting_record(setting_key)
        assert record is not None
        return record

    def get_platform_setting_record(self, setting_key: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM platform_settings WHERE setting_key = ?", (setting_key,)))

    def get_platform_setting(self, setting_key: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
        record = self.get_platform_setting_record(setting_key)
        if record is None:
            return dict(default or {})
        return dict(record.get("value_json") or {})

    def create_task_thread(
        self,
        *,
        team_definition_id: str | None,
        run_id: str | None,
        workspace_id: str,
        project_id: str,
        title: str | None,
        metadata: dict[str, Any],
        status: str = "active",
    ) -> dict[str, Any]:
        record_id = make_id("thread")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO task_threads(id, team_definition_id, run_id, workspace_id, project_id, title, status, metadata_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_id, team_definition_id, run_id, workspace_id, project_id, title, status, json_dumps(metadata), now, now),
        )
        thread = self.fetch_one("SELECT * FROM task_threads WHERE id = ?", (record_id,))
        assert thread is not None
        return self._deserialize(thread) or {}

    def get_task_thread(self, thread_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM task_threads WHERE id = ?", (thread_id,)))

    def delete_task_thread(self, thread_id: str, *, delete_messages: bool = True) -> dict[str, Any] | None:
        existing = self.get_task_thread(thread_id)
        if existing is None:
            return None
        if delete_messages:
            self.execute("DELETE FROM message_events WHERE thread_id = ?", (thread_id,))
        self.execute("DELETE FROM task_threads WHERE id = ?", (thread_id,))
        return existing

    def update_task_thread(
        self,
        thread_id: str,
        *,
        title: str | None = None,
        status: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = self.get_task_thread(thread_id)
        if existing is None:
            return None
        now = utcnow_iso()
        next_title = existing.get("title") if title is None else title
        next_status = str(existing.get("status") or "active") if status is None else status
        next_metadata = dict(existing.get("metadata_json") or {}) if metadata is None else dict(metadata)
        self.execute(
            """
            UPDATE task_threads
            SET title = ?, status = ?, metadata_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (next_title, next_status, json_dumps(next_metadata), now, thread_id),
        )
        return self.get_task_thread(thread_id)

    def list_task_threads(
        self,
        *,
        team_definition_id: str | None = None,
        run_id: str | None = None,
        agent_definition_id: str | None = None,
        mode: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if team_definition_id:
            clauses.append("team_definition_id = ?")
            params.append(team_definition_id)
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        sql = "SELECT * FROM task_threads"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        sql += " ORDER BY updated_at DESC"
        items = self._deserialize_many(self.fetch_all(sql, tuple(params)))
        if not agent_definition_id and not mode:
            return items
        filtered: list[dict[str, Any]] = []
        for item in items:
            metadata = dict(item.get("metadata_json") or {})
            if agent_definition_id and str(metadata.get("agent_definition_id") or "").strip() != str(agent_definition_id):
                continue
            if mode and str(metadata.get("mode") or "").strip() != str(mode):
                continue
            filtered.append(item)
        return filtered

    def find_task_thread_by_session_thread_id(
        self,
        *,
        session_thread_id: str,
        agent_definition_id: str | None = None,
        team_definition_id: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any] | None:
        target = str(session_thread_id or "").strip()
        if not target:
            return None
        for item in self.list_task_threads(team_definition_id=team_definition_id, agent_definition_id=agent_definition_id, mode=mode):
            metadata = dict(item.get("metadata_json") or {})
            if str(metadata.get("session_thread_id") or "").strip() == target:
                return item
        return None

    def add_message_event(
        self,
        *,
        run_id: str | None,
        thread_id: str | None,
        source_agent_id: str | None,
        target_agent_id: str | None,
        message_type: str,
        payload: dict[str, Any],
        status: str = "delivered",
    ) -> dict[str, Any]:
        record_id = make_id("msg")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO message_events(id, run_id, thread_id, source_agent_id, target_agent_id, message_type, payload_json, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_id, run_id, thread_id, source_agent_id, target_agent_id, message_type, json_dumps(payload), status, now),
        )
        event = self.fetch_one("SELECT * FROM message_events WHERE id = ?", (record_id,))
        assert event is not None
        return self._deserialize(event) or {}

    def list_message_events(self, *, thread_id: str | None = None, run_id: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if thread_id:
            clauses.append("thread_id = ?")
            params.append(thread_id)
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        sql = "SELECT * FROM message_events"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        sql += " ORDER BY created_at ASC"
        return self._deserialize_many(self.fetch_all(sql, tuple(params)))

    def save_team_build_snapshot(
        self,
        *,
        snapshot_id: str | None,
        team_definition_id: str | None,
        run_id: str,
        runtime_tree_snapshot: dict[str, Any],
        resource_lock: dict[str, Any],
        compiled_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        record_id = snapshot_id or make_id("teamsnap")
        existing = self.get_team_build_snapshot_by_run(run_id)
        if existing is not None:
            record_id = str(existing["id"])
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO team_build_snapshots(
                id,
                team_definition_id,
                run_id,
                runtime_tree_snapshot_json,
                resource_lock_json,
                compiled_metadata_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM team_build_snapshots WHERE id = ?), ?), ?)
            ON CONFLICT(run_id) DO UPDATE SET
                team_definition_id = excluded.team_definition_id,
                runtime_tree_snapshot_json = excluded.runtime_tree_snapshot_json,
                resource_lock_json = excluded.resource_lock_json,
                compiled_metadata_json = excluded.compiled_metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                record_id,
                team_definition_id,
                run_id,
                json_dumps(runtime_tree_snapshot),
                json_dumps(resource_lock),
                json_dumps(compiled_metadata),
                record_id,
                now,
                now,
            ),
        )
        snapshot = self.get_team_build_snapshot_by_run(run_id)
        assert snapshot is not None
        return snapshot

    def get_team_build_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM team_build_snapshots WHERE id = ?", (snapshot_id,)))

    def get_team_build_snapshot_by_run(self, run_id: str) -> dict[str, Any] | None:
        return self._deserialize(self.fetch_one("SELECT * FROM team_build_snapshots WHERE run_id = ?", (run_id,)))

    def list_team_build_snapshots(self, *, team_definition_id: str | None = None, run_id: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if team_definition_id:
            clauses.append("team_definition_id = ?")
            params.append(team_definition_id)
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        sql = "SELECT * FROM team_build_snapshots"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        sql += " ORDER BY updated_at DESC"
        return self._deserialize_many(self.fetch_all(sql, tuple(params)))

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
        return list(self.list_runs_page(project_id=project_id)["items"])

    def list_runs_page(self, *, project_id: str | None = None, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
        conditions: list[str] = []
        params: list[Any] = []
        if project_id:
            conditions.append("runs.project_id = ?")
            params.append(project_id)
        where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        total = int((self.fetch_one(f"SELECT COUNT(*) AS count FROM runs{where_sql}", tuple(params)) or {}).get("count", 0) or 0)
        safe_offset = max(0, int(offset or 0))
        sql = (
            "SELECT runs.*, task_releases.title AS task_title, task_releases.prompt AS task_prompt, "
            "blueprints.name AS blueprint_name, blueprints.description AS blueprint_description "
            "FROM runs "
            "LEFT JOIN task_releases ON task_releases.id = runs.task_release_id "
            "LEFT JOIN blueprints ON blueprints.id = runs.blueprint_id"
            f"{where_sql} ORDER BY runs.updated_at DESC"
        )
        query_params = list(params)
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += " LIMIT ? OFFSET ?"
            query_params.extend([safe_limit, safe_offset])
        else:
            safe_limit = total or 0
        rows = self._deserialize_many(self.fetch_all(sql, tuple(query_params)))
        return {
            "items": rows,
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

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

    def create_approval(
        self,
        *,
        run_id: str,
        step_id: str,
        node_id: str,
        title: str,
        detail: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        record_id = make_id("approval")
        now = utcnow_iso()
        self.execute(
            """
            INSERT INTO approvals(id, run_id, step_id, node_id, title, detail, status, metadata_json, resolution_json, created_at, updated_at, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_id, run_id, step_id, node_id, title, detail, "pending", json_dumps(metadata or {}), json_dumps({}), now, now, None),
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
        return self._enrich_approval(self._deserialize(self.fetch_one("SELECT * FROM approvals WHERE id = ?", (approval_id,))))

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
        return [self._enrich_approval(item) for item in self._deserialize_many(self.fetch_all(sql, tuple(params)))]

    def list_approvals_page(
        self,
        *,
        run_id: str | None = None,
        status: str | None = None,
        view: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        normalized_view = str(view or "").strip().lower()
        if status:
            clauses.append("status = ?")
            params.append(status)
        elif normalized_view == "pending":
            clauses.append("status = 'pending'")
        elif normalized_view == "history":
            clauses.append("status != 'pending'")
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        total = int((self.fetch_one(f"SELECT COUNT(*) AS count FROM approvals{where_sql}", tuple(params)) or {}).get("count", 0) or 0)
        page_limit = max(1, int(limit or 10))
        page_offset = max(0, int(offset or 0))
        rows = self._deserialize_many(
            self.fetch_all(
                (
                    "SELECT * FROM approvals"
                    f"{where_sql} "
                    "ORDER BY COALESCE(resolved_at, created_at) DESC, created_at DESC "
                    "LIMIT ? OFFSET ?"
                ),
                (*params, page_limit, page_offset),
            )
        )
        return {
            "items": [self._enrich_approval(item) for item in rows],
            "total": total,
            "limit": page_limit,
            "offset": page_offset,
            "view": normalized_view or "all",
        }

    def _enrich_approval(self, approval: dict[str, Any] | None) -> dict[str, Any] | None:
        if approval is None:
            return None
        metadata = dict(approval.get("metadata_json") or {})
        if str(approval.get("status") or "") != "pending":
            return approval
        run_id = str(approval.get("run_id") or "").strip()
        if not run_id:
            return approval
        run = self._deserialize(self.fetch_one("SELECT state_json FROM runs WHERE id = ?", (run_id,)))
        state = dict((run or {}).get("state_json") or {})
        waiting = dict(state.get("waiting") or {})
        if str(waiting.get("approval_id") or "") != str(approval.get("id") or ""):
            return approval
        scope = str(waiting.get("scope") or "").strip()
        if scope == "tool_interrupt":
            interrupt_payload = dict(waiting.get("interrupt_payload") or {})
            action_requests = [
                dict(item or {})
                for item in list(interrupt_payload.get("action_requests") or [])
                if isinstance(item, dict)
            ]
            allowed_decisions = [
                str(item).strip()
                for item in list(interrupt_payload.get("allowed_decisions") or [])
                if str(item).strip()
            ]
            for config in list(interrupt_payload.get("review_configs") or []):
                allowed_decisions.extend(
                    str(item).strip()
                    for item in list((dict(config or {})).get("allowed_decisions") or [])
                    if str(item).strip()
                )
            for action in action_requests:
                allowed_decisions.extend(
                    str(item).strip()
                    for item in list(action.get("allowed_decisions") or [])
                    if str(item).strip()
                )
            metadata.update(
                {
                    "scope": scope,
                    "allowed_decisions": list(dict.fromkeys([*list(metadata.get("allowed_decisions") or []), *allowed_decisions]))
                    or ["approve", "reject"],
                    "action_requests": action_requests or list(metadata.get("action_requests") or []),
                }
            )
            approval["metadata_json"] = metadata
            return approval
        if scope == "final_delivery":
            metadata.update(
                {
                    "scope": scope,
                    "allowed_decisions": list(dict.fromkeys([*list(metadata.get("allowed_decisions") or []), "approve", "reject"])),
                    "pending_result_text": str(state.get("pending_result_text") or ""),
                }
            )
            approval["metadata_json"] = metadata
        return approval

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
            "local_model_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM local_models") or {}).get("count", 0) or 0),
            "plugin_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM plugins") or {}).get("count", 0) or 0),
            "skill_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM skills") or {}).get("count", 0) or 0),
            "static_memory_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM static_memories") or {}).get("count", 0) or 0),
            "knowledge_base_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM knowledge_bases") or {}).get("count", 0) or 0),
            "agent_definition_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM agent_definitions") or {}).get("count", 0) or 0),
            "team_definition_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM team_definitions") or {}).get("count", 0) or 0),
            "review_policy_count": int((self.fetch_one("SELECT COUNT(*) AS count FROM review_policies") or {}).get("count", 0) or 0),
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
