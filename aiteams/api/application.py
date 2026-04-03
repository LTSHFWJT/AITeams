from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import shutil
import sqlite3
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import parse_qs, urlparse

from aiteams.agent_center import AgentCenterService
from aiteams.knowledge import KnowledgeBaseService
from aiteams.plugins import PluginManager
from aiteams.role_specs import normalize_role_spec
from aiteams.runtime.engine import RuntimeEngine
from aiteams.skills import SkillLibraryScan, SkillValidationIssue, ValidatedSkill, scan_skill_library
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import make_id, make_uuid7, slugify, trim_text, utcnow_iso
from aiteams.workspace.manager import WorkspaceManager


LOGGER = logging.getLogger("aiteams")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False


class AppError(RuntimeError):
    def __init__(self, status: int, detail: str, *, extra: dict[str, Any] | None = None):
        super().__init__(detail)
        self.status = status
        self.detail = detail
        self.extra = dict(extra or {})


@dataclass(slots=True)
class AppResponse:
    status: int
    body: bytes
    content_type: str
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ServiceContainer:
    store: MetadataStore
    runtime: RuntimeEngine
    workspace: WorkspaceManager
    agent_center: AgentCenterService
    plugins: PluginManager
    knowledge_bases: KnowledgeBaseService
    static_dir: Path
    local_models_root: Path

    def close(self) -> None:
        agent_memory = getattr(self.runtime.agent_kernel.memory, "close", None)
        if callable(agent_memory):
            agent_memory()
        self.knowledge_bases.close()
        self.plugins.close()
        self.store.close()


class WebApplication:
    def __init__(self, container: ServiceContainer):
        self.container = container

    def close(self) -> None:
        self.container.close()

    def handle(self, method: str, raw_path: str, body: bytes = b"") -> AppResponse:
        parsed = urlparse(raw_path)
        path = parsed.path
        query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
        try:
            if method == "OPTIONS":
                return self._empty(204)
            if method == "GET" and path == "/":
                return self._file_response(self.container.static_dir / "index.html", "text/html; charset=utf-8")
            if method == "GET" and path.startswith("/static/"):
                return self._serve_static(path)
            if method == "GET" and path == "/api/health":
                return self._json(200, {"status": "ok"})
            if method == "GET" and path == "/api/control-plane":
                agent_memory = getattr(self.container.runtime.agent_kernel, "memory", None)
                memory_storage = agent_memory.storage_info() if hasattr(agent_memory, "storage_info") else {}
                runtime_storage = self.container.runtime.storage_info() if hasattr(self.container.runtime, "storage_info") else {}
                return self._json(
                    200,
                    {
                        "summary": self.container.store.dashboard_summary(),
                        "storage": {
                            **self.container.store.storage_info(),
                            **runtime_storage,
                            "memory_stack": memory_storage,
                            "graph_runtime": "langgraph-official",
                        },
                        "provider_types": self.container.agent_center.provider_types(),
                        "recent_runs": self.container.store.list_runs()[:10],
                        "pending_approvals": self.container.store.list_approvals(status="pending")[:10],
                    },
                )
            if method == "GET" and path == "/api/control-plane/requirements-report":
                return self._json(200, self._requirements_report())
            if method == "GET" and path == "/api/workspaces":
                return self._json(200, {"items": self.container.store.list_workspaces()})
            if method == "POST" and path == "/api/workspaces":
                payload = self._parse_json(body)
                self._require_fields(payload, "name")
                workspace_id = self._optional_str(payload.get("id"))
                root_path = str(payload.get("root_path") or self.container.workspace.workspace_dir(workspace_id or "workspace"))
                workspace = self.container.store.create_workspace(
                    workspace_id=workspace_id,
                    name=str(payload["name"]),
                    description=str(payload.get("description") or ""),
                    root_path=root_path,
                )
                return self._json(200, workspace)
            if method == "GET" and path == "/api/projects":
                return self._json(200, {"items": self.container.store.list_projects(workspace_id=self._optional_str(query.get("workspace_id")))})
            if method == "POST" and path == "/api/projects":
                payload = self._parse_json(body)
                self._require_fields(payload, "workspace_id", "name")
                project = self.container.store.create_project(
                    project_id=self._optional_str(payload.get("id")),
                    workspace_id=str(payload["workspace_id"]),
                    name=str(payload["name"]),
                    description=str(payload.get("description") or ""),
                )
                return self._json(200, project)
            if method == "GET" and path == "/api/agent-center/provider-types":
                return self._json(200, {"items": self.container.agent_center.provider_types()})
            if method == "GET" and path == "/api/agent-center/ui-metadata":
                return self._json(200, self.container.agent_center.ui_metadata())
            if method == "GET" and path == "/api/agent-center/providers":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                return self._json(
                    200,
                    self.container.agent_center.list_provider_profiles(
                        query=self._optional_str(query.get("query")),
                        provider_type=self._optional_str(query.get("provider_type")),
                        limit=limit,
                        offset=offset,
                    ),
                )
            if method == "POST" and path == "/api/agent-center/providers":
                payload = self._parse_json(body)
                provider = self._save_provider_profile(payload, provider_id=self._optional_str(payload.get("id")))
                return self._json(200, provider)
            if method == "POST" and path == "/api/agent-center/providers/discover-models":
                payload = self._parse_json(body)
                return self._json(200, self.container.agent_center.discover_provider_models(payload))
            if method == "POST" and path == "/api/agent-center/providers/test-model":
                payload = self._parse_json(body)
                return self._json(200, self.container.agent_center.test_provider_model(payload))
            if method == "GET" and path == "/api/agent-center/local-models":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                return self._json(
                    200,
                    self._local_model_page_payload(
                        limit=limit,
                        offset=offset,
                        query=self._optional_str(query.get("query")),
                        model_type=self._optional_str(query.get("model_type")),
                    ),
                )
            if method == "POST" and path == "/api/agent-center/local-models":
                payload = self._parse_json(body)
                return self._json(200, self._save_local_model(payload, local_model_id=self._optional_str(payload.get("id"))))
            if method == "GET" and path == "/api/agent-center/retrieval-settings":
                return self._json(200, self.container.agent_center.get_retrieval_settings())
            if method == "PUT" and path == "/api/agent-center/retrieval-settings":
                payload = self._parse_json(body)
                saved = self.container.agent_center.save_retrieval_settings(payload)
                try:
                    applied = self.container.runtime.agent_kernel.memory.configure_retrieval(saved.get("runtime"))
                    knowledge_applied = self.container.knowledge_bases.configure_retrieval(saved.get("runtime"))
                except Exception as exc:
                    raise AppError(400, f"Retrieval settings saved but could not be applied: {exc}") from exc
                return self._json(
                    200,
                    {
                        "settings": saved.get("settings"),
                        "updated_at": saved.get("updated_at"),
                        "applied": applied,
                        "runtime_applied": applied,
                        "knowledge_applied": knowledge_applied,
                    },
                )
            if path.startswith("/api/agent-center/providers/"):
                provider_id = path.rsplit("/", 1)[-1]
                provider = self.container.agent_center.normalize_provider_profile(self.container.store.get_provider_profile(provider_id, include_secret=True))
                if provider is None:
                    raise AppError(404, "Provider profile does not exist.")
                if method == "GET":
                    return self._json(200, provider)
                if method == "PUT":
                    payload = self._parse_json(body)
                    updated = self._save_provider_profile(payload, provider_id=provider_id)
                    return self._json(200, updated)
                if method == "DELETE":
                    deleted = self.container.store.delete_provider_profile(provider_id)
                    if deleted is None:
                        raise AppError(404, "Provider profile does not exist.")
                    return self._json(200, {"deleted": True, "id": provider_id})
            if path.startswith("/api/agent-center/local-models/"):
                local_model_id = path.rsplit("/", 1)[-1]
                local_model = self.container.agent_center.normalize_local_model(self.container.store.get_local_model(local_model_id))
                if local_model is None:
                    raise AppError(404, "Local model does not exist.")
                if method == "GET":
                    return self._json(200, self._local_model_resource(local_model))
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_local_model(payload, local_model_id=local_model_id))
                if method == "DELETE":
                    deleted = self._delete_local_model(local_model_id)
                    if deleted is None:
                        raise AppError(404, "Local model does not exist.")
                    return self._json(200, {"deleted": True, "id": local_model_id})
            if method == "GET" and path == "/api/agent-center/plugins":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                page = self.container.store.list_plugins_page(limit=limit, offset=offset)
                items = []
                for item in page["items"]:
                    plugin = dict(item)
                    plugin["runtime"] = self.container.plugins.snapshot(str(item["id"]))
                    items.append(plugin)
                return self._json(
                    200,
                    {
                        "items": items,
                        "total": page["total"],
                        "offset": page["offset"],
                        "limit": page["limit"],
                    },
                )
            if method == "GET" and path == "/api/agent-center/plugins/builtins":
                return self._json(200, {"items": self.container.plugins.builtin_catalog()})
            if method == "POST" and path == "/api/agent-center/plugins/validate-package":
                payload = self._parse_json(body)
                self._require_fields(payload, "path")
                return self._json(200, self.container.plugins.validate_package(str(payload["path"])))
            if method == "POST" and path == "/api/agent-center/plugins":
                payload = self._parse_json(body)
                plugin = self._save_plugin(payload, plugin_id=self._optional_str(payload.get("id")))
                return self._json(200, plugin)
            if path.startswith("/api/agent-center/plugins/") and path.endswith("/install") and method == "POST":
                plugin_id = path.split("/")[4]
                return self._json(200, self.container.plugins.install_plugin(plugin_id))
            if path.startswith("/api/agent-center/plugins/") and path.endswith("/load") and method == "POST":
                plugin_id = path.split("/")[4]
                return self._json(200, self.container.plugins.load_plugin(plugin_id))
            if path.startswith("/api/agent-center/plugins/") and path.endswith("/reload") and method == "POST":
                plugin_id = path.split("/")[4]
                return self._json(200, self.container.plugins.reload_plugin(plugin_id))
            if path.startswith("/api/agent-center/plugins/") and path.endswith("/health") and method == "GET":
                plugin_id = path.split("/")[4]
                return self._json(200, self.container.plugins.health(plugin_id))
            if path.startswith("/api/agent-center/plugins/") and path.endswith("/invoke") and method == "POST":
                plugin_id = path.split("/")[4]
                payload = self._parse_json(body)
                response = self.container.plugins.invoke_plugin(
                    {"id": plugin_id},
                    action=str(payload.get("action") or ""),
                    payload=dict(payload.get("payload") or {}),
                    context=dict(payload.get("context") or {}),
                )
                return self._json(200, {"plugin_id": plugin_id, "result": response})
            if path.startswith("/api/agent-center/plugins/"):
                plugin_id = path.rsplit("/", 1)[-1]
                plugin = self.container.store.get_plugin(plugin_id)
                if plugin is None:
                    raise AppError(404, "Plugin does not exist.")
                if method == "GET":
                    plugin = dict(plugin)
                    plugin["runtime"] = self.container.plugins.snapshot(plugin_id)
                    return self._json(200, plugin)
                if method == "PUT":
                    payload = self._parse_json(body)
                    updated = self._save_plugin(payload, plugin_id=plugin_id)
                    return self._json(200, updated)
            if method == "GET" and path == "/api/agent-center/skill-groups":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                page = self.container.store.list_skill_groups_page(limit=limit, offset=offset)
                return self._json(
                    200,
                    {
                        "items": [self._skill_group_resource(item) for item in page["items"]],
                        "total": page["total"],
                        "offset": page["offset"],
                        "limit": page["limit"],
                    },
                )
            if method == "POST" and path == "/api/agent-center/skill-groups":
                payload = self._parse_json(body)
                return self._json(200, self._save_skill_group(payload, skill_group_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/skill-groups/"):
                skill_group_id = path.rsplit("/", 1)[-1]
                skill_group = self.container.store.get_skill_group(skill_group_id)
                if skill_group is None:
                    raise AppError(404, "Skill group does not exist.")
                if method == "GET":
                    return self._json(200, self._skill_group_resource(skill_group))
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_skill_group(payload, skill_group_id=skill_group_id))
                if method == "DELETE":
                    deleted = self.container.store.delete_skill_group(skill_group_id)
                    if deleted is None:
                        raise AppError(404, "Skill group does not exist.")
                    return self._json(200, {"deleted": True, "id": skill_group_id})
            if method == "POST" and path == "/api/agent-center/skills/scan-library":
                payload = self._parse_json(body)
                self._require_fields(payload, "path")
                return self._json(
                    200,
                    self._scan_skill_library_resource(
                        str(payload["path"]),
                        recursive=self._coerce_bool(payload.get("recursive"), default=True),
                    ),
                )
            if method == "POST" and path == "/api/agent-center/skills/import-library":
                payload = self._parse_json(body)
                self._require_fields(payload, "path")
                return self._json(
                    200,
                    self._import_skill_library(
                        str(payload["path"]),
                        recursive=self._coerce_bool(payload.get("recursive"), default=True),
                    ),
                )
            if method == "POST" and path == "/api/agent-center/skills/scan-upload":
                payload = self._parse_json(body)
                return self._json(
                    200,
                    self._scan_uploaded_skill_library(
                        payload,
                        recursive=self._coerce_bool(payload.get("recursive"), default=True),
                    ),
                )
            if method == "POST" and path == "/api/agent-center/skills/import-upload":
                payload = self._parse_json(body)
                return self._json(
                    200,
                    self._import_uploaded_skill_library(
                        payload,
                        recursive=self._coerce_bool(payload.get("recursive"), default=True),
                    ),
                )
            if method == "GET" and path == "/api/agent-center/skills":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                page = self.container.store.list_skills_page(
                    query=self._optional_str(query.get("query")),
                    group_key=self._optional_str(query.get("group_key")),
                    limit=limit,
                    offset=offset,
                )
                return self._json(
                    200,
                    {
                        "items": [self._skill_resource(item) for item in page["items"]],
                        "total": page["total"],
                        "offset": page["offset"],
                        "limit": page["limit"],
                        "groups": page.get("groups") or [],
                    },
                )
            if method == "POST" and path == "/api/agent-center/skills":
                payload = self._parse_json(body)
                return self._json(200, self._save_skill(payload, skill_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/skills/") and path.endswith("/files") and method == "GET":
                skill_id = path.split("/")[4]
                skill = self.container.store.get_skill(skill_id)
                if skill is None:
                    raise AppError(404, "Skill does not exist.")
                return self._json(200, self._skill_files_resource(skill))
            if path.startswith("/api/agent-center/skills/") and path.endswith("/file-content") and method == "GET":
                skill_id = path.split("/")[4]
                skill = self.container.store.get_skill(skill_id)
                if skill is None:
                    raise AppError(404, "Skill does not exist.")
                return self._json(200, self._skill_file_content_resource(skill, query.get("path")))
            if path.startswith("/api/agent-center/skills/"):
                skill_id = path.rsplit("/", 1)[-1]
                skill = self.container.store.get_skill(skill_id)
                if skill is None:
                    raise AppError(404, "Skill does not exist.")
                if method == "GET":
                    return self._json(200, self._skill_resource(skill))
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_skill(payload, skill_id=skill_id))
                if method == "DELETE":
                    self._delete_deepagents_skill_package(skill)
                    deleted = self.container.store.delete_skill(skill_id)
                    if deleted is None:
                        raise AppError(404, "Skill does not exist.")
                    return self._json(200, {"deleted": True, "id": skill_id})
            if method == "GET" and path == "/api/agent-center/static-memories":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                page = self.container.store.list_static_memories_page(limit=limit, offset=offset)
                return self._json(
                    200,
                    {
                        "items": [self._static_memory_resource(item) for item in page["items"]],
                        "total": page["total"],
                        "offset": page["offset"],
                        "limit": page["limit"],
                    },
                )
            if method == "POST" and path == "/api/agent-center/static-memories":
                payload = self._parse_json(body)
                return self._json(200, self._save_static_memory(payload, static_memory_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/static-memories/"):
                static_memory_id = path.rsplit("/", 1)[-1]
                static_memory = self.container.store.get_static_memory(static_memory_id)
                if static_memory is None:
                    raise AppError(404, "Static memory does not exist.")
                if method == "GET":
                    return self._json(200, self._static_memory_resource(static_memory))
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_static_memory(payload, static_memory_id=static_memory_id))
                if method == "DELETE":
                    deleted = self.container.store.delete_static_memory(static_memory_id)
                    if deleted is None:
                        raise AppError(404, "Static memory does not exist.")
                    return self._json(200, {"deleted": True, "id": static_memory_id})
            if method == "GET" and path == "/api/agent-center/knowledge-bases":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                return self._json(
                    200,
                    self._knowledge_base_page_payload(
                        limit=limit,
                        offset=offset,
                        query=self._optional_str(query.get("query")),
                    ),
                )
            if method == "POST" and path == "/api/agent-center/knowledge-bases":
                payload = self._parse_json(body)
                return self._json(200, self._save_knowledge_base(payload, knowledge_base_id=self._optional_str(payload.get("id"))))
            if method == "GET" and path == "/api/agent-center/knowledge-pool-documents":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                return self._json(
                    200,
                    self.container.knowledge_bases.list_pool_documents_page(
                        limit=limit,
                        offset=offset,
                        query=self._optional_str(query.get("query")),
                        exclude_knowledge_base_id=self._optional_str(query.get("exclude_knowledge_base_id")),
                    ),
                )
            if method == "POST" and path == "/api/agent-center/knowledge-pool-documents/upload":
                payload = self._parse_json(body)
                try:
                    return self._json(200, self.container.knowledge_bases.import_pool_uploaded_files(payload))
                except ValueError as exc:
                    raise AppError(400, str(exc)) from exc
            if method == "POST" and path == "/api/agent-center/knowledge-pool-documents/actions":
                payload = self._parse_json(body)
                try:
                    return self._json(
                        200,
                        self.container.knowledge_bases.manage_pool_documents(
                            action=str(payload.get("action") or ""),
                            document_ids=list(payload.get("document_ids") or []),
                        ),
                    )
                except ValueError as exc:
                    raise AppError(400, str(exc)) from exc
            if path.startswith("/api/agent-center/knowledge-embedding-jobs/"):
                job_id = path.rsplit("/", 1)[-1]
                job = self.container.knowledge_bases.get_document_embedding_job(job_id)
                if job is None:
                    raise AppError(404, "Knowledge embedding job does not exist.")
                if method == "GET":
                    return self._json(200, job)
            if path.startswith("/api/agent-center/knowledge-bases/"):
                suffix = path.removeprefix("/api/agent-center/knowledge-bases/")
                parts = [part for part in suffix.split("/") if part]
                knowledge_base_id = parts[0] if parts else ""
                if len(parts) == 2 and parts[1] == "documents":
                    if self.container.store.get_knowledge_base(knowledge_base_id) is None:
                        raise AppError(404, "Knowledge base does not exist.")
                    if method == "GET":
                        limit = self._optional_int(query.get("limit"))
                        offset = self._optional_int(query.get("offset")) or 0
                        return self._json(
                            200,
                            self.container.knowledge_bases.list_documents_page(
                                knowledge_base_id=knowledge_base_id,
                                limit=limit,
                                offset=offset,
                                query=self._optional_str(query.get("query")),
                                embedding_status=self._optional_str(query.get("embedding_status")),
                            ),
                        )
                if len(parts) == 2 and parts[1] == "pool-documents":
                    if self.container.store.get_knowledge_base(knowledge_base_id) is None:
                        raise AppError(404, "Knowledge base does not exist.")
                    if method == "POST":
                        payload = self._parse_json(body)
                        try:
                            return self._json(
                                200,
                                self.container.knowledge_bases.add_pool_documents_to_knowledge_base(
                                    knowledge_base_id,
                                    pool_document_ids=list(payload.get("document_ids") or []),
                                ),
                            )
                        except ValueError as exc:
                            raise AppError(400, str(exc)) from exc
                if len(parts) == 3 and parts[1] == "documents" and parts[2] == "embeddings":
                    if self.container.store.get_knowledge_base(knowledge_base_id) is None:
                        raise AppError(404, "Knowledge base does not exist.")
                    if method == "POST":
                        payload = self._parse_json(body)
                        try:
                            return self._json(
                                200,
                                self.container.knowledge_bases.start_document_embedding_job(
                                    knowledge_base_id,
                                    action=str(payload.get("action") or ""),
                                    document_ids=list(payload.get("document_ids") or []),
                                ),
                            )
                        except ValueError as exc:
                            raise AppError(400, str(exc)) from exc
                if len(parts) == 2 and parts[1] == "upload":
                    if self.container.store.get_knowledge_base(knowledge_base_id) is None:
                        raise AppError(404, "Knowledge base does not exist.")
                    if method == "POST":
                        payload = self._parse_json(body)
                        return self._json(200, self._import_knowledge_base_uploaded_files(knowledge_base_id, payload))
                knowledge_base = self.container.store.get_knowledge_base(knowledge_base_id)
                if knowledge_base is None:
                    raise AppError(404, "Knowledge base does not exist.")
                if method == "GET":
                    return self._json(200, self._knowledge_base_resource(knowledge_base))
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_knowledge_base(payload, knowledge_base_id=knowledge_base_id))
                if method == "DELETE":
                    deleted = self.container.knowledge_bases.delete_knowledge_base(knowledge_base_id)
                    if deleted is None:
                        raise AppError(404, "Knowledge base does not exist.")
                    return self._json(200, {"deleted": True, "id": knowledge_base_id})
            if method == "GET" and path == "/api/agent-center/knowledge-documents":
                knowledge_base_id = self._optional_str(query.get("knowledge_base_id"))
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                if knowledge_base_id:
                    return self._json(
                        200,
                        self.container.knowledge_bases.list_documents_page(
                            knowledge_base_id=knowledge_base_id,
                            limit=limit,
                            offset=offset,
                        ),
                    )
                return self._json(
                    200,
                    {"items": [self._knowledge_document_resource(item) for item in self.container.store.list_knowledge_documents()]},
                )
            if method == "POST" and path == "/api/agent-center/knowledge-documents":
                payload = self._parse_json(body)
                return self._json(200, self._save_knowledge_document(payload, knowledge_document_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/knowledge-documents/"):
                knowledge_document_id = path.rsplit("/", 1)[-1]
                knowledge_document = self.container.store.get_knowledge_document(knowledge_document_id)
                if knowledge_document is None:
                    raise AppError(404, "Knowledge document does not exist.")
                if method == "GET":
                    payload = self._knowledge_document_resource(knowledge_document)
                    payload["content_text"] = str(knowledge_document.get("content_text") or "")
                    return self._json(200, payload)
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_knowledge_document(payload, knowledge_document_id=knowledge_document_id))
                if method == "DELETE":
                    deleted = self.container.knowledge_bases.delete_document(knowledge_document_id)
                    if deleted is None:
                        raise AppError(404, "Knowledge document does not exist.")
                    return self._json(200, {"deleted": True, "id": knowledge_document_id})
            if method == "GET" and path == "/api/agent-center/review-policies":
                return self._json(200, {"items": self.container.store.list_review_policies()})
            if method == "POST" and path == "/api/agent-center/review-policies":
                payload = self._parse_json(body)
                return self._json(200, self._save_review_policy(payload, review_policy_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/review-policies/"):
                review_policy_id = path.rsplit("/", 1)[-1]
                review_policy = self.container.store.get_review_policy(review_policy_id)
                if review_policy is None:
                    raise AppError(404, "Review policy does not exist.")
                if method == "GET":
                    return self._json(200, review_policy)
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_review_policy(payload, review_policy_id=review_policy_id))
                if method == "DELETE":
                    deleted = self.container.store.delete_review_policy(review_policy_id)
                    if deleted is None:
                        raise AppError(404, "Review policy does not exist.")
                    return self._json(200, {"deleted": True, "id": review_policy_id})
            if method == "GET" and path == "/api/agent-center/agent-definitions":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                return self._json(200, self.container.store.list_agent_definitions_page(limit=limit, offset=offset))
            if method == "POST" and path == "/api/agent-center/agent-definitions":
                payload = self._parse_json(body)
                return self._json(200, self._save_agent_definition(payload, definition_id=None))
            if path.startswith("/api/agent-center/team-definitions/") and path.endswith("/chat/threads") and method == "GET":
                definition_id = path.split("/")[4]
                definition = self.container.store.get_team_definition(definition_id)
                if definition is None:
                    raise AppError(404, "Team definition does not exist.")
                threads = [
                    self._team_chat_thread_resource(item)
                    for item in self.container.store.list_task_threads(team_definition_id=definition_id, mode="team_chat")
                ]
                return self._json(200, {"items": threads, "team_definition": definition})
            if path.startswith("/api/agent-center/team-definitions/") and "/chat/threads/" in path and method == "DELETE":
                path_parts = path.split("/")
                definition_id = path_parts[4]
                thread_id = path_parts[-1]
                definition = self.container.store.get_team_definition(definition_id)
                if definition is None:
                    raise AppError(404, "Team definition does not exist.")
                thread = self.container.store.get_task_thread(thread_id)
                if thread is None:
                    raise AppError(404, "Team chat thread does not exist.")
                metadata = dict(thread.get("metadata_json") or {})
                if str(thread.get("team_definition_id") or "").strip() != str(definition_id) or str(metadata.get("mode") or "").strip() != "team_chat":
                    raise AppError(404, "Team chat thread does not exist.")
                deleted = self.container.store.delete_task_thread(thread_id, delete_messages=True)
                if deleted is None:
                    raise AppError(404, "Team chat thread does not exist.")
                return self._json(200, {"deleted": True, "id": thread_id})
            if path.startswith("/api/agent-center/team-definitions/") and path.endswith("/chat/messages") and method == "POST":
                definition_id = path.split("/")[4]
                definition = self.container.store.get_team_definition(definition_id)
                if definition is None:
                    raise AppError(404, "Team definition does not exist.")
                payload = self._parse_json(body)
                self._require_fields(payload, "message")
                message_text = str(payload["message"]).strip()
                if not message_text:
                    raise AppError(400, "Message cannot be empty.")
                session_thread_id = self._optional_str(payload.get("thread_id")) or make_uuid7()
                thread = self.container.store.find_task_thread_by_session_thread_id(
                    session_thread_id=session_thread_id,
                    agent_definition_id=None,
                    team_definition_id=definition_id,
                    mode="team_chat",
                )
                defaults = self.container.store.default_scope_ids()
                title = self._optional_str(payload.get("title")) or f"团队测试 · {definition.get('name') or definition_id}"
                title = self._optional_str(payload.get("title")) or f"团队测试 · {definition.get('name') or definition_id}"
                title = self._optional_str(payload.get("title")) or f"\u56e2\u961f\u6d4b\u8bd5 - {definition.get('name') or definition_id}"
                build = self.container.agent_center.build_team_definition(definition_id, blueprint_name=f"{definition.get('name') or definition_id} Chat Runtime")
                blueprint = dict(build.get("blueprint") or {})
                bundle = asyncio.run(
                    self.container.runtime.start_task(
                        blueprint=blueprint,
                        title=title,
                        prompt=message_text,
                        inputs=dict(payload.get("inputs") or {}),
                        approval_mode=str(payload.get("approval_mode") or "auto"),
                        session_thread_id=session_thread_id,
                    )
                )
                conversation_thread_id = (
                    self._optional_str(((bundle.get("run") or {}).get("state_json") or {}).get("session_thread_id"))
                    or self._optional_str(((bundle.get("task_thread") or {}).get("metadata_json") or {}).get("session_thread_id"))
                    or session_thread_id
                )
                if thread is None:
                    thread = self.container.store.create_task_thread(
                        team_definition_id=definition_id,
                        run_id=None,
                        workspace_id=defaults["workspace_id"],
                        project_id=defaults["project_id"],
                        title=title,
                        metadata={
                            "mode": "team_chat",
                            "team_definition_id": definition_id,
                            "team_name": str(definition.get("name") or definition_id),
                            "session_thread_id": conversation_thread_id,
                            "last_message_preview": trim_text(message_text, limit=120),
                            "last_message_at": utcnow_iso(),
                        },
                    )
                run_payload = dict(bundle.get("run") or {})
                assistant_text = self._team_chat_response_text(bundle)
                user_event = self.container.store.add_message_event(
                    run_id=self._optional_str(run_payload.get("id")),
                    thread_id=str(thread["id"]),
                    source_agent_id="user",
                    target_agent_id=definition_id,
                    message_type="user",
                    payload={"role": "user", "body": message_text, "thread_id": conversation_thread_id},
                )
                interrupted = str(run_payload.get("status") or "").strip() == "waiting_approval"
                assistant_text = str(run_payload.get("summary") or "").strip()
                assistant_text = self._team_chat_response_text(bundle) or assistant_text
                assistant_payload: dict[str, Any] = {
                    "role": "assistant",
                    "body": assistant_text,
                    "thread_id": conversation_thread_id,
                    "run_id": self._optional_str(run_payload.get("id")),
                }
                assistant_status = "delivered"
                if interrupted:
                    assistant_payload["interrupted"] = True
                    assistant_payload["body"] = assistant_text or "当前团队测试触发了审批等待，测试页暂不支持继续审批流。"
                    assistant_payload["body"] = assistant_text or "当前团队测试触发了审批等待，测试页暂不支持继续审批流。"
                    assistant_payload["body"] = (
                        assistant_text
                        or "\u5f53\u524d\u56e2\u961f\u6d4b\u8bd5\u89e6\u53d1\u4e86\u5ba1\u6279\u7b49\u5f85\uff0c\u6d4b\u8bd5\u9875\u6682\u4e0d\u652f\u6301\u7ee7\u7eed\u5ba1\u6279\u6d41\u3002"
                    )
                    assistant_status = "interrupted"
                assistant_event = self.container.store.add_message_event(
                    run_id=self._optional_str(run_payload.get("id")),
                    thread_id=str(thread["id"]),
                    source_agent_id=definition_id,
                    target_agent_id="user",
                    message_type="assistant",
                    payload=assistant_payload,
                    status=assistant_status,
                )
                thread_metadata = dict(thread.get("metadata_json") or {})
                thread_metadata.update(
                    {
                        "mode": "team_chat",
                        "team_definition_id": definition_id,
                        "team_name": str(definition.get("name") or definition_id),
                        "session_thread_id": conversation_thread_id,
                        "last_run_id": self._optional_str(run_payload.get("id")),
                        "last_message_preview": trim_text(str(assistant_payload.get("body") or message_text), limit=120),
                        "last_message_at": utcnow_iso(),
                    }
                )
                updated_thread = self.container.store.update_task_thread(
                    str(thread["id"]),
                    title=str(thread.get("title") or title),
                    metadata=thread_metadata,
                )
                return self._json(
                    200,
                    {
                        "thread": self._team_chat_thread_resource(updated_thread or thread),
                        "thread_id": conversation_thread_id,
                        "user_message": user_event,
                        "assistant_message": assistant_event,
                        "run": run_payload,
                        "interrupted": interrupted,
                    },
                )
            if path.startswith("/api/agent-center/agent-definitions/"):
                definition_id = path.rsplit("/", 1)[-1]
                definition = self.container.store.get_agent_definition(definition_id)
                if definition is None:
                    raise AppError(404, "Agent definition does not exist.")
                if method == "GET":
                    return self._json(200, definition)
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_agent_definition(payload, definition_id=definition_id))
                if method == "DELETE":
                    deleted = self.container.store.delete_agent_definition(definition_id)
                    if deleted is None:
                        raise AppError(404, "Agent definition does not exist.")
                    return self._json(200, {"deleted": True, "id": definition_id})
            if method == "GET" and path == "/api/agent-center/team-definitions":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                return self._json(200, self.container.store.list_team_definitions_page(limit=limit, offset=offset))
            if method == "POST" and path == "/api/agent-center/team-definitions":
                payload = self._parse_json(body)
                return self._json(200, self._save_team_definition(payload, definition_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/team-definitions/") and path.endswith("/compile") and method == "POST":
                definition_id = path.split("/")[4]
                return self._json(200, self.container.agent_center.compile_team_definition(definition_id))
            if path.startswith("/api/agent-center/team-definitions/") and path.endswith("/tasks") and method == "POST":
                definition_id = path.split("/")[4]
                payload = self._parse_json(body)
                self._require_fields(payload, "prompt")
                build = self.container.agent_center.build_team_definition(definition_id, blueprint_name=self._optional_str(payload.get("name")))
                blueprint = dict(build.get("blueprint") or {})
                bundle = asyncio.run(
                    self.container.runtime.start_task(
                        blueprint=blueprint,
                        title=self._optional_str(payload.get("title")),
                        prompt=str(payload["prompt"]),
                        inputs=dict(payload.get("inputs") or {}),
                        approval_mode=str(payload.get("approval_mode") or "auto"),
                        session_thread_id=self._optional_str(payload.get("thread_id")),
                    )
                )
                bundle["conversation_thread_id"] = (
                    self._optional_str(((bundle.get("run") or {}).get("state_json") or {}).get("session_thread_id"))
                    or self._optional_str(((bundle.get("task_thread") or {}).get("metadata_json") or {}).get("session_thread_id"))
                )
                if not bundle.get("task_thread"):
                    thread_metadata = {"team_definition_id": definition_id, "adjacency": build.get("adjacency") or {}}
                    if build.get("hierarchy"):
                        thread_metadata["hierarchy"] = build.get("hierarchy")
                    if bundle.get("conversation_thread_id"):
                        thread_metadata["session_thread_id"] = bundle.get("conversation_thread_id")
                    thread = self.container.store.create_task_thread(
                        team_definition_id=definition_id,
                        run_id=str(bundle["run"]["id"]),
                        workspace_id=str(bundle["run"]["workspace_id"]),
                        project_id=str(bundle["run"]["project_id"]),
                        title=self._optional_str(payload.get("title")) or str(payload["prompt"])[:80],
                        metadata=thread_metadata,
                    )
                    bundle["task_thread"] = thread
                return self._json(200, bundle)
            if path.startswith("/api/agent-center/team-definitions/"):
                definition_id = path.rsplit("/", 1)[-1]
                definition = self.container.store.get_team_definition(definition_id)
                if definition is None:
                    raise AppError(404, "Team definition does not exist.")
                if method == "GET":
                    return self._json(200, definition)
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_team_definition(payload, definition_id=definition_id))
                if method == "DELETE":
                    deleted = self.container.store.delete_team_definition(definition_id)
                    if deleted is None:
                        raise AppError(404, "Team definition does not exist.")
                    return self._json(200, {"deleted": True, "id": definition_id})
            if method == "GET" and path == "/api/runs":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                return self._json(
                    200,
                    self.container.store.list_runs_page(
                        project_id=self._optional_str(query.get("project_id")),
                        limit=limit,
                        offset=offset,
                    ),
                )
            if path.startswith("/api/runs/") and path.endswith("/resume") and method == "POST":
                run_id = path.split("/")[3]
                bundle = asyncio.run(self.container.runtime.resume_run(run_id))
                return self._json(200, bundle)
            if path.startswith("/api/runs/") and path.endswith("/messages") and method == "POST":
                run_id = path.split("/")[3]
                payload = self._parse_json(body)
                self._require_fields(payload, "target_agent_id", "body")
                bundle = asyncio.run(
                    self.container.runtime.inject_human_message(
                        run_id=run_id,
                        target_agent_id=str(payload["target_agent_id"]),
                        body=str(payload["body"]),
                        message_type=str(payload.get("message_type") or "dialogue"),
                        phase=self._optional_str(payload.get("phase")),
                        metadata=dict(payload.get("metadata") or {}),
                        auto_resume=bool(payload.get("auto_resume", True)),
                    )
                )
                return self._json(200, bundle)
            if path.startswith("/api/runs/") and path.endswith("/events") and method == "GET":
                run_id = path.split("/")[3]
                return self._json(200, {"items": self.container.store.list_events(run_id)})
            if method == "GET" and path == "/api/task-threads":
                return self._json(
                    200,
                    {
                        "items": self.container.store.list_task_threads(
                            team_definition_id=self._optional_str(query.get("team_definition_id")),
                            run_id=self._optional_str(query.get("run_id")),
                        )
                    },
                )
            if method == "GET" and path == "/api/message-events":
                return self._json(
                    200,
                    {
                        "items": self.container.store.list_message_events(
                            thread_id=self._optional_str(query.get("thread_id")),
                            run_id=self._optional_str(query.get("run_id")),
                        )
                    },
                )
            if path.startswith("/api/runs/") and method == "GET":
                run_id = path.rsplit("/", 1)[-1]
                bundle = self.container.store.get_run_bundle(run_id)
                if bundle is None:
                    raise AppError(404, "Run does not exist.")
                bundle["workspace_files"] = self._workspace_files_for_run(bundle["run"])
                return self._json(200, bundle)
            if method == "GET" and path == "/api/approvals":
                return self._json(
                    200,
                    {
                        "items": self.container.store.list_approvals(
                            run_id=self._optional_str(query.get("run_id")),
                            status=self._optional_str(query.get("status")),
                        )
                    },
                )
            if path.startswith("/api/approvals/") and path.endswith("/resolve") and method == "POST":
                approval_id = path.split("/")[3]
                payload = self._parse_json(body)
                approval = self.container.store.resolve_approval(
                    approval_id,
                    approved=bool(payload.get("approved", True)),
                    comment=self._optional_str(payload.get("comment")),
                    metadata=dict(payload.get("metadata") or {}),
                )
                if approval is None:
                    raise AppError(404, "Approval does not exist.")
                return self._json(200, approval)
            if method == "GET" and path == "/api/workspace/files":
                run = self._require_run(query.get("run_id"))
                return self._json(200, {"items": self._workspace_files_for_run(run)})
            if method == "GET" and path == "/api/memory/search":
                workspace_id = self._optional_str(query.get("workspace_id"))
                project_id = self._optional_str(query.get("project_id"))
                run_id = self._optional_str(query.get("run_id"))
                agent_id = self._optional_str(query.get("agent_id"))
                team_id = self._optional_str(query.get("team_id"))
                if not workspace_id or not project_id or not run_id or not agent_id:
                    raise AppError(400, "workspace_id, project_id, run_id and agent_id are required.")
                from aiteams.memory.scope import MemoryScopes

                scope_builder = MemoryScopes(
                    workspace_id=workspace_id,
                    project_id=project_id,
                    run_id=run_id,
                    agent_id=agent_id,
                    team_id=team_id,
                )
                scope_mode = str(query.get("scope") or "combined").strip().lower()
                if scope_mode == "project_shared":
                    scopes = [scope_builder.project_shared()]
                elif scope_mode == "agent_private":
                    scopes = [scope_builder.agent_private()]
                elif scope_mode == "team_shared":
                    scopes = [scope_builder.team_shared()]
                elif scope_mode == "run_retrospective":
                    scopes = [scope_builder.run_retrospective()]
                else:
                    scopes = [scope_builder.agent_private(), scope_builder.project_shared()]
                results: list[dict[str, Any]] = []
                seen: set[str] = set()
                for scope in scopes:
                    recalled = asyncio.run(self.container.runtime.agent_kernel.memory.recall(scope, str(query.get("query") or ""), top_k=8))
                    for item in recalled:
                        key = str(item.get("head_id") or item.get("text") or "")
                        if key and key not in seen:
                            seen.add(key)
                            results.append(item)
                return self._json(200, {"items": results[:8]})
            raise AppError(404, "Not found.")
        except AppError as exc:
            return self._json(exc.status, {"detail": exc.detail, **exc.extra})
        except ValueError as exc:
            return self._json(400, {"detail": str(exc)})
        except Exception as exc:
            LOGGER.exception("Unhandled application error for %s %s", method, path)
            return self._json(500, {"detail": str(exc)})

    def _workspace_files_for_run(self, run: dict[str, Any]) -> list[dict[str, Any]]:
        return self.container.workspace.list_run_files(
            workspace_id=str(run["workspace_id"]),
            project_id=str(run["project_id"]),
            run_id=str(run["id"]),
        )

    def _team_chat_thread_resource(self, thread: dict[str, Any]) -> dict[str, Any]:
        metadata = dict((thread or {}).get("metadata_json") or {})
        payload = dict(thread or {})
        payload["session_thread_id"] = self._optional_str(metadata.get("session_thread_id"))
        payload["thread_id"] = payload["session_thread_id"]
        payload["team_definition_id"] = self._optional_str(metadata.get("team_definition_id")) or self._optional_str(payload.get("team_definition_id"))
        payload["team_name"] = self._optional_str(metadata.get("team_name"))
        payload["mode"] = self._optional_str(metadata.get("mode"))
        payload["last_run_id"] = self._optional_str(metadata.get("last_run_id"))
        payload["last_message_preview"] = self._optional_str(metadata.get("last_message_preview"))
        payload["last_message_at"] = self._optional_str(metadata.get("last_message_at"))
        return payload

    def _team_chat_response_text(self, bundle: dict[str, Any]) -> str:
        run = dict((bundle or {}).get("run") or {})
        result = dict(run.get("result_json") or {})
        state = dict(run.get("state_json") or {})
        final_message = dict(state.get("final_delivery_message") or {})
        candidates = [
            result.get("summary"),
            state.get("result_text"),
            final_message.get("body"),
            state.get("pending_result_text"),
        ]
        for candidate in candidates:
            text = self._optional_str(candidate)
            if text:
                return text
        artifacts = sorted(
            [dict(item) for item in list((bundle or {}).get("artifacts") or []) if isinstance(item, dict)],
            key=lambda item: (
                0 if str(item.get("name") or "").strip().lower() == "team-summary.md" else 1,
                0 if str(item.get("kind") or "").strip().lower() == "report" else 1,
                str(item.get("created_at") or ""),
            ),
        )
        for artifact in artifacts:
            artifact_text = self._team_chat_artifact_text(artifact)
            if artifact_text:
                return artifact_text
        return str(run.get("summary") or "").strip()

    def _team_chat_artifact_text(self, artifact: dict[str, Any]) -> str | None:
        artifact_path = self._optional_str((artifact or {}).get("path"))
        if not artifact_path:
            return None
        try:
            target = Path(artifact_path).expanduser().resolve()
            if not target.is_file() or target.stat().st_size > 1024 * 1024:
                return None
            text = target.read_text(encoding="utf-8").strip()
            return text or None
        except Exception:
            return None

    def _save_provider_profile(self, payload: dict[str, Any], *, provider_id: str | None) -> dict[str, Any]:
        existing = self.container.store.get_provider_profile(provider_id) if provider_id else None
        try:
            normalized = self.container.agent_center.prepare_provider_profile(payload, existing=existing)
        except KeyError as exc:
            supported = [str(item.get("provider_type") or "") for item in self.container.agent_center.provider_types()]
            raise AppError(
                400,
                "Provider 保存失败。",
                extra={
                    "error_type": "provider_validation_error",
                    "errors": [f"不支持的 API 模式：{payload.get('provider_type') or ''}。"],
                    "context": self._provider_error_context(payload, provider_id=provider_id),
                    "supported_provider_types": [item for item in supported if item],
                },
            ) from exc
        except ValueError as exc:
            raise AppError(
                400,
                "Provider 保存失败。",
                extra={
                    "error_type": "provider_validation_error",
                    "errors": [str(exc)],
                    "context": self._provider_error_context(payload, provider_id=provider_id),
                },
            ) from exc
        try:
            provider = self.container.store.save_provider_profile(
                provider_profile_id=provider_id,
                name=str(normalized["name"]),
                provider_type=str(normalized["provider_type"]),
                description=str(normalized["description"]),
                config=dict(normalized["config"]),
                secret=dict(normalized["secret"]) if normalized["secret"] else None,
            )
        except sqlite3.IntegrityError as exc:
            raise AppError(
                400,
                "Provider 保存失败。",
                extra={
                    "error_type": "provider_persistence_error",
                    "errors": [str(exc)],
                    "context": self._provider_error_context(payload, provider_id=provider_id),
                },
            ) from exc
        normalized_provider = self.container.agent_center.normalize_provider_profile(provider)
        assert normalized_provider is not None
        return normalized_provider

    def _provider_error_context(self, payload: dict[str, Any], *, provider_id: str | None) -> dict[str, Any]:
        config = dict(payload.get("config") or {})
        models = payload.get("models", config.get("models"))
        model_count = len(models) if isinstance(models, list) else 0
        return {
            "provider_id": provider_id,
            "name": self._optional_str(payload.get("name")),
            "provider_type": self._optional_str(payload.get("provider_type")),
            "base_url": self._optional_str(payload.get("base_url")) or self._optional_str(config.get("base_url")),
            "model_count": model_count,
            "skip_tls_verify": bool(payload.get("skip_tls_verify", config.get("skip_tls_verify"))),
            "has_secret": bool((payload.get("secret") or {}).get("api_key") or payload.get("api_key")),
        }

    def _local_models_root(self) -> Path:
        root = Path(self.container.local_models_root).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _local_model_absolute_path(self, model_path: Any) -> Path:
        value = str(model_path or "").replace("\\", "/").strip()
        root = self._local_models_root()
        if not value:
            return root
        candidate = Path(value).expanduser()
        if candidate.is_absolute():
            return candidate.resolve()
        parts = [part for part in PurePosixPath(value).parts if part and part not in {".", ".."}]
        if parts and parts[0] == root.name:
            return (root.parent / Path(*parts)).resolve()
        return root.joinpath(*parts).resolve() if parts else root

    def _local_model_relative_path(self, target_dir: Path) -> str:
        root = self._local_models_root()
        resolved = target_dir.expanduser().resolve()
        try:
            return resolved.relative_to(root.parent).as_posix()
        except ValueError:
            try:
                return resolved.relative_to(root).as_posix()
            except ValueError:
                return str(resolved)

    def _local_model_is_managed_path(self, target_dir: Path) -> bool:
        try:
            resolved = target_dir.expanduser().resolve()
            root = self._local_models_root()
            if resolved == root:
                return False
            resolved.relative_to(root)
            return True
        except ValueError:
            return False

    def _normalize_local_model_directory_name(self, raw_name: Any, *, fallback: str) -> str:
        name = trim_text(str(raw_name or "").replace("\\", "/").strip().strip("/"), limit=255)
        if "/" in name:
            name = name.split("/")[-1]
        if not name:
            return fallback
        relative = PurePosixPath(name)
        if relative.is_absolute() or not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
            return fallback
        return name

    def _normalize_local_model_upload_path(self, raw_path: Any) -> PurePosixPath:
        value = str(raw_path or "").replace("\\", "/").strip()
        if not value:
            raise AppError(400, "上传文件缺少 path。")
        relative_path = PurePosixPath(value)
        if relative_path.is_absolute() or not relative_path.parts or any(part in {"", ".", ".."} for part in relative_path.parts):
            raise AppError(400, f"无效的模型文件路径：{value}")
        return relative_path

    def _write_uploaded_local_model_bundle(
        self,
        payload: dict[str, Any],
        root_path: Path,
        *,
        local_model_id: str,
        default_name: str,
    ) -> dict[str, Any]:
        raw_files = payload.get("files")
        if not isinstance(raw_files, list) or not raw_files:
            raise AppError(400, "请先选择要上传的模型文件夹。")
        entries: list[dict[str, Any]] = []
        total_bytes = 0
        for index, item in enumerate(raw_files, start=1):
            if not isinstance(item, dict):
                raise AppError(400, f"文件 #{index} 必须是对象。")
            relative_path = self._normalize_local_model_upload_path(item.get("path"))
            encoded = item.get("content_base64")
            if not isinstance(encoded, str):
                raise AppError(400, f"文件 #{index} 缺少 content_base64。")
            try:
                content = base64.b64decode(encoded, validate=True)
            except Exception as exc:
                raise AppError(400, f"文件 #{index} 的内容不是合法 base64。") from exc
            total_bytes += len(content)
            entries.append(
                {
                    "relative_path": relative_path,
                    "content": content,
                }
            )
        has_nested_path = any(len(entry["relative_path"].parts) > 1 for entry in entries)
        first_parts = {entry["relative_path"].parts[0] for entry in entries if entry["relative_path"].parts}
        common_root = next(iter(first_parts)) if has_nested_path and len(first_parts) == 1 else ""
        fallback_name = slugify(default_name, fallback=f"local-model-{local_model_id[-8:]}")
        directory_name = self._normalize_local_model_directory_name(
            self._optional_str(payload.get("source_name")) or common_root,
            fallback=fallback_name,
        )
        written = 0
        for entry in entries:
            relative_path = entry["relative_path"]
            target_parts = relative_path.parts[1:] if common_root and relative_path.parts and relative_path.parts[0] == common_root else relative_path.parts
            if not target_parts:
                continue
            target = root_path.joinpath(*target_parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(entry["content"])
            written += 1
        if written <= 0:
            raise AppError(400, "模型文件夹内没有可写入的文件。")
        return {
            "directory_name": directory_name,
            "file_count": written,
            "total_bytes": total_bytes,
        }

    def _sync_uploaded_local_model(
        self,
        payload: dict[str, Any],
        *,
        existing: dict[str, Any] | None,
        local_model_id: str,
        model_name: str,
    ) -> dict[str, Any]:
        previous_model_path = self._optional_str((existing or {}).get("model_path"))
        previous_target_dir = self._local_model_absolute_path(previous_model_path) if previous_model_path else None
        with TemporaryDirectory(prefix="aiteams-local-model-upload-") as temp_dir:
            upload_info = self._write_uploaded_local_model_bundle(
                payload,
                Path(temp_dir),
                local_model_id=local_model_id,
                default_name=model_name,
            )
            target_dir = self._local_models_root() / upload_info["directory_name"]
            model_path = self._local_model_relative_path(target_dir)
            conflict = self.container.store.get_local_model_by_path(model_path)
            if conflict is not None and str(conflict.get("id") or "") != local_model_id:
                raise AppError(409, f"模型目录 `{model_path}` 已被本地模型“{conflict.get('name') or conflict.get('id') or ''}”使用。")
            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            target_dir.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copytree(Path(temp_dir), target_dir)
            except OSError as exc:
                raise AppError(500, f"复制模型目录失败：{exc}") from exc
            if previous_target_dir is not None and previous_target_dir != target_dir and self._local_model_is_managed_path(previous_target_dir) and previous_target_dir.exists():
                shutil.rmtree(previous_target_dir, ignore_errors=True)
            return {
                "model_path": model_path,
                "resolved_path": str(target_dir),
                "file_count": int(upload_info["file_count"]),
                "total_bytes": int(upload_info["total_bytes"]),
            }

    def _local_model_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = self.container.agent_center.normalize_local_model(record)
        assert payload is not None
        model_path = str(payload.get("model_path") or "").strip()
        resolved_path = self._local_model_absolute_path(model_path) if model_path else None
        payload["model_path"] = model_path
        payload["path_display"] = model_path or "-"
        payload["resolved_path"] = str(resolved_path) if resolved_path is not None else ""
        payload["exists"] = bool(resolved_path and resolved_path.exists())
        return payload

    def _local_model_page_payload(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        query: str | None = None,
        model_type: str | None = None,
    ) -> dict[str, Any]:
        page = self.container.store.list_local_models_page(limit=limit, offset=offset, query=query, model_type=model_type)
        return {
            "items": [self._local_model_resource(item) for item in page["items"]],
            "total": page["total"],
            "offset": page["offset"],
            "limit": page["limit"],
        }

    def _save_local_model(self, payload: dict[str, Any], *, local_model_id: str | None) -> dict[str, Any]:
        existing = self.container.store.get_local_model(local_model_id) if local_model_id else None
        record_id = local_model_id or self._optional_str(payload.get("id")) or make_uuid7()
        current_name = trim_text(payload.get("name") or (existing or {}).get("name") or "", limit=255)
        upload_result = None
        model_path = self._optional_str(payload.get("model_path")) or self._optional_str((existing or {}).get("model_path"))
        if isinstance(payload.get("files"), list) and payload.get("files"):
            upload_result = self._sync_uploaded_local_model(
                payload,
                existing=existing,
                local_model_id=record_id,
                model_name=current_name or self._optional_str(payload.get("source_name")) or record_id,
            )
            model_path = str(upload_result.get("model_path") or "")
        try:
            normalized = self.container.agent_center.prepare_local_model(
                {
                    **dict(payload),
                    "name": current_name,
                    "model_path": model_path,
                },
                existing=existing,
            )
        except ValueError as exc:
            raise AppError(400, str(exc)) from exc
        conflict = self.container.store.get_local_model_by_path(str(normalized["model_path"]))
        if conflict is not None and str(conflict.get("id") or "") != record_id:
            raise AppError(409, f"模型路径 `{normalized['model_path']}` 已被本地模型“{conflict.get('name') or conflict.get('id') or ''}”使用。")
        try:
            saved = self.container.store.save_local_model(
                local_model_id=record_id,
                name=str(normalized["name"]),
                model_type=str(normalized["model_type"]),
                model_path=str(normalized["model_path"]),
            )
        except sqlite3.IntegrityError as exc:
            raise AppError(400, "本地模型保存失败。", extra={"errors": [str(exc)]}) from exc
        resource = self._local_model_resource(saved)
        if upload_result is not None:
            resource["upload"] = {
                "file_count": int(upload_result["file_count"]),
                "total_bytes": int(upload_result["total_bytes"]),
            }
        return resource

    def _delete_local_model(self, local_model_id: str) -> dict[str, Any] | None:
        existing = self.container.store.get_local_model(local_model_id)
        if existing is None:
            return None
        deleted = self.container.store.delete_local_model(local_model_id)
        model_path = self._optional_str((existing or {}).get("model_path"))
        if model_path:
            target_dir = self._local_model_absolute_path(model_path)
            if self._local_model_is_managed_path(target_dir) and target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
        return deleted

    def _save_plugin(self, payload: dict[str, Any], *, plugin_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "key", "name")
        existing = self.container.store.get_plugin(plugin_id, include_secret=True) if plugin_id else None
        install_path = self._optional_str(payload.get("install_path"))
        incoming_manifest = dict(payload.get("manifest") or {})
        manifest = dict((existing or {}).get("manifest_json") or {})
        manifest.update(incoming_manifest)
        if install_path:
            try:
                package = self.container.plugins.validate_package(install_path)
                package_manifest = dict(package.get("manifest") or {})
                manifest = dict(package_manifest)
                for field in ("workbench_key", "tools", "permissions", "description"):
                    if field in incoming_manifest:
                        manifest[field] = incoming_manifest[field]
            except Exception:
                pass
        config = payload.get("config")
        if config is None:
            config = dict((existing or {}).get("config_json") or {})
        else:
            config = dict(config or {})

        secret_payload = payload.get("secret")
        secret: dict[str, Any] | None = None
        if secret_payload is not None:
            secret = dict((existing or {}).get("secret_json") or {})
            for field, value in dict(secret_payload or {}).items():
                field_name = str(field).strip()
                if not field_name:
                    continue
                if value in (None, ""):
                    continue
                secret[field_name] = value

        saved = self.container.store.save_plugin(
            plugin_id=plugin_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            version=str(payload.get("version") or "v1"),
            plugin_type=str(payload.get("plugin_type") or "toolset"),
            description=str(payload.get("description") or ""),
            manifest=manifest,
            config=config,
            install_path=install_path,
            secret=secret,
        )
        if saved.get("id") and self.container.plugins.snapshot(str(saved["id"])).get("running"):
            self.container.plugins.reload_plugin(str(saved["id"]))
            refreshed = self.container.store.get_plugin(str(saved["id"]))
            if refreshed is not None:
                saved = refreshed
        return saved

    def _normalize_skill_spec(self, spec: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(spec or {})
        instructions_source = normalized.get("instructions") or []
        if isinstance(instructions_source, str):
            instructions_source = str(instructions_source).splitlines()
        instructions = [trim_text(item) for item in instructions_source if trim_text(item)]

        plugins_source = normalized.get("recommended_plugins") or []
        if isinstance(plugins_source, str):
            plugins_source = str(plugins_source).splitlines()
        recommended_plugins = [trim_text(item) for item in plugins_source if trim_text(item)]

        group_refs = self._normalize_skill_group_refs(
            normalized.get("group_refs"),
            legacy_group_id=normalized.get("group_id"),
            legacy_group_key=normalized.get("group_key"),
            legacy_group_name=normalized.get("group_name"),
        )

        normalized["instructions"] = instructions
        normalized["recommended_plugins"] = recommended_plugins
        if group_refs:
            normalized["group_refs"] = group_refs
        else:
            normalized.pop("group_refs", None)
        normalized.pop("group_id", None)
        normalized.pop("group_key", None)
        normalized.pop("group_name", None)
        normalized.pop("group_order", None)
        return normalized

    def _normalize_skill_group_refs(
        self,
        refs_source: Any,
        *,
        legacy_group_id: Any = "",
        legacy_group_key: Any = "",
        legacy_group_name: Any = "",
    ) -> list[dict[str, str]]:
        candidates: list[Any] = []
        if isinstance(refs_source, list):
            candidates.extend(refs_source)
        if any(item not in (None, "") for item in (legacy_group_id, legacy_group_key, legacy_group_name)):
            candidates.append(
                {
                    "id": legacy_group_id,
                    "key": legacy_group_key,
                    "name": legacy_group_name,
                }
            )
        normalized: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in candidates:
            if not isinstance(item, dict):
                continue
            group_id = trim_text(item.get("id") or item.get("group_id") or "")
            group_key = trim_text(item.get("key") or item.get("group_key") or "")
            group_name = trim_text(item.get("name") or item.get("group_name") or "")
            if group_name and not group_key:
                group_key = slugify(group_name, fallback="skill-group")
            if group_key and not group_name:
                group_name = group_key
            if not group_id and not group_key:
                continue
            token = group_id or group_key
            if token in seen:
                continue
            seen.add(token)
            payload: dict[str, str] = {}
            if group_id:
                payload["id"] = group_id
            if group_key:
                payload["key"] = group_key
            if group_name:
                payload["name"] = group_name
            normalized.append(payload)
        return normalized

    def _resolve_skill_group_records(self, group_ids: Any) -> list[dict[str, Any]]:
        if group_ids in (None, ""):
            raw_group_ids: list[Any] = []
        elif isinstance(group_ids, list):
            raw_group_ids = group_ids
        else:
            raise AppError(400, "Field 'group_ids' must be an array.")
        resolved: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in raw_group_ids:
            group_id = self._optional_str(item)
            if not group_id or group_id in seen:
                continue
            group = self.container.store.get_skill_group(group_id)
            if group is None:
                raise AppError(400, f"Selected skill group does not exist: {group_id}")
            seen.add(group_id)
            resolved.append(group)
        return resolved

    def _skill_group_refs_payload(self, groups: list[dict[str, Any]]) -> list[dict[str, str]]:
        payload: list[dict[str, str]] = []
        for group in groups:
            group_id = str(group.get("id") or "").strip()
            group_key = str(group.get("key") or "").strip()
            group_name = str(group.get("name") or group_key).strip()
            if not group_id and not group_key:
                continue
            item: dict[str, str] = {}
            if group_id:
                item["id"] = group_id
            if group_key:
                item["key"] = group_key
            if group_name:
                item["name"] = group_name
            payload.append(item)
        return payload

    def _resolve_skill_groups_from_spec(self, normalized_spec: dict[str, Any]) -> list[dict[str, Any]]:
        refs = self._normalize_skill_group_refs(
            normalized_spec.get("group_refs"),
            legacy_group_id=normalized_spec.get("group_id"),
            legacy_group_key=normalized_spec.get("group_key"),
            legacy_group_name=normalized_spec.get("group_name"),
        )
        resolved: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ref in refs:
            group_id = self._optional_str(ref.get("id"))
            group_key = self._optional_str(ref.get("key"))
            group_name = self._optional_str(ref.get("name"))
            group = None
            if group_id:
                group = self.container.store.get_skill_group(group_id)
                if group is None:
                    raise AppError(400, f"Selected skill group does not exist: {group_id}")
            elif group_key:
                group = self.container.store.get_skill_group_by_key(group_key)
                if group is None:
                    group = self.container.store.save_skill_group(
                        skill_group_id=None,
                        key=group_key,
                        name=group_name or group_key,
                        description="",
                    )
            if group is None:
                continue
            token = str(group.get("id") or group.get("key") or "").strip()
            if not token or token in seen:
                continue
            seen.add(token)
            resolved.append(group)
        return resolved

    def _skill_group_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = dict(record)
        payload.pop("sort_order", None)
        if "count" in record:
            payload["count"] = int(record.get("count", 0) or 0)
        else:
            payload["count"] = next(
                (int(item.get("count", 0) or 0) for item in self.container.store.list_skill_groups() if str(item.get("key") or "") == str(record.get("key") or "")),
                0,
            )
        return payload

    def _skill_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = dict(record)
        groups = self.container.store.resolve_skill_groups(record)
        primary_group = groups[0] if groups else None
        payload["groups"] = [
            {
                "id": str(item.get("id") or ""),
                "key": str(item.get("key") or ""),
                "name": str(item.get("name") or item.get("key") or ""),
            }
            for item in groups
        ]
        payload["group_ids"] = [str(item.get("id") or "") for item in groups if str(item.get("id") or "").strip()]
        payload["group_keys"] = [str(item.get("key") or "") for item in groups if str(item.get("key") or "").strip()]
        payload["group_names"] = [str(item.get("name") or item.get("key") or "") for item in groups if str(item.get("name") or item.get("key") or "").strip()]
        payload["group_id"] = str((primary_group or {}).get("id") or "")
        payload["group_key"] = str((primary_group or {}).get("key") or "")
        payload["group_name"] = str((primary_group or {}).get("name") or (primary_group or {}).get("key") or "")
        return payload

    def _skill_scan_issue_resource(self, issue: SkillValidationIssue) -> dict[str, Any]:
        return {
            "severity": issue.severity,
            "code": issue.code,
            "message": issue.message,
            "path": issue.path,
        }

    def _skill_import_key(self, name: str) -> str:
        return f"skill.{slugify(name, fallback='skill')}"

    def _normalize_skill_storage_path(self, raw_path: Any) -> str:
        value = str(raw_path or "").replace("\\", "/").strip("/")
        parts = [part for part in value.split("/") if part and part not in {".", ".."}]
        return "/".join(parts)

    def _deepagents_skill_library_root(self) -> Path:
        root = Path(self.container.runtime.deepagents_skill_root).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _deepagents_skill_directory_name(self, skill: ValidatedSkill) -> str:
        metadata = skill.metadata
        name = trim_text((metadata.name if metadata is not None else "") or skill.directory_path.name, limit=128)
        if not name:
            raise AppError(400, "Validated skill is missing a directory name for backend sync.")
        if PurePosixPath(name).name != name or any(part in {"", ".", ".."} for part in PurePosixPath(name).parts):
            raise AppError(400, f"Invalid deepagents skill directory name: {name}")
        return name

    def _deepagents_skill_storage_path(self, *, directory_name: str) -> str:
        return PurePosixPath(directory_name).as_posix()

    def _deepagents_skill_library_target(self, storage_path: str) -> Path:
        normalized = self._normalize_skill_storage_path(storage_path)
        return self._deepagents_skill_library_root().joinpath(*normalized.split("/"))

    def _skill_package_root(self, skill_record: dict[str, Any]) -> Path:
        storage_path = self._normalize_skill_storage_path(skill_record.get("storage_path"))
        if storage_path:
            candidate = self._deepagents_skill_library_target(storage_path)
            if candidate.exists() and candidate.is_dir():
                return candidate
        raise AppError(404, "Skill package files are not available.")

    def _normalize_skill_file_relative_path(self, raw_path: Any) -> PurePosixPath:
        value = str(raw_path or "").replace("\\", "/").strip()
        if not value:
            raise AppError(400, "Field 'path' is required.")
        relative_path = PurePosixPath(value)
        if relative_path.is_absolute() or not relative_path.parts or any(part in {"", ".", ".."} for part in relative_path.parts):
            raise AppError(400, "Invalid skill file path.")
        return relative_path

    def _skill_files_resource(self, skill_record: dict[str, Any]) -> dict[str, Any]:
        root = self._skill_package_root(skill_record)
        items: list[dict[str, Any]] = []
        for file_path in sorted((item for item in root.rglob("*") if item.is_file()), key=lambda item: item.relative_to(root).as_posix().lower()):
            relative = file_path.relative_to(root).as_posix()
            try:
                size = int(file_path.stat().st_size)
            except OSError:
                size = 0
            suffix = file_path.suffix.lower()
            items.append(
                {
                    "path": relative,
                    "name": file_path.name,
                    "size": size,
                    "is_markdown": suffix in {".md", ".mdx"},
                    "language": suffix.lstrip("."),
                }
            )
        return {
            "root_path": str(root),
            "items": items,
        }

    def _skill_file_content_resource(self, skill_record: dict[str, Any], relative_path: Any) -> dict[str, Any]:
        root = self._skill_package_root(skill_record)
        normalized_path = self._normalize_skill_file_relative_path(relative_path)
        target = root.joinpath(*normalized_path.parts).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise AppError(400, "Invalid skill file path.") from exc
        if not target.exists() or not target.is_file():
            raise AppError(404, "Skill file does not exist.")

        max_preview_bytes = 1024 * 1024
        try:
            total_size = int(target.stat().st_size)
        except OSError:
            total_size = 0
        try:
            with target.open("rb") as file_handle:
                raw = file_handle.read(max_preview_bytes)
        except OSError as exc:
            raise AppError(500, f"Failed to read skill file: {exc}") from exc

        truncated = total_size > max_preview_bytes
        try:
            content = raw.decode("utf-8")
            is_text = True
            message = ""
        except UnicodeDecodeError:
            content = ""
            is_text = False
            message = "该文件不是 UTF-8 文本，暂不支持预览。"

        suffix = target.suffix.lower()
        return {
            "path": normalized_path.as_posix(),
            "name": target.name,
            "size": total_size,
            "is_text": is_text,
            "content": content,
            "message": message,
            "truncated": truncated,
            "is_markdown": suffix in {".md", ".mdx"},
            "language": suffix.lstrip("."),
        }

    def _sync_validated_skill_to_deepagents_library(
        self,
        skill: ValidatedSkill,
        *,
        existing: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        metadata = skill.metadata
        if metadata is None:
            raise AppError(400, f"Skill metadata is missing for '{skill.directory_path}'.")
        source_dir = skill.directory_path.expanduser().resolve()
        directory_name = self._deepagents_skill_directory_name(skill)
        storage_path = self._deepagents_skill_storage_path(directory_name=directory_name)
        target_dir = self._deepagents_skill_library_target(storage_path)
        previous_storage_path = self._normalize_skill_storage_path((existing or {}).get("storage_path"))
        previous_target_dir = self._deepagents_skill_library_target(previous_storage_path) if previous_storage_path else None

        if source_dir != target_dir:
            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            target_dir.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copytree(source_dir, target_dir)
            except OSError as exc:
                raise AppError(500, f"Failed to sync skill package to deepagents backend: {exc}") from exc
        elif not target_dir.exists():
            raise AppError(500, f"Deepagents skill backend path does not exist: {target_dir}")

        if previous_target_dir is not None and previous_target_dir != target_dir and previous_target_dir.exists():
            shutil.rmtree(previous_target_dir, ignore_errors=True)

        return {
            "storage_path": storage_path,
            "filesystem_path": str(target_dir),
            "filesystem_skill_md_path": str(target_dir / "SKILL.md"),
        }

    def _delete_deepagents_skill_package(self, skill_record: dict[str, Any] | None) -> None:
        if skill_record is None:
            return
        storage_path = self._normalize_skill_storage_path(skill_record.get("storage_path"))
        if not storage_path:
            return
        target_dir = self._deepagents_skill_library_target(storage_path)
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)

    def _find_skill_import_target(self, skill: ValidatedSkill) -> dict[str, Any] | None:
        metadata = skill.metadata
        if metadata is None:
            return None
        return self.container.store.get_skill_by_name(metadata.name)

    def _validated_skill_scan_resource(self, skill: ValidatedSkill) -> dict[str, Any]:
        metadata = skill.metadata
        existing = self._find_skill_import_target(skill) if metadata is not None else None
        return {
            "directory_path": str(skill.directory_path),
            "skill_md_path": str(skill.skill_md_path),
            "is_valid": skill.is_valid,
            "files": [item.as_posix() for item in skill.files],
            "helper_files": [item.as_posix() for item in skill.helper_files],
            "body_preview": trim_text(skill.body, limit=240),
            "metadata": (
                {
                    "name": metadata.name,
                    "description": metadata.description,
                    "path": metadata.path,
                    "license": metadata.license,
                    "compatibility": metadata.compatibility,
                    "metadata": dict(metadata.metadata),
                    "allowed_tools": list(metadata.allowed_tools),
                }
                if metadata is not None
                else None
            ),
            "existing_skill_id": str(existing.get("id") or "") if existing is not None else "",
            "existing_skill_name": str(existing.get("name") or "") if existing is not None else "",
            "issues": [self._skill_scan_issue_resource(item) for item in skill.issues],
        }

    def _skill_library_scan_payload(self, scan: SkillLibraryScan, *, recursive: bool) -> dict[str, Any]:
        return {
            "source_path": str(scan.root_path),
            "recursive": recursive,
            "valid": scan.is_valid,
            "skill_count": len(scan.skills),
            "valid_skill_count": len(scan.valid_skills),
            "issues": [self._skill_scan_issue_resource(item) for item in scan.issues],
            "skills": [self._validated_skill_scan_resource(item) for item in scan.skills],
        }

    def _scan_skill_library_resource(self, source_path: str, *, recursive: bool) -> dict[str, Any]:
        scan = scan_skill_library(source_path, recursive=recursive)
        payload = self._skill_library_scan_payload(scan, recursive=recursive)
        payload["message"] = f"Scanned {payload['skill_count']} skill(s)."
        return payload

    def _write_uploaded_skill_library(self, payload: dict[str, Any], root_path: Path) -> dict[str, Any]:
        raw_files = payload.get("files")
        if not isinstance(raw_files, list) or not raw_files:
            raise AppError(400, "Field 'files' must be a non-empty array.")
        total_bytes = 0
        written = 0
        for index, item in enumerate(raw_files, start=1):
            if not isinstance(item, dict):
                raise AppError(400, f"File #{index} must be an object.")
            raw_path = str(item.get("path") or "").replace("\\", "/").strip()
            if not raw_path:
                raise AppError(400, f"File #{index} is missing 'path'.")
            relative_path = PurePosixPath(raw_path)
            if relative_path.is_absolute() or not relative_path.parts or any(part in {"", ".", ".."} for part in relative_path.parts):
                raise AppError(400, f"File #{index} has an invalid relative path.")
            encoded = item.get("content_base64")
            if not isinstance(encoded, str):
                raise AppError(400, f"File #{index} is missing 'content_base64'.")
            try:
                content = base64.b64decode(encoded, validate=True)
            except Exception as exc:
                raise AppError(400, f"File #{index} has invalid base64 content.") from exc
            total_bytes += len(content)
            if total_bytes > 50 * 1024 * 1024:
                raise AppError(400, "Uploaded skill library exceeds 50 MB.")
            target = root_path.joinpath(*relative_path.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            written += 1
        return {
            "file_count": written,
            "total_bytes": total_bytes,
            "source_name": self._optional_str(payload.get("source_name")) or root_path.name,
        }

    def _scan_uploaded_skill_library(self, payload: dict[str, Any], *, recursive: bool) -> dict[str, Any]:
        with TemporaryDirectory(prefix="aiteams-skill-upload-") as temp_dir:
            upload_info = self._write_uploaded_skill_library(payload, Path(temp_dir))
            scanned = self._scan_skill_library_resource(temp_dir, recursive=recursive)
            scanned["uploaded_file_count"] = upload_info["file_count"]
            scanned["uploaded_total_bytes"] = upload_info["total_bytes"]
            scanned["source_name"] = upload_info["source_name"]
            return scanned

    def _import_uploaded_skill_library(self, payload: dict[str, Any], *, recursive: bool) -> dict[str, Any]:
        with TemporaryDirectory(prefix="aiteams-skill-upload-") as temp_dir:
            upload_info = self._write_uploaded_skill_library(payload, Path(temp_dir))
            imported = self._import_skill_library(
                temp_dir,
                recursive=recursive,
                group_ids=payload.get("group_ids"),
                target_skill_id=self._optional_str(payload.get("target_skill_id")),
            )
            imported["uploaded_file_count"] = upload_info["file_count"]
            imported["uploaded_total_bytes"] = upload_info["total_bytes"]
            imported["source_name"] = upload_info["source_name"]
            return imported

    def _import_validated_skill(
        self,
        skill: ValidatedSkill,
        *,
        selected_groups: list[dict[str, Any]] | None = None,
        target_skill: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = skill.metadata
        if metadata is None:
            raise AppError(400, f"Skill metadata is missing for '{skill.directory_path}'.")
        existing = target_skill or self._find_skill_import_target(skill)
        skill_id = str((existing or {}).get("id") or "").strip() or make_uuid7()
        conflict = self.container.store.get_skill_by_name(metadata.name)
        if conflict is not None and str(conflict.get("id") or "") != skill_id:
            raise AppError(409, f"Skill name '{metadata.name}' already exists.")
        backend_sync = self._sync_validated_skill_to_deepagents_library(skill, existing=existing)
        saved = self.container.store.save_skill(
            skill_id=skill_id,
            name=metadata.name,
            description=metadata.description,
            storage_path=backend_sync["storage_path"],
        )
        if selected_groups is not None:
            self.container.store.replace_skill_group_memberships(
                str(saved.get("id") or ""),
                [str(item.get("id") or "") for item in selected_groups if str(item.get("id") or "").strip()],
            )
        return {
            "created": existing is None,
            "updated": existing is not None,
            "directory_path": str(skill.directory_path),
            "skill": self._skill_resource(saved),
        }

    def _import_skill_library(
        self,
        source_path: str,
        *,
        recursive: bool,
        group_ids: Any = None,
        target_skill_id: str | None = None,
    ) -> dict[str, Any]:
        scan = scan_skill_library(source_path, recursive=recursive)
        imported: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        seen_skill_names: set[str] = set()
        selected_groups = self._resolve_skill_group_records(group_ids)
        target_skill = self.container.store.get_skill(target_skill_id) if target_skill_id else None
        if target_skill_id and target_skill is None:
            raise AppError(404, "Skill does not exist.")
        if target_skill is not None:
            valid_skills = [item for item in scan.skills if item.is_valid and item.metadata is not None]
            if len(valid_skills) != 1:
                raise AppError(400, "重新上传时必须只包含 1 个有效 Skill 文件夹。")
            imported.append(self._import_validated_skill(valid_skills[0], selected_groups=selected_groups, target_skill=target_skill))
            return {
                "message": f"Updated 1 skill from '{scan.root_path}'.",
                "source_path": str(scan.root_path),
                "recursive": recursive,
                "valid": scan.is_valid,
                "imported_count": len(imported),
                "skipped_count": len(skipped),
                "imported": imported,
                "skipped": skipped,
                "scan": self._skill_library_scan_payload(scan, recursive=recursive),
            }

        for skill in scan.skills:
            metadata = skill.metadata
            if not skill.is_valid or metadata is None:
                skipped.append(
                    {
                        "directory_path": str(skill.directory_path),
                        "reason": "invalid-skill",
                        "issues": [self._skill_scan_issue_resource(item) for item in skill.issues],
                    }
                )
                continue
            skill_name = str(metadata.name or "").strip()
            if skill_name in seen_skill_names:
                skipped.append(
                    {
                        "directory_path": str(skill.directory_path),
                        "reason": "duplicate-skill-name",
                        "name": skill_name,
                    }
                )
                continue
            seen_skill_names.add(skill_name)
            imported.append(self._import_validated_skill(skill, selected_groups=selected_groups))

        return {
            "message": f"Imported {len(imported)} skill(s) from '{scan.root_path}'.",
            "source_path": str(scan.root_path),
            "recursive": recursive,
            "valid": scan.is_valid,
            "imported_count": len(imported),
            "skipped_count": len(skipped),
            "imported": imported,
            "skipped": skipped,
            "scan": self._skill_library_scan_payload(scan, recursive=recursive),
        }

    def _save_skill_group(self, payload: dict[str, Any], *, skill_group_id: str | None) -> dict[str, Any]:
        existing = self.container.store.get_skill_group(skill_group_id) if skill_group_id else None
        name = trim_text(payload.get("name") or (existing or {}).get("name") or "")
        if not name:
            raise AppError(400, "Field 'name' is required.")
        # `key` is now internal-only. Preserve existing keys on edit and auto-generate on create.
        key = trim_text((existing or {}).get("key") or "")
        if not key:
            key = slugify(name, fallback="skill-group")
            conflict = self.container.store.get_skill_group_by_key(key)
            if conflict is not None and str(conflict.get("id") or "") != str((existing or {}).get("id") or ""):
                key = f"{key}-{make_uuid7()[-8:]}"
        description = str(payload["description"]) if "description" in payload else str((existing or {}).get("description") or "")
        previous_key = str((existing or {}).get("key") or "")
        skill_ids: list[str] | None = None
        if "skill_ids" in payload:
            raw_skill_ids = payload.get("skill_ids")
            if raw_skill_ids in (None, ""):
                raw_skill_ids = []
            if not isinstance(raw_skill_ids, list):
                raise AppError(400, "Field 'skill_ids' must be an array.")
            skill_ids = []
            seen_skill_ids: set[str] = set()
            for item in raw_skill_ids:
                skill_id = self._optional_str(item)
                if not skill_id or skill_id in seen_skill_ids:
                    continue
                if self.container.store.get_skill(skill_id) is None:
                    raise AppError(400, f"Selected skill does not exist: {skill_id}")
                seen_skill_ids.add(skill_id)
                skill_ids.append(skill_id)
        saved = self.container.store.save_skill_group(
            skill_group_id=skill_group_id,
            key=key,
            name=name,
            description=description,
        )
        self.container.store.sync_skill_group_assignments(skill_group=saved, previous_key=previous_key)
        if skill_ids is not None:
            self.container.store.set_skill_group_members(skill_group=saved, skill_ids=skill_ids)
        return self._skill_group_resource(saved)

    def _save_skill(self, payload: dict[str, Any], *, skill_id: str | None) -> dict[str, Any]:
        existing = self.container.store.get_skill(skill_id) if skill_id else None
        name = trim_text(payload.get("name") or (existing or {}).get("name") or "")
        if not name:
            raise AppError(400, "Field 'name' is required.")
        description = str(payload["description"]) if "description" in payload else str((existing or {}).get("description") or "")
        storage_path = self._normalize_skill_storage_path(payload.get("storage_path") or (existing or {}).get("storage_path") or "")
        if not storage_path:
            raise AppError(400, "Field 'storage_path' is required.")
        if "group_ids" in payload:
            resolved_groups = self._resolve_skill_group_records(payload.get("group_ids"))
        else:
            resolved_groups = self.container.store.resolve_skill_groups(existing or {})
        saved = self.container.store.save_skill(
            skill_id=skill_id,
            name=name,
            description=description,
            storage_path=storage_path,
        )
        self.container.store.replace_skill_group_memberships(
            str(saved.get("id") or ""),
            [str(item.get("id") or "") for item in resolved_groups if str(item.get("id") or "").strip()],
        )
        return self._skill_resource(saved)

    def _save_static_memory(self, payload: dict[str, Any], *, static_memory_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "name")
        existing = self.container.store.get_static_memory(static_memory_id) if static_memory_id else None
        generated_key = f"role_spec.{slugify(str(payload.get('name') or 'role_spec'), fallback='role-spec')}.{make_id('rs').split('_')[-1]}"
        saved = self.container.store.save_static_memory(
            static_memory_id=static_memory_id,
            key=str(payload.get("key") or (existing or {}).get("key") or generated_key),
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=normalize_role_spec(dict(payload.get("spec") or {})),
        )
        return self._static_memory_resource(saved)

    def _static_memory_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = dict(record)
        payload["spec_json"] = normalize_role_spec(dict(record.get("spec_json") or {}))
        return payload

    def _knowledge_base_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        return self.container.knowledge_bases._knowledge_base_resource(record)

    def _knowledge_base_page_payload(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        query: str | None = None,
    ) -> dict[str, Any]:
        page = self.container.store.list_knowledge_bases_page(limit=limit, offset=offset, query=query)
        return {
            "items": [self._knowledge_base_resource(item) for item in page["items"]],
            "total": page["total"],
            "offset": page["offset"],
            "limit": page["limit"],
        }

    def _knowledge_document_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        return self.container.knowledge_bases._document_resource(record)

    def _save_knowledge_base(self, payload: dict[str, Any], *, knowledge_base_id: str | None) -> dict[str, Any]:
        existing = self.container.store.get_knowledge_base(knowledge_base_id) if knowledge_base_id else None
        record_id = knowledge_base_id or self._optional_str(payload.get("id")) or make_uuid7()
        name = trim_text(payload.get("name") or (existing or {}).get("name") or "")
        if not name:
            raise AppError(400, "Field 'name' is required.")
        key = self._optional_str(payload.get("key")) or self._optional_str((existing or {}).get("key")) or record_id
        saved = self.container.store.save_knowledge_base(
            knowledge_base_id=record_id,
            key=key,
            name=name,
            config=dict(payload.get("config") or (existing or {}).get("config_json") or {}),
        )
        return self._knowledge_base_resource(saved)

    def _save_knowledge_document(self, payload: dict[str, Any], *, knowledge_document_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "knowledge_base_id")
        previous = self.container.store.get_knowledge_document(knowledge_document_id) if knowledge_document_id else None
        knowledge_base_id = str(payload["knowledge_base_id"])
        document_id = knowledge_document_id or self._optional_str(payload.get("id"))
        source_path = self._optional_str(payload.get("source_path"))
        title = trim_text(payload.get("title") or (previous or {}).get("title") or source_path or "文档")
        key = (
            self._optional_str(payload.get("key"))
            or self._optional_str((previous or {}).get("key"))
            or hashlib.sha1(str(source_path or title or make_uuid7()).encode("utf-8")).hexdigest()
        )
        saved = self.container.store.save_knowledge_document(
            knowledge_document_id=document_id,
            knowledge_base_id=knowledge_base_id,
            key=key,
            title=title,
            source_path=source_path,
            content_text=str(payload.get("content_text") or ""),
            metadata=dict(payload.get("metadata") or {}),
        )
        synced = self.container.knowledge_bases.sync_document(saved, previous_document=previous)
        return self._knowledge_document_resource(synced)

    def _import_knowledge_base_uploaded_files(self, knowledge_base_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            result = self.container.knowledge_bases.import_uploaded_files(knowledge_base_id, payload)
        except ValueError as exc:
            raise AppError(400, str(exc)) from exc
        return result

    def _save_review_policy(self, payload: dict[str, Any], *, review_policy_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "key", "name")
        return self.container.store.save_review_policy(
            review_policy_id=review_policy_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=dict(payload.get("spec") or {}),
        )

    def _save_agent_definition(self, payload: dict[str, Any], *, definition_id: str | None) -> dict[str, Any]:
        if "key" in payload:
            raise AppError(400, "Agent definition no longer accepts `key`; use the server-generated `id`.")
        self._require_fields(payload, "name")
        existing = self.container.store.get_agent_definition(definition_id) if definition_id else None
        existing_spec = dict((existing or {}).get("spec_json") or {})
        spec = dict(existing_spec)
        spec.update(dict(payload.get("spec") or {}))
        plugin_refs = [str(item).strip() for item in list(spec.get("tool_plugin_refs") or spec.get("plugin_refs") or []) if str(item).strip()]
        spec["tool_plugin_refs"] = list(dict.fromkeys(plugin_refs))
        spec.pop("memory_profile_ref", None)
        spec.pop("memory_profile_id", None)
        spec.pop("memory_profile", None)
        static_memory_ref = self._optional_str(
            spec.get("role_spec_ref") or spec.get("role_spec_id") or spec.get("static_memory_ref") or spec.get("static_memory_id")
        )
        derived_role = None
        if static_memory_ref:
            static_memory = self.container.store.get_static_memory(static_memory_ref) or self.container.store.get_static_memory_by_key(static_memory_ref)
            if static_memory is not None:
                derived_role = self._optional_str(static_memory.get("name")) or self._optional_str(static_memory.get("key"))
        return self.container.store.save_agent_definition(
            agent_definition_id=definition_id,
            name=str(payload["name"]),
            role=self._optional_str(payload.get("role")) or derived_role or self._optional_str((existing or {}).get("role")),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=spec,
        )

    def _save_team_definition(self, payload: dict[str, Any], *, definition_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "name")
        existing = self.container.store.get_team_definition(definition_id) if definition_id else None
        existing_spec = dict((existing or {}).get("spec_json") or {})
        incoming_spec = dict(payload.get("spec") or {})
        spec = dict(existing_spec)
        spec.update(incoming_spec)
        store = self.container.store

        def _resolve_agent_definition_ref(reference: str) -> str:
            definition = store.get_agent_definition(reference)
            if definition is None:
                for item in store.list_agent_definitions():
                    if str(item.get("name") or "") == reference:
                        definition = item
                        break
            if definition is None:
                raise AppError(400, f"Unknown agent definition `{reference}`.")
            return str(definition.get("id") or reference)

        def _resolve_team_definition_ref(reference: str) -> str:
            team_definition = store.get_team_definition(reference) or store.get_team_definition_by_key(reference)
            if team_definition is None:
                for item in store.list_team_definitions():
                    if str(item.get("name") or "") == reference:
                        team_definition = item
                        break
            if team_definition is None:
                raise AppError(400, f"Unknown team definition `{reference}`.")
            team_spec = dict(team_definition.get("spec_json") or {})
            if not isinstance(team_spec.get("root"), dict) and not isinstance(team_spec.get("lead"), dict):
                raise AppError(400, f"Nested team `{reference}` is not a deep hierarchy team definition.")
            return str(team_definition.get("id") or reference)

        def _normalize_child_node(node: dict[str, Any], *, index: int) -> dict[str, Any]:
            source_kind = self._optional_str(node.get("source_kind"))
            if not source_kind:
                source_kind = "team_definition" if self._optional_str(node.get("team_definition_ref") or node.get("team_definition_id")) else "agent_definition"
            if source_kind == "team_definition":
                team_ref = self._optional_str(node.get("team_definition_ref") or node.get("team_definition_id") or node.get("source_ref"))
                if not team_ref:
                    raise AppError(400, f"Child #{index} requires team_definition_ref.")
                team_ref = _resolve_team_definition_ref(team_ref)
                if definition_id and team_ref == definition_id:
                    raise AppError(400, "Team definition cannot reference itself as a child team.")
                normalized = {
                    "kind": "team",
                    "source_kind": "team_definition",
                    "team_definition_ref": team_ref,
                }
            elif source_kind == "agent_definition":
                definition_ref = self._optional_str(node.get("agent_definition_ref") or node.get("agent_definition_id") or node.get("source_ref"))
                if not definition_ref:
                    raise AppError(400, f"Child #{index} requires agent_definition_ref.")
                definition_ref = _resolve_agent_definition_ref(definition_ref)
                normalized = {
                    "kind": "agent",
                    "source_kind": "agent_definition",
                    "agent_definition_ref": definition_ref,
                }
            else:
                raise AppError(400, f"Child #{index} has unsupported source_kind `{source_kind}`.")
            return normalized

        lead_payload = dict(spec.get("lead") or {})
        lead_definition_ref = self._optional_str(
            lead_payload.get("agent_definition_ref") or lead_payload.get("agent_definition_id") or lead_payload.get("source_ref")
        )
        if not lead_definition_ref:
            raise AppError(400, "Team definition requires a lead agent_definition_ref.")
        lead_definition_ref = _resolve_agent_definition_ref(lead_definition_ref)
        spec["lead"] = {
            "kind": "agent",
            "source_kind": "agent_definition",
            "agent_definition_ref": lead_definition_ref,
        }

        children_raw = spec.get("children") or []
        if not isinstance(children_raw, list):
            raise AppError(400, "Team definition children must be an array.")
        spec["children"] = [_normalize_child_node(dict(item or {}), index=index) for index, item in enumerate(children_raw, start=1)]
        if existing is not None:
            spec["workspace_id"] = self._optional_str(existing_spec.get("workspace_id")) or "local-workspace"
            spec["project_id"] = self._optional_str(existing_spec.get("project_id")) or "default-project"
        else:
            spec["workspace_id"] = self._optional_str(spec.get("workspace_id")) or "local-workspace"
            spec["project_id"] = self._optional_str(spec.get("project_id")) or "default-project"
        spec.pop("root", None)
        spec.pop("members", None)
        spec.pop("agents", None)
        spec.pop("review_policy_refs", None)
        spec.pop("review_overrides", None)
        spec.pop("shared_kb_bindings", None)
        spec.pop("shared_knowledge_base_refs", None)
        spec.pop("shared_knowledge_bases", None)
        spec.pop("shared_static_memory_bindings", None)
        spec.pop("shared_static_memory_refs", None)
        spec.pop("shared_static_memories", None)
        spec.pop("task_entry_policy", None)
        spec.pop("task_entry_agent", None)
        spec.pop("termination_policy", None)
        if "description" in payload:
            description = str(payload.get("description") or "")
        else:
            description = str((existing or {}).get("description") or "")
        return self.container.store.save_team_definition(
            team_definition_id=definition_id,
            key=self._optional_str(payload.get("key")) or self._optional_str((existing or {}).get("key")),
            name=str(payload["name"]),
            description=description,
            version=str(payload.get("version") or "v1"),
            spec=spec,
        )

    def _requirements_report(self) -> dict[str, Any]:
        summary = self.container.store.dashboard_summary()
        agent_memory = getattr(self.container.runtime.agent_kernel, "memory", None)
        memory_storage = agent_memory.storage_info() if hasattr(agent_memory, "storage_info") else {}
        runtime_storage = self.container.runtime.storage_info() if hasattr(self.container.runtime, "storage_info") else {}
        builtin_plugins = self.container.plugins.builtin_catalog()
        builtin_keys = {str(item.get("key") or "") for item in builtin_plugins}
        requirement_items = [
            {
                "id": "R1",
                "title": "资源目录与 Agent/Team 组装",
                "status": "partial",
                "coverage": [
                    "Provider / Plugin / Skill / Static Memory / Knowledge Base / Memory Profile 已提供 CRUD 接口。",
                    "AgentDefinition / TeamDefinition 已支持编译与运行，能够按配置绑定插件、技能、知识库、审核策略和记忆画像。",
                    "TeamDefinition 前端已支持显式配置团队名称、团队简介、Leader 与直属 Subagents。",
                ],
                "gaps": [
                    "部分资源管理页仍偏向控制台式编排，和最终交付体验还有差距。",
                    "内建插件目前以内置能力形式提供，不走完整的安装型插件生命周期。",
                ],
                "evidence": {
                    "resource_counts": {
                        "providers": summary.get("provider_profile_count"),
                        "plugins": summary.get("plugin_count"),
                        "skills": summary.get("skill_count"),
                        "static_memories": summary.get("static_memory_count"),
                        "knowledge_bases": summary.get("knowledge_base_count"),
                        "agent_definitions": summary.get("agent_definition_count"),
                        "team_definitions": summary.get("team_definition_count"),
                    },
                    "builtin_plugin_keys": sorted(builtin_keys),
                },
            },
            {
                "id": "R2",
                "title": "团队消息路由与邻接约束",
                "status": "implemented",
                "coverage": [
                    "团队编译阶段会计算成员间的邻接关系并写入运行时。",
                    "消息路由会对团队内显式消息通道做校验。",
                    "人工角色作为特殊 actor，不受团队成员邻接级别限制。",
                ],
                "gaps": [
                    "Agent 何时主动发起点对点通信，仍部分依赖运行时策略与模型行为。",
                ],
                "evidence": {
                    "router_builtins": [key for key in sorted(builtin_keys) if key.startswith("team.message")],
                },
            },
            {
                "id": "R3",
                "title": "人工审批与介入",
                "status": "implemented",
                "coverage": [
                    "review gate 可拦截任务入站、工具调用、记忆写入和最终交付。",
                    "审批中心支持暂停、恢复与结果回写。",
                    "团队测试与运行页都能看到审批中断后的状态。",
                ],
                "gaps": [
                    "更复杂的审核条件仍主要靠配置文本表达，规则化能力还有提升空间。",
                ],
                "evidence": {
                    "approval_count": summary.get("approval_count"),
                    "pending_approval_count": summary.get("pending_approval_count"),
                },
            },
            {
                "id": "R4",
                "title": "存储后端与记忆挂接",
                "status": "implemented",
                "coverage": [
                    "Run / Step / Event / Approval / Memory / Artifact / Release 元数据已落地 SQLite。",
                    "Agent memory 已暴露 storage_info，可查看 short-term / long-term / reflection 后端信息。",
                    "Runtime 已暴露 checkpoint、artifact、workspace 等存储信息。",
                ],
                "gaps": [
                    "Artifact 目前主要落在工作区文件系统，尚未接入外部对象存储。",
                ],
                "evidence": {
                    "metadata_store": summary.get("database_path"),
                    "memory_storage": memory_storage,
                    "runtime_storage": runtime_storage,
                },
            },
        ]
        return {
            "items": requirement_items,
            "summary": summary,
            "builtin_plugins": builtin_plugins,
        }

    def _require_run(self, run_id: str | None) -> dict[str, Any]:
        if not run_id:
            raise AppError(400, "run_id is required.")
        run = self.container.store.get_run(str(run_id))
        if run is None:
            raise AppError(404, "Run does not exist.")
        return run

    def _serve_static(self, path: str) -> AppResponse:
        relative = path.removeprefix("/static/")
        target = (self.container.static_dir / relative).resolve()
        if not str(target).startswith(str(self.container.static_dir.resolve())) or not target.exists():
            raise AppError(404, "Static asset not found.")
        content_type = {
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".html": "text/html; charset=utf-8",
        }.get(target.suffix, "application/octet-stream")
        return self._file_response(target, content_type)

    def _parse_json(self, body: bytes) -> dict[str, Any]:
        if not body:
            return {}
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise AppError(400, "Request body must be valid JSON.") from exc
        if not isinstance(payload, dict):
            raise AppError(400, "JSON body must be an object.")
        return payload

    def _require_fields(self, payload: dict[str, Any], *fields: str) -> None:
        missing = [field for field in fields if payload.get(field) in (None, "", [])]
        if missing:
            raise AppError(400, f"Missing required fields: {', '.join(missing)}")

    def _optional_str(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _optional_int(self, value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise AppError(400, "Query value must be an integer.") from exc

    def _coerce_bool(self, value: Any, *, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if not normalized:
                return default
            return normalized not in {"0", "false", "no", "off"}
        return bool(value)

    def _json(self, status: int, payload: dict[str, Any]) -> AppResponse:
        return AppResponse(status=status, body=json.dumps(payload, ensure_ascii=False).encode("utf-8"), content_type="application/json; charset=utf-8")

    def _empty(self, status: int) -> AppResponse:
        return AppResponse(status=status, body=b"", content_type="text/plain; charset=utf-8")

    def _file_response(self, path: Path, content_type: str) -> AppResponse:
        if not path.exists():
            raise AppError(404, "File not found.")
        return AppResponse(status=200, body=path.read_bytes(), content_type=content_type)


class _RequestHandler(BaseHTTPRequestHandler):
    def _dispatch(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        response = self.server.application.handle(self.command, self.path, body)  # type: ignore[attr-defined]
        if response.status >= 400:
            detail = ""
            if response.body:
                if response.content_type.startswith("application/json"):
                    try:
                        payload = json.loads(response.body.decode("utf-8"))
                        error_lines = []
                        if isinstance(payload, dict):
                            if payload.get("detail"):
                                error_lines.append(str(payload["detail"]))
                            if isinstance(payload.get("errors"), list):
                                error_lines.extend(str(item) for item in payload["errors"] if item)
                        detail = " | ".join(error_lines)
                    except Exception:
                        detail = response.body.decode("utf-8", errors="ignore")
                else:
                    detail = response.body.decode("utf-8", errors="ignore")
            suffix = f" detail={trim_text(detail, limit=320)}" if detail else ""
            LOGGER.warning("%s %s -> %s%s", self.command, self.path, response.status, suffix)
        self.send_response(response.status)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(response.body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        for key, value in response.headers.items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD" and response.body:
            self.wfile.write(response.body)

    def do_GET(self) -> None:
        self._dispatch()

    def do_POST(self) -> None:
        self._dispatch()

    def do_PUT(self) -> None:
        self._dispatch()

    def do_DELETE(self) -> None:
        self._dispatch()

    def do_OPTIONS(self) -> None:
        self._dispatch()

    def log_message(self, format: str, *args: Any) -> None:
        return None


class AITeamsHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], application: WebApplication):
        super().__init__(server_address, _RequestHandler)
        self.application = application
