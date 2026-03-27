from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import yaml

from aiteams.agent_center import AgentCenterService
from aiteams.domain.models import BlueprintSpec
from aiteams.domain.templates import built_in_blueprint_templates
from aiteams.plugins import PluginManager
from aiteams.role_specs import normalize_role_spec
from aiteams.runtime.compiler import BlueprintCompiler
from aiteams.runtime.engine import RuntimeEngine
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import make_id, pretty_json, slugify, trim_text, utcnow_iso
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
    compiler: BlueprintCompiler
    runtime: RuntimeEngine
    workspace: WorkspaceManager
    agent_center: AgentCenterService
    plugins: PluginManager
    static_dir: Path

    def close(self) -> None:
        agent_memory = getattr(self.runtime.agent_kernel.memory, "close", None)
        if callable(agent_memory):
            agent_memory()
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
                        "recent_builds": self.container.store.list_blueprint_builds()[:10],
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
            if method == "GET" and path == "/api/agent-center/retrieval-settings":
                return self._json(200, self.container.agent_center.get_retrieval_settings())
            if method == "PUT" and path == "/api/agent-center/retrieval-settings":
                payload = self._parse_json(body)
                saved = self.container.agent_center.save_retrieval_settings(payload)
                try:
                    applied = self.container.runtime.agent_kernel.memory.configure_retrieval(saved.get("runtime"))
                except Exception as exc:
                    raise AppError(400, f"Retrieval settings saved but could not be applied: {exc}") from exc
                return self._json(
                    200,
                    {
                        "settings": saved.get("settings"),
                        "updated_at": saved.get("updated_at"),
                        "applied": applied,
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
            if method == "GET" and path == "/api/agent-center/agent-templates":
                limit = self._optional_int(query.get("limit"))
                offset = self._optional_int(query.get("offset")) or 0
                return self._json(200, self.container.store.list_agent_templates_page(limit=limit, offset=offset))
            if method == "POST" and path == "/api/agent-center/agent-templates":
                payload = self._parse_json(body)
                template = self._save_agent_template(payload, template_id=None)
                return self._json(200, template)
            if path.startswith("/api/agent-center/agent-templates/"):
                template_id = path.rsplit("/", 1)[-1]
                template = self.container.store.get_agent_template(template_id)
                if template is None:
                    raise AppError(404, "Agent template does not exist.")
                if method == "GET":
                    return self._json(200, template)
                if method == "PUT":
                    payload = self._parse_json(body)
                    updated = self._save_agent_template(payload, template_id=template_id)
                    return self._json(200, updated)
                if method == "DELETE":
                    deleted = self.container.store.delete_agent_template(template_id)
                    if deleted is None:
                        raise AppError(404, "Agent template does not exist.")
                    return self._json(200, {"deleted": True, "id": template_id})
            if method == "GET" and path == "/api/agent-center/skills":
                return self._json(200, {"items": self.container.store.list_skills()})
            if method == "POST" and path == "/api/agent-center/skills":
                payload = self._parse_json(body)
                return self._json(200, self._save_skill(payload, skill_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/skills/"):
                skill_id = path.rsplit("/", 1)[-1]
                skill = self.container.store.get_skill(skill_id)
                if skill is None:
                    raise AppError(404, "Skill does not exist.")
                if method == "GET":
                    return self._json(200, skill)
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_skill(payload, skill_id=skill_id))
                if method == "DELETE":
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
            if method == "GET" and path == "/api/agent-center/memory-profiles":
                return self._json(200, {"items": self.container.store.list_memory_profiles()})
            if method == "POST" and path == "/api/agent-center/memory-profiles":
                payload = self._parse_json(body)
                return self._json(200, self._save_memory_profile(payload, memory_profile_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/memory-profiles/"):
                memory_profile_id = path.rsplit("/", 1)[-1]
                memory_profile = self.container.store.get_memory_profile(memory_profile_id)
                if memory_profile is None:
                    raise AppError(404, "Memory profile does not exist.")
                if method == "GET":
                    return self._json(200, memory_profile)
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_memory_profile(payload, memory_profile_id=memory_profile_id))
                if method == "DELETE":
                    deleted = self.container.store.delete_memory_profile(memory_profile_id)
                    if deleted is None:
                        raise AppError(404, "Memory profile does not exist.")
                    return self._json(200, {"deleted": True, "id": memory_profile_id})
            if method == "GET" and path == "/api/agent-center/knowledge-bases":
                return self._json(200, {"items": self.container.store.list_knowledge_bases()})
            if method == "POST" and path == "/api/agent-center/knowledge-bases":
                payload = self._parse_json(body)
                return self._json(200, self._save_knowledge_base(payload, knowledge_base_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/knowledge-bases/"):
                knowledge_base_id = path.rsplit("/", 1)[-1]
                knowledge_base = self.container.store.get_knowledge_base(knowledge_base_id)
                if knowledge_base is None:
                    raise AppError(404, "Knowledge base does not exist.")
                if method == "GET":
                    return self._json(200, knowledge_base)
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_knowledge_base(payload, knowledge_base_id=knowledge_base_id))
                if method == "DELETE":
                    deleted = self.container.store.delete_knowledge_base(knowledge_base_id)
                    if deleted is None:
                        raise AppError(404, "Knowledge base does not exist.")
                    return self._json(200, {"deleted": True, "id": knowledge_base_id})
            if method == "GET" and path == "/api/agent-center/knowledge-documents":
                return self._json(
                    200,
                    {
                        "items": self.container.store.list_knowledge_documents(
                            knowledge_base_id=self._optional_str(query.get("knowledge_base_id"))
                        )
                    },
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
                    return self._json(200, knowledge_document)
                if method == "PUT":
                    payload = self._parse_json(body)
                    return self._json(200, self._save_knowledge_document(payload, knowledge_document_id=knowledge_document_id))
                if method == "DELETE":
                    deleted = self.container.store.delete_knowledge_document(knowledge_document_id)
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
                return self._json(200, {"items": self.container.store.list_team_definitions()})
            if method == "POST" and path == "/api/agent-center/team-definitions":
                payload = self._parse_json(body)
                return self._json(200, self._save_team_definition(payload, definition_id=self._optional_str(payload.get("id"))))
            if path.startswith("/api/agent-center/team-definitions/") and path.endswith("/compile") and method == "POST":
                definition_id = path.split("/")[4]
                return self._json(200, self.container.agent_center.compile_team_definition(definition_id))
            if path.startswith("/api/agent-center/team-definitions/") and path.endswith("/build") and method == "POST":
                definition_id = path.split("/")[4]
                payload = self._parse_json(body)
                build = self.container.agent_center.build_team_definition(definition_id, blueprint_name=self._optional_str(payload.get("name")))
                blueprint = build.get("blueprint")
                if blueprint:
                    self.container.workspace.write_blueprint(
                        workspace_id=str(blueprint["workspace_id"]),
                        project_id=str(blueprint["project_id"]),
                        blueprint_id=str(blueprint["id"]),
                        raw_text=str(blueprint.get("raw_text") or pretty_json(blueprint.get("spec_json") or {})),
                        raw_format=str(blueprint.get("raw_format") or "json"),
                    )
                return self._json(200, build)
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
                    )
                )
                if not bundle.get("task_thread"):
                    thread_metadata = {"team_definition_id": definition_id, "adjacency": build.get("adjacency") or {}}
                    if build.get("hierarchy"):
                        thread_metadata["hierarchy"] = build.get("hierarchy")
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
            if method == "GET" and path == "/api/agent-center/team-templates":
                return self._json(200, {"items": self.container.store.list_team_templates()})
            if method == "POST" and path == "/api/agent-center/team-templates/graph/normalize":
                payload = self._parse_json(body)
                return self._json(200, {"spec": self.container.agent_center.normalize_team_spec(dict(payload.get("spec") or {}))})
            if method == "POST" and path == "/api/agent-center/team-templates/graph/validate":
                payload = self._parse_json(body)
                return self._json(200, self.container.agent_center.validate_team_spec(dict(payload.get("spec") or {})))
            if method == "POST" and path == "/api/agent-center/team-templates/graph/preview":
                payload = self._parse_json(body)
                self._require_fields(payload, "spec")
                preview = self.container.agent_center.preview_team_spec(
                    dict(payload.get("spec") or {}),
                    team_template_id=self._optional_str(payload.get("team_template_id")),
                    name=self._optional_str(payload.get("name")),
                )
                return self._json(200, preview)
            if method == "POST" and path == "/api/agent-center/team-templates":
                payload = self._parse_json(body)
                template = self._save_team_template(payload, template_id=self._optional_str(payload.get("id")))
                return self._json(200, template)
            if path.startswith("/api/agent-center/team-templates/") and path.endswith("/graph") and method == "GET":
                team_template_id = path.split("/")[4]
                return self._json(200, self.container.agent_center.team_graph_payload(team_template_id))
            if path.startswith("/api/agent-center/team-templates/") and path.endswith("/build") and method == "POST":
                team_template_id = path.split("/")[4]
                payload = self._parse_json(body)
                build = self.container.agent_center.build_team_template(
                    team_template_id,
                    build_name=self._optional_str(payload.get("name")),
                )
                self._write_build_blueprint(build)
                return self._json(200, build)
            if path.startswith("/api/agent-center/team-templates/"):
                template_id = path.rsplit("/", 1)[-1]
                template = self.container.store.get_team_template(template_id)
                if template is None:
                    raise AppError(404, "Team template does not exist.")
                if method == "GET":
                    return self._json(200, template)
                if method == "PUT":
                    payload = self._parse_json(body)
                    updated = self._save_team_template(payload, template_id=template_id)
                    return self._json(200, updated)
            if method == "GET" and path == "/api/agent-center/builds":
                return self._json(200, {"items": self.container.store.list_blueprint_builds()})
            if method == "POST" and path == "/api/agent-center/builds":
                payload = self._parse_json(body)
                self._require_fields(payload, "team_template_id")
                build = self.container.agent_center.build_team_template(
                    str(payload["team_template_id"]),
                    build_name=self._optional_str(payload.get("name")),
                )
                self._write_build_blueprint(build)
                return self._json(200, build)
            if path.startswith("/api/agent-center/builds/"):
                build_id = path.rsplit("/", 1)[-1]
                build = self.container.store.get_blueprint_build(build_id)
                if build is None:
                    raise AppError(404, "Build does not exist.")
                return self._json(200, build)
            if method == "GET" and path == "/api/blueprints/templates":
                items = [self._template_record(item) for item in built_in_blueprint_templates()]
                return self._json(200, {"items": items})
            if method == "POST" and path == "/api/blueprints/validate":
                payload = self._parse_json(body)
                raw_format, raw_text, spec = self._resolve_blueprint_payload(payload)
                compiled = self.container.compiler.compile(spec)
                return self._json(
                    200,
                    {
                        "valid": True,
                        "raw_format": raw_format,
                        "raw_text": raw_text,
                        "spec": spec,
                        "compiled": {
                            "role_template_count": len(compiled.blueprint.role_templates),
                            "agent_count": len(compiled.blueprint.agents),
                            "start_node_id": compiled.start_node_id,
                            "node_count": len(compiled.nodes),
                            "edge_count": sum(len(items) for items in compiled.outgoing.values()),
                            "communication_mode": "graph-ancestor-scoped",
                        },
                    },
                )
            if method == "GET" and path == "/api/blueprints":
                workspace_id = self._optional_str(query.get("workspace_id"))
                project_id = self._optional_str(query.get("project_id"))
                return self._json(200, {"items": self.container.store.list_blueprints(workspace_id=workspace_id, project_id=project_id)})
            if method == "POST" and path == "/api/blueprints":
                payload = self._parse_json(body)
                raw_format, raw_text, spec = self._resolve_blueprint_payload(payload)
                blueprint = self.container.store.save_blueprint(
                    blueprint_id=self._optional_str(payload.get("id")),
                    workspace_id=str(spec["workspace_id"]),
                    project_id=str(spec["project_id"]),
                    name=str(spec["name"]),
                    description=str(spec.get("description") or ""),
                    version=str(spec.get("version") or "v1"),
                    raw_format=raw_format,
                    raw_text=raw_text,
                    spec=spec,
                    is_template=bool(payload.get("is_template", False)),
                )
                self.container.workspace.write_blueprint(
                    workspace_id=str(spec["workspace_id"]),
                    project_id=str(spec["project_id"]),
                    blueprint_id=str(blueprint["id"]),
                    raw_text=raw_text,
                    raw_format=raw_format,
                )
                return self._json(200, blueprint)
            if path.startswith("/api/blueprints/"):
                blueprint_id = path.rsplit("/", 1)[-1]
                blueprint = self.container.store.get_blueprint(blueprint_id)
                if blueprint is None:
                    raise AppError(404, "Blueprint does not exist.")
                if method == "GET":
                    return self._json(200, blueprint)
                if method == "PUT":
                    payload = self._parse_json(body)
                    raw_format, raw_text, spec = self._resolve_blueprint_payload(payload)
                    updated = self.container.store.save_blueprint(
                        blueprint_id=blueprint_id,
                        workspace_id=str(spec["workspace_id"]),
                        project_id=str(spec["project_id"]),
                        name=str(spec["name"]),
                        description=str(spec.get("description") or ""),
                        version=str(spec.get("version") or "v1"),
                        raw_format=raw_format,
                        raw_text=raw_text,
                        spec=spec,
                        is_template=bool(payload.get("is_template", blueprint.get("is_template", False))),
                    )
                    self.container.workspace.write_blueprint(
                        workspace_id=str(spec["workspace_id"]),
                        project_id=str(spec["project_id"]),
                        blueprint_id=blueprint_id,
                        raw_text=raw_text,
                        raw_format=raw_format,
                    )
                    return self._json(200, updated)
                if method == "DELETE":
                    deleted = self.container.store.delete_blueprint(blueprint_id)
                    assert deleted is not None
                    return self._json(200, {"deleted": True, "blueprint": deleted})
            if method == "GET" and path == "/api/task-releases":
                return self._json(200, {"items": self.container.store.list_task_releases(project_id=self._optional_str(query.get("project_id")))})
            if method == "POST" and path == "/api/task-releases":
                payload = self._parse_json(body)
                if payload.get("build_id") in (None, "") and payload.get("blueprint_id") in (None, ""):
                    raise AppError(400, "build_id or blueprint_id is required.")
                self._require_fields(payload, "prompt")
                build_id = self._optional_str(payload.get("build_id"))
                if build_id:
                    build = self.container.store.get_blueprint_build(build_id)
                    if build is None:
                        raise AppError(404, "Build does not exist.")
                    blueprint_id = self._optional_str(build.get("blueprint_id"))
                    if not blueprint_id:
                        raise AppError(400, "Build is missing blueprint snapshot.")
                else:
                    blueprint_id = str(payload["blueprint_id"])
                blueprint = self.container.store.get_blueprint(str(blueprint_id))
                if blueprint is None:
                    raise AppError(404, "Blueprint does not exist.")
                bundle = asyncio.run(
                    self.container.runtime.start_task(
                        blueprint=blueprint,
                        title=self._optional_str(payload.get("title")),
                        prompt=str(payload["prompt"]),
                        inputs=dict(payload.get("inputs") or {}),
                        approval_mode=str(payload.get("approval_mode") or "auto"),
                    )
                )
                return self._json(200, bundle)
            if path.startswith("/api/task-releases/"):
                task_release_id = path.rsplit("/", 1)[-1]
                task = self.container.store.get_task_release(task_release_id)
                if task is None:
                    raise AppError(404, "Task release does not exist.")
                return self._json(200, task)
            if method == "GET" and path == "/api/runs":
                return self._json(200, {"items": self.container.store.list_runs(project_id=self._optional_str(query.get("project_id")))})
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

    def _save_agent_template(self, payload: dict[str, Any], *, template_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "name", "role")
        existing = self.container.store.get_agent_template(template_id) if template_id else None
        existing_spec = dict((existing or {}).get("spec_json") or {})
        incoming_spec = dict(payload.get("spec") or {})
        spec = dict(existing_spec)
        spec.update(incoming_spec)
        plugin_refs = list(spec.get("plugin_refs") or spec.get("tool_plugin_refs") or [])
        spec["plugin_refs"] = self.container.agent_center.ensure_default_plugin_refs(plugin_refs)
        metadata = dict(existing_spec.get("metadata") or {})
        metadata.update(dict(incoming_spec.get("metadata") or {}))
        if metadata:
            spec["metadata"] = metadata
        return self.container.store.save_agent_template(
            agent_template_id=template_id,
            name=str(payload["name"]),
            role=str(payload["role"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=spec,
        )

    def _save_team_template(self, payload: dict[str, Any], *, template_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "name")
        spec = self.container.agent_center.normalize_team_spec(dict(payload.get("spec") or {}))
        validation = self.container.agent_center.validate_team_spec(spec)
        if validation["errors"]:
            raise ValueError("; ".join(validation["errors"]))
        return self.container.store.save_team_template(
            team_template_id=template_id,
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=validation["normalized_spec"],
        )

    def _save_skill(self, payload: dict[str, Any], *, skill_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "key", "name")
        return self.container.store.save_skill(
            skill_id=skill_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=dict(payload.get("spec") or {}),
        )

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

    def _save_memory_profile(self, payload: dict[str, Any], *, memory_profile_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "key", "name")
        return self.container.store.save_memory_profile(
            memory_profile_id=memory_profile_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=dict(payload.get("spec") or {}),
        )

    def _save_knowledge_base(self, payload: dict[str, Any], *, knowledge_base_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "key", "name")
        return self.container.store.save_knowledge_base(
            knowledge_base_id=knowledge_base_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            config=dict(payload.get("config") or {}),
        )

    def _save_knowledge_document(self, payload: dict[str, Any], *, knowledge_document_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "knowledge_base_id", "key", "title")
        return self.container.store.save_knowledge_document(
            knowledge_document_id=knowledge_document_id,
            knowledge_base_id=str(payload["knowledge_base_id"]),
            key=str(payload["key"]),
            title=str(payload["title"]),
            source_path=self._optional_str(payload.get("source_path")),
            content_text=str(payload.get("content_text") or ""),
            metadata=dict(payload.get("metadata") or {}),
        )

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
        self._require_fields(payload, "key", "name")
        existing = self.container.store.get_team_definition(definition_id) if definition_id else None
        spec = dict((existing or {}).get("spec_json") or {})
        spec.update(dict(payload.get("spec") or {}))
        has_tree_root = isinstance(spec.get("root"), dict) or isinstance(spec.get("lead"), dict)
        if has_tree_root:
            spec.pop("members", None)
            spec.pop("agents", None)
        else:
            members = [dict(item or {}) for item in list(spec.get("members") or spec.get("agents") or [])]
            if not members:
                raise AppError(400, "Team definition requires at least one member or a root lead.")
            spec["members"] = members
        return self.container.store.save_team_definition(
            team_definition_id=definition_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=spec,
        )

    def _write_build_blueprint(self, build: dict[str, Any]) -> None:
        blueprint_id = self._optional_str(build.get("blueprint_id"))
        if not blueprint_id:
            return
        blueprint = self.container.store.get_blueprint(blueprint_id)
        if blueprint is None:
            return
        spec = dict(build.get("spec_json") or blueprint.get("spec_json") or {})
        if not spec:
            return
        self.container.workspace.write_blueprint(
            workspace_id=str(spec["workspace_id"]),
            project_id=str(spec["project_id"]),
            blueprint_id=blueprint_id,
            raw_text=pretty_json(spec),
            raw_format="json",
        )

    def _resolve_blueprint_payload(self, payload: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
        if isinstance(payload.get("spec"), dict):
            spec = BlueprintSpec.from_dict(dict(payload["spec"])).to_dict()
            raw_format = str(payload.get("raw_format") or "json")
            raw_text = payload.get("raw_text") or self._dump_blueprint(spec, raw_format)
            return raw_format, str(raw_text), spec
        raw_text = self._optional_str(payload.get("raw_text"))
        raw_format = str(payload.get("raw_format") or "yaml").lower()
        if not raw_text:
            raise AppError(400, "Either spec or raw_text is required.")
        if raw_format == "json":
            parsed = json.loads(raw_text)
        elif raw_format == "yaml":
            parsed = yaml.safe_load(raw_text)
        else:
            raise AppError(400, "raw_format must be yaml or json.")
        if not isinstance(parsed, dict):
            raise AppError(400, "Blueprint document must be an object.")
        spec = BlueprintSpec.from_dict(parsed).to_dict()
        return raw_format, self._dump_blueprint(spec, raw_format), spec

    def _dump_blueprint(self, spec: dict[str, Any], raw_format: str) -> str:
        if raw_format == "json":
            return pretty_json(spec)
        return yaml.safe_dump(spec, allow_unicode=True, sort_keys=False)

    def _template_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        spec = BlueprintSpec.from_dict(payload).to_dict()
        return {
            "name": spec["name"],
            "description": spec.get("description"),
            "spec": spec,
            "raw_format": "yaml",
            "raw_text": yaml.safe_dump(spec, allow_unicode=True, sort_keys=False),
        }

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
                "title": "资源目录与自由组装 Agent/Team",
                "status": "partial",
                "coverage": [
                    "Provider / Plugin / Skill / Static Memory / Knowledge Base / Memory Profile CRUD 已提供控制面接口",
                    "AgentDefinition / TeamDefinition 编译链路已可用，支持按配置显式绑定插件、技能、知识库、审核策略与记忆画像",
                    "TeamDefinition 前端已支持显式配置团队成员、层级、入口/终止策略和共享资源绑定",
                ],
                "gaps": [
                    "前端仍缺 Skill / Static Memory / Knowledge Base / Memory Profile / Review Policy / AgentDefinition 的完整显式管理页，目前主要依赖后端 API。",
                    "builtin plugin 目前以平台内置能力存在，不走安装型插件生命周期；适合内置能力，不适合把所有 builtin 完全等同为可安装插件包。",
                ],
                "evidence": {
                    "resource_counts": {
                        "providers": summary.get("provider_profile_count"),
                        "plugins": summary.get("plugin_count"),
                        "skills": summary.get("skill_count"),
                        "static_memories": summary.get("static_memory_count"),
                        "knowledge_bases": summary.get("knowledge_base_count"),
                        "memory_profiles": summary.get("memory_profile_count"),
                        "agent_definitions": summary.get("agent_definition_count"),
                        "team_definitions": summary.get("team_definition_count"),
                    },
                    "builtin_plugin_keys": sorted(builtin_keys),
                },
            },
            {
                "id": "R2",
                "title": "相邻占用级别点对点对话",
                "status": "implemented",
                "coverage": [
                    "Team 编译时按 occupied-level adjacency 计算相邻关系",
                    "Dialogue Router 对 team.message.send / team.message.reply 做显式路由校验",
                    "human 作为特殊 actor 不受相邻级别约束",
                    "TeamDefinition 前端已支持按成员配置 Agent 和 level，而不是必须手写 members JSON",
                ],
                "gaps": [
                    "Agent 自主决定何时发起点对点消息，目前除 provider-native tool-calling 外，仍保留部分 runtime_plugin_actions / heuristics 兼容路径。",
                ],
                "evidence": {
                    "adjacency_policy": "occupied-level ordered adjacency",
                    "router_builtins": [key for key in sorted(builtin_keys) if key.startswith("team.message")],
                },
            },
            {
                "id": "R3",
                "title": "人审介入任意关键行为",
                "status": "implemented",
                "coverage": [
                    "review_gate 可拦截任务入站、Agent 间消息和最终交付",
                    "before_tool_call / before_memory_write 已支持审批暂停与恢复",
                    "human.escalate 可由 Agent 显式请求人工介入",
                    "team-edge review_overrides 可对指定 Agent 对话链路强制前审",
                    "人类可通过运行时消息注入 API 直接向任意运行中 Agent 插话",
                ],
                "gaps": [
                    "Review Policy 的风险标签等高级条件仍允许自由文本输入，尚未完全规则化。",
                ],
                "evidence": {
                    "pending_approvals": len(self.container.store.list_approvals(status="pending")),
                    "human_escalation_builtin": "human.escalate" in builtin_keys,
                },
            },
            {
                "id": "R4",
                "title": "配置完成后可直接下达任务",
                "status": "implemented",
                "coverage": [
                    "TeamDefinition 可直接发起 task release，前端任务页已支持直接选择 TeamDefinition",
                    "Blueprint、Build 仍保留为兼容旧链路的 task release 入口",
                    "Team event-driven runtime 已接 task_ingress / dispatch_next / finish_or_wait",
                ],
                "gaps": [],
                "evidence": {
                    "task_release_count": summary.get("task_count"),
                    "run_count": summary.get("run_count"),
                },
            },
            {
                "id": "R5",
                "title": "长短期记忆自动压缩、失效、新增、查询",
                "status": "partial",
                "coverage": [
                    "短期记忆已切到 LangMem summarization / background reflection",
                    "memory.search / memory.manage / memory.background_reflection 已是可执行 builtin",
                    "长期记忆支持 TTL、去重合并、搜索、写入和删除",
                    "平台已支持单独配置 retrieval embedding/rerank 模型，并驱动实际检索链路",
                ],
                "gaps": [
                    "TTL 目前仍依赖应用内后台 sweep，不是独立的外部调度服务。",
                    "当前 retrieval 仍以单机 SQLite 为主，若要支撑多实例共享检索索引，建议进一步迁移到服务化 store。",
                ],
                "evidence": {
                    "memory_runtime": memory_storage.get("memory_runtime"),
                    "kv_driver": (memory_storage.get("kv") or {}).get("driver"),
                    "vector_driver": (memory_storage.get("vector") or {}).get("driver"),
                },
            },
            {
                "id": "R6",
                "title": "SQLite 统一存储方案评估",
                "status": "implemented",
                "coverage": [
                    "当前项目已使用 SQLite 做 LangGraph checkpointer",
                    "长期记忆与 working memory 已统一切到 SQLite",
                    "deepagents 文件 backend 已直接挂官方 LangGraph SqliteStore",
                ],
                "gaps": [
                    "当前方案更适合单机/单实例控制面；多实例生产建议迁移到 Postgres checkpointer/store。",
                    "provider embedding / rerank 当前仍在应用层完成，超大记忆规模下需要进一步引入外部向量检索服务。",
                ],
                "evidence": {
                    "checkpoint_driver": runtime_storage.get("checkpoint_driver"),
                    "checkpoint_runtime": runtime_storage.get("checkpoint_runtime"),
                    "memory_stack": memory_storage,
                },
            },
        ]
        counts = {
            "implemented": sum(1 for item in requirement_items if item["status"] == "implemented"),
            "partial": sum(1 for item in requirement_items if item["status"] == "partial"),
            "missing": sum(1 for item in requirement_items if item["status"] == "missing"),
        }
        return {
            "generated_at": utcnow_iso(),
            "overall": counts | {"total": len(requirement_items)},
            "requirements": requirement_items,
            "storage_assessment": {
                "proposal": {
                    "primary_store": "sqlite",
                    "langgraph_checkpointer": "sqlite",
                    "kv": "sqlite",
                    "vector": "sqlite",
                },
                "current": {
                    "metadata": self.container.store.storage_info(),
                    "runtime": runtime_storage,
                    "memory": memory_storage,
                },
                "fit": "official_sqlite_store",
                "recommendation": "当前链路已统一到 SQLite；如需多实例和更强运维能力，优先切到 Postgres checkpointer/store。",
            },
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
