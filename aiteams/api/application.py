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
from aiteams.runtime.compiler import BlueprintCompiler
from aiteams.runtime.engine import RuntimeEngine
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import pretty_json, trim_text
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
                return self._json(
                    200,
                    {
                        "summary": self.container.store.dashboard_summary(),
                        "storage": self.container.store.storage_info(),
                        "provider_types": self.container.agent_center.provider_types(),
                        "recent_builds": self.container.store.list_blueprint_builds()[:10],
                        "recent_runs": self.container.store.list_runs()[:10],
                        "pending_approvals": self.container.store.list_approvals(status="pending")[:10],
                    },
                )
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
                items = []
                for item in self.container.store.list_plugins():
                    plugin = dict(item)
                    plugin["runtime"] = self.container.plugins.snapshot(str(item["id"]))
                    items.append(plugin)
                return self._json(200, {"items": items})
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
                return self._json(200, {"items": self.container.store.list_agent_templates()})
            if method == "POST" and path == "/api/agent-center/agent-templates":
                payload = self._parse_json(body)
                template = self._save_agent_template(payload, template_id=self._optional_str(payload.get("id")))
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
            if path.startswith("/api/runs/") and path.endswith("/events") and method == "GET":
                run_id = path.split("/")[3]
                return self._json(200, {"items": self.container.store.list_events(run_id)})
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
                if not workspace_id or not project_id or not run_id or not agent_id:
                    raise AppError(400, "workspace_id, project_id, run_id and agent_id are required.")
                from aiteams.memory.scope import MemoryScopes

                scope_builder = MemoryScopes(workspace_id=workspace_id, project_id=project_id, run_id=run_id, agent_id=agent_id)
                scope_mode = str(query.get("scope") or "combined").strip().lower()
                if scope_mode == "project_shared":
                    scopes = [scope_builder.project_shared()]
                elif scope_mode == "agent_private":
                    scopes = [scope_builder.agent_private()]
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
                key=str(normalized["key"]),
                name=str(normalized["name"]),
                provider_type=str(normalized["provider_type"]),
                description=str(normalized["description"]),
                config=dict(normalized["config"]),
                secret=dict(normalized["secret"]) if normalized["secret"] else None,
            )
        except sqlite3.IntegrityError as exc:
            message = "Provider Key 已存在，请更换后重试。"
            if "provider_profiles.key" not in str(exc):
                message = str(exc)
            raise AppError(
                400,
                "Provider 保存失败。",
                extra={
                    "error_type": "provider_persistence_error",
                    "errors": [message],
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
            "key": self._optional_str(payload.get("key")),
            "name": self._optional_str(payload.get("name")),
            "provider_type": self._optional_str(payload.get("provider_type")),
            "base_url": self._optional_str(payload.get("base_url")) or self._optional_str(config.get("base_url")),
            "model_count": model_count,
            "skip_tls_verify": bool(payload.get("skip_tls_verify", config.get("skip_tls_verify"))),
            "has_secret": bool((payload.get("secret") or {}).get("api_key") or payload.get("api_key")),
        }

    def _save_plugin(self, payload: dict[str, Any], *, plugin_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "key", "name")
        install_path = self._optional_str(payload.get("install_path"))
        manifest = dict(payload.get("manifest") or {})
        if install_path:
            try:
                package = self.container.plugins.validate_package(install_path)
                package_manifest = dict(package.get("manifest") or {})
                merged_manifest = dict(package_manifest)
                merged_manifest.update(manifest)
                manifest = merged_manifest
            except Exception:
                pass
        return self.container.store.save_plugin(
            plugin_id=plugin_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            version=str(payload.get("version") or "v1"),
            plugin_type=str(payload.get("plugin_type") or "toolset"),
            description=str(payload.get("description") or ""),
            manifest=manifest,
            install_path=install_path,
        )

    def _save_agent_template(self, payload: dict[str, Any], *, template_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "key", "name", "role")
        return self.container.store.save_agent_template(
            agent_template_id=template_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            role=str(payload["role"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=dict(payload.get("spec") or {}),
        )

    def _save_team_template(self, payload: dict[str, Any], *, template_id: str | None) -> dict[str, Any]:
        self._require_fields(payload, "key", "name")
        spec = self.container.agent_center.normalize_team_spec(dict(payload.get("spec") or {}))
        validation = self.container.agent_center.validate_team_spec(spec)
        if validation["errors"]:
            raise ValueError("; ".join(validation["errors"]))
        return self.container.store.save_team_template(
            team_template_id=template_id,
            key=str(payload["key"]),
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            version=str(payload.get("version") or "v1"),
            spec=validation["normalized_spec"],
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
