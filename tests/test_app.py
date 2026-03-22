from __future__ import annotations

import json
import tempfile
import textwrap
import unittest
from pathlib import Path

from aiteams.app import AppSettings, create_app
from aiteams.domain.templates import approval_delivery_template, research_parallel_template, software_delivery_template


class AITeamsPlatformTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        root = Path(self._tempdir.name)
        data_dir = root / "data"
        memory_root = data_dir / "aimemory"
        workspace_root = data_dir / "workspaces"
        for path in (data_dir, memory_root, workspace_root):
            path.mkdir(parents=True, exist_ok=True)
        settings = AppSettings(
            project_root=root,
            data_dir=data_dir,
            metadata_db_path=data_dir / "platform.db",
            memory_root=memory_root,
            workspace_root=workspace_root,
            static_dir=Path(__file__).resolve().parents[1] / "aiteams" / "static",
        )
        self._settings = settings
        self._app = create_app(settings)

    def tearDown(self) -> None:
        self._app.close()
        self._tempdir.cleanup()

    def _request(self, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else b""
        response = self._app.handle(method, path, body)
        parsed = json.loads(response.body.decode("utf-8")) if response.body else {}
        return response.status, parsed

    def _create_blueprint(self, spec: dict) -> dict:
        status, payload = self._request("POST", "/api/blueprints", {"spec": spec, "raw_format": "json"})
        self.assertEqual(status, 200)
        return payload

    def _create_plugin_package(self, *, name: str, code_tag: str) -> Path:
        root = Path(self._tempdir.name) / name
        backend = root / "backend"
        backend.mkdir(parents=True, exist_ok=True)
        (backend / "__init__.py").write_text("", encoding="utf-8")
        (root / "plugin.yaml").write_text(
            textwrap.dedent(
                f"""
                key: {name}
                name: {name}
                version: v1
                plugin_type: toolset
                entrypoint: backend.entry:EchoPlugin
                workbench_key: {name}
                tools:
                  - echo
                permissions:
                  - readonly
                actions:
                  - name: echo
                    description: echo payload
                hot_reload: true
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        (backend / "entry.py").write_text(
            textwrap.dedent(
                f"""
                class EchoPlugin:
                    def __init__(self, *, manifest, root_path):
                        self.manifest = manifest
                        self.root_path = root_path

                    def describe(self):
                        return {{
                            "key": self.manifest["key"],
                            "tools": ["echo"],
                            "permissions": ["readonly"],
                            "actions": [{{"name": "echo", "description": "echo payload"}}],
                        }}

                    def health(self):
                        return {{"status": "ok", "code_tag": "{code_tag}"}}

                    def invoke(self, action, payload, context):
                        return {{
                            "action": action,
                            "payload": payload,
                            "context_node": context.get("node_id"),
                            "code_tag": "{code_tag}",
                        }}

                    def shutdown(self):
                        return {{"status": "bye"}}
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        return root

    def _restart_app(self) -> None:
        self._app.close()
        self._app = create_app(self._settings)

    def test_software_delivery_blueprint_completes_and_writes_artifacts_and_memory(self) -> None:
        blueprint = self._create_blueprint(software_delivery_template())

        status, bundle = self._request(
            "POST",
            "/api/task-releases",
            {
                "blueprint_id": blueprint["id"],
                "title": "Refactor AiTeams",
                "prompt": "Rebuild AiTeams as a Python multi-agent collaboration platform.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")
        self.assertGreaterEqual(len(bundle["steps"]), 8)
        self.assertEqual(len(bundle["artifacts"]), 1)

        plan_step = next(step for step in bundle["steps"] if step["node_id"] == "plan")
        self.assertEqual(plan_step["output_json"]["details"]["role_template"], "strategy_planner")

        run_id = bundle["run"]["id"]
        detail_status, detail = self._request("GET", f"/api/runs/{run_id}")
        self.assertEqual(detail_status, 200)
        self.assertEqual(detail["run"]["status"], "completed")
        self.assertTrue(any(item["relative_path"].startswith("artifacts/") for item in detail["workspace_files"]))

        memory_status, memory = self._request(
            "GET",
            f"/api/memory/search?workspace_id=local-workspace&project_id=default-project&run_id={run_id}&agent_id=planner&query=delivery",
        )
        self.assertEqual(memory_status, 200)
        self.assertGreaterEqual(len(memory["items"]), 1)

    def test_manual_approval_can_be_resolved_and_run_resumed(self) -> None:
        blueprint = self._create_blueprint(approval_delivery_template())

        status, bundle = self._request(
            "POST",
            "/api/task-releases",
            {
                "blueprint_id": blueprint["id"],
                "prompt": "Create a plan that requires manual approval before delivery.",
                "approval_mode": "manual",
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")
        self.assertEqual(len(bundle["approvals"]), 1)

        approval_id = bundle["approvals"][0]["id"]
        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval_id}/resolve",
            {"approved": True, "comment": "Continue the run."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed["run"]["status"], "completed")
        self.assertEqual(len(resumed["artifacts"]), 1)

    def test_manual_approval_rejection_marks_run_failed_after_resume(self) -> None:
        blueprint = self._create_blueprint(approval_delivery_template())

        status, bundle = self._request(
            "POST",
            "/api/task-releases",
            {
                "blueprint_id": blueprint["id"],
                "prompt": "Create a plan that requires manual approval before delivery.",
                "approval_mode": "manual",
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")
        self.assertEqual(len(bundle["approvals"]), 1)

        approval_id = bundle["approvals"][0]["id"]
        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval_id}/resolve",
            {"approved": False, "comment": "Reject this delivery."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "rejected")

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed["run"]["status"], "failed")
        self.assertEqual(resumed["approvals"][0]["status"], "rejected")

    def test_parallel_blueprint_merges_branch_outputs_and_keeps_branches_isolated(self) -> None:
        blueprint = self._create_blueprint(research_parallel_template())

        status, bundle = self._request(
            "POST",
            "/api/task-releases",
            {
                "blueprint_id": blueprint["id"],
                "prompt": "Evaluate a configurable multi-agent platform for complex engineering delivery.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")

        topic_step = next(step for step in bundle["steps"] if step["node_id"] == "topic_research")
        synth_step = next(step for step in bundle["steps"] if step["node_id"] == "synthesize")
        merge_step = next(step for step in bundle["steps"] if step["node_id"] == "merge")

        self.assertEqual(merge_step["output_json"]["branch_count"], 2)
        self.assertNotIn("risk_research", topic_step["output_json"]["visible_output_ids"])
        self.assertIn("topic_research", synth_step["output_json"]["visible_output_ids"])
        self.assertIn("risk_research", synth_step["output_json"]["visible_output_ids"])

    def test_templates_endpoint_returns_builtin_blueprints_with_role_templates(self) -> None:
        status, payload = self._request("GET", "/api/blueprints/templates")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(payload["items"]), 3)
        self.assertGreaterEqual(len(payload["items"][0]["spec"]["role_templates"]), 1)

    def test_control_plane_reports_sqlite_storage(self) -> None:
        status, payload = self._request("GET", "/api/control-plane")
        self.assertEqual(status, 200)
        self.assertEqual(payload["storage"]["metadata_driver"], "sqlite")
        self.assertEqual(payload["storage"]["journal_mode"], "wal")
        self.assertTrue(payload["storage"]["metadata_path"].endswith("platform.db"))

    def test_validate_endpoint_reports_graph_scoped_compilation(self) -> None:
        status, payload = self._request(
            "POST",
            "/api/blueprints/validate",
            {"spec": research_parallel_template(), "raw_format": "json"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["compiled"]["communication_mode"], "graph-ancestor-scoped")
        self.assertGreaterEqual(payload["compiled"]["role_template_count"], 1)
        self.assertGreaterEqual(payload["compiled"]["agent_count"], 1)

    def test_agent_center_defaults_are_seeded(self) -> None:
        status, provider_types = self._request("GET", "/api/agent-center/provider-types")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(provider_types["items"]), 1)
        supported_modes = {item["provider_type"] for item in provider_types["items"]}
        self.assertTrue({"openai", "azure_openai", "anthropic", "gemini", "cohere", "custom_openai", "mock"}.issubset(supported_modes))

        status, providers = self._request("GET", "/api/agent-center/providers")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(providers["items"]), 1)

        status, plugins = self._request("GET", "/api/agent-center/plugins")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(plugins["items"]), 1)

        status, agent_templates = self._request("GET", "/api/agent-center/agent-templates")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(agent_templates["items"]), 1)

        status, team_templates = self._request("GET", "/api/agent-center/team-templates")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(team_templates["items"]), 1)

    def test_deleted_default_mock_provider_is_not_reseeded_after_restart(self) -> None:
        status, providers = self._request("GET", "/api/agent-center/providers")
        self.assertEqual(status, 200)
        mock_provider = next((item for item in providers["items"] if item["id"] == "prov_mock_local"), None)
        self.assertIsNotNone(mock_provider)

        delete_status, deleted = self._request("DELETE", "/api/agent-center/providers/prov_mock_local")
        self.assertEqual(delete_status, 200)
        self.assertTrue(deleted["deleted"])

        self._restart_app()

        reload_status, reloaded_providers = self._request("GET", "/api/agent-center/providers")
        self.assertEqual(reload_status, 200)
        self.assertFalse(any(item["id"] == "prov_mock_local" for item in reloaded_providers["items"]))

    def test_team_template_build_creates_blueprint_snapshot_and_run(self) -> None:
        status, team_templates = self._request("GET", "/api/agent-center/team-templates")
        self.assertEqual(status, 200)
        team_template_id = team_templates["items"][0]["id"]

        build_status, build = self._request(
            "POST",
            "/api/agent-center/builds",
            {"team_template_id": team_template_id, "name": "software_delivery_build"},
        )
        self.assertEqual(build_status, 200)
        self.assertIsNotNone(build.get("blueprint_id"))
        self.assertGreaterEqual(len(build["resource_lock_json"].get("agent_templates", [])), 1)

        blueprint_status, blueprint = self._request("GET", f"/api/blueprints/{build['blueprint_id']}")
        self.assertEqual(blueprint_status, 200)
        self.assertEqual(blueprint["spec_json"]["metadata"]["communication_policy"], "graph-ancestor-scoped")

        run_status, bundle = self._request(
            "POST",
            "/api/task-releases",
            {
                "build_id": build["id"],
                "title": "Build driven delivery",
                "prompt": "Use the compiled team build to drive delivery.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")
        self.assertGreaterEqual(len(bundle["steps"]), 1)

    def test_custom_agent_center_resources_can_be_created_and_built(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "custom_mock_provider",
                "name": "自定义 Mock",
                "provider_type": "mock",
                "description": "Custom provider",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, plugin = self._request(
            "POST",
            "/api/agent-center/plugins",
            {
                "key": "custom_docs_kit",
                "name": "自定义文档插件",
                "version": "v1",
                "plugin_type": "toolset",
                "description": "Docs toolset",
                "manifest": {
                    "workbench_key": "docs_custom",
                    "tools": ["docs", "search"],
                    "permissions": ["readonly"],
                    "description": "Docs workbench",
                },
            },
        )
        self.assertEqual(status, 200)

        status, agent_template = self._request(
            "POST",
            "/api/agent-center/agent-templates",
            {
                "key": "custom_planner",
                "name": "自定义规划师",
                "role": "planner",
                "description": "Custom planner",
                "spec": {
                    "goal": "Plan the work.",
                    "instructions": "Return a structured plan.",
                    "provider_ref": provider["id"],
                    "model": "mock-plan",
                    "memory_policy": "agent_private",
                    "plugin_refs": [plugin["id"]],
                    "skills": ["planning"],
                },
            },
        )
        self.assertEqual(status, 200)

        team_spec = {
            "workspace_id": "local-workspace",
            "project_id": "default-project",
            "agents": [{"key": "planner", "name": "Custom Planner", "agent_template_ref": agent_template["id"]}],
            "flow": {
                "nodes": [
                    {"id": "start", "type": "start"},
                    {"id": "plan", "type": "agent", "agent": "planner", "instruction": "Create the project plan."},
                    {"id": "end", "type": "end"},
                ],
                "edges": [
                    {"from": "start", "to": "plan"},
                    {"from": "plan", "to": "end"},
                ],
            },
            "definition_of_done": [],
            "acceptance_checks": [],
            "metadata": {"communication_policy": "graph-ancestor-scoped"},
        }
        status, team_template = self._request(
            "POST",
            "/api/agent-center/team-templates",
            {
                "key": "custom_team",
                "name": "自定义团队",
                "description": "Custom team",
                "spec": team_spec,
            },
        )
        self.assertEqual(status, 200)

        build_status, build = self._request(
            "POST",
            "/api/agent-center/builds",
            {"team_template_id": team_template["id"], "name": "custom_team_build"},
        )
        self.assertEqual(build_status, 200)
        self.assertEqual(build["resource_lock_json"]["team_template"]["id"], team_template["id"])
        self.assertEqual(build["resource_lock_json"]["agent_templates"][0]["id"], agent_template["id"])

    def test_provider_list_supports_filter_and_pagination(self) -> None:
        for name in ("Alpha Provider", "Beta Provider"):
            status, _payload = self._request(
                "POST",
                "/api/agent-center/providers",
                {
                    "name": name,
                    "provider_type": "mock",
                    "config": {
                        "base_url": "mock://local",
                        "models": [{"name": f"{name.lower().replace(' ', '-')}-chat", "model_type": "chat"}],
                    },
                },
            )
            self.assertEqual(status, 200)

        status, payload = self._request("GET", "/api/agent-center/providers?query=alpha&provider_type=mock&limit=1&offset=0")
        self.assertEqual(status, 200)
        self.assertEqual(payload["limit"], 1)
        self.assertGreaterEqual(payload["total"], 1)
        self.assertEqual(len(payload["items"]), 1)
        self.assertIn("Alpha", payload["items"][0]["name"])

    def test_provider_models_can_be_saved_discovered_and_tested(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Mock Catalog",
                "provider_type": "mock",
                "config": {
                    "base_url": "mock://local",
                    "models": [
                        {"name": "mock-chat", "model_type": "chat", "context_window": 8192},
                        {"name": "mock-embedding", "model_type": "embedding"},
                        {"name": "mock-rerank", "model_type": "rerank"},
                    ],
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(provider["key"], "mock-catalog")
        self.assertEqual(provider["config_json"]["model"], "mock-chat")
        self.assertEqual(len(provider["config_json"]["models"]), 3)

        status, discovered = self._request(
            "POST",
            "/api/agent-center/providers/discover-models",
            {
                "name": "Mock Catalog",
                "provider_type": "mock",
                "config": {"models": provider["config_json"]["models"]},
            },
        )
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(discovered["items"]), 3)

        status, tested = self._request(
            "POST",
            "/api/agent-center/providers/test-model",
            {
                "provider": {
                    "name": "Mock Catalog",
                    "provider_type": "mock",
                    "config": {"models": provider["config_json"]["models"]},
                },
                "model": {"name": "mock-chat", "model_type": "chat"},
            },
        )
        self.assertEqual(status, 200)
        self.assertTrue(tested["ok"])
        self.assertEqual(tested["model_type"], "chat")

    def test_custom_openai_provider_keeps_rerank_models(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Custom OpenAI Catalog",
                "provider_type": "custom_openai",
                "config": {
                    "base_url": "https://example.com/v1",
                    "models": [
                        {"name": "custom-chat", "model_type": "chat"},
                        {"name": "custom-embedding", "model_type": "embedding"},
                        {"name": "custom-reranker", "model_type": "rerank"},
                    ],
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(provider["config_json"]["models"]), 3)
        self.assertEqual(
            [item["model_type"] for item in provider["config_json"]["models"]],
            ["chat", "embedding", "rerank"],
        )
        self.assertEqual(provider["model_count"], 3)
        self.assertIn("rerank", provider["supported_model_types"])

    def test_provider_can_be_deleted(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Delete Provider",
                "provider_type": "mock",
                "config": {
                    "base_url": "mock://local",
                    "models": [{"name": "delete-chat", "model_type": "chat"}],
                },
            },
        )
        self.assertEqual(status, 200)

        delete_status, deleted = self._request("DELETE", f"/api/agent-center/providers/{provider['id']}")
        self.assertEqual(delete_status, 200)
        self.assertTrue(deleted["deleted"])
        self.assertEqual(deleted["id"], provider["id"])

        get_status, get_payload = self._request("GET", f"/api/agent-center/providers/{provider['id']}")
        self.assertEqual(get_status, 404)
        self.assertIn("detail", get_payload)

    def test_team_graph_validate_and_preview_endpoints(self) -> None:
        status, team_templates = self._request("GET", "/api/agent-center/team-templates")
        self.assertEqual(status, 200)
        spec = team_templates["items"][0]["spec_json"]

        validate_status, validation = self._request(
            "POST",
            "/api/agent-center/team-templates/graph/validate",
            {"spec": spec},
        )
        self.assertEqual(validate_status, 200)
        self.assertTrue(validation["valid"])
        self.assertGreaterEqual(validation["summary"]["node_count"], 1)
        self.assertIn("communication_policy", validation["summary"])

        preview_status, preview = self._request(
            "POST",
            "/api/agent-center/team-templates/graph/preview",
            {"spec": spec, "name": "preview_team"},
        )
        self.assertEqual(preview_status, 200)
        self.assertTrue(preview["valid"])
        self.assertGreaterEqual(preview["preview"]["agent_count"], 1)
        self.assertGreaterEqual(preview["preview"]["node_count"], 1)

    def test_plugin_sandbox_load_invoke_and_reload(self) -> None:
        package_dir = self._create_plugin_package(name="echo_plugin", code_tag="v1")
        status, plugin = self._request(
            "POST",
            "/api/agent-center/plugins",
            {
                "key": "echo_plugin",
                "name": "Echo Plugin",
                "version": "v1",
                "plugin_type": "toolset",
                "description": "Echo plugin",
                "install_path": str(package_dir),
                "manifest": {"workbench_key": "echo", "tools": ["echo"], "permissions": ["readonly"]},
            },
        )
        self.assertEqual(status, 200)

        load_status, loaded = self._request("POST", f"/api/agent-center/plugins/{plugin['id']}/load", {})
        self.assertEqual(load_status, 200)
        self.assertEqual(loaded["status"], "running")

        health_status, health = self._request("GET", f"/api/agent-center/plugins/{plugin['id']}/health")
        self.assertEqual(health_status, 200)
        self.assertEqual(health["health"]["status"], "ok")
        self.assertEqual(health["health"]["code_tag"], "v1")

        invoke_status, invoke = self._request(
            "POST",
            f"/api/agent-center/plugins/{plugin['id']}/invoke",
            {"action": "echo", "payload": {"text": "hello"}, "context": {"node_id": "manual_node"}},
        )
        self.assertEqual(invoke_status, 200)
        self.assertEqual(invoke["result"]["payload"]["text"], "hello")
        self.assertEqual(invoke["result"]["code_tag"], "v1")

        (package_dir / "backend" / "entry.py").write_text(
            textwrap.dedent(
                """
                class EchoPlugin:
                    def __init__(self, *, manifest, root_path):
                        self.manifest = manifest
                        self.root_path = root_path

                    def describe(self):
                        return {
                            "key": self.manifest["key"],
                            "tools": ["echo"],
                            "permissions": ["readonly"],
                            "actions": [{"name": "echo", "description": "echo payload"}],
                        }

                    def health(self):
                        return {"status": "ok", "code_tag": "v2"}

                    def invoke(self, action, payload, context):
                        return {
                            "action": action,
                            "payload": payload,
                            "context_node": context.get("node_id"),
                            "code_tag": "v2",
                        }

                    def shutdown(self):
                        return {"status": "bye"}
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        reload_status, reloaded = self._request("POST", f"/api/agent-center/plugins/{plugin['id']}/reload", {})
        self.assertEqual(reload_status, 200)
        self.assertEqual(reloaded["status"], "running")

        invoke_status, invoke = self._request(
            "POST",
            f"/api/agent-center/plugins/{plugin['id']}/invoke",
            {"action": "echo", "payload": {"text": "hello-again"}, "context": {"node_id": "manual_node"}},
        )
        self.assertEqual(invoke_status, 200)
        self.assertEqual(invoke["result"]["payload"]["text"], "hello-again")
        self.assertEqual(invoke["result"]["code_tag"], "v2")

    def test_runtime_agent_node_can_invoke_plugin_actions(self) -> None:
        package_dir = self._create_plugin_package(name="runtime_echo_plugin", code_tag="runtime")
        status, plugin = self._request(
            "POST",
            "/api/agent-center/plugins",
            {
                "key": "runtime_echo_plugin",
                "name": "Runtime Echo Plugin",
                "version": "v1",
                "plugin_type": "toolset",
                "description": "Runtime echo plugin",
                "install_path": str(package_dir),
                "manifest": {"workbench_key": "runtime_echo", "tools": ["echo"], "permissions": ["readonly"]},
            },
        )
        self.assertEqual(status, 200)

        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "runtime_plugin_provider",
                "name": "Runtime Plugin Provider",
                "provider_type": "mock",
                "description": "Mock provider",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_template = self._request(
            "POST",
            "/api/agent-center/agent-templates",
            {
                "key": "runtime_plugin_agent",
                "name": "Runtime Plugin Agent",
                "role": "developer",
                "description": "Agent with plugin",
                "spec": {
                    "goal": "Invoke plugin actions.",
                    "instructions": "Use plugins when configured.",
                    "provider_ref": provider["id"],
                    "model": "mock-dev",
                    "memory_policy": "agent_private",
                    "plugin_refs": [plugin["id"]],
                    "skills": ["plugin"],
                },
            },
        )
        self.assertEqual(status, 200)

        team_spec = {
            "workspace_id": "local-workspace",
            "project_id": "default-project",
            "agents": [{"key": "worker", "name": "Worker", "agent_template_ref": agent_template["id"]}],
            "flow": {
                "nodes": [
                    {"id": "start", "type": "start"},
                    {
                        "id": "work",
                        "type": "agent",
                        "agent": "worker",
                        "instruction": "Run plugin action.",
                        "config": {
                            "plugin_actions": [
                                {
                                    "plugin_id": plugin["id"],
                                    "action": "echo",
                                    "payload": {"text": "runtime-call"},
                                }
                            ]
                        },
                    },
                    {"id": "end", "type": "end"},
                ],
                "edges": [
                    {"from": "start", "to": "work"},
                    {"from": "work", "to": "end"},
                ],
            },
            "definition_of_done": [],
            "acceptance_checks": [],
            "metadata": {"communication_policy": "graph-ancestor-scoped"},
        }
        status, team_template = self._request(
            "POST",
            "/api/agent-center/team-templates",
            {
                "key": "runtime_plugin_team",
                "name": "Runtime Plugin Team",
                "description": "Team with runtime plugin actions",
                "spec": team_spec,
            },
        )
        self.assertEqual(status, 200)

        build_status, build = self._request(
            "POST",
            "/api/agent-center/builds",
            {"team_template_id": team_template["id"], "name": "runtime_plugin_build"},
        )
        self.assertEqual(build_status, 200)

        run_status, bundle = self._request(
            "POST",
            "/api/task-releases",
            {
                "build_id": build["id"],
                "prompt": "Run plugin-integrated node.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")
        work_step = next(step for step in bundle["steps"] if step["node_id"] == "work")
        self.assertEqual(work_step["output_json"]["plugin_results"][0]["result"]["payload"]["text"], "runtime-call")
        self.assertEqual(work_step["output_json"]["plugin_results"][0]["result"]["code_tag"], "runtime")
