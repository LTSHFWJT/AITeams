from __future__ import annotations

import asyncio
import json
import tempfile
import textwrap
import unittest
from uuid import UUID
from urllib.error import URLError
from pathlib import Path
from unittest import mock

from aiteams.app import AppSettings, create_app
from aiteams.memory.scope import MemoryScopes
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

    def _create_configurable_plugin_package(self, *, name: str) -> Path:
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
                entrypoint: backend.entry:ConfigurablePlugin
                workbench_key: {name}
                tools:
                  - inspect_runtime
                permissions:
                  - network_http
                actions:
                  - name: inspect_runtime
                    description: inspect runtime config
                config_schema:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                    service:
                      type: object
                      properties:
                        endpoint:
                          type: string
                        timeout_seconds:
                          type: integer
                        shared_secret:
                          type: string
                          format: password
                runtime:
                  enabled: true
                  service:
                    endpoint: http://127.0.0.1:8000
                    timeout_seconds: 5
                    shared_secret: ""
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        (backend / "entry.py").write_text(
            textwrap.dedent(
                """
                class ConfigurablePlugin:
                    def __init__(self, *, manifest, root_path):
                        self.manifest = manifest
                        self.root_path = root_path

                    def describe(self):
                        return {
                            "key": self.manifest["key"],
                            "tools": ["inspect_runtime"],
                            "permissions": ["network_http"],
                            "actions": [{"name": "inspect_runtime", "description": "inspect runtime config"}],
                        }

                    def health(self):
                        return {
                            "status": "ok",
                            "runtime": dict(self.manifest.get("runtime") or {}),
                            "runtime_secret": dict(self.manifest.get("runtime_secret") or {}),
                        }

                    def invoke(self, action, payload, context):
                        return {
                            "action": action,
                            "runtime": dict(self.manifest.get("runtime") or {}),
                            "runtime_secret": dict(self.manifest.get("runtime_secret") or {}),
                        }

                    def shutdown(self):
                        return {"status": "bye"}
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

    def test_manual_approval_can_resume_after_restart(self) -> None:
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

        approval_id = bundle["approvals"][0]["id"]
        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval_id}/resolve",
            {"approved": True, "comment": "Continue after restart."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        self._restart_app()

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
        self.assertEqual(payload["storage"]["checkpoint_driver"], "sqlite")
        self.assertEqual(payload["storage"]["journal_mode"], "wal")
        self.assertTrue(payload["storage"]["metadata_path"].endswith("platform.db"))
        self.assertTrue(payload["storage"]["checkpoint_path"].endswith("langgraph-checkpoints.sqlite"))
        memory_root = self._settings.memory_root.resolve()
        memory_stack = payload["storage"]["memory_stack"]
        self.assertEqual(Path(memory_stack["store"]["path"]).parent, memory_root)
        self.assertEqual(Path(memory_stack["kv"]["path"]).parent, memory_root)
        self.assertEqual(Path(memory_stack["feedback"]["path"]).parent, memory_root)
        self.assertEqual(Path(memory_stack["store"]["path"]).name, "langgraph-store.sqlite3")
        self.assertEqual(Path(memory_stack["kv"]["path"]).name, "working-memory.sqlite3")
        self.assertEqual(Path(memory_stack["feedback"]["path"]).name, "feedback.sqlite3")

    def test_control_plane_requirements_report_describes_coverage_and_gaps(self) -> None:
        status, payload = self._request("GET", "/api/control-plane/requirements-report")
        self.assertEqual(status, 200)
        self.assertEqual(payload["overall"]["total"], 6)
        self.assertGreaterEqual(payload["overall"]["implemented"], 1)
        self.assertGreaterEqual(payload["overall"]["partial"], 1)
        self.assertEqual(payload["storage_assessment"]["fit"], "official_sqlite_store")
        requirement_ids = {item["id"] for item in payload["requirements"]}
        self.assertEqual(requirement_ids, {"R1", "R2", "R3", "R4", "R5", "R6"})
        r6 = next(item for item in payload["requirements"] if item["id"] == "R6")
        self.assertEqual(r6["status"], "implemented")
        self.assertEqual(r6["evidence"]["checkpoint_driver"], "sqlite")
        self.assertEqual(r6["evidence"]["memory_stack"]["kv"]["driver"], "sqlite")
        self.assertEqual(r6["evidence"]["memory_stack"]["vector"]["driver"], "sqlite")

    def test_working_memory_uses_langmem_running_summary(self) -> None:
        memory = self._app.container.runtime.agent_kernel.memory
        scope = MemoryScopes(
            workspace_id="local-workspace",
            project_id="default-project",
            run_id="run_summary",
            agent_id="memory-agent",
        ).working()
        runtime = {
            "provider": {"provider_type": "mock", "model": "mock-model"},
            "short_term": {"enabled": True, "summary_trigger_tokens": 256, "summary_max_tokens": 96},
        }
        long_message = "需求上下文 " + ("alpha " * 80)
        for index in range(4):
            asyncio.run(
                memory.append_working(
                    scope,
                    "user" if index % 2 == 0 else "assistant",
                    f"{long_message}{index}",
                    runtime=runtime,
                )
            )

        working = asyncio.run(memory.load_working(scope))
        self.assertGreaterEqual(len(working), 2)
        self.assertEqual(working[0]["role"], "system")
        self.assertTrue(working[0]["metadata"].get("running_summary"))
        self.assertIn("summary", working[0]["content"].lower())

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
        openai_preset = next(item for item in provider_types["items"] if item["provider_type"] == "openai")
        mock_preset = next(item for item in provider_types["items"] if item["provider_type"] == "mock")
        self.assertNotIn("gateway_capabilities", openai_preset)
        self.assertNotIn("gateway_capabilities", mock_preset)

        status, providers = self._request("GET", "/api/agent-center/providers")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(providers["items"]), 1)
        mock_provider = next(item for item in providers["items"] if item["provider_type"] == "mock")
        self.assertNotIn("gateway_capabilities", mock_provider["config_json"])

        status, plugins = self._request("GET", "/api/agent-center/plugins")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(plugins["items"]), 1)

        status, agent_templates = self._request("GET", "/api/agent-center/agent-templates")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(agent_templates["items"]), 1)

        status, team_templates = self._request("GET", "/api/agent-center/team-templates")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(team_templates["items"]), 1)

    def test_agent_center_ui_metadata_exposes_structured_review_and_team_edge_options(self) -> None:
        status, payload = self._request("GET", "/api/agent-center/ui-metadata")
        self.assertEqual(status, 200)
        self.assertIn("review_policy", payload)
        self.assertIn("team_edge_review", payload)
        self.assertIn("memory_profile", payload)

        review_triggers = {item["value"] for item in payload["review_policy"]["triggers"]}
        self.assertIn("before_tool_call", review_triggers)
        self.assertIn("before_agent_to_agent_message", review_triggers)

        review_actions = {item["value"] for item in payload["review_policy"]["actions"]}
        self.assertTrue({"approve", "reject", "edit_payload"}.issubset(review_actions))

        review_message_types = {item["value"] for item in payload["review_policy"]["message_types"]}
        self.assertIn("dialogue", review_message_types)
        self.assertIn("delivery", review_message_types)

        team_edge_modes = {item["value"] for item in payload["team_edge_review"]["modes"]}
        self.assertEqual(team_edge_modes, {"must_review_before"})
        team_edge_phases = {item["value"] for item in payload["team_edge_review"]["phases"]}
        self.assertEqual(team_edge_phases, {"down", "up"})

        memory_profile_scopes = {item["value"] for item in payload["memory_profile"]["scopes"]}
        self.assertIn("agent", memory_profile_scopes)
        self.assertIn("retrospective", memory_profile_scopes)

    def test_langgraph_resource_defaults_are_seeded(self) -> None:
        for path in (
            "/api/agent-center/skills",
            "/api/agent-center/static-memories",
            "/api/agent-center/memory-profiles",
            "/api/agent-center/review-policies",
            "/api/agent-center/agent-definitions",
            "/api/agent-center/team-definitions",
        ):
            status, payload = self._request("GET", path)
            self.assertEqual(status, 200)
            self.assertGreaterEqual(len(payload["items"]), 1)

        plugin_status, plugins = self._request("GET", "/api/agent-center/plugins")
        self.assertEqual(plugin_status, 200)
        memory_core = next(item for item in plugins["items"] if item["key"] == "memory_core")
        self.assertIn("memory.background_reflection", memory_core["manifest_json"]["tools"])

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

        blueprint_status, blueprint = self._request("GET", f"/api/blueprints/{build['blueprint_id']}")
        self.assertEqual(blueprint_status, 200)
        role_template = next(iter(blueprint["spec_json"]["role_templates"].values()))
        self.assertIn("memory_core", role_template["workbenches"])
        self.assertTrue(any(item["key"] == "memory_core" for item in role_template["metadata"]["plugins"]))

    def test_agent_definition_allows_empty_plugin_skill_kb_and_review_refs(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "default_memory_provider",
                "name": "Default Memory Provider",
                "provider_type": "mock",
                "description": "Mock provider for default memory plugin",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Default Memory Agent Definition",
                "role": "analyst",
                "description": "Agent definition without explicit plugins",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-dev",
                    "tool_plugin_refs": [],
                    "skill_refs": [],
                    "knowledge_base_refs": [],
                    "review_policy_refs": [],
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(definition["spec_json"]["tool_plugin_refs"], [])
        self.assertEqual(definition["spec_json"]["skill_refs"], [])
        self.assertEqual(definition["spec_json"]["knowledge_base_refs"], [])
        self.assertEqual(definition["spec_json"]["review_policy_refs"], [])

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "empty_refs_team_definition",
                "name": "Empty Refs Team Definition",
                "description": "Single-member team for empty reference coverage",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "members": [
                        {
                            "key": "empty_refs_worker",
                            "name": "Empty Refs Worker",
                            "level": 8,
                            "agent_definition_ref": definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                        }
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        role_template = next(iter(compiled["blueprint_spec"]["role_templates"].values()))
        self.assertEqual(role_template["workbenches"], [])
        self.assertEqual(role_template["metadata"]["plugins"], [])

    def test_team_definition_can_compile_and_start_task(self) -> None:
        status, team_definitions = self._request("GET", "/api/agent-center/team-definitions")
        self.assertEqual(status, 200)
        team_definition_id = team_definitions["items"][0]["id"]

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition_id}/compile", {})
        self.assertEqual(compile_status, 200)
        self.assertEqual(compiled["blueprint_spec"]["metadata"]["runtime"], "langgraph-official")
        self.assertEqual(compiled["blueprint_spec"]["metadata"]["execution_mode"], "team_event_driven")
        self.assertGreaterEqual(compiled["preview"]["member_count"], 1)
        self.assertIn("planner", compiled["adjacency"])

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition_id}/tasks",
            {
                "title": "Team definition delivery",
                "prompt": "Use the hierarchical team to deliver an implementation plan.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")
        self.assertTrue(bundle["task_thread"]["id"].startswith("thread_"))
        self.assertTrue(any(item["node_type"] == "agent" for item in bundle["steps"]))

        messages_status, messages = self._request("GET", f"/api/message-events?run_id={bundle['run']['id']}")
        self.assertEqual(messages_status, 200)
        self.assertGreaterEqual(len(messages["items"]), 1)
        self.assertEqual(messages["items"][0]["message_type"], "handoff")
        self.assertEqual(messages["items"][0]["thread_id"], bundle["task_thread"]["id"])

    def test_deepagents_team_definition_can_compile_nested_tree_from_templates_and_team_refs(self) -> None:
        status, agent_templates = self._request("GET", "/api/agent-center/agent-templates")
        self.assertEqual(status, 200)
        templates_by_name = {item["name"]: item for item in agent_templates["items"]}

        status, child_team = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "deep_child_team",
                "name": "Deep Child Team",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "root": {
                        "kind": "team",
                        "key": "delivery_cell",
                        "name": "Delivery Cell",
                        "lead": {
                            "key": "implementer",
                            "name": "Implementer",
                            "agent_template_ref": templates_by_name["实现工程师"]["id"],
                        },
                        "children": [
                            {
                                "kind": "agent",
                                "key": "qa",
                                "name": "QA",
                                "agent_template_ref": templates_by_name["质量审查员"]["id"],
                            }
                        ],
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        status, parent_team = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "deep_parent_team",
                "name": "Deep Parent Team",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "root": {
                        "kind": "team",
                        "key": "program_root",
                        "name": "Program Root",
                        "lead": {
                            "key": "planner",
                            "name": "Planner",
                            "agent_template_ref": templates_by_name["策略规划师"]["id"],
                        },
                        "children": [
                            {
                                "kind": "agent",
                                "key": "architect",
                                "name": "Architect",
                                "agent_template_ref": templates_by_name["方案架构师"]["id"],
                            },
                            {
                                "kind": "team",
                                "key": "delivery_cell",
                                "team_definition_ref": child_team["id"],
                            },
                        ],
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{parent_team['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        self.assertEqual(compiled["blueprint_spec"]["metadata"]["runtime"], "deepagents")
        self.assertEqual(compiled["blueprint_spec"]["metadata"]["execution_mode"], "deepagents_hierarchy")
        self.assertEqual(compiled["preview"]["agent_count"], 4)
        self.assertEqual(compiled["preview"]["team_count"], 2)
        self.assertEqual(compiled["hierarchy"]["children"][1]["lead"]["name"], "Implementer")

    def test_deepagents_role_spec_binding_applies_to_root_and_nested_agents(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Role Spec Mock Provider",
                "provider_type": "mock",
                "description": "Mock provider for deepagents role spec binding",
                "config": {"model": "mock-model", "base_url": "mock://local", "backend": "mock"},
            },
        )
        self.assertEqual(status, 200)

        status, planner_role = self._request(
            "POST",
            "/api/agent-center/static-memories",
            {
                "key": "role.deep.root",
                "name": "Root Planner Role Spec",
                "description": "Root planner role",
                "spec": {
                    "system_prompt": "Root lead must plan before delegating and summarize final delivery.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, engineer_role = self._request(
            "POST",
            "/api/agent-center/static-memories",
            {
                "key": "role.deep.engineer",
                "name": "Nested Engineer Role Spec",
                "description": "Nested engineer role",
                "spec": {
                    "system_prompt": "Nested engineer lead must execute implementation tasks carefully.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, reviewer_role = self._request(
            "POST",
            "/api/agent-center/static-memories",
            {
                "key": "role.deep.reviewer",
                "name": "Leaf Reviewer Role Spec",
                "description": "Leaf reviewer role",
                "spec": {
                    "system_prompt": "Leaf reviewer must check the delegated result and return a concise verdict.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, root_agent = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Deep Root Agent",
                "role": "planner",
                "description": "Root deepagents lead",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-model",
                    "goal": "Coordinate the whole team.",
                    "instructions": "Delegate only after clarifying the plan.",
                    "static_memory_ref": planner_role["id"],
                },
            },
        )
        self.assertEqual(status, 200)

        status, nested_lead_agent = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Deep Nested Lead Agent",
                "role": "developer",
                "description": "Nested deepagents lead",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-model",
                    "goal": "Lead the delivery cell.",
                    "instructions": "Break the delivery work into direct child tasks.",
                    "static_memory_ref": engineer_role["id"],
                },
            },
        )
        self.assertEqual(status, 200)

        status, leaf_agent = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Deep Leaf Agent",
                "role": "reviewer",
                "description": "Leaf deepagents worker",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-model",
                    "goal": "Review the delegated work.",
                    "instructions": "Return only the review result.",
                    "static_memory_ref": reviewer_role["id"],
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "deep_role_spec_team",
                "name": "Deep Role Spec Team",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "root": {
                        "kind": "team",
                        "key": "program_root",
                        "name": "Program Root",
                        "lead": {
                            "key": "root_lead",
                            "name": "Root Lead",
                            "agent_definition_ref": root_agent["id"],
                        },
                        "children": [
                            {
                                "kind": "team",
                                "key": "delivery_cell",
                                "name": "Delivery Cell",
                                "lead": {
                                    "key": "nested_lead",
                                    "name": "Nested Lead",
                                    "agent_definition_ref": nested_lead_agent["id"],
                                },
                                "children": [
                                    {
                                        "kind": "agent",
                                        "key": "leaf_reviewer",
                                        "name": "Leaf Reviewer",
                                        "agent_definition_ref": leaf_agent["id"],
                                    }
                                ],
                            }
                        ],
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        hierarchy = compiled["hierarchy"]
        self.assertEqual(hierarchy["lead"]["role_spec"]["name"], planner_role["name"])
        self.assertIn("Root lead must plan before delegating", hierarchy["lead"]["system_prompt"])
        nested_team = hierarchy["children"][0]
        self.assertEqual(nested_team["lead"]["role_spec"]["name"], engineer_role["name"])
        self.assertIn("Nested engineer lead must execute implementation tasks carefully.", nested_team["lead"]["system_prompt"])
        leaf = nested_team["children"][0]
        self.assertEqual(leaf["role_spec"]["name"], reviewer_role["name"])
        self.assertIn("Leaf reviewer must check the delegated result", leaf["system_prompt"])
        root_role_template = next(
            item for item in compiled["blueprint_spec"]["role_templates"].values() if item["name"] == planner_role["name"]
        )
        self.assertIsNotNone(root_role_template["metadata"]["role_spec"])

    def test_deepagents_team_node_role_spec_maps_to_team_runtime_fields(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Team Node Role Provider",
                "provider_type": "mock",
                "description": "Mock provider for team node role binding",
                "config": {"model": "mock-model", "base_url": "mock://local", "backend": "mock"},
            },
        )
        self.assertEqual(status, 200)

        status, root_team_role = self._request(
            "POST",
            "/api/agent-center/static-memories",
            {
                "name": "Program Coordinator",
                "description": "Coordinates the whole nested program.",
                "spec": {
                    "system_prompt": "Coordinate the whole program and keep the final delivery consistent.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, nested_team_role = self._request(
            "POST",
            "/api/agent-center/static-memories",
            {
                "name": "Delivery Cell Coordinator",
                "description": "Coordinates the nested delivery cell.",
                "spec": {
                    "system_prompt": "Lead the nested delivery cell and synchronize direct children.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, lead_agent = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Team Lead Runtime",
                "role": "planner",
                "description": "Provides model and tools for team nodes.",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-model",
                    "goal": "Coordinate nested work.",
                    "instructions": "Delegate only to direct children.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, leaf_agent = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Leaf Worker Runtime",
                "role": "developer",
                "description": "Leaf worker",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-model",
                    "goal": "Execute leaf work.",
                    "instructions": "Complete the delegated task directly.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "deep_team_node_role_spec_team",
                "name": "Deep Team Node Role Spec Team",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "root": {
                        "kind": "team",
                        "key": "program_root",
                        "name": "Program Root Runtime",
                        "static_memory_ref": root_team_role["id"],
                        "lead": {
                            "key": "root_lead",
                            "name": "Root Lead Runtime",
                            "agent_definition_ref": lead_agent["id"],
                        },
                        "children": [
                            {
                                "kind": "team",
                                "key": "delivery_cell",
                                "name": "Delivery Cell Runtime",
                                "static_memory_ref": nested_team_role["id"],
                                "lead": {
                                    "key": "nested_lead",
                                    "name": "Nested Lead Runtime",
                                    "agent_definition_ref": lead_agent["id"],
                                },
                                "children": [
                                    {
                                        "kind": "agent",
                                        "key": "leaf_worker",
                                        "name": "Leaf Worker Runtime",
                                        "agent_definition_ref": leaf_agent["id"],
                                    }
                                ],
                            }
                        ],
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        hierarchy = compiled["hierarchy"]
        self.assertEqual(hierarchy["name"], root_team_role["name"])
        self.assertEqual(hierarchy["description"], root_team_role["description"])
        self.assertIn("Coordinate the whole program", hierarchy["system_prompt"])
        nested_team = hierarchy["children"][0]
        self.assertEqual(nested_team["name"], nested_team_role["name"])
        self.assertEqual(nested_team["description"], nested_team_role["description"])
        self.assertIn("Lead the nested delivery cell", nested_team["system_prompt"])

    def test_deepagents_team_template_ref_compiles_and_persists_runtime_snapshot(self) -> None:
        status, agent_templates = self._request("GET", "/api/agent-center/agent-templates")
        self.assertEqual(status, 200)
        templates_by_name = {item["name"]: item for item in agent_templates["items"]}

        status, nested_template = self._request(
            "POST",
            "/api/agent-center/team-templates",
            {
                "key": "deep_nested_delivery_cell",
                "name": "Deep Nested Delivery Cell",
                "description": "Nested delivery cell template",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "root": {
                        "kind": "team",
                        "key": "delivery_cell",
                        "name": "Delivery Cell",
                        "lead": {
                            "key": "implementer",
                            "name": "Implementer",
                            "agent_template_ref": templates_by_name["实现工程师"]["id"],
                        },
                        "children": [
                            {
                                "kind": "agent",
                                "key": "qa",
                                "name": "QA",
                                "agent_template_ref": templates_by_name["质量审查员"]["id"],
                            }
                        ],
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "deep_runtime_template_ref_team",
                "name": "Deep Runtime Template Ref Team",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "root": {
                        "kind": "team",
                        "key": "program_root",
                        "name": "Program Root",
                        "lead": {
                            "key": "planner",
                            "name": "Planner",
                            "agent_template_ref": templates_by_name["策略规划师"]["id"],
                        },
                        "children": [
                            {
                                "kind": "agent",
                                "key": "architect",
                                "name": "Architect",
                                "agent_template_ref": templates_by_name["方案架构师"]["id"],
                            },
                            {
                                "kind": "team",
                                "key": "delivery_cell",
                                "team_template_ref": nested_template["id"],
                            },
                        ],
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        self.assertEqual(compiled["preview"]["agent_count"], 4)
        self.assertEqual(compiled["preview"]["team_count"], 2)
        self.assertEqual(compiled["hierarchy"]["children"][1]["name"], "Delivery Cell")
        self.assertTrue(any(item["id"] == nested_template["id"] for item in compiled["resource_lock"]["team_templates"]))

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Template ref runtime",
                "prompt": "Coordinate the nested delivery cell and summarize the work.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")

        snapshot = self._app.container.store.get_team_build_snapshot_by_run(str(bundle["run"]["id"]))
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["team_definition_id"], team_definition["id"])
        compilation_tree = snapshot["compiled_metadata_json"]["node_compilation_tree"]
        self.assertEqual(compilation_tree["compiled_kind"], "root_deep_agent")
        self.assertEqual(UUID(compilation_tree["agent_id"]).version, 7)
        self.assertEqual(compilation_tree["agent_id"], compilation_tree["lead_agent_id"])
        self.assertEqual(compilation_tree["children"][0]["compiled_kind"], "subagent")
        self.assertEqual(UUID(compilation_tree["children"][0]["agent_id"]).version, 7)
        self.assertEqual(compilation_tree["children"][1]["compiled_kind"], "compiled_subagent")
        self.assertEqual(UUID(compilation_tree["children"][1]["agent_id"]).version, 7)
        self.assertEqual(compilation_tree["children"][1]["children"][0]["compiled_kind"], "subagent")
        runtime_root = snapshot["runtime_tree_snapshot_json"]
        self.assertEqual(UUID(runtime_root["agent_id"]).version, 7)
        self.assertEqual(runtime_root["agent_id"], runtime_root["lead"]["agent_id"])
        self.assertTrue(any(item["id"] == nested_template["id"] for item in snapshot["resource_lock_json"]["team_templates"]))

    def test_deepagents_team_definition_can_start_task_and_write_artifact(self) -> None:
        status, agent_templates = self._request("GET", "/api/agent-center/agent-templates")
        self.assertEqual(status, 200)
        templates_by_name = {item["name"]: item for item in agent_templates["items"]}

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "deep_runtime_team",
                "name": "Deep Runtime Team",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "root": {
                        "kind": "team",
                        "key": "delivery_root",
                        "name": "Delivery Root",
                        "lead": {
                            "key": "lead",
                            "name": "Lead",
                            "agent_template_ref": templates_by_name["策略规划师"]["id"],
                        },
                        "children": [
                            {
                                "kind": "agent",
                                "key": "architect",
                                "name": "Architect",
                                "agent_template_ref": templates_by_name["方案架构师"]["id"],
                            },
                            {
                                "kind": "agent",
                                "key": "qa",
                                "name": "QA",
                                "agent_template_ref": templates_by_name["质量审查员"]["id"],
                            },
                        ],
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Deep runtime delivery",
                "prompt": "Coordinate the nested team and produce a concise delivery summary.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")
        self.assertEqual(bundle["steps"][0]["node_id"], "deepagents_orchestrate")
        self.assertEqual(bundle["artifacts"][0]["name"], "team-summary.md")
        self.assertTrue(bundle["task_thread"]["id"].startswith("thread_"))
        self.assertEqual(bundle["task_thread"]["metadata_json"]["mode"], "deepagents_hierarchy")

    def test_deepagents_team_definition_task_review_can_pause_and_resume(self) -> None:
        status, agent_templates = self._request("GET", "/api/agent-center/agent-templates")
        self.assertEqual(status, 200)
        templates_by_name = {item["name"]: item for item in agent_templates["items"]}

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "deep_review_team",
                "name": "Deep Review Team",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "root": {
                        "kind": "team",
                        "key": "review_root",
                        "name": "Review Root",
                        "lead": {
                            "key": "lead",
                            "name": "Lead",
                            "agent_template_ref": templates_by_name["策略规划师"]["id"],
                            "review_policy_refs": ["review.cross_level_message"],
                        },
                        "children": [
                            {
                                "kind": "agent",
                                "key": "architect",
                                "name": "Architect",
                                "agent_template_ref": templates_by_name["方案架构师"]["id"],
                            }
                        ],
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "prompt": "Delegate work down the hierarchy and wait for approval.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")
        self.assertEqual(len(bundle["approvals"]), 1)
        self.assertIn("delegating work", bundle["approvals"][0]["detail"])

        approval_id = bundle["approvals"][0]["id"]
        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval_id}/resolve",
            {"approved": True, "comment": "Approved delegated handoff."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed["run"]["status"], "completed")
        self.assertEqual(resumed["artifacts"][0]["name"], "team-summary.md")

    def test_team_definition_compile_honors_entry_and_specific_finish_policies(self) -> None:
        status, agent_definitions = self._request("GET", "/api/agent-center/agent-definitions")
        self.assertEqual(status, 200)
        agent_definition_id = agent_definitions["items"][0]["id"]

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "policy_team_definition",
                "name": "Policy Team Definition",
                "description": "Explicit entry and finish policies",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "task_entry_policy": {"mode": "specific_agent", "agent_id": "reviewer"},
                    "termination_policy": {"mode": "specific_agents", "finish_agent_ids": ["planner"]},
                    "members": [
                        {
                            "key": "planner",
                            "name": "Planner",
                            "level": 10,
                            "agent_definition_ref": agent_definition_id,
                            "can_receive_task": True,
                            "can_finish_task": False,
                        },
                        {
                            "key": "reviewer",
                            "name": "Reviewer",
                            "level": 8,
                            "agent_definition_ref": agent_definition_id,
                            "can_receive_task": False,
                            "can_finish_task": False,
                        },
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        team_runtime = dict((compiled["blueprint_spec"].get("metadata") or {}).get("team_runtime") or {})
        self.assertEqual(team_runtime["entry_agent_id"], "reviewer")
        self.assertEqual(team_runtime["finish_agent_ids"], ["planner"])
        self.assertEqual(team_runtime["task_entry_policy"]["mode"], "specific_agent")
        self.assertEqual(team_runtime["task_entry_policy"]["agent_id"], "reviewer")
        self.assertEqual(team_runtime["termination_policy"]["mode"], "specific_agents")
        self.assertEqual(team_runtime["termination_policy"]["finish_agent_ids"], ["planner"])

    def test_team_definition_update_merges_existing_spec_and_entry_agent_termination(self) -> None:
        status, agent_definitions = self._request("GET", "/api/agent-center/agent-definitions")
        self.assertEqual(status, 200)
        agent_definition_id = agent_definitions["items"][0]["id"]

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "merge_policy_team_definition",
                "name": "Merge Policy Team Definition",
                "description": "Spec merge coverage for explicit frontend fields",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "communication_policy": {"type": "adjacent_level_all"},
                    "custom_flag": {"enabled": True},
                    "members": [
                        {
                            "key": "planner",
                            "name": "Planner",
                            "level": 10,
                            "agent_definition_ref": agent_definition_id,
                            "can_receive_task": False,
                            "can_finish_task": False,
                        },
                        {
                            "key": "reviewer",
                            "name": "Reviewer",
                            "level": 8,
                            "agent_definition_ref": agent_definition_id,
                            "can_receive_task": True,
                            "can_finish_task": True,
                        },
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        update_status, updated = self._request(
            "PUT",
            f"/api/agent-center/team-definitions/{team_definition['id']}",
            {
                "key": team_definition["key"],
                "name": "Merge Policy Team Definition Updated",
                "description": "Updated explicit policy fields only",
                "spec": {
                    "task_entry_policy": {"mode": "specific_agent", "agent_id": "planner"},
                    "termination_policy": {"mode": "entry_agent"},
                },
            },
        )
        self.assertEqual(update_status, 200)
        self.assertEqual(updated["spec_json"]["workspace_id"], "local-workspace")
        self.assertEqual(updated["spec_json"]["project_id"], "default-project")
        self.assertEqual(updated["spec_json"]["communication_policy"]["type"], "adjacent_level_all")
        self.assertEqual(updated["spec_json"]["custom_flag"]["enabled"], True)
        self.assertEqual(len(updated["spec_json"]["members"]), 2)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        team_runtime = dict((compiled["blueprint_spec"].get("metadata") or {}).get("team_runtime") or {})
        self.assertEqual(team_runtime["entry_agent_id"], "planner")
        self.assertEqual(team_runtime["finish_agent_ids"], ["planner"])
        self.assertEqual(team_runtime["task_entry_policy"]["mode"], "specific_agent")
        self.assertEqual(team_runtime["termination_policy"]["mode"], "entry_agent")

    def test_team_definition_agent_can_auto_retrieve_knowledge_base(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "kb_runtime_provider",
                "name": "KB Runtime Provider",
                "provider_type": "mock",
                "description": "Mock provider for KB retrieval",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, knowledge_base = self._request(
            "POST",
            "/api/agent-center/knowledge-bases",
            {
                "key": "release_kb",
                "name": "Release Knowledge Base",
                "description": "Release playbooks and checklists",
            },
        )
        self.assertEqual(status, 200)

        status, _document = self._request(
            "POST",
            "/api/agent-center/knowledge-documents",
            {
                "knowledge_base_id": knowledge_base["id"],
                "key": "release-checklist",
                "title": "Release Checklist",
                "content_text": (
                    "Every production release must include smoke tests, a rollback plan, "
                    "and an incident contact roster."
                ),
                "metadata": {"topic": "release"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "KB Worker Definition",
                "role": "analyst",
                "description": "Reads bound knowledge bases",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-dev",
                    "goal": "Use knowledge bases to answer execution questions.",
                    "instructions": "Retrieve relevant knowledge before responding.",
                    "knowledge_base_refs": [knowledge_base["id"]],
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "kb_team_definition",
                "name": "KB Team Definition",
                "description": "Single-member team with KB access",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "members": [
                        {
                            "key": "kb_worker",
                            "name": "KB Worker",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                        }
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        role_template = next(iter(compiled["blueprint_spec"]["role_templates"].values()))
        self.assertEqual(role_template["metadata"]["knowledge_bases"][0]["key"], knowledge_base["key"])

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "KB assisted release plan",
                "prompt": "Summarize the release checklist requirements for a production deployment.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")

        agent_steps = [step for step in bundle["steps"] if step["node_type"] == "agent"]
        self.assertGreaterEqual(len(agent_steps), 1)
        plugin_results = list((agent_steps[0]["output_json"].get("details") or {}).get("plugin_results") or [])
        kb_result = next((item for item in plugin_results if item.get("plugin_key") == "kb.retrieve"), None)
        self.assertIsNotNone(kb_result)
        self.assertGreaterEqual(kb_result["result"]["count"], 1)
        self.assertEqual(kb_result["result"]["items"][0]["knowledge_base_key"], "release_kb")
        self.assertIn("smoke tests", kb_result["result"]["items"][0]["snippet"].lower())

    def test_team_definition_shared_resources_flow_into_runtime(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "shared_resource_provider",
                "name": "Shared Resource Provider",
                "provider_type": "mock",
                "description": "Mock provider for team shared resources",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, knowledge_base = self._request(
            "POST",
            "/api/agent-center/knowledge-bases",
            {
                "key": "shared_release_kb",
                "name": "Shared Release KB",
                "description": "Shared team release playbook",
            },
        )
        self.assertEqual(status, 200)

        status, _document = self._request(
            "POST",
            "/api/agent-center/knowledge-documents",
            {
                "knowledge_base_id": knowledge_base["id"],
                "key": "shared-release-guardrails",
                "title": "Shared Release Guardrails",
                "content_text": "Shared release guardrails require rollback owners, smoke tests, and approval checkpoints.",
                "metadata": {"topic": "release"},
            },
        )
        self.assertEqual(status, 200)

        status, static_memory = self._request(
            "POST",
            "/api/agent-center/static-memories",
            {
                "key": "shared_release_memory",
                "name": "Shared Release Memory",
                "description": "Shared release guardrails for the whole team",
                "spec": {
                    "system_prompt": "统一发布守卫，优先确认回滚负责人。缺少审批节点或缺少回滚路径时必须升级。",
                },
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Shared Resource Agent Definition",
                "role": "analyst",
                "description": "Consumes team shared knowledge and static memory",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-dev",
                    "goal": "Use shared team context before responding.",
                    "instructions": "Work from the team shared operating context.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "shared_resource_team_definition",
                "name": "Shared Resource Team Definition",
                "description": "Single-member team using shared KB and shared static memory",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "shared_kb_bindings": [knowledge_base["id"]],
                    "shared_static_memory_bindings": [static_memory["id"]],
                    "members": [
                        {
                            "key": "shared_worker",
                            "name": "Shared Worker",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                        }
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        compile_status, compiled = self._request("POST", f"/api/agent-center/team-definitions/{team_definition['id']}/compile", {})
        self.assertEqual(compile_status, 200)
        role_template = next(iter(compiled["blueprint_spec"]["role_templates"].values()))
        team_runtime = dict((compiled["blueprint_spec"].get("metadata") or {}).get("team_runtime") or {})
        self.assertEqual(role_template["metadata"]["knowledge_bases"][0]["key"], knowledge_base["key"])
        self.assertEqual(role_template["metadata"]["team_shared_knowledge_bases"][0]["key"], knowledge_base["key"])
        self.assertEqual(team_runtime["shared_knowledge_bases"][0]["key"], knowledge_base["key"])
        self.assertEqual(team_runtime["shared_static_memories"][0]["key"], static_memory["key"])
        self.assertIn("Shared team role spec `Shared Release Memory`", role_template["instructions"])
        self.assertIn("统一发布守卫", role_template["instructions"])

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Shared resource run",
                "prompt": "Summarize the shared release guardrails for deployment readiness.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")

        agent_steps = [step for step in bundle["steps"] if step["node_type"] == "agent"]
        self.assertGreaterEqual(len(agent_steps), 1)
        plugin_results = list((agent_steps[0]["output_json"].get("details") or {}).get("plugin_results") or [])
        kb_result = next((item for item in plugin_results if item.get("plugin_key") == "kb.retrieve"), None)
        self.assertIsNotNone(kb_result)
        self.assertGreaterEqual(kb_result["result"]["count"], 1)
        self.assertEqual(kb_result["result"]["items"][0]["knowledge_base_key"], knowledge_base["key"])
        self.assertIn("rollback owners", kb_result["result"]["items"][0]["snippet"].lower())

    def test_team_definition_tool_call_review_can_pause_and_resume(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "kb_review_provider",
                "name": "KB Review Provider",
                "provider_type": "mock",
                "description": "Mock provider for KB review",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, knowledge_base = self._request(
            "POST",
            "/api/agent-center/knowledge-bases",
            {
                "key": "review_release_kb",
                "name": "Review Release KB",
                "description": "Release guidance for tool-call review",
            },
        )
        self.assertEqual(status, 200)

        status, _document = self._request(
            "POST",
            "/api/agent-center/knowledge-documents",
            {
                "knowledge_base_id": knowledge_base["id"],
                "key": "rollback-guidance",
                "title": "Rollback Guidance",
                "content_text": "Release execution requires smoke tests, rollback steps, and an incident owner.",
                "metadata": {"topic": "release"},
            },
        )
        self.assertEqual(status, 200)

        status, review_policy = self._request(
            "POST",
            "/api/agent-center/review-policies",
            {
                "key": "review.kb_tool_call_only",
                "name": "KB Tool Call Review",
                "description": "Review kb.retrieve before execution.",
                "version": "v1",
                "spec": {
                    "triggers": ["before_tool_call"],
                    "conditions": {"plugin_keys": ["kb.retrieve"]},
                    "actions": ["approve", "reject", "edit_payload"],
                },
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "KB Review Worker Definition",
                "role": "analyst",
                "description": "Reads bound knowledge bases with review",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-dev",
                    "goal": "Use knowledge bases to answer release questions.",
                    "instructions": "Retrieve relevant knowledge before responding.",
                    "knowledge_base_refs": [knowledge_base["id"]],
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "kb_review_team_definition",
                "name": "KB Review Team Definition",
                "description": "Single-member team with KB tool-call review",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "review_policy_refs": [review_policy["id"]],
                    "members": [
                        {
                            "key": "kb_reviewer",
                            "name": "KB Reviewer",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                        }
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        auto_status, auto_bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "KB tool review auto mode",
                "prompt": "Summarize the release execution requirements.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(auto_status, 200)
        self.assertEqual(auto_bundle["run"]["status"], "completed")
        self.assertEqual(len(auto_bundle["approvals"]), 0)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "KB tool review manual mode",
                "prompt": "Summarize the release execution requirements.",
                "approval_mode": "manual",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")
        self.assertEqual(len(bundle["approvals"]), 1)

        approval = bundle["approvals"][0]
        approval_event = next(item for item in bundle["events"] if item["event_type"] == "approval.requested")
        review = dict(approval_event["payload_json"]["review"])
        self.assertEqual(review["scope"], "tool_call")
        self.assertEqual(review["proposed_action"]["plugin_key"], "kb.retrieve")
        self.assertEqual(review["proposed_action"]["action"], "retrieve")

        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval['id']}/resolve",
            {"approved": True, "comment": "Allow KB retrieval."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed["run"]["status"], "completed")
        self.assertEqual(len(resumed["approvals"]), 1)

        agent_step = next(step for step in resumed["steps"] if step["node_type"] == "agent" and step["status"] == "done")
        plugin_results = list(agent_step["output_json"].get("plugin_results") or [])
        kb_result = next((item for item in plugin_results if item.get("plugin_key") == "kb.retrieve"), None)
        self.assertIsNotNone(kb_result)
        review_result = dict(kb_result.get("review") or kb_result.get("result", {}).get("review") or {})
        self.assertEqual(review_result["approval_id"], approval["id"])
        self.assertGreaterEqual(kb_result["result"]["count"], 1)
        self.assertIn("rollback", kb_result["result"]["items"][0]["snippet"].lower())

    def test_team_runtime_builtin_dialogue_router_routes_send_and_reply(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "dialogue_router_provider",
                "name": "Dialogue Router Provider",
                "provider_type": "mock",
                "description": "Mock provider for dialogue router tests",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Dialogue Router Agent Definition",
                "role": "planner",
                "description": "Participates in direct adjacent dialogue",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-dialogue",
                    "goal": "Coordinate adjacent-level dialogue.",
                    "instructions": "Exchange concise messages with adjacent teammates.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "dialogue_router_team_definition",
                "name": "Dialogue Router Team Definition",
                "description": "Two-level team with explicit send/reply builtins",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "members": [
                        {
                            "key": "leader",
                            "name": "Leader",
                            "level": 10,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                            "runtime_plugin_actions": [
                                {
                                    "plugin_key": "team.message.send",
                                    "action": "send",
                                    "when_message_types": ["task"],
                                    "payload": {
                                        "target_agent_id": "worker",
                                        "message_type": "dialogue",
                                    },
                                }
                            ],
                        },
                        {
                            "key": "worker",
                            "name": "Worker",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                            "runtime_plugin_actions": [
                                {
                                    "plugin_key": "team.message.reply",
                                    "action": "reply",
                                    "when_message_types": ["dialogue"],
                                    "payload": {
                                        "message_type": "dialogue",
                                    },
                                }
                            ],
                        },
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Dialogue Router direct exchange",
                "prompt": "Let the leader delegate to the worker and receive a direct reply.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")

        agent_steps = [step for step in bundle["steps"] if step["node_type"] == "agent" and step["status"] == "done"]
        self.assertGreaterEqual(len(agent_steps), 3)

        messages_status, messages = self._request("GET", f"/api/message-events?run_id={bundle['run']['id']}")
        self.assertEqual(messages_status, 200)
        dialogue_events = [item for item in messages["items"] if item["message_type"] == "dialogue"]
        self.assertGreaterEqual(len(dialogue_events), 2)
        self.assertTrue(
            any(item["source_agent_id"] == "leader" and item["target_agent_id"] == "worker" for item in dialogue_events)
        )
        self.assertTrue(
            any(item["source_agent_id"] == "worker" and item["target_agent_id"] == "leader" for item in dialogue_events)
        )
        delivery_events = [item for item in messages["items"] if item["message_type"] == "delivery"]
        self.assertGreaterEqual(len(delivery_events), 1)

    def test_team_definition_review_override_can_force_pre_review_for_agent_edge(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "edge_review_provider",
                "name": "Edge Review Provider",
                "provider_type": "mock",
                "description": "Mock provider for team edge review override",
                "config": {
                    "model": "mock-model",
                    "backend": "mock",
                    "base_url": "mock://local",
                    "models": [{"name": "mock-chat", "model_type": "chat"}],
                },
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Edge Review Agent Definition",
                "role": "planner",
                "description": "Uses explicit adjacent dialogue routing",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-chat",
                    "goal": "Coordinate adjacent delivery work.",
                    "instructions": "Use direct adjacent dialogue when delegation is needed.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "edge_review_team_definition",
                "name": "Edge Review Team Definition",
                "description": "Requires human pre-review on one agent edge",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "review_overrides": [
                        {
                            "source_agent_id": "leader",
                            "target_agent_id": "worker",
                            "message_types": ["dialogue"],
                            "mode": "must_review_before",
                        }
                    ],
                    "members": [
                        {
                            "key": "leader",
                            "name": "Leader",
                            "level": 10,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                            "runtime_plugin_actions": [
                                {
                                    "plugin_key": "team.message.send",
                                    "action": "send",
                                    "when_message_types": ["task"],
                                    "payload": {
                                        "target_agent_id": "worker",
                                        "message_type": "dialogue",
                                    },
                                }
                            ],
                        },
                        {
                            "key": "worker",
                            "name": "Worker",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                        },
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Edge review override",
                "prompt": "Send one direct message from leader to worker and wait for approval.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")
        self.assertEqual(len(bundle["approvals"]), 1)

        approval = bundle["approvals"][0]
        review = json.loads(approval["detail"])
        self.assertEqual(review["scope"], "agent_message")
        self.assertEqual(review["source_agent_id"], "leader")
        self.assertEqual(review["target_agent_id"], "worker")
        self.assertEqual(review["proposed_action"]["message_type"], "dialogue")
        self.assertEqual(len(review["matched_review_overrides"]), 1)

        approval_id = approval["id"]
        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval_id}/resolve",
            {"approved": True, "comment": "Allow this specific edge dialogue."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed["run"]["status"], "completed")

    def test_team_runtime_human_can_inject_message_into_waiting_run(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "human_inject_provider",
                "name": "Human Inject Provider",
                "provider_type": "mock",
                "description": "Mock provider for human injection tests",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Human Inject Agent Definition",
                "role": "reviewer",
                "description": "Supports human intervention while paused",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-human-inject",
                    "goal": "Accept direct human intervention during a paused run.",
                    "instructions": "Escalate for human confirmation before delivery.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "human_inject_team_definition",
                "name": "Human Inject Team Definition",
                "description": "Single-member team for injected human messages",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "members": [
                        {
                            "key": "reviewer",
                            "name": "Reviewer",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                            "runtime_plugin_actions": [
                                {
                                    "plugin_key": "human.escalate",
                                    "action": "escalate",
                                    "when_message_types": ["task"],
                                    "payload": {
                                        "title": "Need human input before delivery",
                                        "detail": "Pause so a human can add extra instructions.",
                                    },
                                }
                            ],
                        }
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Paused for direct human injection",
                "prompt": "Prepare a deployment note and wait for human edits.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")

        inject_status, injected = self._request(
            "POST",
            f"/api/runs/{bundle['run']['id']}/messages",
            {
                "target_agent_id": "reviewer",
                "body": "Add rollback guidance before final delivery.",
                "message_type": "dialogue",
                "auto_resume": False,
            },
        )
        self.assertEqual(inject_status, 200)
        self.assertEqual(injected["run"]["status"], "waiting_approval")

        messages_status, messages = self._request("GET", f"/api/message-events?run_id={bundle['run']['id']}")
        self.assertEqual(messages_status, 200)
        self.assertTrue(
            any(
                item["source_agent_id"] == "human"
                and item["target_agent_id"] == "reviewer"
                and item["message_type"] == "dialogue"
                for item in messages["items"]
            )
        )

        approval_id = bundle["approvals"][0]["id"]
        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval_id}/resolve",
            {"approved": True, "comment": "Proceed with the injected guidance."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed["run"]["status"], "completed")
        agent_steps = [step for step in resumed["steps"] if step["node_type"] == "agent" and step["status"] == "done"]
        self.assertGreaterEqual(len(agent_steps), 2)

    def test_team_runtime_builtin_human_escalate_pauses_and_resumes(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "human_escalate_provider",
                "name": "Human Escalate Provider",
                "provider_type": "mock",
                "description": "Mock provider for human escalation tests",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Human Escalate Agent Definition",
                "role": "reviewer",
                "description": "Requests explicit human escalation",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-escalate",
                    "goal": "Pause until a human confirms the next step.",
                    "instructions": "Escalate to human before finalizing.",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "human_escalate_team_definition",
                "name": "Human Escalate Team Definition",
                "description": "Single-member team with explicit human escalation",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "members": [
                        {
                            "key": "reviewer",
                            "name": "Reviewer",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                            "runtime_plugin_actions": [
                                {
                                    "plugin_key": "human.escalate",
                                    "action": "escalate",
                                    "when_message_types": ["task"],
                                    "payload": {
                                        "title": "Need human confirmation",
                                        "detail": "This task requires explicit human confirmation before delivery.",
                                    },
                                }
                            ],
                        }
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Explicit human escalation",
                "prompt": "Pause this task for human confirmation before delivery.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")
        self.assertEqual(len(bundle["approvals"]), 1)

        approval_event = next(item for item in bundle["events"] if item["event_type"] == "approval.requested")
        review = dict(approval_event["payload_json"]["review"])
        self.assertEqual(review["scope"], "human_escalation")
        self.assertEqual(review["proposed_action"]["title"], "Need human confirmation")

        approval_id = bundle["approvals"][0]["id"]
        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval_id}/resolve",
            {"approved": True, "comment": "Proceed after explicit confirmation."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed["run"]["status"], "completed")
        self.assertGreaterEqual(len(resumed["artifacts"]), 1)

    def test_team_runtime_apply_memory_effects_writes_team_shared_memory(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "shared_memory_provider",
                "name": "Shared Memory Provider",
                "provider_type": "mock",
                "description": "Mock provider for shared memory tests",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Shared Memory Agent Definition",
                "role": "developer",
                "description": "Writes team shared memory",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-dev",
                    "goal": "Capture shared delivery decisions.",
                    "instructions": "Produce concise, reusable delivery guidance.",
                    "memory_profile_ref": "memory.default.collab",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "shared_memory_team_definition",
                "name": "Shared Memory Team Definition",
                "description": "Single-member team with team shared memory",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "members": [
                        {
                            "key": "memory_worker",
                            "name": "Memory Worker",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                        }
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        prompt = "Prepare an incident drill playbook for the delivery team."
        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Shared memory write",
                "prompt": prompt,
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")

        apply_step = next(step for step in bundle["steps"] if step["node_id"] == "apply_memory_effects")
        self.assertGreaterEqual(apply_step["output_json"]["applied_count"], 1)
        self.assertTrue(any(target.startswith("team:") for target in apply_step["output_json"].get("targets", [])))

        memory = self._app.container.runtime.agent_kernel.memory
        scope = MemoryScopes(
            workspace_id="local-workspace",
            project_id="default-project",
            run_id=bundle["run"]["id"],
            agent_id="memory_worker",
            team_id=team_definition["id"],
        ).team_shared()
        recalled = asyncio.run(memory.recall(scope, "incident drill playbook", top_k=5))
        self.assertGreaterEqual(len(recalled), 1)
        self.assertEqual(recalled[0]["metadata"].get("memory_source"), "team_runtime.apply_memory_effects")

    def test_team_definition_memory_write_review_can_pause_and_resume(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "memory_review_provider",
                "name": "Memory Review Provider",
                "provider_type": "mock",
                "description": "Mock provider for memory write review",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, review_policy = self._request(
            "POST",
            "/api/agent-center/review-policies",
            {
                "key": "review.team_memory_write_only",
                "name": "Team Memory Write Review",
                "description": "Review shared team memory writes before commit.",
                "version": "v1",
                "spec": {
                    "triggers": ["before_memory_write"],
                    "conditions": {"memory_scopes": ["team"]},
                    "actions": ["approve", "reject", "edit_records"],
                },
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Memory Review Agent Definition",
                "role": "developer",
                "description": "Writes reviewed team shared memory",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-dev",
                    "goal": "Capture reusable delivery practices.",
                    "instructions": "Produce concise, reusable delivery guidance.",
                    "memory_profile_ref": "memory.default.collab",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "memory_review_team_definition",
                "name": "Memory Review Team Definition",
                "description": "Single-member team with memory-write review",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "review_policy_refs": [review_policy["id"]],
                    "members": [
                        {
                            "key": "memory_reviewer",
                            "name": "Memory Reviewer",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                        }
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        auto_status, auto_bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Memory write review auto mode",
                "prompt": "Prepare a release rollback rehearsal checklist.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(auto_status, 200)
        self.assertEqual(auto_bundle["run"]["status"], "completed")
        self.assertEqual(len(auto_bundle["approvals"]), 0)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Memory write review manual mode",
                "prompt": "Prepare a release rollback rehearsal checklist.",
                "approval_mode": "manual",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")
        self.assertEqual(len(bundle["approvals"]), 1)

        approval = bundle["approvals"][0]
        approval_event = next(item for item in bundle["events"] if item["event_type"] == "approval.requested")
        review = dict(approval_event["payload_json"]["review"])
        self.assertEqual(review["scope"], "memory_write")
        self.assertEqual(review["proposed_action"]["target_scope"], "team")
        self.assertGreaterEqual(review["proposed_action"]["record_count"], 1)

        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{approval['id']}/resolve",
            {"approved": True, "comment": "Persist the reviewed shared memory."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed["run"]["status"], "completed")
        self.assertEqual(len(resumed["approvals"]), 1)

        apply_step = next(step for step in resumed["steps"] if step["node_id"] == "apply_memory_effects" and step["status"] == "done")
        self.assertGreaterEqual(apply_step["output_json"]["applied_count"], 1)
        self.assertTrue(any(target.startswith("team:") for target in apply_step["output_json"].get("targets", [])))

        memory = self._app.container.runtime.agent_kernel.memory
        scope = MemoryScopes(
            workspace_id="local-workspace",
            project_id="default-project",
            run_id=resumed["run"]["id"],
            agent_id="memory_reviewer",
            team_id=team_definition["id"],
        ).team_shared()
        recalled = asyncio.run(memory.recall(scope, "release rollback rehearsal checklist", top_k=5))
        self.assertGreaterEqual(len(recalled), 1)
        self.assertEqual(recalled[0]["metadata"].get("memory_source"), "team_runtime.apply_memory_effects")

    def test_team_definition_manual_review_can_pause_and_resume(self) -> None:
        status, team_definitions = self._request("GET", "/api/agent-center/team-definitions")
        self.assertEqual(status, 200)
        team_definition_id = team_definitions["items"][0]["id"]

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition_id}/tasks",
            {
                "title": "Team definition manual review",
                "prompt": "Use the hierarchical team to deliver an implementation plan with review gates.",
                "approval_mode": "manual",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")
        self.assertEqual(len(bundle["approvals"]), 1)
        self.assertTrue(bundle["task_thread"]["id"].startswith("thread_"))

        current = bundle
        for _ in range(8):
            if current["run"]["status"] == "completed":
                break
            self.assertEqual(current["run"]["status"], "waiting_approval")
            pending_approval = next(item for item in current["approvals"] if item["status"] == "pending")
            resolve_status, resolved = self._request(
                "POST",
                f"/api/approvals/{pending_approval['id']}/resolve",
                {"approved": True, "comment": "Continue team collaboration."},
            )
            self.assertEqual(resolve_status, 200)
            self.assertEqual(resolved["status"], "approved")
            resume_status, current = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
            self.assertEqual(resume_status, 200)

        self.assertEqual(current["run"]["status"], "completed")
        self.assertGreaterEqual(len(current["artifacts"]), 1)

    def test_team_definition_manual_review_can_resume_after_restart(self) -> None:
        status, team_definitions = self._request("GET", "/api/agent-center/team-definitions")
        self.assertEqual(status, 200)
        team_definition_id = team_definitions["items"][0]["id"]

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition_id}/tasks",
            {
                "title": "Restartable team review",
                "prompt": "Use the hierarchical team to deliver an implementation plan with review gates.",
                "approval_mode": "manual",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "waiting_approval")

        pending_approval = next(item for item in bundle["approvals"] if item["status"] == "pending")
        resolve_status, resolved = self._request(
            "POST",
            f"/api/approvals/{pending_approval['id']}/resolve",
            {"approved": True, "comment": "Continue after restart."},
        )
        self.assertEqual(resolve_status, 200)
        self.assertEqual(resolved["status"], "approved")

        self._restart_app()

        resume_status, resumed = self._request("POST", f"/api/runs/{bundle['run']['id']}/resume", {})
        self.assertEqual(resume_status, 200)
        self.assertIn(resumed["run"]["status"], {"waiting_approval", "completed"})

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

    def test_static_memory_list_supports_pagination(self) -> None:
        status, first = self._request(
            "POST",
            "/api/agent-center/static-memories",
            {
                "name": "分页角色 A",
                "description": "first page candidate",
                "spec": {"system_prompt": "Role A prompt"},
            },
        )
        self.assertEqual(status, 200)

        status, second = self._request(
            "POST",
            "/api/agent-center/static-memories",
            {
                "name": "分页角色 B",
                "description": "second page candidate",
                "spec": {"system_prompt": "Role B prompt"},
            },
        )
        self.assertEqual(status, 200)

        status, page_one = self._request("GET", "/api/agent-center/static-memories?limit=1&offset=0")
        self.assertEqual(status, 200)
        self.assertEqual(page_one["limit"], 1)
        self.assertGreaterEqual(page_one["total"], 2)
        self.assertEqual(len(page_one["items"]), 1)
        self.assertEqual(page_one["items"][0]["id"], second["id"])

        status, page_two = self._request("GET", "/api/agent-center/static-memories?limit=1&offset=1")
        self.assertEqual(status, 200)
        self.assertEqual(page_two["limit"], 1)
        self.assertEqual(page_two["offset"], 1)
        self.assertEqual(len(page_two["items"]), 1)
        self.assertEqual(page_two["items"][0]["id"], first["id"])

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
        self.assertEqual(UUID(provider["key"]).version, 7)
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

    def test_provider_detail_includes_secret_for_editor(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Editable Provider",
                "provider_type": "custom_openai",
                "config": {"base_url": "https://example.com/v1", "skip_tls_verify": True},
                "secret": {"api_key": "stored-secret"},
            },
        )
        self.assertEqual(status, 200)

        detail_status, detail = self._request("GET", f"/api/agent-center/providers/{provider['id']}")
        self.assertEqual(detail_status, 200)
        self.assertTrue(detail["has_secret"])
        self.assertEqual(detail["secret_json"]["api_key"], "stored-secret")
        self.assertTrue(detail["config_json"]["skip_tls_verify"])

    def test_provider_profile_drops_legacy_gateway_capabilities(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Capability Controlled Provider",
                "provider_type": "custom_openai",
                "config": {
                    "base_url": "https://example.com/v1",
                    "models": [{"name": "custom-chat", "model_type": "chat"}],
                    "gateway_capabilities": {
                        "native_tools": False,
                        "json_object_response": True,
                    },
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertNotIn("gateway_capabilities", provider["config_json"])
        self.assertNotIn("gateway_capabilities", provider["config_json"]["extra_config"])
        self.assertNotIn("gateway_capabilities", provider)

        detail_status, detail = self._request("GET", f"/api/agent-center/providers/{provider['id']}")
        self.assertEqual(detail_status, 200)
        self.assertNotIn("gateway_capabilities", detail["config_json"])

    def test_provider_can_be_saved_without_models_or_default_model(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Empty Mock Provider",
                "provider_type": "mock",
                "config": {
                    "base_url": "mock://local",
                    "models": [],
                    "model": "",
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(provider["model_count"], 0)
        self.assertEqual(provider["config_json"]["models"], [])
        self.assertNotIn("model", provider["config_json"])
        self.assertEqual(provider["default_model_name"], "")
        self.assertEqual(provider["default_chat_model_name"], "")

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

    def test_retrieval_settings_can_be_saved_and_applied(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Retrieval Config Provider",
                "provider_type": "mock",
                "config": {
                    "base_url": "mock://local",
                    "models": [
                        {"name": "mock-chat", "model_type": "chat"},
                        {"name": "mock-embedding", "model_type": "embedding"},
                        {"name": "mock-rerank", "model_type": "rerank"},
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        memory = self._app.container.runtime.agent_kernel.memory
        scope = MemoryScopes(
            workspace_id="local-workspace",
            project_id="default-project",
            run_id="run-retrieval-settings",
            agent_id="retrieval-agent",
        ).agent_private()
        asyncio.run(
            memory.remember(
                scope,
                [
                    {
                        "text": "Deployment rollback guidance for staged release.",
                        "summary": "Deployment rollback guidance for staged release.",
                    }
                ],
            )
        )

        status, payload = self._request(
            "PUT",
            "/api/agent-center/retrieval-settings",
            {
                "embedding": {
                    "mode": "provider",
                    "provider_id": provider["id"],
                    "model_name": "mock-embedding",
                },
                "rerank": {
                    "mode": "provider",
                    "provider_id": provider["id"],
                    "model_name": "mock-rerank",
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["settings"]["embedding"]["mode"], "provider")
        self.assertEqual(payload["settings"]["embedding"]["model_name"], "mock-embedding")
        self.assertEqual(payload["settings"]["rerank"]["model_name"], "mock-rerank")
        self.assertTrue(payload["applied"]["embedding_reindexed"])
        self.assertGreaterEqual(payload["applied"]["reindexed_items"], 1)

        get_status, current = self._request("GET", "/api/agent-center/retrieval-settings")
        self.assertEqual(get_status, 200)
        self.assertEqual(current["settings"]["embedding"]["provider_id"], provider["id"])
        self.assertEqual(current["settings"]["rerank"]["model_name"], "mock-rerank")

        control_status, control = self._request("GET", "/api/control-plane")
        self.assertEqual(control_status, 200)
        retrieval = control["storage"]["memory_stack"]["retrieval"]
        self.assertEqual(retrieval["embedding"]["mode"], "provider")
        self.assertEqual(retrieval["embedding"]["model_name"], "mock-embedding")
        self.assertEqual(retrieval["rerank"]["model_name"], "mock-rerank")

    def test_retrieval_settings_reject_wrong_model_type(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Retrieval Validation Provider",
                "provider_type": "mock",
                "config": {
                    "base_url": "mock://local",
                    "models": [
                        {"name": "mock-chat", "model_type": "chat"},
                        {"name": "mock-embedding", "model_type": "embedding"},
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        invalid_status, invalid = self._request(
            "PUT",
            "/api/agent-center/retrieval-settings",
            {
                "embedding": {
                    "mode": "provider",
                    "provider_id": provider["id"],
                    "model_name": "mock-chat",
                },
                "rerank": {"mode": "disabled"},
            },
        )
        self.assertEqual(invalid_status, 400)
        self.assertIn("must be one of: embedding", invalid["detail"])

    def test_discover_models_reuses_saved_provider_secret_when_editing(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Secured OpenAI Catalog",
                "provider_type": "custom_openai",
                "config": {
                    "base_url": "https://example.com/v1",
                    "models": [{"name": "seed-chat", "model_type": "chat"}],
                },
                "secret": {"api_key": "stored-secret"},
            },
        )
        self.assertEqual(status, 200)

        get_status, editable = self._request("GET", f"/api/agent-center/providers/{provider['id']}")
        self.assertEqual(get_status, 200)
        self.assertTrue(editable["has_secret"])

        with mock.patch.object(
            self._app.container.agent_center,
            "_request_json",
            return_value={"data": [{"id": "remote-chat"}]},
        ) as request_json:
            discover_status, discovered = self._request(
                "POST",
                "/api/agent-center/providers/discover-models",
                {
                    "id": editable["id"],
                    "name": editable["name"],
                    "provider_type": editable["provider_type"],
                    "config": editable["config_json"],
                },
            )

        self.assertEqual(discover_status, 200)
        self.assertEqual(discovered["items"][0]["name"], "remote-chat")
        _, _, kwargs = request_json.mock_calls[0]
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer stored-secret")

    def test_legacy_provider_gateway_capabilities_do_not_flow_into_built_blueprint_runtime(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "name": "Runtime Capability Provider",
                "provider_type": "custom_openai",
                "config": {
                    "base_url": "https://example.com/v1",
                    "models": [{"name": "runtime-chat", "model_type": "chat"}],
                    "gateway_capabilities": {
                        "native_tools": False,
                        "json_object_response": True,
                    },
                },
            },
        )
        self.assertEqual(status, 200)

        status, agent_template = self._request(
            "POST",
            "/api/agent-center/agent-templates",
            {
                "key": "runtime_capability_agent",
                "name": "Runtime Capability Agent",
                "role": "planner",
                "description": "Carries provider gateway capabilities into runtime",
                "spec": {
                    "goal": "Verify provider capability propagation.",
                    "instructions": "Use configured provider capability metadata.",
                    "provider_ref": provider["id"],
                    "model": "runtime-chat",
                    "memory_policy": "agent_private",
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_template = self._request(
            "POST",
            "/api/agent-center/team-templates",
            {
                "key": "runtime_capability_team",
                "name": "Runtime Capability Team",
                "description": "Minimal build for provider capability propagation",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "agents": [{"key": "planner", "name": "Planner", "agent_template_ref": agent_template["id"]}],
                    "flow": {
                        "nodes": [
                            {"id": "start", "type": "start"},
                            {"id": "plan", "type": "agent", "agent": "planner", "instruction": "Plan the work."},
                            {"id": "end", "type": "end"},
                        ],
                        "edges": [
                            {"from": "start", "to": "plan"},
                            {"from": "plan", "to": "end"},
                        ],
                    },
                    "definition_of_done": [],
                    "acceptance_checks": [],
                },
            },
        )
        self.assertEqual(status, 200)

        build_status, build = self._request(
            "POST",
            "/api/agent-center/builds",
            {"team_template_id": team_template["id"], "name": "runtime_capability_build"},
        )
        self.assertEqual(build_status, 200)

        blueprint_status, blueprint = self._request("GET", f"/api/blueprints/{build['blueprint_id']}")
        self.assertEqual(blueprint_status, 200)
        role_template = next(iter(blueprint["spec_json"]["role_templates"].values()))
        self.assertNotIn("extra_config", role_template)
        locked_provider = blueprint["spec_json"]["metadata"]["resource_lock"]["provider_profiles"][0]
        self.assertNotIn("gateway_capabilities", locked_provider)

    def test_discover_models_uses_unverified_ssl_context_when_enabled(self) -> None:
        class _Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"data":[{"id":"remote-chat"}]}'

        with mock.patch("aiteams.agent_center.service.urlopen", return_value=_Response()) as mocked_urlopen:
            status, payload = self._request(
                "POST",
                "/api/agent-center/providers/discover-models",
                {
                    "name": "Self Signed Provider",
                    "provider_type": "custom_openai",
                    "config": {
                        "base_url": "https://example.com/v1",
                        "skip_tls_verify": True,
                    },
                },
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload["items"][0]["name"], "remote-chat")
        self.assertIsNotNone(mocked_urlopen.call_args.kwargs["context"])

    def test_discover_models_requires_base_url_for_custom_openai(self) -> None:
        status, payload = self._request(
            "POST",
            "/api/agent-center/providers/discover-models",
            {
                "name": "Broken OpenAI Catalog",
                "provider_type": "custom_openai",
                "config": {},
            },
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["detail"], "OpenAI 兼容接口 Base URL 不能为空。")

    def test_mock_discover_models_returns_empty_when_provider_has_no_models(self) -> None:
        status, payload = self._request(
            "POST",
            "/api/agent-center/providers/discover-models",
            {
                "name": "Empty Mock Provider",
                "provider_type": "mock",
                "config": {
                    "base_url": "mock://local",
                    "models": [],
                    "model": "",
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["source"], "local")
        self.assertEqual(payload["items"], [])

    def test_discover_models_ssl_error_recommends_tls_toggle(self) -> None:
        with mock.patch(
            "aiteams.agent_center.service.urlopen",
            side_effect=URLError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate in certificate chain (_ssl.c:1081)"),
        ):
            status, payload = self._request(
                "POST",
                "/api/agent-center/providers/discover-models",
                {
                    "name": "Self Signed Provider",
                    "provider_type": "custom_openai",
                    "config": {"base_url": "https://example.com/v1"},
                },
            )

        self.assertEqual(status, 400)
        self.assertIn("TLS 证书校验失败", payload["detail"])
        self.assertIn("跳过 TLS 证书校验", payload["detail"])

    def test_provider_save_returns_structured_validation_error(self) -> None:
        status, payload = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "provider_type": "mock",
                "config": {"base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["detail"], "Provider 保存失败。")
        self.assertEqual(payload["error_type"], "provider_validation_error")
        self.assertIn("Provider 名称不能为空。", payload["errors"])
        self.assertEqual(payload["context"]["provider_type"], "mock")
        self.assertFalse(payload["context"]["has_secret"])

    def test_provider_key_is_internal_uuid7_and_immutable_on_update(self) -> None:
        first_status, created = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "user-specified-provider-key",
                "name": "Duplicate Provider A",
                "provider_type": "mock",
                "config": {"base_url": "mock://local"},
            },
        )
        self.assertEqual(first_status, 200)
        self.assertEqual(UUID(created["key"]).version, 7)
        self.assertNotEqual(created["key"], "user-specified-provider-key")

        status, updated = self._request(
            "PUT",
            f"/api/agent-center/providers/{created['id']}",
            {
                "key": "another-user-key",
                "name": "Duplicate Provider B",
                "provider_type": "mock",
                "config": {"base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(updated["key"], created["key"])

    def test_agent_template_id_is_internal_uuid7_and_backed_by_separate_table(self) -> None:
        requested_id = "user-specified-template-id"
        status, created = self._request(
            "POST",
            "/api/agent-center/agent-templates",
            {
                "id": requested_id,
                "name": "UUID7 Template",
                "role": "planner",
                "description": "template id should be server generated",
                "spec": {"provider_ref": "", "model": "", "plugin_refs": []},
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(UUID(created["id"]).version, 7)
        self.assertNotEqual(created["id"], requested_id)
        self.assertNotIn("key", created)

        columns = self._app.container.store.fetch_all("PRAGMA table_info(agent_definitions)")
        self.assertNotIn("key", {str(item.get("name") or "") for item in columns})

        row = self._app.container.store.fetch_one(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("agent_templates",),
        )
        self.assertEqual((row or {}).get("name"), "agent_templates")
        stored = self._app.container.store.fetch_one(
            "SELECT COUNT(*) AS count FROM agent_templates WHERE id = ?",
            (created["id"],),
        )
        self.assertEqual(int((stored or {}).get("count", 0) or 0), 1)

    def test_agent_definition_id_is_internal_uuid7_and_ignores_client_supplied_id(self) -> None:
        requested_id = "user-specified-definition-id"
        status, created = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "id": requested_id,
                "name": "UUID7 Definition",
                "role": "planner",
                "description": "definition id should be server generated",
                "spec": {"provider_ref": "", "model": "", "tool_plugin_refs": []},
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(UUID(created["id"]).version, 7)
        self.assertNotEqual(created["id"], requested_id)
        self.assertNotIn("key", created)

        columns = self._app.container.store.fetch_all("PRAGMA table_info(agent_definitions)")
        self.assertNotIn("key", {str(item.get("name") or "") for item in columns})

        duplicate_status, duplicate = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "id": requested_id,
                "name": "UUID7 Definition Second",
                "role": "planner",
                "description": "same fake client id should not overwrite previous record",
                "spec": {"provider_ref": "", "model": "", "tool_plugin_refs": []},
            },
        )
        self.assertEqual(duplicate_status, 200)
        self.assertEqual(UUID(duplicate["id"]).version, 7)
        self.assertNotEqual(duplicate["id"], requested_id)
        self.assertNotEqual(duplicate["id"], created["id"])
        self.assertNotIn("key", duplicate)

    def test_agent_definition_can_be_created_without_role(self) -> None:
        status, created = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "No Role Agent",
                "description": "Agent definition without explicit role",
                "spec": {},
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(UUID(created["id"]).version, 7)
        self.assertNotIn("key", created)
        self.assertEqual(created["role"], "agent")

    def test_agent_definition_rejects_legacy_key_field(self) -> None:
        status, payload = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "key": "legacy.agent.definition",
                "name": "Legacy Key Agent",
                "role": "planner",
                "spec": {},
            },
        )
        self.assertEqual(status, 400)
        self.assertIn("no longer accepts `key`", payload["detail"])

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

    def test_builtin_plugin_catalog_endpoint_lists_platform_builtins(self) -> None:
        status, payload = self._request("GET", "/api/agent-center/plugins/builtins")
        self.assertEqual(status, 200)
        keys = {item["key"] for item in payload["items"]}
        self.assertIn("memory.search", keys)
        self.assertIn("memory.manage", keys)
        self.assertIn("memory.background_reflection", keys)
        self.assertIn("kb.retrieve", keys)
        self.assertIn("team.message.send", keys)
        self.assertIn("team.message.reply", keys)
        self.assertIn("human.escalate", keys)

    def test_plugin_config_schema_runtime_config_and_secret_are_persisted_and_applied(self) -> None:
        package_dir = self._create_configurable_plugin_package(name="configurable_plugin")
        status, plugin = self._request(
            "POST",
            "/api/agent-center/plugins",
            {
                "key": "configurable_plugin",
                "name": "Configurable Plugin",
                "version": "v1",
                "plugin_type": "toolset",
                "description": "Configurable plugin",
                "install_path": str(package_dir),
                "config": {
                    "enabled": False,
                    "service": {
                        "endpoint": "http://example.internal",
                        "timeout_seconds": 12,
                    },
                },
                "secret": {
                    "service": {
                        "shared_secret": "top-secret",
                    }
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertFalse(plugin["config_json"]["enabled"])
        self.assertEqual(plugin["config_json"]["service"]["endpoint"], "http://example.internal")
        self.assertTrue(plugin["has_secret"])
        self.assertIn("service.shared_secret", plugin["secret_field_paths"])

        detail_status, detail = self._request("GET", f"/api/agent-center/plugins/{plugin['id']}")
        self.assertEqual(detail_status, 200)
        self.assertEqual(detail["manifest_json"]["config_schema"]["type"], "object")
        self.assertIn("service.shared_secret", detail["secret_field_paths"])

        load_status, loaded = self._request("POST", f"/api/agent-center/plugins/{plugin['id']}/load", {})
        self.assertEqual(load_status, 200)
        self.assertEqual(loaded["status"], "running")

        health_status, health = self._request("GET", f"/api/agent-center/plugins/{plugin['id']}/health")
        self.assertEqual(health_status, 200)
        self.assertFalse(health["health"]["runtime"]["enabled"])
        self.assertEqual(health["health"]["runtime"]["service"]["endpoint"], "http://example.internal")
        self.assertEqual(health["health"]["runtime"]["service"]["timeout_seconds"], 12)
        self.assertEqual(health["health"]["runtime"]["service"]["shared_secret"], "top-secret")
        self.assertEqual(health["health"]["runtime_secret"]["service"]["shared_secret"], "top-secret")

        update_status, updated = self._request(
            "PUT",
            f"/api/agent-center/plugins/{plugin['id']}",
            {
                "key": "configurable_plugin",
                "name": "Configurable Plugin",
                "version": "v1",
                "plugin_type": "toolset",
                "description": "Configurable plugin updated",
                "install_path": str(package_dir),
                "config": {
                    "enabled": True,
                    "service": {
                        "endpoint": "http://127.0.0.1:9000",
                        "timeout_seconds": 3,
                    },
                },
            },
        )
        self.assertEqual(update_status, 200)
        self.assertTrue(updated["config_json"]["enabled"])
        self.assertEqual(updated["config_json"]["service"]["endpoint"], "http://127.0.0.1:9000")
        self.assertTrue(updated["has_secret"])

        health_status, health = self._request("GET", f"/api/agent-center/plugins/{plugin['id']}/health")
        self.assertEqual(health_status, 200)
        self.assertTrue(health["health"]["runtime"]["enabled"])
        self.assertEqual(health["health"]["runtime"]["service"]["endpoint"], "http://127.0.0.1:9000")
        self.assertEqual(health["health"]["runtime"]["service"]["timeout_seconds"], 3)
        self.assertEqual(health["health"]["runtime"]["service"]["shared_secret"], "top-secret")

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

    def test_runtime_agent_node_can_invoke_builtin_memory_actions(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "builtin_memory_provider",
                "name": "Builtin Memory Provider",
                "provider_type": "mock",
                "description": "Mock provider for builtin memory actions",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_template = self._request(
            "POST",
            "/api/agent-center/agent-templates",
            {
                "key": "builtin_memory_agent",
                "name": "Builtin Memory Agent",
                "role": "analyst",
                "description": "Agent with builtin memory actions",
                "spec": {
                    "goal": "Write and search memory using builtin actions.",
                    "instructions": "Persist a fact and verify it can be searched.",
                    "provider_ref": provider["id"],
                    "model": "mock-memory",
                    "memory_policy": "agent_private",
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
                        "instruction": "Run builtin memory actions.",
                        "config": {
                            "plugin_actions": [
                                {
                                    "plugin_key": "memory.manage",
                                    "action": "manage",
                                    "payload": {
                                        "operation": "create",
                                        "scope": "agent",
                                        "record": {
                                            "text": "Builtin memory action stored release criteria for agent runtime.",
                                            "summary": "Stored release criteria memory.",
                                            "fact_key": "release.criteria.runtime",
                                        },
                                    },
                                },
                                {
                                    "plugin_key": "memory.search",
                                    "action": "search",
                                    "payload": {
                                        "scope": "agent",
                                        "query": "release criteria runtime",
                                        "limit": 3,
                                    },
                                },
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
                "key": "builtin_memory_team",
                "name": "Builtin Memory Team",
                "description": "Team with builtin memory actions",
                "spec": team_spec,
            },
        )
        self.assertEqual(status, 200)

        build_status, build = self._request(
            "POST",
            "/api/agent-center/builds",
            {"team_template_id": team_template["id"], "name": "builtin_memory_build"},
        )
        self.assertEqual(build_status, 200)

        run_status, bundle = self._request(
            "POST",
            "/api/task-releases",
            {
                "build_id": build["id"],
                "prompt": "Verify builtin memory actions.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")
        work_step = next(step for step in bundle["steps"] if step["node_id"] == "work")
        plugin_results = list(work_step["output_json"].get("plugin_results") or [])
        manage_result = next(item for item in plugin_results if item["plugin_key"] == "memory.manage")
        search_result = next(item for item in plugin_results if item["plugin_key"] == "memory.search")
        self.assertEqual(manage_result["result"]["count"], 1)
        self.assertGreaterEqual(search_result["result"]["count"], 1)
        self.assertIn("release criteria", search_result["result"]["items"][0]["text"].lower())

        memory = self._app.container.runtime.agent_kernel.memory
        scope = MemoryScopes(
            workspace_id="local-workspace",
            project_id="default-project",
            run_id=bundle["run"]["id"],
            agent_id="worker",
        ).agent_private()
        recalled = asyncio.run(memory.recall(scope, "release criteria runtime", top_k=5))
        self.assertGreaterEqual(len(recalled), 1)
        self.assertIn("release criteria", recalled[0]["text"].lower())

    def test_runtime_agent_node_can_native_plan_builtin_memory_actions(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "native_memory_provider",
                "name": "Native Memory Provider",
                "provider_type": "mock",
                "description": "Mock provider for native memory planning",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_template = self._request(
            "POST",
            "/api/agent-center/agent-templates",
            {
                "key": "native_memory_agent",
                "name": "Native Memory Agent",
                "role": "analyst",
                "description": "Uses native tool planning for builtin memory",
                "spec": {
                    "goal": "Use native tool planning for memory builtins.",
                    "instructions": "Use memory.manage to persist a fact and then use memory.search to verify it.",
                    "provider_ref": provider["id"],
                    "model": "mock-native-memory",
                    "memory_policy": "agent_private",
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
                        "instruction": "Use memory.manage first, then memory.search.",
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
                "key": "native_memory_team",
                "name": "Native Memory Team",
                "description": "Exercises native memory tool planning",
                "spec": team_spec,
            },
        )
        self.assertEqual(status, 200)

        build_status, build = self._request(
            "POST",
            "/api/agent-center/builds",
            {"team_template_id": team_template["id"], "name": "native_memory_build"},
        )
        self.assertEqual(build_status, 200)

        run_status, bundle = self._request(
            "POST",
            "/api/task-releases",
            {
                "build_id": build["id"],
                "prompt": "Store and verify a native planned memory for release readiness.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")
        work_step = next(step for step in bundle["steps"] if step["node_id"] == "work")
        plugin_results = list(work_step["output_json"].get("plugin_results") or [])
        self.assertTrue(any(item["plugin_key"] == "memory.manage" for item in plugin_results))
        self.assertTrue(any(item["plugin_key"] == "memory.search" for item in plugin_results))

    def test_team_runtime_agent_can_native_plan_adjacent_dialogue(self) -> None:
        status, provider = self._request(
            "POST",
            "/api/agent-center/providers",
            {
                "key": "native_dialogue_provider",
                "name": "Native Dialogue Provider",
                "provider_type": "mock",
                "description": "Mock provider for native dialogue planning",
                "config": {"model": "mock-model", "backend": "mock", "base_url": "mock://local"},
            },
        )
        self.assertEqual(status, 200)

        status, agent_definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Native Dialogue Agent Definition",
                "role": "planner",
                "description": "Uses native tool planning for team dialogue builtins",
                "spec": {
                    "provider_ref": provider["id"],
                    "model": "mock-native-dialogue",
                    "goal": "Use direct adjacent dialogue builtins when helpful.",
                    "instructions": (
                        "When a human task arrives and you have an adjacent teammate, use team.message.send. "
                        "When you receive a down-phase dialogue from another agent, use team.message.reply."
                    ),
                },
            },
        )
        self.assertEqual(status, 200)

        status, team_definition = self._request(
            "POST",
            "/api/agent-center/team-definitions",
            {
                "key": "native_dialogue_team_definition",
                "name": "Native Dialogue Team Definition",
                "description": "Two-level team using native dialogue planning",
                "spec": {
                    "workspace_id": "local-workspace",
                    "project_id": "default-project",
                    "members": [
                        {
                            "key": "leader",
                            "name": "Leader",
                            "level": 10,
                            "agent_definition_ref": agent_definition["id"],
                            "can_receive_task": True,
                            "can_finish_task": True,
                        },
                        {
                            "key": "worker",
                            "name": "Worker",
                            "level": 8,
                            "agent_definition_ref": agent_definition["id"],
                        },
                    ],
                },
            },
        )
        self.assertEqual(status, 200)

        run_status, bundle = self._request(
            "POST",
            f"/api/agent-center/team-definitions/{team_definition['id']}/tasks",
            {
                "title": "Native dialogue planning",
                "prompt": "Ask the adjacent worker for a delivery detail and get one reply back.",
                "approval_mode": "auto",
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(bundle["run"]["status"], "completed")

        messages_status, messages = self._request("GET", f"/api/message-events?run_id={bundle['run']['id']}")
        self.assertEqual(messages_status, 200)
        dialogue_events = [item for item in messages["items"] if item["message_type"] == "dialogue"]
        self.assertTrue(any(item["source_agent_id"] == "leader" and item["target_agent_id"] == "worker" for item in dialogue_events))
        self.assertTrue(any(item["source_agent_id"] == "worker" and item["target_agent_id"] == "leader" for item in dialogue_events))

    def test_agent_center_explicit_resource_routes_support_crud(self) -> None:
        status, providers = self._request("GET", "/api/agent-center/providers")
        self.assertEqual(status, 200)
        provider_id = providers["items"][0]["id"]
        model_name = providers["items"][0]["default_chat_model_name"]

        status, kb = self._request(
            "POST",
            "/api/agent-center/knowledge-bases",
            {
                "key": "kb.test.docs",
                "name": "Test KB",
                "description": "Knowledge base for explicit page testing.",
            },
        )
        self.assertEqual(status, 200)

        status, fetched_kb = self._request("GET", f"/api/agent-center/knowledge-bases/{kb['id']}")
        self.assertEqual(status, 200)
        self.assertEqual(fetched_kb["key"], "kb.test.docs")

        status, document = self._request(
            "POST",
            "/api/agent-center/knowledge-documents",
            {
                "knowledge_base_id": kb["id"],
                "key": "doc.test.one",
                "title": "Test Doc",
                "content_text": "kb retrieve should be able to find this content",
            },
        )
        self.assertEqual(status, 200)

        status, documents = self._request("GET", f"/api/agent-center/knowledge-documents?knowledge_base_id={kb['id']}")
        self.assertEqual(status, 200)
        self.assertEqual(len(documents["items"]), 1)
        self.assertEqual(documents["items"][0]["id"], document["id"])

        status, policy = self._request(
            "POST",
            "/api/agent-center/review-policies",
            {
                "key": "review.test.edge",
                "name": "Test Review Policy",
                "description": "Review policy route test.",
                "spec": {"triggers": ["before_agent_to_agent_message"], "conditions": {"message_types": ["dialogue"]}},
            },
        )
        self.assertEqual(status, 200)

        status, definition = self._request(
            "POST",
            "/api/agent-center/agent-definitions",
            {
                "name": "Route Test Agent",
                "role": "tester",
                "description": "Agent definition route test.",
                "spec": {
                    "provider_ref": provider_id,
                    "model": model_name,
                    "goal": "test routes",
                    "instructions": "keep it concise",
                    "review_policy_refs": [policy["id"]],
                },
            },
        )
        self.assertEqual(status, 200)

        status, fetched_definition = self._request("GET", f"/api/agent-center/agent-definitions/{definition['id']}")
        self.assertEqual(status, 200)
        self.assertNotIn("key", fetched_definition)

        status, delete_document = self._request("DELETE", f"/api/agent-center/knowledge-documents/{document['id']}")
        self.assertEqual(status, 200)
        self.assertTrue(delete_document["deleted"])

        status, delete_policy = self._request("DELETE", f"/api/agent-center/review-policies/{policy['id']}")
        self.assertEqual(status, 200)
        self.assertTrue(delete_policy["deleted"])

        status, delete_definition = self._request("DELETE", f"/api/agent-center/agent-definitions/{definition['id']}")
        self.assertEqual(status, 200)
        self.assertTrue(delete_definition["deleted"])
