from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from aiteams.plugins.manifest import validate_plugin_package
from aiteams.plugins.sandbox import PluginSandbox


PROJECT_ROOT = Path(__file__).resolve().parents[1]

EXAMPLE_CASES = [
    {
        "name": "demo_echo_plugin",
        "path": PROJECT_ROOT / "examples" / "demo_echo_plugin",
        "cases": [
            {
                "action": "echo",
                "payload": {"text": "hello"},
                "assertions": lambda test, result: (
                    test.assertTrue(result["ok"]),
                    test.assertEqual(result["payload"]["text"], "hello"),
                ),
            },
            {
                "action": "inspect_context",
                "payload": {},
                "context": {"workspace_id": "ws", "project_id": "proj", "agent_id": "agent_a"},
                "assertions": lambda test, result: (
                    test.assertEqual(result["workspace_id"], "ws"),
                    test.assertEqual(result["project_id"], "proj"),
                    test.assertEqual(result["agent_id"], "agent_a"),
                ),
            },
        ],
    },
    {
        "name": "demo_text_toolkit_plugin",
        "path": PROJECT_ROOT / "examples" / "demo_text_toolkit_plugin",
        "cases": [
            {
                "action": "normalize_text",
                "payload": {"text": "  Hello   Plugin World  ", "case": "lower"},
                "assertions": lambda test, result: (
                    test.assertEqual(result["normalized"], "hello plugin world"),
                    test.assertEqual(result["action"], "normalize_text"),
                ),
            },
            {
                "action": "extract_keywords",
                "payload": {"text": "alpha beta alpha gamma beta alpha", "top_k": 2},
                "assertions": lambda test, result: (
                    test.assertEqual(result["keyword_count"], 2),
                    test.assertEqual(result["keywords"][0]["keyword"], "alpha"),
                ),
            },
        ],
    },
    {
        "name": "demo_task_board_plugin",
        "path": PROJECT_ROOT / "examples" / "demo_task_board_plugin",
        "cases": [
            {
                "action": "plan_steps",
                "payload": {
                    "goal": "Prepare a release note",
                    "constraints": ["keep it short"],
                    "deliverables": ["release_note.md"],
                },
                "assertions": lambda test, result: (
                    test.assertEqual(result["goal"], "Prepare a release note"),
                    test.assertGreaterEqual(len(result["steps"]), 4),
                    test.assertIn("step_1", result["board"]["backlog"]),
                ),
            },
            {
                "action": "check_completion",
                "payload": {
                    "checklist": ["spec done", "tests done"],
                    "completed": ["spec done"],
                    "evidence": {"tests done": "test report attached"},
                },
                "assertions": lambda test, result: (
                    test.assertTrue(result["passed"]),
                    test.assertEqual(result["missing_count"], 0),
                ),
            },
        ],
    },
    {
        "name": "demo_artifact_review_plugin",
        "path": PROJECT_ROOT / "examples" / "demo_artifact_review_plugin",
        "cases": [
            {
                "action": "summarize_artifacts",
                "payload": {
                    "artifacts": [
                        {"name": "plan.md", "kind": "report", "path": "artifacts/plan.md", "summary": "Plan summary"},
                        {"name": "result.json", "kind": "data", "path": "", "summary": ""},
                    ]
                },
                "assertions": lambda test, result: (
                    test.assertEqual(result["artifact_count"], 2),
                    test.assertEqual(result["kinds"]["report"], 1),
                    test.assertIn("result.json", result["missing_paths"]),
                ),
            },
            {
                "action": "compare_revisions",
                "payload": {"before": "line1\nline2", "after": "line1\nline2 changed\nline3"},
                "assertions": lambda test, result: (
                    test.assertTrue(result["changed"]),
                    test.assertGreaterEqual(result["added_lines"], 1),
                    test.assertGreaterEqual(len(result["preview"]), 1),
                ),
            },
        ],
    },
    {
        "name": "demo_http_ingress_bridge_plugin",
        "path": PROJECT_ROOT / "examples" / "demo_http_ingress_bridge_plugin",
        "cases": [
            {
                "action": "bind_agent",
                "payload": {
                    "ait_api_base": "http://127.0.0.1:8000",
                    "run_id": "run_demo",
                    "target_agent_id": "planner",
                },
                "assertions": lambda test, result: (
                    test.assertTrue(result["ok"]),
                    test.assertEqual(result["binding"]["target_agent_id"], "planner"),
                ),
            }
        ],
    },
    {
        "name": "demo_feishu_command_bridge_plugin",
        "path": PROJECT_ROOT / "examples" / "demo_feishu_command_bridge_plugin",
        "cases": [
            {
                "action": "configure_bridge",
                "payload": {
                    "bridge": {
                        "ait_api_base": "http://127.0.0.1:8000",
                        "run_id": "run_feishu",
                        "target_agent_id": "reviewer",
                        "command_prefix": "/ait",
                    }
                },
                "assertions": lambda test, result: (
                    test.assertTrue(result["ok"]),
                    test.assertEqual(result["health"]["bridge"]["target_agent_id"], "reviewer"),
                ),
            }
        ],
    },
]


class _MockBridgeService:
    def __init__(self, *, auto_reply_text: str | None = None):
        self.auto_reply_text = auto_reply_text
        self.injections: list[dict[str, object]] = []
        self.reply_posts: list[dict[str, object]] = []
        self.task_threads: list[dict[str, object]] = []
        self.message_events_by_run: dict[str, list[dict[str, object]]] = {}
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.base_url = ""
        self.reply_url = ""

    def __enter__(self) -> "_MockBridgeService":
        outer = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/api/task-threads":
                    team_definition_id = parse_qs(parsed.query).get("team_definition_id", [""])[0]
                    items = [item for item in outer.task_threads if str(item.get("team_definition_id") or "") == team_definition_id]
                    self._write_json(200, {"items": items})
                    return
                if parsed.path == "/api/message-events":
                    run_id = parse_qs(parsed.query).get("run_id", [""])[0]
                    self._write_json(200, {"items": list(outer.message_events_by_run.get(run_id, []))})
                    return
                self._write_json(404, {"error": "not_found"})

            def do_POST(self) -> None:  # noqa: N802
                payload = self._read_json()
                parsed = urlparse(self.path)
                if parsed.path.startswith("/api/runs/") and parsed.path.endswith("/messages"):
                    run_id = parsed.path.split("/")[3]
                    outer.injections.append({"run_id": run_id, "payload": payload})
                    if outer.auto_reply_text:
                        events = outer.message_events_by_run.setdefault(run_id, [])
                        events.append(
                            {
                                "id": f"msg_{len(events) + 1}",
                                "run_id": run_id,
                                "source_agent_id": str(payload.get("target_agent_id") or ""),
                                "target_agent_id": "human",
                                "message_type": "dialogue",
                                "payload_json": {"summary": outer.auto_reply_text},
                            }
                        )
                    self._write_json(200, {"ok": True, "run_id": run_id})
                    return
                if parsed.path == "/feishu-reply":
                    outer.reply_posts.append(payload)
                    self._write_json(200, {"ok": True})
                    return
                self._write_json(404, {"error": "not_found"})

            def log_message(self, format: str, *args: object) -> None:  # noqa: A003
                return None

            def _read_json(self) -> dict[str, object]:
                length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(length) if length > 0 else b"{}"
                return json.loads(body.decode("utf-8"))

            def _write_json(self, status_code: int, payload: dict[str, object]) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._server.daemon_threads = True
        host, port = self._server.server_address
        self.base_url = f"http://{host}:{port}"
        self.reply_url = f"{self.base_url}/feishu-reply"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)


class ExamplePluginTests(unittest.TestCase):
    def test_example_plugin_packages_validate(self) -> None:
        for spec in EXAMPLE_CASES:
            with self.subTest(example=spec["name"]):
                manifest = validate_plugin_package(spec["path"])
                self.assertTrue(manifest["key"])
                self.assertTrue(manifest["workbench_key"])
                self.assertTrue(manifest["entry_exists"])
                self.assertGreaterEqual(len(manifest["actions"]), 1)

    def test_example_plugin_sandboxes_invoke(self) -> None:
        for spec in EXAMPLE_CASES:
            with self.subTest(example=spec["name"]):
                manifest = validate_plugin_package(spec["path"])
                sandbox = PluginSandbox(
                    plugin_id=f"example:{manifest['key']}",
                    manifest=manifest,
                    root_path=Path(spec["path"]),
                )
                try:
                    sandbox.start()
                    descriptor = sandbox.describe()
                    self.assertEqual(descriptor["key"], manifest["key"])
                    health = sandbox.health()
                    self.assertEqual(health["status"], "ok")
                    for case in spec["cases"]:
                        result = sandbox.invoke(case["action"], case["payload"], case.get("context", {}))
                        case["assertions"](self, result)
                finally:
                    sandbox.stop()

    def test_http_ingress_bridge_accepts_http_payload_and_forwards(self) -> None:
        spec = PROJECT_ROOT / "examples" / "demo_http_ingress_bridge_plugin"
        manifest = validate_plugin_package(spec)
        with _MockBridgeService() as service:
            sandbox = PluginSandbox(
                plugin_id=f"example:{manifest['key']}",
                manifest=manifest,
                root_path=Path(spec),
            )
            try:
                sandbox.start()
                sandbox.invoke(
                    "bind_agent",
                    {
                        "ait_api_base": service.base_url,
                        "run_id": "run_http_bridge",
                        "target_agent_id": "planner",
                        "message_type": "dialogue",
                        "phase": "down",
                    },
                    {},
                )
                health = sandbox.health()
                request = Request(
                    health["ingress"]["listen_url"],
                    data=json.dumps({"text": "external release signal"}).encode("utf-8"),
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    method="POST",
                )
                with urlopen(request, timeout=5) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                self.assertTrue(payload["ok"])
                self.assertTrue(payload["forwarded"])
                self.assertEqual(payload["run_id"], "run_http_bridge")
                self.assertEqual(len(service.injections), 1)
                self.assertEqual(service.injections[0]["run_id"], "run_http_bridge")
                self.assertEqual(service.injections[0]["payload"]["body"], "external release signal")
                self.assertEqual(service.injections[0]["payload"]["target_agent_id"], "planner")
            finally:
                sandbox.stop()

    def test_feishu_bridge_handles_challenge_and_routes_reply(self) -> None:
        spec = PROJECT_ROOT / "examples" / "demo_feishu_command_bridge_plugin"
        manifest = validate_plugin_package(spec)
        with _MockBridgeService(auto_reply_text="Planner received the Feishu command.") as service:
            sandbox = PluginSandbox(
                plugin_id=f"example:{manifest['key']}",
                manifest=manifest,
                root_path=Path(spec),
            )
            try:
                sandbox.start()
                sandbox.invoke(
                    "configure_bridge",
                    {
                        "bridge": {
                            "ait_api_base": service.base_url,
                            "run_id": "run_feishu_bridge",
                            "target_agent_id": "planner",
                            "command_prefix": "/ait",
                            "reply_webhook_url": service.reply_url,
                        }
                    },
                    {},
                )
                health = sandbox.health()
                challenge_request = Request(
                    health["webhook"]["listen_url"],
                    data=json.dumps({"type": "url_verification", "challenge": "challenge-token"}).encode("utf-8"),
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    method="POST",
                )
                with urlopen(challenge_request, timeout=5) as response:
                    challenge_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(challenge_payload, {"challenge": "challenge-token"})

                result = sandbox.invoke(
                    "ingest_event",
                    {
                        "wait_for_reply": True,
                        "event": {
                            "header": {"event_type": "im.message.receive_v1"},
                            "event": {
                                "sender": {"sender_id": {"open_id": "ou_demo"}},
                                "message": {
                                    "message_id": "om_demo",
                                    "content": json.dumps({"text": "/ait Summarize the delivery status"}, ensure_ascii=False),
                                },
                            },
                        },
                    },
                    {},
                )
                self.assertTrue(result["ok"])
                self.assertEqual(result["kind"], "command")
                self.assertEqual(result["run_id"], "run_feishu_bridge")
                self.assertEqual(result["target_agent_id"], "planner")
                self.assertEqual(result["reply"]["text"], "Planner received the Feishu command.")
                self.assertEqual(len(service.injections), 1)
                self.assertEqual(service.injections[0]["payload"]["body"], "Summarize the delivery status")
                self.assertEqual(len(service.reply_posts), 1)
                self.assertEqual(service.reply_posts[0]["msg_type"], "text")
                self.assertEqual(service.reply_posts[0]["content"]["text"], "Planner received the Feishu command.")
            finally:
                sandbox.stop()
