from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

from langchain_core.tools import BaseTool

from aiteams.plugins import build_plugin_base_tool, describe_plugin_base_tools, plugin_action_args_schema
from aiteams.plugins.manifest import validate_plugin_package


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_ROOT = PROJECT_ROOT / "examples" / "demo_cmd_echo_plugin"


@unittest.skipUnless(os.name == "nt", "demo.cmd_echo requires Windows cmd.exe")
class DemoCmdEchoPluginTests(unittest.TestCase):
    def test_package_validates(self) -> None:
        manifest = validate_plugin_package(PLUGIN_ROOT)
        self.assertEqual(manifest["key"], "demo.cmd_echo")
        self.assertTrue(manifest["entry_exists"])
        self.assertGreaterEqual(len(manifest["actions"]), 1)
        action = dict(manifest["actions"][0] or {})
        self.assertEqual(action["tool_name"], "cmd_echo")
        self.assertEqual(action["input_schema"]["type"], "object")
        self.assertIn("text", dict(action["input_schema"]["properties"] or {}))
        args_schema = plugin_action_args_schema("cmd_echo", action)
        self.assertIsNotNone(args_schema)
        assert args_schema is not None
        schema = args_schema.model_json_schema()
        self.assertIn("text", dict(schema.get("properties") or {}))
        tool = build_plugin_base_tool(
            plugin_key=manifest["key"],
            action=action,
            fallback_tool_name="plugin_demo_cmd_echo_echo_to_cmd",
            invoker=lambda payload: str(payload),
        )
        self.assertIsInstance(tool, BaseTool)
        self.assertEqual(tool.name, "cmd_echo")
        preview = describe_plugin_base_tools(plugin_key=manifest["key"], manifest=manifest)
        self.assertEqual(len(preview), 1)
        self.assertEqual(preview[0]["tool_name"], "cmd_echo")
        self.assertEqual(preview[0]["action_name"], "echo_to_cmd")
        self.assertEqual(preview[0]["mode"], "structured")
        self.assertIn("text", dict((preview[0]["args_schema"] or {}).get("properties") or {}))

    def test_worker_echoes_payload_via_cmd(self) -> None:
        request = {
            "id": "req-1",
            "method": "invoke",
            "action": "echo_to_cmd",
            "payload": {
                "text": "hello & cmd",
                "open_console": False,
            },
            "context": {"agent_id": "tester"},
        }
        process = subprocess.run(
            [sys.executable, "-u", "-m", "aiteams.plugins.worker", str(PLUGIN_ROOT)],
            input=f"{json.dumps(request, ensure_ascii=False)}\n",
            capture_output=True,
            check=False,
            text=True,
            encoding="utf-8",
            cwd=str(PLUGIN_ROOT),
        )
        self.assertEqual(process.returncode, 0, msg=process.stderr)
        line = next((item for item in process.stdout.splitlines() if item.strip()), "")
        self.assertTrue(line, msg=process.stderr)
        response = json.loads(line)
        self.assertTrue(response["ok"], msg=process.stderr)
        result = dict(response["result"] or {})
        self.assertEqual(result["mode"], "capture")
        self.assertEqual(result["echoed_text"], "hello & cmd")
        self.assertEqual(result["stdout"], "hello & cmd")
