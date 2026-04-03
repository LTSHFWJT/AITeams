from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from langchain_core.messages import AIMessage

from aiteams.agent.kernel import AgentKernel, AgentRunContext
from aiteams.ai_gateway import AIGateway, ChatResult, GatewayCapabilityRequest, ProviderRequestError
from aiteams.domain.models import AgentSpec, NodeSpec


class AIGatewayNativeToolsTests(unittest.TestCase):
    @mock.patch("aiteams.ai_gateway.init_chat_model")
    def test_build_chat_model_uses_init_chat_model_for_deepagents_runtime(self, mocked_init: mock.Mock) -> None:
        mocked_init.return_value = object()
        gateway = AIGateway(timeout_seconds=3)

        with mock.patch.dict("os.environ", {"AIT_CUSTOM_PROVIDER_KEY": "env-secret"}, clear=False):
            model = gateway.build_chat_model(
                {
                    "name": "DeepAgents Provider",
                    "provider_type": "custom_openai",
                    "base_url": "https://example.com/v1",
                    "api_key_env": "AIT_CUSTOM_PROVIDER_KEY",
                    "api_version": "2025-03-01-preview",
                    "organization": "org-test",
                    "extra_headers": {"X-Test": "1"},
                    "extra_config": {
                        "custom_llm_provider": "acme-gateway",
                        "reasoning_effort": "high",
                        "gateway_capabilities": {"native_tools": False, "json_object_response": False},
                    },
                },
                model="gpt-4.1",
                temperature=0.15,
                max_tokens=256,
                agent_name="Planner",
            )

        self.assertIs(model, mocked_init.return_value)
        mocked_init.assert_called_once()
        args = mocked_init.call_args.args
        kwargs = mocked_init.call_args.kwargs
        self.assertEqual(args[0], "openai/gpt-4.1")
        self.assertEqual(kwargs["model_provider"], "litellm")
        self.assertEqual(kwargs["api_base"], "https://example.com/v1")
        self.assertEqual(kwargs["api_key"], "env-secret")
        self.assertEqual(kwargs["organization"], "org-test")
        self.assertEqual(kwargs["temperature"], 0.15)
        self.assertEqual(kwargs["max_tokens"], 256)
        self.assertEqual(kwargs["request_timeout"], 3)
        self.assertEqual(kwargs["custom_llm_provider"], "acme-gateway")
        self.assertEqual(kwargs["extra_headers"], {"X-Test": "1"})
        self.assertEqual(
            kwargs["model_kwargs"],
            {"reasoning_effort": "high", "api_version": "2025-03-01-preview"},
        )

    @mock.patch("aiteams.ai_gateway.AIGateway._build_langchain_runnable")
    def test_chat_with_tools_parses_native_tool_calls(self, mocked_build: mock.Mock) -> None:
        runnable = mock.Mock()
        runnable.invoke.return_value = AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "memory_search__search",
                    "args": {"query": "release readiness", "scope": "agent", "limit": 3},
                    "id": "call_1",
                    "type": "tool_call",
                }
            ],
            response_metadata={"finish_reason": "tool_calls", "model_name": "openai/gpt-4.1"},
            usage_metadata={"input_tokens": 11, "output_tokens": 7, "total_tokens": 18},
        )
        mocked_build.return_value = runnable
        gateway = AIGateway(timeout_seconds=3)

        result = gateway.chat_with_tools(
            {
                "name": "Native Provider",
                "provider_type": "custom_openai",
                "model": "gpt-4.1",
                "base_url": "https://example.com/v1",
                "api_key": "secret",
            },
            [
                {"role": "system", "content": "You are a planner."},
                {"role": "user", "content": "Search memory for release readiness."},
            ],
            model="gpt-4.1",
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "memory_search__search",
                        "description": "Search memory.",
                        "parameters": {
                            "type": "object",
                            "properties": {"query": {"type": "string"}},
                            "required": ["query"],
                        },
                    },
                }
            ],
            parallel_tool_calls=False,
        )

        self.assertEqual(result.model, "openai/gpt-4.1")
        self.assertEqual(result.finish_reason, "tool_calls")
        self.assertEqual(result.tool_calls[0]["name"], "memory_search__search")
        self.assertEqual(result.tool_calls[0]["arguments"]["query"], "release readiness")
        self.assertEqual(result.tool_calls[0]["arguments"]["scope"], "agent")

        kwargs = mocked_build.call_args.kwargs
        self.assertEqual(kwargs["model"], "openai/gpt-4.1")
        self.assertEqual(kwargs["tool_choice"], "auto")
        self.assertFalse(kwargs["parallel_tool_calls"])
        self.assertEqual(kwargs["tools"][0]["function"]["name"], "memory_search__search")

    @mock.patch("aiteams.ai_gateway.AIGateway._build_langchain_runnable")
    def test_complete_passes_response_format_through_capability_request(self, mocked_build: mock.Mock) -> None:
        runnable = mock.Mock()
        runnable.invoke.return_value = AIMessage(
            content='{"summary":"Ready","deliverables":["Spec"],"risks":[],"pass":true,"next_focus":"Ship"}',
            response_metadata={"finish_reason": "stop", "model_name": "openai/gpt-4.1"},
            usage_metadata={"input_tokens": 10, "output_tokens": 12, "total_tokens": 22},
        )
        mocked_build.return_value = runnable
        gateway = AIGateway(timeout_seconds=3)

        result = gateway.complete(
            {
                "name": "Native Provider",
                "provider_type": "custom_openai",
                "model": "gpt-4.1",
                "base_url": "https://example.com/v1",
                "api_key": "secret",
            },
            [
                {"role": "system", "content": "Return JSON."},
                {"role": "user", "content": "Summarize the task."},
            ],
            model="gpt-4.1",
            capability_request=GatewayCapabilityRequest.json_object(),
        )

        self.assertEqual(result.finish_reason, "stop")
        self.assertEqual(result.content, '{"summary":"Ready","deliverables":["Spec"],"risks":[],"pass":true,"next_focus":"Ship"}')
        kwargs = mocked_build.call_args.kwargs
        self.assertEqual(kwargs["response_format"], {"type": "json_object"})

    @mock.patch("aiteams.ai_gateway.AIGateway._build_langchain_runnable")
    def test_complete_ignores_legacy_provider_capability_fields(self, mocked_build: mock.Mock) -> None:
        runnable = mock.Mock()
        runnable.invoke.return_value = AIMessage(
            content="plain text",
            response_metadata={"finish_reason": "stop", "model_name": "openai/gpt-4.1"},
            usage_metadata={"input_tokens": 2, "output_tokens": 2, "total_tokens": 4},
        )
        mocked_build.return_value = runnable
        gateway = AIGateway(timeout_seconds=3)

        gateway.complete(
            {
                "name": "Restricted Provider",
                "provider_type": "custom_openai",
                "model": "gpt-4.1",
                "base_url": "https://example.com/v1",
                "api_key": "secret",
                "gateway_capabilities": {"native_tools": False, "json_object_response": False},
            },
            [
                {"role": "system", "content": "You are a planner."},
                {"role": "user", "content": "Do work."},
            ],
            model="gpt-4.1",
            capability_request=GatewayCapabilityRequest.json_object(
                tools=[
                    {
                        "type": "function",
                        "function": {
                            "name": "memory_search__search",
                            "description": "Search memory.",
                            "parameters": {"type": "object", "properties": {"query": {"type": "string"}}},
                        },
                    }
                ],
                tool_choice="auto",
                parallel_tool_calls=False,
            ),
        )

        kwargs = mocked_build.call_args.kwargs
        self.assertEqual(kwargs["tools"][0]["function"]["name"], "memory_search__search")
        self.assertEqual(kwargs["tool_choice"], "auto")
        self.assertFalse(kwargs["parallel_tool_calls"])
        self.assertEqual(kwargs["response_format"], {"type": "json_object"})


class AgentKernelNativePlannerTests(unittest.TestCase):
    def test_tool_plan_response_prefers_native_provider_tools(self) -> None:
        class _NativeGateway:
            def __init__(self) -> None:
                self.complete_calls: list[dict[str, object]] = []

            def complete(self, provider, messages, **kwargs):  # type: ignore[no-untyped-def]
                self.complete_calls.append({"provider": provider, "messages": messages, **kwargs})
                return ChatResult(
                    content="",
                    model="openai/gpt-4.1",
                    usage={},
                    raw={},
                    tool_calls=[
                        {
                            "name": "memory_manage__manage",
                            "arguments": {
                                "operation": "create",
                                "scope": "agent",
                                "record": {"text": "Capture release readiness status."},
                            },
                        }
                    ],
                    finish_reason="tool_calls",
                )

        gateway = _NativeGateway()
        kernel = AgentKernel(memory=mock.Mock(), gateway=gateway, plugin_manager=None)
        agent = AgentSpec(
            key="worker",
            name="Worker",
            role="analyst",
            backend="litellm",
            provider_type="custom_openai",
            model="gpt-4.1",
        )
        node = NodeSpec.from_dict({"id": "work", "type": "agent", "agent": "worker", "instruction": "Use memory.manage."})
        context = AgentRunContext(
            workspace_id="local-workspace",
            project_id="default-project",
            run_id="run-native-tools",
            prompt="Persist a release readiness fact.",
            inputs={},
            outputs={},
            loops={},
            current_node_id="work",
        )

        plan = asyncio.run(
            kernel._tool_plan_response(
                agent,
                node,
                context,
                [{"key": "memory.manage", "actions": ["manage"], "permissions": ["memory_write"]}],
                plugin_results=[],
            )
        )

        self.assertEqual(plan["tool_calls"][0]["plugin_key"], "memory.manage")
        self.assertEqual(plan["tool_calls"][0]["action"], "manage")
        self.assertEqual(plan["tool_calls"][0]["payload"]["operation"], "create")
        self.assertEqual(plan["tool_calls"][0]["payload"]["record"]["text"], "Capture release readiness status.")
        capability_request = gateway.complete_calls[0]["capability_request"]
        self.assertEqual(capability_request.tools[0]["function"]["name"], "memory_manage__manage")
        self.assertEqual(capability_request.response_format, {"type": "json_object"})

    def test_tool_plan_response_falls_back_to_json_when_native_tools_fail(self) -> None:
        class _FallbackGateway:
            def __init__(self) -> None:
                self.complete_calls: list[dict[str, object]] = []

            def complete(self, provider, messages, **kwargs):  # type: ignore[no-untyped-def]
                self.complete_calls.append({"provider": provider, "messages": messages, **kwargs})
                capability_request = kwargs.get("capability_request")
                if capability_request and capability_request.tools:
                    raise ProviderRequestError("tool calling is unsupported")
                return ChatResult(
                    content='{"tool_calls":[{"plugin_key":"memory.search","action":"search","payload":{"query":"release readiness","scope":"agent","limit":2}}]}',
                    model="openai/gpt-4.1",
                    usage={},
                    raw={},
                )

        gateway = _FallbackGateway()
        kernel = AgentKernel(memory=mock.Mock(), gateway=gateway, plugin_manager=None)
        agent = AgentSpec(
            key="worker",
            name="Worker",
            role="analyst",
            backend="litellm",
            provider_type="custom_openai",
            model="gpt-4.1",
        )
        node = NodeSpec.from_dict({"id": "work", "type": "agent", "agent": "worker", "instruction": "Use memory.search."})
        context = AgentRunContext(
            workspace_id="local-workspace",
            project_id="default-project",
            run_id="run-json-fallback",
            prompt="Verify release readiness facts.",
            inputs={},
            outputs={},
            loops={},
            current_node_id="work",
        )

        plan = asyncio.run(
            kernel._tool_plan_response(
                agent,
                node,
                context,
                [{"key": "memory.search", "actions": ["search"], "permissions": ["memory_read"]}],
                plugin_results=[],
            )
        )

        self.assertEqual(len(gateway.complete_calls), 2)
        self.assertTrue(gateway.complete_calls[0]["capability_request"].tools)
        self.assertEqual(gateway.complete_calls[1]["capability_request"].response_format, {"type": "json_object"})
        self.assertEqual(plan["tool_calls"][0]["plugin_key"], "memory.search")
        self.assertEqual(plan["tool_calls"][0]["payload"]["query"], "release readiness")

    def test_llm_response_uses_gateway_capability_layer_for_final_json(self) -> None:
        class _FinalGateway:
            def __init__(self) -> None:
                self.complete_calls: list[dict[str, object]] = []

            def complete(self, provider, messages, **kwargs):  # type: ignore[no-untyped-def]
                self.complete_calls.append({"provider": provider, "messages": messages, **kwargs})
                return ChatResult(
                    content='{"summary":"Ready to deliver","deliverables":["Plan"],"risks":["None"],"pass":true,"next_focus":"Execute"}',
                    model="openai/gpt-4.1",
                    usage={},
                    raw={},
                    finish_reason="stop",
                )

        gateway = _FinalGateway()
        kernel = AgentKernel(memory=mock.Mock(), gateway=gateway, plugin_manager=None)
        agent = AgentSpec(
            key="worker",
            name="Worker",
            role="analyst",
            backend="litellm",
            provider_type="custom_openai",
            model="gpt-4.1",
        )

        result = asyncio.run(kernel._llm_response(agent, "Summarize the current delivery status."))

        self.assertEqual(result["summary"], "Ready to deliver")
        self.assertEqual(result["deliverables"], ["Plan"])
        capability_request = gateway.complete_calls[0]["capability_request"]
        self.assertEqual(capability_request.response_format, {"type": "json_object"})
        self.assertEqual(capability_request.tools, [])
