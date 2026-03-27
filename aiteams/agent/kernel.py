from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any

from aiteams.ai_gateway import AIGateway, GatewayCapabilityRequest, ProviderRequestError
from aiteams.domain.models import AgentSpec, NodeSpec
from aiteams.memory.adapter import MemoryAdapter
from aiteams.memory.scope import MemoryScopes, Scope
from aiteams.plugins import PluginManager
from aiteams.plugins.manager import (
    BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY,
    BUILTIN_KB_RETRIEVE_PLUGIN_KEY,
    BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY,
    BUILTIN_MEMORY_MANAGE_PLUGIN_KEY,
    BUILTIN_MEMORY_SEARCH_PLUGIN_KEY,
    BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY,
    BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY,
    builtin_plugin_ref,
)
from aiteams.utils import pretty_json, render_template, render_template_data, trim_text


@dataclass(slots=True)
class AgentRunContext:
    workspace_id: str
    project_id: str
    run_id: str
    prompt: str
    inputs: dict[str, Any]
    outputs: dict[str, Any]
    loops: dict[str, int]
    current_node_id: str
    team_id: str | None = None


class AgentKernel:
    def __init__(self, memory: MemoryAdapter, gateway: AIGateway | None = None, plugin_manager: PluginManager | None = None):
        self.memory = memory
        self.gateway = gateway or AIGateway()
        self.plugin_manager = plugin_manager

    async def execute(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        *,
        hooks: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scopes = MemoryScopes(
            workspace_id=context.workspace_id,
            project_id=context.project_id,
            run_id=context.run_id,
            agent_id=agent.key,
        )
        memory_runtime = self._memory_runtime(agent)
        working = await self.memory.load_working(scopes.working())
        recalled = await self._recall(agent, scopes, context, node)
        available_plugins = self._describe_plugins(agent)
        plugin_results = await self._execute_plugin_actions(agent, node, context, available_plugins, hooks=hooks)
        plugin_results.extend(await self._auto_knowledge_retrieval(agent, node, context, available_plugins, plugin_results, hooks=hooks))
        planned_plugin_results, planned_result = await self._planned_plugin_loop(
            agent,
            node,
            context,
            available_plugins,
            existing_results=plugin_results,
            hooks=hooks,
        )
        plugin_results.extend(planned_plugin_results)
        prompt_block = self._build_prompt(agent, node, context, working, recalled, available_plugins, plugin_results)

        await self.memory.append_working(
            scopes.working(),
            "user",
            prompt_block,
            metadata={"node_id": node.id, "agent_id": agent.key},
            runtime=memory_runtime,
        )
        result = planned_result or self._mock_response(agent, node, context, recalled, plugin_results)
        if planned_result is None and agent.backend != "mock":
            try:
                result = await self._llm_response(agent, prompt_block)
            except ProviderRequestError:
                result = self._mock_response(agent, node, context, recalled, plugin_results)

        await self.memory.append_working(
            scopes.working(),
            "assistant",
            result["summary"],
            metadata={"node_id": node.id, "agent_id": agent.key, "pass": result.get("pass")},
            runtime=memory_runtime,
        )
        details = dict(result.get("details") or {})
        details["visible_output_ids"] = sorted(context.outputs.keys())
        details["visible_output_count"] = len(context.outputs)
        if agent.role_template:
            details["role_template"] = agent.role_template
        if available_plugins:
            details["available_plugins"] = available_plugins
        if plugin_results:
            details["plugin_results"] = plugin_results
            knowledge_items = self._knowledge_items(plugin_results)
            if knowledge_items:
                details["knowledge_results"] = knowledge_items
        result["details"] = details
        result["visible_output_ids"] = sorted(context.outputs.keys())
        if plugin_results:
            result["plugin_results"] = plugin_results
        await self._remember(agent, scopes, result, runtime=memory_runtime)
        return result

    async def _recall(
        self,
        agent: AgentSpec,
        scopes: MemoryScopes,
        context: AgentRunContext,
        node: NodeSpec,
    ) -> list[dict[str, Any]]:
        query = self._knowledge_query(context, node)
        scope_list: list[Any] = []
        read_scopes = self._read_scope_names(agent)
        if "agent" in read_scopes:
            scope_list.append(scopes.agent_private())
        if "team" in read_scopes and context.team_id:
            scope_list.append(
                Scope(
                    workspace_id=context.workspace_id,
                    project_id=context.project_id,
                    team_id=context.team_id,
                    namespace="team_shared",
                )
            )
        if "project" in read_scopes:
            scope_list.append(scopes.project_shared())
        if read_scopes.intersection({"run", "retrospective"}):
            scope_list.append(
                Scope(
                    workspace_id=context.workspace_id,
                    project_id=context.project_id,
                    run_id=context.run_id,
                    team_id=context.team_id,
                    namespace="run_retrospective",
                )
            )
        if not scope_list:
            scope_list.append(scopes.agent_private())
        recalled: list[dict[str, Any]] = []
        seen: set[str] = set()
        for scope in scope_list:
            for item in await self.memory.recall(scope, query, top_k=4):
                key = str(item.get("head_id") or item.get("text") or "")
                if key and key not in seen:
                    recalled.append(item)
                    seen.add(key)
        return recalled[:8]

    def _read_scope_names(self, agent: AgentSpec) -> set[str]:
        memory_profile = dict((agent.metadata or {}).get("memory_profile") or {})
        config = dict(memory_profile.get("config") or {})
        read_scopes = {str(item).strip().lower() for item in list(config.get("read_scopes") or []) if str(item).strip()}
        if read_scopes:
            return read_scopes
        if agent.memory_policy == "agent_private":
            return {"agent"}
        if agent.memory_policy == "project_shared":
            return {"project"}
        if agent.memory_policy == "agent_private_plus_project":
            return {"agent", "project"}
        if agent.memory_policy == "run_retrospective":
            return {"run"}
        return set()

    def _build_prompt(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        working: list[dict[str, Any]],
        recalled: list[dict[str, Any]],
        available_plugins: list[dict[str, Any]],
        plugin_results: list[dict[str, Any]],
    ) -> str:
        prompt_context = {
            "task": {"prompt": context.prompt, "inputs": context.inputs},
            "outputs": context.outputs,
            "loops": context.loops,
            "message": {
                "source_actor_id": node.config.get("source_actor_id"),
                "target_agent_id": node.config.get("target_agent_id"),
                "message_type": node.config.get("message_type"),
                "phase": node.config.get("phase"),
                "body": node.config.get("body"),
            },
        }
        working_text = "\n".join(f"- {item.get('role')}: {trim_text(item.get('content'), limit=140)}" for item in working[-6:])
        recall_text = "\n".join(f"- {trim_text(item.get('text'), limit=160)}" for item in recalled[:6])
        knowledge_text = self._knowledge_prompt_text(plugin_results)
        instruction = render_template(node.instruction or "", prompt_context)
        plugin_text = "\n".join(
            f"- {item.get('key')}@{item.get('version')}: actions={','.join(item.get('actions', [])) or '-'}"
            for item in available_plugins
        )
        plugin_result_text = "\n".join(
            f"- {item.get('plugin_key')}::{item.get('action')} => {trim_text(pretty_json(item.get('result')), limit=240)}"
            for item in plugin_results
        )
        return (
            f"Agent: {agent.name} ({agent.role})\n"
            f"Role Template: {agent.role_template or 'inline'}\n"
            f"Goal: {agent.goal}\n"
            f"Agent Instructions: {agent.instructions or 'No role-template instructions'}\n"
            f"Task: {context.prompt}\n"
            f"Node: {node.id}\n"
            f"Instruction: {instruction or node.instruction or 'No extra instruction'}\n"
            f"Visible upstream outputs: {pretty_json(context.outputs) if context.outputs else '{}'}\n"
            f"Available plugins:\n{plugin_text or '- none'}\n"
            f"Plugin execution results:\n{plugin_result_text or '- none'}\n"
            f"Knowledge retrieval:\n{knowledge_text or '- none'}\n"
            f"Working memory:\n{working_text or '- none'}\n"
            f"Recalled memory:\n{recall_text or '- none'}"
        )

    def _describe_plugins(self, agent: AgentSpec) -> list[dict[str, Any]]:
        plugin_refs = self._plugin_refs(agent)
        if not plugin_refs:
            return []
        described: list[dict[str, Any]] = []
        for plugin_ref in plugin_refs:
            manifest = dict(plugin_ref.get("manifest") or {})
            actions = [str(item.get("name")) for item in manifest.get("actions", []) if str(item.get("name") or "").strip()]
            descriptor: dict[str, Any] = {}
            if self.plugin_manager:
                try:
                    descriptor = self.plugin_manager.describe_plugin_ref(plugin_ref)
                except Exception:
                    descriptor = {}
            described.append(
                {
                    "id": str(plugin_ref.get("id") or ""),
                    "key": str(plugin_ref.get("key") or ""),
                    "version": str(plugin_ref.get("version") or ""),
                    "tools": [str(item) for item in (descriptor.get("tools") or manifest.get("tools") or [])],
                    "permissions": [str(item) for item in (descriptor.get("permissions") or manifest.get("permissions") or [])],
                    "actions": [
                        str(item.get("name"))
                        for item in (descriptor.get("actions") or manifest.get("actions") or [])
                        if str(item.get("name") or "").strip()
                    ]
                    or actions,
                    "install_path": plugin_ref.get("install_path"),
                }
            )
        return described

    async def _execute_plugin_actions(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        available_plugins: list[dict[str, Any]],
        *,
        hooks: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not self.plugin_manager:
            return []
        configured = list(node.config.get("plugin_actions") or [])
        if not configured:
            return []
        prompt_context = {
            "task": {"prompt": context.prompt, "inputs": context.inputs},
            "outputs": context.outputs,
            "loops": context.loops,
            "node": {"id": node.id, "type": node.type},
            "message": {
                "source_actor_id": node.config.get("source_actor_id"),
                "target_agent_id": node.config.get("target_agent_id"),
                "message_type": node.config.get("message_type"),
                "phase": node.config.get("phase"),
                "body": node.config.get("body"),
            },
        }
        plugin_refs = self._plugin_refs(agent)
        plugin_lookup = {str(item.get("id") or ""): item for item in plugin_refs}
        plugin_lookup.update({str(item.get("key") or ""): item for item in plugin_refs})
        results: list[dict[str, Any]] = []
        for action_spec in configured:
            item = dict(action_spec or {})
            plugin_ref = plugin_lookup.get(str(item.get("plugin_ref") or item.get("plugin_id") or item.get("plugin_key") or ""))
            if plugin_ref is None:
                raise RuntimeError(f"Node `{node.id}` references unavailable plugin `{item.get('plugin_ref') or item.get('plugin_key')}`.")
            raw_payload = render_template_data(item.get("payload") or {}, prompt_context)
            payload = raw_payload if isinstance(raw_payload, dict) else {"value": raw_payload}
            invocation_context = self._plugin_invocation_context(agent, node, context, available_plugins)
            response, review = await self._invoke_plugin(
                plugin_ref=plugin_ref,
                action=str(item.get("action") or ""),
                payload=payload,
                context=invocation_context,
                hooks=hooks,
            )
            results.append(
                {
                    "plugin_id": str(plugin_ref.get("id") or ""),
                    "plugin_key": str(plugin_ref.get("key") or ""),
                    "action": str(item.get("action") or ""),
                    "result": response,
                    "review": review,
                }
            )
        return results

    async def _planned_plugin_loop(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        available_plugins: list[dict[str, Any]],
        *,
        existing_results: list[dict[str, Any]],
        hooks: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        if not self.plugin_manager or not self._tool_planning_enabled(agent, node, available_plugins):
            return [], None
        max_steps = max(1, int(node.config.get("tool_planning_max_steps", 3) or 3))
        plugin_refs = self._plugin_refs(agent)
        plugin_lookup = {str(item.get("id") or ""): item for item in plugin_refs}
        plugin_lookup.update({str(item.get("key") or ""): item for item in plugin_refs})
        prior_results = [dict(item) for item in list(existing_results or [])]
        planned_results: list[dict[str, Any]] = []
        final_result: dict[str, Any] | None = None
        for _step in range(max_steps):
            try:
                plan = await self._tool_plan_response(
                    agent,
                    node,
                    context,
                    available_plugins,
                    plugin_results=[*prior_results, *planned_results],
                )
            except ProviderRequestError:
                break
            tool_calls = self._coerce_tool_calls(plan)
            if tool_calls:
                for call in tool_calls[:4]:
                    plugin_ref = plugin_lookup.get(str(call.get("plugin_ref") or call.get("plugin_id") or call.get("plugin_key") or ""))
                    if plugin_ref is None:
                        continue
                    payload = dict(call.get("payload") or {})
                    response, review = await self._invoke_plugin(
                        plugin_ref=plugin_ref,
                        action=str(call.get("action") or ""),
                        payload=payload,
                        context=self._plugin_invocation_context(agent, node, context, available_plugins),
                        hooks=hooks,
                    )
                    planned_results.append(
                        {
                            "plugin_id": str(plugin_ref.get("id") or ""),
                            "plugin_key": str(plugin_ref.get("key") or ""),
                            "action": str(call.get("action") or ""),
                            "result": response,
                            "review": review,
                        }
                    )
                continue
            final_payload = plan.get("final")
            if isinstance(final_payload, dict):
                final_result = self._normalize_output(agent, final_payload)
            break
        return planned_results, final_result

    def _tool_planning_enabled(self, agent: AgentSpec, node: NodeSpec, available_plugins: list[dict[str, Any]]) -> bool:
        if not available_plugins:
            return False
        if node.config.get("enable_tool_planning") is False:
            return False
        metadata = dict(agent.metadata or {})
        return bool(metadata.get("tool_planning_enabled", True))

    async def _tool_plan_response(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        available_plugins: list[dict[str, Any]],
        *,
        plugin_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if agent.backend == "mock":
            return self._mock_tool_plan_response(agent, node, context, available_plugins, plugin_results)
        provider = self._provider_payload(agent)
        native_tools, native_lookup = self._native_tool_specs(node, available_plugins)
        if native_tools:
            try:
                native_response = self.gateway.complete(
                    provider,
                    [
                        {"role": "system", "content": self._tool_planner_system_prompt(use_native_tools=True)},
                        {"role": "user", "content": self._tool_planning_prompt(agent, node, context, available_plugins, plugin_results)},
                    ],
                    model=agent.model,
                    temperature=agent.temperature,
                    max_tokens=agent.max_tokens,
                    capability_request=GatewayCapabilityRequest.json_object(
                        tools=native_tools,
                        tool_choice="auto",
                        parallel_tool_calls=False,
                    ),
                )
            except ProviderRequestError:
                native_response = None
            if native_response is not None:
                native_plan = self._coerce_native_tool_plan(native_response, native_lookup)
                if native_plan is not None:
                    return native_plan
        response = self.gateway.complete(
            provider,
            [
                {
                    "role": "system",
                    "content": self._tool_planner_system_prompt(use_native_tools=False),
                },
                {"role": "user", "content": self._tool_planning_prompt(agent, node, context, available_plugins, plugin_results)},
            ],
            model=agent.model,
            temperature=agent.temperature,
            max_tokens=agent.max_tokens,
            capability_request=GatewayCapabilityRequest.json_object(),
        )
        parsed = self._parse_tool_planning_payload(response.content)
        return parsed or {}

    def _tool_planner_system_prompt(self, *, use_native_tools: bool) -> str:
        if use_native_tools:
            return (
                "You are an agent runtime planner.\n"
                "Use the available native tools when they are required.\n"
                "When no more tools are needed, return ONLY JSON in the form "
                "{\"final\":{\"summary\":\"...\",\"deliverables\":[...],\"risks\":[...],\"pass\":true,\"next_focus\":\"...\"}}.\n"
                "Do not invent tools or actions outside the provided tool list."
            )
        return (
            "You are an agent runtime planner.\n"
            "Return ONLY JSON.\n"
            "If tools are needed, respond with {\"tool_calls\":[{\"plugin_key\":\"...\",\"action\":\"...\",\"payload\":{}}]}.\n"
            "If no more tools are needed, respond with "
            "{\"final\":{\"summary\":\"...\",\"deliverables\":[...],\"risks\":[...],\"pass\":true,\"next_focus\":\"...\"}}.\n"
            "Use only the listed plugins and actions."
        )

    def _parse_tool_planning_payload(self, content: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(content)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        return parsed if isinstance(parsed, dict) else None

    def _provider_payload(self, agent: AgentSpec) -> dict[str, Any]:
        return {
            "name": agent.name,
            "provider_type": agent.provider_type,
            "model": agent.model,
            "base_url": agent.base_url,
            "api_key": agent.api_key or (os.getenv(agent.api_key_env) if agent.api_key_env else None),
            "api_version": agent.api_version,
            "organization": agent.organization,
            "extra_headers": dict(agent.extra_headers),
            "extra_config": dict(agent.extra_config),
        }

    def _coerce_native_tool_plan(
        self,
        response: Any,
        tool_lookup: dict[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        tool_calls: list[dict[str, Any]] = []
        for item in list(getattr(response, "tool_calls", []) or []):
            plugin_call = self._native_tool_call(item, tool_lookup)
            if plugin_call is not None:
                tool_calls.append(plugin_call)
        if tool_calls:
            return {"tool_calls": tool_calls}
        parsed = self._parse_tool_planning_payload(str(getattr(response, "content", "") or ""))
        return parsed if parsed else None

    def _native_tool_call(
        self,
        item: dict[str, Any],
        tool_lookup: dict[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        tool_name = str(item.get("name") or "").strip()
        if not tool_name:
            return None
        tool_spec = tool_lookup.get(tool_name)
        if tool_spec is None:
            return None
        arguments = dict(item.get("arguments") or {})
        payload = self._native_tool_payload(tool_spec, arguments)
        return {
            "plugin_key": str(tool_spec.get("plugin_key") or ""),
            "action": str(tool_spec.get("action") or ""),
            "payload": payload,
        }

    def _native_tool_payload(self, tool_spec: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
        payload_mode = str(tool_spec.get("payload_mode") or "direct")
        if payload_mode == "nested":
            nested_payload = arguments.get("payload")
            if isinstance(nested_payload, dict):
                return dict(nested_payload)
            return {}
        payload = dict(arguments)
        payload.pop("plugin_key", None)
        payload.pop("action", None)
        return payload

    def _native_tool_specs(
        self,
        node: NodeSpec,
        available_plugins: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        tools: list[dict[str, Any]] = []
        lookup: dict[str, dict[str, Any]] = {}
        for plugin in available_plugins:
            plugin_key = str(plugin.get("key") or "").strip()
            if not plugin_key:
                continue
            for action in [str(item).strip() for item in list(plugin.get("actions") or []) if str(item).strip()]:
                parameters, payload_mode = self._native_tool_parameters(plugin_key, action, node=node)
                tool_name = self._native_tool_name(plugin_key, action)
                lookup[tool_name] = {
                    "plugin_key": plugin_key,
                    "action": action,
                    "payload_mode": payload_mode,
                }
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": tool_name,
                            "description": self._native_tool_description(plugin, plugin_key, action),
                            "parameters": parameters,
                        },
                    }
                )
        return tools, lookup

    def _native_tool_name(self, plugin_key: str, action: str) -> str:
        base = f"{plugin_key}__{action}"
        sanitized = "".join(char.lower() if char.isalnum() else "_" for char in base).strip("_") or "tool"
        if len(sanitized) <= 64:
            return sanitized
        digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
        return f"{sanitized[:55]}_{digest}"

    def _native_tool_description(self, plugin: dict[str, Any], plugin_key: str, action: str) -> str:
        descriptions = {
            (BUILTIN_MEMORY_SEARCH_PLUGIN_KEY, "search"): "Search scoped short-term and long-term memory for relevant facts.",
            (BUILTIN_MEMORY_MANAGE_PLUGIN_KEY, "manage"): "Create, update, or delete scoped long-term memory records.",
            (
                BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY,
                "reflect",
            ): "Trigger LangMem background reflection over recent conversation turns.",
            (BUILTIN_KB_RETRIEVE_PLUGIN_KEY, "retrieve"): "Retrieve relevant documents from the agent's bound knowledge bases.",
            (BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY, "send"): "Send a direct message to an adjacent-level teammate.",
            (BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY, "reply"): "Reply to the current inbound adjacent-level team message.",
            (BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY, "escalate"): "Escalate the current interaction for direct human review.",
        }
        builtin = descriptions.get((plugin_key, action))
        if builtin:
            return builtin
        permissions = [str(item) for item in list(plugin.get("permissions") or []) if str(item).strip()]
        permission_text = f" Permissions: {', '.join(permissions)}." if permissions else ""
        return f"Invoke plugin action `{plugin_key}.{action}` with structured payload arguments.{permission_text}"

    def _native_tool_parameters(self, plugin_key: str, action: str, *, node: NodeSpec) -> tuple[dict[str, Any], str]:
        memory_scope_property = {
            "type": "string",
            "enum": ["agent", "team", "project", "run", "working", "all"],
            "description": "Memory scope to operate on.",
        }
        if plugin_key == BUILTIN_MEMORY_SEARCH_PLUGIN_KEY and action == "search":
            return (
                {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "What to search for in memory."},
                        "scope": memory_scope_property,
                        "limit": {"type": "integer", "minimum": 1, "maximum": 16, "description": "Maximum number of matches."},
                        "filters": {"type": "object", "description": "Optional metadata filters.", "additionalProperties": True},
                    },
                    "required": ["query"],
                },
                "direct",
            )
        if plugin_key == BUILTIN_MEMORY_MANAGE_PLUGIN_KEY and action == "manage":
            return (
                {
                    "type": "object",
                    "properties": {
                        "operation": {
                            "type": "string",
                            "enum": ["create", "upsert", "update", "delete"],
                            "description": "Memory write operation.",
                        },
                        "scope": memory_scope_property,
                        "record": {
                            "type": "object",
                            "description": "Structured memory record to store or update.",
                            "additionalProperties": True,
                        },
                        "memory_id": {"type": "string", "description": "Existing memory id for updates or deletes."},
                        "fact_key": {"type": "string", "description": "Stable fact key for deduplication and upserts."},
                        "text": {"type": "string", "description": "Memory text content."},
                        "summary": {"type": "string", "description": "Short memory summary."},
                        "metadata": {"type": "object", "description": "Optional memory metadata.", "additionalProperties": True},
                    },
                },
                "direct",
            )
        if plugin_key == BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY and action == "reflect":
            return (
                {
                    "type": "object",
                    "properties": {
                        "scope": {
                            "type": "string",
                            "enum": ["agent", "team", "project", "run"],
                            "description": "Scope to reflect into.",
                        }
                    },
                },
                "direct",
            )
        if plugin_key == BUILTIN_KB_RETRIEVE_PLUGIN_KEY and action == "retrieve":
            return (
                {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Knowledge retrieval query."},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 16, "description": "Maximum documents to retrieve."},
                        "knowledge_bases": {
                            "type": "array",
                            "description": "Optional explicit knowledge base bindings.",
                            "items": {"type": "object", "additionalProperties": True},
                        },
                    },
                    "required": ["query"],
                },
                "direct",
            )
        if plugin_key == BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY and action == "send":
            target_property: dict[str, Any] = {
                "type": "string",
                "description": "Adjacent teammate to message.",
            }
            adjacent_targets = [str(item) for item in list(node.config.get("adjacent_targets") or []) if str(item).strip()]
            if adjacent_targets:
                target_property["enum"] = adjacent_targets
            return (
                {
                    "type": "object",
                    "properties": {
                        "target_agent_id": target_property,
                        "message_type": {
                            "type": "string",
                            "enum": ["dialogue", "task", "handoff"],
                            "description": "Message intent type.",
                        },
                        "phase": {
                            "type": "string",
                            "enum": ["down", "up"],
                            "description": "Directional phase for the routed message.",
                        },
                        "body": {"type": "string", "description": "Natural-language message body."},
                        "message_payload": {
                            "type": "object",
                            "description": "Optional structured message payload.",
                            "additionalProperties": True,
                        },
                        "metadata": {"type": "object", "description": "Optional routing metadata.", "additionalProperties": True},
                    },
                    "required": ["target_agent_id", "body"],
                },
                "direct",
            )
        if plugin_key == BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY and action == "reply":
            return (
                {
                    "type": "object",
                    "properties": {
                        "message_type": {
                            "type": "string",
                            "enum": ["dialogue", "task", "handoff"],
                            "description": "Reply message type.",
                        },
                        "phase": {
                            "type": "string",
                            "enum": ["down", "up"],
                            "description": "Directional phase override for the reply.",
                        },
                        "body": {"type": "string", "description": "Reply message body."},
                        "message_payload": {
                            "type": "object",
                            "description": "Optional structured reply payload.",
                            "additionalProperties": True,
                        },
                        "metadata": {"type": "object", "description": "Optional routing metadata.", "additionalProperties": True},
                    },
                    "required": ["body"],
                },
                "direct",
            )
        if plugin_key == BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY and action == "escalate":
            return (
                {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "description": "Short escalation title."},
                        "detail": {"type": "string", "description": "Why human intervention is required."},
                        "body": {"type": "string", "description": "Optional interaction transcript or summary."},
                        "risk_tags": {
                            "type": "array",
                            "description": "Optional human-review risk tags.",
                            "items": {"type": "string"},
                        },
                        "metadata": {"type": "object", "description": "Optional escalation metadata.", "additionalProperties": True},
                    },
                },
                "direct",
            )
        return (
            {
                "type": "object",
                "properties": {
                    "payload": {
                        "type": "object",
                        "description": f"Arguments for plugin action `{plugin_key}.{action}`.",
                        "additionalProperties": True,
                    }
                },
                "required": ["payload"],
            },
            "nested",
        )

    def _tool_planning_prompt(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        available_plugins: list[dict[str, Any]],
        plugin_results: list[dict[str, Any]],
    ) -> str:
        plugin_lines = []
        for item in available_plugins:
            plugin_lines.append(
                {
                    "key": item.get("key"),
                    "actions": list(item.get("actions") or []),
                    "permissions": list(item.get("permissions") or []),
                }
            )
        return pretty_json(
            {
                "agent": {"key": agent.key, "name": agent.name, "role": agent.role},
                "task": {"prompt": context.prompt, "inputs": context.inputs},
                "node": {
                    "id": node.id,
                    "instruction": node.instruction,
                    "message_type": node.config.get("message_type"),
                    "phase": node.config.get("phase"),
                    "source_actor_id": node.config.get("source_actor_id"),
                    "target_agent_id": node.config.get("target_agent_id"),
                    "message_body": node.config.get("body"),
                    "adjacent_targets": list(node.config.get("adjacent_targets") or []),
                },
                "available_plugins": plugin_lines,
                "previous_tool_results": [
                    {
                        "plugin_key": item.get("plugin_key"),
                        "action": item.get("action"),
                        "result": item.get("result"),
                    }
                    for item in plugin_results[-6:]
                ],
                "requirements": {
                    "memory_scopes": ["agent", "team", "project", "run", "working"],
                    "team_message_targets": list(node.config.get("adjacent_targets") or []),
                },
            }
        )

    def _mock_tool_plan_response(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        available_plugins: list[dict[str, Any]],
        plugin_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        available_keys = {str(item.get("key") or "") for item in available_plugins}
        prior_keys = {str(item.get("plugin_key") or "") for item in plugin_results}
        text = " ".join(
            filter(
                None,
                [
                    str(node.instruction or ""),
                    str(context.prompt or ""),
                    str(node.config.get("body") or ""),
                    str(agent.instructions or ""),
                ],
            )
        ).lower()
        adjacent_targets = [str(item) for item in list(node.config.get("adjacent_targets") or []) if str(item).strip()]
        if "memory.manage" in available_keys and "memory.manage" not in prior_keys and any(
            token in text for token in ["memory.manage", "store memory", "persist memory", "写入记忆", "保存记忆"]
        ):
            return {
                "tool_calls": [
                    {
                        "plugin_key": "memory.manage",
                        "action": "manage",
                        "payload": {
                            "operation": "create",
                            "scope": "agent",
                            "record": {
                                "text": f"Native planned memory: {trim_text(context.prompt, limit=240)}",
                                "summary": f"Native planned memory for {agent.key}",
                                "fact_key": f"{agent.key}:native-planned-memory",
                            },
                        },
                    }
                ]
            }
        if "memory.search" in available_keys and "memory.search" not in prior_keys and any(
            token in text for token in ["memory.search", "search memory", "recall memory", "检索记忆", "查询记忆"]
        ):
            return {
                "tool_calls": [
                    {
                        "plugin_key": "memory.search",
                        "action": "search",
                        "payload": {
                            "scope": "agent",
                            "query": trim_text(context.prompt, limit=160),
                            "limit": 4,
                        },
                    }
                ]
            }
        if "team.message.send" in available_keys and "team.message.send" not in prior_keys and adjacent_targets and any(
            token in text for token in ["team.message.send", "send message", "delegate", "ask adjacent", "向相邻"]
        ) and str(node.config.get("message_type") or "") == "task":
            return {
                "tool_calls": [
                    {
                        "plugin_key": "team.message.send",
                        "action": "send",
                        "payload": {
                            "target_agent_id": adjacent_targets[0],
                            "message_type": "dialogue",
                            "body": trim_text(str(node.config.get("body") or context.prompt), limit=360),
                        },
                    }
                ]
            }
        if (
            "team.message.reply" in available_keys
            and "team.message.reply" not in prior_keys
            and str(node.config.get("source_actor_id") or "") != "human"
            and str(node.config.get("message_type") or "") in {"dialogue", "handoff"}
            and str(node.config.get("phase") or "down") != "up"
            and any(token in text for token in ["team.message.reply", "reply", "回复"])
        ):
            return {
                "tool_calls": [
                    {
                        "plugin_key": "team.message.reply",
                        "action": "reply",
                        "payload": {
                            "message_type": "dialogue",
                            "body": trim_text(str(context.prompt or node.config.get("body") or ""), limit=360),
                        },
                    }
                ]
            }
        if "human.escalate" in available_keys and "human.escalate" not in prior_keys and any(
            token in text for token in ["human.escalate", "人工审核", "人工确认", "需要人工", "need human approval"]
        ):
            return {
                "tool_calls": [
                    {
                        "plugin_key": "human.escalate",
                        "action": "escalate",
                        "payload": {
                            "title": "Mock native escalation",
                            "detail": trim_text(context.prompt, limit=240),
                        },
                    }
                ]
            }
        return {}

    def _coerce_tool_calls(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return [dict(item) for item in list(payload.get("tool_calls") or []) if isinstance(item, dict)]

    def _plugin_invocation_context(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        available_plugins: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "workspace_id": context.workspace_id,
            "project_id": context.project_id,
            "run_id": context.run_id,
            "team_id": context.team_id,
            "node_id": node.id,
            "agent_id": agent.key,
            "visible_outputs": context.outputs,
            "available_plugins": available_plugins,
            "knowledge_bases": self._knowledge_base_bindings(agent),
            "memory_runtime": self._memory_runtime(agent),
            "phase": node.config.get("phase"),
            "message_type": node.config.get("message_type"),
            "source_actor_id": node.config.get("source_actor_id"),
            "target_agent_id": node.config.get("target_agent_id"),
            "adjacent_targets": list(node.config.get("adjacent_targets") or []),
            "message": {
                "message_id": node.config.get("message_id"),
                "source_actor_id": node.config.get("source_actor_id"),
                "target_agent_id": node.config.get("target_agent_id"),
                "message_type": node.config.get("message_type"),
                "phase": node.config.get("phase"),
                "body": node.config.get("body"),
            },
        }

    async def _auto_knowledge_retrieval(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        available_plugins: list[dict[str, Any]],
        existing_results: list[dict[str, Any]],
        *,
        hooks: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not self.plugin_manager:
            return []
        if any(str(item.get("plugin_key") or "") == BUILTIN_KB_RETRIEVE_PLUGIN_KEY for item in existing_results):
            return []
        knowledge_bases = self._knowledge_base_bindings(agent)
        if not knowledge_bases:
            return []
        plugin_ref = next((item for item in self._plugin_refs(agent) if str(item.get("key") or "") == BUILTIN_KB_RETRIEVE_PLUGIN_KEY), None)
        if plugin_ref is None:
            return []
        query = self._knowledge_query(context, node)
        if not query.strip():
            return []
        response, review = await self._invoke_plugin(
            plugin_ref=plugin_ref,
            action="retrieve",
            payload={
                "query": query,
                "limit": max(1, int(node.config.get("knowledge_limit", 4) or 4)),
                "knowledge_bases": knowledge_bases,
            },
            context={
                "workspace_id": context.workspace_id,
                "project_id": context.project_id,
                "run_id": context.run_id,
                "node_id": node.id,
                "agent_id": agent.key,
                "visible_outputs": context.outputs,
                "available_plugins": available_plugins,
                "knowledge_bases": knowledge_bases,
            },
            hooks=hooks,
        )
        return [
            {
                "plugin_id": str(plugin_ref.get("id") or ""),
                "plugin_key": str(plugin_ref.get("key") or ""),
                "action": "retrieve",
                "result": response,
                "review": review,
            }
        ]

    def _plugin_refs(self, agent: AgentSpec) -> list[dict[str, Any]]:
        metadata = dict(agent.metadata or {})
        refs = [dict(item or {}) for item in list(metadata.get("plugins") or [])]
        if not refs:
            refs = [
                {"id": str(item), "key": str(item), "version": "", "manifest": {}, "install_path": None}
                for item in metadata.get("plugin_ids", [])
            ]
        has_memory_core = "memory_core" in set(agent.workbenches) or any(
            str(item.get("key") or item.get("id") or "").strip() == "memory_core"
            for item in refs
        )
        if has_memory_core:
            refs.extend(
                [
                    builtin_plugin_ref(BUILTIN_MEMORY_SEARCH_PLUGIN_KEY),
                    builtin_plugin_ref(BUILTIN_MEMORY_MANAGE_PLUGIN_KEY),
                    builtin_plugin_ref(BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY),
                ]
            )
        knowledge_bases = self._knowledge_base_bindings(agent)
        if knowledge_bases:
            refs.append(builtin_plugin_ref(BUILTIN_KB_RETRIEVE_PLUGIN_KEY, knowledge_bases=knowledge_bases))
        is_team_member = any(key in metadata for key in ("level", "reports_to", "can_receive_task", "can_finish_task", "peer_chat_enabled"))
        if is_team_member:
            refs.extend(
                [
                    builtin_plugin_ref(BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY),
                    builtin_plugin_ref(BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY),
                    builtin_plugin_ref(BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY),
                ]
            )
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in refs:
            identity = str(item.get("id") or item.get("key") or "")
            if not identity or identity in seen:
                continue
            seen.add(identity)
            deduped.append(item)
        return deduped

    def _knowledge_base_bindings(self, agent: AgentSpec) -> list[dict[str, Any]]:
        metadata = dict(agent.metadata or {})
        bindings = [dict(item or {}) for item in list(metadata.get("knowledge_bases") or []) if isinstance(item, dict)]
        if bindings:
            return bindings
        return [{"key": str(item)} for item in metadata.get("knowledge_base_refs", []) if str(item).strip()]

    def _knowledge_items(self, plugin_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for result in plugin_results:
            if str(result.get("plugin_key") or "") != BUILTIN_KB_RETRIEVE_PLUGIN_KEY:
                continue
            payload = dict(result.get("result") or {})
            for item in payload.get("items", []) or []:
                if isinstance(item, dict):
                    items.append(dict(item))
        return items

    def _knowledge_prompt_text(self, plugin_results: list[dict[str, Any]]) -> str:
        lines: list[str] = []
        for item in self._knowledge_items(plugin_results)[:6]:
            snippet = trim_text(str(item.get("snippet") or item.get("content_text") or ""), limit=220)
            lines.append(
                f"- [{item.get('knowledge_base_key') or item.get('knowledge_base_id')}/{item.get('key')}] "
                f"{item.get('title')}: {snippet}"
            )
        return "\n".join(lines)

    def _knowledge_query(self, context: AgentRunContext, node: NodeSpec) -> str:
        return "\n".join(
            filter(
                None,
                [
                    context.prompt,
                    node.instruction or "",
                    trim_text(pretty_json(context.inputs), limit=800),
                    trim_text(pretty_json(context.outputs), limit=1200),
                ],
            )
        )

    def _memory_runtime(self, agent: AgentSpec) -> dict[str, Any]:
        memory_profile = dict((agent.metadata or {}).get("memory_profile") or {})
        return {
            "provider": {
                "provider_type": agent.provider_type,
                "model": agent.model,
                "base_url": agent.base_url,
                "api_key": agent.api_key or (os.getenv(agent.api_key_env) if agent.api_key_env else None),
                "api_key_env": agent.api_key_env,
                "organization": agent.organization,
                "extra_headers": dict(agent.extra_headers),
                "extra_config": dict(agent.extra_config),
                "temperature": agent.temperature,
                "max_tokens": agent.max_tokens,
            },
            "memory_profile": memory_profile,
            "short_term": dict((memory_profile.get("config") or {}).get("short_term") or {}),
        }

    async def _invoke_plugin(
        self,
        *,
        plugin_ref: dict[str, Any],
        action: str,
        payload: dict[str, Any],
        context: dict[str, Any],
        hooks: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        review: dict[str, Any] | None = None
        if hooks and callable(hooks.get("before_tool_call")):
            reviewed = await hooks["before_tool_call"](
                plugin_ref=plugin_ref,
                action=action,
                payload=dict(payload),
                context=dict(context),
            )
            if isinstance(reviewed, dict):
                review = {key: value for key, value in reviewed.items() if key != "payload"}
                payload = dict(reviewed.get("payload") or payload)
        response = self.plugin_manager.invoke_plugin(
            plugin_ref,
            action=action,
            payload=payload,
            context=context,
        )
        return response, review

    async def _llm_response(self, agent: AgentSpec, prompt_block: str) -> dict[str, Any]:
        provider = self._provider_payload(agent)
        system_prompt = (
            "You are part of a configurable agent collaboration runtime.\n"
            "Return a JSON object with keys: summary, deliverables, risks, pass, next_focus."
        )
        response = self.gateway.complete(
            provider,
            [{"role": "system", "content": system_prompt}, {"role": "user", "content": prompt_block}],
            model=agent.model,
            temperature=agent.temperature,
            max_tokens=agent.max_tokens,
            capability_request=GatewayCapabilityRequest.json_object(),
        )
        try:
            parsed = json.loads(response.content)
            if isinstance(parsed, dict):
                return self._normalize_output(agent, parsed)
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
        return self._normalize_output(agent, {"summary": response.content})

    def _mock_response(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        recalled: list[dict[str, Any]],
        plugin_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        current_iteration = max(context.loops.values(), default=0)
        task_hint = trim_text(context.prompt, limit=96)
        recall_hint = trim_text(" ".join(item.get("text") or "" for item in recalled), limit=120)
        knowledge_hint = trim_text(" ".join(item.get("title") or "" for item in self._knowledge_items(plugin_results)), limit=120)
        role = agent.role.lower()
        summary = f"{agent.name} 处理节点 `{node.id}`，围绕“{task_hint}”给出可执行输出。"
        deliverables = [
            f"{agent.role} 明确当前阶段目标和完成标准",
            f"{agent.role} 提炼可交付项、约束和依赖",
            f"{agent.role} 为下游节点提供结构化输入",
        ]
        risks = [
            "范围蔓延会导致循环返工次数上升",
            "缺少验收标准会让自动收敛失败",
        ]
        passed = True
        next_focus = "推进到下一节点。"

        if "planner" in role:
            deliverables = [
                "拆解目标、阶段与里程碑",
                "标注前置依赖和风险点",
                "给出建议的交付顺序",
            ]
            next_focus = "交给设计或实现角色继续细化。"
        elif "architect" in role:
            deliverables = [
                "确定核心模块边界",
                "列出关键接口和数据流",
                "识别需要审批或人工确认的变更",
            ]
            risks.append("架构边界不清会导致实现阶段反复修订")
        elif "developer" in role:
            deliverables = [
                "列出实现任务包和文件边界",
                "说明最小可交付顺序",
                "给出验证步骤和返工入口",
            ]
            risks.append("未提前约束工作区输出会降低可回放性")
        elif "reviewer" in role or "qa" in role:
            passed = current_iteration >= 1
            summary = (
                "审查通过，当前方案满足定义完成标准。"
                if passed
                else "审查发现缺口，要求至少再完成一轮返工后重新提交。"
            )
            deliverables = [
                "审查关键输出与验收条件的映射",
                "识别返工入口和需补充的证据",
            ]
            risks = ["返工前未固定验收条件会导致重复审查"]
            next_focus = "进入交付。" if passed else "回到实现节点补齐缺口。"
        elif "synth" in role:
            deliverables = [
                "整合并行分支观点",
                "提炼统一结论和决策建议",
                "指出仍需人工判断的事项",
            ]
            risks = ["并行分支结论不一致时需要显式汇总规则"]
        elif "analyst" in role:
            deliverables = [
                "补充约束、依赖和失败模式",
                "输出风险缓解建议",
            ]

        if recall_hint:
            summary = f"{summary} 复用记忆提示：{recall_hint}"
        if knowledge_hint:
            summary = f"{summary} 检索到知识库条目：{knowledge_hint}"
        result = {
            "summary": summary,
            "deliverables": deliverables,
            "risks": risks,
            "pass": passed,
            "next_focus": next_focus,
            "details": {
                "node": node.id,
                "agent": agent.key,
                "iteration": current_iteration,
            },
        }
        return self._normalize_output(agent, result)

    def _normalize_output(self, agent: AgentSpec, payload: dict[str, Any]) -> dict[str, Any]:
        deliverables = [str(item) for item in payload.get("deliverables", []) if str(item).strip()]
        risks = [str(item) for item in payload.get("risks", []) if str(item).strip()]
        return {
            "agent": agent.key,
            "role": agent.role,
            "summary": str(payload.get("summary") or ""),
            "details": payload.get("details") or {},
            "deliverables": deliverables,
            "deliverables_text": "\n".join(f"- {item}" for item in deliverables),
            "risks": risks,
            "risks_text": "\n".join(f"- {item}" for item in risks),
            "pass": bool(payload.get("pass", True)),
            "next_focus": str(payload.get("next_focus") or ""),
        }

    async def _remember(
        self,
        agent: AgentSpec,
        scopes: MemoryScopes,
        result: dict[str, Any],
        *,
        runtime: dict[str, Any],
    ) -> None:
        records = [
            {
                "text": result["summary"],
                "kind": "fact",
                "layer": "semantic",
                "tier": "working",
                "importance": 0.68,
                "confidence": 0.82,
                "fact_key": f"{agent.key}:summary",
                "metadata": {"agent": agent.key, "role": agent.role},
            }
        ]
        for item in result.get("deliverables", [])[:3]:
            records.append(
                {
                    "text": item,
                    "kind": "fact",
                    "layer": "semantic",
                    "tier": "working",
                    "importance": 0.55,
                    "confidence": 0.75,
                    "fact_key": None,
                    "metadata": {"agent": agent.key, "role": agent.role, "kind": "deliverable"},
                }
            )
        target_scopes: list[Any] = []
        if agent.memory_policy in {"agent_private", "agent_private_plus_project"}:
            target_scopes.append(scopes.agent_private())
        if agent.memory_policy in {"project_shared", "agent_private_plus_project"}:
            target_scopes.append(scopes.project_shared())
        if agent.memory_policy == "run_retrospective":
            target_scopes = [scopes.run_retrospective()]
        if not target_scopes:
            target_scopes = [scopes.agent_private()]
        for scope in target_scopes:
            await self.memory.remember(scope, records, runtime=runtime)
