from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from aiteams.ai_gateway import AIGateway, ProviderRequestError
from aiteams.domain.models import AgentSpec, NodeSpec
from aiteams.memory.adapter import MemoryAdapter
from aiteams.memory.scope import MemoryScopes
from aiteams.plugins import PluginManager
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


class AgentKernel:
    def __init__(self, memory: MemoryAdapter, gateway: AIGateway | None = None, plugin_manager: PluginManager | None = None):
        self.memory = memory
        self.gateway = gateway or AIGateway()
        self.plugin_manager = plugin_manager

    async def execute(self, agent: AgentSpec, node: NodeSpec, context: AgentRunContext) -> dict[str, Any]:
        scopes = MemoryScopes(
            workspace_id=context.workspace_id,
            project_id=context.project_id,
            run_id=context.run_id,
            agent_id=agent.key,
        )
        working = await self.memory.load_working(scopes.working())
        recalled = await self._recall(agent, scopes, context, node)
        available_plugins = self._describe_plugins(agent)
        plugin_results = self._execute_plugin_actions(agent, node, context, available_plugins)
        prompt_block = self._build_prompt(agent, node, context, working, recalled, available_plugins, plugin_results)

        await self.memory.append_working(scopes.working(), "user", prompt_block, metadata={"node_id": node.id, "agent_id": agent.key})
        result = self._mock_response(agent, node, context, recalled)
        if agent.backend != "mock":
            try:
                result = await self._llm_response(agent, prompt_block)
            except ProviderRequestError:
                result = self._mock_response(agent, node, context, recalled)

        await self.memory.append_working(
            scopes.working(),
            "assistant",
            result["summary"],
            metadata={"node_id": node.id, "agent_id": agent.key, "pass": result.get("pass")},
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
        result["details"] = details
        result["visible_output_ids"] = sorted(context.outputs.keys())
        if plugin_results:
            result["plugin_results"] = plugin_results
        await self._remember(agent, scopes, result)
        return result

    async def _recall(
        self,
        agent: AgentSpec,
        scopes: MemoryScopes,
        context: AgentRunContext,
        node: NodeSpec,
    ) -> list[dict[str, Any]]:
        query = "\n".join(
            filter(
                None,
                [
                    context.prompt,
                    node.instruction or "",
                    trim_text(pretty_json(context.outputs), limit=1200),
                ],
            )
        )
        scope_list: list[Any] = []
        if agent.memory_policy in {"agent_private", "agent_private_plus_project"}:
            scope_list.append(scopes.agent_private())
        if agent.memory_policy in {"project_shared", "agent_private_plus_project"}:
            scope_list.append(scopes.project_shared())
        if agent.memory_policy == "run_retrospective":
            scope_list.append(scopes.run_retrospective())
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
        }
        working_text = "\n".join(f"- {item.get('role')}: {trim_text(item.get('content'), limit=140)}" for item in working[-6:])
        recall_text = "\n".join(f"- {trim_text(item.get('text'), limit=160)}" for item in recalled[:6])
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
            if self.plugin_manager and plugin_ref.get("install_path"):
                try:
                    runtime = self.plugin_manager.load_plugin(str(plugin_ref["id"]))
                    descriptor = dict(runtime.get("descriptor") or {})
                    actions = [str(item.get("name")) for item in descriptor.get("actions", []) if str(item.get("name") or "").strip()] or actions
                except Exception:
                    descriptor = {}
            else:
                descriptor = {}
            described.append(
                {
                    "id": str(plugin_ref.get("id") or ""),
                    "key": str(plugin_ref.get("key") or ""),
                    "version": str(plugin_ref.get("version") or ""),
                    "tools": [str(item) for item in (descriptor.get("tools") or manifest.get("tools") or [])],
                    "permissions": [str(item) for item in (descriptor.get("permissions") or manifest.get("permissions") or [])],
                    "actions": actions,
                    "install_path": plugin_ref.get("install_path"),
                }
            )
        return described

    def _execute_plugin_actions(
        self,
        agent: AgentSpec,
        node: NodeSpec,
        context: AgentRunContext,
        available_plugins: list[dict[str, Any]],
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
        }
        plugin_lookup = {str(item.get("id") or ""): item for item in self._plugin_refs(agent)}
        plugin_lookup.update({str(item.get("key") or ""): item for item in self._plugin_refs(agent)})
        results: list[dict[str, Any]] = []
        for action_spec in configured:
            item = dict(action_spec or {})
            plugin_ref = plugin_lookup.get(str(item.get("plugin_ref") or item.get("plugin_id") or item.get("plugin_key") or ""))
            if plugin_ref is None:
                raise RuntimeError(f"Node `{node.id}` references unavailable plugin `{item.get('plugin_ref') or item.get('plugin_key')}`.")
            raw_payload = render_template_data(item.get("payload") or {}, prompt_context)
            payload = raw_payload if isinstance(raw_payload, dict) else {"value": raw_payload}
            invocation_context = {
                "workspace_id": context.workspace_id,
                "project_id": context.project_id,
                "run_id": context.run_id,
                "node_id": node.id,
                "agent_id": agent.key,
                "visible_outputs": context.outputs,
                "available_plugins": available_plugins,
            }
            response = self.plugin_manager.invoke_plugin(
                plugin_ref,
                action=str(item.get("action") or ""),
                payload=payload,
                context=invocation_context,
            )
            results.append(
                {
                    "plugin_id": str(plugin_ref.get("id") or ""),
                    "plugin_key": str(plugin_ref.get("key") or ""),
                    "action": str(item.get("action") or ""),
                    "result": response,
                }
            )
        return results

    def _plugin_refs(self, agent: AgentSpec) -> list[dict[str, Any]]:
        items = list((agent.metadata or {}).get("plugins") or [])
        if items:
            return [dict(item or {}) for item in items]
        return [
            {"id": str(item), "key": str(item), "version": "", "manifest": {}, "install_path": None}
            for item in (agent.metadata or {}).get("plugin_ids", [])
        ]

    async def _llm_response(self, agent: AgentSpec, prompt_block: str) -> dict[str, Any]:
        provider = {
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
        system_prompt = (
            "You are part of a configurable agent collaboration runtime.\n"
            "Return a JSON object with keys: summary, deliverables, risks, pass, next_focus."
        )
        response = self.gateway.chat(
            provider,
            [{"role": "system", "content": system_prompt}, {"role": "user", "content": prompt_block}],
            model=agent.model,
            temperature=agent.temperature,
            max_tokens=agent.max_tokens,
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
    ) -> dict[str, Any]:
        current_iteration = max(context.loops.values(), default=0)
        task_hint = trim_text(context.prompt, limit=96)
        recall_hint = trim_text(" ".join(item.get("text") or "" for item in recalled), limit=120)
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

    async def _remember(self, agent: AgentSpec, scopes: MemoryScopes, result: dict[str, Any]) -> None:
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
            await self.memory.remember(scope, records)
