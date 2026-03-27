from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from deepagents import create_deep_agent

from aiteams.utils import make_uuid7


@dataclass(slots=True)
class TeamBuildContext:
    run_id: str
    team_definition_id: str
    workspace_id: str
    project_id: str
    checkpointer: Any
    langgraph_store: Any


@dataclass(slots=True)
class CompiledTeamTree:
    root_runnable: Any
    runtime_tree_snapshot: dict[str, Any]
    resource_lock: dict[str, Any]
    compiled_metadata: dict[str, Any]
    root_runtime_key: str
    snapshot_record: dict[str, Any] | None = None


class AgentLeafCompiler:
    def __init__(
        self,
        *,
        model_factory: Callable[[dict[str, Any]], Any],
        tools_factory: Callable[..., list[Any]],
        interrupt_factory: Callable[[dict[str, Any]], dict[str, Any] | None],
    ) -> None:
        self._model_factory = model_factory
        self._tools_factory = tools_factory
        self._interrupt_factory = interrupt_factory

    def compile(self, node: dict[str, Any], *, context: TeamBuildContext) -> dict[str, Any]:
        if str(node.get("node_type") or "") == "team":
            raise ValueError("Leaf compiler only accepts agent nodes.")
        return {
            "name": str(node.get("name") or self._delegate_name(node)),
            "description": self._leaf_description(node),
            "system_prompt": str(node.get("system_prompt") or ""),
            "model": self._model_factory(dict(node)),
            "tools": self._tools_factory(
                executor=dict(node),
                run_id=context.run_id,
                team_definition_id=context.team_definition_id,
                workspace_id=context.workspace_id,
                project_id=context.project_id,
            ),
            "interrupt_on": self._interrupt_factory(dict(node)),
        }

    def _delegate_name(self, node: dict[str, Any]) -> str:
        return str(node.get("delegate_name") or node.get("runtime_key") or node.get("key") or node.get("name") or "agent")

    def _leaf_description(self, node: dict[str, Any]) -> str:
        goal = str(node.get("goal") or "").strip()
        role = str(node.get("role") or "agent").strip()
        name = str(node.get("name") or self._delegate_name(node)).strip()
        description = str(node.get("description") or "").strip()
        if description and goal:
            return f"{description} Goal: {goal}"
        if description:
            return description
        if goal:
            return f"{name} handles {role} work. Goal: {goal}"
        return f"{name} handles delegated {role} work and returns a concise result."


class TeamCompositeCompiler:
    def __init__(
        self,
        *,
        leaf_compiler: AgentLeafCompiler,
        model_factory: Callable[[dict[str, Any]], Any],
        tools_factory: Callable[..., list[Any]],
        backend_factory: Callable[..., Any],
        interrupt_factory: Callable[[dict[str, Any]], dict[str, Any] | None],
        disabled_general_subagent_factory: Callable[[], dict[str, Any]],
    ) -> None:
        self._leaf_compiler = leaf_compiler
        self._model_factory = model_factory
        self._tools_factory = tools_factory
        self._backend_factory = backend_factory
        self._interrupt_factory = interrupt_factory
        self._disabled_general_subagent_factory = disabled_general_subagent_factory

    def compile_team(
        self,
        node: dict[str, Any],
        *,
        context: TeamBuildContext,
        is_root: bool,
    ) -> Any:
        if str(node.get("node_type") or "") != "team":
            raise ValueError("Team compiler only accepts team nodes.")
        subagents = [self._disabled_general_subagent_factory()]
        for child in list(node.get("children") or []):
            child_payload = dict(child or {})
            if str(child_payload.get("node_type") or "") == "team":
                subagents.append(self.compile_team(child_payload, context=context, is_root=False))
                continue
            subagents.append(self._leaf_compiler.compile(child_payload, context=context))

        lead = dict(node.get("lead") or {})
        runnable = create_deep_agent(
            model=self._model_factory(lead),
            tools=self._tools_factory(
                executor=lead,
                run_id=context.run_id,
                team_definition_id=context.team_definition_id,
                workspace_id=context.workspace_id,
                project_id=context.project_id,
            ),
            system_prompt=str(node.get("system_prompt") or lead.get("system_prompt") or ""),
            subagents=subagents,
            checkpointer=context.checkpointer,
            store=context.langgraph_store,
            backend=self._backend_factory(run_id=context.run_id, executor=dict(node)),
            interrupt_on=self._interrupt_factory(lead),
            name=str(node.get("name") or self._delegate_name(node)),
        )
        if is_root:
            return runnable
        return {
            "name": str(node.get("name") or self._delegate_name(node)),
            "description": self._team_description(node),
            "runnable": runnable,
        }

    def _delegate_name(self, node: dict[str, Any]) -> str:
        return str(node.get("delegate_name") or node.get("runtime_key") or node.get("key") or node.get("name") or "team")

    def _team_description(self, node: dict[str, Any]) -> str:
        lead = dict(node.get("lead") or {})
        description = str(node.get("description") or "").strip()
        if description:
            return description
        goal = str(lead.get("goal") or "").strip()
        if goal:
            return f"Nested team `{node.get('name') or self._delegate_name(node)}`. Goal: {goal}"
        return f"Nested team `{node.get('name') or self._delegate_name(node)}` coordinates its direct children and returns one result."


class DynamicTeamBuilder:
    def __init__(
        self,
        *,
        leaf_compiler: AgentLeafCompiler,
        team_compiler: TeamCompositeCompiler,
        snapshot_writer: Callable[..., dict[str, Any]] | None = None,
    ) -> None:
        self._leaf_compiler = leaf_compiler
        self._team_compiler = team_compiler
        self._snapshot_writer = snapshot_writer

    def build_for_run(
        self,
        *,
        root: dict[str, Any],
        resource_lock: dict[str, Any],
        run_id: str,
        team_definition_id: str,
        workspace_id: str,
        project_id: str,
        checkpointer: Any,
        langgraph_store: Any,
    ) -> CompiledTeamTree:
        root_payload = self._hydrate_agent_ids(dict(root or {}))
        if str(root_payload.get("node_type") or "") != "team":
            raise ValueError("DeepAgents runtime root must be a team node.")
        context = TeamBuildContext(
            run_id=run_id,
            team_definition_id=team_definition_id,
            workspace_id=workspace_id,
            project_id=project_id,
            checkpointer=checkpointer,
            langgraph_store=langgraph_store,
        )
        root_runnable = self._team_compiler.compile_team(root_payload, context=context, is_root=True)
        compiled_metadata = {
            "compilation_rule_version": "deepagents_team_tree_v1",
            "root_runtime_key": self._runtime_key(root_payload),
            "agent_count": self._count_agents(root_payload),
            "team_count": self._count_teams(root_payload),
            "node_compilation_tree": self._compiled_tree(root_payload, is_root=True),
        }
        snapshot_record = None
        if self._snapshot_writer is not None:
            snapshot_record = self._snapshot_writer(
                team_definition_id=team_definition_id or None,
                run_id=run_id,
                runtime_tree_snapshot=root_payload,
                resource_lock=dict(resource_lock or {}),
                compiled_metadata=compiled_metadata,
            )
        return CompiledTeamTree(
            root_runnable=root_runnable,
            runtime_tree_snapshot=root_payload,
            resource_lock=dict(resource_lock or {}),
            compiled_metadata=compiled_metadata,
            root_runtime_key=self._runtime_key(root_payload),
            snapshot_record=snapshot_record,
        )

    def _compiled_tree(self, node: dict[str, Any], *, is_root: bool) -> dict[str, Any]:
        payload = {
            "node_type": str(node.get("node_type") or ""),
            "runtime_key": self._runtime_key(node),
            "agent_id": str(node.get("agent_id") or ""),
            "delegate_name": str(node.get("delegate_name") or self._runtime_key(node)),
            "name": str(node.get("name") or node.get("key") or self._runtime_key(node)),
        }
        if str(node.get("node_type") or "") == "team":
            payload["compiled_kind"] = "root_deep_agent" if is_root else "compiled_subagent"
            payload["lead_runtime_key"] = str(((node.get("lead") or {}).get("runtime_key")) or "")
            payload["lead_agent_id"] = str(((node.get("lead") or {}).get("agent_id")) or "")
            payload["children"] = [
                self._compiled_tree(dict(child or {}), is_root=False)
                for child in list(node.get("children") or [])
            ]
            return payload
        payload["compiled_kind"] = "subagent"
        payload["role"] = str(node.get("role") or "agent")
        return payload

    def _runtime_key(self, node: dict[str, Any]) -> str:
        return str(node.get("runtime_key") or node.get("key") or node.get("name") or "node")

    def _hydrate_agent_ids(self, node: dict[str, Any], *, owner_agent_id: str | None = None) -> dict[str, Any]:
        payload = dict(node or {})
        node_type = str(payload.get("node_type") or "")
        if node_type == "team":
            agent_id = str(payload.get("agent_id") or owner_agent_id or make_uuid7())
            payload["agent_id"] = agent_id
            payload["lead"] = self._hydrate_agent_ids(dict(payload.get("lead") or {}), owner_agent_id=agent_id)
            payload["children"] = [self._hydrate_agent_ids(dict(child or {})) for child in list(payload.get("children") or [])]
            return payload
        payload["agent_id"] = str(payload.get("agent_id") or owner_agent_id or make_uuid7())
        return payload

    def _count_agents(self, node: dict[str, Any]) -> int:
        if str(node.get("node_type") or "") != "team":
            return 1
        total = 1
        for child in list(node.get("children") or []):
            total += self._count_agents(dict(child or {}))
        return total

    def _count_teams(self, node: dict[str, Any]) -> int:
        if str(node.get("node_type") or "") != "team":
            return 0
        total = 1
        for child in list(node.get("children") or []):
            total += self._count_teams(dict(child or {}))
        return total
