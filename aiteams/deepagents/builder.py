from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from deepagents import create_deep_agent
from langchain_core.messages import AIMessageChunk
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import BaseTool

from aiteams.deepagents.middleware import CheckpointMessageSanitizerMiddleware
from aiteams.utils import make_uuid7, slugify, trim_text


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
        tools_factory: Callable[..., list[BaseTool]],
        interrupt_factory: Callable[[dict[str, Any]], dict[str, Any] | None],
    ) -> None:
        self._model_factory = model_factory
        self._tools_factory = tools_factory
        self._interrupt_factory = interrupt_factory

    def compile(
        self,
        node: dict[str, Any],
        *,
        context: TeamBuildContext,
        skill_sources: list[str] | None,
    ) -> dict[str, Any]:
        if str(node.get("node_type") or "") == "team":
            raise ValueError("Leaf compiler only accepts agent nodes.")
        tools = self._tools_factory(
            executor=dict(node),
            run_id=context.run_id,
            team_definition_id=context.team_definition_id,
            workspace_id=context.workspace_id,
            project_id=context.project_id,
        )
        return {
            "name": str(node.get("name") or self._delegate_name(node)),
            "description": self._leaf_description(node),
            "system_prompt": self._optional_text(node.get("system_prompt")),
            "model": self._resolve_model(node),
            "tools": list(tools or []) or None,
            "skills": list(skill_sources or []) or None,
            "middleware": [CheckpointMessageSanitizerMiddleware()],
            "interrupt_on": self._interrupt_factory(dict(node)),
        }

    def _delegate_name(self, node: dict[str, Any]) -> str:
        return str(node.get("delegate_name") or node.get("runtime_key") or node.get("key") or node.get("name") or "agent")

    def _leaf_description(self, node: dict[str, Any]) -> str:
        goal = str(node.get("goal") or "").strip()
        role = str(node.get("role") or "agent").strip()
        name = str(node.get("name") or self._delegate_name(node)).strip()
        description = str(node.get("description") or "").strip()
        if description:
            return description
        if goal:
            return f"{name} handles {role} work. Goal: {goal}"
        return f"{name} handles delegated {role} work and returns a concise result."

    def _resolve_model(self, node: dict[str, Any]) -> Any:
        has_model = bool(str(node.get("model") or "").strip())
        has_provider = isinstance(node.get("provider"), dict) and bool(dict(node.get("provider") or {}))
        if not has_model and not has_provider:
            return None
        return self._model_factory(dict(node))

    def _optional_text(self, value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None


class TeamCompositeCompiler:
    def __init__(
        self,
        *,
        leaf_compiler: AgentLeafCompiler,
        model_factory: Callable[[dict[str, Any]], Any],
        tools_factory: Callable[..., list[BaseTool]],
        backend_factory: Callable[..., Any],
        skill_backend_root_factory: Callable[..., Path],
        skill_library_root: Path,
        interrupt_factory: Callable[[dict[str, Any]], dict[str, Any] | None],
        disabled_general_subagent_factory: Callable[[], dict[str, Any]],
    ) -> None:
        self._leaf_compiler = leaf_compiler
        self._model_factory = model_factory
        self._tools_factory = tools_factory
        self._backend_factory = backend_factory
        self._skill_backend_root_factory = skill_backend_root_factory
        self._skill_library_root = Path(skill_library_root).expanduser().resolve()
        self._interrupt_factory = interrupt_factory
        self._disabled_general_subagent_factory = disabled_general_subagent_factory

    async def compile_team(
        self,
        node: dict[str, Any],
        *,
        context: TeamBuildContext,
        is_root: bool,
    ) -> Any:
        if str(node.get("node_type") or "") != "team":
            raise ValueError("Team compiler only accepts team nodes.")
        runnable = await self._build_team_runnable(node, context=context)
        if is_root:
            return runnable
        return {
            "name": str(node.get("name") or self._delegate_name(node)),
            "description": self._team_description(node),
            "runnable": self._wrap_compiled_subteam_runnable(node=node, runnable=runnable),
        }

    async def _build_team_runnable(self, node: dict[str, Any], *, context: TeamBuildContext) -> Any:
        subagents = [self._disabled_general_subagent_factory()]
        skill_backend_root = self._skill_backend_root_factory(run_id=context.run_id, executor=dict(node))
        for child in list(node.get("children") or []):
            child_payload = dict(child or {})
            if str(child_payload.get("node_type") or "") == "team":
                subagents.append(await self.compile_team(child_payload, context=context, is_root=False))
                continue
            subagents.append(
                self._leaf_compiler.compile(
                    child_payload,
                    context=context,
                    skill_sources=await self._materialize_skill_sources(
                        executor=child_payload,
                        skill_backend_root=skill_backend_root,
                    ),
                )
            )

        lead = dict(node.get("lead") or {})
        tools = self._tools_factory(
            executor=lead,
            run_id=context.run_id,
            team_definition_id=context.team_definition_id,
            workspace_id=context.workspace_id,
            project_id=context.project_id,
        )
        runnable = create_deep_agent(
            model=self._resolve_model(lead),
            tools=list(tools or []) or None,
            system_prompt=self._optional_text(lead.get("system_prompt")),
            middleware=[CheckpointMessageSanitizerMiddleware()],
            subagents=subagents,
            skills=await self._materialize_skill_sources(
                executor=lead,
                skill_backend_root=skill_backend_root,
            ),
            checkpointer=context.checkpointer,
            store=context.langgraph_store,
            backend=self._backend_factory(run_id=context.run_id, executor=dict(node)),
            interrupt_on=self._interrupt_factory(lead),
            name=str(node.get("name") or self._delegate_name(node)),
        )
        return runnable

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

    def _resolve_model(self, executor: dict[str, Any]) -> Any:
        has_model = bool(str(executor.get("model") or "").strip())
        has_provider = isinstance(executor.get("provider"), dict) and bool(dict(executor.get("provider") or {}))
        if not has_model and not has_provider:
            return None
        return self._model_factory(dict(executor))

    def _optional_text(self, value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    def _wrap_compiled_subteam_runnable(self, *, node: dict[str, Any], runnable: Any) -> RunnableLambda:
        team_name = str(node.get("name") or self._delegate_name(node) or "team")

        def _invoke(state: Any, config: Any = None) -> dict[str, Any]:
            result = runnable.invoke(state, config=config)
            return self._normalize_compiled_subteam_result(result=result, team_name=team_name)

        async def _ainvoke(state: Any, config: Any = None) -> dict[str, Any]:
            result = await runnable.ainvoke(state, config=config)
            return self._normalize_compiled_subteam_result(result=result, team_name=team_name)

        return RunnableLambda(_invoke, afunc=_ainvoke, name=f"{team_name}-subteam-wrapper")

    def _normalize_compiled_subteam_result(self, *, result: Any, team_name: str) -> dict[str, Any]:
        text = self._compiled_subteam_result_text(result)
        return {"messages": [AIMessageChunk(content=text, name=team_name)]}

    def _compiled_subteam_result_text(self, result: Any) -> str:
        if isinstance(result, dict):
            messages = list(result.get("messages") or [])
            for message in reversed(messages):
                text = getattr(message, "text", None)
                if isinstance(text, str) and text.strip():
                    return text.strip()
                content = getattr(message, "content", "")
                if isinstance(content, str) and content.strip():
                    return content.strip()
                if isinstance(content, list):
                    parts: list[str] = []
                    for item in content:
                        if isinstance(item, dict):
                            value = str(item.get("text") or item.get("content") or "").strip()
                        else:
                            value = str(getattr(item, "text", "") or item or "").strip()
                        if value:
                            parts.append(value)
                    if parts:
                        return "\n".join(parts)
        text = str(result or "").strip()
        return text or "Nested team completed."

    async def _materialize_skill_sources(
        self,
        *,
        executor: dict[str, Any],
        skill_backend_root: Path,
    ) -> list[str] | None:
        skills = [dict(item) for item in list(executor.get("skills") or []) if isinstance(item, dict)]
        if not skills:
            return None

        sources: list[str] = []
        seen_sources: set[str] = set()
        inline_skills = [skill for skill in skills if str(skill.get("source") or "") != "catalog"]
        if inline_skills:
            scope = self._skill_scope(executor)
            backend_relative = self._relative_skill_backend_root(skill_backend_root)
            source_root = f"/skills/{backend_relative}/runtime/{scope}/" if backend_relative else f"/skills/runtime/{scope}/"
            source_root_dir = skill_backend_root / "runtime" / scope
            if source_root_dir.exists():
                shutil.rmtree(source_root_dir, ignore_errors=True)
            source_root_dir.mkdir(parents=True, exist_ok=True)
            seen_dirs: set[str] = set()
        for index, skill in enumerate(skills, start=1):
            catalog_source = self._catalog_skill_source(skill)
            if catalog_source:
                if catalog_source not in seen_sources:
                    seen_sources.add(catalog_source)
                    sources.append(catalog_source)
                continue
            skill_dir = self._skill_directory_name(skill=skill, index=index)
            while skill_dir in seen_dirs:
                skill_dir = f"{skill_dir}-{index}"
            seen_dirs.add(skill_dir)
            skill_dir_path = source_root_dir / skill_dir
            skill_dir_path.mkdir(parents=True, exist_ok=True)
            (skill_dir_path / "SKILL.md").write_text(self._skill_markdown(skill=skill, directory_name=skill_dir), encoding="utf-8")
        if inline_skills and source_root not in seen_sources:
            sources.append(source_root)
        return sources or None

    def _relative_skill_backend_root(self, skill_backend_root: Path) -> str:
        try:
            relative = skill_backend_root.relative_to(self._skill_library_root)
        except ValueError:
            return ""
        return relative.as_posix().strip("/")

    def _skill_scope(self, executor: dict[str, Any]) -> str:
        runtime_key = slugify(
            str(executor.get("runtime_key") or executor.get("name") or executor.get("key") or "agent"),
            fallback="agent",
        )
        agent_id = str(executor.get("agent_id") or "").strip()
        if not agent_id:
            return runtime_key
        return f"{runtime_key}-{agent_id[:12]}"

    def _skill_directory_name(self, *, skill: dict[str, Any], index: int) -> str:
        base = str(skill.get("name") or skill.get("id") or f"skill-{index}").strip()
        return self._skill_slug(base, fallback=f"skill-{index}")

    def _catalog_skill_source(self, skill: dict[str, Any]) -> str | None:
        raw_path = str(skill.get("storage_path") or "").replace("\\", "/").strip("/")
        if not raw_path:
            return None
        parts = [part for part in raw_path.split("/") if part and part not in {".", ".."}]
        if not parts:
            return None
        parent = "/".join(parts[:-1])
        return f"/skills/{parent}/" if parent else "/skills/"

    def _skill_slug(self, text: str, *, fallback: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "-", text.strip().lower()).strip("-")
        normalized = normalized[:64].strip("-")
        return normalized or fallback

    def _skill_markdown(self, *, skill: dict[str, Any], directory_name: str) -> str:
        title = str(skill.get("name") or directory_name).strip() or directory_name
        instructions = [str(item).strip() for item in list(skill.get("instructions") or []) if str(item).strip()]
        description = trim_text(" ".join(instructions[:2]), limit=220) or f"Runtime skill for {title}."
        lines = [
            "---",
            f"name: {json.dumps(directory_name, ensure_ascii=False)}",
            f"description: {json.dumps(description, ensure_ascii=False)}",
            "---",
            "",
            f"# {title}",
            "",
            "## Instructions",
            "",
        ]
        if instructions:
            lines.extend(f"- {item}" for item in instructions)
        else:
            lines.append("- Use this skill when it is relevant to the delegated task.")
        return "\n".join(lines)

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

    async def build_for_run(
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
        if checkpointer is None:
            raise ValueError("DeepAgents runtime requires a checkpointer.")
        if langgraph_store is None:
            raise ValueError("DeepAgents runtime requires a LangGraph store.")
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
        root_runnable = await self._team_compiler.compile_team(root_payload, context=context, is_root=True)
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
