from __future__ import annotations

from pathlib import Path
from typing import Any

from deepagents.backends import CompositeBackend, FilesystemBackend, StoreBackend
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.types import Command
from langchain_core.messages import AIMessageChunk, HumanMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import BaseTool, StructuredTool

from aiteams.agent.kernel import AgentKernel
from aiteams.common import events as event_types
from aiteams.deepagents.builder import AgentLeafCompiler, DynamicTeamBuilder, TeamCompositeCompiler
from aiteams.memory.scope import MemoryScopes
from aiteams.plugins import build_plugin_base_tool
from aiteams.plugins.manager import (
    BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY,
    BUILTIN_KB_RETRIEVE_PLUGIN_KEY,
    BUILTIN_MEMORY_MANAGE_PLUGIN_KEY,
    BUILTIN_MEMORY_SEARCH_PLUGIN_KEY,
)
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import make_uuid7, pretty_json, trim_text, utcnow_iso
from aiteams.workspace.manager import WorkspaceManager


class DeepAgentsTeamRuntime:
    def __init__(
        self,
        *,
        store: MetadataStore,
        agent_kernel: AgentKernel,
        workspace: WorkspaceManager,
        checkpoint_db_path: str | Path,
        skill_storage_root: str | Path,
    ):
        self.store = store
        self.agent_kernel = agent_kernel
        self.workspace = workspace
        self.checkpoint_db_path = Path(checkpoint_db_path).expanduser().resolve()
        self.skill_storage_root = Path(skill_storage_root).expanduser().resolve()
        self.checkpoint_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.skill_storage_root.mkdir(parents=True, exist_ok=True)
        leaf_compiler = AgentLeafCompiler(
            model_factory=self._build_chat_model,
            tools_factory=self._agent_tools,
            interrupt_factory=self._interrupt_on,
        )
        team_compiler = TeamCompositeCompiler(
            leaf_compiler=leaf_compiler,
            model_factory=self._build_chat_model,
            tools_factory=self._agent_tools,
            backend_factory=self._backend_factory,
            skill_backend_root_factory=self._skill_backend_root,
            skill_library_root=self.skill_storage_root,
            interrupt_factory=self._interrupt_on,
            disabled_general_subagent_factory=self._disabled_general_purpose_subagent,
        )
        self.team_builder = DynamicTeamBuilder(
            leaf_compiler=leaf_compiler,
            team_compiler=team_compiler,
            snapshot_writer=self._save_team_build_snapshot,
        )

    def handles(self, blueprint_spec: dict[str, Any]) -> bool:
        metadata = dict(blueprint_spec.get("metadata") or {})
        return str(metadata.get("execution_mode") or "") == "deepagents_hierarchy" and isinstance(metadata.get("deepagents_runtime"), dict)

    def initial_state(
        self,
        blueprint_spec: dict[str, Any],
        *,
        title: str | None,
        prompt: str,
        inputs: dict[str, Any],
        approval_mode: str,
        session_thread_id: str | None = None,
    ) -> dict[str, Any]:
        runtime = dict((blueprint_spec.get("metadata") or {}).get("deepagents_runtime") or {})
        resolved_session_thread_id = str(session_thread_id or "").strip() or make_uuid7()
        return {
            "mode": "deepagents_hierarchy",
            "task": {
                "title": title,
                "prompt": prompt,
                "inputs": dict(inputs or {}),
                "approval_mode": approval_mode,
            },
            "deepagents_runtime": runtime,
            "history": [],
            "waiting": None,
            "thread_id": None,
            "session_thread_id": resolved_session_thread_id,
        }

    def ensure_task_thread(
        self,
        *,
        run_id: str,
        blueprint_spec: dict[str, Any],
        workspace_id: str,
        project_id: str,
        title: str | None,
        prompt: str,
        session_thread_id: str | None = None,
    ) -> dict[str, Any] | None:
        if not self.handles(blueprint_spec):
            return None
        existing = self.store.list_task_threads(run_id=run_id)
        if existing:
            return existing[0]
        runtime = dict((blueprint_spec.get("metadata") or {}).get("deepagents_runtime") or {})
        root = dict(runtime.get("root") or {})
        resolved_session_thread_id = str(session_thread_id or "").strip() or make_uuid7()
        thread = self.store.create_task_thread(
            team_definition_id=str(runtime.get("team_definition_id") or "") or None,
            run_id=run_id,
            workspace_id=workspace_id,
            project_id=project_id,
            title=title or prompt[:80],
            metadata={
                "team_definition_id": runtime.get("team_definition_id"),
                "team_definition_key": runtime.get("team_definition_key"),
                "root_team_key": root.get("key"),
                "root_team_name": root.get("name"),
                "session_thread_id": resolved_session_thread_id,
                "mode": "deepagents_hierarchy",
            },
        )
        return thread

    async def start_run(self, run_id: str) -> None:
        await self._run_graph(run_id)

    async def resume_run(self, run_id: str, resolution: dict[str, Any] | None = None) -> None:
        run, runtime_state, _blueprint_spec = self._bundle(run_id)
        waiting = dict(runtime_state.get("waiting") or {})
        if not waiting:
            await self._run_graph(run_id)
            return
        approval = self.store.get_approval(str(waiting.get("approval_id") or ""))
        if approval is None or approval["status"] == "pending":
            return
        resolved_payload = dict(resolution or approval.get("resolution_json") or {})
        self.store.add_event(
            run_id=run_id,
            event_type=event_types.APPROVAL_RESOLVED,
            payload={"approval_id": approval["id"], "resolution": approval.get("resolution_json") or {}},
            step_id=str(waiting.get("step_id") or "") or None,
        )
        if str(waiting.get("scope") or "") == "final_delivery":
            await self._resume_final_delivery(
                run=run,
                runtime_state=runtime_state,
                resolution=resolved_payload,
            )
            return
        await self._run_graph(run_id, resume=self._resume_payload(waiting=waiting, resolution=resolved_payload))

    def inject_human_message(
        self,
        *,
        run_id: str,
        target_agent_id: str,
        body: str,
        message_type: str = "dialogue",
        phase: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        del run_id, target_agent_id, body, message_type, phase, metadata
        raise ValueError("Human message injection is not yet supported for deepagents hierarchy runs.")

    async def _run_graph(self, run_id: str, *, resume: dict[str, Any] | None = None) -> None:
        run, runtime_state, blueprint_spec = self._bundle(run_id)
        task = dict(runtime_state.get("task") or {})
        prompt = str(task.get("prompt") or "")
        session_thread_id = str(runtime_state.get("session_thread_id") or "").strip() or make_uuid7()
        runtime_state["session_thread_id"] = session_thread_id
        root = dict((runtime_state.get("deepagents_runtime") or {}).get("root") or {})
        if not root:
            raise ValueError("DeepAgents runtime payload is missing root node.")
        step = self._ensure_orchestrate_step(run_id=run_id, prompt=prompt, root=root)
        try:
            existing_threads = self.store.list_task_threads(run_id=run_id)
            if existing_threads:
                runtime_state["thread_id"] = existing_threads[0]["id"]
            self.store.update_run(
                run_id,
                status="running",
                current_node_id="deepagents_orchestrate",
                state=runtime_state,
                started_at=run.get("started_at"),
                finished_at=run.get("finished_at"),
            )
            memory = self.agent_kernel.memory
            if not hasattr(memory, "async_langgraph_store"):
                raise ValueError("DeepAgents runtime requires memory.async_langgraph_store() for async LangGraph storage.")
            async with AsyncSqliteSaver.from_conn_string(str(self.checkpoint_db_path)) as checkpointer:
                async with memory.async_langgraph_store() as langgraph_store:
                    compiled_tree = await self.team_builder.build_for_run(
                        root=root,
                        resource_lock=dict((blueprint_spec.get("metadata") or {}).get("resource_lock") or {}),
                        run_id=run_id,
                        team_definition_id=str((runtime_state.get("deepagents_runtime") or {}).get("team_definition_id") or ""),
                        workspace_id=str(blueprint_spec.get("workspace_id") or ""),
                        project_id=str(blueprint_spec.get("project_id") or ""),
                        checkpointer=checkpointer,
                        langgraph_store=langgraph_store,
                    )
                    runtime_state.setdefault("deepagents_runtime", {})["root"] = compiled_tree.runtime_tree_snapshot
                    root = dict(compiled_tree.runtime_tree_snapshot)
                    runtime_state["team_build_snapshot_id"] = (compiled_tree.snapshot_record or {}).get("id")
                    runtime_state["compiled_team_metadata"] = compiled_tree.compiled_metadata
                    self.store.update_run(
                        run_id,
                        status="running",
                        current_node_id="deepagents_orchestrate",
                        state=runtime_state,
                        started_at=run.get("started_at"),
                        finished_at=run.get("finished_at"),
                    )
                    agent = compiled_tree.root_runnable
                    graph_config = self._graph_config(run_id=run_id, session_thread_id=session_thread_id)
                    if resume is None:
                        result = await agent.ainvoke({"messages": [HumanMessage(content=prompt)]}, graph_config)
                    else:
                        result = await agent.ainvoke(Command(resume=resume), graph_config)
                    interrupted = self._interrupt_payload(result)
                    if interrupted is not None:
                        executor = self._executor(root)
                        self._pause_for_interrupt(
                            run_id=run_id,
                            step_id=str(step["id"]),
                            runtime_state=runtime_state,
                            executor=executor,
                            interrupt_payload=interrupted,
                        )
                        return
            final_text = self._final_text(result)
            if self._should_review_final_delivery(self._executor(root)):
                self._pause_for_final_delivery(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    runtime_state=runtime_state,
                    executor=self._executor(root),
                    final_text=final_text,
                )
                return
            self._complete_run(
                run=run,
                runtime_state=runtime_state,
                blueprint_spec=blueprint_spec,
                root=root,
                step_id=str(step["id"]),
                final_text=final_text,
            )
        except Exception as exc:
            self.store.update_step(
                str(step["id"]),
                status="error",
                output_payload={"error": str(exc)},
                error_text=str(exc),
                finished=True,
            )
            self.store.add_event(
                run_id=run_id,
                step_id=str(step["id"]),
                event_type=event_types.STEP_FAILED,
                payload={"node_id": "deepagents_orchestrate", "error": str(exc)},
            )
            self.store.update_run(
                run_id,
                status="failed",
                summary=str(exc),
                current_node_id="deepagents_orchestrate",
                state=runtime_state,
                finished_at=utcnow_iso(),
            )
            self.store.update_task_release(str(run["task_release_id"]), status="failed")
            self.store.add_event(run_id=run_id, event_type=event_types.RUN_FAILED, payload={"error": str(exc)})
            raise

    def _bundle(self, run_id: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        bundle = self.store.get_run_bundle(run_id)
        if bundle is None or bundle.get("run") is None or bundle.get("blueprint") is None:
            raise ValueError("Run bundle does not exist.")
        run = dict(bundle["run"])
        blueprint = dict(bundle["blueprint"]["spec_json"] or {})
        runtime_state = dict(run.get("state_json") or {})
        return run, runtime_state, blueprint

    def _executor(self, node: dict[str, Any]) -> dict[str, Any]:
        if str(node.get("node_type") or "") == "team":
            return dict(node.get("lead") or {})
        return dict(node)

    def _disabled_general_purpose_subagent(self) -> dict[str, Any]:
        disabled = RunnableLambda(
            lambda _state: {
                "messages": [
                    AIMessageChunk(
                        content="The general-purpose subagent is disabled for this hierarchical team. Use explicit child subagents only."
                    )
                ]
            }
        )
        return {"name": "general-purpose", "description": "Disabled in strict hierarchy mode.", "runnable": disabled}

    def _backend_namespace(self, *, run_id: str, executor: dict[str, Any]) -> tuple[str, ...]:
        return (
            "deepagents",
            "run",
            run_id,
            "agent",
            str(executor.get("agent_id") or executor.get("runtime_key") or executor.get("key") or "agent"),
            "files",
        )

    def _backend_factory(self, *, run_id: str, executor: dict[str, Any]):
        namespace = self._backend_namespace(run_id=run_id, executor=executor)

        def _factory(runtime: Any) -> CompositeBackend:
            return CompositeBackend(
                default=StoreBackend(runtime, namespace=lambda _ctx, ns=namespace: ns),
                routes={
                    "/skills/": FilesystemBackend(root_dir=self.skill_storage_root, virtual_mode=True),
                },
            )

        return _factory

    def _skill_backend_root(self, *, run_id: str, executor: dict[str, Any]) -> Path:
        return self.skill_storage_root.joinpath(*self._backend_namespace(run_id=run_id, executor=executor), "skills")

    def _build_chat_model(self, executor: dict[str, Any]) -> Any:
        provider_payload = self._provider_payload(executor)
        return self.agent_kernel.gateway.build_chat_model(
            provider_payload,
            model=str(executor.get("model") or ""),
            temperature=float((provider_payload.get("config") or {}).get("temperature", 0.2) or 0.2),
            max_tokens=(provider_payload.get("config") or {}).get("max_tokens"),
            agent_name=str(executor.get("name") or executor.get("runtime_key") or executor.get("key") or "Agent"),
        )

    def _save_team_build_snapshot(
        self,
        *,
        team_definition_id: str | None,
        run_id: str,
        runtime_tree_snapshot: dict[str, Any],
        resource_lock: dict[str, Any],
        compiled_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        return self.store.save_team_build_snapshot(
            snapshot_id=None,
            team_definition_id=team_definition_id,
            run_id=run_id,
            runtime_tree_snapshot=runtime_tree_snapshot,
            resource_lock=resource_lock,
            compiled_metadata=compiled_metadata,
        )

    def _provider_payload(self, executor: dict[str, Any]) -> dict[str, Any]:
        provider = dict(executor.get("provider") or {})
        config = dict(provider.get("config") or {})
        secret = dict(provider.get("secret") or {})
        return {
            "id": provider.get("id"),
            "name": provider.get("name"),
            "provider_type": provider.get("provider_type"),
            "base_url": config.get("base_url"),
            "api_version": config.get("api_version"),
            "organization": config.get("organization"),
            "api_key": secret.get("api_key"),
            "api_key_env": config.get("api_key_env"),
            "skip_tls_verify": bool(config.get("skip_tls_verify")),
            "extra_config": dict(config.get("extra_config") or {}),
            "extra_headers": dict(config.get("extra_headers") or {}),
            "config": config,
            "model": executor.get("model"),
        }

    def _agent_tools(
        self,
        *,
        executor: dict[str, Any],
        run_id: str,
        team_definition_id: str,
        workspace_id: str,
        project_id: str,
    ) -> list[BaseTool]:
        tools: list[BaseTool] = []
        memory = self.agent_kernel.memory
        scopes = MemoryScopes(
            workspace_id=workspace_id,
            project_id=project_id,
            run_id=run_id,
            agent_id=str(executor.get("agent_id") or executor.get("runtime_key") or executor.get("key") or "agent"),
            team_id=team_definition_id or None,
        )
        if hasattr(memory, "builtin_search"):
            async def _memory_search(query: str, scope: str = "agent", limit: int = 4) -> str:
                payload = memory.builtin_search(self._memory_scope_list(scopes, scope=scope), query=query, top_k=limit)
                return pretty_json(payload)

            tools.append(
                StructuredTool.from_function(
                    name="memory_search",
                    coroutine=_memory_search,
                    description="Search database-backed short-term and long-term memory.",
                )
            )
        if hasattr(memory, "builtin_manage"):
            async def _memory_remember(text: str, scope: str = "agent") -> str:
                target = self._single_memory_scope(scopes, scope=scope)
                payload = memory.builtin_manage(
                    target,
                    operation="upsert",
                    payload={"record": {"text": text, "summary": trim_text(text, limit=180)}},
                )
                return pretty_json(payload)

            tools.append(
                StructuredTool.from_function(
                    name="memory_remember",
                    coroutine=_memory_remember,
                    description="Persist a memory record into the embedded database memory stack.",
                )
            )
        if self.agent_kernel.plugin_manager and list(executor.get("knowledge_bases") or []):
            async def _knowledge_search(query: str, limit: int = 4) -> str:
                payload = self.agent_kernel.plugin_manager.invoke_plugin(
                    {
                        "id": f"builtin:{BUILTIN_KB_RETRIEVE_PLUGIN_KEY}",
                        "key": BUILTIN_KB_RETRIEVE_PLUGIN_KEY,
                        "version": "builtin",
                        "builtin": True,
                    },
                    action="retrieve",
                    payload={"query": query, "limit": limit, "knowledge_bases": list(executor.get("knowledge_bases") or [])},
                    context={
                        "workspace_id": workspace_id,
                        "project_id": project_id,
                        "run_id": run_id,
                        "team_id": team_definition_id,
                        "agent_id": executor.get("agent_id") or executor.get("runtime_key"),
                        "agent_runtime_key": executor.get("runtime_key"),
                        "knowledge_bases": list(executor.get("knowledge_bases") or []),
                    },
                )
                return pretty_json(payload)

            tools.append(
                StructuredTool.from_function(
                    name="knowledge_search",
                    coroutine=_knowledge_search,
                    description="Query the knowledge bases bound to this agent and return grounded source snippets.",
                )
            )
        for plugin in list(executor.get("plugins") or []):
            if str(plugin.get("key") or "") in {
                BUILTIN_KB_RETRIEVE_PLUGIN_KEY,
                BUILTIN_MEMORY_SEARCH_PLUGIN_KEY,
                BUILTIN_MEMORY_MANAGE_PLUGIN_KEY,
                BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY,
            }:
                continue
            manifest = dict(plugin.get("manifest_json") or plugin.get("manifest") or {})
            for action in list(manifest.get("actions") or []):
                action = dict(action or {})
                action_name = str(action.get("name") or "").strip()
                if not action_name:
                    continue
                fallback_tool_name = f"plugin_{str(plugin.get('key') or 'plugin').replace('.', '_')}_{action_name}"
                def _invoke_plugin_payload(payload: dict[str, Any], *, _plugin=plugin, _action=action_name) -> str:
                    result = self.agent_kernel.plugin_manager.invoke_plugin(
                        _plugin,
                        action=_action,
                        payload=payload,
                        context={
                            "workspace_id": workspace_id,
                            "project_id": project_id,
                            "run_id": run_id,
                            "team_id": team_definition_id,
                            "agent_id": executor.get("agent_id") or executor.get("runtime_key"),
                            "agent_runtime_key": executor.get("runtime_key"),
                            "knowledge_bases": list(executor.get("knowledge_bases") or []),
                        },
                    )
                    return pretty_json(result)

                tools.append(
                    _build_plugin_base_tool(
                        plugin_key=str(plugin.get("key") or "plugin"),
                        action=action,
                        fallback_tool_name=fallback_tool_name,
                        invoker=_invoke_plugin_payload,
                    )
                )
        return tools

    def _memory_scope_list(self, scopes: MemoryScopes, *, scope: str) -> list[Any]:
        normalized = str(scope or "agent").strip().lower()
        if normalized == "team":
            return [scopes.team_shared()]
        if normalized == "project":
            return [scopes.project_shared()]
        if normalized == "run":
            return [scopes.run_retrospective()]
        return [scopes.agent_private()]

    def _single_memory_scope(self, scopes: MemoryScopes, *, scope: str):
        return self._memory_scope_list(scopes, scope=scope)[0]

    def _graph_config(self, *, run_id: str, session_thread_id: str | None) -> dict[str, Any]:
        resolved_thread_id = str(session_thread_id or "").strip() or make_uuid7()
        return {"configurable": {"thread_id": resolved_thread_id, "run_id": run_id}}

    def _ensure_orchestrate_step(self, *, run_id: str, prompt: str, root: dict[str, Any]) -> dict[str, Any]:
        existing = self.store.latest_step_for_node(run_id, "deepagents_orchestrate")
        if existing is not None and str(existing.get("status") or "") in {"blocked", "running"}:
            self.store.update_step(
                str(existing["id"]),
                status="running",
                output_payload=dict(existing.get("output_json") or {}),
                finished=False,
            )
            return dict(self.store.get_step(str(existing["id"])) or existing)
        step = self.store.create_step(
            run_id=run_id,
            node_id="deepagents_orchestrate",
            node_type="agent",
            status="running",
            attempt=self.store.next_step_attempt(run_id, "deepagents_orchestrate"),
            input_payload={"prompt": prompt, "root": {"key": root.get("key"), "name": root.get("name")}},
        )
        self.store.add_event(
            run_id=run_id,
            step_id=str(step["id"]),
            event_type=event_types.STEP_STARTED,
            payload={"node_id": "deepagents_orchestrate", "node_type": "agent"},
        )
        return step

    def _interrupt_payload(self, result: Any) -> dict[str, Any] | None:
        if not isinstance(result, dict):
            return None
        interrupts = list(result.get("__interrupt__") or [])
        if not interrupts:
            return None
        first = interrupts[0]
        if isinstance(first, dict):
            return dict(first.get("value") or {})
        return dict(getattr(first, "value", {}) or {})

    def _pause_for_interrupt(
        self,
        *,
        run_id: str,
        step_id: str,
        runtime_state: dict[str, Any],
        executor: dict[str, Any],
        interrupt_payload: dict[str, Any],
    ) -> None:
        approval = self.store.create_approval(
            run_id=run_id,
            step_id=step_id,
            node_id="deepagents_orchestrate",
            title=self._interrupt_title(executor=executor, interrupt_payload=interrupt_payload),
            detail=self._interrupt_detail(interrupt_payload),
        )
        runtime_state["waiting"] = {
            "scope": "tool_interrupt",
            "approval_id": approval["id"],
            "step_id": step_id,
            "executor_runtime_key": executor.get("runtime_key"),
            "interrupt_payload": interrupt_payload,
        }
        self.store.update_step(
            step_id,
            status="blocked",
            output_payload={
                "approval_id": approval["id"],
                "scope": "tool_interrupt",
                "actions": list(interrupt_payload.get("action_requests") or []),
            },
            finished=False,
        )
        self.store.update_run(
            run_id,
            status="waiting_approval",
            current_node_id="deepagents_orchestrate",
            state=runtime_state,
        )
        self.store.add_event(
            run_id=run_id,
            step_id=step_id,
            event_type=event_types.STEP_BLOCKED,
            payload={"node_id": "deepagents_orchestrate", "scope": "tool_interrupt", "approval_id": approval["id"]},
        )
        self.store.add_event(
            run_id=run_id,
            step_id=step_id,
            event_type=event_types.APPROVAL_REQUESTED,
            payload={"approval_id": approval["id"], "scope": "tool_interrupt", "interrupt": interrupt_payload},
        )
        self.store.add_event(run_id=run_id, event_type=event_types.RUN_PAUSED, payload={"reason": "waiting_approval", "approval_id": approval["id"]})

    def _pause_for_final_delivery(
        self,
        *,
        run_id: str,
        step_id: str,
        runtime_state: dict[str, Any],
        executor: dict[str, Any],
        final_text: str,
    ) -> None:
        approval = self.store.create_approval(
            run_id=run_id,
            step_id=step_id,
            node_id="deepagents_orchestrate",
            title=f"Review final delivery for `{executor.get('name') or executor.get('runtime_key') or 'team'}`",
            detail=trim_text(final_text, limit=4000),
        )
        runtime_state["pending_result_text"] = final_text
        runtime_state["waiting"] = {
            "scope": "final_delivery",
            "approval_id": approval["id"],
            "step_id": step_id,
            "executor_runtime_key": executor.get("runtime_key"),
        }
        self.store.update_step(
            step_id,
            status="blocked",
            output_payload={"approval_id": approval["id"], "scope": "final_delivery", "summary": trim_text(final_text, limit=240)},
            finished=False,
        )
        self.store.update_run(
            run_id,
            status="waiting_approval",
            current_node_id="deepagents_orchestrate",
            state=runtime_state,
        )
        self.store.add_event(
            run_id=run_id,
            step_id=step_id,
            event_type=event_types.STEP_BLOCKED,
            payload={"node_id": "deepagents_orchestrate", "scope": "final_delivery", "approval_id": approval["id"]},
        )
        self.store.add_event(
            run_id=run_id,
            step_id=step_id,
            event_type=event_types.APPROVAL_REQUESTED,
            payload={"approval_id": approval["id"], "scope": "final_delivery", "summary": trim_text(final_text, limit=400)},
        )
        self.store.add_event(run_id=run_id, event_type=event_types.RUN_PAUSED, payload={"reason": "waiting_approval", "approval_id": approval["id"]})

    async def _resume_final_delivery(
        self,
        *,
        run: dict[str, Any],
        runtime_state: dict[str, Any],
        resolution: dict[str, Any],
    ) -> None:
        run_id = str(run["id"])
        waiting = dict(runtime_state.get("waiting") or {})
        approved = bool(resolution.get("approved", False))
        if not approved:
            self.store.update_step(
                str(waiting.get("step_id") or ""),
                status="error",
                output_payload={"error": "Final delivery rejected."},
                error_text="Final delivery rejected.",
                finished=True,
            )
            self.store.update_run(
                run_id,
                status="failed",
                summary="Final delivery rejected.",
                current_node_id="deepagents_orchestrate",
                state={**runtime_state, "waiting": None},
                finished_at=utcnow_iso(),
            )
            self.store.update_task_release(str(run["task_release_id"]), status="failed")
            self.store.add_event(run_id=run_id, event_type=event_types.RUN_FAILED, payload={"reason": "final_delivery_rejected"})
            return
        _run, refreshed_state, blueprint_spec = self._bundle(run_id)
        final_text = str(((resolution.get("metadata") or {}).get("edited_body")) or refreshed_state.get("pending_result_text") or "").strip()
        if not final_text:
            raise ValueError("Final delivery resume is missing pending result text.")
        refreshed_state["waiting"] = None
        refreshed_state.pop("pending_result_text", None)
        root = dict((refreshed_state.get("deepagents_runtime") or {}).get("root") or {})
        self._complete_run(
            run=run,
            runtime_state=refreshed_state,
            blueprint_spec=blueprint_spec,
            root=root,
            step_id=str(waiting.get("step_id") or ""),
            final_text=final_text,
        )

    def _complete_run(
        self,
        *,
        run: dict[str, Any],
        runtime_state: dict[str, Any],
        blueprint_spec: dict[str, Any],
        root: dict[str, Any],
        step_id: str,
        final_text: str,
    ) -> None:
        runtime_state["waiting"] = None
        runtime_state.pop("pending_result_text", None)
        artifact_path = self.workspace.write_artifact(
            workspace_id=str(blueprint_spec.get("workspace_id") or ""),
            project_id=str(blueprint_spec.get("project_id") or ""),
            run_id=str(run["id"]),
            name="team-summary.md",
            content=self._artifact_markdown(root=root, text=final_text),
        )
        artifact = self.store.create_artifact(
            run_id=str(run["id"]),
            step_id=step_id,
            kind="report",
            name="team-summary.md",
            path=str(artifact_path),
            summary=trim_text(final_text, limit=240),
            metadata={"runtime": "deepagents", "root_agent": self._executor(root).get("runtime_key")},
        )
        runtime_state.setdefault("history", []).append(
            {
                "node_id": "deepagents_orchestrate",
                "summary": final_text,
                "completed_at": utcnow_iso(),
            }
        )
        runtime_state["delivery_artifact_id"] = artifact["id"]
        runtime_state["result_text"] = final_text
        self.store.update_step(
            step_id,
            status="done",
            output_payload={
                "summary": final_text,
                "artifact_id": artifact["id"],
                "artifact_path": artifact["path"],
            },
            finished=True,
        )
        self.store.add_event(
            run_id=str(run["id"]),
            step_id=step_id,
            event_type=event_types.ARTIFACT_CREATED,
            payload={"artifact_id": artifact["id"], "path": artifact["path"], "name": artifact["name"]},
        )
        self.store.add_event(
            run_id=str(run["id"]),
            step_id=step_id,
            event_type=event_types.STEP_COMPLETED,
            payload={"node_id": "deepagents_orchestrate", "node_type": "agent", "output": {"summary": final_text}},
        )
        self.store.update_run(
            str(run["id"]),
            status="completed",
            summary=trim_text(final_text, limit=400),
            current_node_id="deepagents_orchestrate",
            state=runtime_state,
            result={"summary": final_text, "artifact_id": artifact["id"]},
            finished_at=utcnow_iso(),
        )
        self.store.update_task_release(str(run["task_release_id"]), status="completed")
        self.store.add_event(run_id=str(run["id"]), event_type=event_types.RUN_COMPLETED, payload={"summary": final_text})

    def _resume_payload(self, *, waiting: dict[str, Any], resolution: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(resolution.get("metadata") or {})
        explicit = metadata.get("decisions")
        if isinstance(explicit, list) and explicit:
            return {"decisions": explicit}
        action_count = max(1, len(list((waiting.get("interrupt_payload") or {}).get("action_requests") or [])))
        approved = bool(resolution.get("approved", False))
        if approved and isinstance(metadata.get("edited_action"), dict) and action_count == 1:
            decision = {"type": "edit", "edited_action": dict(metadata["edited_action"])}
            return {"decisions": [decision]}
        if approved:
            return {"decisions": [{"type": "approve"} for _ in range(action_count)]}
        message = str(resolution.get("comment") or "Human reviewer rejected this action.")
        return {"decisions": [{"type": "reject", "message": message} for _ in range(action_count)]}

    def _interrupt_title(self, *, executor: dict[str, Any], interrupt_payload: dict[str, Any]) -> str:
        action_requests = list(interrupt_payload.get("action_requests") or [])
        if len(action_requests) == 1:
            action = dict(action_requests[0] or {})
            return f"Review `{action.get('name')}` for `{executor.get('name') or executor.get('runtime_key') or 'agent'}`"
        return f"Review {len(action_requests)} actions for `{executor.get('name') or executor.get('runtime_key') or 'agent'}`"

    def _interrupt_detail(self, interrupt_payload: dict[str, Any]) -> str:
        lines: list[str] = []
        for item in list(interrupt_payload.get("action_requests") or []):
            action = dict(item or {})
            description = str(action.get("description") or "").strip()
            if description:
                lines.append(description)
                continue
            lines.append(f"Action: {action.get('name')}")
            lines.append(pretty_json(dict(action.get("args") or {})))
        return "\n\n".join(lines) or pretty_json(interrupt_payload)

    def _interrupt_on(self, executor: dict[str, Any]) -> dict[str, Any] | None:
        config: dict[str, Any] = {}
        review_policies = [dict(item) for item in list(executor.get("review_policies") or []) if isinstance(item, dict)]
        if not review_policies:
            return None
        if self._has_review_trigger(review_policies, {"before_agent_to_agent_message", "before_handoff_to_lower_level", "before_agent_receive_task"}):
            config["task"] = {
                "allowed_decisions": ["approve", "reject", "edit"],
                "description": self._task_review_description(executor),
            }
        if self._has_review_trigger(review_policies, {"before_memory_write"}):
            config["memory_remember"] = {
                "allowed_decisions": ["approve", "reject", "edit"],
                "description": self._memory_review_description(executor),
            }
        for plugin in list(executor.get("plugins") or []):
            if not self._plugin_requires_review(review_policies, dict(plugin)):
                continue
            manifest = dict(plugin.get("manifest_json") or plugin.get("manifest") or {})
            for action in list(manifest.get("actions") or []):
                action_name = str((action or {}).get("name") or "").strip()
                if not action_name:
                    continue
                tool_name = f"plugin_{str(plugin.get('key') or 'plugin').replace('.', '_')}_{action_name}"
                config[tool_name] = {
                    "allowed_decisions": ["approve", "reject", "edit"],
                    "description": self._plugin_review_description(executor, dict(plugin), action_name),
                }
        return config or None

    def _has_review_trigger(self, review_policies: list[dict[str, Any]], triggers: set[str]) -> bool:
        for policy in review_policies:
            config = dict(policy.get("config") or {})
            policy_triggers = {str(item).strip() for item in list(config.get("triggers") or []) if str(item).strip()}
            if policy_triggers.intersection(triggers):
                return True
        return False

    def _plugin_requires_review(self, review_policies: list[dict[str, Any]], plugin: dict[str, Any]) -> bool:
        manifest = dict(plugin.get("manifest_json") or plugin.get("manifest") or {})
        plugin_key = str(plugin.get("key") or "")
        permissions = {str(item).strip() for item in list(manifest.get("permissions") or []) if str(item).strip()}
        for policy in review_policies:
            config = dict(policy.get("config") or {})
            policy_triggers = {str(item).strip() for item in list(config.get("triggers") or []) if str(item).strip()}
            if not policy_triggers.intersection({"before_tool_call", "before_external_side_effect"}):
                continue
            conditions = dict(config.get("conditions") or {})
            plugin_keys = {str(item).strip() for item in list(conditions.get("plugin_keys") or []) if str(item).strip()}
            risk_tags = {str(item).strip() for item in list(conditions.get("risk_tags") or []) if str(item).strip()}
            if plugin_keys and plugin_key not in plugin_keys:
                continue
            if risk_tags and not permissions.intersection(risk_tags):
                continue
            return True
        return False

    def _should_review_final_delivery(self, executor: dict[str, Any]) -> bool:
        return self._has_review_trigger(
            [dict(item) for item in list(executor.get("review_policies") or []) if isinstance(item, dict)],
            {"before_final_delivery", "final_delivery"},
        )

    def _task_review_description(self, executor: dict[str, Any]):
        def _describe(tool_call: dict[str, Any], _state: Any, _runtime: Any) -> str:
            args = dict(tool_call.get("args") or {})
            return (
                f"Agent `{executor.get('name') or executor.get('runtime_key')}` is delegating work.\n\n"
                f"Subagent: {args.get('subagent_type')}\n"
                f"Task:\n{args.get('description') or ''}"
            )

        return _describe

    def _memory_review_description(self, executor: dict[str, Any]):
        def _describe(tool_call: dict[str, Any], _state: Any, _runtime: Any) -> str:
            args = dict(tool_call.get("args") or {})
            return (
                f"Agent `{executor.get('name') or executor.get('runtime_key')}` wants to write memory.\n\n"
                f"Scope: {args.get('scope') or 'agent'}\n"
                f"Text:\n{args.get('text') or ''}"
            )

        return _describe

    def _plugin_review_description(self, executor: dict[str, Any], plugin: dict[str, Any], action_name: str):
        def _describe(tool_call: dict[str, Any], _state: Any, _runtime: Any) -> str:
            args = dict(tool_call.get("args") or {})
            return (
                f"Agent `{executor.get('name') or executor.get('runtime_key')}` wants to invoke plugin action.\n\n"
                f"Plugin: {plugin.get('key')}\n"
                f"Action: {action_name}\n"
                f"Arguments:\n{pretty_json(args)}"
            )

        return _describe

    def _final_text(self, result: Any) -> str:
        if isinstance(result, dict):
            messages = list(result.get("messages") or [])
            for message in reversed(messages):
                text = getattr(message, "text", None)
                if isinstance(text, str) and text.strip():
                    return text.strip()
                content = getattr(message, "content", "")
                if isinstance(content, str) and content.strip():
                    return content.strip()
        return trim_text(str(result), limit=4000) or "DeepAgents team completed."

    def _artifact_markdown(self, *, root: dict[str, Any], text: str) -> str:
        lines = [
            "# Team Summary",
            "",
            f"Root team: {root.get('name') or root.get('key')}",
            "",
            text or "No summary.",
        ]
        return "\n".join(lines)
