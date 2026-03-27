from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from aiteams.agent.kernel import AgentKernel, AgentRunContext
from aiteams.common import events as event_types
from aiteams.common.expressions import evaluate_expression
from aiteams.deepagents.runtime import DeepAgentsTeamRuntime
from aiteams.domain.models import BlueprintSpec, NodeSpec
from aiteams.langgraph.team_runtime import LangGraphTeamRuntime
from aiteams.runtime.compiler import BlueprintCompiler, CompiledBlueprint
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import pretty_json, render_template, resolve_path, trim_text, utcnow_iso
from aiteams.workspace.manager import WorkspaceManager


class RuntimeGraphState(TypedDict):
    run_id: str


@dataclass(slots=True)
class NodeExecutionResult:
    status: str
    next_node_id: str | None = None
    merge_id: str | None = None


class RuntimeEngine:
    def __init__(
        self,
        *,
        store: MetadataStore,
        compiler: BlueprintCompiler,
        agent_kernel: AgentKernel,
        workspace: WorkspaceManager,
        checkpoint_db_path: str | Path,
    ):
        self.store = store
        self.compiler = compiler
        self.agent_kernel = agent_kernel
        self.workspace = workspace
        self.checkpoint_db_path = Path(checkpoint_db_path).expanduser().resolve()
        self.checkpoint_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.team_runtime = LangGraphTeamRuntime(
            store=store,
            agent_kernel=agent_kernel,
            workspace=workspace,
            checkpoint_db_path=self.checkpoint_db_path,
        )
        self.deep_team_runtime = DeepAgentsTeamRuntime(
            store=store,
            agent_kernel=agent_kernel,
            workspace=workspace,
            checkpoint_db_path=self.checkpoint_db_path,
        )

    async def start_task(
        self,
        *,
        blueprint: dict[str, Any],
        title: str | None,
        prompt: str,
        inputs: dict[str, Any],
        approval_mode: str,
    ) -> dict[str, Any]:
        blueprint_spec = blueprint["spec_json"] if "spec_json" in blueprint else blueprint
        spec = BlueprintSpec.from_dict(blueprint_spec)
        team_runtime = self._team_runtime_for_blueprint(blueprint_spec)
        is_team_runtime = team_runtime is not None
        compiled = None if is_team_runtime else self.compiler.compile(spec)
        task = self.store.create_task_release(
            blueprint_id=str(blueprint["id"]) if "id" in blueprint else "",
            workspace_id=spec.workspace_id,
            project_id=spec.project_id,
            title=title,
            prompt=prompt,
            inputs=inputs,
            approval_mode=approval_mode,
        )
        if is_team_runtime:
            state = team_runtime.initial_state(
                blueprint_spec,
                title=title,
                prompt=prompt,
                inputs=inputs,
                approval_mode=approval_mode,
            )
        else:
            state = {
                "task": {
                    "title": title,
                    "prompt": prompt,
                    "inputs": inputs,
                    "approval_mode": approval_mode,
                },
                "outputs": {},
                "loops": {},
                "waiting": None,
                "history": [],
            }
        run = self.store.create_run(
            task_release_id=str(task["id"]),
            blueprint_id=str(blueprint["id"]),
            workspace_id=spec.workspace_id,
            project_id=spec.project_id,
            state=state,
        )
        thread = None
        if is_team_runtime:
            thread = team_runtime.ensure_task_thread(
                run_id=str(run["id"]),
                blueprint_spec=blueprint_spec,
                workspace_id=spec.workspace_id,
                project_id=spec.project_id,
                title=title,
                prompt=prompt,
            )
            if thread is not None:
                state["thread_id"] = thread["id"]
        started_at = utcnow_iso()
        self.store.update_task_release(str(task["id"]), status="running")
        self.store.update_run(
            str(run["id"]),
            status="running",
            current_node_id=self._team_runtime_start_node(team_runtime) if is_team_runtime else compiled.start_node_id,
            state=state,
            started_at=started_at,
        )
        self.store.add_event(
            run_id=str(run["id"]),
            event_type=event_types.RUN_CREATED,
            payload={"run_id": run["id"], "task_release_id": task["id"]},
        )
        if is_team_runtime:
            await team_runtime.start_run(str(run["id"]))
        else:
            await self._invoke_graph({"run_id": str(run["id"])}, run_id=str(run["id"]))
        bundle = self.store.get_run_bundle(str(run["id"])) or {}
        if thread is not None:
            bundle["task_thread"] = thread
        return bundle

    async def resume_run(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise ValueError("Run does not exist.")
        bundle = self.store.get_run_bundle(run_id)
        blueprint = (bundle or {}).get("blueprint")
        team_runtime = self._team_runtime_for_blueprint((blueprint or {}).get("spec_json") or {})
        if blueprint is not None and team_runtime is not None:
            runtime_state = dict(run.get("state_json") or {})
            waiting = runtime_state.get("waiting")
            if waiting:
                approval = self.store.get_approval(str(waiting["approval_id"]))
                if approval is None or approval["status"] == "pending":
                    bundle = self.store.get_run_bundle(run_id) or {}
                    threads = self.store.list_task_threads(run_id=run_id)
                    if threads:
                        bundle["task_thread"] = threads[0]
                    return bundle
                resolution = approval.get("resolution_json") or {}
                await team_runtime.resume_run(run_id, resolution)
            else:
                await team_runtime.resume_run(run_id)
            bundle = self.store.get_run_bundle(run_id) or {}
            threads = self.store.list_task_threads(run_id=run_id)
            if threads:
                bundle["task_thread"] = threads[0]
            return bundle
        runtime_state = dict(run.get("state_json") or {})
        waiting = runtime_state.get("waiting")
        if waiting:
            approval = self.store.get_approval(str(waiting["approval_id"]))
            if approval is None or approval["status"] == "pending":
                return self.store.get_run_bundle(run_id) or {}
            resolution = approval.get("resolution_json") or {}
            try:
                await self._invoke_graph(Command(resume=resolution), run_id=run_id)
            except Exception:
                # Fallback for cases where the process lost in-memory checkpoint state.
                runtime_state["resume_resolution"] = resolution
                self.store.update_run(
                    run_id,
                    status=run.get("status"),
                    current_node_id=run.get("current_node_id"),
                    state=runtime_state,
                    started_at=run.get("started_at"),
                    finished_at=run.get("finished_at"),
                )
                await self._invoke_graph({"run_id": run_id}, run_id=run_id)
            return self.store.get_run_bundle(run_id) or {}
        await self._invoke_graph({"run_id": run_id}, run_id=run_id)
        return self.store.get_run_bundle(run_id) or {}

    async def inject_human_message(
        self,
        *,
        run_id: str,
        target_agent_id: str,
        body: str,
        message_type: str = "dialogue",
        phase: str | None = None,
        metadata: dict[str, Any] | None = None,
        auto_resume: bool = True,
    ) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise ValueError("Run does not exist.")
        bundle = self.store.get_run_bundle(run_id)
        blueprint = (bundle or {}).get("blueprint")
        team_runtime = self._team_runtime_for_blueprint((blueprint or {}).get("spec_json") or {})
        if blueprint is None or team_runtime is None:
            raise ValueError("Human message injection is only supported for team-runtime runs.")
        injected = team_runtime.inject_human_message(
            run_id=run_id,
            target_agent_id=target_agent_id,
            body=body,
            message_type=message_type,
            phase=phase,
            metadata=metadata,
        )
        current = self.store.get_run(run_id)
        if current is not None and str(current.get("status") or "") not in {"waiting_approval", "completed", "failed"} and auto_resume:
            return await self.resume_run(run_id)
        return injected

    def _team_runtime_for_blueprint(self, blueprint_spec: dict[str, Any]) -> LangGraphTeamRuntime | DeepAgentsTeamRuntime | None:
        if self.deep_team_runtime.handles(blueprint_spec):
            return self.deep_team_runtime
        if self.team_runtime.handles(blueprint_spec):
            return self.team_runtime
        return None

    def _team_runtime_start_node(self, runtime: LangGraphTeamRuntime | DeepAgentsTeamRuntime | None) -> str:
        if runtime is self.deep_team_runtime:
            return "deepagents_orchestrate"
        return "task_ingress"

    def _build_graph(self, *, checkpointer: AsyncSqliteSaver):
        builder = StateGraph(RuntimeGraphState)
        builder.add_node("runner", self._graph_runner)
        builder.add_edge(START, "runner")
        return builder.compile(checkpointer=checkpointer)

    async def _invoke_graph(self, payload: Any, *, run_id: str) -> None:
        async with AsyncSqliteSaver.from_conn_string(str(self.checkpoint_db_path)) as checkpointer:
            graph = self._build_graph(checkpointer=checkpointer)
            await graph.ainvoke(payload, self._graph_config(run_id))

    def _graph_config(self, run_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": run_id}}

    def storage_info(self) -> dict[str, Any]:
        return {
            "checkpoint_driver": "sqlite",
            "checkpoint_path": str(self.checkpoint_db_path),
            "checkpoint_runtime": "langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver",
        }

    async def _graph_runner(self, state: RuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        run = self.store.get_run(run_id)
        if run is None:
            return Command(update={"run_id": run_id}, goto=END)
        compiled = self._compiled_for_run(run_id)
        runtime_state = dict(run.get("state_json") or {})
        if runtime_state.get("waiting"):
            return await self._handle_waiting_approval(run_id, compiled, runtime_state)
        current_node_id = run.get("current_node_id") or compiled.start_node_id
        self.store.update_run(
            run_id,
            status="running",
            current_node_id=current_node_id,
            state=runtime_state,
            started_at=run.get("started_at") or utcnow_iso(),
        )
        result = await self._execute_node(run_id, compiled, runtime_state, current_node_id)
        if result.status == "waiting_approval":
            refreshed = self.store.get_run(run_id)
            assert refreshed is not None
            return await self._handle_waiting_approval(run_id, compiled, dict(refreshed.get("state_json") or {}))
        if result.next_node_id:
            return Command(update={"run_id": run_id}, goto="runner")
        return self._finalize_run(run_id, compiled, runtime_state)

    async def _handle_waiting_approval(
        self,
        run_id: str,
        compiled: CompiledBlueprint,
        runtime_state: dict[str, Any],
    ) -> Command:
        waiting = dict(runtime_state.get("waiting") or {})
        approval = self.store.get_approval(str(waiting["approval_id"]))
        if approval is None:
            raise RuntimeError("Approval does not exist.")
        payload = {
            "approval_id": approval["id"],
            "node_id": waiting.get("node_id"),
            "title": approval["title"],
            "detail": approval["detail"],
        }
        resolution = runtime_state.pop("resume_resolution", None)
        if resolution is None:
            resolution = interrupt(payload)
        approval = self.store.get_approval(str(waiting["approval_id"]))
        if approval is None or approval["status"] == "pending":
            if isinstance(resolution, dict) and "approved" in resolution:
                approval = self.store.resolve_approval(
                    str(waiting["approval_id"]),
                    approved=bool(resolution.get("approved", True)),
                    comment=str(resolution.get("comment") or ""),
                    metadata=dict(resolution.get("metadata") or {}),
                )
            else:
                raise RuntimeError("Approval is still pending.")
        assert approval is not None
        pending_step = self.store.latest_step_for_node(run_id, str(waiting["node_id"]))
        if approval["status"] == "rejected":
            if pending_step is not None and pending_step["status"] == "blocked":
                self.store.update_step(
                    str(pending_step["id"]),
                    status="error",
                    output_payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
                    error_text="Approval rejected.",
                    finished=True,
                )
            self.store.add_event(
                run_id=run_id,
                step_id=pending_step["id"] if pending_step else None,
                event_type=event_types.APPROVAL_RESOLVED,
                payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
            )
            self.store.add_event(run_id=run_id, event_type=event_types.RUN_FAILED, payload={"reason": "approval_rejected"})
            current_run = self.store.get_run(run_id)
            if current_run is not None:
                self.store.update_task_release(str(current_run["task_release_id"]), status="failed")
            self.store.update_run(
                run_id,
                status="failed",
                summary="Approval rejected.",
                current_node_id=str(waiting["node_id"]),
                state={**runtime_state, "waiting": None},
                finished_at=utcnow_iso(),
            )
            return Command(update={"run_id": run_id}, goto=END)
        if pending_step is not None and pending_step["status"] == "blocked":
            self.store.update_step(
                str(pending_step["id"]),
                status="done",
                output_payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
                finished=True,
            )
        runtime_state.setdefault("outputs", {})[str(waiting["node_id"])] = {
            "approval_id": approval["id"],
            "status": approval["status"],
            "resolution": approval["resolution_json"],
        }
        runtime_state["waiting"] = None
        next_node_id = str(waiting["next_node_id"])
        self.store.add_event(
            run_id=run_id,
            step_id=pending_step["id"] if pending_step else None,
            event_type=event_types.APPROVAL_RESOLVED,
            payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
        )
        self.store.update_run(
            run_id,
            status="running",
            current_node_id=next_node_id,
            state=runtime_state,
            started_at=(self.store.get_run(run_id) or {}).get("started_at") or utcnow_iso(),
        )
        self.store.save_checkpoint(
            run_id=run_id,
            step_id=str(pending_step["id"]) if pending_step else None,
            node_id=str(waiting["node_id"]),
            snapshot=runtime_state,
        )
        return Command(update={"run_id": run_id}, goto="runner")

    def _compiled_for_run(self, run_id: str) -> CompiledBlueprint:
        bundle = self.store.get_run_bundle(run_id)
        if bundle is None or bundle.get("blueprint") is None:
            raise ValueError("Run blueprint does not exist.")
        blueprint = bundle["blueprint"]
        return self.compiler.compile(blueprint["spec_json"])

    def _finalize_run(self, run_id: str, compiled: CompiledBlueprint, state: dict[str, Any]) -> Command:
        run = self.store.get_run(run_id)
        if run is None:
            return Command(update={"run_id": run_id}, goto=END)
        summary = self._resolve_run_summary(state)
        if not self._acceptance_checks_pass(compiled.blueprint, state):
            self.store.update_run(
                run_id,
                status="failed",
                summary=summary,
                state=state,
                result=state.get("outputs", {}),
                finished_at=utcnow_iso(),
            )
            self.store.update_task_release(str(run["task_release_id"]), status="failed")
            self.store.add_event(run_id=run_id, event_type=event_types.RUN_FAILED, payload={"summary": summary})
            return Command(update={"run_id": run_id}, goto=END)
        self.store.update_run(
            run_id,
            status="completed",
            summary=summary,
            state=state,
            result=state.get("outputs", {}),
            finished_at=utcnow_iso(),
        )
        self.store.update_task_release(str(run["task_release_id"]), status="completed")
        self.store.add_event(run_id=run_id, event_type=event_types.RUN_COMPLETED, payload={"summary": summary})
        return Command(update={"run_id": run_id}, goto=END)

    async def _execute_node(
        self,
        run_id: str,
        compiled: CompiledBlueprint,
        state: dict[str, Any],
        node_id: str,
        *,
        stop_before_merge: bool = False,
    ) -> NodeExecutionResult:
        node = compiled.nodes[node_id]
        if stop_before_merge and node.type == "merge":
            return NodeExecutionResult(status="merge_boundary", merge_id=node.id, next_node_id=node.id)

        attempt = self.store.next_step_attempt(run_id, node.id)
        step = self.store.create_step(
            run_id=run_id,
            node_id=node.id,
            node_type=node.type,
            status="running",
            attempt=attempt,
            input_payload={"state": state, "node": node.to_dict()},
        )
        self.store.add_event(
            run_id=run_id,
            step_id=str(step["id"]),
            event_type=event_types.STEP_STARTED,
            payload={"node_id": node.id, "node_type": node.type, "attempt": attempt},
        )

        try:
            next_node_id: str | None
            output: dict[str, Any]
            if node.type == "start":
                output = {"status": "started"}
                next_node_id = compiled.single_next(node.id)
            elif node.type == "agent":
                output = await self._run_agent_node(run_id, compiled, node, state)
                self.store.add_event(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    event_type=event_types.AGENT_MESSAGE,
                    payload={"node_id": node.id, "summary": output.get("summary", "")},
                )
                for plugin_result in output.get("plugin_results", []):
                    self.store.add_event(
                        run_id=run_id,
                        step_id=str(step["id"]),
                        event_type=event_types.PLUGIN_INVOKED,
                        payload={
                            "node_id": node.id,
                            "plugin_id": plugin_result.get("plugin_id"),
                            "plugin_key": plugin_result.get("plugin_key"),
                            "action": plugin_result.get("action"),
                            "result": plugin_result.get("result"),
                        },
                    )
                next_node_id = compiled.single_next(node.id)
            elif node.type == "condition":
                result = bool(evaluate_expression(node.expr or "False", self._node_expression_context(compiled, node.id, state)))
                selected = self._select_condition_target(compiled, node.id, result)
                output = {"expr": node.expr, "result": result, "selected": selected}
                next_node_id = selected
            elif node.type == "router":
                selected = self._select_router_target(compiled, node, state)
                output = {"selected": selected}
                next_node_id = selected
            elif node.type == "loop":
                loops = state.setdefault("loops", {})
                loops[node.id] = int(loops.get(node.id, 0)) + 1
                if node.max_iterations and loops[node.id] > node.max_iterations:
                    raise RuntimeError(f"Loop `{node.id}` exceeded max_iterations={node.max_iterations}")
                output = {"iteration": loops[node.id], "max_iterations": node.max_iterations}
                next_node_id = compiled.single_next(node.id)
            elif node.type == "approval":
                approval_mode = str((state.get("task") or {}).get("approval_mode") or "auto")
                if approval_mode == "auto" or node.auto_approve:
                    approval = self.store.create_approval(
                        run_id=run_id,
                        step_id=str(step["id"]),
                        node_id=node.id,
                        title=node.name or "自动审批",
                        detail=trim_text(pretty_json(self._visible_outputs(compiled, node.id, state)), limit=1000),
                    )
                    approval = self.store.resolve_approval(str(approval["id"]), approved=True, comment="Auto-approved by runtime.")
                    assert approval is not None
                    output = {"approval_id": approval["id"], "status": approval["status"], "resolution": approval["resolution_json"]}
                    self.store.add_event(run_id=run_id, step_id=str(step["id"]), event_type=event_types.APPROVAL_RESOLVED, payload=output)
                    next_node_id = compiled.single_next(node.id)
                else:
                    approval = self.store.create_approval(
                        run_id=run_id,
                        step_id=str(step["id"]),
                        node_id=node.id,
                        title=node.name or "Manual approval",
                        detail=trim_text(pretty_json(self._visible_outputs(compiled, node.id, state)), limit=1400),
                    )
                    state["waiting"] = {"approval_id": approval["id"], "node_id": node.id, "next_node_id": compiled.single_next(node.id)}
                    self.store.update_step(str(step["id"]), status="blocked", output_payload={"approval_id": approval["id"]}, finished=False)
                    self.store.update_run(run_id, status="waiting_approval", current_node_id=node.id, state=state)
                    self.store.save_checkpoint(run_id=run_id, step_id=str(step["id"]), node_id=node.id, snapshot=state)
                    self.store.add_event(
                        run_id=run_id,
                        step_id=str(step["id"]),
                        event_type=event_types.APPROVAL_REQUESTED,
                        payload={"approval_id": approval["id"], "title": approval["title"]},
                    )
                    self.store.add_event(run_id=run_id, event_type=event_types.RUN_PAUSED, payload={"reason": "waiting_approval", "approval_id": approval["id"]})
                    return NodeExecutionResult(status="waiting_approval", next_node_id=None)
            elif node.type == "artifact":
                output = self._write_artifact(run_id, compiled, node, state, str(step["id"]))
                next_node_id = compiled.single_next(node.id)
            elif node.type == "parallel":
                branch_ids = compiled.next_nodes(node.id)
                branch_results = await asyncio.gather(*(self._execute_branch(run_id, compiled, state, branch_id) for branch_id in branch_ids))
                merge_targets = {result["merge_id"] for result in branch_results}
                if len(merge_targets) != 1:
                    raise RuntimeError(f"Parallel node `{node.id}` requires all branches to converge on one merge node.")
                merge_id = next(iter(merge_targets))
                for branch in branch_results:
                    state["outputs"].update(branch["outputs"])
                output = {"branch_count": len(branch_results), "merge_id": merge_id, "branches": branch_results}
                next_node_id = merge_id
            elif node.type == "merge":
                predecessors = [edge.source for edge in compiled.incoming.get(node.id, [])]
                merged = {source: state.get("outputs", {}).get(source) for source in predecessors if source in state.get("outputs", {})}
                output = {
                    "branch_count": len(merged),
                    "summary": " | ".join(
                        trim_text((value or {}).get("summary"), limit=120) for value in merged.values() if isinstance(value, dict)
                    ),
                    "branches": merged,
                }
                next_node_id = compiled.single_next(node.id)
            elif node.type == "subflow":
                subflow = node.config.get("flow")
                if not isinstance(subflow, dict):
                    raise RuntimeError(f"Subflow node `{node.id}` requires config.flow.")
                sub_blueprint = deepcopy(compiled.blueprint.to_dict())
                sub_blueprint["flow"] = subflow
                sub_compiled = self.compiler.compile(sub_blueprint)
                sub_state = deepcopy(state)
                await self._execute_subflow(run_id, sub_compiled, sub_state)
                output = {"summary": self._resolve_run_summary(sub_state), "outputs": sub_state.get("outputs", {})}
                next_node_id = compiled.single_next(node.id)
            elif node.type == "end":
                output = {"status": "completed", "definition_of_done": self._definition_of_done(compiled.blueprint, state)}
                next_node_id = None
            else:
                raise RuntimeError(f"Unsupported node type: {node.type}")

            state.setdefault("outputs", {})[node.id] = output
            state.setdefault("history", []).append({"node_id": node.id, "node_type": node.type, "output": output})
            if node.type == "agent":
                self._record_agent_handoffs(run_id=run_id, compiled=compiled, node=node, next_node_id=next_node_id, output=output)
            self.store.update_step(str(step["id"]), status="done", output_payload=output, finished=True)
            self.store.update_run(run_id, status="running", current_node_id=next_node_id, state=state)
            self.store.save_checkpoint(run_id=run_id, step_id=str(step["id"]), node_id=node.id, snapshot=state)
            self.store.add_event(
                run_id=run_id,
                step_id=str(step["id"]),
                event_type=event_types.STEP_COMPLETED,
                payload={"node_id": node.id, "node_type": node.type, "output": output},
            )
            return NodeExecutionResult(status="ok", next_node_id=next_node_id)
        except Exception as exc:
            self.store.update_step(str(step["id"]), status="error", error_text=str(exc), output_payload={"error": str(exc)}, finished=True)
            self.store.update_run(run_id, status="failed", summary=str(exc), state=state, finished_at=utcnow_iso())
            self.store.add_event(run_id=run_id, step_id=str(step["id"]), event_type=event_types.STEP_FAILED, payload={"node_id": node.id, "error": str(exc)})
            self.store.add_event(run_id=run_id, event_type=event_types.RUN_FAILED, payload={"node_id": node.id, "error": str(exc)})
            raise

    async def _run_agent_node(self, run_id: str, compiled: CompiledBlueprint, node: NodeSpec, state: dict[str, Any]) -> dict[str, Any]:
        task = state.get("task") or {}
        blueprint = compiled.blueprint
        agent = blueprint.agents[str(node.agent)]
        visible_outputs = self._visible_outputs(compiled, node.id, state)
        context = AgentRunContext(
            workspace_id=blueprint.workspace_id,
            project_id=blueprint.project_id,
            run_id=run_id,
            prompt=str(task.get("prompt") or ""),
            inputs=dict(task.get("inputs") or {}),
            outputs=deepcopy(visible_outputs),
            loops=dict(state.get("loops") or {}),
            current_node_id=node.id,
        )
        return await self.agent_kernel.execute(agent, node, context)

    def _write_artifact(self, run_id: str, compiled: CompiledBlueprint, node: NodeSpec, state: dict[str, Any], step_id: str) -> dict[str, Any]:
        blueprint = compiled.blueprint
        context = self._node_expression_context(compiled, node.id, state)
        if node.template:
            content = render_template(node.template, context)
        else:
            value = resolve_path(context, str(node.source or ""))
            content = pretty_json(value) if isinstance(value, (dict, list)) else str(value or "")
        target = self.workspace.write_artifact(
            workspace_id=blueprint.workspace_id,
            project_id=blueprint.project_id,
            run_id=run_id,
            name=node.name or f"{node.id}.md",
            content=content,
        )
        artifact = self.store.create_artifact(
            run_id=run_id,
            step_id=step_id,
            kind=node.artifact_kind,
            name=node.name or target.name,
            path=str(target),
            summary=trim_text(content, limit=180),
            metadata={"node_id": node.id},
        )
        self.store.add_event(
            run_id=run_id,
            step_id=step_id,
            event_type=event_types.ARTIFACT_CREATED,
            payload={"artifact_id": artifact["id"], "path": artifact["path"], "name": artifact["name"]},
        )
        return {"artifact_id": artifact["id"], "path": artifact["path"], "name": artifact["name"], "kind": artifact["kind"]}

    async def _execute_branch(self, run_id: str, compiled: CompiledBlueprint, state: dict[str, Any], start_node_id: str) -> dict[str, Any]:
        branch_state = deepcopy(state)
        node_id = start_node_id
        while node_id:
            result = await self._execute_node(run_id, compiled, branch_state, node_id, stop_before_merge=True)
            if result.status == "merge_boundary":
                return {
                    "branch_start": start_node_id,
                    "merge_id": result.merge_id,
                    "outputs": branch_state.get("outputs", {}),
                }
            if result.status == "waiting_approval":
                raise RuntimeError("Parallel branch cannot pause for approval.")
            node_id = result.next_node_id
        raise RuntimeError(f"Parallel branch `{start_node_id}` did not converge to a merge node.")

    async def _execute_subflow(self, run_id: str, compiled: CompiledBlueprint, state: dict[str, Any]) -> None:
        node_id = compiled.start_node_id
        while node_id:
            result = await self._execute_node(run_id, compiled, state, node_id)
            if result.status == "waiting_approval":
                raise RuntimeError("Subflow cannot pause for approval.")
            node_id = result.next_node_id

    def _select_condition_target(self, compiled: CompiledBlueprint, node_id: str, result: bool) -> str | None:
        for edge in compiled.next_edges(node_id):
            if edge.when is None:
                continue
            normalized = str(edge.when).strip().lower()
            if result and normalized == "true":
                return edge.target
            if not result and normalized == "false":
                return edge.target
        fallback = [edge.target for edge in compiled.next_edges(node_id) if edge.when in (None, "default")]
        return fallback[0] if fallback else None

    def _select_router_target(self, compiled: CompiledBlueprint, node: NodeSpec, state: dict[str, Any]) -> str | None:
        context = self._node_expression_context(compiled, node.id, state)
        for edge in compiled.next_edges(node.id):
            if edge.when in (None, "", "default"):
                continue
            if evaluate_expression(str(edge.when), context):
                return edge.target
        for edge in compiled.next_edges(node.id):
            if edge.when in (None, "", "default"):
                return edge.target
        return None

    def _visible_outputs(self, compiled: CompiledBlueprint, node_id: str, state: dict[str, Any]) -> dict[str, Any]:
        outputs = state.get("outputs") or {}
        visible: dict[str, Any] = {}
        for ancestor_id in compiled.visible_ancestors(node_id):
            if ancestor_id in outputs:
                visible[ancestor_id] = outputs[ancestor_id]
        return visible

    def _node_expression_context(self, compiled: CompiledBlueprint, node_id: str, state: dict[str, Any]) -> dict[str, Any]:
        context = {"task": state.get("task") or {}, "loops": state.get("loops") or {}}
        context.update(self._visible_outputs(compiled, node_id, state))
        return context

    def _expression_context(self, state: dict[str, Any]) -> dict[str, Any]:
        context = {"task": state.get("task") or {}, "loops": state.get("loops") or {}}
        context.update(state.get("outputs") or {})
        return context

    def _definition_of_done(self, blueprint: BlueprintSpec, state: dict[str, Any]) -> list[dict[str, Any]]:
        checks: list[dict[str, Any]] = []
        for expr in blueprint.definition_of_done:
            try:
                checks.append({"expr": expr, "passed": bool(evaluate_expression(expr, self._expression_context(state)))})
            except Exception:
                checks.append({"expr": expr, "passed": False})
        return checks

    def _acceptance_checks_pass(self, blueprint: BlueprintSpec, state: dict[str, Any]) -> bool:
        for expr in blueprint.acceptance_checks:
            if not bool(evaluate_expression(expr, self._expression_context(state))):
                return False
        return True

    def _resolve_run_summary(self, state: dict[str, Any]) -> str:
        outputs = list((state.get("outputs") or {}).values())
        for item in reversed(outputs):
            if isinstance(item, dict) and item.get("summary"):
                return str(item["summary"])
        return "Run completed."

    def _record_agent_handoffs(
        self,
        *,
        run_id: str,
        compiled: CompiledBlueprint,
        node: NodeSpec,
        next_node_id: str | None,
        output: dict[str, Any],
    ) -> None:
        if not node.agent or not next_node_id:
            return
        metadata = dict(compiled.blueprint.metadata or {})
        adjacency = dict(metadata.get("adjacency") or {})
        targets: list[str] = []
        next_node = compiled.nodes.get(next_node_id)
        if next_node and next_node.type == "agent" and next_node.agent:
            targets = [str(next_node.agent)]
        elif next_node and next_node.type == "parallel":
            for branch_id in compiled.next_nodes(next_node_id):
                branch_node = compiled.nodes.get(branch_id)
                if branch_node and branch_node.type == "agent" and branch_node.agent:
                    targets.append(str(branch_node.agent))
        for target_agent_id in sorted(set(targets)):
            allowed_targets = [str(item) for item in adjacency.get(str(node.agent), [])]
            self.store.add_message_event(
                run_id=run_id,
                thread_id=None,
                source_agent_id=str(node.agent),
                target_agent_id=target_agent_id,
                message_type="handoff",
                payload={
                    "node_id": node.id,
                    "next_node_id": next_node_id,
                    "summary": output.get("summary"),
                    "allowed_targets": allowed_targets,
                },
                status="delivered" if (not allowed_targets or target_agent_id in allowed_targets) else "blocked",
            )
