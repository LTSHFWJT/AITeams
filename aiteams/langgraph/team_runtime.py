from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, TypedDict

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.errors import GraphInterrupt
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from aiteams.agent.kernel import AgentKernel, AgentRunContext
from aiteams.common import events as event_types
from aiteams.domain.models import BlueprintSpec, NodeSpec
from aiteams.memory.scope import MemoryScopes
from aiteams.plugins.manager import (
    BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY,
    BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY,
    BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY,
)
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import make_id, pretty_json, trim_text, utcnow_iso
from aiteams.workspace.manager import WorkspaceManager


class TeamRuntimeGraphState(TypedDict):
    run_id: str


class LangGraphTeamRuntime:
    def __init__(self, *, store: MetadataStore, agent_kernel: AgentKernel, workspace: WorkspaceManager, checkpoint_db_path: str | Path):
        self.store = store
        self.agent_kernel = agent_kernel
        self.workspace = workspace
        self.checkpoint_db_path = Path(checkpoint_db_path).expanduser().resolve()
        self.checkpoint_db_path.parent.mkdir(parents=True, exist_ok=True)

    def handles(self, blueprint_spec: dict[str, Any]) -> bool:
        metadata = dict(blueprint_spec.get("metadata") or {})
        return str(metadata.get("execution_mode") or "") == "team_event_driven" and isinstance(metadata.get("team_runtime"), dict)

    def initial_state(
        self,
        blueprint_spec: dict[str, Any],
        *,
        title: str | None,
        prompt: str,
        inputs: dict[str, Any],
        approval_mode: str,
    ) -> dict[str, Any]:
        team = deepcopy(dict((blueprint_spec.get("metadata") or {}).get("team_runtime") or {}))
        entry_agent_id = str(team.get("entry_agent_id") or "")
        initial_group_id = make_id("group")
        return {
            "mode": "team_event_driven",
            "task": {
                "title": title,
                "prompt": prompt,
                "inputs": dict(inputs or {}),
                "approval_mode": approval_mode,
            },
            "team": team,
            "queue": [
                {
                    "message_id": make_id("teammsg"),
                    "group_id": initial_group_id,
                    "source_actor_id": "human",
                    "target_agent_id": entry_agent_id,
                    "message_type": "task",
                    "phase": "down",
                    "body": prompt,
                    "context_outputs": {},
                }
            ],
            "groups": {
                initial_group_id: {
                    "group_id": initial_group_id,
                    "phase": "down",
                    "target_level": self._member(team, entry_agent_id).get("level"),
                    "expected_targets": [entry_agent_id],
                    "completed": [],
                    "results": {},
                }
            },
            "active_message": None,
            "final_delivery_message": None,
            "delivery_emitted": False,
            "agent_outputs": {},
            "pending_memory_effects": [],
            "history": [],
            "waiting": None,
            "thread_id": None,
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
    ) -> dict[str, Any] | None:
        if not self.handles(blueprint_spec):
            return None
        existing = self.store.list_task_threads(run_id=run_id)
        if existing:
            return existing[0]
        team = dict((blueprint_spec.get("metadata") or {}).get("team_runtime") or {})
        thread = self.store.create_task_thread(
            team_definition_id=str(team.get("team_definition_id") or "") or None,
            run_id=run_id,
            workspace_id=workspace_id,
            project_id=project_id,
            title=title or prompt[:80],
            metadata={
                "team_definition_id": team.get("team_definition_id"),
                "team_definition_key": team.get("team_definition_key"),
                "adjacency": dict(team.get("adjacency") or {}),
                "ordered_levels": list(team.get("ordered_levels") or []),
            },
        )
        return thread

    async def start_run(self, run_id: str) -> None:
        async with AsyncSqliteSaver.from_conn_string(str(self.checkpoint_db_path)) as checkpointer:
            graph = self._build_graph(checkpointer=checkpointer)
            await graph.ainvoke({"run_id": run_id}, self._graph_config(run_id))

    async def resume_run(self, run_id: str, resolution: dict[str, Any] | None = None) -> None:
        payload: Any
        if resolution is None:
            payload = {"run_id": run_id}
        else:
            payload = Command(resume=resolution)
        async with AsyncSqliteSaver.from_conn_string(str(self.checkpoint_db_path)) as checkpointer:
            graph = self._build_graph(checkpointer=checkpointer)
            await graph.ainvoke(payload, self._graph_config(run_id))

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
        run, runtime_state, _blueprint = self._bundle(run_id)
        if str(run.get("status") or "") in {"completed", "failed"}:
            raise ValueError("Cannot inject messages into a completed or failed run.")
        team = dict(runtime_state.get("team") or {})
        member = self._member(team, target_agent_id)
        message_phase = str(phase or "down").strip() or "down"
        group_id = make_id("group")
        injected = {
            "message_id": make_id("teammsg"),
            "group_id": group_id,
            "source_actor_id": "human",
            "target_agent_id": target_agent_id,
            "message_type": str(message_type or "dialogue").strip() or "dialogue",
            "phase": message_phase,
            "body": body,
            "payload": dict(metadata or {}),
            "context_outputs": {},
        }
        runtime_state.setdefault("queue", []).append(injected)
        runtime_state.setdefault("groups", {})[group_id] = {
            "group_id": group_id,
            "phase": message_phase,
            "target_level": member.get("level"),
            "expected_targets": [target_agent_id],
            "completed": [],
            "results": {},
            "source_level": 16,
            "human_injected": True,
        }
        self.store.update_run(
            run_id,
            status=str(run.get("status") or "running"),
            current_node_id=run.get("current_node_id"),
            state=runtime_state,
            started_at=run.get("started_at"),
            finished_at=run.get("finished_at"),
        )
        self.store.add_message_event(
            run_id=run_id,
            thread_id=str(runtime_state.get("thread_id") or "") or None,
            source_agent_id="human",
            target_agent_id=target_agent_id,
            message_type=str(injected.get("message_type") or "dialogue"),
            payload={
                "summary": body,
                "phase": message_phase,
                "source_level": 16,
                "target_level": member.get("level"),
                "group_id": group_id,
                "metadata": dict(metadata or {}),
            },
            status="queued",
        )
        self.store.add_event(
            run_id=run_id,
            event_type="human.message.injected",
            payload={
                "target_agent_id": target_agent_id,
                "message_type": injected["message_type"],
                "phase": message_phase,
                "group_id": group_id,
            },
        )
        bundle = self.store.get_run_bundle(run_id) or {}
        threads = self.store.list_task_threads(run_id=run_id)
        if threads:
            bundle["task_thread"] = threads[0]
        return bundle

    def _build_graph(self, *, checkpointer: AsyncSqliteSaver):
        builder = StateGraph(TeamRuntimeGraphState)
        builder.add_node("task_ingress", self._task_ingress)
        builder.add_node("dispatch_next", self._dispatch_next)
        builder.add_node("review_gate", self._review_gate)
        builder.add_node("run_agent", self._run_agent)
        builder.add_node("route_message", self._route_message)
        builder.add_node("apply_memory_effects", self._apply_memory_effects)
        builder.add_node("emit_artifacts", self._emit_artifacts)
        builder.add_node("finish_or_wait", self._finish_or_wait)
        builder.add_edge(START, "task_ingress")
        return builder.compile(checkpointer=checkpointer)

    def _graph_config(self, run_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": f"team-runtime:{run_id}"}}

    def _bundle(self, run_id: str) -> tuple[dict[str, Any], dict[str, Any], BlueprintSpec]:
        bundle = self.store.get_run_bundle(run_id)
        if bundle is None or bundle.get("run") is None or bundle.get("blueprint") is None:
            raise ValueError("Run bundle does not exist.")
        run = dict(bundle["run"])
        blueprint = BlueprintSpec.from_dict(bundle["blueprint"]["spec_json"])
        state = dict(run.get("state_json") or {})
        return run, state, blueprint

    def _begin_stage(self, *, run_id: str, node_id: str, node_type: str, input_payload: dict[str, Any]) -> dict[str, Any]:
        attempt = self.store.next_step_attempt(run_id, node_id)
        step = self.store.create_step(
            run_id=run_id,
            node_id=node_id,
            node_type=node_type,
            status="running",
            attempt=attempt,
            input_payload=input_payload,
        )
        self.store.add_event(
            run_id=run_id,
            step_id=str(step["id"]),
            event_type=event_types.STEP_STARTED,
            payload={"node_id": node_id, "node_type": node_type, "attempt": attempt},
        )
        return step

    def _complete_stage(
        self,
        *,
        run_id: str,
        step_id: str,
        node_id: str,
        node_type: str,
        state: dict[str, Any],
        output_payload: dict[str, Any],
        next_node_id: str | None,
    ) -> None:
        state.setdefault("history", []).append({"node_id": node_id, "node_type": node_type, "output": output_payload})
        self.store.update_step(step_id, status="done", output_payload=output_payload, finished=True)
        self.store.update_run(run_id, status="running", current_node_id=next_node_id, state=state)
        self.store.save_checkpoint(run_id=run_id, step_id=step_id, node_id=node_id, snapshot=state)
        self.store.add_event(
            run_id=run_id,
            step_id=step_id,
            event_type=event_types.STEP_COMPLETED,
            payload={"node_id": node_id, "node_type": node_type, "output": output_payload},
        )

    def _fail_stage(self, *, run_id: str, step_id: str, node_id: str, state: dict[str, Any], error: Exception) -> Command:
        self.store.update_step(step_id, status="error", error_text=str(error), output_payload={"error": str(error)}, finished=True)
        self.store.update_run(run_id, status="failed", summary=str(error), current_node_id=node_id, state=state, finished_at=utcnow_iso())
        self.store.add_event(run_id=run_id, step_id=step_id, event_type=event_types.STEP_FAILED, payload={"node_id": node_id, "error": str(error)})
        self.store.add_event(run_id=run_id, event_type=event_types.RUN_FAILED, payload={"node_id": node_id, "error": str(error)})
        run = self.store.get_run(run_id)
        if run is not None:
            self.store.update_task_release(str(run["task_release_id"]), status="failed")
        return Command(update={"run_id": run_id}, goto=END)

    async def _task_ingress(self, state: TeamRuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        run, runtime_state, blueprint = self._bundle(run_id)
        step = self._begin_stage(
            run_id=run_id,
            node_id="task_ingress",
            node_type="runtime",
            input_payload={"task": runtime_state.get("task"), "team": runtime_state.get("team")},
        )
        try:
            if runtime_state.get("thread_id") is None:
                existing_threads = self.store.list_task_threads(run_id=run_id)
                if existing_threads:
                    runtime_state["thread_id"] = existing_threads[0]["id"]
            output = {
                "status": "ready",
                "entry_agent_id": dict(runtime_state.get("team") or {}).get("entry_agent_id"),
                "queue_size": len(list(runtime_state.get("queue") or [])),
                "thread_id": runtime_state.get("thread_id"),
                "workspace_id": blueprint.workspace_id,
                "project_id": blueprint.project_id,
            }
            self._complete_stage(
                run_id=run_id,
                step_id=str(step["id"]),
                node_id="task_ingress",
                node_type="runtime",
                state=runtime_state,
                output_payload=output,
                next_node_id="dispatch_next",
            )
            return Command(update={"run_id": run_id}, goto="dispatch_next")
        except Exception as exc:
            return self._fail_stage(run_id=run_id, step_id=str(step["id"]), node_id="task_ingress", state=runtime_state, error=exc)

    async def _dispatch_next(self, state: TeamRuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        _run, runtime_state, _blueprint = self._bundle(run_id)
        step = self._begin_stage(
            run_id=run_id,
            node_id="dispatch_next",
            node_type="runtime",
            input_payload={
                "queue_size": len(list(runtime_state.get("queue") or [])),
                "has_delivery": runtime_state.get("final_delivery_message") is not None,
                "delivery_emitted": bool(runtime_state.get("delivery_emitted")),
            },
        )
        try:
            next_stage = "finish_or_wait"
            active_message = None
            queue = list(runtime_state.get("queue") or [])
            if queue:
                active_message = queue.pop(0)
                runtime_state["queue"] = queue
                runtime_state["active_message"] = active_message
                next_stage = "review_gate"
            elif runtime_state.get("final_delivery_message") is not None:
                runtime_state["active_message"] = dict(runtime_state["final_delivery_message"])
                runtime_state["final_delivery_message"] = None
                active_message = dict(runtime_state["active_message"])
                next_stage = "review_gate"
            else:
                runtime_state["active_message"] = None
            output = {
                "selected": next_stage,
                "active_message": active_message,
                "remaining_queue_size": len(list(runtime_state.get("queue") or [])),
            }
            self._complete_stage(
                run_id=run_id,
                step_id=str(step["id"]),
                node_id="dispatch_next",
                node_type="runtime",
                state=runtime_state,
                output_payload=output,
                next_node_id=next_stage,
            )
            return Command(update={"run_id": run_id}, goto=next_stage)
        except Exception as exc:
            return self._fail_stage(run_id=run_id, step_id=str(step["id"]), node_id="dispatch_next", state=runtime_state, error=exc)

    async def _review_gate(self, state: TeamRuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        run, runtime_state, _blueprint = self._bundle(run_id)
        message = dict(runtime_state.get("active_message") or {})
        step = self._begin_stage(
            run_id=run_id,
            node_id="review_gate",
            node_type="runtime",
            input_payload={"message": message, "approval_mode": (runtime_state.get("task") or {}).get("approval_mode")},
        )
        try:
            if not message:
                output = {"status": "idle"}
                self._complete_stage(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    node_id="review_gate",
                    node_type="runtime",
                    state=runtime_state,
                    output_payload=output,
                    next_node_id="finish_or_wait",
                )
                return Command(update={"run_id": run_id}, goto="finish_or_wait")

            next_stage = "emit_artifacts" if str(message.get("message_type") or "") == "delivery" else "run_agent"
            review = self._review_decision(runtime_state, message)
            if review is None:
                output = {"status": "skipped", "selected": next_stage}
                self._complete_stage(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    node_id="review_gate",
                    node_type="runtime",
                    state=runtime_state,
                    output_payload=output,
                    next_node_id=next_stage,
                )
                return Command(update={"run_id": run_id}, goto=next_stage)

            waiting = dict(runtime_state.get("waiting") or {})
            approval = None
            if (
                str(waiting.get("scope") or "") == str(review["scope"])
                and str(waiting.get("message_id") or "") == str(message.get("message_id") or "")
            ):
                approval = self.store.get_approval(str(waiting.get("approval_id") or ""))
            if approval is None:
                approval = self.store.create_approval(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    node_id="review_gate",
                    title=str(review["title"]),
                    detail=str(review["detail"]),
                )
                runtime_state["waiting"] = {"approval_id": approval["id"], "scope": review["scope"], "message_id": message.get("message_id")}
                self.store.update_step(str(step["id"]), status="blocked", output_payload={"approval_id": approval["id"], "scope": review["scope"]}, finished=False)
                self.store.update_run(run_id, status="waiting_approval", current_node_id="review_gate", state=runtime_state)
                self.store.save_checkpoint(run_id=run_id, step_id=str(step["id"]), node_id="review_gate", snapshot=runtime_state)
                self.store.add_event(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    event_type=event_types.APPROVAL_REQUESTED,
                    payload={"approval_id": approval["id"], "scope": review["scope"], "message": message},
                )
                self.store.add_event(run_id=run_id, event_type=event_types.RUN_PAUSED, payload={"reason": "waiting_approval", "approval_id": approval["id"]})
            resolution = interrupt(review["payload"])
            approval = self.store.resolve_approval(
                str(approval["id"]),
                approved=bool((resolution or {}).get("approved", True)),
                comment=str((resolution or {}).get("comment") or ""),
                metadata=dict((resolution or {}).get("metadata") or {}),
            )
            assert approval is not None
            if approval["status"] == "rejected":
                runtime_state["waiting"] = None
                self.store.update_step(
                    str(step["id"]),
                    status="error",
                    output_payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
                    error_text="Approval rejected.",
                    finished=True,
                )
                self.store.add_event(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    event_type=event_types.APPROVAL_RESOLVED,
                    payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
                )
                self.store.update_run(
                    run_id,
                    status="failed",
                    summary="Approval rejected.",
                    current_node_id="review_gate",
                    state=runtime_state,
                    finished_at=utcnow_iso(),
                )
                self.store.update_task_release(str(run["task_release_id"]), status="failed")
                self.store.add_event(run_id=run_id, event_type=event_types.RUN_FAILED, payload={"reason": "approval_rejected"})
                return Command(update={"run_id": run_id}, goto=END)

            runtime_state["waiting"] = None
            edited_body = ((approval.get("resolution_json") or {}).get("metadata") or {}).get("edited_body")
            if isinstance(edited_body, str) and edited_body.strip():
                runtime_state["active_message"]["body"] = edited_body
            output = {"status": approval["status"], "approval_id": approval["id"], "selected": next_stage}
            self._complete_stage(
                run_id=run_id,
                step_id=str(step["id"]),
                node_id="review_gate",
                node_type="runtime",
                state=runtime_state,
                output_payload=output,
                next_node_id=next_stage,
            )
            self.store.add_event(
                run_id=run_id,
                step_id=str(step["id"]),
                event_type=event_types.APPROVAL_RESOLVED,
                payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
            )
            return Command(update={"run_id": run_id}, goto=next_stage)
        except GraphInterrupt:
            raise
        except Exception as exc:
            return self._fail_stage(run_id=run_id, step_id=str(step["id"]), node_id="review_gate", state=runtime_state, error=exc)

    async def _run_agent(self, state: TeamRuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        _run, runtime_state, blueprint = self._bundle(run_id)
        message = dict(runtime_state.get("active_message") or {})
        agent_id = str(message.get("target_agent_id") or "")
        step = self._begin_stage(
            run_id=run_id,
            node_id=agent_id or "run_agent",
            node_type="agent",
            input_payload={"message": message},
        )
        try:
            agent = blueprint.agents[agent_id]
            team = dict(runtime_state.get("team") or {})
            member = self._member(team, agent_id)
            instruction = self._agent_instruction(message, member)
            node = NodeSpec(
                id=f"team_{agent_id}_{message.get('message_id')}",
                type="agent",
                agent=agent_id,
                instruction=instruction,
                config={
                    "message_id": message.get("message_id"),
                    "group_id": message.get("group_id"),
                    "phase": message.get("phase"),
                    "message_type": message.get("message_type"),
                    "source_actor_id": message.get("source_actor_id"),
                    "target_agent_id": message.get("target_agent_id"),
                    "body": message.get("body"),
                    "level": member.get("level"),
                    "adjacent_targets": list((team.get("adjacency") or {}).get(agent_id, [])),
                    "plugin_actions": self._runtime_plugin_actions(member=member, message=message),
                },
            )
            context = AgentRunContext(
                workspace_id=blueprint.workspace_id,
                project_id=blueprint.project_id,
                run_id=run_id,
                prompt=str((runtime_state.get("task") or {}).get("prompt") or ""),
                inputs=dict((runtime_state.get("task") or {}).get("inputs") or {}),
                outputs=deepcopy(dict(message.get("context_outputs") or {})),
                loops={},
                current_node_id=node.id,
                team_id=str(team.get("team_definition_id") or team.get("team_definition_key") or "") or None,
            )
            tool_review_hook = self._make_tool_review_hook(
                run_id=run_id,
                step_id=str(step["id"]),
                runtime_state=runtime_state,
                team=team,
                source_agent_id=agent_id,
            )
            result = await self.agent_kernel.execute(
                agent,
                node,
                context,
                hooks={"before_tool_call": tool_review_hook},
            )
            runtime_state.setdefault("agent_outputs", {})[agent_id] = result
            runtime_state["last_result"] = {"agent_id": agent_id, "message": message, "output": result}
            runtime_state["pending_memory_effects"] = [
                {
                    "agent_id": agent_id,
                    "role": result.get("role"),
                    "memory_profile": member.get("memory_profile"),
                    "summary": result.get("summary"),
                    "deliverables": list(result.get("deliverables") or []),
                    "risks": list(result.get("risks") or []),
                    "next_focus": result.get("next_focus"),
                }
            ]
            group = runtime_state.setdefault("groups", {}).setdefault(
                str(message.get("group_id") or make_id("group")),
                {"expected_targets": [agent_id], "completed": [], "results": {}},
            )
            if agent_id not in group["completed"]:
                group["completed"].append(agent_id)
            group.setdefault("results", {})[agent_id] = result
            self.store.add_event(
                run_id=run_id,
                step_id=str(step["id"]),
                event_type=event_types.AGENT_MESSAGE,
                payload={"agent_id": agent_id, "summary": result.get("summary"), "message_type": message.get("message_type")},
            )
            for plugin_result in result.get("plugin_results", []) or []:
                self.store.add_event(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    event_type=event_types.PLUGIN_INVOKED,
                    payload={
                        "agent_id": agent_id,
                        "plugin_id": plugin_result.get("plugin_id"),
                        "plugin_key": plugin_result.get("plugin_key"),
                        "action": plugin_result.get("action"),
                        "result": plugin_result.get("result"),
                    },
                )
            output = {
                "agent_id": agent_id,
                "group_id": message.get("group_id"),
                "summary": result.get("summary"),
                "deliverables": result.get("deliverables"),
                "visible_output_ids": result.get("visible_output_ids"),
                "plugin_results": result.get("plugin_results"),
                "details": result.get("details"),
            }
            self._complete_stage(
                run_id=run_id,
                step_id=str(step["id"]),
                node_id=agent_id or "run_agent",
                node_type="agent",
                state=runtime_state,
                output_payload=output,
                next_node_id="route_message",
            )
            return Command(update={"run_id": run_id}, goto="route_message")
        except GraphInterrupt:
            raise
        except Exception as exc:
            return self._fail_stage(run_id=run_id, step_id=str(step["id"]), node_id=agent_id or "run_agent", state=runtime_state, error=exc)

    async def _route_message(self, state: TeamRuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        _run, runtime_state, _blueprint = self._bundle(run_id)
        last_result = dict(runtime_state.get("last_result") or {})
        step = self._begin_stage(
            run_id=run_id,
            node_id="route_message",
            node_type="runtime",
            input_payload={"last_result": last_result},
        )
        try:
            message = dict(last_result.get("message") or {})
            output = {"status": "idle", "enqueued": 0}
            if message:
                team = dict(runtime_state.get("team") or {})
                explicit_output = self._route_explicit_dialogue(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    runtime_state=runtime_state,
                    team=team,
                    last_result=last_result,
                )
                if explicit_output is not None:
                    output = explicit_output
                else:
                    group_id = str(message.get("group_id") or "")
                    group = dict((runtime_state.get("groups") or {}).get(group_id) or {})
                    expected = [str(item) for item in list(group.get("expected_targets") or []) if str(item).strip()]
                    completed = [str(item) for item in list(group.get("completed") or []) if str(item).strip()]
                    if expected and set(completed) >= set(expected):
                        merged = self._merge_group_outputs(dict(group.get("results") or {}))
                        follow_ups, delivery_message, next_group = self._next_messages(team=team, message=message, group=group, merged=merged)
                        if next_group is not None:
                            runtime_state.setdefault("groups", {})[str(next_group["group_id"])] = next_group
                        self._enqueue_messages(run_id=run_id, runtime_state=runtime_state, messages=follow_ups)
                        if delivery_message is not None:
                            self._queue_delivery_message(run_id=run_id, runtime_state=runtime_state, message=delivery_message)
                        runtime_state["groups"][group_id]["routed"] = True
                        output = {
                            "status": "routed",
                            "group_id": group_id,
                            "enqueued": len(follow_ups),
                            "delivery_ready": delivery_message is not None,
                        }
                    else:
                        output = {"status": "awaiting_group_completion", "group_id": group_id, "completed": completed, "expected": expected}
            runtime_state["active_message"] = None
            runtime_state["last_result"] = None
            self._complete_stage(
                run_id=run_id,
                step_id=str(step["id"]),
                node_id="route_message",
                node_type="runtime",
                state=runtime_state,
                output_payload=output,
                next_node_id="apply_memory_effects",
            )
            return Command(update={"run_id": run_id}, goto="apply_memory_effects")
        except GraphInterrupt:
            raise
        except Exception as exc:
            return self._fail_stage(run_id=run_id, step_id=str(step["id"]), node_id="route_message", state=runtime_state, error=exc)

    async def _apply_memory_effects(self, state: TeamRuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        _run, runtime_state, blueprint = self._bundle(run_id)
        effects = list(runtime_state.get("pending_memory_effects") or [])
        step = self._begin_stage(
            run_id=run_id,
            node_id="apply_memory_effects",
            node_type="runtime",
            input_payload={"effects": effects},
        )
        try:
            applied_targets: list[str] = []
            team = dict(runtime_state.get("team") or {})
            team_id = str(team.get("team_definition_id") or team.get("team_definition_key") or "")
            for effect in effects:
                records = self._memory_effect_records(team_id=team_id, effect=dict(effect or {}))
                if not records:
                    continue
                for target_name, scope in self._memory_effect_scopes(
                    workspace_id=blueprint.workspace_id,
                    project_id=blueprint.project_id,
                    run_id=run_id,
                    team_id=team_id,
                    effect=dict(effect or {}),
                ):
                    reviewed_records = await self._review_memory_write(
                        run_id=run_id,
                        step_id=str(step["id"]),
                        runtime_state=runtime_state,
                        team=team,
                        effect=dict(effect or {}),
                        target_name=target_name,
                        scope=scope,
                        records=list(records),
                    )
                    await self.agent_kernel.memory.remember(scope, reviewed_records)
                    applied_targets.append(f"{target_name}:{scope.key}")
            runtime_state["pending_memory_effects"] = []
            output = {
                "applied_count": len(applied_targets),
                "strategy": "langmem_hot_path_plus_background_reflection",
                "targets": applied_targets,
            }
            self._complete_stage(
                run_id=run_id,
                step_id=str(step["id"]),
                node_id="apply_memory_effects",
                node_type="runtime",
                state=runtime_state,
                output_payload=output,
                next_node_id="dispatch_next",
            )
            return Command(update={"run_id": run_id}, goto="dispatch_next")
        except GraphInterrupt:
            raise
        except Exception as exc:
            return self._fail_stage(run_id=run_id, step_id=str(step["id"]), node_id="apply_memory_effects", state=runtime_state, error=exc)

    def _make_tool_review_hook(
        self,
        *,
        run_id: str,
        step_id: str,
        runtime_state: dict[str, Any],
        team: dict[str, Any],
        source_agent_id: str,
    ):
        async def _hook(*, plugin_ref: dict[str, Any], action: str, payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
            approval_mode = str((runtime_state.get("task") or {}).get("approval_mode") or "auto")
            if approval_mode == "auto":
                return {"payload": payload}
            matched = self._matched_tool_review_policies(
                team=team,
                source_agent_id=source_agent_id,
                plugin_ref=plugin_ref,
                action=action,
            )
            if not matched:
                return {"payload": payload}
            manifest = dict(plugin_ref.get("manifest") or {})
            risk_tags = self._tool_call_risk_tags(plugin_ref)
            review_payload = {
                "review_id": make_id("rev"),
                "scope": "tool_call",
                "source_agent_id": source_agent_id,
                "target_agent_id": None,
                "risk_tags": risk_tags,
                "proposed_action": {
                    "plugin_id": plugin_ref.get("id"),
                    "plugin_key": plugin_ref.get("key"),
                    "action": action,
                    "payload": payload,
                    "permissions": list(manifest.get("permissions") or []),
                    "tools": list(manifest.get("tools") or []),
                },
                "editable_fields": ["payload"],
                "deadline_at": None,
                "matched_policies": [
                    {"key": item.get("key"), "name": item.get("name"), "version": item.get("version")}
                    for item in matched
                ],
            }
            approval = self._interrupt_review(
                run_id=run_id,
                step_id=step_id,
                node_id=source_agent_id or "run_agent",
                runtime_state=runtime_state,
                scope="tool_call",
                title="Tool call review",
                detail=trim_text(pretty_json(review_payload), limit=1400),
                payload=review_payload,
            )
            resolved_payload = dict(payload)
            if approval["status"] == "approved":
                metadata = dict((approval.get("resolution_json") or {}).get("metadata") or {})
                edited_payload = metadata.get("edited_payload")
                if isinstance(edited_payload, dict):
                    resolved_payload = edited_payload
                return {"payload": resolved_payload, "approval_id": approval["id"], "matched_policies": review_payload["matched_policies"]}
            raise RuntimeError("Tool call approval rejected.")

        return _hook

    def _memory_effect_scopes(
        self,
        *,
        workspace_id: str,
        project_id: str,
        run_id: str,
        team_id: str,
        effect: dict[str, Any],
    ) -> list[tuple[str, Any]]:
        profile = dict(effect.get("memory_profile") or {})
        config = dict(profile.get("config") or {})
        write_scopes = {str(item).strip().lower() for item in list(config.get("write_scopes") or []) if str(item).strip()}
        if not write_scopes:
            return []
        agent_id = str(effect.get("agent_id") or "")
        base_scopes = MemoryScopes(
            workspace_id=workspace_id,
            project_id=project_id,
            run_id=run_id,
            agent_id=agent_id or "agent",
        )
        team_scopes = MemoryScopes(
            workspace_id=workspace_id,
            project_id=project_id,
            run_id=run_id,
            agent_id=agent_id or "agent",
            team_id=team_id or None,
        )
        targets: list[tuple[str, Any]] = []
        if "team" in write_scopes and team_id:
            targets.append(("team", team_scopes.team_shared()))
        if "project" in write_scopes:
            targets.append(("project", base_scopes.project_shared()))
        if "run" in write_scopes or "retrospective" in write_scopes:
            targets.append(("run", team_scopes.run_retrospective()))
        return targets

    def _memory_effect_records(self, *, team_id: str, effect: dict[str, Any]) -> list[dict[str, Any]]:
        summary = trim_text(str(effect.get("summary") or ""), limit=2000)
        if not summary:
            return []
        profile = dict(effect.get("memory_profile") or {})
        config = dict(profile.get("config") or {})
        long_term = dict(config.get("long_term") or {})
        ttl_days = long_term.get("ttl_days")
        ttl_minutes = None
        if isinstance(ttl_days, (int, float)) and float(ttl_days) > 0:
            ttl_minutes = float(ttl_days) * 24.0 * 60.0
        metadata = {
            "team_definition_id": team_id or None,
            "agent": effect.get("agent_id"),
            "role": effect.get("role"),
            "memory_source": "team_runtime.apply_memory_effects",
        }
        records: list[dict[str, Any]] = [
            {
                "text": summary,
                "summary": summary,
                "layer": "semantic",
                "importance": 0.62,
                "confidence": 0.8,
                "metadata": metadata,
                "expires_at": ttl_minutes,
            }
        ]
        next_focus = trim_text(str(effect.get("next_focus") or ""), limit=600)
        if next_focus:
            records.append(
                {
                    "text": next_focus,
                    "summary": next_focus,
                    "layer": "semantic",
                    "importance": 0.5,
                    "confidence": 0.72,
                    "metadata": metadata | {"kind": "next_focus"},
                    "expires_at": ttl_minutes,
                }
            )
        for item in list(effect.get("deliverables") or [])[:3]:
            text = trim_text(str(item or ""), limit=800)
            if not text:
                continue
            records.append(
                {
                    "text": text,
                    "summary": text,
                    "layer": "semantic",
                    "importance": 0.54,
                    "confidence": 0.76,
                    "metadata": metadata | {"kind": "deliverable"},
                    "expires_at": ttl_minutes,
                }
            )
        for item in list(effect.get("risks") or [])[:2]:
            text = trim_text(str(item or ""), limit=800)
            if not text:
                continue
            records.append(
                {
                    "text": text,
                    "summary": text,
                    "layer": "semantic",
                    "importance": 0.58,
                    "confidence": 0.74,
                    "metadata": metadata | {"kind": "risk"},
                    "expires_at": ttl_minutes,
                }
            )
        return records

    async def _emit_artifacts(self, state: TeamRuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        _run, runtime_state, blueprint = self._bundle(run_id)
        message = dict(runtime_state.get("active_message") or {})
        step = self._begin_stage(
            run_id=run_id,
            node_id="emit_artifacts",
            node_type="runtime",
            input_payload={"delivery": message},
        )
        try:
            payload = dict(message.get("payload") or {})
            content = self._render_delivery_markdown(payload)
            target = self.workspace.write_artifact(
                workspace_id=blueprint.workspace_id,
                project_id=blueprint.project_id,
                run_id=run_id,
                name="team-summary.md",
                content=content,
            )
            artifact = self.store.create_artifact(
                run_id=run_id,
                step_id=str(step["id"]),
                kind="report",
                name="team-summary.md",
                path=str(target),
                summary=trim_text(content, limit=180),
                metadata={"node_id": "emit_artifacts", "source_agent_id": message.get("source_actor_id")},
            )
            self.store.add_event(
                run_id=run_id,
                step_id=str(step["id"]),
                event_type=event_types.ARTIFACT_CREATED,
                payload={"artifact_id": artifact["id"], "name": artifact["name"], "path": artifact["path"]},
            )
            runtime_state["delivery_emitted"] = True
            runtime_state["active_message"] = None
            runtime_state["delivery_artifact"] = artifact
            output = {"artifact_id": artifact["id"], "path": artifact["path"], "name": artifact["name"]}
            self._complete_stage(
                run_id=run_id,
                step_id=str(step["id"]),
                node_id="emit_artifacts",
                node_type="runtime",
                state=runtime_state,
                output_payload=output,
                next_node_id="finish_or_wait",
            )
            return Command(update={"run_id": run_id}, goto="finish_or_wait")
        except Exception as exc:
            return self._fail_stage(run_id=run_id, step_id=str(step["id"]), node_id="emit_artifacts", state=runtime_state, error=exc)

    async def _finish_or_wait(self, state: TeamRuntimeGraphState) -> Command:
        run_id = str(state["run_id"])
        run, runtime_state, _blueprint = self._bundle(run_id)
        step = self._begin_stage(
            run_id=run_id,
            node_id="finish_or_wait",
            node_type="runtime",
            input_payload={
                "queue_size": len(list(runtime_state.get("queue") or [])),
                "has_delivery": runtime_state.get("final_delivery_message") is not None,
                "delivery_emitted": runtime_state.get("delivery_emitted"),
            },
        )
        try:
            if runtime_state.get("waiting"):
                output = {"status": "waiting_approval"}
                self.store.update_step(str(step["id"]), status="blocked", output_payload=output, finished=False)
                self.store.update_run(run_id, status="waiting_approval", current_node_id="finish_or_wait", state=runtime_state)
                self.store.save_checkpoint(run_id=run_id, step_id=str(step["id"]), node_id="finish_or_wait", snapshot=runtime_state)
                return Command(update={"run_id": run_id}, goto=END)
            if runtime_state.get("active_message") or list(runtime_state.get("queue") or []) or runtime_state.get("final_delivery_message") is not None:
                output = {"status": "continue"}
                self._complete_stage(
                    run_id=run_id,
                    step_id=str(step["id"]),
                    node_id="finish_or_wait",
                    node_type="runtime",
                    state=runtime_state,
                    output_payload=output,
                    next_node_id="dispatch_next",
                )
                return Command(update={"run_id": run_id}, goto="dispatch_next")

            summary = self._resolve_summary(runtime_state)
            self.store.update_run(
                run_id,
                status="completed",
                summary=summary,
                current_node_id="finish_or_wait",
                state=runtime_state,
                result=runtime_state.get("agent_outputs", {}),
                finished_at=utcnow_iso(),
            )
            self.store.update_task_release(str(run["task_release_id"]), status="completed")
            self.store.add_event(run_id=run_id, event_type=event_types.RUN_COMPLETED, payload={"summary": summary})
            output = {"status": "completed", "summary": summary}
            self.store.update_step(str(step["id"]), status="done", output_payload=output, finished=True)
            self.store.add_event(
                run_id=run_id,
                step_id=str(step["id"]),
                event_type=event_types.STEP_COMPLETED,
                payload={"node_id": "finish_or_wait", "node_type": "runtime", "output": output},
            )
            return Command(update={"run_id": run_id}, goto=END)
        except Exception as exc:
            return self._fail_stage(run_id=run_id, step_id=str(step["id"]), node_id="finish_or_wait", state=runtime_state, error=exc)

    def _member(self, team: dict[str, Any], agent_id: str) -> dict[str, Any]:
        for item in list(team.get("members") or []):
            if str(item.get("key") or "") == agent_id:
                return dict(item)
        raise KeyError(f"Unknown team member `{agent_id}`.")

    def _ordered_levels(self, team: dict[str, Any]) -> list[int]:
        return [int(item) for item in list(team.get("ordered_levels") or [])]

    def _next_level(self, team: dict[str, Any], current_level: int, *, phase: str) -> int | None:
        levels = self._ordered_levels(team)
        if current_level not in levels:
            return None
        index = levels.index(current_level)
        if phase == "down":
            return levels[index + 1] if index + 1 < len(levels) else None
        return levels[index - 1] if index - 1 >= 0 else None

    def _runtime_plugin_actions(self, *, member: dict[str, Any], message: dict[str, Any]) -> list[dict[str, Any]]:
        message_type = str(message.get("message_type") or "").strip()
        phase = str(message.get("phase") or "").strip()
        source_actor_id = str(message.get("source_actor_id") or "").strip()
        configured = [dict(item) for item in list(member.get("runtime_plugin_actions") or []) if isinstance(item, dict)]
        filtered: list[dict[str, Any]] = []
        for item in configured:
            if item.get("enabled") is False:
                continue
            when_message_types = {str(value).strip() for value in list(item.get("when_message_types") or []) if str(value).strip()}
            if when_message_types and message_type not in when_message_types:
                continue
            when_phases = {str(value).strip() for value in list(item.get("when_phases") or []) if str(value).strip()}
            if when_phases and phase not in when_phases:
                continue
            when_sources = {str(value).strip() for value in list(item.get("when_sources") or []) if str(value).strip()}
            if when_sources and source_actor_id not in when_sources:
                continue
            if item.get("when_from_human") is True and source_actor_id != "human":
                continue
            if item.get("when_from_agent") is True and source_actor_id == "human":
                continue
            filtered.append(item)
        return filtered

    def _route_explicit_dialogue(
        self,
        *,
        run_id: str,
        step_id: str,
        runtime_state: dict[str, Any],
        team: dict[str, Any],
        last_result: dict[str, Any],
    ) -> dict[str, Any] | None:
        message = dict(last_result.get("message") or {})
        result = dict(last_result.get("output") or {})
        source_agent_id = str(last_result.get("agent_id") or message.get("target_agent_id") or "").strip()
        plugin_results = [dict(item) for item in list(result.get("plugin_results") or []) if isinstance(item, dict)]
        routes = [
            dict((item.get("result") or {}).get("route") or {})
            for item in plugin_results
            if str(item.get("plugin_key") or "") in {BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY, BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY}
            and isinstance((item.get("result") or {}).get("route"), dict)
        ]
        escalations = [
            dict((item.get("result") or {}).get("review") or {})
            for item in plugin_results
            if str(item.get("plugin_key") or "") == BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY
            and isinstance((item.get("result") or {}).get("review"), dict)
        ]
        if not routes and not escalations:
            return None
        escalation_count = 0
        for review in escalations:
            approval_payload = {
                "review_id": make_id("rev"),
                "scope": "human_escalation",
                "source_agent_id": source_agent_id,
                "target_agent_id": "human",
                "risk_tags": list(review.get("risk_tags") or ["human_escalation"]),
                "proposed_action": {
                    "message_type": "human_escalation",
                    "title": review.get("title"),
                    "body": review.get("body") or result.get("summary"),
                    "detail": review.get("detail") or result.get("summary"),
                    "metadata": dict(review.get("metadata") or {}),
                },
                "editable_fields": ["body"],
                "deadline_at": None,
            }
            approval = self._interrupt_review(
                run_id=run_id,
                step_id=step_id,
                node_id="route_message",
                runtime_state=runtime_state,
                scope="human_escalation",
                title=str(review.get("title") or "Human escalation requested"),
                detail=trim_text(pretty_json(approval_payload), limit=1400),
                payload=approval_payload,
            )
            if approval["status"] != "approved":
                raise RuntimeError("Human escalation rejected.")
            escalation_count += 1
        if not routes:
            return None
        enqueued = 0
        delivery_ready = False
        for route in routes:
            outbound = self._build_explicit_router_message(team=team, message=message, source_agent_id=source_agent_id, result=result, route=route)
            if str(outbound.get("message_type") or "") == "delivery" or str(outbound.get("target_agent_id") or "") == "human":
                self._queue_delivery_message(
                    run_id=run_id,
                    runtime_state=runtime_state,
                    message={
                        "message_id": str(outbound.get("message_id") or make_id("teammsg")),
                        "source_actor_id": source_agent_id,
                        "target_agent_id": None,
                        "message_type": "delivery",
                        "phase": outbound.get("phase"),
                        "body": outbound.get("body"),
                        "payload": {
                            "summary": outbound.get("body"),
                            "deliverables": list(result.get("deliverables") or []),
                            "risks": list(result.get("risks") or []),
                        },
                    },
                )
                delivery_ready = True
                continue
            runtime_state.setdefault("queue", []).append(outbound)
            self._record_message_event(run_id=run_id, runtime_state=runtime_state, message=outbound, status="delivered")
            enqueued += 1
        return {
            "status": "explicit_dialogue_routed",
            "enqueued": enqueued,
            "delivery_ready": delivery_ready,
            "human_escalations": escalation_count,
        }

    def _build_explicit_router_message(
        self,
        *,
        team: dict[str, Any],
        message: dict[str, Any],
        source_agent_id: str,
        result: dict[str, Any],
        route: dict[str, Any],
    ) -> dict[str, Any]:
        target_agent_id = str(route.get("target_agent_id") or "").strip()
        if not target_agent_id:
            raise RuntimeError("Dialogue Router requires `target_agent_id` for explicit team messages.")
        if target_agent_id != "human":
            self._assert_dialogue_allowed(team=team, source_agent_id=source_agent_id, target_agent_id=target_agent_id)
        source_level = self._actor_level(team, source_agent_id)
        target_level = self._actor_level(team, target_agent_id)
        context_outputs = deepcopy(dict(message.get("context_outputs") or {}))
        if source_agent_id:
            context_outputs[source_agent_id] = result
        return {
            "message_id": make_id("teammsg"),
            "group_id": str(route.get("group_id") or make_id("dialogue")),
            "source_actor_id": source_agent_id,
            "target_agent_id": target_agent_id,
            "message_type": str(route.get("message_type") or "dialogue"),
            "phase": str(route.get("phase") or message.get("phase") or "down"),
            "body": self._route_body(result=result, route=route),
            "payload": dict(route.get("payload") or {}),
            "source_level": source_level,
            "target_level": target_level,
            "context_outputs": context_outputs,
        }

    def _route_body(self, *, result: dict[str, Any], route: dict[str, Any]) -> str:
        body = trim_text(str(route.get("body") or result.get("summary") or ""), limit=2400)
        if body:
            return body
        deliverables = [str(item) for item in list(result.get("deliverables") or []) if str(item).strip()]
        if deliverables:
            return trim_text("; ".join(deliverables), limit=2400)
        return "No summary."

    def _actor_level(self, team: dict[str, Any], actor_id: str) -> int | None:
        if actor_id == "human":
            return 16
        if not actor_id:
            return None
        return int(self._member(team, actor_id).get("level") or 0)

    def _assert_dialogue_allowed(self, *, team: dict[str, Any], source_agent_id: str, target_agent_id: str) -> None:
        if source_agent_id == "human" or target_agent_id == "human":
            return
        allowed = {str(item) for item in list((team.get("adjacency") or {}).get(source_agent_id, [])) if str(item).strip()}
        if target_agent_id not in allowed:
            raise RuntimeError(
                f"Dialogue Router blocked `{source_agent_id}` -> `{target_agent_id}` because the agents are not adjacent levels."
            )

    def _enqueue_messages(
        self,
        *,
        run_id: str,
        runtime_state: dict[str, Any],
        messages: list[dict[str, Any]],
    ) -> None:
        for item in messages:
            runtime_state.setdefault("queue", []).append(item)
            self._record_message_event(run_id=run_id, runtime_state=runtime_state, message=item, status="delivered")

    def _queue_delivery_message(
        self,
        *,
        run_id: str,
        runtime_state: dict[str, Any],
        message: dict[str, Any],
    ) -> None:
        runtime_state["final_delivery_message"] = message
        self.store.add_message_event(
            run_id=run_id,
            thread_id=str(runtime_state.get("thread_id") or "") or None,
            source_agent_id=str(message.get("source_actor_id") or ""),
            target_agent_id=None,
            message_type="delivery",
            payload={"summary": message.get("body"), "phase": message.get("phase")},
            status="pending_review",
        )

    def _record_message_event(
        self,
        *,
        run_id: str,
        runtime_state: dict[str, Any],
        message: dict[str, Any],
        status: str,
    ) -> None:
        self.store.add_message_event(
            run_id=run_id,
            thread_id=str(runtime_state.get("thread_id") or "") or None,
            source_agent_id=str(message.get("source_actor_id") or ""),
            target_agent_id=str(message.get("target_agent_id") or ""),
            message_type=str(message.get("message_type") or "handoff"),
            payload={
                "summary": message.get("body"),
                "phase": message.get("phase"),
                "source_level": message.get("source_level"),
                "target_level": message.get("target_level"),
                "group_id": message.get("group_id"),
            },
            status=status,
        )

    async def _review_memory_write(
        self,
        *,
        run_id: str,
        step_id: str,
        runtime_state: dict[str, Any],
        team: dict[str, Any],
        effect: dict[str, Any],
        target_name: str,
        scope: Any,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        approval_mode = str((runtime_state.get("task") or {}).get("approval_mode") or "auto")
        if approval_mode == "auto":
            return records
        source_agent_id = str(effect.get("agent_id") or "")
        matched = self._matched_memory_review_policies(
            team=team,
            source_agent_id=source_agent_id,
            target_name=target_name,
            records=records,
        )
        if not matched:
            return records
        risk_tags = self._memory_write_risk_tags(target_name)
        review_payload = {
            "review_id": make_id("rev"),
            "scope": "memory_write",
            "source_agent_id": source_agent_id,
            "target_agent_id": None,
            "risk_tags": risk_tags,
            "proposed_action": {
                "target_scope": target_name,
                "namespace": list(scope.namespace if isinstance(scope.namespace, tuple) else [scope.namespace]),
                "scope_key": getattr(scope, "key", ""),
                "record_count": len(records),
                "records": records,
            },
            "editable_fields": ["records"],
            "deadline_at": None,
            "matched_policies": [
                {"key": item.get("key"), "name": item.get("name"), "version": item.get("version")}
                for item in matched
            ],
        }
        approval = self._interrupt_review(
            run_id=run_id,
            step_id=step_id,
            node_id="apply_memory_effects",
            runtime_state=runtime_state,
            scope="memory_write",
            title="Memory write review",
            detail=trim_text(pretty_json(review_payload), limit=1400),
            payload=review_payload,
        )
        if approval["status"] != "approved":
            raise RuntimeError("Memory write approval rejected.")
        metadata = dict((approval.get("resolution_json") or {}).get("metadata") or {})
        edited_records = metadata.get("edited_records")
        if isinstance(edited_records, list):
            normalized = [dict(item) for item in edited_records if isinstance(item, dict)]
            if normalized:
                return normalized
        return records

    def _interrupt_review(
        self,
        *,
        run_id: str,
        step_id: str,
        node_id: str,
        runtime_state: dict[str, Any],
        scope: str,
        title: str,
        detail: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        waiting = dict(runtime_state.get("waiting") or {})
        approval = None
        if str(waiting.get("scope") or "") == scope:
            approval = self.store.get_approval(str(waiting.get("approval_id") or ""))
        if approval is None:
            approval = self.store.create_approval(
                run_id=run_id,
                step_id=step_id,
                node_id=node_id,
                title=title,
                detail=detail,
            )
            runtime_state["waiting"] = {"approval_id": approval["id"], "scope": scope}
            self.store.update_step(str(step_id), status="blocked", output_payload={"approval_id": approval["id"], "scope": scope}, finished=False)
            self.store.update_run(run_id, status="waiting_approval", current_node_id=node_id, state=runtime_state)
            self.store.save_checkpoint(run_id=run_id, step_id=step_id, node_id=node_id, snapshot=runtime_state)
            self.store.add_event(
                run_id=run_id,
                step_id=str(step_id),
                event_type=event_types.APPROVAL_REQUESTED,
                payload={"approval_id": approval["id"], "scope": scope, "review": payload},
            )
            self.store.add_event(run_id=run_id, event_type=event_types.RUN_PAUSED, payload={"reason": "waiting_approval", "approval_id": approval["id"]})
        resolution = interrupt(payload)
        approval = self.store.resolve_approval(
            str(approval["id"]),
            approved=bool((resolution or {}).get("approved", True)),
            comment=str((resolution or {}).get("comment") or ""),
            metadata=dict((resolution or {}).get("metadata") or {}),
        )
        assert approval is not None
        runtime_state["waiting"] = None
        self.store.add_event(
            run_id=run_id,
            step_id=str(step_id),
            event_type=event_types.APPROVAL_RESOLVED,
            payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
        )
        return approval

    def _matched_tool_review_policies(
        self,
        *,
        team: dict[str, Any],
        source_agent_id: str,
        plugin_ref: dict[str, Any],
        action: str,
    ) -> list[dict[str, Any]]:
        candidates = self._review_candidates(team=team, source_agent_id=source_agent_id)
        plugin_key = str(plugin_ref.get("key") or "")
        permissions = {str(item) for item in list(dict(plugin_ref.get("manifest") or {}).get("permissions") or []) if str(item).strip()}
        risk_tags = set(self._tool_call_risk_tags(plugin_ref))
        matched: list[dict[str, Any]] = []
        seen: set[str] = set()
        for policy in candidates:
            key = str(policy.get("id") or policy.get("key") or "")
            if key in seen:
                continue
            spec = dict(policy.get("spec") or {})
            triggers = {str(item) for item in list(spec.get("triggers") or []) if str(item).strip()}
            if not triggers.intersection({"before_tool_call", "before_external_side_effect"}):
                continue
            conditions = dict(spec.get("conditions") or {})
            plugin_keys = {str(item) for item in list(conditions.get("plugin_keys") or []) if str(item).strip()}
            if plugin_keys and plugin_key not in plugin_keys:
                continue
            actions = {str(item) for item in list(conditions.get("actions") or []) if str(item).strip()}
            if actions and action not in actions:
                continue
            required_permissions = {str(item) for item in list(conditions.get("permissions") or []) if str(item).strip()}
            if required_permissions and not required_permissions.intersection(permissions):
                continue
            required_risk_tags = {str(item) for item in list(conditions.get("risk_tags") or []) if str(item).strip()}
            if required_risk_tags and not required_risk_tags.intersection(risk_tags):
                continue
            matched.append(policy)
            seen.add(key)
        return matched

    def _matched_memory_review_policies(
        self,
        *,
        team: dict[str, Any],
        source_agent_id: str,
        target_name: str,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        candidates = self._review_candidates(team=team, source_agent_id=source_agent_id)
        risk_tags = set(self._memory_write_risk_tags(target_name))
        record_kinds = {
            str((dict(item.get("metadata") or {})).get("kind") or "summary")
            for item in records
            if isinstance(item, dict)
        }
        matched: list[dict[str, Any]] = []
        seen: set[str] = set()
        for policy in candidates:
            key = str(policy.get("id") or policy.get("key") or "")
            if key in seen:
                continue
            spec = dict(policy.get("spec") or {})
            triggers = {str(item) for item in list(spec.get("triggers") or []) if str(item).strip()}
            if not triggers.intersection({"before_memory_write", "memory_write"}):
                continue
            conditions = dict(spec.get("conditions") or {})
            memory_scopes = {str(item) for item in list(conditions.get("memory_scopes") or conditions.get("scopes") or []) if str(item).strip()}
            if memory_scopes and target_name not in memory_scopes:
                continue
            memory_kinds = {str(item) for item in list(conditions.get("memory_kinds") or []) if str(item).strip()}
            if memory_kinds and not memory_kinds.intersection(record_kinds):
                continue
            required_risk_tags = {str(item) for item in list(conditions.get("risk_tags") or []) if str(item).strip()}
            if required_risk_tags and not required_risk_tags.intersection(risk_tags):
                continue
            matched.append(policy)
            seen.add(key)
        return matched

    def _review_candidates(self, *, team: dict[str, Any], source_agent_id: str) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        candidates.extend(dict(item) for item in list(team.get("team_review_policies") or []))
        if source_agent_id and source_agent_id != "human":
            try:
                member = self._member(team, source_agent_id)
            except KeyError:
                member = {}
            candidates.extend(dict(item) for item in list(member.get("review_policies") or []))
        return candidates

    def _tool_call_risk_tags(self, plugin_ref: dict[str, Any]) -> list[str]:
        manifest = dict(plugin_ref.get("manifest") or {})
        permissions = [str(item) for item in list(manifest.get("permissions") or []) if str(item).strip()]
        risk_tags = ["tool_call", *permissions]
        if any(item not in {"readonly", "memory_read"} for item in permissions):
            risk_tags.append("external_side_effect")
        return sorted(set(risk_tags))

    def _memory_write_risk_tags(self, target_name: str) -> list[str]:
        risk_tags = ["memory_mutation", f"scope:{target_name}"]
        if target_name in {"team", "project"}:
            risk_tags.append("shared_memory")
        return sorted(set(risk_tags))

    def _review_decision(self, runtime_state: dict[str, Any], message: dict[str, Any]) -> dict[str, Any] | None:
        approval_mode = str((runtime_state.get("task") or {}).get("approval_mode") or "auto")
        team = dict(runtime_state.get("team") or {})
        matched_overrides = self._matched_review_overrides(team=team, message=message)
        if approval_mode == "auto" and not matched_overrides:
            return None
        matched = self._matched_review_policies(team=team, message=message)
        if not matched and not matched_overrides:
            return None
        if str(message.get("message_type") or "") == "delivery":
            title = "Final delivery review"
            scope = "final_delivery"
        else:
            title = "Agent message review"
            scope = "agent_message"
        risk_tags = {
            str(item)
            for policy in matched
            for item in list((policy.get("spec") or {}).get("conditions", {}).get("risk_tags") or [])
        }
        if matched_overrides:
            risk_tags.update({"manual_review", "team_edge_review_override"})
        payload = {
            "review_id": make_id("rev"),
            "scope": scope,
            "source_agent_id": message.get("source_actor_id"),
            "target_agent_id": message.get("target_agent_id"),
            "risk_tags": sorted(risk_tags or {"manual_review"}),
            "proposed_action": {
                "message_type": message.get("message_type"),
                "phase": message.get("phase"),
                "body": message.get("body"),
                "payload": message.get("payload"),
            },
            "editable_fields": ["body"],
            "deadline_at": None,
            "matched_policies": [
                {"key": item.get("key"), "name": item.get("name"), "version": item.get("version")}
                for item in matched
            ],
            "matched_review_overrides": [
                {
                    "name": item.get("name"),
                    "mode": item.get("mode"),
                    "source_agent_id": item.get("source_agent_id"),
                    "target_agent_id": item.get("target_agent_id"),
                    "message_types": list(item.get("message_types") or []),
                    "phases": list(item.get("phases") or []),
                }
                for item in matched_overrides
            ],
        }
        return {"scope": scope, "title": title, "detail": trim_text(pretty_json(payload), limit=1400), "payload": payload}

    def _matched_review_overrides(self, *, team: dict[str, Any], message: dict[str, Any]) -> list[dict[str, Any]]:
        matched: list[dict[str, Any]] = []
        for override in list(team.get("review_overrides") or []):
            if not isinstance(override, dict):
                continue
            if self._review_override_matches(dict(override), message=message):
                matched.append(dict(override))
        return matched

    def _review_override_matches(self, override: dict[str, Any], *, message: dict[str, Any]) -> bool:
        if str(override.get("mode") or "must_review_before") != "must_review_before":
            return False
        source_agent_id = str(override.get("source_agent_id") or "").strip()
        if source_agent_id and str(message.get("source_actor_id") or "") != source_agent_id:
            return False
        target_agent_id = str(override.get("target_agent_id") or "").strip()
        if target_agent_id and str(message.get("target_agent_id") or "") != target_agent_id:
            return False
        message_types = {str(item).strip() for item in list(override.get("message_types") or []) if str(item).strip()}
        if message_types and str(message.get("message_type") or "") not in message_types:
            return False
        phases = {str(item).strip() for item in list(override.get("phases") or []) if str(item).strip()}
        if phases and str(message.get("phase") or "") not in phases:
            return False
        return True

    def _matched_review_policies(self, *, team: dict[str, Any], message: dict[str, Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        candidates.extend(dict(item) for item in list(team.get("team_review_policies") or []))
        source_actor_id = str(message.get("source_actor_id") or "")
        target_agent_id = str(message.get("target_agent_id") or "")
        if source_actor_id and source_actor_id != "human":
            candidates.extend(dict(item) for item in list(self._member(team, source_actor_id).get("review_policies") or []))
        if target_agent_id:
            candidates.extend(dict(item) for item in list(self._member(team, target_agent_id).get("review_policies") or []))
        matched: list[dict[str, Any]] = []
        seen: set[str] = set()
        for policy in candidates:
            key = str(policy.get("id") or policy.get("key") or "")
            if key in seen:
                continue
            if self._policy_matches(policy, team=team, message=message):
                matched.append(policy)
                seen.add(key)
        return matched

    def _policy_matches(self, policy: dict[str, Any], *, team: dict[str, Any], message: dict[str, Any]) -> bool:
        spec = dict(policy.get("spec") or {})
        triggers = {str(item) for item in list(spec.get("triggers") or []) if str(item).strip()}
        if not triggers:
            return False
        message_triggers = self._message_triggers(team=team, message=message)
        if not triggers.intersection(message_triggers):
            return False
        conditions = dict(spec.get("conditions") or {})
        message_types = {str(item) for item in list(conditions.get("message_types") or []) if str(item).strip()}
        if message_types and str(message.get("message_type") or "") not in message_types:
            return False
        return True

    def _message_triggers(self, *, team: dict[str, Any], message: dict[str, Any]) -> set[str]:
        message_type = str(message.get("message_type") or "")
        source_actor_id = str(message.get("source_actor_id") or "")
        target_agent_id = str(message.get("target_agent_id") or "")
        triggers: set[str] = set()
        if message_type == "delivery":
            triggers.update({"final_delivery", "before_final_delivery"})
            return triggers
        if source_actor_id == "human":
            triggers.update({"before_agent_receive_task", "before_task_ingress"})
            return triggers
        if target_agent_id:
            triggers.add("before_agent_to_agent_message")
            source_member = self._member(team, source_actor_id)
            target_member = self._member(team, target_agent_id)
            source_level = int(source_member.get("level") or 0)
            target_level = int(target_member.get("level") or 0)
            if target_level > source_level:
                triggers.add("before_escalation_to_upper_level")
            elif target_level < source_level:
                triggers.add("before_handoff_to_lower_level")
        return triggers

    def _agent_instruction(self, message: dict[str, Any], member: dict[str, Any]) -> str:
        source = str(message.get("source_actor_id") or "human")
        phase = str(message.get("phase") or "down")
        return (
            f"Receive a {message.get('message_type')} from {source}. "
            f"Current collaboration phase is {phase}. "
            f"Your team level is {member.get('level')}. "
            f"Keep the response concise, actionable, and suitable for adjacent-level routing.\n\n"
            f"Incoming message:\n{message.get('body') or ''}"
        )

    def _merge_group_outputs(self, results: dict[str, Any]) -> dict[str, Any]:
        summaries: list[str] = []
        deliverables: list[str] = []
        risks: list[str] = []
        for agent_id in sorted(results):
            item = dict(results[agent_id] or {})
            if item.get("summary"):
                summaries.append(f"{agent_id}: {item['summary']}")
            for deliverable in list(item.get("deliverables") or []):
                deliverables.append(str(deliverable))
            for risk in list(item.get("risks") or []):
                risks.append(str(risk))
        return {
            "summary": " | ".join(summaries) if summaries else "No summary.",
            "deliverables": deliverables,
            "risks": risks,
            "results": results,
        }

    def _next_messages(
        self,
        *,
        team: dict[str, Any],
        message: dict[str, Any],
        group: dict[str, Any],
        merged: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, Any] | None]:
        current_level = int(group.get("target_level") or self._member(team, str(message.get("target_agent_id") or "")).get("level") or 0)
        current_targets = [str(item) for item in list(group.get("expected_targets") or []) if str(item).strip()]
        representative = sorted(current_targets)[0] if current_targets else str(message.get("target_agent_id") or "")
        phase = str(group.get("phase") or message.get("phase") or "down")
        finish_agent_ids = {str(item) for item in list(team.get("finish_agent_ids") or [])}
        if representative in finish_agent_ids and phase == "up":
            return [], self._delivery_message(source_agent_id=representative, phase=phase, merged=merged), None
        next_level = self._next_level(team, current_level, phase=phase)
        if next_level is None:
            if phase == "down":
                next_level = self._next_level(team, current_level, phase="up")
                if next_level is None:
                    return [], self._delivery_message(source_agent_id=representative, phase="up", merged=merged), None
                phase = "up"
            else:
                return [], self._delivery_message(source_agent_id=representative, phase=phase, merged=merged), None
        targets = [str(item) for item in list((team.get("tiers") or {}).get(str(next_level), [])) if str(item).strip()]
        group_id = make_id("group")
        follow_ups: list[dict[str, Any]] = []
        next_group = {
            "group_id": group_id,
            "phase": phase,
            "target_level": next_level,
            "expected_targets": targets,
            "completed": [],
            "results": {},
            "source_level": current_level,
        }
        for target in targets:
            follow_ups.append(
                {
                    "message_id": make_id("teammsg"),
                    "group_id": group_id,
                    "source_actor_id": representative,
                    "target_agent_id": target,
                    "message_type": "handoff",
                    "phase": phase,
                    "body": merged["summary"],
                    "source_level": current_level,
                    "target_level": next_level,
                    "context_outputs": {**dict(merged.get("results") or {}), f"tier_{current_level}": merged},
                }
            )
        if not targets:
            return [], self._delivery_message(source_agent_id=representative, phase=phase, merged=merged), None
        return follow_ups, None, next_group

    def _delivery_message(self, *, source_agent_id: str, phase: str, merged: dict[str, Any]) -> dict[str, Any]:
        return {
            "message_id": make_id("teammsg"),
            "source_actor_id": source_agent_id,
            "target_agent_id": None,
            "message_type": "delivery",
            "phase": phase,
            "body": merged.get("summary") or "Team completed the task.",
            "payload": merged,
        }

    def _resolve_summary(self, runtime_state: dict[str, Any]) -> str:
        artifact = dict(runtime_state.get("delivery_artifact") or {})
        if artifact.get("summary"):
            return str(artifact["summary"])
        message = dict(runtime_state.get("final_delivery_message") or {})
        if message.get("body"):
            return str(message["body"])
        outputs = list((runtime_state.get("agent_outputs") or {}).values())
        for item in reversed(outputs):
            if isinstance(item, dict) and item.get("summary"):
                return str(item["summary"])
        return "Team run completed."

    def _render_delivery_markdown(self, payload: dict[str, Any]) -> str:
        summary = str(payload.get("summary") or "No summary.")
        deliverables = list(payload.get("deliverables") or [])
        risks = list(payload.get("risks") or [])
        lines = ["# Team Summary", "", summary, "", "## Deliverables"]
        if deliverables:
            lines.extend(f"- {item}" for item in deliverables)
        else:
            lines.append("- none")
        lines.extend(["", "## Risks"])
        if risks:
            lines.extend(f"- {item}" for item in risks)
        else:
            lines.append("- none")
        return "\n".join(lines) + "\n"
