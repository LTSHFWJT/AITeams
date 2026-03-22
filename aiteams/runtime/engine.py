from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from aiteams.agent.kernel import AgentKernel, AgentRunContext
from aiteams.common import events as event_types
from aiteams.common.expressions import evaluate_expression
from aiteams.domain.models import BlueprintSpec, NodeSpec
from aiteams.runtime.compiler import BlueprintCompiler, CompiledBlueprint
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import pretty_json, render_template, resolve_path, trim_text, utcnow_iso
from aiteams.workspace.manager import WorkspaceManager


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
    ):
        self.store = store
        self.compiler = compiler
        self.agent_kernel = agent_kernel
        self.workspace = workspace

    async def start_task(
        self,
        *,
        blueprint: dict[str, Any],
        title: str | None,
        prompt: str,
        inputs: dict[str, Any],
        approval_mode: str,
    ) -> dict[str, Any]:
        spec = BlueprintSpec.from_dict(blueprint["spec_json"] if "spec_json" in blueprint else blueprint)
        task = self.store.create_task_release(
            blueprint_id=str(blueprint["id"]) if "id" in blueprint else "",
            workspace_id=spec.workspace_id,
            project_id=spec.project_id,
            title=title,
            prompt=prompt,
            inputs=inputs,
            approval_mode=approval_mode,
        )
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
        self.store.update_task_release(str(task["id"]), status="running")
        self.store.add_event(run_id=str(run["id"]), event_type=event_types.RUN_CREATED, payload={"run_id": run["id"], "task_release_id": task["id"]})
        return await self._continue_run(str(run["id"]), compiled=self.compiler.compile(spec))

    async def resume_run(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise ValueError("Run does not exist.")
        bundle = self.store.get_run_bundle(run_id)
        assert bundle is not None
        blueprint = bundle["blueprint"]
        if blueprint is None:
            raise ValueError("Run blueprint does not exist.")
        return await self._continue_run(run_id, compiled=self.compiler.compile(blueprint["spec_json"]))

    async def _continue_run(self, run_id: str, *, compiled: CompiledBlueprint) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise ValueError("Run does not exist.")
        state = dict(run.get("state_json") or {})
        waiting = state.get("waiting")
        if waiting:
            approval = self.store.get_approval(str(waiting["approval_id"]))
            if approval is None or approval["status"] == "pending":
                return self.store.get_run_bundle(run_id) or {}
            if approval["status"] == "rejected":
                self.store.update_run(run_id, status="failed", finished_at=utcnow_iso(), summary="Approval rejected.", state=state)
                self.store.update_task_release(str(run["task_release_id"]), status="failed")
                return self.store.get_run_bundle(run_id) or {}
            pending_step = self.store.latest_step_for_node(run_id, str(waiting["node_id"]))
            if pending_step is not None and pending_step["status"] == "blocked":
                self.store.update_step(
                    str(pending_step["id"]),
                    status="done",
                    output_payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
                    finished=True,
                )
            state["outputs"][str(waiting["node_id"])] = {
                "approval_id": approval["id"],
                "status": approval["status"],
                "resolution": approval["resolution_json"],
            }
            state["waiting"] = None
            self.store.add_event(
                run_id=run_id,
                event_type=event_types.APPROVAL_RESOLVED,
                payload={"approval_id": approval["id"], "resolution": approval["resolution_json"]},
                step_id=pending_step["id"] if pending_step else None,
            )
            next_node_id = str(waiting["next_node_id"])
        else:
            next_node_id = run.get("current_node_id") or compiled.start_node_id
        started_at = run.get("started_at") or utcnow_iso()
        self.store.update_run(run_id, status="running", state=state, started_at=started_at, current_node_id=next_node_id)

        while next_node_id:
            result = await self._execute_node(run_id, compiled, state, next_node_id)
            if result.status == "waiting_approval":
                return self.store.get_run_bundle(run_id) or {}
            next_node_id = result.next_node_id

        summary = self._resolve_run_summary(state)
        if not self._acceptance_checks_pass(compiled.blueprint, state):
            self.store.update_run(run_id, status="failed", summary=summary, state=state, finished_at=utcnow_iso(), result=state.get("outputs", {}))
            self.store.update_task_release(str(run["task_release_id"]), status="failed")
            self.store.add_event(run_id=run_id, event_type=event_types.RUN_FAILED, payload={"summary": summary})
            return self.store.get_run_bundle(run_id) or {}
        self.store.update_run(run_id, status="completed", summary=summary, state=state, result=state.get("outputs", {}), finished_at=utcnow_iso())
        self.store.update_task_release(str(run["task_release_id"]), status="completed")
        self.store.add_event(run_id=run_id, event_type=event_types.RUN_COMPLETED, payload={"summary": summary})
        return self.store.get_run_bundle(run_id) or {}

    async def _execute_node(self, run_id: str, compiled: CompiledBlueprint, state: dict[str, Any], node_id: str, *, stop_before_merge: bool = False) -> NodeExecutionResult:
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
        self.store.add_event(run_id=run_id, step_id=str(step["id"]), event_type=event_types.STEP_STARTED, payload={"node_id": node.id, "node_type": node.type, "attempt": attempt})

        try:
            next_node_id: str | None
            output: dict[str, Any]
            if node.type == "start":
                output = {"status": "started"}
                next_node_id = compiled.single_next(node.id)
            elif node.type == "agent":
                output = await self._run_agent_node(run_id, compiled, node, state)
                self.store.add_event(run_id=run_id, step_id=str(step["id"]), event_type=event_types.AGENT_MESSAGE, payload={"node_id": node.id, "summary": output.get("summary", "")})
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
                    self.store.add_event(run_id=run_id, step_id=str(step["id"]), event_type=event_types.APPROVAL_REQUESTED, payload={"approval_id": approval["id"], "title": approval["title"]})
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
            self.store.update_step(str(step["id"]), status="done", output_payload=output, finished=True)
            self.store.update_run(run_id, status="running", current_node_id=next_node_id, state=state)
            self.store.save_checkpoint(run_id=run_id, step_id=str(step["id"]), node_id=node.id, snapshot=state)
            self.store.add_event(run_id=run_id, step_id=str(step["id"]), event_type=event_types.STEP_COMPLETED, payload={"node_id": node.id, "node_type": node.type, "output": output})
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
