from __future__ import annotations

from collections import deque
from copy import deepcopy
from typing import Any

from aiteams.domain.models import BlueprintSpec, SUPPORTED_NODE_TYPES
from aiteams.runtime.compiler import BlueprintCompiler


CANVAS_X_STEP = 220
CANVAS_Y_STEP = 140


def normalize_team_template_spec(spec: dict[str, Any] | None) -> dict[str, Any]:
    payload = deepcopy(spec or {})
    agents = payload.get("agents") or []
    if isinstance(agents, dict):
        normalized_agents: list[dict[str, Any]] = []
        for key, value in agents.items():
            item = dict(value or {})
            item.setdefault("key", str(key))
            normalized_agents.append(item)
        agents = normalized_agents
    else:
        agents = [dict(item or {}) for item in agents]

    flow = dict(payload.get("flow") or {})
    nodes = [dict(item or {}) for item in flow.get("nodes", [])]
    edges = []
    for item in flow.get("edges", []):
        edge = dict(item or {})
        source = edge.get("from") or edge.get("source")
        target = edge.get("to") or edge.get("target")
        normalized = {"from": str(source or "").strip(), "to": str(target or "").strip()}
        when = edge.get("when")
        if when not in (None, ""):
            normalized["when"] = str(when)
        edges.append(normalized)

    metadata = dict(payload.get("metadata") or {})
    ui_layout = dict(metadata.get("ui_layout") or {})
    positions = dict(ui_layout.get("positions") or {})
    auto_positions = _auto_layout(nodes, edges)
    normalized_positions: dict[str, dict[str, int]] = {}
    for node in nodes:
        node_id = str(node.get("id") or "").strip()
        if not node_id:
            continue
        current = positions.get(node_id) or auto_positions.get(node_id) or {"x": 0, "y": 0}
        normalized_positions[node_id] = {
            "x": int(current.get("x", 0) or 0),
            "y": int(current.get("y", 0) or 0),
        }
        node["id"] = node_id
        node["type"] = str(node.get("type") or "agent").strip()

    metadata["communication_policy"] = str(metadata.get("communication_policy") or "graph-ancestor-scoped")
    metadata["ui_layout"] = {
        "positions": normalized_positions,
        "viewport": dict(ui_layout.get("viewport") or {"x": 0, "y": 0, "zoom": 1}),
    }

    return {
        "workspace_id": str(payload.get("workspace_id") or "local-workspace"),
        "project_id": str(payload.get("project_id") or "default-project"),
        "agents": agents,
        "flow": {"nodes": nodes, "edges": edges},
        "definition_of_done": [str(item) for item in payload.get("definition_of_done", [])],
        "acceptance_checks": [str(item) for item in payload.get("acceptance_checks", [])],
        "metadata": metadata,
    }


def validate_team_template_spec(spec: dict[str, Any] | None) -> dict[str, Any]:
    normalized = normalize_team_template_spec(spec)
    errors: list[str] = []
    warnings: list[str] = []

    agents = normalized["agents"]
    flow = normalized["flow"]
    nodes = flow["nodes"]
    edges = flow["edges"]
    agent_keys = [str(item.get("key") or "").strip() for item in agents]
    node_ids = [str(item.get("id") or "").strip() for item in nodes]
    node_map = {node_id: item for node_id, item in zip(node_ids, nodes, strict=False) if node_id}
    outgoing = _outgoing(edges)
    incoming = _incoming(edges)

    if not agents:
        errors.append("团队模板至少需要一个成员。")
    if any(not key for key in agent_keys):
        errors.append("每个团队成员都必须有 key。")
    if len(set(agent_keys)) != len([key for key in agent_keys if key]):
        errors.append("团队成员 key 必须唯一。")
    for agent in agents:
        if not str(agent.get("agent_template_ref") or agent.get("agent_template_id") or agent.get("agent_template_key") or "").strip():
            errors.append(f"成员 `{agent.get('key') or 'unknown'}` 缺少 agent_template_ref。")

    if not nodes:
        errors.append("流程至少需要一个节点。")
    if any(not node_id for node_id in node_ids):
        errors.append("每个流程节点都必须有非空 id。")
    if len(set(node_ids)) != len([node_id for node_id in node_ids if node_id]):
        errors.append("流程节点 id 必须唯一。")

    for node in nodes:
        node_id = str(node.get("id") or "").strip()
        node_type = str(node.get("type") or "").strip()
        if node_type not in SUPPORTED_NODE_TYPES:
            errors.append(f"节点 `{node_id or 'unknown'}` 使用了不支持的类型 `{node_type}`。")
            continue
        if node_type == "agent" and str(node.get("agent") or "").strip() not in set(agent_keys):
            errors.append(f"Agent 节点 `{node_id}` 引用了不存在的团队成员 `{node.get('agent')}`。")
        if node_type == "condition" and not str(node.get("expr") or "").strip():
            errors.append(f"条件节点 `{node_id}` 缺少 expr。")
        if node_type == "loop":
            max_iterations = node.get("max_iterations")
            if max_iterations in (None, "") or int(max_iterations) < 1:
                errors.append(f"循环节点 `{node_id}` 需要 max_iterations >= 1。")
        if node_type == "approval" and not str(node.get("name") or "").strip():
            errors.append(f"审批节点 `{node_id}` 需要 name。")
        if node_type == "artifact" and not (str(node.get("template") or "").strip() or str(node.get("source") or "").strip()):
            errors.append(f"产物节点 `{node_id}` 需要 template 或 source。")

    for edge in edges:
        source = str(edge.get("from") or "").strip()
        target = str(edge.get("to") or "").strip()
        if not source or not target:
            errors.append("所有连线都必须同时包含 from 和 to。")
            continue
        if source not in node_map or target not in node_map:
            errors.append(f"连线 `{source} -> {target}` 引用了不存在的节点。")

    start_nodes = [node for node in nodes if node.get("type") == "start"]
    end_nodes = [node for node in nodes if node.get("type") == "end"]
    if len(start_nodes) != 1:
        errors.append("流程必须且只能有一个 start 节点。")
    if not end_nodes:
        errors.append("流程至少需要一个 end 节点。")

    for node in nodes:
        node_id = str(node.get("id") or "").strip()
        node_type = str(node.get("type") or "").strip()
        out_count = len(outgoing.get(node_id, []))
        in_count = len(incoming.get(node_id, []))
        if node_type == "start" and in_count:
            errors.append("start 节点不能有入边。")
        if node_type == "end" and out_count:
            errors.append(f"end 节点 `{node_id}` 不能有出边。")
        if node_type not in {"condition", "router", "parallel"} and node_type != "end" and out_count > 1:
            errors.append(f"节点 `{node_id}` 当前运行时只支持单一出口。")
        if node_type == "parallel" and out_count < 2:
            errors.append(f"并行节点 `{node_id}` 至少需要两条分支。")
        if node_type == "merge" and in_count < 2:
            warnings.append(f"汇聚节点 `{node_id}` 建议至少有两条入边。")

    for node in nodes:
        if str(node.get("type") or "") == "parallel":
            merge_id = _resolve_parallel_merge(str(node.get("id") or ""), outgoing, node_map)
            if merge_id is None:
                errors.append(f"并行节点 `{node.get('id')}` 的分支未汇聚到同一个 merge 节点。")

    if not errors and start_nodes:
        start_id = str(start_nodes[0].get("id") or "")
        reachable = _reachable_nodes(start_id, outgoing)
        isolated = [node_id for node_id in node_ids if node_id and node_id not in reachable]
        if isolated:
            warnings.append(f"存在未连通节点: {', '.join(sorted(isolated))}。")

        try:
            compiler = BlueprintCompiler()
            compiled = compiler.compile(_graph_blueprint_payload(normalized))
            communication = {
                node_id: {
                    "visible_ancestors": compiled.visible_ancestors(node_id),
                    "visible_agent_nodes": [
                        ancestor_id
                        for ancestor_id in compiled.visible_ancestors(node_id)
                        if compiled.nodes[ancestor_id].type == "agent"
                    ],
                }
                for node_id in compiled.nodes
            }
        except Exception as exc:
            errors.append(str(exc))
            communication = {}
    else:
        communication = {}

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "summary": {
            "agent_count": len([item for item in agents if item.get("key")]),
            "node_count": len([item for item in nodes if item.get("id")]),
            "edge_count": len([item for item in edges if item.get("from") and item.get("to")]),
            "node_types": _node_type_counts(nodes),
            "communication_policy": normalized["metadata"]["communication_policy"],
        },
        "normalized_spec": normalized,
        "communication": communication,
    }


def _graph_blueprint_payload(spec: dict[str, Any]) -> dict[str, Any]:
    agents: dict[str, dict[str, Any]] = {}
    role_templates: dict[str, dict[str, Any]] = {}
    for member in spec.get("agents", []):
        key = str(member.get("key") or "").strip()
        if not key:
            continue
        role_template_key = f"role_{key}"
        role_templates[role_template_key] = {
            "name": str(member.get("name") or key),
            "role": str(member.get("role") or "agent"),
            "model": "mock-model",
            "backend": "mock",
            "provider_type": "mock",
        }
        agents[key] = {
            "name": str(member.get("name") or key),
            "role": str(member.get("role") or "agent"),
            "role_template": role_template_key,
        }
    return {
        "name": "team-graph-validation",
        "description": "",
        "workspace_id": spec["workspace_id"],
        "project_id": spec["project_id"],
        "version": "graph-check",
        "role_templates": role_templates,
        "agents": agents,
        "workbenches": {},
        "flow": spec.get("flow") or {},
        "definition_of_done": spec.get("definition_of_done") or [],
        "acceptance_checks": spec.get("acceptance_checks") or [],
        "metadata": spec.get("metadata") or {},
    }


def _auto_layout(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    node_ids = [str(node.get("id") or "").strip() for node in nodes if str(node.get("id") or "").strip()]
    if not node_ids:
        return {}
    outgoing = _outgoing(edges)
    incoming = _incoming(edges)
    start_id = next((str(node.get("id")) for node in nodes if node.get("type") == "start"), node_ids[0])
    queue: deque[tuple[str, int]] = deque([(start_id, 0)])
    seen: set[str] = set()
    columns: dict[int, list[str]] = {}
    while queue:
        node_id, depth = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        columns.setdefault(depth, []).append(node_id)
        for edge in outgoing.get(node_id, []):
            queue.append((str(edge["to"]), depth + 1))
    for node_id in node_ids:
        if node_id not in seen:
            columns.setdefault(max(columns, default=0) + 1, []).append(node_id)
    positions: dict[str, dict[str, int]] = {}
    for depth, items in columns.items():
        for row, node_id in enumerate(items):
            positions[node_id] = {"x": depth * CANVAS_X_STEP, "y": row * CANVAS_Y_STEP}
    return positions


def _reachable_nodes(start_id: str, outgoing: dict[str, list[dict[str, Any]]]) -> set[str]:
    visited: set[str] = set()
    stack = [start_id]
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        for edge in outgoing.get(current, []):
            stack.append(str(edge["to"]))
    return visited


def _resolve_parallel_merge(node_id: str, outgoing: dict[str, list[dict[str, Any]]], node_map: dict[str, dict[str, Any]]) -> str | None:
    branch_targets = [str(edge["to"]) for edge in outgoing.get(node_id, [])]
    if len(branch_targets) < 2:
        return None
    reachable_merges = [_find_reachable_merges(branch_id, outgoing, node_map) for branch_id in branch_targets]
    if not reachable_merges:
        return None
    common = set.intersection(*reachable_merges)
    return sorted(common)[0] if common else None


def _find_reachable_merges(
    start_id: str,
    outgoing: dict[str, list[dict[str, Any]]],
    node_map: dict[str, dict[str, Any]],
) -> set[str]:
    merges: set[str] = set()
    visited: set[str] = set()
    stack = [start_id]
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        if str((node_map.get(current) or {}).get("type") or "") == "merge":
            merges.add(current)
        for edge in outgoing.get(current, []):
            stack.append(str(edge["to"]))
    return merges


def _node_type_counts(nodes: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for node in nodes:
        node_type = str(node.get("type") or "").strip()
        if not node_type:
            continue
        counts[node_type] = counts.get(node_type, 0) + 1
    return counts


def _outgoing(edges: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    items: dict[str, list[dict[str, Any]]] = {}
    for edge in edges:
        items.setdefault(str(edge.get("from") or ""), []).append(edge)
    return items


def _incoming(edges: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    items: dict[str, list[dict[str, Any]]] = {}
    for edge in edges:
        items.setdefault(str(edge.get("to") or ""), []).append(edge)
    return items
