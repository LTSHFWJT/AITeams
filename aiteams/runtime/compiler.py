from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aiteams.domain.models import BlueprintSpec, EdgeSpec, NodeSpec


@dataclass(slots=True)
class CompiledBlueprint:
    blueprint: BlueprintSpec
    nodes: dict[str, NodeSpec]
    outgoing: dict[str, list[EdgeSpec]]
    incoming: dict[str, list[EdgeSpec]]
    ancestors: dict[str, set[str]]
    start_node_id: str

    def next_edges(self, node_id: str) -> list[EdgeSpec]:
        return list(self.outgoing.get(node_id, []))

    def next_nodes(self, node_id: str) -> list[str]:
        return [edge.target for edge in self.next_edges(node_id)]

    def single_next(self, node_id: str) -> str | None:
        nodes = self.next_nodes(node_id)
        if not nodes:
            return None
        if len(nodes) != 1:
            raise ValueError(f"Node `{node_id}` does not have exactly one outgoing edge.")
        return nodes[0]

    def visible_ancestors(self, node_id: str) -> list[str]:
        return sorted(self.ancestors.get(node_id, set()))


class BlueprintCompiler:
    def compile(self, payload: dict[str, Any] | BlueprintSpec) -> CompiledBlueprint:
        blueprint = payload if isinstance(payload, BlueprintSpec) else BlueprintSpec.from_dict(payload)
        nodes = {node.id: node for node in blueprint.flow.nodes}
        outgoing: dict[str, list[EdgeSpec]] = {node_id: [] for node_id in nodes}
        incoming: dict[str, list[EdgeSpec]] = {node_id: [] for node_id in nodes}
        for edge in blueprint.flow.edges:
            outgoing[edge.source].append(edge)
            incoming[edge.target].append(edge)
        ancestors: dict[str, set[str]] = {node_id: set() for node_id in nodes}
        changed = True
        while changed:
            changed = False
            for edge in blueprint.flow.edges:
                candidate = ancestors[edge.source] | {edge.source}
                if not candidate.issubset(ancestors[edge.target]):
                    ancestors[edge.target].update(candidate)
                    changed = True
        start_node = next(node for node in blueprint.flow.nodes if node.type == "start")
        return CompiledBlueprint(
            blueprint=blueprint,
            nodes=nodes,
            outgoing=outgoing,
            incoming=incoming,
            ancestors=ancestors,
            start_node_id=start_node.id,
        )
