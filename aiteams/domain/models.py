from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


SUPPORTED_NODE_TYPES = {
    "start",
    "agent",
    "parallel",
    "router",
    "condition",
    "loop",
    "approval",
    "artifact",
    "subflow",
    "merge",
    "end",
}

SUPPORTED_MEMORY_POLICIES = {
    "agent_private",
    "project_shared",
    "agent_private_plus_project",
    "run_retrospective",
}


@dataclass(slots=True)
class WorkbenchSpec:
    key: str
    name: str
    tools: list[str] = field(default_factory=list)
    permissions: list[str] = field(default_factory=list)
    description: str = ""

    @classmethod
    def from_dict(cls, key: str, payload: dict[str, Any]) -> "WorkbenchSpec":
        return cls(
            key=key,
            name=str(payload.get("name") or key),
            tools=[str(item) for item in payload.get("tools", [])],
            permissions=[str(item) for item in payload.get("permissions", [])],
            description=str(payload.get("description") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "tools": list(self.tools),
            "permissions": list(self.permissions),
            "description": self.description,
        }


@dataclass(slots=True)
class RoleTemplateSpec:
    key: str
    name: str
    role: str
    goal: str = ""
    instructions: str = ""
    backend: str = "mock"
    provider_type: str = "mock"
    model: str = "mock-model"
    base_url: str | None = None
    api_key: str | None = None
    api_key_env: str | None = None
    api_version: str | None = None
    organization: str | None = None
    temperature: float = 0.2
    max_tokens: int | None = None
    extra_headers: dict[str, Any] = field(default_factory=dict)
    extra_config: dict[str, Any] = field(default_factory=dict)
    workbenches: list[str] = field(default_factory=list)
    memory_policy: str = "agent_private"
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, key: str, payload: dict[str, Any]) -> "RoleTemplateSpec":
        memory_policy = str(payload.get("memory_policy") or "agent_private")
        if memory_policy not in SUPPORTED_MEMORY_POLICIES:
            raise ValueError(f"Unsupported memory_policy `{memory_policy}` for role template `{key}`.")
        provider_type = str(payload.get("provider_type") or payload.get("backend") or "mock")
        return cls(
            key=key,
            name=str(payload.get("name") or key),
            role=str(payload.get("role") or key),
            goal=str(payload.get("goal") or ""),
            instructions=str(payload.get("instructions") or ""),
            backend=str(payload.get("backend") or "mock"),
            provider_type=provider_type,
            model=str(payload.get("model") or "mock-model"),
            base_url=payload.get("base_url"),
            api_key=payload.get("api_key"),
            api_key_env=payload.get("api_key_env"),
            api_version=payload.get("api_version"),
            organization=payload.get("organization"),
            temperature=float(payload.get("temperature", 0.2)),
            max_tokens=int(payload["max_tokens"]) if payload.get("max_tokens") is not None else None,
            extra_headers=dict(payload.get("extra_headers") or {}),
            extra_config=dict(payload.get("extra_config") or {}),
            workbenches=[str(item) for item in payload.get("workbenches", [])],
            memory_policy=memory_policy,
            metadata=dict(payload.get("metadata") or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "name": self.name,
            "role": self.role,
            "goal": self.goal,
            "instructions": self.instructions,
            "backend": self.backend,
            "provider_type": self.provider_type,
            "model": self.model,
            "base_url": self.base_url,
            "api_key": self.api_key,
            "api_key_env": self.api_key_env,
            "api_version": self.api_version,
            "organization": self.organization,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "extra_headers": dict(self.extra_headers),
            "extra_config": dict(self.extra_config),
            "workbenches": list(self.workbenches),
            "memory_policy": self.memory_policy,
            "metadata": dict(self.metadata),
        }
        return {key: value for key, value in payload.items() if value not in (None, "", {}, [])}


@dataclass(slots=True)
class AgentSpec:
    key: str
    name: str
    role: str
    role_template: str | None = None
    goal: str = ""
    instructions: str = ""
    backend: str = "mock"
    provider_type: str = "mock"
    model: str = "mock-model"
    base_url: str | None = None
    api_key: str | None = None
    api_key_env: str | None = None
    api_version: str | None = None
    organization: str | None = None
    temperature: float = 0.2
    max_tokens: int | None = None
    extra_headers: dict[str, Any] = field(default_factory=dict)
    extra_config: dict[str, Any] = field(default_factory=dict)
    workbenches: list[str] = field(default_factory=list)
    memory_policy: str = "agent_private"
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(
        cls,
        key: str,
        payload: dict[str, Any],
        role_templates: dict[str, RoleTemplateSpec] | None = None,
    ) -> "AgentSpec":
        role_template_key = payload.get("role_template") or payload.get("template")
        template = None
        if role_template_key is not None:
            if not role_templates or str(role_template_key) not in role_templates:
                raise ValueError(f"Agent `{key}` references unknown role_template `{role_template_key}`.")
            template = role_templates[str(role_template_key)]
        memory_policy = str(payload.get("memory_policy") or (template.memory_policy if template else "agent_private"))
        if memory_policy not in SUPPORTED_MEMORY_POLICIES:
            raise ValueError(f"Unsupported memory_policy `{memory_policy}` for agent `{key}`.")
        template_metadata = dict(template.metadata) if template else {}
        template_workbenches = list(template.workbenches) if template else []
        metadata = dict(template_metadata)
        metadata.update(dict(payload.get("metadata") or {}))
        provider_type = str(
            payload.get("provider_type")
            or payload.get("backend")
            or (template.provider_type if template else None)
            or (template.backend if template else None)
            or "mock"
        )
        return cls(
            key=key,
            name=str(payload.get("name") or (template.name if template else key)),
            role=str(payload.get("role") or (template.role if template else key)),
            role_template=str(role_template_key) if role_template_key is not None else None,
            goal=str(payload.get("goal") or (template.goal if template else "")),
            instructions=str(payload.get("instructions") or (template.instructions if template else "")),
            backend=str(payload.get("backend") or (template.backend if template else "mock")),
            provider_type=provider_type,
            model=str(payload.get("model") or (template.model if template else "mock-model")),
            base_url=payload.get("base_url", template.base_url if template else None),
            api_key=payload.get("api_key", template.api_key if template else None),
            api_key_env=payload.get("api_key_env", template.api_key_env if template else None),
            api_version=payload.get("api_version", template.api_version if template else None),
            organization=payload.get("organization", template.organization if template else None),
            temperature=float(payload.get("temperature", template.temperature if template else 0.2)),
            max_tokens=int(payload["max_tokens"]) if payload.get("max_tokens") is not None else None,
            extra_headers=dict(payload.get("extra_headers", template.extra_headers if template else {})),
            extra_config=dict(payload.get("extra_config", template.extra_config if template else {})),
            workbenches=[str(item) for item in payload.get("workbenches", template_workbenches)],
            memory_policy=memory_policy,
            metadata=metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "name": self.name,
            "role": self.role,
            "role_template": self.role_template,
            "goal": self.goal,
            "instructions": self.instructions,
            "backend": self.backend,
            "provider_type": self.provider_type,
            "model": self.model,
            "base_url": self.base_url,
            "api_key": self.api_key,
            "api_key_env": self.api_key_env,
            "api_version": self.api_version,
            "organization": self.organization,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "extra_headers": dict(self.extra_headers),
            "extra_config": dict(self.extra_config),
            "workbenches": list(self.workbenches),
            "memory_policy": self.memory_policy,
            "metadata": dict(self.metadata),
        }
        return {key: value for key, value in payload.items() if value not in (None, "", {}, [])}


@dataclass(slots=True)
class NodeSpec:
    id: str
    type: str
    agent: str | None = None
    instruction: str | None = None
    expr: str | None = None
    template: str | None = None
    source: str | None = None
    name: str | None = None
    artifact_kind: str = "report"
    max_iterations: int | None = None
    auto_approve: bool | None = None
    config: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NodeSpec":
        node_type = str(payload.get("type") or "").strip()
        if node_type not in SUPPORTED_NODE_TYPES:
            raise ValueError(f"Unsupported node type: {node_type}")
        return cls(
            id=str(payload.get("id") or "").strip(),
            type=node_type,
            agent=payload.get("agent"),
            instruction=payload.get("instruction"),
            expr=payload.get("expr"),
            template=payload.get("template"),
            source=payload.get("source"),
            name=payload.get("name"),
            artifact_kind=str(payload.get("artifact_kind") or "report"),
            max_iterations=int(payload["max_iterations"]) if payload.get("max_iterations") is not None else None,
            auto_approve=payload.get("auto_approve"),
            config=dict(payload.get("config") or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "id": self.id,
            "type": self.type,
            "agent": self.agent,
            "instruction": self.instruction,
            "expr": self.expr,
            "template": self.template,
            "source": self.source,
            "name": self.name,
            "artifact_kind": self.artifact_kind,
            "max_iterations": self.max_iterations,
            "auto_approve": self.auto_approve,
            "config": dict(self.config),
        }
        return {key: value for key, value in payload.items() if value not in (None, "", {}, [])}


@dataclass(slots=True)
class EdgeSpec:
    source: str
    target: str
    when: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EdgeSpec":
        return cls(
            source=str(payload.get("from") or payload.get("source") or "").strip(),
            target=str(payload.get("to") or payload.get("target") or "").strip(),
            when=payload.get("when"),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {"from": self.source, "to": self.target}
        if self.when not in (None, ""):
            payload["when"] = self.when
        return payload


@dataclass(slots=True)
class FlowSpec:
    nodes: list[NodeSpec]
    edges: list[EdgeSpec]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FlowSpec":
        nodes = [NodeSpec.from_dict(item) for item in payload.get("nodes", [])]
        edges = [EdgeSpec.from_dict(item) for item in payload.get("edges", [])]
        if not nodes:
            raise ValueError("Flow must contain at least one node.")
        return cls(nodes=nodes, edges=edges)

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [item.to_dict() for item in self.nodes],
            "edges": [item.to_dict() for item in self.edges],
        }


@dataclass(slots=True)
class BlueprintSpec:
    name: str
    description: str
    workspace_id: str
    project_id: str
    version: str = "v1"
    role_templates: dict[str, RoleTemplateSpec] = field(default_factory=dict)
    agents: dict[str, AgentSpec] = field(default_factory=dict)
    workbenches: dict[str, WorkbenchSpec] = field(default_factory=dict)
    flow: FlowSpec = field(default_factory=lambda: FlowSpec(nodes=[], edges=[]))
    definition_of_done: list[str] = field(default_factory=list)
    acceptance_checks: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BlueprintSpec":
        name = str(payload.get("name") or "").strip()
        if not name:
            raise ValueError("Blueprint name is required.")
        workspace_id = str(payload.get("workspace_id") or "local-workspace").strip()
        project_id = str(payload.get("project_id") or "default-project").strip()
        role_template_payload = payload.get("role_templates") or {}
        role_templates = {key: RoleTemplateSpec.from_dict(key, value) for key, value in role_template_payload.items()}
        agents_payload = payload.get("agents") or payload.get("agent_instances") or {}
        if not isinstance(agents_payload, dict) or not agents_payload:
            raise ValueError("Blueprint must define at least one agent.")
        workbench_payload = payload.get("workbenches") or {}
        flow_payload = payload.get("flow") or {}
        spec = cls(
            name=name,
            description=str(payload.get("description") or ""),
            workspace_id=workspace_id,
            project_id=project_id,
            version=str(payload.get("version") or "v1"),
            role_templates=role_templates,
            agents={key: AgentSpec.from_dict(key, value, role_templates) for key, value in agents_payload.items()},
            workbenches={key: WorkbenchSpec.from_dict(key, value) for key, value in workbench_payload.items()},
            flow=FlowSpec.from_dict(flow_payload),
            definition_of_done=[str(item) for item in payload.get("definition_of_done", [])],
            acceptance_checks=[str(item) for item in payload.get("acceptance_checks", [])],
            metadata=dict(payload.get("metadata") or {}),
        )
        spec._validate()
        return spec

    def _validate(self) -> None:
        node_ids = [node.id for node in self.flow.nodes]
        if any(not node_id for node_id in node_ids):
            raise ValueError("Every flow node must have a non-empty id.")
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("Flow node ids must be unique.")
        edge_targets = {edge.target for edge in self.flow.edges}
        edge_sources = {edge.source for edge in self.flow.edges}
        node_set = set(node_ids)
        for edge in self.flow.edges:
            if edge.source not in node_set or edge.target not in node_set:
                raise ValueError(f"Flow edge references unknown node: {edge.source} -> {edge.target}")
        start_nodes = [node for node in self.flow.nodes if node.type == "start"]
        end_nodes = [node for node in self.flow.nodes if node.type == "end"]
        if len(start_nodes) != 1:
            raise ValueError("Flow must contain exactly one start node.")
        if not end_nodes:
            raise ValueError("Flow must contain at least one end node.")
        for node in self.flow.nodes:
            if node.type == "agent" and node.agent not in self.agents:
                raise ValueError(f"Agent node `{node.id}` references unknown agent `{node.agent}`.")
            if node.type == "start" and node.id in edge_targets:
                raise ValueError("Start node cannot have incoming edges.")
            if node.type == "end" and node.id in edge_sources:
                raise ValueError("End node cannot have outgoing edges.")
            if node.type == "condition" and not node.expr:
                raise ValueError(f"Condition node `{node.id}` requires expr.")
            if node.type == "loop" and (node.max_iterations is None or node.max_iterations < 1):
                raise ValueError(f"Loop node `{node.id}` requires max_iterations >= 1.")
            if node.type == "artifact" and not (node.template or node.source):
                raise ValueError(f"Artifact node `{node.id}` requires template or source.")
            if node.type == "approval" and not node.name:
                raise ValueError(f"Approval node `{node.id}` requires name.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "workspace_id": self.workspace_id,
            "project_id": self.project_id,
            "version": self.version,
            "role_templates": {key: template.to_dict() for key, template in self.role_templates.items()},
            "agents": {key: agent.to_dict() for key, agent in self.agents.items()},
            "workbenches": {key: workbench.to_dict() for key, workbench in self.workbenches.items()},
            "flow": self.flow.to_dict(),
            "definition_of_done": list(self.definition_of_done),
            "acceptance_checks": list(self.acceptance_checks),
            "metadata": dict(self.metadata),
        }
