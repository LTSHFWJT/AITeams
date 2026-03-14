from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class MemoryActionType(StrEnum):
    ADD = "ADD"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    NONE = "NONE"


@dataclass(slots=True)
class MemoryScopeContext:
    user_id: str | None = None
    agent_id: str | None = None
    owner_agent_id: str | None = None
    subject_type: str | None = None
    subject_id: str | None = None
    interaction_type: str | None = None
    platform_id: str | None = None
    workspace_id: str | None = None
    team_id: str | None = None
    project_id: str | None = None
    namespace_key: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    actor_id: str | None = None
    role: str | None = None

    def as_metadata(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "user_id": self.user_id,
                "agent_id": self.agent_id,
                "owner_agent_id": self.owner_agent_id,
                "subject_type": self.subject_type,
                "subject_id": self.subject_id,
                "interaction_type": self.interaction_type,
                "platform_id": self.platform_id,
                "workspace_id": self.workspace_id,
                "team_id": self.team_id,
                "project_id": self.project_id,
                "namespace_key": self.namespace_key,
                "session_id": self.session_id,
                "run_id": self.run_id,
                "actor_id": self.actor_id,
                "role": self.role,
            }.items()
            if value is not None
        }

    def clone_with(self, **kwargs: Any) -> "MemoryScopeContext":
        payload = {
            "user_id": self.user_id,
            "agent_id": self.agent_id,
            "owner_agent_id": self.owner_agent_id,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "interaction_type": self.interaction_type,
            "platform_id": self.platform_id,
            "workspace_id": self.workspace_id,
            "team_id": self.team_id,
            "project_id": self.project_id,
            "namespace_key": self.namespace_key,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "actor_id": self.actor_id,
            "role": self.role,
        }
        payload.update(kwargs)
        return MemoryScopeContext(**payload)


@dataclass(slots=True)
class MessagePart:
    kind: str
    text: str | None = None
    payload: Any = None


@dataclass(slots=True)
class NormalizedMessage:
    role: str
    content: str
    actor_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    parts: list[MessagePart] = field(default_factory=list)


@dataclass(slots=True)
class FactCandidate:
    text: str
    memory_type: str
    confidence: float
    importance: float
    source: str = "conversation"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NeighborMemory:
    id: str
    text: str
    score: float
    scope: str | None = None
    memory_type: str | None = None
    importance: float = 0.5
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MemoryAction:
    action_type: MemoryActionType
    candidate: FactCandidate
    reason: str
    target_id: str | None = None
    previous_text: str | None = None
    confidence: float = 0.0
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MemoryMutationResult:
    event: str
    memory: str
    reason: str
    id: str | None = None
    confidence: float = 0.0
    previous_memory: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event": self.event,
            "memory": self.memory,
            "reason": self.reason,
            "confidence": round(float(self.confidence), 6),
            "evidence": dict(self.evidence),
        }
        if self.id is not None:
            payload["id"] = self.id
        if self.previous_memory is not None:
            payload["previous_memory"] = self.previous_memory
        return payload
