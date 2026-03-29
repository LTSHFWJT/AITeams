from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class TeamMemberRuntimeSpec:
    key: str
    name: str
    level: int
    agent_definition: dict[str, Any]
    provider_profile: dict[str, Any]
    plugins: list[dict[str, Any]] = field(default_factory=list)
    skills: list[dict[str, Any]] = field(default_factory=list)
    static_memory: dict[str, Any] | None = None
    knowledge_bases: list[dict[str, Any]] = field(default_factory=list)
    review_policies: list[dict[str, Any]] = field(default_factory=list)
    reports_to: list[str] = field(default_factory=list)
    runtime_plugin_actions: list[dict[str, Any]] = field(default_factory=list)
    can_receive_task: bool = False
    can_finish_task: bool = False
    peer_chat_enabled: bool = True

    @property
    def role(self) -> str:
        return str(self.agent_definition.get("role") or "agent")

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "name": self.name,
            "role": self.role,
            "level": self.level,
            "agent_definition_id": self.agent_definition.get("id"),
            "provider_profile_id": self.provider_profile.get("id"),
            "provider_type": self.provider_profile.get("provider_type"),
            "plugin_ids": [item.get("id") for item in self.plugins],
            "plugin_keys": [item.get("key") for item in self.plugins],
            "skill_keys": [item.get("key") for item in self.skills],
            "static_memory_key": self.static_memory.get("key") if self.static_memory else None,
            "knowledge_base_keys": [item.get("key") for item in self.knowledge_bases],
            "review_policy_keys": [item.get("key") for item in self.review_policies],
            "review_policies": [
                {
                    "id": item.get("id"),
                    "key": item.get("key"),
                    "name": item.get("name"),
                    "version": item.get("version"),
                    "spec": dict(item.get("spec_json") or {}),
                }
                for item in self.review_policies
            ],
            "reports_to": list(self.reports_to),
            "runtime_plugin_actions": [dict(item) for item in self.runtime_plugin_actions],
            "can_receive_task": self.can_receive_task,
            "can_finish_task": self.can_finish_task,
            "peer_chat_enabled": self.peer_chat_enabled,
        }
