from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ProviderPayload:
    id: str | None = None
    name: str = ""
    provider_type: str = ""
    base_url: str | None = None
    api_key: str | None = None
    model: str = ""
    api_version: str | None = None
    organization: str | None = None
    extra_headers: dict[str, str] = field(default_factory=dict)
    extra_config: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ProviderTestPayload:
    provider_id: str | None = None
    config: ProviderPayload | None = None
    prompt: str = "Reply with READY and one short sentence."


@dataclass(slots=True)
class AgentPayload:
    id: str | None = None
    name: str = ""
    role: str = ""
    system_prompt: str = ""
    provider_id: str = ""
    model_override: str | None = None
    temperature: float = 0.2
    max_tokens: int | None = None
    collaboration_style: str = "specialist"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CollaborationRunPayload:
    title: str | None = None
    prompt: str = ""
    agent_ids: list[str] = field(default_factory=list)
    lead_agent_id: str | None = None
    rounds: int = 1
