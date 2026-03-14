from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, fields
from typing import Any, Mapping

SCOPE_FIELD_NAMES = (
    "user_id",
    "agent_id",
    "owner_agent_id",
    "subject_type",
    "subject_id",
    "interaction_type",
    "platform_id",
    "workspace_id",
    "team_id",
    "project_id",
    "namespace_key",
)

TEAM_SCOPE_FIELD_NAMES = (
    "platform_id",
    "workspace_id",
    "team_id",
    "project_id",
    "namespace_key",
)


def _clean_scope_value(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _filesystem_segment(value: str | None, default: str) -> str:
    raw = _clean_scope_value(value) or default
    cleaned = re.sub(r"[^0-9A-Za-z._-]+", "-", raw).strip("-._")
    return cleaned or default


@dataclass(slots=True)
class CollaborationScope:
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

    @classmethod
    def from_value(cls, value: "CollaborationScope | Mapping[str, Any] | None" = None, **kwargs: Any) -> "CollaborationScope":
        payload: dict[str, Any] = {}
        if isinstance(value, cls):
            payload.update(value.as_dict(include_none=True))
        elif isinstance(value, Mapping):
            payload.update(dict(value))
        elif value is not None:
            raise TypeError("scope must be CollaborationScope, mapping, or None")
        payload.update(kwargs)
        allowed = {item.name for item in fields(cls)}
        filtered = {key: _clean_scope_value(item) for key, item in payload.items() if key in allowed}
        return cls(**filtered)

    def merge(self, value: "CollaborationScope | Mapping[str, Any] | None" = None, **kwargs: Any) -> "CollaborationScope":
        payload = self.as_dict(include_none=True)
        if isinstance(value, CollaborationScope):
            payload.update({key: item for key, item in value.as_dict(include_none=True).items() if item is not None})
        elif isinstance(value, Mapping):
            payload.update({key: _clean_scope_value(item) for key, item in dict(value).items() if _clean_scope_value(item) is not None})
        elif value is not None:
            raise TypeError("scope must be CollaborationScope, mapping, or None")
        payload.update({key: _clean_scope_value(item) for key, item in kwargs.items() if _clean_scope_value(item) is not None})
        return CollaborationScope.from_value(payload)

    def as_dict(self, *, include_none: bool = False) -> dict[str, str | None]:
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
        }
        if include_none:
            return payload
        return {key: value for key, value in payload.items() if value is not None}

    def has_team_namespace(self) -> bool:
        return any(getattr(self, key) for key in TEAM_SCOPE_FIELD_NAMES)

    def resolved_namespace_key(self) -> str | None:
        if self.namespace_key:
            return self.namespace_key
        parts: list[str] = []
        if self.platform_id:
            parts.append(f"platform={self.platform_id}")
        if self.workspace_id:
            parts.append(f"workspace={self.workspace_id}")
        if self.team_id:
            parts.append(f"team={self.team_id}")
        if self.project_id:
            parts.append(f"project={self.project_id}")
        owner = self.owner_agent_id or self.agent_id
        if owner:
            parts.append(f"owner={owner}")
        if self.subject_type or self.subject_id:
            if self.subject_type and self.subject_id:
                parts.append(f"subject={self.subject_type}:{self.subject_id}")
            elif self.subject_id:
                parts.append(f"subject={self.subject_id}")
            else:
                parts.append(f"subject_type={self.subject_type}")
        if self.interaction_type:
            parts.append(f"interaction={self.interaction_type}")
        if self.user_id and not self.subject_id:
            parts.append(f"user={self.user_id}")
        return "|".join(parts) if parts else None

    def as_metadata(self) -> dict[str, str]:
        payload = self.as_dict()
        namespace_key = self.resolved_namespace_key()
        if namespace_key:
            payload["namespace_key"] = namespace_key
        return payload

    def apply_to_kwargs(self, kwargs: Mapping[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(kwargs or {})
        scope_kwargs = self.as_metadata()
        for key, value in scope_kwargs.items():
            payload.setdefault(key, value)
        return payload

    def storage_prefix(self, domain: str) -> str:
        namespace_key = self.resolved_namespace_key() or "global"
        digest = hashlib.sha1(namespace_key.encode("utf-8")).hexdigest()[:12]
        human_segment = "__".join(
            [
                _filesystem_segment(self.platform_id, "local"),
                _filesystem_segment(self.workspace_id or self.team_id, "shared"),
                _filesystem_segment(self.project_id or self.owner_agent_id, "default"),
            ]
        )
        return f"{domain}/{human_segment}-{digest}"


def scope_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "owner_agent_id": {"type": "string"},
            "agent_id": {"type": "string"},
            "subject_type": {"type": "string"},
            "subject_id": {"type": "string"},
            "interaction_type": {"type": "string"},
            "platform_id": {"type": "string"},
            "workspace_id": {"type": "string"},
            "team_id": {"type": "string"},
            "project_id": {"type": "string"},
            "namespace_key": {"type": "string"},
        },
        "additionalProperties": False,
    }


def apply_scope_to_payload(
    arguments: Mapping[str, Any] | None = None,
    *,
    default_scope: CollaborationScope | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(arguments or {})
    nested_scope = payload.pop("context_scope", None)
    if nested_scope is None and isinstance(payload.get("scope"), Mapping):
        nested_scope = payload.pop("scope")
    top_level_scope = {key: payload.get(key) for key in SCOPE_FIELD_NAMES if key in payload}
    scope = CollaborationScope.from_value(default_scope).merge(nested_scope).merge(top_level_scope)
    return scope.apply_to_kwargs(payload)
