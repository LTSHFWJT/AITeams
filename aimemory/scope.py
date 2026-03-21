from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha1
from typing import Any

from aimemory.errors import InvalidScope


@dataclass(frozen=True, slots=True)
class Scope:
    tenant_id: str = "local"
    workspace_id: str | None = None
    project_id: str | None = None
    user_id: str | None = None
    agent_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    namespace: str = "default"
    visibility: str = "private"

    @classmethod
    def from_value(cls, value: "Scope | dict[str, Any] | None") -> "Scope":
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            return cls(**value)
        raise InvalidScope(f"Unsupported scope value: {type(value)!r}")

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "Scope":
        metadata = record.get("metadata", {})
        return cls(
            tenant_id=record.get("tenant_id") or metadata.get("tenant_id") or "local",
            workspace_id=record.get("workspace_id") or metadata.get("workspace_id"),
            project_id=record.get("project_id") or metadata.get("project_id"),
            user_id=record.get("user_id") or metadata.get("user_id"),
            agent_id=record.get("agent_id") or metadata.get("agent_id"),
            session_id=record.get("session_id") or metadata.get("session_id"),
            run_id=record.get("run_id") or metadata.get("run_id"),
            namespace=record.get("namespace") or metadata.get("namespace") or "default",
            visibility=record.get("visibility") or metadata.get("visibility") or "private",
        )

    def bind(self, **overrides: Any) -> "Scope":
        payload = self.as_dict()
        payload.update({key: value for key, value in overrides.items() if value is not None})
        return Scope(**payload)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def path(self) -> str:
        parts = [
            self.tenant_id,
            self.workspace_id or "-",
            self.project_id or "-",
            self.user_id or "-",
            self.agent_id or "-",
            self.session_id or "-",
            self.run_id or "-",
            self.namespace,
            self.visibility,
        ]
        return "/".join(parts)

    @property
    def key(self) -> str:
        return sha1(self.path.encode("utf-8")).hexdigest()
