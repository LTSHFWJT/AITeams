from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class CapabilityDescriptor:
    category: str
    provider: str
    active_provider: str | None = None
    features: dict[str, Any] = field(default_factory=dict)
    items: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "category": self.category,
            "provider": self.provider,
            "features": dict(self.features),
        }
        if self.active_provider is not None:
            payload["active_provider"] = self.active_provider
        if self.items:
            payload["items"] = dict(self.items)
        if self.notes:
            payload["notes"] = list(self.notes)
        return payload


def capability_dict(
    *,
    category: str,
    provider: str,
    features: dict[str, Any],
    active_provider: str | None = None,
    items: dict[str, Any] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    return CapabilityDescriptor(
        category=category,
        provider=provider,
        active_provider=active_provider,
        features=features,
        items=items or {},
        notes=notes or [],
    ).as_dict()
