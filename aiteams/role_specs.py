from __future__ import annotations

from typing import Any

def normalize_role_spec(spec: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(spec or {})
    normalized = {
        key: value
        for key, value in payload.items()
        if key not in {"applies_to", "binding_type", "kind", "system_prompt"}
    }
    normalized["system_prompt"] = str(payload.get("system_prompt") or "").strip()
    return normalized


def role_spec_system_prompt(spec: dict[str, Any] | None) -> str:
    return str(normalize_role_spec(spec).get("system_prompt") or "").strip()
