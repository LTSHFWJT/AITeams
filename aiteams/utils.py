from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4, uuid7


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


def make_uuid7() -> str:
    return str(uuid7())


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def trim_text(text: str | None, *, limit: int = 280) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return f"{value[: max(0, limit - 3)].rstrip()}..."


def slugify(text: str, *, fallback: str = "item") -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "-", text.strip().lower()).strip("-_")
    return normalized or fallback


def ensure_parent(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def resolve_path(data: Any, dotted_path: str, default: Any = None) -> Any:
    current = data
    for chunk in dotted_path.split("."):
        if chunk == "":
            continue
        if isinstance(current, dict):
            if chunk not in current:
                return default
            current = current[chunk]
            continue
        if isinstance(current, list):
            try:
                current = current[int(chunk)]
            except (ValueError, IndexError):
                return default
            continue
        if hasattr(current, chunk):
            current = getattr(current, chunk)
            continue
        return default
    return current


def render_template(template: str, context: dict[str, Any]) -> str:
    def _replace(match: re.Match[str]) -> str:
        value = resolve_path(context, match.group(1).strip(), "")
        if value is None:
            return ""
        if isinstance(value, list):
            return "\n".join(f"- {item}" for item in value)
        if isinstance(value, dict):
            return pretty_json(value)
        return str(value)

    return re.sub(r"\{\{\s*([^}]+?)\s*\}\}", _replace, template)


def render_template_data(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return render_template(value, context)
    if isinstance(value, list):
        return [render_template_data(item, context) for item in value]
    if isinstance(value, dict):
        return {key: render_template_data(item, context) for key, item in value.items()}
    return value
