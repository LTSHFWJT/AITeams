from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def ensure_dir(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def json_loads(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    if value == "":
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def merge_metadata(existing: dict[str, Any] | None, incoming: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if existing:
        merged.update(existing)
    if incoming:
        merged.update(incoming)
    return merged


def stable_edge_id(source_node_id: str, edge_type: str, target_node_id: str) -> str:
    seed = f"{source_node_id}:{edge_type}:{target_node_id}"
    return f"edge_{uuid.uuid5(uuid.NAMESPACE_URL, seed).hex}"
