from __future__ import annotations

import json
from typing import Any


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def json_loads(value: str | bytes | None, default: Any) -> Any:
    if value in (None, "", b""):
        return default
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default
