from __future__ import annotations

from typing import Any


def match_filters(record: dict[str, Any], filters: dict[str, Any] | None) -> bool:
    if not filters:
        return True
    for key, condition in filters.items():
        value = record.get(key)
        if isinstance(condition, dict):
            for op, expected in condition.items():
                if op == "eq" and value != expected:
                    return False
                if op == "ne" and value == expected:
                    return False
                if op == "in" and value not in expected:
                    return False
                if op == "gte" and (value is None or value < expected):
                    return False
                if op == "lte" and (value is None or value > expected):
                    return False
                if op == "contains" and expected not in (value or ""):
                    return False
        elif value != condition:
            return False
    return True
