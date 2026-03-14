from __future__ import annotations

from typing import Any


def get_field_value(record: dict[str, Any], path: str) -> Any:
    current: Any = record
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        if part == "metadata" and isinstance(record.get("metadata"), dict):
            current = record["metadata"]
            continue
        if isinstance(current, dict) and "metadata" in current and isinstance(current["metadata"], dict) and part in current["metadata"]:
            current = current["metadata"][part]
            continue
        return None
    return current


def evaluate_filter(record: dict[str, Any], expression: dict[str, Any] | None) -> bool:
    if not expression:
        return True
    if "AND" in expression:
        return all(evaluate_filter(record, item) for item in expression["AND"])
    if "OR" in expression:
        return any(evaluate_filter(record, item) for item in expression["OR"])
    if "NOT" in expression:
        return not any(evaluate_filter(record, item) for item in expression["NOT"])

    for field, condition in expression.items():
        value = get_field_value(record, field)
        if isinstance(condition, dict):
            for operator, expected in condition.items():
                if not _evaluate_operator(value, operator, expected):
                    return False
            continue
        if value != condition:
            return False
    return True


def filter_records(records: list[dict[str, Any]], expression: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [record for record in records if evaluate_filter(record, expression)]


def _evaluate_operator(value: Any, operator: str, expected: Any) -> bool:
    if operator == "eq":
        return value == expected
    if operator == "ne":
        return value != expected
    if operator == "in":
        return value in expected
    if operator == "nin":
        return value not in expected
    if operator == "gt":
        return value is not None and value > expected
    if operator == "gte":
        return value is not None and value >= expected
    if operator == "lt":
        return value is not None and value < expected
    if operator == "lte":
        return value is not None and value <= expected
    if operator == "contains":
        if value is None:
            return False
        return str(expected) in str(value)
    if operator == "icontains":
        if value is None:
            return False
        return str(expected).lower() in str(value).lower()
    if operator == "*":
        return value is not None
    raise ValueError(f"Unsupported filter operator `{operator}`")
