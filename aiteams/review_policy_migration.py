from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable

from aiteams.review_policies import SUPPORTED_REVIEW_TRIGGERS, review_policy_spec
from aiteams.utils import utcnow_iso


EDIT_DECISION_ALIASES = {"edit", "edit_payload", "edit_records", "reroute"}
TOOL_REVIEW_TRIGGER_DEFAULTS = ["before_tool_call", "before_external_side_effect"]


def _unique_strings(values: Iterable[Any]) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        items.append(text)
    return items


def _normalize_allowed_decisions(values: Iterable[Any], *, default: list[str] | None = None) -> list[str]:
    items: list[str] = []
    for value in values:
        token = str(value or "").strip()
        if token in {"approve", "reject", "edit"}:
            items.append(token)
        elif token in EDIT_DECISION_ALIASES:
            items.append("edit")
    normalized = _unique_strings(items)
    if normalized:
        return normalized
    return list(default or ["approve", "reject"])


def _normalize_plugin_actions(source: dict[str, Any], conditions: dict[str, Any]) -> list[dict[str, str]]:
    explicit: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    raw_items = list(conditions.get("plugin_actions") or source.get("plugin_actions") or [])
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        plugin_key = str(item.get("plugin_key") or "").strip()
        action = str(item.get("action") or "").strip() or "*"
        if not plugin_key:
            continue
        identity = (plugin_key, action)
        if identity in seen:
            continue
        seen.add(identity)
        explicit.append({"plugin_key": plugin_key, "action": action})
    if explicit:
        return explicit

    plugin_keys = _unique_strings(list(conditions.get("plugin_keys") or source.get("plugin_keys") or []))
    action_names = _unique_strings(list(conditions.get("actions") or conditions.get("tool_actions") or [])) or ["*"]
    generated: list[dict[str, str]] = []
    for plugin_key in plugin_keys:
        for action in action_names:
            identity = (plugin_key, action)
            if identity in seen:
                continue
            seen.add(identity)
            generated.append({"plugin_key": plugin_key, "action": action or "*"})
    return generated


def _normalize_rules(
    source: dict[str, Any],
    conditions: dict[str, Any],
    *,
    fallback_allowed_decisions: list[str],
) -> list[dict[str, Any]]:
    explicit: dict[tuple[str, str], list[str]] = {}
    for item in list(source.get("rules") or conditions.get("rules") or []):
        if not isinstance(item, dict):
            continue
        plugin_key = str(item.get("plugin_key") or "").strip()
        action = str(item.get("action") or "").strip() or "*"
        if not plugin_key:
            continue
        identity = (plugin_key, action)
        decisions = _normalize_allowed_decisions(
            list(item.get("allowed_decisions") or item.get("actions") or []),
            default=fallback_allowed_decisions,
        )
        explicit[identity] = _unique_strings([*(explicit.get(identity) or []), *decisions])
    if explicit:
        return [
            {"plugin_key": plugin_key, "action": action, "allowed_decisions": decisions}
            for (plugin_key, action), decisions in explicit.items()
        ]

    return [
        {
            "plugin_key": str(item.get("plugin_key") or "").strip(),
            "action": str(item.get("action") or "").strip() or "*",
            "allowed_decisions": list(fallback_allowed_decisions),
        }
        for item in _normalize_plugin_actions(source, conditions)
        if str(item.get("plugin_key") or "").strip()
    ]


def migrate_review_policy_spec(spec: dict[str, Any] | None) -> dict[str, Any]:
    source = dict(spec or {})
    conditions = dict(source.get("conditions") or {})
    allowed_decisions = _normalize_allowed_decisions(
        list(source.get("allowed_decisions") or source.get("actions") or []),
        default=["approve", "reject"],
    )
    rules = _normalize_rules(source, conditions, fallback_allowed_decisions=allowed_decisions)
    triggers = [
        value for value in _unique_strings(list(source.get("triggers") or [])) if value in SUPPORTED_REVIEW_TRIGGERS
    ]
    if not triggers and rules:
        triggers = list(TOOL_REVIEW_TRIGGER_DEFAULTS)
    candidate = {
        "triggers": triggers,
        "allowed_decisions": allowed_decisions,
        "rules": rules,
        "conditions": {
            "plugin_actions": _normalize_plugin_actions(source, conditions),
            "permissions": _unique_strings(list(conditions.get("permissions") or [])),
        },
    }
    return review_policy_spec({"spec": candidate})


def migrate_review_policies_in_connection(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = False,
    commit: bool = False,
    updated_at: str | None = None,
) -> dict[str, Any]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute("SELECT id, name, spec_json FROM review_policies ORDER BY updated_at DESC, id DESC").fetchall()
    scanned = 0
    migrated = 0
    changed_items: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    now = updated_at or utcnow_iso()
    for row in rows:
        scanned += 1
        record_id = str(row["id"] or "").strip()
        record_name = str(row["name"] or record_id).strip() or record_id
        raw_spec = row["spec_json"] or "{}"
        try:
            original = json.loads(raw_spec)
        except json.JSONDecodeError as exc:
            errors.append({"id": record_id, "name": record_name, "error": f"Invalid spec_json: {exc.msg}"})
            continue
        if not isinstance(original, dict):
            errors.append({"id": record_id, "name": record_name, "error": "spec_json is not an object"})
            continue
        updated = migrate_review_policy_spec(original)
        if updated == original:
            continue
        changed_items.append({"id": record_id, "name": record_name})
        migrated += 1
        if dry_run:
            continue
        connection.execute(
            "UPDATE review_policies SET spec_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(updated, ensure_ascii=False, separators=(",", ":")), now, record_id),
        )
    if commit and not dry_run:
        connection.commit()
    return {
        "scanned": scanned,
        "migrated": migrated,
        "changed_items": changed_items,
        "errors": errors,
    }
