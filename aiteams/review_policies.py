from __future__ import annotations

from typing import Any, Iterable


SUPPORTED_DECISIONS = ("approve", "reject", "edit")
TOOL_REVIEW_TRIGGERS = {"before_tool_call", "before_external_side_effect"}
TASK_REVIEW_TRIGGERS = {
    "before_agent_to_agent_message",
    "before_handoff_to_lower_level",
    "before_escalation_to_upper_level",
    "before_agent_receive_task",
}
MEMORY_REVIEW_TRIGGERS = {"before_memory_write", "memory_write"}
FINAL_DELIVERY_REVIEW_TRIGGERS = {"before_final_delivery", "final_delivery"}
SUPPORTED_REVIEW_TRIGGERS = (
    TOOL_REVIEW_TRIGGERS | TASK_REVIEW_TRIGGERS | MEMORY_REVIEW_TRIGGERS | FINAL_DELIVERY_REVIEW_TRIGGERS
)


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


def _normalize_plugin_actions(values: Any) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for value in list(values or []):
        if not isinstance(value, dict):
            continue
        plugin_key = str(value.get("plugin_key") or "").strip()
        action = str(value.get("action") or "").strip()
        if not plugin_key:
            continue
        action = action or "*"
        identity = (plugin_key, action)
        if identity in seen:
            continue
        seen.add(identity)
        entries.append({"plugin_key": plugin_key, "action": action})
    return entries


def _normalize_allowed_decisions(values: Any, *, default: list[str] | None = None) -> list[str]:
    allowed = [item for item in _unique_strings(values or []) if item in SUPPORTED_DECISIONS]
    if allowed:
        return allowed
    return list(default or ["approve", "reject"])


def _normalize_rule_items(
    values: Any,
    *,
    fallback_plugin_actions: list[dict[str, str]],
    fallback_allowed_decisions: list[str],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], list[str]] = {}
    raw_values = list(values or [])
    if raw_values:
        for item in raw_values:
            if not isinstance(item, dict):
                continue
            plugin_key = str(item.get("plugin_key") or "").strip()
            action = str(item.get("action") or "").strip() or "*"
            if not plugin_key:
                continue
            identity = (plugin_key, action)
            decisions = _normalize_allowed_decisions(item.get("allowed_decisions") or [], default=fallback_allowed_decisions)
            existing = merged.get(identity) or []
            merged[identity] = _unique_strings([*existing, *decisions])
    else:
        for item in fallback_plugin_actions:
            plugin_key = str(item.get("plugin_key") or "").strip()
            action = str(item.get("action") or "").strip() or "*"
            if not plugin_key:
                continue
            merged[(plugin_key, action)] = list(fallback_allowed_decisions)
    return [
        {"plugin_key": plugin_key, "action": action, "allowed_decisions": decisions}
        for (plugin_key, action), decisions in merged.items()
    ]


def review_policy_spec(policy: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(policy, dict):
        return {"triggers": [], "allowed_decisions": ["approve", "reject"], "rules": [], "conditions": {"plugin_actions": []}}
    if isinstance(policy.get("config"), dict):
        source = dict(policy.get("config") or {})
    elif isinstance(policy.get("spec_json"), dict):
        source = dict(policy.get("spec_json") or {})
    elif isinstance(policy.get("spec"), dict):
        source = dict(policy.get("spec") or {})
    else:
        source = dict(policy)
    conditions = dict(source.get("conditions") or {})
    plugin_actions = _normalize_plugin_actions(conditions.get("plugin_actions") or source.get("plugin_actions") or [])
    allowed_decisions = _normalize_allowed_decisions(source.get("allowed_decisions") or [])
    rules = _normalize_rule_items(
        source.get("rules") or conditions.get("rules") or [],
        fallback_plugin_actions=plugin_actions,
        fallback_allowed_decisions=allowed_decisions,
    )
    aggregated_plugin_actions = _normalize_plugin_actions(rules)
    aggregated_allowed_decisions = _normalize_allowed_decisions(
        [decision for item in rules for decision in list(item.get("allowed_decisions") or [])] or allowed_decisions
    )
    normalized_conditions = {
        "plugin_actions": aggregated_plugin_actions,
        "permissions": _unique_strings(conditions.get("permissions") or []),
    }
    return {
        "triggers": [item for item in _unique_strings(source.get("triggers") or []) if item in SUPPORTED_REVIEW_TRIGGERS],
        "allowed_decisions": aggregated_allowed_decisions,
        "rules": rules,
        "conditions": normalized_conditions,
    }


def review_policy_allowed_decisions(policy: dict[str, Any] | None) -> list[str]:
    return list(review_policy_spec(policy).get("allowed_decisions") or ["approve", "reject"])


def union_allowed_decisions(policies: list[dict[str, Any]]) -> list[str]:
    merged: list[str] = []
    for policy in policies:
        merged.extend(review_policy_allowed_decisions(policy))
    decisions = _unique_strings(merged)
    return decisions or ["approve", "reject"]


def policy_has_trigger(policy: dict[str, Any] | None, triggers: set[str]) -> bool:
    policy_triggers = set(review_policy_spec(policy).get("triggers") or [])
    return bool(policy_triggers.intersection(triggers))


def policy_matches_tool(
    policy: dict[str, Any] | None,
    *,
    plugin_key: str,
    action_name: str,
    permissions: Iterable[str] = (),
) -> bool:
    return bool(tool_policy_allowed_decisions(policy, plugin_key=plugin_key, action_name=action_name, permissions=permissions))


def tool_policy_allowed_decisions(
    policy: dict[str, Any] | None,
    *,
    plugin_key: str,
    action_name: str,
    permissions: Iterable[str] = (),
) -> list[str]:
    spec = review_policy_spec(policy)
    if not set(spec.get("triggers") or []).intersection(TOOL_REVIEW_TRIGGERS):
        return []
    conditions = dict(spec.get("conditions") or {})
    normalized_plugin_key = str(plugin_key or "").strip()
    normalized_action_name = str(action_name or "").strip()
    permission_set = set(_unique_strings(permissions))
    required_permissions = set(_unique_strings(conditions.get("permissions") or []))
    if required_permissions and not required_permissions.intersection(permission_set):
        return []
    matched: list[str] = []
    for item in list(spec.get("rules") or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("plugin_key") or "").strip() != normalized_plugin_key:
            continue
        if str(item.get("action") or "*").strip() not in {normalized_action_name, "*"}:
            continue
        matched.extend(_normalize_allowed_decisions(item.get("allowed_decisions") or []))
    return _unique_strings(matched)


def policy_matches_memory(
    policy: dict[str, Any] | None,
    *,
    memory_scope: str,
    memory_kinds: Iterable[str] = (),
) -> bool:
    spec = review_policy_spec(policy)
    if not set(spec.get("triggers") or []).intersection(MEMORY_REVIEW_TRIGGERS):
        return False
    return True
