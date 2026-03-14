from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from aimemory.algorithms.affinity import STRATEGY_SCOPE_PROTOTYPES, best_label, blend_score_maps, prototype_affinities
from aimemory.core.capabilities import capability_dict
from aimemory.domains.memory.models import MemoryType


USER_SCOPE = "user"
AGENT_SCOPE = "agent"
RUN_SCOPE = "run"


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def resolve_strategy_scope(
    memory_type: str | None,
    *,
    agent_id: str | None = None,
    run_id: str | None = None,
    role: str | None = None,
    metadata: dict[str, Any] | None = None,
    text: str | None = None,
) -> str:
    metadata = dict(metadata or {})
    explicit = metadata.get("strategy_scope")
    if explicit in {USER_SCOPE, AGENT_SCOPE, RUN_SCOPE}:
        return str(explicit)

    normalized_type = str(memory_type or MemoryType.SEMANTIC)
    source_role = str(metadata.get("source_role") or role or "").lower()
    type_bias: dict[str, float] = {
        USER_SCOPE: 0.18 if normalized_type in {str(MemoryType.PREFERENCE), str(MemoryType.PROFILE), str(MemoryType.RELATIONSHIP_SUMMARY)} else 0.0,
        AGENT_SCOPE: 0.18 if normalized_type == str(MemoryType.PROCEDURAL) else 0.0,
        RUN_SCOPE: 0.18 if normalized_type == str(MemoryType.EPISODIC) else 0.0,
    }
    context_bias = {
        USER_SCOPE: 0.04,
        AGENT_SCOPE: 0.08 if agent_id else 0.0,
        RUN_SCOPE: 0.08 if run_id else 0.0,
    }
    if source_role == "assistant" or metadata.get("source") == "skill":
        context_bias[AGENT_SCOPE] += 0.08
    if metadata.get("source") in {"execution", "checkpoint", "run"}:
        context_bias[RUN_SCOPE] += 0.1

    text_scores = prototype_affinities(str(text or metadata.get("source_text") or ""), STRATEGY_SCOPE_PROTOTYPES, dense_weight=0.78, sparse_weight=0.18, containment_weight=0.04)
    combined = blend_score_maps(text_scores, type_bias, context_bias)
    return best_label(combined, default=USER_SCOPE)


def governance_scope_profile(strategy_scope: str) -> dict[str, float]:
    if strategy_scope == AGENT_SCOPE:
        return {
            "importance_bias": 0.06,
            "update_bias": 0.06,
            "merge_bias": 0.08,
            "retention_bias": 0.04,
            "promotion_bias": 0.02,
            "cleanup_penalty": 0.02,
            "recall_bias": 0.08,
        }
    if strategy_scope == RUN_SCOPE:
        return {
            "importance_bias": 0.02,
            "update_bias": 0.08,
            "merge_bias": 0.04,
            "retention_bias": -0.12,
            "promotion_bias": -0.18,
            "cleanup_penalty": 0.18,
            "recall_bias": 0.12,
        }
    return {
        "importance_bias": 0.08,
        "update_bias": 0.01,
        "merge_bias": 0.03,
        "retention_bias": 0.12,
        "promotion_bias": 0.1,
        "cleanup_penalty": -0.08,
        "recall_bias": 0.06,
    }


def governance_scope_rules(strategy_scope: str) -> dict[str, Any]:
    if strategy_scope == AGENT_SCOPE:
        return {
            "promotion_enabled": True,
            "promotion_min_importance": 0.58,
            "promotable_memory_types": [
                str(MemoryType.PROCEDURAL),
                str(MemoryType.SEMANTIC),
                str(MemoryType.RELATIONSHIP_SUMMARY),
            ],
            "cleanup_threshold_delta": 0.03,
            "cleanup_action": "archive",
            "recall_priority": "agent-operational",
            "notes": ["agent memory prefers reusable procedures over transient run facts"],
        }
    if strategy_scope == RUN_SCOPE:
        return {
            "promotion_enabled": False,
            "promotion_min_importance": 0.88,
            "promotable_memory_types": [
                str(MemoryType.EPISODIC),
                str(MemoryType.PROCEDURAL),
            ],
            "cleanup_threshold_delta": 0.12,
            "cleanup_action": "archive",
            "recall_priority": "run-continuity",
            "notes": ["run memory is retained briefly and archived early unless forced"],
        }
    return {
        "promotion_enabled": True,
        "promotion_min_importance": 0.46,
        "promotable_memory_types": [
            str(MemoryType.PREFERENCE),
            str(MemoryType.PROFILE),
            str(MemoryType.RELATIONSHIP_SUMMARY),
            str(MemoryType.SEMANTIC),
        ],
        "cleanup_threshold_delta": -0.04,
        "cleanup_action": "archive",
        "recall_priority": "user-personalization",
        "notes": ["user memory is favored for long-term personalization and stable traits"],
    }


def memory_type_policy_profile(memory_type: str | None) -> dict[str, Any]:
    normalized_type = str(memory_type or MemoryType.SEMANTIC)
    base = {
        "retention_bonus": 0.06,
        "promotion_min_importance": 0.54,
        "cleanup_threshold_delta": 0.0,
        "recall_bias": 0.04,
        "promote_by_default": True,
        "slot_strategy": "semantic-cluster",
        "notes": ["default semantic memory"],
    }
    if normalized_type == str(MemoryType.PREFERENCE):
        base.update(
            retention_bonus=0.16,
            promotion_min_importance=0.42,
            cleanup_threshold_delta=-0.08,
            recall_bias=0.12,
            slot_strategy="preference-slot",
            notes=["preferences are sticky and should update the latest slot value"],
        )
    elif normalized_type == str(MemoryType.PROFILE):
        base.update(
            retention_bonus=0.16,
            promotion_min_importance=0.44,
            cleanup_threshold_delta=-0.08,
            recall_bias=0.1,
            slot_strategy="profile-slot",
            notes=["profile facts represent stable self-description"],
        )
    elif normalized_type == str(MemoryType.PROCEDURAL):
        base.update(
            retention_bonus=0.12,
            promotion_min_importance=0.56,
            cleanup_threshold_delta=-0.02,
            recall_bias=0.1,
            slot_strategy="procedure-steps",
            notes=["procedural memory favors richer merged instructions"],
        )
    elif normalized_type == str(MemoryType.EPISODIC):
        base.update(
            retention_bonus=0.02,
            promotion_min_importance=0.8,
            cleanup_threshold_delta=0.08,
            recall_bias=0.08,
            promote_by_default=False,
            slot_strategy="timeline-event",
            notes=["episodic memory is valuable for recent continuity but decays quickly"],
        )
    elif normalized_type == str(MemoryType.RELATIONSHIP_SUMMARY):
        base.update(
            retention_bonus=0.1,
            promotion_min_importance=0.5,
            cleanup_threshold_delta=-0.04,
            recall_bias=0.09,
            slot_strategy="relationship-entity",
            notes=["relationship summaries track how the agent should treat people or roles"],
        )
    return base


def memory_type_retention_bonus(memory_type: str | None) -> float:
    return float(memory_type_policy_profile(memory_type)["retention_bonus"])


def recency_score(timestamp: str | None) -> float:
    if not timestamp:
        return 0.0
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() / 3600.0)
    if age_hours <= 6:
        return 0.2
    if age_hours <= 24:
        return 0.14
    if age_hours <= 24 * 7:
        return 0.08
    if age_hours <= 24 * 30:
        return 0.04
    return 0.0


def evaluate_memory_value(memory: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(memory.get("metadata") or {})
    strategy_scope = str(
        metadata.get("strategy_scope")
        or resolve_strategy_scope(
            memory.get("memory_type"),
            agent_id=memory.get("agent_id"),
            run_id=memory.get("run_id"),
            role=memory.get("role"),
            metadata=metadata,
            text=memory.get("text"),
        )
    )
    scope_profile = governance_scope_profile(strategy_scope)
    scope_rules = governance_scope_rules(strategy_scope)
    type_profile = memory_type_policy_profile(memory.get("memory_type"))
    importance = float(memory.get("importance", 0.5))
    recency = recency_score(memory.get("updated_at") or memory.get("created_at"))
    type_bonus = float(type_profile["retention_bonus"])
    promotion_state = dict(metadata.get("promotion") or {})
    promotion_bonus = 0.08 if promotion_state.get("status") == "promoted" else 0.0
    graph_context = dict(memory.get("graph_context") or {})
    relation_bonus = min(0.08, float(graph_context.get("matched_relation_count", 0)) * 0.02)
    score = (
        (importance * 0.55)
        + (recency * 0.18)
        + type_bonus
        + float(type_profile["recall_bias"])
        + promotion_bonus
        + relation_bonus
        + float(scope_profile["retention_bias"])
        - float(scope_profile["cleanup_penalty"])
    )
    dynamic_cleanup_threshold = _clamp(0.34 + float(scope_rules["cleanup_threshold_delta"]) + float(type_profile["cleanup_threshold_delta"]))
    return {
        "strategy_scope": strategy_scope,
        "importance": round(importance, 6),
        "recency_score": round(recency, 6),
        "type_bonus": round(type_bonus, 6),
        "promotion_bonus": round(promotion_bonus, 6),
        "relation_bonus": round(relation_bonus, 6),
        "retention_bias": round(float(scope_profile["retention_bias"]), 6),
        "cleanup_penalty": round(float(scope_profile["cleanup_penalty"]), 6),
        "cleanup_threshold": round(dynamic_cleanup_threshold, 6),
        "cleanup_action": str(scope_rules["cleanup_action"]),
        "promotion_min_importance": round(
            _clamp(max(float(scope_rules["promotion_min_importance"]), float(type_profile["promotion_min_importance"]))),
            6,
        ),
        "value_score": round(_clamp(score), 6),
    }


def describe_governance_capabilities() -> dict[str, Any]:
    return capability_dict(
        category="governance",
        provider="lite",
        features={
            "scope_policies": True,
            "memory_type_policies": True,
            "promotion_policy": True,
            "cleanup_policy": True,
            "session_governance": True,
            "background_platform": False,
        },
        items={
            USER_SCOPE: governance_scope_rules(USER_SCOPE),
            AGENT_SCOPE: governance_scope_rules(AGENT_SCOPE),
            RUN_SCOPE: governance_scope_rules(RUN_SCOPE),
        },
        notes=[
            "governance remains local and synchronous by default",
            "user / agent / run scopes follow different retention and promotion policies",
        ],
    )


def describe_memory_type_policies() -> dict[str, Any]:
    return capability_dict(
        category="memory_type_policy",
        provider="lite",
        features={
            "typed_promotion_thresholds": True,
            "typed_cleanup_thresholds": True,
            "typed_slot_strategies": True,
        },
        items={
            memory_type.value: memory_type_policy_profile(memory_type.value)
            for memory_type in MemoryType
        },
        notes=["typed policies stay heuristic and avoid requiring an LLM"],
    )
