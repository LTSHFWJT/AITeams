from __future__ import annotations

import re
from enum import StrEnum
from typing import Any

from aimemory.core.text import extract_keywords, normalize_text, split_sentences
from aimemory.domains.memory.models import MemoryType


class SemanticMemoryCategory(StrEnum):
    PROFILE = "profile"
    PREFERENCES = "preferences"
    ENTITIES = "entities"
    EVENTS = "events"
    CASES = "cases"
    PATTERNS = "patterns"


MEMORY_CATEGORIES = tuple(item.value for item in SemanticMemoryCategory)

ALWAYS_MERGE_CATEGORIES = frozenset({SemanticMemoryCategory.PROFILE.value})
MERGE_SUPPORTED_CATEGORIES = frozenset(
    {
        SemanticMemoryCategory.PREFERENCES.value,
        SemanticMemoryCategory.ENTITIES.value,
        SemanticMemoryCategory.PATTERNS.value,
    }
)
TEMPORAL_VERSIONED_CATEGORIES = frozenset(
    {
        SemanticMemoryCategory.PREFERENCES.value,
        SemanticMemoryCategory.ENTITIES.value,
    }
)
APPEND_ONLY_CATEGORIES = frozenset(
    {
        SemanticMemoryCategory.EVENTS.value,
        SemanticMemoryCategory.CASES.value,
    }
)

CATEGORY_TO_MEMORY_TYPE = {
    SemanticMemoryCategory.PROFILE.value: str(MemoryType.PROFILE),
    SemanticMemoryCategory.PREFERENCES.value: str(MemoryType.PREFERENCE),
    SemanticMemoryCategory.ENTITIES.value: str(MemoryType.SEMANTIC),
    SemanticMemoryCategory.EVENTS.value: str(MemoryType.EPISODIC),
    SemanticMemoryCategory.CASES.value: str(MemoryType.EPISODIC),
    SemanticMemoryCategory.PATTERNS.value: str(MemoryType.PROCEDURAL),
}

MEMORY_TYPE_TO_CATEGORY = {
    str(MemoryType.PROFILE): SemanticMemoryCategory.PROFILE.value,
    str(MemoryType.PREFERENCE): SemanticMemoryCategory.PREFERENCES.value,
    str(MemoryType.PROCEDURAL): SemanticMemoryCategory.PATTERNS.value,
    str(MemoryType.EPISODIC): SemanticMemoryCategory.EVENTS.value,
    str(MemoryType.RELATIONSHIP_SUMMARY): SemanticMemoryCategory.ENTITIES.value,
}

DEFAULT_IMPORTANCE_BY_CATEGORY = {
    SemanticMemoryCategory.PROFILE.value: 0.9,
    SemanticMemoryCategory.PREFERENCES.value: 0.8,
    SemanticMemoryCategory.ENTITIES.value: 0.72,
    SemanticMemoryCategory.EVENTS.value: 0.62,
    SemanticMemoryCategory.CASES.value: 0.82,
    SemanticMemoryCategory.PATTERNS.value: 0.86,
}

DEFAULT_CONFIDENCE_BY_CATEGORY = {
    SemanticMemoryCategory.PROFILE.value: 0.82,
    SemanticMemoryCategory.PREFERENCES.value: 0.78,
    SemanticMemoryCategory.ENTITIES.value: 0.74,
    SemanticMemoryCategory.EVENTS.value: 0.72,
    SemanticMemoryCategory.CASES.value: 0.8,
    SemanticMemoryCategory.PATTERNS.value: 0.8,
}

PROFILE_PATTERNS = (
    r"\b(i am|i'm|my name is|i work as|i live in|我是|我叫|我的名字|我在.*工作|我是一个)\b",
)
PREFERENCE_PATTERNS = (
    r"\b(prefer|preferred|like|likes|love|usually|always|tend to|default to|偏好|更喜欢|喜欢|习惯|通常|默认|最好)\b",
)
EVENT_PATTERNS = (
    r"\b(decided|completed|finished|launched|released|shipped|moved to|migrated|changed|updated|switched|met|yesterday|today|tomorrow|on monday|at \d|上线|发布|完成|决定|改为|更新为|切换到|迁移到|刚刚|今天|昨天|周[一二三四五六日天])\b",
)
ENTITY_STATE_PATTERNS = (
    r"\b(status|state|owner|window|version|endpoint|address|deadline|负责人|状态|窗口|版本|配置|地址|端点|负责人是|当前使用|当前为)\b",
)
CASE_PATTERNS = (
    r"\b(problem|issue|incident|outage|root cause|resolved by|fixed by|solution|troubleshoot|debug|error ->|解决方案|问题|故障|排查|修复|定位|根因)\b",
)
PATTERN_PATTERNS = (
    r"\b(runbook|workflow|playbook|steps|process|procedure|checklist|best practice|whenever|when .* then|standard|流程|步骤|规范|模式|做法|遇到.*时|需要先|应当|必须先)\b",
)

TIME_CONTEXT_ALIASES = {
    "morning": {"morning", "上午", "早上", "早晨"},
    "afternoon": {"afternoon", "下午"},
    "evening": {"evening", "傍晚", "晚上"},
    "night": {"night", "深夜", "夜里", "凌晨"},
    "weekday": {"weekday", "工作日", "平时"},
    "weekend": {"weekend", "周末", "假日"},
    "work": {"work", "办公", "工作", "上班"},
    "leisure": {"leisure", "休闲", "放松", "休息"},
    "summer": {"summer", "夏天", "夏季"},
    "winter": {"winter", "冬天", "冬季"},
    "travel": {"travel", "旅行", "出差", "旅游"},
}

_NON_WORD_PATTERN = re.compile(r"[^a-z0-9\u4e00-\u9fff]+", re.IGNORECASE)


def normalize_semantic_category(raw: str | None) -> str | None:
    candidate = str(raw or "").strip().lower()
    return candidate if candidate in MEMORY_CATEGORIES else None


def category_to_memory_type(category: str | None, *, default: str = str(MemoryType.SEMANTIC)) -> str:
    normalized = normalize_semantic_category(category)
    if normalized is None:
        return default
    return CATEGORY_TO_MEMORY_TYPE.get(normalized, default)


def memory_type_to_category(memory_type: str | None) -> str | None:
    candidate = str(memory_type or "").strip()
    if candidate in MEMORY_TYPE_TO_CATEGORY:
        return MEMORY_TYPE_TO_CATEGORY[candidate]
    if candidate == str(MemoryType.SEMANTIC):
        return None
    return normalize_semantic_category(candidate)


def default_importance_for_category(category: str | None, *, fallback: float = 0.5) -> float:
    normalized = normalize_semantic_category(category)
    if normalized is None:
        return fallback
    return float(DEFAULT_IMPORTANCE_BY_CATEGORY.get(normalized, fallback))


def default_confidence_for_category(category: str | None, *, fallback: float = 0.7) -> float:
    normalized = normalize_semantic_category(category)
    if normalized is None:
        return fallback
    return float(DEFAULT_CONFIDENCE_BY_CATEGORY.get(normalized, fallback))


def default_tier_for_category(category: str | None) -> str:
    del category
    return "working"


def semantic_key_text(text: str | None) -> str:
    normalized = normalize_text(text)
    normalized = _NON_WORD_PATTERN.sub(" ", normalized).strip()
    return re.sub(r"\s+", " ", normalized)


def _topic_from_abstract(abstract: str) -> str:
    trimmed = abstract.strip()
    if not trimmed:
        return ""
    prefix = re.match(r"^(.{1,120}?)[：:]", trimmed)
    if prefix and prefix.group(1):
        return prefix.group(1).strip()
    arrow = re.match(r"^(.{1,120}?)(?:\s*->|\s*=>)", trimmed)
    if arrow and arrow.group(1):
        return arrow.group(1).strip()
    return trimmed


def derive_topic_key(category: str | None, abstract: str | None) -> str | None:
    normalized_category = normalize_semantic_category(category)
    topic = semantic_key_text(_topic_from_abstract(str(abstract or "")))
    if normalized_category is None or not topic:
        return None
    return f"{normalized_category}:{topic}"


def derive_fact_key(category: str | None, abstract: str | None) -> str | None:
    normalized_category = normalize_semantic_category(category)
    if normalized_category not in TEMPORAL_VERSIONED_CATEGORIES:
        return None
    return derive_topic_key(normalized_category, abstract)


def append_relation(existing: Any, relation_type: str, target_id: str) -> list[dict[str, str]]:
    relations: list[dict[str, str]] = []
    for item in existing if isinstance(existing, list) else []:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type") or "").strip()
        item_target = str(item.get("target_id") or item.get("targetId") or "").strip()
        if not item_type or not item_target:
            continue
        relations.append({"type": item_type, "target_id": item_target})
    normalized = {"type": str(relation_type).strip(), "target_id": str(target_id).strip()}
    if normalized["type"] and normalized["target_id"]:
        if all(item != normalized for item in relations):
            relations.append(normalized)
    return relations


def normalize_context_label(raw: str | None) -> str:
    value = normalize_text(raw)
    if not value:
        return "general"
    for target, aliases in TIME_CONTEXT_ALIASES.items():
        if value in aliases:
            return target
    return value


def infer_context_label(text: str | None) -> str:
    normalized = normalize_text(text)
    if not normalized:
        return "general"
    for target, aliases in TIME_CONTEXT_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                return target
    return "general"


def parse_support_info(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {"global_strength": 0.5, "total_observations": 0, "slices": []}
    slices: list[dict[str, Any]] = []
    for item in raw.get("slices", []) if isinstance(raw.get("slices"), list) else []:
        if not isinstance(item, dict):
            continue
        context = normalize_context_label(item.get("context"))
        confirmations = max(0, int(item.get("confirmations", 0) or 0))
        contradictions = max(0, int(item.get("contradictions", 0) or 0))
        total = confirmations + contradictions
        strength = float(item.get("strength", confirmations / total if total else 0.5) or 0.5)
        last_observed_at = int(item.get("last_observed_at", 0) or 0)
        slices.append(
            {
                "context": context,
                "confirmations": confirmations,
                "contradictions": contradictions,
                "strength": max(0.0, min(1.0, strength)),
                "last_observed_at": last_observed_at,
            }
        )
    total_observations = int(raw.get("total_observations", 0) or 0)
    global_strength = float(raw.get("global_strength", 0.5) or 0.5)
    if total_observations <= 0 and slices:
        total_observations = sum(int(item["confirmations"]) + int(item["contradictions"]) for item in slices)
    if not slices and {"confirmations", "contradictions"} & set(raw.keys()):
        confirmations = max(0, int(raw.get("confirmations", 0) or 0))
        contradictions = max(0, int(raw.get("contradictions", 0) or 0))
        total = confirmations + contradictions
        if total:
            slices = [
                {
                    "context": "general",
                    "confirmations": confirmations,
                    "contradictions": contradictions,
                    "strength": confirmations / total,
                    "last_observed_at": 0,
                }
            ]
            total_observations = total
            global_strength = confirmations / total
    return {
        "global_strength": max(0.0, min(1.0, global_strength)),
        "total_observations": max(0, total_observations),
        "slices": slices,
    }


def update_support_info(existing: Any, context_label: str | None, event: str, *, observed_at: int) -> dict[str, Any]:
    info = parse_support_info(existing)
    slices = [dict(item) for item in info["slices"]]
    context = normalize_context_label(context_label)
    selected = next((item for item in slices if item.get("context") == context), None)
    if selected is None:
        selected = {
            "context": context,
            "confirmations": 0,
            "contradictions": 0,
            "strength": 0.5,
            "last_observed_at": observed_at,
        }
        slices.append(selected)
    if event == "contradict":
        selected["contradictions"] = int(selected.get("contradictions", 0) or 0) + 1
    else:
        selected["confirmations"] = int(selected.get("confirmations", 0) or 0) + 1
    selected["last_observed_at"] = observed_at
    total = int(selected.get("confirmations", 0) or 0) + int(selected.get("contradictions", 0) or 0)
    selected["strength"] = (int(selected.get("confirmations", 0) or 0) / total) if total else 0.5

    slices.sort(key=lambda item: int(item.get("last_observed_at", 0) or 0), reverse=True)
    slices = slices[:8]
    total_confirmations = sum(int(item.get("confirmations", 0) or 0) for item in slices)
    total_contradictions = sum(int(item.get("contradictions", 0) or 0) for item in slices)
    total_observations = total_confirmations + total_contradictions
    return {
        "global_strength": (total_confirmations / total_observations) if total_observations else 0.5,
        "total_observations": total_observations,
        "slices": slices,
    }


def infer_semantic_category(
    text: str,
    *,
    role: str | None = None,
    memory_type: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    explicit = normalize_semantic_category((metadata or {}).get("semantic_category") or (metadata or {}).get("memory_category"))
    if explicit is not None:
        return explicit
    mapped = memory_type_to_category(memory_type)
    normalized = normalize_text(text)
    if mapped is not None and mapped != SemanticMemoryCategory.EVENTS.value:
        return mapped

    if any(re.search(pattern, normalized, re.IGNORECASE) for pattern in PROFILE_PATTERNS):
        return SemanticMemoryCategory.PROFILE.value
    if any(re.search(pattern, normalized, re.IGNORECASE) for pattern in PREFERENCE_PATTERNS):
        return SemanticMemoryCategory.PREFERENCES.value
    if any(re.search(pattern, normalized, re.IGNORECASE) for pattern in CASE_PATTERNS):
        return SemanticMemoryCategory.CASES.value
    if any(re.search(pattern, normalized, re.IGNORECASE) for pattern in PATTERN_PATTERNS):
        return SemanticMemoryCategory.PATTERNS.value
    if any(re.search(pattern, normalized, re.IGNORECASE) for pattern in ENTITY_STATE_PATTERNS):
        return SemanticMemoryCategory.ENTITIES.value
    if any(re.search(pattern, normalized, re.IGNORECASE) for pattern in EVENT_PATTERNS):
        return SemanticMemoryCategory.EVENTS.value
    if str(role or "").lower() == "assistant" and ("\n" in text or "1." in text or "步骤" in text):
        return SemanticMemoryCategory.PATTERNS.value
    return mapped or SemanticMemoryCategory.ENTITIES.value


def build_semantic_abstract(text: str, category: str, *, keywords: list[str] | None = None) -> str:
    sentences = split_sentences(text)
    primary = str((sentences[0] if sentences else text) or "").strip()
    if not primary:
        return ""
    primary = re.sub(r"\s+", " ", primary).strip()
    category = normalize_semantic_category(category) or SemanticMemoryCategory.ENTITIES.value
    dominant = next((item for item in list(keywords or []) if item and len(item) >= 2), "")
    if category == SemanticMemoryCategory.PREFERENCES.value and dominant and ":" not in primary and "：" not in primary:
        return f"{dominant}: {primary}"[:160]
    if category == SemanticMemoryCategory.ENTITIES.value and dominant and ":" not in primary and "：" not in primary:
        return f"{dominant}: {primary}"[:160]
    if category == SemanticMemoryCategory.PATTERNS.value and dominant and ":" not in primary and "：" not in primary:
        return f"{dominant}: {primary}"[:160]
    return primary[:160]


def build_semantic_overview(text: str, category: str, *, keywords: list[str] | None = None) -> str:
    category = normalize_semantic_category(category) or SemanticMemoryCategory.ENTITIES.value
    sentences = [item.strip() for item in split_sentences(text) if item.strip()]
    excerpt = sentences[:3] or [text.strip()]
    topic = ", ".join(list(keywords or [])[:4])
    if category == SemanticMemoryCategory.PROFILE.value:
        lines = ["## Background"]
        lines.extend(f"- {item}" for item in excerpt[:3])
        return "\n".join(lines)
    if category == SemanticMemoryCategory.PREFERENCES.value:
        lines = ["## Preference"]
        if topic:
            lines.append(f"- Topic: {topic}")
        lines.extend(f"- {item}" for item in excerpt[:3])
        return "\n".join(lines)
    if category == SemanticMemoryCategory.ENTITIES.value:
        lines = ["## Entity State"]
        if topic:
            lines.append(f"- Entity: {topic}")
        lines.extend(f"- {item}" for item in excerpt[:3])
        return "\n".join(lines)
    if category == SemanticMemoryCategory.EVENTS.value:
        lines = ["## Event"]
        lines.extend(f"- {item}" for item in excerpt[:3])
        return "\n".join(lines)
    if category == SemanticMemoryCategory.CASES.value:
        problem = excerpt[0] if excerpt else text.strip()
        solution = excerpt[1] if len(excerpt) > 1 else text.strip()
        return "\n".join(["## Problem", f"- {problem}", "", "## Solution", f"- {solution}"])
    lines = ["## Pattern"]
    if topic:
        lines.append(f"- Topic: {topic}")
    lines.extend(f"- {item}" for item in excerpt[:3])
    return "\n".join(lines)


def extract_semantic_keywords(text: str, *, limit: int = 8) -> list[str]:
    return extract_keywords(text, limit=limit)
