from __future__ import annotations

from typing import Any


def _tier_index(levels: list[int]) -> dict[int, int]:
    ordered = sorted({int(level) for level in levels}, reverse=True)
    return {level: index for index, level in enumerate(ordered)}


def build_adjacency_map(members: list[dict[str, Any]]) -> dict[str, list[str]]:
    levels = [int(member.get("level", 0)) for member in members]
    tiers = _tier_index(levels)
    grouped: dict[int, list[str]] = {}
    for member in members:
        grouped.setdefault(int(member.get("level", 0)), []).append(str(member.get("key") or ""))
    adjacency: dict[str, list[str]] = {}
    for member in members:
        key = str(member.get("key") or "")
        level = int(member.get("level", 0))
        index = tiers[level]
        allowed: list[str] = []
        for other in members:
            other_key = str(other.get("key") or "")
            if not other_key or other_key == key:
                continue
            other_level = int(other.get("level", 0))
            if abs(tiers[other_level] - index) == 1:
                allowed.append(other_key)
        adjacency[key] = sorted(set(allowed))
    return adjacency


def adjacency_targets(adjacency: dict[str, list[str]], source_agent_id: str) -> list[str]:
    return list(adjacency.get(source_agent_id, []))


def can_message(adjacency: dict[str, list[str]], source_agent_id: str, target_agent_id: str) -> bool:
    return target_agent_id in adjacency.get(source_agent_id, [])
