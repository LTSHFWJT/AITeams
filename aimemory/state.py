from __future__ import annotations

HEAD_STATE_ACTIVE = "active"
HEAD_STATE_ARCHIVED = "archived"
HEAD_STATE_DELETED = "deleted"

VERSION_STATE_ACTIVE = "active"
VERSION_STATE_SUPERSEDED = "superseded"
VERSION_STATE_ARCHIVED = "archived"
VERSION_STATE_DELETED = "deleted"

HEAD_STATES = {
    HEAD_STATE_ACTIVE,
    HEAD_STATE_ARCHIVED,
    HEAD_STATE_DELETED,
}

HEAD_STATE_TRANSITIONS = {
    HEAD_STATE_ACTIVE: {HEAD_STATE_ARCHIVED, HEAD_STATE_DELETED},
    HEAD_STATE_ARCHIVED: {HEAD_STATE_ACTIVE, HEAD_STATE_DELETED},
    HEAD_STATE_DELETED: {HEAD_STATE_ACTIVE},
}


def can_transition_head_state(current_state: str, target_state: str) -> bool:
    if current_state == target_state:
        return True
    return target_state in HEAD_STATE_TRANSITIONS.get(current_state, set())


def is_searchable_head_state(state: str) -> bool:
    return state == HEAD_STATE_ACTIVE


def derive_version_state(
    *,
    head_state: str,
    current_version_id: str,
    version_id: str,
    valid_to: int | None,
) -> str:
    if version_id != current_version_id and valid_to is not None:
        return VERSION_STATE_SUPERSEDED
    if head_state == HEAD_STATE_ARCHIVED:
        return VERSION_STATE_ARCHIVED
    if head_state == HEAD_STATE_DELETED:
        return VERSION_STATE_DELETED
    return VERSION_STATE_ACTIVE
