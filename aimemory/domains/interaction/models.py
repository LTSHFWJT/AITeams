from __future__ import annotations

from enum import StrEnum


class SessionStatus(StrEnum):
    ACTIVE = "active"
    IDLE = "idle"
    ARCHIVED = "archived"
    CLOSED = "closed"
