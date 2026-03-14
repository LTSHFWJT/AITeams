from __future__ import annotations

from enum import StrEnum


class MemoryScope(StrEnum):
    SESSION = "session"
    LONG_TERM = "long-term"


class MemoryType(StrEnum):
    SEMANTIC = "semantic"
    EPISODIC = "episodic"
    PROCEDURAL = "procedural"
    PROFILE = "profile"
    PREFERENCE = "preference"
    RELATIONSHIP_SUMMARY = "relationship_summary"
