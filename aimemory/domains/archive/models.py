from __future__ import annotations

from enum import StrEnum


class ArchiveDomain(StrEnum):
    SESSION = "session"
    MEMORY = "memory"
    DOCUMENT = "document"
    RUN = "run"
