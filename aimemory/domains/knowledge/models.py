from __future__ import annotations

from enum import StrEnum


class KnowledgeSourceType(StrEnum):
    MANUAL = "manual"
    DIRECTORY = "directory"
    URL = "url"
    GIT = "git"
