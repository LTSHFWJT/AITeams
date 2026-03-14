from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class StoredObject:
    object_key: str
    object_type: str
    size_bytes: int
    checksum: str
    path: str
