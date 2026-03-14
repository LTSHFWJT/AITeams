from __future__ import annotations

import hashlib
from pathlib import Path

from aimemory.domains.object.models import StoredObject


class LocalObjectStore:
    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path).expanduser().resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)

    def put_bytes(self, content: bytes, object_type: str, suffix: str = ".bin", prefix: str | None = None) -> StoredObject:
        checksum = hashlib.sha256(content).hexdigest()
        relative = Path(prefix) / object_type / checksum[:2] / f"{checksum}{suffix}" if prefix else Path(object_type) / checksum[:2] / f"{checksum}{suffix}"
        target = self.base_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_bytes(content)
        return StoredObject(
            object_key=str(relative).replace("\\", "/"),
            object_type=object_type,
            size_bytes=len(content),
            checksum=checksum,
            path=str(target),
        )

    def put_text(self, text: str, object_type: str, suffix: str = ".txt", prefix: str | None = None) -> StoredObject:
        return self.put_bytes(text.encode("utf-8"), object_type=object_type, suffix=suffix, prefix=prefix)

    def get_bytes(self, object_key: str) -> bytes:
        return (self.base_path / object_key).read_bytes()

    def get_text(self, object_key: str) -> str:
        return self.get_bytes(object_key).decode("utf-8")

    def delete(self, object_key: str) -> None:
        path = self.base_path / object_key
        if path.exists():
            path.unlink()
