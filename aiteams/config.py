from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class AppSettings:
    project_root: Path
    data_dir: Path
    platform_db_path: Path
    aimemory_root: Path
    aimemory_sqlite_path: Path
    static_dir: Path
    default_user_id: str = "platform-operator"
    request_timeout_seconds: float = 60.0

    @classmethod
    def load(cls) -> "AppSettings":
        project_root = Path(__file__).resolve().parents[1]
        data_dir = Path(os.getenv("AITEAMS_DATA_DIR", project_root / "data")).resolve()
        aimemory_root = Path(os.getenv("AITEAMS_MEMORY_DIR", data_dir / "aimemory")).resolve()
        platform_db_path = Path(os.getenv("AITEAMS_PLATFORM_DB", data_dir / "platform.db")).resolve()
        aimemory_sqlite_path = Path(os.getenv("AITEAMS_MEMORY_DB", aimemory_root / "aimemory.db")).resolve()
        static_dir = Path(__file__).resolve().parent / "static"

        data_dir.mkdir(parents=True, exist_ok=True)
        aimemory_root.mkdir(parents=True, exist_ok=True)
        platform_db_path.parent.mkdir(parents=True, exist_ok=True)
        aimemory_sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        return cls(
            project_root=project_root,
            data_dir=data_dir,
            platform_db_path=platform_db_path,
            aimemory_root=aimemory_root,
            aimemory_sqlite_path=aimemory_sqlite_path,
            static_dir=static_dir,
            default_user_id=os.getenv("AITEAMS_DEFAULT_USER_ID", "platform-operator"),
            request_timeout_seconds=float(os.getenv("AITEAMS_REQUEST_TIMEOUT", "60")),
        )
