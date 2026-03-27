from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class AppSettings:
    project_root: Path
    data_dir: Path
    metadata_db_path: Path
    memory_root: Path
    workspace_root: Path
    static_dir: Path
    checkpoint_db_path: Path | None = None
    default_workspace_id: str = "local-workspace"
    default_workspace_name: str = "Local Workspace"
    default_project_id: str = "default-project"
    default_project_name: str = "Default Project"
    server_host: str = "127.0.0.1"
    server_port: int = 8000

    def __post_init__(self) -> None:
        if self.checkpoint_db_path is None:
            self.checkpoint_db_path = (self.data_dir / "langgraph-checkpoints.sqlite").expanduser().resolve()
        else:
            self.checkpoint_db_path = Path(self.checkpoint_db_path).expanduser().resolve()
        self.checkpoint_db_path.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def load(cls) -> "AppSettings":
        project_root = Path(__file__).resolve().parents[2]
        data_dir = Path(os.getenv("AITEAMS_DATA_DIR", project_root / "data")).expanduser().resolve()
        metadata_db_path = Path(os.getenv("AITEAMS_METADATA_DB", data_dir / "platform.db")).expanduser().resolve()
        memory_root = Path(os.getenv("AITEAMS_MEMORY_DIR", data_dir / "aimemory")).expanduser().resolve()
        workspace_root = Path(os.getenv("AITEAMS_WORKSPACE_DIR", data_dir / "workspaces")).expanduser().resolve()
        checkpoint_db_path = Path(os.getenv("AITEAMS_CHECKPOINT_DB", data_dir / "langgraph-checkpoints.sqlite")).expanduser().resolve()
        static_dir = Path(__file__).resolve().parents[1] / "static"

        data_dir.mkdir(parents=True, exist_ok=True)
        metadata_db_path.parent.mkdir(parents=True, exist_ok=True)
        memory_root.mkdir(parents=True, exist_ok=True)
        workspace_root.mkdir(parents=True, exist_ok=True)
        checkpoint_db_path.parent.mkdir(parents=True, exist_ok=True)

        return cls(
            project_root=project_root,
            data_dir=data_dir,
            metadata_db_path=metadata_db_path,
            memory_root=memory_root,
            workspace_root=workspace_root,
            checkpoint_db_path=checkpoint_db_path,
            static_dir=static_dir,
            default_workspace_id=os.getenv("AITEAMS_DEFAULT_WORKSPACE_ID", "local-workspace"),
            default_workspace_name=os.getenv("AITEAMS_DEFAULT_WORKSPACE_NAME", "Local Workspace"),
            default_project_id=os.getenv("AITEAMS_DEFAULT_PROJECT_ID", "default-project"),
            default_project_name=os.getenv("AITEAMS_DEFAULT_PROJECT_NAME", "Default Project"),
            server_host=os.getenv("AITEAMS_HOST", "127.0.0.1"),
            server_port=int(os.getenv("AITEAMS_PORT", "8000")),
        )
