from __future__ import annotations

from pathlib import Path
from typing import Any

from aiteams.utils import ensure_parent, slugify


class WorkspaceManager:
    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir).expanduser().resolve()
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def workspace_dir(self, workspace_id: str) -> Path:
        path = self.root_dir / workspace_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def project_dir(self, workspace_id: str, project_id: str) -> Path:
        path = self.workspace_dir(workspace_id) / "projects" / project_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def blueprint_dir(self, workspace_id: str, project_id: str) -> Path:
        path = self.project_dir(workspace_id, project_id) / "blueprints"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def run_dir(self, workspace_id: str, project_id: str, run_id: str) -> Path:
        path = self.project_dir(workspace_id, project_id) / "runs" / run_id
        for child in ("input", "output", "logs", "artifacts"):
            (path / child).mkdir(parents=True, exist_ok=True)
        return path

    def write_blueprint(self, *, workspace_id: str, project_id: str, blueprint_id: str, raw_text: str, raw_format: str) -> Path:
        target = self.blueprint_dir(workspace_id, project_id) / f"{blueprint_id}.{raw_format}"
        target.write_text(raw_text, encoding="utf-8")
        return target

    def write_artifact(self, *, workspace_id: str, project_id: str, run_id: str, name: str, content: str) -> Path:
        safe_name = slugify(Path(name).stem, fallback="artifact") + (Path(name).suffix or ".md")
        target = self.run_dir(workspace_id, project_id, run_id) / "artifacts" / safe_name
        ensure_parent(target)
        target.write_text(content, encoding="utf-8")
        return target

    def list_run_files(self, *, workspace_id: str, project_id: str, run_id: str) -> list[dict[str, Any]]:
        root = self.run_dir(workspace_id, project_id, run_id)
        items: list[dict[str, Any]] = []
        for path in sorted(root.rglob("*")):
            if path.is_dir():
                continue
            items.append(
                {
                    "relative_path": str(path.relative_to(root)).replace("\\", "/"),
                    "absolute_path": str(path),
                    "size_bytes": path.stat().st_size,
                }
            )
        return items
