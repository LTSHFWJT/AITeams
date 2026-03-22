from __future__ import annotations

from dataclasses import dataclass

from aimemory import Scope


@dataclass(slots=True)
class MemoryScopes:
    workspace_id: str
    project_id: str
    run_id: str
    agent_id: str

    def working(self) -> Scope:
        return Scope(
            workspace_id=self.workspace_id,
            project_id=self.project_id,
            agent_id=self.agent_id,
            session_id=self.run_id,
            run_id=self.run_id,
            namespace="working",
        )

    def project_shared(self) -> Scope:
        return Scope(workspace_id=self.workspace_id, project_id=self.project_id, namespace="project_shared")

    def agent_private(self) -> Scope:
        return Scope(workspace_id=self.workspace_id, project_id=self.project_id, agent_id=self.agent_id, namespace="agent_private")

    def run_retrospective(self) -> Scope:
        return Scope(workspace_id=self.workspace_id, project_id=self.project_id, run_id=self.run_id, namespace="run_retrospective")
