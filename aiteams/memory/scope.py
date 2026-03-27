from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Scope:
    workspace_id: str
    project_id: str
    namespace: str
    agent_id: str | None = None
    run_id: str | None = None
    team_id: str | None = None
    user_id: str | None = None
    session_id: str | None = None

    @property
    def key(self) -> str:
        parts = [
            f"workspace={self.workspace_id}",
            f"project={self.project_id}",
            f"namespace={self.namespace}",
        ]
        if self.team_id:
            parts.append(f"team={self.team_id}")
        if self.agent_id:
            parts.append(f"agent={self.agent_id}")
        if self.run_id:
            parts.append(f"run={self.run_id}")
        if self.user_id:
            parts.append(f"user={self.user_id}")
        if self.session_id:
            parts.append(f"session={self.session_id}")
        return "|".join(parts)


@dataclass(slots=True)
class MemoryScopes:
    workspace_id: str
    project_id: str
    run_id: str
    agent_id: str
    team_id: str | None = None
    user_id: str | None = None

    def working(self) -> Scope:
        return Scope(
            workspace_id=self.workspace_id,
            project_id=self.project_id,
            agent_id=self.agent_id,
            run_id=self.run_id,
            team_id=self.team_id,
            user_id=self.user_id,
            session_id=self.run_id,
            namespace="working",
        )

    def project_shared(self) -> Scope:
        return Scope(
            workspace_id=self.workspace_id,
            project_id=self.project_id,
            team_id=self.team_id,
            user_id=self.user_id,
            namespace="project_shared",
        )

    def agent_private(self) -> Scope:
        return Scope(
            workspace_id=self.workspace_id,
            project_id=self.project_id,
            agent_id=self.agent_id,
            team_id=self.team_id,
            user_id=self.user_id,
            namespace="agent_private",
        )

    def team_shared(self) -> Scope:
        return Scope(
            workspace_id=self.workspace_id,
            project_id=self.project_id,
            team_id=self.team_id,
            user_id=self.user_id,
            namespace="team_shared",
        )

    def user_profile(self) -> Scope:
        return Scope(
            workspace_id=self.workspace_id,
            project_id=self.project_id,
            user_id=self.user_id,
            namespace="user_profile",
        )

    def run_retrospective(self) -> Scope:
        return Scope(
            workspace_id=self.workspace_id,
            project_id=self.project_id,
            run_id=self.run_id,
            team_id=self.team_id,
            user_id=self.user_id,
            namespace="run_retrospective",
        )
