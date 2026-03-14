from __future__ import annotations

from typing import Any

from aimemory.core.utils import utcnow_iso
from aimemory.domains.skill.models import SkillStatus
from aimemory.services.base import ServiceBase


class SkillService(ServiceBase):
    def register(
        self,
        name: str,
        description: str,
        owner_id: str | None = None,
        owner_agent_id: str | None = None,
        source_subject_type: str | None = None,
        source_subject_id: str | None = None,
        prompt_template: str | None = None,
        workflow: dict[str, Any] | str | None = None,
        schema: dict[str, Any] | None = None,
        version: str = "0.1.0",
        tools: list[str] | None = None,
        tests: list[dict[str, Any]] | None = None,
        topics: list[str] | None = None,
        assets: dict[str, Any] | None = None,
        status: str = SkillStatus.DRAFT,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        skill = self._kernel().save_skill(
            name=name,
            description=description,
            owner_agent_id=owner_agent_id or owner_id,
            source_subject_type=source_subject_type,
            source_subject_id=source_subject_id,
            prompt_template=prompt_template,
            workflow=workflow,
            schema=schema,
            version=version,
            tools=tools,
            tests=tests,
            topics=topics,
            metadata={**dict(metadata or {}), "assets": assets or {}},
        )
        if str(status) != str(SkillStatus.ACTIVE):
            self.db.execute("UPDATE skills SET status = ?, updated_at = ? WHERE id = ?", (str(status), utcnow_iso(), skill["id"]))
            refreshed = self.get_skill(skill["id"])
            assert refreshed is not None
            return refreshed
        return skill

    def get_skill(self, skill_id: str) -> dict[str, Any] | None:
        return self._kernel().get_skill(skill_id)

    def list_skills(self, status: str | None = None, owner_agent_id: str | None = None) -> dict[str, Any]:
        if status and owner_agent_id:
            rows = self.db.fetch_all(
                "SELECT * FROM skills WHERE status = ? AND (owner_agent_id = ? OR (owner_agent_id IS NULL AND owner_id = ?)) ORDER BY updated_at DESC",
                (status, owner_agent_id, owner_agent_id),
            )
        elif status:
            rows = self.db.fetch_all("SELECT * FROM skills WHERE status = ? ORDER BY updated_at DESC", (status,))
        elif owner_agent_id:
            rows = self.db.fetch_all(
                "SELECT * FROM skills WHERE (owner_agent_id = ? OR (owner_agent_id IS NULL AND owner_id = ?)) ORDER BY updated_at DESC",
                (owner_agent_id, owner_agent_id),
            )
        else:
            rows = self.db.fetch_all("SELECT * FROM skills ORDER BY updated_at DESC")
        return {"results": self._deserialize_rows(rows)}

    def activate_version(self, skill_id: str, version: str) -> dict[str, Any]:
        skill = self.get_skill(skill_id)
        if skill is None:
            raise ValueError(f"Skill `{skill_id}` does not exist.")
        match = next((item for item in skill["versions"] if item["version"] == version), None)
        if match is None:
            raise ValueError(f"Version `{version}` does not exist for skill `{skill_id}`.")
        self.db.execute("UPDATE skills SET status = ?, updated_at = ? WHERE id = ?", (str(SkillStatus.ACTIVE), utcnow_iso(), skill_id))
        refreshed = self.get_skill(skill_id)
        assert refreshed is not None
        return refreshed
