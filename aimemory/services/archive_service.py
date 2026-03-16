from __future__ import annotations

from typing import Any

from aimemory.services.base import ServiceBase


class ArchiveService(ServiceBase):
    def __init__(self, db, projection, config, object_store, interaction_service, memory_service):
        super().__init__(db=db, projection=projection, config=config, object_store=object_store)
        self.interaction_service = interaction_service
        self.memory_service = memory_service

    def archive_session(
        self,
        session_id: str,
        user_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        summary: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().archive_session(
            session_id,
            user_id=user_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            metadata=metadata,
            budget_chars=None,
        )

    def archive_memory(
        self,
        memory_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().archive_memory(memory_id, metadata=metadata)

    def get_archive(self, archive_unit_id: str) -> dict[str, Any] | None:
        return self._kernel().get_archive_unit(archive_unit_id)

    def restore_archive(self, archive_unit_id: str) -> dict[str, Any]:
        unit = self.get_archive(archive_unit_id)
        if unit is None:
            raise ValueError(f"Archive `{archive_unit_id}` does not exist.")
        content_id = unit.get("content_id")
        if not content_id:
            return {"archive": unit, "payload": None}
        payload = self._kernel().memory_content_store.get_json("archive", content_id, {})
        from aimemory.core.utils import utcnow_iso

        now = utcnow_iso()
        self.db.execute(
            "UPDATE archive_memories SET last_rehydrated_at = ? WHERE id = ?",
            (now, archive_unit_id),
        )
        return {"archive": unit, "payload": payload}
