from __future__ import annotations

import time

from aimemory.core.capabilities import capability_dict


class SessionMemoryPromoterWorker:
    def __init__(self, memory_service):
        self.memory_service = memory_service

    def run_once(
        self,
        session_id: str,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        run_id: str | None = None,
        limit: int = 50,
        min_importance: float = 0.55,
        include_memory_types: list[str] | None = None,
        force: bool = False,
        archive_after_promotion: bool = False,
        metadata: dict | None = None,
    ) -> dict:
        return self.memory_service.promote_session_memories(
            session_id=session_id,
            user_id=user_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            run_id=run_id,
            limit=limit,
            min_importance=min_importance,
            include_memory_types=include_memory_types,
            force=force,
            archive_after_promotion=archive_after_promotion,
            metadata=metadata,
        )

    def run_forever(self, session_ids: list[str], poll_interval: float = 5.0, **kwargs) -> None:
        while True:
            for session_id in session_ids:
                self.run_once(session_id=session_id, **kwargs)
            time.sleep(poll_interval)

    def describe_capabilities(self) -> dict:
        return capability_dict(
            category="worker",
            provider="session-promoter",
            features={
                "session_promotion": True,
                "scope_aware": True,
                "background_platform": False,
            },
            notes=["local worker for promoting session memories into long-term memory"],
        )
