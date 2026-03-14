from __future__ import annotations

import time

from aimemory.core.capabilities import capability_dict


class SessionCompactionWorker:
    def __init__(self, interaction_service):
        self.interaction_service = interaction_service

    def run_once(
        self,
        session_id: str,
        *,
        preserve_recent_turns: int | None = None,
        budget_chars: int | None = None,
        run_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
    ) -> dict:
        return self.interaction_service.compress_session_context(
            session_id=session_id,
            preserve_recent_turns=preserve_recent_turns,
            budget_chars=budget_chars,
            run_id=run_id,
            owner_agent_id=owner_agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
        )

    def run_forever(self, session_ids: list[str], poll_interval: float = 30.0, **kwargs) -> None:
        while True:
            for session_id in session_ids:
                self.run_once(session_id=session_id, **kwargs)
            time.sleep(poll_interval)

    def describe_capabilities(self) -> dict:
        return capability_dict(
            category="worker",
            provider="session-compactor",
            features={
                "session_compaction": True,
                "background_platform": False,
            },
            notes=["local worker for session summary and snapshot generation"],
        )
