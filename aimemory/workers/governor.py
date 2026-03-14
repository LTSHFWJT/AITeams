from __future__ import annotations

import time

from aimemory.core.capabilities import capability_dict


class GovernanceAutomationWorker:
    def __init__(self, interaction_service, memory_service, cleaner_worker):
        self.interaction_service = interaction_service
        self.memory_service = memory_service
        self.cleaner_worker = cleaner_worker

    def assess_session(self, session_id: str) -> dict:
        return self.interaction_service.session_health(session_id)

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
        compact: bool = True,
        promote: bool = True,
        prune_snapshots: bool = True,
        cleanup: bool = False,
        cleanup_scope: str = "long-term",
        cleanup_threshold: float | None = None,
        cleanup_dry_run: bool = False,
        force: bool = False,
        compaction_kwargs: dict | None = None,
        promotion_kwargs: dict | None = None,
        prune_kwargs: dict | None = None,
        cleanup_kwargs: dict | None = None,
    ) -> dict:
        before = self.assess_session(session_id)
        session = self.interaction_service.get_session(session_id) or {}
        effective_user_id = user_id or session.get("user_id")
        effective_agent_id = agent_id or session.get("agent_id") or session.get("owner_agent_id")
        effective_owner_agent_id = owner_agent_id or session.get("owner_agent_id") or session.get("agent_id") or effective_agent_id
        effective_subject_type = subject_type or session.get("subject_type")
        effective_subject_id = subject_id or session.get("subject_id")
        effective_interaction_type = interaction_type or session.get("interaction_type")
        recommendations = list(before.get("recommendations", []))

        result: dict[str, object] = {
            "session_id": session_id,
            "owner_agent_id": effective_owner_agent_id,
            "subject_type": effective_subject_type,
            "subject_id": effective_subject_id,
            "interaction_type": effective_interaction_type,
            "health_before": before,
            "actions": [],
        }

        if compact and (force or "compact" in recommendations):
            compaction_args = dict(compaction_kwargs or {})
            compaction_args.setdefault("owner_agent_id", effective_owner_agent_id)
            compaction_args.setdefault("subject_type", effective_subject_type)
            compaction_args.setdefault("subject_id", effective_subject_id)
            compaction_args.setdefault("interaction_type", effective_interaction_type)
            result["compaction"] = self.interaction_service.compress_session_context(session_id=session_id, **compaction_args)
            result["actions"].append("compact")

        if promote and (force or "promote" in recommendations):
            promotion_args = dict(promotion_kwargs or {})
            promotion_args.setdefault("user_id", effective_user_id)
            promotion_args.setdefault("agent_id", effective_agent_id)
            promotion_args.setdefault("owner_agent_id", effective_owner_agent_id)
            promotion_args.setdefault("subject_type", effective_subject_type)
            promotion_args.setdefault("subject_id", effective_subject_id)
            promotion_args.setdefault("interaction_type", effective_interaction_type)
            promotion_args.setdefault("run_id", run_id)
            result["promotion"] = self.memory_service.promote_session_memories(session_id=session_id, **promotion_args)
            result["actions"].append("promote")

        if prune_snapshots and (force or "prune_snapshots" in recommendations):
            result["snapshot_prune"] = self.interaction_service.prune_snapshots(
                session_id=session_id,
                **dict(prune_kwargs or {}),
            )
            result["actions"].append("prune_snapshots")

        if cleanup:
            cleanup_args = dict(cleanup_kwargs or {})
            cleanup_args.setdefault("user_id", effective_user_id)
            cleanup_args.setdefault("agent_id", effective_agent_id)
            cleanup_args.setdefault("owner_agent_id", effective_owner_agent_id)
            cleanup_args.setdefault("subject_type", effective_subject_type)
            cleanup_args.setdefault("subject_id", effective_subject_id)
            cleanup_args.setdefault("interaction_type", effective_interaction_type)
            cleanup_args.setdefault("run_id", run_id)
            cleanup_args.setdefault("scope", cleanup_scope)
            cleanup_args.setdefault("threshold", cleanup_threshold)
            cleanup_args.setdefault("dry_run", cleanup_dry_run)
            result["cleanup"] = self.cleaner_worker.run_once(**cleanup_args)
            result["actions"].append("cleanup")

        result["health_after"] = self.assess_session(session_id)
        return result

    def run_forever(self, session_ids: list[str], poll_interval: float = 60.0, **kwargs) -> None:
        while True:
            for session_id in session_ids:
                self.run_once(session_id, **kwargs)
            time.sleep(poll_interval)

    def describe_capabilities(self) -> dict:
        return capability_dict(
            category="worker",
            provider="governance-automation",
            features={
                "session_health": True,
                "compaction": True,
                "promotion": True,
                "snapshot_pruning": True,
                "cleanup": True,
                "background_platform": False,
            },
            notes=["local orchestration worker for lightweight governance automation"],
        )
