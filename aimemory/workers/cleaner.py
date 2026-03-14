from __future__ import annotations

import time

from aimemory.core.capabilities import capability_dict


class LowValueMemoryCleanerWorker:
    def __init__(self, memory_service, archive_service):
        self.memory_service = memory_service
        self.archive_service = archive_service

    def run_once(
        self,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        run_id: str | None = None,
        scope: str = "long-term",
        limit: int = 100,
        threshold: float | None = None,
        archive: bool = True,
        delete: bool = False,
        dry_run: bool = False,
    ) -> dict:
        plan = self.memory_service.plan_low_value_cleanup(
            user_id=user_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            run_id=run_id,
            scope=scope,
            limit=limit,
            threshold=threshold,
        )
        if dry_run:
            return {"planned": len(plan["results"]), "results": plan["results"], "threshold": plan["threshold"]}

        archived: list[str] = []
        deleted: list[str] = []
        skipped: list[dict[str, str]] = []
        for candidate in plan["results"]:
            if archive:
                self.archive_service.archive_memory(
                    candidate["id"],
                    metadata={"cleanup": {"value_score": candidate["value_score"], "strategy_scope": candidate["strategy_scope"]}},
                )
                archived.append(candidate["id"])
            elif delete:
                self.memory_service.delete(candidate["id"])
                deleted.append(candidate["id"])
            else:
                skipped.append({"id": candidate["id"], "reason": "no-action-selected"})

        return {
            "planned": len(plan["results"]),
            "threshold": plan["threshold"],
            "archived_ids": archived,
            "deleted_ids": deleted,
            "skipped": skipped,
        }

    def run_forever(self, poll_interval: float = 300.0, **kwargs) -> None:
        while True:
            self.run_once(**kwargs)
            time.sleep(poll_interval)

    def describe_capabilities(self) -> dict:
        return capability_dict(
            category="worker",
            provider="memory-cleaner",
            features={
                "low_value_cleanup": True,
                "archive_first": True,
                "background_platform": False,
            },
            notes=["low-value memories are archived by default instead of hard-deleted"],
        )
