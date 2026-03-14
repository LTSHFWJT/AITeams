from __future__ import annotations

import time

from aimemory.core.capabilities import capability_dict


class ProjectorWorker:
    def __init__(self, projection_service):
        self.projection_service = projection_service

    def run_once(self, limit: int | None = None) -> dict:
        return self.projection_service.project_pending(limit=limit)

    def run_forever(self, poll_interval: float = 1.0, limit: int | None = None) -> None:
        while True:
            self.run_once(limit=limit)
            time.sleep(poll_interval)

    def describe_capabilities(self) -> dict:
        return capability_dict(
            category="worker",
            provider="projector",
            features={
                "outbox_projection": True,
                "batch_projection": True,
                "background_platform": False,
            },
            notes=["projects pending outbox events into index and graph backends"],
        )
