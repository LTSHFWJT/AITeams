from __future__ import annotations

from typing import Any

from aimemory.core.utils import json_dumps, make_id, utcnow_iso
from aimemory.domains.execution.models import RunStatus, TaskStatus
from aimemory.services.base import ServiceBase


class ExecutionService(ServiceBase):
    def start_run(
        self,
        user_id: str | None,
        goal: str,
        session_id: str | None = None,
        run_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._kernel().start_run(
            user_id=user_id,
            goal=goal,
            session_id=session_id,
            run_id=run_id,
            agent_id=agent_id,
            owner_agent_id=owner_agent_id or agent_id,
            subject_type=subject_type,
            subject_id=subject_id,
            interaction_type=interaction_type,
            metadata=metadata,
            status=str(RunStatus.RUNNING),
        )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM runs WHERE id = ?", (run_id,)))

    def update_run(self, run_id: str, status: str, metadata: dict[str, Any] | None = None, ended: bool = False) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run is None:
            raise ValueError(f"Run `{run_id}` does not exist.")
        updated_metadata = dict(run.get("metadata") or {})
        if metadata:
            updated_metadata.update(metadata)
        ended_at = utcnow_iso() if ended else run.get("ended_at")
        self.db.execute(
            "UPDATE runs SET status = ?, metadata = ?, ended_at = ?, updated_at = ? WHERE id = ?",
            (status, json_dumps(updated_metadata), ended_at, utcnow_iso(), run_id),
        )
        return self.get_run(run_id)

    def create_task(
        self,
        run_id: str,
        title: str,
        task_id: str | None = None,
        session_id: str | None = None,
        parent_task_id: str | None = None,
        priority: int = 50,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        task_id = task_id or make_id("task")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO tasks(id, run_id, session_id, parent_task_id, title, status, priority, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, run_id, session_id, parent_task_id, title, str(TaskStatus.PENDING), priority, json_dumps(metadata or {}), now, now),
        )
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,)))

    def add_task_step(
        self,
        task_id: str,
        run_id: str,
        title: str,
        detail: str | None = None,
        step_index: int | None = None,
        status: str = TaskStatus.PENDING,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        step_id = make_id("step")
        if step_index is None:
            row = self.db.fetch_one("SELECT COALESCE(MAX(step_index), -1) AS last_step FROM task_steps WHERE task_id = ?", (task_id,))
            step_index = int(row["last_step"]) + 1 if row else 0
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO task_steps(id, task_id, run_id, step_index, title, status, detail, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (step_id, task_id, run_id, step_index, title, status, detail, json_dumps(metadata or {}), now, now),
        )
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM task_steps WHERE id = ?", (step_id,)))

    def checkpoint(
        self,
        run_id: str,
        snapshot: dict[str, Any],
        session_id: str | None = None,
        checkpoint_name: str = "checkpoint",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        checkpoint_id = make_id("ckpt")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO run_checkpoints(id, run_id, session_id, checkpoint_name, snapshot, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (checkpoint_id, run_id, session_id, checkpoint_name, json_dumps(snapshot), json_dumps(metadata or {}), now),
        )
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM run_checkpoints WHERE id = ?", (checkpoint_id,)), ("snapshot", "metadata"))

    def log_tool_call(
        self,
        run_id: str,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
        result: Any = None,
        task_id: str | None = None,
        session_id: str | None = None,
        status: str = "completed",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        tool_call_id = make_id("toolcall")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO tool_calls(id, run_id, task_id, session_id, tool_name, arguments, result, status, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (tool_call_id, run_id, task_id, session_id, tool_name, json_dumps(arguments or {}), json_dumps(result), status, json_dumps(metadata or {}), now),
        )
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM tool_calls WHERE id = ?", (tool_call_id,)), ("arguments", "result", "metadata"))

    def add_observation(
        self,
        run_id: str,
        kind: str,
        content: str,
        task_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        observation_id = make_id("obs")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO observations(id, run_id, task_id, session_id, kind, content, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (observation_id, run_id, task_id, session_id, kind, content, json_dumps(metadata or {}), now),
        )
        observation = self._deserialize_row(self.db.fetch_one("SELECT * FROM observations WHERE id = ?", (observation_id,)))
        if observation is not None:
            run = self.get_run(run_id)
            self._kernel()._index_execution_observation(observation, run=run)
        return observation

    def get_run_timeline(self, run_id: str) -> dict[str, Any]:
        return {
            "run": self.get_run(run_id),
            "tasks": self._deserialize_rows(self.db.fetch_all("SELECT * FROM tasks WHERE run_id = ? ORDER BY created_at ASC", (run_id,))),
            "steps": self._deserialize_rows(self.db.fetch_all("SELECT * FROM task_steps WHERE run_id = ? ORDER BY step_index ASC, created_at ASC", (run_id,))),
            "checkpoints": self._deserialize_rows(self.db.fetch_all("SELECT * FROM run_checkpoints WHERE run_id = ? ORDER BY created_at ASC", (run_id,)), ("snapshot", "metadata")),
            "tool_calls": self._deserialize_rows(self.db.fetch_all("SELECT * FROM tool_calls WHERE run_id = ? ORDER BY created_at ASC", (run_id,)), ("arguments", "result", "metadata")),
            "observations": self._deserialize_rows(self.db.fetch_all("SELECT * FROM observations WHERE run_id = ? ORDER BY created_at ASC", (run_id,))),
        }
