from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from aimemory.core.utils import json_dumps, make_id, utcnow_iso
from aimemory.domains.interaction.models import SessionStatus
from aimemory.services.base import ServiceBase


class InteractionService(ServiceBase):
    def create_session(
        self,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
        title: str | None = None,
        ttl_seconds: int | None = None,
        metadata: dict[str, Any] | None = None,
        status: str = SessionStatus.ACTIVE,
    ) -> dict[str, Any]:
        session_kwargs: dict[str, Any] = {
            "user_id": user_id,
            "session_id": session_id,
            "agent_id": agent_id,
            "owner_agent_id": owner_agent_id,
            "subject_type": subject_type,
            "subject_id": subject_id,
            "interaction_type": interaction_type,
            "title": title,
            "metadata": metadata,
        }
        if ttl_seconds is not None:
            session_kwargs["ttl_seconds"] = ttl_seconds
        session = self._kernel().create_session(
            **session_kwargs,
        )
        if str(status) != str(SessionStatus.ACTIVE):
            self.db.execute("UPDATE sessions SET status = ?, updated_at = ? WHERE id = ?", (str(status), utcnow_iso(), session["id"]))
            session = self.get_session(session["id"])
            assert session is not None
        return session

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        return self._kernel().get_session(session_id)

    def append_turn(
        self,
        session_id: str,
        role: str,
        content: str,
        run_id: str | None = None,
        user_id: str | None = None,
        name: str | None = None,
        metadata: dict[str, Any] | None = None,
        tokens_in: int | None = None,
        tokens_out: int | None = None,
        turn_id: str | None = None,
        speaker_participant_id: str | None = None,
        target_participant_id: str | None = None,
        speaker_type: str | None = None,
        speaker_external_id: str | None = None,
        target_type: str | None = None,
        target_external_id: str | None = None,
        turn_type: str = "message",
        salience_score: float | None = None,
    ) -> dict[str, Any]:
        if self.get_session(session_id) is None:
            if not user_id and not speaker_external_id:
                raise ValueError(f"Session `{session_id}` does not exist.")
            self.create_session(
                user_id=user_id,
                session_id=session_id,
                owner_agent_id=target_external_id if target_type == "agent" else None,
                subject_type="human" if user_id else speaker_type,
                subject_id=user_id or speaker_external_id,
            )
        append_kwargs: dict[str, Any] = {
            "run_id": run_id,
            "name": name,
            "metadata": metadata,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "speaker_participant_id": speaker_participant_id,
            "target_participant_id": target_participant_id,
            "speaker_type": speaker_type,
            "speaker_external_id": speaker_external_id,
            "target_type": target_type,
            "target_external_id": target_external_id,
            "turn_type": turn_type,
        }
        if salience_score is not None:
            append_kwargs["salience_score"] = salience_score
        result = self._kernel().append_turn(
            session_id=session_id,
            role=role,
            content=content,
            **append_kwargs,
        )
        if turn_id and turn_id != result["id"]:
            self.db.execute("UPDATE conversation_turns SET id = ? WHERE id = ?", (turn_id, result["id"]))
            result["id"] = turn_id
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM conversation_turns WHERE id = ?", (result["id"],)))

    def list_turns(self, session_id: str, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        rows = self.db.fetch_all(
            """
            SELECT * FROM conversation_turns
            WHERE session_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            (session_id, limit, offset),
        )
        turns = self._deserialize_rows(rows)
        turns.reverse()
        return turns

    def upsert_snapshot(
        self,
        session_id: str,
        summary: str | None = None,
        plan: str | None = None,
        scratchpad: str | None = None,
        run_id: str | None = None,
        window_size: int = 20,
        metadata: dict[str, Any] | None = None,
        constraints: list[Any] | None = None,
        resolved_items: list[Any] | None = None,
        unresolved_items: list[Any] | None = None,
        next_actions: list[Any] | None = None,
        budget_tokens: int | None = None,
        salience_vector: list[float] | None = None,
        compression_revision: int = 1,
    ) -> dict[str, Any]:
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        snapshot_id = make_id("snapshot")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO working_memory_snapshots(
                id, session_id, run_id, owner_agent_id, interaction_type, subject_type, subject_id,
                summary, plan, scratchpad, window_size, constraints, resolved_items, unresolved_items,
                next_actions, budget_tokens, salience_vector, compression_revision, metadata, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                session_id,
                run_id,
                session.get("owner_agent_id") or session.get("agent_id"),
                session.get("interaction_type"),
                session.get("subject_type"),
                session.get("subject_id"),
                summary,
                plan,
                scratchpad,
                window_size,
                json_dumps(constraints or []),
                json_dumps(resolved_items or []),
                json_dumps(unresolved_items or []),
                json_dumps(next_actions or []),
                budget_tokens,
                json_dumps(salience_vector or []),
                compression_revision,
                json_dumps(metadata or {}),
                now,
                now,
            ),
        )
        return self._deserialize_row(self.db.fetch_one("SELECT * FROM working_memory_snapshots WHERE id = ?", (snapshot_id,)))

    def set_tool_state(
        self,
        session_id: str,
        tool_name: str,
        state_key: str,
        state_value: Any,
        run_id: str | None = None,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        record_id = make_id("tstate")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO tool_states(id, session_id, run_id, tool_name, state_key, state_value, expires_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id, run_id, tool_name, state_key) DO UPDATE SET
                state_value = excluded.state_value,
                expires_at = excluded.expires_at,
                updated_at = excluded.updated_at
            """,
            (record_id, session_id, run_id, tool_name, state_key, json_dumps(state_value), expires_at, now),
        )
        return self._deserialize_row(
            self.db.fetch_one(
                "SELECT * FROM tool_states WHERE session_id = ? AND COALESCE(run_id, '') = COALESCE(?, '') AND tool_name = ? AND state_key = ?",
                (session_id, run_id, tool_name, state_key),
            )
        )

    def set_variable(self, session_id: str, key: str, value: Any) -> dict[str, Any]:
        record_id = make_id("svar")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO session_variables(id, session_id, key, value, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(session_id, key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (record_id, session_id, key, json_dumps(value), now),
        )
        return self._deserialize_row(
            self.db.fetch_one("SELECT * FROM session_variables WHERE session_id = ? AND key = ?", (session_id, key))
        )

    def get_context(self, session_id: str, limit: int = 12) -> dict[str, Any]:
        session = self.get_session(session_id)
        if session is None:
            return {"session": None, "turns": [], "snapshot": None, "variables": [], "tool_states": [], "participants": []}
        snapshot = self._deserialize_row(
            self.db.fetch_one(
                "SELECT * FROM working_memory_snapshots WHERE session_id = ? ORDER BY updated_at DESC LIMIT 1",
                (session_id,),
            )
        )
        variables = self._deserialize_rows(
            self.db.fetch_all("SELECT * FROM session_variables WHERE session_id = ? ORDER BY updated_at DESC", (session_id,))
        )
        tool_states = self._deserialize_rows(
            self.db.fetch_all("SELECT * FROM tool_states WHERE session_id = ? ORDER BY updated_at DESC", (session_id,))
        )
        return {
            "session": session,
            "turns": self.list_turns(session_id=session_id, limit=limit, offset=0),
            "snapshot": snapshot,
            "variables": variables,
            "tool_states": tool_states,
            "participants": session.get("participants", []),
        }

    def compress_session_context(
        self,
        session_id: str,
        preserve_recent_turns: int | None = None,
        budget_chars: int | None = None,
        run_id: str | None = None,
        owner_agent_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        interaction_type: str | None = None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if preserve_recent_turns is not None:
            kwargs["preserve_recent_turns"] = preserve_recent_turns
        if budget_chars is not None:
            kwargs["budget_chars"] = budget_chars
        if run_id is not None:
            kwargs["run_id"] = run_id
        return self._kernel().compress_session_context(session_id, **kwargs)

    def session_health(self, session_id: str) -> dict[str, Any]:
        return self._kernel().session_health(session_id)

    def prune_snapshots(self, session_id: str, *, keep_recent: int | None = None) -> dict[str, Any]:
        kwargs = {"keep_recent": keep_recent} if keep_recent is not None else {}
        return self._kernel().prune_session_snapshots(session_id, **kwargs)

    def clear_session(self, session_id: str) -> dict[str, Any]:
        turn_rows = self.db.fetch_all("SELECT id FROM conversation_turns WHERE session_id = ?", (session_id,))
        snapshot_rows = self.db.fetch_all("SELECT id FROM working_memory_snapshots WHERE session_id = ?", (session_id,))
        self.db.execute("DELETE FROM conversation_turns WHERE session_id = ?", (session_id,))
        self.db.execute("DELETE FROM working_memory_snapshots WHERE session_id = ?", (session_id,))
        self.db.execute("DELETE FROM session_participants WHERE session_id = ?", (session_id,))
        self.db.execute("DELETE FROM tool_states WHERE session_id = ?", (session_id,))
        self.db.execute("DELETE FROM session_variables WHERE session_id = ?", (session_id,))
        kernel = self._kernel()
        for row in turn_rows:
            kernel._delete_auxiliary_index_record(row["id"], collection="interaction_turn")
        for row in snapshot_rows:
            kernel._delete_auxiliary_index_record(row["id"], collection="interaction_snapshot")
        self.db.execute("UPDATE sessions SET status = ?, updated_at = ? WHERE id = ?", (str(SessionStatus.CLOSED), utcnow_iso(), session_id))
        return {"message": "Session cleared", "session_id": session_id}
