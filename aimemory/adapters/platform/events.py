from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from aimemory.core.utils import utcnow_iso

if TYPE_CHECKING:
    from aimemory.core.facade import AIMemory


@runtime_checkable
class PlatformEventAdapter(Protocol):
    def on_turn_end(self, **payload: Any) -> Any: ...

    def on_agent_end(self, **payload: Any) -> Any: ...

    def on_handoff(self, **payload: Any) -> Any: ...

    def on_session_close(self, session_id: str, **payload: Any) -> Any: ...


class NullPlatformEventAdapter:
    def __init__(self, memory: "AIMemory" | None = None):
        self.memory = memory

    def on_turn_end(self, **payload: Any) -> dict[str, Any]:
        return {"handled": False, "event": "turn_end", "payload": dict(payload)}

    def on_agent_end(self, **payload: Any) -> dict[str, Any]:
        return {"handled": False, "event": "agent_end", "payload": dict(payload)}

    def on_handoff(self, **payload: Any) -> dict[str, Any]:
        return {"handled": False, "event": "handoff", "payload": dict(payload)}

    def on_session_close(self, session_id: str, **payload: Any) -> dict[str, Any]:
        return {"handled": False, "event": "session_close", "session_id": session_id, "payload": dict(payload)}


class AIMemoryPlatformEventAdapter:
    def __init__(self, memory: "AIMemory"):
        self.memory = memory

    def _session(self, session_id: str | None) -> dict[str, Any] | None:
        return self.memory.get_session(session_id) if session_id else None

    def _run(self, run_id: str | None) -> dict[str, Any] | None:
        if not run_id:
            return None
        return self.memory.db.fetch_one("SELECT * FROM runs WHERE id = ?", (run_id,))

    def _turn(self, turn_id: str | None) -> dict[str, Any] | None:
        if not turn_id:
            return None
        return self.memory.db.fetch_one("SELECT * FROM conversation_turns WHERE id = ?", (turn_id,))

    def _scope_kwargs(
        self,
        *,
        payload: dict[str, Any],
        session: dict[str, Any] | None,
        run: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "user_id": payload.get("user_id") or (run.get("user_id") if run else None) or (session.get("user_id") if session else None),
            "agent_id": payload.get("agent_id") or payload.get("owner_agent_id") or (run.get("owner_agent_id") if run else None) or (session.get("owner_agent_id") if session else None),
            "owner_agent_id": payload.get("owner_agent_id") or (run.get("owner_agent_id") if run else None) or (session.get("owner_agent_id") if session else None),
            "subject_type": payload.get("subject_type") or (run.get("subject_type") if run else None) or (session.get("subject_type") if session else None),
            "subject_id": payload.get("subject_id") or (run.get("subject_id") if run else None) or (session.get("subject_id") if session else None),
            "interaction_type": payload.get("interaction_type") or (run.get("interaction_type") if run else None) or (session.get("interaction_type") if session else None),
            "platform_id": payload.get("platform_id") or (run.get("platform_id") if run else None) or (session.get("platform_id") if session else None),
            "workspace_id": payload.get("workspace_id") or (run.get("workspace_id") if run else None) or (session.get("workspace_id") if session else None),
            "team_id": payload.get("team_id") or (run.get("team_id") if run else None) or (session.get("team_id") if session else None),
            "project_id": payload.get("project_id") or (run.get("project_id") if run else None) or (session.get("project_id") if session else None),
            "namespace_key": payload.get("namespace_key") or (run.get("namespace_key") if run else None) or (session.get("namespace_key") if session else None),
        }

    def _reflection_scope_kwargs(self, scope: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in scope.items() if key != "agent_id"}

    def _seed_query(
        self,
        *,
        payload: dict[str, Any],
        session: dict[str, Any] | None,
        run: dict[str, Any] | None,
        turn: dict[str, Any] | None,
    ) -> str:
        return str(
            payload.get("query")
            or (turn.get("content") if turn else None)
            or (run.get("goal") if run else None)
            or (session.get("title") if session else None)
            or "latest collaboration context"
        ).strip()

    def on_turn_end(self, **payload: Any) -> dict[str, Any]:
        session_id = str(payload.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("`session_id` is required for `on_turn_end`.")
        run_id = str(payload.get("run_id") or "").strip() or None
        turn_id = str(payload.get("turn_id") or "").strip() or None
        session = self._session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        run = self._run(run_id) if run_id else None
        turn = self._turn(turn_id) if turn_id else None
        scope = self._scope_kwargs(payload=payload, session=session, run=run)
        budget_chars = int(payload.get("budget_chars") or self.memory.config.memory_policy.compression_budget_chars)
        query = self._seed_query(payload=payload, session=session, run=run, turn=turn)
        auto_recall = bool(payload.get("auto_recall", True))
        auto_context = bool(payload.get("auto_context", False))
        auto_compress = bool(payload.get("auto_compress", True))

        result: dict[str, Any] = {
            "handled": True,
            "event": "turn_end",
            "session_id": session_id,
            "turn_id": turn_id,
            "run_id": run_id,
            "query": query,
        }
        if auto_compress:
            result["compression"] = self.memory.compress_session_context(session_id, run_id=run_id, budget_chars=budget_chars)
        if auto_recall and query:
            result["recall_plan"] = self.memory.plan_recall(query, session_id=session_id, run_id=run_id, **scope)
        if auto_context and query:
            result["context"] = self.memory.build_context(
                query,
                session_id=session_id,
                run_id=run_id,
                target_agent_id=payload.get("target_agent_id"),
                include_domains=payload.get("include_domains"),
                budget_chars=budget_chars,
                use_platform_llm=bool(payload.get("use_platform_llm", True)),
                metadata=dict(payload.get("metadata") or {}),
                **scope,
            )
        return result

    def on_agent_end(self, **payload: Any) -> dict[str, Any]:
        run_id = str(payload.get("run_id") or "").strip() or None
        session_id = str(payload.get("session_id") or "").strip() or None
        if not run_id and not session_id:
            raise ValueError("`run_id` or `session_id` is required for `on_agent_end`.")
        run = self._run(run_id) if run_id else None
        session_id = session_id or (str(run.get("session_id")) if run and run.get("session_id") else None)
        session = self._session(session_id) if session_id else None
        scope = self._scope_kwargs(payload=payload, session=session, run=run)
        budget_chars = int(payload.get("budget_chars") or max(self.memory.config.memory_policy.compression_budget_chars, 900))
        query = self._seed_query(payload=payload, session=session, run=run, turn=None)
        auto_recall = bool(payload.get("auto_recall", True))
        auto_context = bool(payload.get("auto_context", True))
        auto_reflect = bool(payload.get("auto_reflect", True))
        auto_compress = bool(payload.get("auto_compress", True))

        result: dict[str, Any] = {
            "handled": True,
            "event": "agent_end",
            "session_id": session_id,
            "run_id": run_id,
            "query": query,
        }
        if auto_compress and session_id:
            result["compression"] = self.memory.compress_session_context(session_id, run_id=run_id, budget_chars=budget_chars)
        if auto_recall and query:
            result["recall_plan"] = self.memory.plan_recall(query, session_id=session_id, run_id=run_id, **scope)
        if auto_context and query:
            result["context"] = self.memory.build_context(
                query,
                session_id=session_id,
                run_id=run_id,
                target_agent_id=payload.get("target_agent_id"),
                include_domains=payload.get("include_domains"),
                budget_chars=budget_chars,
                use_platform_llm=bool(payload.get("use_platform_llm", True)),
                metadata=dict(payload.get("metadata") or {}),
                **scope,
            )
        if auto_reflect:
            if session_id:
                result["reflection"] = self.memory.reflect_session(
                    session_id,
                    run_id=run_id,
                    mode=payload.get("mode"),
                    budget_chars=budget_chars,
                    use_platform_llm=bool(payload.get("use_platform_llm", True)),
                    metadata=dict(payload.get("metadata") or {}),
                    **self._reflection_scope_kwargs(scope),
                )
            elif run_id:
                result["reflection"] = self.memory.reflect_run(
                    run_id,
                    mode=payload.get("mode"),
                    budget_chars=budget_chars,
                    use_platform_llm=bool(payload.get("use_platform_llm", True)),
                    metadata=dict(payload.get("metadata") or {}),
                )
        return result

    def on_handoff(self, **payload: Any) -> dict[str, Any]:
        target_agent_id = str(payload.get("target_agent_id") or "").strip()
        if not target_agent_id:
            raise ValueError("`target_agent_id` is required for `on_handoff`.")
        source_session_id = str(payload.get("source_session_id") or payload.get("session_id") or "").strip() or None
        run_id = str(payload.get("source_run_id") or payload.get("run_id") or "").strip() or None
        session = self._session(source_session_id) if source_session_id else None
        run = self._run(run_id) if run_id else None
        scope = self._scope_kwargs(payload=payload, session=session, run=run)
        budget_chars = int(payload.get("budget_chars") or max(self.memory.config.memory_policy.compression_budget_chars, 900))
        query = self._seed_query(payload=payload, session=session, run=run, turn=None)
        include_context = bool(payload.get("include_context", False))

        result: dict[str, Any] = {
            "handled": True,
            "event": "handoff",
            "source_session_id": source_session_id,
            "run_id": run_id,
            "target_agent_id": target_agent_id,
        }
        result["handoff"] = self.memory.build_handoff_pack(
            target_agent_id,
            source_run_id=run_id,
            source_session_id=source_session_id,
            source_agent_id=payload.get("source_agent_id"),
            visibility=str(payload.get("visibility") or "target_agent"),
            query=query,
            budget_chars=budget_chars,
            use_platform_llm=bool(payload.get("use_platform_llm", True)),
            metadata=dict(payload.get("metadata") or {}),
            **scope,
        )
        if include_context and query:
            result["context"] = self.memory.build_context(
                query,
                session_id=source_session_id,
                run_id=run_id,
                target_agent_id=target_agent_id,
                include_domains=payload.get("include_domains"),
                budget_chars=budget_chars,
                use_platform_llm=bool(payload.get("use_platform_llm", True)),
                metadata={"handoff_event": True, **dict(payload.get("metadata") or {})},
                **scope,
            )
        return result

    def on_session_close(self, session_id: str, **payload: Any) -> dict[str, Any]:
        session = self._session(session_id)
        if session is None:
            raise ValueError(f"Session `{session_id}` does not exist.")
        run_id = str(payload.get("run_id") or "").strip() or None
        run = self._run(run_id) if run_id else None
        scope = self._scope_kwargs(payload=payload, session=session, run=run)
        budget_chars = int(payload.get("budget_chars") or max(self.memory.config.memory_policy.compression_budget_chars, 900))
        auto_compress = bool(payload.get("auto_compress", True))
        auto_reflect = bool(payload.get("auto_reflect", True))
        prune_snapshots = bool(payload.get("prune_snapshots", True))
        auto_archive = bool(payload.get("auto_archive", False))

        self.memory.db.execute("UPDATE sessions SET status = ?, updated_at = ? WHERE id = ?", ("closed", utcnow_iso(), session_id))

        result: dict[str, Any] = {
            "handled": True,
            "event": "session_close",
            "session_id": session_id,
            "run_id": run_id,
            "status": "closed",
        }
        if auto_compress:
            result["compression"] = self.memory.compress_session_context(session_id, run_id=run_id, budget_chars=budget_chars)
        if auto_reflect:
            result["reflection"] = self.memory.reflect_session(
                session_id,
                run_id=run_id,
                mode=payload.get("mode"),
                budget_chars=budget_chars,
                use_platform_llm=bool(payload.get("use_platform_llm", True)),
                metadata=dict(payload.get("metadata") or {}),
                **self._reflection_scope_kwargs(scope),
            )
        if prune_snapshots:
            result["prune"] = self.memory.prune_session_snapshots(
                session_id,
                keep_recent=int(payload.get("keep_recent", self.memory.config.memory_policy.snapshot_keep_recent)),
            )
        if auto_archive:
            result["archive"] = self.memory.archive_session(
                session_id,
                budget_chars=budget_chars,
                metadata=dict(payload.get("metadata") or {}),
            )
        return result
