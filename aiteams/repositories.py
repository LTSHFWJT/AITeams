from __future__ import annotations

from typing import Any

from aiteams.platform_db import PlatformDatabase
from aiteams.utils import json_dumps, json_loads, make_id, mask_secret, utcnow_iso


class PlatformRepository:
    def __init__(self, db: PlatformDatabase):
        self.db = db

    def _deserialize_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        item = dict(row)
        if "refs_json" in item and "references" not in item:
            item["references"] = item.pop("refs_json")
        for field in ("extra_headers", "extra_config", "metadata", "references"):
            if field in item:
                default: Any = [] if field == "references" else {}
                item[field] = json_loads(item.get(field), default)
        return item

    def _deserialize_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self._deserialize_row(row) for row in rows if row is not None]

    def _mask_provider(self, provider: dict[str, Any]) -> dict[str, Any]:
        masked = dict(provider)
        api_key = masked.pop("api_key", None)
        masked["has_api_key"] = bool(api_key)
        masked["api_key_masked"] = mask_secret(api_key)
        return masked

    def _provider_name_exists(self, name: str, *, exclude_id: str | None = None) -> bool:
        row = self.db.fetch_one("SELECT id FROM provider_configs WHERE name = ?", (name,))
        return bool(row and row["id"] != exclude_id)

    def _agent_name_exists(self, name: str, *, exclude_id: str | None = None) -> bool:
        row = self.db.fetch_one("SELECT id FROM agents WHERE name = ?", (name,))
        return bool(row and row["id"] != exclude_id)

    def provider_dependency_counts(self, provider_id: str) -> dict[str, int]:
        row = self.db.fetch_one(
            """
            SELECT
                (SELECT COUNT(*) FROM agents WHERE provider_id = ?) AS agent_count,
                (SELECT COUNT(*) FROM collaboration_messages WHERE provider_id = ?) AS message_count
            """,
            (provider_id, provider_id),
        ) or {"agent_count": 0, "message_count": 0}
        return {
            "agent_count": int(row.get("agent_count", 0) or 0),
            "message_count": int(row.get("message_count", 0) or 0),
        }

    def agent_dependency_counts(self, agent_id: str) -> dict[str, int]:
        row = self.db.fetch_one(
            """
            SELECT
                (SELECT COUNT(*) FROM collaboration_sessions WHERE lead_agent_id = ?) AS lead_session_count,
                (SELECT COUNT(*) FROM collaboration_participants WHERE agent_id = ?) AS participant_count,
                (SELECT COUNT(*) FROM collaboration_messages WHERE agent_id = ?) AS message_count
            """,
            (agent_id, agent_id, agent_id),
        ) or {"lead_session_count": 0, "participant_count": 0, "message_count": 0}
        return {
            "lead_session_count": int(row.get("lead_session_count", 0) or 0),
            "participant_count": int(row.get("participant_count", 0) or 0),
            "message_count": int(row.get("message_count", 0) or 0),
        }

    def _with_provider_stats(self, provider: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(provider)
        enriched.update(self.provider_dependency_counts(str(provider["id"])))
        return enriched

    def _with_agent_stats(self, agent: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(agent)
        enriched.update(self.agent_dependency_counts(str(agent["id"])))
        enriched["resolved_model"] = enriched.get("model_override") or enriched.get("provider_model")
        return enriched

    def _provider_filter_sql(self, filters: dict[str, Any] | None = None) -> tuple[str, tuple[Any, ...]]:
        if not filters:
            return "", ()
        clauses: list[str] = []
        params: list[Any] = []

        name = str(filters.get("name") or "").strip().lower()
        if name:
            clauses.append("LOWER(name) LIKE ?")
            params.append(f"%{name}%")

        provider_type = str(filters.get("provider_type") or "").strip()
        if provider_type:
            clauses.append("provider_type = ?")
            params.append(provider_type)

        model = str(filters.get("model") or "").strip().lower()
        if model:
            clauses.append("LOWER(model) LIKE ?")
            params.append(f"%{model}%")

        if not clauses:
            return "", ()
        return f" WHERE {' AND '.join(clauses)}", tuple(params)

    def _agent_filter_sql(self, filters: dict[str, Any] | None = None) -> tuple[str, tuple[Any, ...]]:
        if not filters:
            return "", ()
        clauses: list[str] = []
        params: list[Any] = []

        name = str(filters.get("name") or "").strip().lower()
        if name:
            clauses.append("LOWER(a.name) LIKE ?")
            params.append(f"%{name}%")

        role = str(filters.get("role") or "").strip().lower()
        if role:
            clauses.append("LOWER(a.role) LIKE ?")
            params.append(f"%{role}%")

        provider_id = str(filters.get("provider_id") or "").strip()
        if provider_id:
            clauses.append("a.provider_id = ?")
            params.append(provider_id)

        model = str(filters.get("model") or "").strip().lower()
        if model:
            clauses.append("LOWER(COALESCE(NULLIF(a.model_override, ''), p.model)) LIKE ?")
            params.append(f"%{model}%")

        if not clauses:
            return "", ()
        return f" WHERE {' AND '.join(clauses)}", tuple(params)

    def count_providers(self, *, filters: dict[str, Any] | None = None) -> int:
        where_sql, params = self._provider_filter_sql(filters)
        row = self.db.fetch_one(f"SELECT COUNT(*) AS count FROM provider_configs{where_sql}", params)
        return int((row or {}).get("count", 0) or 0)

    def count_agents(self, *, filters: dict[str, Any] | None = None) -> int:
        where_sql, params = self._agent_filter_sql(filters)
        row = self.db.fetch_one(
            f"""
            SELECT COUNT(*) AS count
            FROM agents a
            JOIN provider_configs p ON p.id = a.provider_id
            {where_sql}
            """,
            params,
        )
        return int((row or {}).get("count", 0) or 0)

    def save_provider(self, payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload["name"]).strip()
        provider_id = str(payload["id"]) if payload.get("id") else make_id("prov")
        existing = self.db.fetch_one(
            "SELECT id, created_at, api_key FROM provider_configs WHERE id = ?",
            (provider_id,),
        )
        if payload.get("id") and existing is None:
            raise ValueError("Provider does not exist.")
        if self._provider_name_exists(name, exclude_id=provider_id):
            raise ValueError("Provider name already exists.")

        created_at = existing["created_at"] if existing else utcnow_iso()
        now = utcnow_iso()
        incoming_api_key = payload.get("api_key")
        if payload.get("clear_api_key"):
            api_key = None
        elif incoming_api_key not in (None, ""):
            api_key = str(incoming_api_key)
        elif existing is not None:
            api_key = existing.get("api_key")
        else:
            api_key = None
        self.db.execute(
            """
            INSERT INTO provider_configs(
                id, name, provider_type, base_url, api_key, model, api_version, organization,
                extra_headers, extra_config, is_active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                provider_type = excluded.provider_type,
                base_url = excluded.base_url,
                api_key = excluded.api_key,
                model = excluded.model,
                api_version = excluded.api_version,
                organization = excluded.organization,
                extra_headers = excluded.extra_headers,
                extra_config = excluded.extra_config,
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
            """,
            (
                provider_id,
                name,
                str(payload["provider_type"]),
                payload.get("base_url"),
                api_key,
                str(payload["model"]),
                payload.get("api_version"),
                payload.get("organization"),
                json_dumps(payload.get("extra_headers", {})),
                json_dumps(payload.get("extra_config", {})),
                1 if payload.get("is_active", True) else 0,
                created_at,
                now,
            ),
        )
        provider = self.get_provider(provider_id, include_secret=True)
        assert provider is not None
        return self._mask_provider(provider)

    def list_providers(self, *, limit: int | None = None, offset: int = 0, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        where_sql, where_params = self._provider_filter_sql(filters)
        sql = f"SELECT * FROM provider_configs{where_sql} ORDER BY updated_at DESC, created_at DESC"
        params = where_params
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params = (*where_params, limit, offset)
        rows = self._deserialize_rows(self.db.fetch_all(sql, params))
        return [self._mask_provider(self._with_provider_stats(row)) for row in rows]

    def get_provider(self, provider_id: str, *, include_secret: bool = False) -> dict[str, Any] | None:
        provider = self._deserialize_row(self.db.fetch_one("SELECT * FROM provider_configs WHERE id = ?", (provider_id,)))
        if provider is None:
            return None
        provider = self._with_provider_stats(provider)
        return provider if include_secret else self._mask_provider(provider)

    def save_agent(self, payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload["name"]).strip()
        agent_id = str(payload["id"]) if payload.get("id") else make_id("agent")
        existing = self.db.fetch_one("SELECT id, created_at FROM agents WHERE id = ?", (agent_id,))
        if payload.get("id") and existing is None:
            raise ValueError("Agent does not exist.")
        if self._agent_name_exists(name, exclude_id=agent_id):
            raise ValueError("Agent name already exists.")

        created_at = existing["created_at"] if existing else utcnow_iso()
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO agents(
                id, name, role, system_prompt, provider_id, model_override, temperature, max_tokens,
                collaboration_style, is_enabled, metadata, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                role = excluded.role,
                system_prompt = excluded.system_prompt,
                provider_id = excluded.provider_id,
                model_override = excluded.model_override,
                temperature = excluded.temperature,
                max_tokens = excluded.max_tokens,
                collaboration_style = excluded.collaboration_style,
                is_enabled = excluded.is_enabled,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            (
                agent_id,
                name,
                str(payload["role"]),
                str(payload["system_prompt"]),
                str(payload["provider_id"]),
                payload.get("model_override"),
                float(payload.get("temperature", 0.2)),
                payload.get("max_tokens"),
                str(payload.get("collaboration_style", "specialist")),
                1 if payload.get("is_enabled", True) else 0,
                json_dumps(payload.get("metadata", {})),
                created_at,
                now,
            ),
        )
        agent = self.get_agent(agent_id)
        assert agent is not None
        return agent

    def list_agents(self, *, limit: int | None = None, offset: int = 0, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        where_sql, where_params = self._agent_filter_sql(filters)
        sql = """
            SELECT
                a.*,
                p.name AS provider_name,
                p.provider_type AS provider_type,
                p.model AS provider_model
            FROM agents a
            JOIN provider_configs p ON p.id = a.provider_id
        """
        sql += where_sql
        sql += """
            ORDER BY a.updated_at DESC, a.created_at DESC
        """
        params = where_params
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params = (*where_params, limit, offset)
        rows = self.db.fetch_all(
            sql,
            params,
        )
        agents = self._deserialize_rows(rows)
        return [self._with_agent_stats(agent) for agent in agents]

    def get_agent(self, agent_id: str) -> dict[str, Any] | None:
        rows = self.db.fetch_all(
            """
            SELECT
                a.*,
                p.name AS provider_name,
                p.provider_type AS provider_type,
                p.model AS provider_model
            FROM agents a
            JOIN provider_configs p ON p.id = a.provider_id
            WHERE a.id = ?
            """,
            (agent_id,),
        )
        if not rows:
            return None
        agent = self._deserialize_row(rows[0])
        assert agent is not None
        return self._with_agent_stats(agent)

    def get_agents_by_ids(self, agent_ids: list[str]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for agent_id in agent_ids:
            agent = self.get_agent(agent_id)
            if agent is not None:
                result.append(agent)
        return result

    def delete_provider(self, provider_id: str) -> dict[str, Any] | None:
        provider = self.get_provider(provider_id, include_secret=False)
        if provider is None:
            return None
        self.db.execute("DELETE FROM provider_configs WHERE id = ?", (provider_id,))
        return provider

    def delete_agent(self, agent_id: str) -> dict[str, Any] | None:
        agent = self.get_agent(agent_id)
        if agent is None:
            return None
        self.db.execute("DELETE FROM agents WHERE id = ?", (agent_id,))
        return agent

    def create_session(
        self,
        *,
        title: str | None,
        user_prompt: str,
        lead_agent_id: str | None,
        strategy: str,
        rounds: int,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        session_id = make_id("sess")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO collaboration_sessions(
                id, title, user_prompt, lead_agent_id, strategy, rounds, status, final_summary, metadata, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                title,
                user_prompt,
                lead_agent_id,
                strategy,
                rounds,
                "running",
                None,
                json_dumps(metadata or {}),
                now,
                now,
            ),
        )
        session = self.get_session(session_id)
        assert session is not None
        return session

    def update_session(
        self,
        session_id: str,
        *,
        status: str | None = None,
        final_summary: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        session = self.get_session(session_id)
        if session is None:
            return None
        merged_metadata = dict(session.get("metadata", {}))
        if metadata:
            merged_metadata.update(metadata)
        self.db.execute(
            """
            UPDATE collaboration_sessions
            SET status = ?, final_summary = ?, metadata = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                status or session["status"],
                final_summary if final_summary is not None else session.get("final_summary"),
                json_dumps(merged_metadata),
                utcnow_iso(),
                session_id,
            ),
        )
        return self.get_session(session_id)

    def add_participant(self, session_id: str, agent_id: str, turn_order: int) -> dict[str, Any]:
        record_id = make_id("part")
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO collaboration_participants(id, session_id, agent_id, turn_order, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(session_id, agent_id) DO UPDATE SET
                turn_order = excluded.turn_order
            """,
            (record_id, session_id, agent_id, turn_order, now),
        )
        participant = self.db.fetch_one(
            """
            SELECT
                cp.*,
                a.name AS agent_name,
                a.role AS agent_role
            FROM collaboration_participants cp
            JOIN agents a ON a.id = cp.agent_id
            WHERE cp.session_id = ? AND cp.agent_id = ?
            """,
            (session_id, agent_id),
        )
        assert participant is not None
        return participant

    def list_participants(self, session_id: str) -> list[dict[str, Any]]:
        return self.db.fetch_all(
            """
            SELECT
                cp.*,
                a.name AS agent_name,
                a.role AS agent_role
            FROM collaboration_participants cp
            JOIN agents a ON a.id = cp.agent_id
            WHERE cp.session_id = ?
            ORDER BY cp.turn_order ASC
            """,
            (session_id,),
        )

    def add_message(
        self,
        *,
        session_id: str,
        agent_id: str | None,
        role: str,
        round_index: int,
        content: str,
        provider_id: str | None = None,
        model: str | None = None,
        references: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        record_id = make_id("msg")
        row = self.db.fetch_one("SELECT COALESCE(MAX(order_index), -1) AS max_order FROM collaboration_messages WHERE session_id = ?", (session_id,))
        order_index = int(row["max_order"]) + 1 if row else 0
        now = utcnow_iso()
        self.db.execute(
            """
            INSERT INTO collaboration_messages(
                id, session_id, agent_id, role, round_index, order_index, content, provider_id, model, refs_json, metadata, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                session_id,
                agent_id,
                role,
                round_index,
                order_index,
                content,
                provider_id,
                model,
                json_dumps(references or []),
                json_dumps(metadata or {}),
                now,
            ),
        )
        self.db.execute("UPDATE collaboration_sessions SET updated_at = ? WHERE id = ?", (now, session_id))
        message = self.db.fetch_one(
            """
            SELECT
                cm.id,
                cm.session_id,
                cm.agent_id,
                cm.role,
                cm.round_index,
                cm.order_index,
                cm.content,
                cm.provider_id,
                cm.model,
                cm.refs_json,
                cm.metadata,
                cm.created_at,
                a.name AS agent_name,
                a.role AS agent_role
            FROM collaboration_messages cm
            LEFT JOIN agents a ON a.id = cm.agent_id
            WHERE cm.id = ?
            """,
            (record_id,),
        )
        assert message is not None
        deserialized = self._deserialize_row(message)
        assert deserialized is not None
        return deserialized

    def list_messages(self, session_id: str) -> list[dict[str, Any]]:
        rows = self.db.fetch_all(
            """
            SELECT
                cm.id,
                cm.session_id,
                cm.agent_id,
                cm.role,
                cm.round_index,
                cm.order_index,
                cm.content,
                cm.provider_id,
                cm.model,
                cm.refs_json,
                cm.metadata,
                cm.created_at,
                a.name AS agent_name,
                a.role AS agent_role
            FROM collaboration_messages cm
            LEFT JOIN agents a ON a.id = cm.agent_id
            WHERE cm.session_id = ?
            ORDER BY cm.order_index ASC
            """,
            (session_id,),
        )
        return self._deserialize_rows(rows)

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        rows = self.db.fetch_all(
            """
            SELECT
                cs.*,
                a.name AS lead_agent_name
            FROM collaboration_sessions cs
            LEFT JOIN agents a ON a.id = cs.lead_agent_id
            WHERE cs.id = ?
            """,
            (session_id,),
        )
        if not rows:
            return None
        return self._deserialize_row(rows[0])

    def get_session_bundle(self, session_id: str) -> dict[str, Any] | None:
        session = self.get_session(session_id)
        if session is None:
            return None
        return {
            "session": session,
            "participants": self.list_participants(session_id),
            "messages": self.list_messages(session_id),
        }

    def list_sessions(self, *, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.db.fetch_all(
            """
            SELECT
                cs.*,
                a.name AS lead_agent_name,
                COALESCE(msg.message_count, 0) AS message_count,
                COALESCE(part.participant_count, 0) AS participant_count
            FROM collaboration_sessions cs
            LEFT JOIN agents a ON a.id = cs.lead_agent_id
            LEFT JOIN (
                SELECT session_id, COUNT(*) AS message_count
                FROM collaboration_messages
                GROUP BY session_id
            ) msg ON msg.session_id = cs.id
            LEFT JOIN (
                SELECT session_id, COUNT(*) AS participant_count
                FROM collaboration_participants
                GROUP BY session_id
            ) part ON part.session_id = cs.id
            ORDER BY cs.updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return self._deserialize_rows(rows)
