from __future__ import annotations

from typing import Any

from aiteams.ai_gateway import AIGateway
from aiteams.memory_service import AgentMemoryService
from aiteams.repositories import PlatformRepository
from aiteams.utils import trim_text


class CollaborationService:
    def __init__(self, repository: PlatformRepository, gateway: AIGateway, memory_service: AgentMemoryService):
        self.repository = repository
        self.gateway = gateway
        self.memory_service = memory_service

    def run(
        self,
        *,
        prompt: str,
        agent_ids: list[str],
        lead_agent_id: str | None = None,
        rounds: int = 1,
        title: str | None = None,
    ) -> dict[str, Any]:
        if not agent_ids:
            raise ValueError("At least one agent must be selected.")

        agents = self.repository.get_agents_by_ids(agent_ids)
        if len(agents) != len(agent_ids):
            raise ValueError("Some selected agents do not exist.")

        lead_agent = self._resolve_lead_agent(agents, lead_agent_id)
        session = self.repository.create_session(
            title=title or trim_text(prompt, limit=48),
            user_prompt=prompt,
            lead_agent_id=lead_agent["id"],
            strategy="lead-specialist-round-robin",
            rounds=rounds,
            metadata={"agent_ids": agent_ids},
        )

        for order_index, agent in enumerate(agents):
            self.repository.add_participant(session["id"], agent["id"], order_index)

        self.repository.add_message(
            session_id=session["id"],
            agent_id=None,
            role="user",
            round_index=0,
            content=prompt,
            metadata={"kind": "user_prompt"},
        )

        try:
            final_summary = ""
            if len(agents) == 1:
                single_message = self._run_agent_turn(
                    session_id=session["id"],
                    agent=lead_agent,
                    prompt=prompt,
                    round_index=1,
                    role="lead",
                    collaboration_brief="You are the only agent. Provide the complete answer in Chinese.",
                )
                final_summary = single_message["content"]
            else:
                specialist_agents = [agent for agent in agents if agent["id"] != lead_agent["id"]]
                coordinator_summary = ""
                for round_index in range(1, rounds + 1):
                    round_outputs: list[dict[str, Any]] = []
                    for agent in specialist_agents:
                        collaboration_brief = self._build_specialist_brief(
                            coordinator_summary=coordinator_summary,
                            round_outputs=round_outputs,
                            round_index=round_index,
                        )
                        round_outputs.append(
                            self._run_agent_turn(
                                session_id=session["id"],
                                agent=agent,
                                prompt=prompt,
                                round_index=round_index,
                                role="agent",
                                collaboration_brief=collaboration_brief,
                            )
                        )
                    coordinator_summary = self._run_agent_turn(
                        session_id=session["id"],
                        agent=lead_agent,
                        prompt=prompt,
                        round_index=round_index,
                        role="lead",
                        collaboration_brief=self._build_lead_brief(
                            prompt=prompt,
                            round_outputs=round_outputs,
                            round_index=round_index,
                            final_round=round_index == rounds,
                        ),
                    )["content"]
                    final_summary = coordinator_summary

            bundle = self.repository.update_session(
                session["id"],
                status="completed",
                final_summary=final_summary,
            )
            assert bundle is not None
            return self.repository.get_session_bundle(session["id"]) or {"session": bundle, "participants": [], "messages": []}
        except Exception as exc:
            self.repository.update_session(session["id"], status="failed", metadata={"error": str(exc)})
            raise

    def _resolve_lead_agent(self, agents: list[dict[str, Any]], lead_agent_id: str | None) -> dict[str, Any]:
        if lead_agent_id:
            for agent in agents:
                if agent["id"] == lead_agent_id:
                    return agent
            raise ValueError("Lead agent was not found in selected agents.")
        return agents[0]

    def _run_agent_turn(
        self,
        *,
        session_id: str,
        agent: dict[str, Any],
        prompt: str,
        round_index: int,
        role: str,
        collaboration_brief: str,
    ) -> dict[str, Any]:
        provider = self.repository.get_provider(str(agent["provider_id"]), include_secret=True)
        if provider is None:
            raise ValueError(f"Provider `{agent['provider_id']}` does not exist.")

        recalled = self.memory_service.recall(agent, session_id, f"{prompt}\n\n{collaboration_brief}", limit=5)
        memory_brief = self._render_memory_block(recalled)
        transcript_brief = self._render_transcript_block(session_id)
        user_brief = (
            f"Original request:\n{prompt}\n\n"
            f"Round instructions:\n{collaboration_brief}\n\n"
            f"Recent collaboration transcript:\n{transcript_brief}\n\n"
            f"Relevant memories:\n{memory_brief}\n\n"
            "Respond in Chinese. Be concrete and avoid generic filler."
        )

        self.memory_service.remember_brief(
            agent,
            session_id,
            user_brief,
            metadata={"round_index": round_index, "kind": "collaboration_brief", "role": role},
        )

        system_prompt = (
            f"You are {agent['name']}, acting as {agent['role']} in a multi-agent collaboration platform.\n"
            f"Working style: {agent.get('collaboration_style') or 'specialist'}.\n\n"
            f"{agent['system_prompt']}\n\n"
            "Always provide a useful incremental contribution. If there is uncertainty, state it explicitly."
        )

        response = self.gateway.chat(
            provider,
            [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_brief}],
            model=str(agent.get("resolved_model") or provider["model"]),
            temperature=float(agent.get("temperature") or 0.2),
            max_tokens=int(agent["max_tokens"]) if agent.get("max_tokens") is not None else None,
        )

        self.memory_service.remember_response(
            agent,
            session_id,
            response.content,
            metadata={"round_index": round_index, "kind": "agent_response", "role": role, "model": response.model},
        )

        references = [
            {"memory_id": item.get("id"), "score": item.get("score"), "summary": trim_text(item.get("text"), limit=120)}
            for item in recalled
        ]
        return self.repository.add_message(
            session_id=session_id,
            agent_id=str(agent["id"]),
            role=role,
            round_index=round_index,
            content=response.content,
            provider_id=str(provider["id"]),
            model=response.model,
            references=references,
            metadata={
                "agent_name": agent["name"],
                "agent_role": agent["role"],
                "usage": response.usage,
            },
        )

    def _build_specialist_brief(
        self,
        *,
        coordinator_summary: str,
        round_outputs: list[dict[str, Any]],
        round_index: int,
    ) -> str:
        peer_notes = "\n\n".join(
            f"{item.get('agent_name') or item.get('metadata', {}).get('agent_name') or 'Agent'}:\n{item['content']}"
            for item in round_outputs
        ) or "None yet."
        coordinator_block = coordinator_summary or "No coordinator summary yet."
        return (
            f"Round {round_index}. Expand the user's request from your specialty.\n"
            f"Coordinator guidance:\n{coordinator_block}\n\n"
            f"Peer contributions this round:\n{peer_notes}\n\n"
            "Return observations, risks, and a concrete recommendation."
        )

    def _build_lead_brief(
        self,
        *,
        prompt: str,
        round_outputs: list[dict[str, Any]],
        round_index: int,
        final_round: bool,
    ) -> str:
        contributions = "\n\n".join(
            f"{item.get('agent_name') or item.get('metadata', {}).get('agent_name') or 'Agent'} ({item.get('agent_role') or item.get('metadata', {}).get('agent_role') or 'specialist'}):\n{item['content']}"
            for item in round_outputs
        ) or "No specialist contributions."
        closing_instruction = (
            "Synthesize a final answer with a recommended execution plan."
            if final_round
            else "Summarize the current state and define what the next round should refine."
        )
        return (
            f"Round {round_index}. Coordinate the specialists for this user request:\n{prompt}\n\n"
            f"Specialist contributions:\n{contributions}\n\n"
            f"{closing_instruction}\n"
            "Be explicit about tradeoffs and next actions."
        )

    def _render_memory_block(self, recalled: list[dict[str, Any]]) -> str:
        if not recalled:
            return "No relevant memory recalled."
        lines = []
        for item in recalled[:5]:
            lines.append(f"- ({item.get('score', 0):.3f}) {trim_text(item.get('text'), limit=180)}")
        return "\n".join(lines)

    def _render_transcript_block(self, session_id: str) -> str:
        messages = self.repository.list_messages(session_id)[-6:]
        if not messages:
            return "No transcript yet."
        lines = []
        for item in messages:
            speaker = item.get("agent_name") or item["role"]
            lines.append(f"- {speaker}: {trim_text(item['content'], limit=160)}")
        return "\n".join(lines)
