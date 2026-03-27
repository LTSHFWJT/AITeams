from __future__ import annotations

import itertools
import re
from typing import Any

from pydantic import Field, PrivateAttr
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult


SUBAGENT_LINE_RE = re.compile(r"^\s*-\s*([A-Za-z0-9_.:@+\-]+)\s*:", re.MULTILINE)


class MockDeepAgentChatModel(BaseChatModel):
    agent_name: str
    bound_tools: list[Any] = Field(default_factory=list)
    tool_choice: str | dict[str, Any] | None = None
    parallel_tool_calls: bool | None = None
    response_format: dict[str, Any] | None = None
    _tool_call_seq: int = PrivateAttr(default=0)

    @property
    def _llm_type(self) -> str:
        return "aiteams-deepagents-mock"

    def bind_tools(
        self,
        tools: list[Any],
        *,
        tool_choice: str | dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
        **kwargs: Any,
    ) -> "MockDeepAgentChatModel":
        return self.model_copy(
            update={
                "bound_tools": list(tools or []),
                "tool_choice": tool_choice,
                "parallel_tool_calls": parallel_tool_calls,
                "response_format": dict(kwargs.get("response_format") or {}) or None,
            }
        )

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        message = self._next_message(messages)
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _next_message(self, messages: list[BaseMessage]) -> AIMessage:
        custom_subagents = self._custom_subagent_types()
        tool_results = self._recent_tool_messages(messages)
        latest_prompt = self._latest_prompt(messages)
        if custom_subagents and not tool_results:
            tool_calls = []
            for index, subagent_type in enumerate(custom_subagents, start=1):
                self._tool_call_seq += 1
                tool_calls.append(
                    {
                        "id": f"call_{self._tool_call_seq}",
                        "name": "task",
                        "args": {
                            "subagent_type": subagent_type,
                            "description": f"Handle this delegated task for `{self.agent_name}`:\n\n{latest_prompt or 'No task provided.'}",
                        },
                        "type": "tool_call",
                    }
                )
                if not self.parallel_tool_calls and index >= 1:
                    break
            return AIMessage(content="", tool_calls=tool_calls)
        if tool_results:
            summary = " | ".join(item.content for item in tool_results if str(item.content or "").strip())
            text = summary or f"{self.agent_name} completed delegated work."
            return AIMessage(content=f"{self.agent_name}: {text}")
        prompt = latest_prompt or "No task provided."
        return AIMessage(content=f"{self.agent_name}: completed `{prompt}`.")

    def _custom_subagent_types(self) -> list[str]:
        task_tool = next((tool for tool in self.bound_tools if getattr(tool, "name", "") == "task"), None)
        if task_tool is None:
            return []
        description = str(getattr(task_tool, "description", "") or "")
        names = [match.group(1).strip() for match in SUBAGENT_LINE_RE.finditer(description)]
        return [name for name in names if name and name != "general-purpose"]

    def _recent_tool_messages(self, messages: list[BaseMessage]) -> list[ToolMessage]:
        tool_messages: list[ToolMessage] = []
        for message in reversed(messages):
            if isinstance(message, HumanMessage):
                break
            if isinstance(message, ToolMessage):
                tool_messages.append(message)
        tool_messages.reverse()
        return tool_messages

    def _latest_prompt(self, messages: list[BaseMessage]) -> str:
        for message in reversed(messages):
            if isinstance(message, HumanMessage):
                return self._message_text(message)
        for message in reversed(messages):
            text = self._message_text(message)
            if text:
                return text
        return ""

    def _message_text(self, message: BaseMessage) -> str:
        content = getattr(message, "content", "")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict):
                    text = str(item.get("text") or item.get("content") or "").strip()
                else:
                    text = str(item or "").strip()
                if text:
                    parts.append(text)
            return " ".join(parts)
        return str(content or "").strip()
