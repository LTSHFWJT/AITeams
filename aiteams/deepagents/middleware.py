from __future__ import annotations

import dataclasses
from datetime import date, datetime, time
from enum import Enum
from pathlib import Path
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import BaseMessage, messages_from_dict, messages_to_dict


def sanitize_checkpoint_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return sanitize_checkpoint_value(value.value)
    if isinstance(value, dict):
        return {key: sanitize_checkpoint_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [sanitize_checkpoint_value(item) for item in value]
    if dataclasses.is_dataclass(value):
        return {
            field.name: sanitize_checkpoint_value(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if hasattr(value, "_asdict") and callable(value._asdict):
        return sanitize_checkpoint_value(value._asdict())
    if hasattr(value, "model_dump") and callable(value.model_dump):
        for kwargs in (
            {"mode": "json", "warnings": False},
            {"warnings": False},
            {},
        ):
            try:
                return sanitize_checkpoint_value(value.model_dump(**kwargs))
            except TypeError:
                continue
            except Exception:
                break
    if hasattr(value, "dict") and callable(value.dict):
        try:
            return sanitize_checkpoint_value(value.dict())
        except Exception:
            pass
    return str(value)


def sanitize_message_for_checkpoint(message: BaseMessage) -> BaseMessage:
    payload = messages_to_dict([message])
    sanitized = sanitize_checkpoint_value(payload)
    rebuilt = messages_from_dict(sanitized)
    if rebuilt and isinstance(rebuilt[0], BaseMessage):
        return rebuilt[0]
    return message


class CheckpointMessageSanitizerMiddleware(AgentMiddleware[Any, Any, Any]):
    name = "checkpoint_message_sanitizer"

    def wrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Any,
    ) -> ModelResponse[Any]:
        response = handler(request)
        return self._sanitize_model_response(response)

    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Any,
    ) -> ModelResponse[Any]:
        response = await handler(request)
        return self._sanitize_model_response(response)

    def _sanitize_model_response(self, response: ModelResponse[Any]) -> ModelResponse[Any]:
        return ModelResponse(
            result=[
                sanitize_message_for_checkpoint(message)
                if isinstance(message, BaseMessage)
                else message
                for message in list(response.result or [])
            ],
            structured_response=response.structured_response,
        )
