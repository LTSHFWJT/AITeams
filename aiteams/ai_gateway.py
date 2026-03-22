from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "true")

try:
    import litellm
    from litellm import completion
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    litellm = None
    completion = None

from aiteams.catalog import preset_for
from aiteams.utils import trim_text


if litellm is not None:
    litellm.suppress_debug_info = True
    litellm.telemetry = False


@dataclass(slots=True)
class ChatResult:
    content: str
    model: str
    usage: dict[str, Any]
    raw: dict[str, Any]


class ProviderRequestError(RuntimeError):
    pass


class AIGateway:
    def __init__(self, *, timeout_seconds: float = 60.0):
        self._timeout_seconds = timeout_seconds

    def close(self) -> None:
        return None

    def chat(
        self,
        provider: dict[str, Any],
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = None,
    ) -> ChatResult:
        provider_type = str(provider["provider_type"])
        resolved_model = str(model or provider.get("model") or "").strip()
        if not resolved_model:
            raise ProviderRequestError("Model is required.")

        if provider_type == "mock":
            return self._chat_mock(provider, messages, resolved_model)
        return self._chat_litellm(provider, messages, resolved_model, temperature, max_tokens)

    def _chat_mock(self, provider: dict[str, Any], messages: list[dict[str, str]], model: str) -> ChatResult:
        system = next((item["content"] for item in messages if item["role"] == "system"), "")
        user = next((item["content"] for item in reversed(messages) if item["role"] == "user"), "")
        provider_name = str(provider.get("name") or "mock")
        content = f"[{provider_name}/{model}] {trim_text(system, limit=70)} | {trim_text(user, limit=180)}"
        return ChatResult(content=content, model=model, usage={"prompt_tokens": 0, "completion_tokens": 0}, raw={"mock": True})

    def _chat_litellm(
        self,
        provider: dict[str, Any],
        messages: list[dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int | None,
    ) -> ChatResult:
        if completion is None:
            raise ProviderRequestError("litellm is not installed. Install it or switch the agent backend to mock.")
        preset = preset_for(str(provider["provider_type"]))
        litellm_model = self._resolve_litellm_model(provider, model, preset)
        request_kwargs: dict[str, Any] = {
            "model": litellm_model,
            "messages": messages,
            "temperature": temperature,
            "timeout": self._timeout_seconds,
            "drop_params": True,
        }
        if max_tokens is not None:
            request_kwargs["max_tokens"] = max_tokens

        base_url = self._resolve_base_url(provider, preset)
        if base_url:
            request_kwargs["base_url"] = base_url
        if provider.get("api_key"):
            request_kwargs["api_key"] = str(provider["api_key"])
        if provider.get("api_version"):
            request_kwargs["api_version"] = str(provider["api_version"])
        if provider.get("organization"):
            request_kwargs["organization"] = str(provider["organization"])

        extra_headers = provider.get("extra_headers")
        if isinstance(extra_headers, dict) and extra_headers:
            request_kwargs["extra_headers"] = {str(key): str(value) for key, value in extra_headers.items()}

        extra_config = provider.get("extra_config")
        if isinstance(extra_config, dict) and extra_config:
            request_kwargs.update(extra_config)

        try:
            response = completion(**request_kwargs)
        except Exception as exc:
            detail = trim_text(str(exc), limit=500) or exc.__class__.__name__
            raise ProviderRequestError(f"Provider request failed: {detail}") from exc

        raw = self._to_dict(response)
        content = self._extract_content(response)
        usage = self._extract_usage(response, raw)
        response_model = str(raw.get("model") or litellm_model)
        return ChatResult(content=content, model=response_model, usage=usage, raw=raw)

    def _resolve_litellm_model(self, provider: dict[str, Any], model: str, preset: dict[str, Any]) -> str:
        normalized = str(model).strip()
        if not normalized:
            raise ProviderRequestError("Model is required.")
        llm_provider = str(preset.get("litellm_provider") or provider.get("provider_type") or "").strip()
        if llm_provider and normalized.startswith(f"{llm_provider}/"):
            return normalized
        return f"{llm_provider}/{normalized}" if llm_provider and llm_provider != "mock" else normalized

    def _resolve_base_url(self, provider: dict[str, Any], preset: dict[str, Any]) -> str | None:
        configured = str(provider.get("base_url") or "").strip()
        if configured:
            return configured
        if preset.get("use_default_base_url_when_blank"):
            default_base_url = str(preset.get("default_base_url") or "").strip()
            return default_base_url or None
        return None

    def _extract_content(self, response: Any) -> str:
        choices = self._get_value(response, "choices")
        if isinstance(choices, list) and choices:
            message = self._get_value(choices[0], "message")
            if message is not None:
                return self._coerce_content(self._get_value(message, "content"))
        return ""

    def _extract_usage(self, response: Any, raw: dict[str, Any]) -> dict[str, Any]:
        usage = self._get_value(response, "usage")
        if usage is not None:
            usage_dict = self._to_dict(usage)
            if usage_dict:
                return usage_dict
        raw_usage = raw.get("usage")
        if isinstance(raw_usage, dict):
            return raw_usage
        return {}

    def _get_value(self, obj: Any, key: str) -> Any:
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    def _to_dict(self, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        if hasattr(value, "model_dump"):
            dumped = value.model_dump()
            return dumped if isinstance(dumped, dict) else {}
        if hasattr(value, "dict"):
            dumped = value.dict()
            return dumped if isinstance(dumped, dict) else {}
        try:
            return dict(value)
        except Exception:
            return {}

    def _coerce_content(self, content: Any) -> str:
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            texts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    texts.append(item)
                    continue
                if isinstance(item, dict):
                    if item.get("type") == "text" and item.get("text"):
                        texts.append(str(item["text"]))
                        continue
                    if "content" in item:
                        texts.append(str(item["content"]))
                        continue
                text = getattr(item, "text", None)
                if text:
                    texts.append(str(text))
            return "\n".join(part for part in texts if part).strip()
        return str(content or "").strip()
