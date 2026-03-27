from __future__ import annotations

import os
import json
import hashlib
import math
import ssl
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "true")

try:
    from langchain.chat_models import init_chat_model
    from langchain_core.messages import AIMessage, BaseMessage, ChatMessage, HumanMessage, SystemMessage, ToolMessage
    from langchain_core.language_models.chat_models import BaseChatModel
    from langchain_litellm import ChatLiteLLM
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    init_chat_model = None
    AIMessage = None
    BaseChatModel = None
    BaseMessage = None
    ChatLiteLLM = None
    ChatMessage = None
    HumanMessage = None
    SystemMessage = None
    ToolMessage = None

from aiteams.catalog import preset_for
from aiteams.utils import trim_text


TOKEN_RE = __import__("re").compile(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]")


@dataclass(slots=True)
class ChatResult:
    content: str
    model: str
    usage: dict[str, Any]
    raw: dict[str, Any]
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    finish_reason: str | None = None


@dataclass(slots=True)
class EmbeddingResult:
    model: str
    vectors: list[list[float]]
    raw: dict[str, Any]


@dataclass(slots=True)
class RerankResult:
    model: str
    items: list[dict[str, Any]]
    raw: dict[str, Any]


@dataclass(slots=True)
class GatewayCapabilityRequest:
    tools: list[dict[str, Any]] = field(default_factory=list)
    tool_choice: str | dict[str, Any] | None = None
    parallel_tool_calls: bool | None = None
    response_format: dict[str, Any] | None = None

    @classmethod
    def json_object(
        cls,
        *,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
    ) -> "GatewayCapabilityRequest":
        return cls(
            tools=[dict(item) for item in list(tools or []) if isinstance(item, dict)],
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            response_format={"type": "json_object"},
        )


class ProviderRequestError(RuntimeError):
    pass


class AIGateway:
    def __init__(self, *, timeout_seconds: float = 60.0):
        self._timeout_seconds = timeout_seconds

    def close(self) -> None:
        return None

    def build_chat_model(
        self,
        provider: dict[str, Any],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = None,
        agent_name: str | None = None,
    ) -> Any:
        provider_type = str(provider.get("provider_type") or "").strip()
        resolved_model = str(model or provider.get("model") or "").strip() or "mock-model"
        if provider_type == "mock":
            from aiteams.deepagents.mock_model import MockDeepAgentChatModel

            return MockDeepAgentChatModel(agent_name=agent_name or str(provider.get("name") or "Mock Agent"))
        preset = preset_for(provider_type)
        if init_chat_model is not None:
            return self._build_init_chat_model(
                provider=provider,
                preset=preset,
                model=self._resolve_litellm_model(provider, resolved_model, preset),
                temperature=temperature,
                max_tokens=max_tokens,
            )
        return self._build_langchain_runnable(
            provider=provider,
            preset=preset,
            model=self._resolve_litellm_model(provider, resolved_model, preset),
            temperature=temperature,
            max_tokens=max_tokens,
            tools=None,
            tool_choice=None,
            parallel_tool_calls=None,
            response_format=None,
        )

    def chat(
        self,
        provider: dict[str, Any],
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = None,
    ) -> ChatResult:
        return self.complete(
            provider,
            messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    def complete(
        self,
        provider: dict[str, Any],
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = None,
        capability_request: GatewayCapabilityRequest | None = None,
    ) -> ChatResult:
        capability_request = capability_request or GatewayCapabilityRequest()
        provider_type = str(provider["provider_type"])
        resolved_model = str(model or provider.get("model") or "").strip()
        if not resolved_model:
            raise ProviderRequestError("Model is required.")

        if provider_type == "mock":
            return self._chat_mock(provider, messages, resolved_model)
        return self._chat_litellm(
            provider,
            messages,
            resolved_model,
            temperature,
            max_tokens,
            tools=capability_request.tools or None,
            tool_choice=capability_request.tool_choice,
            parallel_tool_calls=capability_request.parallel_tool_calls,
            response_format=capability_request.response_format,
        )

    def chat_with_tools(
        self,
        provider: dict[str, Any],
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = "auto",
        parallel_tool_calls: bool | None = None,
    ) -> ChatResult:
        return self.complete(
            provider,
            messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            capability_request=GatewayCapabilityRequest(
                tools=[dict(item) for item in list(tools or []) if isinstance(item, dict)],
                tool_choice=tool_choice,
                parallel_tool_calls=parallel_tool_calls,
            ),
        )

    def embed(
        self,
        provider: dict[str, Any],
        texts: list[str],
        *,
        model: str | None = None,
        input_type: str = "search_document",
    ) -> EmbeddingResult:
        provider_type = str(provider.get("provider_type") or "").strip()
        resolved_model = str(model or provider.get("model") or "").strip()
        if not resolved_model:
            raise ProviderRequestError("Embedding model is required.")
        if provider_type == "mock":
            return EmbeddingResult(
                model=resolved_model,
                vectors=[self._mock_embedding(text) for text in texts],
                raw={"mock": True},
            )
        try:
            raw = self._embedding_request(provider, texts=texts, model=resolved_model, input_type=input_type)
        except ValueError as exc:
            raise ProviderRequestError(str(exc)) from exc
        vectors = self._extract_embedding_vectors(provider_type, raw)
        if len(vectors) != len(texts):
            raise ProviderRequestError("Provider returned an unexpected embedding vector count.")
        return EmbeddingResult(model=resolved_model, vectors=vectors, raw=raw)

    def rerank(
        self,
        provider: dict[str, Any],
        *,
        query: str,
        documents: list[str],
        model: str | None = None,
        top_n: int | None = None,
    ) -> RerankResult:
        provider_type = str(provider.get("provider_type") or "").strip()
        resolved_model = str(model or provider.get("model") or "").strip()
        if not resolved_model:
            raise ProviderRequestError("Rerank model is required.")
        if provider_type == "mock":
            return RerankResult(
                model=resolved_model,
                items=self._mock_rerank(query=query, documents=documents, top_n=top_n),
                raw={"mock": True},
            )
        try:
            raw = self._rerank_request(provider, query=query, documents=documents, model=resolved_model, top_n=top_n)
        except ValueError as exc:
            raise ProviderRequestError(str(exc)) from exc
        return RerankResult(model=resolved_model, items=self._extract_rerank_items(raw), raw=raw)

    def _chat_mock(self, provider: dict[str, Any], messages: list[dict[str, str]], model: str) -> ChatResult:
        system = next((item["content"] for item in messages if item["role"] == "system"), "")
        user = next((item["content"] for item in reversed(messages) if item["role"] == "user"), "")
        provider_name = str(provider.get("name") or "mock")
        content = f"[{provider_name}/{model}] {trim_text(system, limit=70)} | {trim_text(user, limit=180)}"
        return ChatResult(
            content=content,
            model=model,
            usage={"prompt_tokens": 0, "completion_tokens": 0},
            raw={"mock": True},
            finish_reason="stop",
        )

    def _mock_embedding(self, text: str, *, dimension: int = 96) -> list[float]:
        vector = [0.0] * dimension
        tokens = TOKEN_RE.findall(text.lower())
        if not tokens:
            return vector
        for token in tokens:
            digest = hashlib.sha1(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], "big") % dimension
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            vector[index] += sign * (1.0 + (digest[5] / 255.0))
        norm = math.sqrt(sum(value * value for value in vector))
        if norm <= 0:
            return vector
        return [value / norm for value in vector]

    def _mock_rerank(self, *, query: str, documents: list[str], top_n: int | None) -> list[dict[str, Any]]:
        terms = {term for term in TOKEN_RE.findall(query.lower()) if term}
        ranked: list[dict[str, Any]] = []
        for index, document in enumerate(documents):
            doc_terms = {term for term in TOKEN_RE.findall(str(document).lower()) if term}
            overlap = len(terms.intersection(doc_terms))
            score = float(overlap) / float(max(len(terms), 1))
            ranked.append({"index": index, "relevance_score": score, "document": document})
        ranked.sort(key=lambda item: (float(item.get("relevance_score") or 0.0), -int(item.get("index", 0))), reverse=True)
        if top_n is not None and top_n > 0:
            ranked = ranked[:top_n]
        return ranked

    def _chat_litellm(
        self,
        provider: dict[str, Any],
        messages: list[dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int | None,
        *,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = "auto",
        parallel_tool_calls: bool | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> ChatResult:
        if ChatLiteLLM is None:
            raise ProviderRequestError("langchain-litellm is not installed. Install it or switch the agent backend to mock.")
        preset = preset_for(str(provider["provider_type"]))
        litellm_model = self._resolve_litellm_model(provider, model, preset)
        normalized_tools = [dict(item) for item in tools if isinstance(item, dict)] if tools else None
        runnable = self._build_langchain_runnable(
            provider=provider,
            preset=preset,
            model=litellm_model,
            temperature=temperature,
            max_tokens=max_tokens,
            tools=normalized_tools,
            tool_choice=tool_choice if normalized_tools else None,
            parallel_tool_calls=parallel_tool_calls if normalized_tools else None,
            response_format=dict(response_format) if response_format else None,
        )
        langchain_messages = self._coerce_langchain_messages(messages)

        try:
            response = self._invoke_langchain_runnable(runnable, langchain_messages)
        except Exception as exc:
            detail = trim_text(str(exc), limit=500) or exc.__class__.__name__
            raise ProviderRequestError(f"Provider request failed: {detail}") from exc

        raw = self._response_raw(response)
        content = self._extract_content(response)
        usage = self._extract_usage(response, raw)
        tool_calls = self._extract_tool_calls(response, raw)
        finish_reason = self._extract_finish_reason(response, raw)
        response_model = str(
            self._get_value(self._get_value(response, "response_metadata"), "model_name")
            or raw.get("model")
            or raw.get("model_name")
            or litellm_model
        )
        return ChatResult(
            content=content,
            model=response_model,
            usage=usage,
            raw=raw,
            tool_calls=tool_calls,
            finish_reason=finish_reason,
        )

    def _build_langchain_runnable(
        self,
        *,
        provider: dict[str, Any],
        preset: dict[str, Any],
        model: str,
        temperature: float,
        max_tokens: int | None,
        tools: list[dict[str, Any]] | None,
        tool_choice: str | dict[str, Any] | None,
        parallel_tool_calls: bool | None,
        response_format: dict[str, Any] | None,
    ) -> Any:
        if ChatLiteLLM is None:
            raise ProviderRequestError("langchain-litellm is not installed. Install it or switch the agent backend to mock.")
        provider_alias = self._resolve_provider_alias(provider, preset)
        extra_config = self._sanitize_model_kwargs(provider.get("extra_config"))
        if provider.get("api_version"):
            extra_config.setdefault("api_version", str(provider["api_version"]))
        api_key = self._resolve_api_key(provider)
        chat_kwargs: dict[str, Any] = {
            "model": model,
            "temperature": temperature,
            "request_timeout": self._timeout_seconds,
            "max_tokens": max_tokens,
            "model_kwargs": extra_config,
        }
        base_url = self._resolve_base_url(provider, preset)
        if base_url:
            chat_kwargs["api_base"] = base_url
        if api_key:
            chat_kwargs["api_key"] = api_key
        if provider.get("organization"):
            chat_kwargs["organization"] = str(provider["organization"])
        extra_headers = provider.get("extra_headers")
        if isinstance(extra_headers, dict) and extra_headers:
            chat_kwargs["extra_headers"] = {str(key): str(value) for key, value in extra_headers.items()}
        if provider_alias and provider_alias != "mock" and not model.startswith(f"{provider_alias}/"):
            chat_kwargs["custom_llm_provider"] = provider_alias

        runnable: Any = ChatLiteLLM(**chat_kwargs)
        if tools:
            bind_kwargs: dict[str, Any] = {"tool_choice": tool_choice or "auto"}
            if parallel_tool_calls is not None:
                bind_kwargs["parallel_tool_calls"] = parallel_tool_calls
            runnable = runnable.bind_tools([dict(item) for item in tools], **bind_kwargs)
        if response_format:
            runnable = runnable.bind(response_format=dict(response_format))
        return runnable

    def _build_init_chat_model(
        self,
        *,
        provider: dict[str, Any],
        preset: dict[str, Any],
        model: str,
        temperature: float,
        max_tokens: int | None,
    ) -> Any:
        if init_chat_model is None:
            raise ProviderRequestError("langchain init_chat_model is unavailable.")
        provider_alias = self._resolve_provider_alias(provider, preset)
        extra_config = self._sanitize_model_kwargs(provider.get("extra_config"))
        if provider.get("api_version"):
            extra_config.setdefault("api_version", str(provider["api_version"]))
        api_key = self._resolve_api_key(provider)
        chat_kwargs: dict[str, Any] = {
            "temperature": temperature,
            "request_timeout": self._timeout_seconds,
        }
        if max_tokens is not None:
            chat_kwargs["max_tokens"] = max_tokens
        if extra_config:
            chat_kwargs["model_kwargs"] = extra_config
        base_url = self._resolve_base_url(provider, preset)
        if base_url:
            chat_kwargs["api_base"] = base_url
        if api_key:
            chat_kwargs["api_key"] = api_key
        if provider.get("organization"):
            chat_kwargs["organization"] = str(provider["organization"])
        extra_headers = provider.get("extra_headers")
        if isinstance(extra_headers, dict) and extra_headers:
            chat_kwargs["extra_headers"] = {str(key): str(value) for key, value in extra_headers.items()}
        if provider_alias and provider_alias != "mock" and not model.startswith(f"{provider_alias}/"):
            chat_kwargs["custom_llm_provider"] = provider_alias
        return init_chat_model(model, model_provider="litellm", **chat_kwargs)

    def _embedding_request(
        self,
        provider: dict[str, Any],
        *,
        texts: list[str],
        model: str,
        input_type: str,
    ) -> dict[str, Any]:
        provider_type = str(provider.get("provider_type") or "").strip()
        if provider_type in {"openai", "custom_openai", "deepseek", "openrouter", "ollama"}:
            return self._request_json(
                "POST",
                self._append_path(self._require_base_url(provider), "/embeddings"),
                headers=self._auth_headers(provider, bearer=True),
                payload={"model": model, "input": texts},
                skip_tls_verify=bool(provider.get("skip_tls_verify")),
            )
        if provider_type == "azure_openai":
            return self._request_json(
                "POST",
                self._azure_embeddings_url(provider, model),
                headers=self._auth_headers(provider, azure=True),
                payload={"input": texts},
                skip_tls_verify=bool(provider.get("skip_tls_verify")),
            )
        if provider_type == "gemini":
            if len(texts) != 1:
                raise ValueError("Gemini embedding currently supports one text per request.")
            return self._request_json(
                "POST",
                self._gemini_url(provider, f"/models/{model}:embedContent"),
                payload={"content": {"parts": [{"text": texts[0]}]}},
                skip_tls_verify=bool(provider.get("skip_tls_verify")),
            )
        if provider_type == "cohere":
            return self._request_json(
                "POST",
                self._append_path(self._cohere_base(provider), "/embed"),
                headers=self._auth_headers(provider, bearer=True),
                payload={"model": model, "texts": texts, "input_type": input_type},
                skip_tls_verify=bool(provider.get("skip_tls_verify")),
            )
        raise ValueError(f"Provider type `{provider_type}` does not support embedding requests.")

    def _rerank_request(
        self,
        provider: dict[str, Any],
        *,
        query: str,
        documents: list[str],
        model: str,
        top_n: int | None,
    ) -> dict[str, Any]:
        provider_type = str(provider.get("provider_type") or "").strip()
        limit = top_n if top_n is not None and top_n > 0 else len(documents)
        if provider_type == "cohere":
            return self._request_json(
                "POST",
                self._append_path(self._cohere_base(provider), "/rerank"),
                headers=self._auth_headers(provider, bearer=True),
                payload={"model": model, "query": query, "documents": documents, "top_n": limit},
                skip_tls_verify=bool(provider.get("skip_tls_verify")),
            )
        if provider_type == "custom_openai":
            return self._request_json(
                "POST",
                self._append_path(self._require_base_url(provider), "/rerank"),
                headers=self._auth_headers(provider, bearer=True),
                payload={"model": model, "query": query, "documents": documents, "top_n": limit},
                skip_tls_verify=bool(provider.get("skip_tls_verify")),
            )
        raise ValueError(f"Provider type `{provider_type}` does not support rerank requests.")

    def _extract_embedding_vectors(self, provider_type: str, payload: dict[str, Any]) -> list[list[float]]:
        if provider_type == "gemini":
            vector = ((payload.get("embedding") or {}).get("values")) or []
            return [[float(value) for value in vector]]
        if provider_type == "cohere":
            embeddings = payload.get("embeddings") or {}
            vectors = embeddings.get("float") or embeddings.get("int8") or embeddings.get("uint8") or []
            return [[float(value) for value in list(vector or [])] for vector in list(vectors or [])]
        data = payload.get("data") or []
        return [[float(value) for value in list((item or {}).get("embedding") or [])] for item in list(data or [])]

    def _extract_rerank_items(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for raw in list(payload.get("results") or []):
            if not isinstance(raw, dict):
                continue
            items.append(
                {
                    "index": int(raw.get("index", 0) or 0),
                    "relevance_score": float(raw.get("relevance_score", 0.0) or 0.0),
                    "document": raw.get("document"),
                }
            )
        items.sort(key=lambda item: (float(item.get("relevance_score") or 0.0), -int(item.get("index", 0))), reverse=True)
        return items

    def _invoke_langchain_runnable(self, runnable: Any, messages: list[Any]) -> Any:
        return runnable.invoke(messages)

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

    def _require_base_url(self, provider: dict[str, Any]) -> str:
        base_url = str(provider.get("base_url") or "").strip()
        if not base_url:
            raise ValueError("Provider Base URL is required.")
        return base_url

    def _resolve_api_key(self, provider: dict[str, Any]) -> str:
        direct = str(provider.get("api_key") or "").strip()
        if direct:
            return direct
        env_name = str(provider.get("api_key_env") or "").strip()
        if env_name:
            return str(os.getenv(env_name) or "").strip()
        return ""

    def _resolve_provider_alias(self, provider: dict[str, Any], preset: dict[str, Any]) -> str:
        extra_config = dict(provider.get("extra_config") or {})
        alias = str(extra_config.get("custom_llm_provider") or preset.get("litellm_provider") or provider.get("provider_type") or "").strip()
        return alias

    def _sanitize_model_kwargs(self, extra_config: Any) -> dict[str, Any]:
        if not isinstance(extra_config, dict):
            return {}
        blocked = {"gateway_capabilities", "custom_llm_provider"}
        return {str(key): value for key, value in extra_config.items() if str(key) not in blocked}

    def _auth_headers(
        self,
        provider: dict[str, Any],
        *,
        bearer: bool = False,
        azure: bool = False,
        anthropic: bool = False,
    ) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        api_key = self._resolve_api_key(provider)
        if bearer and api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        if azure and api_key:
            headers["api-key"] = api_key
        if anthropic and api_key:
            headers["x-api-key"] = api_key
            headers["anthropic-version"] = "2023-06-01"
        organization = str(provider.get("organization") or "").strip()
        if organization:
            headers["OpenAI-Organization"] = organization
        return headers

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        payload: dict[str, Any] | None = None,
        skip_tls_verify: bool = False,
    ) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(url, data=data, headers=headers or {}, method=method)
        try:
            context = ssl._create_unverified_context() if skip_tls_verify else None
            with urlopen(request, timeout=self._timeout_seconds, context=context) as response:
                body = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
            raise ValueError(trim_text(detail or str(exc), limit=320) or "Provider request failed.") from exc
        except URLError as exc:
            detail = trim_text(str(exc.reason), limit=320) or "Provider request failed."
            if "certificate verify failed" in detail.lower() or "certificate_verify_failed" in detail.lower():
                raise ValueError(f"TLS 证书校验失败。若为自签名证书，请启用“跳过 TLS 证书校验”后重试。原始错误：{detail}") from exc
            raise ValueError(detail) from exc
        try:
            decoded = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            raise ValueError("Provider returned invalid JSON.") from exc
        if isinstance(decoded, dict):
            return decoded
        raise ValueError("Provider returned a non-object JSON payload.")

    def _append_path(self, base_url: str, path: str, query: dict[str, Any] | None = None) -> str:
        parsed = urlsplit(base_url.rstrip("/"))
        base_path = parsed.path.rstrip("/")
        extra_path = path if path.startswith("/") else f"/{path}"
        merged_query: dict[str, Any] = {}
        if parsed.query:
            for chunk in parsed.query.split("&"):
                if "=" in chunk:
                    key, value = chunk.split("=", 1)
                    merged_query[key] = value
        if query:
            merged_query.update({key: value for key, value in query.items() if value not in (None, "")})
        return urlunsplit((parsed.scheme, parsed.netloc, f"{base_path}{extra_path}", urlencode(merged_query), ""))

    def _azure_embeddings_url(self, provider: dict[str, Any], model_name: str) -> str:
        base_url = self._require_base_url(provider).rstrip("/")
        api_version = str(provider.get("api_version") or "2024-10-21").strip()
        if "/openai/v1" in base_url:
            return self._append_path(base_url, "/embeddings", {"api-version": "preview"})
        return self._append_path(base_url, f"/openai/deployments/{model_name}/embeddings", {"api-version": api_version})

    def _gemini_url(self, provider: dict[str, Any], path: str) -> str:
        api_key = self._resolve_api_key(provider)
        if not api_key:
            raise ValueError("Gemini API Key is required.")
        return self._append_path(self._require_base_url(provider).rstrip("/"), path, {"key": api_key})

    def _cohere_base(self, provider: dict[str, Any]) -> str:
        base_url = str(provider.get("base_url") or "https://api.cohere.com/v2").rstrip("/")
        return base_url[:-3] if base_url.endswith("/v2") else base_url

    def _coerce_langchain_messages(self, messages: list[dict[str, str]]) -> list[Any]:
        coerced: list[Any] = []
        for item in messages:
            role = str(item.get("role") or "").strip().lower()
            content = self._coerce_content(item.get("content"))
            if role == "system":
                coerced.append(SystemMessage(content=content))
            elif role == "user":
                coerced.append(HumanMessage(content=content))
            elif role == "assistant":
                coerced.append(AIMessage(content=content))
            elif role == "tool":
                coerced.append(
                    ToolMessage(
                        content=content,
                        tool_call_id=str(item.get("tool_call_id") or item.get("id") or "tool_call"),
                    )
                )
            else:
                coerced.append(ChatMessage(role=role or "user", content=content))
        return coerced

    def _extract_content(self, response: Any) -> str:
        if AIMessage is not None and isinstance(response, AIMessage):
            return self._coerce_content(response.content)
        choices = self._get_value(response, "choices")
        if isinstance(choices, list) and choices:
            message = self._get_value(choices[0], "message")
            if message is not None:
                return self._coerce_content(self._get_value(message, "content"))
        return ""

    def _extract_usage(self, response: Any, raw: dict[str, Any]) -> dict[str, Any]:
        usage_metadata = self._get_value(response, "usage_metadata")
        if isinstance(usage_metadata, dict) and usage_metadata:
            usage: dict[str, Any] = {}
            if usage_metadata.get("input_tokens") is not None:
                usage["prompt_tokens"] = usage_metadata.get("input_tokens")
            if usage_metadata.get("output_tokens") is not None:
                usage["completion_tokens"] = usage_metadata.get("output_tokens")
            if usage_metadata.get("total_tokens") is not None:
                usage["total_tokens"] = usage_metadata.get("total_tokens")
            usage.update({key: value for key, value in usage_metadata.items() if key not in {"input_tokens", "output_tokens"}})
            return usage
        response_metadata = self._get_value(response, "response_metadata")
        token_usage = self._get_value(response_metadata, "token_usage")
        if isinstance(token_usage, dict) and token_usage:
            return dict(token_usage)
        usage = self._get_value(response, "usage")
        if usage is not None:
            usage_dict = self._to_dict(usage)
            if usage_dict:
                return usage_dict
        raw_usage = raw.get("usage")
        if isinstance(raw_usage, dict):
            return raw_usage
        return {}

    def _extract_tool_calls(self, response: Any, raw: dict[str, Any]) -> list[dict[str, Any]]:
        direct_tool_calls = self._get_value(response, "tool_calls")
        normalized_direct = self._normalize_tool_calls(direct_tool_calls)
        if normalized_direct:
            return normalized_direct
        choices = self._get_value(response, "choices")
        if isinstance(choices, list) and choices:
            message = self._get_value(choices[0], "message")
            tool_calls = self._get_value(message, "tool_calls") if message is not None else None
            normalized = self._normalize_tool_calls(tool_calls)
            if normalized:
                return normalized
        raw_choices = raw.get("choices")
        if isinstance(raw_choices, list) and raw_choices:
            message = dict(raw_choices[0].get("message") or {})
            normalized = self._normalize_tool_calls(message.get("tool_calls"))
            if normalized:
                return normalized
        return []

    def _extract_finish_reason(self, response: Any, raw: dict[str, Any]) -> str | None:
        response_metadata = self._get_value(response, "response_metadata")
        finish_reason = self._get_value(response_metadata, "finish_reason")
        if finish_reason is not None:
            return str(finish_reason)
        choices = self._get_value(response, "choices")
        if isinstance(choices, list) and choices:
            finish_reason = self._get_value(choices[0], "finish_reason")
            if finish_reason is not None:
                return str(finish_reason)
        raw_choices = raw.get("choices")
        if isinstance(raw_choices, list) and raw_choices:
            finish_reason = raw_choices[0].get("finish_reason")
            if finish_reason is not None:
                return str(finish_reason)
        return None

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

    def _normalize_tool_calls(self, tool_calls: Any) -> list[dict[str, Any]]:
        if not isinstance(tool_calls, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in tool_calls:
            tool_call = self._normalize_tool_call(item)
            if tool_call is not None:
                normalized.append(tool_call)
        return normalized

    def _normalize_tool_call(self, tool_call: Any) -> dict[str, Any] | None:
        function = self._get_value(tool_call, "function")
        function_name = str(
            self._get_value(function, "name")
            or self._get_value(tool_call, "name")
            or ""
        ).strip()
        if not function_name:
            return None
        arguments_raw = self._get_value(function, "arguments")
        if arguments_raw is None:
            arguments_raw = self._get_value(tool_call, "arguments")
        if arguments_raw is None:
            arguments_raw = self._get_value(tool_call, "args")
        raw = self._to_dict(tool_call)
        return {
            "id": str(self._get_value(tool_call, "id") or raw.get("id") or ""),
            "type": str(self._get_value(tool_call, "type") or raw.get("type") or "function"),
            "name": function_name,
            "arguments": self._coerce_tool_arguments(arguments_raw),
            "raw_arguments": arguments_raw,
            "raw": raw,
        }

    def _coerce_tool_arguments(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict) and "args" in value and len(value) == 1:
            return self._coerce_tool_arguments(value.get("args"))
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError, json.JSONDecodeError):
                return {"value": text}
            return self._coerce_tool_arguments(parsed)
        if isinstance(value, list):
            return {"items": list(value)}
        dumped = self._to_dict(value)
        if dumped:
            return dumped
        if value is None:
            return {}
        return {"value": value}

    def _response_raw(self, response: Any) -> dict[str, Any]:
        raw = self._to_dict(response)
        if raw:
            return raw
        return {
            "content": self._extract_content(response),
            "tool_calls": self._extract_tool_calls(response, {}),
            "response_metadata": self._to_dict(self._get_value(response, "response_metadata")),
            "usage_metadata": self._to_dict(self._get_value(response, "usage_metadata")),
        }
