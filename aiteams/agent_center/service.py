from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from aiteams.agent_center.defaults import (
    default_agent_templates,
    default_plugins,
    default_provider_profiles,
    default_team_templates,
)
from aiteams.agent_center.resolver import BuildResolver
from aiteams.agent_center.team_graph import normalize_team_template_spec, validate_team_template_spec
from aiteams.ai_gateway import AIGateway, ProviderRequestError
from aiteams.catalog import list_provider_presets, preset_for
from aiteams.plugins import PluginManager
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import pretty_json, slugify, trim_text

MODEL_TYPES = {"chat", "embedding", "rerank"}


class AgentCenterService:
    def __init__(self, store: MetadataStore, plugin_manager: PluginManager | None = None, gateway: AIGateway | None = None):
        self.store = store
        self.resolver = BuildResolver(store)
        self.plugin_manager = plugin_manager
        self.gateway = gateway or AIGateway()

    def ensure_defaults(self) -> None:
        for provider in default_provider_profiles():
            if self.store.get_provider_profile(str(provider["id"])) is None:
                normalized = self.prepare_provider_profile(provider)
                self.store.save_provider_profile(
                    provider_profile_id=str(provider["id"]),
                    key=str(normalized["key"]),
                    name=str(normalized["name"]),
                    provider_type=str(normalized["provider_type"]),
                    description=str(normalized["description"]),
                    config=dict(normalized["config"]),
                    secret=dict(normalized["secret"]),
                )
        for plugin in default_plugins():
            if self.store.get_plugin(str(plugin["id"])) is None:
                self.store.save_plugin(
                    plugin_id=str(plugin["id"]),
                    key=str(plugin["key"]),
                    name=str(plugin["name"]),
                    version=str(plugin["version"]),
                    plugin_type=str(plugin["plugin_type"]),
                    description=str(plugin.get("description") or ""),
                    manifest=dict(plugin.get("manifest") or {}),
                    install_path=None,
                )
        for template in default_agent_templates():
            if self.store.get_agent_template(str(template["id"])) is None:
                self.store.save_agent_template(
                    agent_template_id=str(template["id"]),
                    key=str(template["key"]),
                    name=str(template["name"]),
                    role=str(template["role"]),
                    description=str(template.get("description") or ""),
                    version="v1",
                    spec=dict(template.get("spec") or {}),
                )
        for team in default_team_templates():
            if self.store.get_team_template(str(team["id"])) is None:
                self.store.save_team_template(
                    team_template_id=str(team["id"]),
                    key=str(team["key"]),
                    name=str(team["name"]),
                    description=str(team.get("description") or ""),
                    version="v1",
                    spec=dict(team.get("spec") or {}),
                )

    def provider_types(self) -> list[dict[str, Any]]:
        return list_provider_presets()

    def list_provider_profiles(
        self,
        *,
        query: str | None = None,
        provider_type: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        items = [self.normalize_provider_profile(item) for item in self.store.list_provider_profiles()]
        keyword = str(query or "").strip().lower()
        if keyword:
            items = [
                item
                for item in items
                if keyword in str(item.get("name") or "").lower()
                or keyword in str(item.get("key") or "").lower()
                or keyword in str(item.get("provider_type") or "").lower()
                or keyword in str(item.get("description") or "").lower()
            ]
        selected_type = str(provider_type or "").strip()
        if selected_type:
            items = [item for item in items if str(item.get("provider_type") or "") == selected_type]
        total = len(items)
        safe_offset = max(0, int(offset or 0))
        if limit is not None:
            safe_limit = max(1, int(limit))
            paged = items[safe_offset : safe_offset + safe_limit]
        else:
            safe_limit = total or 0
            paged = items
        return {
            "items": paged,
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
        }

    def prepare_provider_profile(self, payload: dict[str, Any], *, existing: dict[str, Any] | None = None) -> dict[str, Any]:
        existing_item = self.normalize_provider_profile(existing) if existing else None
        name = str(payload.get("name") or (existing_item or {}).get("name") or "").strip()
        if not name:
            raise ValueError("Provider 名称不能为空。")
        provider_type = str(payload.get("provider_type") or (existing_item or {}).get("provider_type") or "").strip()
        if not provider_type:
            raise ValueError("API 模式不能为空。")
        preset = preset_for(provider_type)
        existing_config = dict((existing_item or {}).get("config_json") or {})
        payload_config = dict(payload.get("config") or {})
        merged_config = dict(existing_config)
        merged_config.update(payload_config)
        models = self._normalize_models(
            payload.get("models", merged_config.get("models")),
            provider_type=provider_type,
            fallback_model=str(payload.get("model") or merged_config.get("model") or preset.get("default_model") or "").strip(),
        )
        if not models and provider_type == "mock":
            models = [{"name": str(preset.get("default_model") or "mock-model"), "model_type": "chat"}]
        default_chat = self._find_default_model(models, "chat")
        default_model_name = default_chat["name"] if default_chat else (models[0]["name"] if models else str(preset.get("default_model") or ""))

        base_url = str(payload.get("base_url") or merged_config.get("base_url") or "").strip()
        if not base_url and preset.get("use_default_base_url_when_blank"):
            base_url = str(preset.get("default_base_url") or "").strip()
        api_version = str(payload.get("api_version") or merged_config.get("api_version") or preset.get("default_api_version") or "").strip()
        organization = str(payload.get("organization") or merged_config.get("organization") or "").strip()
        api_key_env = str(payload.get("api_key_env") or merged_config.get("api_key_env") or "").strip()

        normalized_config = dict(merged_config)
        normalized_config["backend"] = provider_type
        normalized_config["models"] = models
        normalized_config["model"] = default_model_name
        normalized_config["temperature"] = float(merged_config.get("temperature", 0.2))
        if base_url:
            normalized_config["base_url"] = base_url
        else:
            normalized_config.pop("base_url", None)
        if api_version:
            normalized_config["api_version"] = api_version
        else:
            normalized_config.pop("api_version", None)
        if organization:
            normalized_config["organization"] = organization
        else:
            normalized_config.pop("organization", None)
        if api_key_env:
            normalized_config["api_key_env"] = api_key_env
        else:
            normalized_config.pop("api_key_env", None)

        secret_payload = payload.get("secret")
        if secret_payload is None and payload.get("api_key"):
            secret_payload = {"api_key": payload.get("api_key")}
        secret = dict(secret_payload or {})
        if "api_key" in secret:
            secret["api_key"] = str(secret.get("api_key") or "").strip()
            if not secret["api_key"]:
                secret.pop("api_key", None)

        return {
            "key": slugify(str(payload.get("key") or (existing_item or {}).get("key") or name), fallback="provider"),
            "name": name,
            "provider_type": provider_type,
            "description": str(payload.get("description") or (existing_item or {}).get("description") or "").strip(),
            "config": normalized_config,
            "secret": secret,
        }

    def normalize_provider_profile(self, provider: dict[str, Any] | None) -> dict[str, Any] | None:
        if provider is None:
            return None
        item = dict(provider)
        provider_type = str(item.get("provider_type") or "mock")
        preset = preset_for(provider_type)
        config = dict(item.get("config_json") or {})
        models = self._normalize_models(config.get("models"), provider_type=provider_type, fallback_model=str(config.get("model") or preset.get("default_model") or "").strip())
        default_chat = self._find_default_model(models, "chat")
        default_model_name = default_chat["name"] if default_chat else (models[0]["name"] if models else str(preset.get("default_model") or ""))
        config["backend"] = str(config.get("backend") or provider_type)
        config["models"] = models
        config["model"] = str(config.get("model") or default_model_name)
        if not config.get("base_url") and preset.get("use_default_base_url_when_blank"):
            config["base_url"] = str(preset.get("default_base_url") or "")
        if preset.get("default_api_version") and not config.get("api_version"):
            config["api_version"] = str(preset.get("default_api_version") or "")
        item["config_json"] = config
        item["api_mode"] = provider_type
        item["model_count"] = len(models)
        item["default_model_name"] = default_model_name
        item["default_chat_model_name"] = default_chat["name"] if default_chat else default_model_name
        item["supported_model_types"] = list(preset.get("supported_model_types") or [])
        item["preset"] = preset
        return item

    def discover_provider_models(self, payload: dict[str, Any]) -> dict[str, Any]:
        runtime_provider = self._runtime_provider(payload)
        preset = preset_for(str(runtime_provider["provider_type"]))
        discovery_mode = str(preset.get("discovery_mode") or "local")
        if discovery_mode == "local":
            models = runtime_provider.get("models") or [{"name": str(preset.get("default_model") or "mock-model"), "model_type": "chat"}]
            return {"items": models, "source": "local"}
        if not preset.get("supports_model_discovery"):
            raise ValueError("当前 API 模式不支持自动获取模型列表。")
        items = self._discover_remote_models(runtime_provider, discovery_mode)
        return {"items": items, "source": "remote", "provider_type": runtime_provider["provider_type"]}

    def test_provider_model(self, payload: dict[str, Any]) -> dict[str, Any]:
        runtime_provider = self._runtime_provider(dict(payload.get("provider") or payload))
        model_payload = dict(payload.get("model") or {})
        model_name = str(model_payload.get("name") or model_payload.get("model") or "").strip()
        if not model_name:
            raise ValueError("模型名称不能为空。")
        model_type = str(model_payload.get("model_type") or "chat").strip() or "chat"
        if model_type not in MODEL_TYPES:
            raise ValueError("模型类型不支持。")
        if runtime_provider["provider_type"] == "mock":
            return self._mock_model_test(model_name, model_type)
        if model_type == "chat":
            return self._chat_model_test(runtime_provider, model_name)
        if model_type == "embedding":
            return self._embedding_model_test(runtime_provider, model_name)
        return self._rerank_model_test(runtime_provider, model_name)

    def normalize_team_spec(self, spec: dict[str, Any] | None) -> dict[str, Any]:
        return normalize_team_template_spec(spec)

    def validate_team_spec(self, spec: dict[str, Any] | None) -> dict[str, Any]:
        return validate_team_template_spec(spec)

    def team_graph_payload(self, team_template_id: str) -> dict[str, Any]:
        team_template = self.store.get_team_template(team_template_id)
        if team_template is None:
            raise ValueError("Team template does not exist.")
        validation = self.validate_team_spec(team_template.get("spec_json") or {})
        return {
            "team_template": team_template,
            "spec": validation["normalized_spec"],
            "validation": validation,
        }

    def preview_team_spec(self, spec: dict[str, Any], *, team_template_id: str | None = None, name: str | None = None) -> dict[str, Any]:
        normalized = self.normalize_team_spec(spec)
        validation = self.validate_team_spec(normalized)
        if validation["errors"]:
            return {
                "valid": False,
                "errors": validation["errors"],
                "warnings": validation["warnings"],
                "summary": validation["summary"],
            }
        pseudo_template = {
            "id": team_template_id or "preview_team_template",
            "key": "preview_team_template",
            "name": name or "Preview Team Template",
            "version": "draft",
            "description": "",
            "spec_json": validation["normalized_spec"],
        }
        blueprint_spec, resource_lock = self.resolver.build(pseudo_template)
        return {
            "valid": True,
            "errors": [],
            "warnings": validation["warnings"],
            "summary": validation["summary"],
            "preview": {
                "role_template_count": len(blueprint_spec.get("role_templates", {})),
                "agent_count": len(blueprint_spec.get("agents", {})),
                "node_count": len((blueprint_spec.get("flow") or {}).get("nodes", [])),
                "edge_count": len((blueprint_spec.get("flow") or {}).get("edges", [])),
                "communication_policy": str((blueprint_spec.get("metadata") or {}).get("communication_policy") or "graph-ancestor-scoped"),
            },
            "resource_lock": resource_lock,
        }

    def build_team_template(self, team_template_id: str, *, build_name: str | None = None) -> dict[str, Any]:
        team_template = self.store.get_team_template(team_template_id)
        if team_template is None:
            raise ValueError("Team template does not exist.")
        validation = self.validate_team_spec(team_template.get("spec_json") or {})
        if validation["errors"]:
            raise ValueError("; ".join(validation["errors"]))
        spec, resource_lock = self.resolver.build(team_template)
        build_title = build_name or f"{team_template['name']} Build"
        blueprint = self.store.save_blueprint(
            blueprint_id=None,
            workspace_id=str(spec["workspace_id"]),
            project_id=str(spec["project_id"]),
            name=build_title,
            description=trim_text(str(team_template.get("description") or ""), limit=280),
            version="build",
            raw_format="json",
            raw_text=pretty_json(spec),
            spec=spec,
            is_template=False,
        )
        build = self.store.save_blueprint_build(
            build_id=None,
            team_template_id=str(team_template["id"]),
            key=str(team_template["key"]),
            name=build_title,
            description=str(team_template.get("description") or ""),
            spec=spec,
            resource_lock=resource_lock,
            blueprint_id=str(blueprint["id"]),
        )
        return build

    def _runtime_provider(self, payload: dict[str, Any]) -> dict[str, Any]:
        provider_type = str(payload.get("provider_type") or "mock").strip()
        preset = preset_for(provider_type)
        config = dict(payload.get("config") or {})
        models = self._normalize_models(payload.get("models", config.get("models")), provider_type=provider_type, fallback_model=str(config.get("model") or preset.get("default_model") or "").strip())
        base_url = str(payload.get("base_url") or config.get("base_url") or "").strip()
        if not base_url and preset.get("use_default_base_url_when_blank"):
            base_url = str(preset.get("default_base_url") or "").strip()
        api_version = str(payload.get("api_version") or config.get("api_version") or preset.get("default_api_version") or "").strip()
        organization = str(payload.get("organization") or config.get("organization") or "").strip()
        secret = dict(payload.get("secret") or {})
        api_key = str(secret.get("api_key") or payload.get("api_key") or "").strip()
        if not api_key:
            api_key = str((payload.get("secret_json") or {}).get("api_key") or "").strip()
        return {
            "name": str(payload.get("name") or "Provider Test"),
            "provider_type": provider_type,
            "base_url": base_url,
            "api_version": api_version,
            "organization": organization,
            "api_key": api_key,
            "models": models,
            "model": str(config.get("model") or (self._find_default_model(models, "chat") or {}).get("name") or preset.get("default_model") or "").strip(),
        }

    def _normalize_models(self, raw_models: Any, *, provider_type: str, fallback_model: str) -> list[dict[str, Any]]:
        models: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for raw in raw_models if isinstance(raw_models, list) else []:
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("name") or raw.get("model") or "").strip()
            if not name:
                continue
            model_type = self._coerce_model_type(raw.get("model_type") or raw.get("type") or self._infer_model_type(name))
            context_window = self._coerce_context_window(raw.get("context_window"))
            identity = (name, model_type)
            if identity in seen:
                continue
            seen.add(identity)
            entry: dict[str, Any] = {"name": name, "model_type": model_type}
            if context_window is not None:
                entry["context_window"] = context_window
            models.append(entry)
        if not models and fallback_model:
            models.append({"name": fallback_model, "model_type": "chat"})
        supported = set(preset_for(provider_type).get("supported_model_types") or [])
        if supported:
            models = [item for item in models if item["model_type"] in supported] or models
        models.sort(key=lambda item: (0 if item["model_type"] == "chat" else 1 if item["model_type"] == "embedding" else 2, item["name"]))
        return models

    def _find_default_model(self, models: list[dict[str, Any]], model_type: str) -> dict[str, Any] | None:
        return next((item for item in models if item.get("model_type") == model_type), None)

    def _coerce_model_type(self, value: Any) -> str:
        normalized = str(value or "chat").strip().lower()
        if normalized in MODEL_TYPES:
            return normalized
        return "chat"

    def _coerce_context_window(self, value: Any) -> int | None:
        if value in (None, "", 0):
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def _infer_model_type(self, model_name: str) -> str:
        normalized = model_name.lower()
        if "rerank" in normalized or "reranker" in normalized:
            return "rerank"
        if "embed" in normalized or "embedding" in normalized:
            return "embedding"
        return "chat"

    def _discover_remote_models(self, provider: dict[str, Any], discovery_mode: str) -> list[dict[str, Any]]:
        if discovery_mode == "openai":
            payload = self._request_json("GET", self._append_path(provider["base_url"], "/models"), headers=self._auth_headers(provider, bearer=True))
            items = payload.get("data") or payload.get("models") or []
            return self._normalize_remote_models(items)
        if discovery_mode == "azure-openai":
            payload = self._request_json("GET", self._azure_models_url(provider), headers=self._auth_headers(provider, azure=True))
            items = payload.get("data") or payload.get("value") or payload.get("models") or []
            return self._normalize_remote_models(items)
        if discovery_mode == "anthropic":
            payload = self._request_json("GET", self._append_path(provider["base_url"], "/v1/models"), headers=self._auth_headers(provider, anthropic=True))
            items = payload.get("data") or payload.get("models") or []
            normalized = [{"name": str(item.get("id") or item.get("name") or ""), "model_type": "chat"} for item in items]
            return self._normalize_models(normalized, provider_type=str(provider["provider_type"]), fallback_model="")
        if discovery_mode == "gemini":
            payload = self._request_json("GET", self._gemini_url(provider, "/models"))
            items = payload.get("models") or []
            normalized: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").replace("models/", "").strip()
                if not name:
                    continue
                methods = [str(value) for value in item.get("supportedGenerationMethods", [])]
                model_type = "embedding" if "embedContent" in methods else "chat"
                entry: dict[str, Any] = {"name": name, "model_type": model_type}
                context_window = self._coerce_context_window(item.get("inputTokenLimit"))
                if context_window is not None:
                    entry["context_window"] = context_window
                normalized.append(entry)
            return self._normalize_models(normalized, provider_type=str(provider["provider_type"]), fallback_model="")
        if discovery_mode == "cohere":
            payload = self._request_json("GET", self._append_path(self._cohere_base(provider), "/models"), headers=self._auth_headers(provider, bearer=True))
            items = payload.get("models") or payload.get("data") or []
            normalized: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or item.get("id") or "").strip()
                if not name:
                    continue
                endpoints = [str(value).lower() for value in item.get("endpoints", [])]
                model_type = "rerank" if "rerank" in endpoints else "embedding" if "embed" in endpoints else "chat"
                entry: dict[str, Any] = {"name": name, "model_type": model_type}
                context_window = self._coerce_context_window(item.get("context_length"))
                if context_window is not None:
                    entry["context_window"] = context_window
                normalized.append(entry)
            return self._normalize_models(normalized, provider_type=str(provider["provider_type"]), fallback_model="")
        raise ValueError("当前 API 模式尚未实现模型发现。")

    def _normalize_remote_models(self, items: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("id") or item.get("name") or "").strip()
            if not name:
                continue
            model_type = self._infer_model_type(name)
            context_window = self._coerce_context_window(
                item.get("context_window")
                or item.get("contextLength")
                or item.get("context_length")
                or item.get("max_context_length")
                or item.get("inputTokenLimit")
            )
            entry: dict[str, Any] = {"name": name, "model_type": model_type}
            if isinstance(item.get("capabilities"), dict):
                capabilities = {str(key).lower(): bool(value) for key, value in item["capabilities"].items()}
                if capabilities.get("embeddings"):
                    entry["model_type"] = "embedding"
                if capabilities.get("rerank"):
                    entry["model_type"] = "rerank"
                if capabilities.get("chat_completion") or capabilities.get("completion"):
                    entry["model_type"] = "chat"
            if context_window is not None:
                entry["context_window"] = context_window
            normalized.append(entry)
        return normalized

    def _mock_model_test(self, model_name: str, model_type: str) -> dict[str, Any]:
        if model_type == "chat":
            return {"ok": True, "model": model_name, "model_type": model_type, "message": "Mock chat 测试通过。", "preview": "[mock] OK"}
        if model_type == "embedding":
            return {"ok": True, "model": model_name, "model_type": model_type, "message": "Mock embedding 测试通过。", "vector_size": 3}
        return {"ok": True, "model": model_name, "model_type": model_type, "message": "Mock rerank 测试通过。", "top_document_index": 0}

    def _chat_model_test(self, provider: dict[str, Any], model_name: str) -> dict[str, Any]:
        try:
            result = self.gateway.chat(
                provider,
                [
                    {"role": "system", "content": "You are a provider connectivity probe."},
                    {"role": "user", "content": "Reply with OK."},
                ],
                model=model_name,
                temperature=0.0,
                max_tokens=32,
            )
        except ProviderRequestError as exc:
            raise ValueError(str(exc)) from exc
        return {
            "ok": True,
            "model": result.model,
            "model_type": "chat",
            "message": "聊天模型测试通过。",
            "preview": trim_text(result.content, limit=120),
            "usage": result.usage,
        }

    def _embedding_model_test(self, provider: dict[str, Any], model_name: str) -> dict[str, Any]:
        provider_type = str(provider["provider_type"])
        if provider_type in {"openai", "custom_openai", "deepseek", "openrouter", "ollama"}:
            payload = self._request_json("POST", self._append_path(provider["base_url"], "/embeddings"), headers=self._auth_headers(provider, bearer=True), payload={"model": model_name, "input": "provider embedding probe"})
            data = (payload.get("data") or [{}])[0]
            vector = data.get("embedding") or []
            return {"ok": True, "model": model_name, "model_type": "embedding", "message": "嵌入模型测试通过。", "vector_size": len(vector)}
        if provider_type == "azure_openai":
            payload = self._request_json("POST", self._azure_embeddings_url(provider, model_name), headers=self._auth_headers(provider, azure=True), payload={"input": "provider embedding probe"})
            data = (payload.get("data") or [{}])[0]
            vector = data.get("embedding") or []
            return {"ok": True, "model": model_name, "model_type": "embedding", "message": "嵌入模型测试通过。", "vector_size": len(vector)}
        if provider_type == "gemini":
            payload = self._request_json("POST", self._gemini_url(provider, f"/models/{model_name}:embedContent"), payload={"content": {"parts": [{"text": "provider embedding probe"}]}})
            vector = ((payload.get("embedding") or {}).get("values")) or []
            return {"ok": True, "model": model_name, "model_type": "embedding", "message": "嵌入模型测试通过。", "vector_size": len(vector)}
        if provider_type == "cohere":
            payload = self._request_json("POST", self._append_path(self._cohere_base(provider), "/embed"), headers=self._auth_headers(provider, bearer=True), payload={"model": model_name, "texts": ["provider embedding probe"], "input_type": "search_document"})
            embeddings = payload.get("embeddings") or {}
            vectors = embeddings.get("float") or embeddings.get("int8") or embeddings.get("uint8") or []
            first = vectors[0] if isinstance(vectors, list) and vectors else []
            return {"ok": True, "model": model_name, "model_type": "embedding", "message": "嵌入模型测试通过。", "vector_size": len(first)}
        raise ValueError("当前 API 模式暂不支持 embedding 在线测试。")

    def _rerank_model_test(self, provider: dict[str, Any], model_name: str) -> dict[str, Any]:
        provider_type = str(provider["provider_type"])
        if provider_type == "cohere":
            payload = self._request_json(
                "POST",
                self._append_path(self._cohere_base(provider), "/rerank"),
                headers=self._auth_headers(provider, bearer=True),
                payload={"model": model_name, "query": "provider rerank probe", "documents": ["provider rerank probe", "unrelated text"], "top_n": 1},
            )
            results = payload.get("results") or []
            top = results[0] if results else {}
            return {
                "ok": True,
                "model": model_name,
                "model_type": "rerank",
                "message": "重排模型测试通过。",
                "top_document_index": int(top.get("index", 0) or 0),
                "top_score": top.get("relevance_score"),
            }
        raise ValueError("当前 API 模式暂不支持 rerank 在线测试。")

    def _auth_headers(
        self,
        provider: dict[str, Any],
        *,
        bearer: bool = False,
        azure: bool = False,
        anthropic: bool = False,
    ) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        api_key = str(provider.get("api_key") or "").strip()
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

    def _request_json(self, method: str, url: str, *, headers: dict[str, str] | None = None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(url, data=data, headers=headers or {}, method=method)
        try:
            with urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
            raise ValueError(trim_text(detail or str(exc), limit=320) or "Provider request failed.") from exc
        except URLError as exc:
            raise ValueError(trim_text(str(exc.reason), limit=320) or "Provider request failed.") from exc
        try:
            decoded = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            raise ValueError("Provider 返回了无法解析的 JSON。") from exc
        if isinstance(decoded, dict):
            return decoded
        raise ValueError("Provider 返回了非对象类型的 JSON。")

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

    def _azure_models_url(self, provider: dict[str, Any]) -> str:
        base_url = str(provider.get("base_url") or "").rstrip("/")
        api_version = str(provider.get("api_version") or "2024-10-21").strip()
        if "/openai/v1" in base_url:
            return self._append_path(base_url, "/models", {"api-version": "preview"})
        return self._append_path(base_url, "/openai/models", {"api-version": api_version})

    def _azure_embeddings_url(self, provider: dict[str, Any], model_name: str) -> str:
        base_url = str(provider.get("base_url") or "").rstrip("/")
        api_version = str(provider.get("api_version") or "2024-10-21").strip()
        if "/openai/v1" in base_url:
            return self._append_path(base_url, "/embeddings", {"api-version": "preview"})
        return self._append_path(base_url, f"/openai/deployments/{model_name}/embeddings", {"api-version": api_version})

    def _gemini_url(self, provider: dict[str, Any], path: str) -> str:
        api_key = str(provider.get("api_key") or "").strip()
        if not api_key:
            raise ValueError("Gemini API Key 不能为空。")
        return self._append_path(str(provider.get("base_url") or "").rstrip("/"), path, {"key": api_key})

    def _cohere_base(self, provider: dict[str, Any]) -> str:
        base_url = str(provider.get("base_url") or "https://api.cohere.com/v2").rstrip("/")
        return base_url[:-3] if base_url.endswith("/v2") else base_url
