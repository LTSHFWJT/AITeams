from __future__ import annotations

import json
import ssl
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from aiteams.agent_center.defaults import (
    default_agent_definitions,
    default_review_policies,
    default_provider_profiles,
    default_static_memories,
    default_team_definitions,
)
from aiteams.ai_gateway import AIGateway, ProviderRequestError
from aiteams.catalog import list_provider_presets, preset_for
from aiteams.deepagents import DeepAgentsTeamCompiler
from aiteams.langgraph import LangGraphTeamCompiler
from aiteams.plugins import PluginManager
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import pretty_json, trim_text

MODEL_TYPES = {"chat", "embedding", "rerank"}
LOCAL_MODEL_TYPES = {"Embed", "Rerank", "Chat"}
LOCAL_MODEL_TYPE_BY_KIND = {
    "embedding": "Embed",
    "rerank": "Rerank",
    "chat": "Chat",
}
LOCAL_MODEL_TYPE_ALIASES = {
    "embed": "Embed",
    "embedding": "Embed",
    "rerank": "Rerank",
    "reranker": "Rerank",
    "chat": "Chat",
    "llm": "Chat",
}
DEFAULT_MEMORY_PLUGIN_KEY = "memory_core"
RETRIEVAL_SETTINGS_KEY = "retrieval_models"
DEFAULT_LOCAL_EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
DEFAULT_LOCAL_RERANK_MODEL = "BAAI/bge-reranker-v2-m3"
LEGACY_DEFAULT_SKILL_KEYS = {
    "planning_skill",
    "architecture_skill",
    "delivery_skill",
    "review_skill",
}
AGENT_CENTER_UI_METADATA = {
    "review_policy": {
        "decision_types": [
            {"value": "approve", "label": "approve / 批准"},
            {"value": "reject", "label": "reject / 拒绝"},
            {"value": "edit", "label": "edit / 编辑"},
        ],
    },
    "team_edge_review": {
        "modes": [
            {"value": "must_review_before", "label": "must_review_before / 必须前审"},
        ],
        "message_types": [
            {"value": "task", "label": "task / 任务"},
            {"value": "dialogue", "label": "dialogue / 对话"},
            {"value": "handoff", "label": "handoff / 交接"},
        ],
        "phases": [
            {"value": "down", "label": "down / 向下"},
            {"value": "up", "label": "up / 向上"},
        ],
    },
    "memory_profile": {
        "scopes": [
            {"value": "agent", "label": "agent / Agent 私有"},
            {"value": "team", "label": "team / 团队共享"},
            {"value": "project", "label": "project / 项目共享"},
            {"value": "run", "label": "run / 当前运行"},
            {"value": "retrospective", "label": "retrospective / 运行回顾"},
        ]
    },
}


class AgentCenterService:
    def __init__(
        self,
        store: MetadataStore,
        plugin_manager: PluginManager | None = None,
        gateway: AIGateway | None = None,
        local_models_root: str | Path | None = None,
    ):
        self.store = store
        self.team_compiler = LangGraphTeamCompiler(store)
        self.deep_team_compiler = DeepAgentsTeamCompiler(store)
        self.plugin_manager = plugin_manager
        self.gateway = gateway or AIGateway()
        self.local_models_root = Path(local_models_root).expanduser().resolve() if local_models_root is not None else None
        if self.local_models_root is not None:
            self.local_models_root.mkdir(parents=True, exist_ok=True)

    def ensure_defaults(self) -> None:
        provider_id_map: dict[str, str] = {}
        for provider in default_provider_profiles():
            existing = self._find_default_provider_profile(provider)
            saved = existing
            if saved is None:
                normalized = self.prepare_provider_profile(self._default_provider_payload(provider))
                saved = self.store.save_provider_profile(
                    provider_profile_id=f"prov_{provider['builtin_ref']}",
                    name=str(normalized["name"]),
                    provider_type=str(normalized["provider_type"]),
                    description=str(normalized["description"]),
                    config=dict(normalized["config"]),
                    secret=dict(normalized["secret"]),
                )
            provider_id_map[str(provider["builtin_ref"])] = str(saved["id"])

        self._cleanup_legacy_default_skills()
        self.store.sync_skill_groups_from_skills()

        for static_memory in default_static_memories():
            if self.store.get_static_memory_by_key(str(static_memory["key"])) is None:
                self.store.save_static_memory(
                    static_memory_id=None,
                    key=str(static_memory["key"]),
                    name=str(static_memory["name"]),
                    description=str(static_memory.get("description") or ""),
                    version=str(static_memory.get("version") or "v1"),
                    spec=dict(static_memory.get("spec") or {}),
                )

        for policy in default_review_policies():
            if self.store.get_review_policy_by_key(str(policy["key"])) is None:
                self.store.save_review_policy(
                    review_policy_id=None,
                    key=str(policy["key"]),
                    name=str(policy["name"]),
                    description=str(policy.get("description") or ""),
                    version=str(policy.get("version") or "v1"),
                    spec=dict(policy.get("spec") or {}),
                )

        for agent_definition in default_agent_definitions():
            if self.store.get_agent_definition(str(agent_definition["id"])) is None:
                self.store.save_agent_definition(
                    agent_definition_id=str(agent_definition["id"]),
                    name=str(agent_definition["name"]),
                    role=str(agent_definition["role"]),
                    description=str(agent_definition.get("description") or ""),
                    version=str(agent_definition.get("version") or "v1"),
                    spec=self._default_agent_definition_spec(
                        agent_definition,
                        provider_id_map=provider_id_map,
                    ),
                )

        for team_definition in default_team_definitions():
            if self.store.get_team_definition_by_key(str(team_definition["key"])) is None:
                self.store.save_team_definition(
                    team_definition_id=None,
                    key=str(team_definition["key"]),
                    name=str(team_definition["name"]),
                    description=str(team_definition.get("description") or ""),
                    version=str(team_definition.get("version") or "v1"),
                    spec=self._default_team_definition_spec(team_definition),
                )

    def ensure_local_model_defaults(self) -> None:
        manifest_paths = self._local_model_manifest_paths()
        existing_models = [self.normalize_local_model(item) for item in self.store.list_local_models()]
        default_specs = [
            {
                "name": DEFAULT_LOCAL_EMBEDDING_MODEL,
                "model_type": "Embed",
                "model_path": manifest_paths.get(DEFAULT_LOCAL_EMBEDDING_MODEL) or self._default_local_model_path(DEFAULT_LOCAL_EMBEDDING_MODEL),
            },
            {
                "name": DEFAULT_LOCAL_RERANK_MODEL,
                "model_type": "Rerank",
                "model_path": manifest_paths.get(DEFAULT_LOCAL_RERANK_MODEL) or self._default_local_model_path(DEFAULT_LOCAL_RERANK_MODEL),
            },
        ]
        for spec in default_specs:
            existing = self.store.get_local_model_by_path(str(spec["model_path"]))
            if existing is None:
                existing = next(
                    (
                        item
                        for item in existing_models
                        if item is not None
                        and str(item.get("name") or "") == str(spec["name"])
                        and str(item.get("model_type") or "") == str(spec["model_type"])
                    ),
                    None,
                )
            if existing is not None:
                continue
            self.store.save_local_model(
                local_model_id=None,
                name=str(spec["name"]),
                model_type=str(spec["model_type"]),
                model_path=str(spec["model_path"]),
            )

    def ensure_retrieval_settings_defaults(self) -> None:
        if self.store.get_platform_setting_record(RETRIEVAL_SETTINGS_KEY) is not None:
            return
        self.save_retrieval_settings(self._default_retrieval_settings_payload())

    def _default_retrieval_settings_payload(self) -> dict[str, Any]:
        return {
            "embedding": {"mode": "disabled"},
            "rerank": {"mode": "disabled"},
        }

    def provider_types(self) -> list[dict[str, Any]]:
        return list_provider_presets()

    def ui_metadata(self) -> dict[str, Any]:
        payload = json.loads(json.dumps(AGENT_CENTER_UI_METADATA))
        payload.pop("memory_profile", None)
        return payload

    def list_provider_profiles(
        self,
        *,
        query: str | None = None,
        provider_type: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        page = self.store.list_provider_profiles_page(query=query, provider_type=provider_type, limit=limit, offset=offset)
        items = [self.normalize_provider_profile(item) for item in page["items"]]
        return {
            "items": items,
            "total": page["total"],
            "offset": page["offset"],
            "limit": page["limit"],
        }

    def normalize_local_model_type(self, value: Any) -> str:
        normalized = str(value or "").strip()
        if normalized in LOCAL_MODEL_TYPES:
            return normalized
        alias = LOCAL_MODEL_TYPE_ALIASES.get(normalized.lower())
        if alias:
            return alias
        raise ValueError("本地模型类型必须为 Embed / Rerank / Chat。")

    def local_model_kind(self, model_type: Any) -> str:
        canonical = self.normalize_local_model_type(model_type)
        for kind, label in LOCAL_MODEL_TYPE_BY_KIND.items():
            if label == canonical:
                return kind
        return "chat"

    def normalize_local_model(self, model: dict[str, Any] | None) -> dict[str, Any] | None:
        if model is None:
            return None
        item = dict(model)
        try:
            item["model_type"] = self.normalize_local_model_type(item.get("model_type"))
            item["model_kind"] = self.local_model_kind(item.get("model_type"))
        except ValueError:
            item["model_type"] = str(item.get("model_type") or "").strip()
            item["model_kind"] = "chat"
        item["label"] = str(item.get("name") or item.get("id") or "")
        return item

    def prepare_local_model(self, payload: dict[str, Any], *, existing: dict[str, Any] | None = None) -> dict[str, Any]:
        existing_item = self.normalize_local_model(existing) if existing else None
        name = trim_text(payload.get("name") or (existing_item or {}).get("name") or "", limit=255)
        if not name:
            raise ValueError("本地模型名称不能为空。")
        model_type = self.normalize_local_model_type(payload.get("model_type") or payload.get("type") or (existing_item or {}).get("model_type") or "")
        model_path = str(payload.get("model_path") or payload.get("path") or (existing_item or {}).get("model_path") or "").strip()
        if not model_path:
            raise ValueError("本地模型路径不能为空。")
        return {
            "name": name,
            "model_type": model_type,
            "model_path": model_path,
        }

    def _default_local_model_path(self, repo_id: str) -> str:
        directory = str(repo_id or "").strip().replace("\\", "_").replace("/", "__")
        if self.local_models_root is None:
            return directory
        return f"{self.local_models_root.name}/{directory}"

    def _local_model_manifest_paths(self) -> dict[str, str]:
        if self.local_models_root is None:
            return {}
        manifest_path = self.local_models_root / "model-manifest.json"
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        items = payload.get("downloaded")
        if not isinstance(items, list):
            return {}
        resolved: dict[str, str] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            repo_id = str(item.get("repo_id") or "").strip()
            local_dir = str(item.get("local_dir") or "").strip()
            if not repo_id or not local_dir:
                continue
            target = Path(local_dir).expanduser().resolve()
            try:
                relative = target.relative_to(self.local_models_root.parent).as_posix()
            except ValueError:
                try:
                    relative = target.relative_to(self.local_models_root).as_posix()
                except ValueError:
                    relative = self._default_local_model_path(repo_id)
            resolved[repo_id] = relative
        return resolved

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
        merged_config.pop("gateway_capabilities", None)
        models = self._normalize_models(
            payload.get("models", merged_config.get("models")),
            provider_type=provider_type,
            fallback_model=str(payload.get("model") or merged_config.get("model") or "").strip(),
        )
        default_chat = self._find_default_model(models, "chat")
        default_model_name = default_chat["name"] if default_chat else (models[0]["name"] if models else "")

        base_url = str(payload.get("base_url") or merged_config.get("base_url") or "").strip()
        if not base_url and preset.get("use_default_base_url_when_blank"):
            base_url = str(preset.get("default_base_url") or "").strip()
        api_version = str(payload.get("api_version") or merged_config.get("api_version") or preset.get("default_api_version") or "").strip()
        organization = str(payload.get("organization") or merged_config.get("organization") or "").strip()
        api_key_env = str(payload.get("api_key_env") or merged_config.get("api_key_env") or "").strip()
        skip_tls_verify = self._coerce_bool(payload.get("skip_tls_verify", merged_config.get("skip_tls_verify")))

        normalized_config = dict(merged_config)
        normalized_config["backend"] = provider_type
        normalized_config["models"] = models
        normalized_config["skip_tls_verify"] = skip_tls_verify
        normalized_config["temperature"] = float(merged_config.get("temperature", 0.2))
        extra_config = dict(merged_config.get("extra_config") or {})
        extra_config.pop("gateway_capabilities", None)
        normalized_config["extra_config"] = extra_config
        if default_model_name:
            normalized_config["model"] = default_model_name
        else:
            normalized_config.pop("model", None)
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
        config.pop("gateway_capabilities", None)
        models = self._normalize_models(config.get("models"), provider_type=provider_type, fallback_model=str(config.get("model") or "").strip())
        default_chat = self._find_default_model(models, "chat")
        default_model_name = default_chat["name"] if default_chat else (models[0]["name"] if models else "")
        config["backend"] = str(config.get("backend") or provider_type)
        config["models"] = models
        if default_model_name:
            config["model"] = default_model_name
        else:
            config.pop("model", None)
        config["skip_tls_verify"] = self._coerce_bool(config.get("skip_tls_verify"))
        extra_config = dict(config.get("extra_config") or {})
        extra_config.pop("gateway_capabilities", None)
        config["extra_config"] = extra_config
        if not config.get("base_url") and preset.get("use_default_base_url_when_blank"):
            config["base_url"] = str(preset.get("default_base_url") or "")
        if preset.get("default_api_version") and not config.get("api_version"):
            config["api_version"] = str(preset.get("default_api_version") or "")
        item["config_json"] = config
        item.pop("gateway_capabilities", None)
        item["key"] = str(item.get("key") or item.get("id") or "")
        item["api_mode"] = provider_type
        item["model_count"] = len(models)
        item["default_model_name"] = default_model_name
        item["default_chat_model_name"] = default_chat["name"] if default_chat else ""
        item["supported_model_types"] = list(preset.get("supported_model_types") or [])
        item["preset"] = preset
        return item

    def discover_provider_models(self, payload: dict[str, Any]) -> dict[str, Any]:
        runtime_provider = self._runtime_provider(payload)
        preset = preset_for(str(runtime_provider["provider_type"]))
        discovery_mode = str(preset.get("discovery_mode") or "local")
        if discovery_mode == "local":
            return {"items": runtime_provider.get("models") or [], "source": "local"}
        self._require_runtime_base_url(runtime_provider, label=str(preset.get("label") or runtime_provider["provider_type"]))
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
        if runtime_provider["provider_type"] != "mock":
            preset = preset_for(str(runtime_provider["provider_type"]))
            self._require_runtime_base_url(runtime_provider, label=str(preset.get("label") or runtime_provider["provider_type"]))
        if runtime_provider["provider_type"] == "mock":
            return self._mock_model_test(model_name, model_type)
        if model_type == "chat":
            return self._chat_model_test(runtime_provider, model_name)
        if model_type == "embedding":
            return self._embedding_model_test(runtime_provider, model_name)
        return self._rerank_model_test(runtime_provider, model_name)

    def get_retrieval_settings(self) -> dict[str, Any]:
        record = self.store.get_platform_setting_record(RETRIEVAL_SETTINGS_KEY)
        normalized, _runtime, warnings = self._normalize_retrieval_settings(
            dict((record or {}).get("value_json") or {}),
            strict=False,
        )
        return {
            "settings": normalized,
            "warnings": warnings,
            "updated_at": (record or {}).get("updated_at"),
        }

    def save_retrieval_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized, runtime, _warnings = self._normalize_retrieval_settings(payload, strict=True)
        record = self.store.save_platform_setting(RETRIEVAL_SETTINGS_KEY, normalized)
        return {
            "settings": normalized,
            "runtime": runtime,
            "updated_at": record.get("updated_at"),
        }

    def retrieval_runtime_config(self) -> dict[str, Any]:
        settings = self.store.get_platform_setting(RETRIEVAL_SETTINGS_KEY, default={})
        _normalized, runtime, _warnings = self._normalize_retrieval_settings(settings, strict=False)
        return runtime

    def compile_team_definition(self, team_definition_id: str) -> dict[str, Any]:
        team_definition = self.store.get_team_definition(team_definition_id)
        if team_definition is None:
            raise ValueError("Team definition does not exist.")
        if self.deep_team_compiler.handles(team_definition):
            compiled = self.deep_team_compiler.compile(team_definition)
            return {
                "team_definition": team_definition,
                "resource_lock": compiled.resource_lock,
                "hierarchy": compiled.root,
                "preview": {
                    "agent_count": compiled.agent_count,
                    "team_count": compiled.team_count,
                    "node_count": len((compiled.blueprint.get("flow") or {}).get("nodes", [])),
                    "edge_count": len((compiled.blueprint.get("flow") or {}).get("edges", [])),
                    "execution_mode": (compiled.blueprint.get("metadata") or {}).get("execution_mode"),
                    "hierarchy_mode": "strict_tree",
                },
                "blueprint_spec": compiled.blueprint,
            }
        compiled = self.team_compiler.compile(team_definition)
        return {
            "team_definition": team_definition,
            "resource_lock": compiled.resource_lock,
            "adjacency": compiled.adjacency,
            "preview": {
                "member_count": len(compiled.members),
                "node_count": len((compiled.blueprint.get("flow") or {}).get("nodes", [])),
                "edge_count": len((compiled.blueprint.get("flow") or {}).get("edges", [])),
                "communication_policy": (compiled.blueprint.get("metadata") or {}).get("communication_policy"),
                "execution_mode": (compiled.blueprint.get("metadata") or {}).get("execution_mode"),
            },
            "blueprint_spec": compiled.blueprint,
        }

    def build_team_definition(self, team_definition_id: str, *, blueprint_name: str | None = None) -> dict[str, Any]:
        team_definition = self.store.get_team_definition(team_definition_id)
        if team_definition is None:
            raise ValueError("Team definition does not exist.")
        compiled = self.deep_team_compiler.compile(team_definition) if self.deep_team_compiler.handles(team_definition) else self.team_compiler.compile(team_definition)
        spec = compiled.blueprint
        name = blueprint_name or f"{team_definition['name']} Runtime"
        blueprint = self.store.save_blueprint(
            blueprint_id=None,
            workspace_id=str(spec["workspace_id"]),
            project_id=str(spec["project_id"]),
            name=name,
            description=trim_text(str(team_definition.get("description") or ""), limit=280),
            version="team-definition-build",
            raw_format="json",
            raw_text=pretty_json(spec),
            spec=spec,
            is_template=False,
        )
        return {
            "team_definition": team_definition,
            "blueprint": blueprint,
            "resource_lock": compiled.resource_lock,
            "adjacency": getattr(compiled, "adjacency", {}),
            "hierarchy": getattr(compiled, "root", None),
        }

    def _default_provider_payload(self, provider: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "name": str(provider.get("name") or ""),
            "provider_type": str(provider.get("provider_type") or ""),
            "description": str(provider.get("description") or ""),
            "config": json.loads(json.dumps(provider.get("config") or {}, ensure_ascii=False)),
            "secret": json.loads(json.dumps(provider.get("secret") or {}, ensure_ascii=False)),
        }
        config = dict(payload.get("config") or {})
        config["builtin_ref"] = str(provider.get("builtin_ref") or "")
        payload["config"] = config
        return payload

    def _cleanup_legacy_default_skills(self) -> None:
        self._remove_legacy_skill_refs(LEGACY_DEFAULT_SKILL_KEYS)
        for skill in self.store.list_skills():
            skill_name = str(skill.get("name") or "").strip()
            if skill_name not in LEGACY_DEFAULT_SKILL_KEYS:
                continue
            self.store.delete_skill(str(skill.get("id") or ""))

    def _remove_legacy_skill_refs(self, skill_keys: set[str]) -> None:
        if not skill_keys:
            return
        for agent_definition in self.store.list_agent_definitions():
            spec = dict(agent_definition.get("spec_json") or {})
            current_refs = [str(item).strip() for item in list(spec.get("skill_refs") or []) if str(item).strip()]
            next_refs = [item for item in current_refs if item not in skill_keys]
            if len(next_refs) == len(current_refs):
                continue
            if next_refs:
                spec["skill_refs"] = next_refs
            else:
                spec.pop("skill_refs", None)
            self.store.save_agent_definition(
                agent_definition_id=str(agent_definition.get("id") or ""),
                name=str(agent_definition.get("name") or ""),
                role=str(agent_definition.get("role") or ""),
                description=str(agent_definition.get("description") or ""),
                version=str(agent_definition.get("version") or "v1"),
                spec=spec,
                status=str(agent_definition.get("status") or "active"),
            )

    def _default_agent_definition_spec(
        self,
        definition: dict[str, Any],
        *,
        provider_id_map: dict[str, str],
    ) -> dict[str, Any]:
        spec = json.loads(json.dumps(definition.get("spec") or {}, ensure_ascii=False))
        provider_ref = str(spec.get("provider_ref") or "").strip()
        if provider_ref:
            spec["provider_ref"] = provider_id_map.get(provider_ref, provider_ref)
        spec["tool_plugin_refs"] = self._ensure_default_plugin_refs(list(spec.get("tool_plugin_refs", [])))
        spec.pop("memory_profile_ref", None)
        spec.pop("memory_profile_id", None)
        spec.pop("memory_profile", None)
        return spec

    def _default_team_definition_spec(self, team_definition: dict[str, Any]) -> dict[str, Any]:
        spec = json.loads(json.dumps(team_definition.get("spec") or {}, ensure_ascii=False))
        members = []
        for item in spec.get("members", []):
            member = dict(item or {})
            reference = str(member.get("agent_definition_ref") or "").strip()
            if reference:
                resolved = self.store.get_agent_definition(reference)
                if resolved is not None:
                    member["agent_definition_ref"] = str(resolved["id"])
            members.append(member)
        spec["members"] = members
        shared_kb_bindings: list[str] = []
        for reference in list(spec.get("shared_kb_bindings") or []):
            resolved = self.store.get_knowledge_base_by_key(str(reference))
            shared_kb_bindings.append(str(resolved["id"]) if resolved is not None else str(reference))
        if shared_kb_bindings:
            spec["shared_kb_bindings"] = shared_kb_bindings
        shared_static_memory_bindings: list[str] = []
        for reference in list(spec.get("shared_static_memory_bindings") or []):
            resolved = self.store.get_static_memory_by_key(str(reference))
            shared_static_memory_bindings.append(str(resolved["id"]) if resolved is not None else str(reference))
        if shared_static_memory_bindings:
            spec["shared_static_memory_bindings"] = shared_static_memory_bindings
        return spec

    def ensure_default_plugin_refs(self, references: list[Any]) -> list[str]:
        return self._ensure_default_plugin_refs(references)

    def _ensure_default_plugin_refs(self, references: list[Any]) -> list[str]:
        memory_plugin = self.store.get_plugin_by_key(DEFAULT_MEMORY_PLUGIN_KEY)
        default_ref = str(memory_plugin["id"]) if memory_plugin is not None else DEFAULT_MEMORY_PLUGIN_KEY
        aliases = {DEFAULT_MEMORY_PLUGIN_KEY, default_ref}
        normalized: list[str] = []
        seen: set[str] = set()
        for item in list(references or []):
            value = str(item or "").strip()
            if not value:
                continue
            plugin = self.store.get_plugin(value) or self.store.get_plugin_by_key(value)
            canonical = str(plugin["id"]) if plugin is not None else value
            if canonical in seen:
                continue
            seen.add(canonical)
            normalized.append(canonical)
        if not any(item in aliases for item in normalized):
            normalized.insert(0, default_ref)
        return normalized

    def _find_default_provider_profile(self, provider: dict[str, Any]) -> dict[str, Any] | None:
        builtin_ref = str(provider.get("builtin_ref") or "").strip()
        provider_type = str(provider.get("provider_type") or "").strip()
        name = str(provider.get("name") or "").strip()
        for item in self.store.list_provider_profiles():
            config = dict(item.get("config_json") or {})
            if builtin_ref and str(config.get("builtin_ref") or "").strip() == builtin_ref:
                return item
            if str(item.get("provider_type") or "").strip() == provider_type and str(item.get("name") or "").strip() == name:
                return item
        return None

    def _runtime_provider(self, payload: dict[str, Any]) -> dict[str, Any]:
        existing = self._stored_runtime_provider(payload)
        existing_config = dict((existing or {}).get("config_json") or {})
        config = dict(existing_config)
        config.update(dict(payload.get("config") or {}))
        config.pop("gateway_capabilities", None)
        provider_type = str(payload.get("provider_type") or (existing or {}).get("provider_type") or "mock").strip()
        preset = preset_for(provider_type)
        models = self._normalize_models(payload.get("models", config.get("models")), provider_type=provider_type, fallback_model=str(config.get("model") or "").strip())
        base_url = str(payload.get("base_url") or config.get("base_url") or "").strip()
        if not base_url and preset.get("use_default_base_url_when_blank"):
            base_url = str(preset.get("default_base_url") or "").strip()
        api_version = str(payload.get("api_version") or config.get("api_version") or preset.get("default_api_version") or "").strip()
        organization = str(payload.get("organization") or config.get("organization") or "").strip()
        skip_tls_verify = self._coerce_bool(payload.get("skip_tls_verify", config.get("skip_tls_verify")))
        extra_config = dict(config.get("extra_config") or {})
        extra_config.pop("gateway_capabilities", None)
        secret = dict((existing or {}).get("secret_json") or {})
        secret.update(dict(payload.get("secret") or {}))
        api_key = str(secret.get("api_key") or payload.get("api_key") or "").strip()
        if not api_key:
            api_key = str((payload.get("secret_json") or {}).get("api_key") or "").strip()
        return {
            "name": str(payload.get("name") or (existing or {}).get("name") or "Provider Test"),
            "provider_type": provider_type,
            "base_url": base_url,
            "api_version": api_version,
            "organization": organization,
            "skip_tls_verify": skip_tls_verify,
            "api_key": api_key,
            "models": models,
            "model": str(config.get("model") or (self._find_default_model(models, "chat") or {}).get("name") or "").strip(),
            "extra_config": extra_config,
        }

    def _runtime_provider_from_profile(self, provider_profile: dict[str, Any]) -> dict[str, Any]:
        normalized = self.normalize_provider_profile(provider_profile)
        if normalized is None:
            raise ValueError("Provider profile does not exist.")
        config = dict(normalized.get("config_json") or {})
        return {
            "id": normalized.get("id"),
            "name": str(normalized.get("name") or "Provider"),
            "provider_type": str(normalized.get("provider_type") or "mock"),
            "base_url": str(config.get("base_url") or "").strip(),
            "api_version": str(config.get("api_version") or "").strip(),
            "organization": str(config.get("organization") or "").strip(),
            "skip_tls_verify": self._coerce_bool(config.get("skip_tls_verify")),
            "api_key": str((normalized.get("secret_json") or {}).get("api_key") or "").strip(),
            "models": [dict(item) for item in list(config.get("models") or []) if isinstance(item, dict)],
            "model": str(config.get("model") or "").strip(),
            "extra_config": dict(config.get("extra_config") or {}),
        }

    def _normalize_retrieval_settings(
        self,
        payload: dict[str, Any],
        *,
        strict: bool,
    ) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
        warnings: list[str] = []

        def _fallback_embedding() -> tuple[dict[str, Any], dict[str, Any]]:
            return (
                {"mode": "disabled"},
                {"mode": "disabled"},
            )

        def _fallback_rerank() -> tuple[dict[str, Any], dict[str, Any]]:
            return (
                {"mode": "disabled"},
                {"mode": "disabled"},
            )

        def _resolve_runtime_local_model_name(model_name: str) -> str:
            candidate = str(model_name or "").strip()
            if not candidate:
                return candidate
            if candidate.startswith("models/") or candidate.startswith("models\\") or Path(candidate).expanduser().is_absolute():
                return self._resolve_local_model_path(candidate)
            return candidate

        def _resolve_managed_local_model(
            raw: dict[str, Any],
            *,
            kind: str,
            backend: str,
        ) -> tuple[dict[str, Any], dict[str, Any]] | None:
            local_model_id = str(raw.get("local_model_id") or raw.get("model_id") or "").strip()
            if not local_model_id:
                return None
            local_model = self.normalize_local_model(self.store.get_local_model(local_model_id))
            if local_model is None:
                raise ValueError(f"{kind} 本地模型 `{local_model_id}` 不存在。")
            expected_type = LOCAL_MODEL_TYPE_BY_KIND[kind]
            actual_type = str(local_model.get("model_type") or "").strip()
            if actual_type != expected_type:
                raise ValueError(f"{kind} 本地模型 `{local_model.get('name') or local_model_id}` 类型必须是 {expected_type}。")
            model_path = str(local_model.get("model_path") or "").strip()
            if not model_path:
                raise ValueError(f"{kind} 本地模型 `{local_model.get('name') or local_model_id}` 缺少模型路径。")
            resolved_path = self._resolve_local_model_path(model_path)
            if not Path(resolved_path).exists():
                raise ValueError(f"{kind} 本地模型路径不存在：{model_path}")
            public = {
                "mode": "local",
                "backend": backend,
                "local_model_id": local_model_id,
                "model_name": model_path,
                "model_path": model_path,
                "model_label": str(local_model.get("name") or local_model_id),
                "model_type": actual_type,
            }
            runtime = {
                "mode": "local",
                "backend": backend,
                "local_model_id": local_model_id,
                "model_name": resolved_path,
                "model": resolved_path,
                "model_path": model_path,
                "model_label": public["model_label"],
                "model_type": actual_type,
            }
            return public, runtime

        def _resolve_local_embedding(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            managed = _resolve_managed_local_model(raw, kind="embedding", backend="huggingface")
            if managed is not None:
                return managed
            model_name = str(raw.get("model_name") or raw.get("model") or DEFAULT_LOCAL_EMBEDDING_MODEL).strip()
            if not model_name:
                raise ValueError("本地 embedding model_name 不能为空。")
            public = {
                "mode": "local",
                "backend": "huggingface",
                "model_name": model_name,
            }
            runtime = {
                "mode": "local",
                "backend": "huggingface",
                "model_name": _resolve_runtime_local_model_name(model_name),
                "model": _resolve_runtime_local_model_name(model_name),
            }
            return public, runtime

        def _resolve_local_rerank(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            managed = _resolve_managed_local_model(raw, kind="rerank", backend="flag_embedding")
            if managed is not None:
                return managed
            model_name = str(raw.get("model_name") or raw.get("model") or DEFAULT_LOCAL_RERANK_MODEL).strip()
            if not model_name:
                raise ValueError("本地 rerank model_name 不能为空。")
            public = {
                "mode": "local",
                "backend": "flag_embedding",
                "model_name": model_name,
            }
            runtime = {
                "mode": "local",
                "backend": "flag_embedding",
                "model_name": _resolve_runtime_local_model_name(model_name),
                "model": _resolve_runtime_local_model_name(model_name),
            }
            return public, runtime

        def _resolve_model_selection(
            raw: dict[str, Any],
            *,
            kind: str,
            allowed_types: set[str],
        ) -> tuple[dict[str, Any], dict[str, Any]] | None:
            mode = str(raw.get("mode") or "").strip().lower()
            if kind == "embedding" and mode == "local":
                return _resolve_local_embedding(raw)
            if kind == "rerank" and mode == "local":
                return _resolve_local_rerank(raw)
            if kind == "embedding" and mode != "provider":
                return _fallback_embedding()
            if kind == "rerank" and mode != "provider":
                return _fallback_rerank()
            provider_id = str(raw.get("provider_id") or raw.get("provider_ref") or raw.get("provider") or "").strip()
            model_name = str(raw.get("model_name") or raw.get("model") or "").strip()
            if not provider_id or not model_name:
                raise ValueError(f"{kind} provider_id 和 model_name 不能为空。")
            provider = self.store.get_provider_profile(provider_id, include_secret=True)
            if provider is None:
                raise ValueError(f"{kind} provider `{provider_id}` does not exist.")
            runtime_provider = self._runtime_provider_from_profile(provider)
            models = [dict(item) for item in list(runtime_provider.get("models") or []) if isinstance(item, dict)]
            matched = next((item for item in models if str(item.get("name") or "") == model_name), None)
            if matched is None:
                raise ValueError(f"{kind} model `{model_name}` is not configured on provider `{provider_id}`.")
            model_type = str(matched.get("model_type") or "chat")
            if model_type not in allowed_types:
                allowed = ", ".join(sorted(allowed_types))
                raise ValueError(f"{kind} model `{model_name}` must be one of: {allowed}.")
            public = {
                "mode": "provider",
                "provider_id": str(provider.get("id") or provider_id),
                "provider_name": str(provider.get("name") or provider_id),
                "provider_type": str(provider.get("provider_type") or runtime_provider.get("provider_type") or ""),
                "model_name": model_name,
            }
            runtime = {
                "mode": "provider",
                "provider_id": public["provider_id"],
                "provider_name": public["provider_name"],
                "provider_type": public["provider_type"],
                "model_name": model_name,
                "model_type": model_type,
                "provider": runtime_provider,
                "model": model_name,
            }
            return public, runtime

        raw_embedding = dict(payload.get("embedding") or {})
        raw_rerank = dict(payload.get("rerank") or {})

        try:
            embedding = _resolve_model_selection(raw_embedding, kind="embedding", allowed_types={"embedding"})
            assert embedding is not None
            public_embedding, runtime_embedding = embedding
        except ValueError as exc:
            if strict:
                raise
            warnings.append(str(exc))
            public_embedding, runtime_embedding = _fallback_embedding()

        try:
            rerank = _resolve_model_selection(raw_rerank, kind="rerank", allowed_types={"rerank"})
            assert rerank is not None
            public_rerank, runtime_rerank = rerank
        except ValueError as exc:
            if strict:
                raise
            warnings.append(str(exc))
            public_rerank, runtime_rerank = _fallback_rerank()

        return (
            {
                "embedding": public_embedding,
                "rerank": public_rerank,
            },
            {
                "embedding": runtime_embedding,
                "rerank": runtime_rerank,
            },
            warnings,
        )

    def _resolve_local_model_path(self, raw_path: str) -> str:
        text = str(raw_path or "").strip()
        if not text:
            return text
        candidate = Path(text).expanduser()
        if candidate.is_absolute():
            return str(candidate.resolve())
        if self.local_models_root is None:
            return str(candidate)
        parts = [part for part in candidate.parts if part and part != "."]
        if not parts:
            return str(self.local_models_root)
        if parts[0] == self.local_models_root.name:
            return str((self.local_models_root.parent / Path(*parts)).resolve())
        return str((self.local_models_root / Path(*parts)).resolve())

    def _stored_runtime_provider(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        provider_id = str(payload.get("id") or "").strip()
        if not provider_id:
            return None
        return self.store.get_provider_profile(provider_id, include_secret=True)

    def _require_runtime_base_url(self, provider: dict[str, Any], *, label: str) -> None:
        if str(provider.get("base_url") or "").strip():
            return
        raise ValueError(f"{label} Base URL 不能为空。")

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

    def _coerce_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(value, (int, float)):
            return bool(value)
        return False

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
            payload = self._request_json("GET", self._append_path(provider["base_url"], "/models"), headers=self._auth_headers(provider, bearer=True), skip_tls_verify=bool(provider.get("skip_tls_verify")))
            items = payload.get("data") or payload.get("models") or []
            return self._normalize_remote_models(items)
        if discovery_mode == "azure-openai":
            payload = self._request_json("GET", self._azure_models_url(provider), headers=self._auth_headers(provider, azure=True), skip_tls_verify=bool(provider.get("skip_tls_verify")))
            items = payload.get("data") or payload.get("value") or payload.get("models") or []
            return self._normalize_remote_models(items)
        if discovery_mode == "anthropic":
            payload = self._request_json("GET", self._append_path(provider["base_url"], "/v1/models"), headers=self._auth_headers(provider, anthropic=True), skip_tls_verify=bool(provider.get("skip_tls_verify")))
            items = payload.get("data") or payload.get("models") or []
            normalized = [{"name": str(item.get("id") or item.get("name") or ""), "model_type": "chat"} for item in items]
            return self._normalize_models(normalized, provider_type=str(provider["provider_type"]), fallback_model="")
        if discovery_mode == "gemini":
            payload = self._request_json("GET", self._gemini_url(provider, "/models"), skip_tls_verify=bool(provider.get("skip_tls_verify")))
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
            payload = self._request_json("GET", self._append_path(self._cohere_base(provider), "/models"), headers=self._auth_headers(provider, bearer=True), skip_tls_verify=bool(provider.get("skip_tls_verify")))
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
            payload = self._request_json("POST", self._append_path(provider["base_url"], "/embeddings"), headers=self._auth_headers(provider, bearer=True), payload={"model": model_name, "input": "provider embedding probe"}, skip_tls_verify=bool(provider.get("skip_tls_verify")))
            data = (payload.get("data") or [{}])[0]
            vector = data.get("embedding") or []
            return {"ok": True, "model": model_name, "model_type": "embedding", "message": "嵌入模型测试通过。", "vector_size": len(vector)}
        if provider_type == "azure_openai":
            payload = self._request_json("POST", self._azure_embeddings_url(provider, model_name), headers=self._auth_headers(provider, azure=True), payload={"input": "provider embedding probe"}, skip_tls_verify=bool(provider.get("skip_tls_verify")))
            data = (payload.get("data") or [{}])[0]
            vector = data.get("embedding") or []
            return {"ok": True, "model": model_name, "model_type": "embedding", "message": "嵌入模型测试通过。", "vector_size": len(vector)}
        if provider_type == "gemini":
            payload = self._request_json("POST", self._gemini_url(provider, f"/models/{model_name}:embedContent"), payload={"content": {"parts": [{"text": "provider embedding probe"}]}}, skip_tls_verify=bool(provider.get("skip_tls_verify")))
            vector = ((payload.get("embedding") or {}).get("values")) or []
            return {"ok": True, "model": model_name, "model_type": "embedding", "message": "嵌入模型测试通过。", "vector_size": len(vector)}
        if provider_type == "cohere":
            payload = self._request_json("POST", self._append_path(self._cohere_base(provider), "/embed"), headers=self._auth_headers(provider, bearer=True), payload={"model": model_name, "texts": ["provider embedding probe"], "input_type": "search_document"}, skip_tls_verify=bool(provider.get("skip_tls_verify")))
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
                skip_tls_verify=bool(provider.get("skip_tls_verify")),
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

    def _request_json(self, method: str, url: str, *, headers: dict[str, str] | None = None, payload: dict[str, Any] | None = None, skip_tls_verify: bool = False) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(url, data=data, headers=headers or {}, method=method)
        try:
            context = ssl._create_unverified_context() if skip_tls_verify else None
            with urlopen(request, timeout=30, context=context) as response:
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
