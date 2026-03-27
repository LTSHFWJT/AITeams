from __future__ import annotations

import json
import ssl
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from aiteams.agent_center.defaults import (
    default_agent_templates,
    default_agent_definitions,
    default_memory_profiles,
    default_review_policies,
    default_plugins,
    default_provider_profiles,
    default_skills,
    default_static_memories,
    default_team_templates,
    default_team_definitions,
)
from aiteams.agent_center.resolver import BuildResolver
from aiteams.agent_center.team_graph import normalize_team_template_spec, validate_team_template_spec
from aiteams.ai_gateway import AIGateway, ProviderRequestError
from aiteams.catalog import list_provider_presets, preset_for
from aiteams.deepagents import DeepAgentsTeamCompiler
from aiteams.deepagents.compiler import is_deepagents_team_spec
from aiteams.langgraph import LangGraphTeamCompiler
from aiteams.plugins import PluginManager
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import pretty_json, trim_text

MODEL_TYPES = {"chat", "embedding", "rerank"}
DEFAULT_MEMORY_PLUGIN_KEY = "memory_core"
RETRIEVAL_SETTINGS_KEY = "retrieval_models"
AGENT_CENTER_UI_METADATA = {
    "review_policy": {
        "triggers": [
            {"value": "before_tool_call", "label": "before_tool_call / 工具调用前"},
            {"value": "before_external_side_effect", "label": "before_external_side_effect / 外部副作用前"},
            {"value": "before_memory_write", "label": "before_memory_write / 记忆写入前"},
            {"value": "before_agent_to_agent_message", "label": "before_agent_to_agent_message / Agent 消息前"},
            {"value": "before_handoff_to_lower_level", "label": "before_handoff_to_lower_level / 向下交接前"},
            {"value": "before_escalation_to_upper_level", "label": "before_escalation_to_upper_level / 向上升级前"},
            {"value": "before_final_delivery", "label": "before_final_delivery / 最终交付前"},
            {"value": "before_agent_receive_task", "label": "before_agent_receive_task / Agent 接任务前"},
            {"value": "before_task_ingress", "label": "before_task_ingress / 任务入站前"},
            {"value": "final_delivery", "label": "final_delivery / 交付消息"},
        ],
        "actions": [
            {"value": "approve", "label": "approve / 允许"},
            {"value": "reject", "label": "reject / 拒绝"},
            {"value": "edit_payload", "label": "edit_payload / 编辑载荷"},
            {"value": "edit_records", "label": "edit_records / 编辑记忆记录"},
            {"value": "reroute", "label": "reroute / 改路由"},
        ],
        "message_types": [
            {"value": "task", "label": "task / 任务"},
            {"value": "dialogue", "label": "dialogue / 对话"},
            {"value": "handoff", "label": "handoff / 交接"},
            {"value": "delivery", "label": "delivery / 交付"},
            {"value": "human_escalation", "label": "human_escalation / 人工介入"},
            {"value": "escalation", "label": "escalation / 升级"},
        ],
        "memory_scopes": [
            {"value": "agent", "label": "agent / Agent 私有"},
            {"value": "team", "label": "team / 团队共享"},
            {"value": "project", "label": "project / 项目共享"},
            {"value": "run", "label": "run / 运行回顾"},
            {"value": "working", "label": "working / 工作记忆"},
        ],
        "memory_kinds": [
            {"value": "summary", "label": "summary / 摘要"},
            {"value": "fact", "label": "fact / 事实"},
            {"value": "deliverable", "label": "deliverable / 交付物"},
            {"value": "risk", "label": "risk / 风险"},
            {"value": "next_focus", "label": "next_focus / 下一步焦点"},
            {"value": "team_message", "label": "team_message / 团队消息"},
            {"value": "human_escalation", "label": "human_escalation / 人工介入"},
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
    def __init__(self, store: MetadataStore, plugin_manager: PluginManager | None = None, gateway: AIGateway | None = None):
        self.store = store
        self.resolver = BuildResolver(store)
        self.team_compiler = LangGraphTeamCompiler(store)
        self.deep_team_compiler = DeepAgentsTeamCompiler(store)
        self.plugin_manager = plugin_manager
        self.gateway = gateway or AIGateway()

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

        plugin_id_map: dict[str, str] = {}
        for plugin in default_plugins():
            existing = self.store.get_plugin_by_key(str(plugin["key"]))
            saved = existing
            if saved is None:
                saved = self.store.save_plugin(
                    plugin_id=None,
                    key=str(plugin["key"]),
                    name=str(plugin["name"]),
                    version=str(plugin["version"]),
                    plugin_type=str(plugin["plugin_type"]),
                    description=str(plugin.get("description") or ""),
                    manifest=dict(plugin.get("manifest") or {}),
                    config={},
                    install_path=None,
                )
            elif str(plugin.get("key") or "") == DEFAULT_MEMORY_PLUGIN_KEY:
                manifest = dict(saved.get("manifest_json") or {})
                tools = {str(item) for item in list(manifest.get("tools") or []) if str(item).strip()}
                if "memory.background_reflection" not in tools:
                    merged_manifest = dict(manifest)
                    merged_manifest["tools"] = list(dict.fromkeys([*list(manifest.get("tools") or []), "memory.background_reflection"]))
                    if plugin.get("manifest", {}).get("description"):
                        merged_manifest["description"] = str(plugin["manifest"]["description"])
                    saved = self.store.save_plugin(
                        plugin_id=str(saved["id"]),
                        key=str(saved["key"]),
                        name=str(saved["name"]),
                        version=str(saved["version"]),
                        plugin_type=str(saved["plugin_type"]),
                        description=str(saved.get("description") or plugin.get("description") or ""),
                        manifest=merged_manifest,
                        config=dict(saved.get("config_json") or {}),
                        install_path=saved.get("install_path"),
                        status=str(saved.get("status") or "active"),
                    )
            plugin_id_map[str(plugin["key"])] = str(saved["id"])

        agent_template_id_map: dict[str, str] = {}
        for template in default_agent_templates():
            existing = self._find_default_agent_template(template)
            saved = existing
            if saved is None:
                saved = self.store.save_agent_template(
                    agent_template_id=None,
                    name=str(template["name"]),
                    role=str(template["role"]),
                    description=str(template.get("description") or ""),
                    version="v1",
                    spec=self._default_agent_template_spec(template, provider_id_map=provider_id_map, plugin_id_map=plugin_id_map),
                )
            agent_template_id_map[str(template["builtin_ref"])] = str(saved["id"])

        for team in default_team_templates():
            if self._find_default_team_template(team) is None:
                self.store.save_team_template(
                    team_template_id=None,
                    name=str(team["name"]),
                    description=str(team.get("description") or ""),
                    version="v1",
                    spec=self._default_team_template_spec(team, agent_template_id_map=agent_template_id_map),
                )

        for skill in default_skills():
            if self.store.get_skill_by_key(str(skill["key"])) is None:
                self.store.save_skill(
                    skill_id=None,
                    key=str(skill["key"]),
                    name=str(skill["name"]),
                    description=str(skill.get("description") or ""),
                    version=str(skill.get("version") or "v1"),
                    spec=dict(skill.get("spec") or {}),
                )

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

        memory_profile_id_map: dict[str, str] = {}
        for memory_profile in default_memory_profiles():
            existing = self.store.get_memory_profile_by_key(str(memory_profile["key"]))
            saved = existing
            if saved is None:
                saved = self.store.save_memory_profile(
                    memory_profile_id=None,
                    key=str(memory_profile["key"]),
                    name=str(memory_profile["name"]),
                    description=str(memory_profile.get("description") or ""),
                    version=str(memory_profile.get("version") or "v1"),
                    spec=dict(memory_profile.get("spec") or {}),
                )
            memory_profile_id_map[str(memory_profile["key"])] = str(saved["id"])

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
                        plugin_id_map=plugin_id_map,
                        memory_profile_id_map=memory_profile_id_map,
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

    def provider_types(self) -> list[dict[str, Any]]:
        return list_provider_presets()

    def ui_metadata(self) -> dict[str, Any]:
        return json.loads(json.dumps(AGENT_CENTER_UI_METADATA))

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

    def normalize_team_spec(self, spec: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(spec or {})
        if is_deepagents_team_spec(payload):
            normalized = json.loads(json.dumps(payload, ensure_ascii=False))
            normalized.setdefault("workspace_id", "local-workspace")
            normalized.setdefault("project_id", "default-project")
            return normalized
        return normalize_team_template_spec(spec)

    def validate_team_spec(self, spec: dict[str, Any] | None) -> dict[str, Any]:
        normalized = self.normalize_team_spec(spec)
        if is_deepagents_team_spec(normalized):
            pseudo_definition = {
                "id": "preview_deep_team_definition",
                "key": str(normalized.get("key") or "preview_deep_team"),
                "name": str(normalized.get("name") or "Preview Deep Team"),
                "description": "",
                "version": "draft",
                "spec_json": normalized,
            }
            try:
                compiled = self.deep_team_compiler.compile(pseudo_definition)
            except Exception as exc:
                return {
                    "valid": False,
                    "errors": [str(exc)],
                    "warnings": [],
                    "summary": {"hierarchy_mode": "strict_tree"},
                    "normalized_spec": normalized,
                    "communication": {},
                }
            return {
                "valid": True,
                "errors": [],
                "warnings": [],
                "summary": {
                    "agent_count": compiled.agent_count,
                    "team_count": compiled.team_count,
                    "node_count": len((compiled.blueprint.get("flow") or {}).get("nodes", [])),
                    "edge_count": len((compiled.blueprint.get("flow") or {}).get("edges", [])),
                    "execution_mode": (compiled.blueprint.get("metadata") or {}).get("execution_mode"),
                    "hierarchy_mode": "strict_tree",
                },
                "normalized_spec": normalized,
                "communication": {},
                "resource_lock": compiled.resource_lock,
                "hierarchy": compiled.root,
            }
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
        if is_deepagents_team_spec(normalized):
            if validation["errors"]:
                return {
                    "valid": False,
                    "errors": validation["errors"],
                    "warnings": validation["warnings"],
                    "summary": validation["summary"],
                }
            return {
                "valid": True,
                "errors": [],
                "warnings": validation["warnings"],
                "summary": validation["summary"],
                "preview": {
                    "agent_count": validation["summary"].get("agent_count", 0),
                    "team_count": validation["summary"].get("team_count", 0),
                    "node_count": validation["summary"].get("node_count", 0),
                    "edge_count": validation["summary"].get("edge_count", 0),
                    "execution_mode": validation["summary"].get("execution_mode"),
                    "hierarchy_mode": validation["summary"].get("hierarchy_mode", "strict_tree"),
                },
                "resource_lock": validation.get("resource_lock") or {},
                "hierarchy": validation.get("hierarchy"),
            }
        if validation["errors"]:
            return {
                "valid": False,
                "errors": validation["errors"],
                "warnings": validation["warnings"],
                "summary": validation["summary"],
            }
        pseudo_template = {
            "id": team_template_id or "preview_team_template",
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
        if is_deepagents_team_spec(team_template.get("spec_json") or {}):
            pseudo_definition = {
                "id": f"team_template::{team_template['id']}",
                "key": str((team_template.get("spec_json") or {}).get("key") or team_template["id"]),
                "name": str(team_template.get("name") or "Deep Team Template"),
                "description": str(team_template.get("description") or ""),
                "version": str(team_template.get("version") or "draft"),
                "spec_json": dict(team_template.get("spec_json") or {}),
            }
            compiled = self.deep_team_compiler.compile(pseudo_definition)
            spec = compiled.blueprint
            resource_lock = json.loads(json.dumps(compiled.resource_lock, ensure_ascii=False))
            resource_lock.setdefault("team_templates", [])
            template_lock = {"id": team_template["id"], "name": team_template["name"], "version": team_template.get("version")}
            if template_lock not in resource_lock["team_templates"]:
                resource_lock["team_templates"].append(template_lock)
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
            return self.store.save_blueprint_build(
                build_id=None,
                team_template_id=str(team_template["id"]),
                name=build_title,
                description=str(team_template.get("description") or ""),
                spec=spec,
                resource_lock=resource_lock,
                blueprint_id=str(blueprint["id"]),
            )
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
            name=build_title,
            description=str(team_template.get("description") or ""),
            spec=spec,
            resource_lock=resource_lock,
            blueprint_id=str(blueprint["id"]),
        )
        return build

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

    def _default_agent_template_spec(
        self,
        template: dict[str, Any],
        *,
        provider_id_map: dict[str, str],
        plugin_id_map: dict[str, str],
    ) -> dict[str, Any]:
        spec = json.loads(json.dumps(template.get("spec") or {}, ensure_ascii=False))
        provider_ref = str(spec.get("provider_ref") or "").strip()
        if provider_ref:
            spec["provider_ref"] = provider_id_map.get(provider_ref, provider_ref)
        spec["plugin_refs"] = [plugin_id_map.get(str(item), str(item)) for item in spec.get("plugin_refs", [])]
        metadata = dict(spec.get("metadata") or {})
        metadata["builtin_ref"] = str(template.get("builtin_ref") or "")
        spec["metadata"] = metadata
        return spec

    def _default_team_template_spec(self, team: dict[str, Any], *, agent_template_id_map: dict[str, str]) -> dict[str, Any]:
        spec = json.loads(json.dumps(team.get("spec") or {}, ensure_ascii=False))
        agents = []
        for item in spec.get("agents", []):
            agent = dict(item or {})
            reference = str(agent.get("agent_template_ref") or "").strip()
            if reference:
                agent["agent_template_ref"] = agent_template_id_map.get(reference, reference)
            agents.append(agent)
        spec["agents"] = agents
        metadata = dict(spec.get("metadata") or {})
        metadata["builtin_ref"] = str(team.get("builtin_ref") or "")
        spec["metadata"] = metadata
        return spec

    def _default_agent_definition_spec(
        self,
        definition: dict[str, Any],
        *,
        provider_id_map: dict[str, str],
        plugin_id_map: dict[str, str],
        memory_profile_id_map: dict[str, str],
    ) -> dict[str, Any]:
        spec = json.loads(json.dumps(definition.get("spec") or {}, ensure_ascii=False))
        provider_ref = str(spec.get("provider_ref") or "").strip()
        if provider_ref:
            spec["provider_ref"] = provider_id_map.get(provider_ref, provider_ref)
        spec["tool_plugin_refs"] = self._ensure_default_plugin_refs([plugin_id_map.get(str(item), str(item)) for item in spec.get("tool_plugin_refs", [])])
        memory_profile_ref = str(spec.get("memory_profile_ref") or "").strip()
        if memory_profile_ref:
            spec["memory_profile_ref"] = memory_profile_id_map.get(memory_profile_ref, memory_profile_ref)
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

    def _find_default_agent_template(self, template: dict[str, Any]) -> dict[str, Any] | None:
        builtin_ref = str(template.get("builtin_ref") or "").strip()
        role = str(template.get("role") or "").strip()
        name = str(template.get("name") or "").strip()
        for item in self.store.list_agent_templates():
            spec = dict(item.get("spec_json") or {})
            metadata = dict(spec.get("metadata") or {})
            if builtin_ref and str(metadata.get("builtin_ref") or "").strip() == builtin_ref:
                return item
            if str(item.get("role") or "").strip() == role and str(item.get("name") or "").strip() == name:
                return item
        return None

    def _find_default_team_template(self, team: dict[str, Any]) -> dict[str, Any] | None:
        builtin_ref = str(team.get("builtin_ref") or "").strip()
        name = str(team.get("name") or "").strip()
        for item in self.store.list_team_templates():
            spec = dict(item.get("spec_json") or {})
            metadata = dict(spec.get("metadata") or {})
            if builtin_ref and str(metadata.get("builtin_ref") or "").strip() == builtin_ref:
                return item
            if str(item.get("name") or "").strip() == name:
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

        def _resolve_model_selection(
            raw: dict[str, Any],
            *,
            kind: str,
            allowed_types: set[str],
        ) -> tuple[dict[str, Any], dict[str, Any]] | None:
            mode = str(raw.get("mode") or "").strip().lower()
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
