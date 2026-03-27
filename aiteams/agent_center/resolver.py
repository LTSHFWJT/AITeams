from __future__ import annotations

from typing import Any

from aiteams.catalog import preset_for
from aiteams.domain.models import BlueprintSpec
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import slugify

DEFAULT_MEMORY_PLUGIN_KEY = "memory_core"


class BuildResolver:
    def __init__(self, store: MetadataStore):
        self.store = store

    def build(self, team_template: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        team_spec = dict(team_template.get("spec_json") or {})
        name = str(team_template.get("name") or team_spec.get("name") or "team_build")
        description = str(team_template.get("description") or team_spec.get("description") or "")
        workspace_id = str(team_spec.get("workspace_id") or "local-workspace")
        project_id = str(team_spec.get("project_id") or "default-project")
        members = self._normalize_members(team_spec.get("agents") or [])

        role_templates: dict[str, Any] = {}
        agents: dict[str, Any] = {}
        workbenches: dict[str, Any] = {}
        locked_templates: list[dict[str, Any]] = []
        locked_providers: list[dict[str, Any]] = []
        locked_plugins: list[dict[str, Any]] = []

        for member in members:
            agent_key = str(member["key"])
            template = self._resolve_agent_template(member)
            template_spec = dict(template.get("spec_json") or {})
            provider = self._resolve_provider(member, template_spec)
            provider_config = dict(provider.get("config_json") or {})
            secret = dict(provider.get("secret_json") or {})
            plugins = self._resolve_plugins(member, template_spec)

            role_template_key = f"role_{slugify(agent_key, fallback='agent')}"
            agent_workbenches: list[str] = []
            locked_plugin_entries: list[dict[str, Any]] = []
            for plugin in plugins:
                manifest = dict(plugin.get("manifest_json") or {})
                workbench_key = slugify(str(manifest.get("workbench_key") or plugin.get("key") or plugin.get("id") or "plugin"), fallback="plugin")
                if workbench_key not in workbenches:
                    workbenches[workbench_key] = {
                        "name": str(plugin.get("name") or workbench_key),
                        "tools": [str(item) for item in manifest.get("tools", [])],
                        "permissions": [str(item) for item in manifest.get("permissions", [])],
                        "description": str(manifest.get("description") or plugin.get("description") or ""),
                    }
                if workbench_key not in agent_workbenches:
                    agent_workbenches.append(workbench_key)
                locked_entry = {
                    "id": plugin["id"],
                    "key": plugin["key"],
                    "version": plugin["version"],
                    "install_path": plugin.get("install_path"),
                    "manifest": manifest,
                }
                locked_plugins.append(locked_entry)
                locked_plugin_entries.append(locked_entry)

            provider_type = str(provider.get("provider_type") or template_spec.get("provider_type") or "mock")
            preset = preset_for(provider_type)
            model = str(
                member.get("model")
                or template_spec.get("model")
                or provider_config.get("model")
                or preset.get("default_model")
                or "mock-model"
            )
            role_templates[role_template_key] = {
                "name": str(member.get("name") or template.get("name") or agent_key),
                "role": str(member.get("role") or template.get("role") or template_spec.get("role") or agent_key),
                "goal": str(member.get("goal") or template_spec.get("goal") or ""),
                "instructions": str(member.get("instructions") or template_spec.get("instructions") or ""),
                "backend": str(provider_config.get("backend") or provider_type),
                "provider_type": provider_type,
                "model": model,
                "base_url": provider_config.get("base_url"),
                "api_key": secret.get("api_key"),
                "api_key_env": provider_config.get("api_key_env"),
                "api_version": provider_config.get("api_version"),
                "organization": provider_config.get("organization"),
                "temperature": float(member.get("temperature", template_spec.get("temperature", provider_config.get("temperature", 0.2)))),
                "max_tokens": member.get("max_tokens", template_spec.get("max_tokens", provider_config.get("max_tokens"))),
                "workbenches": agent_workbenches,
                "memory_policy": str(member.get("memory_policy") or template_spec.get("memory_policy") or "agent_private"),
                "extra_headers": dict(provider_config.get("extra_headers") or {}),
                "extra_config": dict(provider_config.get("extra_config") or {}),
                "metadata": {
                    "agent_template_id": template["id"],
                    "agent_template_name": template["name"],
                    "provider_profile_id": provider["id"],
                    "provider_profile_name": provider["name"],
                    "skills": list(template_spec.get("skills", [])),
                    "plugin_ids": [item["id"] for item in plugins],
                    "plugins": locked_plugin_entries,
                },
            }
            agents[agent_key] = {
                "name": str(member.get("name") or template.get("name") or agent_key),
                "role": str(member.get("role") or template.get("role") or template_spec.get("role") or agent_key),
                "role_template": role_template_key,
                "metadata": {
                    "agent_template_id": template["id"],
                    "agent_template_name": template["name"],
                    "provider_profile_id": provider["id"],
                    "provider_profile_name": provider["name"],
                    "plugin_ids": [item["id"] for item in plugins],
                    "plugins": locked_plugin_entries,
                },
            }
            locked_templates.append({"id": template["id"], "name": template["name"], "version": template["version"]})
            locked_providers.append(
                {
                    "id": provider["id"],
                    "name": provider["name"],
                    "provider_type": provider["provider_type"],
                }
            )

        blueprint_payload = {
            "name": slugify(name, fallback="team_build"),
            "description": description,
            "workspace_id": workspace_id,
            "project_id": project_id,
            "version": "v1",
            "role_templates": role_templates,
            "agents": agents,
            "workbenches": workbenches,
            "flow": dict(team_spec.get("flow") or {}),
            "definition_of_done": [str(item) for item in team_spec.get("definition_of_done", [])],
            "acceptance_checks": [str(item) for item in team_spec.get("acceptance_checks", [])],
            "metadata": {
                "team_template_id": team_template["id"],
                "team_template_name": team_template["name"],
                "communication_policy": str((team_spec.get("metadata") or {}).get("communication_policy") or "graph-ancestor-scoped"),
                "resource_lock": {
                    "team_template": {"id": team_template["id"], "name": team_template["name"], "version": team_template["version"]},
                    "agent_templates": self._unique_locked(locked_templates, "id"),
                    "provider_profiles": self._unique_locked(locked_providers, "id"),
                    "plugins": self._unique_locked(locked_plugins, "id"),
                },
            },
        }
        spec = BlueprintSpec.from_dict(blueprint_payload).to_dict()
        resource_lock = dict(spec.get("metadata", {}).get("resource_lock") or {})
        return spec, resource_lock

    def _normalize_members(self, payload: list[dict[str, Any]] | dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            items: list[dict[str, Any]] = []
            for key, value in payload.items():
                record = dict(value or {})
                record.setdefault("key", key)
                items.append(record)
            return items
        return [dict(item or {}) for item in payload]

    def _resolve_agent_template(self, member: dict[str, Any]) -> dict[str, Any]:
        reference = member.get("agent_template_ref") or member.get("agent_template_id")
        if reference is None:
            raise ValueError(f"Agent member `{member.get('key')}` requires agent_template_ref.")
        template = self.store.get_agent_template(str(reference))
        if template is None:
            raise ValueError(f"Unknown agent template `{reference}`.")
        return template

    def _resolve_provider(self, member: dict[str, Any], template_spec: dict[str, Any]) -> dict[str, Any]:
        reference = (
            member.get("provider_ref")
            or member.get("provider_profile_id")
            or template_spec.get("provider_ref")
            or template_spec.get("provider_profile_id")
        )
        if reference is None:
            raise ValueError(f"Agent `{member.get('key')}` requires provider_ref.")
        provider = self.store.get_provider_profile(str(reference), include_secret=True)
        if provider is None:
            raise ValueError(f"Unknown provider profile `{reference}`.")
        return provider

    def _resolve_plugins(self, member: dict[str, Any], template_spec: dict[str, Any]) -> list[dict[str, Any]]:
        references = list(template_spec.get("plugin_refs", []))
        references.extend(list(member.get("plugin_refs", [])))
        memory_plugin = self.store.get_plugin_by_key(DEFAULT_MEMORY_PLUGIN_KEY)
        memory_reference = str(memory_plugin["id"]) if memory_plugin is not None else DEFAULT_MEMORY_PLUGIN_KEY
        if not any(str(item or "").strip() in {DEFAULT_MEMORY_PLUGIN_KEY, memory_reference} for item in references):
            references.insert(0, memory_reference)
        resolved: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ref in references:
            if ref is None:
                continue
            plugin = self.store.get_plugin(str(ref))
            if plugin is None:
                plugin = self.store.get_plugin_by_key(str(ref))
            if plugin is None:
                raise ValueError(f"Unknown plugin `{ref}`.")
            if str(plugin["id"]) in seen:
                continue
            seen.add(str(plugin["id"]))
            resolved.append(plugin)
        return resolved

    def _unique_locked(self, items: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for item in items:
            key = str(item.get(field) or "")
            if not key or key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique
