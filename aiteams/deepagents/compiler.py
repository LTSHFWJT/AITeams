from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aiteams.role_specs import normalize_role_spec, role_spec_system_prompt
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import slugify, trim_text


def is_deepagents_team_spec(spec: dict[str, Any] | None) -> bool:
    payload = dict(spec or {})
    return isinstance(payload.get("root"), dict) or isinstance(payload.get("lead"), dict)


@dataclass(slots=True)
class DeepAgentsCompiledTeamDefinition:
    team_definition: dict[str, Any]
    root: dict[str, Any]
    blueprint: dict[str, Any]
    resource_lock: dict[str, Any]
    agent_count: int
    team_count: int


class DeepAgentsTeamCompiler:
    def __init__(self, store: MetadataStore):
        self.store = store

    def handles(self, team_definition: dict[str, Any] | None) -> bool:
        if not team_definition:
            return False
        return is_deepagents_team_spec(team_definition.get("spec_json") or {})

    def compile(self, team_definition: dict[str, Any]) -> DeepAgentsCompiledTeamDefinition:
        spec = dict(team_definition.get("spec_json") or {})
        workspace_id = str(spec.get("workspace_id") or "local-workspace")
        project_id = str(spec.get("project_id") or "default-project")
        root_payload = self._root_payload(team_definition, spec)
        resource_lock = self._empty_resource_lock()
        root = self._resolve_team_node(
            payload=root_payload,
            workspace_id=workspace_id,
            project_id=project_id,
            resource_lock=resource_lock,
            team_path=(str(team_definition.get("key") or team_definition.get("id") or "team"),),
            visited_team_refs={f"definition:{str(team_definition.get('id') or team_definition.get('key') or 'root')}"},
        )
        agents = self._flatten_agents(root)
        role_templates, blueprint_agents, workbenches = self._blueprint_agent_payloads(agents)
        blueprint = {
            "name": slugify(str(team_definition.get("name") or team_definition.get("key") or "deepagents_team"), fallback="deepagents_team"),
            "description": str(team_definition.get("description") or ""),
            "workspace_id": workspace_id,
            "project_id": project_id,
            "version": str(team_definition.get("version") or "v1"),
            "role_templates": role_templates,
            "agents": blueprint_agents,
            "workbenches": workbenches,
            "flow": self._minimal_flow(root),
            "definition_of_done": ["finalize.path != null"],
            "acceptance_checks": [],
            "metadata": {
                "runtime": "deepagents",
                "execution_mode": "deepagents_hierarchy",
                "team_definition_id": team_definition.get("id"),
                "team_definition_key": team_definition.get("key"),
                "resource_lock": resource_lock,
                "deepagents_runtime": {
                    "team_definition_id": team_definition.get("id"),
                    "team_definition_key": team_definition.get("key"),
                    "team_definition_name": team_definition.get("name"),
                    "workspace_id": workspace_id,
                    "project_id": project_id,
                    "root": root,
                },
            },
        }
        return DeepAgentsCompiledTeamDefinition(
            team_definition=team_definition,
            root=root,
            blueprint=blueprint,
            resource_lock=resource_lock,
            agent_count=len(agents),
            team_count=self._count_teams(root),
        )

    def _root_payload(self, team_definition: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
        if isinstance(spec.get("root"), dict):
            return dict(spec["root"])
        lead = dict(spec.get("lead") or {})
        if not lead:
            raise ValueError("DeepAgents team definition requires a root lead.")
        return {
            "kind": "team",
            "key": str(spec.get("key") or team_definition.get("key") or team_definition.get("id") or "team"),
            "name": str(spec.get("name") or team_definition.get("name") or "Team"),
            "description": str(spec.get("description") or team_definition.get("description") or ""),
            "lead": lead,
            "children": [dict(item or {}) for item in list(spec.get("children") or [])],
        }

    def _resolve_team_node(
        self,
        *,
        payload: dict[str, Any],
        workspace_id: str,
        project_id: str,
        resource_lock: dict[str, list[dict[str, Any]]],
        team_path: tuple[str, ...],
        visited_team_refs: set[str],
    ) -> dict[str, Any]:
        resolved_payload = self._resolve_team_reference(
            payload,
            workspace_id=workspace_id,
            project_id=project_id,
            resource_lock=resource_lock,
            visited_team_refs=visited_team_refs,
        )
        team_key = str(resolved_payload.get("key") or team_path[-1] or "team")
        team_runtime_key = self._runtime_key(*team_path, team_key)
        team_role_spec = self._resolve_role_spec(
            resolved_payload.get("role_spec_ref")
            or resolved_payload.get("role_spec_id")
            or resolved_payload.get("static_memory_ref")
            or resolved_payload.get("static_memory_id"),
            resource_lock=resource_lock,
        )
        team_name = str((team_role_spec or {}).get("name") or resolved_payload.get("name") or team_key)
        team_description = str((team_role_spec or {}).get("description") or resolved_payload.get("description") or "")
        children: list[dict[str, Any]] = []
        child_keys: set[str] = set()
        for index, child_payload in enumerate(list(resolved_payload.get("children") or []), start=1):
            child = dict(child_payload or {})
            child_kind = str(child.get("kind") or "agent").strip().lower()
            child_key = str(child.get("key") or f"child_{index}")
            if child_key in child_keys:
                raise ValueError(f"Duplicate child key `{child_key}` under team `{team_key}`.")
            child_keys.add(child_key)
            if child_kind == "team":
                children.append(
                    self._resolve_team_node(
                        payload=child,
                        workspace_id=workspace_id,
                        project_id=project_id,
                        resource_lock=resource_lock,
                        team_path=team_path + (team_key,),
                        visited_team_refs=set(visited_team_refs),
                    )
                )
                continue
            children.append(
                self._resolve_agent_node(
                    payload=child,
                    workspace_id=workspace_id,
                    project_id=project_id,
                    resource_lock=resource_lock,
                    team_path=team_path + (team_key,),
                    role_hint="child",
                )
            )
        lead_payload = dict(resolved_payload.get("lead") or {})
        if not lead_payload:
            raise ValueError(f"Team `{team_key}` requires a lead agent.")
        child_names = [str(item.get("name") or item.get("delegate_name") or item.get("runtime_key") or item.get("key") or "") for item in children]
        lead = self._resolve_agent_node(
            payload=lead_payload,
            workspace_id=workspace_id,
            project_id=project_id,
            resource_lock=resource_lock,
            team_path=team_path + (team_key,),
            role_hint="lead",
            child_names=child_names,
        )
        team_system_prompt = self._team_system_prompt(
            team_name=team_name,
            role_spec=team_role_spec,
            lead_system_prompt=str(lead.get("system_prompt") or ""),
        )
        return {
            "node_type": "team",
            "key": team_key,
            "runtime_key": team_runtime_key,
            "delegate_name": team_runtime_key,
            "name": team_name,
            "description": team_description,
            "system_prompt": team_system_prompt,
            "role_spec": team_role_spec,
            "lead": lead,
            "children": children,
        }

    def _resolve_team_reference(
        self,
        payload: dict[str, Any],
        *,
        workspace_id: str,
        project_id: str,
        resource_lock: dict[str, list[dict[str, Any]]],
        visited_team_refs: set[str],
    ) -> dict[str, Any]:
        definition_reference = payload.get("team_definition_ref") or payload.get("team_definition_id")
        legacy_template_keys = tuple(key for key in payload if key.startswith("team_template_") and payload.get(key) is not None)
        if legacy_template_keys:
            raise ValueError("Nested team templates are no longer supported; use team_definition_ref instead.")
        if definition_reference is None:
            return dict(payload)
        reference = definition_reference
        source_kind = "definition"
        source_record = self.store.get_team_definition(str(reference)) or self.store.get_team_definition_by_key(str(reference))
        if source_record is None:
            raise ValueError(f"Unknown team definition `{reference}`.")
        source_id = str(source_record.get("id") or source_record.get("key") or reference)
        visit_key = f"definition:{source_id}"
        if visit_key in visited_team_refs:
            raise ValueError(f"Nested team definition cycle detected at `{reference}`.")
        visited_team_refs.add(visit_key)
        self._lock_resource(resource_lock, "team_definitions", source_record, fields=("id", "key", "version"))
        source_spec = dict(source_record.get("spec_json") or {})
        if not is_deepagents_team_spec(source_spec):
            raise ValueError(f"Nested team {source_kind} `{reference}` is not a deepagents hierarchy spec.")
        base = self._root_payload(source_record, source_spec)
        merged = dict(base)
        merged.update(
            {
                key: value
                for key, value in payload.items()
                if key not in {"team_definition_ref", "team_definition_id", *legacy_template_keys}
            }
        )
        if "lead" not in payload and "lead" in base:
            merged["lead"] = dict(base["lead"])
        if "children" not in payload and "children" in base:
            merged["children"] = [dict(item or {}) for item in list(base.get("children") or [])]
        merged.setdefault("workspace_id", workspace_id)
        merged.setdefault("project_id", project_id)
        return merged

    def _resolve_agent_node(
        self,
        *,
        payload: dict[str, Any],
        workspace_id: str,
        project_id: str,
        resource_lock: dict[str, list[dict[str, Any]]],
        team_path: tuple[str, ...],
        role_hint: str,
        child_names: list[str] | None = None,
    ) -> dict[str, Any]:
        source = self._resolve_agent_source(payload, resource_lock=resource_lock)
        provider = self._resolve_provider(
            payload.get("provider_ref")
            or payload.get("provider_profile_id")
            or source["provider_ref"]
        )
        self._lock_resource(resource_lock, "provider_profiles", provider, fields=("id", "name", "provider_type"))
        source_kind = str(source.get("source_kind") or "agent_definition")
        self._lock_resource(resource_lock, "agent_definitions", source["record"], fields=("id", "version"))

        plugin_refs = self._resolve_plugins(
            [*list(source.get("plugin_refs") or []), *list(payload.get("plugin_refs") or [])],
            resource_lock=resource_lock,
        )
        skills = self._resolve_skills(
            refs=list(source.get("skill_refs") or []) + list(payload.get("skill_refs") or []),
            inline=list(source.get("inline_skills") or []) + list(payload.get("skills") or []),
            resource_lock=resource_lock,
        )
        knowledge_bases = self._resolve_knowledge_bases(
            list(source.get("knowledge_base_refs") or []) + list(payload.get("knowledge_base_refs") or []),
            resource_lock=resource_lock,
        )
        review_policies = self._resolve_review_policies(
            list(source.get("review_policy_refs") or []) + list(payload.get("review_policy_refs") or []),
            resource_lock=resource_lock,
        )
        role_spec = self._resolve_role_spec(
            payload.get("role_spec_ref")
            or payload.get("role_spec_id")
            or payload.get("static_memory_ref")
            or payload.get("static_memory_id")
            or source.get("role_spec_ref")
            or source.get("static_memory_ref"),
            resource_lock=resource_lock,
        )
        runtime_key = self._runtime_key(*team_path, str(payload.get("key") or source.get("key") or source.get("name") or source["record"].get("id") or "agent"))
        role = str(payload.get("role_identity") or payload.get("role") or source.get("role") or role_hint or "agent")
        model = str(payload.get("model") or source.get("model") or (provider.get("config_json") or {}).get("model") or "mock-model")
        instructions = str(payload.get("system_prompt_override") or payload.get("instructions") or source.get("instructions") or "")
        goal = str(payload.get("goal") or source.get("goal") or "")
        resolved_child_names = [str(item) for item in list(child_names or []) if str(item).strip()]
        if not resolved_child_names:
            resolved_child_names = [
                str(item.get("delegate_name") or item.get("runtime_key") or item.get("key") or "")
                for item in list(payload.get("children") or [])
                if isinstance(item, dict)
            ]
        system_prompt = self._system_prompt(
            name=str((role_spec or {}).get("name") or payload.get("name") or source.get("name") or source["record"].get("name") or runtime_key),
            role=role,
            goal=goal,
            instructions=instructions,
            role_spec=role_spec,
            skills=skills,
            knowledge_bases=knowledge_bases,
            plugins=plugin_refs,
            child_names=resolved_child_names,
            is_team_lead=role_hint == "lead",
        )
        return {
            "node_type": "agent",
            "key": str(payload.get("key") or source.get("key") or runtime_key),
            "runtime_key": runtime_key,
            "delegate_name": runtime_key,
            "name": str((role_spec or {}).get("name") or payload.get("name") or source.get("name") or source["record"].get("name") or runtime_key),
            "description": str(payload.get("description") or source.get("description") or source["record"].get("description") or (role_spec or {}).get("description") or ""),
            "role": role,
            "goal": goal,
            "instructions": instructions,
            "system_prompt": system_prompt,
            "workspace_id": workspace_id,
            "project_id": project_id,
            "provider": {
                "id": provider.get("id"),
                "name": provider.get("name"),
                "provider_type": provider.get("provider_type"),
                "config": dict(provider.get("config_json") or {}),
                "secret": dict(provider.get("secret_json") or {}),
            },
            "model": model,
            "skills": skills,
            "role_spec": role_spec,
            "knowledge_bases": knowledge_bases,
            "plugins": plugin_refs,
            "review_policies": review_policies,
            "source": {
                "kind": source_kind,
                "id": source["record"].get("id"),
                "name": source["record"].get("name"),
            },
        }

    def _resolve_agent_source(self, payload: dict[str, Any], *, resource_lock: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        agent_definition_ref = payload.get("agent_definition_ref") or payload.get("agent_definition_id")
        if agent_definition_ref is None:
            raise ValueError(f"Agent node `{payload.get('key') or 'unknown'}` requires agent_definition_ref.")
        record = self.store.get_agent_definition(str(agent_definition_ref))
        if record is None:
            raise ValueError(f"Unknown agent definition `{agent_definition_ref}`.")
        spec = dict(record.get("spec_json") or {})
        return {
            "source_kind": "agent_definition",
            "record": record,
            "name": record.get("name"),
            "description": record.get("description"),
            "role": record.get("role"),
            "goal": spec.get("goal"),
            "instructions": spec.get("instructions"),
            "provider_ref": spec.get("provider_ref") or spec.get("provider_profile_id"),
            "plugin_refs": list(spec.get("tool_plugin_refs") or spec.get("plugin_refs") or []),
            "skill_refs": list(spec.get("skill_refs") or []),
            "inline_skills": list(spec.get("skills") or []),
            "static_memory_ref": spec.get("role_spec_ref") or spec.get("role_spec_id") or spec.get("static_memory_ref") or spec.get("static_memory_id"),
            "knowledge_base_refs": list(spec.get("knowledge_base_refs") or []),
            "review_policy_refs": list(spec.get("review_policy_refs") or []),
            "model": spec.get("model"),
        }

    def _resolve_provider(self, reference: Any) -> dict[str, Any]:
        provider = self.store.get_provider_profile(str(reference), include_secret=True)
        if provider is not None:
            return provider
        for item in self.store.list_provider_profiles(include_secret=True):
            if str(item.get("key") or "") == str(reference):
                return item
            if str((item.get("config_json") or {}).get("builtin_ref") or "") == str(reference):
                return item
            if str(item.get("name") or "") == str(reference):
                return item
        raise ValueError(f"Unknown provider profile `{reference}`.")

    def _resolve_plugins(self, refs: list[Any], *, resource_lock: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ref in refs:
            value = str(ref or "").strip()
            if not value:
                continue
            plugin = self.store.get_plugin(value) or self.store.get_plugin_by_key(value)
            if plugin is None:
                raise ValueError(f"Unknown plugin `{ref}`.")
            plugin_id = str(plugin.get("id") or value)
            if plugin_id in seen:
                continue
            seen.add(plugin_id)
            resolved.append(plugin)
            self._lock_resource(resource_lock, "plugins", plugin, fields=("id", "key", "version"))
        return resolved

    def _resolve_skills(
        self,
        *,
        refs: list[Any],
        inline: list[Any],
        resource_lock: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ref in refs:
            value = str(ref or "").strip()
            if not value:
                continue
            skill = self.store.get_skill(value) or self.store.get_skill_by_name(value) or self.store.get_skill_by_key(value)
            if skill is None:
                raise ValueError(f"Unknown skill `{ref}`.")
            identity = str(skill.get("id") or value)
            if identity in seen:
                continue
            seen.add(identity)
            resolved.append(
                {
                    "id": skill.get("id"),
                    "name": skill.get("name"),
                    "description": skill.get("description"),
                    "storage_path": skill.get("storage_path"),
                    "source": "catalog",
                }
            )
            self._lock_resource(resource_lock, "skills", skill, fields=("id", "name", "storage_path"))
        for item in inline:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            resolved.append({"id": None, "name": text, "description": text, "instructions": [text], "source": "inline"})
        return resolved

    def _resolve_knowledge_bases(self, refs: list[Any], *, resource_lock: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ref in refs:
            value = str(ref or "").strip()
            if not value:
                continue
            kb = self.store.get_knowledge_base(value) or self.store.get_knowledge_base_by_key(value)
            if kb is None:
                raise ValueError(f"Unknown knowledge base `{ref}`.")
            identity = str(kb.get("id") or value)
            if identity in seen:
                continue
            seen.add(identity)
            resolved.append(
                {
                    "id": kb.get("id"),
                    "key": kb.get("key"),
                    "name": kb.get("name"),
                    "description": kb.get("description"),
                    "config": dict(kb.get("config_json") or {}),
                }
            )
            self._lock_resource(resource_lock, "knowledge_bases", kb, fields=("id", "key"))
        return resolved

    def _resolve_role_spec(
        self,
        reference: Any,
        *,
        resource_lock: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any] | None:
        if not reference:
            return None
        record = self.store.get_static_memory(str(reference)) or self.store.get_static_memory_by_key(str(reference))
        if record is None:
            raise ValueError(f"Unknown role spec `{reference}`.")
        self._lock_resource(resource_lock, "static_memories", record, fields=("id", "key", "version"))
        return {
            "id": record.get("id"),
            "key": record.get("key"),
            "name": record.get("name"),
            "description": record.get("description"),
            "version": record.get("version"),
            "spec": normalize_role_spec(dict(record.get("spec_json") or {})),
        }

    def _resolve_review_policies(
        self,
        refs: list[Any],
        *,
        resource_lock: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ref in refs:
            value = str(ref or "").strip()
            if not value:
                continue
            policy = self.store.get_review_policy(value) or self.store.get_review_policy_by_key(value)
            if policy is None:
                raise ValueError(f"Unknown review policy `{ref}`.")
            identity = str(policy.get("id") or value)
            if identity in seen:
                continue
            seen.add(identity)
            resolved.append(
                {
                    "id": policy.get("id"),
                    "key": policy.get("key"),
                    "name": policy.get("name"),
                    "description": policy.get("description"),
                    "config": dict(policy.get("spec_json") or {}),
                }
            )
            self._lock_resource(resource_lock, "review_policies", policy, fields=("id", "key", "version"))
        return resolved

    def _system_prompt(
        self,
        *,
        name: str,
        role: str,
        goal: str,
        instructions: str,
        role_spec: dict[str, Any] | None,
        skills: list[dict[str, Any]],
        knowledge_bases: list[dict[str, Any]],
        plugins: list[dict[str, Any]],
        child_names: list[str],
        is_team_lead: bool,
    ) -> str:
        lines = [
            f"You are `{name}`.",
            f"Role: {role}.",
        ]
        if role_spec is not None:
            role_spec_name = str(role_spec.get("name") or role_spec.get("key") or "role_spec")
            lines.append(f"Bound role spec: {role_spec_name}.")
            if str(role_spec.get("description") or "").strip():
                lines.append(f"Role spec description: {role_spec.get('description')}.")
            prompt = role_spec_system_prompt(dict(role_spec.get("spec") or {}))
            if prompt:
                lines.append("Role spec system prompt:")
                lines.append(prompt)
        if goal:
            lines.append(f"Goal: {goal}.")
        if instructions:
            lines.append(instructions)
        if skills:
            lines.append("Skills:")
            for skill in skills:
                skill_text = str(skill.get("description") or "").strip()
                if not skill_text:
                    skill_text = " ".join(str(item) for item in list(skill.get("instructions") or []) if str(item).strip())
                lines.append(f"- {skill.get('name') or skill.get('id')}: {skill_text or skill.get('name')}")
        if knowledge_bases:
            lines.append("Bound knowledge bases:")
            for item in knowledge_bases:
                lines.append(f"- {item.get('name') or item.get('key')}")
        if plugins:
            lines.append("Bound plugins:")
            for plugin in plugins:
                lines.append(f"- {plugin.get('name') or plugin.get('key')}")
        if child_names:
            lines.append("You may delegate only to these direct child subagents via the task tool:")
            for child_name in child_names:
                lines.append(f"- {child_name}")
            lines.append("Never delegate to siblings, cousins, or hidden descendants.")
            lines.append("Never use any general-purpose subagent for hierarchical routing.")
        elif is_team_lead:
            lines.append("You currently have no child subagents. Solve the task directly.")
        else:
            lines.append("You are a leaf agent. Solve the delegated task directly and return a concise result.")
        return "\n".join(lines)

    def _team_system_prompt(
        self,
        *,
        team_name: str,
        role_spec: dict[str, Any] | None,
        lead_system_prompt: str,
    ) -> str:
        blocks: list[str] = []
        if role_spec is not None:
            blocks.append(f"Team name: {team_name}.")
            if str(role_spec.get("description") or "").strip():
                blocks.append(f"Team description: {role_spec.get('description')}.")
            prompt = role_spec_system_prompt(dict(role_spec.get("spec") or {}))
            if prompt:
                blocks.append(prompt)
        if lead_system_prompt:
            blocks.append(lead_system_prompt)
        return "\n\n".join(item for item in blocks if str(item).strip())

    def _flatten_agents(self, node: dict[str, Any]) -> list[dict[str, Any]]:
        agents: list[dict[str, Any]] = []
        if node.get("node_type") == "team":
            agents.append(dict(node["lead"]))
            for child in list(node.get("children") or []):
                agents.extend(self._flatten_agents(dict(child)))
            return agents
        agents.append(dict(node))
        return agents

    def _blueprint_agent_payloads(self, agents: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        role_templates: dict[str, Any] = {}
        blueprint_agents: dict[str, Any] = {}
        workbenches: dict[str, Any] = {}
        for agent in agents:
            runtime_key = str(agent["runtime_key"])
            role_template_key = f"role_{slugify(runtime_key, fallback='agent')}"
            provider = dict(agent.get("provider") or {})
            provider_config = dict(provider.get("config") or {})
            role_templates[role_template_key] = {
                "name": agent.get("name"),
                "role": agent.get("role"),
                "goal": agent.get("goal"),
                "instructions": agent.get("system_prompt"),
                "backend": str(provider_config.get("backend") or provider.get("provider_type") or "mock"),
                "provider_type": provider.get("provider_type"),
                "model": agent.get("model"),
                "base_url": provider_config.get("base_url"),
                "api_key": dict(provider.get("secret") or {}).get("api_key"),
                "api_key_env": provider_config.get("api_key_env"),
                "api_version": provider_config.get("api_version"),
                "organization": provider_config.get("organization"),
                "temperature": float(provider_config.get("temperature", 0.2) or 0.2),
                "max_tokens": provider_config.get("max_tokens"),
                "workbenches": self._workbench_keys(agent, workbenches),
                "memory_policy": "agent_private_plus_project",
                "metadata": {
                    "skills": [item.get("name") or item.get("id") for item in list(agent.get("skills") or [])],
                    "role_spec": dict(agent.get("role_spec") or {}) if agent.get("role_spec") else None,
                    "knowledge_bases": [dict(item) for item in list(agent.get("knowledge_bases") or [])],
                    "plugins": [self._plugin_payload(item) for item in list(agent.get("plugins") or [])],
                    "review_policies": [dict(item) for item in list(agent.get("review_policies") or [])],
                },
            }
            blueprint_agents[runtime_key] = {
                "name": agent.get("name"),
                "role": agent.get("role"),
                "role_template": role_template_key,
                "metadata": {
                    "runtime_key": runtime_key,
                    "source": dict(agent.get("source") or {}),
                    "role_spec": dict(agent.get("role_spec") or {}) if agent.get("role_spec") else None,
                },
            }
        return role_templates, blueprint_agents, workbenches

    def _workbench_keys(self, agent: dict[str, Any], workbenches: dict[str, Any]) -> list[str]:
        keys: list[str] = []
        for plugin in list(agent.get("plugins") or []):
            manifest = dict(plugin.get("manifest_json") or plugin.get("manifest") or {})
            workbench_key = slugify(str(manifest.get("workbench_key") or plugin.get("key") or plugin.get("id") or "plugin"), fallback="plugin")
            if workbench_key not in workbenches:
                workbenches[workbench_key] = {
                    "name": str(plugin.get("name") or workbench_key),
                    "tools": [str(item) for item in list(manifest.get("tools") or [])],
                    "permissions": [str(item) for item in list(manifest.get("permissions") or [])],
                    "description": str(manifest.get("description") or plugin.get("description") or ""),
                }
            if workbench_key not in keys:
                keys.append(workbench_key)
        return keys

    def _plugin_payload(self, plugin: dict[str, Any]) -> dict[str, Any]:
        manifest = dict(plugin.get("manifest_json") or plugin.get("manifest") or {})
        return {
            "id": plugin.get("id"),
            "key": plugin.get("key"),
            "name": plugin.get("name"),
            "version": plugin.get("version"),
            "install_path": plugin.get("install_path"),
            "manifest": manifest,
        }

    def _minimal_flow(self, root: dict[str, Any]) -> dict[str, Any]:
        root_agent = dict(root.get("lead") or {})
        return {
            "nodes": [
                {"id": "start", "type": "start"},
                {
                    "id": "orchestrate",
                    "type": "agent",
                    "agent": root_agent.get("runtime_key"),
                    "instruction": "Execute the hierarchical deepagents team and return the final delivery summary.",
                },
                {
                    "id": "finalize",
                    "type": "artifact",
                    "name": "team-summary.md",
                    "artifact_kind": "report",
                    "template": "# Team Summary\n\n{{orchestrate.summary}}",
                },
                {"id": "end", "type": "end"},
            ],
            "edges": [
                {"from": "start", "to": "orchestrate"},
                {"from": "orchestrate", "to": "finalize"},
                {"from": "finalize", "to": "end"},
            ],
        }

    def _count_teams(self, node: dict[str, Any]) -> int:
        if node.get("node_type") != "team":
            return 0
        return 1 + sum(self._count_teams(dict(child)) for child in list(node.get("children") or []))

    def _empty_resource_lock(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "team_definitions": [],
            "agent_definitions": [],
            "provider_profiles": [],
            "plugins": [],
            "skills": [],
            "static_memories": [],
            "knowledge_bases": [],
            "review_policies": [],
        }

    def _lock_resource(
        self,
        resource_lock: dict[str, list[dict[str, Any]]],
        bucket: str,
        payload: dict[str, Any],
        *,
        fields: tuple[str, ...],
    ) -> None:
        existing = {str(item.get("id") or item.get("key") or item.get("name") or "") for item in resource_lock[bucket]}
        identity = str(payload.get("id") or payload.get("key") or payload.get("name") or "")
        if not identity or identity in existing:
            return
        resource_lock[bucket].append({field: payload.get(field) for field in fields})

    def _runtime_key(self, *parts: str) -> str:
        joined = "__".join(slugify(part, fallback="node") for part in parts if str(part).strip())
        return trim_text(joined or "node", limit=240)
