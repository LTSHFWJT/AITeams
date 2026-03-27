from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aiteams.catalog import preset_for
from aiteams.langgraph.router import build_adjacency_map
from aiteams.langgraph.state import MemoryProfileRuntimeSpec, TeamMemberRuntimeSpec
from aiteams.role_specs import normalize_role_spec, role_spec_system_prompt
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import slugify

DEFAULT_MEMORY_PLUGIN_KEY = "memory_core"


@dataclass(slots=True)
class CompiledTeamDefinition:
    team_definition: dict[str, Any]
    members: list[TeamMemberRuntimeSpec]
    team_shared_knowledge_bases: list[dict[str, Any]]
    team_shared_static_memories: list[dict[str, Any]]
    ordered_levels: list[int]
    adjacency: dict[str, list[str]]
    entry_agent_id: str
    finish_agent_ids: list[str]
    team_review_policies: list[dict[str, Any]]
    review_overrides: list[dict[str, Any]]
    blueprint: dict[str, Any]
    resource_lock: dict[str, Any]


class LangGraphTeamCompiler:
    def __init__(self, store: MetadataStore):
        self.store = store

    def compile(self, team_definition: dict[str, Any]) -> CompiledTeamDefinition:
        spec = dict(team_definition.get("spec_json") or {})
        workspace_id = str(spec.get("workspace_id") or "local-workspace")
        project_id = str(spec.get("project_id") or "default-project")
        members = [self._resolve_member(item) for item in list(spec.get("members") or spec.get("agents") or [])]
        if not members:
            raise ValueError("Team definition must contain at least one member.")
        ordered_levels = sorted({member.level for member in members}, reverse=True)
        adjacency = build_adjacency_map([{"key": member.key, "level": member.level} for member in members])
        team_shared_knowledge_bases = [
            self._resolve_knowledge_base(ref)
            for ref in list(
                spec.get("shared_kb_bindings")
                or spec.get("shared_knowledge_base_refs")
                or spec.get("shared_knowledge_bases")
                or []
            )
        ]
        team_shared_static_memories = [
            self._resolve_static_memory(ref)
            for ref in list(
                spec.get("shared_static_memory_bindings")
                or spec.get("shared_static_memory_refs")
                or spec.get("shared_static_memories")
                or []
            )
        ]
        team_review_policies = [self._resolve_review_policy(ref) for ref in list(spec.get("review_policy_refs") or [])]
        review_overrides = [
            self._normalize_review_override(item, members=members)
            for item in list(spec.get("review_overrides") or [])
            if isinstance(item, dict)
        ]
        entry_agent_id = self._entry_agent_id(spec, members)
        finish_agent_ids = self._finish_agent_ids(spec, members, entry_agent_id=entry_agent_id)
        role_templates, agents, workbenches, resource_lock = self._compile_members(
            members,
            team_shared_knowledge_bases=team_shared_knowledge_bases,
            team_shared_static_memories=team_shared_static_memories,
        )
        existing_review_ids = {str(item.get("id") or item.get("key") or "") for item in resource_lock["review_policies"]}
        for review_policy in team_review_policies:
            review_id = str(review_policy.get("id") or review_policy.get("key") or "")
            if review_id and review_id not in existing_review_ids:
                resource_lock["review_policies"].append(
                    {field: review_policy.get(field) for field in ("id", "key", "version")}
                )
                existing_review_ids.add(review_id)
        seen_lock: dict[str, set[str]] = {
            bucket: {str(item.get("id") or item.get("key") or "") for item in items}
            for bucket, items in resource_lock.items()
        }
        for knowledge_base in team_shared_knowledge_bases:
            self._lock_resource(resource_lock, seen_lock, "knowledge_bases", knowledge_base, fields=("id", "key"))
        for static_memory in team_shared_static_memories:
            self._lock_resource(resource_lock, seen_lock, "static_memories", static_memory, fields=("id", "key", "version"))
        flow = self._compile_flow(members)
        team_runtime = self._team_runtime_payload(
            team_definition=team_definition,
            members=members,
            team_shared_knowledge_bases=team_shared_knowledge_bases,
            team_shared_static_memories=team_shared_static_memories,
            ordered_levels=ordered_levels,
            adjacency=adjacency,
            entry_agent_id=entry_agent_id,
            finish_agent_ids=finish_agent_ids,
            team_review_policies=team_review_policies,
            review_overrides=review_overrides,
        )
        blueprint = {
            "name": slugify(str(team_definition.get("name") or spec.get("name") or "team_definition"), fallback="team_definition"),
            "description": str(team_definition.get("description") or spec.get("description") or ""),
            "workspace_id": workspace_id,
            "project_id": project_id,
            "version": str(team_definition.get("version") or "v1"),
            "role_templates": role_templates,
            "agents": agents,
            "workbenches": workbenches,
            "flow": flow,
            "definition_of_done": ["finalize.path != null"],
            "acceptance_checks": [],
            "metadata": {
                "runtime": "langgraph-official",
                "execution_mode": "team_event_driven",
                "team_definition_id": team_definition["id"],
                "team_definition_name": team_definition["name"],
                "communication_policy": dict(spec.get("communication_policy") or {"type": "adjacent_level_all"}),
                "review_policy_refs": list(spec.get("review_policy_refs") or []),
                "adjacency": adjacency,
                "resource_lock": resource_lock,
                "team_runtime": team_runtime,
            },
        }
        return CompiledTeamDefinition(
            team_definition=team_definition,
            members=members,
            team_shared_knowledge_bases=team_shared_knowledge_bases,
            team_shared_static_memories=team_shared_static_memories,
            ordered_levels=ordered_levels,
            adjacency=adjacency,
            entry_agent_id=entry_agent_id,
            finish_agent_ids=finish_agent_ids,
            team_review_policies=team_review_policies,
            review_overrides=review_overrides,
            blueprint=blueprint,
            resource_lock=resource_lock,
        )

    def _resolve_member(self, payload: dict[str, Any]) -> TeamMemberRuntimeSpec:
        definition_ref = payload.get("agent_definition_ref") or payload.get("agent_definition_id")
        if definition_ref is None:
            raise ValueError(f"Team member `{payload.get('key')}` is missing agent_definition_ref.")
        definition = self.store.get_agent_definition(str(definition_ref))
        if definition is None:
            raise ValueError(f"Unknown agent definition `{definition_ref}`.")
        spec = dict(definition.get("spec_json") or {})
        provider_ref = spec.get("provider_ref") or spec.get("provider_profile_id")
        if provider_ref is None:
            raise ValueError(f"Agent definition `{definition.get('id')}` is missing provider_ref.")
        provider = self._resolve_provider(provider_ref)
        if provider is None:
            raise ValueError(f"Unknown provider profile `{provider_ref}`.")
        plugin_refs = list(spec.get("tool_plugin_refs") or spec.get("plugin_refs") or [])
        plugins = [self._resolve_plugin(ref) for ref in plugin_refs]
        skills = [self._resolve_skill(ref) for ref in list(spec.get("skill_refs") or spec.get("skills") or [])]
        static_memory = None
        if spec.get("static_memory_ref"):
            static_memory = self.store.get_static_memory(str(spec["static_memory_ref"])) or self.store.get_static_memory_by_key(str(spec["static_memory_ref"]))
            if static_memory is None:
                raise ValueError(f"Unknown static memory `{spec['static_memory_ref']}`.")
        knowledge_bases = [self._resolve_knowledge_base(ref) for ref in list(spec.get("knowledge_base_refs") or [])]
        review_policies = [self._resolve_review_policy(ref) for ref in list(spec.get("review_policy_refs") or [])]
        memory_profile = self._resolve_memory_profile(spec)
        level = int(payload.get("level") or 0)
        if level < 1 or level > 16:
            raise ValueError(f"Team member `{payload.get('key') or definition.get('id')}` level must be within 1-16.")
        return TeamMemberRuntimeSpec(
            key=str(payload.get("key") or definition["id"]),
            name=str(payload.get("name") or definition.get("name") or definition["id"]),
            level=level,
            agent_definition=definition,
            provider_profile=provider,
            plugins=plugins,
            skills=skills,
            static_memory=static_memory,
            knowledge_bases=knowledge_bases,
            review_policies=review_policies,
            memory_profile=memory_profile,
            reports_to=[str(item) for item in list(payload.get("reports_to") or []) if str(item).strip()],
            runtime_plugin_actions=[dict(item) for item in list(payload.get("runtime_plugin_actions") or []) if isinstance(item, dict)],
            can_receive_task=bool(payload.get("can_receive_task", False)),
            can_finish_task=bool(payload.get("can_finish_task", False)),
            peer_chat_enabled=bool(payload.get("peer_chat_enabled", True)),
        )

    def _resolve_plugin(self, reference: Any) -> dict[str, Any]:
        plugin = self.store.get_plugin(str(reference)) or self.store.get_plugin_by_key(str(reference))
        if plugin is None:
            raise ValueError(f"Unknown plugin `{reference}`.")
        return plugin

    def _resolve_provider(self, reference: Any) -> dict[str, Any] | None:
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
        return None

    def _resolve_skill(self, reference: Any) -> dict[str, Any]:
        skill = self.store.get_skill(str(reference)) or self.store.get_skill_by_key(str(reference))
        if skill is None:
            raise ValueError(f"Unknown skill `{reference}`.")
        return skill

    def _resolve_static_memory(self, reference: Any) -> dict[str, Any]:
        static_memory = self.store.get_static_memory(str(reference)) or self.store.get_static_memory_by_key(str(reference))
        if static_memory is None:
            raise ValueError(f"Unknown static memory `{reference}`.")
        return static_memory

    def _resolve_knowledge_base(self, reference: Any) -> dict[str, Any]:
        kb = self.store.get_knowledge_base(str(reference)) or self.store.get_knowledge_base_by_key(str(reference))
        if kb is None:
            raise ValueError(f"Unknown knowledge base `{reference}`.")
        return kb

    def _resolve_review_policy(self, reference: Any) -> dict[str, Any]:
        policy = self.store.get_review_policy(str(reference)) or self.store.get_review_policy_by_key(str(reference))
        if policy is None:
            raise ValueError(f"Unknown review policy `{reference}`.")
        return policy

    def _resolve_memory_profile(self, definition_spec: dict[str, Any]) -> MemoryProfileRuntimeSpec | None:
        reference = definition_spec.get("memory_profile_ref") or definition_spec.get("memory_profile_id")
        if reference:
            record = self.store.get_memory_profile(str(reference)) or self.store.get_memory_profile_by_key(str(reference))
            if record is None:
                raise ValueError(f"Unknown memory profile `{reference}`.")
            return MemoryProfileRuntimeSpec(
                key=str(record.get("key") or record.get("id") or ""),
                name=str(record.get("name") or record.get("key") or ""),
                config=dict(record.get("spec_json") or {}),
                source={"id": record.get("id"), "key": record.get("key"), "version": record.get("version")},
            )
        inline = dict(definition_spec.get("memory_profile") or {})
        if not inline:
            return None
        key = str(inline.get("key") or f"inline.{slugify(str(definition_spec.get('provider_ref') or 'memory_profile'), fallback='memory_profile')}").strip()
        name = str(inline.get("name") or "Inline memory profile").strip()
        return MemoryProfileRuntimeSpec(key=key, name=name, config=inline, source={"inline": True})

    def _normalize_review_override(
        self,
        payload: dict[str, Any],
        *,
        members: list[TeamMemberRuntimeSpec],
    ) -> dict[str, Any]:
        member_keys = {member.key for member in members}
        source_agent_id = str(payload.get("source_agent_id") or payload.get("source") or payload.get("from") or "").strip()
        target_agent_id = str(payload.get("target_agent_id") or payload.get("target") or payload.get("to") or "").strip()
        if source_agent_id and source_agent_id != "human" and source_agent_id not in member_keys:
            raise ValueError(f"Unknown review override source agent `{source_agent_id}`.")
        if target_agent_id and target_agent_id != "human" and target_agent_id not in member_keys:
            raise ValueError(f"Unknown review override target agent `{target_agent_id}`.")
        mode = str(payload.get("mode") or payload.get("policy") or "must_review_before").strip() or "must_review_before"
        if mode not in {"must_review_before"}:
            raise ValueError(f"Unsupported review override mode `{mode}`.")
        message_types = [str(item).strip() for item in list(payload.get("message_types") or []) if str(item).strip()]
        phases = [str(item).strip() for item in list(payload.get("phases") or []) if str(item).strip()]
        name = str(payload.get("name") or payload.get("title") or "").strip()
        return {
            "name": name,
            "mode": mode,
            "source_agent_id": source_agent_id or None,
            "target_agent_id": target_agent_id or None,
            "message_types": message_types,
            "phases": phases,
            "metadata": dict(payload.get("metadata") or {}),
        }

    def _compile_members(
        self,
        members: list[TeamMemberRuntimeSpec],
        *,
        team_shared_knowledge_bases: list[dict[str, Any]],
        team_shared_static_memories: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        role_templates: dict[str, Any] = {}
        agents: dict[str, Any] = {}
        workbenches: dict[str, Any] = {}
        resource_lock = {
            "agent_definitions": [],
            "provider_profiles": [],
            "plugins": [],
            "skills": [],
            "static_memories": [],
            "knowledge_bases": [],
            "review_policies": [],
            "memory_profiles": [],
        }
        seen: dict[str, set[str]] = {key: set() for key in resource_lock}

        for member in members:
            definition = member.agent_definition
            definition_spec = dict(definition.get("spec_json") or {})
            provider = member.provider_profile
            provider_config = dict(provider.get("config_json") or {})
            preset = preset_for(str(provider.get("provider_type") or definition_spec.get("provider_type") or "mock"))
            model = str(definition_spec.get("model") or provider_config.get("model") or preset.get("default_model") or "mock-model")
            role_template_key = f"role_{slugify(member.key, fallback='agent')}"
            effective_knowledge_bases = self._dedupe_resources([*member.knowledge_bases, *team_shared_knowledge_bases])
            instructions = self._compose_instructions(member, team_shared_static_memories=team_shared_static_memories)
            plugin_entries: list[dict[str, Any]] = []
            workbench_keys: list[str] = []
            for plugin in member.plugins:
                manifest = dict(plugin.get("manifest_json") or {})
                workbench_key = slugify(str(manifest.get("workbench_key") or plugin.get("key") or plugin.get("id") or "plugin"), fallback="plugin")
                if workbench_key not in workbenches:
                    workbenches[workbench_key] = {
                        "name": str(plugin.get("name") or workbench_key),
                        "tools": [str(item) for item in manifest.get("tools", [])],
                        "permissions": [str(item) for item in manifest.get("permissions", [])],
                        "description": str(manifest.get("description") or plugin.get("description") or ""),
                    }
                if workbench_key not in workbench_keys:
                    workbench_keys.append(workbench_key)
                plugin_entries.append(
                    {
                        "id": plugin["id"],
                        "key": plugin["key"],
                        "version": plugin["version"],
                        "install_path": plugin.get("install_path"),
                        "manifest": manifest,
                    }
                )
                self._lock_resource(resource_lock, seen, "plugins", plugin, fields=("id", "key", "version"))

            for skill in member.skills:
                self._lock_resource(resource_lock, seen, "skills", skill, fields=("id", "key", "version"))
            for review_policy in member.review_policies:
                self._lock_resource(resource_lock, seen, "review_policies", review_policy, fields=("id", "key", "version"))
            for knowledge_base in effective_knowledge_bases:
                self._lock_resource(resource_lock, seen, "knowledge_bases", knowledge_base, fields=("id", "key"))
            if member.static_memory is not None:
                self._lock_resource(resource_lock, seen, "static_memories", member.static_memory, fields=("id", "key", "version"))
            for static_memory in team_shared_static_memories:
                self._lock_resource(resource_lock, seen, "static_memories", static_memory, fields=("id", "key", "version"))
            if member.memory_profile is not None and not member.memory_profile.source.get("inline"):
                self._lock_resource(
                    resource_lock,
                    seen,
                    "memory_profiles",
                    {
                        "id": member.memory_profile.source.get("id"),
                        "key": member.memory_profile.source.get("key") or member.memory_profile.key,
                        "version": member.memory_profile.source.get("version"),
                    },
                    fields=("id", "key", "version"),
                )
            self._lock_resource(resource_lock, seen, "agent_definitions", definition, fields=("id", "version"))
            self._lock_resource(resource_lock, seen, "provider_profiles", provider, fields=("id", "name", "provider_type"))

            role_templates[role_template_key] = {
                "name": member.name,
                "role": member.role,
                "goal": str(definition_spec.get("goal") or ""),
                "instructions": instructions,
                "backend": str(provider_config.get("backend") or provider.get("provider_type") or "mock"),
                "provider_type": str(provider.get("provider_type") or "mock"),
                "model": model,
                "base_url": provider_config.get("base_url"),
                "api_key": dict(provider.get("secret_json") or {}).get("api_key"),
                "api_key_env": provider_config.get("api_key_env"),
                "api_version": provider_config.get("api_version"),
                "organization": provider_config.get("organization"),
                "temperature": float(definition_spec.get("temperature", provider_config.get("temperature", 0.2))),
                "max_tokens": definition_spec.get("max_tokens", provider_config.get("max_tokens")),
                "workbenches": workbench_keys,
                "memory_policy": self._runtime_memory_policy(member.memory_profile),
                "extra_headers": dict(provider_config.get("extra_headers") or {}),
                "extra_config": dict(provider_config.get("extra_config") or {}),
                "metadata": {
                    "agent_definition_id": definition["id"],
                    "provider_profile_id": provider["id"],
                    "skills": [skill["key"] for skill in member.skills],
                    "static_memory_ref": member.static_memory["key"] if member.static_memory else None,
                    "knowledge_base_refs": [kb["key"] for kb in effective_knowledge_bases],
                    "member_knowledge_base_refs": [kb["key"] for kb in member.knowledge_bases],
                    "team_shared_knowledge_base_refs": [kb["key"] for kb in team_shared_knowledge_bases],
                    "knowledge_bases": [
                        self._knowledge_base_binding_payload(kb) for kb in effective_knowledge_bases
                    ],
                    "team_shared_knowledge_bases": [
                        self._knowledge_base_binding_payload(kb) for kb in team_shared_knowledge_bases
                    ],
                    "team_shared_static_memory_refs": [item["key"] for item in team_shared_static_memories],
                    "team_shared_static_memories": [
                        self._static_memory_binding_payload(item) for item in team_shared_static_memories
                    ],
                    "review_policy_refs": [policy["key"] for policy in member.review_policies],
                    "memory_profile": member.memory_profile.to_dict() if member.memory_profile else None,
                    "plugins": plugin_entries,
                },
            }
            agents[member.key] = {
                "name": member.name,
                "role": member.role,
                "role_template": role_template_key,
                "metadata": {
                    "level": member.level,
                    "reports_to": list(member.reports_to),
                    "can_receive_task": member.can_receive_task,
                    "can_finish_task": member.can_finish_task,
                    "peer_chat_enabled": member.peer_chat_enabled,
                    "memory_profile": member.memory_profile.to_dict() if member.memory_profile else None,
                    "knowledge_bases": [
                        self._knowledge_base_binding_payload(kb) for kb in effective_knowledge_bases
                    ],
                    "member_knowledge_base_refs": [kb["key"] for kb in member.knowledge_bases],
                    "team_shared_knowledge_base_refs": [kb["key"] for kb in team_shared_knowledge_bases],
                    "team_shared_knowledge_bases": [
                        self._knowledge_base_binding_payload(kb) for kb in team_shared_knowledge_bases
                    ],
                    "static_memory_ref": member.static_memory["key"] if member.static_memory else None,
                    "team_shared_static_memory_refs": [item["key"] for item in team_shared_static_memories],
                    "team_shared_static_memories": [
                        self._static_memory_binding_payload(item) for item in team_shared_static_memories
                    ],
                    "plugins": plugin_entries,
                },
            }
        return role_templates, agents, workbenches, resource_lock

    def _compose_instructions(
        self,
        member: TeamMemberRuntimeSpec,
        *,
        team_shared_static_memories: list[dict[str, Any]] | None = None,
    ) -> str:
        definition = dict(member.agent_definition.get("spec_json") or {})
        blocks = [str(definition.get("instructions") or "").strip()]
        if member.static_memory is not None:
            memory_name = str(member.static_memory.get("name") or member.static_memory.get("key") or "Role spec")
            prompt = role_spec_system_prompt(dict(member.static_memory.get("spec_json") or {}))
            if prompt:
                blocks.append(f"Bound role spec `{memory_name}`:")
                blocks.append(prompt)
        if member.memory_profile is not None:
            read_scopes = ", ".join(str(item) for item in member.memory_profile.config.get("read_scopes", []))
            write_scopes = ", ".join(str(item) for item in member.memory_profile.config.get("write_scopes", []))
            if read_scopes:
                blocks.append(f"Memory read scopes: {read_scopes}.")
            if write_scopes:
                blocks.append(f"Memory write scopes: {write_scopes}.")
        for skill in member.skills:
            skill_spec = dict(skill.get("spec_json") or {})
            skill_instructions = [str(item).strip() for item in skill_spec.get("instructions", []) if str(item).strip()]
            if skill_instructions:
                blocks.append(f"Skill `{skill['name']}`: " + " ".join(skill_instructions))
        for static_memory in list(team_shared_static_memories or []):
            memory_name = str(static_memory.get("name") or static_memory.get("key") or "Shared role spec")
            prompt = role_spec_system_prompt(dict(static_memory.get("spec_json") or {}))
            if prompt:
                blocks.append(f"Shared team role spec `{memory_name}`:")
                blocks.append(prompt)
        return "\n".join(item for item in blocks if item)

    def _compile_flow(self, members: list[TeamMemberRuntimeSpec]) -> dict[str, Any]:
        grouped_levels = self._group_members_by_level(members)
        ordered_levels = sorted(grouped_levels.keys(), reverse=True)
        path = [(level, "down") for level in ordered_levels]
        if len(ordered_levels) > 1:
            path.extend((level, "up") for level in reversed(ordered_levels[:-1]))

        nodes: list[dict[str, Any]] = [{"id": "start", "type": "start"}]
        edges: list[dict[str, Any]] = []
        current = "start"
        for index, (level, phase) in enumerate(path):
            current = self._append_level_segment(
                nodes=nodes,
                edges=edges,
                prev_node_id=current,
                members=grouped_levels[level],
                phase=phase,
                segment_index=index,
            )
        nodes.append(
            {
                "id": "finalize",
                "type": "artifact",
                "name": "team-summary.md",
                "artifact_kind": "report",
                "template": "# Team Summary\n\n{{" + current + ".summary}}\n\n## Outputs\n{{" + current + ".deliverables_text}}",
            }
        )
        edges.append({"from": current, "to": "finalize"})
        nodes.append({"id": "end", "type": "end"})
        edges.append({"from": "finalize", "to": "end"})
        return {"nodes": nodes, "edges": edges}

    def _append_level_segment(
        self,
        *,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        prev_node_id: str,
        members: list[TeamMemberRuntimeSpec],
        phase: str,
        segment_index: int,
    ) -> str:
        if len(members) == 1:
            member = members[0]
            node_id = f"{slugify(member.key, fallback='agent')}_{phase}_{segment_index}"
            nodes.append(
                {
                    "id": node_id,
                    "type": "agent",
                    "agent": member.key,
                    "instruction": self._segment_instruction(member, phase=phase),
                    "config": {"level": member.level, "phase": phase},
                }
            )
            edges.append({"from": prev_node_id, "to": node_id})
            return node_id

        parallel_id = f"parallel_{phase}_{segment_index}"
        merge_id = f"merge_{phase}_{segment_index}"
        nodes.append({"id": parallel_id, "type": "parallel"})
        edges.append({"from": prev_node_id, "to": parallel_id})
        for member in members:
            node_id = f"{slugify(member.key, fallback='agent')}_{phase}_{segment_index}"
            nodes.append(
                {
                    "id": node_id,
                    "type": "agent",
                    "agent": member.key,
                    "instruction": self._segment_instruction(member, phase=phase),
                    "config": {"level": member.level, "phase": phase},
                }
            )
            edges.append({"from": parallel_id, "to": node_id})
            edges.append({"from": node_id, "to": merge_id})
        nodes.append({"id": merge_id, "type": "merge"})
        return merge_id

    def _segment_instruction(self, member: TeamMemberRuntimeSpec, *, phase: str) -> str:
        if phase == "down":
            return f"Receive task or handoff from adjacent upper level and produce actionable output for level {member.level}."
        return f"Receive results from adjacent lower level, review them, and return a concise escalation summary for level {member.level}."

    def _group_members_by_level(self, members: list[TeamMemberRuntimeSpec]) -> dict[int, list[TeamMemberRuntimeSpec]]:
        grouped: dict[int, list[TeamMemberRuntimeSpec]] = {}
        for member in members:
            grouped.setdefault(member.level, []).append(member)
        for level in grouped:
            grouped[level].sort(key=lambda item: item.key)
        return grouped

    def _entry_agent_id(self, spec: dict[str, Any], members: list[TeamMemberRuntimeSpec]) -> str:
        task_entry_policy = dict(spec.get("task_entry_policy") or {})
        entry_mode = str(task_entry_policy.get("mode") or "").strip().lower()
        if entry_mode == "specific_agent":
            requested_policy = str(task_entry_policy.get("agent_id") or task_entry_policy.get("target_agent_id") or "").strip()
            if requested_policy and any(member.key == requested_policy for member in members):
                return requested_policy
        requested = str(spec.get("task_entry_agent") or "").strip()
        if requested and any(member.key == requested for member in members):
            return requested
        receivers = [member.key for member in members if member.can_receive_task]
        if receivers:
            return sorted(receivers)[0]
        highest = sorted(members, key=lambda item: (-item.level, item.key))
        return highest[0].key

    def _finish_agent_ids(
        self,
        spec: dict[str, Any],
        members: list[TeamMemberRuntimeSpec],
        *,
        entry_agent_id: str,
    ) -> list[str]:
        member_keys = {member.key for member in members}
        termination_policy = dict(spec.get("termination_policy") or {})
        mode = str(termination_policy.get("mode") or "").strip().lower()
        if mode == "entry_agent":
            return [entry_agent_id]
        if mode == "specific_agents":
            configured = [
                str(item).strip()
                for item in list(
                    termination_policy.get("finish_agent_ids")
                    or termination_policy.get("agent_ids")
                    or termination_policy.get("targets")
                    or []
                )
                if str(item).strip()
            ]
            selected = [item for item in configured if item in member_keys]
            if selected:
                return selected
        finishers = [member.key for member in members if member.can_finish_task]
        return finishers or [entry_agent_id]

    def _runtime_memory_policy(self, profile: MemoryProfileRuntimeSpec | None) -> str:
        if profile is None:
            return "agent_private_plus_project"
        read_scopes = {str(item) for item in profile.config.get("read_scopes", [])}
        if "project" in read_scopes:
            return "agent_private_plus_project"
        if "team" in read_scopes:
            return "project_shared"
        return "agent_private"

    def _team_runtime_payload(
        self,
        *,
        team_definition: dict[str, Any],
        members: list[TeamMemberRuntimeSpec],
        team_shared_knowledge_bases: list[dict[str, Any]],
        team_shared_static_memories: list[dict[str, Any]],
        ordered_levels: list[int],
        adjacency: dict[str, list[str]],
        entry_agent_id: str,
        finish_agent_ids: list[str],
        team_review_policies: list[dict[str, Any]],
        review_overrides: list[dict[str, Any]],
    ) -> dict[str, Any]:
        tier_members = self._group_members_by_level(members)
        return {
            "team_definition_id": team_definition.get("id"),
            "team_definition_key": team_definition.get("key"),
            "entry_agent_id": entry_agent_id,
            "finish_agent_ids": finish_agent_ids,
            "ordered_levels": ordered_levels,
            "tiers": {str(level): [member.key for member in tier_members[level]] for level in ordered_levels},
            "adjacency": adjacency,
            "communication_policy": dict((team_definition.get("spec_json") or {}).get("communication_policy") or {"type": "adjacent_level_all"}),
            "task_entry_policy": dict((team_definition.get("spec_json") or {}).get("task_entry_policy") or {}),
            "team_review_policy_refs": list((team_definition.get("spec_json") or {}).get("review_policy_refs") or []),
            "termination_policy": dict((team_definition.get("spec_json") or {}).get("termination_policy") or {}),
            "shared_knowledge_bases": [self._knowledge_base_binding_payload(item) for item in team_shared_knowledge_bases],
            "shared_static_memories": [self._static_memory_binding_payload(item) for item in team_shared_static_memories],
            "team_review_policies": [
                {
                    "id": item.get("id"),
                    "key": item.get("key"),
                    "name": item.get("name"),
                    "version": item.get("version"),
                    "spec": dict(item.get("spec_json") or {}),
                }
                for item in team_review_policies
            ],
            "review_overrides": [dict(item) for item in review_overrides],
            "members": [member.to_dict() for member in members],
        }

    def _knowledge_base_binding_payload(self, knowledge_base: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": knowledge_base.get("id"),
            "key": knowledge_base.get("key"),
            "name": knowledge_base.get("name"),
            "description": knowledge_base.get("description"),
            "config": dict(knowledge_base.get("config_json") or {}),
            "status": knowledge_base.get("status"),
        }

    def _static_memory_binding_payload(self, static_memory: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": static_memory.get("id"),
            "key": static_memory.get("key"),
            "name": static_memory.get("name"),
            "description": static_memory.get("description"),
            "version": static_memory.get("version"),
            "spec": normalize_role_spec(dict(static_memory.get("spec_json") or {})),
        }

    def _dedupe_resources(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in items:
            identity = str(item.get("id") or item.get("key") or "")
            if not identity or identity in seen:
                continue
            seen.add(identity)
            deduped.append(item)
        return deduped

    def _lock_resource(
        self,
        resource_lock: dict[str, list[dict[str, Any]]],
        seen: dict[str, set[str]],
        bucket: str,
        payload: dict[str, Any],
        *,
        fields: tuple[str, ...],
    ) -> None:
        key = str(payload.get("id") or payload.get("key") or "")
        if not key or key in seen[bucket]:
            return
        seen[bucket].add(key)
        resource_lock[bucket].append({field: payload.get(field) for field in fields})
