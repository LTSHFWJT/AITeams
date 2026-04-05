from __future__ import annotations

import shutil
from copy import deepcopy
from pathlib import Path
from typing import Any

from aiteams.memory.scope import Scope
from aiteams.plugins.builtin import KnowledgeBaseQueryBuiltinPlugin
from aiteams.plugins.manifest import PLUGIN_MANIFEST, validate_plugin_package
from aiteams.plugins.sandbox import PluginSandbox
from aiteams.storage.metadata import MetadataStore


BUILTIN_MEMORY_SEARCH_PLUGIN_KEY = "memory.search"
BUILTIN_MEMORY_MANAGE_PLUGIN_KEY = "memory.manage"
BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY = "memory.background_reflection"
BUILTIN_KB_RETRIEVE_PLUGIN_KEY = "kb.retrieve"
BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY = "team.message.send"
BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY = "team.message.reply"
BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY = "human.escalate"

BUILTIN_MEMORY_SEARCH_PLUGIN_ID = f"builtin:{BUILTIN_MEMORY_SEARCH_PLUGIN_KEY}"
BUILTIN_MEMORY_MANAGE_PLUGIN_ID = f"builtin:{BUILTIN_MEMORY_MANAGE_PLUGIN_KEY}"
BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_ID = f"builtin:{BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY}"
BUILTIN_KB_RETRIEVE_PLUGIN_ID = f"builtin:{BUILTIN_KB_RETRIEVE_PLUGIN_KEY}"
BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_ID = f"builtin:{BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY}"
BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_ID = f"builtin:{BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY}"
BUILTIN_HUMAN_ESCALATE_PLUGIN_ID = f"builtin:{BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY}"

BUILTIN_PLUGIN_DESCRIPTORS: dict[str, dict[str, Any]] = {
    BUILTIN_MEMORY_SEARCH_PLUGIN_KEY: {
        "key": BUILTIN_MEMORY_SEARCH_PLUGIN_KEY,
        "tools": ["memory.search"],
        "permissions": ["memory_read"],
        "actions": [{"name": "search", "description": "Search long-term and working memory within scoped namespaces."}],
    },
    BUILTIN_MEMORY_MANAGE_PLUGIN_KEY: {
        "key": BUILTIN_MEMORY_MANAGE_PLUGIN_KEY,
        "tools": ["memory.manage"],
        "permissions": ["memory_read", "memory_write"],
        "actions": [{"name": "manage", "description": "Create, update, or delete scoped memory records."}],
    },
    BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY: {
        "key": BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY,
        "tools": ["memory.background_reflection"],
        "permissions": ["memory_read", "memory_write"],
        "actions": [{"name": "reflect", "description": "Schedule LangMem background reflection over recent conversation turns."}],
    },
    BUILTIN_KB_RETRIEVE_PLUGIN_KEY: {
        "key": BUILTIN_KB_RETRIEVE_PLUGIN_KEY,
        "name": "Knowledge Base Query",
        "description": "Query bound knowledge bases through the built-in LlamaIndex query engine.",
        "tools": ["knowledge_search"],
        "permissions": ["readonly"],
        "actions": [{"name": "retrieve", "description": "Query bound knowledge bases and return grounded source nodes."}],
    },
    BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY: {
        "key": BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY,
        "tools": ["team.message.send"],
        "permissions": ["team_message"],
        "actions": [{"name": "send", "description": "Send a direct adjacent-level message via the Dialogue Router."}],
    },
    BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY: {
        "key": BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY,
        "tools": ["team.message.reply"],
        "permissions": ["team_message"],
        "actions": [{"name": "reply", "description": "Reply to the current inbound team message via the Dialogue Router."}],
    },
    BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY: {
        "key": BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY,
        "tools": ["human.escalate"],
        "permissions": ["human_review"],
        "actions": [{"name": "escalate", "description": "Request direct human intervention for the current interaction."}],
    },
}


def builtin_plugin_id(plugin_key: str) -> str:
    return f"builtin:{plugin_key}"


def builtin_plugin_ref(plugin_key: str, **overrides: Any) -> dict[str, Any]:
    descriptor = deepcopy(BUILTIN_PLUGIN_DESCRIPTORS[plugin_key])
    manifest = {
        "tools": list(descriptor.get("tools") or []),
        "permissions": list(descriptor.get("permissions") or []),
        "actions": [dict(item) for item in list(descriptor.get("actions") or [])],
        "description": str(descriptor.get("description") or ""),
    }
    override_manifest = dict(overrides.pop("manifest", {}) or {})
    manifest.update(override_manifest)
    return {
        "id": builtin_plugin_id(plugin_key),
        "key": plugin_key,
        "version": "builtin",
        "manifest": manifest,
        "install_path": None,
        "builtin": True,
        **overrides,
    }


class PluginManager:
    def __init__(
        self,
        *,
        store: MetadataStore,
        install_root: str | Path,
        memory: Any | None = None,
        knowledge_bases: Any | None = None,
    ):
        self.store = store
        self.install_root = Path(install_root).expanduser().resolve()
        self.install_root.mkdir(parents=True, exist_ok=True)
        self.memory = memory
        self.knowledge_bases = knowledge_bases
        self._sandboxes: dict[str, PluginSandbox] = {}

    def close(self) -> None:
        for sandbox in list(self._sandboxes.values()):
            sandbox.stop()
        self._sandboxes.clear()

    def validate_package(self, path: str | Path) -> dict[str, Any]:
        manifest = validate_plugin_package(path)
        return {
            "valid": True,
            "manifest": manifest,
            "source_path": str(Path(path).expanduser().resolve()),
        }

    def install_plugin(self, plugin_id: str) -> dict[str, Any]:
        plugin = self._require_plugin(plugin_id)
        source_path = plugin.get("install_path")
        if not source_path:
            raise ValueError("Plugin install_path is required for installation.")
        source = Path(source_path).expanduser().resolve()
        manifest = validate_plugin_package(source)
        target = self.install_root / manifest["key"] / manifest["version"]
        if source != target:
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(source, target)
        installed_manifest = validate_plugin_package(target)
        saved = self.store.save_plugin(
            plugin_id=str(plugin["id"]),
            key=str(plugin["key"]),
            name=str(plugin["name"]),
            version=str(plugin["version"]),
            plugin_type=str(plugin["plugin_type"]),
            description=str(plugin.get("description") or installed_manifest.get("description") or ""),
            manifest=installed_manifest,
            config=dict(plugin.get("config_json") or {}),
            install_path=str(target),
            secret=dict(plugin.get("secret_json") or {}) if plugin.get("has_secret") else None,
            status="installed",
        )
        return {"plugin": saved, "manifest": installed_manifest, "installed_path": str(target)}

    def load_plugin(self, plugin_id: str) -> dict[str, Any]:
        plugin = self._require_plugin(plugin_id)
        sandbox = self._sandbox_for_record(plugin)
        sandbox.start()
        descriptor = sandbox.describe()
        return {"plugin_id": plugin_id, "status": "running", "descriptor": descriptor, "runtime": sandbox.snapshot()}

    def reload_plugin(self, plugin_id: str) -> dict[str, Any]:
        plugin = self._require_plugin(plugin_id)
        sandbox = self._sandbox_for_record(plugin, force_reload=True)
        descriptor = sandbox.describe()
        return {"plugin_id": plugin_id, "status": "running", "descriptor": descriptor, "runtime": sandbox.snapshot()}

    def health(self, plugin_id: str) -> dict[str, Any]:
        plugin = self._require_plugin(plugin_id)
        sandbox = self._sandboxes.get(str(plugin["id"]))
        if sandbox is None:
            return {
                "plugin_id": str(plugin["id"]),
                "status": "not_loaded",
                "runtime": self.snapshot(plugin_id),
            }
        return {
            "plugin_id": str(plugin["id"]),
            "status": "running",
            "health": sandbox.health(),
            "runtime": sandbox.snapshot(),
        }

    def delete_plugin(self, plugin_id: str) -> dict[str, Any]:
        plugin = self._require_plugin(plugin_id)
        sandbox = self._sandboxes.pop(str(plugin["id"]), None)
        if sandbox is not None:
            sandbox.stop()

        removed_install_path: str | None = None
        install_path = str(plugin.get("install_path") or "").strip()
        if install_path:
            root = Path(install_path).expanduser().resolve()
            deletable_root: Path | None = root
            try:
                root.relative_to(self.install_root)
            except ValueError:
                deletable_root = None
            if deletable_root is not None and deletable_root.exists():
                shutil.rmtree(deletable_root)
                removed_install_path = str(deletable_root)
                parent = deletable_root.parent
                while parent != self.install_root and parent.exists():
                    try:
                        parent.rmdir()
                    except OSError:
                        break
                    parent = parent.parent

        deleted = self.store.delete_plugin(plugin_id)
        if deleted is None:
            raise ValueError("Plugin does not exist.")
        return {
            "deleted": True,
            "plugin": plugin,
            "removed_install_path": removed_install_path,
        }

    def snapshot(self, plugin_id: str) -> dict[str, Any]:
        plugin = self._require_plugin(plugin_id)
        sandbox = self._sandboxes.get(str(plugin["id"]))
        if sandbox:
            return sandbox.snapshot()
        runtime_status = "metadata_only" if not plugin.get("install_path") else "idle"
        return {
            "plugin_id": str(plugin["id"]),
            "running": False,
            "pid": None,
            "started_at": None,
            "last_error": None,
            "descriptor": dict(plugin.get("manifest_json") or {}),
            "fingerprint": None,
            "stderr_tail": [],
            "status": runtime_status,
        }

    def invoke_plugin(
        self,
        plugin_ref: dict[str, Any],
        *,
        action: str,
        payload: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        if self.is_builtin_plugin(plugin_ref):
            return self._invoke_builtin_plugin(plugin_ref, action=action, payload=payload, context=context)
        sandbox = self._sandbox_for_ref(plugin_ref)
        return sandbox.invoke(action, payload, context)

    def describe_plugin_ref(self, plugin_ref: dict[str, Any]) -> dict[str, Any]:
        builtin = self._builtin_descriptor(plugin_ref)
        if builtin is not None:
            return builtin
        if plugin_ref.get("install_path"):
            runtime = self.load_plugin(str(plugin_ref["id"]))
            return dict(runtime.get("descriptor") or {})
        return dict(plugin_ref.get("manifest") or plugin_ref.get("manifest_json") or {})

    def builtin_catalog(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for plugin_key in sorted(BUILTIN_PLUGIN_DESCRIPTORS):
            ref = builtin_plugin_ref(plugin_key)
            descriptor = self.describe_plugin_ref(ref)
            items.append(
                {
                    "id": str(ref["id"]),
                    "key": str(ref["key"]),
                    "version": str(ref["version"]),
                    "builtin": True,
                    "manifest": dict(ref.get("manifest") or {}),
                    "descriptor": descriptor,
                }
            )
        return items

    def is_builtin_plugin(self, plugin_ref: dict[str, Any]) -> bool:
        plugin_id = str(plugin_ref.get("id") or "").strip()
        plugin_key = str(plugin_ref.get("key") or "").strip()
        return bool(plugin_ref.get("builtin")) or plugin_id.startswith("builtin:") or plugin_key in BUILTIN_PLUGIN_DESCRIPTORS

    def _sandbox_for_record(self, plugin: dict[str, Any], *, force_reload: bool = False) -> PluginSandbox:
        ref = {
            "id": str(plugin["id"]),
            "key": str(plugin.get("key") or ""),
            "version": str(plugin.get("version") or "v1"),
            "install_path": plugin.get("install_path"),
            "manifest": dict(plugin.get("manifest_json") or {}),
            "config_json": dict(plugin.get("config_json") or {}),
            "secret_json": dict(plugin.get("secret_json") or {}),
        }
        return self._sandbox_for_ref(ref, force_reload=force_reload)

    def _sandbox_for_ref(self, plugin_ref: dict[str, Any], *, force_reload: bool = False) -> PluginSandbox:
        plugin_id = str(plugin_ref.get("id") or "")
        if not plugin_id:
            raise ValueError("Plugin reference requires id.")
        manifest = dict(plugin_ref.get("manifest") or plugin_ref.get("manifest_json") or {})
        runtime_config = dict(plugin_ref.get("config") or plugin_ref.get("config_json") or {})
        runtime_secret = dict(plugin_ref.get("secret") or plugin_ref.get("secret_json") or {})
        install_path = str(plugin_ref.get("install_path") or "").strip()
        if not install_path:
            record = self.store.get_plugin(plugin_id, include_secret=True)
            if record is None or not record.get("install_path"):
                raise ValueError(f"Plugin `{plugin_id}` has no executable install_path.")
            install_path = str(record["install_path"])
            if not manifest:
                manifest = dict(record.get("manifest_json") or {})
            if not runtime_config:
                runtime_config = dict(record.get("config_json") or {})
            if not runtime_secret:
                runtime_secret = dict(record.get("secret_json") or {})
        root = Path(install_path).expanduser().resolve()
        if not root.exists() or not (root / PLUGIN_MANIFEST).exists():
            raise ValueError(f"Plugin package `{install_path}` does not exist or is missing `{PLUGIN_MANIFEST}`.")
        loaded_manifest = validate_plugin_package(root)
        sandbox = self._sandboxes.get(plugin_id)
        if sandbox is None:
            sandbox = PluginSandbox(
                plugin_id=plugin_id,
                manifest=loaded_manifest,
                root_path=root,
                runtime_config=runtime_config,
                runtime_secret=runtime_secret,
            )
            self._sandboxes[plugin_id] = sandbox
        else:
            sandbox.manifest = loaded_manifest
            sandbox.root_path = root
            sandbox.runtime_config = runtime_config
            sandbox.runtime_secret = runtime_secret
            if force_reload:
                sandbox.restart()
        return sandbox

    def _require_plugin(self, plugin_id: str) -> dict[str, Any]:
        plugin = self.store.get_plugin(plugin_id, include_secret=True)
        if plugin is None:
            raise ValueError("Plugin does not exist.")
        return plugin

    def _builtin_descriptor(self, plugin_ref: dict[str, Any]) -> dict[str, Any] | None:
        if not self.is_builtin_plugin(plugin_ref):
            return None
        plugin_key = self._builtin_plugin_key(plugin_ref)
        return deepcopy(BUILTIN_PLUGIN_DESCRIPTORS.get(plugin_key))

    def _invoke_builtin_plugin(
        self,
        plugin_ref: dict[str, Any],
        *,
        action: str,
        payload: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        plugin_key = self._builtin_plugin_key(plugin_ref)
        if plugin_key == BUILTIN_MEMORY_SEARCH_PLUGIN_KEY:
            if action != "search":
                raise ValueError(f"Builtin plugin `{plugin_key}` does not support action `{action}`.")
            scopes = self._memory_scopes(payload, context, allow_multiple=True)
            return self._require_memory().builtin_search(
                scopes,
                query=str(payload.get("query") or payload.get("text") or ""),
                top_k=int(payload.get("top_k") or payload.get("limit") or 8),
                filters=dict(payload.get("filters") or payload.get("filter") or {}),
            )
        if plugin_key == BUILTIN_MEMORY_MANAGE_PLUGIN_KEY:
            if action != "manage":
                raise ValueError(f"Builtin plugin `{plugin_key}` does not support action `{action}`.")
            scope = self._memory_scopes(payload, context, allow_multiple=False)[0]
            operation = str(payload.get("operation") or "upsert").strip().lower()
            return self._require_memory().builtin_manage(scope, operation=operation, payload=payload)
        if plugin_key == BUILTIN_MEMORY_BACKGROUND_REFLECTION_PLUGIN_KEY:
            if action != "reflect":
                raise ValueError(f"Builtin plugin `{plugin_key}` does not support action `{action}`.")
            scope = self._memory_scopes(payload, context, allow_multiple=False)[0]
            return self._require_memory().builtin_background_reflection(
                scope,
                runtime=dict(context.get("memory_runtime") or {}),
            )
        if plugin_key == BUILTIN_KB_RETRIEVE_PLUGIN_KEY:
            return KnowledgeBaseQueryBuiltinPlugin(
                store=self.store,
                knowledge_bases=self.knowledge_bases,
            ).invoke(
                action=action,
                payload=payload,
                context=context,
                plugin_ref=plugin_ref,
            )
        if plugin_key == BUILTIN_TEAM_MESSAGE_SEND_PLUGIN_KEY:
            if action != "send":
                raise ValueError(f"Builtin plugin `{plugin_key}` does not support action `{action}`.")
            return {
                "intent": plugin_key,
                "route": {
                    "kind": "team_message",
                    "mode": "send",
                    "target_agent_id": str(payload.get("target_agent_id") or payload.get("target") or "").strip() or None,
                    "message_type": str(payload.get("message_type") or "dialogue").strip() or "dialogue",
                    "phase": str(payload.get("phase") or context.get("phase") or "down").strip() or "down",
                    "body": self._optional_text(payload.get("body")),
                    "payload": dict(payload.get("message_payload") or payload.get("payload") or {}),
                    "metadata": dict(payload.get("metadata") or {}),
                },
            }
        if plugin_key == BUILTIN_TEAM_MESSAGE_REPLY_PLUGIN_KEY:
            if action != "reply":
                raise ValueError(f"Builtin plugin `{plugin_key}` does not support action `{action}`.")
            reply_target = (
                str(payload.get("target_agent_id") or payload.get("target") or "").strip()
                or str((context.get("message") or {}).get("source_actor_id") or context.get("source_actor_id") or "").strip()
                or None
            )
            current_phase = str((context.get("message") or {}).get("phase") or context.get("phase") or "down").strip() or "down"
            reply_phase = str(payload.get("phase") or ("up" if current_phase == "down" else "down")).strip() or "up"
            return {
                "intent": plugin_key,
                "route": {
                    "kind": "team_message",
                    "mode": "reply",
                    "target_agent_id": reply_target,
                    "message_type": str(payload.get("message_type") or "dialogue").strip() or "dialogue",
                    "phase": reply_phase,
                    "body": self._optional_text(payload.get("body")),
                    "payload": dict(payload.get("message_payload") or payload.get("payload") or {}),
                    "metadata": dict(payload.get("metadata") or {}),
                },
            }
        if plugin_key == BUILTIN_HUMAN_ESCALATE_PLUGIN_KEY:
            if action != "escalate":
                raise ValueError(f"Builtin plugin `{plugin_key}` does not support action `{action}`.")
            detail = self._optional_text(payload.get("detail")) or self._optional_text(payload.get("body"))
            return {
                "intent": plugin_key,
                "review": {
                    "kind": "human_escalation",
                    "title": str(payload.get("title") or "Human escalation requested").strip() or "Human escalation requested",
                    "detail": detail,
                    "body": self._optional_text(payload.get("body")),
                    "risk_tags": [str(item) for item in list(payload.get("risk_tags") or ["human_escalation"]) if str(item).strip()],
                    "metadata": dict(payload.get("metadata") or {}),
                },
            }
        raise ValueError(f"Unsupported builtin plugin `{plugin_key}`.")

    def _builtin_plugin_key(self, plugin_ref: dict[str, Any]) -> str:
        plugin_key = str(plugin_ref.get("key") or "").strip()
        if plugin_key:
            return plugin_key
        plugin_id = str(plugin_ref.get("id") or "").strip()
        if plugin_id.startswith("builtin:"):
            return plugin_id.split("builtin:", 1)[1]
        raise ValueError("Builtin plugin reference requires key.")

    def _require_memory(self) -> Any:
        if self.memory is None:
            raise ValueError("Builtin memory plugins require an attached memory adapter.")
        return self.memory

    def _memory_scopes(self, payload: dict[str, Any], context: dict[str, Any], *, allow_multiple: bool) -> list[Scope]:
        scope_payload = payload.get("scope")
        scope_details = dict(scope_payload or {}) if isinstance(scope_payload, dict) else {}
        scope_name = str(
            scope_details.get("name")
            or scope_details.get("type")
            or scope_payload
            or payload.get("scope_name")
            or context.get("memory_scope")
            or "agent"
        ).strip().lower()
        workspace_id = str(
            scope_details.get("workspace_id") or payload.get("workspace_id") or context.get("workspace_id") or "local-workspace"
        )
        project_id = str(
            scope_details.get("project_id") or payload.get("project_id") or context.get("project_id") or "default-project"
        )
        agent_id = self._optional_text(scope_details.get("agent_id")) or self._optional_text(payload.get("agent_id")) or self._optional_text(context.get("agent_id"))
        run_id = self._optional_text(scope_details.get("run_id")) or self._optional_text(payload.get("run_id")) or self._optional_text(context.get("run_id"))
        team_id = self._optional_text(scope_details.get("team_id")) or self._optional_text(payload.get("team_id")) or self._optional_text(context.get("team_id"))
        user_id = self._optional_text(scope_details.get("user_id")) or self._optional_text(payload.get("user_id")) or self._optional_text(context.get("user_id"))
        scopes: list[Scope] = []
        if scope_name in {"all", "combined", "default"}:
            if agent_id:
                scopes.append(
                    Scope(
                        workspace_id=workspace_id,
                        project_id=project_id,
                        namespace="agent_private",
                        agent_id=agent_id,
                        team_id=team_id,
                        user_id=user_id,
                    )
                )
            if team_id:
                scopes.append(
                    Scope(
                        workspace_id=workspace_id,
                        project_id=project_id,
                        namespace="team_shared",
                        team_id=team_id,
                        user_id=user_id,
                    )
                )
            scopes.append(
                Scope(
                    workspace_id=workspace_id,
                    project_id=project_id,
                    namespace="project_shared",
                    team_id=team_id,
                    user_id=user_id,
                )
            )
            if run_id:
                scopes.append(
                    Scope(
                        workspace_id=workspace_id,
                        project_id=project_id,
                        namespace="run_retrospective",
                        run_id=run_id,
                        team_id=team_id,
                        user_id=user_id,
                    )
                )
        elif scope_name == "team":
            scopes.append(
                Scope(
                    workspace_id=workspace_id,
                    project_id=project_id,
                    namespace="team_shared",
                    team_id=team_id,
                    user_id=user_id,
                )
            )
        elif scope_name == "project":
            scopes.append(
                Scope(
                    workspace_id=workspace_id,
                    project_id=project_id,
                    namespace="project_shared",
                    team_id=team_id,
                    user_id=user_id,
                )
            )
        elif scope_name in {"run", "retrospective"}:
            scopes.append(
                Scope(
                    workspace_id=workspace_id,
                    project_id=project_id,
                    namespace="run_retrospective",
                    run_id=run_id,
                    team_id=team_id,
                    user_id=user_id,
                )
            )
        elif scope_name == "working":
            scopes.append(
                Scope(
                    workspace_id=workspace_id,
                    project_id=project_id,
                    namespace="working",
                    agent_id=agent_id,
                    run_id=run_id,
                    team_id=team_id,
                    user_id=user_id,
                    session_id=run_id,
                )
            )
        else:
            scopes.append(
                Scope(
                    workspace_id=workspace_id,
                    project_id=project_id,
                    namespace="agent_private",
                    agent_id=agent_id,
                    team_id=team_id,
                    user_id=user_id,
                )
            )
        if not allow_multiple:
            return [scopes[0]]
        deduped: list[Scope] = []
        seen: set[str] = set()
        for scope in scopes:
            if scope.key in seen:
                continue
            seen.add(scope.key)
            deduped.append(scope)
        return deduped or [
            Scope(
                workspace_id=workspace_id,
                project_id=project_id,
                namespace="agent_private",
                agent_id=agent_id,
                team_id=team_id,
                user_id=user_id,
            )
        ]

    def _optional_text(self, value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None
