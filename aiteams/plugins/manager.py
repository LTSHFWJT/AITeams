from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from aiteams.plugins.manifest import PLUGIN_MANIFEST, validate_plugin_package
from aiteams.plugins.sandbox import PluginSandbox
from aiteams.storage.metadata import MetadataStore


class PluginManager:
    def __init__(self, *, store: MetadataStore, install_root: str | Path):
        self.store = store
        self.install_root = Path(install_root).expanduser().resolve()
        self.install_root.mkdir(parents=True, exist_ok=True)
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
        manifest = validate_plugin_package(source_path)
        target = self.install_root / manifest["key"] / manifest["version"]
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(Path(source_path).expanduser().resolve(), target)
        installed_manifest = validate_plugin_package(target)
        saved = self.store.save_plugin(
            plugin_id=str(plugin["id"]),
            key=str(plugin["key"]),
            name=str(plugin["name"]),
            version=str(plugin["version"]),
            plugin_type=str(plugin["plugin_type"]),
            description=str(plugin.get("description") or installed_manifest.get("description") or ""),
            manifest=installed_manifest,
            install_path=str(target),
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
        sandbox = self._sandbox_for_ref(plugin_ref)
        return sandbox.invoke(action, payload, context)

    def _sandbox_for_record(self, plugin: dict[str, Any], *, force_reload: bool = False) -> PluginSandbox:
        ref = {
            "id": str(plugin["id"]),
            "key": str(plugin.get("key") or ""),
            "version": str(plugin.get("version") or "v1"),
            "install_path": plugin.get("install_path"),
            "manifest": dict(plugin.get("manifest_json") or {}),
        }
        return self._sandbox_for_ref(ref, force_reload=force_reload)

    def _sandbox_for_ref(self, plugin_ref: dict[str, Any], *, force_reload: bool = False) -> PluginSandbox:
        plugin_id = str(plugin_ref.get("id") or "")
        if not plugin_id:
            raise ValueError("Plugin reference requires id.")
        manifest = dict(plugin_ref.get("manifest") or plugin_ref.get("manifest_json") or {})
        install_path = str(plugin_ref.get("install_path") or "").strip()
        if not install_path:
            record = self.store.get_plugin(plugin_id)
            if record is None or not record.get("install_path"):
                raise ValueError(f"Plugin `{plugin_id}` has no executable install_path.")
            install_path = str(record["install_path"])
            if not manifest:
                manifest = dict(record.get("manifest_json") or {})
        root = Path(install_path).expanduser().resolve()
        if not root.exists() or not (root / PLUGIN_MANIFEST).exists():
            raise ValueError(f"Plugin package `{install_path}` does not exist or is missing `{PLUGIN_MANIFEST}`.")
        loaded_manifest = validate_plugin_package(root)
        sandbox = self._sandboxes.get(plugin_id)
        if sandbox is None:
            sandbox = PluginSandbox(plugin_id=plugin_id, manifest=loaded_manifest, root_path=root)
            self._sandboxes[plugin_id] = sandbox
        else:
            sandbox.manifest = loaded_manifest
            sandbox.root_path = root
            if force_reload:
                sandbox.restart()
        return sandbox

    def _require_plugin(self, plugin_id: str) -> dict[str, Any]:
        plugin = self.store.get_plugin(plugin_id)
        if plugin is None:
            raise ValueError("Plugin does not exist.")
        return plugin
