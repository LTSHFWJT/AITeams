from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


PLUGIN_MANIFEST = "plugin.yaml"


def load_plugin_manifest(path: str | Path) -> dict[str, Any]:
    root = Path(path).expanduser().resolve()
    manifest_path = root / PLUGIN_MANIFEST
    if not manifest_path.exists():
        raise ValueError(f"Plugin package `{root}` is missing `{PLUGIN_MANIFEST}`.")
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("plugin.yaml must be an object.")
    return normalize_plugin_manifest(payload, root)


def normalize_plugin_manifest(payload: dict[str, Any], root: str | Path | None = None) -> dict[str, Any]:
    manifest = dict(payload or {})
    key = str(manifest.get("key") or "").strip()
    name = str(manifest.get("name") or "").strip()
    version = str(manifest.get("version") or "v1").strip()
    entrypoint = str(manifest.get("entrypoint") or "").strip()
    if not key:
        raise ValueError("Plugin manifest requires package slug `key`.")
    if not name:
        raise ValueError("Plugin manifest requires name.")
    if not version:
        raise ValueError("Plugin manifest requires version.")
    if ":" not in entrypoint:
        raise ValueError("Plugin manifest requires entrypoint in `module.path:object` format.")
    module_name, object_name = entrypoint.split(":", 1)
    if not module_name.strip() or not object_name.strip():
        raise ValueError("Plugin manifest entrypoint is invalid.")

    normalized = {
        "key": key,
        "name": name,
        "version": version,
        "plugin_type": str(manifest.get("plugin_type") or "toolset"),
        "description": str(manifest.get("description") or ""),
        "entrypoint": entrypoint,
        "workbench_key": str(manifest.get("workbench_key") or key),
        "tools": [str(item) for item in manifest.get("tools", [])],
        "permissions": [str(item) for item in manifest.get("permissions", [])],
        "actions": _normalize_actions(manifest.get("actions") or []),
        "config_schema": manifest.get("config_schema"),
        "hot_reload": bool(manifest.get("hot_reload", True)),
        "runtime": dict(manifest.get("runtime") or {}),
        "requirements": [str(item) for item in manifest.get("requirements", [])],
    }
    if root is not None:
        normalized["root_path"] = str(Path(root).expanduser().resolve())
        normalized["manifest_path"] = str((Path(root).expanduser().resolve() / PLUGIN_MANIFEST))
        normalized["entry_module"] = module_name.strip()
        normalized["entry_object"] = object_name.strip()
        normalized["entry_exists"] = _entrypoint_exists(Path(root).expanduser().resolve(), module_name.strip())
        normalized["fingerprint"] = compute_plugin_fingerprint(root)
    return normalized


def compute_plugin_fingerprint(path: str | Path) -> str:
    root = Path(path).expanduser().resolve()
    values: list[str] = []
    for item in sorted(root.rglob("*")):
        if item.is_dir():
            continue
        if item.name == "__pycache__":
            continue
        stat = item.stat()
        values.append(f"{item.relative_to(root).as_posix()}:{stat.st_mtime_ns}:{stat.st_size}")
    return "|".join(values)


def validate_plugin_package(path: str | Path) -> dict[str, Any]:
    manifest = load_plugin_manifest(path)
    if not manifest.get("entry_exists"):
        raise ValueError(f"Plugin entry module `{manifest['entry_module']}` does not exist under package root.")
    return manifest


def _normalize_actions(items: list[Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, str):
            actions.append({"name": item, "description": ""})
            continue
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
            if name:
                actions.append({"name": name, "description": str(item.get("description") or "")})
    return actions


def _entrypoint_exists(root: Path, module_name: str) -> bool:
    module_path = root / Path(*module_name.split("."))
    return module_path.with_suffix(".py").exists() or (module_path / "__init__.py").exists()
