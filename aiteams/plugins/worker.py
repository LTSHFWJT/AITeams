from __future__ import annotations

import contextlib
import importlib
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

from aiteams.plugins.manifest import load_plugin_manifest


class DefaultPluginRuntime:
    def __init__(self, *, manifest: dict[str, Any], root_path: Path):
        self.manifest = manifest
        self.root_path = root_path

    def describe(self) -> dict[str, Any]:
        return {
            "key": self.manifest["key"],
            "name": self.manifest["name"],
            "version": self.manifest["version"],
            "tools": self.manifest.get("tools", []),
            "permissions": self.manifest.get("permissions", []),
            "actions": self.manifest.get("actions", []),
            "description": self.manifest.get("description", ""),
        }

    def health(self) -> dict[str, Any]:
        return {"status": "ok"}

    def invoke(self, action: str, payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        return {
            "ok": True,
            "action": action,
            "payload": payload,
            "context_keys": sorted(context.keys()),
        }

    def shutdown(self) -> dict[str, Any]:
        return {"status": "stopping"}


def _load_runtime(root_path: Path) -> tuple[dict[str, Any], Any]:
    manifest = load_plugin_manifest(root_path)
    runtime_config = _load_json_env("AITEAMS_PLUGIN_RUNTIME_CONFIG")
    runtime_secret = _load_json_env("AITEAMS_PLUGIN_RUNTIME_SECRET")
    runtime = _deep_merge_dicts(dict(manifest.get("runtime") or {}), runtime_config)
    runtime = _deep_merge_dicts(runtime, runtime_secret)
    manifest["runtime"] = runtime
    manifest["runtime_config"] = runtime_config
    manifest["runtime_secret"] = runtime_secret
    _purge_bytecode(root_path)
    importlib.invalidate_caches()
    sys.path.insert(0, str(root_path))
    module_name, object_name = str(manifest["entrypoint"]).split(":", 1)
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    factory = getattr(module, object_name)
    if isinstance(factory, type):
        runtime = factory(manifest=manifest, root_path=str(root_path))
    else:
        runtime = factory(manifest=manifest, root_path=str(root_path))
    if runtime is None:
        runtime = DefaultPluginRuntime(manifest=manifest, root_path=root_path)
    return manifest, runtime


def _purge_bytecode(root_path: Path) -> None:
    for cache_dir in root_path.rglob("__pycache__"):
        if cache_dir.is_dir():
            shutil.rmtree(cache_dir, ignore_errors=True)


def _load_json_env(name: str) -> dict[str, Any]:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _deep_merge_dicts(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base or {})
    for key, value in dict(updates or {}).items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dicts(existing, value)
        else:
            merged[key] = value
    return merged


def _result(payload_id: str, ok: bool, result: dict[str, Any] | None = None, error: str | None = None) -> dict[str, Any]:
    data = {"id": payload_id, "ok": ok}
    if ok:
        data["result"] = result or {}
    else:
        data["error"] = error or "Plugin worker request failed."
    return data


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m aiteams.plugins.worker <plugin-root>")
    root_path = Path(sys.argv[1]).expanduser().resolve()
    with contextlib.redirect_stdout(sys.stderr):
        manifest, runtime = _load_runtime(root_path)
    fallback = DefaultPluginRuntime(manifest=manifest, root_path=root_path)

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
            payload_id = str(payload.get("id") or "")
            method = str(payload.get("method") or "").strip()
            with contextlib.redirect_stdout(sys.stderr):
                if method == "describe":
                    handler = getattr(runtime, "describe", None) or fallback.describe
                    response = _result(payload_id, True, dict(handler() or {}))
                elif method == "health":
                    handler = getattr(runtime, "health", None) or fallback.health
                    response = _result(payload_id, True, dict(handler() or {}))
                elif method == "invoke":
                    handler = getattr(runtime, "invoke", None) or fallback.invoke
                    result = handler(
                        str(payload.get("action") or ""),
                        dict(payload.get("payload") or {}),
                        dict(payload.get("context") or {}),
                    )
                    response = _result(payload_id, True, dict(result or {}))
                elif method == "shutdown":
                    handler = getattr(runtime, "shutdown", None) or fallback.shutdown
                    response = _result(payload_id, True, dict(handler() or {}))
                    print(json.dumps(response, ensure_ascii=False), flush=True)
                    break
                else:
                    response = _result(payload_id, False, error=f"Unknown method `{method}`.")
        except Exception as exc:
            response = _result(str((locals().get("payload") or {}).get("id") or ""), False, error=str(exc))
        print(json.dumps(response, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
