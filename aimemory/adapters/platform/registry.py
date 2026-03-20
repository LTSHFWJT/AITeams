from __future__ import annotations

from collections.abc import Callable
from typing import Any


PlatformLLMFactory = Callable[[dict[str, Any]], Any]

_PLATFORM_LLM_PLUGINS: dict[str, PlatformLLMFactory] = {}


def _normalize_plugin_name(name: str) -> str:
    normalized = str(name or "").strip()
    if not normalized:
        raise ValueError("platform LLM plugin name must be a non-empty string")
    return normalized


def register_platform_llm_plugin(name: str, factory: PlatformLLMFactory, *, overwrite: bool = True) -> None:
    plugin_name = _normalize_plugin_name(name)
    if not callable(factory):
        raise TypeError("platform LLM plugin factory must be callable")
    if not overwrite and plugin_name in _PLATFORM_LLM_PLUGINS:
        raise ValueError(f"platform LLM plugin `{plugin_name}` is already registered")
    _PLATFORM_LLM_PLUGINS[plugin_name] = factory


def unregister_platform_llm_plugin(name: str) -> None:
    plugin_name = _normalize_plugin_name(name)
    _PLATFORM_LLM_PLUGINS.pop(plugin_name, None)


def get_platform_llm_plugin(name: str) -> PlatformLLMFactory:
    plugin_name = _normalize_plugin_name(name)
    factory = _PLATFORM_LLM_PLUGINS.get(plugin_name)
    if factory is None:
        raise ValueError(f"unknown platform LLM plugin `{plugin_name}`")
    return factory


def create_platform_llm_plugin(name: str, config: dict[str, Any] | None = None) -> Any:
    return get_platform_llm_plugin(name)(dict(config or {}))


def list_platform_llm_plugins() -> list[str]:
    return sorted(_PLATFORM_LLM_PLUGINS)
