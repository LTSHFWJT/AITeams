from __future__ import annotations

from typing import Any

__all__ = ["PluginManager", "build_plugin_base_tool", "describe_plugin_base_tools", "plugin_action_args_schema", "sanitize_tool_name"]


def __getattr__(name: str) -> Any:
    if name == "PluginManager":
        from aiteams.plugins.manager import PluginManager

        return PluginManager
    if name in {"build_plugin_base_tool", "describe_plugin_base_tools", "plugin_action_args_schema", "sanitize_tool_name"}:
        from aiteams.plugins.tool_adapter import (
            build_plugin_base_tool,
            describe_plugin_base_tools,
            plugin_action_args_schema,
            sanitize_tool_name,
        )

        exports = {
            "build_plugin_base_tool": build_plugin_base_tool,
            "describe_plugin_base_tools": describe_plugin_base_tools,
            "plugin_action_args_schema": plugin_action_args_schema,
            "sanitize_tool_name": sanitize_tool_name,
        }
        return exports[name]
    raise AttributeError(f"module 'aiteams.plugins' has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
