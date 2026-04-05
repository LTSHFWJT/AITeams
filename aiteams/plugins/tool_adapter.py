from __future__ import annotations

import json
from typing import Any, Callable, Literal

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import Field, create_model


def sanitize_tool_name(raw_name: str, *, fallback: str) -> str:
    sanitized = "".join(char.lower() if char.isalnum() else "_" for char in str(raw_name or "").strip()).strip("_")
    if sanitized:
        return sanitized[:64]
    return fallback[:64]


def _json_schema_annotation(schema: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    payload = dict(schema or {})
    enum_values = payload.get("enum")
    if isinstance(enum_values, list) and enum_values:
        try:
            return Literal.__getitem__(tuple(enum_values)), payload
        except Exception:
            pass
    schema_type = str(payload.get("type") or "").strip().lower()
    if schema_type == "string":
        return str, payload
    if schema_type == "integer":
        return int, payload
    if schema_type == "number":
        return float, payload
    if schema_type == "boolean":
        return bool, payload
    if schema_type == "array":
        item_schema = payload.get("items")
        if isinstance(item_schema, dict):
            item_type, _extras = _json_schema_annotation(item_schema)
        else:
            item_type = Any
        return list[item_type], payload
    if schema_type == "object":
        return dict[str, Any], payload
    return Any, payload


def plugin_action_args_schema(tool_name: str, action: dict[str, Any]) -> type | None:
    input_schema = dict(action.get("input_schema") or {})
    if str(input_schema.get("type") or "").strip().lower() != "object":
        return None
    properties = dict(input_schema.get("properties") or {})
    if not properties:
        return None
    required = {str(item).strip() for item in list(input_schema.get("required") or []) if str(item).strip()}
    fields: dict[str, tuple[Any, Any]] = {}
    for raw_name, raw_schema in properties.items():
        field_name = str(raw_name or "").strip()
        if not field_name:
            continue
        schema = dict(raw_schema or {}) if isinstance(raw_schema, dict) else {}
        annotation, extras = _json_schema_annotation(schema)
        description = str(extras.get("description") or "").strip() or None
        default = extras.get("default", ...)
        if field_name not in required:
            annotation = annotation | None
            if default is ...:
                default = None
        fields[field_name] = (
            annotation,
            Field(
                default=default,
                description=description,
                json_schema_extra={key: extras[key] for key in ("enum", "examples") if key in extras},
            ),
        )
    if not fields:
        return None
    model_name = "".join(part.capitalize() for part in sanitize_tool_name(tool_name, fallback="plugin_tool").split("_")) or "PluginTool"
    return create_model(f"{model_name}Args", **fields)


def build_plugin_base_tool(
    *,
    plugin_key: str,
    action: dict[str, Any],
    fallback_tool_name: str,
    invoker: Callable[[dict[str, Any]], str],
) -> BaseTool:
    action_name = str(action.get("name") or "").strip()
    tool_name = sanitize_tool_name(str(action.get("tool_name") or ""), fallback=fallback_tool_name)
    args_schema = plugin_action_args_schema(tool_name, action)
    tool_description = str(action.get("description") or f"Invoke plugin `{plugin_key}` action `{action_name}`.")

    if args_schema is not None:
        async def _invoke_plugin_structured(**kwargs: Any) -> str:
            payload = {key: value for key, value in kwargs.items() if value is not None}
            return invoker(payload)

        return StructuredTool.from_function(
            name=tool_name,
            coroutine=_invoke_plugin_structured,
            description=tool_description,
            args_schema=args_schema,
            infer_schema=False,
        )

    async def _invoke_plugin_legacy(payload_json: str = "{}") -> str:
        try:
            payload = json.loads(payload_json) if payload_json.strip() else {}
        except json.JSONDecodeError as exc:
            return f"Invalid payload_json: {exc}"
        return invoker(payload if isinstance(payload, dict) else {"value": payload})

    return StructuredTool.from_function(
        name=tool_name,
        coroutine=_invoke_plugin_legacy,
        description=tool_description,
    )


def describe_plugin_base_tools(*, plugin_key: str, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    descriptions: list[dict[str, Any]] = []
    manifest_payload = dict(manifest.get("manifest") or manifest)
    actions = list(manifest_payload.get("actions") or [])
    for index, raw_action in enumerate(actions, start=1):
        action = dict(raw_action or {})
        action_name = str(action.get("name") or "").strip()
        if not action_name:
            continue
        fallback_tool_name = f"plugin_{sanitize_tool_name(plugin_key, fallback='plugin')}_{index}"
        tool = build_plugin_base_tool(
            plugin_key=plugin_key,
            action=action,
            fallback_tool_name=fallback_tool_name,
            invoker=lambda _payload: "",
        )
        args_schema = getattr(tool, "args_schema", None)
        schema_json = None
        if args_schema is not None and hasattr(args_schema, "model_json_schema"):
            schema_json = args_schema.model_json_schema()
        descriptions.append(
            {
                "tool_name": str(getattr(tool, "name", "") or fallback_tool_name),
                "action_name": action_name,
                "description": str(getattr(tool, "description", "") or action.get("description") or "").strip(),
                "mode": "structured" if plugin_action_args_schema(str(getattr(tool, "name", "") or fallback_tool_name), action) is not None else "legacy",
                "args_schema": schema_json,
            }
        )
    return descriptions
