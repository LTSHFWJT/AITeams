from __future__ import annotations

import base64
import json
import mimetypes
from pathlib import PurePosixPath
from typing import Any


TEXTUAL_EXTENSIONS = {
    ".md",
    ".markdown",
    ".txt",
    ".py",
    ".sh",
    ".bash",
    ".zsh",
    ".js",
    ".ts",
    ".json",
    ".json5",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".xml",
    ".csv",
    ".sql",
    ".html",
    ".css",
}

TEXTUAL_MIME_PREFIXES = ("text/",)
TEXTUAL_MIME_VALUES = {
    "application/json",
    "application/ld+json",
    "application/x-yaml",
    "application/yaml",
    "application/xml",
    "application/javascript",
    "application/x-sh",
    "application/x-python-code",
}


def looks_like_skill_file_mapping(value: Any) -> bool:
    if isinstance(value, list):
        return True
    if not isinstance(value, dict) or not value:
        return False
    return all(_looks_like_relative_path(str(key)) for key in value)


def normalize_skill_package_inputs(
    *,
    name: str,
    description: str,
    prompt_template: str | None = None,
    workflow: Any = None,
    tools: list[str] | None = None,
    topics: list[str] | None = None,
    skill_markdown: str | None = None,
    base_files: list[dict[str, Any]] | None = None,
    files: list[dict[str, Any]] | None = None,
    references: dict[str, Any] | list[dict[str, Any]] | None = None,
    scripts: dict[str, Any] | list[dict[str, Any]] | None = None,
    assets: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}

    for item in base_files or []:
        normalized_item = _normalize_skill_file_item(item, default_role=None)
        normalized[normalized_item["relative_path"]] = normalized_item
    for item in _expand_mapping_entries(references, default_role="reference"):
        normalized[item["relative_path"]] = item
    for item in _expand_mapping_entries(scripts, default_role="script"):
        normalized[item["relative_path"]] = item
    for item in _expand_mapping_entries(assets, default_role="asset"):
        normalized[item["relative_path"]] = item
    for item in files or []:
        normalized_item = _normalize_skill_file_item(item, default_role=None)
        normalized[normalized_item["relative_path"]] = normalized_item

    if skill_markdown is not None:
        normalized["SKILL.md"] = _normalize_skill_file_item(
            {"path": "SKILL.md", "content": skill_markdown, "role": "skill_md", "metadata": {"generated": False}},
            default_role="skill_md",
        )
    elif "SKILL.md" not in normalized:
        normalized["SKILL.md"] = _normalize_skill_file_item(
            {
                "path": "SKILL.md",
                "content": default_skill_markdown(
                    name=name,
                    description=description,
                    prompt_template=prompt_template,
                    workflow=workflow,
                    tools=tools or [],
                    topics=topics or [],
                ),
                "role": "skill_md",
                "metadata": {"generated": True},
            },
            default_role="skill_md",
        )

    return [normalized[path] for path in sorted(normalized)]


def default_skill_markdown(
    *,
    name: str,
    description: str,
    prompt_template: str | None = None,
    workflow: Any = None,
    tools: list[str] | None = None,
    topics: list[str] | None = None,
) -> str:
    lines = [
        "---",
        f"name: {json.dumps(str(name), ensure_ascii=False)}",
        f"description: {json.dumps(str(description), ensure_ascii=False)}",
        "---",
        "",
        f"# {name}",
        "",
        str(description).strip(),
    ]
    if prompt_template:
        lines.extend(["", "## Prompt Template", "", str(prompt_template).strip()])
    if workflow not in (None, "", {}, []):
        workflow_text = workflow if isinstance(workflow, str) else json.dumps(workflow, ensure_ascii=False, indent=2, sort_keys=True)
        lines.extend(["", "## Workflow", "", "```json" if not isinstance(workflow, str) else "```", str(workflow_text).strip(), "```"])
    if topics:
        lines.extend(["", "## Topics", ""])
        lines.extend([f"- {topic}" for topic in topics if str(topic).strip()])
    if tools:
        lines.extend(["", "## Tools", ""])
        lines.extend([f"- {tool}" for tool in tools if str(tool).strip()])
    return "\n".join(lines).strip() + "\n"


def is_textual_skill_file(
    *,
    relative_path: str,
    mime_type: str | None = None,
    role: str | None = None,
) -> bool:
    if mime_type:
        normalized_mime = mime_type.strip().lower()
        if normalized_mime.startswith(TEXTUAL_MIME_PREFIXES):
            return True
        if normalized_mime in TEXTUAL_MIME_VALUES:
            return True
    suffix = PurePosixPath(relative_path).suffix.lower()
    if suffix in TEXTUAL_EXTENSIONS:
        return True
    return str(role or "").strip().lower() in {"skill_md", "reference", "script"}


def guess_skill_file_mime_type(relative_path: str, role: str | None = None) -> str:
    guessed, _encoding = mimetypes.guess_type(relative_path)
    if guessed:
        return guessed
    normalized_role = str(role or "").strip().lower()
    suffix = PurePosixPath(relative_path).suffix.lower()
    if normalized_role == "skill_md" or suffix in {".md", ".markdown"}:
        return "text/markdown"
    if normalized_role == "reference":
        return "text/plain"
    if normalized_role == "script":
        return "text/plain"
    if suffix in {".json", ".json5"}:
        return "application/json"
    if suffix in {".yaml", ".yml"}:
        return "application/x-yaml"
    return "application/octet-stream"


def _expand_mapping_entries(
    value: dict[str, Any] | list[dict[str, Any]] | None,
    *,
    default_role: str,
) -> list[dict[str, Any]]:
    if value is None:
        return []
    items: list[dict[str, Any]] = []
    if isinstance(value, dict):
        for key, raw in value.items():
            if isinstance(raw, dict):
                item = dict(raw)
                item.setdefault("path", key)
            else:
                item = {"path": key, "content": raw}
            items.append(_normalize_skill_file_item(item, default_role=default_role))
        return items
    for raw in value:
        items.append(_normalize_skill_file_item(raw, default_role=default_role))
    return items


def _normalize_skill_file_item(item: dict[str, Any], *, default_role: str | None) -> dict[str, Any]:
    relative_path = _normalize_relative_path(item.get("relative_path") or item.get("path"))
    role = _normalize_role(item.get("role") or default_role, relative_path)
    mime_type = str(item.get("mime_type") or guess_skill_file_mime_type(relative_path, role)).strip()
    content_bytes, text_content = _normalize_content(item, relative_path=relative_path, mime_type=mime_type, role=role)
    indexable = bool(item.get("index")) if "index" in item else role == "reference"
    return {
        "relative_path": relative_path,
        "role": role,
        "mime_type": mime_type,
        "content_bytes": content_bytes,
        "text_content": text_content,
        "metadata": dict(item.get("metadata") or {}),
        "indexable": bool(indexable and is_textual_skill_file(relative_path=relative_path, mime_type=mime_type, role=role)),
    }


def _normalize_content(
    item: dict[str, Any],
    *,
    relative_path: str,
    mime_type: str,
    role: str,
) -> tuple[bytes, str | None]:
    if "content_base64" in item and item.get("content_base64") not in (None, ""):
        raw = base64.b64decode(str(item["content_base64"]))
        if is_textual_skill_file(relative_path=relative_path, mime_type=mime_type, role=role):
            try:
                return raw, raw.decode("utf-8")
            except UnicodeDecodeError:
                return raw, None
        return raw, None
    content = item.get("content", "")
    if isinstance(content, bytes):
        if is_textual_skill_file(relative_path=relative_path, mime_type=mime_type, role=role):
            try:
                return content, content.decode("utf-8")
            except UnicodeDecodeError:
                return content, None
        return content, None
    if content is None:
        content = ""
    text = str(content)
    return text.encode("utf-8"), text


def _normalize_relative_path(raw: Any) -> str:
    path_text = str(raw or "").strip().replace("\\", "/")
    if not path_text:
        raise ValueError("Skill file path is required.")
    if path_text.startswith("/"):
        raise ValueError(f"Skill file path `{path_text}` must be relative.")
    normalized = PurePosixPath(path_text)
    if normalized.is_absolute():
        raise ValueError(f"Skill file path `{path_text}` must be relative.")
    parts = normalized.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Skill file path `{path_text}` is invalid.")
    return str(normalized)


def _normalize_role(raw: Any, relative_path: str) -> str:
    text = str(raw or "").strip().lower().replace("-", "_")
    if not text:
        inferred = _infer_role(relative_path)
        return inferred or "asset"
    if text in {"skill", "skill_md", "skillmd"}:
        return "skill_md"
    if text in {"reference", "references"}:
        return "reference"
    if text in {"script", "scripts"}:
        return "script"
    if text in {"asset", "assets"}:
        return "asset"
    return text


def _infer_role(relative_path: str) -> str | None:
    normalized = relative_path.lower()
    if normalized == "skill.md":
        return "skill_md"
    if normalized.startswith("references/"):
        return "reference"
    if normalized.startswith("scripts/"):
        return "script"
    if normalized.startswith("assets/"):
        return "asset"
    return None


def _looks_like_relative_path(value: str) -> bool:
    text = value.strip().replace("\\", "/")
    if not text or text.startswith("/"):
        return False
    if text.upper() == "SKILL.MD":
        return True
    return "/" in text or "." in PurePosixPath(text).name
