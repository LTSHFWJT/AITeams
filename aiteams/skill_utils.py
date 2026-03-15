from __future__ import annotations

import re
from pathlib import PurePosixPath


_TEXT_SUFFIXES = {
    ".c",
    ".cfg",
    ".conf",
    ".cpp",
    ".css",
    ".csv",
    ".env",
    ".go",
    ".h",
    ".html",
    ".ini",
    ".java",
    ".js",
    ".json",
    ".jsx",
    ".md",
    ".mjs",
    ".py",
    ".rb",
    ".rs",
    ".sh",
    ".sql",
    ".svg",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}

_YAML_KEY_PATTERN = re.compile(r"^([A-Za-z0-9_-]+)\s*:\s*(.*)$")


def parse_skill_frontmatter(markdown: str) -> tuple[dict[str, str], str]:
    text = (markdown or "").replace("\r\n", "\n").lstrip("\ufeff")
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end < 0:
        return {}, text
    block = text[4:end]
    body = text[end + 5 :]
    metadata: dict[str, str] = {}
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _YAML_KEY_PATTERN.match(line)
        if not match:
            continue
        key = match.group(1).strip()
        value = match.group(2).strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        metadata[key] = value
    return metadata, body


def summarize_skill_body(markdown: str, *, fallback: str = "Imported skill.") -> str:
    _, body = parse_skill_frontmatter(markdown)
    lines: list[str] = []
    in_code_block = False
    for raw_line in body.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block or not stripped:
            continue
        if stripped.startswith("#"):
            continue
        cleaned = re.sub(r"^[>\-\*\d\.\)\s]+", "", stripped).strip()
        if cleaned:
            lines.append(cleaned)
        if len(" ".join(lines)) >= 220:
            break
    if not lines:
        return fallback
    summary = " ".join(lines).strip()
    return summary[:217].rstrip() + "..." if len(summary) > 220 else summary


def slugify_skill_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "-", value or "").strip("-").lower()
    return normalized or "untitled-skill"


def humanize_skill_name(value: str) -> str:
    words = [part for part in re.split(r"[-_]+", value or "") if part]
    if not words:
        return "Untitled Skill"
    return " ".join(word[:1].upper() + word[1:] for word in words)


def yaml_scalar(value: str) -> str:
    escaped = (value or "").replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def ensure_skill_markdown(markdown: str, *, name: str, description: str) -> str:
    _, body = parse_skill_frontmatter(markdown)
    frontmatter = "\n".join(
        [
            "---",
            f"name: {yaml_scalar(name)}",
            f"description: {yaml_scalar(description)}",
            "---",
            "",
        ]
    )
    normalized_body = body.lstrip("\n")
    return frontmatter + normalized_body


def resolve_skill_identity(
    *,
    name: str | None,
    description: str | None,
    skill_markdown: str,
    folder_name: str | None = None,
) -> tuple[str, str, str]:
    frontmatter, _ = parse_skill_frontmatter(skill_markdown)
    resolved_name = (name or frontmatter.get("name") or "").strip()
    if not resolved_name:
        resolved_name = humanize_skill_name(folder_name or "untitled-skill")
    resolved_description = (description or frontmatter.get("description") or "").strip()
    if not resolved_description:
        resolved_description = summarize_skill_body(skill_markdown, fallback=f"Imported skill from {folder_name or resolved_name}.")
    normalized_markdown = ensure_skill_markdown(skill_markdown, name=resolved_name, description=resolved_description)
    return resolved_name, resolved_description, normalized_markdown


def normalize_asset_path(path: str) -> str:
    normalized = str(path or "").replace("\\", "/").strip().lstrip("/")
    normalized = str(PurePosixPath(normalized))
    if normalized in {".", ""}:
        raise ValueError("Asset path is required.")
    return normalized


def asset_category_from_path(path: str, *, fallback: str | None = None) -> str:
    normalized = normalize_asset_path(path)
    head = PurePosixPath(normalized).parts[0].lower()
    if head in {"references", "scripts", "assets", "templates", "agents"}:
        return head
    return fallback or "other"


def is_text_asset(path: str, mime_type: str | None = None) -> bool:
    if mime_type:
        lowered = mime_type.lower()
        if lowered.startswith("text/"):
            return True
        if lowered in {
            "application/json",
            "application/ld+json",
            "application/toml",
            "application/x-httpd-php",
            "application/x-sh",
            "application/x-yaml",
            "application/xml",
            "image/svg+xml",
        }:
            return True
    return PurePosixPath(path).suffix.lower() in _TEXT_SUFFIXES


def summarize_asset_manifest(assets: list[dict[str, object]]) -> dict[str, int]:
    counts = {"references": 0, "scripts": 0, "templates": 0, "assets": 0, "agents": 0, "other": 0}
    for item in assets:
        category = str(item.get("category") or "other")
        counts[category] = counts.get(category, 0) + 1
    counts["total"] = len(assets)
    return counts
