from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml


MAX_SKILL_FILE_SIZE = 10 * 1024 * 1024
MAX_SKILL_NAME_LENGTH = 64
MAX_SKILL_DESCRIPTION_LENGTH = 1024
MAX_SKILL_COMPATIBILITY_LENGTH = 500

_SKILL_FRONTMATTER_PATTERN = re.compile(r"^---\s*\n(.*?)\n---\s*(?:\n|$)", re.DOTALL)

IssueSeverity = Literal["error", "warning"]


@dataclass(slots=True, frozen=True)
class SkillValidationIssue:
    severity: IssueSeverity
    code: str
    message: str
    path: str


@dataclass(slots=True, frozen=True)
class SkillMetadata:
    name: str
    description: str
    path: str
    license: str | None = None
    compatibility: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    allowed_tools: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ValidatedSkill:
    directory_path: Path
    skill_md_path: Path
    files: list[Path] = field(default_factory=list)
    metadata: SkillMetadata | None = None
    content: str = ""
    body: str = ""
    issues: list[SkillValidationIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not any(item.severity == "error" for item in self.issues)

    @property
    def helper_files(self) -> list[Path]:
        return [item for item in self.files if item.name != "SKILL.md"]


@dataclass(slots=True)
class SkillLibraryScan:
    root_path: Path
    skills: list[ValidatedSkill] = field(default_factory=list)
    issues: list[SkillValidationIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        if any(item.severity == "error" for item in self.issues):
            return False
        return all(skill.is_valid for skill in self.skills)

    @property
    def valid_skills(self) -> list[ValidatedSkill]:
        return [item for item in self.skills if item.is_valid]


def is_skill_directory(path: str | Path) -> bool:
    skill_dir = Path(path).expanduser()
    return skill_dir.is_dir() and (skill_dir / "SKILL.md").is_file()


def discover_skill_directories(root_dir: str | Path, *, recursive: bool = True) -> list[Path]:
    root_path = Path(root_dir).expanduser().resolve()
    if not root_path.exists() or not root_path.is_dir():
        return []

    skill_dirs: set[Path] = set()
    if (root_path / "SKILL.md").is_file():
        skill_dirs.add(root_path)

    pattern = "**/SKILL.md" if recursive else "*/SKILL.md"
    for skill_md_path in root_path.glob(pattern):
        if skill_md_path.is_file():
            skill_dirs.add(skill_md_path.parent.resolve())

    return sorted(skill_dirs, key=lambda item: item.as_posix().lower())


def validate_skill_directory(skill_dir: str | Path) -> ValidatedSkill:
    directory_path = Path(skill_dir).expanduser().resolve()
    skill_md_path = directory_path / "SKILL.md"
    issues: list[SkillValidationIssue] = []
    files: list[Path] = []

    if not directory_path.exists():
        issues.append(_issue("error", "path-not-found", "Skill directory does not exist.", directory_path))
        return ValidatedSkill(directory_path=directory_path, skill_md_path=skill_md_path, issues=issues)

    if not directory_path.is_dir():
        issues.append(_issue("error", "not-a-directory", "Skill path must point to a directory.", directory_path))
        return ValidatedSkill(directory_path=directory_path, skill_md_path=skill_md_path, issues=issues)

    files = sorted(
        [item.relative_to(directory_path) for item in directory_path.rglob("*") if item.is_file()],
        key=lambda item: item.as_posix().lower(),
    )

    if not skill_md_path.is_file():
        issues.append(_issue("error", "missing-skill-md", "Skill directory must contain a SKILL.md file.", skill_md_path))
        return ValidatedSkill(directory_path=directory_path, skill_md_path=skill_md_path, files=files, issues=issues)

    try:
        raw = skill_md_path.read_bytes()
    except OSError as exc:
        issues.append(_issue("error", "read-failed", f"Failed to read SKILL.md: {exc}", skill_md_path))
        return ValidatedSkill(directory_path=directory_path, skill_md_path=skill_md_path, files=files, issues=issues)

    if len(raw) > MAX_SKILL_FILE_SIZE:
        issues.append(
            _issue(
                "error",
                "skill-file-too-large",
                f"SKILL.md exceeds {MAX_SKILL_FILE_SIZE} bytes.",
                skill_md_path,
            )
        )
        return ValidatedSkill(directory_path=directory_path, skill_md_path=skill_md_path, files=files, issues=issues)

    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        issues.append(_issue("error", "decode-failed", f"SKILL.md must be UTF-8 text: {exc}", skill_md_path))
        return ValidatedSkill(directory_path=directory_path, skill_md_path=skill_md_path, files=files, issues=issues)

    metadata, body, parse_issues = _parse_skill_markdown(
        content=content,
        skill_md_path=skill_md_path,
        directory_name=directory_path.name,
    )
    issues.extend(parse_issues)
    return ValidatedSkill(
        directory_path=directory_path,
        skill_md_path=skill_md_path,
        files=files,
        metadata=metadata,
        content=content,
        body=body,
        issues=issues,
    )


def scan_skill_library(root_dir: str | Path, *, recursive: bool = True) -> SkillLibraryScan:
    root_path = Path(root_dir).expanduser().resolve()
    issues: list[SkillValidationIssue] = []

    if not root_path.exists():
        issues.append(_issue("error", "path-not-found", "Skill library directory does not exist.", root_path))
        return SkillLibraryScan(root_path=root_path, issues=issues)
    if not root_path.is_dir():
        issues.append(_issue("error", "not-a-directory", "Skill library path must point to a directory.", root_path))
        return SkillLibraryScan(root_path=root_path, issues=issues)

    skill_dirs = discover_skill_directories(root_path, recursive=recursive)
    skills = [validate_skill_directory(item) for item in skill_dirs]

    seen_names: dict[str, Path] = {}
    for skill in skills:
        if skill.metadata is None:
            continue
        previous = seen_names.get(skill.metadata.name)
        if previous is None:
            seen_names[skill.metadata.name] = skill.directory_path
            continue
        issues.append(
            _issue(
                "warning",
                "duplicate-skill-name",
                f"Duplicate skill name '{skill.metadata.name}' found in '{previous}' and '{skill.directory_path}'.",
                skill.directory_path,
            )
        )

    if not skills:
        issues.append(_issue("warning", "no-skills-found", "No skills were found under the provided directory.", root_path))

    return SkillLibraryScan(root_path=root_path, skills=skills, issues=issues)


def _parse_skill_markdown(
    *,
    content: str,
    skill_md_path: Path,
    directory_name: str,
) -> tuple[SkillMetadata | None, str, list[SkillValidationIssue]]:
    issues: list[SkillValidationIssue] = []
    match = _SKILL_FRONTMATTER_PATTERN.match(content)
    if not match:
        issues.append(
            _issue(
                "error",
                "missing-frontmatter",
                "SKILL.md must start with YAML frontmatter delimited by --- lines.",
                skill_md_path,
            )
        )
        return None, "", issues

    frontmatter_text = match.group(1)
    body = content[match.end() :].lstrip("\n")
    try:
        frontmatter = yaml.safe_load(frontmatter_text)
    except yaml.YAMLError as exc:
        issues.append(_issue("error", "invalid-yaml", f"Invalid YAML frontmatter: {exc}", skill_md_path))
        return None, body, issues

    if not isinstance(frontmatter, dict):
        issues.append(_issue("error", "frontmatter-not-mapping", "YAML frontmatter must be a mapping object.", skill_md_path))
        return None, body, issues

    name = str(frontmatter.get("name", "")).strip()
    description = str(frontmatter.get("description", "")).strip()
    if not name or not description:
        issues.append(
            _issue(
                "error",
                "missing-required-fields",
                "YAML frontmatter must include non-empty 'name' and 'description'.",
                skill_md_path,
            )
        )
        return None, body, issues

    is_valid_name, name_error = _validate_skill_name(name=name, directory_name=directory_name)
    if not is_valid_name:
        issues.append(_issue("warning", "invalid-name", name_error, skill_md_path))

    if len(description) > MAX_SKILL_DESCRIPTION_LENGTH:
        issues.append(
            _issue(
                "warning",
                "description-too-long",
                f"Description exceeds {MAX_SKILL_DESCRIPTION_LENGTH} characters and will be truncated.",
                skill_md_path,
            )
        )
        description = description[:MAX_SKILL_DESCRIPTION_LENGTH]

    compatibility = str(frontmatter.get("compatibility", "")).strip() or None
    if compatibility and len(compatibility) > MAX_SKILL_COMPATIBILITY_LENGTH:
        issues.append(
            _issue(
                "warning",
                "compatibility-too-long",
                f"Compatibility exceeds {MAX_SKILL_COMPATIBILITY_LENGTH} characters and will be truncated.",
                skill_md_path,
            )
        )
        compatibility = compatibility[:MAX_SKILL_COMPATIBILITY_LENGTH]

    allowed_tools: list[str] = []
    raw_allowed_tools = frontmatter.get("allowed-tools")
    if isinstance(raw_allowed_tools, str):
        allowed_tools = [item.strip(",") for item in raw_allowed_tools.split() if item.strip(",")]
    elif raw_allowed_tools is not None:
        issues.append(
            _issue(
                "warning",
                "invalid-allowed-tools",
                "'allowed-tools' should be a space-delimited string; the current value will be ignored.",
                skill_md_path,
            )
        )

    metadata = _normalize_metadata(frontmatter.get("metadata"), skill_md_path, issues)
    normalized = SkillMetadata(
        name=name,
        description=description,
        path=str(skill_md_path),
        license=str(frontmatter.get("license", "")).strip() or None,
        compatibility=compatibility,
        metadata=metadata,
        allowed_tools=allowed_tools,
    )
    return normalized, body, issues


def _normalize_metadata(
    raw: object,
    skill_md_path: Path,
    issues: list[SkillValidationIssue],
) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        issues.append(
            _issue(
                "warning",
                "invalid-metadata",
                f"'metadata' should be a mapping, got {type(raw).__name__}; the current value will be ignored.",
                skill_md_path,
            )
        )
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def _validate_skill_name(*, name: str, directory_name: str) -> tuple[bool, str]:
    if not name:
        return False, "Skill name is required."
    if len(name) > MAX_SKILL_NAME_LENGTH:
        return False, f"Skill name exceeds {MAX_SKILL_NAME_LENGTH} characters."
    if name.startswith("-") or name.endswith("-") or "--" in name:
        return False, "Skill name must use lowercase alphanumeric characters with single hyphens only."
    for char in name:
        if char == "-":
            continue
        if (char.isalpha() and char.islower()) or char.isdigit():
            continue
        return False, "Skill name must use lowercase alphanumeric characters with single hyphens only."
    if name != directory_name:
        return False, f"Skill name '{name}' must match directory name '{directory_name}'."
    return True, ""


def _issue(severity: IssueSeverity, code: str, message: str, path: Path) -> SkillValidationIssue:
    return SkillValidationIssue(severity=severity, code=code, message=message, path=str(path))


__all__ = [
    "MAX_SKILL_COMPATIBILITY_LENGTH",
    "MAX_SKILL_DESCRIPTION_LENGTH",
    "MAX_SKILL_FILE_SIZE",
    "MAX_SKILL_NAME_LENGTH",
    "SkillLibraryScan",
    "SkillMetadata",
    "SkillValidationIssue",
    "ValidatedSkill",
    "discover_skill_directories",
    "is_skill_directory",
    "scan_skill_library",
    "validate_skill_directory",
]
