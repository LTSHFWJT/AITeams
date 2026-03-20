from __future__ import annotations

SUPPORTED_RAW_STORE_POLICIES = {"never", "large_only", "always"}


def normalize_raw_store_policy(policy: str | None, *, default: str = "large_only") -> str:
    candidate = str(policy or default).strip().lower()
    if candidate not in SUPPORTED_RAW_STORE_POLICIES:
        return default
    return candidate


def payload_size_bytes(text: str | bytes | None) -> int:
    if isinstance(text, bytes):
        return len(text)
    return len(str(text or "").encode("utf-8"))


def build_inline_excerpt(text: str | None, *, max_chars: int = 512) -> str:
    content = str(text or "").strip()
    if len(content) <= max_chars:
        return content
    return content[: max(0, max_chars - 1)].rstrip() + "..."


def should_externalize_text(
    text: str | None,
    *,
    policy: str,
    inline_char_limit: int,
) -> bool:
    normalized_policy = normalize_raw_store_policy(policy)
    content = str(text or "")
    if normalized_policy == "always":
        return True
    if normalized_policy == "never":
        return False
    return len(content) > max(0, int(inline_char_limit))
