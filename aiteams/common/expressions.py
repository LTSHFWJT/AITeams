from __future__ import annotations

import re
from typing import Any

from aiteams.utils import resolve_path


TOKEN_PATTERN = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\b")
RESERVED_TOKENS = {"True", "False", "None", "and", "or", "not", "in", "is"}


def evaluate_expression(expr: str, context: dict[str, Any]) -> Any:
    normalized = (
        expr.replace(" true", " True")
        .replace(" false", " False")
        .replace(" null", " None")
        .replace("true", "True")
        .replace("false", "False")
        .replace("null", "None")
        .replace("&&", " and ")
        .replace("||", " or ")
    )

    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        if token in RESERVED_TOKENS:
            return token
        return f'__lookup__("{token}")'

    transformed = TOKEN_PATTERN.sub(_replace, normalized)
    return eval(transformed, {"__builtins__": {}}, {"__lookup__": lambda path: resolve_path(context, path)})
