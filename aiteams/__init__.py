from __future__ import annotations

from typing import Any

__all__ = ["__version__", "AppSettings", "create_app", "run"]

__version__ = "1.0.0"


def __getattr__(name: str) -> Any:
    if name in {"AppSettings", "create_app", "run"}:
        from aiteams.app import AppSettings, create_app, run

        exports = {
            "AppSettings": AppSettings,
            "create_app": create_app,
            "run": run,
        }
        return exports[name]
    raise AttributeError(f"module 'aiteams' has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
