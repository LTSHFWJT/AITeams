from __future__ import annotations

from collections.abc import Callable
from typing import Any

from aimemory.providers.defaults import (
    AdaptiveMemoryPlanner,
    AdaptiveRecallPlanner,
    HybridReranker,
    NoopLLMProvider,
    SemanticFactExtractor,
    TextOnlyVisionProcessor,
)


class LiteProviderRegistry:
    def __init__(self):
        self._providers: dict[str, dict[str, Callable[..., Any]]] = {
            "llm": {},
            "vision": {},
            "extractor": {},
            "planner": {},
            "recall_planner": {},
            "reranker": {},
        }

    def register(self, category: str, name: str, factory: Callable[..., Any]) -> None:
        self._providers.setdefault(category, {})
        self._providers[category][name] = factory

    def create(self, category: str, name: str, **kwargs: Any) -> Any:
        if category not in self._providers or name not in self._providers[category]:
            raise ValueError(f"Unknown provider `{category}:{name}`")
        return self._providers[category][name](**kwargs)


class LiteProviderFactory:
    registry = LiteProviderRegistry()
    _bootstrapped = False

    @classmethod
    def bootstrap_defaults(cls) -> None:
        if cls._bootstrapped:
            return
        cls.registry.register("llm", "noop", lambda **_: NoopLLMProvider())
        cls.registry.register("vision", "text-only", lambda **_: TextOnlyVisionProcessor())
        cls.registry.register("extractor", "adaptive", lambda **_: SemanticFactExtractor())
        cls.registry.register("extractor", "rule", lambda **_: SemanticFactExtractor())
        cls.registry.register("planner", "adaptive", lambda **_: AdaptiveMemoryPlanner())
        cls.registry.register("planner", "heuristic", lambda **_: AdaptiveMemoryPlanner())
        cls.registry.register("planner", "evidence", lambda **_: AdaptiveMemoryPlanner())
        cls.registry.register("recall_planner", "adaptive", lambda **_: AdaptiveRecallPlanner())
        cls.registry.register("recall_planner", "lite", lambda **_: AdaptiveRecallPlanner())
        cls.registry.register("reranker", "hybrid", lambda **_: HybridReranker())
        cls.registry.register("reranker", "rule", lambda **_: HybridReranker())
        cls._bootstrapped = True

    @classmethod
    def create(cls, category: str, name: str, **kwargs: Any) -> Any:
        cls.bootstrap_defaults()
        return cls.registry.create(category, name, **kwargs)


LiteProviderFactory.bootstrap_defaults()
