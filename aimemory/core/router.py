from __future__ import annotations


DEFAULT_RETRIEVAL_DOMAINS: tuple[str, ...] = ("memory", "interaction", "knowledge", "skill", "archive")


class RetrievalRouter:
    def route(self, query: str, domain: str | None = None, session_id: str | None = None) -> list[str]:
        del query, session_id
        if domain and domain != "auto":
            return [domain]
        return list(DEFAULT_RETRIEVAL_DOMAINS)

    def explain(self, query: str, *, session_id: str | None = None) -> dict[str, float]:
        del query, session_id
        return {domain: 1.0 for domain in DEFAULT_RETRIEVAL_DOMAINS}
