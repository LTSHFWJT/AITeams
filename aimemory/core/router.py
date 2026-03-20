from __future__ import annotations


DEFAULT_RETRIEVAL_DOMAINS: tuple[str, ...] = ("memory", "interaction", "knowledge", "skill", "archive")
ALL_RETRIEVAL_DOMAINS: tuple[str, ...] = (
    "memory",
    "interaction",
    "knowledge",
    "skill",
    "archive",
    "execution",
    "context",
    "handoff",
    "reflection",
)

DOMAIN_HINTS: dict[str, tuple[str, ...]] = {
    "context": ("context",),
    "上下文": ("context",),
    "brief": ("context",),
    "summary": ("context", "reflection"),
    "摘要": ("context", "reflection"),
    "handoff": ("handoff", "context", "reflection"),
    "交接": ("handoff", "context", "reflection"),
    "接手": ("handoff", "context"),
    "reflection": ("reflection",),
    "reflect": ("reflection",),
    "lesson": ("reflection",),
    "经验": ("reflection",),
    "反思": ("reflection",),
    "执行": ("execution",),
    "run": ("execution",),
}


def hinted_domains(query: str) -> list[str]:
    lowered = str(query or "").lower()
    selected: list[str] = []
    for key, domains in DOMAIN_HINTS.items():
        if key in lowered:
            for domain in domains:
                if domain not in selected:
                    selected.append(domain)
    return selected


class RetrievalRouter:
    def route(self, query: str, domain: str | None = None, session_id: str | None = None) -> list[str]:
        del query, session_id
        if domain and domain != "auto":
            return [domain]
        return list(DEFAULT_RETRIEVAL_DOMAINS)

    def explain(self, query: str, *, session_id: str | None = None) -> dict[str, float]:
        del query, session_id
        return {domain: 1.0 for domain in self.route("")}
