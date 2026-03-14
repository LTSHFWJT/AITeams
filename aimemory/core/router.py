from __future__ import annotations

from aimemory.algorithms.affinity import DOMAIN_PROTOTYPES, normalize_score_map, prototype_affinities, ranked_labels


class RetrievalRouter:
    def route(self, query: str, domain: str | None = None, session_id: str | None = None) -> list[str]:
        if domain and domain != "auto":
            return [domain]

        scores = prototype_affinities(query, DOMAIN_PROTOTYPES)
        if session_id:
            scores["interaction"] = round(scores.get("interaction", 0.0) + 0.08, 6)
            scores["memory"] = round(scores.get("memory", 0.0) + 0.03, 6)
        normalized = normalize_score_map(scores)
        routed = ranked_labels(normalized, top_n=5, min_score=0.2, relative_threshold=0.72)
        if "memory" not in routed:
            routed.append("memory")
        return routed

    def explain(self, query: str, *, session_id: str | None = None) -> dict[str, float]:
        scores = prototype_affinities(query, DOMAIN_PROTOTYPES)
        if session_id:
            scores["interaction"] = round(scores.get("interaction", 0.0) + 0.08, 6)
        return normalize_score_map(scores)
