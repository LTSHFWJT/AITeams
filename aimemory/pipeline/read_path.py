from __future__ import annotations

from dataclasses import asdict
from hashlib import sha1
from typing import Any

from aimemory.catalog.sqlite_store import SQLiteCatalog
from aimemory.config import MemoryConfig
from aimemory.errors import InvalidScope
from aimemory.filters import match_filters
from aimemory.hotstore.lmdb_store import LMDBHotStore
from aimemory.pipeline.lifecycle import (
    access_bonus,
    freshness_multiplier,
    lexical_score,
    normalize_text,
    now_ms,
    should_skip_vector_search,
    tier_multiplier,
    vector_score,
)
from aimemory.plugins.protocols import RetrievalGate, Reranker
from aimemory.serialization import json_dumps
from aimemory.scope import Scope
from aimemory.types import SearchHit
from aimemory.vector.embeddings import Embedder
from aimemory.vector.lancedb_store import LanceVectorStore


class MemoryReadPath:
    def __init__(
        self,
        *,
        config: MemoryConfig,
        catalog: SQLiteCatalog,
        hotstore: LMDBHotStore,
        vector_store: LanceVectorStore,
        embedder: Embedder,
        reranker: Reranker | None = None,
        retrieval_gate: RetrievalGate | None = None,
    ):
        self.config = config
        self.catalog = catalog
        self.hotstore = hotstore
        self.vector_store = vector_store
        self.embedder = embedder
        self.reranker = reranker
        self.retrieval_gate = retrieval_gate

    def get(self, *, scope: Scope, head_id: str) -> dict[str, Any] | None:
        record = self.catalog.get_head(head_id)
        if record is None:
            return None
        if record["scope_key"] != scope.key:
            raise InvalidScope(f"Head {head_id} is outside the requested scope")
        return record

    def list(self, *, scope: Scope, filters: dict[str, Any] | None = None, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.catalog.list_heads(scope.key, limit=limit)
        return [row for row in rows if match_filters(row, filters)]

    def history(self, *, scope: Scope, head_id: str) -> dict[str, Any]:
        self.get(scope=scope, head_id=head_id)
        return self.catalog.get_history(head_id)

    def search(self, *, scope: Scope, query: str, top_k: int = 10, filters: dict[str, Any] | None = None) -> list[SearchHit]:
        normalized = normalize_text(query)
        working_hits = self._search_working_memory(scope=scope, query=normalized, top_k=top_k)
        use_longterm = self._should_retrieve_longterm(scope=scope, query=normalized)

        cache_key = self._cache_key(
            scope=scope,
            query=normalized,
            top_k=top_k,
            filters=filters,
            use_longterm=use_longterm,
        )
        use_cache = self.config.query_cache_enabled and self.catalog.count_pending_jobs() == 0
        if use_cache:
            cached = self.hotstore.get_query_cache(cache_key)
            if cached is not None:
                hits = [SearchHit(**row) for row in cached]
                self._bump_access(hits[:top_k])
                return hits[:top_k]

        if not use_longterm:
            hits = self._rerank_if_needed(query=normalized, hits=working_hits, top_k=top_k)
            if use_cache:
                self.hotstore.put_query_cache(cache_key, [asdict(hit) for hit in hits[:top_k]])
            self._bump_access(hits[:top_k])
            return hits[:top_k]

        longterm_hits = self._search_longterm(scope=scope, query=normalized, top_k=top_k, filters=filters)
        hits = self._merge_hits(working_hits, longterm_hits)
        hits = self._rerank_if_needed(query=normalized, hits=hits, top_k=top_k)
        if use_cache:
            self.hotstore.put_query_cache(cache_key, [asdict(hit) for hit in hits[:top_k]])
        self._bump_access(hits[:top_k])
        return hits[:top_k]

    def _search_longterm(self, *, scope: Scope, query: str, top_k: int, filters: dict[str, Any] | None) -> list[SearchHit]:
        lexical_rows = self.catalog.search_lexical(scope.key, query, max(top_k * 5, 10))
        merged: dict[str, dict[str, Any]] = {}
        for row in lexical_rows:
            existing = merged.setdefault(
                row["head_id"],
                row | {"lexical_score": 0.0, "vector_score": 0.0},
            )
            existing["lexical_score"] = max(existing["lexical_score"], lexical_score(row["rank"], row["text"], query))
            if len(row["text"]) > len(existing["text"]):
                existing["text"] = row["text"]

        if not should_skip_vector_search(query):
            embedding_cache_key = f"{self.embedder.model_name}:{sha1(query.encode('utf-8')).hexdigest()}"
            vector = self.hotstore.get_embedding(embedding_cache_key)
            if vector is None:
                vector = self.embedder.embed_texts([query])[0]
                self.hotstore.put_embedding(embedding_cache_key, vector)
            vector_rows = self.vector_store.search(scope_key=scope.key, vector=vector, limit=max(top_k * 5, 10))
            for row in vector_rows:
                entry = merged.setdefault(
                    row["head_id"],
                    {
                        "chunk_id": row["chunk_id"],
                        "head_id": row["head_id"],
                        "version_id": row["version_id"],
                        "text": row["text"],
                        "abstract": row["abstract"],
                        "overview": row["overview"],
                        "kind": row["kind"],
                        "layer": "longterm",
                        "tier": row["tier"],
                        "importance": float(row["importance"]),
                        "confidence": float(row["confidence"]),
                        "access_count": 0,
                        "valid_from": int(row.get("valid_from") or 0),
                        "valid_to": row.get("valid_to"),
                        "updated_at": int(row["updated_at"]),
                        "metadata": {},
                        "lexical_score": 0.0,
                        "vector_score": 0.0,
                    },
                )
                entry["vector_score"] = max(entry["vector_score"], vector_score(float(row["_distance"])))

        now = now_ms()
        hits: list[SearchHit] = []
        for row in merged.values():
            if row.get("valid_to") is not None and int(row["valid_to"]) <= now:
                continue
            if int(row.get("valid_from") or 0) > now:
                continue
            record_view = {
                "kind": row["kind"],
                "tier": row["tier"],
                "state": "active",
            }
            if not match_filters(record_view, filters):
                continue
            score = (
                self.config.retrieval_vector_weight * row["vector_score"]
                + self.config.retrieval_lexical_weight * row["lexical_score"]
                + 0.05 * row["importance"]
                + 0.05 * row["confidence"]
            )
            score *= freshness_multiplier(row["updated_at"], now=now)
            score *= tier_multiplier(row["tier"])
            score += access_bonus(int(row.get("access_count", 0)))
            hits.append(
                SearchHit(
                    head_id=row["head_id"],
                    version_id=row["version_id"],
                    chunk_id=row["chunk_id"],
                    kind=row["kind"],
                    layer=row["layer"],
                    tier=row["tier"],
                    text=row["text"],
                    abstract=row["abstract"],
                    overview=row["overview"],
                    score=score,
                    lexical_score=row["lexical_score"],
                    vector_score=row["vector_score"],
                    access_count=int(row.get("access_count", 0)),
                    valid_from=int(row.get("valid_from") or 0),
                    valid_to=row.get("valid_to"),
                    metadata=row["metadata"],
                )
            )
        hits.sort(key=lambda item: item.score, reverse=True)
        return hits[: max(top_k * 2, top_k)]

    def _search_working_memory(self, *, scope: Scope, query: str, top_k: int) -> list[SearchHit]:
        now = now_ms()
        candidates = [
            ("working_set", self.hotstore.working_snapshot(scope.key, self.config.working_memory_limit)),
            ("turn_buffer", list(reversed(self.hotstore.turn_snapshot(scope.key, self.config.working_memory_limit)))),
        ]
        hits_by_head: dict[str, SearchHit] = {}
        for source_name, items in candidates:
            for index, item in enumerate(items):
                if item.get("head_id"):
                    record = self.catalog.get_head(item["head_id"])
                    if record is None or record["scope_key"] != scope.key or record["state"] != "active":
                        continue
                    text = normalize_text(str(item.get("content") or record["text"]))
                    item = {
                        **item,
                        "kind": record["kind"],
                        "tier": record["tier"],
                        "version_id": record["version_id"],
                        "metadata": dict(record["metadata"]) | dict(item.get("metadata") or {}),
                    }
                else:
                    text = normalize_text(str(item.get("content") or item.get("abstract") or item.get("text") or ""))
                if not text:
                    continue
                overlap = self._working_overlap_score(query, text)
                if overlap <= 0:
                    continue
                recency_bonus = max(0.0, 0.08 - (index * 0.01))
                source_bonus = 0.08 if source_name == "working_set" else 0.04
                score = min(1.35, overlap + recency_bonus + source_bonus)
                head_id = item.get("head_id") or f"wrk:{sha1(f'{scope.key}|{source_name}|{index}|{text}'.encode('utf-8')).hexdigest()[:16]}"
                metadata = dict(item.get("metadata") or {})
                metadata.update({"source": source_name})
                if item.get("role"):
                    metadata["role"] = item["role"]
                hit = SearchHit(
                    head_id=head_id,
                    version_id=item.get("version_id") or head_id,
                    chunk_id=f"{head_id}:{source_name}:{index}",
                    kind=item.get("kind") or "summary",
                    layer="working",
                    tier=item.get("tier") or "core",
                    text=text,
                    abstract=item.get("abstract") or text[:160],
                    overview=item.get("overview") or ("- " + text[:240]),
                    score=score,
                    lexical_score=min(1.0, overlap),
                    vector_score=0.0,
                    access_count=0,
                    valid_from=now,
                    valid_to=None,
                    metadata=metadata,
                )
                existing = hits_by_head.get(hit.head_id)
                if existing is None or hit.score > existing.score:
                    hits_by_head[hit.head_id] = hit
        hits = sorted(hits_by_head.values(), key=lambda item: item.score, reverse=True)
        return hits[: max(top_k * 2, top_k)]

    def _should_retrieve_longterm(self, *, scope: Scope, query: str) -> bool:
        if self.retrieval_gate is not None:
            return bool(self.retrieval_gate.should_retrieve(query, scope))
        return not should_skip_vector_search(query)

    def _merge_hits(self, working_hits: list[SearchHit], longterm_hits: list[SearchHit]) -> list[SearchHit]:
        merged: dict[str, SearchHit] = {hit.head_id: hit for hit in longterm_hits}
        for working_hit in working_hits:
            existing = merged.get(working_hit.head_id)
            if existing is None:
                merged[working_hit.head_id] = working_hit
                continue
            existing.score = max(existing.score, working_hit.score) + 0.05
            existing.lexical_score = max(existing.lexical_score, working_hit.lexical_score)
            existing.metadata = dict(existing.metadata) | {"working_memory": True} | dict(working_hit.metadata)
        hits = sorted(merged.values(), key=lambda item: item.score, reverse=True)
        return hits

    def _rerank_if_needed(self, *, query: str, hits: list[SearchHit], top_k: int) -> list[SearchHit]:
        if self.reranker is None or len(hits) <= 1:
            return sorted(hits, key=lambda item: item.score, reverse=True)
        candidate_hits = hits[: max(top_k * 2, top_k)]
        order = self.reranker.rerank(query, [hit.text for hit in candidate_hits], top_k)
        reranked: list[SearchHit] = []
        seen: set[int] = set()
        for index, rerank_score in order:
            if index < 0 or index >= len(candidate_hits) or index in seen:
                continue
            seen.add(index)
            hit = candidate_hits[index]
            hit.score = max(hit.score, float(rerank_score))
            reranked.append(hit)
        for index, hit in enumerate(candidate_hits):
            if index not in seen:
                reranked.append(hit)
        return reranked

    def _bump_access(self, hits: list[SearchHit]) -> None:
        for hit in hits:
            if hit.head_id.startswith("wrk:"):
                continue
            self.hotstore.bump_access(hit.head_id)

    @staticmethod
    def _working_overlap_score(query: str, text: str) -> float:
        if not query or not text:
            return 0.0
        if query.lower() in text.lower():
            return 0.95
        query_tokens = set(query.lower().split())
        text_tokens = set(text.lower().split())
        if not query_tokens or not text_tokens:
            return 0.0
        overlap = len(query_tokens & text_tokens)
        if overlap == 0:
            return 0.0
        return min(0.9, overlap / len(query_tokens) * 0.85)

    def _cache_key(
        self,
        *,
        scope: Scope,
        query: str,
        top_k: int,
        filters: dict[str, Any] | None,
        use_longterm: bool,
    ) -> str:
        payload = json_dumps(
            {
                "filters": filters or {},
                "gate": type(self.retrieval_gate).__name__ if self.retrieval_gate else "default",
                "query": query,
                "reranker": type(self.reranker).__name__ if self.reranker else "none",
                "top_k": top_k,
                "use_longterm": use_longterm,
            }
        )
        digest = sha1(payload.encode("utf-8")).hexdigest()
        return f"{scope.key}:{digest}"
