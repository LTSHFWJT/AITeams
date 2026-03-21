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
    confidence_multiplier,
    derive_fact_key,
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
from aimemory.state import HEAD_STATE_ACTIVE
from aimemory.types import SearchHit, SearchQuery, SearchResult
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
        rows = self.catalog.list_heads(scope.key, state=self._effective_state(filters), limit=limit)
        return [row for row in rows if match_filters(row, filters)]

    def history(self, *, scope: Scope, head_id: str) -> dict[str, Any]:
        self.get(scope=scope, head_id=head_id)
        return self.catalog.get_history(head_id)

    def search(self, *, scope: Scope, query: str, top_k: int = 10, filters: dict[str, Any] | None = None) -> list[SearchHit]:
        search = SearchQuery(query=query, top_k=top_k, filters=filters or {})
        return self.query(scope=scope, search=search).hits

    def query(self, *, scope: Scope, search: SearchQuery) -> SearchResult:
        normalized = normalize_text(search.query)
        use_longterm = self._should_retrieve_longterm(scope=scope, query=normalized)

        cache_key = self._cache_key(
            scope=scope,
            query=normalized,
            top_k=search.top_k,
            filters=search.filters,
            use_longterm=use_longterm,
        )
        use_cache = self.config.query_cache_enabled and self.catalog.count_pending_jobs() == 0
        if use_cache:
            cached = self.hotstore.get_query_cache(cache_key)
            if cached is not None:
                result = self._cache_to_result(cached, query=search.query, top_k=search.top_k, use_longterm=use_longterm)
                self._bump_access(result.hits)
                return result

        working_hits = self._search_working_memory(
            scope=scope,
            query=normalized,
            top_k=search.top_k,
            filters=search.filters,
        )

        if not use_longterm:
            hits = self._rerank_if_needed(query=normalized, hits=working_hits, top_k=search.top_k)
            used_working_memory = bool(working_hits)
            if use_cache:
                self.hotstore.put_query_cache(
                    cache_key,
                    self._result_to_cache(
                        SearchResult(
                            query=search.query,
                            hits=hits[: search.top_k],
                            used_working_memory=used_working_memory,
                            used_longterm_memory=False,
                        )
                    ),
                )
            hits = hits[: search.top_k]
            self._bump_access(hits)
            return SearchResult(
                query=search.query,
                hits=hits,
                used_working_memory=used_working_memory,
                used_longterm_memory=False,
            )

        longterm_hits = self._search_longterm(scope=scope, query=normalized, top_k=search.top_k, filters=search.filters)
        hits = self._merge_hits(working_hits, longterm_hits)
        hits = self._rerank_if_needed(query=normalized, hits=hits, top_k=search.top_k)
        used_working_memory = bool(working_hits)
        used_longterm_memory = bool(longterm_hits)
        if use_cache:
            self.hotstore.put_query_cache(
                cache_key,
                self._result_to_cache(
                    SearchResult(
                        query=search.query,
                        hits=hits[: search.top_k],
                        used_working_memory=used_working_memory,
                        used_longterm_memory=used_longterm_memory,
                    )
                ),
            )
        hits = hits[: search.top_k]
        self._bump_access(hits)
        return SearchResult(
            query=search.query,
            hits=hits,
            used_working_memory=used_working_memory,
            used_longterm_memory=used_longterm_memory,
        )

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
            vector_rows = self.vector_store.search(
                scope_key=scope.key,
                vector=vector,
                limit=max(top_k * 5, 10),
                filters=filters,
            )
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
                        "created_at": int(row["created_at"]),
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
        head_cache: dict[str, dict[str, Any] | None] = {}
        for row in merged.values():
            if row.get("valid_to") is not None and int(row["valid_to"]) <= now:
                continue
            if int(row.get("valid_from") or 0) > now:
                continue
            head_record = head_cache.get(row["head_id"])
            if row["head_id"] not in head_cache:
                head_record = self.catalog.get_head(row["head_id"])
                head_cache[row["head_id"]] = head_record
            if head_record is None or head_record["state"] != HEAD_STATE_ACTIVE:
                continue
            record_view = {
                "access_count": int(head_record.get("access_count", row.get("access_count", 0))),
                "confidence": float(head_record.get("confidence", row["confidence"])),
                "created_at": int(head_record.get("created_at") or row.get("created_at") or 0),
                "importance": float(head_record.get("importance", row["importance"])),
                "kind": head_record["kind"],
                "layer": head_record.get("layer", row.get("layer", "longterm")),
                "tier": head_record["tier"],
                "text": row["text"],
                "updated_at": int(head_record.get("updated_at") or row["updated_at"]),
                "valid_from": int(row.get("valid_from") or 0),
                "valid_to": row.get("valid_to"),
                "state": head_record["state"],
                "fact_key": head_record.get("fact_key"),
                "workspace_id": head_record.get("workspace_id"),
                "project_id": head_record.get("project_id"),
                "user_id": head_record.get("user_id"),
                "agent_id": head_record.get("agent_id"),
                "session_id": head_record.get("session_id"),
                "run_id": head_record.get("run_id"),
                "namespace": head_record.get("namespace"),
            }
            if not match_filters(record_view, filters):
                continue
            score, score_parts = self._score_longterm_candidate(
                scope=scope,
                query=query,
                row=row,
                head_record=head_record,
                now=now,
            )
            metadata = dict(head_record["metadata"]) | dict(row.get("metadata") or {})
            metadata.update(score_parts)
            hits.append(
                SearchHit(
                    head_id=row["head_id"],
                    version_id=row["version_id"],
                    chunk_id=row["chunk_id"],
                    kind=head_record["kind"],
                    layer=head_record["layer"],
                    tier=head_record["tier"],
                    text=row["text"],
                    abstract=row["abstract"],
                    overview=row["overview"],
                    score=score,
                    lexical_score=row["lexical_score"],
                    vector_score=row["vector_score"],
                    access_count=int(head_record.get("access_count", 0)),
                    valid_from=int(row.get("valid_from") or 0),
                    valid_to=row.get("valid_to"),
                    metadata=metadata,
                )
            )
        hits.sort(key=lambda item: item.score, reverse=True)
        return hits[: max(top_k * 2, top_k)]

    def _search_working_memory(self, *, scope: Scope, query: str, top_k: int, filters: dict[str, Any] | None) -> list[SearchHit]:
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
                    if not match_filters(record, filters):
                        continue
                    text = normalize_text(str(item.get("content") or record["text"]))
                    item = {
                        **item,
                        "kind": record["kind"],
                        "layer": record["layer"],
                        "tier": record["tier"],
                        "version_id": record["version_id"],
                        "importance": record["importance"],
                        "confidence": record["confidence"],
                        "created_at": record["created_at"],
                        "updated_at": record["updated_at"],
                        "metadata": dict(record["metadata"]) | dict(item.get("metadata") or {}),
                    }
                else:
                    text = normalize_text(str(item.get("content") or item.get("abstract") or item.get("text") or ""))
                    ephemeral_view = {
                        "kind": item.get("kind") or "summary",
                        "layer": "working",
                        "tier": item.get("tier") or "core",
                        "text": text,
                        "state": "active",
                    }
                    if not match_filters(ephemeral_view, filters):
                        continue
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
        observed_at = now_ms()
        for hit in hits:
            if hit.head_id.startswith("wrk:"):
                continue
            self.hotstore.bump_access(hit.head_id, recorded_at_ms=observed_at)

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

    @staticmethod
    def _effective_state(filters: dict[str, Any] | None) -> str | list[str] | None:
        if not filters or "state" not in filters:
            return HEAD_STATE_ACTIVE
        requested = filters["state"]
        if isinstance(requested, dict):
            if "eq" in requested:
                return str(requested["eq"])
            if "in" in requested:
                return [str(item) for item in requested["in"]]
            return None
        return str(requested)

    def _score_longterm_candidate(
        self,
        *,
        scope: Scope,
        query: str,
        row: dict[str, Any],
        head_record: dict[str, Any],
        now: int,
    ) -> tuple[float, dict[str, float]]:
        exact_fact = self._exact_fact_boost(query=query, head_record=head_record, text=row["text"])
        scope_specificity = self._scope_specificity_boost(scope=scope, head_record=head_record)
        base_score = (
            self.config.retrieval_vector_weight * row["vector_score"]
            + self.config.retrieval_lexical_weight * row["lexical_score"]
            + self.config.retrieval_exact_fact_weight * exact_fact
            + self.config.retrieval_scope_specificity_weight * scope_specificity
        )
        freshness = freshness_multiplier(int(head_record["updated_at"]), now=now)
        tier = tier_multiplier(str(head_record["tier"]))
        confidence = confidence_multiplier(
            float(head_record["confidence"]),
            floor=float(self.config.retrieval_confidence_floor),
        )
        final_score = (base_score * freshness * tier * confidence) + access_bonus(int(head_record.get("access_count", 0)))
        return final_score, {
            "exact_fact_boost": round(exact_fact, 6),
            "scope_specificity_boost": round(scope_specificity, 6),
            "confidence_multiplier": round(confidence, 6),
        }

    @staticmethod
    def _exact_fact_boost(*, query: str, head_record: dict[str, Any], text: str) -> float:
        normalized_query = normalize_text(query)
        if not normalized_query:
            return 0.0
        if normalize_text(text).lower() == normalized_query.lower():
            return 1.0
        fact_key = head_record.get("fact_key")
        kind = str(head_record.get("kind") or "")
        if fact_key:
            derived = derive_fact_key(kind, normalized_query)
            if derived is not None and derived == fact_key:
                return 1.0
            if str(fact_key).lower() == normalized_query.lower():
                return 1.0
        return 0.0

    @staticmethod
    def _scope_specificity_boost(*, scope: Scope, head_record: dict[str, Any]) -> float:
        fields = [
            "workspace_id",
            "project_id",
            "user_id",
            "agent_id",
            "session_id",
            "run_id",
            "namespace",
        ]
        matched = 0
        relevant = 0
        for field in fields:
            scope_value = getattr(scope, field)
            if scope_value is None:
                continue
            relevant += 1
            record_value = head_record.get(field)
            if record_value == scope_value:
                matched += 1
        if relevant == 0:
            return 1.0
        return matched / relevant

    @staticmethod
    def _result_to_cache(result: SearchResult) -> dict[str, Any]:
        return {
            "query": result.query,
            "hits": [asdict(hit) for hit in result.hits],
            "used_working_memory": result.used_working_memory,
            "used_longterm_memory": result.used_longterm_memory,
        }

    @staticmethod
    def _cache_to_result(
        cached: Any,
        *,
        query: str,
        top_k: int,
        use_longterm: bool,
    ) -> SearchResult:
        if isinstance(cached, dict):
            hits_payload = cached.get("hits", [])
            return SearchResult(
                query=str(cached.get("query") or query),
                hits=[SearchHit(**row) for row in hits_payload[:top_k]],
                used_working_memory=bool(cached.get("used_working_memory", False)),
                used_longterm_memory=bool(cached.get("used_longterm_memory", use_longterm)),
            )
        hits = [SearchHit(**row) for row in (cached or [])[:top_k]]
        return SearchResult(
            query=query,
            hits=hits,
            used_working_memory=any(hit.layer == "working" or hit.metadata.get("working_memory") for hit in hits),
            used_longterm_memory=any(hit.layer == "longterm" for hit in hits) or bool(use_longterm and hits),
        )
