from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class MemoryConfig:
    root_dir: str | Path = ".aimemory"
    vector_dim: int = 32
    chunk_size: int = 480
    chunk_overlap: int = 64
    semantic_dedupe_enabled: bool = True
    semantic_dedupe_threshold: float = 0.9
    semantic_dedupe_candidates: int = 8
    working_memory_limit: int = 64
    auto_flush: bool = True
    flush_access_every: int = 256
    flush_access_interval_ms: int = 1000
    worker_mode: str = "library_only"
    worker_poll_interval_ms: int = 250
    worker_lease_ttl_ms: int = 3000
    query_cache_enabled: bool = True
    recover_on_open: bool = True
    recovery_batch_size: int = 512
    retrieval_vector_weight: float = 0.55
    retrieval_lexical_weight: float = 0.30
    retrieval_exact_fact_weight: float = 0.10
    retrieval_scope_specificity_weight: float = 0.05
    retrieval_confidence_floor: float = 0.85
    procedure_version_mode: str = "append_only"
    embedding_model: str = "hash32"
    lifecycle_enabled: bool = True
    lifecycle_batch_size: int = 512
    lifecycle_freshness_window_ms: int = 30 * 24 * 60 * 60 * 1000
    lifecycle_cold_after_ms: int = 45 * 24 * 60 * 60 * 1000
    lifecycle_core_promote_importance: float = 0.8
    lifecycle_core_promote_access_count: int = 10
    lifecycle_core_promote_score: float = 0.7
    lifecycle_core_demote_score: float = 0.45
    lifecycle_cold_demote_score: float = 0.2
    lifecycle_cold_reactivate_score: float = 0.4

    def resolved_root(self) -> Path:
        return Path(self.root_dir).expanduser().resolve()
