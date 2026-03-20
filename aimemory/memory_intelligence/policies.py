from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class MemoryPolicy:
    infer_by_default: bool = True
    conflict_threshold: float = 0.72
    contradiction_threshold: float = 0.76
    merge_threshold: float = 0.9
    duplicate_threshold: float = 0.96
    support_threshold: float = 0.93
    contextualize_threshold: float = 0.72
    candidate_merge_threshold: float = 0.86
    relation_threshold: float = 0.68
    update_min_score: float = 0.58
    search_limit: int = 8
    auxiliary_search_limit: int = 6
    search_scan_limit: int = 400
    background_sample_limit: int = 40
    background_similarity_limit: int = 24
    short_term_capture_threshold: float = 0.46
    compression_turn_threshold: int = 18
    compression_preserve_recent_turns: int = 8
    compression_budget_chars: int = 640
    long_term_char_threshold: int = 12000
    long_term_compression_budget_chars: int = 1600
    short_term_char_threshold: int = 6000
    short_term_compression_budget_chars: int = 900
    archive_char_threshold: int = 18000
    archive_compression_budget_chars: int = 1800
    cleanup_importance_threshold: float = 0.26
    cleanup_staleness_days: int = 30
    snapshot_keep_recent: int = 3
    session_health_snapshot_stale_hours: int = 24
    max_candidates: int = 8
    max_candidate_chars: int = 320
    min_candidate_chars: int = 8
    candidate_information_weight: float = 0.3
    candidate_novelty_weight: float = 0.35
    candidate_density_weight: float = 0.2
    candidate_length_weight: float = 0.15
    diversity_lambda: float = 0.72
    short_term_half_life_days: float = 3.0
    long_term_half_life_days: float = 120.0
    knowledge_half_life_days: float = 3650.0
    archive_half_life_days: float = 720.0
    chunk_size: int = 540
    chunk_overlap: int = 96
