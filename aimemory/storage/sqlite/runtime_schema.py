ADDITIONAL_COLUMNS: dict[str, dict[str, str]] = {
    "short_term_memories": {
        "bundle_id": "TEXT",
    },
    "long_term_memories": {
        "bundle_id": "TEXT",
    },
    "archive_memories": {
        "bundle_id": "TEXT",
    },
    "sessions": {
        "owner_agent_id": "TEXT",
        "interaction_type": "TEXT",
        "subject_type": "TEXT",
        "subject_id": "TEXT",
        "namespace_key": "TEXT",
    },
    "conversation_turns": {
        "speaker_participant_id": "TEXT",
        "target_participant_id": "TEXT",
        "turn_type": "TEXT",
        "salience_score": "REAL",
    },
    "working_memory_snapshots": {
        "owner_agent_id": "TEXT",
        "interaction_type": "TEXT",
        "subject_type": "TEXT",
        "subject_id": "TEXT",
        "constraints": "TEXT",
        "resolved_items": "TEXT",
        "unresolved_items": "TEXT",
        "next_actions": "TEXT",
        "budget_tokens": "INTEGER",
        "salience_vector": "TEXT",
        "compression_revision": "INTEGER NOT NULL DEFAULT 1",
        "namespace_key": "TEXT",
    },
    "runs": {
        "owner_agent_id": "TEXT",
        "interaction_type": "TEXT",
        "subject_type": "TEXT",
        "subject_id": "TEXT",
        "namespace_key": "TEXT",
    },
    "documents": {
        "owner_agent_id": "TEXT",
        "kb_namespace": "TEXT",
        "source_subject_type": "TEXT",
        "source_subject_id": "TEXT",
        "retrieval_count": "INTEGER NOT NULL DEFAULT 0",
        "last_retrieved_at": "TEXT",
        "credibility_score": "REAL NOT NULL DEFAULT 0.5",
        "namespace_key": "TEXT",
    },
    "skills": {
        "owner_agent_id": "TEXT",
        "source_subject_type": "TEXT",
        "source_subject_id": "TEXT",
        "usage_count": "INTEGER NOT NULL DEFAULT 0",
        "last_used_at": "TEXT",
        "success_score": "REAL NOT NULL DEFAULT 0.5",
        "capability_tags": "TEXT",
        "tool_affinity": "TEXT",
        "namespace_key": "TEXT",
    },
    "memory_index": {
        "owner_agent_id": "TEXT",
        "subject_type": "TEXT",
        "subject_id": "TEXT",
        "interaction_type": "TEXT",
        "namespace_key": "TEXT",
    },
    "knowledge_chunk_index": {
        "owner_agent_id": "TEXT",
        "source_subject_type": "TEXT",
        "source_subject_id": "TEXT",
        "namespace_key": "TEXT",
    },
    "skill_index": {
        "owner_agent_id": "TEXT",
        "source_subject_type": "TEXT",
        "source_subject_id": "TEXT",
        "namespace_key": "TEXT",
    },
    "archive_summary_index": {
        "owner_agent_id": "TEXT",
        "subject_type": "TEXT",
        "subject_id": "TEXT",
        "interaction_type": "TEXT",
        "source_type": "TEXT",
        "namespace_key": "TEXT",
    },
}

EXTRA_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS memory_bundles (
        id TEXT PRIMARY KEY,
        scope TEXT NOT NULL,
        scope_key TEXT NOT NULL UNIQUE,
        user_id TEXT,
        owner_agent_id TEXT,
        subject_type TEXT,
        subject_id TEXT,
        interaction_type TEXT,
        namespace_key TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_memory_bundles_scope ON memory_bundles(scope, owner_agent_id, subject_type, subject_id, interaction_type, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_memory_bundles_namespace ON memory_bundles(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_short_term_memories_bundle ON short_term_memories(bundle_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_long_term_memories_bundle ON long_term_memories(bundle_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_memories_bundle ON archive_memories(bundle_id, created_at)",
    """
    CREATE TABLE IF NOT EXISTS participants (
        id TEXT PRIMARY KEY,
        participant_type TEXT NOT NULL,
        external_id TEXT NOT NULL,
        display_name TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(participant_type, external_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS session_participants (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        participant_id TEXT NOT NULL,
        participant_role TEXT NOT NULL,
        joined_at TEXT NOT NULL,
        metadata TEXT,
        UNIQUE(session_id, participant_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS semantic_index_cache (
        record_id TEXT PRIMARY KEY,
        domain TEXT NOT NULL,
        collection TEXT NOT NULL,
        text TEXT NOT NULL,
        embedding TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        quality REAL NOT NULL DEFAULT 0.0,
        updated_at TEXT NOT NULL,
        metadata TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_files (
        id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        skill_version_id TEXT NOT NULL,
        object_id TEXT NOT NULL,
        relative_path TEXT NOT NULL,
        role TEXT NOT NULL,
        mime_type TEXT,
        size_bytes INTEGER NOT NULL,
        checksum TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(skill_version_id, relative_path)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_reference_chunks (
        id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        skill_version_id TEXT NOT NULL,
        file_id TEXT NOT NULL,
        object_id TEXT NOT NULL,
        relative_path TEXT NOT NULL,
        chunk_index INTEGER NOT NULL,
        title TEXT,
        content TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(file_id, chunk_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_reference_index (
        record_id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        skill_version_id TEXT NOT NULL,
        file_id TEXT NOT NULL,
        object_id TEXT NOT NULL,
        owner_agent_id TEXT,
        source_subject_type TEXT,
        source_subject_id TEXT,
        namespace_key TEXT,
        relative_path TEXT NOT NULL,
        title TEXT,
        text TEXT NOT NULL,
        keywords TEXT,
        updated_at TEXT NOT NULL,
        metadata TEXT
    )
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS text_search_index USING fts5(
        record_id UNINDEXED,
        domain UNINDEXED,
        collection UNINDEXED,
        title,
        text,
        keywords,
        lexical,
        path,
        updated_at UNINDEXED,
        user_id UNINDEXED,
        owner_agent_id UNINDEXED,
        subject_type UNINDEXED,
        subject_id UNINDEXED,
        interaction_type UNINDEXED,
        session_id UNINDEXED,
        run_id UNINDEXED,
        namespace_key UNINDEXED,
        tokenize = 'unicode61 remove_diacritics 2'
    )
    """,
]

POST_MIGRATION_SCHEMA_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_participants_lookup ON participants(participant_type, external_id)",
    "CREATE INDEX IF NOT EXISTS idx_session_participants_session ON session_participants(session_id, joined_at)",
    "CREATE INDEX IF NOT EXISTS idx_semantic_index_domain ON semantic_index_cache(domain, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_semantic_index_collection ON semantic_index_cache(collection, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_owner_subject ON sessions(owner_agent_id, subject_type, subject_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_namespace ON sessions(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_turns_speaker ON conversation_turns(session_id, speaker_participant_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_documents_owner ON documents(owner_agent_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_documents_namespace ON documents(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skills_owner ON skills(owner_agent_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skills_namespace ON skills(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skill_files_version ON skill_files(skill_version_id, relative_path)",
    "CREATE INDEX IF NOT EXISTS idx_skill_files_skill ON skill_files(skill_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_skill_ref_chunks_version ON skill_reference_chunks(skill_version_id, relative_path, chunk_index)",
    "CREATE INDEX IF NOT EXISTS idx_skill_ref_index_skill ON skill_reference_index(skill_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skill_ref_index_owner ON skill_reference_index(owner_agent_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skill_ref_index_namespace ON skill_reference_index(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_memory_index_owner ON memory_index(owner_agent_id, subject_type, subject_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_memory_index_namespace ON memory_index(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_index_owner ON knowledge_chunk_index(owner_agent_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_index_namespace ON knowledge_chunk_index(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skill_index_owner ON skill_index(owner_agent_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skill_index_namespace ON skill_index(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_index_owner ON archive_summary_index(owner_agent_id, subject_type, subject_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_index_namespace ON archive_summary_index(namespace_key, updated_at)",
]
