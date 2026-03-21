SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS memory_heads (
        head_id TEXT PRIMARY KEY,
        scope_key TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        workspace_id TEXT,
        project_id TEXT,
        user_id TEXT,
        agent_id TEXT,
        session_id TEXT,
        run_id TEXT,
        namespace TEXT NOT NULL,
        visibility TEXT NOT NULL,
        kind TEXT NOT NULL,
        layer TEXT NOT NULL,
        tier TEXT NOT NULL,
        state TEXT NOT NULL,
        fact_key TEXT,
        current_version_id TEXT NOT NULL,
        importance REAL NOT NULL,
        confidence REAL NOT NULL,
        access_count INTEGER NOT NULL DEFAULT 0,
        last_accessed_at INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_versions (
        version_id TEXT PRIMARY KEY,
        head_id TEXT NOT NULL,
        version_no INTEGER NOT NULL,
        text TEXT NOT NULL,
        abstract TEXT NOT NULL,
        overview TEXT NOT NULL,
        checksum TEXT NOT NULL,
        change_type TEXT NOT NULL,
        valid_from INTEGER NOT NULL,
        valid_to INTEGER,
        source_type TEXT,
        source_ref TEXT,
        embedding_model TEXT,
        chunk_strategy TEXT,
        created_by TEXT,
        created_at INTEGER NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_chunks (
        chunk_pk INTEGER PRIMARY KEY AUTOINCREMENT,
        chunk_id TEXT NOT NULL UNIQUE,
        head_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        chunk_no INTEGER NOT NULL,
        text TEXT NOT NULL,
        token_count INTEGER NOT NULL DEFAULT 0,
        char_start INTEGER NOT NULL,
        char_end INTEGER NOT NULL,
        embedding_state TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS memory_chunks_fts USING fts5(
        text,
        scope_key UNINDEXED,
        head_id UNINDEXED,
        version_id UNINDEXED
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_links (
        link_id TEXT PRIMARY KEY,
        src_head_id TEXT NOT NULL,
        dst_head_id TEXT NOT NULL,
        relation_type TEXT NOT NULL,
        weight REAL NOT NULL DEFAULT 1.0,
        created_at INTEGER NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS outbox_jobs (
        job_id TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        op_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL,
        retry_count INTEGER NOT NULL DEFAULT 0,
        available_at INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS history_events (
        event_id TEXT PRIMARY KEY,
        scope_key TEXT NOT NULL,
        head_id TEXT,
        version_id TEXT,
        event_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_heads_scope_kind ON memory_heads(scope_key, kind, state)",
    "CREATE INDEX IF NOT EXISTS idx_heads_scope_layer ON memory_heads(scope_key, layer, tier)",
    "CREATE INDEX IF NOT EXISTS idx_heads_scope_updated ON memory_heads(scope_key, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_heads_fact_key ON memory_heads(scope_key, kind, fact_key)",
    "CREATE INDEX IF NOT EXISTS idx_versions_head ON memory_versions(head_id, version_no DESC)",
    "CREATE INDEX IF NOT EXISTS idx_versions_validity ON memory_versions(head_id, valid_from, valid_to)",
    "CREATE INDEX IF NOT EXISTS idx_chunks_scope_version ON memory_chunks(scope_key, version_id)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_jobs(status, available_at)",
]
