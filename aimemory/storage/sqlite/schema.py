SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        agent_id TEXT,
        title TEXT,
        status TEXT NOT NULL,
        metadata TEXT,
        active_window TEXT,
        ttl_seconds INTEGER,
        expires_at TEXT,
        last_accessed_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS conversation_turns (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        run_id TEXT,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        name TEXT,
        metadata TEXT,
        tokens_in INTEGER,
        tokens_out INTEGER,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS working_memory_snapshots (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        run_id TEXT,
        summary TEXT,
        plan TEXT,
        scratchpad TEXT,
        window_size INTEGER,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tool_states (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        run_id TEXT,
        tool_name TEXT NOT NULL,
        state_key TEXT NOT NULL,
        state_value TEXT,
        expires_at TEXT,
        updated_at TEXT NOT NULL,
        UNIQUE(session_id, run_id, tool_name, state_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS session_variables (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT,
        updated_at TEXT NOT NULL,
        UNIQUE(session_id, key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY,
        session_id TEXT,
        user_id TEXT NOT NULL,
        agent_id TEXT,
        goal TEXT,
        status TEXT NOT NULL,
        metadata TEXT,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        session_id TEXT,
        parent_task_id TEXT,
        title TEXT NOT NULL,
        status TEXT NOT NULL,
        priority INTEGER NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS task_steps (
        id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        step_index INTEGER NOT NULL,
        title TEXT NOT NULL,
        status TEXT NOT NULL,
        detail TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_checkpoints (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        session_id TEXT,
        checkpoint_name TEXT NOT NULL,
        snapshot TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tool_calls (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        task_id TEXT,
        session_id TEXT,
        tool_name TEXT NOT NULL,
        arguments TEXT,
        result TEXT,
        status TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS observations (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        task_id TEXT,
        session_id TEXT,
        kind TEXT NOT NULL,
        content TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
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
        bundle_summary TEXT,
        item_count INTEGER NOT NULL DEFAULT 0,
        compression_state TEXT,
        hot_refs TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS short_term_memories (
        id TEXT PRIMARY KEY,
        bundle_id TEXT,
        content_id TEXT NOT NULL UNIQUE,
        text TEXT,
        text_hash TEXT,
        storage_policy TEXT NOT NULL DEFAULT 'inline',
        storage_ref TEXT,
        payload_bytes INTEGER,
        user_id TEXT NOT NULL,
        agent_id TEXT,
        owner_agent_id TEXT,
        session_id TEXT,
        run_id TEXT,
        source_session_id TEXT,
        source_run_id TEXT,
        subject_type TEXT,
        subject_id TEXT,
        interaction_type TEXT,
        namespace_key TEXT,
        memory_type TEXT NOT NULL,
        summary TEXT,
        summary_l0 TEXT,
        summary_l1 TEXT,
        importance REAL NOT NULL DEFAULT 0.5,
        confidence REAL NOT NULL DEFAULT 0.5,
        visibility TEXT NOT NULL DEFAULT 'private_agent',
        tier TEXT NOT NULL DEFAULT 'working',
        version INTEGER NOT NULL DEFAULT 1,
        supersedes_memory_id TEXT,
        superseded_by_memory_id TEXT,
        access_count INTEGER NOT NULL DEFAULT 0,
        last_accessed_at TEXT,
        expires_at TEXT,
        actor_id TEXT,
        participants TEXT,
        compression_state TEXT,
        status TEXT NOT NULL,
        source TEXT,
        metadata TEXT,
        content_format TEXT NOT NULL DEFAULT 'text/plain',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        archived_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS long_term_memories (
        id TEXT PRIMARY KEY,
        bundle_id TEXT,
        content_id TEXT NOT NULL UNIQUE,
        text TEXT,
        text_hash TEXT,
        storage_policy TEXT NOT NULL DEFAULT 'inline',
        storage_ref TEXT,
        payload_bytes INTEGER,
        user_id TEXT NOT NULL,
        agent_id TEXT,
        owner_agent_id TEXT,
        session_id TEXT,
        run_id TEXT,
        source_session_id TEXT,
        source_run_id TEXT,
        subject_type TEXT,
        subject_id TEXT,
        interaction_type TEXT,
        namespace_key TEXT,
        memory_type TEXT NOT NULL,
        summary TEXT,
        summary_l0 TEXT,
        summary_l1 TEXT,
        importance REAL NOT NULL DEFAULT 0.5,
        confidence REAL NOT NULL DEFAULT 0.5,
        visibility TEXT NOT NULL DEFAULT 'private_agent',
        tier TEXT NOT NULL DEFAULT 'working',
        version INTEGER NOT NULL DEFAULT 1,
        supersedes_memory_id TEXT,
        superseded_by_memory_id TEXT,
        access_count INTEGER NOT NULL DEFAULT 0,
        last_accessed_at TEXT,
        expires_at TEXT,
        actor_id TEXT,
        participants TEXT,
        compression_state TEXT,
        status TEXT NOT NULL,
        source TEXT,
        metadata TEXT,
        content_format TEXT NOT NULL DEFAULT 'text/plain',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        archived_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_events (
        id TEXT PRIMARY KEY,
        memory_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        session_id TEXT,
        run_id TEXT,
        actor_id TEXT,
        reason_code TEXT,
        source_table TEXT,
        version INTEGER,
        payload TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_links (
        id TEXT PRIMARY KEY,
        source_memory_id TEXT NOT NULL,
        target_memory_id TEXT NOT NULL,
        link_type TEXT NOT NULL,
        bundle_id TEXT,
        weight REAL DEFAULT 1.0,
        confidence REAL NOT NULL DEFAULT 0.5,
        source_domain TEXT,
        target_domain TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS profiles (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT,
        metadata TEXT,
        updated_at TEXT NOT NULL,
        UNIQUE(user_id, key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS preferences (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        category TEXT NOT NULL,
        value TEXT,
        confidence REAL DEFAULT 0.5,
        metadata TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_sources (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        source_type TEXT NOT NULL,
        uri TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS documents (
        id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        title TEXT NOT NULL,
        user_id TEXT,
        owner_agent_id TEXT,
        kb_namespace TEXT,
        source_subject_type TEXT,
        source_subject_id TEXT,
        namespace_key TEXT,
        inline_text TEXT,
        inline_excerpt TEXT,
        storage_policy TEXT NOT NULL DEFAULT 'large_only',
        storage_ref TEXT,
        payload_bytes INTEGER,
        external_id TEXT,
        status TEXT NOT NULL,
        metadata TEXT,
        retrieval_count INTEGER NOT NULL DEFAULT 0,
        last_retrieved_at TEXT,
        credibility_score REAL NOT NULL DEFAULT 0.5,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_versions (
        id TEXT PRIMARY KEY,
        document_id TEXT NOT NULL,
        version_label TEXT NOT NULL,
        object_id TEXT,
        checksum TEXT,
        size_bytes INTEGER,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS document_chunks (
        id TEXT PRIMARY KEY,
        document_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        chunk_index INTEGER NOT NULL,
        content TEXT NOT NULL,
        tokens INTEGER,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS citations (
        id TEXT PRIMARY KEY,
        document_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        chunk_id TEXT NOT NULL,
        label TEXT NOT NULL,
        location TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion_jobs (
        id TEXT PRIMARY KEY,
        source_id TEXT,
        document_id TEXT,
        status TEXT NOT NULL,
        message TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skills (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT NOT NULL,
        owner_id TEXT,
        owner_agent_id TEXT,
        source_subject_type TEXT,
        source_subject_id TEXT,
        usage_count INTEGER NOT NULL DEFAULT 0,
        last_used_at TEXT,
        success_score REAL NOT NULL DEFAULT 0.5,
        capability_tags TEXT,
        tool_affinity TEXT,
        namespace_key TEXT,
        current_snapshot_id TEXT,
        status TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_snapshots (
        id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        prompt_template TEXT,
        workflow TEXT,
        schema_json TEXT,
        object_id TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_bindings (
        id TEXT PRIMARY KEY,
        skill_snapshot_id TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        binding_type TEXT NOT NULL,
        config TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_tests (
        id TEXT PRIMARY KEY,
        skill_snapshot_id TEXT NOT NULL,
        input_payload TEXT,
        expected_output TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_files (
        id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        skill_snapshot_id TEXT NOT NULL,
        object_id TEXT NOT NULL,
        relative_path TEXT NOT NULL,
        role TEXT NOT NULL,
        mime_type TEXT,
        size_bytes INTEGER NOT NULL,
        checksum TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(skill_snapshot_id, relative_path)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_reference_chunks (
        id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        skill_snapshot_id TEXT NOT NULL,
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
    CREATE TABLE IF NOT EXISTS archive_memories (
        id TEXT PRIMARY KEY,
        bundle_id TEXT,
        content_id TEXT NOT NULL UNIQUE,
        storage_policy TEXT NOT NULL DEFAULT 'cold_only',
        storage_ref TEXT,
        payload_bytes INTEGER,
        inline_excerpt TEXT,
        domain TEXT NOT NULL,
        source_id TEXT NOT NULL,
        user_id TEXT,
        owner_agent_id TEXT,
        subject_type TEXT,
        subject_id TEXT,
        interaction_type TEXT,
        namespace_key TEXT,
        source_type TEXT,
        session_id TEXT,
        summary TEXT,
        summary_l0 TEXT,
        summary_l1 TEXT,
        visibility TEXT,
        tier TEXT,
        compression_state TEXT,
        source_memory_ids TEXT,
        archive_reason TEXT,
        metadata TEXT,
        content_format TEXT NOT NULL DEFAULT 'application/json',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_rehydrated_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive_summaries (
        id TEXT PRIMARY KEY,
        archive_unit_id TEXT NOT NULL,
        summary TEXT NOT NULL,
        highlights TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS handoff_packs (
        id TEXT PRIMARY KEY,
        source_run_id TEXT,
        source_session_id TEXT,
        source_agent_id TEXT,
        target_agent_id TEXT,
        namespace_key TEXT,
        visibility TEXT NOT NULL,
        summary TEXT NOT NULL,
        highlights TEXT,
        open_tasks TEXT,
        constraints TEXT,
        source_refs TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        expires_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reflection_memories (
        id TEXT PRIMARY KEY,
        owner_agent_id TEXT,
        session_id TEXT,
        run_id TEXT,
        reflection_type TEXT NOT NULL,
        summary TEXT NOT NULL,
        details TEXT,
        confidence REAL NOT NULL DEFAULT 0.5,
        decay_half_life_days REAL,
        source_refs TEXT,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS context_artifacts (
        id TEXT PRIMARY KEY,
        artifact_type TEXT NOT NULL,
        session_id TEXT,
        run_id TEXT,
        owner_agent_id TEXT,
        target_agent_id TEXT,
        namespace_key TEXT,
        provider TEXT,
        model TEXT,
        budget_chars INTEGER,
        source_refs TEXT,
        content TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL,
        expires_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS compression_jobs (
        id TEXT PRIMARY KEY,
        job_type TEXT NOT NULL,
        session_id TEXT,
        run_id TEXT,
        owner_agent_id TEXT,
        status TEXT NOT NULL,
        provider TEXT,
        model TEXT,
        request_payload TEXT,
        result_artifact_id TEXT,
        error TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scope_acl_rules (
        id TEXT PRIMARY KEY,
        namespace_key TEXT NOT NULL,
        resource_type TEXT NOT NULL,
        resource_scope TEXT NOT NULL,
        principal_type TEXT NOT NULL,
        principal_id TEXT NOT NULL,
        permission TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS objects (
        id TEXT PRIMARY KEY,
        object_key TEXT NOT NULL UNIQUE,
        object_type TEXT NOT NULL,
        mime_type TEXT,
        size_bytes INTEGER NOT NULL,
        checksum TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS outbox_events (
        id TEXT PRIMARY KEY,
        topic TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        action TEXT NOT NULL,
        payload TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        processed_at TEXT,
        last_error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_index (
        record_id TEXT PRIMARY KEY,
        domain TEXT NOT NULL,
        scope TEXT NOT NULL,
        user_id TEXT,
        owner_agent_id TEXT,
        subject_type TEXT,
        subject_id TEXT,
        interaction_type TEXT,
        namespace_key TEXT,
        visibility TEXT,
        tier TEXT,
        access_count INTEGER NOT NULL DEFAULT 0,
        last_accessed_at TEXT,
        actor_id TEXT,
        session_id TEXT,
        text TEXT NOT NULL,
        keywords TEXT,
        score_boost REAL NOT NULL DEFAULT 0.0,
        updated_at TEXT NOT NULL,
        metadata TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_chunk_index (
        record_id TEXT PRIMARY KEY,
        document_id TEXT NOT NULL,
        source_id TEXT,
        title TEXT,
        text TEXT NOT NULL,
        keywords TEXT,
        updated_at TEXT NOT NULL,
        metadata TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_index (
        record_id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        skill_snapshot_id TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        text TEXT NOT NULL,
        keywords TEXT,
        updated_at TEXT NOT NULL,
        metadata TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_reference_index (
        record_id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        skill_snapshot_id TEXT NOT NULL,
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
    CREATE TABLE IF NOT EXISTS archive_summary_index (
        record_id TEXT PRIMARY KEY,
        archive_unit_id TEXT NOT NULL,
        domain TEXT NOT NULL,
        user_id TEXT,
        session_id TEXT,
        text TEXT NOT NULL,
        keywords TEXT,
        updated_at TEXT NOT NULL,
        metadata TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_nodes (
        id TEXT PRIMARY KEY,
        node_type TEXT NOT NULL,
        ref_id TEXT NOT NULL,
        label TEXT NOT NULL,
        metadata TEXT,
        updated_at TEXT NOT NULL,
        UNIQUE(node_type, ref_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_edges (
        id TEXT PRIMARY KEY,
        source_node_id TEXT NOT NULL,
        target_node_id TEXT NOT NULL,
        edge_type TEXT NOT NULL,
        metadata TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_turns_session ON conversation_turns(session_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_runs_session ON runs(session_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_run ON tasks(run_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_short_term_memories_scope ON short_term_memories(user_id, session_id, status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_short_term_memories_owner ON short_term_memories(owner_agent_id, subject_type, subject_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_short_term_memories_namespace ON short_term_memories(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_short_term_memories_bundle ON short_term_memories(bundle_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_short_term_memories_text_hash ON short_term_memories(text_hash, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_long_term_memories_scope ON long_term_memories(user_id, status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_long_term_memories_owner ON long_term_memories(owner_agent_id, subject_type, subject_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_long_term_memories_namespace ON long_term_memories(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_long_term_memories_bundle ON long_term_memories(bundle_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_long_term_memories_text_hash ON long_term_memories(text_hash, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_memory_bundles_scope ON memory_bundles(scope, owner_agent_id, subject_type, subject_id, interaction_type, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_memory_bundles_namespace ON memory_bundles(namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_memory_events_memory ON memory_events(memory_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_memory_links_source_target ON memory_links(source_memory_id, target_memory_id, link_type)",
    "CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_documents_namespace_storage ON documents(namespace_key, storage_policy, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_chunks_document ON document_chunks(document_id, chunk_index)",
    "CREATE INDEX IF NOT EXISTS idx_skills_status ON skills(status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skills_lookup ON skills(name, owner_agent_id, namespace_key, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skills_current_snapshot ON skills(current_snapshot_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_skill_files_snapshot ON skill_files(skill_snapshot_id, relative_path)",
    "CREATE INDEX IF NOT EXISTS idx_skill_files_skill ON skill_files(skill_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_skill_ref_chunks_snapshot ON skill_reference_chunks(skill_snapshot_id, relative_path, chunk_index)",
    "CREATE INDEX IF NOT EXISTS idx_skill_ref_index_skill ON skill_reference_index(skill_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events(status, available_at, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_memory_index_scope ON memory_index(user_id, session_id, scope, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_memories_scope ON archive_memories(user_id, session_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_memories_owner ON archive_memories(owner_agent_id, subject_type, subject_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_memories_namespace ON archive_memories(namespace_key, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_memories_bundle ON archive_memories(bundle_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_index_scope ON archive_summary_index(user_id, session_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_handoff_packs_route ON handoff_packs(source_agent_id, target_agent_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_reflection_memories_owner_type ON reflection_memories(owner_agent_id, reflection_type, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_context_artifacts_lookup ON context_artifacts(artifact_type, session_id, run_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_compression_jobs_owner_status ON compression_jobs(owner_agent_id, status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_scope_acl_rules_lookup ON scope_acl_rules(namespace_key, principal_type, principal_id, permission)",
]
