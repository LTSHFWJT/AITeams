from __future__ import annotations

OUTBOX_UPSERT_VECTOR = "upsert_vector"
OUTBOX_DELETE_VECTOR = "delete_vector"
OUTBOX_REBUILD_VECTOR = "rebuild_vector"
OUTBOX_FLUSH_ACCESS = "flush_access"

VECTOR_WRITE_OPS = {
    OUTBOX_UPSERT_VECTOR,
    OUTBOX_REBUILD_VECTOR,
}
