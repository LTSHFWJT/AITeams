from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from langgraph.store.base import PutOp, SearchOp

from aiteams.memory.adapter import LangMemAdapter
from aiteams.memory.scope import MemoryScopes
from aiteams.memory.store import LMDBLanceDBStore


class MemoryStoreTests(unittest.TestCase):
    def test_provider_embedding_enables_vector_index_and_disabled_mode_falls_back_to_lexical_search(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            store = LMDBLanceDBStore(Path(tempdir) / "store", vector_dim=32)
            try:
                namespace = ("workspace", "demo", "project", "default", "memory", "project_shared")
                store.batch(
                    [
                        PutOp(
                            namespace=namespace,
                            key="item_1",
                            value={"content": "provider native embedding retrieval"},
                            index=["content"],
                        )
                    ]
                )
                applied = store.configure_retrieval(
                    {
                        "embedding": {
                            "mode": "provider",
                            "provider": {"provider_type": "mock", "name": "Mock Provider"},
                            "provider_id": "mock_provider",
                            "provider_name": "Mock Provider",
                            "provider_type": "mock",
                            "model": "mock-embed",
                            "model_name": "mock-embed",
                        }
                    }
                )
                self.assertTrue(applied["embedding_reindexed"])
                self.assertTrue(applied["retrieval"]["embedding"]["vector_enabled"])
                self.assertEqual(applied["retrieval"]["embedding"]["vector_dim"], 96)
                provider_results = store.batch([SearchOp(namespace_prefix=namespace, query="native embedding", limit=4)])
                self.assertGreaterEqual(len(provider_results[0]), 1)

                reverted = store.configure_retrieval({"embedding": {"mode": "disabled"}})
                self.assertTrue(reverted["embedding_reindexed"])
                self.assertEqual(reverted["retrieval"]["embedding"]["mode"], "disabled")
                self.assertFalse(reverted["retrieval"]["embedding"]["vector_enabled"])
                self.assertIsNone(reverted["retrieval"]["embedding"]["vector_dim"])
                lexical_results = store.batch([SearchOp(namespace_prefix=namespace, query="native embedding", limit=4)])
                self.assertGreaterEqual(len(lexical_results[0]), 1)
            finally:
                store.close()

    def test_background_maintenance_expires_ttl_without_read_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            adapter = LangMemAdapter(str(Path(tempdir) / "adapter"), long_term_ttl_minutes=0.001)
            try:
                storage = adapter.storage_info()
                self.assertEqual(storage["store"]["driver"], "sqlite")
                self.assertEqual(storage["kv"]["driver"], "sqlite")
                self.assertEqual(storage["vector"]["driver"], "sqlite")
                adapter.start_background_maintenance(interval_seconds=0.02)
                scope = MemoryScopes(
                    workspace_id="local-workspace",
                    project_id="default-project",
                    run_id="run-1",
                    agent_id="agent-1",
                ).project_shared()

                created = asyncio.run(adapter.remember(scope, [{"text": "this memory should expire in background"}]))
                self.assertEqual(len(created), 1)

                initial_count = int(adapter._store.conn.execute("SELECT COUNT(*) FROM store").fetchone()[0])
                self.assertGreaterEqual(initial_count, 1)

                deadline = time.time() + 2.0
                final_count = initial_count
                while time.time() < deadline:
                    final_count = int(adapter._store.conn.execute("SELECT COUNT(*) FROM store").fetchone()[0])
                    if final_count == 0:
                        break
                    time.sleep(0.05)

                self.assertEqual(final_count, 0)
            finally:
                adapter.close()
