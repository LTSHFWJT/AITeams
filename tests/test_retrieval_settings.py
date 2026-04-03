from __future__ import annotations

import base64
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from aiteams.agent_center.service import AgentCenterService
from aiteams.knowledge.service import KnowledgeBaseService
from aiteams.storage.metadata import MetadataStore


def build_store(root: Path) -> MetadataStore:
    return MetadataStore(
        root / "metadata.sqlite3",
        default_workspace_id="workspace-default",
        default_workspace_name="Default Workspace",
        default_project_id="project-default",
        default_project_name="Default Project",
        workspace_root=root,
    )


class RetrievalSettingsTests(unittest.TestCase):
    def test_metadata_store_migrates_knowledge_bases_description_column_away(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            database_path = root / "metadata.sqlite3"
            connection = sqlite3.connect(str(database_path))
            connection.execute(
                """
                CREATE TABLE knowledge_bases (
                    id TEXT PRIMARY KEY,
                    key TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    config_json TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO knowledge_bases(id, key, name, description, config_json, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("kb-legacy", "kb-legacy", "旧知识库", "旧简介", "{}", "active", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            )
            connection.commit()
            connection.close()

            store = build_store(root)

            columns = {str(row["name"]) for row in store.fetch_all("PRAGMA table_info(knowledge_bases)")}
            self.assertNotIn("description", columns)
            migrated = store.get_knowledge_base("kb-legacy")
            self.assertIsNotNone(migrated)
            self.assertEqual((migrated or {}).get("name"), "旧知识库")
            self.assertNotIn("description", migrated or {})

    def test_default_retrieval_settings_seed_embedding_bge_m3_and_disable_rerank(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            models_root = root / "models"
            models_root.mkdir(parents=True, exist_ok=True)

            store = build_store(root)
            service = AgentCenterService(store=store, local_models_root=models_root)

            service.ensure_local_model_defaults()
            service.ensure_retrieval_settings_defaults()

            settings = service.get_retrieval_settings()["settings"]
            runtime = service.retrieval_runtime_config()

            self.assertEqual(settings["embedding"]["mode"], "local")
            self.assertEqual(settings["embedding"]["model_name"], "BAAI/bge-m3")
            self.assertEqual(settings["rerank"]["mode"], "disabled")
            self.assertEqual(runtime["embedding"]["mode"], "local")
            self.assertEqual(runtime["embedding"]["model_name"], "BAAI/bge-m3")
            self.assertEqual(runtime["rerank"]["mode"], "disabled")

    def test_default_retrieval_settings_do_not_override_existing_records(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            models_root = root / "models"
            models_root.mkdir(parents=True, exist_ok=True)

            store = build_store(root)
            service = AgentCenterService(store=store, local_models_root=models_root)
            service.save_retrieval_settings(
                {
                    "embedding": {"mode": "disabled"},
                    "rerank": {"mode": "disabled"},
                }
            )

            service.ensure_retrieval_settings_defaults()

            settings = service.get_retrieval_settings()["settings"]
            self.assertEqual(settings["embedding"]["mode"], "disabled")
            self.assertEqual(settings["rerank"]["mode"], "disabled")

    def test_agent_center_preserves_managed_local_model_runtime_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            models_root = root / "models"
            models_root.mkdir(parents=True, exist_ok=True)
            (models_root / "BAAI__bge-m3").mkdir()
            (models_root / "BAAI__bge-reranker-v2-m3").mkdir()

            store = build_store(root)
            store.save_local_model(
                local_model_id="embed-local-id",
                name="BAAI/bge-m3",
                model_type="Embed",
                model_path="BAAI__bge-m3",
            )
            store.save_local_model(
                local_model_id="rerank-local-id",
                name="BAAI/bge-reranker-v2-m3",
                model_type="Rerank",
                model_path="BAAI__bge-reranker-v2-m3",
            )

            service = AgentCenterService(store=store, local_models_root=models_root)
            saved = service.save_retrieval_settings(
                {
                    "embedding": {"mode": "local", "local_model_id": "embed-local-id"},
                    "rerank": {"mode": "local", "local_model_id": "rerank-local-id"},
                }
            )

            self.assertEqual(saved["settings"]["embedding"]["backend"], "huggingface")
            self.assertEqual(saved["settings"]["embedding"]["local_model_id"], "embed-local-id")
            self.assertEqual(saved["settings"]["embedding"]["model_label"], "BAAI/bge-m3")
            self.assertEqual(saved["settings"]["rerank"]["backend"], "flag_embedding")
            self.assertEqual(saved["settings"]["rerank"]["local_model_id"], "rerank-local-id")
            self.assertEqual(saved["settings"]["rerank"]["model_label"], "BAAI/bge-reranker-v2-m3")
            self.assertEqual(saved["runtime"]["embedding"]["model_type"], "Embed")
            self.assertEqual(saved["runtime"]["rerank"]["model_type"], "Rerank")
            self.assertTrue(saved["runtime"]["embedding"]["model"].endswith("models/BAAI__bge-m3"))
            self.assertTrue(saved["runtime"]["rerank"]["model"].endswith("models/BAAI__bge-reranker-v2-m3"))

    def test_knowledge_service_keeps_local_model_metadata_without_loading_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)

            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )

            class FakeEmbedding:
                def __init__(self, *, model_name: str, **_: object) -> None:
                    raise AssertionError("configure_retrieval should not instantiate embedding runtime")

                def get_query_embedding(self, _query: str) -> list[float]:
                    return [0.1, 0.2, 0.3]

            class FakeReranker:
                def __init__(self, *, model: str, top_n: int, use_fp16: bool) -> None:
                    raise AssertionError("configure_retrieval should not instantiate rerank runtime")

            runtime = {
                "embedding": {
                    "mode": "local",
                    "model": str(root / "models" / "BAAI__bge-m3"),
                    "model_name": str(root / "models" / "BAAI__bge-m3"),
                    "model_path": "BAAI__bge-m3",
                    "model_label": "BAAI/bge-m3",
                    "local_model_id": "embed-local-id",
                },
                "rerank": {
                    "mode": "local",
                    "model": str(root / "models" / "BAAI__bge-reranker-v2-m3"),
                    "model_name": str(root / "models" / "BAAI__bge-reranker-v2-m3"),
                    "model_path": "BAAI__bge-reranker-v2-m3",
                    "model_label": "BAAI/bge-reranker-v2-m3",
                    "local_model_id": "rerank-local-id",
                },
            }

            with (
                patch("aiteams.knowledge.service.HuggingFaceEmbedding", FakeEmbedding),
                patch("aiteams.knowledge.service.FlagEmbeddingReranker", FakeReranker),
            ):
                applied = service.configure_retrieval(runtime)

            self.assertTrue(applied["retrieval"]["embedding"]["vector_enabled"])
            self.assertEqual(applied["retrieval"]["embedding"]["backend"], "huggingface")
            self.assertEqual(applied["retrieval"]["embedding"]["local_model_id"], "embed-local-id")
            self.assertEqual(applied["retrieval"]["embedding"]["model_label"], "BAAI/bge-m3")
            self.assertEqual(applied["retrieval"]["embedding"]["model_path"], "BAAI__bge-m3")
            self.assertTrue(applied["retrieval"]["embedding"]["resolved_model_name"].endswith("BAAI__bge-m3"))
            self.assertFalse(applied["retrieval"]["embedding"]["runtime_loaded"])
            self.assertIsNone(applied["retrieval"]["embedding"]["vector_dim"])
            self.assertEqual(applied["retrieval"]["rerank"]["backend"], "flag_embedding")
            self.assertEqual(applied["retrieval"]["rerank"]["local_model_id"], "rerank-local-id")
            self.assertEqual(applied["retrieval"]["rerank"]["model_label"], "BAAI/bge-reranker-v2-m3")
            self.assertEqual(applied["retrieval"]["rerank"]["model_path"], "BAAI__bge-reranker-v2-m3")
            self.assertTrue(applied["retrieval"]["rerank"]["resolved_model_name"].endswith("BAAI__bge-reranker-v2-m3"))
            self.assertFalse(applied["retrieval"]["rerank"]["runtime_loaded"])

    def test_knowledge_service_loads_local_runtime_only_when_embedding_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)

            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )

            embed_calls: list[str] = []
            rerank_calls: list[str] = []

            class FakeEmbedding:
                def __init__(self, *, model_name: str, **_: object) -> None:
                    embed_calls.append(model_name)
                    self.model_name = model_name

                def get_query_embedding(self, _query: str) -> list[float]:
                    return [0.1, 0.2, 0.3]

            class FakeReranker:
                def __init__(self, *, model: str, top_n: int, use_fp16: bool) -> None:
                    rerank_calls.append(model)
                    self.model = model
                    self.top_n = top_n
                    self.use_fp16 = use_fp16

            runtime = {
                "embedding": {
                    "mode": "local",
                    "model": str(root / "models" / "BAAI__bge-m3"),
                    "model_name": str(root / "models" / "BAAI__bge-m3"),
                    "model_path": "BAAI__bge-m3",
                    "model_label": "BAAI/bge-m3",
                    "local_model_id": "embed-local-id",
                },
                "rerank": {
                    "mode": "local",
                    "model": str(root / "models" / "BAAI__bge-reranker-v2-m3"),
                    "model_name": str(root / "models" / "BAAI__bge-reranker-v2-m3"),
                    "model_path": "BAAI__bge-reranker-v2-m3",
                    "model_label": "BAAI/bge-reranker-v2-m3",
                    "local_model_id": "rerank-local-id",
                },
            }

            with (
                patch("aiteams.knowledge.service.HuggingFaceEmbedding", FakeEmbedding),
                patch("aiteams.knowledge.service.FlagEmbeddingReranker", FakeReranker),
                patch.object(service, "_require_vector_dependencies", lambda: None),
                patch.object(service, "_require_local_embedding_dependencies", lambda: None),
                patch.object(service, "_require_postprocessor_base_dependencies", lambda: None),
                patch.object(service, "_require_local_rerank_dependencies", lambda: None),
            ):
                service.configure_retrieval(runtime)
                self.assertEqual(embed_calls, [])
                self.assertEqual(rerank_calls, [])
                service._ensure_retrieval_loaded_for_embedding()

            self.assertEqual(embed_calls, [str(root / "models" / "BAAI__bge-m3")])
            self.assertEqual(rerank_calls, [str(root / "models" / "BAAI__bge-reranker-v2-m3")])
            self.assertTrue(service.retrieval_info()["embedding"]["runtime_loaded"])
            self.assertTrue(service.retrieval_info()["rerank"]["runtime_loaded"])
            self.assertEqual(service.retrieval_info()["embedding"]["vector_dim"], 3)

    def test_manage_document_embeddings_supports_add_and_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            knowledge_base = store.save_knowledge_base(
                knowledge_base_id="kb-docs",
                key="kb-docs",
                name="知识库",
                config={},
            )
            document = store.save_knowledge_document(
                knowledge_document_id="doc-1",
                knowledge_base_id="kb-docs",
                key="doc-1",
                title="doc.txt",
                source_path="doc.txt",
                content_text="alpha beta gamma\n" * 20,
                metadata={"file_size": 128, "chunk_count": 2, "embedding_status": "not_embedded", "embedded_chunk_count": 0},
            )

            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )
            service._embedding_model = object()
            service._embedding_dimension = 3
            service._embedding_runtime = {
                "mode": "local",
                "backend": "huggingface",
                "model_name": "BAAI/bge-m3",
                "model_label": "BAAI/bge-m3",
                "vector_enabled": True,
                "vector_dim": 3,
            }

            deleted_pairs: list[tuple[str, str]] = []

            def fake_delete_document_vectors(*, knowledge_base_id: str, document_id: str) -> None:
                deleted_pairs.append((knowledge_base_id, document_id))

            with patch.object(service, "_delete_document_vectors", fake_delete_document_vectors):
                added = service.manage_document_embeddings("kb-docs", action="add", document_ids=["doc-1"])
                deleted = service.manage_document_embeddings("kb-docs", action="delete", document_ids=["doc-1"])

            self.assertEqual(added["affected_count"], 1)
            self.assertEqual(added["items"][0]["embedding_status"], "embedded")
            self.assertGreaterEqual(added["items"][0]["embedded_chunk_count"], 1)
            self.assertEqual(deleted["affected_count"], 1)
            self.assertEqual(deleted["items"][0]["embedding_status"], "not_embedded")
            self.assertEqual(deleted["items"][0]["embedded_chunk_count"], 0)
            self.assertEqual(deleted_pairs, [("kb-docs", "doc-1")])
            refreshed = store.get_knowledge_document("doc-1")
            self.assertIsNotNone(refreshed)
            refreshed_metadata = dict((refreshed or {}).get("metadata_json") or {})
            self.assertEqual(refreshed_metadata.get("embedding_status"), "not_embedded")
            self.assertEqual(int(refreshed_metadata.get("embedded_chunk_count") or 0), 0)

    def test_manage_document_embeddings_supports_reembed_and_skips_duplicate_add(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            store.save_knowledge_base(
                knowledge_base_id="kb-docs",
                key="kb-docs",
                name="知识库",
                config={},
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-1",
                knowledge_base_id="kb-docs",
                key="doc-1",
                title="doc.txt",
                source_path="doc.txt",
                content_text="alpha beta gamma\n" * 20,
                metadata={"file_size": 128, "chunk_count": 2, "embedding_status": "embedded", "embedded_chunk_count": 2},
            )

            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )
            service._embedding_model = object()
            service._embedding_dimension = 3
            service._embedding_runtime = {
                "mode": "local",
                "backend": "huggingface",
                "model_name": "BAAI/bge-m3",
                "model_label": "BAAI/bge-m3",
                "vector_enabled": True,
                "vector_dim": 3,
            }

            delete_calls: list[tuple[str, str]] = []

            def fake_delete_document_vectors(*, knowledge_base_id: str, document_id: str) -> None:
                delete_calls.append((knowledge_base_id, document_id))

            with patch.object(service, "_delete_document_vectors", fake_delete_document_vectors):
                skipped = service.manage_document_embeddings("kb-docs", action="add", document_ids=["doc-1"])
                reembedded = service.manage_document_embeddings("kb-docs", action="reembed", document_ids=["doc-1"])

            self.assertEqual(skipped["affected_count"], 0)
            self.assertEqual(skipped["skipped_count"], 1)
            self.assertEqual(skipped["skipped"][0]["reason"], "skipped")
            self.assertEqual(reembedded["affected_count"], 1)
            self.assertEqual(reembedded["items"][0]["embedding_status"], "embedded")
            self.assertGreaterEqual(reembedded["items"][0]["embedded_chunk_count"], 1)
            self.assertEqual(delete_calls, [("kb-docs", "doc-1")])
            refreshed = store.get_knowledge_document("doc-1")
            self.assertIsNotNone(refreshed)
            refreshed_metadata = dict((refreshed or {}).get("metadata_json") or {})
            self.assertEqual(refreshed_metadata.get("embedding_status"), "embedded")
            self.assertGreaterEqual(int(refreshed_metadata.get("embedded_chunk_count") or 0), 1)

    def test_start_document_embedding_job_persists_progress_and_result(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            store.save_knowledge_base(
                knowledge_base_id="kb-docs",
                key="kb-docs",
                name="知识库",
                config={},
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-1",
                knowledge_base_id="kb-docs",
                key="doc-1",
                title="doc.txt",
                source_path="doc.txt",
                content_text="alpha beta gamma\n" * 20,
                metadata={"file_size": 128, "chunk_count": 2, "embedding_status": "not_embedded", "embedded_chunk_count": 0},
            )

            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )
            service._embedding_model = object()
            service._embedding_dimension = 3
            service._embedding_runtime = {
                "mode": "local",
                "backend": "huggingface",
                "model_name": "BAAI/bge-m3",
                "model_label": "BAAI/bge-m3",
                "vector_enabled": True,
                "vector_dim": 3,
            }

            with patch.object(service, "_ensure_retrieval_loaded_for_embedding", lambda: None):
                started = service.start_document_embedding_job("kb-docs", action="add", document_ids=["doc-1"])

                self.assertFalse(started["reused"])
                job_id = str(started["job"]["id"] or "")
                self.assertTrue(job_id)

                latest = None
                for _ in range(100):
                    latest = service.get_document_embedding_job(job_id)
                    if latest and latest["status"] in {"completed", "error"}:
                        break
                    time.sleep(0.05)

            self.assertIsNotNone(latest)
            self.assertEqual((latest or {}).get("status"), "completed")
            self.assertGreaterEqual(float((latest or {}).get("progress_percent") or 0.0), 100.0)
            result = dict((latest or {}).get("result") or {})
            self.assertEqual(result.get("affected_count"), 1)
            self.assertEqual(result.get("action"), "add")

    def test_list_documents_page_supports_query_status_and_pagination(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            store.save_knowledge_base(
                knowledge_base_id="kb-docs",
                key="kb-docs",
                name="知识库",
                config={},
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-1",
                knowledge_base_id="kb-docs",
                key="doc-1",
                title="Alpha Notes",
                source_path="notes/alpha.md",
                content_text="alpha content",
                metadata={"file_size": 10, "chunk_count": 2, "embedding_status": "embedded", "embedded_chunk_count": 2},
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-2",
                knowledge_base_id="kb-docs",
                key="doc-2",
                title="Beta Draft",
                source_path="drafts/beta.md",
                content_text="beta content",
                metadata={"file_size": 12, "chunk_count": 1, "embedding_status": "not_embedded", "embedded_chunk_count": 0},
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-3",
                knowledge_base_id="kb-docs",
                key="doc-3",
                title="Alpha Appendix",
                source_path="appendix/alpha.txt",
                content_text="appendix",
                metadata={"file_size": 8, "chunk_count": 1, "embedding_status": "embedded", "embedded_chunk_count": 1},
            )

            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )

            filtered = service.list_documents_page(
                knowledge_base_id="kb-docs",
                limit=1,
                offset=0,
                query="alpha",
                embedding_status="embedded",
            )
            next_page = service.list_documents_page(
                knowledge_base_id="kb-docs",
                limit=1,
                offset=1,
                query="alpha",
                embedding_status="embedded",
            )

            self.assertEqual(filtered["total"], 2)
            self.assertEqual(filtered["limit"], 1)
            self.assertEqual(filtered["filters"]["query"], "alpha")
            self.assertEqual(filtered["filters"]["embedding_status"], "embedded")
            self.assertEqual(len(filtered["items"]), 1)
            self.assertEqual(len(next_page["items"]), 1)
            returned_ids = {filtered["items"][0]["id"], next_page["items"][0]["id"]}
            self.assertEqual(returned_ids, {"doc-1", "doc-3"})

    def test_knowledge_base_resource_reports_vector_count_and_embedding_model(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            knowledge_base = store.save_knowledge_base(
                knowledge_base_id="kb-docs",
                key="kb-docs",
                name="知识库",
                config={},
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-1",
                knowledge_base_id="kb-docs",
                key="doc-1",
                title="Alpha Notes",
                source_path="notes/alpha.md",
                content_text="alpha content",
                document_status="embedded",
                embedded_at="2026-04-01T00:00:00Z",
                metadata={
                    "embedding_status": "embedded",
                    "embedded_chunk_count": 3,
                    "embedding_model_name": "BAAI/bge-m3",
                    "embedding_model_label": "BAAI/bge-m3",
                },
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-2",
                knowledge_base_id="kb-docs",
                key="doc-2",
                title="Beta Draft",
                source_path="drafts/beta.md",
                content_text="beta content",
                document_status="embedded",
                embedded_at="2026-04-02T00:00:00Z",
                metadata={
                    "embedding_status": "embedded",
                    "embedded_chunk_count": 4,
                    "embedding_model_name": "BAAI/bge-m3",
                    "embedding_model_label": "BAAI/bge-m3",
                },
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-3",
                knowledge_base_id="kb-docs",
                key="doc-3",
                title="Gamma Removed",
                source_path="drafts/gamma.md",
                content_text="gamma content",
                document_status="removed",
                embedded_at="2026-04-03T00:00:00Z",
                metadata={
                    "embedding_status": "removed",
                    "embedded_chunk_count": 9,
                    "embedding_model_name": "ignored/model",
                    "embedding_model_label": "ignored/model",
                },
            )

            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )

            payload = service._knowledge_base_resource(knowledge_base)

            self.assertEqual(payload["document_count"], 2)
            self.assertEqual(payload["file_count"], 2)
            self.assertEqual(payload["vector_count"], 7)
            self.assertEqual(payload["embedding_model_name"], "BAAI/bge-m3")
            self.assertEqual(payload["embedding_model_label"], "BAAI/bge-m3")

    def test_knowledge_pool_documents_can_be_added_to_kb_and_block_pool_delete_while_in_use(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            store.save_knowledge_base(
                knowledge_base_id="kb-docs",
                key="kb-docs",
                name="知识库",
                config={},
            )
            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )
            service._embedding_model = object()
            service._embedding_dimension = 3
            service._embedding_runtime = {
                "mode": "local",
                "backend": "huggingface",
                "model_name": "BAAI/bge-m3",
                "model_label": "BAAI/bge-m3",
                "vector_enabled": True,
                "vector_dim": 3,
            }

            with patch.object(service, "_delete_document_vectors", lambda **_: None), patch.object(
                service, "_index_document_chunks", lambda **_: 2
            ):
                uploaded = service.import_pool_uploaded_files(
                    {
                        "files": [
                            {
                                "path": "alpha/guide.txt",
                                "content_base64": base64.b64encode(b"alpha beta gamma\n" * 30).decode("ascii"),
                            }
                        ]
                    }
                )
                pool_id = uploaded["items"][0]["id"]
                added = service.add_pool_documents_to_knowledge_base("kb-docs", pool_document_ids=[pool_id])
                blocked_delete = service.manage_pool_documents(action="delete", document_ids=[pool_id])
                kb_document_id = added["items"][0]["id"]
                service.delete_document(kb_document_id)
                deleted = service.manage_pool_documents(action="delete", document_ids=[pool_id])

            self.assertEqual(uploaded["imported_count"], 1)
            self.assertEqual(added["affected_count"], 1)
            self.assertEqual(added["items"][0]["pool_document_id"], pool_id)
            self.assertEqual(blocked_delete["affected_count"], 0)
            self.assertEqual(blocked_delete["skipped_count"], 1)
            self.assertEqual(blocked_delete["skipped"][0]["reason"], "in-use")
            self.assertEqual(deleted["affected_count"], 1)
            self.assertIsNone(store.get_knowledge_pool_document(pool_id))

    def test_pool_upload_deduplicates_blob_storage_for_same_kb(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            store.save_knowledge_base(
                knowledge_base_id="kb-docs",
                key="kb-docs",
                name="知识库",
                config={},
            )
            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )

            service.import_pool_uploaded_files(
                {
                    "knowledge_base_id": "kb-docs",
                    "files": [
                        {
                            "path": "alpha/guide-a.txt",
                            "content_base64": base64.b64encode(b"same body\n" * 16).decode("ascii"),
                        }
                    ],
                }
            )
            service.import_pool_uploaded_files(
                {
                    "knowledge_base_id": "kb-docs",
                    "files": [
                        {
                            "path": "beta/guide-b.txt",
                            "content_base64": base64.b64encode(b"same body\n" * 16).decode("ascii"),
                        }
                    ],
                }
            )

            self.assertEqual(len(store.list_knowledge_file_blobs()), 1)
            self.assertEqual(len(store.list_knowledge_pool_documents(knowledge_base_id="kb-docs")), 1)
