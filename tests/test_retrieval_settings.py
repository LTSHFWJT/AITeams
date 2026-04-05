from __future__ import annotations

import base64
import io
import json
import sqlite3
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from aiteams.agent_center.service import AgentCenterService, DEFAULT_LOCAL_EMBEDDING_MODEL
from aiteams.api.application import ServiceContainer, WebApplication
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


def build_docx_bytes(*paragraphs: str) -> bytes:
    body = "".join(f"<w:p><w:r><w:t>{paragraph}</w:t></w:r></w:p>" for paragraph in paragraphs)
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{body}</w:body>"
        "</w:document>"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("word/document.xml", document_xml)
    return buffer.getvalue()


def build_pptx_bytes(*slides: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for index, slide in enumerate(slides, start=1):
            slide_xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
                'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
                "<p:cSld><p:spTree><p:sp><p:txBody><a:p><a:r>"
                f"<a:t>{slide}</a:t>"
                "</a:r></a:p></p:txBody></p:sp></p:spTree></p:cSld>"
                "</p:sld>"
            )
            archive.writestr(f"ppt/slides/slide{index}.xml", slide_xml)
    return buffer.getvalue()


def build_xlsx_bytes(rows: list[list[str]]) -> bytes:
    shared_strings: list[str] = []
    shared_indexes: dict[str, int] = {}
    row_xml: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        cell_xml: list[str] = []
        for column_index, value in enumerate(row, start=1):
            text = str(value)
            if text not in shared_indexes:
                shared_indexes[text] = len(shared_strings)
                shared_strings.append(text)
            ref = f"{chr(64 + column_index)}{row_index}"
            cell_xml.append(f'<c r="{ref}" t="s"><v>{shared_indexes[text]}</v></c>')
        row_xml.append(f'<row r="{row_index}">{"".join(cell_xml)}</row>')
    shared_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        + "".join(f"<si><t>{item}</t></si>" for item in shared_strings)
        + "</sst>"
    )
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(row_xml)}</sheetData>"
        "</worksheet>"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("xl/sharedStrings.xml", shared_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()


def build_ipynb_bytes() -> bytes:
    payload = {
        "cells": [
            {"cell_type": "markdown", "source": ["# Heading\n", "alpha"]},
            {
                "cell_type": "code",
                "source": ["print('beta')"],
                "outputs": [{"text": ["beta\n"]}],
            },
        ]
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


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

    def test_default_retrieval_settings_default_to_disabled_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            models_root = root / "models"
            models_root.mkdir(parents=True, exist_ok=True)

            store = build_store(root)
            service = AgentCenterService(store=store, local_models_root=models_root)

            try:
                service.ensure_local_model_defaults()
                service.ensure_retrieval_settings_defaults()

                settings = service.get_retrieval_settings()["settings"]
                runtime = service.retrieval_runtime_config()

                self.assertEqual(settings["embedding"]["mode"], "disabled")
                self.assertEqual(settings["rerank"]["mode"], "disabled")
                self.assertEqual(runtime["embedding"]["mode"], "disabled")
                self.assertEqual(runtime["rerank"]["mode"], "disabled")
                self.assertNotIn("model_name", settings["embedding"])
                self.assertNotIn("local_model_id", settings["embedding"])
            finally:
                store.close()

    def test_local_model_defaults_include_multilingual_embedding_model(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            models_root = root / "models"
            models_root.mkdir(parents=True, exist_ok=True)

            store = build_store(root)
            service = AgentCenterService(store=store, local_models_root=models_root)

            try:
                service.ensure_local_model_defaults()
                models = store.list_local_models()
                embed = next(
                    (
                        item
                        for item in models
                        if str(item.get("name") or "") == DEFAULT_LOCAL_EMBEDDING_MODEL
                        and str(item.get("model_type") or "") == "Embed"
                    ),
                    None,
                )
                self.assertIsNotNone(embed)
                self.assertEqual(
                    str((embed or {}).get("model_path") or ""),
                    "models/sentence-transformers__paraphrase-multilingual-MiniLM-L12-v2",
                )
            finally:
                store.close()

    def test_default_retrieval_settings_do_not_override_existing_records(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            models_root = root / "models"
            models_root.mkdir(parents=True, exist_ok=True)

            store = build_store(root)
            service = AgentCenterService(store=store, local_models_root=models_root)
            try:
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
            finally:
                store.close()

    def test_retrieval_settings_apply_failure_rolls_back_persisted_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            agent_center = AgentCenterService(store=store, local_models_root=root / "models")
            agent_center.ensure_retrieval_settings_defaults()

            memory_calls: list[dict[str, object]] = []
            knowledge_calls: list[dict[str, object]] = []

            class FakeMemory:
                def configure_retrieval(self, settings: dict[str, object] | None = None) -> dict[str, object]:
                    payload = dict(settings or {})
                    memory_calls.append(payload)
                    embedding = dict(payload.get("embedding") or {})
                    if str(embedding.get("mode") or "") == "local":
                        raise ValueError("embedding probe failed")
                    return {"retrieval": payload}

            class FakeKnowledgeBases:
                def configure_retrieval(self, settings: dict[str, object] | None = None) -> dict[str, object]:
                    payload = dict(settings or {})
                    knowledge_calls.append(payload)
                    return {"retrieval": payload}

                def close(self) -> None:
                    return None

            class FakePlugins:
                def close(self) -> None:
                    return None

            app = WebApplication(
                ServiceContainer(
                    store=store,
                    runtime=SimpleNamespace(agent_kernel=SimpleNamespace(memory=FakeMemory())),
                    workspace=SimpleNamespace(),
                    agent_center=agent_center,
                    plugins=FakePlugins(),
                    knowledge_bases=FakeKnowledgeBases(),
                    static_dir=root,
                    local_models_root=root / "models",
                )
            )

            try:
                response = app.handle(
                    "PUT",
                    "/api/agent-center/retrieval-settings",
                    body=json.dumps(
                        {
                            "embedding": {"mode": "local", "model_name": "BAAI/bge-m3"},
                            "rerank": {"mode": "disabled"},
                        }
                    ).encode("utf-8"),
                )

                self.assertEqual(response.status, 400)
                payload = json.loads(response.body.decode("utf-8"))
                self.assertIn("rolled back", str(payload.get("detail") or ""))
                saved = agent_center.get_retrieval_settings()["settings"]
                self.assertEqual(saved["embedding"]["mode"], "disabled")
                self.assertEqual(saved["rerank"]["mode"], "disabled")
                self.assertEqual(len(memory_calls), 2)
                self.assertEqual(str((memory_calls[-1].get("embedding") or {}).get("mode") or ""), "disabled")
                self.assertEqual(len(knowledge_calls), 1)
                self.assertEqual(str((knowledge_calls[-1].get("embedding") or {}).get("mode") or ""), "disabled")
            finally:
                app.close()

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

    def test_knowledge_service_strips_openai_v1_from_ollama_litellm_base_url(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )

            try:
                self.assertEqual(
                    service._resolve_litellm_api_base(
                        {
                            "provider_type": "ollama",
                            "base_url": "http://127.0.0.1:11434/v1",
                        }
                    ),
                    "http://127.0.0.1:11434",
                )
                self.assertEqual(
                    service._resolve_litellm_api_base(
                        {
                            "provider_type": "ollama",
                            "base_url": "http://127.0.0.1:11434",
                        }
                    ),
                    "http://127.0.0.1:11434",
                )
            finally:
                service.close()
                store.close()

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

            try:
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
            finally:
                service.close()
                store.close()

    def test_manage_document_embeddings_requires_embed_configuration(self) -> None:
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

            try:
                with self.assertRaisesRegex(ValueError, "Embed 模型"):
                    service.manage_document_embeddings("kb-docs", action="add", document_ids=["doc-1"])
            finally:
                service.close()
                store.close()

    def test_manage_document_embeddings_refreshes_embed_configuration_from_loader(self) -> None:
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

            agent_center = AgentCenterService(store=store, local_models_root=root / "models")
            agent_center.save_retrieval_settings(
                {
                    "embedding": {"mode": "local", "model_name": "BAAI/bge-m3"},
                    "rerank": {"mode": "disabled"},
                }
            )

            service = KnowledgeBaseService(
                store=store,
                root_dir=root / "knowledge",
                retrieval_runtime=None,
            )
            service.set_retrieval_runtime_loader(agent_center.retrieval_runtime_config)

            def fake_ensure_retrieval_loaded() -> None:
                service._embedding_model = object()
                service._embedding_dimension = 3
                service._embedding_runtime = {
                    "mode": "local",
                    "backend": "huggingface",
                    "model_name": "BAAI/bge-m3",
                    "vector_enabled": True,
                    "vector_dim": 3,
                }

            try:
                with patch.object(service, "_ensure_retrieval_loaded_for_embedding", fake_ensure_retrieval_loaded):
                    added = service.manage_document_embeddings("kb-docs", action="add", document_ids=["doc-1"])

                self.assertEqual(added["affected_count"], 1)
                self.assertEqual(added["items"][0]["embedding_status"], "embedded")
                self.assertEqual(service.retrieval_info()["embedding"]["mode"], "local")
            finally:
                service.close()
                store.close()

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

            try:
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
            finally:
                service.close()
                store.close()

    def test_manage_document_embeddings_save_syncs_removed_and_pending_documents(self) -> None:
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
                title="embedded.txt",
                source_path="embedded.txt",
                content_text="alpha beta gamma\n" * 20,
                metadata={"file_size": 128, "chunk_count": 2, "embedding_status": "embedded", "embedded_chunk_count": 2},
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-2",
                knowledge_base_id="kb-docs",
                key="doc-2",
                title="pending.txt",
                source_path="pending.txt",
                content_text="delta epsilon zeta\n" * 20,
                metadata={"file_size": 128, "chunk_count": 2, "embedding_status": "not_embedded", "embedded_chunk_count": 0},
            )
            store.save_knowledge_document(
                knowledge_document_id="doc-3",
                knowledge_base_id="kb-docs",
                key="doc-3",
                title="stable.txt",
                source_path="stable.txt",
                content_text="theta iota kappa\n" * 20,
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

            try:
                removed = service.delete_document("doc-1")
                self.assertIsNotNone(removed)
                with (
                    patch.object(service, "_delete_document_vectors", fake_delete_document_vectors),
                    patch.object(service, "_ensure_retrieval_loaded_for_embedding", lambda: None),
                ):
                    synced = service.manage_document_embeddings("kb-docs", action="save", document_ids=[])

                self.assertEqual(synced["action"], "save")
                self.assertEqual(synced["affected_count"], 2)
                self.assertEqual(synced["skipped_count"], 0)
                self.assertEqual(delete_calls, [("kb-docs", "doc-1")])
                self.assertEqual([item["id"] for item in synced["items"]], ["doc-2"])

                deleted_document = store.get_knowledge_document("doc-1")
                self.assertIsNone(deleted_document)

                embedded_document = store.get_knowledge_document("doc-2")
                self.assertIsNotNone(embedded_document)
                embedded_metadata = dict((embedded_document or {}).get("metadata_json") or {})
                self.assertEqual(embedded_metadata.get("embedding_status"), "embedded")
                self.assertGreaterEqual(int(embedded_metadata.get("embedded_chunk_count") or 0), 1)

                stable_document = store.get_knowledge_document("doc-3")
                self.assertIsNotNone(stable_document)
                stable_metadata = dict((stable_document or {}).get("metadata_json") or {})
                self.assertEqual(stable_metadata.get("embedding_status"), "embedded")
                self.assertEqual(int(stable_metadata.get("embedded_chunk_count") or 0), 2)
            finally:
                service.close()
                store.close()

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

            try:
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
            finally:
                service.close()
                store.close()

    def test_start_document_embedding_job_requires_embed_configuration(self) -> None:
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

            try:
                with self.assertRaisesRegex(ValueError, "Embed 模型"):
                    service.start_document_embedding_job("kb-docs", action="add", document_ids=["doc-1"])
                self.assertIsNone(store.get_active_knowledge_embedding_job("kb-docs"))
            finally:
                service.close()
                store.close()

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

            try:
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
            finally:
                service.close()
                store.close()

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

            try:
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
            finally:
                service.close()
                store.close()

    def test_pool_upload_extracts_docx_without_textutil(self) -> None:
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

            try:
                with patch.object(service, "_extract_with_command", return_value=""):
                    uploaded = service.import_pool_uploaded_files(
                        {
                            "knowledge_base_id": "kb-docs",
                            "files": [
                                {
                                    "path": "docs/guide.docx",
                                    "content_base64": base64.b64encode(
                                        build_docx_bytes("alpha paragraph", "beta paragraph")
                                    ).decode("ascii"),
                                }
                            ],
                        }
                    )

                self.assertEqual(uploaded["imported_count"], 1)
                self.assertEqual(uploaded["skipped_count"], 0)
                self.assertEqual(uploaded["items"][0]["title"], "guide.docx")
                stored = store.get_knowledge_pool_document(str(uploaded["items"][0]["id"]))
                self.assertIsNotNone(stored)
                self.assertIn("alpha paragraph", str((stored or {}).get("content_text") or ""))
                self.assertIn("beta paragraph", str((stored or {}).get("content_text") or ""))
                self.assertEqual(dict((stored or {}).get("metadata_json") or {}).get("extension"), ".docx")
            finally:
                service.close()
                store.close()

    def test_pool_upload_extracts_multiple_structured_formats_with_internal_fallbacks(self) -> None:
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

            try:
                with (
                    patch.object(service, "_extract_with_llama_reader", return_value=""),
                    patch.object(service, "_extract_with_office_converter", return_value=""),
                ):
                    uploaded = service.import_pool_uploaded_files(
                        {
                            "knowledge_base_id": "kb-docs",
                            "files": [
                                {
                                    "path": "docs/deck.pptx",
                                    "content_base64": base64.b64encode(
                                        build_pptx_bytes("slide alpha", "slide beta")
                                    ).decode("ascii"),
                                },
                                {
                                    "path": "docs/table.xlsx",
                                    "content_base64": base64.b64encode(
                                        build_xlsx_bytes([["name", "score"], ["alice", "100"]])
                                    ).decode("ascii"),
                                },
                                {
                                    "path": "docs/notebook.ipynb",
                                    "content_base64": base64.b64encode(build_ipynb_bytes()).decode("ascii"),
                                },
                                {
                                    "path": "docs/sample.rtf",
                                    "content_base64": base64.b64encode(
                                        b"{\\rtf1\\ansi hello \\b world\\b0}"
                                    ).decode("ascii"),
                                },
                                {
                                    "path": "docs/table.csv",
                                    "content_base64": base64.b64encode(
                                        "name,score\nalice,100\n".encode("utf-8")
                                    ).decode("ascii"),
                                },
                            ],
                        }
                    )

                self.assertEqual(uploaded["imported_count"], 5)
                self.assertEqual(uploaded["skipped_count"], 0)

                stored_by_title = {
                    str(item.get("title") or ""): store.get_knowledge_pool_document(str(item.get("id") or ""))
                    for item in uploaded["items"]
                }
                self.assertIn("slide alpha", str((stored_by_title["deck.pptx"] or {}).get("content_text") or ""))
                self.assertIn("Sheet 1", str((stored_by_title["table.xlsx"] or {}).get("content_text") or ""))
                self.assertIn("alice", str((stored_by_title["table.xlsx"] or {}).get("content_text") or ""))
                self.assertIn("Heading", str((stored_by_title["notebook.ipynb"] or {}).get("content_text") or ""))
                self.assertIn("beta", str((stored_by_title["notebook.ipynb"] or {}).get("content_text") or ""))
                self.assertIn("hello world", str((stored_by_title["sample.rtf"] or {}).get("content_text") or ""))
                self.assertIn("name, score", str((stored_by_title["table.csv"] or {}).get("content_text") or ""))
                self.assertEqual(dict((stored_by_title["deck.pptx"] or {}).get("metadata_json") or {}).get("extension"), ".pptx")
                self.assertEqual(dict((stored_by_title["table.xlsx"] or {}).get("metadata_json") or {}).get("extension"), ".xlsx")
                self.assertEqual(dict((stored_by_title["notebook.ipynb"] or {}).get("metadata_json") or {}).get("extension"), ".ipynb")
                self.assertEqual(dict((stored_by_title["sample.rtf"] or {}).get("metadata_json") or {}).get("content_type"), "application/rtf")
                self.assertEqual(dict((stored_by_title["table.csv"] or {}).get("metadata_json") or {}).get("content_type"), "text/csv")
            finally:
                service.close()
                store.close()

    def test_pool_upload_skipped_items_include_title(self) -> None:
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

            try:
                with patch.object(service, "_store_uploaded_file_to_pool", side_effect=ValueError("当前环境不支持该 Office 文档的文本提取。")):
                    result = service.import_pool_uploaded_files(
                        {
                            "knowledge_base_id": "kb-docs",
                            "files": [
                                {
                                    "path": "docs/report.docx",
                                    "content_base64": base64.b64encode(b"placeholder").decode("ascii"),
                                }
                            ],
                        }
                    )

                self.assertEqual(result["imported_count"], 0)
                self.assertEqual(result["skipped_count"], 1)
                self.assertEqual(result["skipped"][0]["title"], "report.docx")
                self.assertEqual(result["skipped"][0]["path"], "docs/report.docx")
            finally:
                service.close()
                store.close()
