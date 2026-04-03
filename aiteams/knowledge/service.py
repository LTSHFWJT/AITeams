from __future__ import annotations

import base64
import hashlib
from importlib import metadata as importlib_metadata
import json
import logging
import mimetypes
import os
import re
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path, PurePosixPath
from typing import Any

try:  # pragma: no cover - optional runtime dependency
    import xxhash
except Exception:  # pragma: no cover - optional runtime dependency
    xxhash = None

from aiteams.ai_gateway import AIGateway, ProviderRequestError
from aiteams.catalog import preset_for
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import make_uuid7, trim_text, utcnow_iso


MAX_UPLOAD_TOTAL_BYTES = 100 * 1024 * 1024
MAX_UPLOAD_FILE_BYTES = 25 * 1024 * 1024
CHUNK_SIZE = 1400
CHUNK_OVERLAP = 220
DEFAULT_MOCK_EMBED_DIM = 256
DEFAULT_EMBED_BATCH_SIZE = 16
VECTOR_HEAD_MULTIPLIER = 4
TEXT_EXTENSIONS = {
    ".txt",
    ".md",
    ".mdx",
    ".markdown",
    ".rst",
    ".log",
    ".csv",
    ".tsv",
    ".json",
    ".jsonl",
    ".yaml",
    ".yml",
    ".xml",
    ".html",
    ".htm",
    ".css",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".py",
    ".java",
    ".go",
    ".rs",
    ".c",
    ".cc",
    ".cpp",
    ".h",
    ".hpp",
    ".sh",
    ".bash",
    ".zsh",
    ".sql",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".env",
}
OFFICE_EXTENSIONS = {".doc", ".docx", ".rtf", ".odt"}
PDF_EXTENSIONS = {".pdf"}
HTML_TAG_RE = re.compile(r"<[^>]+>")
HTML_SCRIPT_RE = re.compile(r"<(script|style)\b.*?</\1>", re.IGNORECASE | re.DOTALL)
LOGGER = logging.getLogger(__name__)

DEFAULT_LOCAL_EMBEDDING_MODEL = "BAAI/bge-m3"
DEFAULT_LOCAL_RERANK_MODEL = "BAAI/bge-reranker-v2-m3"

VECTOR_IMPORT_ERROR: Exception | None = None
LITELLM_IMPORT_ERROR: Exception | None = None
PROVIDER_EMBED_IMPORT_ERROR: Exception | None = None
LOCAL_EMBED_IMPORT_ERROR: Exception | None = None
LOCAL_RERANK_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional runtime dependency
    import lancedb
    from llama_index.core import SimpleDirectoryReader, StorageContext, VectorStoreIndex
    from llama_index.core.bridge.pydantic import Field, PrivateAttr
    from llama_index.core.embeddings import MockEmbedding
    from llama_index.core.node_parser import SentenceSplitter
    try:
        from llama_index.core.postprocessor import LLMRerank
    except Exception:  # pragma: no cover - optional runtime dependency
        LLMRerank = None
    from llama_index.core.postprocessor.types import BaseNodePostprocessor
    from llama_index.core.schema import NodeRelationship, NodeWithScore, QueryBundle, RelatedNodeInfo, TextNode
    from llama_index.core.vector_stores.types import VectorStoreQuery
    from llama_index.vector_stores.lancedb import LanceDBVectorStore
except Exception as exc:  # pragma: no cover - optional runtime dependency
    VECTOR_IMPORT_ERROR = exc
    lancedb = None
    SimpleDirectoryReader = None
    StorageContext = None
    VectorStoreIndex = None
    Field = None
    PrivateAttr = None
    MockEmbedding = None
    SentenceSplitter = None
    LLMRerank = None
    BaseNodePostprocessor = None
    NodeRelationship = None
    NodeWithScore = None
    QueryBundle = None
    RelatedNodeInfo = None
    TextNode = None
    VectorStoreQuery = None
    LanceDBVectorStore = None

try:  # pragma: no cover - optional runtime dependency
    from litellm import embedding as litellm_embedding
    from litellm import rerank as litellm_rerank
except Exception as exc:  # pragma: no cover - optional runtime dependency
    LITELLM_IMPORT_ERROR = exc
    litellm_embedding = None
    litellm_rerank = None

try:  # pragma: no cover - optional runtime dependency
    from llama_index.embeddings.litellm import LiteLLMEmbedding
except Exception as exc:  # pragma: no cover - optional runtime dependency
    PROVIDER_EMBED_IMPORT_ERROR = exc
    LiteLLMEmbedding = None

try:  # pragma: no cover - optional runtime dependency
    from llama_index.embeddings.huggingface import HuggingFaceEmbedding
except Exception as exc:  # pragma: no cover - optional runtime dependency
    LOCAL_EMBED_IMPORT_ERROR = exc
    HuggingFaceEmbedding = None

try:  # pragma: no cover - optional runtime dependency
    from llama_index.postprocessor.flag_embedding_reranker import FlagEmbeddingReranker
except Exception as exc:  # pragma: no cover - optional runtime dependency
    LOCAL_RERANK_IMPORT_ERROR = exc
    FlagEmbeddingReranker = None


def _optional_import_detail(exc: Exception | None) -> str:
    if exc is None:
        return ""
    detail = trim_text(str(exc), limit=300) or exc.__class__.__name__
    return f" ({detail})"


def _installed_package_version(name: str) -> str:
    try:
        return str(importlib_metadata.version(name)).strip()
    except Exception:
        return ""


def _local_rerank_transformers_hint() -> str:
    version = _installed_package_version("transformers")
    if not version:
        return ""
    major_text = version.split(".", 1)[0].strip()
    if major_text.isdigit() and int(major_text) >= 5:
        return (
            f" 当前检测到 `transformers=={version}`，`FlagEmbedding` 本地 Rerank 请改用 4.x，"
            "例如执行 `./.venv/bin/pip install \"transformers>=4.44.2,<5\"`。"
        )
    return ""


if LiteLLMEmbedding is not None and PrivateAttr is not None:

    class ProviderLiteLLMEmbedding(LiteLLMEmbedding):
        _extra_litellm_kwargs: dict[str, Any] = PrivateAttr(default_factory=dict)

        def __init__(self, *, extra_litellm_kwargs: dict[str, Any] | None = None, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self._extra_litellm_kwargs = dict(extra_litellm_kwargs or {})

        def _embed_many(self, texts: list[str]) -> list[list[float]]:
            if litellm_embedding is None:
                raise RuntimeError(_missing_provider_embedding_dependency_message())
            response = litellm_embedding(
                model=self.model_name,
                input=texts,
                dimensions=self.dimensions,
                timeout=self.timeout,
                api_base=self.api_base,
                api_key=self.api_key,
                **dict(self._extra_litellm_kwargs),
            )
            payload = getattr(response, "data", None) or []
            vectors: list[list[float]] = []
            for item in list(payload):
                raw_vector = item.get("embedding") if isinstance(item, dict) else getattr(item, "embedding", None)
                vectors.append([float(value or 0.0) for value in list(raw_vector or [])])
            return vectors

        def _get_query_embedding(self, query: str) -> list[float]:
            embeddings = self._embed_many([query])
            return embeddings[0] if embeddings else []

        def _get_text_embedding(self, text: str) -> list[float]:
            embeddings = self._embed_many([text])
            return embeddings[0] if embeddings else []

        def _get_text_embeddings(self, texts: list[str]) -> list[list[float]]:
            return self._embed_many(texts)

else:

    class ProviderLiteLLMEmbedding:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs
            raise RuntimeError(_missing_provider_embedding_dependency_message())


def _missing_vector_dependency_message() -> str:
    detail = _optional_import_detail(VECTOR_IMPORT_ERROR)
    return (
        "知识库向量检索依赖未安装，请安装 `llama-index-core`、"
        "`llama-index-vector-stores-lancedb`、`lancedb`、`pandas`。"
        f"{detail}"
    )


def _missing_provider_embedding_dependency_message() -> str:
    return (
        "知识库云端 Embedding 依赖未安装，请安装 `llama-index-embeddings-litellm`、`litellm`。"
        f"{_optional_import_detail(PROVIDER_EMBED_IMPORT_ERROR or LITELLM_IMPORT_ERROR)}"
    )


def _missing_local_embedding_dependency_message() -> str:
    return (
        "知识库本地 Embedding 依赖未安装，请安装 `llama-index-embeddings-huggingface` 及其 SentenceTransformer 依赖。"
        f"{_optional_import_detail(LOCAL_EMBED_IMPORT_ERROR)}"
    )


def _missing_provider_rerank_dependency_message() -> str:
    return (
        "知识库云端 Rerank 依赖未安装，请安装 `litellm` 与 `llama-index-core`。"
        f"{_optional_import_detail(LITELLM_IMPORT_ERROR or VECTOR_IMPORT_ERROR)}"
    )


def _missing_local_rerank_dependency_message() -> str:
    return (
        "知识库本地 Rerank 依赖未安装，或运行时依赖不兼容；请安装 "
        "`llama-index-postprocessor-flag-embedding-reranker`、`FlagEmbedding`，并确保 `transformers<5`。"
        f"{_local_rerank_transformers_hint()}"
        f"{_optional_import_detail(LOCAL_RERANK_IMPORT_ERROR)}"
    )


if BaseNodePostprocessor is not None and Field is not None and PrivateAttr is not None:

    class LiteLLMRerankPostprocessor(BaseNodePostprocessor):
        model_name: str = Field(..., description="The provider rerank model name.")
        resolved_model_name: str | None = Field(default=None, description="Resolved LiteLLM model name.")
        top_n: int | None = Field(default=None, description="Optional top-n cutoff.")

        _gateway: AIGateway = PrivateAttr()
        _provider: dict[str, Any] = PrivateAttr(default_factory=dict)
        _provider_type: str = PrivateAttr(default="")
        _litellm_kwargs: dict[str, Any] = PrivateAttr(default_factory=dict)

        def __init__(
            self,
            *,
            provider: dict[str, Any],
            provider_type: str,
            model_name: str,
            resolved_model_name: str | None,
            gateway: AIGateway,
            top_n: int | None = None,
            litellm_kwargs: dict[str, Any] | None = None,
        ) -> None:
            super().__init__(
                model_name=model_name,
                resolved_model_name=resolved_model_name,
                top_n=top_n,
            )
            self._provider = dict(provider or {})
            self._provider_type = str(provider_type or self._provider.get("provider_type") or "").strip()
            self._gateway = gateway
            self._litellm_kwargs = dict(litellm_kwargs or {})

        @classmethod
        def class_name(cls) -> str:
            return "LiteLLMRerankPostprocessor"

        def _postprocess_nodes(
            self,
            nodes: list[NodeWithScore],
            query_bundle: QueryBundle | None = None,
        ) -> list[NodeWithScore]:
            if not nodes:
                return nodes
            query = str((query_bundle.query_str if query_bundle is not None else "") or "").strip()
            if not query:
                return nodes
            documents = [self._node_text(item) for item in nodes]
            if not any(text.strip() for text in documents):
                return nodes
            limit = min(max(1, int(self.top_n or len(nodes))), len(nodes))
            results = self._rerank_documents(query=query, documents=documents, top_n=limit)
            ordered: list[NodeWithScore] = []
            seen_indexes: set[int] = set()
            for entry in results:
                raw_index = self._coerce_int(entry.get("index"))
                if raw_index is None or raw_index < 0 or raw_index >= len(nodes) or raw_index in seen_indexes:
                    continue
                seen_indexes.add(raw_index)
                ordered.append(
                    NodeWithScore(
                        node=nodes[raw_index].node,
                        score=float(entry.get("relevance_score") or 0.0),
                    )
                )
            ordered.extend(nodes[index] for index in range(len(nodes)) if index not in seen_indexes)
            return ordered

        def _node_text(self, node_with_score: NodeWithScore) -> str:
            try:
                return str(node_with_score.node.get_content() or "").strip()
            except Exception:
                return str(getattr(node_with_score.node, "text", "") or "").strip()

        def _rerank_documents(self, *, query: str, documents: list[str], top_n: int) -> list[dict[str, Any]]:
            try:
                if litellm_rerank is None:
                    raise RuntimeError(_missing_provider_rerank_dependency_message())
                response = litellm_rerank(
                    model=self.resolved_model_name or self.model_name,
                    query=query,
                    documents=documents,
                    top_n=top_n,
                    **dict(self._litellm_kwargs),
                )
                return self._normalize_results(response, top_n=top_n)
            except Exception as exc:
                LOGGER.warning("LiteLLM rerank failed, falling back to gateway rerank: %s", exc)
                result = self._gateway.rerank(
                    self._provider,
                    query=query,
                    documents=documents,
                    model=self.model_name,
                    top_n=top_n,
                )
                normalized = [dict(item) for item in list(result.items or [])]
                normalized.sort(
                    key=lambda item: (float(item.get("relevance_score") or 0.0), -int(item.get("index") or 0)),
                    reverse=True,
                )
                return normalized[:top_n]

        def _normalize_results(self, response: Any, *, top_n: int) -> list[dict[str, Any]]:
            results = getattr(response, "results", None)
            if results is None and isinstance(response, dict):
                results = response.get("results")
            normalized: list[dict[str, Any]] = []
            for item in list(results or []):
                raw_index = item.get("index") if isinstance(item, dict) else getattr(item, "index", None)
                index = self._coerce_int(raw_index)
                if index is None:
                    continue
                score = item.get("relevance_score") if isinstance(item, dict) else getattr(item, "relevance_score", 0.0)
                document = item.get("document") if isinstance(item, dict) else getattr(item, "document", None)
                normalized.append(
                    {
                        "index": index,
                        "relevance_score": float(score or 0.0),
                        "document": document,
                    }
                )
            normalized.sort(
                key=lambda item: (float(item.get("relevance_score") or 0.0), -int(item.get("index") or 0)),
                reverse=True,
            )
            return normalized[:top_n]

        @staticmethod
        def _coerce_int(value: Any) -> int | None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

else:
    LiteLLMRerankPostprocessor = None


class KnowledgeBaseService:
    def __init__(
        self,
        *,
        store: MetadataStore,
        root_dir: str | Path,
        gateway: AIGateway | None = None,
        retrieval_runtime: dict[str, Any] | None = None,
    ) -> None:
        self.store = store
        self.root_dir = Path(root_dir).expanduser().resolve()
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self._gateway = gateway or AIGateway()
        self._vector_root = self.root_dir / "vector-store"
        self._vector_root.mkdir(parents=True, exist_ok=True)
        self._blob_root = self.root_dir / "blobs"
        self._blob_root.mkdir(parents=True, exist_ok=True)
        self._ingest_tmp_root = self.root_dir / "ingest-tmp"
        self._ingest_tmp_root.mkdir(parents=True, exist_ok=True)
        self._embedding_runtime_settings: dict[str, Any] = {"mode": "disabled"}
        self._rerank_runtime_settings: dict[str, Any] = {"mode": "disabled"}
        self._embedding_model: Any = None
        self._embedding_signature = ""
        self._embedding_dimension: int | None = None
        self._embedding_runtime: dict[str, Any] = {
            "mode": "disabled",
            "vector_enabled": False,
            "vector_dim": None,
            "runtime_loaded": False,
        }
        self._rerank_runtime: dict[str, Any] = {"mode": "disabled", "runtime_loaded": False}
        self._rerank_postprocessor: Any = None
        self._embedding_job_lock = threading.RLock()
        self._embedding_execution_lock = threading.Lock()
        self._embedding_job_threads: dict[str, threading.Thread] = {}
        self.store.fail_active_knowledge_embedding_jobs("服务重启后，之前的知识库嵌入任务已中断。")
        self.configure_retrieval(retrieval_runtime)

    def close(self) -> None:
        with self._embedding_job_lock:
            self._embedding_job_threads.clear()
        return None

    def configure_retrieval(self, runtime_settings: dict[str, Any] | None = None) -> dict[str, Any]:
        runtime = dict(runtime_settings or {})
        self._embedding_runtime_settings = dict(runtime.get("embedding") or {})
        self._rerank_runtime_settings = dict(runtime.get("rerank") or {})
        self._unload_retrieval_runtime()
        self._embedding_runtime = self._preview_embedding_runtime(self._embedding_runtime_settings)
        self._rerank_runtime = self._preview_rerank_runtime(self._rerank_runtime_settings)

        return {
            "embedding_reindexed": False,
            "reindexed_knowledge_bases": 0,
            "reindexed_documents": 0,
            "reindexed_chunks": 0,
            "retrieval": self.retrieval_info(),
        }

    def retrieval_info(self) -> dict[str, Any]:
        return {
            "embedding": dict(self._embedding_runtime),
            "rerank": dict(self._rerank_runtime),
            "vector_dim": self._embedding_dimension,
        }

    def start_document_embedding_job(
        self,
        knowledge_base_id: str,
        *,
        action: str,
        document_ids: list[str],
    ) -> dict[str, Any]:
        knowledge_base = self._require_knowledge_base(knowledge_base_id)
        normalized_action = self._normalize_embedding_action(action)
        selected_ids = self._normalize_embedding_document_ids(document_ids)
        if normalized_action in {"add", "reembed", "delete"} and not selected_ids:
            raise ValueError("请至少选择一个文件。")
        with self._embedding_job_lock:
            existing = self.store.get_active_knowledge_embedding_job(knowledge_base_id)
            if existing is not None:
                return {
                    "message": "当前知识库已有嵌入任务在执行，已切换到现有任务进度。",
                    "reused": True,
                    "job": self._embedding_job_resource(existing),
                }
            created = self.store.save_knowledge_embedding_job(
                job_id=None,
                knowledge_base_id=knowledge_base_id,
                action=normalized_action or "save",
                status="pending",
                stage="queued",
                message=self._embedding_job_status_text(
                    status="pending",
                    stage="queued",
                    total_documents=len(selected_ids),
                    processed_documents=0,
                ),
                result={
                    "action": normalized_action or "save",
                    "document_ids": list(selected_ids),
                    "knowledge_base": self._knowledge_base_resource(knowledge_base),
                },
            )
            thread = threading.Thread(
                target=self._run_document_embedding_job,
                args=(str(created.get("id") or ""), knowledge_base_id, normalized_action or "save", list(selected_ids)),
                daemon=True,
                name=f"kb-embed-{str(created.get('id') or '')[:8]}",
            )
            self._embedding_job_threads[str(created.get("id") or "")] = thread
            thread.start()
        return {
            "message": "已开始知识库嵌入任务。",
            "reused": False,
            "job": self._embedding_job_resource(created),
        }

    def get_document_embedding_job(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_knowledge_embedding_job(job_id)
        if job is None:
            return None
        return self._embedding_job_resource(job)

    def _normalize_embedding_action(self, action: str) -> str:
        normalized_action = str(action or "").strip().lower()
        if normalized_action not in {"", "save", "sync", "add", "reembed", "delete"}:
            raise ValueError("知识库文档嵌入操作仅支持 save / add / reembed / delete。")
        return normalized_action

    def _normalize_embedding_document_ids(self, document_ids: list[str]) -> list[str]:
        return list(dict.fromkeys(str(item or "").strip() for item in list(document_ids or []) if str(item or "").strip()))

    def _save_embedding_job_record(self, job: dict[str, Any], **overrides: Any) -> dict[str, Any]:
        return self.store.save_knowledge_embedding_job(
            job_id=str(overrides.pop("job_id", job.get("id")) or "").strip() or None,
            knowledge_base_id=str(overrides.pop("knowledge_base_id", job.get("knowledge_base_id")) or "").strip(),
            action=str(overrides.pop("action", job.get("action")) or "save"),
            status=str(overrides.pop("status", job.get("status")) or "pending"),
            stage=str(overrides.pop("stage", job.get("stage")) or "queued"),
            total_documents=int(overrides.pop("total_documents", job.get("total_documents")) or 0),
            processed_documents=int(overrides.pop("processed_documents", job.get("processed_documents")) or 0),
            completed_documents=int(overrides.pop("completed_documents", job.get("completed_documents")) or 0),
            failed_documents=int(overrides.pop("failed_documents", job.get("failed_documents")) or 0),
            total_chunks_estimated=int(overrides.pop("total_chunks_estimated", job.get("total_chunks_estimated")) or 0),
            embedded_chunks_completed=int(overrides.pop("embedded_chunks_completed", job.get("embedded_chunks_completed")) or 0),
            current_document_id=str(overrides.pop("current_document_id", job.get("current_document_id")) or "").strip() or None,
            current_document_title=str(overrides.pop("current_document_title", job.get("current_document_title")) or "").strip() or None,
            message=str(overrides.pop("message", job.get("message")) or "").strip() or None,
            error_text=str(overrides.pop("error_text", job.get("error_text")) or "").strip() or None,
            result=dict(overrides.pop("result", job.get("result_json") or {})),
            started_at=str(overrides.pop("started_at", job.get("started_at")) or "").strip() or None,
            finished_at=str(overrides.pop("finished_at", job.get("finished_at")) or "").strip() or None,
        )

    def _update_embedding_job(
        self,
        job_id: str,
        *,
        status: str | None = None,
        stage: str | None = None,
        total_documents: int | None = None,
        processed_documents: int | None = None,
        completed_documents: int | None = None,
        failed_documents: int | None = None,
        total_chunks_estimated: int | None = None,
        embedded_chunks_completed: int | None = None,
        current_document_id: str | None = None,
        current_document_title: str | None = None,
        message: str | None = None,
        error_text: str | None = None,
        result: dict[str, Any] | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
        clear_current_document: bool = False,
    ) -> dict[str, Any] | None:
        current = self.store.get_knowledge_embedding_job(job_id)
        if current is None:
            return None
        return self._save_embedding_job_record(
            current,
            status=status if status is not None else current.get("status"),
            stage=stage if stage is not None else current.get("stage"),
            total_documents=total_documents if total_documents is not None else current.get("total_documents"),
            processed_documents=processed_documents if processed_documents is not None else current.get("processed_documents"),
            completed_documents=completed_documents if completed_documents is not None else current.get("completed_documents"),
            failed_documents=failed_documents if failed_documents is not None else current.get("failed_documents"),
            total_chunks_estimated=total_chunks_estimated if total_chunks_estimated is not None else current.get("total_chunks_estimated"),
            embedded_chunks_completed=embedded_chunks_completed
            if embedded_chunks_completed is not None
            else current.get("embedded_chunks_completed"),
            current_document_id=None if clear_current_document else (current_document_id if current_document_id is not None else current.get("current_document_id")),
            current_document_title=None
            if clear_current_document
            else (current_document_title if current_document_title is not None else current.get("current_document_title")),
            message=message if message is not None else current.get("message"),
            error_text=error_text if error_text is not None else current.get("error_text"),
            result=result if result is not None else dict(current.get("result_json") or {}),
            started_at=started_at if started_at is not None else current.get("started_at"),
            finished_at=finished_at if finished_at is not None else current.get("finished_at"),
        )

    def _increment_embedding_job(
        self,
        job_id: str,
        *,
        processed_documents: int = 0,
        completed_documents: int = 0,
        failed_documents: int = 0,
        total_chunks_estimated: int = 0,
        embedded_chunks_completed: int = 0,
        stage: str | None = None,
        current_document_id: str | None = None,
        current_document_title: str | None = None,
        status: str | None = None,
        message: str | None = None,
    ) -> dict[str, Any] | None:
        current = self.store.get_knowledge_embedding_job(job_id)
        if current is None:
            return None
        next_processed = int(current.get("processed_documents") or 0) + int(processed_documents or 0)
        next_completed = int(current.get("completed_documents") or 0) + int(completed_documents or 0)
        next_failed = int(current.get("failed_documents") or 0) + int(failed_documents or 0)
        next_chunks_total = int(current.get("total_chunks_estimated") or 0) + int(total_chunks_estimated or 0)
        next_chunks_completed = int(current.get("embedded_chunks_completed") or 0) + int(embedded_chunks_completed or 0)
        resolved_status = status if status is not None else str(current.get("status") or "pending")
        resolved_stage = stage if stage is not None else str(current.get("stage") or "queued")
        resolved_message = message if message is not None else self._embedding_job_status_text(
            status=resolved_status,
            stage=resolved_stage,
            total_documents=int(current.get("total_documents") or 0),
            processed_documents=next_processed,
            total_chunks_estimated=next_chunks_total,
            embedded_chunks_completed=next_chunks_completed,
            current_document_title=current_document_title
            if current_document_title is not None
            else str(current.get("current_document_title") or "").strip()
            or None,
        )
        return self._save_embedding_job_record(
            current,
            status=resolved_status,
            stage=resolved_stage,
            processed_documents=next_processed,
            completed_documents=next_completed,
            failed_documents=next_failed,
            total_chunks_estimated=next_chunks_total,
            embedded_chunks_completed=next_chunks_completed,
            current_document_id=current_document_id if current_document_id is not None else current.get("current_document_id"),
            current_document_title=current_document_title if current_document_title is not None else current.get("current_document_title"),
            message=resolved_message,
        )

    def _run_document_embedding_job(
        self,
        job_id: str,
        knowledge_base_id: str,
        action: str,
        document_ids: list[str],
    ) -> None:
        try:
            with self._embedding_execution_lock:
                started_at = utcnow_iso()
                running = self._update_embedding_job(
                    job_id,
                    status="running",
                    stage="preparing",
                    started_at=started_at,
                    message=self._embedding_job_status_text(
                        status="running",
                        stage="preparing",
                        total_documents=0,
                        processed_documents=0,
                    ),
                )
                result = self._run_document_embedding_action(
                    knowledge_base_id,
                    action=action,
                    document_ids=document_ids,
                    job_id=job_id,
                )
                completed = self.store.get_knowledge_embedding_job(job_id) or running
                self._update_embedding_job(
                    job_id,
                    status="completed",
                    stage="completed",
                    finished_at=utcnow_iso(),
                    clear_current_document=True,
                    message=str((result or {}).get("message") or "知识库嵌入任务已完成。").strip() or "知识库嵌入任务已完成。",
                    result=result or dict((completed or {}).get("result_json") or {}),
                    error_text=None,
                )
        except Exception as exc:
            LOGGER.exception("Knowledge embedding job failed for %s", knowledge_base_id)
            self._update_embedding_job(
                job_id,
                status="error",
                stage="failed",
                finished_at=utcnow_iso(),
                clear_current_document=True,
                message=trim_text(str(exc), limit=300) or "知识库嵌入任务失败。",
                error_text=trim_text(str(exc), limit=1000) or "知识库嵌入任务失败。",
            )
        finally:
            with self._embedding_job_lock:
                self._embedding_job_threads.pop(job_id, None)

    def _embedding_job_status_text(
        self,
        *,
        status: str,
        stage: str,
        total_documents: int,
        processed_documents: int,
        total_chunks_estimated: int = 0,
        embedded_chunks_completed: int = 0,
        current_document_title: str | None = None,
    ) -> str:
        total = max(0, int(total_documents or 0))
        processed = max(0, int(processed_documents or 0))
        chunk_total = max(0, int(total_chunks_estimated or 0))
        chunk_done = max(0, int(embedded_chunks_completed or 0))
        if status == "pending":
            return "任务已进入队列，等待开始。"
        if status == "completed":
            return "知识库嵌入任务已完成。"
        if status == "error":
            return "知识库嵌入任务失败。"
        stage_map = {
            "preparing": "正在准备文档",
            "deleting": "正在清理旧嵌入",
            "embedding": "正在嵌入文档",
            "finalizing": "正在整理结果",
            "queued": "任务排队中",
        }
        prefix = stage_map.get(stage, "正在处理")
        doc_text = f"{processed}/{total}" if total else str(processed)
        chunk_text = f"，chunk {chunk_done}/{chunk_total}" if chunk_total else ""
        current_text = f"：{current_document_title}" if current_document_title else ""
        return f"{prefix}{current_text}（文档 {doc_text}{chunk_text}）"

    def _embedding_job_progress_percent(self, record: dict[str, Any]) -> float:
        total_documents = max(0, int(record.get("total_documents") or 0))
        processed_documents = max(0, int(record.get("processed_documents") or 0))
        total_chunks_estimated = max(0, int(record.get("total_chunks_estimated") or 0))
        embedded_chunks_completed = max(0, int(record.get("embedded_chunks_completed") or 0))
        if total_documents <= 0 and total_chunks_estimated <= 0:
            return 100.0 if str(record.get("status") or "").strip() == "completed" else 0.0
        total_units = total_documents + total_chunks_estimated
        completed_units = processed_documents + embedded_chunks_completed
        if total_units <= 0:
            return 0.0
        return max(0.0, min(100.0, round((completed_units / total_units) * 100, 1)))

    def list_documents_page(
        self,
        *,
        knowledge_base_id: str,
        limit: int | None = None,
        offset: int = 0,
        query: str | None = None,
        embedding_status: str | None = None,
    ) -> dict[str, Any]:
        normalized_status = str(embedding_status or "").strip().lower() or "all"
        if normalized_status == "all":
            normalized_status = ""
        page = self.store.list_knowledge_documents_page(
            knowledge_base_id=knowledge_base_id,
            limit=limit,
            offset=offset,
            query=query,
            embedding_status=normalized_status or None,
            include_removed=False,
        )
        return {
            "items": [self._document_resource(item) for item in page["items"]],
            "total": page["total"],
            "offset": page["offset"],
            "limit": page["limit"],
            "filters": {
                "query": str(query or "").strip(),
                "embedding_status": normalized_status or "all",
            },
        }

    def list_pool_documents_page(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        query: str | None = None,
        exclude_knowledge_base_id: str | None = None,
    ) -> dict[str, Any]:
        knowledge_base_id = str(exclude_knowledge_base_id or "").strip()
        normalized_query = str(query or "").strip().lower()
        active_blob_ids: set[str] = set()
        if knowledge_base_id:
            for document in self.store.list_knowledge_documents(knowledge_base_id=knowledge_base_id, include_removed=False):
                blob_id = str(document.get("blob_id") or "").strip()
                if blob_id:
                    active_blob_ids.add(blob_id)
            documents = [
                self._pool_document_resource(item)
                for item in self.store.list_knowledge_pool_documents(knowledge_base_id=knowledge_base_id)
                if str(item.get("blob_id") or "").strip() not in active_blob_ids
            ]
        else:
            documents = [self._pool_document_resource(item) for item in self.store.list_knowledge_pool_documents()]
        if normalized_query:
            documents = [
                item
                for item in documents
                if normalized_query
                in "\n".join(
                    [
                        str(item.get("title") or ""),
                        str(item.get("source_path") or ""),
                        str(item.get("preview") or ""),
                    ]
                ).lower()
            ]
        total = len(documents)
        safe_offset = max(0, int(offset or 0))
        if limit is None:
            safe_limit = total or 0
            items = documents[safe_offset:]
        else:
            safe_limit = max(1, int(limit))
            items = documents[safe_offset : safe_offset + safe_limit]
        return {
            "items": items,
            "total": total,
            "offset": safe_offset,
            "limit": safe_limit,
            "filters": {
                "query": str(query or "").strip(),
                "exclude_knowledge_base_id": knowledge_base_id or None,
            },
        }

    def import_pool_uploaded_files(self, payload: dict[str, Any]) -> dict[str, Any]:
        knowledge_base_id = str(payload.get("knowledge_base_id") or "").strip()
        if not knowledge_base_id:
            knowledge_bases = self.store.list_knowledge_bases()
            if len(knowledge_bases) == 1:
                knowledge_base_id = str(knowledge_bases[0].get("id") or "").strip()
            if not knowledge_base_id:
                raise ValueError("请先保存知识库，再上传文件。")
        self._require_knowledge_base(knowledge_base_id)
        files = self._normalize_uploaded_files(payload.get("files"))
        imported: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        total_bytes = 0
        for entry in files:
            total_bytes += len(entry["content"])
            if total_bytes > MAX_UPLOAD_TOTAL_BYTES:
                raise ValueError("上传文件总大小不能超过 100 MB。")
            try:
                record = self._store_uploaded_file_to_pool(
                    knowledge_base_id=knowledge_base_id,
                    upload_path=entry["path"],
                    content=entry["content"],
                )
            except ValueError as exc:
                skipped.append(
                    {
                        "path": entry["path"],
                        "reason": "invalid-file",
                        "message": str(exc),
                    }
                )
                continue
            imported.append(self._pool_document_resource(record))
        self.store.touch_knowledge_base(knowledge_base_id)
        return {
            "message": f"知识库文档池已处理 {len(files)} 个文件。",
            "imported_count": len(imported),
            "skipped_count": len(skipped),
            "uploaded_total_bytes": total_bytes,
            "items": imported,
            "skipped": skipped,
        }

    def add_pool_documents_to_knowledge_base(
        self,
        knowledge_base_id: str,
        *,
        pool_document_ids: list[str],
    ) -> dict[str, Any]:
        knowledge_base = self._require_knowledge_base(knowledge_base_id)
        selected_ids = list(dict.fromkeys(str(item or "").strip() for item in list(pool_document_ids or []) if str(item or "").strip()))
        if not selected_ids:
            raise ValueError("请至少选择一个文档池文件。")
        items: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for pool_document_id in selected_ids:
            pool_document = self.store.get_knowledge_pool_document(pool_document_id)
            if pool_document is None:
                skipped.append({"id": pool_document_id, "reason": "missing", "message": "文档池文件不存在。"})
                continue
            if str(pool_document.get("knowledge_base_id") or "").strip() != knowledge_base_id:
                skipped.append({"id": pool_document_id, "reason": "knowledge-base-mismatch", "message": "文件不属于当前知识库文档池。"})
                continue
            blob_id = str(pool_document.get("blob_id") or "").strip()
            existing = self.store.get_knowledge_document_by_blob(knowledge_base_id=knowledge_base_id, blob_id=blob_id) if blob_id else None
            pool_metadata = dict(pool_document.get("metadata_json") or {})
            merged_metadata = {
                **pool_metadata,
                "pool_document_id": pool_document_id,
                "pool_document_key": str(pool_document.get("key") or "").strip() or None,
                "source_path": str(pool_document.get("source_path") or "").strip() or None,
            }
            if existing is not None:
                current_status = str(existing.get("document_status") or "").strip().lower()
                if current_status == "removed":
                    saved = self.store.save_knowledge_document(
                        knowledge_document_id=str(existing.get("id") or "").strip() or None,
                        knowledge_base_id=knowledge_base_id,
                        pool_document_id=pool_document_id,
                        blob_id=blob_id or None,
                        alias_id=str(pool_document.get("alias_id") or "").strip() or None,
                        key=str(existing.get("key") or pool_document.get("key") or f"blob:{blob_id}"),
                        title=str(pool_document.get("title") or existing.get("title") or pool_document_id),
                        source_path=str(pool_document.get("source_path") or existing.get("source_path") or "").strip() or None,
                        content_text=str(pool_document.get("content_text") or existing.get("content_text") or ""),
                        document_status="not_embedded",
                        sync_status="idle",
                        last_error=None,
                        embedded_at=str(existing.get("embedded_at") or "").strip() or None,
                        removed_at=None,
                        metadata=merged_metadata,
                        status=str(existing.get("status") or "active"),
                    )
                    items.append(self._document_resource(saved))
                    continue
                skipped.append(
                    {
                        "id": pool_document_id,
                        "title": str(pool_document.get("title") or pool_document.get("source_path") or pool_document_id),
                        "reason": "already-added",
                        "message": "该文档已在当前知识库中。",
                    }
                )
                continue
            saved = self.store.save_knowledge_document(
                knowledge_document_id=None,
                knowledge_base_id=knowledge_base_id,
                pool_document_id=pool_document_id,
                blob_id=blob_id or None,
                alias_id=str(pool_document.get("alias_id") or "").strip() or None,
                key=str(pool_document.get("key") or "").strip() or f"blob:{blob_id}",
                title=str(pool_document.get("title") or pool_document.get("source_path") or pool_document_id),
                source_path=str(pool_document.get("source_path") or "").strip() or None,
                content_text=str(pool_document.get("content_text") or ""),
                document_status="not_embedded",
                sync_status="idle",
                last_error=None,
                embedded_at=None,
                removed_at=None,
                metadata=merged_metadata,
                status="active",
            )
            items.append(self._document_resource(saved))
        self.store.touch_knowledge_base(knowledge_base_id)
        summary = self.store.get_knowledge_base(knowledge_base_id) or knowledge_base
        return {
            "message": f"已从文档池加入 {len(items)} 个文档。",
            "knowledge_base": self._knowledge_base_resource(summary),
            "affected_count": len(items),
            "skipped_count": len(skipped),
            "items": items,
            "skipped": skipped,
        }

    def manage_pool_documents(
        self,
        *,
        action: str,
        document_ids: list[str],
    ) -> dict[str, Any]:
        normalized_action = str(action or "").strip().lower()
        if normalized_action != "delete":
            raise ValueError("知识库文档池当前仅支持 delete 操作。")
        selected_ids = list(dict.fromkeys(str(item or "").strip() for item in list(document_ids or []) if str(item or "").strip()))
        if not selected_ids:
            raise ValueError("请至少选择一个文档池文件。")
        linked_pool_ids: dict[str, int] = {}
        for document in self.store.list_knowledge_documents(include_removed=False):
            pool_document_id = str(document.get("pool_document_id") or "").strip()
            if pool_document_id:
                linked_pool_ids[pool_document_id] = linked_pool_ids.get(pool_document_id, 0) + 1
        deleted: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for document_id in selected_ids:
            document = self.store.get_knowledge_pool_document(document_id)
            if document is None:
                skipped.append({"id": document_id, "reason": "missing", "message": "文档池文件不存在。"})
                continue
            if linked_pool_ids.get(document_id, 0) > 0:
                skipped.append(
                    {
                        "id": document_id,
                        "title": str(document.get("title") or document.get("source_path") or document_id),
                        "reason": "in-use",
                        "message": "该文档仍在当前知识库中使用，不能从文档池删除。",
                    }
                )
                continue
            blob_id = str(document.get("blob_id") or "").strip()
            removed = self.store.delete_knowledge_pool_document(document_id)
            if removed is not None:
                deleted.append(self._pool_document_resource(removed))
                if blob_id:
                    self._cleanup_blob_if_orphan(blob_id)
        return {
            "message": f"文档池已删除 {len(deleted)} 个文档。",
            "affected_count": len(deleted),
            "skipped_count": len(skipped),
            "items": deleted,
            "skipped": skipped,
        }

    def import_uploaded_files(self, knowledge_base_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        delegated_payload = dict(payload or {})
        delegated_payload["knowledge_base_id"] = knowledge_base_id
        result = self.import_pool_uploaded_files(delegated_payload)
        result["knowledge_base"] = self._knowledge_base_resource(self._require_knowledge_base(knowledge_base_id))
        return result

    def manage_document_embeddings(
        self,
        knowledge_base_id: str,
        *,
        action: str,
        document_ids: list[str],
    ) -> dict[str, Any]:
        return self._run_document_embedding_action(
            knowledge_base_id,
            action=action,
            document_ids=document_ids,
            job_id=None,
        )

    def _run_document_embedding_action(
        self,
        knowledge_base_id: str,
        *,
        action: str,
        document_ids: list[str],
        job_id: str | None,
    ) -> dict[str, Any]:
        knowledge_base = self._require_knowledge_base(knowledge_base_id)
        normalized_action = self._normalize_embedding_action(action)
        selected_ids = self._normalize_embedding_document_ids(document_ids)
        if normalized_action in {"add", "reembed", "delete"} and not selected_ids:
            raise ValueError("请至少选择一个文件。")
        if normalized_action == "delete":
            if job_id:
                self._update_embedding_job(
                    job_id,
                    status="running",
                    stage="deleting",
                    total_documents=len(selected_ids),
                    processed_documents=0,
                    completed_documents=0,
                    failed_documents=0,
                    total_chunks_estimated=0,
                    embedded_chunks_completed=0,
                    message=self._embedding_job_status_text(
                        status="running",
                        stage="deleting",
                        total_documents=len(selected_ids),
                        processed_documents=0,
                    ),
                )
            deleted = self._delete_document_embeddings(
                knowledge_base_id=knowledge_base_id,
                document_ids=selected_ids,
                job_id=job_id,
            )
            self.store.touch_knowledge_base(knowledge_base_id)
            summary = self.store.get_knowledge_base(knowledge_base_id) or knowledge_base
            result = {
                "message": f"删除嵌入已处理 {len(deleted['items'])} 个文件。",
                "action": "delete",
                "knowledge_base": self._knowledge_base_resource(summary),
                "affected_count": len(deleted["items"]),
                "skipped_count": len(deleted["skipped"]),
                "items": deleted["items"],
                "skipped": deleted["skipped"],
                "retrieval": self.retrieval_info(),
            }
            if job_id:
                self._update_embedding_job(job_id, stage="finalizing", result=result)
            return result
        action_mode = normalized_action or "save"
        reembed_rows: list[dict[str, Any]] = []
        items: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        affected_count = 0
        if action_mode in {"add", "reembed"}:
            for document_id in selected_ids:
                document = self.store.get_knowledge_document(document_id)
                if document is None:
                    skipped.append({"id": document_id, "reason": "missing", "message": "文件不存在。"})
                    continue
                if str(document.get("knowledge_base_id") or "").strip() != knowledge_base_id:
                    skipped.append({"id": document_id, "reason": "knowledge-base-mismatch", "message": "文件不属于当前知识库。"})
                    continue
                current_status = self._document_embedding_status(document)
                if action_mode == "add" and current_status == "embedded":
                    skipped.append(
                        {
                            "id": document_id,
                            "title": str(document.get("title") or document.get("source_path") or document_id),
                            "reason": "skipped",
                            "message": "当前文件已经是已嵌入状态。",
                        }
                    )
                    continue
                if action_mode == "reembed" and current_status == "embedded":
                    reembed_rows.append(document)
        result = self._save_and_embed_documents(
            knowledge_base=knowledge_base,
            selected_document_ids=selected_ids if action_mode in {"add", "reembed"} else None,
            reembed_rows=reembed_rows,
            job_id=job_id,
        )
        items.extend(result["items"])
        skipped.extend(result["skipped"])
        affected_count = int(result.get("affected_count") or len(items))
        self.store.touch_knowledge_base(knowledge_base_id)
        summary = self.store.get_knowledge_base(knowledge_base_id) or knowledge_base
        action_label = {"save": "保存并嵌入", "sync": "保存并嵌入", "add": "保存并嵌入", "reembed": "保存并嵌入"}[action_mode]
        payload = {
            "message": f"{action_label}已处理 {affected_count} 个文件。",
            "action": action_mode,
            "knowledge_base": self._knowledge_base_resource(summary),
            "affected_count": affected_count,
            "skipped_count": len(skipped),
            "items": items,
            "skipped": skipped,
            "retrieval": self.retrieval_info(),
        }
        if job_id:
            self._update_embedding_job(job_id, stage="finalizing", result=payload)
        return payload

    def sync_document(
        self,
        document: dict[str, Any],
        *,
        previous_document: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        del previous_document
        knowledge_base_id = str(document.get("knowledge_base_id") or "").strip()
        self._require_knowledge_base(knowledge_base_id)
        metadata = dict(document.get("metadata_json") or document.get("metadata") or {})
        source_path = str(document.get("source_path") or "").strip() or None
        document_id = str(document.get("id") or "").strip()
        title = str(document.get("title") or source_path or document_id or "文档").strip()
        text = str(document.get("content_text") or "")
        key = str(document.get("key") or "").strip() or self._document_key(source_path or title)
        saved = self.store.save_knowledge_document(
            knowledge_document_id=document_id or None,
            knowledge_base_id=knowledge_base_id,
            pool_document_id=str(document.get("pool_document_id") or "").strip() or None,
            blob_id=str(document.get("blob_id") or "").strip() or None,
            alias_id=str(document.get("alias_id") or "").strip() or None,
            key=key,
            title=title,
            source_path=source_path,
            content_text=text,
            document_status="not_embedded",
            sync_status="idle",
            last_error=None,
            embedded_at=None,
            removed_at=None,
            metadata={
                **metadata,
                "file_size": int(metadata.get("file_size") or len(text.encode("utf-8"))),
                "source_path": source_path,
                "preview": trim_text(text, limit=200),
                **self._embedding_metadata_fields(status="not_embedded", embedded_chunk_count=0),
            },
            status=str(document.get("status") or "active"),
        )
        self.store.touch_knowledge_base(knowledge_base_id)
        return saved

    def delete_document(self, knowledge_document_id: str) -> dict[str, Any] | None:
        existing = self.store.get_knowledge_document(knowledge_document_id)
        if existing is None:
            return None
        knowledge_base_id = str(existing.get("knowledge_base_id") or "").strip()
        if not knowledge_base_id:
            return None
        current_status = self._document_embedding_status(existing)
        if current_status == "embedded":
            saved = self.store.save_knowledge_document(
                knowledge_document_id=str(existing.get("id") or "").strip() or None,
                knowledge_base_id=knowledge_base_id,
                pool_document_id=str(existing.get("pool_document_id") or "").strip() or None,
                blob_id=str(existing.get("blob_id") or "").strip() or None,
                alias_id=str(existing.get("alias_id") or "").strip() or None,
                key=str(existing.get("key") or "").strip() or self._document_key(str(existing.get("source_path") or existing.get("title") or existing.get("id") or "document")),
                title=str(existing.get("title") or existing.get("source_path") or existing.get("id") or "文档"),
                source_path=str(existing.get("source_path") or "").strip() or None,
                content_text=str(existing.get("content_text") or ""),
                document_status="removed",
                sync_status="idle",
                last_error=None,
                embedded_at=str(existing.get("embedded_at") or "").strip() or None,
                removed_at=utcnow_iso(),
                metadata=dict(existing.get("metadata_json") or {}),
                status=str(existing.get("status") or "active"),
            )
            self.store.touch_knowledge_base(knowledge_base_id)
            return saved
        deleted = self.store.delete_knowledge_document(knowledge_document_id)
        blob_id = str((deleted or {}).get("blob_id") or "").strip()
        if blob_id:
            self._cleanup_blob_if_orphan(blob_id)
        if knowledge_base_id:
            self.store.touch_knowledge_base(knowledge_base_id)
        return deleted

    def delete_knowledge_base(self, knowledge_base_id: str) -> dict[str, Any] | None:
        existing = self.store.get_knowledge_base(knowledge_base_id)
        if existing is None:
            return None
        related_blob_ids = {
            str(item.get("blob_id") or "").strip()
            for item in self.store.list_knowledge_documents(knowledge_base_id=knowledge_base_id, include_removed=True)
            + self.store.list_knowledge_pool_documents(knowledge_base_id=knowledge_base_id)
            if str(item.get("blob_id") or "").strip()
        }
        self._drop_vector_table(knowledge_base_id)
        deleted = self.store.delete_knowledge_base(knowledge_base_id)
        for blob_id in related_blob_ids:
            self._cleanup_blob_if_orphan(blob_id)
        return deleted

    def search(
        self,
        *,
        query: str,
        knowledge_base_ids: list[str] | None = None,
        knowledge_base_keys: list[str] | None = None,
        limit: int = 8,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, int(limit or 8))
        kb_ids = {str(item).strip() for item in list(knowledge_base_ids or []) if str(item).strip()}
        for key in list(knowledge_base_keys or []):
            record = self.store.get_knowledge_base_by_key(str(key))
            if record is not None:
                kb_ids.add(str(record.get("id") or ""))

        items: list[dict[str, Any]]
        if self._embedding_model is not None and self._embedding_dimension:
            try:
                items = self._search_vector(query=str(query or "").strip(), knowledge_base_ids=sorted(kb_ids), limit=safe_limit)
            except Exception as exc:
                LOGGER.warning("Knowledge base vector search failed, falling back to lexical search: %s", exc)
                items = []
        else:
            items = []

        if not items:
            items = self._search_lexical(
                query=str(query or "").strip(),
                knowledge_base_ids=sorted(kb_ids) or None,
                knowledge_base_keys=knowledge_base_keys,
                limit=max(safe_limit * VECTOR_HEAD_MULTIPLIER, safe_limit),
            )
        return self._maybe_rerank_results(query=str(query or "").strip(), items=items, limit=safe_limit)

    def _unload_retrieval_runtime(self) -> None:
        self._embedding_model = None
        self._embedding_signature = ""
        self._embedding_dimension = None
        self._rerank_postprocessor = None

    def _ensure_retrieval_loaded_for_embedding(self) -> None:
        self._ensure_embedding_loaded_for_embedding()
        try:
            self._ensure_rerank_loaded_for_embedding()
        except Exception as exc:
            LOGGER.warning("Knowledge base rerank runtime load skipped during embedding: %s", exc)

    def _ensure_embedding_loaded_for_embedding(self) -> None:
        if self._embedding_model is not None and self._embedding_dimension is not None:
            return
        embedding_state = self._build_embedding_state(dict(self._embedding_runtime_settings or {}))
        self._embedding_model = embedding_state["model"]
        self._embedding_signature = str(embedding_state["signature"])
        self._embedding_dimension = embedding_state["vector_dim"]
        self._embedding_runtime = dict(embedding_state["public"])

    def _ensure_rerank_loaded_for_embedding(self) -> None:
        if self._rerank_postprocessor is not None:
            return
        rerank_state = self._build_rerank_state(dict(self._rerank_runtime_settings or {}))
        self._rerank_runtime = dict(rerank_state["public"])
        self._rerank_postprocessor = rerank_state["postprocessor"]

    def _preview_embedding_runtime(self, runtime: dict[str, Any]) -> dict[str, Any]:
        mode = str(runtime.get("mode") or "").strip().lower()
        if mode in {"", "disabled"}:
            return {
                "mode": "disabled",
                "vector_enabled": False,
                "vector_dim": None,
                "runtime_loaded": False,
            }
        if mode == "local":
            runtime_model_name = str(runtime.get("model") or runtime.get("model_name") or DEFAULT_LOCAL_EMBEDDING_MODEL).strip()
            public_model_name = str(runtime.get("model_path") or runtime.get("model_name") or runtime_model_name).strip() or runtime_model_name
            local_model_id = str(runtime.get("local_model_id") or "").strip()
            model_label = str(runtime.get("model_label") or public_model_name or local_model_id).strip() or public_model_name
            model_path = str(runtime.get("model_path") or public_model_name or runtime_model_name).strip() or public_model_name
            return {
                "mode": "local",
                "backend": "huggingface",
                "local_model_id": local_model_id,
                "model_name": public_model_name,
                "resolved_model_name": runtime_model_name,
                "model_path": model_path,
                "model_label": model_label,
                "vector_enabled": True,
                "vector_dim": None,
                "runtime_loaded": False,
            }
        if mode != "provider":
            return {
                "mode": "disabled",
                "vector_enabled": False,
                "vector_dim": None,
                "runtime_loaded": False,
            }
        provider = dict(runtime.get("provider") or {})
        provider_id = str(runtime.get("provider_id") or provider.get("id") or "").strip()
        provider_name = str(runtime.get("provider_name") or provider.get("name") or provider_id).strip()
        provider_type = str(runtime.get("provider_type") or provider.get("provider_type") or "").strip()
        model_name = str(runtime.get("model") or runtime.get("model_name") or "").strip()
        resolved_model_name = self._resolve_litellm_model(provider, model_name) if provider and model_name else model_name
        return {
            "mode": "provider",
            "backend": "litellm",
            "provider_id": provider_id,
            "provider_name": provider_name,
            "provider_type": provider_type,
            "model_name": model_name,
            "resolved_model_name": resolved_model_name,
            "vector_enabled": True,
            "vector_dim": None,
            "runtime_loaded": False,
        }

    def _preview_rerank_runtime(self, runtime: dict[str, Any]) -> dict[str, Any]:
        mode = str(runtime.get("mode") or "").strip().lower()
        if mode in {"", "disabled"}:
            return {"mode": "disabled", "strategy": "disabled", "runtime_loaded": False}
        if mode == "local":
            runtime_model_name = str(runtime.get("model") or runtime.get("model_name") or DEFAULT_LOCAL_RERANK_MODEL).strip()
            public_model_name = str(runtime.get("model_path") or runtime.get("model_name") or runtime_model_name).strip() or runtime_model_name
            local_model_id = str(runtime.get("local_model_id") or "").strip()
            model_label = str(runtime.get("model_label") or public_model_name or local_model_id).strip() or public_model_name
            model_path = str(runtime.get("model_path") or public_model_name or runtime_model_name).strip() or public_model_name
            return {
                "mode": "local",
                "backend": "flag_embedding",
                "local_model_id": local_model_id,
                "model_name": public_model_name,
                "resolved_model_name": runtime_model_name,
                "model_path": model_path,
                "model_label": model_label,
                "strategy": "flag_embedding_reranker",
                "runtime_loaded": False,
            }
        if mode != "provider":
            return {"mode": "disabled", "strategy": "disabled", "runtime_loaded": False}
        provider = dict(runtime.get("provider") or {})
        provider_id = str(runtime.get("provider_id") or provider.get("id") or "").strip()
        provider_name = str(runtime.get("provider_name") or provider.get("name") or provider_id).strip()
        provider_type = str(runtime.get("provider_type") or provider.get("provider_type") or "").strip()
        model_name = str(runtime.get("model") or runtime.get("model_name") or "").strip()
        llm_rerank_available = bool(LLMRerank is not None)
        selected_model_type = str(runtime.get("model_type") or "rerank").strip().lower() or "rerank"
        llm_rerank_compatible = llm_rerank_available and selected_model_type == "chat"
        llm_rerank_supports_selected_model = selected_model_type == "chat"
        resolved_model_name = self._resolve_litellm_model(provider, model_name) if provider and model_name else model_name
        strategy_reason = (
            "当前知识库 Rerank 使用 LiteLLM BaseNodePostprocessor 包装真实 rerank 模型；LLMRerank 仅保留兼容性探测。"
            if selected_model_type == "rerank"
            else (
                "检测到聊天 LLM rerank 场景，但知识库资源中心当前仍优先使用 LiteLLM rerank 路径，以支持真实 rerank 模型。"
                if llm_rerank_available
                else "当前环境未提供 LLMRerank；若后续接入聊天 LLM rerank，仍需单独实现。"
            )
        )
        return {
            "mode": "provider",
            "provider_id": provider_id,
            "provider_name": provider_name,
            "provider_type": provider_type,
            "model_name": model_name,
            "resolved_model_name": resolved_model_name,
            "selected_model_type": selected_model_type,
            "llm_rerank_available": llm_rerank_available,
            "llm_rerank_compatible": llm_rerank_compatible,
            "llm_rerank_supports_selected_model": llm_rerank_supports_selected_model,
            "strategy": "litellm_base_node_postprocessor",
            "strategy_reason": strategy_reason,
            "runtime_loaded": False,
        }

    def _build_embedding_state(self, runtime: dict[str, Any]) -> dict[str, Any]:
        mode = str(runtime.get("mode") or "").strip().lower()
        if mode in {"", "disabled"}:
            return {
                "enabled": False,
                "model": None,
                "signature": "",
                "vector_dim": None,
                "public": {"mode": "disabled", "vector_enabled": False, "vector_dim": None, "runtime_loaded": False},
            }

        if mode == "local":
            self._require_vector_dependencies()
            self._require_local_embedding_dependencies()
            runtime_model_name = str(runtime.get("model") or runtime.get("model_name") or DEFAULT_LOCAL_EMBEDDING_MODEL).strip()
            if not runtime_model_name:
                raise ValueError("知识库本地 Embedding 模型不能为空。")
            public_model_name = str(runtime.get("model_path") or runtime.get("model_name") or runtime_model_name).strip() or runtime_model_name
            local_model_id = str(runtime.get("local_model_id") or "").strip()
            model_label = str(runtime.get("model_label") or public_model_name or local_model_id).strip() or public_model_name
            model_path = str(runtime.get("model_path") or public_model_name or runtime_model_name).strip() or public_model_name
            cache_folder = self.root_dir / "huggingface-cache"
            cache_folder.mkdir(parents=True, exist_ok=True)
            assert HuggingFaceEmbedding is not None
            model = HuggingFaceEmbedding(
                model_name=runtime_model_name,
                cache_folder=str(cache_folder),
                embed_batch_size=DEFAULT_EMBED_BATCH_SIZE,
                show_progress_bar=False,
            )
            probe_vector = [float(value or 0.0) for value in list(model.get_query_embedding("knowledge base embedding probe") or [])]
            if not probe_vector:
                raise ProviderRequestError(f"本地 Embedding 模型 `{runtime_model_name}` 返回了空向量。")
            vector_dim = len(probe_vector)
            signature_payload = {
                "mode": "local",
                "backend": "huggingface",
                "model_name": runtime_model_name,
                "cache_folder": str(cache_folder),
                "vector_dim": vector_dim,
            }
            signature = hashlib.sha1(json.dumps(signature_payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            return {
                "enabled": True,
                "model": model,
                "signature": signature,
                "vector_dim": vector_dim,
                "public": {
                    "mode": "local",
                    "backend": "huggingface",
                    "local_model_id": local_model_id,
                    "model_name": public_model_name,
                    "resolved_model_name": runtime_model_name,
                    "model_path": model_path,
                    "model_label": model_label,
                    "vector_enabled": True,
                    "vector_dim": vector_dim,
                    "runtime_loaded": True,
                },
            }

        if mode != "provider":
            return {
                "enabled": False,
                "model": None,
                "signature": "",
                "vector_dim": None,
                "public": {"mode": "disabled", "vector_enabled": False, "vector_dim": None, "runtime_loaded": False},
            }

        self._require_vector_dependencies()
        provider = dict(runtime.get("provider") or {})
        provider_id = str(runtime.get("provider_id") or provider.get("id") or "").strip()
        provider_name = str(runtime.get("provider_name") or provider.get("name") or provider_id).strip()
        provider_type = str(runtime.get("provider_type") or provider.get("provider_type") or "").strip()
        model_name = str(runtime.get("model") or runtime.get("model_name") or "").strip()
        if not provider_type or not model_name or not provider:
            raise ValueError("知识库 Embedding 配置不完整。")

        if provider_type == "mock":
            assert MockEmbedding is not None
            model = MockEmbedding(
                embed_dim=DEFAULT_MOCK_EMBED_DIM,
                model_name=f"mock:{provider_id or provider_name}:{model_name}:{DEFAULT_MOCK_EMBED_DIM}",
                embed_batch_size=DEFAULT_EMBED_BATCH_SIZE,
            )
            vector_dim = DEFAULT_MOCK_EMBED_DIM
            resolved_model = model_name
        else:
            self._require_provider_embedding_dependencies()
            resolved_model = self._resolve_litellm_model(provider, model_name)
            embedding_dimensions = self._coerce_int(dict(provider.get("extra_config") or {}).get("dimensions"))
            model = ProviderLiteLLMEmbedding(
                model_name=resolved_model,
                api_base=self._resolve_litellm_api_base(provider),
                api_key=self._resolve_api_key(provider) or None,
                dimensions=embedding_dimensions,
                timeout=60,
                embed_batch_size=DEFAULT_EMBED_BATCH_SIZE,
                extra_litellm_kwargs=self._litellm_embedding_kwargs(provider),
            )
            probe_vector = [float(value or 0.0) for value in list(model.get_query_embedding("knowledge base embedding probe") or [])]
            if not probe_vector:
                raise ProviderRequestError(f"Embedding model `{model_name}` 返回了空向量。")
            vector_dim = len(probe_vector)

        signature_payload = {
            "mode": "provider",
            "provider_id": provider_id,
            "provider_type": provider_type,
            "model_name": model_name,
            "resolved_model": resolved_model,
            "base_url": self._resolve_litellm_api_base(provider),
            "api_version": str(provider.get("api_version") or "").strip(),
            "organization": str(provider.get("organization") or "").strip(),
            "skip_tls_verify": bool(provider.get("skip_tls_verify")),
            "custom_llm_provider": self._resolve_provider_alias(provider),
            "extra_config": self._sanitize_extra_config(provider.get("extra_config")),
            "vector_dim": vector_dim,
        }
        signature = hashlib.sha1(json.dumps(signature_payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        return {
            "enabled": True,
            "model": model,
            "signature": signature,
            "vector_dim": vector_dim,
            "public": {
                "mode": "provider",
                "backend": "litellm",
                "provider_id": provider_id,
                "provider_name": provider_name,
                "provider_type": provider_type,
                "model_name": model_name,
                "resolved_model_name": resolved_model,
                "vector_enabled": True,
                "vector_dim": vector_dim,
                "runtime_loaded": True,
            },
        }

    def _build_rerank_state(self, runtime: dict[str, Any]) -> dict[str, Any]:
        mode = str(runtime.get("mode") or "").strip().lower()
        if mode in {"", "disabled"}:
            return {
                "enabled": False,
                "postprocessor": None,
                "public": {"mode": "disabled", "strategy": "disabled", "runtime_loaded": False},
            }
        if mode == "local":
            self._require_postprocessor_base_dependencies()
            self._require_local_rerank_dependencies()
            runtime_model_name = str(runtime.get("model") or runtime.get("model_name") or DEFAULT_LOCAL_RERANK_MODEL).strip()
            if not runtime_model_name:
                raise ValueError("知识库本地 Rerank 模型不能为空。")
            public_model_name = str(runtime.get("model_path") or runtime.get("model_name") or runtime_model_name).strip() or runtime_model_name
            local_model_id = str(runtime.get("local_model_id") or "").strip()
            model_label = str(runtime.get("model_label") or public_model_name or local_model_id).strip() or public_model_name
            model_path = str(runtime.get("model_path") or public_model_name or runtime_model_name).strip() or public_model_name
            assert FlagEmbeddingReranker is not None
            try:
                postprocessor = FlagEmbeddingReranker(
                    model=runtime_model_name,
                    top_n=8,
                    use_fp16=False,
                )
            except Exception as exc:
                message = trim_text(str(exc), limit=300) or exc.__class__.__name__
                raise RuntimeError(f"{_missing_local_rerank_dependency_message()} ({message})") from exc
            return {
                "enabled": True,
                "postprocessor": postprocessor,
                "public": {
                    "mode": "local",
                    "backend": "flag_embedding",
                    "local_model_id": local_model_id,
                    "model_name": public_model_name,
                    "resolved_model_name": runtime_model_name,
                    "model_path": model_path,
                    "model_label": model_label,
                    "strategy": "flag_embedding_reranker",
                    "runtime_loaded": True,
                },
            }
        if mode != "provider":
            return {
                "enabled": False,
                "postprocessor": None,
                "public": {"mode": "disabled", "strategy": "disabled", "runtime_loaded": False},
            }

        provider = dict(runtime.get("provider") or {})
        provider_id = str(runtime.get("provider_id") or provider.get("id") or "").strip()
        provider_name = str(runtime.get("provider_name") or provider.get("name") or provider_id).strip()
        provider_type = str(runtime.get("provider_type") or provider.get("provider_type") or "").strip()
        model_name = str(runtime.get("model") or runtime.get("model_name") or "").strip()
        if not provider_type or not model_name or not provider:
            raise ValueError("知识库 Rerank 配置不完整。")
        self._require_postprocessor_base_dependencies()
        self._require_provider_rerank_postprocessor_dependencies()
        if provider_type != "mock":
            self._require_provider_rerank_dependencies()
        llm_rerank_available = bool(LLMRerank is not None)
        selected_model_type = str(runtime.get("model_type") or "rerank").strip().lower() or "rerank"
        llm_rerank_compatible = llm_rerank_available and selected_model_type == "chat"
        llm_rerank_supports_selected_model = selected_model_type == "chat"
        resolved_model = self._resolve_litellm_model(provider, model_name) if provider_type != "mock" else model_name
        strategy = "litellm_base_node_postprocessor"
        strategy_reason = (
            "当前知识库 Rerank 使用 LiteLLM BaseNodePostprocessor 包装真实 rerank 模型；LLMRerank 仅保留兼容性探测。"
            if selected_model_type == "rerank"
            else (
                "检测到聊天 LLM rerank 场景，但知识库资源中心当前仍优先使用 LiteLLM rerank 路径，以支持真实 rerank 模型。"
                if llm_rerank_available
                else "当前环境未提供 LLMRerank；若后续接入聊天 LLM rerank，仍需单独实现。"
            )
        )
        assert LiteLLMRerankPostprocessor is not None
        return {
            "enabled": True,
            "postprocessor": LiteLLMRerankPostprocessor(
                provider=provider,
                provider_type=provider_type,
                model_name=model_name,
                resolved_model_name=resolved_model,
                gateway=self._gateway,
                litellm_kwargs=self._litellm_rerank_kwargs(provider),
            ),
            "public": {
                "mode": "provider",
                "provider_id": provider_id,
                "provider_name": provider_name,
                "provider_type": provider_type,
                "model_name": model_name,
                "resolved_model_name": resolved_model,
                "selected_model_type": selected_model_type,
                "llm_rerank_available": llm_rerank_available,
                "llm_rerank_compatible": llm_rerank_compatible,
                "llm_rerank_supports_selected_model": llm_rerank_supports_selected_model,
                "strategy": strategy,
                "strategy_reason": strategy_reason,
                "runtime_loaded": True,
            },
        }

    def _reindex_all_knowledge_bases(self) -> tuple[int, int, int]:
        knowledge_bases = [
            item
            for item in self.store.list_knowledge_bases()
            if str(item.get("status") or "active").strip().lower() == "active"
        ]
        documents_indexed = 0
        chunks_indexed = 0
        for knowledge_base in knowledge_bases:
            knowledge_base_id = str(knowledge_base.get("id") or "").strip()
            if not knowledge_base_id:
                continue
            self._drop_vector_table(knowledge_base_id)
            active_documents = self.store.list_knowledge_documents(knowledge_base_id=knowledge_base_id, include_removed=False)
            selected_ids: list[str] = []
            for document in active_documents:
                if str(document.get("status") or "active").strip().lower() != "active":
                    continue
                selected_ids.append(str(document.get("id") or ""))
                refreshed = self.store.save_knowledge_document(
                    knowledge_document_id=str(document.get("id") or "").strip() or None,
                    knowledge_base_id=knowledge_base_id,
                    pool_document_id=str(document.get("pool_document_id") or "").strip() or None,
                    blob_id=str(document.get("blob_id") or "").strip() or None,
                    alias_id=str(document.get("alias_id") or "").strip() or None,
                    key=str(document.get("key") or "").strip() or self._document_key(str(document.get("source_path") or document.get("title") or document.get("id") or "document")),
                    title=str(document.get("title") or document.get("source_path") or document.get("id") or "文档"),
                    source_path=str(document.get("source_path") or "").strip() or None,
                    content_text=str(document.get("content_text") or ""),
                    document_status="not_embedded",
                    sync_status="idle",
                    last_error=None,
                    embedded_at=None,
                    removed_at=None,
                    metadata=dict(document.get("metadata_json") or {}),
                    status=str(document.get("status") or "active"),
                )
                document.update(refreshed)
            if not selected_ids:
                continue
            result = self._save_and_embed_documents(
                knowledge_base=knowledge_base,
                selected_document_ids=selected_ids,
                reembed_rows=[],
            )
            documents_indexed += len(result["items"])
            chunks_indexed += sum(int((item.get("metadata") or {}).get("embedded_chunk_count") or item.get("embedded_chunk_count") or 0) for item in result["items"])
        return len(knowledge_bases), documents_indexed, chunks_indexed

    def _search_vector(self, *, query: str, knowledge_base_ids: list[str], limit: int) -> list[dict[str, Any]]:
        if self._embedding_model is None or self._embedding_dimension is None:
            return []
        if not query:
            return []
        kb_records = self._candidate_knowledge_bases(knowledge_base_ids)
        if not kb_records:
            return []
        head_size = max(limit * VECTOR_HEAD_MULTIPLIER, limit)
        items: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for knowledge_base in kb_records:
            knowledge_base_id = str(knowledge_base.get("id") or "").strip()
            if not knowledge_base_id or not self._vector_table_exists(knowledge_base_id):
                continue
            try:
                retriever = self._index_for_kb(knowledge_base_id).as_retriever(similarity_top_k=head_size)
                nodes = retriever.retrieve(query)
            except Warning:
                continue
            except Exception as exc:
                LOGGER.warning("Knowledge base vector query failed for %s: %s", knowledge_base_id, exc)
                continue
            for node_with_score in list(nodes or []):
                node = getattr(node_with_score, "node", None)
                metadata = dict(getattr(node, "metadata", {}) or {})
                document_id = str(getattr(node, "ref_doc_id", None) or metadata.get("knowledge_document_id") or "").strip()
                if not document_id:
                    continue
                node_id = str(getattr(node, "node_id", None) or getattr(node, "id_", None) or document_id).strip() or document_id
                identity = (document_id, node_id)
                if identity in seen:
                    continue
                seen.add(identity)
                text = str(node.get_content() if hasattr(node, "get_content") else getattr(node, "text", "") or "").strip()
                score = float(getattr(node_with_score, "score", 0.0) or 0.0)
                items.append(
                    {
                        "id": document_id,
                        "knowledge_base_id": knowledge_base_id,
                        "knowledge_base_key": metadata.get("knowledge_base_key") or knowledge_base.get("key"),
                        "knowledge_base_name": metadata.get("knowledge_base_name") or knowledge_base.get("name"),
                        "key": metadata.get("document_key") or metadata.get("key"),
                        "title": metadata.get("title"),
                        "source_path": metadata.get("source_path"),
                        "metadata_json": {
                            "chunk_count": self._coerce_int(metadata.get("chunk_count"), minimum=0) or 0,
                            "file_size": self._coerce_int(metadata.get("file_size"), minimum=0) or 0,
                        },
                        "score": score,
                        "chunk_index": self._coerce_int(metadata.get("chunk_index"), minimum=0) or 0,
                        "chunk_count": self._coerce_int(metadata.get("chunk_count"), minimum=0) or 0,
                        "snippet": trim_text(text, limit=320),
                        "content_text": text,
                    }
                )
        items.sort(key=lambda item: (float(item.get("score") or 0.0), str(item.get("title") or "")), reverse=True)
        return items[: max(limit * VECTOR_HEAD_MULTIPLIER, limit)]

    def _search_lexical(
        self,
        *,
        query: str,
        knowledge_base_ids: list[str] | None,
        knowledge_base_keys: list[str] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        return self.store.search_knowledge_documents(
            query=query,
            knowledge_base_ids=knowledge_base_ids,
            knowledge_base_keys=knowledge_base_keys,
            limit=limit,
        )

    def _maybe_rerank_results(self, *, query: str, items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        head = [dict(item) for item in items[: max(limit * VECTOR_HEAD_MULTIPLIER, limit)]]
        if (
            not head
            or str(self._rerank_runtime.get("mode") or "").strip().lower() not in {"provider", "local"}
            or not query
            or self._rerank_postprocessor is None
            or NodeWithScore is None
            or TextNode is None
        ):
            return head[:limit]
        documents = [self._document_text_for_rerank(item) for item in head]
        if not any(text.strip() for text in documents):
            return head[:limit]
        try:
            nodes: list[NodeWithScore] = []
            for index, item in enumerate(head):
                score_value = item.get("rerank_score")
                if score_value is None:
                    score_value = item.get("score")
                try:
                    base_score = float(score_value) if score_value is not None else None
                except (TypeError, ValueError):
                    base_score = None
                nodes.append(
                    NodeWithScore(
                        node=TextNode(
                            id_=f"kb-rerank:{item.get('id') or index}:{index}",
                            text=documents[index],
                            metadata={
                                "result_index": index,
                                "document_id": item.get("id"),
                                "knowledge_base_id": item.get("knowledge_base_id"),
                                "source_path": item.get("source_path"),
                                "title": item.get("title"),
                            },
                        ),
                        score=base_score,
                    )
                )
            if hasattr(self._rerank_postprocessor, "top_n"):
                try:
                    self._rerank_postprocessor.top_n = len(nodes)
                except Exception:
                    pass
            reranked = self._rerank_postprocessor.postprocess_nodes(nodes, query_str=query)
        except Exception as exc:
            LOGGER.warning("Knowledge base rerank failed, keeping base order: %s", exc)
            return head[:limit]
        ordered: list[dict[str, Any]] = []
        seen_indexes: set[int] = set()
        for entry in reranked:
            metadata = dict(getattr(entry, "metadata", {}) or {})
            raw_index = self._coerce_int(metadata.get("result_index"))
            if raw_index is None or raw_index < 0 or raw_index >= len(head) or raw_index in seen_indexes:
                continue
            seen_indexes.add(raw_index)
            payload = dict(head[raw_index])
            if getattr(entry, "score", None) is not None:
                payload["rerank_score"] = float(entry.score or 0.0)
            ordered.append(payload)
        ordered.extend(dict(head[index]) for index in range(len(head)) if index not in seen_indexes)
        return ordered[:limit]

    def _index_document_chunks(
        self,
        *,
        knowledge_base: dict[str, Any],
        document: dict[str, Any],
        chunks: list[str],
        title: str,
        source_path: str | None,
        file_size: int,
    ) -> int:
        if self._embedding_model is None or self._embedding_dimension is None or not chunks:
            return 0
        knowledge_base_id = str(knowledge_base.get("id") or "").strip()
        document_id = str(document.get("id") or "").strip()
        if not knowledge_base_id or not document_id:
            return 0
        if str(document.get("status") or "active").strip().lower() != "active":
            return 0
        vector_store = self._vector_store_for_kb(knowledge_base_id)
        nodes = self._build_chunk_nodes(
            knowledge_base=knowledge_base,
            document=document,
            chunks=chunks,
            title=title,
            source_path=source_path,
            file_size=file_size,
        )
        if not nodes:
            return 0
        vector_store.add(nodes)
        return len(nodes)

    def _build_chunk_nodes(
        self,
        *,
        knowledge_base: dict[str, Any],
        document: dict[str, Any],
        chunks: list[str],
        title: str,
        source_path: str | None,
        file_size: int,
    ) -> list[Any]:
        if self._embedding_model is None or TextNode is None or NodeRelationship is None or RelatedNodeInfo is None:
            return []
        document_id = str(document.get("id") or "").strip()
        if not document_id:
            return []
        embeddings = self._embedding_model.get_text_embedding_batch(chunks)
        nodes: list[Any] = []
        knowledge_base_id = str(knowledge_base.get("id") or "").strip()
        knowledge_base_key = str(knowledge_base.get("key") or knowledge_base_id).strip()
        knowledge_base_name = str(knowledge_base.get("name") or knowledge_base_id).strip()
        chunk_count = len(chunks)
        for index, chunk in enumerate(chunks):
            embedding = [float(value or 0.0) for value in list(embeddings[index] if index < len(embeddings) else [])]
            if len(embedding) != self._embedding_dimension:
                raise ValueError(
                    f"Embedding 维度不匹配：预期 {self._embedding_dimension}，实际 {len(embedding)}。"
                )
            node = TextNode(
                id_=self._chunk_node_id(document_id, index),
                text=chunk,
                metadata={
                    "knowledge_base_id": knowledge_base_id,
                    "knowledge_base_key": knowledge_base_key,
                    "knowledge_base_name": knowledge_base_name,
                    "document_id": document_id,
                    "document_key": document.get("key"),
                    "title": title,
                    "source_path": source_path,
                    "chunk_index": index,
                    "chunk_count": chunk_count,
                    "file_size": file_size,
                },
                relationships={
                    NodeRelationship.SOURCE: RelatedNodeInfo(node_id=document_id),
                },
            )
            node.embedding = embedding
            nodes.append(node)
        return nodes

    def _delete_document_vectors(self, *, knowledge_base_id: str, document_id: str) -> None:
        if not knowledge_base_id or not document_id or not self._vector_backend_available():
            return
        if not self._vector_table_exists(knowledge_base_id):
            return
        try:
            self._index_for_kb(knowledge_base_id).delete_ref_doc(document_id, delete_from_docstore=True)
        except Exception as exc:
            LOGGER.warning("Failed to delete knowledge document vectors for %s/%s: %s", knowledge_base_id, document_id, exc)

    def _drop_vector_table(self, knowledge_base_id: str) -> None:
        if not knowledge_base_id or not self._vector_backend_available():
            return
        assert lancedb is not None
        try:
            connection = lancedb.connect(str(self._vector_root))
            connection.drop_table(self._knowledge_base_table_name(knowledge_base_id), ignore_missing=True)
        except Exception as exc:
            LOGGER.warning("Failed to drop knowledge base vector table for %s: %s", knowledge_base_id, exc)

    def _vector_table_exists(self, knowledge_base_id: str) -> bool:
        if not knowledge_base_id or not self._vector_backend_available():
            return False
        assert lancedb is not None
        try:
            connection = lancedb.connect(str(self._vector_root))
            target = self._knowledge_base_table_name(knowledge_base_id)
            page_token: str | None = None
            seen_tokens: set[str | None] = set()
            while page_token not in seen_tokens:
                seen_tokens.add(page_token)
                page = list(connection.table_names(page_token))
                if target in page:
                    return True
                if not page:
                    return False
                next_page_token = page[-1]
                if next_page_token == page_token:
                    return False
                page_token = next_page_token
        except Exception:
            return False
        return False

    def _vector_store_for_kb(self, knowledge_base_id: str) -> Any:
        self._require_vector_dependencies()
        assert LanceDBVectorStore is not None
        return LanceDBVectorStore(
            uri=str(self._vector_root),
            table_name=self._knowledge_base_table_name(knowledge_base_id),
        )

    def _index_for_kb(self, knowledge_base_id: str) -> Any:
        self._require_vector_dependencies()
        assert VectorStoreIndex is not None
        embed_model = self._embedding_model
        if not (
            embed_model is not None
            and hasattr(embed_model, "get_query_embedding")
            and hasattr(embed_model, "get_text_embedding_batch")
        ):
            embed_model = MockEmbedding(embed_dim=int(self._embedding_dimension or DEFAULT_MOCK_EMBED_DIM))
        return VectorStoreIndex.from_vector_store(
            self._vector_store_for_kb(knowledge_base_id),
            embed_model=embed_model,
            transformations=self._vector_transformations(),
        )

    def _build_index_for_kb_insert(self, knowledge_base_id: str, document: Any | None = None) -> Any:
        self._require_vector_dependencies()
        assert VectorStoreIndex is not None
        assert StorageContext is not None
        embed_model = self._embedding_model
        if not (
            embed_model is not None
            and hasattr(embed_model, "get_query_embedding")
            and hasattr(embed_model, "get_text_embedding_batch")
        ):
            embed_model = MockEmbedding(embed_dim=int(self._embedding_dimension or DEFAULT_MOCK_EMBED_DIM))
        vector_store = self._vector_store_for_kb(knowledge_base_id)
        if self._vector_table_exists(knowledge_base_id) or document is None:
            return VectorStoreIndex.from_vector_store(
                vector_store,
                embed_model=embed_model,
                transformations=self._vector_transformations(),
            )
        return VectorStoreIndex.from_documents(
            [document],
            storage_context=StorageContext.from_defaults(vector_store=vector_store),
            embed_model=embed_model,
            transformations=self._vector_transformations(),
            show_progress=False,
        )

    def _candidate_knowledge_bases(self, knowledge_base_ids: list[str]) -> list[dict[str, Any]]:
        if knowledge_base_ids:
            records = [self.store.get_knowledge_base(item_id) for item_id in knowledge_base_ids]
        else:
            records = self.store.list_knowledge_bases()
        return [
            dict(item)
            for item in records
            if item is not None and str(item.get("status") or "active").strip().lower() == "active"
        ]

    def _document_text_for_rerank(self, value: dict[str, Any]) -> str:
        parts: list[str] = []
        for candidate in ("title", "source_path", "snippet", "content_text"):
            text = str(value.get(candidate) or "").strip()
            if text:
                parts.append(text)
        if parts:
            return "\n".join(parts[:3])
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

    def _require_knowledge_base(self, knowledge_base_id: str) -> dict[str, Any]:
        knowledge_base = self.store.get_knowledge_base(knowledge_base_id)
        if knowledge_base is None:
            raise ValueError("Knowledge base does not exist.")
        return knowledge_base

    def _require_vector_dependencies(self) -> None:
        if not self._vector_backend_available():
            raise RuntimeError(_missing_vector_dependency_message())

    def _require_provider_embedding_dependencies(self) -> None:
        if LiteLLMEmbedding is None or litellm_embedding is None or PrivateAttr is None:
            raise RuntimeError(_missing_provider_embedding_dependency_message())

    def _require_local_embedding_dependencies(self) -> None:
        if HuggingFaceEmbedding is None:
            raise RuntimeError(_missing_local_embedding_dependency_message())

    def _require_postprocessor_base_dependencies(self) -> None:
        if not self._postprocessor_base_available():
            raise RuntimeError(_missing_vector_dependency_message())

    def _require_provider_rerank_postprocessor_dependencies(self) -> None:
        if LiteLLMRerankPostprocessor is None:
            raise RuntimeError(_missing_vector_dependency_message())

    def _require_provider_rerank_dependencies(self) -> None:
        if litellm_rerank is None:
            raise RuntimeError(_missing_provider_rerank_dependency_message())

    def _require_local_rerank_dependencies(self) -> None:
        if FlagEmbeddingReranker is None:
            raise RuntimeError(_missing_local_rerank_dependency_message())

    def _vector_backend_available(self) -> bool:
        return all(
            item is not None
            for item in (
                lancedb,
                SimpleDirectoryReader,
                VectorStoreIndex,
                SentenceSplitter,
                MockEmbedding,
                NodeRelationship,
                RelatedNodeInfo,
                TextNode,
                LanceDBVectorStore,
            )
        )

    def _postprocessor_base_available(self) -> bool:
        return all(
            item is not None
            for item in (
                BaseNodePostprocessor,
                NodeWithScore,
                QueryBundle,
                TextNode,
                Field,
                PrivateAttr,
            )
        )

    def _vector_transformations(self) -> list[Any]:
        if SentenceSplitter is None:
            return []
        return [SentenceSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)]

    def _store_uploaded_file_to_pool(
        self,
        *,
        knowledge_base_id: str,
        upload_path: str,
        content: bytes,
    ) -> dict[str, Any]:
        if xxhash is None:
            raise RuntimeError("知识库文件去重依赖 `xxhash` 未安装。")
        normalized_path = self._normalize_upload_path(upload_path)
        if not normalized_path:
            raise ValueError("上传文件路径无效。")
        filename = PurePosixPath(normalized_path).name
        if not filename:
            raise ValueError("上传文件名无效。")
        text, extra_metadata = self._extract_text_from_upload(filename=filename, content=content)
        if not text.strip():
            raise ValueError("文件内容为空，无法加入文档池。")
        file_hash = xxhash.xxh128_hexdigest(content)
        blob = self.store.get_knowledge_file_blob_by_xxh128(file_hash)
        blob_path = self._blob_path_for_hash(file_hash)
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        if blob is None:
            if not blob_path.exists():
                blob_path.write_bytes(content)
            blob = self.store.save_knowledge_file_blob(
                blob_id=None,
                xxh128=file_hash,
                storage_name=file_hash,
                storage_relpath=blob_path.relative_to(self.root_dir).as_posix(),
                byte_size=len(content),
                mime_type=str(extra_metadata.get("content_type") or "").strip() or None,
                ext_hint=str(extra_metadata.get("extension") or "").strip() or None,
            )
        elif not blob_path.exists():
            blob_path.write_bytes(content)
        alias = self.store.get_knowledge_file_alias_by_blob_filename(blob_id=str(blob.get("id") or ""), filename=filename)
        if alias is None:
            alias = self.store.save_knowledge_file_alias(
                alias_id=None,
                blob_id=str(blob.get("id") or ""),
                filename=filename,
                suffix=str(Path(filename).suffix.lower() or extra_metadata.get("extension") or ""),
            )
        existing = self.store.get_knowledge_pool_document_by_blob(
            knowledge_base_id=knowledge_base_id,
            blob_id=str(blob.get("id") or ""),
        )
        metadata = {
            **dict((existing or {}).get("metadata_json") or {}),
            **extra_metadata,
            "blob_id": str(blob.get("id") or ""),
            "blob_xxh128": str(blob.get("xxh128") or file_hash),
            "file_size": len(content),
            "source_path": normalized_path,
            "filename": filename,
            "suffix": str(Path(filename).suffix.lower() or extra_metadata.get("extension") or ""),
            "preview": trim_text(text, limit=200),
        }
        return self.store.save_knowledge_pool_document(
            knowledge_pool_document_id=str((existing or {}).get("id") or "").strip() or None,
            knowledge_base_id=knowledge_base_id,
            blob_id=str(blob.get("id") or ""),
            alias_id=str(alias.get("id") or ""),
            key=f"blob:{blob.get('id')}",
            title=filename,
            source_path=normalized_path,
            content_text=text,
            upload_method="http",
            metadata=metadata,
            status=str((existing or {}).get("status") or "active"),
        )

    def _save_and_embed_documents(
        self,
        *,
        knowledge_base: dict[str, Any],
        selected_document_ids: list[str] | None,
        reembed_rows: list[dict[str, Any]],
        job_id: str | None = None,
    ) -> dict[str, Any]:
        knowledge_base_id = str(knowledge_base.get("id") or "").strip()
        selected_ids = {str(item or "").strip() for item in list(selected_document_ids or []) if str(item or "").strip()}
        removed_rows = self.store.list_knowledge_documents(
            knowledge_base_id=knowledge_base_id,
            include_removed=True,
            document_statuses=["removed"],
        )
        pending_rows: list[dict[str, Any]] = []
        if selected_ids:
            for document_id in selected_ids:
                document = self.store.get_knowledge_document(document_id)
                if document is None or str(document.get("knowledge_base_id") or "").strip() != knowledge_base_id:
                    continue
                if self._document_embedding_status(document) != "embedded":
                    pending_rows.append(document)
        else:
            pending_rows = self.store.list_knowledge_documents(
                knowledge_base_id=knowledge_base_id,
                include_removed=False,
                document_statuses=["not_embedded"],
            )
        pending_map = {str(item.get("id") or ""): item for item in pending_rows if str(item.get("id") or "").strip()}
        affected_rows_map: dict[str, dict[str, Any]] = {}
        for row in [*removed_rows, *pending_map.values(), *reembed_rows]:
            document_id = str(row.get("id") or "").strip()
            if document_id:
                affected_rows_map[document_id] = row
        affected_rows = list(affected_rows_map.values())
        if job_id:
            self._update_embedding_job(
                job_id,
                status="running",
                stage="preparing",
                total_documents=len(affected_rows),
                processed_documents=0,
                completed_documents=0,
                failed_documents=0,
                total_chunks_estimated=0,
                embedded_chunks_completed=0,
                message=self._embedding_job_status_text(
                    status="running",
                    stage="preparing",
                    total_documents=len(affected_rows),
                    processed_documents=0,
                ),
            )
        if pending_map or reembed_rows:
            self._ensure_retrieval_loaded_for_embedding()
        if (pending_map or reembed_rows) and (self._embedding_model is None or self._embedding_dimension is None):
            raise ValueError("当前未启用 Embedding，无法执行保存并嵌入。")
        items: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        deleted_count = 0
        for row in affected_rows:
            self._save_document_record(row, sync_status="processing", last_error=None)
        for row in reembed_rows:
            document_id = str(row.get("id") or "").strip()
            if not document_id:
                continue
            document_title = str(row.get("title") or row.get("source_path") or document_id)
            try:
                if job_id:
                    self._update_embedding_job(
                        job_id,
                        status="running",
                        stage="deleting",
                        current_document_id=document_id,
                        current_document_title=document_title,
                        message=self._embedding_job_status_text(
                            status="running",
                            stage="deleting",
                            total_documents=len(affected_rows),
                            processed_documents=int((self.store.get_knowledge_embedding_job(job_id) or {}).get("processed_documents") or 0),
                            current_document_title=document_title,
                        ),
                    )
                self._delete_document_vectors(knowledge_base_id=knowledge_base_id, document_id=document_id)
                refreshed = self._save_document_record(
                    row,
                    document_status="not_embedded",
                    sync_status="processing",
                    last_error=None,
                    embedded_at=None,
                    removed_at=None,
                    metadata={
                        **dict(row.get("metadata_json") or {}),
                        **self._embedding_metadata_fields(status="not_embedded", embedded_chunk_count=0),
                    },
                )
                pending_map[document_id] = refreshed
            except Exception as exc:
                skipped.append(
                    {
                        "id": document_id,
                        "title": str(row.get("title") or row.get("source_path") or document_id),
                        "reason": "reembed-delete-failed",
                        "message": trim_text(str(exc), limit=300) or "删除旧向量失败。",
                    }
                )
                self._save_document_record(row, sync_status="error", last_error=trim_text(str(exc), limit=500))
                if job_id:
                    self._increment_embedding_job(
                        job_id,
                        processed_documents=1,
                        failed_documents=1,
                        stage="deleting",
                        current_document_id=document_id,
                        current_document_title=document_title,
                    )
        for row in removed_rows:
            document_id = str(row.get("id") or "").strip()
            if not document_id:
                continue
            document_title = str(row.get("title") or row.get("source_path") or document_id)
            try:
                if job_id:
                    self._update_embedding_job(
                        job_id,
                        status="running",
                        stage="deleting",
                        current_document_id=document_id,
                        current_document_title=document_title,
                        message=self._embedding_job_status_text(
                            status="running",
                            stage="deleting",
                            total_documents=len(affected_rows),
                            processed_documents=int((self.store.get_knowledge_embedding_job(job_id) or {}).get("processed_documents") or 0),
                            current_document_title=document_title,
                        ),
                    )
                self._delete_document_vectors(knowledge_base_id=knowledge_base_id, document_id=document_id)
                deleted = self.store.delete_knowledge_document(document_id)
                deleted_count += 1
                blob_id = str((deleted or {}).get("blob_id") or "").strip()
                if blob_id:
                    self._cleanup_blob_if_orphan(blob_id)
                if job_id:
                    self._increment_embedding_job(
                        job_id,
                        processed_documents=1,
                        completed_documents=1,
                        stage="deleting",
                        current_document_id=document_id,
                        current_document_title=document_title,
                    )
            except Exception as exc:
                skipped.append(
                    {
                        "id": document_id,
                        "title": str(row.get("title") or row.get("source_path") or document_id),
                        "reason": "delete-failed",
                        "message": trim_text(str(exc), limit=300) or "删除旧向量失败。",
                    }
                )
                self._save_document_record(row, sync_status="error", last_error=trim_text(str(exc), limit=500))
                if job_id:
                    self._increment_embedding_job(
                        job_id,
                        processed_documents=1,
                        failed_documents=1,
                        stage="deleting",
                        current_document_id=document_id,
                        current_document_title=document_title,
                    )
        pending_rows = [row for row in pending_map.values() if self.store.get_knowledge_document(str(row.get("id") or "")) is not None]
        if not pending_rows:
            return {"items": items, "skipped": skipped, "affected_count": len(items) + deleted_count}
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"kb-sync-{knowledge_base_id[:8]}-", dir=str(self._ingest_tmp_root)))
        try:
            materialized: dict[str, tuple[Path, dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]] = {}
            for row in pending_rows:
                document_id = str(row.get("id") or "").strip()
                if not document_id:
                    continue
                document_title = str(row.get("title") or row.get("source_path") or document_id)
                try:
                    if job_id:
                        self._update_embedding_job(
                            job_id,
                            status="running",
                            stage="preparing",
                            current_document_id=document_id,
                            current_document_title=document_title,
                            message=self._embedding_job_status_text(
                                status="running",
                                stage="preparing",
                                total_documents=len(affected_rows),
                                processed_documents=int((self.store.get_knowledge_embedding_job(job_id) or {}).get("processed_documents") or 0),
                                total_chunks_estimated=int((self.store.get_knowledge_embedding_job(job_id) or {}).get("total_chunks_estimated") or 0),
                                embedded_chunks_completed=int((self.store.get_knowledge_embedding_job(job_id) or {}).get("embedded_chunks_completed") or 0),
                                current_document_title=document_title,
                            ),
                        )
                    temp_path, blob, alias = self._materialize_document_for_ingest(tmp_dir=tmp_dir, document=row)
                    materialized[str(temp_path.resolve())] = (temp_path, row, blob, alias)
                except Exception as exc:
                    skipped.append(
                        {
                            "id": document_id,
                            "title": str(row.get("title") or row.get("source_path") or document_id),
                            "reason": "materialize-failed",
                            "message": trim_text(str(exc), limit=300) or "准备嵌入文件失败。",
                        }
                    )
                    self._save_document_record(row, sync_status="error", last_error=trim_text(str(exc), limit=500))
                    if job_id:
                        self._increment_embedding_job(
                            job_id,
                            processed_documents=1,
                            failed_documents=1,
                            stage="preparing",
                            current_document_id=document_id,
                            current_document_title=document_title,
                        )
            if not materialized:
                return {"items": items, "skipped": skipped, "affected_count": len(items) + deleted_count}
            assert SimpleDirectoryReader is not None
            documents = SimpleDirectoryReader(input_dir=str(tmp_dir), recursive=True).load_data()
            parsed_by_path = {
                str(Path(str((doc.metadata or {}).get("file_path") or "")).resolve()): doc
                for doc in list(documents or [])
                if str((doc.metadata or {}).get("file_path") or "").strip()
            }
            index: Any | None = None
            for resolved_path, (_temp_path, row, blob, alias) in materialized.items():
                document_id = str(row.get("id") or "").strip()
                document_title = str(row.get("title") or row.get("source_path") or document_id)
                parsed = parsed_by_path.get(resolved_path)
                if parsed is None:
                    message = "文件已准备完成，但解析结果缺失。"
                    skipped.append(
                        {
                            "id": document_id,
                            "title": str(row.get("title") or row.get("source_path") or document_id),
                            "reason": "reader-missing",
                            "message": message,
                        }
                    )
                    self._save_document_record(row, sync_status="error", last_error=message)
                    if job_id:
                        self._increment_embedding_job(
                            job_id,
                            processed_documents=1,
                            failed_documents=1,
                            stage="embedding",
                            current_document_id=document_id,
                            current_document_title=document_title,
                        )
                    continue
                body = str(getattr(parsed, "text", "") or (parsed.get_content() if hasattr(parsed, "get_content") else "") or "").strip()
                if not body:
                    message = "文件解析结果为空，无法嵌入。"
                    skipped.append(
                        {
                            "id": document_id,
                            "title": str(row.get("title") or row.get("source_path") or document_id),
                            "reason": "empty-document",
                            "message": message,
                        }
                    )
                    self._save_document_record(row, sync_status="error", last_error=message)
                    if job_id:
                        self._increment_embedding_job(
                            job_id,
                            processed_documents=1,
                            failed_documents=1,
                            stage="embedding",
                            current_document_id=document_id,
                            current_document_title=document_title,
                        )
                    continue
                parsed.id_ = document_id
                chunk_count = len(self._chunk_text(body))
                metadata = self._llama_document_metadata(
                    knowledge_base=knowledge_base,
                    document=row,
                    blob=blob,
                    alias=alias,
                    chunk_count=chunk_count,
                        file_size=int(((blob or {}).get("byte_size") or len(body.encode("utf-8"))) or 0),
                )
                parsed.metadata = metadata
                if job_id:
                    self._increment_embedding_job(
                        job_id,
                        total_chunks_estimated=chunk_count,
                        stage="embedding",
                        current_document_id=document_id,
                        current_document_title=document_title,
                    )
                try:
                    if index is None:
                        if self._vector_table_exists(knowledge_base_id):
                            index = self._build_index_for_kb_insert(knowledge_base_id)
                            index.insert(parsed)
                        else:
                            index = self._build_index_for_kb_insert(knowledge_base_id, document=parsed)
                    else:
                        index.insert(parsed)
                except Exception as exc:
                    skipped.append(
                        {
                            "id": document_id,
                            "title": str(row.get("title") or row.get("source_path") or document_id),
                            "reason": "insert-failed",
                            "message": trim_text(str(exc), limit=300) or "向量写入失败。",
                        }
                    )
                    self._save_document_record(row, sync_status="error", last_error=trim_text(str(exc), limit=500))
                    if job_id:
                        self._increment_embedding_job(
                            job_id,
                            processed_documents=1,
                            failed_documents=1,
                            stage="embedding",
                            current_document_id=document_id,
                            current_document_title=document_title,
                        )
                    continue
                saved = self._save_document_record(
                    row,
                    content_text=body,
                    document_status="embedded",
                    sync_status="idle",
                    last_error=None,
                    embedded_at=utcnow_iso(),
                    removed_at=None,
                    metadata={
                        **dict(row.get("metadata_json") or {}),
                        **metadata,
                        "preview": trim_text(body, limit=200),
                        **self._embedding_metadata_fields(status="embedded", embedded_chunk_count=chunk_count),
                    },
                )
                items.append(self._document_resource(saved))
                if job_id:
                    self._increment_embedding_job(
                        job_id,
                        processed_documents=1,
                        completed_documents=1,
                        embedded_chunks_completed=chunk_count,
                        stage="embedding",
                        current_document_id=document_id,
                        current_document_title=document_title,
                    )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return {"items": items, "skipped": skipped, "affected_count": len(items) + deleted_count}

    def _delete_document_embeddings(
        self,
        *,
        knowledge_base_id: str,
        document_ids: list[str],
        job_id: str | None = None,
    ) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for document_id in list(dict.fromkeys(document_ids)):
            document = self.store.get_knowledge_document(str(document_id))
            if document is None:
                skipped.append({"id": document_id, "reason": "missing", "message": "文件不存在。"})
                if job_id:
                    self._increment_embedding_job(job_id, processed_documents=1, failed_documents=1, stage="deleting")
                continue
            if str(document.get("knowledge_base_id") or "").strip() != knowledge_base_id:
                skipped.append({"id": document_id, "reason": "knowledge-base-mismatch", "message": "文件不属于当前知识库。"})
                if job_id:
                    self._increment_embedding_job(job_id, processed_documents=1, failed_documents=1, stage="deleting")
                continue
            document_title = str(document.get("title") or document.get("source_path") or document_id)
            try:
                if job_id:
                    self._update_embedding_job(
                        job_id,
                        status="running",
                        stage="deleting",
                        current_document_id=document_id,
                        current_document_title=document_title,
                        message=self._embedding_job_status_text(
                            status="running",
                            stage="deleting",
                            total_documents=int((self.store.get_knowledge_embedding_job(job_id) or {}).get("total_documents") or 0),
                            processed_documents=int((self.store.get_knowledge_embedding_job(job_id) or {}).get("processed_documents") or 0),
                            current_document_title=document_title,
                        ),
                    )
                self._delete_document_vectors(knowledge_base_id=knowledge_base_id, document_id=str(document.get("id") or ""))
                saved = self._save_document_record(
                    document,
                    document_status="not_embedded",
                    sync_status="idle",
                    last_error=None,
                    embedded_at=None,
                    metadata={
                        **dict(document.get("metadata_json") or {}),
                        **self._embedding_metadata_fields(status="not_embedded", embedded_chunk_count=0),
                    },
                )
                items.append(self._document_resource(saved))
                if job_id:
                    self._increment_embedding_job(
                        job_id,
                        processed_documents=1,
                        completed_documents=1,
                        stage="deleting",
                        current_document_id=document_id,
                        current_document_title=document_title,
                    )
            except Exception as exc:
                skipped.append(
                    {
                        "id": document_id,
                        "title": str(document.get("title") or document.get("source_path") or document_id),
                        "reason": "delete-failed",
                        "message": trim_text(str(exc), limit=300) or "删除嵌入失败。",
                    }
                )
                if job_id:
                    self._increment_embedding_job(
                        job_id,
                        processed_documents=1,
                        failed_documents=1,
                        stage="deleting",
                        current_document_id=document_id,
                        current_document_title=document_title,
                    )
        return {"items": items, "skipped": skipped}

    def _save_document_record(self, document: dict[str, Any], **overrides: Any) -> dict[str, Any]:
        metadata = dict(overrides.pop("metadata", document.get("metadata_json") or document.get("metadata") or {}))
        return self.store.save_knowledge_document(
            knowledge_document_id=str(overrides.pop("knowledge_document_id", document.get("id")) or "").strip() or None,
            knowledge_base_id=str(overrides.pop("knowledge_base_id", document.get("knowledge_base_id")) or "").strip(),
            pool_document_id=str(overrides.pop("pool_document_id", document.get("pool_document_id")) or "").strip() or None,
            blob_id=str(overrides.pop("blob_id", document.get("blob_id")) or "").strip() or None,
            alias_id=str(overrides.pop("alias_id", document.get("alias_id")) or "").strip() or None,
            key=str(overrides.pop("key", document.get("key")) or "").strip() or self._document_key(str(document.get("source_path") or document.get("title") or document.get("id") or "document")),
            title=str(overrides.pop("title", document.get("title")) or document.get("source_path") or document.get("id") or "文档"),
            source_path=str(overrides.pop("source_path", document.get("source_path")) or "").strip() or None,
            content_text=str(overrides.pop("content_text", document.get("content_text")) or ""),
            document_status=str(overrides.pop("document_status", document.get("document_status")) or "not_embedded"),
            sync_status=str(overrides.pop("sync_status", document.get("sync_status")) or "idle"),
            last_error=str(overrides.pop("last_error", document.get("last_error")) or "").strip() or None,
            embedded_at=str(overrides.pop("embedded_at", document.get("embedded_at")) or "").strip() or None,
            removed_at=str(overrides.pop("removed_at", document.get("removed_at")) or "").strip() or None,
            metadata=metadata,
            status=str(overrides.pop("status", document.get("status")) or "active"),
        )

    def _llama_document_metadata(
        self,
        *,
        knowledge_base: dict[str, Any],
        document: dict[str, Any],
        blob: dict[str, Any] | None,
        alias: dict[str, Any] | None,
        chunk_count: int,
        file_size: int,
    ) -> dict[str, Any]:
        metadata = dict(document.get("metadata_json") or {})
        metadata.update(
            {
                "knowledge_base_id": str(knowledge_base.get("id") or ""),
                "knowledge_base_key": str(knowledge_base.get("key") or knowledge_base.get("id") or ""),
                "knowledge_base_name": str(knowledge_base.get("name") or knowledge_base.get("id") or ""),
                "knowledge_document_id": str(document.get("id") or ""),
                "document_key": str(document.get("key") or ""),
                "key": str(document.get("key") or ""),
                "title": str(document.get("title") or document.get("source_path") or document.get("id") or "文档"),
                "source_path": str(document.get("source_path") or "").strip() or None,
                "file_size": int(file_size or 0),
                "chunk_count": int(chunk_count or 0),
                "blob_id": str((blob or {}).get("id") or document.get("blob_id") or ""),
                "xxh128": str((blob or {}).get("xxh128") or metadata.get("blob_xxh128") or ""),
                "filename": str((alias or {}).get("filename") or metadata.get("filename") or ""),
                "suffix": str((alias or {}).get("suffix") or metadata.get("suffix") or ""),
            }
        )
        return metadata

    def _materialize_document_for_ingest(
        self,
        *,
        tmp_dir: Path,
        document: dict[str, Any],
    ) -> tuple[Path, dict[str, Any] | None, dict[str, Any] | None]:
        blob_id = str(document.get("blob_id") or "").strip()
        alias_id = str(document.get("alias_id") or "").strip()
        blob = self.store.get_knowledge_file_blob(blob_id) if blob_id else None
        alias = self.store.get_knowledge_file_alias(alias_id) if alias_id else None
        filename = str((alias or {}).get("filename") or document.get("title") or document.get("source_path") or document.get("id") or "document.txt").strip()
        filename = self._safe_materialized_filename(filename=filename, document_id=str(document.get("id") or ""))
        target_path = tmp_dir / filename
        if blob is not None:
            source_path = self.root_dir / str(blob.get("storage_relpath") or "").strip()
            if not source_path.exists():
                raise ValueError("物理文件不存在。")
            try:
                os.link(str(source_path), str(target_path))
            except Exception:
                shutil.copy2(source_path, target_path)
            return target_path, blob, alias
        body = str(document.get("content_text") or "").strip()
        if not body:
            raise ValueError("文件缺少可用正文内容。")
        target_path.write_text(body, encoding="utf-8")
        return target_path, blob, alias

    def _cleanup_blob_if_orphan(self, blob_id: str) -> None:
        normalized_blob_id = str(blob_id or "").strip()
        if not normalized_blob_id:
            return
        for record in self.store.list_knowledge_pool_documents():
            if str(record.get("blob_id") or "").strip() == normalized_blob_id:
                return
        for record in self.store.list_knowledge_documents(include_removed=True):
            if str(record.get("blob_id") or "").strip() == normalized_blob_id:
                return
        blob = self.store.get_knowledge_file_blob(normalized_blob_id)
        if blob is None:
            return
        storage_relpath = str(blob.get("storage_relpath") or "").strip()
        if storage_relpath:
            target = self.root_dir / storage_relpath
            try:
                target.unlink(missing_ok=True)
            finally:
                self._prune_empty_dirs(target.parent, stop_at=self._blob_root)
        self.store.delete_knowledge_file_blob(normalized_blob_id)

    def _extract_text_from_upload(self, *, filename: str, content: bytes) -> tuple[str, dict[str, Any]]:
        with tempfile.TemporaryDirectory(prefix="kb-upload-", dir=str(self._ingest_tmp_root)) as tempdir:
            temp_path = Path(tempdir) / filename
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path.write_bytes(content)
            return self._extract_text(temp_path)

    def _blob_path_for_hash(self, xxh128_value: str) -> Path:
        digest = str(xxh128_value or "").strip().lower()
        return self._blob_root / digest[:2] / digest[2:4] / digest

    def _safe_materialized_filename(self, *, filename: str, document_id: str) -> str:
        base_name = Path(str(filename or "").strip() or "document.txt").name
        prefix = str(document_id or make_uuid7()).strip() or make_uuid7()
        return f"{prefix}__{base_name}"

    def _normalize_uploaded_files(self, raw_files: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_files, list) or not raw_files:
            raise ValueError("上传内容不能为空。")
        seen: set[str] = set()
        files: list[dict[str, Any]] = []
        for index, item in enumerate(raw_files, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"上传文件 #{index} 格式不正确。")
            path = self._normalize_upload_path(item.get("path"))
            if not path or path in seen:
                continue
            encoded = item.get("content_base64")
            if not isinstance(encoded, str) or not encoded.strip():
                raise ValueError(f"上传文件 #{index} 缺少内容。")
            try:
                content = base64.b64decode(encoded, validate=True)
            except Exception as exc:  # pragma: no cover - defensive
                raise ValueError(f"上传文件 #{index} Base64 内容无效。") from exc
            if len(content) > MAX_UPLOAD_FILE_BYTES:
                raise ValueError(f"文件 `{path}` 超过 25 MB 限制。")
            seen.add(path)
            files.append({"path": path, "content": content})
        if not files:
            raise ValueError("上传内容不能为空。")
        return files

    def _upsert_document_from_file(
        self,
        *,
        knowledge_base: dict[str, Any],
        file_path: Path,
        source_path: str,
        file_size: int,
    ) -> dict[str, Any]:
        text, extra_metadata = self._extract_text(file_path)
        if not text.strip():
            raise ValueError("文件内容为空，无法入库。")
        key = self._document_key(source_path)
        existing = self.store.get_knowledge_document_by_key(
            knowledge_base_id=str(knowledge_base.get("id") or ""),
            key=key,
        )
        return self.sync_document(
            {
                "id": str((existing or {}).get("id") or "").strip() or None,
                "knowledge_base_id": str(knowledge_base.get("id") or ""),
                "key": key,
                "title": file_path.name,
                "source_path": source_path,
                "content_text": text,
                "metadata": {
                    **dict((existing or {}).get("metadata_json") or {}),
                    **extra_metadata,
                    "file_size": file_size,
                    "source_path": source_path,
                },
            },
            previous_document=existing,
        )

    def _upsert_pool_document_from_file(
        self,
        *,
        file_path: Path,
        source_path: str,
        file_size: int,
    ) -> dict[str, Any]:
        text, extra_metadata = self._extract_text(file_path)
        if not text.strip():
            raise ValueError("文件内容为空，无法加入全局文档池。")
        key = self._document_key(source_path)
        existing = self.store.get_knowledge_pool_document_by_key(key)
        return self.store.save_knowledge_pool_document(
            knowledge_pool_document_id=str((existing or {}).get("id") or "").strip() or None,
            key=key,
            title=file_path.name,
            source_path=source_path,
            content_text=text,
            metadata={
                **dict((existing or {}).get("metadata_json") or {}),
                **extra_metadata,
                "file_size": file_size,
                "source_path": source_path,
                "chunk_count": len(self._chunk_text(text)),
                "preview": trim_text(text, limit=200),
            },
        )

    def _extract_text(self, file_path: Path) -> tuple[str, dict[str, Any]]:
        suffix = file_path.suffix.lower()
        if suffix in PDF_EXTENSIONS:
            text = self._extract_with_command(["pdftotext", "-q", str(file_path), "-"])
            if text.strip():
                return self._normalize_text(text), {"content_type": "application/pdf", "extension": suffix}
            raise ValueError("当前环境不支持 PDF 文本提取。")
        if suffix in OFFICE_EXTENSIONS:
            text = self._extract_with_command(["textutil", "-convert", "txt", "-stdout", str(file_path)])
            if text.strip():
                return self._normalize_text(text), {"content_type": "application/octet-stream", "extension": suffix}
            raise ValueError("当前环境不支持该 Office 文档的文本提取。")
        raw = file_path.read_bytes()
        mime_type, _ = mimetypes.guess_type(file_path.name)
        text = self._decode_text(raw)
        if not text and suffix not in TEXT_EXTENSIONS and not str(mime_type or "").startswith("text/"):
            raise ValueError("暂不支持该文件类型的文本提取。")
        if suffix in {".html", ".htm"}:
            text = self._html_to_text(text)
        return self._normalize_text(text), {"content_type": mime_type or "text/plain", "extension": suffix}

    def _decode_text(self, raw: bytes) -> str:
        if not raw:
            return ""
        if b"\x00" in raw:
            return ""
        for encoding in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "gb18030", "big5", "latin-1"):
            try:
                text = raw.decode(encoding)
            except UnicodeDecodeError:
                continue
            if self._looks_like_text(text):
                return text
        return ""

    def _extract_with_command(self, command: list[str]) -> str:
        executable = shutil.which(command[0])
        if not executable:
            return ""
        resolved = [executable, *command[1:]]
        try:
            completed = subprocess.run(resolved, check=False, capture_output=True, text=True, timeout=30)
        except Exception:
            return ""
        if completed.returncode != 0:
            return ""
        return str(completed.stdout or "")

    def _html_to_text(self, text: str) -> str:
        cleaned = HTML_SCRIPT_RE.sub(" ", text)
        cleaned = HTML_TAG_RE.sub(" ", cleaned)
        return cleaned.replace("&nbsp;", " ")

    def _normalize_text(self, text: str) -> str:
        normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
        normalized = "\n".join(line.rstrip() for line in normalized.split("\n"))
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        return normalized.strip()

    def _looks_like_text(self, text: str) -> bool:
        sample = str(text or "")[:4096]
        if not sample:
            return True
        control_count = sum(1 for char in sample if ord(char) < 32 and char not in {"\n", "\r", "\t"})
        replacement_count = sample.count("\ufffd")
        return (control_count / len(sample)) < 0.02 and (replacement_count / len(sample)) < 0.05

    def _chunk_text(self, text: str) -> list[str]:
        body = self._normalize_text(text)
        if not body:
            return []
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", body) if part.strip()]
        if not paragraphs:
            paragraphs = [body]
        chunks: list[str] = []
        current = ""
        for paragraph in paragraphs:
            candidate = paragraph if not current else f"{current}\n\n{paragraph}"
            if current and len(candidate) > CHUNK_SIZE:
                chunks.append(current)
                tail = current[-CHUNK_OVERLAP:] if CHUNK_OVERLAP > 0 else ""
                current = f"{tail}\n\n{paragraph}".strip() if tail else paragraph
                continue
            if len(paragraph) > CHUNK_SIZE and not current:
                start = 0
                step = max(1, CHUNK_SIZE - CHUNK_OVERLAP)
                while start < len(paragraph):
                    chunks.append(paragraph[start : start + CHUNK_SIZE].strip())
                    start += step
                current = ""
                continue
            current = candidate
        if current:
            chunks.append(current)
        return [chunk for chunk in (item.strip() for item in chunks) if chunk]

    def _document_key(self, source_path: str) -> str:
        return hashlib.sha1(str(source_path or "document").strip().lower().encode("utf-8")).hexdigest()

    def _chunk_node_id(self, document_id: str, index: int) -> str:
        return f"{document_id}:{index}"

    def _normalize_upload_path(self, raw_path: Any) -> str:
        value = str(raw_path or "").replace("\\", "/").strip()
        if not value:
            return ""
        relative = PurePosixPath(value)
        if relative.is_absolute() or not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
            return ""
        return relative.as_posix().strip("/")

    def _knowledge_base_source_root(self, knowledge_base_id: str) -> Path:
        return self.root_dir / "files" / knowledge_base_id

    def _knowledge_pool_source_root(self) -> Path:
        return self.root_dir / "pool"

    def _knowledge_base_table_name(self, knowledge_base_id: str) -> str:
        digest = hashlib.sha1(knowledge_base_id.encode("utf-8")).hexdigest()[:16]
        return f"kb_{digest}"

    def _embedding_job_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        progress_percent = self._embedding_job_progress_percent(record)
        result = dict(record.get("result_json") or {})
        return {
            "id": str(record.get("id") or ""),
            "knowledge_base_id": str(record.get("knowledge_base_id") or ""),
            "action": str(record.get("action") or "save"),
            "status": str(record.get("status") or "pending"),
            "stage": str(record.get("stage") or "queued"),
            "total_documents": int(record.get("total_documents") or 0),
            "processed_documents": int(record.get("processed_documents") or 0),
            "completed_documents": int(record.get("completed_documents") or 0),
            "failed_documents": int(record.get("failed_documents") or 0),
            "total_chunks_estimated": int(record.get("total_chunks_estimated") or 0),
            "embedded_chunks_completed": int(record.get("embedded_chunks_completed") or 0),
            "progress_percent": progress_percent,
            "current_document_id": str(record.get("current_document_id") or "").strip() or None,
            "current_document_title": str(record.get("current_document_title") or "").strip() or None,
            "message": str(record.get("message") or "").strip() or None,
            "error": str(record.get("error_text") or "").strip() or None,
            "result": result,
            "created_at": str(record.get("created_at") or "").strip() or None,
            "started_at": str(record.get("started_at") or "").strip() or None,
            "finished_at": str(record.get("finished_at") or "").strip() or None,
            "updated_at": str(record.get("updated_at") or "").strip() or None,
        }

    def _knowledge_base_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = dict(record)
        payload.pop("description", None)
        payload["config_json"] = dict(record.get("config_json") or {})
        knowledge_base_id = str(payload.get("id") or "").strip()
        document_count = int(payload.get("document_count") or 0) if "document_count" in payload else None
        vector_count = int(payload.get("vector_count") or 0) if "vector_count" in payload else 0
        embedding_model_name = str(payload.get("embedding_model_name") or "").strip() or None
        embedding_model_label = str(payload.get("embedding_model_label") or "").strip() or None
        if knowledge_base_id:
            documents = [
                item
                for item in self.store.list_knowledge_documents(knowledge_base_id=knowledge_base_id)
                if str(item.get("status") or "active").strip().lower() == "active"
            ]
            if document_count is None:
                document_count = len(documents)
            latest_embedding_marker = ""
            for document in documents:
                metadata = dict(document.get("metadata_json") or {})
                embedding_status = self._document_embedding_status(document)
                if embedding_status != "embedded":
                    continue
                embedded_chunk_count = int(metadata.get("embedded_chunk_count") or metadata.get("chunk_count") or 0)
                if embedded_chunk_count > 0:
                    vector_count += embedded_chunk_count
                marker = (
                    str(document.get("embedded_at") or "").strip()
                    or str(metadata.get("embedding_updated_at") or "").strip()
                    or str(document.get("updated_at") or "").strip()
                )
                model_name = str(metadata.get("embedding_model_name") or "").strip() or None
                model_label = str(metadata.get("embedding_model_label") or model_name or "").strip() or None
                if model_label and marker >= latest_embedding_marker:
                    latest_embedding_marker = marker
                    embedding_model_name = model_name
                    embedding_model_label = model_label
        payload["document_count"] = int(document_count or 0)
        payload["file_count"] = int(payload.get("document_count") or 0)
        payload["vector_count"] = int(vector_count or 0)
        payload["embedding_model_name"] = embedding_model_name
        payload["embedding_model_label"] = embedding_model_label or embedding_model_name
        return payload

    def _document_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(record.get("metadata_json") or {})
        chunk_count = int(metadata.get("chunk_count") or 0)
        embedding_status = self._document_embedding_status(record)
        embedded_chunk_count = int(metadata.get("embedded_chunk_count") or (chunk_count if embedding_status == "embedded" else 0))
        payload = {
            "id": record.get("id"),
            "knowledge_base_id": record.get("knowledge_base_id"),
            "key": record.get("key"),
            "title": record.get("title"),
            "source_path": record.get("source_path"),
            "updated_at": record.get("updated_at"),
            "created_at": record.get("created_at"),
            "status": record.get("status"),
            "file_size": int(metadata.get("file_size") or 0),
            "chunk_count": chunk_count,
            "embedded_chunk_count": embedded_chunk_count,
            "embedding_status": embedding_status,
            "embedding_status_label": self._document_embedding_status_label(embedding_status),
            "embedding_enabled": embedding_status == "embedded" and embedded_chunk_count > 0,
            "embedding_updated_at": str(metadata.get("embedding_updated_at") or "").strip() or None,
            "embedding_backend": str(metadata.get("embedding_backend") or "").strip() or None,
            "embedding_model_name": str(metadata.get("embedding_model_name") or "").strip() or None,
            "embedding_model_label": str(metadata.get("embedding_model_label") or "").strip() or None,
            "pool_document_id": str(record.get("pool_document_id") or metadata.get("pool_document_id") or "").strip() or None,
            "preview": str(metadata.get("preview") or "").strip() or trim_text(str(record.get("content_text") or ""), limit=180),
            "metadata": metadata,
        }
        return payload

    def _pool_document_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(record.get("metadata_json") or {})
        return {
            "id": record.get("id"),
            "key": record.get("key"),
            "title": record.get("title"),
            "source_path": record.get("source_path"),
            "updated_at": record.get("updated_at"),
            "created_at": record.get("created_at"),
            "status": record.get("status"),
            "file_size": int(metadata.get("file_size") or 0),
            "chunk_count": int(metadata.get("chunk_count") or 0),
            "preview": str(metadata.get("preview") or "").strip() or trim_text(str(record.get("content_text") or ""), limit=180),
            "source_label": "文档池",
            "metadata": metadata,
        }

    def _apply_document_embedding_action(
        self,
        *,
        knowledge_base: dict[str, Any],
        document: dict[str, Any],
        action: str,
    ) -> dict[str, Any] | None:
        knowledge_base_id = str(document.get("knowledge_base_id") or "").strip()
        document_id = str(document.get("id") or "").strip()
        title = str(document.get("title") or document.get("source_path") or document_id or "文档").strip()
        source_path = str(document.get("source_path") or "").strip() or None
        text = str(document.get("content_text") or "").strip()
        if not knowledge_base_id or not document_id:
            raise ValueError("文件索引信息不完整。")
        if action == "delete":
            self._delete_document_vectors(knowledge_base_id=knowledge_base_id, document_id=document_id)
            return self._update_document_embedding_state(document, status="not_embedded", embedded_chunk_count=0)
        if not text:
            raise ValueError("文件正文为空，无法执行嵌入。")
        chunks = self._chunk_text(text)
        if not chunks:
            raise ValueError("文件切分结果为空，无法执行嵌入。")
        current_status = self._document_embedding_status(document)
        if action == "add" and current_status == "embedded":
            return None
        self._delete_document_vectors(knowledge_base_id=knowledge_base_id, document_id=document_id)
        embedded_chunk_count = self._index_document_chunks(
            knowledge_base=knowledge_base,
            document=document,
            chunks=chunks,
            title=title,
            source_path=source_path,
            file_size=int((document.get("metadata_json") or {}).get("file_size") or 0),
        )
        return self._update_document_embedding_state(
            document,
            status="embedded",
            embedded_chunk_count=embedded_chunk_count,
        )

    def _update_document_embedding_state(
        self,
        document: dict[str, Any],
        *,
        status: str,
        embedded_chunk_count: int,
    ) -> dict[str, Any]:
        metadata = dict(document.get("metadata_json") or document.get("metadata") or {})
        metadata.update(self._embedding_metadata_fields(status=status, embedded_chunk_count=embedded_chunk_count))
        saved = self.store.save_knowledge_document(
            knowledge_document_id=str(document.get("id") or "").strip() or None,
            knowledge_base_id=str(document.get("knowledge_base_id") or "").strip(),
            key=str(document.get("key") or "").strip() or self._document_key(str(document.get("source_path") or document.get("title") or document.get("id") or "document")),
            title=str(document.get("title") or document.get("source_path") or document.get("id") or "文档"),
            source_path=str(document.get("source_path") or "").strip() or None,
            content_text=str(document.get("content_text") or ""),
            metadata=metadata,
            status=str(document.get("status") or "active"),
        )
        return saved

    def _embedding_metadata_fields(self, *, status: str, embedded_chunk_count: int) -> dict[str, Any]:
        normalized_status = str(status or "not_embedded").strip().lower()
        runtime = dict(self._embedding_runtime or {})
        payload: dict[str, Any] = {
            "embedding_status": normalized_status,
            "embedded_chunk_count": max(0, int(embedded_chunk_count or 0)),
            "embedding_updated_at": utcnow_iso(),
        }
        if normalized_status == "embedded":
            payload["embedding_backend"] = str(runtime.get("backend") or "").strip()
            payload["embedding_model_name"] = str(runtime.get("model_name") or "").strip()
            payload["embedding_model_label"] = str(runtime.get("model_label") or runtime.get("model_name") or "").strip()
            payload["embedding_vector_dim"] = int(self._embedding_dimension or 0)
        else:
            payload["embedding_backend"] = None
            payload["embedding_model_name"] = None
            payload["embedding_model_label"] = None
            payload["embedding_vector_dim"] = None
        return payload

    def _document_embedding_status(self, record: dict[str, Any]) -> str:
        metadata = dict(record.get("metadata_json") or record.get("metadata") or {})
        document_status = str(record.get("document_status") or "").strip().lower()
        if document_status in {"embedded", "not_embedded", "removed"}:
            return document_status
        chunk_count = int(metadata.get("chunk_count") or 0)
        status = str(metadata.get("embedding_status") or "").strip().lower()
        if status in {"embedded", "not_embedded", "removed", "empty"}:
            return status
        if chunk_count <= 0:
            return "empty"
        return "unknown"

    @staticmethod
    def _document_embedding_status_label(status: str) -> str:
        normalized = str(status or "").strip().lower()
        return {
            "embedded": "已嵌入",
            "not_embedded": "未嵌入",
            "removed": "已移除",
            "empty": "空文件",
            "unknown": "未记录",
        }.get(normalized, "未记录")

    def _prune_empty_dirs(self, path: Path, *, stop_at: Path) -> None:
        current = path
        boundary = stop_at.expanduser().resolve()
        while True:
            try:
                resolved = current.expanduser().resolve()
            except FileNotFoundError:
                return
            if resolved == boundary or boundary not in resolved.parents:
                return
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    def _resolve_base_url(self, provider: dict[str, Any]) -> str | None:
        configured = str(provider.get("base_url") or "").strip()
        if configured:
            return configured
        preset = preset_for(str(provider.get("provider_type") or ""))
        if preset.get("use_default_base_url_when_blank"):
            default_base_url = str(preset.get("default_base_url") or "").strip()
            return default_base_url or None
        return None

    def _resolve_litellm_api_base(self, provider: dict[str, Any]) -> str | None:
        provider_type = str(provider.get("provider_type") or "").strip()
        base_url = self._resolve_base_url(provider)
        if not base_url:
            return None
        if provider_type == "cohere":
            return base_url[:-3] if base_url.endswith("/v2") else base_url
        return base_url

    def _resolve_api_key(self, provider: dict[str, Any]) -> str:
        direct = str(provider.get("api_key") or "").strip()
        if direct:
            return direct
        env_name = str(provider.get("api_key_env") or "").strip()
        if env_name:
            return str(os.getenv(env_name) or "").strip()
        return ""

    def _resolve_provider_alias(self, provider: dict[str, Any]) -> str:
        preset = preset_for(str(provider.get("provider_type") or ""))
        extra_config = dict(provider.get("extra_config") or {})
        return str(extra_config.get("custom_llm_provider") or preset.get("litellm_provider") or provider.get("provider_type") or "").strip()

    def _resolve_litellm_model(self, provider: dict[str, Any], model_name: str) -> str:
        normalized = str(model_name or "").strip()
        if not normalized:
            raise ValueError("Model is required.")
        alias = self._resolve_provider_alias(provider)
        if alias and normalized.startswith(f"{alias}/"):
            return normalized
        return f"{alias}/{normalized}" if alias and alias != "mock" else normalized

    def _sanitize_extra_config(self, extra_config: Any) -> dict[str, Any]:
        if not isinstance(extra_config, dict):
            return {}
        blocked = {"gateway_capabilities", "custom_llm_provider", "dimensions"}
        return {str(key): value for key, value in extra_config.items() if str(key) not in blocked}

    def _litellm_embedding_kwargs(self, provider: dict[str, Any]) -> dict[str, Any]:
        kwargs = self._sanitize_extra_config(provider.get("extra_config"))
        alias = self._resolve_provider_alias(provider)
        if alias and alias != "mock":
            kwargs.setdefault("custom_llm_provider", alias)
        api_version = str(provider.get("api_version") or "").strip()
        if api_version:
            kwargs.setdefault("api_version", api_version)
        organization = str(provider.get("organization") or "").strip()
        if organization:
            kwargs.setdefault("organization", organization)
        if bool(provider.get("skip_tls_verify")):
            kwargs.setdefault("ssl_verify", False)
        headers: dict[str, str] = {}
        extra_headers = provider.get("extra_headers")
        if isinstance(extra_headers, dict):
            headers.update({str(key): str(value) for key, value in extra_headers.items()})
        if organization:
            headers.setdefault("OpenAI-Organization", organization)
        if headers:
            kwargs.setdefault("headers", headers)
        return kwargs

    def _litellm_rerank_kwargs(self, provider: dict[str, Any]) -> dict[str, Any]:
        kwargs = self._sanitize_extra_config(provider.get("extra_config"))
        base_url = self._resolve_litellm_api_base(provider)
        api_key = self._resolve_api_key(provider)
        alias = self._resolve_provider_alias(provider)
        api_version = str(provider.get("api_version") or "").strip()
        organization = str(provider.get("organization") or "").strip()
        if base_url:
            kwargs["api_base"] = base_url
        if api_key:
            kwargs["api_key"] = api_key
        if alias and alias != "mock":
            kwargs.setdefault("custom_llm_provider", alias)
        if api_version:
            kwargs.setdefault("api_version", api_version)
        if organization:
            kwargs.setdefault("organization", organization)
        if bool(provider.get("skip_tls_verify")):
            kwargs.setdefault("ssl_verify", False)
        headers: dict[str, str] = {}
        extra_headers = provider.get("extra_headers")
        if isinstance(extra_headers, dict):
            headers.update({str(key): str(value) for key, value in extra_headers.items()})
        if organization:
            headers.setdefault("OpenAI-Organization", organization)
        if headers:
            kwargs.setdefault("headers", headers)
        return kwargs

    def _coerce_int(self, value: Any, *, minimum: int | None = None) -> int | None:
        try:
            resolved = int(value)
        except (TypeError, ValueError):
            return None
        if minimum is not None:
            resolved = max(minimum, resolved)
        return resolved
