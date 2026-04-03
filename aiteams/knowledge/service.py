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
from pathlib import Path, PurePosixPath
from typing import Any

from aiteams.ai_gateway import AIGateway, ProviderRequestError
from aiteams.catalog import preset_for
from aiteams.storage.metadata import MetadataStore
from aiteams.utils import trim_text, utcnow_iso


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
    from llama_index.core.bridge.pydantic import Field, PrivateAttr
    from llama_index.core.embeddings import MockEmbedding
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
    Field = None
    PrivateAttr = None
    MockEmbedding = None
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
        self._embedding_model: Any = None
        self._embedding_signature = ""
        self._embedding_dimension: int | None = None
        self._embedding_runtime: dict[str, Any] = {"mode": "disabled", "vector_enabled": False, "vector_dim": None}
        self._rerank_runtime: dict[str, Any] = {"mode": "disabled"}
        self._rerank_postprocessor: Any = None
        self.configure_retrieval(retrieval_runtime)

    def close(self) -> None:
        return None

    def configure_retrieval(self, runtime_settings: dict[str, Any] | None = None) -> dict[str, Any]:
        runtime = dict(runtime_settings or {})
        embedding_state = self._build_embedding_state(dict(runtime.get("embedding") or {}))
        rerank_state = self._build_rerank_state(dict(runtime.get("rerank") or {}))
        reindex_required = bool(embedding_state["enabled"]) and (
            self._embedding_model is None or self._embedding_signature != str(embedding_state["signature"])
        )

        self._embedding_model = embedding_state["model"]
        self._embedding_signature = str(embedding_state["signature"])
        self._embedding_dimension = embedding_state["vector_dim"]
        self._embedding_runtime = dict(embedding_state["public"])
        self._rerank_runtime = dict(rerank_state["public"])
        self._rerank_postprocessor = rerank_state["postprocessor"]

        reindexed_knowledge_bases = 0
        reindexed_documents = 0
        reindexed_chunks = 0
        if reindex_required:
            reindexed_knowledge_bases, reindexed_documents, reindexed_chunks = self._reindex_all_knowledge_bases()

        return {
            "embedding_reindexed": reindex_required,
            "reindexed_knowledge_bases": reindexed_knowledge_bases,
            "reindexed_documents": reindexed_documents,
            "reindexed_chunks": reindexed_chunks,
            "retrieval": self.retrieval_info(),
        }

    def retrieval_info(self) -> dict[str, Any]:
        return {
            "embedding": dict(self._embedding_runtime),
            "rerank": dict(self._rerank_runtime),
            "vector_dim": self._embedding_dimension,
        }

    def list_documents_page(
        self,
        *,
        knowledge_base_id: str,
        limit: int | None = None,
        offset: int = 0,
        query: str | None = None,
        embedding_status: str | None = None,
    ) -> dict[str, Any]:
        normalized_query = str(query or "").strip().lower()
        normalized_status = str(embedding_status or "").strip().lower()
        if normalized_status in {"", "all"}:
            normalized_status = ""
        documents = [
            self._document_resource(item)
            for item in self.store.list_knowledge_documents(knowledge_base_id=knowledge_base_id)
        ]
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
        if normalized_status:
            documents = [item for item in documents if str(item.get("embedding_status") or "").strip().lower() == normalized_status]
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
        normalized_query = str(query or "").strip().lower()
        excluded_pool_ids: set[str] = set()
        if exclude_knowledge_base_id:
            for document in self.store.list_knowledge_documents(knowledge_base_id=exclude_knowledge_base_id):
                metadata = dict(document.get("metadata_json") or {})
                pool_document_id = str(metadata.get("pool_document_id") or "").strip()
                if pool_document_id:
                    excluded_pool_ids.add(pool_document_id)
        documents = [
            self._pool_document_resource(item)
            for item in self.store.list_knowledge_pool_documents()
            if str(item.get("id") or "").strip() not in excluded_pool_ids
        ]
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
                "exclude_knowledge_base_id": str(exclude_knowledge_base_id or "").strip() or None,
            },
        }

    def import_pool_uploaded_files(self, payload: dict[str, Any]) -> dict[str, Any]:
        source_root = self._knowledge_pool_source_root()
        source_root.mkdir(parents=True, exist_ok=True)
        files = self._normalize_uploaded_files(payload.get("files"))
        imported: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        total_bytes = 0
        for entry in files:
            total_bytes += len(entry["content"])
            if total_bytes > MAX_UPLOAD_TOTAL_BYTES:
                raise ValueError("上传文件总大小不能超过 100 MB。")
            target = source_root.joinpath(*PurePosixPath(entry["path"]).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(entry["content"])
            try:
                record = self._upsert_pool_document_from_file(file_path=target, source_path=entry["path"], file_size=len(entry["content"]))
            except ValueError as exc:
                skipped.append(
                    {
                        "path": entry["path"],
                        "reason": "invalid-file",
                        "message": str(exc),
                    }
                )
                try:
                    target.unlink(missing_ok=True)
                finally:
                    self._prune_empty_dirs(target.parent, stop_at=source_root)
                continue
            imported.append(self._pool_document_resource(record))
        return {
            "message": f"全局文档池已处理 {len(files)} 个文件。",
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
            raise ValueError("请至少选择一个全局文档。")
        items: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for pool_document_id in selected_ids:
            pool_document = self.store.get_knowledge_pool_document(pool_document_id)
            if pool_document is None:
                skipped.append({"id": pool_document_id, "reason": "missing", "message": "全局文档不存在。"})
                continue
            document_key = f"pool:{pool_document_id}"
            existing = self.store.get_knowledge_document_by_key(knowledge_base_id=knowledge_base_id, key=document_key)
            if existing is not None:
                skipped.append(
                    {
                        "id": pool_document_id,
                        "title": str(pool_document.get("title") or pool_document.get("source_path") or pool_document_id),
                        "reason": "already-added",
                        "message": "该文档已在当前知识库中。",
                    }
                )
                continue
            pool_metadata = dict(pool_document.get("metadata_json") or {})
            saved = self.sync_document(
                {
                    "knowledge_base_id": knowledge_base_id,
                    "key": document_key,
                    "title": str(pool_document.get("title") or pool_document.get("source_path") or pool_document_id),
                    "source_path": str(pool_document.get("source_path") or "").strip() or None,
                    "content_text": str(pool_document.get("content_text") or ""),
                    "metadata": {
                        **pool_metadata,
                        "pool_document_id": pool_document_id,
                        "pool_document_key": str(pool_document.get("key") or "").strip() or None,
                        "source_path": str(pool_document.get("source_path") or "").strip() or None,
                    },
                },
                previous_document=existing,
            )
            items.append(self._document_resource(saved))
        self.store.touch_knowledge_base(knowledge_base_id)
        summary = self.store.get_knowledge_base(knowledge_base_id) or knowledge_base
        return {
            "message": f"已从全局文档池加入 {len(items)} 个文档。",
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
            raise ValueError("全局文档池当前仅支持 delete 操作。")
        selected_ids = list(dict.fromkeys(str(item or "").strip() for item in list(document_ids or []) if str(item or "").strip()))
        if not selected_ids:
            raise ValueError("请至少选择一个全局文档。")
        linked_pool_ids: dict[str, int] = {}
        for document in self.store.list_knowledge_documents():
            metadata = dict(document.get("metadata_json") or {})
            pool_document_id = str(metadata.get("pool_document_id") or "").strip()
            if pool_document_id:
                linked_pool_ids[pool_document_id] = linked_pool_ids.get(pool_document_id, 0) + 1
        deleted: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        source_root = self._knowledge_pool_source_root()
        for document_id in selected_ids:
            document = self.store.get_knowledge_pool_document(document_id)
            if document is None:
                skipped.append({"id": document_id, "reason": "missing", "message": "全局文档不存在。"})
                continue
            if linked_pool_ids.get(document_id, 0) > 0:
                skipped.append(
                    {
                        "id": document_id,
                        "title": str(document.get("title") or document.get("source_path") or document_id),
                        "reason": "in-use",
                        "message": "该文档仍被其他知识库引用，不能从全局文档池删除。",
                    }
                )
                continue
            source_path = str(document.get("source_path") or "").strip()
            if source_path:
                target = source_root.joinpath(*PurePosixPath(source_path).parts)
                try:
                    target.unlink(missing_ok=True)
                finally:
                    self._prune_empty_dirs(target.parent, stop_at=source_root)
            removed = self.store.delete_knowledge_pool_document(document_id)
            if removed is not None:
                deleted.append(self._pool_document_resource(removed))
        return {
            "message": f"全局文档池已删除 {len(deleted)} 个文档。",
            "affected_count": len(deleted),
            "skipped_count": len(skipped),
            "items": deleted,
            "skipped": skipped,
        }

    def import_uploaded_files(self, knowledge_base_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        knowledge_base = self._require_knowledge_base(knowledge_base_id)
        source_root = self._knowledge_base_source_root(knowledge_base_id)
        source_root.mkdir(parents=True, exist_ok=True)
        files = self._normalize_uploaded_files(payload.get("files"))
        imported: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        total_bytes = 0
        for entry in files:
            total_bytes += len(entry["content"])
            if total_bytes > MAX_UPLOAD_TOTAL_BYTES:
                raise ValueError("上传文件总大小不能超过 100 MB。")
            target = source_root.joinpath(*PurePosixPath(entry["path"]).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(entry["content"])
            try:
                record = self._upsert_document_from_file(
                    knowledge_base=knowledge_base,
                    file_path=target,
                    source_path=entry["path"],
                    file_size=len(entry["content"]),
                )
                imported.append(self._document_resource(record))
            except ValueError as exc:
                skipped.append(
                    {
                        "path": entry["path"],
                        "reason": "unsupported-file",
                        "message": str(exc),
                    }
                )
                try:
                    target.unlink(missing_ok=True)
                finally:
                    self._prune_empty_dirs(target.parent, stop_at=source_root)
        self.store.touch_knowledge_base(knowledge_base_id)
        summary = self.store.get_knowledge_base(knowledge_base_id) or knowledge_base
        return {
            "message": f"已处理 {len(files)} 个文件。",
            "knowledge_base": self._knowledge_base_resource(summary),
            "imported_count": len(imported),
            "skipped_count": len(skipped),
            "uploaded_total_bytes": total_bytes,
            "items": imported,
            "skipped": skipped,
        }

    def manage_document_embeddings(
        self,
        knowledge_base_id: str,
        *,
        action: str,
        document_ids: list[str],
    ) -> dict[str, Any]:
        knowledge_base = self._require_knowledge_base(knowledge_base_id)
        normalized_action = str(action or "").strip().lower()
        if normalized_action not in {"add", "reembed", "delete"}:
            raise ValueError("知识库文档嵌入操作仅支持 add / reembed / delete。")
        selected_ids = list(dict.fromkeys(str(item or "").strip() for item in list(document_ids or []) if str(item or "").strip()))
        if not selected_ids:
            raise ValueError("请至少选择一个文件。")
        if normalized_action in {"add", "reembed"} and (self._embedding_model is None or self._embedding_dimension is None):
            raise ValueError("当前未启用 Embedding，无法执行嵌入操作。")

        items: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for document_id in selected_ids:
            document = self.store.get_knowledge_document(document_id)
            if document is None:
                skipped.append({"id": document_id, "reason": "missing", "message": "文件不存在。"})
                continue
            if str(document.get("knowledge_base_id") or "").strip() != knowledge_base_id:
                skipped.append({"id": document_id, "reason": "knowledge-base-mismatch", "message": "文件不属于当前知识库。"})
                continue
            try:
                updated = self._apply_document_embedding_action(
                    knowledge_base=knowledge_base,
                    document=document,
                    action=normalized_action,
                )
            except ValueError as exc:
                skipped.append(
                    {
                        "id": document_id,
                        "title": str(document.get("title") or document.get("source_path") or document_id),
                        "reason": "invalid-document",
                        "message": str(exc),
                    }
                )
                continue
            if updated is None:
                skipped.append(
                    {
                        "id": document_id,
                        "title": str(document.get("title") or document.get("source_path") or document_id),
                        "reason": "skipped",
                        "message": "当前文件无需执行该操作。",
                    }
                )
                continue
            items.append(self._document_resource(updated))

        self.store.touch_knowledge_base(knowledge_base_id)
        summary = self.store.get_knowledge_base(knowledge_base_id) or knowledge_base
        action_label = {"add": "新增嵌入", "reembed": "重新嵌入", "delete": "删除嵌入"}[normalized_action]
        return {
            "message": f"{action_label}已处理 {len(items)} 个文件。",
            "action": normalized_action,
            "knowledge_base": self._knowledge_base_resource(summary),
            "affected_count": len(items),
            "skipped_count": len(skipped),
            "items": items,
            "skipped": skipped,
            "retrieval": self.retrieval_info(),
        }

    def sync_document(
        self,
        document: dict[str, Any],
        *,
        previous_document: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        knowledge_base_id = str(document.get("knowledge_base_id") or "").strip()
        knowledge_base = self._require_knowledge_base(knowledge_base_id)
        metadata = dict(document.get("metadata_json") or document.get("metadata") or {})
        source_path = str(document.get("source_path") or "").strip() or None
        document_id = str(document.get("id") or "").strip()
        key = str(document.get("key") or "").strip() or self._document_key(
            source_path or str(document.get("title") or document_id or "document")
        )
        title = str(document.get("title") or source_path or key or document_id or "文档").strip()
        text = str(document.get("content_text") or "").strip()
        self._delete_document_vectors(knowledge_base_id=knowledge_base_id, document_id=document_id)
        if not text:
            saved = self.store.save_knowledge_document(
                knowledge_document_id=document_id or None,
                knowledge_base_id=knowledge_base_id,
                key=key,
                title=title,
                source_path=source_path,
                content_text="",
                metadata={
                    **metadata,
                    "chunk_count": 0,
                    "file_size": int(metadata.get("file_size") or 0),
                    "source_path": source_path,
                    **self._embedding_metadata_fields(status="empty", embedded_chunk_count=0),
                },
                status=str(document.get("status") or "active"),
            )
            self.store.touch_knowledge_base(knowledge_base_id)
            return saved
        chunks = self._chunk_text(text)
        saved = self.store.save_knowledge_document(
            knowledge_document_id=document_id or None,
            knowledge_base_id=knowledge_base_id,
            key=key,
            title=title,
            source_path=source_path,
            content_text=text,
            metadata={
                **metadata,
                "chunk_count": len(chunks),
                "file_size": int(metadata.get("file_size") or len(text.encode("utf-8"))),
                "source_path": source_path,
                "preview": trim_text(text, limit=200),
                **self._embedding_metadata_fields(status="not_embedded", embedded_chunk_count=0),
            },
            status=str(document.get("status") or "active"),
        )
        if chunks and self._embedding_model is not None and self._embedding_dimension is not None:
            embedded_chunk_count = self._index_document_chunks(
                knowledge_base=knowledge_base,
                document=saved,
                chunks=chunks,
                title=title,
                source_path=source_path,
                file_size=int((saved.get("metadata_json") or {}).get("file_size") or 0),
            )
            saved = self._update_document_embedding_state(
                saved,
                status="embedded",
                embedded_chunk_count=embedded_chunk_count,
            )
        self.store.touch_knowledge_base(knowledge_base_id)
        return saved

    def delete_document(self, knowledge_document_id: str) -> dict[str, Any] | None:
        existing = self.store.get_knowledge_document(knowledge_document_id)
        if existing is None:
            return None
        knowledge_base_id = str(existing.get("knowledge_base_id") or "").strip()
        if knowledge_base_id:
            self._delete_document_vectors(knowledge_base_id=knowledge_base_id, document_id=knowledge_document_id)
        source_path = str(existing.get("source_path") or "").strip()
        if knowledge_base_id and source_path:
            target = self._knowledge_base_source_root(knowledge_base_id).joinpath(*PurePosixPath(source_path).parts)
            try:
                target.unlink(missing_ok=True)
            finally:
                self._prune_empty_dirs(target.parent, stop_at=self._knowledge_base_source_root(knowledge_base_id))
        deleted = self.store.delete_knowledge_document(knowledge_document_id)
        if knowledge_base_id:
            self.store.touch_knowledge_base(knowledge_base_id)
        return deleted

    def delete_knowledge_base(self, knowledge_base_id: str) -> dict[str, Any] | None:
        existing = self.store.get_knowledge_base(knowledge_base_id)
        if existing is None:
            return None
        for document in self.store.list_knowledge_documents(knowledge_base_id=knowledge_base_id):
            self.delete_document(str(document.get("id") or ""))
        self._drop_vector_table(knowledge_base_id)
        deleted = self.store.delete_knowledge_base(knowledge_base_id)
        target_dir = self.root_dir / "files" / knowledge_base_id
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
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

    def _build_embedding_state(self, runtime: dict[str, Any]) -> dict[str, Any]:
        mode = str(runtime.get("mode") or "").strip().lower()
        if mode in {"", "disabled"}:
            return {
                "enabled": False,
                "model": None,
                "signature": "",
                "vector_dim": None,
                "public": {"mode": "disabled", "vector_enabled": False, "vector_dim": None},
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
                },
            }

        if mode != "provider":
            return {
                "enabled": False,
                "model": None,
                "signature": "",
                "vector_dim": None,
                "public": {"mode": "disabled", "vector_enabled": False, "vector_dim": None},
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
            },
        }

    def _build_rerank_state(self, runtime: dict[str, Any]) -> dict[str, Any]:
        mode = str(runtime.get("mode") or "").strip().lower()
        if mode in {"", "disabled"}:
            return {
                "enabled": False,
                "postprocessor": None,
                "public": {"mode": "disabled", "strategy": "disabled"},
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
                },
            }
        if mode != "provider":
            return {
                "enabled": False,
                "postprocessor": None,
                "public": {"mode": "disabled", "strategy": "disabled"},
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
            for document in self.store.list_knowledge_documents(knowledge_base_id=knowledge_base_id):
                if str(document.get("status") or "active").strip().lower() != "active":
                    continue
                content_text = str(document.get("content_text") or "").strip()
                if not content_text:
                    continue
                chunks = self._chunk_text(content_text)
                if not chunks:
                    continue
                documents_indexed += 1
                indexed_chunk_count = self._index_document_chunks(
                    knowledge_base=knowledge_base,
                    document=document,
                    chunks=chunks,
                    title=str(document.get("title") or document.get("source_path") or document.get("id") or "文档"),
                    source_path=str(document.get("source_path") or "").strip() or None,
                    file_size=int((document.get("metadata_json") or {}).get("file_size") or 0),
                )
                chunks_indexed += indexed_chunk_count
                self._update_document_embedding_state(
                    document,
                    status="embedded",
                    embedded_chunk_count=indexed_chunk_count,
                )
        return len(knowledge_bases), documents_indexed, chunks_indexed

    def _search_vector(self, *, query: str, knowledge_base_ids: list[str], limit: int) -> list[dict[str, Any]]:
        if self._embedding_model is None or self._embedding_dimension is None:
            return []
        if not query:
            return []
        assert VectorStoreQuery is not None
        kb_records = self._candidate_knowledge_bases(knowledge_base_ids)
        if not kb_records:
            return []
        query_vector = [float(value or 0.0) for value in list(self._embedding_model.get_query_embedding(query) or [])]
        if len(query_vector) != self._embedding_dimension:
            raise ValueError(
                f"Embedding 维度不匹配：预期 {self._embedding_dimension}，实际 {len(query_vector)}。"
            )
        head_size = max(limit * VECTOR_HEAD_MULTIPLIER, limit)
        items: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for knowledge_base in kb_records:
            knowledge_base_id = str(knowledge_base.get("id") or "").strip()
            if not knowledge_base_id or not self._vector_table_exists(knowledge_base_id):
                continue
            vector_store = self._vector_store_for_kb(knowledge_base_id)
            try:
                result = vector_store.query(
                    VectorStoreQuery(
                        query_embedding=query_vector,
                        query_str=query,
                        similarity_top_k=head_size,
                    )
                )
            except Warning:
                continue
            similarities = list(result.similarities or [])
            for index, node in enumerate(list(result.nodes or [])):
                metadata = dict(getattr(node, "metadata", {}) or {})
                document_id = str(metadata.get("document_id") or getattr(node, "ref_doc_id", None) or "").strip()
                if not document_id:
                    continue
                chunk_index = self._coerce_int(metadata.get("chunk_index"), minimum=0) or 0
                identity = (document_id, chunk_index)
                if identity in seen:
                    continue
                seen.add(identity)
                text = str(node.get_content() if hasattr(node, "get_content") else getattr(node, "text", "") or "").strip()
                score = float(similarities[index] if index < len(similarities) else 0.0)
                items.append(
                    {
                        "id": document_id,
                        "knowledge_base_id": knowledge_base_id,
                        "knowledge_base_key": metadata.get("knowledge_base_key") or knowledge_base.get("key"),
                        "knowledge_base_name": metadata.get("knowledge_base_name") or knowledge_base.get("name"),
                        "key": metadata.get("document_key"),
                        "title": metadata.get("title"),
                        "source_path": metadata.get("source_path"),
                        "metadata_json": {
                            "chunk_count": self._coerce_int(metadata.get("chunk_count"), minimum=0) or 0,
                            "file_size": self._coerce_int(metadata.get("file_size"), minimum=0) or 0,
                        },
                        "score": score,
                        "chunk_index": chunk_index,
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
            self._vector_store_for_kb(knowledge_base_id).delete(document_id)
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
                MockEmbedding,
                NodeRelationship,
                RelatedNodeInfo,
                TextNode,
                VectorStoreQuery,
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

    def _knowledge_base_resource(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = dict(record)
        payload.pop("description", None)
        if "document_count" not in payload and str(payload.get("id") or "").strip():
            payload["document_count"] = len(
                self.store.list_knowledge_documents(knowledge_base_id=str(payload["id"]))
            )
        payload["file_count"] = int(payload.get("document_count") or 0)
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
            "pool_document_id": str(metadata.get("pool_document_id") or "").strip() or None,
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
            "source_label": "全局文档池",
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
        chunk_count = int(metadata.get("chunk_count") or 0)
        status = str(metadata.get("embedding_status") or "").strip().lower()
        if status in {"embedded", "not_embedded", "empty"}:
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
