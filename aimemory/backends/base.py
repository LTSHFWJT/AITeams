from __future__ import annotations

from typing import Any, Protocol


class IndexBackend(Protocol):
    backend_name: str
    active_backend: str
    available: bool

    def describe_capabilities(self) -> dict[str, Any]: ...

    def upsert_memory(self, payload: dict[str, Any]) -> None: ...

    def delete_memory(self, record_id: str) -> None: ...

    def list_memory_candidates(
        self,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        limit: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def search_memory_candidates(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        scope: str = "all",
        limit: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def upsert_knowledge_chunk(self, payload: dict[str, Any]) -> None: ...

    def delete_knowledge_chunk(self, record_id: str) -> None: ...

    def list_knowledge_chunks(self, *, limit: int | None = None) -> list[dict[str, Any]]: ...

    def search_knowledge_chunks(self, query: str, *, limit: int | None = None) -> list[dict[str, Any]]: ...

    def upsert_skill(self, payload: dict[str, Any]) -> None: ...

    def delete_skill(self, record_id: str) -> None: ...

    def list_skill_records(self, *, limit: int | None = None) -> list[dict[str, Any]]: ...

    def search_skill_records(self, query: str, *, limit: int | None = None) -> list[dict[str, Any]]: ...

    def upsert_archive_summary(self, payload: dict[str, Any]) -> None: ...

    def delete_archive_summary(self, record_id: str) -> None: ...

    def list_archive_summaries(
        self,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def search_archive_summaries(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]: ...


class GraphBackend(Protocol):
    backend_name: str
    active_backend: str
    available: bool

    def describe_capabilities(self) -> dict[str, Any]: ...

    def upsert_node(
        self,
        node_type: str,
        ref_id: str,
        label: str,
        metadata: dict[str, Any] | None = None,
    ) -> str: ...

    def upsert_edge(
        self,
        source_type: str,
        source_ref_id: str,
        edge_type: str,
        target_type: str,
        target_ref_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> str: ...

    def delete_reference(self, ref_id: str) -> None: ...

    def relations_for_ref(self, ref_id: str, *, limit: int = 12) -> list[dict[str, Any]]: ...
