from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
from pathlib import Path
from typing import Any
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from aiteams.ai_gateway import AIGateway, ProviderRequestError
from aiteams.catalog import list_provider_presets
from aiteams.config import AppSettings
from aiteams.memory_service import AgentMemoryService
from aiteams.platform_db import PlatformDatabase
from aiteams.repositories import PlatformRepository
from aiteams.services.collaboration import CollaborationService

LOGGER = logging.getLogger("aiteams")
if not LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    LOGGER.addHandler(_handler)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False


class AppError(RuntimeError):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status
        self.detail = detail


@dataclass(slots=True)
class ServiceContainer:
    settings: AppSettings
    db: PlatformDatabase
    repository: PlatformRepository
    gateway: AIGateway
    memory_service: AgentMemoryService
    collaboration_service: CollaborationService

    def close(self) -> None:
        self.gateway.close()
        self.memory_service.close()
        self.db.close()


@dataclass(slots=True)
class AppResponse:
    status: int
    body: bytes
    content_type: str
    headers: dict[str, str] = field(default_factory=dict)


def _build_container(settings: AppSettings) -> ServiceContainer:
    db = PlatformDatabase(settings.platform_db_path)
    repository = PlatformRepository(db)
    gateway = AIGateway(timeout_seconds=settings.request_timeout_seconds)
    memory_service = AgentMemoryService(settings)
    collaboration_service = CollaborationService(repository, gateway, memory_service)
    return ServiceContainer(
        settings=settings,
        db=db,
        repository=repository,
        gateway=gateway,
        memory_service=memory_service,
        collaboration_service=collaboration_service,
    )


class WebApplication:
    def __init__(self, settings: AppSettings | None = None):
        self.settings = settings or AppSettings.load()
        self.container = _build_container(self.settings)

    def close(self) -> None:
        self.container.close()

    def handle(self, method: str, raw_path: str, body: bytes = b"") -> AppResponse:
        parsed = urlparse(raw_path)
        path = parsed.path
        query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}

        try:
            if method == "OPTIONS":
                return self._empty(204)
            if method == "GET" and path == "/":
                return self._file_response(self.settings.static_dir / "index.html", "text/html; charset=utf-8")
            if method == "GET" and path.startswith("/static/"):
                return self._serve_static(path)
            if method == "GET" and path == "/api/health":
                return self._json(200, {"status": "ok"})
            if method == "GET" and path == "/api/catalog/providers":
                return self._json(200, {"items": list_provider_presets()})
            if method == "GET" and path == "/api/providers":
                include_all = self._parse_bool(query.get("all"))
                filters = self._provider_filters_from_query(query)
                limit = None if include_all or query.get("limit") in (None, "") else self._parse_int(query.get("limit"), default=10, minimum=1, maximum=100)
                offset = 0 if include_all else self._parse_int(query.get("offset"), default=0, minimum=0, maximum=100000)
                count = self.container.repository.count_providers(filters=filters)
                items = self.container.repository.list_providers(limit=limit, offset=offset, filters=filters)
                resolved_limit = count if limit is None else limit
                return self._json(200, {"items": items, "count": count, "limit": resolved_limit, "offset": offset})
            if method == "POST" and path == "/api/providers":
                payload = self._parse_json(body)
                self._require_fields(payload, "name", "provider_type", "model")
                return self._json(200, self.container.repository.save_provider(payload))
            if method == "POST" and path == "/api/providers/test":
                payload = self._parse_json(body)
                provider = None
                if payload.get("provider_id"):
                    provider = self.container.repository.get_provider(str(payload["provider_id"]), include_secret=True)
                elif isinstance(payload.get("config"), dict):
                    provider = self._resolve_provider_test_config(payload["config"])
                if provider is None:
                    raise AppError(400, "Either provider_id or config is required.")
                result = self.container.gateway.chat(
                    provider,
                    [{"role": "user", "content": str(payload.get("prompt") or "Reply with READY.")}],
                    model=provider.get("model"),
                )
                return self._json(200, {"content": result.content, "model": result.model, "usage": result.usage})
            if path.startswith("/api/providers/"):
                parts = [part for part in path.split("/") if part]
                if len(parts) == 3:
                    provider_id = parts[2]
                    if method == "GET":
                        provider = self.container.repository.get_provider(provider_id, include_secret=False)
                        if provider is None:
                            raise AppError(404, "Provider does not exist.")
                        return self._json(200, provider)
                    if method == "PUT":
                        existing = self.container.repository.get_provider(provider_id, include_secret=False)
                        if existing is None:
                            raise AppError(404, "Provider does not exist.")
                        payload = self._parse_json(body)
                        payload["id"] = provider_id
                        self._require_fields(payload, "name", "provider_type", "model")
                        return self._json(200, self.container.repository.save_provider(payload))
                    if method == "DELETE":
                        provider = self.container.repository.get_provider(provider_id, include_secret=False)
                        if provider is None:
                            raise AppError(404, "Provider does not exist.")
                        counts = self.container.repository.provider_dependency_counts(provider_id)
                        if counts["agent_count"] or counts["message_count"]:
                            raise AppError(409, self._render_provider_dependency_message(counts))
                        deleted = self.container.repository.delete_provider(provider_id)
                        assert deleted is not None
                        return self._json(200, deleted)
            if method == "GET" and path == "/api/agents":
                include_all = self._parse_bool(query.get("all"))
                filters = self._agent_filters_from_query(query)
                limit = None if include_all or query.get("limit") in (None, "") else self._parse_int(query.get("limit"), default=10, minimum=1, maximum=100)
                offset = 0 if include_all else self._parse_int(query.get("offset"), default=0, minimum=0, maximum=100000)
                count = self.container.repository.count_agents(filters=filters)
                items = self.container.repository.list_agents(limit=limit, offset=offset, filters=filters)
                resolved_limit = count if limit is None else limit
                return self._json(200, {"items": items, "count": count, "limit": resolved_limit, "offset": offset})
            if method == "POST" and path == "/api/agents":
                payload = self._parse_json(body)
                self._require_fields(payload, "name", "role", "system_prompt", "provider_id")
                provider = self.container.repository.get_provider(str(payload["provider_id"]), include_secret=False)
                if provider is None:
                    raise AppError(404, "Provider does not exist.")
                return self._json(200, self.container.repository.save_agent(payload))
            if method == "GET" and path.startswith("/api/agents/") and path.endswith("/memory"):
                parts = [part for part in path.split("/") if part]
                if len(parts) < 4:
                    raise AppError(404, "Agent does not exist.")
                agent_id = parts[2]
                agent = self.container.repository.get_agent(agent_id)
                if agent is None:
                    raise AppError(404, "Agent does not exist.")
                limit = self._parse_int(query.get("limit"), default=8, minimum=1, maximum=50)
                search = query.get("query")
                results = (
                    self.container.memory_service.search_agent_memories(agent_id, search, limit=limit)
                    if search
                    else self.container.memory_service.list_agent_memories(agent_id, limit=limit)
                )
                return self._json(200, {"agent": agent, "results": results})
            if path.startswith("/api/agents/"):
                parts = [part for part in path.split("/") if part]
                if len(parts) == 3:
                    agent_id = parts[2]
                    if method == "GET":
                        agent = self.container.repository.get_agent(agent_id)
                        if agent is None:
                            raise AppError(404, "Agent does not exist.")
                        return self._json(200, agent)
                    if method == "PUT":
                        existing = self.container.repository.get_agent(agent_id)
                        if existing is None:
                            raise AppError(404, "Agent does not exist.")
                        payload = self._parse_json(body)
                        payload["id"] = agent_id
                        self._require_fields(payload, "name", "role", "system_prompt", "provider_id")
                        provider = self.container.repository.get_provider(str(payload["provider_id"]), include_secret=False)
                        if provider is None:
                            raise AppError(404, "Provider does not exist.")
                        return self._json(200, self.container.repository.save_agent(payload))
                    if method == "DELETE":
                        agent = self.container.repository.get_agent(agent_id)
                        if agent is None:
                            raise AppError(404, "Agent does not exist.")
                        counts = self.container.repository.agent_dependency_counts(agent_id)
                        if counts["lead_session_count"] or counts["participant_count"] or counts["message_count"]:
                            raise AppError(409, self._render_agent_dependency_message(counts))
                        deleted = self.container.repository.delete_agent(agent_id)
                        assert deleted is not None
                        return self._json(200, deleted)
            if method == "GET" and path == "/api/skills":
                limit = self._parse_int(query.get("limit"), default=50, minimum=1, maximum=200)
                offset = self._parse_int(query.get("offset"), default=0, minimum=0, maximum=10000)
                status = str(query.get("status") or "").strip() or None
                items = self.container.memory_service.list_skills(limit=limit, offset=offset, status=status)
                return self._json(200, {"items": items, "count": len(items), "limit": limit, "offset": offset})
            if method == "POST" and path == "/api/skills":
                payload = self._parse_json(body)
                self._require_fields(payload, "name", "description")
                skill = self.container.memory_service.add_skill(
                    str(payload["name"]),
                    str(payload["description"]),
                    skill_markdown=self._optional_str(payload.get("skill_markdown")),
                    prompt_template=self._optional_str(payload.get("prompt_template")),
                    workflow=payload.get("workflow"),
                    tools=self._string_list(payload.get("tools")),
                    topics=self._string_list(payload.get("topics")),
                    metadata=self._object_dict(payload.get("metadata"), field_name="metadata"),
                    status=str(payload.get("status") or "draft"),
                    version=self._optional_str(payload.get("version")),
                    assets=self._skill_assets(payload.get("assets")),
                    folder_name=self._optional_str(payload.get("folder_name")),
                    source_kind=self._optional_str(payload.get("source_kind")),
                )
                return self._json(200, skill)
            if method == "POST" and path == "/api/skills/import":
                payload = self._parse_json(body)
                items = payload.get("items")
                if not isinstance(items, list) or not items:
                    raise AppError(400, "items must be a non-empty list.")
                imported: list[dict[str, Any]] = []
                errors: list[dict[str, Any]] = []
                for item in items:
                    if not isinstance(item, dict):
                        errors.append({"folder_name": None, "detail": "Each import item must be an object."})
                        continue
                    folder_name = self._optional_str(item.get("folder_name"))
                    try:
                        skill_markdown = self._optional_str(item.get("skill_markdown")) or self._optional_str(item.get("prompt_template"))
                        if not skill_markdown:
                            raise ValueError("skill_markdown is required.")
                        imported.append(
                            self.container.memory_service.import_skill(
                                name=self._optional_str(item.get("name")),
                                description=self._optional_str(item.get("description")),
                                skill_markdown=skill_markdown,
                                assets=self._skill_assets(item.get("assets")),
                                metadata=self._object_dict(item.get("metadata"), field_name="metadata"),
                                status=self._optional_str(item.get("status")) or "draft",
                                version=self._optional_str(item.get("version")),
                                folder_name=folder_name,
                                source_kind=self._optional_str(item.get("source_kind")) or "folder-import",
                            )
                        )
                    except Exception as exc:
                        errors.append({"folder_name": folder_name, "detail": str(exc)})
                return self._json(
                    200,
                    {
                        "items": imported,
                        "errors": errors,
                        "count": len(imported),
                        "error_count": len(errors),
                    },
                )
            if method == "GET" and path == "/api/skills/search":
                query_text = str(query.get("query") or "").strip()
                if not query_text:
                    raise AppError(400, "query is required.")
                limit = self._parse_int(query.get("limit"), default=10, minimum=1, maximum=100)
                items = self.container.memory_service.search_skills(query_text, limit=limit)
                return self._json(200, {"items": items, "query": query_text, "count": len(items)})
            if path.startswith("/api/skills/"):
                skill_id = path.rsplit("/", 1)[-1]
                if method == "GET":
                    skill = self.container.memory_service.get_skill(skill_id)
                    if skill is None:
                        raise AppError(404, "Skill does not exist.")
                    return self._json(200, skill)
                if method == "PUT":
                    existing = self.container.memory_service.get_skill(skill_id)
                    if existing is None:
                        raise AppError(404, "Skill does not exist.")
                    payload = self._parse_json(body)
                    self._require_fields(payload, "name", "description")
                    skill = self.container.memory_service.update_skill(
                        skill_id,
                        name=str(payload["name"]),
                        description=str(payload["description"]),
                        skill_markdown=self._optional_str(payload.get("skill_markdown")),
                        prompt_template=self._optional_str(payload.get("prompt_template")),
                        workflow=payload.get("workflow"),
                        tools=self._string_list(payload.get("tools")),
                        topics=self._string_list(payload.get("topics")),
                        metadata=self._object_dict(payload.get("metadata"), field_name="metadata"),
                        status=self._optional_str(payload.get("status")),
                        version=self._optional_str(payload.get("version")),
                        assets=self._skill_assets(payload.get("assets")),
                        folder_name=self._optional_str(payload.get("folder_name")),
                        source_kind=self._optional_str(payload.get("source_kind")),
                    )
                    return self._json(200, skill)
                if method == "DELETE":
                    existing = self.container.memory_service.get_skill(skill_id)
                    if existing is None:
                        raise AppError(404, "Skill does not exist.")
                    return self._json(200, self.container.memory_service.delete_skill(skill_id))
            if method == "GET" and path == "/api/rag/documents":
                limit = self._parse_int(query.get("limit"), default=50, minimum=1, maximum=200)
                offset = self._parse_int(query.get("offset"), default=0, minimum=0, maximum=10000)
                items = self.container.memory_service.list_rag_documents(limit=limit, offset=offset)
                return self._json(200, {"items": items, "count": len(items), "limit": limit, "offset": offset})
            if method == "POST" and path == "/api/rag/documents":
                payload = self._parse_json(body)
                self._require_fields(payload, "title", "text")
                document = self.container.memory_service.add_rag_document(
                    str(payload["title"]),
                    str(payload["text"]),
                    metadata=self._rag_metadata(payload),
                )
                return self._json(200, document)
            if method == "GET" and path == "/api/rag/search":
                query_text = str(query.get("query") or "").strip()
                if not query_text:
                    raise AppError(400, "query is required.")
                limit = self._parse_int(query.get("limit"), default=10, minimum=1, maximum=100)
                items = self.container.memory_service.search_rag_documents(query_text, limit=limit)
                return self._json(200, {"items": items, "query": query_text, "count": len(items)})
            if path.startswith("/api/rag/documents/"):
                document_id = path.rsplit("/", 1)[-1]
                if method == "GET":
                    document = self.container.memory_service.get_rag_document(document_id)
                    if document is None:
                        raise AppError(404, "RAG document does not exist.")
                    return self._json(200, document)
                if method == "PUT":
                    existing = self.container.memory_service.get_rag_document(document_id)
                    if existing is None:
                        raise AppError(404, "RAG document does not exist.")
                    payload = self._parse_json(body)
                    self._require_fields(payload, "title", "text")
                    document = self.container.memory_service.update_rag_document(
                        document_id,
                        title=str(payload["title"]),
                        text=str(payload["text"]),
                        metadata=self._rag_metadata(payload),
                    )
                    return self._json(200, document)
                if method == "DELETE":
                    existing = self.container.memory_service.get_rag_document(document_id)
                    if existing is None:
                        raise AppError(404, "RAG document does not exist.")
                    return self._json(200, self.container.memory_service.delete_rag_document(document_id))
            if method == "GET" and path == "/api/sessions":
                limit = self._parse_int(query.get("limit"), default=20, minimum=1, maximum=100)
                return self._json(200, {"items": self.container.repository.list_sessions(limit=limit)})
            if method == "GET" and path.startswith("/api/sessions/"):
                session_id = path.rsplit("/", 1)[-1]
                bundle = self.container.repository.get_session_bundle(session_id)
                if bundle is None:
                    raise AppError(404, "Session does not exist.")
                return self._json(200, bundle)
            if method == "GET" and path == "/api/knowledge":
                limit = self._parse_int(query.get("limit"), default=50, minimum=1, maximum=200)
                offset = self._parse_int(query.get("offset"), default=0, minimum=0, maximum=10000)
                items = self.container.memory_service.list_rag_documents(limit=limit, offset=offset)
                return self._json(200, {"items": items, "count": len(items), "limit": limit, "offset": offset})
            if method == "POST" and path == "/api/knowledge":
                payload = self._parse_json(body)
                self._require_fields(payload, "title", "text")
                document = self.container.memory_service.add_rag_document(
                    str(payload["title"]),
                    str(payload["text"]),
                    metadata=self._rag_metadata(payload),
                )
                return self._json(200, document)
            if method == "GET" and path == "/api/knowledge/search":
                query_text = str(query.get("query") or "").strip()
                if not query_text:
                    raise AppError(400, "query is required.")
                limit = self._parse_int(query.get("limit"), default=10, minimum=1, maximum=100)
                items = self.container.memory_service.search_rag_documents(query_text, limit=limit)
                return self._json(200, {"items": items, "query": query_text, "count": len(items)})
            if method == "GET" and path.startswith("/api/knowledge/"):
                document_id = path.rsplit("/", 1)[-1]
                document = self.container.memory_service.get_rag_document(document_id)
                if document is None:
                    raise AppError(404, "Knowledge document does not exist.")
                return self._json(200, document)
            if method == "POST" and path == "/api/collaborations/run":
                payload = self._parse_json(body)
                self._require_fields(payload, "prompt", "agent_ids")
                agent_ids = payload.get("agent_ids")
                if not isinstance(agent_ids, list) or not agent_ids:
                    raise AppError(400, "agent_ids must be a non-empty list.")
                rounds = self._parse_int(payload.get("rounds"), default=1, minimum=1, maximum=5)
                bundle = self.container.collaboration_service.run(
                    prompt=str(payload["prompt"]),
                    agent_ids=[str(item) for item in agent_ids],
                    lead_agent_id=str(payload["lead_agent_id"]) if payload.get("lead_agent_id") else None,
                    rounds=rounds,
                    title=str(payload["title"]) if payload.get("title") else None,
                )
                return self._json(200, bundle)
            raise AppError(404, "Not found.")
        except ProviderRequestError as exc:
            return self._json(400, {"detail": str(exc)})
        except AppError as exc:
            return self._json(exc.status, {"detail": exc.detail})
        except ValueError as exc:
            return self._json(400, {"detail": str(exc)})
        except Exception as exc:
            LOGGER.exception("Unhandled application error for %s %s", method, path)
            return self._json(500, {"detail": str(exc)})

    def _parse_json(self, body: bytes) -> dict[str, Any]:
        if not body:
            return {}
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise AppError(400, "Request body must be valid JSON.") from exc
        if not isinstance(payload, dict):
            raise AppError(400, "JSON body must be an object.")
        return payload

    def _require_fields(self, payload: dict[str, Any], *fields: str) -> None:
        missing = [field for field in fields if field not in payload or payload[field] in (None, "", [])]
        if missing:
            raise AppError(400, f"Missing required fields: {', '.join(missing)}")

    def _parse_int(self, value: Any, *, default: int, minimum: int, maximum: int) -> int:
        if value in (None, ""):
            return default
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise AppError(400, "Numeric parameter is invalid.") from exc
        return max(minimum, min(maximum, parsed))

    def _parse_bool(self, value: Any) -> bool:
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

    def _optional_str(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _string_list(self, value: Any) -> list[str] | None:
        if value in (None, ""):
            return None
        if isinstance(value, list):
            items = [str(item).strip() for item in value if str(item).strip()]
            return items or None
        text = str(value).strip()
        if not text:
            return None
        items = [item.strip() for item in text.split(",") if item.strip()]
        return items or None

    def _rag_metadata(self, payload: dict[str, Any]) -> dict[str, Any]:
        metadata = self._object_dict(payload.get("metadata"), field_name="metadata")
        source_name = self._optional_str(payload.get("source_name"))
        if source_name:
            metadata["source_name"] = source_name
        return metadata

    def _object_dict(self, value: Any, *, field_name: str) -> dict[str, Any]:
        if value in (None, ""):
            return {}
        if not isinstance(value, dict):
            raise AppError(400, f"{field_name} must be an object.")
        return dict(value)

    def _skill_assets(self, value: Any) -> list[dict[str, Any]] | None:
        if value in (None, ""):
            return None
        if not isinstance(value, list):
            raise AppError(400, "assets must be a list.")
        assets: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                raise AppError(400, "Each asset must be an object.")
            assets.append(dict(item))
        return assets or None

    def _provider_filters_from_query(self, query: dict[str, Any]) -> dict[str, str]:
        return {
            "name": str(query.get("name") or "").strip(),
            "provider_type": str(query.get("provider_type") or "").strip(),
            "model": str(query.get("model") or "").strip(),
        }

    def _agent_filters_from_query(self, query: dict[str, Any]) -> dict[str, str]:
        return {
            "name": str(query.get("name") or "").strip(),
            "role": str(query.get("role") or "").strip(),
            "provider_id": str(query.get("provider_id") or "").strip(),
            "model": str(query.get("model") or "").strip(),
        }

    def _resolve_provider_test_config(self, config: dict[str, Any]) -> dict[str, Any]:
        provider = dict(config)
        provider_id = provider.get("id")
        if provider_id:
            stored = self.container.repository.get_provider(str(provider_id), include_secret=True)
            if stored is not None:
                clear_api_key = bool(provider.get("clear_api_key"))
                incoming_api_key = provider.get("api_key")
                merged = dict(stored)
                merged.update(provider)
                if clear_api_key:
                    merged["api_key"] = None
                elif incoming_api_key not in (None, ""):
                    merged["api_key"] = incoming_api_key
                else:
                    merged["api_key"] = stored.get("api_key")
                provider = merged
        provider.pop("clear_api_key", None)
        return provider

    def _render_provider_dependency_message(self, counts: dict[str, int]) -> str:
        parts: list[str] = []
        if counts.get("agent_count"):
            parts.append(f"{counts['agent_count']} agent records")
        if counts.get("message_count"):
            parts.append(f"{counts['message_count']} collaboration messages")
        detail = " and ".join(parts) if parts else "existing dependencies"
        return f"Provider cannot be deleted because it is referenced by {detail}."

    def _render_agent_dependency_message(self, counts: dict[str, int]) -> str:
        parts: list[str] = []
        if counts.get("lead_session_count"):
            parts.append(f"{counts['lead_session_count']} lead sessions")
        if counts.get("participant_count"):
            parts.append(f"{counts['participant_count']} collaboration participants")
        if counts.get("message_count"):
            parts.append(f"{counts['message_count']} collaboration messages")
        detail = " and ".join(parts) if parts else "existing dependencies"
        return f"Agent cannot be deleted because it is referenced by {detail}."

    def _json(self, status: int, payload: dict[str, Any]) -> AppResponse:
        return AppResponse(status=status, body=json.dumps(payload, ensure_ascii=False).encode("utf-8"), content_type="application/json; charset=utf-8")

    def _empty(self, status: int) -> AppResponse:
        return AppResponse(status=status, body=b"", content_type="text/plain; charset=utf-8")

    def _serve_static(self, path: str) -> AppResponse:
        relative = path.removeprefix("/static/")
        target = (self.settings.static_dir / relative).resolve()
        if not str(target).startswith(str(self.settings.static_dir.resolve())) or not target.exists():
            raise AppError(404, "Static asset not found.")
        content_type = {
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".html": "text/html; charset=utf-8",
        }.get(target.suffix, "application/octet-stream")
        return self._file_response(target, content_type)

    def _file_response(self, path: Path, content_type: str) -> AppResponse:
        if not path.exists():
            raise AppError(404, "File not found.")
        return AppResponse(status=200, body=path.read_bytes(), content_type=content_type)


class _RequestHandler(BaseHTTPRequestHandler):
    def _dispatch(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        response = self.server.application.handle(self.command, self.path, body)  # type: ignore[attr-defined]
        if response.status >= 400:
            LOGGER.warning("%s %s -> %s", self.command, self.path, response.status)
        self.send_response(response.status)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(response.body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        for key, value in response.headers.items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD" and response.body:
            self.wfile.write(response.body)

    def do_GET(self) -> None:
        self._dispatch()

    def do_POST(self) -> None:
        self._dispatch()

    def do_PUT(self) -> None:
        self._dispatch()

    def do_DELETE(self) -> None:
        self._dispatch()

    def do_OPTIONS(self) -> None:
        self._dispatch()

    def log_message(self, format: str, *args: Any) -> None:
        return None


class AITeamsHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], application: WebApplication):
        super().__init__(server_address, _RequestHandler)
        self.application = application


def create_app(settings: AppSettings | None = None) -> WebApplication:
    return WebApplication(settings)


app = create_app()


def run() -> None:
    host = "127.0.0.1"
    port = 8000
    server = AITeamsHTTPServer((host, port), app)
    LOGGER.info("AITeams server starting")
    LOGGER.info("Listen: http://%s:%s", host, port)
    LOGGER.info("Static dir: %s", app.settings.static_dir)
    LOGGER.info("Platform DB: %s", app.settings.platform_db_path)
    LOGGER.info("AIMemory root: %s", app.settings.aimemory_root)
    LOGGER.info("AIMemory SQLite: %s", app.settings.aimemory_sqlite_path)
    LOGGER.info("Default user: %s", app.settings.default_user_id)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("AITeams server interrupted by user")
    finally:
        LOGGER.info("AITeams server shutting down")
        server.server_close()
        app.close()


if __name__ == "__main__":
    run()
