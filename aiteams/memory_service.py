from __future__ import annotations

import base64
import binascii
import mimetypes
from pathlib import PurePosixPath
from typing import Any

from aimemory import AIMemory, AIMemoryConfig, EmbeddingLiteConfig
from aimemory.core.utils import json_dumps, json_loads, utcnow_iso

from aiteams.config import AppSettings
from aiteams.skill_utils import asset_category_from_path, is_text_asset, normalize_asset_path, resolve_skill_identity, summarize_asset_manifest


class AgentMemoryService:
    def __init__(self, settings: AppSettings):
        config = AIMemoryConfig(
            root_dir=settings.aimemory_root,
            sqlite_path=settings.aimemory_sqlite_path,
            default_user_id=settings.default_user_id,
            index_backend="sqlite",
            graph_backend="sqlite",
            embeddings=EmbeddingLiteConfig(provider="hash", model="lexical-hash", dimensions=384),
        )
        self._memory = AIMemory(config)
        self._default_user_id = settings.default_user_id
        self._namespace_key = "aiteams"
        self._rag_namespace_key = f"{self._namespace_key}-knowledge"
        self._skill_namespace_key = f"{self._namespace_key}-skills"
        self._skill_owner_agent_id = "platform-skill-library"
        self._skill_asset_object_type = "skill-assets"

    def close(self) -> None:
        self._memory.close()

    def _session_id(self, platform_session_id: str, agent_id: str) -> str:
        return f"collab__{platform_session_id}__{agent_id}"

    def ensure_agent_session(self, agent: dict[str, Any], platform_session_id: str) -> str:
        session_id = self._session_id(platform_session_id, str(agent["id"]))
        existing = self._memory.get_session(session_id)
        if existing is None:
            self._memory.create_session(
                user_id=self._default_user_id,
                session_id=session_id,
                owner_agent_id=str(agent["id"]),
                subject_type="human",
                subject_id=self._default_user_id,
                interaction_type="human_agent",
                namespace_key=self._namespace_key,
                title=f"{agent['name']} @ {platform_session_id}",
            )
        return session_id

    def remember_brief(self, agent: dict[str, Any], platform_session_id: str, brief: str, *, metadata: dict[str, Any] | None = None) -> None:
        session_id = self.ensure_agent_session(agent, platform_session_id)
        self._memory.append_turn(
            session_id=session_id,
            role="user",
            content=brief,
            metadata=metadata or {},
            speaker_type="human",
            speaker_external_id=self._default_user_id,
            target_type="agent",
            target_external_id=str(agent["id"]),
            auto_capture=True,
        )

    def remember_response(self, agent: dict[str, Any], platform_session_id: str, content: str, *, metadata: dict[str, Any] | None = None) -> None:
        session_id = self.ensure_agent_session(agent, platform_session_id)
        self._memory.append_turn(
            session_id=session_id,
            role="assistant",
            content=content,
            metadata=metadata or {},
            speaker_type="agent",
            speaker_external_id=str(agent["id"]),
            target_type="human",
            target_external_id=self._default_user_id,
            auto_capture=True,
        )
        self._memory.remember_long_term(
            content,
            user_id=self._default_user_id,
            owner_agent_id=str(agent["id"]),
            session_id=session_id,
            namespace_key=self._namespace_key,
            metadata=metadata or {},
            source="platform_collaboration",
        )

    def recall(self, agent: dict[str, Any], platform_session_id: str, query: str, *, limit: int = 5) -> list[dict[str, Any]]:
        session_id = self.ensure_agent_session(agent, platform_session_id)
        result = self._memory.search(
            query,
            user_id=self._default_user_id,
            owner_agent_id=str(agent["id"]),
            session_id=session_id,
            scope="all",
            top_k=limit,
            namespace_key=self._namespace_key,
        )
        return list(result.get("results", []))

    def list_agent_memories(self, agent_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        result = self._memory.get_all(
            user_id=self._default_user_id,
            owner_agent_id=agent_id,
            namespace_key=self._namespace_key,
            scope="all",
            limit=limit,
        )
        return list(result.get("results", []))

    def search_agent_memories(self, agent_id: str, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        result = self._memory.search(
            query,
            user_id=self._default_user_id,
            owner_agent_id=agent_id,
            namespace_key=self._namespace_key,
            scope="all",
            top_k=limit,
        )
        return list(result.get("results", []))

    def add_rag_document(self, title: str, text: str, *, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._memory.api.knowledge.add(
            title=title,
            text=text,
            user_id=self._default_user_id,
            namespace_key=self._rag_namespace_key,
            global_scope=True,
            metadata=metadata or {},
        )

    def list_rag_documents(self, *, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        result = self._memory.api.knowledge.list(
            user_id=self._default_user_id,
            namespace_key=self._rag_namespace_key,
            include_global=True,
            limit=limit,
            offset=offset,
        )
        return list(result.get("results", []))

    def search_rag_documents(self, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        result = self._memory.api.knowledge.search(
            query,
            user_id=self._default_user_id,
            namespace_key=self._rag_namespace_key,
            include_global=True,
            limit=limit,
        )
        return list(result.get("results", []))

    def get_rag_document(self, document_id: str) -> dict[str, Any] | None:
        try:
            return self._memory.api.knowledge.get(document_id)
        except ValueError:
            return None

    def update_rag_document(
        self,
        document_id: str,
        *,
        title: str | None = None,
        text: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "user_id": self._default_user_id,
            "namespace_key": self._rag_namespace_key,
            "global_scope": True,
        }
        if title is not None:
            payload["title"] = title
        if text is not None:
            payload["text"] = text
        if metadata is not None:
            payload["metadata"] = metadata
        return self._memory.api.knowledge.update(document_id, **payload)

    def delete_rag_document(self, document_id: str) -> dict[str, Any]:
        return self._memory.api.knowledge.delete(document_id)

    def _skill_scope(self) -> dict[str, Any]:
        return self._memory._resolve_scope(
            user_id=self._default_user_id,
            owner_agent_id=self._skill_owner_agent_id,
            namespace_key=self._skill_namespace_key,
        )

    def _latest_skill_version(self, skill: dict[str, Any] | None) -> dict[str, Any] | None:
        versions = list(skill.get("versions") or []) if skill else []
        if not versions:
            return None
        versions.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return versions[0]

    def _decorate_skill_summary(self, item: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(item.get("metadata") or {})
        item["metadata"] = metadata
        item["asset_summary"] = metadata.get("asset_summary") or {"total": 0}
        item["source_kind"] = metadata.get("source_kind")
        item["folder_name"] = metadata.get("folder_name")
        latest_version = item.get("latest_version")
        if isinstance(latest_version, dict):
            latest_version["asset_count"] = int((latest_version.get("metadata") or {}).get("asset_summary", {}).get("total", 0))
        return item

    def _decorate_skill(self, skill: dict[str, Any]) -> dict[str, Any]:
        skill["metadata"] = dict(skill.get("metadata") or {})
        versions = list(skill.get("versions") or [])
        versions.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        for version in versions:
            version["metadata"] = dict(version.get("metadata") or {})
            version["assets"] = list(version["metadata"].get("assets") or [])
            version["asset_summary"] = version["metadata"].get("asset_summary") or summarize_asset_manifest(version["assets"])
        skill["versions"] = versions
        latest_version = versions[0] if versions else None
        skill["skill_markdown"] = latest_version.get("prompt_template") if latest_version else None
        skill["assets"] = list(latest_version.get("assets") or []) if latest_version else []
        skill["asset_summary"] = skill["metadata"].get("asset_summary") or summarize_asset_manifest(skill["assets"])
        skill["source_kind"] = skill["metadata"].get("source_kind")
        skill["folder_name"] = skill["metadata"].get("folder_name")
        return skill

    def _decode_asset_bytes(self, asset: dict[str, Any]) -> bytes:
        encoded = asset.get("content_base64")
        if encoded in (None, ""):
            text = asset.get("content_text")
            if isinstance(text, str):
                return text.encode("utf-8")
            raise ValueError("Asset content is required.")
        try:
            return base64.b64decode(str(encoded), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("Asset content_base64 is invalid.") from exc

    def _normalize_asset_payload(self, asset: dict[str, Any]) -> dict[str, Any]:
        relative_path = normalize_asset_path(str(asset.get("relative_path") or asset.get("path") or asset.get("name") or ""))
        content = self._decode_asset_bytes(asset)
        category = asset_category_from_path(relative_path, fallback=str(asset.get("category") or "").strip() or None)
        mime_type = (
            str(asset.get("mime_type") or "").strip()
            or mimetypes.guess_type(relative_path)[0]
            or ("text/plain" if is_text_asset(relative_path) else "application/octet-stream")
        )
        return {
            "relative_path": relative_path,
            "file_name": PurePosixPath(relative_path).name,
            "category": category,
            "mime_type": mime_type,
            "content": content,
            "is_text": is_text_asset(relative_path, mime_type),
        }

    def _store_skill_assets(self, skill_id: str, version_id: str, assets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not assets:
            return []
        scope = self._skill_scope()
        prefix = f"{self._memory._object_store_prefix(scope, 'skill')}/assets/{skill_id}"
        manifest: list[dict[str, Any]] = []
        for raw_asset in assets:
            asset = self._normalize_asset_payload(raw_asset)
            suffix = PurePosixPath(asset["relative_path"]).suffix or ".bin"
            stored = self._memory.object_store.put_bytes(
                asset["content"],
                object_type=self._skill_asset_object_type,
                suffix=suffix,
                prefix=prefix,
            )
            object_row = self._memory._persist_object(
                stored,
                mime_type=asset["mime_type"],
                metadata={
                    "skill_id": skill_id,
                    "skill_version_id": version_id,
                    "relative_path": asset["relative_path"],
                    "category": asset["category"],
                    "namespace_key": self._skill_namespace_key,
                    "owner_agent_id": self._skill_owner_agent_id,
                },
            )
            manifest.append(
                {
                    "object_id": object_row["id"],
                    "object_key": object_row["object_key"],
                    "relative_path": asset["relative_path"],
                    "file_name": asset["file_name"],
                    "category": asset["category"],
                    "mime_type": asset["mime_type"],
                    "size_bytes": object_row["size_bytes"],
                    "checksum": object_row["checksum"],
                    "is_text": asset["is_text"],
                }
            )
        manifest.sort(key=lambda item: str(item["relative_path"]))
        return manifest

    def _merge_asset_manifests(self, base_assets: list[dict[str, Any]], next_assets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for item in base_assets:
            relative_path = str(item.get("relative_path") or "")
            if relative_path:
                merged[relative_path] = dict(item)
        for item in next_assets:
            relative_path = str(item.get("relative_path") or "")
            if relative_path:
                merged[relative_path] = dict(item)
        return [merged[key] for key in sorted(merged)]

    def _update_skill_metadata(self, skill_id: str, *, assets: list[dict[str, Any]], folder_name: str | None, source_kind: str | None) -> None:
        row = self._memory.db.fetch_one("SELECT metadata FROM skills WHERE id = ?", (skill_id,))
        metadata = dict(json_loads(row.get("metadata"), {}) if row else {})
        metadata["asset_summary"] = summarize_asset_manifest(assets)
        if folder_name:
            metadata["folder_name"] = folder_name
        if source_kind:
            metadata["source_kind"] = source_kind
        self._memory.db.execute(
            "UPDATE skills SET metadata = ?, updated_at = ? WHERE id = ?",
            (json_dumps(metadata), utcnow_iso(), skill_id),
        )

    def _sync_skill_assets(
        self,
        skill_id: str,
        *,
        assets: list[dict[str, Any]] | None,
        inherited_assets: list[dict[str, Any]] | None = None,
        folder_name: str | None = None,
        source_kind: str | None = None,
    ) -> None:
        skill = self._memory.api.skill.get(skill_id)
        latest_version = self._latest_skill_version(skill)
        if latest_version is None:
            return
        base_assets = list(inherited_assets or latest_version.get("metadata", {}).get("assets") or [])
        stored_assets = self._store_skill_assets(skill_id, latest_version["id"], list(assets or []))
        merged_assets = self._merge_asset_manifests(base_assets, stored_assets)
        version_metadata = dict(latest_version.get("metadata") or {})
        version_metadata["assets"] = merged_assets
        version_metadata["asset_summary"] = summarize_asset_manifest(merged_assets)
        if folder_name:
            version_metadata["folder_name"] = folder_name
        if source_kind:
            version_metadata["source_kind"] = source_kind
        self._memory.db.execute(
            "UPDATE skill_versions SET metadata = ? WHERE id = ?",
            (json_dumps(version_metadata), latest_version["id"]),
        )
        self._update_skill_metadata(skill_id, assets=merged_assets, folder_name=folder_name, source_kind=source_kind)

    def _delete_object_if_unreferenced(self, object_id: str) -> None:
        if not object_id:
            return
        remaining_refs = 0
        for table in ("skill_versions", "document_versions", "archive_units"):
            row = self._memory.db.fetch_one(f"SELECT COUNT(*) AS count FROM {table} WHERE object_id = ?", (object_id,))
            remaining_refs += int((row or {}).get("count", 0) or 0)
        if remaining_refs:
            return
        object_row = self._memory.db.fetch_one("SELECT * FROM objects WHERE id = ?", (object_id,))
        if object_row is None:
            return
        object_key = str(object_row.get("object_key") or "")
        if object_key:
            self._memory.object_store.delete(object_key)
        self._memory.db.execute("DELETE FROM objects WHERE id = ?", (object_id,))

    def import_skill(
        self,
        *,
        name: str | None,
        description: str | None,
        skill_markdown: str,
        assets: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
        status: str = "draft",
        version: str | None = None,
        folder_name: str | None = None,
        source_kind: str = "folder-import",
    ) -> dict[str, Any]:
        resolved_name, resolved_description, normalized_markdown = resolve_skill_identity(
            name=name,
            description=description,
            skill_markdown=skill_markdown,
            folder_name=folder_name,
        )
        return self.add_skill(
            resolved_name,
            resolved_description,
            prompt_template=normalized_markdown,
            skill_markdown=normalized_markdown,
            workflow=None,
            tools=None,
            topics=None,
            metadata=metadata,
            status=status,
            version=version,
            assets=assets,
            folder_name=folder_name,
            source_kind=source_kind,
        )

    def add_skill(
        self,
        name: str,
        description: str,
        *,
        skill_markdown: str | None = None,
        prompt_template: str | None = None,
        workflow: dict[str, Any] | str | None = None,
        tools: list[str] | None = None,
        topics: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        status: str = "draft",
        version: str | None = None,
        assets: list[dict[str, Any]] | None = None,
        folder_name: str | None = None,
        source_kind: str | None = None,
    ) -> dict[str, Any]:
        metadata_payload = dict(metadata or {})
        if topics:
            metadata_payload["topics"] = list(topics)
        if folder_name:
            metadata_payload["folder_name"] = folder_name
        if source_kind:
            metadata_payload["source_kind"] = source_kind
        effective_markdown = skill_markdown if skill_markdown is not None else prompt_template
        if effective_markdown is not None:
            name, description, effective_markdown = resolve_skill_identity(
                name=name,
                description=description,
                skill_markdown=effective_markdown,
                folder_name=folder_name,
            )
            metadata_payload.setdefault("skill_format", "codex-skill")
        payload: dict[str, Any] = {
            "name": name,
            "description": description,
            "owner_agent_id": self._skill_owner_agent_id,
            "user_id": self._default_user_id,
            "namespace_key": self._skill_namespace_key,
            "prompt_template": effective_markdown,
            "workflow": workflow,
            "tools": list(tools or []),
            "topics": list(topics or []),
            "metadata": metadata_payload,
        }
        if version is not None:
            payload["version"] = version
        skill = self._memory.api.skill.add(
            **payload,
        )
        if effective_markdown is not None or assets or folder_name or source_kind:
            self._sync_skill_assets(
                skill["id"],
                assets=assets,
                inherited_assets=[],
                folder_name=folder_name,
                source_kind=source_kind,
            )
        if status and status != "active":
            skill = self._memory.api.skill.update(skill["id"], status=status)
        refreshed = self.get_skill(skill["id"])
        assert refreshed is not None
        return refreshed

    def list_skills(self, *, limit: int = 50, offset: int = 0, status: str | None = None) -> list[dict[str, Any]]:
        result = self._memory.api.skill.list(
            owner_agent_id=self._skill_owner_agent_id,
            namespace_key=self._skill_namespace_key,
            status=status,
            limit=limit,
            offset=offset,
        )
        return [self._decorate_skill_summary(item) for item in result.get("results", [])]

    def search_skills(self, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        result = self._memory.api.skill.search(
            query,
            owner_agent_id=self._skill_owner_agent_id,
            namespace_key=self._skill_namespace_key,
            limit=limit,
        )
        deduped: dict[str, dict[str, Any]] = {}
        for item in result.get("results", []):
            skill_id = str(item.get("skill_id") or item.get("id") or "")
            if not skill_id or skill_id in deduped:
                continue
            deduped[skill_id] = {
                "skill_id": skill_id,
                "name": item.get("name"),
                "description": item.get("description"),
                "version": item.get("version"),
                "text": item.get("text"),
                "status": item.get("status"),
                "score": item.get("score"),
                "updated_at": item.get("updated_at"),
                "metadata": item.get("metadata") or {},
            }
        hits = list(deduped.values())
        if hits:
            return [self._decorate_skill_summary(item) for item in hits]

        needle = query.strip().lower()
        fallback: list[dict[str, Any]] = []
        for item in self.list_skills(limit=200, offset=0, status=None):
            haystack = " ".join(
                [
                    str(item.get("name") or ""),
                    str(item.get("description") or ""),
                    str(item.get("status") or ""),
                ]
            ).lower()
            if needle and needle not in haystack:
                continue
            fallback.append(
                {
                    "skill_id": item.get("id"),
                    "name": item.get("name"),
                    "description": item.get("description"),
                    "version": item.get("latest_version", {}).get("version"),
                    "text": item.get("description"),
                    "status": item.get("status"),
                    "score": None,
                    "updated_at": item.get("updated_at"),
                    "metadata": item.get("metadata") or {},
                }
            )
            if len(fallback) >= limit:
                break
        return [self._decorate_skill_summary(item) for item in fallback]

    def get_skill(self, skill_id: str) -> dict[str, Any] | None:
        try:
            skill = self._memory.api.skill.get(skill_id)
        except ValueError:
            return None
        return self._decorate_skill(skill)

    def update_skill(
        self,
        skill_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        skill_markdown: str | None = None,
        prompt_template: str | None = None,
        workflow: dict[str, Any] | str | None = None,
        tools: list[str] | None = None,
        topics: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        status: str | None = None,
        version: str | None = None,
        assets: list[dict[str, Any]] | None = None,
        folder_name: str | None = None,
        source_kind: str | None = None,
    ) -> dict[str, Any]:
        existing = self.get_skill(skill_id)
        if existing is None:
            raise ValueError(f"Skill `{skill_id}` does not exist.")
        resolved_name = name or str(existing.get("name") or "")
        resolved_description = description or str(existing.get("description") or "")
        previous_assets = list((self._latest_skill_version(existing) or {}).get("assets") or [])
        metadata_payload = dict(metadata or {})
        if topics is not None:
            metadata_payload["topics"] = list(topics)
        if folder_name:
            metadata_payload["folder_name"] = folder_name
        elif existing.get("folder_name"):
            metadata_payload["folder_name"] = existing["folder_name"]
        if source_kind:
            metadata_payload["source_kind"] = source_kind
        elif existing.get("source_kind"):
            metadata_payload["source_kind"] = existing["source_kind"]
        effective_markdown = skill_markdown if skill_markdown is not None else prompt_template
        if effective_markdown is not None:
            resolved_name, resolved_description, effective_markdown = resolve_skill_identity(
                name=resolved_name,
                description=resolved_description,
                skill_markdown=effective_markdown,
                folder_name=folder_name or existing.get("folder_name"),
            )
            latest_version = self._latest_skill_version(existing)
            if latest_version and str(latest_version.get("prompt_template") or "") == effective_markdown:
                effective_markdown = None
            metadata_payload.setdefault("skill_format", existing.get("metadata", {}).get("skill_format") or "codex-skill")
        payload: dict[str, Any] = {}
        if name is not None or effective_markdown is not None:
            payload["name"] = resolved_name
        if description is not None or effective_markdown is not None:
            payload["description"] = resolved_description
        if effective_markdown is not None:
            payload["prompt_template"] = effective_markdown
        if workflow is not None:
            payload["workflow"] = workflow
        if tools is not None:
            payload["tools"] = list(tools)
        if topics is not None:
            payload["topics"] = list(topics)
        if metadata is not None or topics is not None or folder_name or source_kind:
            payload["metadata"] = metadata_payload
        if status is not None:
            payload["status"] = status
        if version is not None:
            payload["version"] = version
        skill = self._memory.api.skill.update(skill_id, **payload)
        if effective_markdown is not None or assets or folder_name or source_kind:
            self._sync_skill_assets(
                skill_id,
                assets=assets,
                inherited_assets=previous_assets,
                folder_name=folder_name or existing.get("folder_name"),
                source_kind=source_kind or existing.get("source_kind"),
            )
        refreshed = self.get_skill(skill_id)
        assert refreshed is not None
        return refreshed

    def delete_skill(self, skill_id: str) -> dict[str, Any]:
        existing = self.get_skill(skill_id)
        if existing is None:
            raise ValueError(f"Skill `{skill_id}` does not exist.")
        object_ids = {
            str(version.get("object_id") or "")
            for version in existing.get("versions", [])
            if version.get("object_id")
        }
        for version in existing.get("versions", []):
            for asset in version.get("assets", []):
                object_id = str(asset.get("object_id") or "")
                if object_id:
                    object_ids.add(object_id)
        deleted = self._memory.api.skill.delete(skill_id)
        for object_id in sorted(object_ids):
            self._delete_object_if_unreferenced(object_id)
        return deleted

    def add_knowledge_document(self, title: str, text: str, *, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.add_rag_document(title, text, metadata=metadata)

    def list_knowledge_documents(self, *, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        return self.list_rag_documents(limit=limit, offset=offset)

    def search_knowledge_documents(self, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        return self.search_rag_documents(query, limit=limit)

    def get_knowledge_document(self, document_id: str) -> dict[str, Any] | None:
        return self.get_rag_document(document_id)
