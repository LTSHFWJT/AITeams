from __future__ import annotations

from typing import Any


class KnowledgeBaseQueryBuiltinPlugin:
    def __init__(
        self,
        *,
        store: Any,
        knowledge_bases: Any | None = None,
    ) -> None:
        self.store = store
        self.knowledge_bases = knowledge_bases

    def invoke(
        self,
        *,
        action: str,
        payload: dict[str, Any],
        context: dict[str, Any],
        plugin_ref: dict[str, Any],
    ) -> dict[str, Any]:
        action_name = str(action or "").strip().lower()
        if action_name != "retrieve":
            raise ValueError(f"Builtin plugin `kb.retrieve` does not support action `{action}`.")
        bindings = self._knowledge_base_bindings(payload=payload, context=context, plugin_ref=plugin_ref)
        knowledge_base_ids = [str(item.get("id") or "").strip() for item in bindings if str(item.get("id") or "").strip()]
        knowledge_base_keys = [str(item.get("key") or "").strip() for item in bindings if str(item.get("key") or "").strip()]
        query = str(payload.get("query") or payload.get("text") or "").strip()
        limit = max(1, int(payload.get("limit", 4) or 4))

        if self.knowledge_bases is not None and hasattr(self.knowledge_bases, "query"):
            result = self.knowledge_bases.query(
                query=query,
                knowledge_base_ids=knowledge_base_ids or None,
                knowledge_base_keys=knowledge_base_keys or None,
                limit=limit,
            )
            return self._normalize_result(result=result, query=query, bindings=bindings)

        if self.knowledge_bases is not None and hasattr(self.knowledge_bases, "search"):
            items = self.knowledge_bases.search(
                query=query,
                knowledge_base_ids=knowledge_base_ids or None,
                knowledge_base_keys=knowledge_base_keys or None,
                limit=limit,
            )
            return {
                "query": query,
                "count": len(items),
                "items": items,
                "knowledge_bases": self._binding_payload(bindings),
                "engine": {
                    "backend": "metadata_search",
                    "mode": "search",
                },
            }

        items = self.store.search_knowledge_documents(
            query=query,
            knowledge_base_ids=knowledge_base_ids or None,
            knowledge_base_keys=knowledge_base_keys or None,
            limit=limit,
        )
        return {
            "query": query,
            "count": len(items),
            "items": items,
            "knowledge_bases": self._binding_payload(bindings),
            "engine": {
                "backend": "metadata_store",
                "mode": "search",
            },
        }

    def _normalize_result(
        self,
        *,
        result: Any,
        query: str,
        bindings: list[dict[str, Any]],
    ) -> dict[str, Any]:
        payload = dict(result or {}) if isinstance(result, dict) else {}
        items = [dict(item) for item in list(payload.get("items") or []) if isinstance(item, dict)]
        payload["query"] = query
        payload["items"] = items
        payload["count"] = int(payload.get("count") or len(items))
        payload.setdefault("knowledge_bases", self._binding_payload(bindings))
        return payload

    def _knowledge_base_bindings(
        self,
        *,
        payload: dict[str, Any],
        context: dict[str, Any],
        plugin_ref: dict[str, Any],
    ) -> list[dict[str, Any]]:
        candidates = list(payload.get("knowledge_bases") or plugin_ref.get("knowledge_bases") or context.get("knowledge_bases") or [])
        bindings: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in candidates:
            if not isinstance(item, dict):
                continue
            binding = {
                "id": str(item.get("id") or "").strip(),
                "key": str(item.get("key") or "").strip(),
                "name": str(item.get("name") or "").strip(),
            }
            identity = binding["id"] or binding["key"]
            if not identity or identity in seen:
                continue
            seen.add(identity)
            bindings.append(binding)
        return bindings

    def _binding_payload(self, bindings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": item.get("id") or None,
                "key": item.get("key") or None,
                "name": item.get("name") or None,
            }
            for item in bindings
        ]
