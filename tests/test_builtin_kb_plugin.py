from __future__ import annotations

import unittest

from aiteams.plugins.builtin import KnowledgeBaseQueryBuiltinPlugin


class _FakeKnowledgeBaseService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def query(
        self,
        *,
        query: str,
        knowledge_base_ids: list[str] | None = None,
        knowledge_base_keys: list[str] | None = None,
        limit: int = 8,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "query": query,
                "knowledge_base_ids": list(knowledge_base_ids or []),
                "knowledge_base_keys": list(knowledge_base_keys or []),
                "limit": limit,
            }
        )
        return {
            "query": query,
            "count": 1,
            "items": [{"id": "doc-1", "title": "KB Doc"}],
            "engine": {"backend": "llamaindex_query_engine", "response_mode": "no_text"},
        }


class KnowledgeBaseQueryBuiltinPluginTests(unittest.TestCase):
    def test_invoke_prefers_query_engine_service_when_available(self) -> None:
        service = _FakeKnowledgeBaseService()
        plugin = KnowledgeBaseQueryBuiltinPlugin(store=object(), knowledge_bases=service)

        result = plugin.invoke(
            action="retrieve",
            payload={"query": "release notes", "limit": 3},
            context={"knowledge_bases": [{"id": "kb-1", "key": "release", "name": "Release KB"}]},
            plugin_ref={"id": "builtin:kb.retrieve", "key": "kb.retrieve"},
        )

        self.assertEqual(
            service.calls,
            [
                {
                    "query": "release notes",
                    "knowledge_base_ids": ["kb-1"],
                    "knowledge_base_keys": ["release"],
                    "limit": 3,
                }
            ],
        )
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["engine"], {"backend": "llamaindex_query_engine", "response_mode": "no_text"})
        self.assertEqual(
            result["knowledge_bases"],
            [{"id": "kb-1", "key": "release", "name": "Release KB"}],
        )


if __name__ == "__main__":
    unittest.main()
