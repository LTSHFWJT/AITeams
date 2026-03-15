from __future__ import annotations

import base64
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aiteams.app import create_app
from aiteams.config import AppSettings


class AITeamsAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        root = Path(self._tempdir.name)
        data_dir = root / "data"
        aimemory_root = data_dir / "aimemory"
        data_dir.mkdir(parents=True, exist_ok=True)
        aimemory_root.mkdir(parents=True, exist_ok=True)
        settings = AppSettings(
            project_root=root,
            data_dir=data_dir,
            platform_db_path=data_dir / "platform.db",
            aimemory_root=aimemory_root,
            aimemory_sqlite_path=aimemory_root / "aimemory.db",
            static_dir=Path(__file__).resolve().parents[1] / "aiteams" / "static",
        )
        self._app = create_app(settings)

    def tearDown(self) -> None:
        self._app.close()
        self._tempdir.cleanup()

    def _request(self, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else b""
        response = self._app.handle(method, path, body)
        parsed = json.loads(response.body.decode("utf-8")) if response.body else {}
        return response.status, parsed

    def test_collaboration_persists_platform_records_and_memories(self) -> None:
        provider_status, provider = self._request(
            "POST",
            "/api/providers",
            {
                "name": "Mock Provider",
                "provider_type": "mock",
                "base_url": "mock://local",
                "api_key": "",
                "model": "mock-model",
                "extra_headers": {},
                "extra_config": {},
            },
        )
        self.assertEqual(provider_status, 200)
        provider_id = provider["id"]

        planner_status, planner = self._request(
            "POST",
            "/api/agents",
            {
                "name": "Planner",
                "role": "planner",
                "system_prompt": "Design execution plans with milestones.",
                "provider_id": provider_id,
                "temperature": 0.1,
            },
        )
        self.assertEqual(planner_status, 200)
        planner_id = planner["id"]

        reviewer_status, reviewer = self._request(
            "POST",
            "/api/agents",
            {
                "name": "Reviewer",
                "role": "reviewer",
                "system_prompt": "Synthesize specialist outputs into a final answer.",
                "provider_id": provider_id,
                "temperature": 0.1,
            },
        )
        self.assertEqual(reviewer_status, 200)
        reviewer_id = reviewer["id"]

        run_status, payload = self._request(
            "POST",
            "/api/collaborations/run",
            {
                "title": "Launch Plan",
                "prompt": "Create an implementation plan and risk list for a new multi-agent product.",
                "agent_ids": [planner_id, reviewer_id],
                "lead_agent_id": reviewer_id,
                "rounds": 1,
            },
        )
        self.assertEqual(run_status, 200)
        self.assertEqual(payload["session"]["status"], "completed")
        self.assertGreaterEqual(len(payload["messages"]), 3)
        self.assertTrue(payload["session"]["final_summary"])

        memory_status, memories = self._request("GET", f"/api/agents/{planner_id}/memory?query=implementation%20plan&limit=5")
        self.assertEqual(memory_status, 200)
        self.assertGreaterEqual(len(memories["results"]), 1)

    def test_knowledge_endpoints_store_and_search_documents(self) -> None:
        create_status, created = self._request(
            "POST",
            "/api/knowledge",
            {
                "title": "Platform Collaboration Spec",
                "text": "Lead Agent summarizes work, specialists provide analysis, and shared knowledge is stored in the platform knowledge base.",
            },
        )
        self.assertEqual(create_status, 200)
        self.assertTrue(created["id"])

        list_status, listed = self._request("GET", "/api/knowledge?limit=10")
        self.assertEqual(list_status, 200)
        self.assertGreaterEqual(listed["count"], 1)

        search_status, searched = self._request("GET", "/api/knowledge/search?query=Lead%20Agent&limit=5")
        self.assertEqual(search_status, 200)
        self.assertGreaterEqual(searched["count"], 1)

        detail_status, detail = self._request("GET", f"/api/knowledge/{created['id']}")
        self.assertEqual(detail_status, 200)
        self.assertEqual(detail["title"], "Platform Collaboration Spec")

    def test_skill_and_rag_endpoints_support_crud_and_search(self) -> None:
        skill_status, skill = self._request(
            "POST",
            "/api/skills",
            {
                "name": "RAG Planner",
                "description": "Plan retrieval augmented generation workflows.",
                "prompt_template": "Summarize the best retrieval plan.",
                "workflow": {"steps": ["collect", "rank", "compose"]},
                "tools": ["web.search", "rag.lookup"],
                "topics": ["rag", "retrieval"],
                "status": "draft",
                "version": "v1",
            },
        )
        self.assertEqual(skill_status, 200)
        self.assertEqual(skill["name"], "RAG Planner")
        self.assertEqual(skill["status"], "draft")
        skill_id = skill["id"]

        skill_list_status, skill_list = self._request("GET", "/api/skills?limit=10")
        self.assertEqual(skill_list_status, 200)
        self.assertGreaterEqual(skill_list["count"], 1)

        skill_search_status, skill_search = self._request("GET", "/api/skills/search?query=retrieval&limit=5")
        self.assertEqual(skill_search_status, 200)
        self.assertGreaterEqual(skill_search["count"], 1)

        skill_detail_status, skill_detail = self._request("GET", f"/api/skills/{skill_id}")
        self.assertEqual(skill_detail_status, 200)
        self.assertEqual(skill_detail["name"], "RAG Planner")
        self.assertEqual(len(skill_detail.get("versions", [])), 1)

        skill_update_status, updated_skill = self._request(
            "PUT",
            f"/api/skills/{skill_id}",
            {
                "name": "RAG Planner Pro",
                "description": "Plan retrieval and synthesis workflows.",
                "prompt_template": "Produce the final RAG plan.",
                "workflow": "collect -> rerank -> answer",
                "tools": ["rag.lookup", "memory.search"],
                "topics": ["rag", "answering"],
                "status": "active",
                "version": "v2",
            },
        )
        self.assertEqual(skill_update_status, 200)
        self.assertEqual(updated_skill["name"], "RAG Planner Pro")
        self.assertEqual(updated_skill["status"], "active")
        self.assertGreaterEqual(len(updated_skill.get("versions", [])), 2)

        delete_skill_status, deleted_skill = self._request("DELETE", f"/api/skills/{skill_id}")
        self.assertEqual(delete_skill_status, 200)
        self.assertTrue(deleted_skill["deleted"])

        missing_skill_status, missing_skill = self._request("GET", f"/api/skills/{skill_id}")
        self.assertEqual(missing_skill_status, 404)
        self.assertIn("does not exist", missing_skill["detail"])

        rag_create_status, rag_document = self._request(
            "POST",
            "/api/rag/documents",
            {
                "title": "Platform RAG Guide",
                "source_name": "manual",
                "text": "Use SQLite for metadata, aimemory for chunks, and retrieve before answering.",
            },
        )
        self.assertEqual(rag_create_status, 200)
        self.assertEqual(rag_document["title"], "Platform RAG Guide")
        rag_document_id = rag_document["id"]

        rag_list_status, rag_list = self._request("GET", "/api/rag/documents?limit=10")
        self.assertEqual(rag_list_status, 200)
        self.assertGreaterEqual(rag_list["count"], 1)

        rag_search_status, rag_search = self._request("GET", "/api/rag/search?query=retrieve&limit=5")
        self.assertEqual(rag_search_status, 200)
        self.assertGreaterEqual(rag_search["count"], 1)

        rag_detail_status, rag_detail = self._request("GET", f"/api/rag/documents/{rag_document_id}")
        self.assertEqual(rag_detail_status, 200)
        self.assertEqual(rag_detail["title"], "Platform RAG Guide")

        rag_update_status, updated_rag = self._request(
            "PUT",
            f"/api/rag/documents/{rag_document_id}",
            {
                "title": "Platform RAG Guide v2",
                "source_name": "playbook",
                "text": "Use SQLite for metadata, aimemory for chunk storage, and rerank before answering.",
            },
        )
        self.assertEqual(rag_update_status, 200)
        self.assertEqual(updated_rag["title"], "Platform RAG Guide v2")

        delete_rag_status, deleted_rag = self._request("DELETE", f"/api/rag/documents/{rag_document_id}")
        self.assertEqual(delete_rag_status, 200)
        self.assertTrue(deleted_rag["deleted"])

        missing_rag_status, missing_rag = self._request("GET", f"/api/rag/documents/{rag_document_id}")
        self.assertEqual(missing_rag_status, 404)
        self.assertIn("does not exist", missing_rag["detail"])

    def test_skill_import_endpoint_stores_skill_markdown_and_assets(self) -> None:
        guide_b64 = base64.b64encode(b"# Guide\n\nReference content").decode("ascii")
        script_b64 = base64.b64encode(b"print('hello')\n").decode("ascii")
        template_b64 = base64.b64encode(b"TEMPLATE=ready\n").decode("ascii")

        import_status, imported = self._request(
            "POST",
            "/api/skills/import",
            {
                "items": [
                    {
                        "folder_name": "rag-planner",
                        "source_kind": "single-folder-import",
                        "skill_markdown": "---\nname: rag-planner\ndescription: Imported planning skill.\n---\n\n# RAG Planner\n\nUse the references before answering.\n",
                        "assets": [
                            {
                                "relative_path": "references/guide.md",
                                "mime_type": "text/markdown",
                                "content_base64": guide_b64,
                            },
                            {
                                "relative_path": "scripts/run.py",
                                "mime_type": "text/x-python",
                                "content_base64": script_b64,
                            },
                        ],
                    }
                ]
            },
        )
        self.assertEqual(import_status, 200)
        self.assertEqual(imported["count"], 1)
        self.assertEqual(imported["error_count"], 0)
        created = imported["items"][0]
        self.assertEqual(created["name"], "rag-planner")
        self.assertEqual(created["source_kind"], "single-folder-import")
        self.assertEqual(created["folder_name"], "rag-planner")
        self.assertIn('name: "rag-planner"', created["skill_markdown"])
        self.assertIn('description: "Imported planning skill."', created["skill_markdown"])
        self.assertEqual(created["asset_summary"]["total"], 2)
        self.assertEqual({item["relative_path"] for item in created["assets"]}, {"references/guide.md", "scripts/run.py"})
        skill_id = created["id"]

        update_status, updated = self._request(
            "PUT",
            f"/api/skills/{skill_id}",
            {
                "name": "rag-planner",
                "description": "Imported planning skill updated.",
                "skill_markdown": "---\nname: rag-planner\ndescription: Imported planning skill updated.\n---\n\n# RAG Planner\n\nUse the references and templates before answering.\n",
                "status": "active",
                "assets": [
                    {
                        "relative_path": "templates/prompt.txt",
                        "mime_type": "text/plain",
                        "content_base64": template_b64,
                    }
                ],
            },
        )
        self.assertEqual(update_status, 200)
        self.assertEqual(updated["status"], "active")
        self.assertIn('description: "Imported planning skill updated."', updated["skill_markdown"])
        self.assertEqual(updated["asset_summary"]["total"], 3)
        self.assertTrue(any(item["relative_path"] == "templates/prompt.txt" for item in updated["assets"]))

        batch_status, batch = self._request(
            "POST",
            "/api/skills/import",
            {
                "items": [
                    {
                        "folder_name": "review-skill",
                        "skill_markdown": "---\nname: review-skill\ndescription: Review imported work.\n---\n\n# Review\n\nCheck output quality.\n",
                    },
                    {
                        "folder_name": "delivery-skill",
                        "skill_markdown": "---\nname: delivery-skill\ndescription: Deliver imported work.\n---\n\n# Deliver\n\nShip the final result.\n",
                    },
                ]
            },
        )
        self.assertEqual(batch_status, 200)
        self.assertEqual(batch["count"], 2)
        self.assertEqual(batch["error_count"], 0)
        self.assertEqual({item["name"] for item in batch["items"]}, {"review-skill", "delivery-skill"})

    def test_provider_and_agent_crud_endpoints(self) -> None:
        provider_status, provider = self._request(
            "POST",
            "/api/providers",
            {
                "name": "Provider CRUD",
                "provider_type": "mock",
                "base_url": "mock://crud",
                "api_key": "secret-token",
                "model": "mock-v1",
                "extra_headers": {"X-Test": "1"},
                "extra_config": {"temperature": 0.3},
            },
        )
        self.assertEqual(provider_status, 200)
        provider_id = provider["id"]
        self.assertTrue(provider["has_api_key"])

        detail_status, detail = self._request("GET", f"/api/providers/{provider_id}")
        self.assertEqual(detail_status, 200)
        self.assertEqual(detail["name"], "Provider CRUD")
        self.assertEqual(detail["agent_count"], 0)

        update_status, updated_provider = self._request(
            "PUT",
            f"/api/providers/{provider_id}",
            {
                "name": "Provider CRUD Updated",
                "provider_type": "mock",
                "base_url": "mock://crud-v2",
                "model": "mock-v2",
                "api_version": "v2",
                "organization": "org-demo",
                "extra_headers": {"X-Test": "2"},
                "extra_config": {"temperature": 0.1},
            },
        )
        self.assertEqual(update_status, 200)
        self.assertEqual(updated_provider["name"], "Provider CRUD Updated")
        self.assertTrue(updated_provider["has_api_key"])

        agent_status, agent = self._request(
            "POST",
            "/api/agents",
            {
                "name": "Agent CRUD",
                "role": "planner",
                "system_prompt": "Plan work.",
                "provider_id": provider_id,
                "temperature": 0.2,
            },
        )
        self.assertEqual(agent_status, 200)
        agent_id = agent["id"]

        agent_detail_status, agent_detail = self._request("GET", f"/api/agents/{agent_id}")
        self.assertEqual(agent_detail_status, 200)
        self.assertEqual(agent_detail["name"], "Agent CRUD")
        self.assertEqual(agent_detail["provider_id"], provider_id)

        agent_update_status, updated_agent = self._request(
            "PUT",
            f"/api/agents/{agent_id}",
            {
                "name": "Agent CRUD Updated",
                "role": "reviewer",
                "system_prompt": "Review work.",
                "provider_id": provider_id,
                "model_override": "mock-v2",
                "temperature": 0.4,
                "max_tokens": 512,
            },
        )
        self.assertEqual(agent_update_status, 200)
        self.assertEqual(updated_agent["name"], "Agent CRUD Updated")
        self.assertEqual(updated_agent["resolved_model"], "mock-v2")

        delete_agent_status, deleted_agent = self._request("DELETE", f"/api/agents/{agent_id}")
        self.assertEqual(delete_agent_status, 200)
        self.assertEqual(deleted_agent["id"], agent_id)

        missing_agent_status, missing_agent = self._request("GET", f"/api/agents/{agent_id}")
        self.assertEqual(missing_agent_status, 404)
        self.assertIn("does not exist", missing_agent["detail"])

        delete_provider_status, deleted_provider = self._request("DELETE", f"/api/providers/{provider_id}")
        self.assertEqual(delete_provider_status, 200)
        self.assertEqual(deleted_provider["id"], provider_id)

        missing_provider_status, missing_provider = self._request("GET", f"/api/providers/{provider_id}")
        self.assertEqual(missing_provider_status, 404)
        self.assertIn("does not exist", missing_provider["detail"])

    def test_provider_accepts_minimal_required_fields(self) -> None:
        provider_status, provider = self._request(
            "POST",
            "/api/providers",
            {
                "name": "Minimal Provider",
                "provider_type": "openai",
                "model": "gpt-4.1-mini",
            },
        )
        self.assertEqual(provider_status, 200)
        self.assertFalse(provider["has_api_key"])

        detail_status, detail = self._request("GET", f"/api/providers/{provider['id']}")
        self.assertEqual(detail_status, 200)
        self.assertIsNone(detail["base_url"])
        self.assertEqual(detail["model"], "gpt-4.1-mini")

    def test_provider_test_endpoint_uses_litellm_for_non_mock_provider(self) -> None:
        provider_status, provider = self._request(
            "POST",
            "/api/providers",
            {
                "name": "LiteLLM Provider",
                "provider_type": "openai",
                "base_url": "https://api.openai.com/v1",
                "api_key": "secret-token",
                "model": "gpt-4.1-mini",
                "organization": "org-demo",
                "extra_headers": {"X-Test": "1"},
                "extra_config": {"top_p": 0.9},
            },
        )
        self.assertEqual(provider_status, 200)

        with patch(
            "aiteams.ai_gateway.completion",
            return_value={
                "choices": [{"message": {"content": "READY via LiteLLM"}}],
                "model": "openai/gpt-4.1-mini",
                "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7},
            },
        ) as mocked_completion:
            test_status, payload = self._request(
                "POST",
                "/api/providers/test",
                {
                    "provider_id": provider["id"],
                    "prompt": "Say READY.",
                },
            )

        self.assertEqual(test_status, 200)
        self.assertEqual(payload["content"], "READY via LiteLLM")
        self.assertEqual(payload["model"], "openai/gpt-4.1-mini")

        kwargs = mocked_completion.call_args.kwargs
        self.assertEqual(kwargs["model"], "openai/gpt-4.1-mini")
        self.assertEqual(kwargs["api_key"], "secret-token")
        self.assertEqual(kwargs["base_url"], "https://api.openai.com/v1")
        self.assertEqual(kwargs["organization"], "org-demo")
        self.assertEqual(kwargs["extra_headers"], {"X-Test": "1"})
        self.assertEqual(kwargs["top_p"], 0.9)
        self.assertTrue(kwargs["drop_params"])

    def test_delete_guards_for_referenced_provider_and_agent(self) -> None:
        provider_status, provider = self._request(
            "POST",
            "/api/providers",
            {
                "name": "Protected Provider",
                "provider_type": "mock",
                "base_url": "mock://protected",
                "api_key": "secret-token",
                "model": "mock-model",
                "extra_headers": {},
                "extra_config": {},
            },
        )
        self.assertEqual(provider_status, 200)
        provider_id = provider["id"]

        agent_status, agent = self._request(
            "POST",
            "/api/agents",
            {
                "name": "Protected Agent",
                "role": "planner",
                "system_prompt": "Protect task history.",
                "provider_id": provider_id,
                "temperature": 0.1,
            },
        )
        self.assertEqual(agent_status, 200)
        agent_id = agent["id"]

        run_status, _ = self._request(
            "POST",
            "/api/collaborations/run",
            {
                "title": "Protected Session",
                "prompt": "Generate a short protected answer.",
                "agent_ids": [agent_id],
                "lead_agent_id": agent_id,
                "rounds": 1,
            },
        )
        self.assertEqual(run_status, 200)

        delete_agent_status, delete_agent_payload = self._request("DELETE", f"/api/agents/{agent_id}")
        self.assertEqual(delete_agent_status, 409)
        self.assertIn("cannot be deleted", delete_agent_payload["detail"])

        delete_provider_status, delete_provider_payload = self._request("DELETE", f"/api/providers/{provider_id}")
        self.assertEqual(delete_provider_status, 409)
        self.assertIn("cannot be deleted", delete_provider_payload["detail"])

    def test_provider_and_agent_list_endpoints_support_pagination(self) -> None:
        provider_ids: list[str] = []
        for index in range(7):
            status, payload = self._request(
                "POST",
                "/api/providers",
                {
                    "name": f"Provider Page {index}",
                    "provider_type": "mock",
                    "base_url": f"mock://provider-{index}",
                    "api_key": "",
                    "model": f"mock-model-{index}",
                    "extra_headers": {},
                    "extra_config": {},
                },
            )
            self.assertEqual(status, 200)
            provider_ids.append(payload["id"])

        list_status, provider_page = self._request("GET", "/api/providers?limit=3&offset=3")
        self.assertEqual(list_status, 200)
        self.assertEqual(provider_page["count"], 7)
        self.assertEqual(provider_page["limit"], 3)
        self.assertEqual(provider_page["offset"], 3)
        self.assertEqual(len(provider_page["items"]), 3)

        agent_ids: list[str] = []
        for index in range(5):
            status, payload = self._request(
                "POST",
                "/api/agents",
                {
                    "name": f"Agent Page {index}",
                    "role": "specialist",
                    "system_prompt": f"Handle pagination case {index}.",
                    "provider_id": provider_ids[index % len(provider_ids)],
                    "temperature": 0.2,
                },
            )
            self.assertEqual(status, 200)
            agent_ids.append(payload["id"])

        agent_list_status, agent_page = self._request("GET", "/api/agents?limit=2&offset=2")
        self.assertEqual(agent_list_status, 200)
        self.assertEqual(agent_page["count"], 5)
        self.assertEqual(agent_page["limit"], 2)
        self.assertEqual(agent_page["offset"], 2)
        self.assertEqual(len(agent_page["items"]), 2)

    def test_provider_and_agent_list_endpoints_support_filtering(self) -> None:
        provider_payloads = [
            {
                "name": "Alpha Provider",
                "provider_type": "openai",
                "base_url": "https://api.openai.test/v1",
                "api_key": "alpha-key",
                "model": "gpt-4o",
                "extra_headers": {},
                "extra_config": {},
            },
            {
                "name": "Beta Local",
                "provider_type": "mock",
                "base_url": "mock://beta",
                "api_key": "",
                "model": "local-vision",
                "extra_headers": {},
                "extra_config": {},
            },
            {
                "name": "Gamma Provider",
                "provider_type": "openai",
                "base_url": "https://gateway.example/v1",
                "api_key": "gamma-key",
                "model": "gpt-4.1-mini",
                "extra_headers": {},
                "extra_config": {},
            },
        ]

        provider_ids: dict[str, str] = {}
        for payload in provider_payloads:
            status, created = self._request("POST", "/api/providers", payload)
            self.assertEqual(status, 200)
            provider_ids[payload["name"]] = created["id"]

        by_name_status, by_name_page = self._request("GET", "/api/providers?name=alpha")
        self.assertEqual(by_name_status, 200)
        self.assertEqual(by_name_page["count"], 1)
        self.assertEqual(by_name_page["items"][0]["name"], "Alpha Provider")

        by_type_status, by_type_page = self._request("GET", "/api/providers?provider_type=openai")
        self.assertEqual(by_type_status, 200)
        self.assertEqual(by_type_page["count"], 2)
        self.assertEqual({item["name"] for item in by_type_page["items"]}, {"Alpha Provider", "Gamma Provider"})

        by_model_status, by_model_page = self._request("GET", "/api/providers?model=vision")
        self.assertEqual(by_model_status, 200)
        self.assertEqual(by_model_page["count"], 1)
        self.assertEqual(by_model_page["items"][0]["name"], "Beta Local")

        combined_provider_status, combined_provider_page = self._request("GET", "/api/providers?provider_type=openai&model=mini")
        self.assertEqual(combined_provider_status, 200)
        self.assertEqual(combined_provider_page["count"], 1)
        self.assertEqual(combined_provider_page["items"][0]["name"], "Gamma Provider")

        agent_payloads = [
            {
                "name": "Planner Agent",
                "role": "planner",
                "system_prompt": "Break work into milestones and dependencies.",
                "provider_id": provider_ids["Alpha Provider"],
                "temperature": 0.2,
            },
            {
                "name": "Reviewer Agent",
                "role": "reviewer",
                "system_prompt": "Check proposals for correctness and gaps.",
                "provider_id": provider_ids["Alpha Provider"],
                "model_override": "gpt-4o-mini",
                "temperature": 0.2,
            },
            {
                "name": "Vision Scout",
                "role": "analyst",
                "system_prompt": "Inspect multimodal inputs and summarize findings.",
                "provider_id": provider_ids["Beta Local"],
                "temperature": 0.2,
            },
        ]

        for payload in agent_payloads:
            status, created = self._request("POST", "/api/agents", payload)
            self.assertEqual(status, 200)
            self.assertEqual(created["name"], payload["name"])

        agent_by_name_status, agent_by_name_page = self._request("GET", "/api/agents?name=planner")
        self.assertEqual(agent_by_name_status, 200)
        self.assertEqual(agent_by_name_page["count"], 1)
        self.assertEqual(agent_by_name_page["items"][0]["name"], "Planner Agent")

        agent_by_role_status, agent_by_role_page = self._request("GET", "/api/agents?role=review")
        self.assertEqual(agent_by_role_status, 200)
        self.assertEqual(agent_by_role_page["count"], 1)
        self.assertEqual(agent_by_role_page["items"][0]["name"], "Reviewer Agent")

        provider_filter_status, provider_filter_page = self._request(
            "GET",
            f"/api/agents?provider_id={provider_ids['Beta Local']}",
        )
        self.assertEqual(provider_filter_status, 200)
        self.assertEqual(provider_filter_page["count"], 1)
        self.assertEqual(provider_filter_page["items"][0]["name"], "Vision Scout")

        model_filter_status, model_filter_page = self._request("GET", "/api/agents?model=4o")
        self.assertEqual(model_filter_status, 200)
        self.assertEqual(model_filter_page["count"], 2)
        self.assertEqual({item["name"] for item in model_filter_page["items"]}, {"Planner Agent", "Reviewer Agent"})

        combined_agent_status, combined_agent_page = self._request(
            "GET",
            f"/api/agents?provider_id={provider_ids['Alpha Provider']}&model=mini",
        )
        self.assertEqual(combined_agent_status, 200)
        self.assertEqual(combined_agent_page["count"], 1)
        self.assertEqual(combined_agent_page["items"][0]["name"], "Reviewer Agent")
