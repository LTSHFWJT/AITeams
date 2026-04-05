from __future__ import annotations

import base64
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiteams.agent_center.service import AgentCenterService
from aiteams.api.application import ServiceContainer, WebApplication
from aiteams.plugins import PluginManager
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


def create_plugin_package(root: Path, *, key: str, name: str, version: str = "v1") -> Path:
    package_dir = root / key.replace(".", "_")
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "plugin.yaml").write_text(
        "\n".join(
            [
                f"key: {key}",
                f"name: {name}",
                f"version: {version}",
                "plugin_type: toolset",
                "entrypoint: runtime:create_plugin",
                f"workbench_key: {key.replace('.', '_')}",
                "tools:",
                f"  - {key}.tool",
                "permissions:",
                "  - readonly",
                "actions:",
                "  - name: echo",
                f"    description: Echo from {name}",
                "    tool_name: echo_tool",
                "    input_schema:",
                "      type: object",
                "      properties:",
                "        text:",
                "          type: string",
                "          description: Text to echo",
                "      required:",
                "        - text",
                "hot_reload: true",
            ]
        ),
        encoding="utf-8",
    )
    (package_dir / "runtime.py").write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "class DemoRuntime:",
                "    def __init__(self, *, manifest, root_path):",
                "        self.manifest = manifest",
                "        self.root_path = root_path",
                "",
                "    def describe(self):",
                "        return {",
                "            'key': self.manifest['key'],",
                "            'name': self.manifest['name'],",
                "            'version': self.manifest['version'],",
                "            'tools': self.manifest.get('tools', []),",
                "            'actions': self.manifest.get('actions', []),",
                "            'hot_reload': self.manifest.get('hot_reload', True),",
                "        }",
                "",
                "    def health(self):",
                "        return {'status': 'ok'}",
                "",
                "    def invoke(self, action, payload, context):",
                "        return {'action': action, 'payload': payload, 'context_keys': sorted(context.keys())}",
                "",
                "def create_plugin(*, manifest, root_path):",
                "    return DemoRuntime(manifest=manifest, root_path=root_path)",
            ]
        ),
        encoding="utf-8",
    )
    return package_dir


def build_upload_payload(source_root: Path) -> dict[str, object]:
    files = []
    for file_path in sorted(item for item in source_root.rglob("*") if item.is_file()):
        files.append(
            {
                "path": file_path.relative_to(source_root).as_posix(),
                "content_base64": base64.b64encode(file_path.read_bytes()).decode("ascii"),
            }
        )
    return {
        "source_name": source_root.name,
        "files": files,
    }


class PluginUploadImportTestCase(unittest.TestCase):
    def build_app(self, root: Path) -> tuple[WebApplication, PluginManager, MetadataStore]:
        store = build_store(root)
        plugin_manager = PluginManager(store=store, install_root=root / "data" / "plugins")
        agent_center = AgentCenterService(store=store, local_models_root=root / "models")

        class FakeKnowledgeBases:
            def close(self) -> None:
                return None

        app = WebApplication(
            ServiceContainer(
                store=store,
                runtime=SimpleNamespace(agent_kernel=SimpleNamespace(memory=SimpleNamespace(close=lambda: None))),
                workspace=SimpleNamespace(),
                agent_center=agent_center,
                plugins=plugin_manager,
                knowledge_bases=FakeKnowledgeBases(),
                static_dir=root,
                local_models_root=root / "models",
            )
        )
        return app, plugin_manager, store

    def test_plugin_scan_upload_detects_single_plugin_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            upload_root = root / "upload"
            create_plugin_package(upload_root, key="demo.single", name="Demo Single Plugin")
            app, _, _ = self.build_app(root)
            try:
                response = app.handle(
                    "POST",
                    "/api/agent-center/plugins/scan-upload",
                    body=json.dumps(build_upload_payload(upload_root)).encode("utf-8"),
                )
                self.assertEqual(response.status, 200)
                payload = json.loads(response.body.decode("utf-8"))
                self.assertEqual(payload["source_kind"], "single")
                self.assertEqual(payload["plugin_count"], 1)
                self.assertEqual(payload["valid_plugin_count"], 1)
                self.assertTrue(payload["valid"])
                self.assertEqual(payload["plugins"][0]["manifest"]["key"], "demo.single")
            finally:
                app.close()

    def test_plugin_storage_schema_is_minimal_and_manifest_is_derived_from_install_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            package_root = create_plugin_package(root, key="demo.derived", name="Demo Derived Plugin")
            store = build_store(root)
            try:
                columns = {str(row.get("name") or "") for row in store.fetch_all("PRAGMA table_info(plugins)")}
                self.assertEqual(columns, {"id", "name", "install_path"})

                saved = store.save_plugin(
                    plugin_id=None,
                    key="ignored.key",
                    name="Demo Derived Plugin",
                    version="ignored",
                    plugin_type="ignored",
                    description="ignored",
                    manifest={},
                    config={},
                    install_path=str(package_root),
                )
                plugin = store.get_plugin(str(saved["id"]))
                self.assertIsNotNone(plugin)
                self.assertEqual(plugin["key"], "demo.derived")
                self.assertEqual(plugin["version"], "v1")
                self.assertEqual(plugin["plugin_type"], "toolset")
                self.assertEqual(plugin["description"], "")
                self.assertEqual(plugin["manifest_json"]["key"], "demo.derived")
            finally:
                store.close()

    def test_default_plugin_catalog_resolves_without_database_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            try:
                plugin = store.get_plugin_by_key("research_kit")
                self.assertIsNotNone(plugin)
                self.assertEqual(plugin["id"], "catalog:research_kit")
                self.assertEqual(plugin["key"], "research_kit")
                self.assertTrue(isinstance(plugin.get("manifest_json"), dict))
            finally:
                store.close()

    def test_agent_center_default_initialization_does_not_insert_default_plugins(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = build_store(root)
            try:
                service = AgentCenterService(store=store, local_models_root=root / "models")
                service.ensure_defaults()
                self.assertEqual(store.list_plugins(), [])
                catalog_plugin = store.get_plugin_by_key("research_kit")
                self.assertIsNotNone(catalog_plugin)
                self.assertEqual(str(catalog_plugin["id"]), "catalog:research_kit")
            finally:
                store.close()

    def test_plugin_import_upload_copies_plugins_into_managed_directory_and_loads(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            upload_root = root / "upload"
            collection_root = upload_root / "collection"
            create_plugin_package(collection_root, key="demo.alpha", name="Demo Alpha Plugin")
            create_plugin_package(collection_root, key="demo.beta", name="Demo Beta Plugin")
            app, plugin_manager, store = self.build_app(root)
            try:
                response = app.handle(
                    "POST",
                    "/api/agent-center/plugins/import-upload",
                    body=json.dumps(build_upload_payload(upload_root)).encode("utf-8"),
                )
                self.assertEqual(response.status, 200)
                payload = json.loads(response.body.decode("utf-8"))
                self.assertEqual(payload["source_kind"], "collection")
                self.assertEqual(payload["imported_count"], 2)
                self.assertEqual(payload["skipped_count"], 0)

                plugins = store.list_plugins()
                self.assertEqual(len(plugins), 2)
                for plugin in plugins:
                    install_path = Path(str(plugin.get("install_path") or "")).resolve()
                    self.assertTrue(install_path.exists())
                    self.assertTrue(str(install_path).startswith(str(plugin_manager.install_root)))
                    snapshot = plugin_manager.snapshot(str(plugin["id"]))
                    self.assertEqual(snapshot["status"], "idle")
                    reinstall = plugin_manager.install_plugin(str(plugin["id"]))
                    self.assertEqual(reinstall["plugin"]["install_path"], str(install_path))
            finally:
                app.close()

    def test_plugin_import_upload_updates_existing_key_and_version(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            app, _, store = self.build_app(root)
            try:
                first_upload = root / "upload-1"
                create_plugin_package(first_upload, key="demo.same", name="Demo Same Plugin", version="v1")
                first_response = app.handle(
                    "POST",
                    "/api/agent-center/plugins/import-upload",
                    body=json.dumps(build_upload_payload(first_upload)).encode("utf-8"),
                )
                self.assertEqual(first_response.status, 200)
                plugins_after_first = store.list_plugins()
                self.assertEqual(len(plugins_after_first), 1)
                first_id = str(plugins_after_first[0]["id"])

                second_upload = root / "upload-2"
                create_plugin_package(second_upload, key="demo.same", name="Demo Same Plugin Updated", version="v1")
                second_response = app.handle(
                    "POST",
                    "/api/agent-center/plugins/import-upload",
                    body=json.dumps(build_upload_payload(second_upload)).encode("utf-8"),
                )
                self.assertEqual(second_response.status, 200)
                plugins_after_second = store.list_plugins()
                self.assertEqual(len(plugins_after_second), 1)
                self.assertEqual(str(plugins_after_second[0]["id"]), first_id)
                self.assertEqual(str(plugins_after_second[0]["name"]), "Demo Same Plugin Updated")
            finally:
                app.close()

    def test_plugin_base_tools_endpoint_returns_manifest_derived_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            upload_root = root / "upload"
            create_plugin_package(upload_root, key="demo.preview", name="Demo Preview Plugin")
            app, _, store = self.build_app(root)
            try:
                import_response = app.handle(
                    "POST",
                    "/api/agent-center/plugins/import-upload",
                    body=json.dumps(build_upload_payload(upload_root)).encode("utf-8"),
                )
                self.assertEqual(import_response.status, 200)
                plugin_id = str(store.list_plugins()[0]["id"])

                response = app.handle("GET", f"/api/agent-center/plugins/{plugin_id}/base-tools")
                self.assertEqual(response.status, 200)
                payload = json.loads(response.body.decode("utf-8"))
                self.assertEqual(payload["plugin_key"], "demo.preview")
                self.assertEqual(payload["tool_count"], 1)
                self.assertEqual(payload["base_tools"][0]["tool_name"], "echo_tool")
                self.assertEqual(payload["base_tools"][0]["action_name"], "echo")
                self.assertEqual(payload["base_tools"][0]["mode"], "structured")
                self.assertIn("text", dict((payload["base_tools"][0]["args_schema"] or {}).get("properties") or {}))
            finally:
                app.close()

    def test_plugin_delete_endpoint_removes_database_row_install_path_and_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            upload_root = root / "upload"
            create_plugin_package(upload_root, key="demo.delete", name="Demo Delete Plugin")
            app, plugin_manager, store = self.build_app(root)
            try:
                import_response = app.handle(
                    "POST",
                    "/api/agent-center/plugins/import-upload",
                    body=json.dumps(build_upload_payload(upload_root)).encode("utf-8"),
                )
                self.assertEqual(import_response.status, 200)
                plugin = store.list_plugins()[0]
                plugin_id = str(plugin["id"])
                install_path = Path(str(plugin.get("install_path") or "")).resolve()

                plugin_manager.load_plugin(plugin_id)
                self.assertIn(plugin_id, plugin_manager._sandboxes)
                self.assertTrue(install_path.exists())

                response = app.handle("DELETE", f"/api/agent-center/plugins/{plugin_id}")
                self.assertEqual(response.status, 200)
                payload = json.loads(response.body.decode("utf-8"))
                self.assertTrue(payload["deleted"])
                self.assertEqual(str(payload["plugin"]["id"]), plugin_id)
                self.assertFalse(install_path.exists())
                self.assertEqual(store.list_plugins(), [])
                self.assertNotIn(plugin_id, plugin_manager._sandboxes)
            finally:
                app.close()
