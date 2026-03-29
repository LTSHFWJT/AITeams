from __future__ import annotations

from aiteams.agent_center import AgentCenterService
from aiteams.agent.kernel import AgentKernel
from aiteams.ai_gateway import AIGateway
from aiteams.api.application import AITeamsHTTPServer, LOGGER, ServiceContainer, WebApplication
from aiteams.app.settings import AppSettings
from aiteams.memory.adapter import LangMemAdapter
from aiteams.plugins import PluginManager
from aiteams.runtime.compiler import BlueprintCompiler
from aiteams.runtime.engine import RuntimeEngine
from aiteams.storage.metadata import MetadataStore
from aiteams.workspace.manager import WorkspaceManager


def _build_container(settings: AppSettings) -> ServiceContainer:
    seed_agent_center_defaults = not settings.metadata_db_path.exists()
    store = MetadataStore(
        settings.metadata_db_path,
        default_workspace_id=settings.default_workspace_id,
        default_workspace_name=settings.default_workspace_name,
        default_project_id=settings.default_project_id,
        default_project_name=settings.default_project_name,
        workspace_root=settings.workspace_root,
    )
    workspace = WorkspaceManager(settings.workspace_root)
    gateway = AIGateway()
    memory = LangMemAdapter(str(settings.memory_root), gateway=gateway)
    compiler = BlueprintCompiler()
    plugin_manager = PluginManager(store=store, install_root=settings.data_dir / "plugins", memory=memory)
    agent_kernel = AgentKernel(memory=memory, gateway=gateway, plugin_manager=plugin_manager)
    runtime = RuntimeEngine(
        store=store,
        compiler=compiler,
        agent_kernel=agent_kernel,
        workspace=workspace,
        checkpoint_db_path=settings.checkpoint_db_path,
        deepagents_skill_root=settings.deepagents_skill_root,
    )
    agent_center = AgentCenterService(store, plugin_manager=plugin_manager, gateway=gateway)
    if seed_agent_center_defaults:
        agent_center.ensure_defaults()
    memory.configure_retrieval(agent_center.retrieval_runtime_config())
    memory.start_background_maintenance()
    return ServiceContainer(
        store=store,
        runtime=runtime,
        workspace=workspace,
        agent_center=agent_center,
        plugins=plugin_manager,
        static_dir=settings.static_dir,
    )


def create_app(settings: AppSettings | None = None) -> WebApplication:
    resolved = settings or AppSettings.load()
    return WebApplication(_build_container(resolved))


def run() -> None:
    settings = AppSettings.load()
    application = create_app(settings)
    server = AITeamsHTTPServer((settings.server_host, settings.server_port), application)
    LOGGER.info("AITeams control plane starting")
    LOGGER.info("Listen: http://%s:%s", settings.server_host, settings.server_port)
    LOGGER.info("Metadata DB: %s", settings.metadata_db_path)
    LOGGER.info("Checkpoint DB: %s", settings.checkpoint_db_path)
    LOGGER.info("Memory root: %s", settings.memory_root)
    LOGGER.info("Workspace root: %s", settings.workspace_root)
    LOGGER.info("DeepAgents skill root: %s", settings.deepagents_skill_root)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("AITeams server interrupted by user")
    finally:
        server.server_close()
        application.close()
