const DEFAULT_UI_METADATA = {
  review_policy: {
    triggers: [
      { value: "before_tool_call", label: "before_tool_call / 工具调用前" },
      { value: "before_external_side_effect", label: "before_external_side_effect / 外部副作用前" },
      { value: "before_memory_write", label: "before_memory_write / 记忆写入前" },
      { value: "before_agent_to_agent_message", label: "before_agent_to_agent_message / Agent 消息前" },
      { value: "before_handoff_to_lower_level", label: "before_handoff_to_lower_level / 向下交接前" },
      { value: "before_escalation_to_upper_level", label: "before_escalation_to_upper_level / 向上升级前" },
      { value: "before_final_delivery", label: "before_final_delivery / 最终交付前" },
      { value: "before_agent_receive_task", label: "before_agent_receive_task / Agent 接任务前" },
      { value: "before_task_ingress", label: "before_task_ingress / 任务入站前" },
      { value: "final_delivery", label: "final_delivery / 交付消息" },
    ],
    actions: [
      { value: "approve", label: "approve / 允许" },
      { value: "reject", label: "reject / 拒绝" },
      { value: "edit_payload", label: "edit_payload / 编辑载荷" },
      { value: "edit_records", label: "edit_records / 编辑记忆记录" },
      { value: "reroute", label: "reroute / 改路由" },
    ],
    message_types: [
      { value: "task", label: "task / 任务" },
      { value: "dialogue", label: "dialogue / 对话" },
      { value: "handoff", label: "handoff / 交接" },
      { value: "delivery", label: "delivery / 交付" },
      { value: "human_escalation", label: "human_escalation / 人工介入" },
      { value: "escalation", label: "escalation / 升级" },
    ],
    memory_scopes: [
      { value: "agent", label: "agent / Agent 私有" },
      { value: "team", label: "team / 团队共享" },
      { value: "project", label: "project / 项目共享" },
      { value: "run", label: "run / 运行回顾" },
      { value: "working", label: "working / 工作记忆" },
    ],
    memory_kinds: [
      { value: "summary", label: "summary / 摘要" },
      { value: "fact", label: "fact / 事实" },
      { value: "deliverable", label: "deliverable / 交付物" },
      { value: "risk", label: "risk / 风险" },
      { value: "next_focus", label: "next_focus / 下一步焦点" },
      { value: "team_message", label: "team_message / 团队消息" },
      { value: "human_escalation", label: "human_escalation / 人工介入" },
    ],
  },
  team_edge_review: {
    modes: [{ value: "must_review_before", label: "must_review_before / 必须前审" }],
    message_types: [
      { value: "task", label: "task / 任务" },
      { value: "dialogue", label: "dialogue / 对话" },
      { value: "handoff", label: "handoff / 交接" },
    ],
    phases: [
      { value: "down", label: "down / 向下" },
      { value: "up", label: "up / 向上" },
    ],
  },
  memory_profile: {
    scopes: [
      { value: "agent", label: "agent / Agent 私有" },
      { value: "team", label: "team / 团队共享" },
      { value: "project", label: "project / 项目共享" },
      { value: "run", label: "run / 当前运行" },
      { value: "retrospective", label: "retrospective / 运行回顾" },
    ],
  },
};

const state = {
  summary: {},
  storage: null,
  providerTypes: [],
  uiMetadata: null,
  recentBuilds: [],
  recentRuns: [],
  providers: [],
  retrievalSettings: {
    settings: {
      embedding: { mode: "hash" },
      rerank: { mode: "disabled" },
    },
    warnings: [],
    updated_at: null,
  },
  providerPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
    query: "",
    providerType: "",
  },
  plugins: [],
  builtinPlugins: [],
  pluginPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  skills: [],
  staticMemories: [],
  staticMemoryPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  knowledgeBases: [],
  reviewPolicies: [],
  memoryProfiles: [],
  agentTemplates: [],
  agentTemplatePage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  agentDefinitions: [],
  agentDefinitionPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  teamDefinitions: [],
  teamTemplates: [],
  builds: [],
  blueprints: [],
  runs: [],
  approvals: [],
  selectedTaskTeamDefinitionId: null,
  activePage: "overview",
  activeNavSection: "overview",
  editingProviderId: null,
  editingPluginId: null,
  editingStaticMemoryId: null,
  staticMemoryEditorMode: "role",
  editingKnowledgeBaseId: null,
  editingReviewPolicyId: null,
  editingMemoryProfileId: null,
  editingAgentDefinitionId: null,
  editingAgentTemplateId: null,
  editingTeamDefinitionId: null,
  editingTeamTemplateId: null,
  knowledgeBaseDocuments: [],
  knowledgeBasePersistedDocumentIds: [],
  knowledgeBaseBaseConfig: {},
  reviewPolicyBaseSpec: {},
  memoryProfileBaseSpec: {},
  agentDefinitionBaseSpec: {},
  teamDefinitionBaseSpec: {},
  selectedTeamTemplateId: null,
  selectedBuildId: null,
  selectedBlueprintId: null,
  selectedRunId: null,
  providerEditor: {
    models: [],
    savedModels: [],
    editingModelIndex: null,
  },
  pluginEditor: {
    manifest: {},
    schema: null,
    config: {},
    secretFieldPaths: [],
  },
  loaded: {
    controlPlane: false,
    providerTypes: false,
    uiMetadata: false,
    providerRefs: false,
    retrievalSettings: false,
    providerPage: false,
    builtinPluginRefs: false,
    pluginRefs: false,
    pluginPage: false,
    skillRefs: false,
    staticMemoryRefs: false,
    staticMemoryPage: false,
    knowledgeBaseRefs: false,
    reviewPolicyRefs: false,
    memoryProfileRefs: false,
    agentTemplateRefs: false,
    agentTemplatePage: false,
    agentDefinitionRefs: false,
    agentDefinitionPage: false,
    teamDefinitions: false,
    teamTemplates: false,
    builds: false,
    blueprints: false,
    runs: false,
    approvals: false,
  },
  teamEditor: {
    spec: null,
    selectedNodeId: null,
    linkFromNodeId: null,
    drag: null,
    validation: null,
    preview: null,
  },
};

const PAGE_SECTIONS = {
  overview: "overview",
  providers: "resources",
  "retrieval-config": "resources",
  plugins: "resources",
  "responsibility-specs": "resources",
  "knowledge-bases": "resources",
  "review-policies": "resources",
  "memory-profiles": "resources",
  "agent-definitions": "resources",
  "agent-templates": "resources",
  "team-definitions": "orchestration",
  "team-templates": "orchestration",
  runtime: "delivery",
  approvals: "delivery",
  builds: "delivery",
  blueprints: "delivery",
};

const SECTION_DEFAULT_PAGE = {
  overview: "overview",
  resources: "providers",
  orchestration: "team-definitions",
  delivery: "runtime",
};

const MODEL_TYPE_LABELS = {
  chat: "\u804a\u5929",
  embedding: "\u5d4c\u5165",
  rerank: "\u91cd\u6392",
};

const STATIC_MEMORY_MODE_CONFIG = {
  role: {
    page: "responsibility-specs",
    resourceLabel: "角色管理",
    resourcePluralLabel: "角色管理",
    descriptionLabel: "角色简介",
    modalTitleCreate: "新增角色管理项",
    modalTitleEdit: "编辑角色管理项",
    panelTitle: "角色详情",
    namePlaceholder: "规划负责人角色管理项",
    descriptionPlaceholder: "简要说明这个角色的定位和边界",
    promptPlaceholder: "说明这个角色如何处理任务、哪些边界不能越过、何时必须升级。",
    emptyTitle: "暂无角色管理项",
    emptyBody: "先创建第一份角色管理项。",
    saveMessage: "角色管理项已保存",
    deleteMessage: "角色管理项已删除",
    deletePrompt: "确认删除角色管理项",
  },
};

const topNavButtons = Array.from(document.querySelectorAll("[data-nav-section]"));
const navButtons = Array.from(document.querySelectorAll("[data-page-target]"));
const subMenuPanels = Array.from(document.querySelectorAll("[data-nav-panel]"));
const pageViews = Array.from(document.querySelectorAll("[data-page]"));

const summaryCards = document.querySelector("#summary-cards");
const storageBanner = document.querySelector("#storage-banner");
const providerTypeGrid = document.querySelector("#provider-type-grid");
const overviewBuilds = document.querySelector("#overview-builds");
const overviewRuns = document.querySelector("#overview-runs");

const providerForm = document.querySelector("#provider-form");
const providerName = document.querySelector("#provider-name");
const providerType = document.querySelector("#provider-type");
const providerBaseUrl = document.querySelector("#provider-base-url");
const providerApiKey = document.querySelector("#provider-api-key");
const providerApiKeyToggle = document.querySelector("#provider-api-key-toggle");
const providerApiKeyIconShow = document.querySelector("#provider-api-key-icon-show");
const providerApiKeyIconHide = document.querySelector("#provider-api-key-icon-hide");
const providerSkipTlsVerify = document.querySelector("#provider-skip-tls-verify");
const providerPageSize = document.querySelector("#provider-page-size");
const providerOpenCreate = document.querySelector("#provider-open-create");
const providerList = document.querySelector("#provider-list");
const providerPaginationMeta = document.querySelector("#provider-pagination-meta");
const providerResult = document.querySelector("#provider-result");
const retrievalSettingsForm = document.querySelector("#retrieval-settings-form");
const retrievalSummary = document.querySelector("#retrieval-summary");
const retrievalEmbeddingMode = document.querySelector("#retrieval-embedding-mode");
const retrievalEmbeddingProvider = document.querySelector("#retrieval-embedding-provider");
const retrievalEmbeddingModel = document.querySelector("#retrieval-embedding-model");
const retrievalRerankMode = document.querySelector("#retrieval-rerank-mode");
const retrievalRerankProvider = document.querySelector("#retrieval-rerank-provider");
const retrievalRerankModel = document.querySelector("#retrieval-rerank-model");
const retrievalSettingsRefresh = document.querySelector("#retrieval-settings-refresh");
const retrievalSettingsResult = document.querySelector("#retrieval-settings-result");
const providerModal = document.querySelector("#provider-modal");
const providerModalTitle = document.querySelector("#provider-modal-title");
const providerModalCloseButtons = Array.from(document.querySelectorAll("[data-provider-modal-close]"));
const providerCancel = document.querySelector("#provider-cancel");
const providerModelSummary = document.querySelector("#provider-model-summary");
const providerModelEditorModal = document.querySelector("#provider-model-editor-modal");
const providerModelEditorTitle = document.querySelector("#provider-model-editor-title");
const providerModelEditorModalCloseButtons = Array.from(document.querySelectorAll("[data-provider-model-editor-modal-close]"));
const providerModelList = document.querySelector("#provider-model-list");
const providerModelNew = document.querySelector("#provider-model-new");
const providerModelReset = document.querySelector("#provider-model-reset");
const providerModelFetch = document.querySelector("#provider-model-fetch");
const providerModelEditor = document.querySelector("#provider-model-editor");
const providerModelName = document.querySelector("#provider-model-name");
const providerModelType = document.querySelector("#provider-model-type");
const providerModelContextWindow = document.querySelector("#provider-model-context-window");
const providerModelCancel = document.querySelector("#provider-model-cancel");
const providerModelTest = document.querySelector("#provider-model-test");
const providerModelSave = document.querySelector("#provider-model-save");
const providerModelTestResult = document.querySelector("#provider-model-test-result");

const pluginForm = document.querySelector("#plugin-form");
const pluginKey = document.querySelector("#plugin-key");
const pluginName = document.querySelector("#plugin-name");
const pluginVersion = document.querySelector("#plugin-version");
const pluginType = document.querySelector("#plugin-type");
const pluginWorkbenchKey = document.querySelector("#plugin-workbench-key");
const pluginInstallPath = document.querySelector("#plugin-install-path");
const pluginTools = document.querySelector("#plugin-tools");
const pluginPermissions = document.querySelector("#plugin-permissions");
const pluginDescription = document.querySelector("#plugin-description");
const pluginOpenCreate = document.querySelector("#plugin-open-create");
const pluginPageSize = document.querySelector("#plugin-page-size");
const pluginList = document.querySelector("#plugin-list");
const pluginPaginationMeta = document.querySelector("#plugin-pagination-meta");
const pluginResult = document.querySelector("#plugin-result");
const pluginModal = document.querySelector("#plugin-modal");
const pluginModalTitle = document.querySelector("#plugin-modal-title");
const pluginModalCloseButtons = Array.from(document.querySelectorAll("[data-plugin-modal-close]"));
const pluginCancel = document.querySelector("#plugin-cancel");
const pluginConfigPanel = document.querySelector("#plugin-config-panel");
const pluginConfigHint = document.querySelector("#plugin-config-hint");
const pluginConfigSchemaEmpty = document.querySelector("#plugin-config-schema-empty");
const pluginConfigForm = document.querySelector("#plugin-config-form");
const pluginModalResult = document.querySelector("#plugin-modal-result");

const responsibilitySpecOpenRoleCreate = document.querySelector("#responsibility-spec-open-role-create");
const staticMemoryPageSize = document.querySelector("#static-memory-page-size");
const responsibilitySpecList = document.querySelector("#responsibility-spec-list");
const staticMemoryPaginationMeta = document.querySelector("#static-memory-pagination-meta");
const responsibilitySpecResult = document.querySelector("#responsibility-spec-result");
const staticMemoryForm = document.querySelector("#static-memory-form");
const staticMemoryModal = document.querySelector("#static-memory-modal");
const staticMemoryModalTitle = document.querySelector("#static-memory-modal-title");
const staticMemoryModalCloseButtons = Array.from(document.querySelectorAll("[data-static-memory-modal-close]"));
const staticMemoryModalResult = document.querySelector("#static-memory-modal-result");
const staticMemoryContextHint = document.querySelector("#static-memory-context-hint");
const staticMemoryNameLabel = document.querySelector("#static-memory-name-label");
const staticMemoryDescriptionLabel = document.querySelector("#static-memory-description-label");
const staticMemorySystemPromptLabel = document.querySelector("#static-memory-system-prompt-label");
const staticMemoryPanelTitle = document.querySelector("#static-memory-panel-title");
const staticMemoryPanelSummary = document.querySelector("#static-memory-panel-summary");
const staticMemoryName = document.querySelector("#static-memory-name");
const staticMemoryDescription = document.querySelector("#static-memory-description");
const staticMemorySystemPrompt = document.querySelector("#static-memory-system-prompt");
const staticMemoryCancel = document.querySelector("#static-memory-cancel");

const knowledgeBaseForm = document.querySelector("#knowledge-base-form");
const knowledgeBaseKey = document.querySelector("#knowledge-base-key");
const knowledgeBaseName = document.querySelector("#knowledge-base-name");
const knowledgeBaseDescription = document.querySelector("#knowledge-base-description");
const knowledgeBaseDocumentAdd = document.querySelector("#knowledge-base-document-add");
const knowledgeBaseDocumentList = document.querySelector("#knowledge-base-document-list");
const knowledgeBaseDocuments = document.querySelector("#knowledge-base-documents");
const knowledgeBaseReset = document.querySelector("#knowledge-base-reset");
const knowledgeBaseList = document.querySelector("#knowledge-base-list");
const knowledgeBaseResult = document.querySelector("#knowledge-base-result");

const reviewPolicyForm = document.querySelector("#review-policy-form");
const reviewPolicyKey = document.querySelector("#review-policy-key");
const reviewPolicyName = document.querySelector("#review-policy-name");
const reviewPolicyDescription = document.querySelector("#review-policy-description");
const reviewPolicyTriggers = document.querySelector("#review-policy-triggers");
const reviewPolicyActions = document.querySelector("#review-policy-actions");
const reviewPolicyPluginKeys = document.querySelector("#review-policy-plugin-keys");
const reviewPolicyRiskTags = document.querySelector("#review-policy-risk-tags");
const reviewPolicyMessageTypes = document.querySelector("#review-policy-message-types");
const reviewPolicyMemoryScopes = document.querySelector("#review-policy-memory-scopes");
const reviewPolicyMemoryKinds = document.querySelector("#review-policy-memory-kinds");
const reviewPolicyReset = document.querySelector("#review-policy-reset");
const reviewPolicyList = document.querySelector("#review-policy-list");
const reviewPolicyResult = document.querySelector("#review-policy-result");

const memoryProfileForm = document.querySelector("#memory-profile-form");
const memoryProfileKey = document.querySelector("#memory-profile-key");
const memoryProfileName = document.querySelector("#memory-profile-name");
const memoryProfileDescription = document.querySelector("#memory-profile-description");
const memoryProfileShortTermEnabled = document.querySelector("#memory-profile-short-term-enabled");
const memoryProfileSummaryTriggerTokens = document.querySelector("#memory-profile-summary-trigger-tokens");
const memoryProfileSummaryMaxTokens = document.querySelector("#memory-profile-summary-max-tokens");
const memoryProfileLongTermEnabled = document.querySelector("#memory-profile-long-term-enabled");
const memoryProfileNamespaceStrategy = document.querySelector("#memory-profile-namespace-strategy");
const memoryProfileTtlDays = document.querySelector("#memory-profile-ttl-days");
const memoryProfileBackgroundEnabled = document.querySelector("#memory-profile-background-enabled");
const memoryProfileDebounceSeconds = document.querySelector("#memory-profile-debounce-seconds");
const memoryProfileReadScopes = document.querySelector("#memory-profile-read-scopes");
const memoryProfileWriteScopes = document.querySelector("#memory-profile-write-scopes");
const memoryProfileReset = document.querySelector("#memory-profile-reset");
const memoryProfileList = document.querySelector("#memory-profile-list");
const memoryProfileResult = document.querySelector("#memory-profile-result");

const agentDefinitionForm = document.querySelector("#agent-definition-form");
const agentDefinitionName = document.querySelector("#agent-definition-name");
const agentDefinitionProvider = document.querySelector("#agent-definition-provider");
const agentDefinitionModel = document.querySelector("#agent-definition-model");
const agentDefinitionStaticMemory = document.querySelector("#agent-definition-static-memory");
const agentDefinitionPlugins = document.querySelector("#agent-definition-plugins");
const agentDefinitionSkills = document.querySelector("#agent-definition-skills");
const agentDefinitionKnowledgeBases = document.querySelector("#agent-definition-knowledge-bases");
const agentDefinitionReviewPolicies = document.querySelector("#agent-definition-review-policies");
const agentDefinitionOpenCreate = document.querySelector("#agent-definition-open-create");
const agentDefinitionList = document.querySelector("#agent-definition-list");
const agentDefinitionPageSize = document.querySelector("#agent-definition-page-size");
const agentDefinitionPaginationMeta = document.querySelector("#agent-definition-pagination-meta");
const agentDefinitionResult = document.querySelector("#agent-definition-result");
const agentDefinitionModal = document.querySelector("#agent-definition-modal");
const agentDefinitionModalTitle = document.querySelector("#agent-definition-modal-title");
const agentDefinitionModalCloseButtons = Array.from(document.querySelectorAll("[data-agent-definition-modal-close]"));
const agentDefinitionCancel = document.querySelector("#agent-definition-cancel");
const agentDefinitionModalResult = document.querySelector("#agent-definition-modal-result");

const agentTemplateForm = document.querySelector("#agent-template-form");
const agentTemplateName = document.querySelector("#agent-template-name");
const agentTemplateRole = document.querySelector("#agent-template-role");
const agentTemplateProvider = document.querySelector("#agent-template-provider");
const agentTemplateModel = document.querySelector("#agent-template-model");
const agentTemplateMemoryPolicy = document.querySelector("#agent-template-memory-policy");
const agentTemplateSkills = document.querySelector("#agent-template-skills");
const agentTemplatePlugins = document.querySelector("#agent-template-plugins");
const agentTemplateGoal = document.querySelector("#agent-template-goal");
const agentTemplateInstructions = document.querySelector("#agent-template-instructions");
const agentTemplateDescription = document.querySelector("#agent-template-description");
const agentTemplateOpenCreate = document.querySelector("#agent-template-open-create");
const agentTemplatePageSize = document.querySelector("#agent-template-page-size");
const agentTemplateList = document.querySelector("#agent-template-list");
const agentTemplatePaginationMeta = document.querySelector("#agent-template-pagination-meta");
const agentTemplateResult = document.querySelector("#agent-template-result");
const agentTemplateModal = document.querySelector("#agent-template-modal");
const agentTemplateModalTitle = document.querySelector("#agent-template-modal-title");
const agentTemplateModalCloseButtons = Array.from(document.querySelectorAll("[data-agent-template-modal-close]"));
const agentTemplateCancel = document.querySelector("#agent-template-cancel");
const agentTemplateModalResult = document.querySelector("#agent-template-modal-result");

const teamDefinitionForm = document.querySelector("#team-definition-form");
const teamDefinitionKey = document.querySelector("#team-definition-key");
const teamDefinitionName = document.querySelector("#team-definition-name");
const teamDefinitionWorkspace = document.querySelector("#team-definition-workspace");
const teamDefinitionProject = document.querySelector("#team-definition-project");
const teamDefinitionDescription = document.querySelector("#team-definition-description");
const teamDefinitionLeadAgentTemplate = document.querySelector("#team-definition-lead-agent-template");
const teamDefinitionEntryMode = document.querySelector("#team-definition-entry-mode");
const teamDefinitionEntryAgent = document.querySelector("#team-definition-entry-agent");
const teamDefinitionSharedKbs = document.querySelector("#team-definition-shared-kbs");
const teamDefinitionSharedStaticMemories = document.querySelector("#team-definition-shared-static-memories");
const teamDefinitionTerminationMode = document.querySelector("#team-definition-termination-mode");
const teamDefinitionTerminationAgents = document.querySelector("#team-definition-termination-agents");
const teamDefinitionMembers = document.querySelector("#team-definition-members");
const teamDefinitionMemberList = document.querySelector("#team-definition-member-list");
const teamDefinitionMemberAdd = document.querySelector("#team-definition-member-add");
const teamDefinitionReviewPolicies = document.querySelector("#team-definition-review-policies");
const teamDefinitionReviewOverrideAdd = document.querySelector("#team-definition-review-override-add");
const teamDefinitionReviewOverrideList = document.querySelector("#team-definition-review-override-list");
const teamDefinitionReviewOverrides = document.querySelector("#team-definition-review-overrides");
const teamDefinitionOpenCreate = document.querySelector("#team-definition-open-create");
const teamDefinitionList = document.querySelector("#team-definition-list");
const teamDefinitionResult = document.querySelector("#team-definition-result");
const teamDefinitionModal = document.querySelector("#team-definition-modal");
const teamDefinitionModalTitle = document.querySelector("#team-definition-modal-title");
const teamDefinitionModalCloseButtons = Array.from(document.querySelectorAll("[data-team-definition-modal-close]"));
const teamDefinitionCancel = document.querySelector("#team-definition-cancel");
const teamDefinitionModalResult = document.querySelector("#team-definition-modal-result");

const tagMultiSelectRegistry = new Map();

const teamTemplateForm = document.querySelector("#team-template-form");
const teamTemplateName = document.querySelector("#team-template-name");
const teamTemplateWorkspace = document.querySelector("#team-template-workspace");
const teamTemplateProject = document.querySelector("#team-template-project");
const teamTemplateDescription = document.querySelector("#team-template-description");
const teamTemplateAgents = document.querySelector("#team-template-agents");
const teamTemplateFlow = document.querySelector("#team-template-flow");
const teamTemplateDod = document.querySelector("#team-template-dod");
const teamTemplateChecks = document.querySelector("#team-template-checks");
const teamTemplatePreview = document.querySelector("#team-template-preview");
const teamTemplateList = document.querySelector("#team-template-list");
const teamTemplateResult = document.querySelector("#team-template-result");
const teamMemberList = document.querySelector("#team-member-list");
const teamGraphStage = document.querySelector("#team-graph-stage");
const teamGraphCanvas = document.querySelector("#team-graph-canvas");
const teamGraphEdges = document.querySelector("#team-graph-edges");
const teamGraphValidate = document.querySelector("#team-graph-validate");
const teamGraphPreview = document.querySelector("#team-graph-preview");
const teamGraphAutolayout = document.querySelector("#team-graph-autolayout");
const teamValidationResult = document.querySelector("#team-validation-result");
const teamLinkHint = document.querySelector("#team-link-hint");
const teamNodeEmpty = document.querySelector("#team-node-empty");
const teamNodeFormShell = document.querySelector("#team-node-form-shell");
const teamNodeId = document.querySelector("#team-node-id");
const teamNodeType = document.querySelector("#team-node-type");
const teamNodeName = document.querySelector("#team-node-name");
const teamNodeAgent = document.querySelector("#team-node-agent");
const teamNodeInstruction = document.querySelector("#team-node-instruction");
const teamNodeExpr = document.querySelector("#team-node-expr");
const teamNodeMaxIterations = document.querySelector("#team-node-max-iterations");
const teamNodeArtifactKind = document.querySelector("#team-node-artifact-kind");
const teamNodeArtifactName = document.querySelector("#team-node-artifact-name");
const teamNodeTemplate = document.querySelector("#team-node-template");
const teamNodeSource = document.querySelector("#team-node-source");
const teamEdgeList = document.querySelector("#team-edge-list");
const teamNodeLink = document.querySelector("#team-node-link");
const teamNodeDelete = document.querySelector("#team-node-delete");

const buildForm = document.querySelector("#build-form");
const buildTeamTemplate = document.querySelector("#build-team-template");
const buildName = document.querySelector("#build-name");
const buildList = document.querySelector("#build-list");
const buildDetail = document.querySelector("#build-detail");
const buildResult = document.querySelector("#build-result");

const taskForm = document.querySelector("#task-form");
const taskTeamDefinition = document.querySelector("#task-team-definition");
const taskBuild = document.querySelector("#task-build");
const taskBlueprint = document.querySelector("#task-blueprint");
const taskTitle = document.querySelector("#task-title");
const taskApprovalMode = document.querySelector("#task-approval-mode");
const taskPrompt = document.querySelector("#task-prompt");
const taskResult = document.querySelector("#task-result");
const runList = document.querySelector("#run-list");
const runDetail = document.querySelector("#run-detail");
const approvalList = document.querySelector("#approval-list");
const blueprintList = document.querySelector("#blueprint-list");
const blueprintDetail = document.querySelector("#blueprint-detail");

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value).replace(/\r?\n/g, "&#10;");
}

function providerModelTooltipText(models, fallbackModelName = "") {
  if (Array.isArray(models) && models.length) {
    return models
      .map((item, index) => {
        const typeLabel = MODEL_TYPE_LABELS[item.model_type] || item.model_type || "-";
        const context = item.context_window ? ` / 上下文 ${item.context_window}` : "";
        return `${index + 1}. ${item.name || "-"} / ${typeLabel}${context}`;
      })
      .join("\n");
  }
  if (fallbackModelName) {
    return `默认模型：${fallbackModelName}`;
  }
  return "暂无模型";
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = formatApiError(payload, response.statusText);
    const error = new Error(message);
    error.payload = payload;
    throw error;
  }
  return payload;
}

function formatApiError(payload, fallbackMessage = "Request failed") {
  if (!payload || typeof payload !== "object") {
    return fallbackMessage;
  }
  const lines = [];
  if (typeof payload.detail === "string" && payload.detail.trim()) {
    lines.push(payload.detail.trim());
  }
  if (Array.isArray(payload.errors)) {
    payload.errors
      .map((item) => (typeof item === "string" ? item.trim() : ""))
      .filter(Boolean)
      .forEach((item) => lines.push(item));
  }
  return lines.length ? lines.join("\n") : fallbackMessage;
}

function errorResult(error) {
  if (error && typeof error === "object" && error.payload && typeof error.payload === "object" && Object.keys(error.payload).length) {
    return error.payload;
  }
  return { error: error?.message || "Request failed." };
}

function showResult(target, value) {
  if (!target) {
    return;
  }
  target.classList.remove("hidden");
  target.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

function hideResult(target) {
  if (!target) {
    return;
  }
  target.classList.add("hidden");
  target.textContent = "";
}

async function switchPage(pageName, options = {}) {
  if (pageName === "static-memories" || pageName === "role-management" || pageName === "team-rules") {
    pageName = "responsibility-specs";
  }
  state.activePage = pageName;
  state.activeNavSection = PAGE_SECTIONS[pageName] || "overview";
  topNavButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.navSection === state.activeNavSection);
  });
  subMenuPanels.forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.navPanel === state.activeNavSection);
  });
  navButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.pageTarget === pageName);
  });
  pageViews.forEach((view) => {
    view.classList.toggle("active", view.dataset.page === pageName);
  });
  try {
    await ensurePageData(pageName, { ...options, force: true });
  } catch (error) {
    showResult(taskResult, errorResult(error));
  }
}

async function switchNavSection(sectionName) {
  const targetPage = SECTION_DEFAULT_PAGE[sectionName] || "overview";
  if (PAGE_SECTIONS[state.activePage] === sectionName) {
    topNavButtons.forEach((button) => {
      button.classList.toggle("active", button.dataset.navSection === sectionName);
    });
    subMenuPanels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.navPanel === sectionName);
    });
    state.activeNavSection = sectionName;
    try {
      await ensurePageData(state.activePage, { force: true });
    } catch (error) {
      showResult(taskResult, errorResult(error));
    }
    return;
  }
  await switchPage(targetPage);
}

function linesToList(value) {
  return String(value || "")
    .split("\n")
    .map((item) => item.trim())
    .filter(Boolean);
}

function listToLines(items) {
  return (items || []).join("\n");
}

function safeParseJson(text, fallback) {
  const value = String(text || "").trim();
  if (!value) {
    return fallback;
  }
  return JSON.parse(value);
}

function prettyJson(value) {
  return JSON.stringify(value, null, 2);
}

function commaListToArray(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function getMultiSelectValues(select) {
  return Array.from(select.selectedOptions).map((option) => option.value);
}

function setMultiSelectValues(select, values) {
  const active = new Set(values || []);
  Array.from(select.options).forEach((option) => {
    option.selected = active.has(option.value);
  });
  syncTagMultiSelect(select);
}

function fieldValue(target) {
  if (target instanceof HTMLSelectElement && target.multiple) {
    return getMultiSelectValues(target);
  }
  if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement || target instanceof HTMLSelectElement) {
    return target.value;
  }
  return "";
}

function cardMarkup({ title, body, meta = "", actions = "", active = false, attrs = "" }) {
  return `
    <article class="card ${active ? "active" : ""}" ${attrs}>
      <h3>${escapeHtml(title)}</h3>
      <p>${escapeHtml(body)}</p>
      <div class="meta">${escapeHtml(meta)}</div>
      ${actions ? `<div class="card-actions">${actions}</div>` : ""}
    </article>
  `;
}

function chip(label, value) {
  return `<span class="system-chip"><strong>${escapeHtml(label)}</strong>${escapeHtml(value)}</span>`;
}

function currentAgentTemplateMetadata() {
  const template =
    state.agentTemplatePage.items.find((item) => item.id === state.editingAgentTemplateId) ||
    state.agentTemplates.find((item) => item.id === state.editingAgentTemplateId) ||
    null;
  return { ...(template?.spec_json?.metadata || {}) };
}

function buildTeamTemplateLabel(item) {
  const lock = item?.resource_lock_json?.team_template || {};
  return lock.name || state.teamTemplates.find((entry) => entry.id === item?.team_template_id)?.name || item?.team_template_id || "-";
}

function pluginDisplayName(item) {
  return item?.name || "未命名插件";
}

function pluginDisplaySubtitle(item) {
  return item?.description || item?.plugin_type || "未配置说明";
}

function teamSummary(spec) {
  return {
    agents: Array.isArray(spec?.agents) ? spec.agents : [],
    nodes: Array.isArray(spec?.flow?.nodes) ? spec.flow.nodes : [],
    edges: Array.isArray(spec?.flow?.edges) ? spec.flow.edges : [],
    definitionOfDone: Array.isArray(spec?.definition_of_done) ? spec.definition_of_done : [],
    acceptanceChecks: Array.isArray(spec?.acceptance_checks) ? spec.acceptance_checks : [],
  };
}

function blueprintSummary(spec) {
  return {
    roleTemplates: Object.keys(spec?.role_templates || {}),
    agents: Object.keys(spec?.agents || {}),
    nodes: Array.isArray(spec?.flow?.nodes) ? spec.flow.nodes : [],
    edges: Array.isArray(spec?.flow?.edges) ? spec.flow.edges : [],
  };
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function initializeTagMultiSelects() {
  document.querySelectorAll("[data-tag-select]").forEach((root) => {
    const selectId = root.dataset.tagSelect;
    if (!selectId) {
      return;
    }
    const select = document.getElementById(selectId);
    if (!(select instanceof HTMLSelectElement) || tagMultiSelectRegistry.has(select)) {
      return;
    }
    root.innerHTML = `
      <div class="tag-multi-select-control" data-tag-select-control>
        <div class="tag-multi-select-tags" data-tag-select-tags></div>
        <input type="text" class="tag-multi-select-input" data-tag-select-input autocomplete="off" spellcheck="false" />
      </div>
      <div class="tag-multi-select-menu hidden" data-tag-select-menu></div>
    `;
    const input = root.querySelector("[data-tag-select-input]");
    const tags = root.querySelector("[data-tag-select-tags]");
    const menu = root.querySelector("[data-tag-select-menu]");
    if (!(input instanceof HTMLInputElement) || !(tags instanceof HTMLElement) || !(menu instanceof HTMLElement)) {
      return;
    }
    const ui = {
      root,
      select,
      input,
      tags,
      menu,
      open: false,
      placeholder: root.dataset.placeholder || "搜索后点击选择",
      emptySelectionLabel: root.dataset.emptySelectionLabel || "可多选，也可以不选",
      emptyListLabel: root.dataset.emptyListLabel || "暂无可选项",
      noMatchLabel: root.dataset.noMatchLabel || "没有匹配项",
    };
    input.addEventListener("focus", () => {
      openTagMultiSelect(select);
    });
    input.addEventListener("input", () => {
      ui.open = true;
      renderTagMultiSelect(select);
    });
    root.addEventListener("pointerdown", (event) => {
      if (!(event.target instanceof Element)) {
        return;
      }
      if (event.target instanceof HTMLInputElement) {
        return;
      }
      if (event.target.closest("[data-tag-select-option]") || event.target.closest("[data-tag-select-remove]")) {
        event.preventDefault();
      }
    });
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        const first = filterTagMultiSelectItems(select, input.value)[0];
        if (first) {
          setTagMultiSelectOption(select, first.value, true);
        }
        return;
      }
      if (event.key === "Backspace" && !input.value.trim()) {
        const selected = tagMultiSelectItems(select).filter((item) => item.selected);
        const last = selected[selected.length - 1];
        if (last) {
          event.preventDefault();
          setTagMultiSelectOption(select, last.value, false);
        }
        return;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        closeTagMultiSelect(select);
      }
    });
    root.addEventListener("click", (event) => {
      if (!(event.target instanceof Element)) {
        return;
      }
      const remove = event.target.closest("[data-tag-select-remove]");
      if (remove) {
        event.preventDefault();
        event.stopPropagation();
        setTagMultiSelectOption(select, remove.dataset.tagSelectRemove || "", false);
        input.focus();
        return;
      }
      const option = event.target.closest("[data-tag-select-option]");
      if (option) {
        event.preventDefault();
        event.stopPropagation();
        const value = option.dataset.tagSelectOption || "";
        const items = tagMultiSelectItems(select);
        const current = items.find((item) => item.value === value) || null;
        setTagMultiSelectOption(select, value, !current?.selected);
        input.focus();
        return;
      }
      openTagMultiSelect(select);
      input.focus();
    });
    tagMultiSelectRegistry.set(select, ui);
    renderTagMultiSelect(select);
  });
}

function tagMultiSelectItems(select) {
  return Array.from(select?.options || [])
    .filter((option) => option.value)
    .map((option) => ({
      value: option.value,
      label: option.textContent || option.value,
      selected: option.selected,
    }));
}

function filterTagMultiSelectItems(select, query = "") {
  const normalized = String(query || "").trim().toLowerCase();
  const items = tagMultiSelectItems(select);
  if (!normalized) {
    return items;
  }
  return items.filter((item) => item.label.toLowerCase().includes(normalized));
}

function renderTagMultiSelect(select) {
  const ui = tagMultiSelectRegistry.get(select);
  if (!ui) {
    return;
  }
  const items = tagMultiSelectItems(select);
  const selectedItems = items.filter((item) => item.selected);
  const filteredItems = filterTagMultiSelectItems(select, ui.input.value);
  ui.root.classList.toggle("open", ui.open);
  ui.root.setAttribute("aria-expanded", ui.open ? "true" : "false");
  ui.tags.innerHTML = selectedItems.length
    ? selectedItems
        .map(
          (item) => `
            <span class="tag-multi-select-token" title="${escapeAttribute(item.label)}">
              <span class="tag-multi-select-token-label">${escapeHtml(item.label)}</span>
              <button
                type="button"
                class="tag-multi-select-token-remove"
                data-tag-select-remove="${escapeAttribute(item.value)}"
                aria-label="${escapeAttribute(`移除 ${item.label}`)}"
              >
                ×
              </button>
            </span>
          `,
        )
        .join("")
    : `<span class="tag-multi-select-placeholder">${escapeHtml(ui.emptySelectionLabel)}</span>`;
  ui.input.placeholder = selectedItems.length ? "继续搜索添加" : ui.placeholder;
  ui.menu.classList.toggle("hidden", !ui.open);
  if (!ui.open) {
    ui.menu.innerHTML = "";
    return;
  }
  ui.menu.innerHTML = filteredItems.length
    ? filteredItems
        .map(
          (item) => `
            <button
              type="button"
              class="tag-multi-select-option ${item.selected ? "selected" : ""}"
              data-tag-select-option="${escapeAttribute(item.value)}"
              title="${escapeAttribute(item.label)}"
            >
              <span class="tag-multi-select-check">${item.selected ? "✓" : ""}</span>
              <span class="tag-multi-select-option-label">${escapeHtml(item.label)}</span>
            </button>
          `,
        )
        .join("")
    : `<div class="tag-multi-select-empty">${escapeHtml(items.length ? ui.noMatchLabel : ui.emptyListLabel)}</div>`;
}

function openTagMultiSelect(select) {
  const ui = tagMultiSelectRegistry.get(select);
  if (!ui) {
    return;
  }
  closeAllTagMultiSelects({ except: select });
  ui.open = true;
  renderTagMultiSelect(select);
}

function closeTagMultiSelect(select) {
  const ui = tagMultiSelectRegistry.get(select);
  if (!ui) {
    return;
  }
  ui.open = false;
  ui.input.value = "";
  renderTagMultiSelect(select);
}

function closeAllTagMultiSelects({ except = null } = {}) {
  tagMultiSelectRegistry.forEach((ui, select) => {
    if (select === except) {
      return;
    }
    ui.open = false;
    ui.input.value = "";
    renderTagMultiSelect(select);
  });
}

function syncTagMultiSelect(select) {
  if (!tagMultiSelectRegistry.has(select)) {
    return;
  }
  renderTagMultiSelect(select);
}

function setTagMultiSelectOption(select, value, nextSelected) {
  const option = Array.from(select?.options || []).find((item) => item.value === value);
  if (!option) {
    return;
  }
  option.selected = Boolean(nextSelected);
  const ui = tagMultiSelectRegistry.get(select);
  if (ui) {
    ui.open = true;
  }
  syncTagMultiSelect(select);
  select.dispatchEvent(new Event("change", { bubbles: true }));
}

function deepMerge(base, updates) {
  if (Array.isArray(base) || Array.isArray(updates)) {
    return clone(updates ?? base ?? []);
  }
  if (!base || typeof base !== "object") {
    return clone(updates ?? base ?? {});
  }
  if (!updates || typeof updates !== "object") {
    return clone(base);
  }
  const merged = { ...base };
  Object.entries(updates).forEach(([key, value]) => {
    const current = merged[key];
    if (current && typeof current === "object" && !Array.isArray(current) && value && typeof value === "object" && !Array.isArray(value)) {
      merged[key] = deepMerge(current, value);
      return;
    }
    merged[key] = clone(value);
  });
  return merged;
}

function nestedValue(source, path, fallback = undefined) {
  const parts = Array.isArray(path) ? path : String(path || "").split(".").filter(Boolean);
  let current = source;
  for (const part of parts) {
    if (!current || typeof current !== "object" || !(part in current)) {
      return fallback;
    }
    current = current[part];
  }
  return current === undefined ? fallback : current;
}

function assignNestedValue(target, path, value) {
  const parts = Array.isArray(path) ? path : String(path || "").split(".").filter(Boolean);
  if (!parts.length) {
    return target;
  }
  let current = target;
  for (const part of parts.slice(0, -1)) {
    if (!current[part] || typeof current[part] !== "object" || Array.isArray(current[part])) {
      current[part] = {};
    }
    current = current[part];
  }
  current[parts[parts.length - 1]] = value;
  return target;
}

function pluginConfigPathKey(path) {
  return Array.isArray(path) ? path.join(".") : String(path || "");
}

function pluginConfigFieldIsSecret(path, schema) {
  const format = String(schema?.format || "").trim().toLowerCase();
  const key = String(Array.isArray(path) ? path[path.length - 1] || "" : path || "").trim().toLowerCase();
  if (schema?.writeOnly || schema?.["x-secret"]) {
    return true;
  }
  if (format === "password" || format === "secret") {
    return true;
  }
  return /(secret|token|password|api[_-]?key|encrypt[_-]?key)/i.test(key);
}

function pluginEditorSchema() {
  return state.pluginEditor?.schema && typeof state.pluginEditor.schema === "object" ? state.pluginEditor.schema : null;
}

function pluginEditorManifest() {
  return state.pluginEditor?.manifest && typeof state.pluginEditor.manifest === "object" ? state.pluginEditor.manifest : {};
}

function pluginEditorConfig() {
  return state.pluginEditor?.config && typeof state.pluginEditor.config === "object" ? state.pluginEditor.config : {};
}

function pluginEditorSecretPaths() {
  return new Set(Array.isArray(state.pluginEditor?.secretFieldPaths) ? state.pluginEditor.secretFieldPaths : []);
}

function coercePluginSchemaValue(rawValue, schema) {
  const type = String(schema?.type || "").trim().toLowerCase();
  if (type === "integer") {
    const parsed = Number.parseInt(String(rawValue ?? "").trim(), 10);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  if (type === "number") {
    const parsed = Number.parseFloat(String(rawValue ?? "").trim());
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return String(rawValue ?? "");
}

function parsePluginConfigTextareaValue(rawValue, schema) {
  const text = String(rawValue ?? "").trim();
  if (!text) {
    if (schema?.type === "array") {
      return [];
    }
    return schema?.type === "object" ? {} : "";
  }
  if (schema?.type === "array") {
    const itemSchema = schema?.items || {};
    return text
      .split("\n")
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => coercePluginSchemaValue(item, itemSchema));
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    return {};
  }
}

function invalidateData(...keys) {
  keys.forEach((key) => {
    if (Object.prototype.hasOwnProperty.call(state.loaded, key)) {
      state.loaded[key] = false;
    }
  });
}

function teamEditorSpec() {
  return state.teamEditor.spec || buildTeamTemplateSpecFromForm();
}

function setTeamEditorSpec(spec) {
  state.teamEditor.spec = clone(spec);
  syncTeamEditorToForm();
  renderTeamEditor();
}

function syncTeamEditorToForm() {
  const spec = teamEditorSpec();
  teamTemplateAgents.value = prettyJson(spec.agents || []);
  teamTemplateFlow.value = prettyJson(spec.flow || { nodes: [], edges: [] });
  teamTemplateDod.value = listToLines(spec.definition_of_done || []);
  teamTemplateChecks.value = listToLines(spec.acceptance_checks || []);
}

function ensureTeamEditorMetadata(spec) {
  spec.metadata = spec.metadata || {};
  spec.metadata.communication_policy = spec.metadata.communication_policy || "graph-ancestor-scoped";
  spec.metadata.ui_layout = spec.metadata.ui_layout || {};
  spec.metadata.ui_layout.positions = spec.metadata.ui_layout.positions || {};
  spec.metadata.ui_layout.viewport = spec.metadata.ui_layout.viewport || { x: 0, y: 0, zoom: 1 };
  return spec;
}

function defaultNodeConfig(type) {
  switch (type) {
    case "agent":
      return { instruction: "补充节点指令", agent: teamEditorSpec().agents?.[0]?.key || "" };
    case "condition":
      return { expr: "review.pass == true" };
    case "router":
      return { expr: "" };
    case "loop":
      return { max_iterations: 2 };
    case "approval":
      return { name: "人工审批" };
    case "artifact":
      return { name: "artifact.md", artifact_kind: "report", template: "{{review.summary}}" };
    default:
      return {};
  }
}

function createNodeId(type) {
  const base = String(type || "node")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "node";
  const existing = new Set((teamEditorSpec().flow?.nodes || []).map((item) => item.id));
  let index = 1;
  let candidate = `${base}_${index}`;
  while (existing.has(candidate) || candidate === "start") {
    index += 1;
    candidate = `${base}_${index}`;
  }
  return candidate;
}

function graphPositionFor(nodeId, index = 0) {
  const spec = ensureTeamEditorMetadata(teamEditorSpec());
  const positions = spec.metadata.ui_layout.positions || {};
  if (!positions[nodeId]) {
    positions[nodeId] = {
      x: 120 + (index % 4) * 220,
      y: 80 + Math.floor(index / 4) * 140,
    };
  }
  return positions[nodeId];
}

function autoLayoutTeamSpec(spec) {
  const nodes = spec.flow?.nodes || [];
  const edges = spec.flow?.edges || [];
  const outgoing = new Map();
  nodes.forEach((node) => outgoing.set(node.id, []));
  edges.forEach((edge) => {
    const items = outgoing.get(edge.from) || [];
    items.push(edge.to);
    outgoing.set(edge.from, items);
  });
  const start = nodes.find((node) => node.type === "start")?.id || nodes[0]?.id;
  const queue = start ? [[start, 0]] : [];
  const visited = new Set();
  const columns = new Map();
  while (queue.length) {
    const [nodeId, depth] = queue.shift();
    if (visited.has(nodeId)) {
      continue;
    }
    visited.add(nodeId);
    const items = columns.get(depth) || [];
    items.push(nodeId);
    columns.set(depth, items);
    (outgoing.get(nodeId) || []).forEach((target) => queue.push([target, depth + 1]));
  }
  nodes.forEach((node) => {
    if (visited.has(node.id)) {
      return;
    }
    const depth = columns.size;
    const items = columns.get(depth) || [];
    items.push(node.id);
    columns.set(depth, items);
  });
  ensureTeamEditorMetadata(spec);
  columns.forEach((items, depth) => {
    items.forEach((nodeId, row) => {
      spec.metadata.ui_layout.positions[nodeId] = {
        x: 120 + depth * 220,
        y: 80 + row * 140,
      };
    });
  });
}

function setDetailPlaceholder(target, text) {
  target.classList.add("empty");
  target.textContent = text;
}

function populateProviderTypeOptions() {
  const options = state.providerTypes.map((item) => `<option value="${escapeHtml(item.provider_type)}">${escapeHtml(item.label)}</option>`).join("");
  providerType.innerHTML = options;
  if (!providerType.value && state.providerTypes.length) {
    providerType.value = state.providerTypes[0].provider_type;
  }
}

function populateProviderOptions() {
  const options = state.providers.map((item) => `<option value="${item.id}">${escapeHtml(item.name)} / ${escapeHtml(item.provider_type)}</option>`).join("");
  agentTemplateProvider.innerHTML = options || '<option value="">暂无 Provider</option>';
}

function pluginKeyOptions() {
  const seen = new Set();
  const items = [];
  for (const item of state.builtinPlugins || []) {
    const key = String(item.key || "").trim();
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    items.push({
      value: key,
      label: `${item.name || key} / ${key} / builtin`,
    });
  }
  for (const item of state.plugins || []) {
    const key = String(item.key || "").trim();
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    items.push({
      value: key,
      label: `${item.name || key} / ${key}`,
    });
  }
  return items;
}

function providerModelsByType(providerId, modelType) {
  const provider = state.providers.find((item) => item.id === providerId) || null;
  const models = Array.isArray(provider?.config_json?.models) ? provider.config_json.models : [];
  return models.filter((item) => item.model_type === modelType);
}

function providerOptionsByModelType(modelType) {
  return state.providers.filter((item) => providerModelsByType(item.id, modelType).length > 0);
}

function renderProviderSelect(select, items, selectedValue = "") {
  if (!select) {
    return;
  }
  const options = items.length
    ? items
        .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)} / ${escapeHtml(item.provider_type)}</option>`)
        .join("")
    : '<option value="">暂无可用 Provider</option>';
  select.innerHTML = options;
  if (selectedValue && items.some((item) => item.id === selectedValue)) {
    select.value = selectedValue;
    return;
  }
  select.value = items[0]?.id || "";
}

function renderModelSelect(select, models, selectedValue = "") {
  if (!select) {
    return;
  }
  const options = models.length
    ? models
        .map((item) => `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name)}</option>`)
        .join("")
    : '<option value="">暂无模型</option>';
  select.innerHTML = options;
  if (selectedValue && models.some((item) => item.name === selectedValue)) {
    select.value = selectedValue;
    return;
  }
  select.value = models[0]?.name || "";
}

function renderSingleSelect(select, items, selectedValue = "", fallbackLabel = "暂无数据", options = {}) {
  if (!select) {
    return;
  }
  const allowBlank = Boolean(options.allowBlank);
  const blankLabel = options.blankLabel || "不选择";
  const selectItems = allowBlank ? [{ value: "", label: blankLabel }, ...items] : items;
  const markup = selectItems.length
    ? selectItems.map((item) => `<option value="${escapeHtml(item.value)}">${escapeHtml(item.label)}</option>`).join("")
    : `<option value="">${escapeHtml(fallbackLabel)}</option>`;
  select.innerHTML = markup;
  if (selectedValue && selectItems.some((item) => item.value === selectedValue)) {
    select.value = selectedValue;
    return;
  }
  select.value = allowBlank ? "" : selectItems[0]?.value || "";
}

function renderMultiSelect(select, items) {
  if (!select) {
    return;
  }
  const selected = getMultiSelectValues(select);
  select.innerHTML = items.length
    ? items.map((item) => `<option value="${escapeHtml(item.value)}">${escapeHtml(item.label)}</option>`).join("")
    : '<option value="">暂无数据</option>';
  setMultiSelectValues(
    select,
    selected.filter((value) => items.some((item) => item.value === value)),
  );
  syncTagMultiSelect(select);
}

function staticMemoryMode(mode) {
  return "role";
}

function staticMemoryModeConfig(mode = state.staticMemoryEditorMode) {
  return STATIC_MEMORY_MODE_CONFIG[staticMemoryMode(mode)];
}

function staticMemoryModeFromPage(pageName = state.activePage) {
  return "role";
}

function currentStaticMemoryMode() {
  return staticMemoryMode(state.staticMemoryEditorMode || staticMemoryModeFromPage(state.activePage));
}

function staticMemorySpec(item) {
  return dictOrEmpty(item?.spec_json || item?.spec);
}

function staticMemoryAppliesTo(item) {
  return "role_spec";
}

function staticMemoryVisibleInMode(item, mode) {
  return true;
}

function staticMemoryCompatibilityTag(item) {
  return "角色管理";
}

function staticMemorySummary(text, fallback = "-") {
  const value = String(text || "").trim();
  if (!value) {
    return fallback;
  }
  const singleLine = value.replace(/\s+/g, " ").trim();
  return singleLine.length > 96 ? `${singleLine.slice(0, 96)}...` : singleLine;
}

function staticMemorySelectableItems(mode, includeRefs = []) {
  const includeSet = new Set(
    (includeRefs || [])
      .map((value) => String(value || "").trim())
      .filter(Boolean),
  );
  const visible = state.staticMemories.filter(
    (item) => staticMemoryVisibleInMode(item, mode) || includeSet.has(String(item.id || "").trim()) || includeSet.has(String(item.key || "").trim()),
  );
  const deduped = [];
  const seen = new Set();
  visible.forEach((item) => {
    const key = String(item.id || item.key || "").trim();
    if (!key || seen.has(key)) {
      return;
    }
    seen.add(key);
    deduped.push(item);
  });
  return deduped;
}

function staticMemoryOptions(mode, includeRefs = []) {
  return staticMemorySelectableItems(mode, includeRefs).map((item) => ({
    value: item.id,
    label: item.name || item.key || item.id,
  }));
}

function staticMemoryResultTarget(mode = currentStaticMemoryMode()) {
  return responsibilitySpecResult;
}

function syncStaticMemoryEditorMode(mode = currentStaticMemoryMode()) {
  const config = staticMemoryModeConfig(mode);
  state.staticMemoryEditorMode = staticMemoryMode(mode);
  if (staticMemoryModalTitle) {
    staticMemoryModalTitle.textContent = state.editingStaticMemoryId ? config.modalTitleEdit : config.modalTitleCreate;
  }
  if (staticMemoryNameLabel) {
    staticMemoryNameLabel.textContent = "角色名称";
  }
  if (staticMemoryDescriptionLabel) {
    staticMemoryDescriptionLabel.textContent = config.descriptionLabel;
  }
  if (staticMemoryPanelTitle) {
    staticMemoryPanelTitle.textContent = config.panelTitle;
  }
  if (staticMemoryName) {
    staticMemoryName.placeholder = config.namePlaceholder;
  }
  if (staticMemoryDescription) {
    staticMemoryDescription.placeholder = config.descriptionPlaceholder;
  }
  if (staticMemorySystemPrompt) {
    staticMemorySystemPrompt.placeholder = config.promptPlaceholder;
  }
}

function uiMetadataSection(section) {
  const source = state.uiMetadata && typeof state.uiMetadata === "object" ? state.uiMetadata : DEFAULT_UI_METADATA;
  const fallback = DEFAULT_UI_METADATA[section];
  const value = source[section];
  if (value && typeof value === "object") {
    return value;
  }
  return fallback && typeof fallback === "object" ? fallback : {};
}

function uiMetadataOptions(section, key) {
  const items = uiMetadataSection(section)[key];
  if (Array.isArray(items) && items.length) {
    return items;
  }
  const fallback = DEFAULT_UI_METADATA[section]?.[key];
  return Array.isArray(fallback) ? fallback : [];
}

function multiSelectOptionsMarkup(items, selectedValues = []) {
  const selected = new Set(selectedValues || []);
  return items
    .map(
      (item) =>
        `<option value="${escapeHtml(item.value)}"${selected.has(item.value) ? " selected" : ""}>${escapeHtml(item.label)}</option>`,
    )
    .join("");
}

function populatePluginOptions() {
  renderMultiSelect(
    agentTemplatePlugins,
    state.plugins.map((item) => ({ value: item.id, label: `${item.name || item.key || item.id} / ${item.version || "-"}` })),
  );
}

function populateReviewPolicyPluginOptions() {
  renderMultiSelect(reviewPolicyPluginKeys, pluginKeyOptions());
}

function populateReviewPolicyConditionOptions() {
  renderMultiSelect(reviewPolicyTriggers, uiMetadataOptions("review_policy", "triggers"));
  renderMultiSelect(reviewPolicyActions, uiMetadataOptions("review_policy", "actions"));
  renderMultiSelect(reviewPolicyMessageTypes, uiMetadataOptions("review_policy", "message_types"));
  renderMultiSelect(reviewPolicyMemoryScopes, uiMetadataOptions("review_policy", "memory_scopes"));
  renderMultiSelect(reviewPolicyMemoryKinds, uiMetadataOptions("review_policy", "memory_kinds"));
}

function populateTeamDefinitionReviewPolicyOptions() {
  renderMultiSelect(
    teamDefinitionReviewPolicies,
    state.reviewPolicies.map((item) => ({ value: item.id, label: `${item.name || item.key || item.id} / ${item.key || item.id}` })),
  );
}

function populateAgentTemplateReferenceOptions() {
  renderProviderSelect(agentTemplateProvider, state.providers, agentTemplateProvider?.value || "");
  renderModelSelect(agentTemplateModel, providerModelsByType(agentTemplateProvider?.value || "", "chat"), agentTemplateModel?.value || "");
  renderMultiSelect(
    agentTemplateSkills,
    state.skills.map((item) => ({ value: item.id, label: `${item.name || item.key || item.id} / ${item.key || item.id}` })),
  );
  populatePluginOptions();
}

function populateStaticMemoryScopeOptions() {
  renderMultiSelect(memoryProfileReadScopes, uiMetadataOptions("memory_profile", "scopes"));
  renderMultiSelect(memoryProfileWriteScopes, uiMetadataOptions("memory_profile", "scopes"));
}

function populateAgentDefinitionReferenceOptions(includeStaticMemoryRefs = []) {
  renderProviderSelect(agentDefinitionProvider, state.providers, agentDefinitionProvider?.value || "");
  renderModelSelect(agentDefinitionModel, providerModelsByType(agentDefinitionProvider?.value || "", "chat"), agentDefinitionModel?.value || "");
  renderSingleSelect(
    agentDefinitionStaticMemory,
    staticMemoryOptions("role", includeStaticMemoryRefs),
    agentDefinitionStaticMemory?.value || "",
    "暂无角色管理项",
  );
  renderMultiSelect(
    agentDefinitionPlugins,
    state.plugins.map((item) => ({ value: item.id, label: `${item.name || item.key || item.id} / ${item.version || "-"}` })),
  );
  renderMultiSelect(
    agentDefinitionSkills,
    state.skills.map((item) => ({ value: item.id, label: `${item.name || item.key || item.id} / ${item.key || item.id}` })),
  );
  renderSingleSelect(
    agentDefinitionKnowledgeBases,
    state.knowledgeBases.map((item) => ({ value: item.id, label: `${item.name || item.key || item.id} / ${item.key || item.id}` })),
    agentDefinitionKnowledgeBases?.value || "",
    "暂无知识库",
    { allowBlank: true, blankLabel: "不选择" },
  );
  renderSingleSelect(
    agentDefinitionReviewPolicies,
    state.reviewPolicies.map((item) => ({ value: item.id, label: `${item.name || item.key || item.id} / ${item.key || item.id}` })),
    agentDefinitionReviewPolicies?.value || "",
    "暂无审核策略",
    { allowBlank: true, blankLabel: "不选择" },
  );
}

const TEAM_DEFINITION_CHILD_SOURCE_KIND_OPTIONS = [
  { value: "agent_template", label: "Agent 模板" },
  { value: "team_definition", label: "团队" },
];

function teamDefinitionSourceKind(value) {
  return String(value || "").trim() === "team_definition" ? "team_definition" : "agent_template";
}

function normalizeTeamDefinitionReference(sourceKind, value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  if (teamDefinitionSourceKind(sourceKind) === "team_definition") {
    const matched = state.teamDefinitions.find((item) => item.id === raw || item.key === raw || item.name === raw) || null;
    return matched?.id || raw;
  }
  const matched =
    state.agentTemplates.find((item) => item.id === raw) ||
    state.agentTemplates.find((item) => String(dictOrEmpty(item.spec_json).metadata?.builtin_ref || "").trim() === raw) ||
    state.agentTemplates.find((item) => item.name === raw) ||
    null;
  return matched?.id || raw;
}

function normalizeTeamDefinitionMember(item) {
  const payload = item && typeof item === "object" ? { ...item } : {};
  let sourceKind = String(payload.source_kind || "").trim();
  if (!sourceKind) {
    sourceKind = payload.team_definition_ref || payload.team_definition_id ? "team_definition" : "agent_template";
  }
  sourceKind = teamDefinitionSourceKind(sourceKind);
  const kind = sourceKind === "team_definition" ? "team" : "agent";
  const sourceRef =
    sourceKind === "team_definition"
      ? String(payload.team_definition_ref || payload.team_definition_id || payload.source_ref || "").trim()
      : String(payload.agent_template_ref || payload.agent_template_id || payload.source_ref || "").trim();
  return {
    kind,
    source_kind: sourceKind,
    source_ref: normalizeTeamDefinitionReference(sourceKind, sourceRef),
    name: String(payload.name || "").trim(),
  };
}

function isDeepTeamDefinitionRecord(item) {
  const spec = dictOrEmpty(item?.spec_json);
  return Boolean((spec.root && typeof spec.root === "object") || (spec.lead && typeof spec.lead === "object"));
}

function teamDefinitionReferenceOptions(sourceKind, { excludeDefinitionId = state.editingTeamDefinitionId } = {}) {
  if (teamDefinitionSourceKind(sourceKind) === "team_definition") {
    return state.teamDefinitions
      .filter((item) => isDeepTeamDefinitionRecord(item))
      .filter((item) => item.id !== excludeDefinitionId)
      .map((item) => ({ value: item.id, label: item.name || item.id }));
  }
  return state.agentTemplates.map((item) => ({ value: item.id, label: item.name || item.id }));
}

function teamDefinitionSelectOptionsMarkup(items, selectedValue = "", { allowBlank = false, blankLabel = "请选择", fallbackLabel = "暂无可选项" } = {}) {
  const selectItems = allowBlank ? [{ value: "", label: blankLabel }, ...items] : items;
  return selectItems.length
    ? selectItems
        .map((item) => `<option value="${escapeHtml(item.value)}" ${item.value === selectedValue ? "selected" : ""}>${escapeHtml(item.label)}</option>`)
        .join("")
    : `<option value="">${escapeHtml(fallbackLabel)}</option>`;
}

function teamDefinitionMemberSourceLabel(member) {
  const item = normalizeTeamDefinitionMember(member);
  const options = teamDefinitionReferenceOptions(item.source_kind, { excludeDefinitionId: null });
  return (
    options.find((entry) => entry.value === item.source_ref)?.label ||
    item.source_ref ||
    (item.source_kind === "team_definition" ? "未选择团队" : "未选择 Agent 模板")
  );
}

function renderTeamDefinitionLeadAgentOptions(selectedValue = "") {
  renderSingleSelect(
    teamDefinitionLeadAgentTemplate,
    state.agentTemplates.map((item) => ({ value: item.id, label: item.name || item.id })),
    selectedValue,
    "暂无 Agent 模板",
    { allowBlank: true, blankLabel: "请选择" },
  );
}

function teamDefinitionMembersValue() {
  try {
    const parsed = safeParseJson(teamDefinitionMembers?.value, []);
    return Array.isArray(parsed) ? parsed.filter((item) => item && typeof item === "object").map(normalizeTeamDefinitionMember) : [];
  } catch (error) {
    return [];
  }
}

function teamDefinitionMemberOptions() {
  return teamDefinitionMembersValue().map((item, index) => ({
    key: item.source_ref || `member_${index + 1}`,
    name: item.name || teamDefinitionMemberSourceLabel(item) || `成员 ${index + 1}`,
  }));
}

function populateTeamDefinitionSharedOptions() {}

function populateTeamDefinitionPolicyAgentOptions() {}

function syncTeamDefinitionPolicyControls() {}

function setTeamDefinitionMembers(members) {
  teamDefinitionMembers.value = prettyJson((members || []).map((item) => normalizeTeamDefinitionMember(item)));
  renderTeamDefinitionMembers();
}

function mutateTeamDefinitionMembers(mutator) {
  const members = teamDefinitionMembersValue().map((item) => ({ ...item }));
  mutator(members);
  setTeamDefinitionMembers(members);
}

function renderTeamDefinitionMembers() {
  if (!teamDefinitionMemberList) {
    return;
  }
  const members = teamDefinitionMembersValue();
  teamDefinitionMemberList.innerHTML = members.length
    ? members
        .map((member, index) => {
          const sourceKind = teamDefinitionSourceKind(member.source_kind);
          const label = member.name || teamDefinitionMemberSourceLabel(member) || `Subagent ${index + 1}`;
          return `
            <article class="member-card">
              <div class="member-card-head">
                <strong>${escapeHtml(label)}</strong>
                <button type="button" class="ghost" data-team-definition-member-remove="${index}">移除</button>
              </div>
              <div class="form-grid two">
                <label><span>显示名称</span><input data-team-definition-member-field="name" data-member-index="${index}" value="${escapeHtml(member.name || "")}" placeholder="可选" /></label>
                <label>
                  <span>节点类型</span>
                  <select data-team-definition-member-field="source_kind" data-member-index="${index}">
                    ${teamDefinitionSelectOptionsMarkup(TEAM_DEFINITION_CHILD_SOURCE_KIND_OPTIONS, sourceKind)}
                  </select>
                </label>
              </div>
              <label>
                <span>引用来源</span>
                <select data-team-definition-member-field="source_ref" data-member-index="${index}">
                  ${teamDefinitionSelectOptionsMarkup(teamDefinitionReferenceOptions(sourceKind), member.source_ref, {
                    allowBlank: true,
                    blankLabel: sourceKind === "team_definition" ? "请选择团队" : "请选择 Agent 模板",
                    fallbackLabel: sourceKind === "team_definition" ? "暂无可引用团队" : "暂无 Agent 模板",
                  })}
                </select>
              </label>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail">暂无直属 Subagent。可添加 Agent 模板或另一个团队作为子节点。</div>';
}

function addTeamDefinitionMember() {
  mutateTeamDefinitionMembers((members) => {
    const firstAgentTemplate = state.agentTemplates[0] || null;
    members.push({
      kind: "agent",
      source_kind: "agent_template",
      source_ref: firstAgentTemplate?.id || "",
      name: "",
    });
  });
}

function updateTeamDefinitionMemberField(index, field, value) {
  mutateTeamDefinitionMembers((members) => {
    const member = members[index];
    if (!member) {
      return;
    }
    if (field === "source_kind") {
      const sourceKind = teamDefinitionSourceKind(value);
      const options = teamDefinitionReferenceOptions(sourceKind);
      member.source_kind = sourceKind;
      member.kind = sourceKind === "team_definition" ? "team" : "agent";
      member.source_ref = options.some((item) => item.value === member.source_ref) ? member.source_ref : options[0]?.value || "";
      return;
    }
    if (field === "source_ref") {
      member.source_ref = String(value || "").trim();
      return;
    }
    member[field] = String(value || "");
  });
}

function updateTeamDefinitionMemberCheck() {}

function removeTeamDefinitionMember(index) {
  mutateTeamDefinitionMembers((members) => {
    members.splice(index, 1);
  });
}

function populateTeamTemplateOptions() {
  const options = state.teamTemplates
    .map((item) => `<option value="${item.id}" ${item.id === state.selectedTeamTemplateId ? "selected" : ""}>${escapeHtml(item.name)}</option>`)
    .join("");
  buildTeamTemplate.innerHTML = options || '<option value="">暂无团队模板</option>';
}

function populateTaskOptions() {
  const teamDefinitionOptions = state.teamDefinitions
    .map(
      (item) =>
        `<option value="${item.id}" ${item.id === state.selectedTaskTeamDefinitionId ? "selected" : ""}>${escapeHtml(item.name || item.key || item.id)}</option>`,
    )
    .join("");
  if (taskTeamDefinition) {
    taskTeamDefinition.innerHTML = `<option value="">直接选择 TeamDefinition</option>${teamDefinitionOptions}`;
    taskTeamDefinition.value = state.selectedTaskTeamDefinitionId || "";
  }

  const buildOptions = state.builds
    .map((item) => `<option value="${item.id}" ${item.id === state.selectedBuildId ? "selected" : ""}>${escapeHtml(item.name)}</option>`)
    .join("");
  taskBuild.innerHTML = `<option value="">不使用 Build</option>${buildOptions}`;
  taskBuild.value = state.selectedBuildId || "";

  const blueprintOptions = state.blueprints
    .map((item) => `<option value="${item.id}" ${item.id === state.selectedBlueprintId ? "selected" : ""}>${escapeHtml(item.name)}</option>`)
    .join("");
  taskBlueprint.innerHTML = `<option value="">不使用内部蓝图</option>${blueprintOptions}`;
  taskBlueprint.value = state.selectedBlueprintId || "";
  syncTaskLaunchControls();
}

function syncTaskLaunchControls() {
  const usingTeamDefinition = Boolean(taskTeamDefinition?.value);
  if (taskBuild) {
    taskBuild.disabled = usingTeamDefinition;
  }
  if (taskBlueprint) {
    taskBlueprint.disabled = usingTeamDefinition;
  }
}

function providerPreset(type = providerType.value) {
  return state.providerTypes.find((item) => item.provider_type === type) || null;
}

function retrievalSettingsSnapshot() {
  return state.retrievalSettings?.settings || { embedding: { mode: "hash" }, rerank: { mode: "disabled" } };
}

function syncRetrievalProviderOptions() {
  const settings = retrievalSettingsSnapshot();
  const embeddingProviders = providerOptionsByModelType("embedding");
  const rerankProviders = providerOptionsByModelType("rerank");
  renderProviderSelect(retrievalEmbeddingProvider, embeddingProviders, settings.embedding?.provider_id || "");
  renderProviderSelect(retrievalRerankProvider, rerankProviders, settings.rerank?.provider_id || "");
  syncRetrievalModelOptions();
}

function syncRetrievalModelOptions() {
  const settings = retrievalSettingsSnapshot();
  renderModelSelect(
    retrievalEmbeddingModel,
    providerModelsByType(retrievalEmbeddingProvider?.value || "", "embedding"),
    settings.embedding?.model_name || "",
  );
  renderModelSelect(
    retrievalRerankModel,
    providerModelsByType(retrievalRerankProvider?.value || "", "rerank"),
    settings.rerank?.model_name || "",
  );
  const embeddingProviderEnabled = retrievalEmbeddingMode?.value === "provider";
  const rerankProviderEnabled = retrievalRerankMode?.value === "provider";
  if (retrievalEmbeddingProvider) {
    retrievalEmbeddingProvider.disabled = !embeddingProviderEnabled;
  }
  if (retrievalEmbeddingModel) {
    retrievalEmbeddingModel.disabled = !embeddingProviderEnabled;
  }
  if (retrievalRerankProvider) {
    retrievalRerankProvider.disabled = !rerankProviderEnabled;
  }
  if (retrievalRerankModel) {
    retrievalRerankModel.disabled = !rerankProviderEnabled;
  }
}

function fillRetrievalSettingsForm() {
  const settings = retrievalSettingsSnapshot();
  if (retrievalEmbeddingMode) {
    retrievalEmbeddingMode.value = settings.embedding?.mode || "hash";
  }
  if (retrievalRerankMode) {
    retrievalRerankMode.value = settings.rerank?.mode || "disabled";
  }
  syncRetrievalProviderOptions();
  renderRetrievalSettings();
}

function buildRetrievalPayloadFromForm() {
  return {
    embedding:
      retrievalEmbeddingMode.value === "provider"
        ? {
            mode: "provider",
            provider_id: retrievalEmbeddingProvider.value,
            model_name: retrievalEmbeddingModel.value,
          }
        : { mode: "hash" },
    rerank:
      retrievalRerankMode.value === "provider"
        ? {
            mode: "provider",
            provider_id: retrievalRerankProvider.value,
            model_name: retrievalRerankModel.value,
          }
        : { mode: "disabled" },
  };
}

function renderRetrievalSettings() {
  const settings = retrievalSettingsSnapshot();
  const warnings = state.retrievalSettings?.warnings || [];
  if (retrievalSummary) {
    const embeddingLabel =
      settings.embedding?.mode === "provider"
        ? `${settings.embedding.provider_name || settings.embedding.provider_id || "-"} / ${settings.embedding.model_name || "-"}`
        : "HashEmbedder";
    const rerankLabel =
      settings.rerank?.mode === "provider"
        ? `${settings.rerank.provider_name || settings.rerank.provider_id || "-"} / ${settings.rerank.model_name || "-"}`
        : "关闭";
    retrievalSummary.innerHTML = [
      chip("Embedding", embeddingLabel),
      chip("Rerank", rerankLabel),
      chip("更新时间", state.retrievalSettings?.updated_at || "-"),
    ].join("");
  }
  if (warnings.length) {
    showResult(retrievalSettingsResult, { warnings });
  } else {
    hideResult(retrievalSettingsResult);
  }
}

function openProviderModal() {
  providerModal.classList.remove("hidden");
}

function closeProviderModal() {
  closeProviderModelEditorModal();
  providerModal.classList.add("hidden");
}

function openProviderModelEditorModal() {
  providerModelEditorModal.classList.remove("hidden");
}

function closeProviderModelEditorModal() {
  providerModelEditorModal.classList.add("hidden");
  resetProviderModelEditor();
}

function resetProviderModelEditor() {
  state.providerEditor.editingModelIndex = null;
  providerModelName.value = "";
  providerModelType.value = "chat";
  providerModelContextWindow.value = "";
  providerModelEditorTitle.textContent = "新建模型";
  hideResult(providerModelTestResult);
}

function renderProviderModelSummary() {
  providerModelSummary.textContent = state.providerEditor.models.length
    ? `\u5df2\u914d\u7f6e ${state.providerEditor.models.length} \u4e2a\u6a21\u578b`
    : "\u672a\u914d\u7f6e\u6a21\u578b";
}

function renderProviderModelList() {
  renderProviderModelSummary();
  providerModelList.innerHTML = state.providerEditor.models.length
    ? state.providerEditor.models
        .map(
          (item, index) => `
            <article class="provider-model-item">
              <div class="provider-model-item-main">
                <strong>${escapeHtml(item.name)}</strong>
                <span>${escapeHtml(MODEL_TYPE_LABELS[item.model_type] || item.model_type)} / context=${escapeHtml(item.context_window || "-")}</span>
              </div>
              <div class="provider-model-item-actions">
                <button type="button" class="ghost" data-provider-model-edit="${index}">\u7f16\u8f91</button>
                <button type="button" class="ghost" data-provider-model-remove="${index}">\u5220\u9664</button>
              </div>
            </article>
          `,
        )
        .join("")
    : '<div class="detail empty compact-detail">\u6682\u65e0\u6a21\u578b\uff0c\u5148\u65b0\u5efa\u6216\u83b7\u53d6\u3002</div>';
}

function startProviderModelEditor(index = null) {
  state.providerEditor.editingModelIndex = index;
  const model = index === null ? { name: "", model_type: "chat", context_window: "" } : state.providerEditor.models[index];
  providerModelName.value = model?.name || "";
  providerModelType.value = model?.model_type || "chat";
  providerModelContextWindow.value = model?.context_window || "";
  providerModelEditorTitle.textContent = index === null ? "新建模型" : "编辑模型";
  openProviderModelEditorModal();
  hideResult(providerModelTestResult);
}

function readProviderModelEditor() {
  const contextWindow = providerModelContextWindow.value.trim();
  return {
    name: providerModelName.value.trim(),
    model_type: providerModelType.value,
    ...(contextWindow ? { context_window: Number(contextWindow) } : {}),
  };
}

function setProviderApiKeyVisibility(visible) {
  providerApiKey.type = visible ? "text" : "password";
  providerApiKeyToggle.setAttribute("aria-pressed", visible ? "true" : "false");
  providerApiKeyToggle.setAttribute("aria-label", visible ? "隐藏 API Key" : "显示 API Key");
  providerApiKeyToggle.title = visible ? "隐藏 API Key" : "显示 API Key";
  providerApiKeyIconShow.hidden = visible;
  providerApiKeyIconHide.hidden = !visible;
}

function setProviderApiKeyValue(value) {
  providerApiKey.value = value;
  setProviderApiKeyVisibility(false);
}

function openPluginModal() {
  pluginModal.classList.remove("hidden");
}

function closePluginModal() {
  pluginModal.classList.add("hidden");
  hideResult(pluginModalResult);
}

function openStaticMemoryModal(mode = currentStaticMemoryMode()) {
  syncStaticMemoryEditorMode(mode);
  staticMemoryModal?.classList.remove("hidden");
}

function closeStaticMemoryModal() {
  staticMemoryModal?.classList.add("hidden");
  hideResult(staticMemoryModalResult);
}

function setPluginEditorState({ manifest = {}, config = {}, secretFieldPaths = [] } = {}) {
  const nextManifest = manifest && typeof manifest === "object" ? clone(manifest) : {};
  const nextConfig = deepMerge(dictOrEmpty(nextManifest.runtime), config && typeof config === "object" ? config : {});
  state.pluginEditor = {
    manifest: nextManifest,
    schema: nextManifest.config_schema && typeof nextManifest.config_schema === "object" ? clone(nextManifest.config_schema) : null,
    config: nextConfig,
    secretFieldPaths: Array.isArray(secretFieldPaths) ? [...secretFieldPaths] : [],
  };
  renderPluginConfigEditor();
}

function pluginConfigFieldLabel(path, schema) {
  const fieldName = String(Array.isArray(path) ? path[path.length - 1] || "" : path || "").trim();
  return String(schema?.title || fieldName || "field");
}

function pluginConfigFieldNote(schema, { isSecret = false, hasSavedSecret = false } = {}) {
  const notes = [];
  if (typeof schema?.description === "string" && schema.description.trim()) {
    notes.push(schema.description.trim());
  }
  if (isSecret && hasSavedSecret) {
    notes.push("留空则保留已保存值。");
  } else if (isSecret) {
    notes.push("敏感字段，不会在页面回显。");
  }
  return notes.join(" ");
}

function pluginConfigFieldMarkup(path, schema) {
  const fieldType = String(schema?.type || "string").trim().toLowerCase();
  const pathKey = pluginConfigPathKey(path);
  const currentValue = nestedValue(pluginEditorConfig(), path, fieldType === "boolean" ? false : fieldType === "array" ? [] : "");
  const secretPaths = pluginEditorSecretPaths();
  const isSecret = pluginConfigFieldIsSecret(path, schema);
  const hasSavedSecret = secretPaths.has(pathKey);
  const label = pluginConfigFieldLabel(path, schema);
  const note = pluginConfigFieldNote(schema, { isSecret, hasSavedSecret });
  const sharedAttrs =
    `data-plugin-config-path="${escapeAttribute(pathKey)}" ` +
    `data-plugin-config-field-type="${escapeAttribute(fieldType)}" ` +
    `data-plugin-config-secret="${isSecret ? "true" : "false"}"`;

  if (fieldType === "boolean") {
    return `
      <label class="toggle-chip plugin-config-toggle">
        <input type="checkbox" ${sharedAttrs} ${currentValue ? "checked" : ""} />
        <span>${escapeHtml(label)}</span>
        ${note ? `<small class="field-note">${escapeHtml(note)}</small>` : ""}
      </label>
    `;
  }

  if (fieldType === "object" && schema?.properties && typeof schema.properties === "object") {
    const children = Object.entries(schema.properties)
      .map(([key, childSchema]) => pluginConfigFieldMarkup([...path, key], childSchema || {}))
      .join("");
    return `
      <fieldset class="plugin-config-group">
        <legend>${escapeHtml(schema?.title || label)}</legend>
        ${note ? `<p class="field-note">${escapeHtml(note)}</p>` : ""}
        <div class="form-grid two">${children}</div>
      </fieldset>
    `;
  }

  if (fieldType === "array") {
    const itemSchema = schema?.items || {};
    const itemType = String(itemSchema?.type || "string").trim().toLowerCase();
    const selectedValues = Array.isArray(currentValue) ? currentValue.map((item) => String(item)) : [];
    if (Array.isArray(itemSchema?.enum) && itemSchema.enum.length) {
      const options = itemSchema.enum.map((item) => ({ value: String(item), label: String(item) }));
      return `
        <label>
          <span>${escapeHtml(label)}</span>
          <select ${sharedAttrs} data-plugin-config-array-mode="enum" data-plugin-config-item-type="${escapeAttribute(itemType)}" multiple size="${Math.min(
            Math.max(options.length, 3),
            8,
          )}">
            ${multiSelectOptionsMarkup(options, selectedValues)}
          </select>
          ${note ? `<small class="field-note">${escapeHtml(note)}</small>` : ""}
        </label>
      `;
    }
    return `
      <label>
        <span>${escapeHtml(label)}</span>
        <textarea ${sharedAttrs} data-plugin-config-array-mode="lines" data-plugin-config-item-type="${escapeAttribute(itemType)}" rows="4" placeholder="每行一个值">${escapeHtml(
          selectedValues.join("\n"),
        )}</textarea>
        ${note ? `<small class="field-note">${escapeHtml(note)}</small>` : ""}
      </label>
    `;
  }

  if (fieldType === "object") {
    return `
      <label>
        <span>${escapeHtml(label)}</span>
        <textarea ${sharedAttrs} data-plugin-config-object-mode="json" rows="5" placeholder='{"key":"value"}'>${escapeHtml(
          currentValue && typeof currentValue === "object" ? prettyJson(currentValue) : "",
        )}</textarea>
        ${note ? `<small class="field-note">${escapeHtml(note)}</small>` : ""}
      </label>
    `;
  }

  if (Array.isArray(schema?.enum) && schema.enum.length) {
    const options = schema.enum.map((item) => ({ value: String(item), label: String(item) }));
    return `
      <label>
        <span>${escapeHtml(label)}</span>
        <select ${sharedAttrs}>
          ${options.map((item) => `<option value="${escapeHtml(item.value)}"${String(currentValue) === item.value ? " selected" : ""}>${escapeHtml(item.label)}</option>`).join("")}
        </select>
        ${note ? `<small class="field-note">${escapeHtml(note)}</small>` : ""}
      </label>
    `;
  }

  const inputType =
    isSecret ? "password" : fieldType === "integer" || fieldType === "number" ? "number" : "text";
  const extraAttrs =
    fieldType === "integer"
      ? ` step="1"${schema?.minimum !== undefined ? ` min="${escapeAttribute(schema.minimum)}"` : ""}`
      : fieldType === "number"
        ? ` step="any"${schema?.minimum !== undefined ? ` min="${escapeAttribute(schema.minimum)}"` : ""}`
        : "";
  const placeholder = hasSavedSecret ? "已保存，留空则保留" : String(schema?.default ?? "");
  const value = isSecret ? "" : currentValue;
  return `
    <label>
      <span>${escapeHtml(label)}</span>
      <input type="${inputType}" ${sharedAttrs}${extraAttrs} value="${escapeAttribute(value)}" placeholder="${escapeAttribute(placeholder)}" />
      ${note ? `<small class="field-note">${escapeHtml(note)}</small>` : ""}
    </label>
  `;
}

function renderPluginConfigEditor() {
  if (!pluginConfigForm || !pluginConfigSchemaEmpty) {
    return;
  }
  const schema = pluginEditorSchema();
  if (!schema || String(schema.type || "").trim().toLowerCase() !== "object" || !schema.properties) {
    pluginConfigSchemaEmpty.classList.remove("hidden");
    pluginConfigForm.innerHTML = "";
    if (pluginConfigHint) {
      pluginConfigHint.textContent = "当前插件未声明 `config_schema`，无需额外运行配置。";
    }
    return;
  }
  pluginConfigSchemaEmpty.classList.add("hidden");
  const fields = Object.entries(schema.properties)
    .map(([key, childSchema]) => pluginConfigFieldMarkup([key], childSchema || {}))
    .join("");
  pluginConfigForm.innerHTML = `<div class="form-grid two">${fields}</div>`;
  if (pluginConfigHint) {
    const secretCount = pluginEditorSecretPaths().size;
    pluginConfigHint.textContent =
      secretCount > 0
        ? `从插件包 config_schema 渲染。检测到 ${secretCount} 个已保存敏感字段，留空会保留原值。`
        : "从插件包 config_schema 渲染。敏感字段不会在页面回显，留空表示不改。";
  }
}

function readPluginConfigFieldValue(field) {
  const fieldType = String(field.dataset.pluginConfigFieldType || "string").trim().toLowerCase();
  const fieldPath = String(field.dataset.pluginConfigPath || "").trim();
  if (fieldType === "boolean") {
    return Boolean(field.checked);
  }
  if (fieldType === "integer") {
    const text = String(field.value || "").trim();
    return text ? Number.parseInt(text, 10) : nestedValue(pluginEditorConfig(), fieldPath, 0);
  }
  if (fieldType === "number") {
    const text = String(field.value || "").trim();
    return text ? Number.parseFloat(text) : nestedValue(pluginEditorConfig(), fieldPath, 0);
  }
  if (fieldType === "array") {
    const itemType = String(field.dataset.pluginConfigItemType || "string").trim().toLowerCase();
    if (field instanceof HTMLSelectElement && field.multiple) {
      return getMultiSelectValues(field).map((item) => coercePluginSchemaValue(item, { type: itemType }));
    }
    return String(field.value || "")
      .split("\n")
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => coercePluginSchemaValue(item, { type: itemType }));
  }
  if (fieldType === "object") {
    const text = String(field.value || "").trim();
    if (!text) {
      return {};
    }
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error(`配置项 \`${field.dataset.pluginConfigPath}\` 必须是 JSON 对象。`);
    }
    return parsed;
  }
  return String(field.value ?? "");
}

function readPluginConfigFromForm() {
  const config = {};
  const secret = {};
  const fields = Array.from(pluginConfigForm?.querySelectorAll("[data-plugin-config-path]") || []);
  fields.forEach((field) => {
    const pathKey = String(field.dataset.pluginConfigPath || "").trim();
    if (!pathKey) {
      return;
    }
    const path = pathKey.split(".").filter(Boolean);
    const isSecret = String(field.dataset.pluginConfigSecret || "false") === "true";
    const value = readPluginConfigFieldValue(field);
    if (isSecret) {
      const textValue = typeof value === "string" ? value.trim() : value;
      if (textValue === "" || textValue === null || textValue === undefined) {
        return;
      }
      assignNestedValue(secret, path, value);
      return;
    }
    assignNestedValue(config, path, value);
  });
  return { config, secret };
}

function applyPluginManifestToForm(manifest, { preserveEntered = false } = {}) {
  const normalized = manifest && typeof manifest === "object" ? manifest : {};
  if (!preserveEntered || !pluginKey.value.trim()) {
    pluginKey.value = normalized.key || pluginKey.value || "";
  }
  if (!preserveEntered || !pluginName.value.trim()) {
    pluginName.value = normalized.name || pluginName.value || "";
  }
  if (!preserveEntered || !pluginVersion.value.trim()) {
    pluginVersion.value = normalized.version || pluginVersion.value || "v1";
  }
  if (!preserveEntered || !pluginType.value.trim()) {
    pluginType.value = normalized.plugin_type || pluginType.value || "toolset";
  }
  pluginWorkbenchKey.value = normalized.workbench_key || pluginWorkbenchKey.value || "";
  pluginTools.value = Array.isArray(normalized.tools) ? normalized.tools.join(", ") : pluginTools.value || "";
  pluginPermissions.value = Array.isArray(normalized.permissions) ? normalized.permissions.join(", ") : pluginPermissions.value || "";
  if (!preserveEntered || !pluginDescription.value.trim()) {
    pluginDescription.value = normalized.description || pluginDescription.value || "";
  }
}

async function syncPluginManifestFromInstallPath() {
  const installPath = pluginInstallPath?.value?.trim();
  if (!installPath) {
    if (!state.editingPluginId) {
      setPluginEditorState({ manifest: {}, config: {}, secretFieldPaths: [] });
    } else {
      setPluginEditorState({
        manifest: pluginEditorManifest(),
        config: pluginEditorConfig(),
        secretFieldPaths: Array.from(pluginEditorSecretPaths()),
      });
    }
    return;
  }
  try {
    const payload = await api("/api/agent-center/plugins/validate-package", {
      method: "POST",
      body: JSON.stringify({ path: installPath }),
    });
    const manifest = payload.manifest || {};
    applyPluginManifestToForm(manifest, { preserveEntered: true });
    setPluginEditorState({
      manifest,
      config: pluginEditorConfig(),
      secretFieldPaths: Array.from(pluginEditorSecretPaths()),
    });
  } catch (error) {
    showResult(pluginModalResult, errorResult(error));
  }
}

function openAgentTemplateModal() {
  agentTemplateModal.classList.remove("hidden");
}

function closeAgentTemplateModal() {
  agentTemplateModal.classList.add("hidden");
  hideResult(agentTemplateModalResult);
}

function openAgentDefinitionModal() {
  agentDefinitionModal?.classList.remove("hidden");
}

function closeAgentDefinitionModal() {
  agentDefinitionModal?.classList.add("hidden");
  closeAllTagMultiSelects();
  hideResult(agentDefinitionModalResult);
}

function openTeamDefinitionModal() {
  teamDefinitionModal?.classList.remove("hidden");
}

function closeTeamDefinitionModal() {
  teamDefinitionModal?.classList.add("hidden");
  hideResult(teamDefinitionModalResult);
}

function resetProviderForm() {
  state.editingProviderId = null;
  providerName.value = "";
  providerBaseUrl.value = "";
  setProviderApiKeyValue("");
  providerSkipTlsVerify.checked = true;
  state.providerEditor = {
    models: [],
    savedModels: [],
    editingModelIndex: null,
  };
  if (state.providerTypes.length) {
    providerType.value = state.providerTypes[0].provider_type;
  }
  const preset = providerPreset();
  if (preset?.default_base_url && preset.use_default_base_url_when_blank) {
    providerBaseUrl.value = preset.default_base_url;
  }
  providerModalTitle.textContent = "\u65b0\u589e\u6a21\u578b\u63d0\u4f9b\u65b9";
  renderProviderModelList();
  resetProviderModelEditor();
}

function resetPluginForm() {
  state.editingPluginId = null;
  pluginKey.value = "";
  pluginName.value = "";
  pluginVersion.value = "v1";
  pluginType.value = "toolset";
  pluginWorkbenchKey.value = "";
  pluginInstallPath.value = "";
  pluginTools.value = "";
  pluginPermissions.value = "";
  pluginDescription.value = "";
  setPluginEditorState({ manifest: {}, config: {}, secretFieldPaths: [] });
  pluginModalTitle.textContent = "新增插件";
  hideResult(pluginModalResult);
}

function resetAgentTemplateForm() {
  state.editingAgentTemplateId = null;
  agentTemplateName.value = "";
  agentTemplateRole.value = "";
  agentTemplateGoal.value = "";
  agentTemplateInstructions.value = "";
  agentTemplateDescription.value = "";
  agentTemplateMemoryPolicy.value = "agent_private";
  populateAgentTemplateReferenceOptions();
  setMultiSelectValues(agentTemplatePlugins, []);
  setMultiSelectValues(agentTemplateSkills, []);
  renderModelSelect(agentTemplateModel, providerModelsByType(agentTemplateProvider?.value || "", "chat"));
  agentTemplateModalTitle.textContent = "新增 Agent 模板";
  hideResult(agentTemplateModalResult);
}

function defaultTeamAgents() {
  return prettyJson([
    {
      key: "planner",
      name: "规划负责人",
      agent_template_ref: state.agentTemplates[0]?.id || "",
    },
  ]);
}

function defaultTeamFlow() {
  return prettyJson({
    nodes: [
      { id: "start", type: "start" },
      { id: "plan", type: "agent", agent: "planner", instruction: "输出规划结果。" },
      { id: "end", type: "end" },
    ],
    edges: [
      { from: "start", to: "plan" },
      { from: "plan", to: "end" },
    ],
  });
}

function resetTeamTemplateForm() {
  state.editingTeamTemplateId = null;
  state.selectedTeamTemplateId = null;
  teamTemplateName.value = "";
  teamTemplateWorkspace.value = "local-workspace";
  teamTemplateProject.value = "default-project";
  teamTemplateDescription.value = "";
  teamTemplateAgents.value = defaultTeamAgents();
  teamTemplateFlow.value = defaultTeamFlow();
  teamTemplateDod.value = "";
  teamTemplateChecks.value = "";
  const spec = ensureTeamEditorMetadata(buildTeamTemplateSpecFromForm());
  autoLayoutTeamSpec(spec);
  state.teamEditor = {
    spec,
    selectedNodeId: spec.flow?.nodes?.find((item) => item.type === "start")?.id || null,
    linkFromNodeId: null,
    drag: null,
    validation: null,
    preview: null,
  };
  syncTeamEditorToForm();
  renderTeamEditor();
}

function fillProviderForm(provider) {
  state.editingProviderId = provider.id;
  const config = provider.config_json || {};
  const secret = provider.secret_json || {};
  providerName.value = provider.name || "";
  providerType.value = provider.provider_type || "";
  providerBaseUrl.value = config.base_url || "";
  setProviderApiKeyValue(secret.api_key || "");
  providerSkipTlsVerify.checked = Boolean(config.skip_tls_verify);
  state.providerEditor = {
    models: clone(config.models || []),
    savedModels: clone(config.models || []),
    editingModelIndex: null,
  };
  providerModalTitle.textContent = "\u7f16\u8f91\u6a21\u578b\u63d0\u4f9b\u65b9";
  renderProviderModelList();
  resetProviderModelEditor();
  openProviderModal();
}

function fillPluginForm(plugin) {
  state.editingPluginId = plugin.id;
  const manifest = plugin.manifest_json || {};
  pluginKey.value = plugin.key || "";
  pluginName.value = plugin.name || "";
  pluginVersion.value = plugin.version || "v1";
  pluginType.value = plugin.plugin_type || "toolset";
  pluginWorkbenchKey.value = manifest.workbench_key || "";
  pluginInstallPath.value = plugin.install_path || "";
  pluginTools.value = (manifest.tools || []).join(", ");
  pluginPermissions.value = (manifest.permissions || []).join(", ");
  pluginDescription.value = plugin.description || "";
  setPluginEditorState({
    manifest,
    config: plugin.config_json || {},
    secretFieldPaths: plugin.secret_field_paths || [],
  });
  pluginModalTitle.textContent = "编辑插件";
  hideResult(pluginModalResult);
  openPluginModal();
}

function fillAgentTemplateForm(template) {
  state.editingAgentTemplateId = template.id;
  const spec = template.spec_json || {};
  agentTemplateName.value = template.name || "";
  agentTemplateRole.value = template.role || "";
  agentTemplateMemoryPolicy.value = spec.memory_policy || "agent_private";
  agentTemplateGoal.value = spec.goal || "";
  agentTemplateInstructions.value = spec.instructions || "";
  agentTemplateDescription.value = template.description || "";
  populateAgentTemplateReferenceOptions();
  const providerValue = normalizeProviderSelection(spec.provider_ref || "");
  if (providerValue) {
    agentTemplateProvider.value = providerValue;
  }
  renderModelSelect(agentTemplateModel, providerModelsByType(agentTemplateProvider?.value || "", "chat"), spec.model || "");
  setMultiSelectValues(agentTemplateSkills, normalizeResourceSelections(spec.skills || spec.skill_refs || [], state.skills));
  setMultiSelectValues(agentTemplatePlugins, normalizeResourceSelections(spec.plugin_refs || [], state.plugins));
  agentTemplateModalTitle.textContent = "编辑 Agent 模板";
  hideResult(agentTemplateModalResult);
  openAgentTemplateModal();
}

function fillTeamTemplateForm(template) {
  state.editingTeamTemplateId = template.id;
  state.selectedTeamTemplateId = template.id;
  const spec = template.spec_json || {};
  teamTemplateName.value = template.name || "";
  teamTemplateWorkspace.value = spec.workspace_id || "local-workspace";
  teamTemplateProject.value = spec.project_id || "default-project";
  teamTemplateDescription.value = template.description || "";
  teamTemplateAgents.value = prettyJson(spec.agents || []);
  teamTemplateFlow.value = prettyJson(spec.flow || { nodes: [], edges: [] });
  teamTemplateDod.value = listToLines(spec.definition_of_done || []);
  teamTemplateChecks.value = listToLines(spec.acceptance_checks || []);
  const editorSpec = ensureTeamEditorMetadata(clone(spec));
  autoLayoutTeamSpec(editorSpec);
  state.teamEditor = {
    spec: editorSpec,
    selectedNodeId: editorSpec.flow?.nodes?.[0]?.id || null,
    linkFromNodeId: null,
    drag: null,
    validation: null,
    preview: null,
  };
  syncTeamEditorToForm();
  renderTeamEditor();
}

function renderOverview() {
  const items = [
    ["Provider", state.summary.provider_profile_count || 0],
    ["插件", state.summary.plugin_count || 0],
    ["角色管理", state.summary.static_memory_count || 0],
    ["知识库", state.summary.knowledge_base_count || 0],
    ["记忆画像", state.summary.memory_profile_count || 0],
    ["Agent 管理", state.summary.agent_definition_count || 0],
    ["团队定义", state.summary.team_definition_count || 0],
    ["Run", state.summary.run_count || 0],
    ["待审批", state.summary.pending_approval_count || 0],
  ];
  summaryCards.innerHTML = items
    .map(([label, value]) => `<div class="stat-card"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`)
    .join("");

  if (storageBanner) {
    storageBanner.innerHTML = state.storage
    ? [chip("元数据库", state.storage.metadata_driver || "sqlite"), chip("日志模式", state.storage.journal_mode || "wal"), chip("路径", state.storage.metadata_path || "")].join("")
      : "";
  }

  providerTypeGrid.innerHTML = state.providerTypes
    .map(
      (item) => `
        <article class="rule-card">
          <h3>${escapeHtml(item.label)}</h3>
          <p>${escapeHtml(item.protocol || item.provider_type)}</p>
        </article>
      `,
    )
    .join("");

  overviewBuilds.innerHTML = state.recentBuilds.length
    ? state.recentBuilds
        .map((item) =>
          cardMarkup({
            title: item.name,
            body: item.description || buildTeamTemplateLabel(item),
            meta: `team=${buildTeamTemplateLabel(item)} / blueprint=${item.blueprint_id || "-"}`,
            actions: `<button type="button" class="ghost" data-build-open="${item.id}">查看</button>`,
          }),
        )
        .join("")
    : cardMarkup({ title: "暂无 Build", body: "先构建团队模板。", meta: "" });

  overviewRuns.innerHTML = state.recentRuns.length
    ? state.recentRuns
        .map((item) =>
          cardMarkup({
            title: item.summary || item.id,
            body: `状态：${item.status}`,
            meta: `run=${item.id}`,
            actions: `<button type="button" class="ghost" data-run-open="${item.id}">查看</button>`,
          }),
        )
        .join("")
    : cardMarkup({ title: "暂无 Run", body: "启动任务后显示。", meta: "" });
}

function renderProviderPagination() {
  renderOffsetPagination(state.providerPage, providerPaginationMeta, "provider-page");
}

function renderOffsetPagination(pageState, target, dataAttribute) {
  const { total, limit, offset } = pageState;
  if (!target) {
    return;
  }
  if (!total) {
    target.innerHTML = "";
    return;
  }
  const currentPage = Math.floor(offset / limit) + 1;
  const pageCount = Math.max(1, Math.ceil(total / limit));
  const prevOffset = Math.max(0, offset - limit);
  const nextOffset = offset + limit < total ? offset + limit : offset;
  target.innerHTML = `
    <span class="page-status">\u7b2c ${currentPage} / ${pageCount} \u9875\uff0c\u5171 ${total} \u6761</span>
    <button type="button" class="ghost" data-${dataAttribute}="${prevOffset}" ${offset === 0 ? "disabled" : ""}>\u4e0a\u4e00\u9875</button>
    <button type="button" class="ghost" data-${dataAttribute}="${nextOffset}" ${offset + limit >= total ? "disabled" : ""}>\u4e0b\u4e00\u9875</button>
  `;
}

function renderProviders() {
  providerList.innerHTML = state.providerPage.items.length
    ? state.providerPage.items
        .map((item) => {
          const config = item.config_json || {};
          const preset = item.preset || providerPreset(item.provider_type) || {};
          const models = Array.isArray(config.models) ? config.models : [];
          const modelTooltip = providerModelTooltipText(models, item.default_chat_model_name || config.model || "");
          const baseUrl = config.base_url || "-";
          const baseUrlTooltip = `${baseUrl}\n${item.has_secret ? "已保存密钥" : "未保存密钥"}`;
          return `
            <article class="provider-row">
              <div class="provider-main">
                <strong>${escapeHtml(item.name)}</strong>
              </div>
              <div class="provider-cell">
                <strong>${escapeHtml(preset.label || item.provider_type)}</strong>
              </div>
              <div class="provider-cell provider-model-cell">
                <strong title="${escapeAttribute(modelTooltip)}">${escapeHtml(item.model_count || 0)} \u4e2a\u6a21\u578b</strong>
              </div>
              <div class="provider-cell">
                <strong class="provider-base-url" title="${escapeAttribute(baseUrlTooltip)}">${escapeHtml(baseUrl)}</strong>
              </div>
              <div class="provider-row-actions">
                <button type="button" data-provider-edit="${item.id}">\u7f16\u8f91</button>
                <button type="button" class="ghost warn" data-provider-delete="${item.id}">\u5220\u9664</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail">\u6682\u65e0\u7b26\u5408\u6761\u4ef6\u7684\u6a21\u578b\u63d0\u4f9b\u65b9\u3002</div>';
  renderProviderPagination();
}

function renderPlugins() {
  pluginList.innerHTML = state.pluginPage.items.length
    ? state.pluginPage.items
        .map((item) => {
          const manifest = item.manifest_json || {};
          const runtime = item.runtime || {};
          const descriptor = runtime.descriptor || {};
          const runtimeState = runtime.running ? "running" : runtime.status || (item.install_path ? "idle" : "metadata_only");
          const tools = (descriptor.tools || manifest.tools || []).join(", ") || "-";
          const workbenchKey = manifest.workbench_key || "-";
          const statusHint = item.install_path ? item.install_path : "未配置安装路径";
          return `
            <article class="resource-row plugin-row">
              <div class="resource-main">
                <strong title="${escapeAttribute(pluginDisplayName(item))}">${escapeHtml(pluginDisplayName(item))}</strong>
                <span title="${escapeAttribute(pluginDisplaySubtitle(item))}">${escapeHtml(pluginDisplaySubtitle(item))}</span>
              </div>
              <div class="resource-cell">
                <strong>${escapeHtml(item.version || "-")}</strong>
                <span>${escapeHtml(item.plugin_type || "-")}</span>
              </div>
              <div class="resource-cell">
                <strong title="${escapeAttribute(workbenchKey)}">${escapeHtml(workbenchKey)}</strong>
                <span title="${escapeAttribute(`tools=${tools}`)}">${escapeHtml(`tools=${tools}`)}</span>
              </div>
              <div class="resource-cell">
                <strong>${escapeHtml(runtimeState)}</strong>
                <span title="${escapeAttribute(statusHint)}">${escapeHtml(statusHint)}</span>
              </div>
              <div class="resource-row-actions">
                <button type="button" data-plugin-edit="${item.id}">编辑</button>
                <button type="button" class="ghost" data-plugin-validate="${item.id}">校验</button>
                <button type="button" class="ghost" data-plugin-install="${item.id}">安装</button>
                <button type="button" class="ghost" data-plugin-load="${item.id}">加载</button>
                <button type="button" class="ghost" data-plugin-reload="${item.id}">重载</button>
                <button type="button" class="ghost" data-plugin-health="${item.id}">健康</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail">暂无插件，先新建一个插件。</div>';
  renderOffsetPagination(state.pluginPage, pluginPaginationMeta, "plugin-page");
}

function renderAgentTemplates() {
  agentTemplateList.innerHTML = state.agentTemplatePage.items.length
    ? state.agentTemplatePage.items
        .map((item) => {
          const spec = item.spec_json || {};
          const provider = state.providers.find((entry) => entry.id === spec.provider_ref);
          const providerLabel = provider?.name || spec.provider_ref || "-";
          const skills = Array.isArray(spec.skills) && spec.skills.length ? spec.skills.join(", ") : "未配置技能";
          return `
            <article class="resource-row agent-template-row">
              <div class="resource-main">
                <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
                <span title="${escapeAttribute(item.description || item.role || "-")}">${escapeHtml(item.description || item.role || "-")}</span>
              </div>
              <div class="resource-cell">
                <strong>${escapeHtml(item.role || "-")}</strong>
                <span>${escapeHtml(spec.memory_policy || "-")}</span>
              </div>
              <div class="resource-cell">
                <strong title="${escapeAttribute(providerLabel)}">${escapeHtml(providerLabel)}</strong>
                <span>${escapeHtml(`${(spec.plugin_refs || []).length} 个插件`)}</span>
              </div>
              <div class="resource-cell">
                <strong title="${escapeAttribute(spec.model || "-")}">${escapeHtml(spec.model || "-")}</strong>
                <span title="${escapeAttribute(skills)}">${escapeHtml(skills)}</span>
              </div>
              <div class="resource-row-actions">
                <button type="button" data-agent-template-edit="${item.id}">编辑</button>
                <button type="button" class="ghost warn" data-agent-template-delete="${item.id}">删除</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail">暂无 Agent 模板，先新建一个模板。</div>';
  renderOffsetPagination(state.agentTemplatePage, agentTemplatePaginationMeta, "agent-template-page");
}

function buildTeamTemplateSpecFromForm() {
  if (state.teamEditor?.spec) {
    const spec = clone(state.teamEditor.spec);
    spec.name = teamTemplateName.value.trim();
    spec.workspace_id = teamTemplateWorkspace.value.trim() || "local-workspace";
    spec.project_id = teamTemplateProject.value.trim() || "default-project";
    spec.definition_of_done = linesToList(teamTemplateDod.value);
    spec.acceptance_checks = linesToList(teamTemplateChecks.value);
    return ensureTeamEditorMetadata(spec);
  }
  return {
    name: teamTemplateName.value.trim(),
    workspace_id: teamTemplateWorkspace.value.trim() || "local-workspace",
    project_id: teamTemplateProject.value.trim() || "default-project",
    agents: safeParseJson(teamTemplateAgents.value, []),
    flow: safeParseJson(teamTemplateFlow.value, { nodes: [], edges: [] }),
    definition_of_done: linesToList(teamTemplateDod.value),
    acceptance_checks: linesToList(teamTemplateChecks.value),
    metadata: { communication_policy: "graph-ancestor-scoped", ui_layout: { positions: {}, viewport: { x: 0, y: 0, zoom: 1 } } },
  };
}

function renderTeamTemplatePreview(specOverride = null) {
  let spec;
  try {
    spec = specOverride || buildTeamTemplateSpecFromForm();
  } catch (error) {
    teamTemplatePreview.innerHTML = `
      <article class="inspector-card">
        <h3>团队摘要</h3>
        <p>成员或流程 JSON 暂不可解析。</p>
      </article>
    `;
    return;
  }
  const summary = teamSummary(spec);
  const agentLines = summary.agents.length
    ? summary.agents
        .map((item) => {
          const templateRef = item.agent_template_ref || item.agent_template_id || "";
          const template = state.agentTemplates.find((entry) => entry.id === templateRef);
          const templateLabel = template?.name || templateRef || "-";
          return `
            <div class="inspector-line">
              <strong>${escapeHtml(item.name || item.key)}</strong>
              <span>${escapeHtml(item.key)} / ${escapeHtml(templateLabel)}</span>
            </div>
          `;
        })
        .join("")
    : `<div class="inspector-line"><strong>暂无成员</strong><span>请先添加团队成员</span></div>`;
  const validation = state.teamEditor.validation;
  const preview = state.teamEditor.preview;
  teamTemplatePreview.innerHTML = `
    <article class="inspector-card">
      <h3>${escapeHtml(spec.name || teamTemplateName.value || "团队模板")}</h3>
      <div class="system-banner compact">
        ${chip("成员", summary.agents.length)}
        ${chip("节点", summary.nodes.length)}
        ${chip("边", summary.edges.length)}
      </div>
    </article>
    <article class="inspector-card">
      <h3>成员</h3>
      <div class="inspector-list">${agentLines}</div>
    </article>
    ${
      validation
        ? `<article class="inspector-card">
             <h3>校验</h3>
             <div class="system-banner compact">
               <span class="validation-badge ${validation.valid ? "ok" : "error"}">${validation.valid ? "通过" : `错误 ${validation.errors.length}`}</span>
               <span class="validation-badge ${validation.warnings?.length ? "warn" : "ok"}">${validation.warnings?.length ? `警告 ${validation.warnings.length}` : "无警告"}</span>
             </div>
           </article>`
        : ""
    }
    ${
      preview?.preview
        ? `<article class="inspector-card">
             <h3>Build 预览</h3>
             <div class="system-banner compact">
               ${chip("角色模板", preview.preview.role_template_count || 0)}
               ${chip("Agent", preview.preview.agent_count || 0)}
             </div>
           </article>`
        : ""
    }
  `;
}

function mutateTeamSpec(mutator) {
  const spec = ensureTeamEditorMetadata(teamEditorSpec());
  mutator(spec);
  state.teamEditor.spec = spec;
  state.teamEditor.validation = null;
  state.teamEditor.preview = null;
  syncTeamEditorToForm();
  renderTeamEditor();
}

function renderTeamEditor() {
  if (!state.teamEditor.spec) {
    state.teamEditor.spec = ensureTeamEditorMetadata(buildTeamTemplateSpecFromForm());
  }
  syncTeamEditorToForm();
  renderTeamTemplatePreview(state.teamEditor.spec);
  renderTeamMembers();
  renderTeamGraph();
  renderTeamNodeInspector();
  renderTeamValidationPanel();
}

function renderTeamMembers() {
  const spec = teamEditorSpec();
  const providerOptions = state.agentTemplates
    .map(
      (template) =>
        `<option value="${escapeHtml(template.id)}">${escapeHtml(template.name)} / ${escapeHtml(template.role)}</option>`,
    )
    .join("");
  teamMemberList.innerHTML = (spec.agents || [])
    .map((member, index) => {
      const nodeUsage = (spec.flow?.nodes || []).filter((node) => node.agent === member.key).map((node) => node.id);
      return `
        <article class="member-card">
          <div class="member-card-head">
            <strong>${escapeHtml(member.name || member.key || `成员 ${index + 1}`)}</strong>
            <button type="button" class="ghost" data-team-member-remove="${escapeHtml(member.key)}">移除</button>
          </div>
          <div class="form-grid two">
            <label><span>Key</span><input data-team-member-field="key" data-member-index="${index}" value="${escapeHtml(member.key || "")}" /></label>
            <label><span>名称</span><input data-team-member-field="name" data-member-index="${index}" value="${escapeHtml(member.name || "")}" /></label>
          </div>
          <label>
            <span>Agent 模板</span>
            <select data-team-member-field="agent_template_ref" data-member-index="${index}">
              ${providerOptions.replace(`value="${escapeHtml(member.agent_template_ref || "")}"`, `value="${escapeHtml(member.agent_template_ref || "")}" selected`)}
            </select>
          </label>
          <div class="meta">节点引用：${escapeHtml(nodeUsage.join(", ") || "未绑定")}</div>
        </article>
      `;
    })
    .join("");
}

function renderTeamGraph() {
  const spec = ensureTeamEditorMetadata(teamEditorSpec());
  const nodes = spec.flow?.nodes || [];
  const positions = spec.metadata.ui_layout.positions || {};
  let maxX = 1200;
  let maxY = 800;
  teamGraphCanvas.innerHTML = nodes
    .map((node, index) => {
      const pos = graphPositionFor(node.id, index);
      maxX = Math.max(maxX, pos.x + 260);
      maxY = Math.max(maxY, pos.y + 220);
      const memberName = spec.agents?.find((item) => item.key === node.agent)?.name || node.agent || "";
      return `
        <article
          class="graph-node ${escapeHtml(node.type)} ${node.id === state.teamEditor.selectedNodeId ? "selected" : ""} ${node.id === state.teamEditor.linkFromNodeId ? "linking" : ""}"
          data-node-id="${escapeHtml(node.id)}"
          style="left:${pos.x}px; top:${pos.y}px;"
        >
          <strong>${escapeHtml(node.name || node.id)}</strong>
          <span>${escapeHtml(memberName)}</span>
          <span class="graph-node-type">${escapeHtml(node.type)}</span>
        </article>
      `;
    })
    .join("");
  teamGraphCanvas.style.minWidth = `${maxX}px`;
  teamGraphCanvas.style.minHeight = `${maxY}px`;
  teamGraphEdges.setAttribute("width", String(maxX));
  teamGraphEdges.setAttribute("height", String(maxY));
  teamGraphEdges.innerHTML = (spec.flow?.edges || [])
    .map((edge) => {
      const from = positions[edge.from];
      const to = positions[edge.to];
      if (!from || !to) {
        return "";
      }
      const startX = from.x + 170;
      const startY = from.y + 50;
      const endX = to.x;
      const endY = to.y + 50;
      const midX = (startX + endX) / 2;
      const midY = (startY + endY) / 2 - 8;
      return `
        <line class="graph-edge-line" x1="${startX}" y1="${startY}" x2="${endX}" y2="${endY}"></line>
        ${edge.when ? `<text class="graph-edge-label" x="${midX}" y="${midY}">${escapeHtml(edge.when)}</text>` : ""}
      `;
    })
    .join("");
  teamLinkHint.classList.toggle("hidden", !state.teamEditor.linkFromNodeId);
  teamLinkHint.textContent = state.teamEditor.linkFromNodeId
    ? `连线模式：请选择目标节点，当前起点 ${state.teamEditor.linkFromNodeId}`
    : "";
}

function renderTeamNodeInspector() {
  const spec = teamEditorSpec();
  const node = (spec.flow?.nodes || []).find((item) => item.id === state.teamEditor.selectedNodeId) || null;
  const memberOptions =
    spec.agents
      .map((item) => `<option value="${escapeHtml(item.key)}">${escapeHtml(item.name || item.key)}</option>`)
      .join("") || `<option value="">暂无成员</option>`;
  teamNodeAgent.innerHTML = `<option value="">不绑定</option>${memberOptions}`;
  if (!node) {
    teamNodeEmpty.classList.remove("hidden");
    teamNodeFormShell.classList.add("hidden");
    teamEdgeList.innerHTML = "";
    return;
  }
  teamNodeEmpty.classList.add("hidden");
  teamNodeFormShell.classList.remove("hidden");
  teamNodeId.value = node.id || "";
  teamNodeType.value = node.type || "";
  teamNodeName.value = node.name || "";
  teamNodeAgent.value = node.agent || "";
  teamNodeInstruction.value = node.instruction || "";
  teamNodeExpr.value = node.expr || "";
  teamNodeMaxIterations.value = node.max_iterations ?? "";
  teamNodeArtifactKind.value = node.artifact_kind || "report";
  teamNodeArtifactName.value = node.name || "";
  teamNodeTemplate.value = node.template || "";
  teamNodeSource.value = node.source || "";
  const outgoing = (spec.flow?.edges || []).filter((edge) => edge.from === node.id);
  teamEdgeList.innerHTML = outgoing.length
    ? outgoing
        .map(
          (edge, index) => `
            <article class="edge-card">
              <div class="edge-card-head">
                <strong>${escapeHtml(edge.from)} → ${escapeHtml(edge.to)}</strong>
                <button type="button" class="ghost" data-team-edge-remove="${index}" data-edge-from="${escapeHtml(edge.from)}" data-edge-to="${escapeHtml(edge.to)}">删除</button>
              </div>
              <label><span>条件</span><input data-team-edge-when="${index}" data-edge-from="${escapeHtml(edge.from)}" data-edge-to="${escapeHtml(edge.to)}" value="${escapeHtml(edge.when || "")}" placeholder="true / false / default / expr" /></label>
            </article>
          `,
        )
        .join("")
    : `<div class="detail empty compact-detail">暂无出边。可先点“开始连线”再选择目标节点。</div>`;
}

function renderTeamValidationPanel() {
  const validation = state.teamEditor.validation;
  const preview = state.teamEditor.preview;
  if (!validation && !preview) {
    teamValidationResult.innerHTML = `
      <article class="inspector-card">
        <h3>待校验</h3>
        <p>修改画布后可直接校验或预览 Build。</p>
      </article>
    `;
    return;
  }
  const errorLines = (validation?.errors || []).map((item) => `<div class="inspector-line"><strong>错误</strong><span>${escapeHtml(item)}</span></div>`).join("");
  const warningLines = (validation?.warnings || []).map((item) => `<div class="inspector-line"><strong>警告</strong><span>${escapeHtml(item)}</span></div>`).join("");
  const communicationItems = Object.entries(validation?.communication || {})
    .slice(0, 8)
    .map(
      ([nodeId, item]) => `
        <div class="inspector-line">
          <strong>${escapeHtml(nodeId)}</strong>
          <span>${escapeHtml((item.visible_agent_nodes || []).join(", ") || "无可见 Agent")}</span>
        </div>
      `,
    )
    .join("");
  teamValidationResult.innerHTML = `
    ${
      validation
        ? `<article class="inspector-card">
             <h3>${validation.valid ? "图校验通过" : "图校验失败"}</h3>
             <div class="system-banner compact">
               <span class="validation-badge ${validation.valid ? "ok" : "error"}">${validation.valid ? "可保存" : `错误 ${validation.errors.length}`}</span>
               <span class="validation-badge ${validation.warnings?.length ? "warn" : "ok"}">${validation.warnings?.length ? `警告 ${validation.warnings.length}` : "无警告"}</span>
             </div>
           </article>`
        : ""
    }
    ${errorLines}
    ${warningLines}
    ${
      preview?.preview
        ? `<article class="inspector-card">
             <h3>Build 预览</h3>
             <div class="system-banner compact">
               ${chip("角色模板", preview.preview.role_template_count || 0)}
               ${chip("Agent", preview.preview.agent_count || 0)}
               ${chip("节点", preview.preview.node_count || 0)}
               ${chip("边", preview.preview.edge_count || 0)}
             </div>
           </article>`
        : ""
    }
    ${
      communicationItems
        ? `<article class="inspector-card">
             <h3>可见性</h3>
             <div class="inspector-list">${communicationItems}</div>
           </article>`
        : ""
    }
  `;
}

function selectTeamNode(nodeId) {
  state.teamEditor.selectedNodeId = nodeId;
  renderTeamEditor();
}

function addTeamMember() {
  mutateTeamSpec((spec) => {
    const existing = new Set((spec.agents || []).map((item) => item.key));
    let index = 1;
    let key = `agent_${index}`;
    while (existing.has(key)) {
      index += 1;
      key = `agent_${index}`;
    }
    spec.agents = spec.agents || [];
    spec.agents.push({
      key,
      name: `成员 ${index}`,
      agent_template_ref: state.agentTemplates[0]?.id || "",
    });
  });
}

function addGraphNode(type) {
  mutateTeamSpec((spec) => {
    ensureTeamEditorMetadata(spec);
    spec.flow = spec.flow || { nodes: [], edges: [] };
    if (type === "start" && spec.flow.nodes.some((item) => item.type === "start")) {
      showResult(teamTemplateResult, { error: "start 节点只能有一个。" });
      return;
    }
    const nodeId = createNodeId(type);
    const node = {
      id: nodeId,
      type,
      ...defaultNodeConfig(type),
    };
    spec.flow.nodes.push(node);
    graphPositionFor(nodeId, spec.flow.nodes.length - 1);
    state.teamEditor.selectedNodeId = nodeId;
  });
}

function deleteSelectedNode() {
  const targetId = state.teamEditor.selectedNodeId;
  if (!targetId) {
    return;
  }
  mutateTeamSpec((spec) => {
    spec.flow.nodes = (spec.flow.nodes || []).filter((item) => item.id !== targetId);
    spec.flow.edges = (spec.flow.edges || []).filter((item) => item.from !== targetId && item.to !== targetId);
    delete spec.metadata?.ui_layout?.positions?.[targetId];
    state.teamEditor.selectedNodeId = spec.flow.nodes[0]?.id || null;
    state.teamEditor.linkFromNodeId = null;
  });
}

function startLinkMode() {
  if (!state.teamEditor.selectedNodeId) {
    return;
  }
  state.teamEditor.linkFromNodeId = state.teamEditor.selectedNodeId;
  renderTeamGraph();
}

function connectNodes(sourceId, targetId) {
  if (!sourceId || !targetId || sourceId === targetId) {
    state.teamEditor.linkFromNodeId = null;
    renderTeamGraph();
    return;
  }
  mutateTeamSpec((spec) => {
    spec.flow.edges = spec.flow.edges || [];
    if (!spec.flow.edges.some((edge) => edge.from === sourceId && edge.to === targetId)) {
      spec.flow.edges.push({ from: sourceId, to: targetId });
    }
    state.teamEditor.linkFromNodeId = null;
    state.teamEditor.selectedNodeId = targetId;
  });
}

function updateMemberField(index, field, value) {
  mutateTeamSpec((spec) => {
    const member = spec.agents?.[index];
    if (!member) {
      return;
    }
    const oldKey = member.key;
    member[field] = value;
    if (field === "key" && oldKey && oldKey !== value) {
      (spec.flow?.nodes || []).forEach((node) => {
        if (node.agent === oldKey) {
          node.agent = value;
        }
      });
    }
  });
}

function removeMember(memberKey) {
  mutateTeamSpec((spec) => {
    spec.agents = (spec.agents || []).filter((item) => item.key !== memberKey);
    (spec.flow?.nodes || []).forEach((node) => {
      if (node.agent === memberKey) {
        node.agent = "";
      }
    });
  });
}

function updateSelectedNode() {
  const nodeId = state.teamEditor.selectedNodeId;
  if (!nodeId) {
    return;
  }
  mutateTeamSpec((spec) => {
    const node = (spec.flow?.nodes || []).find((item) => item.id === nodeId);
    if (!node) {
      return;
    }
    const oldId = node.id;
    node.id = teamNodeId.value.trim() || node.id;
    node.name = teamNodeName.value.trim() || undefined;
    node.agent = teamNodeAgent.value || undefined;
    node.instruction = teamNodeInstruction.value.trim() || undefined;
    node.expr = teamNodeExpr.value.trim() || undefined;
    node.max_iterations = teamNodeMaxIterations.value.trim() ? Number(teamNodeMaxIterations.value.trim()) : undefined;
    node.artifact_kind = teamNodeArtifactKind.value.trim() || "report";
    if (node.type === "artifact") {
      node.name = teamNodeArtifactName.value.trim() || node.name || undefined;
    }
    node.template = teamNodeTemplate.value.trim() || undefined;
    node.source = teamNodeSource.value.trim() || undefined;
    if (oldId !== node.id) {
      (spec.flow?.edges || []).forEach((edge) => {
        if (edge.from === oldId) {
          edge.from = node.id;
        }
        if (edge.to === oldId) {
          edge.to = node.id;
        }
      });
      if (spec.metadata?.ui_layout?.positions?.[oldId]) {
        spec.metadata.ui_layout.positions[node.id] = spec.metadata.ui_layout.positions[oldId];
        delete spec.metadata.ui_layout.positions[oldId];
      }
      state.teamEditor.selectedNodeId = node.id;
    }
  });
}

function updateEdgeWhen(sourceId, targetId, value) {
  mutateTeamSpec((spec) => {
    const edge = (spec.flow?.edges || []).find((item) => item.from === sourceId && item.to === targetId);
    if (!edge) {
      return;
    }
    if (value.trim()) {
      edge.when = value.trim();
    } else {
      delete edge.when;
    }
  });
}

function removeEdge(sourceId, targetId) {
  mutateTeamSpec((spec) => {
    spec.flow.edges = (spec.flow?.edges || []).filter((item) => !(item.from === sourceId && item.to === targetId));
  });
}

async function validateTeamGraph() {
  try {
    const payload = await api("/api/agent-center/team-templates/graph/validate", {
      method: "POST",
      body: JSON.stringify({ spec: buildTeamTemplateSpecFromForm() }),
    });
    if (payload.normalized_spec) {
      state.teamEditor.spec = payload.normalized_spec;
      syncTeamEditorToForm();
    }
    state.teamEditor.validation = payload;
    renderTeamEditor();
  } catch (error) {
    showResult(teamTemplateResult, { error: error.message });
  }
}

async function previewTeamGraph() {
  try {
    const payload = await api("/api/agent-center/team-templates/graph/preview", {
      method: "POST",
      body: JSON.stringify({
        team_template_id: state.editingTeamTemplateId,
        name: teamTemplateName.value.trim() || "Preview Team",
        spec: buildTeamTemplateSpecFromForm(),
      }),
    });
    state.teamEditor.preview = payload;
    if (!state.teamEditor.validation) {
      state.teamEditor.validation = {
        valid: payload.valid,
        errors: payload.errors || [],
        warnings: payload.warnings || [],
        communication: {},
      };
    }
    renderTeamEditor();
  } catch (error) {
    showResult(teamTemplateResult, { error: error.message });
  }
}

function renderTeamTemplates() {
  teamTemplateList.innerHTML = state.teamTemplates.length
    ? state.teamTemplates
        .map((item) => {
          const summary = teamSummary(item.spec_json || {});
          return cardMarkup({
            title: item.name,
            body: item.description || "团队模板",
            meta: `agents=${summary.agents.length} / nodes=${summary.nodes.length} / edges=${summary.edges.length}`,
            actions: `
              <button type="button" data-team-template-edit="${item.id}">编辑</button>
              <button type="button" class="ghost" data-team-build="${item.id}">构建</button>
            `,
            active: item.id === state.editingTeamTemplateId || item.id === state.selectedTeamTemplateId,
          });
        })
        .join("")
    : cardMarkup({ title: "暂无团队模板", body: "创建第一个团队模板。", meta: "" });
}

function teamDefinitionHierarchySpec(spec) {
  const payload = dictOrEmpty(spec);
  const root = payload.root && typeof payload.root === "object" ? dictOrEmpty(payload.root) : {};
  const children = Array.isArray(root.children) ? root.children : Array.isArray(payload.children) ? payload.children : [];
  const legacyMembers = Array.isArray(payload.members) ? payload.members : Array.isArray(payload.agents) ? payload.agents : [];
  return {
    workspaceId: String(payload.workspace_id || "local-workspace"),
    projectId: String(payload.project_id || "default-project"),
    lead: normalizeTeamDefinitionMember(root.lead || payload.lead || {}),
    children: children.map((item) => normalizeTeamDefinitionMember(item)),
    legacyMembers,
  };
}

function renderTeamDefinitions() {
  if (!teamDefinitionList) {
    return;
  }
  teamDefinitionList.innerHTML = state.teamDefinitions.length
    ? state.teamDefinitions
        .map((item) => {
          const spec = dictOrEmpty(item.spec_json);
          const hierarchy = teamDefinitionHierarchySpec(spec);
          const directChildCount = hierarchy.children.length || hierarchy.legacyMembers.length;
          const childAgentCount = hierarchy.children.filter((child) => child.kind === "agent").length;
          const childTeamCount = hierarchy.children.filter((child) => child.kind === "team").length;
          const leadLabel = hierarchy.lead.source_ref ? teamDefinitionMemberSourceLabel(hierarchy.lead) : "未配置 Lead Agent 模板";
          const summaryPrimary = hierarchy.children.length || hierarchy.lead.source_ref
            ? `Lead: ${leadLabel}`
            : hierarchy.legacyMembers.length
              ? `旧版团队定义 / 成员 ${hierarchy.legacyMembers.length}`
              : "尚未配置层级";
          const summarySecondary = hierarchy.children.length || hierarchy.lead.source_ref
            ? `直属 Subagent ${hierarchy.children.length} / Agent ${childAgentCount} / Team ${childTeamCount}`
            : "重新编辑后会切换为 create_deep_agent 层级";
          return `
            <article class="provider-row team-management-row">
              <div class="provider-main">
                <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
                <span title="${escapeAttribute(`直属 Subagent ${directChildCount}`)}">${escapeHtml(`直属 Subagent ${directChildCount}`)}</span>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(item.description || "未填写说明")}">${escapeHtml(item.description || "未填写说明")}</strong>
                <span title="${escapeAttribute(`${hierarchy.workspaceId} / ${hierarchy.projectId}`)}">${escapeHtml(`${hierarchy.workspaceId} / ${hierarchy.projectId}`)}</span>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(summaryPrimary)}">${escapeHtml(summaryPrimary)}</strong>
                <span title="${escapeAttribute(summarySecondary)}">${escapeHtml(summarySecondary)}</span>
              </div>
              <div class="provider-row-actions">
                <button type="button" data-team-definition-edit="${item.id}">编辑</button>
                <button type="button" class="ghost" data-team-definition-task="${item.id}">启动任务</button>
                <button type="button" class="ghost" data-team-definition-compile="${item.id}">编译</button>
                <button type="button" class="ghost" data-team-definition-build="${item.id}">兼容构建</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail"><strong>暂无团队管理项</strong><p>先创建第一个团队。</p></div>';
}

function renderBuilds() {
  buildList.innerHTML = state.builds.length
    ? state.builds
        .map((item) =>
          cardMarkup({
            title: item.name,
            body: item.description || buildTeamTemplateLabel(item),
            meta: `team=${buildTeamTemplateLabel(item)} / blueprint=${item.blueprint_id || "-"}`,
            actions: `
              <button type="button" data-build-open="${item.id}">查看</button>
              <button type="button" class="ghost" data-build-use="${item.id}">用于任务</button>
            `,
            active: item.id === state.selectedBuildId,
          }),
        )
        .join("")
    : cardMarkup({ title: "暂无 Build", body: "先构建团队模板。", meta: "" });
}

function renderBuildDetail(build) {
  if (!build) {
    setDetailPlaceholder(buildDetail, "请选择一个 Build。");
    return;
  }
  state.selectedBuildId = build.id;
  buildDetail.classList.remove("empty");
  const summary = blueprintSummary(build.spec_json || {});
  buildDetail.textContent =
    `Build：${build.id}\n` +
    `名称：${build.name}\n` +
    `团队模板：${buildTeamTemplateLabel(build)}\n` +
    `蓝图快照：${build.blueprint_id || ""}\n` +
    `角色模板：${summary.roleTemplates.length}\n` +
    `Agent：${summary.agents.length}\n` +
    `节点：${summary.nodes.length}\n` +
    `边：${summary.edges.length}\n\n` +
    `资源锁定\n${prettyJson(build.resource_lock_json || {})}\n\n` +
    `BlueprintSpec\n${prettyJson(build.spec_json || {})}`;
}

function renderBlueprints() {
  blueprintList.innerHTML = state.blueprints.length
    ? state.blueprints
        .map((item) => {
          const summary = blueprintSummary(item.spec_json || {});
          return cardMarkup({
            title: item.name,
            body: item.description || "内部蓝图快照",
            meta: `roles=${summary.roleTemplates.length} / agents=${summary.agents.length} / nodes=${summary.nodes.length}`,
            actions: `
              <button type="button" data-blueprint-open="${item.id}">查看</button>
              <button type="button" class="ghost" data-blueprint-use="${item.id}">用于任务</button>
            `,
            active: item.id === state.selectedBlueprintId,
          });
        })
        .join("")
    : cardMarkup({ title: "暂无蓝图", body: "Build 后会生成蓝图快照。", meta: "" });
}

function renderBlueprintDetail(blueprint) {
  if (!blueprint) {
    setDetailPlaceholder(blueprintDetail, "请选择一个蓝图。");
    return;
  }
  state.selectedBlueprintId = blueprint.id;
  blueprintDetail.classList.remove("empty");
  const summary = blueprintSummary(blueprint.spec_json || {});
  blueprintDetail.textContent =
    `Blueprint：${blueprint.id}\n` +
    `名称：${blueprint.name}\n` +
    `说明：${blueprint.description || ""}\n` +
    `角色模板：${summary.roleTemplates.length}\n` +
    `Agent：${summary.agents.length}\n` +
    `节点：${summary.nodes.length}\n` +
    `边：${summary.edges.length}\n\n` +
    prettyJson(blueprint.spec_json || {});
}

function renderRuns() {
  runList.innerHTML = state.runs.length
    ? state.runs
        .map((item) =>
          cardMarkup({
            title: item.summary || item.id,
            body: `状态：${item.status}`,
            meta: `run=${item.id}`,
            actions:
              item.status === "waiting_approval"
                ? `<button type="button" class="warn" data-run-resume="${item.id}">恢复</button>
                   <button type="button" class="ghost" data-run-open="${item.id}">查看</button>`
                : `<button type="button" class="ghost" data-run-open="${item.id}">查看</button>`,
            active: item.id === state.selectedRunId,
          }),
        )
        .join("")
    : cardMarkup({ title: "暂无 Run", body: "启动任务后显示。", meta: "" });
}

function renderApprovals() {
  approvalList.innerHTML = state.approvals.length
    ? state.approvals
        .map((item) =>
          cardMarkup({
            title: item.title,
            body: item.detail || "",
            meta: `run=${item.run_id}`,
            actions: `
              <button type="button" data-approval-approve="${item.id}" data-run-id="${item.run_id}">批准</button>
              <button type="button" class="ghost" data-approval-reject="${item.id}" data-run-id="${item.run_id}">拒绝</button>
            `,
          }),
        )
        .join("")
    : cardMarkup({ title: "暂无待审批项", body: "当前无待审批。", meta: "" });
}

function buildProviderPayloadFromForm() {
  const selectedType = providerPreset();
  const models = clone(state.providerEditor.models);
  const defaultChatModel = models.find((item) => item.model_type === "chat") || models[0] || null;
  return {
    id: state.editingProviderId,
    name: providerName.value.trim(),
    provider_type: providerType.value,
    config: {
      base_url: providerBaseUrl.value.trim(),
      api_version: selectedType?.default_api_version || "",
      backend: providerType.value,
      model: defaultChatModel?.name || "",
      models,
      skip_tls_verify: providerSkipTlsVerify.checked,
      temperature: 0.2,
    },
    ...(providerApiKey.value.trim() ? { secret: { api_key: providerApiKey.value.trim() } } : {}),
  };
}

function buildPluginPayloadFromForm() {
  const configBundle = readPluginConfigFromForm();
  const baseManifest = pluginEditorManifest();
  return {
    id: state.editingPluginId,
    key: pluginKey.value.trim(),
    name: pluginName.value.trim(),
    version: pluginVersion.value.trim() || "v1",
    plugin_type: pluginType.value.trim() || "toolset",
    description: pluginDescription.value.trim(),
    install_path: pluginInstallPath.value.trim() || null,
    manifest: {
      ...baseManifest,
      workbench_key: pluginWorkbenchKey.value.trim(),
      tools: commaListToArray(pluginTools.value),
      permissions: commaListToArray(pluginPermissions.value),
      description: pluginDescription.value.trim(),
    },
    config: configBundle.config,
    ...(Object.keys(configBundle.secret).length ? { secret: configBundle.secret } : {}),
  };
}

function buildAgentTemplatePayloadFromForm() {
  return {
    id: state.editingAgentTemplateId,
    name: agentTemplateName.value.trim(),
    role: agentTemplateRole.value.trim(),
    description: agentTemplateDescription.value.trim(),
    version: "v1",
    spec: {
      goal: agentTemplateGoal.value.trim(),
      instructions: agentTemplateInstructions.value.trim(),
      provider_ref: agentTemplateProvider.value,
      model: agentTemplateModel.value,
      memory_policy: agentTemplateMemoryPolicy.value,
      plugin_refs: getMultiSelectValues(agentTemplatePlugins),
      skills: getMultiSelectValues(agentTemplateSkills),
      delegation_mode: "none",
      metadata: currentAgentTemplateMetadata(),
    },
  };
}

function buildTeamTemplatePayloadFromForm() {
  syncTeamEditorToForm();
  return {
    id: state.editingTeamTemplateId,
    name: teamTemplateName.value.trim(),
    description: teamTemplateDescription.value.trim(),
    version: "v1",
    spec: buildTeamTemplateSpecFromForm(),
  };
}

function normalizeResourceSelections(values, collection) {
  return (values || [])
    .map((value) => {
      const raw =
        value && typeof value === "object" ? String(value.id || value.key || value.name || "").trim() : String(value || "").trim();
      const normalized = raw;
      if (!normalized) {
        return "";
      }
      const matched = collection.find((item) => item.id === normalized || item.key === normalized) || null;
      return matched?.id || normalized;
    })
    .filter(Boolean);
}

function normalizeProviderSelection(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const matched =
    state.providers.find((item) => item.id === raw) ||
    state.providers.find((item) => item.name === raw) ||
    state.providers.find((item) => String(item.config_json?.builtin_ref || "").trim() === raw) ||
    null;
  return matched?.id || raw;
}

function resetStaticMemoryForm(mode = currentStaticMemoryMode()) {
  mode = staticMemoryMode(mode);
  state.editingStaticMemoryId = null;
  state.staticMemoryEditorMode = mode;
  syncStaticMemoryEditorMode(mode);
  staticMemoryName.value = "";
  staticMemoryDescription.value = "";
  staticMemorySystemPrompt.value = "";
  hideResult(staticMemoryModalResult);
}

function fillStaticMemoryForm(item, { mode = null } = {}) {
  const resolvedMode = mode ? staticMemoryMode(mode) : currentStaticMemoryMode();
  const spec = staticMemorySpec(item);
  state.editingStaticMemoryId = item.id;
  state.staticMemoryEditorMode = resolvedMode;
  syncStaticMemoryEditorMode(resolvedMode);
  staticMemoryName.value = item.name || "";
  staticMemoryDescription.value = item.description || "";
  staticMemorySystemPrompt.value = spec.system_prompt || "";
  hideResult(staticMemoryModalResult);
  openStaticMemoryModal(resolvedMode);
}

function buildStaticMemoryPayloadFromForm() {
  const current = state.staticMemories.find((item) => item.id === state.editingStaticMemoryId) || null;
  const existingSpec = staticMemorySpec(current);
  const config = staticMemoryModeConfig();
  return {
    id: state.editingStaticMemoryId,
    name: staticMemoryName.value.trim(),
    description: staticMemoryDescription.value.trim(),
    version: "v1",
    spec: {
      ...existingSpec,
      system_prompt: staticMemorySystemPrompt.value.trim(),
    },
  };
}

function staticMemoryRowMarkup(item, mode) {
  const spec = staticMemorySpec(item);
  const title = item.name || item.key || item.id;
  const description = item.description || "可绑定到 Agent 或 deepagent 节点";
  const prompt = staticMemorySummary(spec.system_prompt, "未配置系统提示词");
  return `
    <article class="provider-row static-memory-row">
      <div class="provider-main">
        <strong title="${escapeAttribute(title)}">${escapeHtml(title)}</strong>
      </div>
      <div class="provider-cell">
        <strong>${escapeHtml(description)}</strong>
      </div>
      <div class="provider-cell">
        <strong title="${escapeAttribute(prompt)}">${escapeHtml(prompt)}</strong>
      </div>
      <div class="provider-row-actions">
        <button type="button" data-static-memory-edit="${item.id}">编辑</button>
        <button type="button" class="ghost warn" data-static-memory-delete="${item.id}">删除</button>
      </div>
    </article>
  `;
}

function renderStaticMemories() {
  if (!responsibilitySpecList) {
    return;
  }
  responsibilitySpecList.innerHTML = state.staticMemoryPage.items.length
    ? state.staticMemoryPage.items.map((item) => staticMemoryRowMarkup(item, "role")).join("")
    : '<div class="detail empty compact-detail"><strong>暂无角色管理项</strong><p>先创建第一份角色管理项。</p></div>';
  renderOffsetPagination(state.staticMemoryPage, staticMemoryPaginationMeta, "static-memory-page");
}

function knowledgeBaseDocumentsValue() {
  try {
    const parsed = safeParseJson(knowledgeBaseDocuments?.value, []);
    return Array.isArray(parsed) ? parsed.filter((item) => item && typeof item === "object") : [];
  } catch (error) {
    return Array.isArray(state.knowledgeBaseDocuments) ? state.knowledgeBaseDocuments.filter((item) => item && typeof item === "object") : [];
  }
}

function setKnowledgeBaseDocuments(items) {
  state.knowledgeBaseDocuments = (items || []).map((item) => ({ ...item }));
  if (knowledgeBaseDocuments) {
    knowledgeBaseDocuments.value = prettyJson(state.knowledgeBaseDocuments);
  }
  renderKnowledgeBaseDocuments();
}

function mutateKnowledgeBaseDocuments(mutator) {
  const items = knowledgeBaseDocumentsValue().map((item) => ({ ...item }));
  mutator(items);
  setKnowledgeBaseDocuments(items);
}

function renderKnowledgeBaseDocuments() {
  if (!knowledgeBaseDocumentList) {
    return;
  }
  const documents = knowledgeBaseDocumentsValue();
  knowledgeBaseDocumentList.innerHTML = documents.length
    ? documents
        .map(
          (item, index) => `
            <article class="member-card">
              <div class="member-card-head">
                <strong>${escapeHtml(item.title || item.key || `文档 ${index + 1}`)}</strong>
                <button type="button" class="ghost" data-knowledge-document-remove="${index}">移除</button>
              </div>
              <div class="form-grid two">
                <label><span>Key</span><input data-knowledge-document-field="key" data-document-index="${index}" value="${escapeHtml(item.key || "")}" /></label>
                <label><span>标题</span><input data-knowledge-document-field="title" data-document-index="${index}" value="${escapeHtml(item.title || "")}" /></label>
              </div>
              <label><span>来源路径</span><input data-knowledge-document-field="source_path" data-document-index="${index}" value="${escapeHtml(item.source_path || "")}" placeholder="可选" /></label>
              <label><span>内容</span><textarea data-knowledge-document-field="content_text" data-document-index="${index}" rows="8" placeholder="输入文档正文">${escapeHtml(item.content_text || "")}</textarea></label>
            </article>
          `,
        )
        .join("")
    : '<div class="detail empty compact-detail">暂无文档。先添加一篇供 `kb.retrieve` 检索的文档。</div>';
}

function addKnowledgeBaseDocument() {
  mutateKnowledgeBaseDocuments((items) => {
    const usedKeys = new Set(items.map((item) => String(item.key || "").trim()).filter(Boolean));
    let index = 1;
    let candidate = "doc_1";
    while (usedKeys.has(candidate)) {
      index += 1;
      candidate = `doc_${index}`;
    }
    items.push({
      key: candidate,
      title: `文档 ${index}`,
      source_path: "",
      content_text: "",
    });
  });
}

function updateKnowledgeBaseDocumentField(index, field, value) {
  mutateKnowledgeBaseDocuments((items) => {
    const item = items[index];
    if (!item) {
      return;
    }
    item[field] = value;
  });
}

function removeKnowledgeBaseDocument(index) {
  mutateKnowledgeBaseDocuments((items) => {
    items.splice(index, 1);
  });
}

function resetKnowledgeBaseForm() {
  state.editingKnowledgeBaseId = null;
  state.knowledgeBaseBaseConfig = {};
  state.knowledgeBasePersistedDocumentIds = [];
  knowledgeBaseKey.value = "";
  knowledgeBaseName.value = "";
  knowledgeBaseDescription.value = "";
  setKnowledgeBaseDocuments([]);
  hideResult(knowledgeBaseResult);
  renderKnowledgeBases();
}

async function fillKnowledgeBaseForm(item) {
  state.editingKnowledgeBaseId = item.id;
  state.knowledgeBaseBaseConfig = clone(item.config_json || {});
  knowledgeBaseKey.value = item.key || "";
  knowledgeBaseName.value = item.name || "";
  knowledgeBaseDescription.value = item.description || "";
  await loadKnowledgeBaseDocuments(item.id);
  renderKnowledgeBaseDocuments();
  hideResult(knowledgeBaseResult);
  renderKnowledgeBases();
}

function buildKnowledgeBasePayloadFromForm() {
  return {
    id: state.editingKnowledgeBaseId,
    key: knowledgeBaseKey.value.trim(),
    name: knowledgeBaseName.value.trim(),
    description: knowledgeBaseDescription.value.trim(),
    config: clone(state.knowledgeBaseBaseConfig || {}),
  };
}

function renderKnowledgeBases() {
  knowledgeBaseList.innerHTML = state.knowledgeBases.length
    ? state.knowledgeBases
        .map((item) => {
          const documents = state.editingKnowledgeBaseId === item.id ? knowledgeBaseDocumentsValue().length : null;
          return cardMarkup({
            title: item.name || item.key || item.id,
            body: item.description || "知识库",
            meta: `key=${item.key || "-"}${documents === null ? "" : ` / docs=${documents}`}`,
            actions: `
              <button type="button" data-knowledge-base-edit="${item.id}">编辑</button>
              <button type="button" class="ghost" data-knowledge-base-delete="${item.id}">删除</button>
            `,
            active: item.id === state.editingKnowledgeBaseId,
          });
        })
        .join("")
    : cardMarkup({ title: "暂无知识库", body: "先创建第一个知识库。", meta: "" });
}

function resetReviewPolicyForm() {
  state.editingReviewPolicyId = null;
  state.reviewPolicyBaseSpec = {};
  reviewPolicyKey.value = "";
  reviewPolicyName.value = "";
  reviewPolicyDescription.value = "";
  populateReviewPolicyPluginOptions();
  populateReviewPolicyConditionOptions();
  setMultiSelectValues(reviewPolicyTriggers, []);
  setMultiSelectValues(reviewPolicyActions, []);
  setMultiSelectValues(reviewPolicyPluginKeys, []);
  reviewPolicyRiskTags.value = "";
  setMultiSelectValues(reviewPolicyMessageTypes, []);
  setMultiSelectValues(reviewPolicyMemoryScopes, []);
  setMultiSelectValues(reviewPolicyMemoryKinds, []);
  hideResult(reviewPolicyResult);
  renderReviewPolicies();
}

function fillReviewPolicyForm(item) {
  const spec = dictOrEmpty(item?.spec_json);
  const conditions = dictOrEmpty(spec.conditions);
  state.editingReviewPolicyId = item.id;
  state.reviewPolicyBaseSpec = clone(spec);
  reviewPolicyKey.value = item.key || "";
  reviewPolicyName.value = item.name || "";
  reviewPolicyDescription.value = item.description || "";
  populateReviewPolicyPluginOptions();
  populateReviewPolicyConditionOptions();
  setMultiSelectValues(reviewPolicyTriggers, spec.triggers || []);
  setMultiSelectValues(reviewPolicyActions, spec.actions || []);
  setMultiSelectValues(reviewPolicyPluginKeys, conditions.plugin_keys || []);
  reviewPolicyRiskTags.value = listToLines(conditions.risk_tags || []);
  setMultiSelectValues(reviewPolicyMessageTypes, conditions.message_types || []);
  setMultiSelectValues(reviewPolicyMemoryScopes, conditions.memory_scopes || conditions.scopes || []);
  setMultiSelectValues(reviewPolicyMemoryKinds, conditions.memory_kinds || []);
  hideResult(reviewPolicyResult);
  renderReviewPolicies();
}

function buildReviewPolicyPayloadFromForm() {
  const spec = clone(state.reviewPolicyBaseSpec || {});
  const conditions = dictOrEmpty(spec.conditions);
  conditions.plugin_keys = getMultiSelectValues(reviewPolicyPluginKeys);
  conditions.risk_tags = linesToList(reviewPolicyRiskTags.value);
  conditions.message_types = getMultiSelectValues(reviewPolicyMessageTypes);
  conditions.memory_scopes = getMultiSelectValues(reviewPolicyMemoryScopes);
  conditions.memory_kinds = getMultiSelectValues(reviewPolicyMemoryKinds);
  spec.triggers = getMultiSelectValues(reviewPolicyTriggers);
  spec.actions = getMultiSelectValues(reviewPolicyActions);
  spec.conditions = conditions;
  return {
    id: state.editingReviewPolicyId,
    key: reviewPolicyKey.value.trim(),
    name: reviewPolicyName.value.trim(),
    description: reviewPolicyDescription.value.trim(),
    version: "v1",
    spec,
  };
}

function renderReviewPolicies() {
  reviewPolicyList.innerHTML = state.reviewPolicies.length
    ? state.reviewPolicies
        .map((item) => {
          const spec = dictOrEmpty(item.spec_json);
          return cardMarkup({
            title: item.name || item.key || item.id,
            body: item.description || "审核策略",
            meta: `key=${item.key || "-"} / triggers=${(spec.triggers || []).length}`,
            actions: `
              <button type="button" data-review-policy-edit="${item.id}">编辑</button>
              <button type="button" class="ghost" data-review-policy-delete="${item.id}">删除</button>
            `,
            active: item.id === state.editingReviewPolicyId,
          });
        })
        .join("")
    : cardMarkup({ title: "暂无审核策略", body: "先创建第一个审核策略。", meta: "" });
}

function resetMemoryProfileForm() {
  state.editingMemoryProfileId = null;
  state.memoryProfileBaseSpec = {};
  memoryProfileKey.value = "";
  memoryProfileName.value = "";
  memoryProfileDescription.value = "";
  memoryProfileShortTermEnabled.checked = true;
  memoryProfileSummaryTriggerTokens.value = "1800";
  memoryProfileSummaryMaxTokens.value = "320";
  memoryProfileLongTermEnabled.checked = true;
  memoryProfileNamespaceStrategy.value = "agent_team_project";
  memoryProfileTtlDays.value = "30";
  memoryProfileBackgroundEnabled.checked = true;
  memoryProfileDebounceSeconds.value = "30";
  populateStaticMemoryScopeOptions();
  setMultiSelectValues(memoryProfileReadScopes, ["agent", "team", "project"]);
  setMultiSelectValues(memoryProfileWriteScopes, ["agent", "team"]);
  hideResult(memoryProfileResult);
  renderMemoryProfiles();
}

function fillMemoryProfileForm(item) {
  const spec = dictOrEmpty(item?.spec_json);
  const shortTerm = dictOrEmpty(spec.short_term);
  const longTerm = dictOrEmpty(spec.long_term);
  const backgroundReflection = dictOrEmpty(spec.background_reflection);
  state.editingMemoryProfileId = item.id;
  state.memoryProfileBaseSpec = clone(spec);
  memoryProfileKey.value = item.key || "";
  memoryProfileName.value = item.name || "";
  memoryProfileDescription.value = item.description || "";
  memoryProfileShortTermEnabled.checked = Boolean(shortTerm.enabled ?? true);
  memoryProfileSummaryTriggerTokens.value = String(shortTerm.summary_trigger_tokens ?? 1800);
  memoryProfileSummaryMaxTokens.value = String(shortTerm.summary_max_tokens ?? 320);
  memoryProfileLongTermEnabled.checked = Boolean(longTerm.enabled ?? true);
  memoryProfileNamespaceStrategy.value = String(longTerm.namespace_strategy || "agent_team_project");
  memoryProfileTtlDays.value = String(longTerm.ttl_days ?? 30);
  memoryProfileBackgroundEnabled.checked = Boolean(backgroundReflection.enabled ?? true);
  memoryProfileDebounceSeconds.value = String(backgroundReflection.debounce_seconds ?? 30);
  populateStaticMemoryScopeOptions();
  setMultiSelectValues(memoryProfileReadScopes, spec.read_scopes || []);
  setMultiSelectValues(memoryProfileWriteScopes, spec.write_scopes || []);
  hideResult(memoryProfileResult);
  renderMemoryProfiles();
}

function buildMemoryProfilePayloadFromForm() {
  const spec = clone(state.memoryProfileBaseSpec || {});
  spec.short_term = {
    enabled: Boolean(memoryProfileShortTermEnabled.checked),
    summary_trigger_tokens: Number(memoryProfileSummaryTriggerTokens.value || 1800),
    summary_max_tokens: Number(memoryProfileSummaryMaxTokens.value || 320),
  };
  spec.long_term = {
    enabled: Boolean(memoryProfileLongTermEnabled.checked),
    namespace_strategy: memoryProfileNamespaceStrategy.value.trim() || "agent_team_project",
    ttl_days: Number(memoryProfileTtlDays.value || 30),
  };
  spec.background_reflection = {
    enabled: Boolean(memoryProfileBackgroundEnabled.checked),
    debounce_seconds: Number(memoryProfileDebounceSeconds.value || 30),
  };
  spec.read_scopes = getMultiSelectValues(memoryProfileReadScopes);
  spec.write_scopes = getMultiSelectValues(memoryProfileWriteScopes);
  return {
    id: state.editingMemoryProfileId,
    key: memoryProfileKey.value.trim(),
    name: memoryProfileName.value.trim(),
    description: memoryProfileDescription.value.trim(),
    version: "v1",
    spec,
  };
}

function renderMemoryProfiles() {
  memoryProfileList.innerHTML = state.memoryProfiles.length
    ? state.memoryProfiles
        .map((item) => {
          const spec = dictOrEmpty(item.spec_json);
          const longTerm = dictOrEmpty(spec.long_term);
          return cardMarkup({
            title: item.name || item.key || item.id,
            body: item.description || "记忆画像",
            meta: `key=${item.key || "-"} / ttl_days=${longTerm.ttl_days ?? "-"} / read=${(spec.read_scopes || []).length}`,
            actions: `
              <button type="button" data-memory-profile-edit="${item.id}">编辑</button>
              <button type="button" class="ghost" data-memory-profile-delete="${item.id}">删除</button>
            `,
            active: item.id === state.editingMemoryProfileId,
          });
        })
        .join("")
    : cardMarkup({ title: "暂无记忆画像", body: "先创建第一个记忆画像。", meta: "" });
}

function syncAgentDefinitionModelOptions(selectedModel = "") {
  renderModelSelect(agentDefinitionModel, providerModelsByType(agentDefinitionProvider?.value || "", "chat"), selectedModel);
}

function resetAgentDefinitionForm({ openModal = false } = {}) {
  state.editingAgentDefinitionId = null;
  state.agentDefinitionBaseSpec = {};
  closeAllTagMultiSelects();
  agentDefinitionName.value = "";
  populateAgentDefinitionReferenceOptions();
  setMultiSelectValues(agentDefinitionPlugins, []);
  setMultiSelectValues(agentDefinitionSkills, []);
  agentDefinitionKnowledgeBases.value = "";
  agentDefinitionReviewPolicies.value = "";
  syncAgentDefinitionModelOptions();
  if (agentDefinitionModalTitle) {
    agentDefinitionModalTitle.textContent = "新增 Agent 管理项";
  }
  hideResult(agentDefinitionResult);
  hideResult(agentDefinitionModalResult);
  renderAgentDefinitions();
  if (openModal) {
    openAgentDefinitionModal();
  }
}

function fillAgentDefinitionForm(item, { openModal = false } = {}) {
  const spec = dictOrEmpty(item?.spec_json);
  const staticMemoryRefs = [spec.static_memory_ref || spec.static_memory_id || ""];
  state.editingAgentDefinitionId = item.id;
  state.agentDefinitionBaseSpec = clone(spec);
  closeAllTagMultiSelects();
  const providerValue = normalizeProviderSelection(spec.provider_ref || spec.provider_profile_id || "");
  const staticMemoryValue = normalizeResourceSelections(staticMemoryRefs, state.staticMemories)[0] || "";
  agentDefinitionName.value = item.name || "";
  populateAgentDefinitionReferenceOptions(staticMemoryRefs);
  if (providerValue) {
    agentDefinitionProvider.value = providerValue;
  }
  syncAgentDefinitionModelOptions(spec.model || "");
  if (staticMemoryValue) {
    agentDefinitionStaticMemory.value = staticMemoryValue;
  }
  setMultiSelectValues(
    agentDefinitionPlugins,
    normalizeResourceSelections(spec.tool_plugin_refs || spec.plugin_refs || [], state.plugins),
  );
  setMultiSelectValues(
    agentDefinitionSkills,
    normalizeResourceSelections(spec.skill_refs || spec.skills || [], state.skills),
  );
  agentDefinitionKnowledgeBases.value = normalizeResourceSelections(spec.knowledge_base_refs || [], state.knowledgeBases)[0] || "";
  agentDefinitionReviewPolicies.value = normalizeResourceSelections(spec.review_policy_refs || [], state.reviewPolicies)[0] || "";
  if (agentDefinitionModalTitle) {
    agentDefinitionModalTitle.textContent = "编辑 Agent 管理项";
  }
  hideResult(agentDefinitionResult);
  hideResult(agentDefinitionModalResult);
  renderAgentDefinitions();
  if (openModal) {
    openAgentDefinitionModal();
  }
}

function buildAgentDefinitionPayloadFromForm() {
  const spec = clone(state.agentDefinitionBaseSpec || {});
  const current = state.editingAgentDefinitionId
    ? state.agentDefinitions.find((item) => item.id === state.editingAgentDefinitionId) || null
    : null;
  spec.provider_ref = agentDefinitionProvider.value;
  spec.model = agentDefinitionModel.value;
  spec.tool_plugin_refs = getMultiSelectValues(agentDefinitionPlugins);
  spec.plugin_refs = [];
  spec.skill_refs = getMultiSelectValues(agentDefinitionSkills);
  spec.skills = [];
  spec.static_memory_ref = agentDefinitionStaticMemory.value || null;
  spec.static_memory_id = null;
  spec.knowledge_base_refs = agentDefinitionKnowledgeBases.value ? [agentDefinitionKnowledgeBases.value] : [];
  delete spec.memory_profile_ref;
  delete spec.memory_profile_id;
  spec.review_policy_refs = agentDefinitionReviewPolicies.value ? [agentDefinitionReviewPolicies.value] : [];
  return {
    id: state.editingAgentDefinitionId,
    name: agentDefinitionName.value.trim(),
    description: current?.description || "",
    version: "v1",
    spec,
  };
}

function agentDefinitionReferenceLabel(collection, value, emptyLabel = "未配置") {
  if (!value) {
    return emptyLabel;
  }
  const item = collection.find((entry) => entry.id === value || entry.key === value) || null;
  return item?.name || item?.key || value;
}

function agentDefinitionRowMarkup(item) {
  const spec = dictOrEmpty(item.spec_json);
  const provider = state.providers.find((entry) => entry.id === spec.provider_ref) || null;
  const providerLabel = provider?.name || spec.provider_ref || "未配置 Provider";
  const modelLabel = spec.model || "未配置模型";
  const staticMemoryLabel = agentDefinitionReferenceLabel(
    state.staticMemories,
    spec.static_memory_ref || spec.static_memory_id || "",
    "未配置角色管理",
  );
  const pluginCount = (spec.tool_plugin_refs || spec.plugin_refs || []).length;
  const skillCount = (spec.skill_refs || spec.skills || []).length;
  const knowledgeBaseLabel = agentDefinitionReferenceLabel(state.knowledgeBases, (spec.knowledge_base_refs || [])[0] || "", "未配置知识库");
  const reviewPolicyLabel = agentDefinitionReferenceLabel(state.reviewPolicies, (spec.review_policy_refs || [])[0] || "", "未配置审核策略");
  const resourceSummary =
    `角色管理: ${staticMemoryLabel} / 插件 ${pluginCount} / 技能 ${skillCount} / ` +
    `知识库: ${knowledgeBaseLabel} / 审核: ${reviewPolicyLabel}`;
  return `
    <article class="provider-row agent-management-row">
      <div class="provider-main">
        <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
      </div>
      <div class="provider-cell">
        <strong title="${escapeAttribute(item.description || "未填写说明")}">${escapeHtml(item.description || "未填写说明")}</strong>
      </div>
      <div class="provider-cell">
        <strong title="${escapeAttribute(`${providerLabel} / ${modelLabel}`)}">${escapeHtml(`${providerLabel} / ${modelLabel}`)}</strong>
        <span title="${escapeAttribute(resourceSummary)}">${escapeHtml(resourceSummary)}</span>
      </div>
      <div class="provider-row-actions">
        <button type="button" data-agent-definition-edit="${item.id}">编辑</button>
        <button type="button" class="ghost warn" data-agent-definition-delete="${item.id}">删除</button>
      </div>
    </article>
  `;
}

function renderAgentDefinitions() {
  if (!agentDefinitionList) {
    return;
  }
  renderOffsetPagination(state.agentDefinitionPage, agentDefinitionPaginationMeta, "agent-definition-page");
  agentDefinitionList.innerHTML = state.agentDefinitionPage.items.length
    ? state.agentDefinitionPage.items.map((item) => agentDefinitionRowMarkup(item)).join("")
    : '<div class="detail empty compact-detail"><strong>暂无 Agent 管理项</strong><p>先组合出第一个完整 Agent。</p></div>';
}

function dictOrEmpty(value) {
  return value && typeof value === "object" ? { ...value } : {};
}

function teamDefinitionReviewOverridesValue() {
  try {
    const parsed = safeParseJson(teamDefinitionReviewOverrides?.value, []);
    return Array.isArray(parsed) ? parsed.filter((item) => item && typeof item === "object") : [];
  } catch (error) {
    return [];
  }
}

function setTeamDefinitionReviewOverrides(items) {
  teamDefinitionReviewOverrides.value = prettyJson(items || []);
  renderTeamDefinitionReviewOverrides();
}

function mutateTeamDefinitionReviewOverrides(mutator) {
  const items = teamDefinitionReviewOverridesValue().map((item) => ({ ...item }));
  mutator(items);
  setTeamDefinitionReviewOverrides(items);
}

function renderTeamDefinitionReviewOverrides() {
  if (!teamDefinitionReviewOverrideList) {
    return;
  }
  const overrides = teamDefinitionReviewOverridesValue();
  const members = teamDefinitionMemberOptions();
  const modeOptions = uiMetadataOptions("team_edge_review", "modes");
  const messageTypeOptions = uiMetadataOptions("team_edge_review", "message_types");
  const phaseOptions = uiMetadataOptions("team_edge_review", "phases");
  const memberOptions = members
    .map((item) => `<option value="${escapeHtml(item.key)}">${escapeHtml(item.name)} / ${escapeHtml(item.key)}</option>`)
    .join("");
  teamDefinitionReviewOverrideList.innerHTML = overrides.length
    ? overrides
        .map((item, index) => {
          const sourceAgentId = String(item.source_agent_id || "").trim();
          const targetAgentId = String(item.target_agent_id || "").trim();
          return `
            <article class="member-card">
              <div class="member-card-head">
                <strong>${escapeHtml(item.name || `${sourceAgentId || "任意来源"} -> ${targetAgentId || "任意目标"}`)}</strong>
                <button type="button" class="ghost" data-team-definition-review-override-remove="${index}">移除</button>
              </div>
              <div class="form-grid two">
                <label><span>名称</span><input data-team-definition-review-override-field="name" data-review-override-index="${index}" value="${escapeHtml(item.name || "")}" placeholder="可留空" /></label>
                <label>
                  <span>模式</span>
                  <select data-team-definition-review-override-field="mode" data-review-override-index="${index}">
                    ${multiSelectOptionsMarkup(modeOptions, [String(item.mode || "must_review_before")])}
                  </select>
                </label>
              </div>
              <div class="form-grid two">
                <label>
                  <span>源 Agent</span>
                  <select data-team-definition-review-override-field="source_agent_id" data-review-override-index="${index}">
                    <option value="">任意来源</option>
                    ${memberOptions.replace(`value="${escapeHtml(sourceAgentId)}"`, `value="${escapeHtml(sourceAgentId)}" selected`)}
                  </select>
                </label>
                <label>
                  <span>目标 Agent</span>
                  <select data-team-definition-review-override-field="target_agent_id" data-review-override-index="${index}">
                    <option value="">任意目标</option>
                    ${memberOptions.replace(`value="${escapeHtml(targetAgentId)}"`, `value="${escapeHtml(targetAgentId)}" selected`)}
                  </select>
                </label>
              </div>
              <div class="form-grid two">
                <label>
                  <span>消息类型</span>
                  <select data-team-definition-review-override-field="message_types" data-review-override-index="${index}" multiple size="4">
                    ${multiSelectOptionsMarkup(messageTypeOptions, Array.isArray(item.message_types) ? item.message_types : [])}
                  </select>
                </label>
                <label>
                  <span>阶段</span>
                  <select data-team-definition-review-override-field="phases" data-review-override-index="${index}" multiple size="3">
                    ${multiSelectOptionsMarkup(phaseOptions, Array.isArray(item.phases) ? item.phases : [])}
                  </select>
                </label>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail">暂无 Team Edge 审批策略。需要对某条 Agent 边做人工前审时再添加。</div>';
}

function addTeamDefinitionReviewOverride() {
  mutateTeamDefinitionReviewOverrides((items) => {
    items.push({
      name: "",
      mode: "must_review_before",
      source_agent_id: "",
      target_agent_id: "",
      message_types: ["dialogue"],
      phases: [],
    });
  });
}

function updateTeamDefinitionReviewOverrideField(index, field, value) {
  mutateTeamDefinitionReviewOverrides((items) => {
    const item = items[index];
    if (!item) {
      return;
    }
    if (field === "message_types" || field === "phases") {
      item[field] = Array.isArray(value) ? value.filter(Boolean) : commaListToArray(value);
      return;
    }
    item[field] = value;
  });
}

function removeTeamDefinitionReviewOverride(index) {
  mutateTeamDefinitionReviewOverrides((items) => {
    items.splice(index, 1);
  });
}

function resetTeamDefinitionForm({ openModal = false } = {}) {
  state.editingTeamDefinitionId = null;
  state.teamDefinitionBaseSpec = {};
  teamDefinitionKey.value = "";
  teamDefinitionName.value = "";
  teamDefinitionWorkspace.value = "local-workspace";
  teamDefinitionProject.value = "default-project";
  teamDefinitionDescription.value = "";
  renderTeamDefinitionLeadAgentOptions("");
  if (teamDefinitionLeadAgentTemplate) {
    teamDefinitionLeadAgentTemplate.value = "";
  }
  setTeamDefinitionMembers([]);
  if (teamDefinitionEntryAgent) {
    teamDefinitionEntryAgent.value = "";
  }
  if (teamDefinitionTerminationMode) {
    teamDefinitionTerminationMode.value = "member_finishers";
  }
  if (teamDefinitionTerminationAgents) {
    setMultiSelectValues(teamDefinitionTerminationAgents, []);
  }
  syncTeamDefinitionPolicyControls();
  hideResult(teamDefinitionResult);
  hideResult(teamDefinitionModalResult);
  if (teamDefinitionModalTitle) {
    teamDefinitionModalTitle.textContent = "新增团队管理项";
  }
  renderTeamDefinitions();
  if (openModal) {
    openTeamDefinitionModal();
  }
}

function fillTeamDefinitionForm(definition, { openModal = false } = {}) {
  const spec = clone(definition?.spec_json || {});
  const hierarchy = teamDefinitionHierarchySpec(spec);

  state.editingTeamDefinitionId = definition.id;
  state.teamDefinitionBaseSpec = clone(spec);
  teamDefinitionKey.value = definition.key || "";
  teamDefinitionName.value = definition.name || "";
  teamDefinitionWorkspace.value = hierarchy.workspaceId;
  teamDefinitionProject.value = hierarchy.projectId;
  teamDefinitionDescription.value = definition.description || "";
  renderTeamDefinitionLeadAgentOptions(hierarchy.lead.source_ref);
  if (teamDefinitionLeadAgentTemplate && hierarchy.lead.source_ref) {
    teamDefinitionLeadAgentTemplate.value = hierarchy.lead.source_ref;
  }
  setTeamDefinitionMembers(hierarchy.children);
  hideResult(teamDefinitionResult);
  hideResult(teamDefinitionModalResult);
  if (teamDefinitionModalTitle) {
    teamDefinitionModalTitle.textContent = "编辑团队管理项";
  }
  renderTeamDefinitions();
  if (openModal) {
    openTeamDefinitionModal();
  }
}

function buildTeamDefinitionPayloadFromForm() {
  const children = teamDefinitionMembersValue();
  const leadAgentTemplateRef = normalizeTeamDefinitionReference("agent_template", teamDefinitionLeadAgentTemplate?.value || "");
  if (!leadAgentTemplateRef) {
    throw new Error("请先选择 Lead Agent 模板。");
  }
  children.forEach((member, index) => {
    if (!String(member.source_kind || "").trim()) {
      throw new Error(`第 ${index + 1} 个 Subagent 缺少节点类型。`);
    }
    if (!String(member.source_ref || "").trim()) {
      throw new Error(`第 ${index + 1} 个 Subagent 缺少引用来源。`);
    }
  });
  const spec = clone(state.teamDefinitionBaseSpec || {});
  spec.workspace_id = teamDefinitionWorkspace.value.trim() || "local-workspace";
  spec.project_id = teamDefinitionProject.value.trim() || "default-project";
  spec.lead = {
    kind: "agent",
    source_kind: "agent_template",
    agent_template_ref: leadAgentTemplateRef,
  };
  spec.children = children.map((member) =>
    member.source_kind === "team_definition"
      ? {
          kind: "team",
          source_kind: "team_definition",
          team_definition_ref: member.source_ref,
          ...(member.name ? { name: member.name } : {}),
        }
      : {
          kind: "agent",
          source_kind: "agent_template",
          agent_template_ref: member.source_ref,
          ...(member.name ? { name: member.name } : {}),
        },
  );
  delete spec.root;
  delete spec.members;
  delete spec.agents;
  delete spec.review_policy_refs;
  delete spec.review_overrides;
  delete spec.shared_kb_bindings;
  delete spec.shared_knowledge_base_refs;
  delete spec.shared_knowledge_bases;
  delete spec.shared_static_memory_bindings;
  delete spec.shared_static_memory_refs;
  delete spec.shared_static_memories;
  delete spec.task_entry_policy;
  delete spec.task_entry_agent;
  delete spec.termination_policy;

  return {
    id: state.editingTeamDefinitionId,
    name: teamDefinitionName.value.trim(),
    description: teamDefinitionDescription.value.trim(),
    version: "v1",
    spec,
  };
}

async function loadControlPlane() {
  const payload = await api("/api/control-plane");
  state.summary = payload.summary || {};
  state.storage = payload.storage || null;
  state.providerTypes = payload.provider_types || [];
  state.recentBuilds = payload.recent_builds || [];
  state.recentRuns = payload.recent_runs || [];
}

async function loadProviderTypes() {
  const payload = await api("/api/agent-center/provider-types");
  state.providerTypes = payload.items || [];
}

async function loadUiMetadata() {
  const payload = await api("/api/agent-center/ui-metadata");
  state.uiMetadata = payload && typeof payload === "object" ? payload : DEFAULT_UI_METADATA;
}

async function loadProviders() {
  const payload = await api("/api/agent-center/providers");
  state.providers = payload.items || [];
  populateProviderOptions();
}

async function loadRetrievalSettings() {
  const payload = await api("/api/agent-center/retrieval-settings");
  state.retrievalSettings = {
    settings: payload.settings || { embedding: { mode: "hash" }, rerank: { mode: "disabled" } },
    warnings: payload.warnings || [],
    updated_at: payload.updated_at || null,
  };
}

async function loadProviderPage() {
  const params = new URLSearchParams({
    limit: String(state.providerPage.limit || 10),
    offset: String(state.providerPage.offset || 0),
  });
  if (state.providerPage.query) {
    params.set("query", state.providerPage.query);
  }
  if (state.providerPage.providerType) {
    params.set("provider_type", state.providerPage.providerType);
  }
  const payload = await api(`/api/agent-center/providers?${params.toString()}`);
  state.providerPage.items = payload.items || [];
  state.providerPage.total = payload.total || 0;
  state.providerPage.limit = payload.limit || state.providerPage.limit;
  state.providerPage.offset = payload.offset || 0;
}

async function loadPlugins() {
  const payload = await api("/api/agent-center/plugins");
  state.plugins = payload.items || [];
  populatePluginOptions();
}

async function loadBuiltinPlugins() {
  const payload = await api("/api/agent-center/plugins/builtins");
  state.builtinPlugins = payload.items || [];
}

async function loadSkills() {
  const payload = await api("/api/agent-center/skills");
  state.skills = payload.items || [];
}

async function loadReviewPolicies() {
  const payload = await api("/api/agent-center/review-policies");
  state.reviewPolicies = payload.items || [];
}

async function loadMemoryProfiles() {
  const payload = await api("/api/agent-center/memory-profiles");
  state.memoryProfiles = payload.items || [];
}

async function loadPluginPage() {
  const params = new URLSearchParams({
    limit: String(state.pluginPage.limit || 10),
    offset: String(state.pluginPage.offset || 0),
  });
  const payload = await api(`/api/agent-center/plugins?${params.toString()}`);
  state.pluginPage.items = payload.items || [];
  state.pluginPage.total = payload.total || 0;
  state.pluginPage.limit = payload.limit || state.pluginPage.limit;
  state.pluginPage.offset = payload.offset || 0;
}

async function loadAgentTemplates() {
  const payload = await api("/api/agent-center/agent-templates");
  state.agentTemplates = payload.items || [];
}

async function loadAgentDefinitions() {
  const payload = await api("/api/agent-center/agent-definitions");
  state.agentDefinitions = payload.items || [];
}

async function loadAgentDefinitionPage() {
  const params = new URLSearchParams({
    limit: String(state.agentDefinitionPage.limit || 10),
    offset: String(state.agentDefinitionPage.offset || 0),
  });
  const payload = await api(`/api/agent-center/agent-definitions?${params.toString()}`);
  state.agentDefinitionPage.items = payload.items || [];
  state.agentDefinitionPage.total = payload.total || 0;
  state.agentDefinitionPage.limit = payload.limit || state.agentDefinitionPage.limit;
  state.agentDefinitionPage.offset = payload.offset || 0;
}

async function loadKnowledgeBases() {
  const payload = await api("/api/agent-center/knowledge-bases");
  state.knowledgeBases = payload.items || [];
}

async function loadStaticMemories() {
  const payload = await api("/api/agent-center/static-memories");
  state.staticMemories = payload.items || [];
}

async function loadStaticMemoryPage() {
  const params = new URLSearchParams({
    limit: String(state.staticMemoryPage.limit || 10),
    offset: String(state.staticMemoryPage.offset || 0),
  });
  const payload = await api(`/api/agent-center/static-memories?${params.toString()}`);
  state.staticMemoryPage.items = payload.items || [];
  state.staticMemoryPage.total = payload.total || 0;
  state.staticMemoryPage.limit = payload.limit || state.staticMemoryPage.limit;
  state.staticMemoryPage.offset = payload.offset || 0;
}

async function loadKnowledgeBaseDocuments(knowledgeBaseId) {
  if (!knowledgeBaseId) {
    state.knowledgeBaseDocuments = [];
    state.knowledgeBasePersistedDocumentIds = [];
    if (knowledgeBaseDocuments) {
      knowledgeBaseDocuments.value = prettyJson([]);
    }
    return;
  }
  const payload = await api(`/api/agent-center/knowledge-documents?knowledge_base_id=${encodeURIComponent(knowledgeBaseId)}`);
  state.knowledgeBaseDocuments = payload.items || [];
  state.knowledgeBasePersistedDocumentIds = state.knowledgeBaseDocuments
    .map((item) => String(item.id || "").trim())
    .filter(Boolean);
  if (knowledgeBaseDocuments) {
    knowledgeBaseDocuments.value = prettyJson(state.knowledgeBaseDocuments);
  }
}

async function loadTeamDefinitions() {
  const payload = await api("/api/agent-center/team-definitions");
  state.teamDefinitions = payload.items || [];
  if (state.editingTeamDefinitionId && !state.teamDefinitions.some((item) => item.id === state.editingTeamDefinitionId)) {
    state.editingTeamDefinitionId = null;
    state.teamDefinitionBaseSpec = {};
  }
  if (state.selectedTaskTeamDefinitionId && !state.teamDefinitions.some((item) => item.id === state.selectedTaskTeamDefinitionId)) {
    state.selectedTaskTeamDefinitionId = null;
  }
}

async function loadAgentTemplatePage() {
  const params = new URLSearchParams({
    limit: String(state.agentTemplatePage.limit || 10),
    offset: String(state.agentTemplatePage.offset || 0),
  });
  const payload = await api(`/api/agent-center/agent-templates?${params.toString()}`);
  state.agentTemplatePage.items = payload.items || [];
  state.agentTemplatePage.total = payload.total || 0;
  state.agentTemplatePage.limit = payload.limit || state.agentTemplatePage.limit;
  state.agentTemplatePage.offset = payload.offset || 0;
}

async function loadTeamTemplates() {
  const payload = await api("/api/agent-center/team-templates");
  state.teamTemplates = payload.items || [];
  if (state.selectedTeamTemplateId && !state.teamTemplates.some((item) => item.id === state.selectedTeamTemplateId)) {
    state.selectedTeamTemplateId = null;
  }
  if (!state.selectedTeamTemplateId && state.teamTemplates.length) {
    state.selectedTeamTemplateId = state.teamTemplates[0].id;
  }
  populateTeamTemplateOptions();
}

async function loadBuilds() {
  const payload = await api("/api/agent-center/builds");
  state.builds = payload.items || [];
  if (state.selectedBuildId && !state.builds.some((item) => item.id === state.selectedBuildId)) {
    state.selectedBuildId = null;
  }
  if (!state.selectedBuildId && state.builds.length) {
    state.selectedBuildId = state.builds[0].id;
  }
  populateTaskOptions();
}

async function loadBlueprints() {
  const payload = await api("/api/blueprints");
  state.blueprints = payload.items || [];
  if (state.selectedBlueprintId && !state.blueprints.some((item) => item.id === state.selectedBlueprintId)) {
    state.selectedBlueprintId = null;
  }
  if (!state.selectedBlueprintId && state.blueprints.length) {
    state.selectedBlueprintId = state.blueprints[0].id;
  }
  populateTaskOptions();
}

async function loadRuns() {
  const payload = await api("/api/runs");
  state.runs = payload.items || [];
  if (state.selectedRunId && !state.runs.some((item) => item.id === state.selectedRunId)) {
    state.selectedRunId = null;
  }
}

async function loadApprovals() {
  const payload = await api("/api/approvals?status=pending");
  state.approvals = payload.items || [];
}

async function loadRunDetail(runId) {
  const payload = await api(`/api/runs/${runId}`);
  state.selectedRunId = runId;
  const steps = (payload.steps || [])
    .map((item) => {
      const visibleIds = item.output_json?.visible_output_ids || item.output_json?.details?.visible_output_ids || [];
      return `- ${item.node_id} [${item.status}]${visibleIds.length ? ` / 可见=${visibleIds.join(",")}` : ""}`;
    })
    .join("\n");
  const artifacts = (payload.artifacts || []).map((item) => `- ${item.name}: ${item.path}`).join("\n");
  const events = (payload.events || []).slice(-12).map((item) => `- ${item.event_type}`).join("\n");
  const files = (payload.workspace_files || []).map((item) => `- ${item.relative_path}`).join("\n");
  runDetail.classList.remove("empty");
  runDetail.textContent =
    `Run：${payload.run.id}\n` +
    `状态：${payload.run.status}\n` +
    `摘要：${payload.run.summary || ""}\n\n` +
    `步骤\n${steps || "- 无"}\n\n` +
    `产物\n${artifacts || "- 无"}\n\n` +
    `最近事件\n${events || "- 无"}\n\n` +
    `工作区文件\n${files || "- 无"}`;
}

async function openBuild(buildId) {
  const build = await api(`/api/agent-center/builds/${buildId}`);
  renderBuildDetail(build);
  renderBuilds();
  populateTaskOptions();
}

async function openBlueprint(blueprintId) {
  const blueprint = await api(`/api/blueprints/${blueprintId}`);
  renderBlueprintDetail(blueprint);
  renderBlueprints();
  populateTaskOptions();
}

async function ensureAgentTemplateEditorData(force = false) {
  if (!state.loaded.providerRefs || force) {
    await loadProviders();
    state.loaded.providerRefs = true;
  }
  if (!state.loaded.pluginRefs || force) {
    await loadPlugins();
    state.loaded.pluginRefs = true;
  }
  if (!state.loaded.skillRefs || force) {
    await loadSkills();
    state.loaded.skillRefs = true;
  }
  populateAgentTemplateReferenceOptions();
}

async function ensureAgentDefinitionEditorData(force = false) {
  if (!state.loaded.providerRefs || force) {
    await loadProviders();
    state.loaded.providerRefs = true;
  }
  if (!state.loaded.pluginRefs || force) {
    await loadPlugins();
    state.loaded.pluginRefs = true;
  }
  if (!state.loaded.skillRefs || force) {
    await loadSkills();
    state.loaded.skillRefs = true;
  }
  if (!state.loaded.staticMemoryRefs || force) {
    await loadStaticMemories();
    state.loaded.staticMemoryRefs = true;
  }
  if (!state.loaded.knowledgeBaseRefs || force) {
    await loadKnowledgeBases();
    state.loaded.knowledgeBaseRefs = true;
  }
  if (!state.loaded.reviewPolicyRefs || force) {
    await loadReviewPolicies();
    state.loaded.reviewPolicyRefs = true;
  }
  if (!state.loaded.memoryProfileRefs || force) {
    await loadMemoryProfiles();
    state.loaded.memoryProfileRefs = true;
  }
  if (!state.loaded.agentDefinitionRefs || force) {
    await loadAgentDefinitions();
    state.loaded.agentDefinitionRefs = true;
  }
  populateAgentDefinitionReferenceOptions();
}

async function ensureOverviewData(force = false) {
  if (!state.loaded.controlPlane || force) {
    await loadControlPlane();
    state.loaded.controlPlane = true;
    state.loaded.providerTypes = true;
  }
  populateProviderTypeOptions();
  renderOverview();
}

async function ensureProvidersPage(force = false) {
  if (!state.loaded.providerTypes || force) {
    await loadProviderTypes();
    state.loaded.providerTypes = true;
  }
  if (!state.loaded.providerPage || force) {
    await loadProviderPage();
    state.loaded.providerPage = true;
  }
  populateProviderTypeOptions();
  providerPageSize.value = String(state.providerPage.limit || 10);
  renderProviders();
}

async function ensureRetrievalSettingsPage(force = false) {
  if (!state.loaded.providerRefs || force) {
    await loadProviders();
    state.loaded.providerRefs = true;
  }
  if (!state.loaded.retrievalSettings || force) {
    await loadRetrievalSettings();
    state.loaded.retrievalSettings = true;
  }
  fillRetrievalSettingsForm();
}

async function ensurePluginsPage(force = false) {
  if (!state.loaded.pluginPage || force) {
    await loadPluginPage();
    state.loaded.pluginPage = true;
  }
  pluginPageSize.value = String(state.pluginPage.limit || 10);
  renderPlugins();
}

async function ensureAgentTemplatesPage(force = false) {
  if (!state.loaded.providerRefs || force) {
    await loadProviders();
    state.loaded.providerRefs = true;
  }
  if (!state.loaded.agentTemplatePage || force) {
    await loadAgentTemplatePage();
    state.loaded.agentTemplatePage = true;
  }
  populateProviderOptions();
  agentTemplatePageSize.value = String(state.agentTemplatePage.limit || 10);
  renderAgentTemplates();
}

async function ensureStaticMemoryCollections(force = false) {
  if (!state.loaded.staticMemoryRefs || force) {
    await loadStaticMemories();
    state.loaded.staticMemoryRefs = true;
  }
  if (!state.loaded.staticMemoryPage || force) {
    await loadStaticMemoryPage();
    state.loaded.staticMemoryPage = true;
  }
  if (state.editingStaticMemoryId) {
    const current = state.staticMemories.find((item) => item.id === state.editingStaticMemoryId) || null;
    if (!current) {
      resetStaticMemoryForm();
      closeStaticMemoryModal();
    } else if (force && !staticMemoryModal?.classList.contains("hidden")) {
      fillStaticMemoryForm(current, { mode: state.staticMemoryEditorMode });
    }
  }
  if (staticMemoryPageSize) {
    staticMemoryPageSize.value = String(state.staticMemoryPage.limit || 10);
  }
  renderStaticMemories();
}

async function ensureResponsibilitySpecsPage(force = false) {
  await ensureStaticMemoryCollections(force);
}

async function ensureRoleManagementPage(force = false) {
  await ensureResponsibilitySpecsPage(force);
}

async function ensureTeamRulesPage(force = false) {
  await ensureResponsibilitySpecsPage(force);
}

async function ensureStaticMemoriesPage(force = false) {
  await ensureResponsibilitySpecsPage(force);
}

async function ensureKnowledgeBasesPage(force = false) {
  if (!state.loaded.knowledgeBaseRefs || force) {
    await loadKnowledgeBases();
    state.loaded.knowledgeBaseRefs = true;
  }
  if (state.editingKnowledgeBaseId) {
    const current = state.knowledgeBases.find((item) => item.id === state.editingKnowledgeBaseId) || null;
    if (!current) {
      resetKnowledgeBaseForm();
    } else if (force) {
      await fillKnowledgeBaseForm(current);
    } else {
      renderKnowledgeBases();
      renderKnowledgeBaseDocuments();
    }
  } else {
    if (!knowledgeBaseKey.value.trim() && !knowledgeBaseName.value.trim() && !knowledgeBaseDocumentsValue().length) {
      resetKnowledgeBaseForm();
    } else {
      renderKnowledgeBases();
      renderKnowledgeBaseDocuments();
    }
  }
}

async function ensureReviewPoliciesPage(force = false) {
  if (!state.loaded.uiMetadata || force) {
    await loadUiMetadata();
    state.loaded.uiMetadata = true;
  }
  if (!state.loaded.pluginRefs || force) {
    await loadPlugins();
    state.loaded.pluginRefs = true;
  }
  if (!state.loaded.builtinPluginRefs || force) {
    await loadBuiltinPlugins();
    state.loaded.builtinPluginRefs = true;
  }
  if (!state.loaded.reviewPolicyRefs || force) {
    await loadReviewPolicies();
    state.loaded.reviewPolicyRefs = true;
  }
  populateReviewPolicyPluginOptions();
  populateReviewPolicyConditionOptions();
  if (state.editingReviewPolicyId) {
    const current = state.reviewPolicies.find((item) => item.id === state.editingReviewPolicyId) || null;
    if (!current) {
      resetReviewPolicyForm();
    } else if (force) {
      fillReviewPolicyForm(current);
    } else {
      renderReviewPolicies();
    }
  } else {
    if (!reviewPolicyKey.value.trim() && !reviewPolicyName.value.trim()) {
      resetReviewPolicyForm();
    } else {
      renderReviewPolicies();
    }
  }
}

async function ensureMemoryProfilesPage(force = false) {
  if (!state.loaded.uiMetadata || force) {
    await loadUiMetadata();
    state.loaded.uiMetadata = true;
  }
  if (!state.loaded.memoryProfileRefs || force) {
    await loadMemoryProfiles();
    state.loaded.memoryProfileRefs = true;
  }
  populateStaticMemoryScopeOptions();
  if (state.editingMemoryProfileId) {
    const current = state.memoryProfiles.find((item) => item.id === state.editingMemoryProfileId) || null;
    if (!current) {
      resetMemoryProfileForm();
    } else if (force) {
      fillMemoryProfileForm(current);
    } else {
      renderMemoryProfiles();
    }
  } else {
    if (!memoryProfileKey.value.trim() && !memoryProfileName.value.trim()) {
      resetMemoryProfileForm();
    } else {
      renderMemoryProfiles();
    }
  }
}

async function ensureAgentDefinitionsPage(force = false) {
  await ensureAgentDefinitionEditorData(force);
  if (!state.loaded.agentDefinitionPage || force) {
    await loadAgentDefinitionPage();
    state.loaded.agentDefinitionPage = true;
  }
  if (agentDefinitionPageSize) {
    agentDefinitionPageSize.value = String(state.agentDefinitionPage.limit || 10);
  }
  if (state.editingAgentDefinitionId) {
    const current = state.agentDefinitions.find((item) => item.id === state.editingAgentDefinitionId) || null;
    if (!current) {
      resetAgentDefinitionForm();
    } else if (force) {
      fillAgentDefinitionForm(current);
    } else {
      renderAgentDefinitions();
    }
  } else {
    if (!agentDefinitionName.value.trim()) {
      resetAgentDefinitionForm();
    } else {
      renderAgentDefinitions();
    }
  }
}

async function ensureTeamDefinitionsPage(force = false) {
  if (!state.loaded.agentTemplateRefs || force) {
    await loadAgentTemplates();
    state.loaded.agentTemplateRefs = true;
  }
  if (!state.loaded.teamDefinitions || force) {
    await loadTeamDefinitions();
    state.loaded.teamDefinitions = true;
  }
  if (state.editingTeamDefinitionId) {
    const current = state.teamDefinitions.find((item) => item.id === state.editingTeamDefinitionId) || null;
    if (!current) {
      resetTeamDefinitionForm();
    } else {
      if (force) {
        fillTeamDefinitionForm(current);
      } else {
        renderTeamDefinitionLeadAgentOptions(teamDefinitionLeadAgentTemplate?.value || "");
        renderTeamDefinitionMembers();
        renderTeamDefinitions();
      }
    }
  } else {
    renderTeamDefinitionLeadAgentOptions(teamDefinitionLeadAgentTemplate?.value || "");
    renderTeamDefinitionMembers();
    renderTeamDefinitions();
  }
}

async function ensureTeamTemplatesPage(force = false) {
  if (!state.loaded.agentTemplateRefs || force) {
    await loadAgentTemplates();
    state.loaded.agentTemplateRefs = true;
  }
  if (!state.loaded.teamTemplates || force) {
    await loadTeamTemplates();
    state.loaded.teamTemplates = true;
  }
  renderTeamTemplates();
  if (!state.teamEditor.spec) {
    resetTeamTemplateForm();
  } else {
    renderTeamEditor();
  }
}

async function ensureBuildsPage(force = false) {
  if (!state.loaded.teamTemplates || force) {
    await loadTeamTemplates();
    state.loaded.teamTemplates = true;
  }
  if (!state.loaded.builds || force) {
    await loadBuilds();
    state.loaded.builds = true;
  }
  renderBuilds();
  if (state.selectedBuildId) {
    renderBuildDetail(state.builds.find((item) => item.id === state.selectedBuildId) || null);
  } else {
    setDetailPlaceholder(buildDetail, "请选择一个 Build。");
  }
}

async function ensureBlueprintsPage(force = false) {
  if (!state.loaded.blueprints || force) {
    await loadBlueprints();
    state.loaded.blueprints = true;
  }
  renderBlueprints();
  if (state.selectedBlueprintId) {
    renderBlueprintDetail(state.blueprints.find((item) => item.id === state.selectedBlueprintId) || null);
  } else {
    setDetailPlaceholder(blueprintDetail, "请选择一个蓝图。");
  }
}

async function ensureRuntimePage(force = false) {
  if (!state.loaded.teamDefinitions || force) {
    await loadTeamDefinitions();
    state.loaded.teamDefinitions = true;
  }
  if (!state.loaded.builds || force) {
    await loadBuilds();
    state.loaded.builds = true;
  }
  if (!state.loaded.blueprints || force) {
    await loadBlueprints();
    state.loaded.blueprints = true;
  }
  if (!state.loaded.runs || force) {
    await loadRuns();
    state.loaded.runs = true;
  }
  renderRuns();
  if (state.selectedRunId) {
    await loadRunDetail(state.selectedRunId);
  } else {
    setDetailPlaceholder(runDetail, "请选择一个 Run。");
  }
}

async function ensureApprovalsPage(force = false) {
  if (!state.loaded.approvals || force) {
    await loadApprovals();
    state.loaded.approvals = true;
  }
  renderApprovals();
}

async function ensurePageData(pageName, options = {}) {
  const force = Boolean(options.force);
  switch (pageName) {
    case "overview":
      await ensureOverviewData(force);
      break;
    case "providers":
      await ensureProvidersPage(force);
      break;
    case "retrieval-config":
      await ensureRetrievalSettingsPage(force);
      break;
    case "plugins":
      await ensurePluginsPage(force);
      break;
    case "responsibility-specs":
      await ensureResponsibilitySpecsPage(force);
      break;
    case "role-management":
      await ensureRoleManagementPage(force);
      break;
    case "team-rules":
      await ensureTeamRulesPage(force);
      break;
    case "static-memories":
      await ensureRoleManagementPage(force);
      break;
    case "knowledge-bases":
      await ensureKnowledgeBasesPage(force);
      break;
    case "review-policies":
      await ensureReviewPoliciesPage(force);
      break;
    case "memory-profiles":
      await ensureMemoryProfilesPage(force);
      break;
    case "agent-definitions":
      await ensureAgentDefinitionsPage(force);
      break;
    case "agent-templates":
      await ensureAgentTemplatesPage(force);
      break;
    case "team-definitions":
      await ensureTeamDefinitionsPage(force);
      break;
    case "team-templates":
      await ensureTeamTemplatesPage(force);
      break;
    case "builds":
      await ensureBuildsPage(force);
      break;
    case "runtime":
      await ensureRuntimePage(force);
      break;
    case "approvals":
      await ensureApprovalsPage(force);
      break;
    case "blueprints":
      await ensureBlueprintsPage(force);
      break;
    default:
      break;
  }
}

navButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    await switchPage(button.dataset.pageTarget);
  });
});

topNavButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    await switchNavSection(button.dataset.navSection);
  });
});

const refreshAllButton = document.querySelector("#refresh-all");
if (refreshAllButton) {
  refreshAllButton.addEventListener("click", async () => {
  try {
    await ensurePageData(state.activePage, { force: true });
  } catch (error) {
    showResult(taskResult, { error: error.message });
  }
  });
}

const quickBuildButton = document.querySelector("#quick-build");
if (quickBuildButton) {
  quickBuildButton.addEventListener("click", async () => {
  try {
    await ensureTeamTemplatesPage(true);
    const targetId = state.selectedTeamTemplateId || state.teamTemplates[0]?.id;
    if (!targetId) {
      throw new Error("暂无可构建的团队模板。");
    }
    const payload = await api(`/api/agent-center/team-templates/${targetId}/build`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    state.selectedBuildId = payload.id;
    if (payload.blueprint_id) {
      state.selectedBlueprintId = payload.blueprint_id;
    }
    hideResult(buildResult);
    invalidateData("builds", "blueprints", "controlPlane");
    await switchPage("builds", { force: true });
    await openBuild(payload.id);
  } catch (error) {
    showResult(buildResult, { error: error.message });
  }
  });
}

providerForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const isCreate = !state.editingProviderId;
    const payload = buildProviderPayloadFromForm();
    const method = state.editingProviderId ? "PUT" : "POST";
    const path = state.editingProviderId ? `/api/agent-center/providers/${state.editingProviderId}` : "/api/agent-center/providers";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    if (isCreate) {
      state.providerPage.offset = 0;
    }
    invalidateData("providerPage", "providerRefs", "retrievalSettings", "controlPlane");
    await ensureProvidersPage(true);
    closeProviderModal();
    providerResult?.classList.remove("empty");
    showResult(providerResult, { message: "\u63d0\u4f9b\u65b9\u5df2\u4fdd\u5b58", id: saved.id });
  } catch (error) {
    providerResult?.classList.remove("empty");
    showResult(providerResult, errorResult(error));
  }
});

if (retrievalSettingsForm) {
  retrievalSettingsForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const payload = await api("/api/agent-center/retrieval-settings", {
        method: "PUT",
        body: JSON.stringify(buildRetrievalPayloadFromForm()),
      });
      state.retrievalSettings = {
        settings: payload.settings || { embedding: { mode: "hash" }, rerank: { mode: "disabled" } },
        warnings: [],
        updated_at: payload.updated_at || null,
      };
      invalidateData("retrievalSettings", "controlPlane");
      fillRetrievalSettingsForm();
      showResult(retrievalSettingsResult, payload.applied || { message: "检索配置已保存" });
    } catch (error) {
      showResult(retrievalSettingsResult, errorResult(error));
    }
  });
}

pluginForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(pluginResult);
  hideResult(pluginModalResult);
  try {
    const isCreate = !state.editingPluginId;
    const payload = buildPluginPayloadFromForm();
    const method = state.editingPluginId ? "PUT" : "POST";
    const path = state.editingPluginId ? `/api/agent-center/plugins/${state.editingPluginId}` : "/api/agent-center/plugins";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    if (isCreate) {
      state.pluginPage.offset = 0;
    }
    invalidateData("pluginPage", "pluginRefs", "controlPlane");
    await ensurePluginsPage(true);
    closePluginModal();
    showResult(pluginResult, { message: "插件已保存", id: saved.id });
  } catch (error) {
    showResult(pluginModalResult, errorResult(error));
  }
});

staticMemoryForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(staticMemoryModalResult);
  hideResult(staticMemoryResultTarget());
  try {
    const config = staticMemoryModeConfig("role");
    const isCreate = !state.editingStaticMemoryId;
    const payload = buildStaticMemoryPayloadFromForm();
    const method = state.editingStaticMemoryId ? "PUT" : "POST";
    const path = state.editingStaticMemoryId
      ? `/api/agent-center/static-memories/${state.editingStaticMemoryId}`
      : "/api/agent-center/static-memories";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    state.editingStaticMemoryId = saved.id;
    state.staticMemoryEditorMode = "role";
    if (isCreate) {
      state.staticMemoryPage.offset = 0;
    }
    invalidateData("staticMemoryRefs", "staticMemoryPage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureStaticMemoryCollections(true);
    closeStaticMemoryModal();
    showResult(staticMemoryResultTarget("role"), { message: config.saveMessage, id: saved.id });
  } catch (error) {
    showResult(staticMemoryModalResult, errorResult(error));
  }
});

knowledgeBaseForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(knowledgeBaseResult);
  try {
    const payload = buildKnowledgeBasePayloadFromForm();
    const currentDocuments = knowledgeBaseDocumentsValue();
    currentDocuments.forEach((item, index) => {
      if (!String(item.key || "").trim()) {
        throw new Error(`第 ${index + 1} 篇文档缺少 Key。`);
      }
      if (!String(item.title || "").trim()) {
        throw new Error(`第 ${index + 1} 篇文档缺少标题。`);
      }
    });
    const method = state.editingKnowledgeBaseId ? "PUT" : "POST";
    const path = state.editingKnowledgeBaseId
      ? `/api/agent-center/knowledge-bases/${state.editingKnowledgeBaseId}`
      : "/api/agent-center/knowledge-bases";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    const seenIds = new Set();
    for (const item of currentDocuments) {
      const documentPayload = {
        id: item.id || null,
        knowledge_base_id: saved.id,
        key: String(item.key || "").trim(),
        title: String(item.title || "").trim(),
        source_path: String(item.source_path || "").trim() || null,
        content_text: String(item.content_text || "").trim(),
        metadata: dictOrEmpty(item.metadata_json || item.metadata),
      };
      const documentPath = item.id ? `/api/agent-center/knowledge-documents/${item.id}` : "/api/agent-center/knowledge-documents";
      const savedDocument = await api(documentPath, {
        method: item.id ? "PUT" : "POST",
        body: JSON.stringify(documentPayload),
      });
      if (savedDocument?.id) {
        seenIds.add(savedDocument.id);
      }
    }
    for (const documentId of state.knowledgeBasePersistedDocumentIds || []) {
      if (!seenIds.has(documentId)) {
        await api(`/api/agent-center/knowledge-documents/${documentId}`, { method: "DELETE" });
      }
    }
    invalidateData("knowledgeBaseRefs", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureKnowledgeBasesPage(true);
    const refreshed = state.knowledgeBases.find((item) => item.id === saved.id) || saved;
    await fillKnowledgeBaseForm(refreshed);
    showResult(knowledgeBaseResult, { message: "知识库已保存", id: saved.id, document_count: currentDocuments.length });
  } catch (error) {
    showResult(knowledgeBaseResult, errorResult(error));
  }
});

reviewPolicyForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(reviewPolicyResult);
  try {
    const payload = buildReviewPolicyPayloadFromForm();
    const method = state.editingReviewPolicyId ? "PUT" : "POST";
    const path = state.editingReviewPolicyId
      ? `/api/agent-center/review-policies/${state.editingReviewPolicyId}`
      : "/api/agent-center/review-policies";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    fillReviewPolicyForm(saved);
    invalidateData("reviewPolicyRefs", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureReviewPoliciesPage(true);
    showResult(reviewPolicyResult, { message: "审核策略已保存", id: saved.id });
  } catch (error) {
    showResult(reviewPolicyResult, errorResult(error));
  }
});

memoryProfileForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(memoryProfileResult);
  try {
    const payload = buildMemoryProfilePayloadFromForm();
    const method = state.editingMemoryProfileId ? "PUT" : "POST";
    const path = state.editingMemoryProfileId
      ? `/api/agent-center/memory-profiles/${state.editingMemoryProfileId}`
      : "/api/agent-center/memory-profiles";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    fillMemoryProfileForm(saved);
    invalidateData("memoryProfileRefs", "agentDefinitionRefs", "controlPlane");
    await ensureMemoryProfilesPage(true);
    showResult(memoryProfileResult, { message: "记忆画像已保存", id: saved.id });
  } catch (error) {
    showResult(memoryProfileResult, errorResult(error));
  }
});

agentDefinitionForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(agentDefinitionResult);
  hideResult(agentDefinitionModalResult);
  try {
    const isCreate = !state.editingAgentDefinitionId;
    const payload = buildAgentDefinitionPayloadFromForm();
    const method = state.editingAgentDefinitionId ? "PUT" : "POST";
    const path = state.editingAgentDefinitionId
      ? `/api/agent-center/agent-definitions/${state.editingAgentDefinitionId}`
      : "/api/agent-center/agent-definitions";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    if (isCreate) {
      state.agentDefinitionPage.offset = 0;
    }
    invalidateData("agentDefinitionPage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureAgentDefinitionsPage(true);
    state.editingAgentDefinitionId = saved.id;
    state.agentDefinitionBaseSpec = clone(dictOrEmpty(saved.spec_json));
    closeAgentDefinitionModal();
    showResult(agentDefinitionResult, { message: "Agent 管理项已保存", id: saved.id });
  } catch (error) {
    showResult(agentDefinitionModalResult, errorResult(error));
  }
});

agentTemplateForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(agentTemplateResult);
  hideResult(agentTemplateModalResult);
  try {
    const isCreate = !state.editingAgentTemplateId;
    const payload = buildAgentTemplatePayloadFromForm();
    const method = state.editingAgentTemplateId ? "PUT" : "POST";
    const path = state.editingAgentTemplateId
      ? `/api/agent-center/agent-templates/${state.editingAgentTemplateId}`
      : "/api/agent-center/agent-templates";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    if (isCreate) {
      state.agentTemplatePage.offset = 0;
    }
    invalidateData("agentTemplatePage", "agentTemplateRefs", "controlPlane");
    await ensureAgentTemplatesPage(true);
    closeAgentTemplateModal();
    showResult(agentTemplateResult, { message: "Agent 模板已保存", id: saved.id });
  } catch (error) {
    showResult(agentTemplateModalResult, errorResult(error));
  }
});

teamDefinitionForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(teamDefinitionResult);
  hideResult(teamDefinitionModalResult);
  try {
    const payload = buildTeamDefinitionPayloadFromForm();
    const method = state.editingTeamDefinitionId ? "PUT" : "POST";
    const path = state.editingTeamDefinitionId
      ? `/api/agent-center/team-definitions/${state.editingTeamDefinitionId}`
      : "/api/agent-center/team-definitions";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    state.editingTeamDefinitionId = saved.id;
    state.teamDefinitionBaseSpec = clone(dictOrEmpty(saved.spec_json));
    invalidateData("teamDefinitions", "controlPlane");
    await ensureTeamDefinitionsPage(true);
    closeTeamDefinitionModal();
    showResult(teamDefinitionResult, { message: "团队管理项已保存", id: saved.id });
  } catch (error) {
    showResult(teamDefinitionModalResult, errorResult(error));
  }
});

teamTemplateForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(teamTemplateResult);
  try {
    const payload = buildTeamTemplatePayloadFromForm();
    const method = state.editingTeamTemplateId ? "PUT" : "POST";
    const path = state.editingTeamTemplateId
      ? `/api/agent-center/team-templates/${state.editingTeamTemplateId}`
      : "/api/agent-center/team-templates";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    fillTeamTemplateForm(saved);
    invalidateData("teamTemplates", "builds", "controlPlane");
    await ensureTeamTemplatesPage(true);
    showResult(teamTemplateResult, { message: "团队模板已保存", id: saved.id });
  } catch (error) {
    showResult(teamTemplateResult, { error: error.message });
  }
});

buildForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(buildResult);
  try {
    if (!buildTeamTemplate.value) {
      throw new Error("请选择团队模板。");
    }
    const payload = await api("/api/agent-center/builds", {
      method: "POST",
      body: JSON.stringify({
        team_template_id: buildTeamTemplate.value,
        name: buildName.value.trim() || null,
      }),
    });
    state.selectedBuildId = payload.id;
    if (payload.blueprint_id) {
      state.selectedBlueprintId = payload.blueprint_id;
    }
    invalidateData("builds", "blueprints", "controlPlane");
    await ensureBuildsPage(true);
    await openBuild(payload.id);
    showResult(buildResult, { message: "Build 已生成", id: payload.id, blueprint_id: payload.blueprint_id });
  } catch (error) {
    showResult(buildResult, { error: error.message });
  }
});

taskForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(taskResult);
  try {
    if (!taskPrompt.value.trim()) {
      throw new Error("任务描述不能为空。");
    }
    const payload = {
      title: taskTitle.value.trim() || null,
      prompt: taskPrompt.value.trim(),
      approval_mode: taskApprovalMode.value,
    };
    let runBundle;
    if (taskTeamDefinition?.value) {
      state.selectedTaskTeamDefinitionId = taskTeamDefinition.value;
      runBundle = await api(`/api/agent-center/team-definitions/${taskTeamDefinition.value}/tasks`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
    } else if (taskBuild.value) {
      payload.build_id = taskBuild.value;
      runBundle = await api("/api/task-releases", {
        method: "POST",
        body: JSON.stringify(payload),
      });
    } else if (taskBlueprint.value) {
      payload.blueprint_id = taskBlueprint.value;
      runBundle = await api("/api/task-releases", {
        method: "POST",
        body: JSON.stringify(payload),
      });
    } else {
      throw new Error("请选择 TeamDefinition，或使用兼容 Build / 蓝图链路。");
    }
    state.selectedRunId = runBundle.run.id;
    invalidateData("runs", "approvals", "controlPlane");
    await switchPage("runtime", { force: true });
    await loadRunDetail(runBundle.run.id);
    showResult(taskResult, { message: "Run 已启动", run_id: runBundle.run.id, status: runBundle.run.status });
  } catch (error) {
    showResult(taskResult, { error: error.message });
  }
});

providerOpenCreate.addEventListener("click", async () => {
  await loadProviderTypes();
  state.loaded.providerTypes = true;
  populateProviderTypeOptions();
  resetProviderForm();
  openProviderModal();
});
providerModalCloseButtons.forEach((button) => button.addEventListener("click", closeProviderModal));
providerModelEditorModalCloseButtons.forEach((button) => button.addEventListener("click", closeProviderModelEditorModal));
providerCancel.addEventListener("click", closeProviderModal);
providerApiKeyToggle.addEventListener("click", () => {
  setProviderApiKeyVisibility(providerApiKey.type === "password");
});
providerModelNew.addEventListener("click", () => {
  startProviderModelEditor();
});
providerModelReset.addEventListener("click", () => {
  state.providerEditor.models = [];
  renderProviderModelList();
  resetProviderModelEditor();
});
providerModelCancel.addEventListener("click", closeProviderModelEditorModal);
providerType.addEventListener("change", () => {
  const preset = providerPreset();
  if (preset?.default_base_url && !providerBaseUrl.value.trim()) {
    providerBaseUrl.value = preset.default_base_url;
  }
});
providerPageSize.addEventListener("change", async () => {
  state.providerPage.limit = Number(providerPageSize.value || 10);
  state.providerPage.offset = 0;
  await ensureProvidersPage(true);
});
agentDefinitionPageSize?.addEventListener("change", async () => {
  state.agentDefinitionPage.limit = Number(agentDefinitionPageSize.value || 10);
  state.agentDefinitionPage.offset = 0;
  await ensureAgentDefinitionsPage(true);
});
retrievalEmbeddingMode?.addEventListener("change", () => {
  syncRetrievalModelOptions();
});
retrievalRerankMode?.addEventListener("change", () => {
  syncRetrievalModelOptions();
});
retrievalEmbeddingProvider?.addEventListener("change", () => {
  renderModelSelect(retrievalEmbeddingModel, providerModelsByType(retrievalEmbeddingProvider.value, "embedding"));
  syncRetrievalModelOptions();
});
retrievalRerankProvider?.addEventListener("change", () => {
  renderModelSelect(retrievalRerankModel, providerModelsByType(retrievalRerankProvider.value, "rerank"));
  syncRetrievalModelOptions();
});
retrievalSettingsRefresh?.addEventListener("click", async () => {
  try {
    invalidateData("retrievalSettings", "providerRefs", "controlPlane");
    await ensureRetrievalSettingsPage(true);
  } catch (error) {
    showResult(retrievalSettingsResult, errorResult(error));
  }
});
providerModelSave.addEventListener("click", () => {
  const model = readProviderModelEditor();
  if (!model.name) {
    showResult(providerModelTestResult, { error: "\u6a21\u578b\u540d\u79f0\u4e0d\u80fd\u4e3a\u7a7a\u3002" });
    return;
  }
  const index = state.providerEditor.editingModelIndex;
  if (index === null) {
    state.providerEditor.models.push(model);
  } else {
    state.providerEditor.models[index] = model;
  }
  renderProviderModelList();
  closeProviderModelEditorModal();
});
providerModelFetch.addEventListener("click", async () => {
  try {
    const payload = await api("/api/agent-center/providers/discover-models", {
      method: "POST",
      body: JSON.stringify(buildProviderPayloadFromForm()),
    });
    state.providerEditor.models = payload.items || [];
    state.providerEditor.savedModels = clone(state.providerEditor.models);
    renderProviderModelList();
    resetProviderModelEditor();
    providerResult?.classList.remove("empty");
    showResult(providerResult, payload);
  } catch (error) {
    providerResult?.classList.remove("empty");
    showResult(providerResult, errorResult(error));
  }
});
providerModelTest.addEventListener("click", async () => {
  try {
    const payload = await api("/api/agent-center/providers/test-model", {
      method: "POST",
      body: JSON.stringify({
        provider: buildProviderPayloadFromForm(),
        model: readProviderModelEditor(),
      }),
    });
    showResult(providerModelTestResult, payload);
  } catch (error) {
    showResult(providerModelTestResult, errorResult(error));
  }
});
pluginOpenCreate.addEventListener("click", () => {
  resetPluginForm();
  openPluginModal();
});
pluginModalCloseButtons.forEach((button) => button.addEventListener("click", closePluginModal));
pluginCancel.addEventListener("click", closePluginModal);
pluginInstallPath?.addEventListener("change", async () => {
  hideResult(pluginModalResult);
  await syncPluginManifestFromInstallPath();
});
responsibilitySpecOpenRoleCreate?.addEventListener("click", async () => {
  await ensureResponsibilitySpecsPage(true);
  resetStaticMemoryForm("role");
  openStaticMemoryModal("role");
});
staticMemoryModalCloseButtons.forEach((button) => button.addEventListener("click", closeStaticMemoryModal));
staticMemoryCancel?.addEventListener("click", closeStaticMemoryModal);
knowledgeBaseReset?.addEventListener("click", async () => {
  await ensureKnowledgeBasesPage(true);
  resetKnowledgeBaseForm();
});
knowledgeBaseDocumentAdd?.addEventListener("click", addKnowledgeBaseDocument);
reviewPolicyReset?.addEventListener("click", async () => {
  await ensureReviewPoliciesPage(true);
  resetReviewPolicyForm();
});
memoryProfileReset?.addEventListener("click", async () => {
  await ensureMemoryProfilesPage(true);
  resetMemoryProfileForm();
});
agentDefinitionOpenCreate?.addEventListener("click", async () => {
  await ensureAgentDefinitionsPage(true);
  resetAgentDefinitionForm({ openModal: true });
});
agentDefinitionModalCloseButtons.forEach((button) => button.addEventListener("click", closeAgentDefinitionModal));
agentDefinitionCancel?.addEventListener("click", closeAgentDefinitionModal);
agentDefinitionProvider?.addEventListener("change", () => {
  syncAgentDefinitionModelOptions();
});
pluginPageSize.addEventListener("change", async () => {
  state.pluginPage.limit = Number(pluginPageSize.value || 10);
  state.pluginPage.offset = 0;
  await ensurePluginsPage(true);
});
agentTemplateOpenCreate.addEventListener("click", async () => {
  await ensureAgentTemplateEditorData(true);
  resetAgentTemplateForm();
  openAgentTemplateModal();
});
agentTemplateModalCloseButtons.forEach((button) => button.addEventListener("click", closeAgentTemplateModal));
agentTemplateCancel.addEventListener("click", closeAgentTemplateModal);
agentTemplateProvider?.addEventListener("change", () => {
  renderModelSelect(agentTemplateModel, providerModelsByType(agentTemplateProvider?.value || "", "chat"));
});
agentTemplatePageSize.addEventListener("change", async () => {
  state.agentTemplatePage.limit = Number(agentTemplatePageSize.value || 10);
  state.agentTemplatePage.offset = 0;
  await ensureAgentTemplatesPage(true);
});
staticMemoryPageSize?.addEventListener("change", async () => {
  state.staticMemoryPage.limit = Number(staticMemoryPageSize.value || 10);
  state.staticMemoryPage.offset = 0;
  await ensureStaticMemoryCollections(true);
});
teamDefinitionOpenCreate?.addEventListener("click", async () => {
  await ensureTeamDefinitionsPage(true);
  resetTeamDefinitionForm({ openModal: true });
});
teamDefinitionMemberAdd?.addEventListener("click", async () => {
  await ensureTeamDefinitionsPage();
  addTeamDefinitionMember();
});
teamDefinitionReviewOverrideAdd?.addEventListener("click", async () => {
  await ensureTeamDefinitionsPage();
  addTeamDefinitionReviewOverride();
});
teamDefinitionEntryMode?.addEventListener("change", () => {
  syncTeamDefinitionPolicyControls();
});
teamDefinitionTerminationMode?.addEventListener("change", () => {
  syncTeamDefinitionPolicyControls();
});
teamDefinitionMembers?.addEventListener("input", () => {
  renderTeamDefinitionMembers();
  populateTeamDefinitionPolicyAgentOptions();
  renderTeamDefinitionReviewOverrides();
});
teamDefinitionModalCloseButtons.forEach((button) => button.addEventListener("click", closeTeamDefinitionModal));
teamDefinitionCancel?.addEventListener("click", closeTeamDefinitionModal);
document.querySelector("#team-template-reset").addEventListener("click", async () => {
  await ensureTeamTemplatesPage(true);
  resetTeamTemplateForm();
});
document.querySelector("#team-member-add").addEventListener("click", addTeamMember);
teamGraphValidate.addEventListener("click", validateTeamGraph);
teamGraphPreview.addEventListener("click", previewTeamGraph);
teamGraphAutolayout.addEventListener("click", () => {
  mutateTeamSpec((spec) => {
    autoLayoutTeamSpec(spec);
  });
});
teamNodeLink.addEventListener("click", startLinkMode);
teamNodeDelete.addEventListener("click", deleteSelectedNode);

[teamNodeId, teamNodeName, teamNodeAgent, teamNodeInstruction, teamNodeExpr, teamNodeMaxIterations, teamNodeArtifactKind, teamNodeArtifactName, teamNodeTemplate, teamNodeSource].forEach(
  (input) => input.addEventListener("input", updateSelectedNode),
);

teamTemplateAgents.addEventListener("input", () => {
  try {
    const spec = ensureTeamEditorMetadata(teamEditorSpec());
    spec.agents = safeParseJson(teamTemplateAgents.value, []);
    state.teamEditor.spec = spec;
    renderTeamEditor();
  } catch (error) {
    renderTeamTemplatePreview();
  }
});
teamTemplateFlow.addEventListener("input", () => {
  try {
    const spec = ensureTeamEditorMetadata(teamEditorSpec());
    spec.flow = safeParseJson(teamTemplateFlow.value, { nodes: [], edges: [] });
    state.teamEditor.spec = spec;
    renderTeamEditor();
  } catch (error) {
    renderTeamTemplatePreview();
  }
});
[teamTemplateName, teamTemplateWorkspace, teamTemplateProject, teamTemplateDod, teamTemplateChecks].forEach((input) =>
  input.addEventListener("input", () => renderTeamEditor()),
);
teamTemplateDescription.addEventListener("input", () => renderTeamTemplatePreview());

taskBuild.addEventListener("change", () => {
  state.selectedBuildId = taskBuild.value || null;
  if (state.selectedBuildId) {
    state.selectedTaskTeamDefinitionId = null;
    state.selectedBlueprintId = null;
    if (taskTeamDefinition) {
      taskTeamDefinition.value = "";
    }
    taskBlueprint.value = "";
  }
  syncTaskLaunchControls();
});

taskBlueprint.addEventListener("change", () => {
  state.selectedBlueprintId = taskBlueprint.value || null;
  if (state.selectedBlueprintId) {
    state.selectedTaskTeamDefinitionId = null;
    state.selectedBuildId = null;
    if (taskTeamDefinition) {
      taskTeamDefinition.value = "";
    }
    taskBuild.value = "";
  }
  syncTaskLaunchControls();
});

taskTeamDefinition?.addEventListener("change", () => {
  state.selectedTaskTeamDefinitionId = taskTeamDefinition.value || null;
  if (state.selectedTaskTeamDefinitionId) {
    state.selectedBuildId = null;
    state.selectedBlueprintId = null;
    taskBuild.value = "";
    taskBlueprint.value = "";
  }
  syncTaskLaunchControls();
});

document.addEventListener("click", async (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }

  const providerDelete = event.target.closest("[data-provider-delete]");
  if (providerDelete) {
    const providerId = providerDelete.dataset.providerDelete;
    const provider =
      state.providerPage.items.find((item) => item.id === providerId) || state.providers.find((item) => item.id === providerId) || null;
    const providerNameText = provider?.name || providerId;
    if (!window.confirm(`\u786e\u8ba4\u5220\u9664\u63d0\u4f9b\u65b9\u201c${providerNameText}\u201d\uff1f`)) {
      return;
    }
    try {
      if (state.providerPage.items.length === 1 && state.providerPage.offset > 0) {
        state.providerPage.offset = Math.max(0, state.providerPage.offset - state.providerPage.limit);
      }
      await api(`/api/agent-center/providers/${providerId}`, { method: "DELETE" });
      if (state.editingProviderId === providerId) {
        resetProviderForm();
        closeProviderModal();
      }
      invalidateData("providerPage", "providerRefs", "retrievalSettings", "controlPlane");
      await ensureProvidersPage(true);
      providerResult?.classList.remove("empty");
      showResult(providerResult, { message: "\u63d0\u4f9b\u65b9\u5df2\u5220\u9664", id: providerId });
    } catch (error) {
      providerResult?.classList.remove("empty");
      showResult(providerResult, errorResult(error));
    }
    return;
  }

  const providerEdit = event.target.closest("[data-provider-edit]");
  if (providerEdit) {
    try {
      const provider = await api(`/api/agent-center/providers/${providerEdit.dataset.providerEdit}`);
      fillProviderForm(provider);
      await switchPage("providers");
    } catch (error) {
      providerResult?.classList.remove("empty");
      showResult(providerResult, errorResult(error));
    }
    return;
  }

  const providerPageButton = event.target.closest("[data-provider-page]");
  if (providerPageButton && !providerPageButton.hasAttribute("disabled")) {
    state.providerPage.offset = Number(providerPageButton.dataset.providerPage || 0);
    await ensureProvidersPage(true);
    return;
  }

  const agentDefinitionPageButton = event.target.closest("[data-agent-definition-page]");
  if (agentDefinitionPageButton && !agentDefinitionPageButton.hasAttribute("disabled")) {
    state.agentDefinitionPage.offset = Number(agentDefinitionPageButton.dataset.agentDefinitionPage || 0);
    await ensureAgentDefinitionsPage(true);
    return;
  }

  const pluginPageButton = event.target.closest("[data-plugin-page]");
  if (pluginPageButton && !pluginPageButton.hasAttribute("disabled")) {
    state.pluginPage.offset = Number(pluginPageButton.dataset.pluginPage || 0);
    await ensurePluginsPage(true);
    return;
  }

  const agentTemplatePageButton = event.target.closest("[data-agent-template-page]");
  if (agentTemplatePageButton && !agentTemplatePageButton.hasAttribute("disabled")) {
    state.agentTemplatePage.offset = Number(agentTemplatePageButton.dataset.agentTemplatePage || 0);
    await ensureAgentTemplatesPage(true);
    return;
  }

  const staticMemoryPageButton = event.target.closest("[data-static-memory-page]");
  if (staticMemoryPageButton && !staticMemoryPageButton.hasAttribute("disabled")) {
    state.staticMemoryPage.offset = Number(staticMemoryPageButton.dataset.staticMemoryPage || 0);
    await ensureStaticMemoryCollections(true);
    return;
  }

  const providerModelEdit = event.target.closest("[data-provider-model-edit]");
  if (providerModelEdit) {
    startProviderModelEditor(Number(providerModelEdit.dataset.providerModelEdit));
    return;
  }

  const providerModelRemove = event.target.closest("[data-provider-model-remove]");
  if (providerModelRemove) {
    state.providerEditor.models.splice(Number(providerModelRemove.dataset.providerModelRemove), 1);
    renderProviderModelList();
    resetProviderModelEditor();
    return;
  }

  const pluginEdit = event.target.closest("[data-plugin-edit]");
  if (pluginEdit) {
    try {
      const plugin = await api(`/api/agent-center/plugins/${pluginEdit.dataset.pluginEdit}`);
      fillPluginForm(plugin);
      await switchPage("plugins");
    } catch (error) {
      showResult(pluginResult, errorResult(error));
    }
    return;
  }

  const pluginValidate = event.target.closest("[data-plugin-validate]");
  if (pluginValidate) {
    try {
      const plugin = state.pluginPage.items.find((item) => item.id === pluginValidate.dataset.pluginValidate) || null;
      if (!plugin?.install_path) {
        throw new Error("插件未配置安装路径。");
      }
      const payload = await api("/api/agent-center/plugins/validate-package", {
        method: "POST",
        body: JSON.stringify({ path: plugin.install_path }),
      });
      showResult(pluginResult, payload);
    } catch (error) {
      showResult(pluginResult, { error: error.message });
    }
    return;
  }

  const pluginInstall = event.target.closest("[data-plugin-install]");
  if (pluginInstall) {
    try {
      const payload = await api(`/api/agent-center/plugins/${pluginInstall.dataset.pluginInstall}/install`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      showResult(pluginResult, payload);
      invalidateData("pluginPage", "pluginRefs");
      await ensurePluginsPage(true);
    } catch (error) {
      showResult(pluginResult, { error: error.message });
    }
    return;
  }

  const pluginLoad = event.target.closest("[data-plugin-load]");
  if (pluginLoad) {
    try {
      const payload = await api(`/api/agent-center/plugins/${pluginLoad.dataset.pluginLoad}/load`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      showResult(pluginResult, payload);
      invalidateData("pluginPage", "pluginRefs");
      await ensurePluginsPage(true);
    } catch (error) {
      showResult(pluginResult, { error: error.message });
    }
    return;
  }

  const pluginReload = event.target.closest("[data-plugin-reload]");
  if (pluginReload) {
    try {
      const payload = await api(`/api/agent-center/plugins/${pluginReload.dataset.pluginReload}/reload`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      showResult(pluginResult, payload);
      invalidateData("pluginPage", "pluginRefs");
      await ensurePluginsPage(true);
    } catch (error) {
      showResult(pluginResult, { error: error.message });
    }
    return;
  }

  const pluginHealth = event.target.closest("[data-plugin-health]");
  if (pluginHealth) {
    try {
      const payload = await api(`/api/agent-center/plugins/${pluginHealth.dataset.pluginHealth}/health`);
      showResult(pluginResult, payload);
      invalidateData("pluginPage");
      await ensurePluginsPage(true);
    } catch (error) {
      showResult(pluginResult, { error: error.message });
    }
    return;
  }

  const staticMemoryEdit = event.target.closest("[data-static-memory-edit]");
  if (staticMemoryEdit) {
    try {
      await switchPage(staticMemoryModeConfig("role").page);
      const item =
        state.staticMemories.find((entry) => entry.id === staticMemoryEdit.dataset.staticMemoryEdit) ||
        (await api(`/api/agent-center/static-memories/${staticMemoryEdit.dataset.staticMemoryEdit}`));
      fillStaticMemoryForm(item, { mode: "role" });
    } catch (error) {
      showResult(staticMemoryResultTarget("role"), errorResult(error));
    }
    return;
  }

  const staticMemoryDelete = event.target.closest("[data-static-memory-delete]");
  if (staticMemoryDelete) {
    const config = staticMemoryModeConfig("role");
    const itemId = staticMemoryDelete.dataset.staticMemoryDelete;
    const item = state.staticMemories.find((entry) => entry.id === itemId) || null;
    if (!window.confirm(`${config.deletePrompt}“${item?.name || item?.key || itemId}”？`)) {
      return;
    }
    try {
      if (state.staticMemoryPage.items.length === 1 && state.staticMemoryPage.offset > 0) {
        state.staticMemoryPage.offset = Math.max(0, state.staticMemoryPage.offset - state.staticMemoryPage.limit);
      }
      await api(`/api/agent-center/static-memories/${itemId}`, { method: "DELETE" });
      if (state.editingStaticMemoryId === itemId) {
        resetStaticMemoryForm();
        closeStaticMemoryModal();
      }
      invalidateData("staticMemoryRefs", "staticMemoryPage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
      await ensureStaticMemoryCollections(true);
      showResult(staticMemoryResultTarget("role"), { message: config.deleteMessage, id: itemId });
    } catch (error) {
      showResult(staticMemoryResultTarget("role"), errorResult(error));
    }
    return;
  }

  const knowledgeBaseEdit = event.target.closest("[data-knowledge-base-edit]");
  if (knowledgeBaseEdit) {
    try {
      await switchPage("knowledge-bases");
      const item =
        state.knowledgeBases.find((entry) => entry.id === knowledgeBaseEdit.dataset.knowledgeBaseEdit) ||
        (await api(`/api/agent-center/knowledge-bases/${knowledgeBaseEdit.dataset.knowledgeBaseEdit}`));
      await fillKnowledgeBaseForm(item);
    } catch (error) {
      showResult(knowledgeBaseResult, errorResult(error));
    }
    return;
  }

  const knowledgeBaseDelete = event.target.closest("[data-knowledge-base-delete]");
  if (knowledgeBaseDelete) {
    const itemId = knowledgeBaseDelete.dataset.knowledgeBaseDelete;
    const item = state.knowledgeBases.find((entry) => entry.id === itemId) || null;
    if (!window.confirm(`确认删除知识库“${item?.name || item?.key || itemId}”？`)) {
      return;
    }
    try {
      await api(`/api/agent-center/knowledge-bases/${itemId}`, { method: "DELETE" });
      if (state.editingKnowledgeBaseId === itemId) {
        resetKnowledgeBaseForm();
      }
      invalidateData("knowledgeBaseRefs", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
      await ensureKnowledgeBasesPage(true);
      showResult(knowledgeBaseResult, { message: "知识库已删除", id: itemId });
    } catch (error) {
      showResult(knowledgeBaseResult, errorResult(error));
    }
    return;
  }

  const knowledgeDocumentRemove = event.target.closest("[data-knowledge-document-remove]");
  if (knowledgeDocumentRemove) {
    removeKnowledgeBaseDocument(Number(knowledgeDocumentRemove.dataset.knowledgeDocumentRemove));
    return;
  }

  const reviewPolicyEdit = event.target.closest("[data-review-policy-edit]");
  if (reviewPolicyEdit) {
    try {
      await switchPage("review-policies");
      const item =
        state.reviewPolicies.find((entry) => entry.id === reviewPolicyEdit.dataset.reviewPolicyEdit) ||
        (await api(`/api/agent-center/review-policies/${reviewPolicyEdit.dataset.reviewPolicyEdit}`));
      fillReviewPolicyForm(item);
    } catch (error) {
      showResult(reviewPolicyResult, errorResult(error));
    }
    return;
  }

  const reviewPolicyDelete = event.target.closest("[data-review-policy-delete]");
  if (reviewPolicyDelete) {
    const itemId = reviewPolicyDelete.dataset.reviewPolicyDelete;
    const item = state.reviewPolicies.find((entry) => entry.id === itemId) || null;
    if (!window.confirm(`确认删除审核策略“${item?.name || item?.key || itemId}”？`)) {
      return;
    }
    try {
      await api(`/api/agent-center/review-policies/${itemId}`, { method: "DELETE" });
      if (state.editingReviewPolicyId === itemId) {
        resetReviewPolicyForm();
      }
      invalidateData("reviewPolicyRefs", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
      await ensureReviewPoliciesPage(true);
      showResult(reviewPolicyResult, { message: "审核策略已删除", id: itemId });
    } catch (error) {
      showResult(reviewPolicyResult, errorResult(error));
    }
    return;
  }

  const memoryProfileEdit = event.target.closest("[data-memory-profile-edit]");
  if (memoryProfileEdit) {
    try {
      await switchPage("memory-profiles");
      const item =
        state.memoryProfiles.find((entry) => entry.id === memoryProfileEdit.dataset.memoryProfileEdit) ||
        (await api(`/api/agent-center/memory-profiles/${memoryProfileEdit.dataset.memoryProfileEdit}`));
      fillMemoryProfileForm(item);
    } catch (error) {
      showResult(memoryProfileResult, errorResult(error));
    }
    return;
  }

  const memoryProfileDelete = event.target.closest("[data-memory-profile-delete]");
  if (memoryProfileDelete) {
    const itemId = memoryProfileDelete.dataset.memoryProfileDelete;
    const item = state.memoryProfiles.find((entry) => entry.id === itemId) || null;
    if (!window.confirm(`确认删除记忆画像“${item?.name || item?.key || itemId}”？`)) {
      return;
    }
    try {
      await api(`/api/agent-center/memory-profiles/${itemId}`, { method: "DELETE" });
      if (state.editingMemoryProfileId === itemId) {
        resetMemoryProfileForm();
      }
      invalidateData("memoryProfileRefs", "agentDefinitionRefs", "controlPlane");
      await ensureMemoryProfilesPage(true);
      showResult(memoryProfileResult, { message: "记忆画像已删除", id: itemId });
    } catch (error) {
      showResult(memoryProfileResult, errorResult(error));
    }
    return;
  }

  const agentDefinitionEdit = event.target.closest("[data-agent-definition-edit]");
  if (agentDefinitionEdit) {
    try {
      await switchPage("agent-definitions");
      const item =
        state.agentDefinitions.find((entry) => entry.id === agentDefinitionEdit.dataset.agentDefinitionEdit) ||
        (await api(`/api/agent-center/agent-definitions/${agentDefinitionEdit.dataset.agentDefinitionEdit}`));
      fillAgentDefinitionForm(item, { openModal: true });
    } catch (error) {
      showResult(agentDefinitionResult, errorResult(error));
    }
    return;
  }

  const agentDefinitionDelete = event.target.closest("[data-agent-definition-delete]");
  if (agentDefinitionDelete) {
    const itemId = agentDefinitionDelete.dataset.agentDefinitionDelete;
    const item = state.agentDefinitions.find((entry) => entry.id === itemId) || null;
    if (!window.confirm(`确认删除 Agent 管理项“${item?.name || itemId}”？`)) {
      return;
    }
    try {
      if (state.agentDefinitionPage.items.length === 1 && state.agentDefinitionPage.offset > 0) {
        state.agentDefinitionPage.offset = Math.max(0, state.agentDefinitionPage.offset - state.agentDefinitionPage.limit);
      }
      await api(`/api/agent-center/agent-definitions/${itemId}`, { method: "DELETE" });
      if (state.editingAgentDefinitionId === itemId) {
        resetAgentDefinitionForm();
        closeAgentDefinitionModal();
      }
      invalidateData("agentDefinitionPage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
      await ensureAgentDefinitionsPage(true);
      showResult(agentDefinitionResult, { message: "Agent 管理项已删除", id: itemId });
    } catch (error) {
      showResult(agentDefinitionResult, errorResult(error));
    }
    return;
  }

  const nodePalette = event.target.closest("[data-node-type]");
  if (nodePalette) {
    addGraphNode(nodePalette.dataset.nodeType);
    return;
  }

  const memberRemove = event.target.closest("[data-team-member-remove]");
  if (memberRemove) {
    removeMember(memberRemove.dataset.teamMemberRemove);
    return;
  }

  const teamDefinitionMemberRemove = event.target.closest("[data-team-definition-member-remove]");
  if (teamDefinitionMemberRemove) {
    removeTeamDefinitionMember(Number(teamDefinitionMemberRemove.dataset.teamDefinitionMemberRemove));
    return;
  }

  const teamDefinitionReviewOverrideRemove = event.target.closest("[data-team-definition-review-override-remove]");
  if (teamDefinitionReviewOverrideRemove) {
    removeTeamDefinitionReviewOverride(Number(teamDefinitionReviewOverrideRemove.dataset.teamDefinitionReviewOverrideRemove));
    return;
  }

  const edgeRemove = event.target.closest("[data-edge-from][data-edge-to]");
  if (edgeRemove && edgeRemove.hasAttribute("data-team-edge-remove")) {
    removeEdge(edgeRemove.dataset.edgeFrom, edgeRemove.dataset.edgeTo);
    return;
  }

  const agentTemplateEdit = event.target.closest("[data-agent-template-edit]");
  if (agentTemplateEdit) {
    try {
      await ensureAgentTemplateEditorData();
      const template = await api(`/api/agent-center/agent-templates/${agentTemplateEdit.dataset.agentTemplateEdit}`);
      fillAgentTemplateForm(template);
      await switchPage("agent-templates");
    } catch (error) {
      showResult(agentTemplateResult, errorResult(error));
    }
    return;
  }

  const agentTemplateDelete = event.target.closest("[data-agent-template-delete]");
  if (agentTemplateDelete) {
    const templateId = agentTemplateDelete.dataset.agentTemplateDelete;
    const template = state.agentTemplatePage.items.find((item) => item.id === templateId) || state.agentTemplates.find((item) => item.id === templateId) || null;
    const templateName = template?.name || templateId;
    if (!window.confirm(`确认删除 Agent 模板“${templateName}”？`)) {
      return;
    }
    try {
      if (state.agentTemplatePage.items.length === 1 && state.agentTemplatePage.offset > 0) {
        state.agentTemplatePage.offset = Math.max(0, state.agentTemplatePage.offset - state.agentTemplatePage.limit);
      }
      await api(`/api/agent-center/agent-templates/${templateId}`, { method: "DELETE" });
      if (state.editingAgentTemplateId === templateId) {
        resetAgentTemplateForm();
        closeAgentTemplateModal();
      }
      invalidateData("agentTemplatePage", "agentTemplateRefs", "controlPlane");
      await ensureAgentTemplatesPage(true);
      showResult(agentTemplateResult, { message: "Agent 模板已删除", id: templateId });
    } catch (error) {
      showResult(agentTemplateResult, errorResult(error));
    }
    return;
  }

  const teamDefinitionEdit = event.target.closest("[data-team-definition-edit]");
  if (teamDefinitionEdit) {
    try {
      await switchPage("team-definitions");
      const definition =
        state.teamDefinitions.find((item) => item.id === teamDefinitionEdit.dataset.teamDefinitionEdit) ||
        (await api(`/api/agent-center/team-definitions/${teamDefinitionEdit.dataset.teamDefinitionEdit}`));
      fillTeamDefinitionForm(definition, { openModal: true });
    } catch (error) {
      showResult(teamDefinitionResult, errorResult(error));
    }
    return;
  }

  const teamDefinitionTask = event.target.closest("[data-team-definition-task]");
  if (teamDefinitionTask) {
    state.selectedTaskTeamDefinitionId = teamDefinitionTask.dataset.teamDefinitionTask || null;
    state.selectedBuildId = null;
    state.selectedBlueprintId = null;
    await switchPage("runtime", { force: true });
    return;
  }

  const teamDefinitionCompile = event.target.closest("[data-team-definition-compile]");
  if (teamDefinitionCompile) {
    try {
      await switchPage("team-definitions");
      const payload = await api(`/api/agent-center/team-definitions/${teamDefinitionCompile.dataset.teamDefinitionCompile}/compile`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      showResult(teamDefinitionResult, payload);
    } catch (error) {
      showResult(teamDefinitionResult, errorResult(error));
    }
    return;
  }

  const teamDefinitionBuild = event.target.closest("[data-team-definition-build]");
  if (teamDefinitionBuild) {
    try {
      const payload = await api(`/api/agent-center/team-definitions/${teamDefinitionBuild.dataset.teamDefinitionBuild}/build`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      if (payload.blueprint?.id) {
        state.selectedBlueprintId = payload.blueprint.id;
      }
      invalidateData("blueprints", "controlPlane");
      await switchPage("blueprints", { force: true });
      if (payload.blueprint?.id) {
        await openBlueprint(payload.blueprint.id);
      }
    } catch (error) {
      showResult(teamDefinitionResult, errorResult(error));
    }
    return;
  }

  const teamTemplateEdit = event.target.closest("[data-team-template-edit]");
  if (teamTemplateEdit) {
    const template = state.teamTemplates.find((item) => item.id === teamTemplateEdit.dataset.teamTemplateEdit);
    if (template) {
      fillTeamTemplateForm(template);
      await switchPage("team-templates");
    }
    return;
  }

  const teamBuild = event.target.closest("[data-team-build]");
  if (teamBuild) {
    try {
      const payload = await api(`/api/agent-center/team-templates/${teamBuild.dataset.teamBuild}/build`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      state.selectedBuildId = payload.id;
      if (payload.blueprint_id) {
        state.selectedBlueprintId = payload.blueprint_id;
      }
      invalidateData("builds", "blueprints", "controlPlane");
      await switchPage("builds", { force: true });
      await openBuild(payload.id);
    } catch (error) {
      showResult(buildResult, { error: error.message });
    }
    return;
  }

  const buildOpen = event.target.closest("[data-build-open]");
  if (buildOpen) {
    await switchPage("builds");
    await openBuild(buildOpen.dataset.buildOpen);
    return;
  }

  const buildUse = event.target.closest("[data-build-use]");
  if (buildUse) {
    state.selectedBuildId = buildUse.dataset.buildUse;
    populateTaskOptions();
    await switchPage("runtime");
    return;
  }

  const blueprintOpen = event.target.closest("[data-blueprint-open]");
  if (blueprintOpen) {
    await switchPage("blueprints");
    await openBlueprint(blueprintOpen.dataset.blueprintOpen);
    return;
  }

  const blueprintUse = event.target.closest("[data-blueprint-use]");
  if (blueprintUse) {
    state.selectedBlueprintId = blueprintUse.dataset.blueprintUse;
    populateTaskOptions();
    await switchPage("runtime");
    return;
  }

  const runOpen = event.target.closest("[data-run-open]");
  if (runOpen) {
    await switchPage("runtime");
    await loadRunDetail(runOpen.dataset.runOpen);
    renderRuns();
    return;
  }

  const runResume = event.target.closest("[data-run-resume]");
  if (runResume) {
    try {
      const payload = await api(`/api/runs/${runResume.dataset.runResume}/resume`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      state.selectedRunId = payload.run.id;
      invalidateData("runs", "approvals", "controlPlane");
      await switchPage("runtime", { force: true });
      await loadRunDetail(payload.run.id);
    } catch (error) {
      showResult(taskResult, { error: error.message });
    }
    return;
  }

  const approveButton = event.target.closest("[data-approval-approve]");
  if (approveButton) {
    try {
      await api(`/api/approvals/${approveButton.dataset.approvalApprove}/resolve`, {
        method: "POST",
        body: JSON.stringify({ approved: true, comment: "Approved from control plane." }),
      });
      const payload = await api(`/api/runs/${approveButton.dataset.runId}/resume`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      state.selectedRunId = payload.run.id;
      invalidateData("runs", "approvals", "controlPlane");
      await switchPage("runtime", { force: true });
      await loadRunDetail(payload.run.id);
    } catch (error) {
      showResult(taskResult, { error: error.message });
    }
    return;
  }

  const rejectButton = event.target.closest("[data-approval-reject]");
  if (rejectButton) {
    try {
      await api(`/api/approvals/${rejectButton.dataset.approvalReject}/resolve`, {
        method: "POST",
        body: JSON.stringify({ approved: false, comment: "Rejected from control plane." }),
      });
      const payload = await api(`/api/runs/${rejectButton.dataset.runId}/resume`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      state.selectedRunId = payload.run.id;
      invalidateData("runs", "approvals", "controlPlane");
      await switchPage("runtime", { force: true });
      await loadRunDetail(payload.run.id);
    } catch (error) {
      showResult(taskResult, { error: error.message });
    }
  }
});

document.addEventListener("input", (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }
  const teamDefinitionReviewOverrideField = event.target.closest("[data-team-definition-review-override-field]");
  if (teamDefinitionReviewOverrideField) {
    updateTeamDefinitionReviewOverrideField(
      Number(teamDefinitionReviewOverrideField.dataset.reviewOverrideIndex),
      teamDefinitionReviewOverrideField.dataset.teamDefinitionReviewOverrideField,
      fieldValue(teamDefinitionReviewOverrideField),
    );
    return;
  }
  const knowledgeDocumentField = event.target.closest("[data-knowledge-document-field]");
  if (knowledgeDocumentField) {
    updateKnowledgeBaseDocumentField(
      Number(knowledgeDocumentField.dataset.documentIndex),
      knowledgeDocumentField.dataset.knowledgeDocumentField,
      knowledgeDocumentField.value,
    );
    return;
  }
  const memberField = event.target.closest("[data-team-member-field]");
  if (memberField) {
    updateMemberField(Number(memberField.dataset.memberIndex), memberField.dataset.teamMemberField, memberField.value);
    return;
  }
  const edgeWhen = event.target.closest("[data-team-edge-when]");
  if (edgeWhen) {
    updateEdgeWhen(edgeWhen.dataset.edgeFrom, edgeWhen.dataset.edgeTo, edgeWhen.value);
  }
});

document.addEventListener("change", (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }
  const knowledgeDocumentField = event.target.closest("[data-knowledge-document-field]");
  if (knowledgeDocumentField) {
    updateKnowledgeBaseDocumentField(
      Number(knowledgeDocumentField.dataset.documentIndex),
      knowledgeDocumentField.dataset.knowledgeDocumentField,
      knowledgeDocumentField.value,
    );
  }
  const teamDefinitionReviewOverrideField = event.target.closest("[data-team-definition-review-override-field]");
  if (teamDefinitionReviewOverrideField) {
    updateTeamDefinitionReviewOverrideField(
      Number(teamDefinitionReviewOverrideField.dataset.reviewOverrideIndex),
      teamDefinitionReviewOverrideField.dataset.teamDefinitionReviewOverrideField,
      fieldValue(teamDefinitionReviewOverrideField),
    );
  }
  const memberField = event.target.closest("[data-team-member-field]");
  if (memberField) {
    updateMemberField(Number(memberField.dataset.memberIndex), memberField.dataset.teamMemberField, memberField.value);
  }
  const teamDefinitionMemberField = event.target.closest("[data-team-definition-member-field]");
  if (teamDefinitionMemberField) {
    updateTeamDefinitionMemberField(
      Number(teamDefinitionMemberField.dataset.memberIndex),
      teamDefinitionMemberField.dataset.teamDefinitionMemberField,
      teamDefinitionMemberField.value,
    );
  }
  const teamDefinitionMemberCheck = event.target.closest("[data-team-definition-member-check]");
  if (teamDefinitionMemberCheck) {
    updateTeamDefinitionMemberCheck(
      Number(teamDefinitionMemberCheck.dataset.memberIndex),
      teamDefinitionMemberCheck.dataset.teamDefinitionMemberCheck,
      teamDefinitionMemberCheck.checked,
    );
  }
});

document.addEventListener("pointerdown", (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }
  const node = event.target.closest(".graph-node");
  if (!node) {
    return;
  }
  const nodeId = node.dataset.nodeId;
  if (state.teamEditor.linkFromNodeId) {
    connectNodes(state.teamEditor.linkFromNodeId, nodeId);
    return;
  }
  const spec = teamEditorSpec();
  const position = spec.metadata?.ui_layout?.positions?.[nodeId];
  if (!position) {
    return;
  }
  state.teamEditor.selectedNodeId = nodeId;
  state.teamEditor.drag = {
    nodeId,
    startX: event.clientX,
    startY: event.clientY,
    originX: position.x,
    originY: position.y,
  };
  renderTeamEditor();
});

window.addEventListener("pointermove", (event) => {
  const drag = state.teamEditor.drag;
  if (!drag) {
    return;
  }
  const spec = teamEditorSpec();
  const positions = spec.metadata?.ui_layout?.positions || {};
  positions[drag.nodeId] = {
    x: Math.max(16, drag.originX + (event.clientX - drag.startX)),
    y: Math.max(16, drag.originY + (event.clientY - drag.startY)),
  };
  state.teamEditor.spec = spec;
  renderTeamGraph();
});

window.addEventListener("pointerup", () => {
  if (!state.teamEditor.drag) {
    return;
  }
  state.teamEditor.drag = null;
  syncTeamEditorToForm();
  renderTeamTemplatePreview();
});

document.addEventListener("click", (event) => {
  if (!(event.target instanceof Element)) {
    closeAllTagMultiSelects();
    return;
  }
  if (event.target.closest("[data-tag-select]")) {
    return;
  }
  closeAllTagMultiSelects();
});

initializeTagMultiSelects();

(async function bootstrap() {
  try {
    await switchPage(state.activePage);
  } catch (error) {
    showResult(taskResult, { error: error.message });
  }
})();
