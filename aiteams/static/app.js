const DEFAULT_UI_METADATA = {
  review_policy: {
    decision_types: [],
  },
  team_edge_review: {
    modes: [],
    message_types: [],
    phases: [],
  },
};

function loadPersistedTaskSessionThreads() {
  try {
    const raw = window.localStorage.getItem("aiteams.taskSessionThreads");
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (error) {
    console.warn("Failed to restore persisted task session threads", error);
    return {};
  }
}

const state = {
  summary: {},
  storage: null,
  providerTypes: [],
  uiMetadata: null,
  recentRuns: [],
  providers: [],
  localModels: [],
  retrievalSettings: {
    settings: {
      embedding: { mode: "disabled" },
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
  localModelPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
    query: "",
  },
  plugins: [],
  builtinPlugins: [],
  pluginPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  pluginImportFiles: [],
  pluginImportDragActive: false,
  pluginImportBusy: false,
  pluginImportMode: "import",
  pluginImportTargetId: null,
  pluginImportTargetKey: "",
  pluginImportTargetVersion: "",
  pluginImportTargetName: "",
  pluginImportScanResult: null,
  skills: [],
  skillGroups: [],
  skillGroupCatalog: [],
  skillGroupPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  skillPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
    query: "",
    groupKey: "",
  },
  staticMemories: [],
  staticMemoryPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  knowledgeBases: [],
  knowledgeBasePage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  reviewPolicies: [],
  reviewPolicyPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
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
  teamDefinitionPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  runPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
  },
  teamChat: {
    selectedTeamDefinitionId: null,
    threads: [],
    selectedThreadRecordId: null,
    selectedSessionThreadId: "",
    messages: [],
    sending: false,
    draftMode: false,
    pollTimer: 0,
    pollBusy: false,
    resultTimer: 0,
  },
  approvals: [],
  approvalPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
    view: "pending",
  },
  approvalEditor: {
    item: null,
  },
  selectedTaskTeamDefinitionId: null,
  taskSessionThreads: loadPersistedTaskSessionThreads(),
  activePage: "overview",
  activeNavSection: "overview",
  editingProviderId: null,
  editingLocalModelId: null,
  editingPluginId: null,
  editingSkillId: null,
  skillModalMode: "import",
  skillImportFiles: [],
  skillImportDragActive: false,
  skillImportBusy: false,
  editingSkillGroupId: null,
  skillManagementView: "skills",
  skillPreview: {
    skillId: null,
    skill: null,
    files: [],
    selectedPath: "",
    fileCache: {},
    loading: false,
    fileLoading: false,
    errorText: "",
  },
  editingStaticMemoryId: null,
  staticMemoryEditorMode: "role",
  editingKnowledgeBaseId: null,
  localModelUploadFiles: [],
  localModelUploadDragActive: false,
  localModelUploadBusy: false,
  editingReviewPolicyId: null,
  editingAgentDefinitionId: null,
  editingAgentTemplateId: null,
  editingTeamDefinitionId: null,
  editingTeamTemplateId: null,
  knowledgeBaseDocuments: [],
  knowledgeBasePoolPage: {
    items: [],
    total: 0,
    limit: 8,
    offset: 0,
    query: "",
  },
  knowledgeBaseDocumentPage: {
    items: [],
    total: 0,
    limit: 8,
    offset: 0,
    query: "",
    embeddingStatus: "all",
  },
  knowledgeBaseDocumentSelection: [],
  knowledgeBaseDocumentActionBusy: false,
  knowledgeBaseEmbeddingJobId: null,
  knowledgeBaseEmbeddingJobPollVersion: 0,
  knowledgeBaseUploadFiles: [],
  knowledgeBaseStagedSelection: [],
  knowledgeBaseUploadDragActive: false,
  knowledgeBaseUploadBusy: false,
  reviewPolicyBaseSpec: {},
  agentDefinitionBaseSpec: {},
  teamDefinitionBaseSpec: {},
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
    record: null,
  },
  loaded: {
    controlPlane: false,
    providerTypes: false,
    uiMetadata: false,
    providerRefs: false,
    localModelRefs: false,
    retrievalSettings: false,
    providerPage: false,
    localModelPage: false,
    builtinPluginRefs: false,
    pluginRefs: false,
    pluginPage: false,
    skillRefs: false,
    skillGroupCatalog: false,
    skillGroupPage: false,
    skillPage: false,
    staticMemoryRefs: false,
    staticMemoryPage: false,
    knowledgeBaseRefs: false,
    knowledgeBasePage: false,
    reviewPolicyRefs: false,
    reviewPolicyPage: false,
    agentTemplateRefs: false,
    agentTemplatePage: false,
    agentDefinitionRefs: false,
    agentDefinitionPage: false,
    teamDefinitions: false,
    teamDefinitionPage: false,
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

const DEFAULT_LOCAL_EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2";
const DEFAULT_LOCAL_RERANK_MODEL = "BAAI/bge-reranker-v2-m3";

const PAGE_SECTIONS = {
  overview: "overview",
  providers: "resources",
  "local-models": "resources",
  "retrieval-config": "config",
  plugins: "resources",
  skills: "resources",
  "responsibility-specs": "resources",
  "knowledge-bases": "resources",
  "review-policies": "resources",
  "agent-definitions": "resources",
  "team-definitions": "orchestration",
  "team-chat": "delivery",
  runtime: "delivery",
  approvals: "delivery",
};

const MODEL_TYPE_LABELS = {
  chat: "\u804a\u5929",
  embedding: "\u5d4c\u5165",
  rerank: "\u91cd\u6392",
};

const LOCAL_MODEL_TYPE_LABELS = {
  Embed: "Embed",
  Rerank: "Rerank",
  Chat: "Chat",
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
const localModelForm = document.querySelector("#local-model-form");
const localModelName = document.querySelector("#local-model-name");
const localModelType = document.querySelector("#local-model-type");
const localModelPath = document.querySelector("#local-model-path");
const localModelOpenCreate = document.querySelector("#local-model-open-create");
const localModelPageSize = document.querySelector("#local-model-page-size");
const localModelList = document.querySelector("#local-model-list");
const localModelPaginationMeta = document.querySelector("#local-model-pagination-meta");
const localModelResult = document.querySelector("#local-model-result");
const localModelModal = document.querySelector("#local-model-modal");
const localModelModalTitle = document.querySelector("#local-model-modal-title");
const localModelModalCloseButtons = Array.from(document.querySelectorAll("[data-local-model-modal-close]"));
const localModelCancel = document.querySelector("#local-model-cancel");
const localModelFolderInput = document.querySelector("#local-model-folder-input");
const localModelUploadFolderButton = document.querySelector("#local-model-upload-folder");
const localModelDropzone = document.querySelector("#local-model-dropzone");
const localModelUploadSelection = document.querySelector("#local-model-upload-selection");
const localModelModalResult = document.querySelector("#local-model-modal-result");
const localModelSave = document.querySelector("#local-model-save");
const retrievalSettingsForm = document.querySelector("#retrieval-settings-form");
const retrievalSummary = document.querySelector("#retrieval-summary");
const retrievalEmbeddingMode = document.querySelector("#retrieval-embedding-mode");
const retrievalEmbeddingFields = document.querySelector("#retrieval-embedding-fields");
const retrievalEmbeddingProviderWrap = document.querySelector("#retrieval-embedding-provider-wrap");
const retrievalEmbeddingProvider = document.querySelector("#retrieval-embedding-provider");
const retrievalEmbeddingModelWrap = document.querySelector("#retrieval-embedding-model-wrap");
const retrievalEmbeddingModel = document.querySelector("#retrieval-embedding-model");
const retrievalEmbeddingLocalModelWrap = document.querySelector("#retrieval-embedding-local-model-wrap");
const retrievalEmbeddingLocalModel = document.querySelector("#retrieval-embedding-local-model");
const retrievalRerankMode = document.querySelector("#retrieval-rerank-mode");
const retrievalRerankFields = document.querySelector("#retrieval-rerank-fields");
const retrievalRerankProviderWrap = document.querySelector("#retrieval-rerank-provider-wrap");
const retrievalRerankProvider = document.querySelector("#retrieval-rerank-provider");
const retrievalRerankModelWrap = document.querySelector("#retrieval-rerank-model-wrap");
const retrievalRerankModel = document.querySelector("#retrieval-rerank-model");
const retrievalRerankLocalModelWrap = document.querySelector("#retrieval-rerank-local-model-wrap");
const retrievalRerankLocalModel = document.querySelector("#retrieval-rerank-local-model");
const retrievalSettingsRefresh = document.querySelector("#retrieval-settings-refresh");
const retrievalSettingsResult = document.querySelector("#retrieval-settings-result");
let knowledgeBasePoolQueryTimer = 0;
let knowledgeBaseDocumentQueryTimer = 0;
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
const pluginName = document.querySelector("#plugin-name");
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
const pluginImportModal = document.querySelector("#plugin-import-modal");
const pluginImportModalTitle = document.querySelector("#plugin-import-modal-title");
const pluginImportModalCloseButtons = Array.from(document.querySelectorAll("[data-plugin-import-modal-close]"));
const pluginImportForm = document.querySelector("#plugin-import-form");
const pluginImportDirectoryInput = document.querySelector("#plugin-import-directory-input");
const pluginImportDropzone = document.querySelector("#plugin-import-dropzone");
const pluginImportDropzoneTitle = document.querySelector("#plugin-import-dropzone-title");
const pluginImportDropzoneHint = document.querySelector("#plugin-import-dropzone-hint");
const pluginImportDropzoneTrigger = document.querySelector("#plugin-import-dropzone-trigger");
const pluginImportSelection = document.querySelector("#plugin-import-selection");
const pluginImportResult = document.querySelector("#plugin-import-result");
const pluginImportCancel = document.querySelector("#plugin-import-cancel");
const pluginImportValidate = document.querySelector("#plugin-import-validate");
const pluginImportSave = document.querySelector("#plugin-import-save");

const skillForm = document.querySelector("#skill-form");
const skillViewSkills = document.querySelector("#skill-view-skills");
const skillViewGroups = document.querySelector("#skill-view-groups");
const skillOpenCreate = document.querySelector("#skill-open-create");
const skillManagementSkillsView = document.querySelector("#skill-management-skills-view");
const skillManagementGroupsView = document.querySelector("#skill-management-groups-view");
const skillGroupOpenManageInline = document.querySelector("#skill-group-open-manage-inline");
const skillGroupPageSize = document.querySelector("#skill-group-page-size");
const skillGroupPaginationMeta = document.querySelector("#skill-group-pagination-meta");
const skillPageSize = document.querySelector("#skill-page-size");
const skillList = document.querySelector("#skill-list");
const skillPaginationMeta = document.querySelector("#skill-pagination-meta");
const skillResult = null;
const skillModal = document.querySelector("#skill-modal");
const skillModalTitle = document.querySelector("#skill-modal-title");
const skillModalCloseButtons = Array.from(document.querySelectorAll("[data-skill-modal-close]"));
const skillCancel = document.querySelector("#skill-cancel");
const skillValidate = document.querySelector("#skill-validate");
const skillSave = document.querySelector("#skill-save");
const skillImportPanel = document.querySelector("#skill-import-panel");
const skillEditorPanel = document.querySelector("#skill-editor-panel");
const skillImportDirectoryInput = document.querySelector("#skill-import-directory-input");
const skillImportDropzone = document.querySelector("#skill-import-dropzone");
const skillImportDropzoneTitle = document.querySelector("#skill-import-dropzone-title");
const skillImportDropzoneHint = document.querySelector("#skill-import-dropzone-hint");
const skillImportDropzoneTrigger = document.querySelector("#skill-import-dropzone-trigger");
const skillImportSelection = document.querySelector("#skill-import-selection");
const skillImportGroups = document.querySelector("#skill-import-groups");
const skillModalResult = document.querySelector("#skill-modal-result");
const skillGroupResult = document.querySelector("#skill-group-result");
const skillGroupModal = document.querySelector("#skill-group-modal");
const skillGroupModalTitle = document.querySelector("#skill-group-modal-title");
const skillGroupModalCloseButtons = Array.from(document.querySelectorAll("[data-skill-group-modal-close]"));
const skillGroupManagementList = document.querySelector("#skill-group-management-list");
const skillGroupForm = document.querySelector("#skill-group-form");
const skillGroupManagementName = document.querySelector("#skill-group-management-name");
const skillGroupManagementDescription = document.querySelector("#skill-group-management-description");
const skillGroupSkills = document.querySelector("#skill-group-skills");
const skillGroupCancel = document.querySelector("#skill-group-cancel");
const skillGroupModalResult = document.querySelector("#skill-group-modal-result");
const skillPreviewModal = document.querySelector("#skill-preview-modal");
const skillPreviewTitle = document.querySelector("#skill-preview-title");
const skillPreviewSubtitle = document.querySelector("#skill-preview-subtitle");
const skillPreviewSummary = document.querySelector("#skill-preview-summary");
const skillPreviewContent = document.querySelector("#skill-preview-content");
const skillPreviewBodyMeta = document.querySelector("#skill-preview-body-meta");
const skillPreviewFileMeta = document.querySelector("#skill-preview-file-meta");
const skillPreviewFileList = document.querySelector("#skill-preview-file-list");
const skillPreviewFileTitle = document.querySelector("#skill-preview-file-title");
const skillPreviewFileContent = document.querySelector("#skill-preview-file-content");
const skillPreviewCloseButtons = Array.from(document.querySelectorAll("[data-skill-preview-close]"));

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

const knowledgeBaseOpenCreate = document.querySelector("#knowledge-base-open-create");
const knowledgeBasePageSize = document.querySelector("#knowledge-base-page-size");
const knowledgeBasePaginationMeta = document.querySelector("#knowledge-base-pagination-meta");
const knowledgeBaseList = document.querySelector("#knowledge-base-list");
const knowledgeBaseResult = document.querySelector("#knowledge-base-result");
const knowledgeBaseModal = document.querySelector("#knowledge-base-modal");
const knowledgeBaseModalTitle = document.querySelector("#knowledge-base-modal-title");
const knowledgeBaseModalCloseButtons = Array.from(document.querySelectorAll("[data-knowledge-base-modal-close]"));
const knowledgeBaseForm = document.querySelector("#knowledge-base-form");
const knowledgeBaseName = document.querySelector("#knowledge-base-name");
const knowledgeBaseTransferLayout = document.querySelector("#knowledge-base-transfer-layout");
const knowledgeBaseFileInput = document.querySelector("#knowledge-base-file-input");
const knowledgeBaseFolderInput = document.querySelector("#knowledge-base-folder-input");
const knowledgeBaseDropzone = document.querySelector("#knowledge-base-dropzone");
const knowledgeBaseUploadFolder = document.querySelector("#knowledge-base-upload-folder");
const knowledgeBasePoolQuery = document.querySelector("#knowledge-base-pool-query");
const knowledgeBaseUploadSelection = document.querySelector("#knowledge-base-upload-selection");
const knowledgeBaseStageSelectAll = document.querySelector("#knowledge-base-stage-select-all");
const knowledgeBaseStageSelectionActions = document.querySelector("#knowledge-base-stage-selection-actions");
const knowledgeBaseStageRemove = document.querySelector("#knowledge-base-stage-remove");
const knowledgeBaseStageMove = document.querySelector("#knowledge-base-stage-move");
const knowledgeBaseDocumentMoveBack = document.querySelector("#knowledge-base-document-move-back");
const knowledgeBaseDocumentQuery = document.querySelector("#knowledge-base-document-query");
const knowledgeBaseDocumentStatus = document.querySelector("#knowledge-base-document-status");
const knowledgeBaseDocumentList = document.querySelector("#knowledge-base-document-list");
const knowledgeDocumentSelectionActions = document.querySelector("#knowledge-document-selection-actions");
const knowledgeDocumentSelectAll = document.querySelector("#knowledge-document-select-all");
const knowledgeDocumentEmbedAdd = document.querySelector("#knowledge-document-embed-add");
const knowledgeBaseCancel = document.querySelector("#knowledge-base-cancel");
const knowledgeBaseSave = document.querySelector("#knowledge-base-save");
const knowledgeBaseModalResult = document.querySelector("#knowledge-base-modal-result");

const reviewPolicyForm = document.querySelector("#review-policy-form");
const reviewPolicyName = document.querySelector("#review-policy-name");
const reviewPolicyRuleAdd = document.querySelector("#review-policy-rule-add");
const reviewPolicyRuleList = document.querySelector("#review-policy-rule-list");
const reviewPolicyRules = document.querySelector("#review-policy-rules");
const reviewPolicyOpenCreate = document.querySelector("#review-policy-open-create");
const reviewPolicyPageSize = document.querySelector("#review-policy-page-size");
const reviewPolicyPaginationMeta = document.querySelector("#review-policy-pagination-meta");
const reviewPolicyList = document.querySelector("#review-policy-list");
const reviewPolicyResult = document.querySelector("#review-policy-result");
const reviewPolicyModal = document.querySelector("#review-policy-modal");
const reviewPolicyModalTitle = document.querySelector("#review-policy-modal-title");
const reviewPolicyModalCloseButtons = Array.from(document.querySelectorAll("[data-review-policy-modal-close]"));
const reviewPolicyCancel = document.querySelector("#review-policy-cancel");
const reviewPolicyModalResult = document.querySelector("#review-policy-modal-result");

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
const teamDefinitionDescription = document.querySelector("#team-definition-description");
const teamDefinitionLeadAgentDefinition = document.querySelector("#team-definition-lead-agent-definition");
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
const teamDefinitionPageSize = document.querySelector("#team-definition-page-size");
const teamDefinitionPaginationMeta = document.querySelector("#team-definition-pagination-meta");
const teamDefinitionResult = document.querySelector("#team-definition-result");
const teamDefinitionModal = document.querySelector("#team-definition-modal");
const teamDefinitionModalTitle = document.querySelector("#team-definition-modal-title");
const teamDefinitionModalCloseButtons = Array.from(document.querySelectorAll("[data-team-definition-modal-close]"));
const teamDefinitionCancel = document.querySelector("#team-definition-cancel");
const teamDefinitionModalResult = document.querySelector("#team-definition-modal-result");
const teamDefinitionPreviewModal = document.querySelector("#team-definition-preview-modal");
const teamDefinitionPreviewTitle = document.querySelector("#team-definition-preview-title");
const teamDefinitionPreviewCloseButtons = Array.from(document.querySelectorAll("[data-team-definition-preview-close]"));
const teamDefinitionPreviewSummary = document.querySelector("#team-definition-preview-summary");
const teamDefinitionPreviewTree = document.querySelector("#team-definition-preview-tree");

const teamDefinitionPreviewState = {
  payload: null,
  loading: false,
  errorText: "",
};

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

const taskForm = document.querySelector("#task-form");
const taskTeamDefinition = document.querySelector("#task-team-definition");
const taskTitle = document.querySelector("#task-title");
const taskApprovalMode = document.querySelector("#task-approval-mode");
const taskNewSession = document.querySelector("#task-new-session");
const taskSessionHint = document.querySelector("#task-session-hint");
const taskPrompt = document.querySelector("#task-prompt");
const taskResult = document.querySelector("#task-result");
const runList = document.querySelector("#run-list");
const runPageSize = document.querySelector("#run-page-size");
const runPaginationMeta = document.querySelector("#run-pagination-meta");
const runDetail = document.querySelector("#run-detail");
const approvalList = document.querySelector("#approval-list");
const approvalsPanelTitle = document.querySelector('.page-view[data-page="approvals"] .panel-title h3');
const approvalsViewPending = document.querySelector("#approvals-view-pending");
const approvalsViewHistory = document.querySelector("#approvals-view-history");
const approvalPageSize = document.querySelector("#approval-page-size");
const approvalPaginationMeta = document.querySelector("#approval-pagination-meta");
const approvalEditModal = document.querySelector("#approval-edit-modal");
const approvalEditModalTitle = document.querySelector("#approval-edit-modal-title");
const approvalEditModalCloseButtons = Array.from(document.querySelectorAll("[data-approval-edit-close]"));
const approvalEditForm = document.querySelector("#approval-edit-form");
const approvalEditSummary = document.querySelector("#approval-edit-summary");
const approvalEditFields = document.querySelector("#approval-edit-fields");
const approvalEditResult = document.querySelector("#approval-edit-result");
const approvalEditCancel = document.querySelector("#approval-edit-cancel");
const teamChatTeamDefinition = document.querySelector("#team-chat-team-definition");
const teamChatNewThread = document.querySelector("#team-chat-new-thread");
const teamChatThreadList = document.querySelector("#team-chat-thread-list");
const teamChatTitle = document.querySelector("#team-chat-title");
const teamChatThreadMeta = document.querySelector("#team-chat-thread-meta");
const teamChatOpenTeam = document.querySelector("#team-chat-open-team");
const teamChatMessageList = document.querySelector("#team-chat-message-list");
const teamChatForm = document.querySelector("#team-chat-form");
const teamChatInput = document.querySelector("#team-chat-input");
const teamChatSend = document.querySelector("#team-chat-send");
const teamChatResult = document.querySelector("#team-chat-result");
const teamChatPageView = document.querySelector('.page-view[data-page="team-chat"]');
const teamChatLayout = teamChatPageView?.querySelector(".team-chat-layout") || null;

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

function fileNameFromPath(path) {
  const text = String(path || "").replace(/\\/g, "/");
  const parts = text.split("/").filter(Boolean);
  return parts[parts.length - 1] || text || "-";
}

function sanitizeMarkdownUrl(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }
  if (/^(javascript|data|vbscript):/i.test(text)) {
    return "";
  }
  if (/^(https?:|mailto:|tel:)/i.test(text)) {
    return text;
  }
  if (/^(#|\/|\.\.?(\/|$))/.test(text)) {
    return text;
  }
  return "";
}

function splitMarkdownTableRow(value) {
  const text = String(value ?? "").trim();
  if (!text || !text.includes("|")) {
    return [];
  }
  const body = text.replace(/^\|/, "").replace(/\|$/, "");
  const cells = [];
  let current = "";
  for (let index = 0; index < body.length; index += 1) {
    const char = body[index];
    if (char === "\\") {
      const next = body[index + 1];
      if (next === "|" || next === "\\") {
        current += next;
        index += 1;
        continue;
      }
    }
    if (char === "|") {
      cells.push(current.trim());
      current = "";
      continue;
    }
    current += char;
  }
  cells.push(current.trim());
  return cells;
}

function markdownTableAlignments(separatorLine) {
  return splitMarkdownTableRow(separatorLine).map((cell) => {
    const normalized = cell.replace(/\s+/g, "");
    if (!/^:?-{3,}:?$/.test(normalized)) {
      return "";
    }
    if (normalized.startsWith(":") && normalized.endsWith(":")) {
      return "center";
    }
    if (normalized.endsWith(":")) {
      return "right";
    }
    if (normalized.startsWith(":")) {
      return "left";
    }
    return "";
  });
}

function isMarkdownTableSeparator(value) {
  const cells = splitMarkdownTableRow(value);
  return Boolean(cells.length) && cells.every((cell) => /^:?-{3,}:?$/.test(cell.replace(/\s+/g, "")));
}

function renderMarkdownTable(headers, alignments, rows) {
  const width = Math.max(headers.length, ...rows.map((row) => row.length));
  if (!width) {
    return "";
  }
  const normalizedHeaders = Array.from({ length: width }, (_, index) => headers[index] || "");
  const normalizedRows = rows.map((row) => Array.from({ length: width }, (_, index) => row[index] || ""));
  const alignAttr = (index) => {
    const value = alignments[index];
    return value ? ` style="text-align:${value}"` : "";
  };
  return `
    <div class="markdown-table-wrap">
      <table>
        <thead>
          <tr>${normalizedHeaders
            .map((cell, index) => `<th${alignAttr(index)}>${renderMarkdownInline(cell)}</th>`)
            .join("")}</tr>
        </thead>
        <tbody>${normalizedRows
          .map(
            (row) =>
              `<tr>${row
                .map((cell, index) => `<td${alignAttr(index)}>${renderMarkdownInline(cell)}</td>`)
                .join("")}</tr>`,
          )
          .join("")}</tbody>
      </table>
    </div>
  `;
}

function renderMarkdownInline(value) {
  const codeTokens = [];
  const raw = String(value ?? "").replace(/`([^`\n]+)`/g, (_, code) => {
    const token = `@@MDCODESPAN${codeTokens.length}@@`;
    codeTokens.push(`<code>${escapeHtml(code)}</code>`);
    return token;
  });
  let html = escapeHtml(raw);
  html = html.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, (match, label, target) => {
    const safeTarget = sanitizeMarkdownUrl(target);
    if (!safeTarget) {
      return escapeHtml(label);
    }
    return `<a href="${escapeAttribute(safeTarget)}" target="_blank" rel="noreferrer noopener">${escapeHtml(label)}</a>`;
  });
  html = html.replace(/(\*\*|__)(.+?)\1/g, "<strong>$2</strong>");
  html = html.replace(/~~(.+?)~~/g, "<del>$1</del>");
  codeTokens.forEach((token, index) => {
    html = html.replace(`@@MDCODESPAN${index}@@`, token);
  });
  return html;
}

function renderMarkdown(value) {
  const normalized = String(value ?? "").replace(/\r\n?/g, "\n").trim();
  if (!normalized) {
    return "";
  }

  const codeBlocks = [];
  const text = normalized.replace(/```([^\n`]*)\n?([\s\S]*?)```/g, (match, language, code) => {
    const token = `@@MDCODEBLOCK${codeBlocks.length}@@`;
    codeBlocks.push({
      token,
      language: String(language || "")
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_-]+/g, "-"),
      code: String(code || "").replace(/\n$/, ""),
    });
    return `\n${token}\n`;
  });

  const html = [];
  const lines = text.split("\n");
  let paragraph = [];
  let listType = "";
  let listItems = [];
  let quoteLines = [];

  function flushParagraph() {
    if (!paragraph.length) {
      return;
    }
    html.push(`<p>${paragraph.map((line) => renderMarkdownInline(line)).join("<br>")}</p>`);
    paragraph = [];
  }

  function flushList() {
    if (!listType || !listItems.length) {
      listType = "";
      listItems = [];
      return;
    }
    html.push(
      `<${listType}>${listItems
        .map((item) => `<li>${renderMarkdownInline(item).replace(/\n/g, "<br>")}</li>`)
        .join("")}</${listType}>`,
    );
    listType = "";
    listItems = [];
  }

  function flushQuote() {
    if (!quoteLines.length) {
      return;
    }
    html.push(`<blockquote>${quoteLines.map((line) => renderMarkdownInline(line)).join("<br>")}</blockquote>`);
    quoteLines = [];
  }

  function flushBlocks() {
    flushParagraph();
    flushList();
    flushQuote();
  }

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const trimmed = line.trim();
    if (!trimmed) {
      flushBlocks();
      continue;
    }

    if (/^@@MDCODEBLOCK\d+@@$/.test(trimmed)) {
      flushBlocks();
      html.push(trimmed);
      continue;
    }

    const nextLine = lines[index + 1] || "";
    if (trimmed.includes("|") && isMarkdownTableSeparator(nextLine)) {
      flushBlocks();
      const headers = splitMarkdownTableRow(trimmed);
      const alignments = markdownTableAlignments(nextLine);
      const rows = [];
      index += 2;
      while (index < lines.length) {
        const rowLine = lines[index];
        const rowTrimmed = rowLine.trim();
        if (!rowTrimmed || !rowTrimmed.includes("|") || isMarkdownTableSeparator(rowTrimmed)) {
          index -= 1;
          break;
        }
        const rowCells = splitMarkdownTableRow(rowTrimmed);
        if (!rowCells.length) {
          index -= 1;
          break;
        }
        rows.push(rowCells);
        index += 1;
      }
      if (!rows.length && index >= lines.length) {
        index -= 1;
      }
      html.push(renderMarkdownTable(headers, alignments, rows));
      continue;
    }

    const headingMatch = /^(#{1,6})\s+(.*)$/.exec(trimmed);
    if (headingMatch) {
      flushBlocks();
      const level = headingMatch[1].length;
      html.push(`<h${level}>${renderMarkdownInline(headingMatch[2])}</h${level}>`);
      continue;
    }

    if (/^(-{3,}|\*{3,})$/.test(trimmed)) {
      flushBlocks();
      html.push("<hr>");
      continue;
    }

    const quoteMatch = /^>\s?(.*)$/.exec(line);
    if (quoteMatch) {
      flushParagraph();
      flushList();
      quoteLines.push(quoteMatch[1]);
      continue;
    }
    flushQuote();

    const orderedMatch = /^\d+\.\s+(.*)$/.exec(trimmed);
    if (orderedMatch) {
      flushParagraph();
      if (listType && listType !== "ol") {
        flushList();
      }
      listType = "ol";
      listItems.push(orderedMatch[1]);
      continue;
    }

    const unorderedMatch = /^[-*+]\s+(.*)$/.exec(trimmed);
    if (unorderedMatch) {
      flushParagraph();
      if (listType && listType !== "ul") {
        flushList();
      }
      listType = "ul";
      listItems.push(unorderedMatch[1]);
      continue;
    }

    if (listType && /^\s{2,}\S/.test(line)) {
      listItems[listItems.length - 1] = `${listItems[listItems.length - 1]}\n${trimmed}`;
      continue;
    }

    flushList();
    paragraph.push(trimmed);
  }

  flushBlocks();

  let output = html.join("");
  codeBlocks.forEach((block) => {
    const languageClass = block.language ? ` class="language-${escapeAttribute(block.language)}"` : "";
    output = output.replace(block.token, `<pre><code${languageClass}>${escapeHtml(block.code)}</code></pre>`);
  });
  return output;
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

function showSkillPageError(error) {
  window.alert(formatApiError(error?.payload, error?.message || "\u64cd\u4f5c\u5931\u8d25"));
}

function isRecord(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function skillModalResultBadge(label, tone = "ok") {
  return `<span class="validation-badge ${escapeHtml(tone)}">${escapeHtml(label)}</span>`;
}

function skillModalResultMetaMarkup(items) {
  const chips = (items || []).map((item) => String(item || "").trim()).filter(Boolean);
  if (!chips.length) {
    return "";
  }
  return `<div class="skill-modal-result-meta">${chips.map((item) => `<span>${escapeHtml(item)}</span>`).join("")}</div>`;
}

function skillModalResultIssuesMarkup(issues) {
  const items = Array.isArray(issues) ? issues.filter((item) => isRecord(item)) : [];
  if (!items.length) {
    return "";
  }
  return `<div class="skill-modal-result-issues">${items
    .map((issue) => {
      const severity = String(issue.severity || "").trim().toLowerCase() === "error" ? "error" : "warn";
      const code = String(issue.code || issue.reason || "").trim();
      const message = String(issue.message || "").trim() || "\u672a\u63d0\u4f9b\u8be6\u60c5";
      const path = String(issue.path || issue.directory_path || "").trim();
      const title = code || (severity === "error" ? "\u9519\u8bef" : "\u63d0\u793a");
      return `
        <div class="skill-modal-result-issue ${severity}">
          <strong>${escapeHtml(title)}</strong>
          <span>${escapeHtml(message)}</span>
          ${path ? `<span>${escapeHtml(path)}</span>` : ""}
        </div>
      `;
    })
    .join("")}</div>`;
}

function skillModalResultSectionMarkup(title, body, extra = "") {
  if (!body) {
    return "";
  }
  return `
    <section class="skill-modal-result-section">
      <div class="skill-modal-result-section-head">
        <h4>${escapeHtml(title)}</h4>
        ${extra}
      </div>
      ${body}
    </section>
  `;
}

function renderSkillModalResultSkillCard(skill) {
  const metadata = isRecord(skill?.metadata) ? skill.metadata : {};
  const existingSkillName = String(skill?.existing_skill_name || "").trim();
  const existingSkillId = String(skill?.existing_skill_id || "").trim();
  const name =
    String(metadata.name || "").trim() ||
    fileNameFromPath(skill?.directory_path || skill?.skill_md_path || "") ||
    "Skill";
  const description = String(metadata.description || skill?.body_preview || "").trim();
  const isValid = Boolean(skill?.is_valid);
  const badgeTone = !isValid ? "error" : existingSkillId ? "warn" : "ok";
  const badgeLabel = !isValid ? "\u672a\u901a\u8fc7" : existingSkillId ? "\u53ef\u8986\u76d6" : "\u901a\u8fc7";
  const metaItems = [
    skill?.directory_path ? `\u76ee\u5f55 ${skill.directory_path}` : "",
    skill?.skill_md_path ? `SKILL.md ${fileNameFromPath(skill.skill_md_path)}` : "",
    Array.isArray(skill?.files) ? `\u6587\u4ef6 ${skill.files.length}` : "",
    Array.isArray(skill?.helper_files) && skill.helper_files.length ? `\u9644\u52a0 ${skill.helper_files.length}` : "",
    metadata.path ? `path ${metadata.path}` : "",
    existingSkillId ? `\u5df2\u5b58\u5728 ${existingSkillName || existingSkillId}` : "",
  ];
  return `
    <article class="skill-modal-result-card">
      <div class="skill-modal-result-card-head">
        <div>
          <strong>${escapeHtml(name)}</strong>
          ${skill?.directory_path ? `<span class="skill-modal-result-card-path">${escapeHtml(skill.directory_path)}</span>` : ""}
        </div>
        ${skillModalResultBadge(badgeLabel, badgeTone)}
      </div>
      ${description ? `<p class="skill-modal-result-card-description">${escapeHtml(description)}</p>` : ""}
      ${skillModalResultMetaMarkup(metaItems)}
      ${skillModalResultIssuesMarkup(skill?.issues)}
    </article>
  `;
}

function renderSkillModalImportedCard(item) {
  const skill = isRecord(item?.skill) ? item.skill : {};
  const name = String(skill.name || "").trim() || fileNameFromPath(item?.directory_path || skill.storage_path || "") || "Skill";
  const description = String(skill.description || "").trim();
  const badgeLabel = item?.updated ? "\u5df2\u8986\u76d6" : "\u5df2\u5bfc\u5165";
  const badgeTone = item?.updated ? "warn" : "ok";
  const groups = Array.isArray(skill?.groups) ? skill.groups.filter((group) => isRecord(group)) : [];
  const metaItems = [
    skill?.storage_path ? `\u5b58\u50a8 ${skill.storage_path}` : "",
    groups.length ? `\u5206\u7ec4 ${groups.length}` : "",
    item?.directory_path ? `\u6765\u6e90 ${item.directory_path}` : "",
  ];
  return `
    <article class="skill-modal-result-card">
      <div class="skill-modal-result-card-head">
        <div>
          <strong>${escapeHtml(name)}</strong>
          ${skill?.storage_path ? `<span class="skill-modal-result-card-path">${escapeHtml(skill.storage_path)}</span>` : ""}
        </div>
        ${skillModalResultBadge(badgeLabel, badgeTone)}
      </div>
      ${description ? `<p class="skill-modal-result-card-description">${escapeHtml(description)}</p>` : ""}
      ${skillModalResultMetaMarkup(metaItems)}
    </article>
  `;
}

function renderSkillModalSkippedCard(item) {
  const reason = String(item?.reason || "").trim();
  const reasonLabelMap = {
    "invalid-skill": "\u6821\u9a8c\u672a\u901a\u8fc7",
    "duplicate-skill-name": "\u540c\u540d Skill",
  };
  const title = String(item?.name || "").trim() || fileNameFromPath(item?.directory_path || "") || "\u5df2\u8df3\u8fc7";
  const description = reasonLabelMap[reason] || reason || "\u5df2\u8df3\u8fc7";
  return `
    <article class="skill-modal-result-card">
      <div class="skill-modal-result-card-head">
        <div>
          <strong>${escapeHtml(title)}</strong>
          ${item?.directory_path ? `<span class="skill-modal-result-card-path">${escapeHtml(item.directory_path)}</span>` : ""}
        </div>
        ${skillModalResultBadge("\u8df3\u8fc7", "warn")}
      </div>
      <p class="skill-modal-result-card-description">${escapeHtml(description)}</p>
      ${skillModalResultIssuesMarkup(item?.issues)}
    </article>
  `;
}

function renderSkillModalResult(value) {
  if (typeof value === "string") {
    return `
      <div class="skill-modal-result-shell">
        <div class="skill-modal-result-banner ok">
          <strong>${escapeHtml(value)}</strong>
        </div>
      </div>
    `;
  }
  if (!isRecord(value)) {
    return `
      <div class="skill-modal-result-shell">
        <pre class="skill-modal-result-json">${escapeHtml(prettyJson(value))}</pre>
      </div>
    `;
  }

  const payload = value;
  const scan = isRecord(payload.scan) ? payload.scan : payload;
  const tone =
    typeof payload.error === "string" && payload.error.trim()
      ? "error"
      : scan.valid === false || (Array.isArray(scan.skills) && scan.skills.some((item) => item?.is_valid === false))
        ? "warn"
        : "ok";
  const message =
    String(payload.error || "").trim() ||
    String(payload.detail || "").trim() ||
    String(payload.message || "").trim() ||
    (tone === "error" ? "\u64cd\u4f5c\u5931\u8d25" : "\u64cd\u4f5c\u5b8c\u6210");
  const bannerMeta = [
    scan?.source_path ? `\u6765\u6e90 ${scan.source_path}` : "",
    payload?.uploaded_file_count ? `\u4e0a\u4f20\u6587\u4ef6 ${payload.uploaded_file_count}` : "",
    scan?.recursive ? "\u9012\u5f52\u626b\u63cf" : "",
  ]
    .filter(Boolean)
    .join(" \u00b7 ");

  const stats = [
    payload?.imported_count != null ? { label: "\u5bfc\u5165", value: payload.imported_count } : null,
    payload?.skipped_count != null ? { label: "\u8df3\u8fc7", value: payload.skipped_count } : null,
    scan?.skill_count != null ? { label: "\u8bc6\u522b Skill", value: scan.skill_count } : null,
    scan?.valid_skill_count != null ? { label: "\u901a\u8fc7\u6821\u9a8c", value: scan.valid_skill_count } : null,
    Array.isArray(scan?.issues) && scan.issues.length ? { label: "\u5168\u5c40\u95ee\u9898", value: scan.issues.length } : null,
  ]
    .filter(Boolean)
    .map(
      (item) => `
        <div class="skill-modal-result-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(String(item.value))}</strong>
        </div>
      `,
    )
    .join("");

  const sections = [
    skillModalResultSectionMarkup(
      "\u5168\u5c40\u95ee\u9898",
      skillModalResultIssuesMarkup(scan?.issues),
      Array.isArray(scan?.issues) && scan.issues.length ? skillModalResultBadge(String(scan.issues.length), tone === "error" ? "error" : "warn") : "",
    ),
    skillModalResultSectionMarkup(
      "\u6821\u9a8c\u7ed3\u679c",
      Array.isArray(scan?.skills) && scan.skills.length
        ? `<div class="skill-modal-result-card-list">${scan.skills.map((item) => renderSkillModalResultSkillCard(item)).join("")}</div>`
        : "",
      Array.isArray(scan?.skills) && scan.skills.length ? skillModalResultBadge(String(scan.skills.length), "ok") : "",
    ),
    skillModalResultSectionMarkup(
      "\u5df2\u5bfc\u5165",
      Array.isArray(payload?.imported) && payload.imported.length
        ? `<div class="skill-modal-result-card-list">${payload.imported.map((item) => renderSkillModalImportedCard(item)).join("")}</div>`
        : "",
      Array.isArray(payload?.imported) && payload.imported.length ? skillModalResultBadge(String(payload.imported.length), "ok") : "",
    ),
    skillModalResultSectionMarkup(
      "\u5df2\u8df3\u8fc7",
      Array.isArray(payload?.skipped) && payload.skipped.length
        ? `<div class="skill-modal-result-card-list">${payload.skipped.map((item) => renderSkillModalSkippedCard(item)).join("")}</div>`
        : "",
      Array.isArray(payload?.skipped) && payload.skipped.length ? skillModalResultBadge(String(payload.skipped.length), "warn") : "",
    ),
  ]
    .filter(Boolean)
    .join("");

  const fallbackPayload = { ...payload };
  delete fallbackPayload.scan;
  delete fallbackPayload.skills;
  delete fallbackPayload.issues;
  delete fallbackPayload.message;
  delete fallbackPayload.detail;
  delete fallbackPayload.error;
  delete fallbackPayload.source_path;
  delete fallbackPayload.recursive;
  delete fallbackPayload.valid;
  delete fallbackPayload.skill_count;
  delete fallbackPayload.valid_skill_count;
  delete fallbackPayload.uploaded_file_count;
  delete fallbackPayload.imported_count;
  delete fallbackPayload.skipped_count;
  delete fallbackPayload.imported;
  delete fallbackPayload.skipped;
  const fallbackJson = !sections && Object.keys(fallbackPayload).length ? `<pre class="skill-modal-result-json">${escapeHtml(prettyJson(payload))}</pre>` : "";

  return `
    <div class="skill-modal-result-shell">
      <div class="skill-modal-result-banner ${escapeHtml(tone)}">
        <div class="skill-modal-result-banner-main">
          <strong>${escapeHtml(message)}</strong>
          ${skillModalResultBadge(tone === "error" ? "\u5931\u8d25" : tone === "warn" ? "\u9700\u5904\u7406" : "\u5b8c\u6210", tone)}
        </div>
        ${bannerMeta ? `<span>${escapeHtml(bannerMeta)}</span>` : ""}
      </div>
      ${stats ? `<div class="skill-modal-result-stats">${stats}</div>` : ""}
      ${sections}
      ${fallbackJson}
    </div>
  `;
}

function renderPluginModalResultPluginCard(item) {
  const manifest = isRecord(item?.manifest) ? item.manifest : {};
  const existingPluginName = String(item?.existing_plugin_name || "").trim();
  const existingPluginId = String(item?.existing_plugin_id || "").trim();
  const name = String(manifest.name || "").trim() || fileNameFromPath(item?.directory_path || item?.manifest_path || "") || "Plugin";
  const description = String(manifest.description || "").trim();
  const isValid = Boolean(item?.is_valid);
  const badgeTone = !isValid ? "error" : existingPluginId ? "warn" : "ok";
  const badgeLabel = !isValid ? "未通过" : existingPluginId ? "可覆盖" : "通过";
  const metaItems = [
    item?.directory_path ? `目录 ${item.directory_path}` : "",
    item?.manifest_path ? `plugin.yaml ${fileNameFromPath(item.manifest_path)}` : "",
    manifest.key ? `key ${manifest.key}` : "",
    Array.isArray(manifest?.actions) ? `动作 ${manifest.actions.length}` : "",
    Array.isArray(manifest?.tools) ? `工具 ${manifest.tools.length}` : "",
    manifest.hot_reload === false ? "不支持热加载" : manifest.key ? "支持热加载" : "",
    existingPluginId ? `已存在 ${existingPluginName || existingPluginId}` : "",
  ];
  return `
    <article class="skill-modal-result-card">
      <div class="skill-modal-result-card-head">
        <div>
          <strong>${escapeHtml(name)}</strong>
          ${item?.directory_path ? `<span class="skill-modal-result-card-path">${escapeHtml(item.directory_path)}</span>` : ""}
        </div>
        ${skillModalResultBadge(badgeLabel, badgeTone)}
      </div>
      ${description ? `<p class="skill-modal-result-card-description">${escapeHtml(description)}</p>` : ""}
      ${skillModalResultMetaMarkup(metaItems)}
      ${skillModalResultIssuesMarkup(item?.issues)}
    </article>
  `;
}

function renderPluginModalImportedCard(item) {
  const plugin = isRecord(item?.plugin) ? item.plugin : {};
  const manifest = isRecord(plugin?.manifest_json) ? plugin.manifest_json : {};
  const name = String(plugin.name || manifest.name || "").trim() || fileNameFromPath(item?.installed_path || item?.directory_path || "") || "Plugin";
  const description = String(plugin.description || manifest.description || "").trim();
  const badgeLabel = item?.updated ? "已覆盖" : "已导入";
  const badgeTone = item?.updated ? "warn" : "ok";
  const metaItems = [
    item?.installed_path ? `安装 ${item.installed_path}` : "",
    plugin?.key ? `key ${plugin.key}` : "",
    manifest.hot_reload === false ? "不支持热加载" : manifest.key ? "支持热加载" : "",
  ];
  return `
    <article class="skill-modal-result-card">
      <div class="skill-modal-result-card-head">
        <div>
          <strong>${escapeHtml(name)}</strong>
          ${item?.installed_path ? `<span class="skill-modal-result-card-path">${escapeHtml(item.installed_path)}</span>` : ""}
        </div>
        ${skillModalResultBadge(badgeLabel, badgeTone)}
      </div>
      ${description ? `<p class="skill-modal-result-card-description">${escapeHtml(description)}</p>` : ""}
      ${skillModalResultMetaMarkup(metaItems)}
    </article>
  `;
}

function renderPluginModalSkippedCard(item) {
  const reason = String(item?.reason || "").trim();
  const reasonLabelMap = {
    "invalid-plugin": "校验未通过",
    "duplicate-plugin-version": "同一批次存在重复插件",
  };
  const title = String(item?.name || "").trim() || fileNameFromPath(item?.directory_path || "") || "已跳过";
  const description = reasonLabelMap[reason] || reason || "已跳过";
  return `
    <article class="skill-modal-result-card">
      <div class="skill-modal-result-card-head">
        <div>
          <strong>${escapeHtml(title)}</strong>
          ${item?.directory_path ? `<span class="skill-modal-result-card-path">${escapeHtml(item.directory_path)}</span>` : ""}
        </div>
        ${skillModalResultBadge("跳过", "warn")}
      </div>
      <p class="skill-modal-result-card-description">${escapeHtml(description)}</p>
      ${skillModalResultIssuesMarkup(item?.issues)}
    </article>
  `;
}

function renderPluginBaseToolCard(item) {
  const toolName = String(item?.tool_name || "").trim() || "tool";
  const actionName = String(item?.action_name || "").trim();
  const description = String(item?.description || "").trim();
  const mode = String(item?.mode || "").trim() || "legacy";
  const argsSchema = isRecord(item?.args_schema) ? item.args_schema : null;
  const metaItems = [actionName ? `action ${actionName}` : "", mode ? `mode ${mode}` : ""];
  return `
    <article class="skill-modal-result-card">
      <div class="skill-modal-result-card-head">
        <div>
          <strong>${escapeHtml(toolName)}</strong>
        </div>
        ${skillModalResultBadge(mode === "structured" ? "Structured" : "Legacy", mode === "structured" ? "ok" : "warn")}
      </div>
      ${description ? `<p class="skill-modal-result-card-description">${escapeHtml(description)}</p>` : ""}
      ${skillModalResultMetaMarkup(metaItems)}
      ${argsSchema ? `<pre class="skill-modal-result-json">${escapeHtml(prettyJson(argsSchema))}</pre>` : ""}
    </article>
  `;
}

function renderPluginModalResult(value) {
  if (typeof value === "string") {
    return `
      <div class="skill-modal-result-shell">
        <div class="skill-modal-result-banner ok">
          <strong>${escapeHtml(value)}</strong>
        </div>
      </div>
    `;
  }
  if (!isRecord(value)) {
    return `
      <div class="skill-modal-result-shell">
        <pre class="skill-modal-result-json">${escapeHtml(prettyJson(value))}</pre>
      </div>
    `;
  }

  const payload = value;
  if (Array.isArray(payload.base_tools)) {
    const baseTools = payload.base_tools;
    const tone = typeof payload.error === "string" && payload.error.trim() ? "error" : baseTools.length ? "ok" : "warn";
    const pluginName = String(payload.plugin_name || payload.plugin_key || "Plugin").trim() || "Plugin";
    const message =
      String(payload.error || "").trim() ||
      String(payload.detail || "").trim() ||
      String(payload.message || "").trim() ||
      `${pluginName} exposes ${baseTools.length} BaseTool(s).`;
    const bannerMeta = [payload.plugin_key ? `key ${payload.plugin_key}` : "", payload.plugin_id ? `id ${payload.plugin_id}` : ""]
      .filter(Boolean)
      .join(" 路 ");
    const stats = [
      { label: "BaseTool", value: payload.tool_count != null ? payload.tool_count : baseTools.length },
      { label: "Structured", value: baseTools.filter((item) => item?.mode === "structured").length },
    ]
      .map(
        (item) => `
          <div class="skill-modal-result-stat">
            <span>${escapeHtml(item.label)}</span>
            <strong>${escapeHtml(String(item.value))}</strong>
          </div>
        `,
      )
      .join("");
    const sections = skillModalResultSectionMarkup(
      "BaseTool Preview",
      baseTools.length
        ? `<div class="skill-modal-result-card-list">${baseTools.map((item) => renderPluginBaseToolCard(item)).join("")}</div>`
        : '<div class="detail empty compact-detail">This plugin does not expose any BaseTool.</div>',
      skillModalResultBadge(String(baseTools.length), tone === "error" ? "error" : baseTools.length ? "ok" : "warn"),
    );
    return `
      <div class="skill-modal-result-shell">
        <div class="skill-modal-result-banner ${escapeHtml(tone)}">
          <div class="skill-modal-result-banner-main">
            <strong>${escapeHtml(message)}</strong>
            ${skillModalResultBadge(tone === "error" ? "Failed" : baseTools.length ? "Ready" : "Empty", tone)}
          </div>
          ${bannerMeta ? `<span>${escapeHtml(bannerMeta)}</span>` : ""}
        </div>
        <div class="skill-modal-result-stats">${stats}</div>
        ${sections}
      </div>
    `;
  }
  const scan = isRecord(payload.scan) ? payload.scan : payload;
  const tone =
    typeof payload.error === "string" && payload.error.trim()
      ? "error"
      : scan.valid === false || (Array.isArray(scan.plugins) && scan.plugins.some((item) => item?.is_valid === false))
        ? "warn"
        : "ok";
  const message =
    String(payload.error || "").trim() ||
    String(payload.detail || "").trim() ||
    String(payload.message || "").trim() ||
    (tone === "error" ? "操作失败" : "操作完成");
  const bannerMeta = [
    payload?.source_name ? `来源 ${payload.source_name}` : "",
    scan?.source_kind ? `模式 ${scan.source_kind}` : "",
    payload?.uploaded_file_count ? `上传文件 ${payload.uploaded_file_count}` : "",
  ]
    .filter(Boolean)
    .join(" · ");
  const stats = [
    payload?.imported_count != null ? { label: "导入", value: payload.imported_count } : null,
    payload?.skipped_count != null ? { label: "跳过", value: payload.skipped_count } : null,
    scan?.plugin_count != null ? { label: "识别插件", value: scan.plugin_count } : null,
    scan?.valid_plugin_count != null ? { label: "通过校验", value: scan.valid_plugin_count } : null,
    Array.isArray(scan?.issues) && scan.issues.length ? { label: "全局问题", value: scan.issues.length } : null,
  ]
    .filter(Boolean)
    .map(
      (item) => `
        <div class="skill-modal-result-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(String(item.value))}</strong>
        </div>
      `,
    )
    .join("");

  const sections = [
    skillModalResultSectionMarkup(
      "全局问题",
      skillModalResultIssuesMarkup(scan?.issues),
      Array.isArray(scan?.issues) && scan.issues.length ? skillModalResultBadge(String(scan.issues.length), tone === "error" ? "error" : "warn") : "",
    ),
    skillModalResultSectionMarkup(
      "校验结果",
      Array.isArray(scan?.plugins) && scan.plugins.length
        ? `<div class="skill-modal-result-card-list">${scan.plugins.map((item) => renderPluginModalResultPluginCard(item)).join("")}</div>`
        : "",
      Array.isArray(scan?.plugins) && scan.plugins.length ? skillModalResultBadge(String(scan.plugins.length), "ok") : "",
    ),
    skillModalResultSectionMarkup(
      "已导入",
      Array.isArray(payload?.imported) && payload.imported.length
        ? `<div class="skill-modal-result-card-list">${payload.imported.map((item) => renderPluginModalImportedCard(item)).join("")}</div>`
        : "",
      Array.isArray(payload?.imported) && payload.imported.length ? skillModalResultBadge(String(payload.imported.length), "ok") : "",
    ),
    skillModalResultSectionMarkup(
      "已跳过",
      Array.isArray(payload?.skipped) && payload.skipped.length
        ? `<div class="skill-modal-result-card-list">${payload.skipped.map((item) => renderPluginModalSkippedCard(item)).join("")}</div>`
        : "",
      Array.isArray(payload?.skipped) && payload.skipped.length ? skillModalResultBadge(String(payload.skipped.length), "warn") : "",
    ),
  ]
    .filter(Boolean)
    .join("");

  const fallbackPayload = { ...payload };
  delete fallbackPayload.scan;
  delete fallbackPayload.plugins;
  delete fallbackPayload.issues;
  delete fallbackPayload.message;
  delete fallbackPayload.detail;
  delete fallbackPayload.error;
  delete fallbackPayload.source_path;
  delete fallbackPayload.source_kind;
  delete fallbackPayload.valid;
  delete fallbackPayload.plugin_count;
  delete fallbackPayload.valid_plugin_count;
  delete fallbackPayload.uploaded_file_count;
  delete fallbackPayload.source_name;
  delete fallbackPayload.imported_count;
  delete fallbackPayload.skipped_count;
  delete fallbackPayload.imported;
  delete fallbackPayload.skipped;
  const fallbackJson = !sections && Object.keys(fallbackPayload).length ? `<pre class="skill-modal-result-json">${escapeHtml(prettyJson(payload))}</pre>` : "";

  return `
    <div class="skill-modal-result-shell">
      <div class="skill-modal-result-banner ${escapeHtml(tone)}">
        <div class="skill-modal-result-banner-main">
          <strong>${escapeHtml(message)}</strong>
          ${skillModalResultBadge(tone === "error" ? "失败" : tone === "warn" ? "需处理" : "完成", tone)}
        </div>
        ${bannerMeta ? `<span>${escapeHtml(bannerMeta)}</span>` : ""}
      </div>
      ${stats ? `<div class="skill-modal-result-stats">${stats}</div>` : ""}
      ${sections}
      ${fallbackJson}
    </div>
  `;
}

function clearTeamChatResultTimer() {
  if (state.teamChat.resultTimer) {
    window.clearTimeout(state.teamChat.resultTimer);
    state.teamChat.resultTimer = 0;
  }
}

function teamChatResultPayload(value) {
  if (typeof value === "string") {
    return { message: value };
  }
  return isRecord(value) ? value : { message: String(value ?? "").trim() };
}

function teamChatResultTone(value) {
  const payload = teamChatResultPayload(value);
  if (typeof payload.error === "string" && payload.error.trim()) {
    return "error";
  }
  if (
    (typeof payload.detail === "string" && payload.detail.trim() && !String(payload.message || "").trim()) ||
    (Array.isArray(payload.errors) && payload.errors.length)
  ) {
    return "error";
  }
  if (typeof payload.detail === "string" && payload.detail.trim()) {
    return "warn";
  }
  return "ok";
}

function renderTeamChatResult(value) {
  const payload = teamChatResultPayload(value);
  const tone = teamChatResultTone(payload);
  const title =
    String(payload.error || "").trim() ||
    String(payload.message || "").trim() ||
    String(payload.detail || "").trim() ||
    "\u64cd\u4f5c\u5b8c\u6210";
  const meta = [
    payload.thread_id ? `thread_id ${payload.thread_id}` : "",
    payload.run_id ? `run ${payload.run_id}` : "",
    payload.status ? `status ${payload.status}` : "",
    payload.id ? `id ${payload.id}` : "",
  ]
    .filter(Boolean)
    .map((item) => `<span>${escapeHtml(item)}</span>`)
    .join("");
  return `
    <div class="team-chat-result-shell ${escapeHtml(tone)}">
      <div class="team-chat-result-main">
        <div class="team-chat-result-title">${escapeHtml(title)}</div>
        ${meta ? `<div class="team-chat-result-meta">${meta}</div>` : ""}
      </div>
      <button type="button" class="team-chat-result-close" data-team-chat-result-close aria-label="\u5173\u95ed">
        <span aria-hidden="true">\u00d7</span>
      </button>
    </div>
  `;
}

function showTeamChatResult(value) {
  if (!teamChatResult) {
    return;
  }
  clearTeamChatResultTimer();
  teamChatResult.innerHTML = renderTeamChatResult(value);
  const tone = teamChatResultTone(value);
  if (tone === "error") {
    return;
  }
  state.teamChat.resultTimer = window.setTimeout(() => {
    state.teamChat.resultTimer = 0;
    hideResult(teamChatResult);
  }, 10000);
}

function showResult(target, value) {
  if (!target) {
    return;
  }
  target.classList.remove("hidden");
  target.classList.remove("pending");
  if (target === teamChatResult || target.dataset.resultFormat === "team-chat-inline") {
    showTeamChatResult(value);
    return;
  }
  if (target.dataset.resultFormat === "skill-modal") {
    target.innerHTML = renderSkillModalResult(value);
    return;
  }
  if (target.dataset.resultFormat === "plugin-modal" || (isRecord(value) && Array.isArray(value.base_tools))) {
    target.innerHTML = renderPluginModalResult(value);
    return;
  }
  target.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

function showPendingResult(target, message = "处理中，请等待...") {
  if (!target) {
    return;
  }
  target.classList.remove("hidden");
  target.classList.add("pending");
  target.textContent = String(message || "处理中，请等待...");
}

function clearKnowledgeEmbeddingJobPolling() {
  state.knowledgeBaseEmbeddingJobId = null;
  state.knowledgeBaseEmbeddingJobPollVersion = Number(state.knowledgeBaseEmbeddingJobPollVersion || 0) + 1;
}

function renderKnowledgeEmbeddingJobProgress(job) {
  const percent = Math.max(0, Math.min(100, Number(job?.progress_percent || 0)));
  const message = String(job?.message || "处理中，请等待...").trim() || "处理中，请等待...";
  const currentTitle = String(job?.current_document_title || "").trim();
  const totalDocuments = Math.max(0, Number(job?.total_documents || 0));
  const processedDocuments = Math.max(0, Number(job?.processed_documents || 0));
  const completedDocuments = Math.max(0, Number(job?.completed_documents || 0));
  const failedDocuments = Math.max(0, Number(job?.failed_documents || 0));
  const totalChunks = Math.max(0, Number(job?.total_chunks_estimated || 0));
  const embeddedChunks = Math.max(0, Number(job?.embedded_chunks_completed || 0));
  const details = [
    `进度 ${percent.toFixed(1)}%`,
    `文档 ${processedDocuments}/${totalDocuments || 0}`,
    totalChunks ? `Chunk ${embeddedChunks}/${totalChunks}` : "",
    failedDocuments ? `失败 ${failedDocuments}` : "",
    completedDocuments ? `完成 ${completedDocuments}` : "",
  ]
    .filter(Boolean)
    .join(" · ");
  return `
    <div class="knowledge-embedding-progress">
      <div class="knowledge-embedding-progress-head">
        <strong>${escapeHtml(message)}</strong>
        <span>${escapeHtml(details)}</span>
      </div>
      <div class="knowledge-embedding-progress-bar" aria-hidden="true">
        <span style="width:${percent.toFixed(1)}%"></span>
      </div>
      ${currentTitle ? `<div class="knowledge-embedding-progress-current">${escapeHtml(currentTitle)}</div>` : ""}
    </div>
  `;
}

function showKnowledgeEmbeddingJobProgress(target, job) {
  if (!target) {
    return;
  }
  target.classList.remove("hidden");
  target.classList.add("pending");
  target.innerHTML = renderKnowledgeEmbeddingJobProgress(job || {});
}

async function waitForKnowledgeEmbeddingJob(jobRef) {
  const jobId = String(jobRef?.job?.id || jobRef?.id || "").trim();
  if (!jobId) {
    throw new Error("知识库嵌入任务创建失败。");
  }
  const pollVersion = Number(state.knowledgeBaseEmbeddingJobPollVersion || 0) + 1;
  state.knowledgeBaseEmbeddingJobId = jobId;
  state.knowledgeBaseEmbeddingJobPollVersion = pollVersion;
  let latest = jobRef?.job || jobRef || null;
  if (latest) {
    showKnowledgeEmbeddingJobProgress(knowledgeBaseModalResult, latest);
  }
  while (state.knowledgeBaseEmbeddingJobPollVersion === pollVersion) {
    latest = await api(`/api/agent-center/knowledge-embedding-jobs/${encodeURIComponent(jobId)}`);
    showKnowledgeEmbeddingJobProgress(knowledgeBaseModalResult, latest);
    if (latest.status === "completed") {
      if (state.knowledgeBaseEmbeddingJobPollVersion === pollVersion) {
        clearKnowledgeEmbeddingJobPolling();
      }
      return latest;
    }
    if (latest.status === "error") {
      if (state.knowledgeBaseEmbeddingJobPollVersion === pollVersion) {
        clearKnowledgeEmbeddingJobPolling();
      }
      const error = new Error(latest.error || latest.message || "知识库嵌入任务失败。");
      error.payload = { detail: latest.error || latest.message || "知识库嵌入任务失败。" };
      error.job = latest;
      throw error;
    }
    await new Promise((resolve) => window.setTimeout(resolve, 800));
  }
  return latest;
}

function hideResult(target) {
  if (!target) {
    return;
  }
  if (target === teamChatResult || target.dataset.resultFormat === "team-chat-inline") {
    clearTeamChatResultTimer();
  }
  target.classList.add("hidden");
  target.classList.remove("pending");
  target.innerHTML = "";
  target.textContent = "";
}

async function switchPage(pageName, options = {}) {
  if (pageName === "static-memories" || pageName === "role-management" || pageName === "team-rules") {
    pageName = "responsibility-specs";
  }
  if (pageName !== "team-chat") {
    stopTeamChatRunPolling();
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
    if (pageName === "team-chat") {
      requestAnimationFrame(() => {
        syncTeamChatViewportHeight();
        scrollTeamChatToBottom();
      });
    }
  } catch (error) {
    showResult(taskResult, errorResult(error));
  }
}

function navSectionEntryPage(sectionName) {
  const panel = subMenuPanels.find((item) => item.dataset.navPanel === sectionName);
  const firstButton = panel?.querySelector("[data-page-target]");
  if (firstButton?.dataset.pageTarget) {
    return firstButton.dataset.pageTarget;
  }
  const fallbackButton = navButtons.find((button) => PAGE_SECTIONS[button.dataset.pageTarget] === sectionName);
  return fallbackButton?.dataset.pageTarget || "overview";
}

async function switchNavSection(sectionName) {
  await switchPage(navSectionEntryPage(sectionName));
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

function pluginDisplayName(item) {
  return item?.name || "未命名插件";
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

function pluginEditorRecord() {
  return state.pluginEditor?.record && typeof state.pluginEditor.record === "object" ? state.pluginEditor.record : null;
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

function populateProviderTypeOptions(selectedType = "") {
  const normalizedSelectedType = String(selectedType || providerType.value || "").trim();
  const options = state.providerTypes.map((item) => `<option value="${escapeHtml(item.provider_type)}">${escapeHtml(item.label)}</option>`).join("");
  providerType.innerHTML = options;
  if (normalizedSelectedType) {
    providerType.value = normalizedSelectedType;
  }
  if (!providerType.value && state.providerTypes.length) {
    providerType.value = state.providerTypes[0].provider_type;
  }
}

function populateProviderOptions() {
  const options = state.providers.map((item) => `<option value="${item.id}">${escapeHtml(item.name)} / ${escapeHtml(item.provider_type)}</option>`).join("");
  if (agentTemplateProvider) {
    agentTemplateProvider.innerHTML = options || '<option value="">暂无 Provider</option>';
  }
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

function localModelsByType(modelType) {
  return state.localModels.filter((item) => item.model_type === modelType);
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

function retrievalLocalModelOptions(kind, settings = {}, currentValue = "") {
  const selectedSettings = settings && typeof settings === "object" ? settings : {};
  const modelType = kind === "rerank" ? "Rerank" : "Embed";
  const defaultModelName = kind === "rerank" ? DEFAULT_LOCAL_RERANK_MODEL : DEFAULT_LOCAL_EMBEDDING_MODEL;
  const records = localModelsByType(modelType);
  const items = records.map((item) => ({
    value: item.id,
    label: item.name || item.path_display || item.model_path || item.id,
  }));
  let selectedValue = "";
  let usesFallbackDefault = false;
  let usesLegacyManual = false;
  if (selectedSettings.local_model_id && records.some((item) => item.id === selectedSettings.local_model_id)) {
    selectedValue = selectedSettings.local_model_id;
  } else if (selectedSettings.model_name) {
    const matched = records.find((item) =>
      [item.model_path, item.path_display, item.resolved_path].some((value) => value && value === selectedSettings.model_name),
    );
    if (matched) {
      selectedValue = matched.id;
    } else {
      const manualValue = `manual:${selectedSettings.model_name}`;
      items.unshift({
        value: manualValue,
        label: selectedSettings.model_label || selectedSettings.model_name,
      });
      selectedValue = manualValue;
      usesLegacyManual = true;
    }
  }
  if (!items.length) {
    const defaultValue = `manual:${defaultModelName}`;
    items.push({
      value: defaultValue,
      label: defaultModelName,
    });
    if (!selectedValue) {
      selectedValue = defaultValue;
    }
    usesFallbackDefault = true;
  }
  if (currentValue && items.some((item) => item.value === currentValue)) {
    selectedValue = currentValue;
  }
  if (!selectedValue) {
    selectedValue = items[0]?.value || "";
  }
  return {
    items,
    selectedValue,
    hasManaged: records.length > 0,
    managedCount: records.length,
    usesFallbackDefault,
    usesLegacyManual,
  };
}

function parseRetrievalLocalModelSelection(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return { local_model_id: "", model_name: "" };
  }
  if (raw.startsWith("manual:")) {
    return {
      local_model_id: "",
      model_name: raw.slice("manual:".length),
    };
  }
  return {
    local_model_id: raw,
    model_name: "",
  };
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

function normalizeSkillGroupRecord(item) {
  if (!item || typeof item !== "object") {
    return null;
  }
  const id = String(item.id || "").trim();
  const key = String(item.key || "").trim();
  const name = String(item.name || key).trim();
  if (!id && !key && !name) {
    return null;
  }
  return {
    id,
    key: key || name,
    name: name || key,
  };
}

function skillGroupsForItem(item) {
  const rawGroups = Array.isArray(item?.groups) ? item.groups : [];
  const groups = [];
  const seen = new Set();
  rawGroups.forEach((entry) => {
    const group = normalizeSkillGroupRecord(entry);
    if (!group) {
      return;
    }
    const token = group.id || group.key;
    if (!token || seen.has(token)) {
      return;
    }
    seen.add(token);
    groups.push(group);
  });
  return groups;
}

function skillOptionLabel(item) {
  const groups = skillGroupsForItem(item);
  const groupSummary = groups.length ? groups.map((entry) => entry.name || entry.key || "-").join("、") : "未分组";
  const description = String(item?.description || "").trim();
  return description
    ? `${item?.name || item?.id || "-"} / ${groupSummary} / ${description}`
    : `${item?.name || item?.id || "-"} / ${groupSummary}`;
}

function skillIdsForGroup(group) {
  const groupId = String(group?.id || "").trim();
  const groupKey = String(group?.key || "").trim();
  if (!groupId && !groupKey) {
    return [];
  }
  return state.skills
    .filter((item) => {
      const groups = skillGroupsForItem(item);
      return groups.some((entry) => (groupId && String(entry.id || "") === groupId) || (groupKey && String(entry.key || "") === groupKey));
    })
    .map((item) => String(item.id || "").trim())
    .filter(Boolean);
}

function renderSkillGroupSkillOptions(selectedValues = null) {
  if (!skillGroupSkills) {
    return;
  }
  renderMultiSelect(
    skillGroupSkills,
    state.skills.map((item) => ({ value: item.id, label: skillOptionLabel(item) })),
  );
  if (Array.isArray(selectedValues)) {
    setMultiSelectValues(skillGroupSkills, selectedValues.map((value) => String(value || "").trim()).filter(Boolean));
  }
}

function skillGroupCatalogOptions() {
  return [
    ...state.skillGroupCatalog.map((item) => ({
      value: item.id,
      label: `${item.name || item.key || item.id} / ${item.key || item.id}`,
    })),
  ];
}

function renderSkillImportGroupOptions(selectedValues = null) {
  if (!skillImportGroups) {
    return;
  }
  const options = skillGroupCatalogOptions();
  const selected = Array.isArray(selectedValues)
    ? selectedValues.map((value) => String(value || "").trim()).filter(Boolean)
    : getMultiSelectValues(skillImportGroups).map((value) => String(value || "").trim()).filter(Boolean);
  const optionValues = new Set(options.map((item) => item.value));
  renderMultiSelect(skillImportGroups, options);
  setMultiSelectValues(
    skillImportGroups,
    selected.filter((value) => optionValues.has(value)),
  );
}

function populatePluginOptions() {
  renderMultiSelect(
    agentTemplatePlugins,
    state.plugins.map((item) => ({ value: item.id, label: `${item.name || item.key || item.id} / ${item.version || "-"}` })),
  );
}

function reviewPolicyDecisionTypeLabel(value) {
  const normalized = String(value || "").trim();
  if (normalized === "approve") {
    return "批准";
  }
  if (normalized === "reject") {
    return "拒绝";
  }
  if (normalized === "edit") {
    return "编辑";
  }
  return normalized;
}

function reviewPolicyDecisionTypeOptions() {
  const options = uiMetadataOptions("review_policy", "decision_types");
  const normalizedOptions = (options.length
    ? options
    : [{ value: "approve" }, { value: "reject" }, { value: "edit" }])
    .map((item) => {
      const value = String(item?.value || "").trim();
      if (!value) {
        return null;
      }
      return {
        value,
        label: reviewPolicyDecisionTypeLabel(value),
      };
    })
    .filter(Boolean);
  return normalizedOptions;
}

function reviewPolicyDecisionTypeValues() {
  return reviewPolicyDecisionTypeOptions()
    .map((item) => String(item.value || "").trim())
    .filter(Boolean);
}

function reviewPolicyDefaultDecisionValues() {
  return Array.from(new Set(reviewPolicyDecisionTypeValues()));
}

function normalizeReviewPolicyRule(rule = {}) {
  const pluginKey = String(rule?.plugin_key || rule?.pluginKey || "").trim();
  const action = String(rule?.action || rule?.action_name || rule?.actionName || "").trim();
  const allowedDecisionSet = new Set(reviewPolicyDecisionTypeValues());
  const allowedDecisions = Array.from(
    new Set((Array.isArray(rule?.allowed_decisions) ? rule.allowed_decisions : []).map((value) => String(value || "").trim()).filter(Boolean)),
  ).filter((value) => allowedDecisionSet.has(value));
  return {
    plugin_key: pluginKey,
    action: pluginKey ? action || "*" : "",
    allowed_decisions: allowedDecisions,
  };
}

function reviewPolicyRulesValue() {
  try {
    const parsed = safeParseJson(reviewPolicyRules?.value, []);
    return Array.isArray(parsed)
      ? parsed.filter((item) => item && typeof item === "object").map((item) => normalizeReviewPolicyRule(item))
      : [];
  } catch (error) {
    return [];
  }
}

function reviewPolicyPluginCatalog() {
  const items = [];
  const seen = new Set();
  state.plugins.forEach((item) => {
    const pluginKey = String(item?.key || "").trim();
    if (!pluginKey || seen.has(pluginKey)) {
      return;
    }
    seen.add(pluginKey);
    items.push(item);
  });
  return items.sort((left, right) => {
    const leftLabel = String(left?.name || left?.key || "").trim();
    const rightLabel = String(right?.name || right?.key || "").trim();
    return leftLabel.localeCompare(rightLabel, "zh-CN");
  });
}

function reviewPolicyPluginLabel(pluginKey) {
  const normalized = String(pluginKey || "").trim();
  if (!normalized) {
    return "";
  }
  const record = reviewPolicyPluginCatalog().find((item) => String(item?.key || "").trim() === normalized);
  return String(record?.name || record?.key || normalized).trim() || normalized;
}

function reviewPolicyPluginOptions() {
  return reviewPolicyPluginCatalog().map((item) => {
    const pluginKey = String(item?.key || "").trim();
    return {
      value: pluginKey,
      label: String(item?.name || pluginKey).trim() || pluginKey,
    };
  });
}

function reviewPolicyActionOptions(pluginKey) {
  const normalizedPluginKey = String(pluginKey || "").trim();
  if (!normalizedPluginKey) {
    return [];
  }
  const options = [];
  const seen = new Set();
  const pushOption = (value, label) => {
    const normalizedValue = String(value || "").trim();
    if (!normalizedValue || seen.has(normalizedValue)) {
      return;
    }
    seen.add(normalizedValue);
    options.push({ value: normalizedValue, label });
  };
  pushOption("*", "\u5168\u90e8\u52a8\u4f5c");
  reviewPolicyPluginCatalog()
    .filter((plugin) => String(plugin?.key || "").trim() === normalizedPluginKey)
    .forEach((plugin) => {
      const manifest = isRecord(plugin?.manifest_json) ? plugin.manifest_json : isRecord(plugin?.manifest) ? plugin.manifest : isRecord(plugin?.descriptor) ? plugin.descriptor : {};
      const actions = Array.isArray(manifest?.actions) ? manifest.actions : [];
      actions.forEach((action) => {
        const actionName = String(action?.name || "").trim();
        if (!actionName) {
          return;
        }
        pushOption(actionName, actionName);
      });
    });
  return options.sort((left, right) => {
    if (left.value === "*") {
      return -1;
    }
    if (right.value === "*") {
      return 1;
    }
    return left.label.localeCompare(right.label, "zh-CN");
  });
}

function reviewPolicyRulesFromSpec(spec = {}) {
  const directRules = Array.isArray(spec?.rules)
    ? spec.rules.map((item) => normalizeReviewPolicyRule(item)).filter((item) => item.plugin_key)
    : [];
  if (directRules.length) {
    return directRules;
  }
  const fallbackDecisions = Array.from(
    new Set((spec?.allowed_decisions || []).map((value) => String(value || "").trim()).filter(Boolean)),
  );
  const decisions = fallbackDecisions.length ? fallbackDecisions : reviewPolicyDefaultDecisionValues();
  const conditions = dictOrEmpty(spec.conditions);
  return (conditions.plugin_actions || [])
    .map((item) =>
      normalizeReviewPolicyRule({
        plugin_key: item?.plugin_key,
        action: item?.action,
        allowed_decisions: decisions,
      }),
    )
    .filter((item) => item.plugin_key);
}

function setReviewPolicyRules(rules) {
  if (!reviewPolicyRules) {
    return;
  }
  reviewPolicyRules.value = prettyJson((rules || []).map((item) => normalizeReviewPolicyRule(item)));
  renderReviewPolicyRules();
}

function mutateReviewPolicyRules(mutator) {
  const rules = reviewPolicyRulesValue().map((item) => ({
    ...item,
    allowed_decisions: [...(item.allowed_decisions || [])],
  }));
  mutator(rules);
  setReviewPolicyRules(rules);
}

function reviewPolicyRuleDecisionSet(rule) {
  return new Set((rule?.allowed_decisions || []).map((value) => String(value || "").trim()).filter(Boolean));
}

function reviewPolicyRuleCardMarkup(rule, index) {
  const normalizedRule = normalizeReviewPolicyRule(rule);
  const pluginOptions = reviewPolicyPluginOptions();
  const actionOptions = reviewPolicyActionOptions(normalizedRule.plugin_key);
  if (normalizedRule.action && !actionOptions.some((item) => item.value === normalizedRule.action)) {
    actionOptions.push({ value: normalizedRule.action, label: normalizedRule.action });
  }
  const selectedAction = actionOptions.some((item) => item.value === normalizedRule.action)
    ? normalizedRule.action
    : actionOptions[0]?.value || "";
  const decisionOptions = reviewPolicyDecisionTypeOptions();
  const decisionValues = reviewPolicyDefaultDecisionValues();
  const selectedDecisionSet = reviewPolicyRuleDecisionSet(normalizedRule);
  const allSelected = decisionValues.length > 0 && decisionValues.every((value) => selectedDecisionSet.has(value));
  const decisionMarkup = decisionOptions
    .map((item) => {
      const value = String(item.value || "").trim();
      return `
        <label class="toggle-chip">
          <input type="checkbox" data-review-policy-rule-decision="${escapeAttribute(value)}" data-rule-index="${index}" ${selectedDecisionSet.has(value) ? "checked" : ""} />
          <span>${escapeHtml(item.label || value)}</span>
        </label>
      `;
    })
    .join("");
  return `
    <article class="member-card review-policy-rule-card">
      <div class="member-card-head">
        <strong>${escapeHtml(`\u7b56\u7565 ${index + 1}`)}</strong>
        <div class="member-toolbar-actions">
          <button type="button" class="ghost" data-review-policy-rule-remove="${index}">\u79fb\u9664</button>
        </div>
      </div>
      <div class="form-grid two">
        <label>
          <span>\u63d2\u4ef6</span>
          <select data-review-policy-rule-field="plugin_key" data-rule-index="${index}">
            ${teamDefinitionSelectOptionsMarkup(pluginOptions, normalizedRule.plugin_key, {
              allowBlank: true,
              blankLabel: "\u8bf7\u9009\u62e9\u63d2\u4ef6",
              fallbackLabel: "\u6682\u65e0\u53ef\u7528\u63d2\u4ef6",
            })}
          </select>
        </label>
        <label>
          <span>\u63d2\u4ef6\u52a8\u4f5c</span>
          <select data-review-policy-rule-field="action" data-rule-index="${index}" ${normalizedRule.plugin_key ? "" : "disabled"}>
            ${teamDefinitionSelectOptionsMarkup(actionOptions, selectedAction, {
              fallbackLabel: "\u8bf7\u5148\u9009\u62e9\u63d2\u4ef6",
            })}
          </select>
        </label>
      </div>
      <div class="review-policy-rule-decisions">
        <div class="member-card-head">
          <strong>\u51b3\u7b56\u7c7b\u578b</strong>
          <div class="member-toolbar-actions">
            <button type="button" class="ghost" data-review-policy-rule-toggle-all="${index}">
              ${allSelected ? "\u53d6\u6d88\u5168\u9009" : "\u5168\u9009"}
            </button>
          </div>
        </div>
        <div class="review-policy-rule-decision-list">${decisionMarkup}</div>
      </div>
    </article>
  `;
}

function renderReviewPolicyRules() {
  if (!reviewPolicyRuleList) {
    return;
  }
  const rules = reviewPolicyRulesValue();
  reviewPolicyRuleList.innerHTML = rules.length
    ? rules.map((rule, index) => reviewPolicyRuleCardMarkup(rule, index)).join("")
    : '<div class="detail empty compact-detail"><strong>\u6682\u65e0\u7b56\u7565</strong><p>\u8bf7\u5148\u6dfb\u52a0\u4e00\u6761\u63d2\u4ef6\u5ba1\u6838\u89c4\u5219\u3002</p></div>';
}

function addReviewPolicyRule() {
  mutateReviewPolicyRules((rules) => {
    const firstPlugin = reviewPolicyPluginOptions()[0]?.value || "";
    const firstAction = reviewPolicyActionOptions(firstPlugin)[0]?.value || (firstPlugin ? "*" : "");
    rules.push({
      plugin_key: firstPlugin,
      action: firstAction,
      allowed_decisions: reviewPolicyDefaultDecisionValues(),
    });
  });
}

function removeReviewPolicyRule(index) {
  mutateReviewPolicyRules((rules) => {
    rules.splice(index, 1);
  });
}

function updateReviewPolicyRuleField(index, field, value) {
  mutateReviewPolicyRules((rules) => {
    const rule = rules[index];
    if (!rule) {
      return;
    }
    if (field === "plugin_key") {
      rule.plugin_key = String(value || "").trim();
      const actionOptions = reviewPolicyActionOptions(rule.plugin_key);
      rule.action = actionOptions.some((item) => item.value === rule.action) ? rule.action : actionOptions[0]?.value || (rule.plugin_key ? "*" : "");
      return;
    }
    if (field === "action") {
      rule.action = String(value || "").trim() || (rule.plugin_key ? "*" : "");
    }
  });
}

function toggleReviewPolicyRuleDecision(index, decisionValue, checked) {
  mutateReviewPolicyRules((rules) => {
    const rule = rules[index];
    if (!rule) {
      return;
    }
    const normalizedValue = String(decisionValue || "").trim();
    const selected = new Set((rule.allowed_decisions || []).map((value) => String(value || "").trim()).filter(Boolean));
    if (checked) {
      selected.add(normalizedValue);
    } else {
      selected.delete(normalizedValue);
    }
    rule.allowed_decisions = Array.from(selected);
  });
}

function toggleReviewPolicyRuleAllDecisions(index) {
  mutateReviewPolicyRules((rules) => {
    const rule = rules[index];
    if (!rule) {
      return;
    }
    const decisionValues = reviewPolicyDefaultDecisionValues();
    const selected = new Set((rule.allowed_decisions || []).map((value) => String(value || "").trim()).filter(Boolean));
    const allSelected = decisionValues.length > 0 && decisionValues.every((value) => selected.has(value));
    rule.allowed_decisions = allSelected ? [] : decisionValues;
  });
}

function populateTeamDefinitionReviewPolicyOptions() {
  renderMultiSelect(
    teamDefinitionReviewPolicies,
    state.reviewPolicies.map((item) => ({ value: item.id, label: `${item.name || item.id}` })),
  );
}

function populateAgentTemplateReferenceOptions() {
  renderProviderSelect(agentTemplateProvider, state.providers, agentTemplateProvider?.value || "");
  renderModelSelect(agentTemplateModel, providerModelsByType(agentTemplateProvider?.value || "", "chat"), agentTemplateModel?.value || "");
  renderMultiSelect(
    agentTemplateSkills,
    state.skills.map((item) => ({ value: item.id, label: skillOptionLabel(item) })),
  );
  populatePluginOptions();
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
    state.skills.map((item) => ({ value: item.id, label: skillOptionLabel(item) })),
  );
  renderSingleSelect(
    agentDefinitionKnowledgeBases,
    state.knowledgeBases.map((item) => ({ value: item.id, label: `${item.name || item.id}` })),
    agentDefinitionKnowledgeBases?.value || "",
    "暂无知识库",
    { allowBlank: true, blankLabel: "不选择" },
  );
  renderMultiSelect(
    agentDefinitionReviewPolicies,
    state.reviewPolicies.map((item) => ({ value: item.id, label: `${item.name || item.id}` })),
  );
}

const TEAM_DEFINITION_CHILD_SOURCE_KIND_OPTIONS = [
  { value: "agent_definition", label: "Agent" },
  { value: "team_definition", label: "团队" },
];

function teamDefinitionSourceKind(value) {
  return String(value || "").trim() === "team_definition" ? "team_definition" : "agent_definition";
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
    state.agentDefinitions.find((item) => item.id === raw) ||
    state.agentDefinitions.find((item) => item.name === raw) ||
    null;
  return matched?.id || raw;
}

function normalizeTeamDefinitionMember(item) {
  const payload = item && typeof item === "object" ? { ...item } : {};
  let sourceKind = String(payload.source_kind || "").trim();
  if (!sourceKind) {
    sourceKind = payload.team_definition_ref || payload.team_definition_id ? "team_definition" : "agent_definition";
  }
  sourceKind = teamDefinitionSourceKind(sourceKind);
  const kind = sourceKind === "team_definition" ? "team" : "agent";
  const sourceRef =
    sourceKind === "team_definition"
      ? String(payload.team_definition_ref || payload.team_definition_id || payload.source_ref || "").trim()
      : String(payload.agent_definition_ref || payload.agent_definition_id || payload.source_ref || "").trim();
  return {
    kind,
    source_kind: sourceKind,
    source_ref: normalizeTeamDefinitionReference(sourceKind, sourceRef),
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
  return state.agentDefinitions.map((item) => ({ value: item.id, label: item.name || item.id }));
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
    (item.source_kind === "team_definition" ? "未选择团队" : "未选择 Agent")
  );
}

function renderTeamDefinitionLeadAgentOptions(selectedValue = "") {
  renderSingleSelect(
    teamDefinitionLeadAgentDefinition,
    state.agentDefinitions.map((item) => ({ value: item.id, label: item.name || item.id })),
    selectedValue,
    "暂无 Agent",
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
    name: teamDefinitionMemberSourceLabel(item) || `成员 ${index + 1}`,
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
          return `
            <article class="member-card">
              <div class="member-card-head">
                <strong>${escapeHtml(`Subagent ${index + 1}`)}</strong>
                <button type="button" class="ghost" data-team-definition-member-remove="${index}">移除</button>
              </div>
              <div class="form-grid two">
                <label>
                  <span>SubAgent 类型</span>
                  <select data-team-definition-member-field="source_kind" data-member-index="${index}">
                    ${teamDefinitionSelectOptionsMarkup(TEAM_DEFINITION_CHILD_SOURCE_KIND_OPTIONS, sourceKind)}
                  </select>
                </label>
                <label>
                  <span>对象选择</span>
                  <select data-team-definition-member-field="source_ref" data-member-index="${index}">
                    ${teamDefinitionSelectOptionsMarkup(teamDefinitionReferenceOptions(sourceKind), member.source_ref, {
                      allowBlank: true,
                      blankLabel: sourceKind === "team_definition" ? "请选择团队" : "请选择 Agent",
                      fallbackLabel: sourceKind === "team_definition" ? "暂无可引用团队" : "暂无 Agent",
                    })}
                  </select>
                </label>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail">暂无直属 Subagent。可添加 Agent 或另一个团队作为子节点。</div>';
}

function addTeamDefinitionMember() {
  mutateTeamDefinitionMembers((members) => {
    const firstAgentDefinition = state.agentDefinitions[0] || null;
    members.push({
      kind: "agent",
      source_kind: "agent_definition",
      source_ref: firstAgentDefinition?.id || "",
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
  });
}

function updateTeamDefinitionMemberCheck() {}

function removeTeamDefinitionMember(index) {
  mutateTeamDefinitionMembers((members) => {
    members.splice(index, 1);
  });
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
  renderTaskSessionHint();
}

function currentTaskSessionThreadId(teamDefinitionId = state.selectedTaskTeamDefinitionId || taskTeamDefinition?.value || null) {
  if (!teamDefinitionId) {
    return "";
  }
  return String((state.taskSessionThreads || {})[teamDefinitionId] || "").trim();
}

function setTaskSessionThreadId(teamDefinitionId, threadId) {
  const resolvedTeamDefinitionId = String(teamDefinitionId || "").trim();
  if (!resolvedTeamDefinitionId) {
    return;
  }
  const next = { ...(state.taskSessionThreads || {}) };
  const resolvedThreadId = String(threadId || "").trim();
  if (resolvedThreadId) {
    next[resolvedTeamDefinitionId] = resolvedThreadId;
  } else {
    delete next[resolvedTeamDefinitionId];
  }
  state.taskSessionThreads = next;
  try {
    window.localStorage.setItem("aiteams.taskSessionThreads", JSON.stringify(next));
  } catch (error) {
    console.warn("Failed to persist task session threads", error);
  }
}

function resolveConversationThreadId(bundle) {
  return String(
    bundle?.conversation_thread_id ||
      bundle?.run?.state_json?.session_thread_id ||
      bundle?.task_thread?.metadata_json?.session_thread_id ||
      "",
  ).trim();
}

function renderTaskSessionHint() {
  if (!taskSessionHint) {
    return;
  }
  const teamDefinitionId = state.selectedTaskTeamDefinitionId || taskTeamDefinition?.value || "";
  const currentThreadId = currentTaskSessionThreadId(teamDefinitionId);
  if (!teamDefinitionId) {
    taskSessionHint.textContent = "当前会话：先选择 TeamDefinition，再决定是否复用现有 thread_id。";
    return;
  }
  if (taskNewSession?.checked || !currentThreadId) {
    taskSessionHint.textContent = "当前会话：本次启动会自动生成新的 uuidv7 thread_id。";
    return;
  }
  taskSessionHint.textContent = `当前会话 thread_id：${currentThreadId}`;
}

function currentTeamChatDefinition() {
  return state.teamDefinitions.find((item) => item.id === state.teamChat.selectedTeamDefinitionId) || null;
}

function currentTeamChatThread() {
  return state.teamChat.threads.find((item) => item.id === state.teamChat.selectedThreadRecordId) || null;
}

function teamChatThreadSessionId(thread) {
  return String(thread?.thread_id || thread?.session_thread_id || thread?.metadata_json?.session_thread_id || "").trim();
}

function currentTeamChatRunId(thread = currentTeamChatThread()) {
  return String(thread?.last_run_id || thread?.metadata_json?.last_run_id || "").trim();
}

function teamChatNeedsRunPolling(thread = currentTeamChatThread()) {
  if (state.activePage !== "team-chat" || !thread) {
    return false;
  }
  const runId = currentTeamChatRunId(thread);
  if (!runId) {
    return false;
  }
  let hasInterrupted = false;
  let hasResolvedAssistant = false;
  state.teamChat.messages.forEach((item) => {
    const payload = dictOrEmpty(item?.payload_json);
    const messageRunId = String(item?.run_id || payload.run_id || "").trim();
    if (messageRunId !== runId) {
      return;
    }
    const interrupted = Boolean(payload.interrupted) || String(item?.status || "").trim() === "interrupted";
    if (interrupted) {
      hasInterrupted = true;
      return;
    }
    const role = String(payload.role || item?.message_type || "").trim().toLowerCase();
    if (role === "assistant") {
      hasResolvedAssistant = true;
    }
  });
  return hasInterrupted && !hasResolvedAssistant;
}

function teamChatResultMessageForStatus(status) {
  return status === "completed"
    ? "\u5ba1\u6279\u5df2\u5904\u7406\uff0c\u56e2\u961f\u6d4b\u8bd5\u7ed3\u679c\u5df2\u56de\u5199\u5230\u5f53\u524d\u4f1a\u8bdd\u3002"
    : "\u5ba1\u6279\u5df2\u5904\u7406\uff0c\u5f53\u524d\u4f1a\u8bdd\u5df2\u540c\u6b65\u6700\u65b0\u8fd0\u884c\u72b6\u6001\u3002";
}

function hasTeamChatStatusNotice(runId, status) {
  const expectedMessage = teamChatResultMessageForStatus(status);
  return state.teamChat.messages.some((item) => {
    const payload = dictOrEmpty(item?.payload_json);
    const messageRunId = String(item?.run_id || payload.run_id || "").trim();
    const body = String(payload.body || payload.content || payload.text || "").trim();
    return messageRunId === String(runId || "").trim() && body === expectedMessage;
  });
}

function stopTeamChatRunPolling() {
  if (state.teamChat.pollTimer) {
    window.clearTimeout(state.teamChat.pollTimer);
    state.teamChat.pollTimer = 0;
  }
}

async function pollTeamChatRun(expectedRunId) {
  const activeThread = currentTeamChatThread();
  const activeRunId = currentTeamChatRunId(activeThread);
  if (
    state.activePage !== "team-chat" ||
    !activeThread ||
    !activeRunId ||
    activeRunId !== String(expectedRunId || "").trim() ||
    !teamChatNeedsRunPolling(activeThread)
  ) {
    return;
  }
  state.teamChat.pollBusy = true;
  try {
    const payload = await api(`/api/runs/${activeRunId}`);
    const status = String(payload?.run?.status || "").trim();
    if (status && status !== "waiting_approval") {
      await refreshTeamChatThreads(state.teamChat.selectedTeamDefinitionId, {
        selectRecordId: state.teamChat.selectedThreadRecordId || "",
        selectSessionThreadId: state.teamChat.selectedSessionThreadId || "",
        preserveSelection: true,
        allowEmptySelection: state.teamChat.draftMode,
      });
      await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
      renderTeamChat();
      scrollTeamChatToBottom();
      if (!teamChatNeedsRunPolling(currentTeamChatThread()) && !hasTeamChatStatusNotice(activeRunId, status)) {
        showResult(teamChatResult, {
          message: teamChatResultMessageForStatus(status),
          run_id: activeRunId,
          status,
        });
      }
    }
  } catch (error) {
    console.warn("Failed to poll team chat run", error);
  } finally {
    state.teamChat.pollBusy = false;
    scheduleTeamChatRunPolling();
  }
}

function scheduleTeamChatRunPolling() {
  stopTeamChatRunPolling();
  if (state.teamChat.pollBusy) {
    return;
  }
  const activeThread = currentTeamChatThread();
  const runId = currentTeamChatRunId(activeThread);
  if (!runId || !teamChatNeedsRunPolling(activeThread)) {
    return;
  }
  state.teamChat.pollTimer = window.setTimeout(() => {
    state.teamChat.pollTimer = 0;
    void pollTeamChatRun(runId);
  }, 2500);
}

function teamChatMessageRole(item) {
  const payload = dictOrEmpty(item?.payload_json);
  if (payload.interrupted || String(item?.status || "").trim() === "interrupted") {
    return "system";
  }
  const role = String(payload.role || item?.message_type || "").trim().toLowerCase();
  if (role === "user") {
    return "user";
  }
  if (role === "assistant") {
    return "assistant";
  }
  return "system";
}

function teamChatMessageBody(item) {
  const payload = dictOrEmpty(item?.payload_json);
  const candidates = [payload.body, payload.content, payload.text, payload.error];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }
  return "";
}

function formatTeamChatTime(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) {
    return text;
  }
  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function resizeTeamChatInput() {
  if (!teamChatInput) {
    return;
  }
  teamChatInput.style.height = "auto";
  teamChatInput.style.height = `${Math.min(teamChatInput.scrollHeight, 180)}px`;
  syncTeamChatViewportHeight();
}

function syncTeamChatViewportHeight() {
  if (!teamChatLayout || !teamChatPageView?.classList.contains("active")) {
    return;
  }
  const viewportHeight = Math.round(window.visualViewport?.height || window.innerHeight || document.documentElement.clientHeight || 0);
  if (!viewportHeight) {
    return;
  }
  const rect = teamChatLayout.getBoundingClientRect();
  const bottomGap = 12;
  const availableHeight = Math.max(420, Math.floor(viewportHeight - rect.top - bottomGap));
  teamChatLayout.style.setProperty("--team-chat-layout-height", `${availableHeight}px`);
}

function populateTeamChatTeamOptions(selectedValue = state.teamChat.selectedTeamDefinitionId || "") {
  if (!teamChatTeamDefinition) {
    return;
  }
  const options = state.teamDefinitions
    .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name || item.id)}</option>`)
    .join("");
  teamChatTeamDefinition.innerHTML = `<option value="">选择团队</option>${options}`;
  teamChatTeamDefinition.value = selectedValue || "";
}

async function loadTeamChatThreads(teamDefinitionId) {
  if (!teamDefinitionId) {
    state.teamChat.threads = [];
    return;
  }
  const payload = await api(`/api/agent-center/team-definitions/${teamDefinitionId}/chat/threads`);
  state.teamChat.threads = Array.isArray(payload.items) ? payload.items : [];
}

async function loadTeamChatMessages(threadRecordId) {
  if (!threadRecordId) {
    state.teamChat.messages = [];
    return;
  }
  const payload = await api(`/api/message-events?thread_id=${encodeURIComponent(threadRecordId)}`);
  state.teamChat.messages = Array.isArray(payload.items) ? payload.items : [];
}

async function refreshTeamChatThreads(
  teamDefinitionId,
  { selectRecordId = "", selectSessionThreadId = "", preserveSelection = true, allowEmptySelection = false } = {},
) {
  await loadTeamChatThreads(teamDefinitionId);
  let nextThread =
    state.teamChat.threads.find((item) => item.id === selectRecordId) ||
    state.teamChat.threads.find((item) => teamChatThreadSessionId(item) === String(selectSessionThreadId || "").trim()) ||
    null;
  if (!nextThread && preserveSelection) {
    nextThread =
      state.teamChat.threads.find((item) => item.id === state.teamChat.selectedThreadRecordId) ||
      state.teamChat.threads.find((item) => teamChatThreadSessionId(item) === state.teamChat.selectedSessionThreadId) ||
      null;
  }
  if (!nextThread && !allowEmptySelection && state.teamChat.threads.length) {
    nextThread = state.teamChat.threads[0];
  }
  if (nextThread) {
    state.teamChat.selectedThreadRecordId = nextThread.id;
    state.teamChat.selectedSessionThreadId = teamChatThreadSessionId(nextThread);
    return;
  }
  state.teamChat.selectedThreadRecordId = null;
  if (!allowEmptySelection) {
    state.teamChat.selectedSessionThreadId = "";
  }
}

function renderTeamChatThreads() {
  if (!teamChatThreadList) {
    return;
  }
  if (!state.teamChat.selectedTeamDefinitionId) {
    teamChatThreadList.innerHTML = '<div class="team-chat-empty">先选择一个团队。</div>';
    return;
  }
  teamChatThreadList.innerHTML = state.teamChat.threads.length
    ? state.teamChat.threads
        .map((item) => {
          const active = item.id === state.teamChat.selectedThreadRecordId;
          const preview = item.last_message_preview || item.title || "空会话";
          const updatedAt = formatTeamChatTime(item.last_message_at || item.updated_at);
          return `
            <button type="button" class="team-chat-thread-item${active ? " active" : ""}" data-team-chat-thread="${item.id}">
              <strong title="${escapeAttribute(item.title || "新会话")}">${escapeHtml(item.title || "新会话")}</strong>
              <span title="${escapeAttribute(preview)}">${escapeHtml(preview)}</span>
              <div class="team-chat-thread-meta-line">
                <span>${escapeHtml(updatedAt || "刚刚创建")}</span>
              </div>
            </button>
          `;
        })
        .join("")
    : '<div class="team-chat-empty">还没有团队测试会话，点击“新建会话”后发送第一条消息。</div>';
}

function teamChatMessageMarkup(item) {
  const role = teamChatMessageRole(item);
  const body = teamChatMessageBody(item) || "无内容";
  const label = role === "user" ? "你" : role === "assistant" ? currentTeamChatDefinition()?.name || "团队" : "系统";
  return `
    <article class="team-chat-message ${role}">
      <div class="team-chat-bubble">
        <div class="team-chat-bubble-head">
          <strong>${escapeHtml(label)}</strong>
          <span>${escapeHtml(formatTeamChatTime(item.created_at) || "")}</span>
        </div>
        <div class="team-chat-bubble-body">${escapeHtml(body)}</div>
      </div>
    </article>
  `;
}

function renderTeamChatMessages() {
  if (!teamChatMessageList) {
    return;
  }
  if (!state.teamChat.selectedTeamDefinitionId) {
    teamChatMessageList.innerHTML = '<div class="team-chat-empty">选择团队后，这里会显示测试对话。</div>';
    return;
  }
  if (!state.teamChat.selectedThreadRecordId && !state.teamChat.messages.length) {
    teamChatMessageList.innerHTML = '<div class="team-chat-empty">当前是新会话，发送第一条消息开始测试团队。</div>';
    return;
  }
  teamChatMessageList.innerHTML = state.teamChat.messages.length
    ? state.teamChat.messages.map((item) => teamChatMessageMarkup(item)).join("")
    : '<div class="team-chat-empty">这条会话还没有消息。</div>';
}

function renderTeamChatHeader() {
  const definition = currentTeamChatDefinition();
  const thread = currentTeamChatThread();
  if (teamChatTitle) {
    teamChatTitle.textContent = definition?.name || "选择一个团队";
  }
  if (teamChatThreadMeta) {
    const threadId = state.teamChat.selectedSessionThreadId || teamChatThreadSessionId(thread);
    teamChatThreadMeta.textContent = threadId ? `thread_id：${threadId}` : "新会话会自动生成 uuidv7 thread_id";
  }
}

function renderTeamChatStatus() {
  const hasDefinition = Boolean(state.teamChat.selectedTeamDefinitionId);
  const disabled = !hasDefinition || state.teamChat.sending;
  if (teamChatInput) {
    teamChatInput.disabled = disabled;
  }
  if (teamChatSend) {
    teamChatSend.disabled = disabled;
    teamChatSend.textContent = state.teamChat.sending ? "发送中..." : "发送";
  }
}

function renderTeamChat() {
  populateTeamChatTeamOptions();
  renderTeamChatHeader();
  renderTeamChatThreads();
  renderTeamChatMessages();
  renderTeamChatStatus();
  requestAnimationFrame(syncTeamChatViewportHeight);
}

function scrollTeamChatToBottom() {
  if (!teamChatMessageList) {
    return;
  }
  requestAnimationFrame(() => {
    syncTeamChatViewportHeight();
    teamChatMessageList.scrollTop = teamChatMessageList.scrollHeight;
  });
}

function startNewTeamChatThread() {
  state.teamChat.selectedThreadRecordId = null;
  state.teamChat.selectedSessionThreadId = "";
  state.teamChat.messages = [];
  state.teamChat.draftMode = true;
  hideResult(teamChatResult);
  renderTeamChat();
  resizeTeamChatInput();
  teamChatInput?.focus();
}

async function ensureTeamChatPage(force = false) {
  if (!state.loaded.teamDefinitions || force) {
    await loadTeamDefinitions();
    state.loaded.teamDefinitions = true;
  }
  if (
    state.teamChat.selectedTeamDefinitionId &&
    !state.teamDefinitions.some((item) => item.id === state.teamChat.selectedTeamDefinitionId)
  ) {
    state.teamChat.selectedTeamDefinitionId = null;
    state.teamChat.selectedThreadRecordId = null;
    state.teamChat.selectedSessionThreadId = "";
    state.teamChat.messages = [];
    state.teamChat.draftMode = false;
  }
  if (!state.teamChat.selectedTeamDefinitionId && state.teamDefinitions.length) {
    state.teamChat.selectedTeamDefinitionId = state.teamDefinitions[0].id;
  }
  populateTeamChatTeamOptions();
  if (!state.teamChat.selectedTeamDefinitionId) {
    state.teamChat.threads = [];
    state.teamChat.messages = [];
    renderTeamChat();
    return;
  }
  await refreshTeamChatThreads(state.teamChat.selectedTeamDefinitionId, {
    preserveSelection: true,
    allowEmptySelection: state.teamChat.draftMode,
  });
  await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
  if (state.teamChat.selectedThreadRecordId) {
    state.teamChat.draftMode = false;
  }
  renderTeamChat();
  scrollTeamChatToBottom();
}

async function selectTeamChatTeam(teamDefinitionId, { allowEmptySelection = false } = {}) {
  state.teamChat.selectedTeamDefinitionId = teamDefinitionId || null;
  state.teamChat.selectedThreadRecordId = null;
  state.teamChat.selectedSessionThreadId = "";
  state.teamChat.messages = [];
  state.teamChat.draftMode = false;
  hideResult(teamChatResult);
  if (!state.teamChat.selectedTeamDefinitionId) {
    renderTeamChat();
    return;
  }
  await refreshTeamChatThreads(state.teamChat.selectedTeamDefinitionId, {
    preserveSelection: false,
    allowEmptySelection,
  });
  await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
  renderTeamChat();
  scrollTeamChatToBottom();
}

async function selectTeamChatThread(threadRecordId) {
  state.teamChat.selectedThreadRecordId = threadRecordId || null;
  const thread = currentTeamChatThread();
  state.teamChat.selectedSessionThreadId = teamChatThreadSessionId(thread);
  state.teamChat.draftMode = false;
  await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
  hideResult(teamChatResult);
  renderTeamChat();
  scrollTeamChatToBottom();
}

async function submitTeamChatMessage() {
  if (state.teamChat.sending) {
    return;
  }
  const teamDefinitionId = state.teamChat.selectedTeamDefinitionId;
  const text = String(teamChatInput?.value || "").trim();
  if (!teamDefinitionId) {
    throw new Error("请先选择一个团队。");
  }
  if (!text) {
    throw new Error("消息不能为空。");
  }
  state.teamChat.sending = true;
  hideResult(teamChatResult);
  renderTeamChatStatus();
  try {
    const response = await api(`/api/agent-center/team-definitions/${teamDefinitionId}/chat/messages`, {
      method: "POST",
      body: JSON.stringify({
        message: text,
        thread_id: state.teamChat.selectedSessionThreadId || null,
      }),
    });
    state.teamChat.selectedSessionThreadId = String(response.thread_id || response.thread?.thread_id || "").trim();
    state.teamChat.draftMode = false;
    if (teamChatInput) {
      teamChatInput.value = "";
    }
    resizeTeamChatInput();
    await refreshTeamChatThreads(teamDefinitionId, {
      selectRecordId: String(response.thread?.id || "").trim(),
      selectSessionThreadId: state.teamChat.selectedSessionThreadId,
      preserveSelection: false,
    });
    await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
    renderTeamChat();
    scrollTeamChatToBottom();
    if (response.interrupted) {
      showResult(teamChatResult, {
        message: "本次团队测试触发了审核中断，消息已记录到会话流中。",
        thread_id: state.teamChat.selectedSessionThreadId || null,
      });
    }
  } finally {
    state.teamChat.sending = false;
    renderTeamChatStatus();
  }
}

async function openTeamChatPageForTeamDefinition(teamDefinitionId) {
  state.teamChat.selectedTeamDefinitionId = teamDefinitionId || null;
  state.teamChat.selectedThreadRecordId = null;
  state.teamChat.selectedSessionThreadId = "";
  state.teamChat.messages = [];
  state.teamChat.draftMode = false;
  await switchPage("team-chat");
}

async function deleteTeamChatThread(threadRecordId) {
  const teamDefinitionId = String(state.teamChat.selectedTeamDefinitionId || "").trim();
  const targetThreadId = String(threadRecordId || "").trim();
  if (!teamDefinitionId || !targetThreadId) {
    return;
  }
  if (state.teamChat.sending) {
    throw new Error("当前消息发送中，暂时不能删除会话。");
  }
  const wasSelected = state.teamChat.selectedThreadRecordId === targetThreadId;
  const preservedRecordId = wasSelected ? "" : state.teamChat.selectedThreadRecordId || "";
  const preservedSessionThreadId = wasSelected ? "" : state.teamChat.selectedSessionThreadId || "";
  hideResult(teamChatResult);
  await api(`/api/agent-center/team-definitions/${teamDefinitionId}/chat/threads/${targetThreadId}`, {
    method: "DELETE",
  });
  if (wasSelected) {
    state.teamChat.selectedThreadRecordId = null;
    state.teamChat.selectedSessionThreadId = "";
    state.teamChat.messages = [];
  }
  await refreshTeamChatThreads(teamDefinitionId, {
    selectRecordId: preservedRecordId,
    selectSessionThreadId: preservedSessionThreadId,
    preserveSelection: !wasSelected,
    allowEmptySelection: state.teamChat.draftMode,
  });
  await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
  state.teamChat.draftMode = !state.teamChat.selectedThreadRecordId;
  renderTeamChat();
  scrollTeamChatToBottom();
  showResult(teamChatResult, { message: "团队测试会话已删除", id: targetThreadId });
}

function populateTeamChatTeamOptions(selectedValue = state.teamChat.selectedTeamDefinitionId || "") {
  if (!teamChatTeamDefinition) {
    return;
  }
  const options = state.teamDefinitions
    .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name || item.id)}</option>`)
    .join("");
  teamChatTeamDefinition.innerHTML = `<option value="">\u9009\u62e9\u56e2\u961f</option>${options}`;
  teamChatTeamDefinition.value = selectedValue || "";
}

function renderTeamChatThreads() {
  if (!teamChatThreadList) {
    return;
  }
  if (!state.teamChat.selectedTeamDefinitionId) {
    teamChatThreadList.innerHTML = '<div class="team-chat-empty">\u5148\u9009\u62e9\u4e00\u4e2a\u56e2\u961f\u3002</div>';
    return;
  }
  teamChatThreadList.innerHTML = state.teamChat.threads.length
    ? state.teamChat.threads
        .map((item) => {
          const active = item.id === state.teamChat.selectedThreadRecordId;
          const title = item.title || "\u65b0\u4f1a\u8bdd";
          const preview = item.last_message_preview || title || "\u7a7a\u4f1a\u8bdd";
          const updatedAt = formatTeamChatTime(item.last_message_at || item.updated_at) || "\u521a\u521a\u521b\u5efa";
          return `
            <article class="team-chat-thread-row">
              <button
                type="button"
                class="team-chat-thread-item${active ? " active" : ""}"
                data-team-chat-thread="${escapeAttribute(item.id || "")}"
              >
                <strong title="${escapeAttribute(title)}">${escapeHtml(title)}</strong>
                <span title="${escapeAttribute(preview)}">${escapeHtml(preview)}</span>
                <div class="team-chat-thread-meta-line">
                  <span>${escapeHtml(updatedAt)}</span>
                </div>
              </button>
              <button
                type="button"
                class="team-chat-thread-delete ghost warn"
                data-team-chat-thread-delete="${escapeAttribute(item.id || "")}"
                title="删除会话"
              >
                删除
              </button>
            </article>
          `;
        })
        .join("")
    : '<div class="team-chat-empty">\u8fd8\u6ca1\u6709\u56e2\u961f\u6d4b\u8bd5\u4f1a\u8bdd\uff0c\u70b9\u51fb\u201c\u65b0\u5efa\u4f1a\u8bdd\u201d\u540e\u53d1\u9001\u7b2c\u4e00\u6761\u6d88\u606f\u3002</div>';
}

function teamChatMessageMarkup(item) {
  const role = teamChatMessageRole(item);
  const body = teamChatMessageBody(item) || "\u65e0\u5185\u5bb9";
  const label = role === "user" ? "\u4f60" : role === "assistant" ? currentTeamChatDefinition()?.name || "\u56e2\u961f" : "\u7cfb\u7edf";
  return `
    <article class="team-chat-message ${role}">
      <div class="team-chat-bubble">
        <div class="team-chat-bubble-head">
          <strong>${escapeHtml(label)}</strong>
          <span>${escapeHtml(formatTeamChatTime(item.created_at) || "")}</span>
        </div>
        <div class="team-chat-bubble-body">${renderMarkdown(body)}</div>
      </div>
    </article>
  `;
}

function renderTeamChatMessages() {
  if (!teamChatMessageList) {
    return;
  }
  if (!state.teamChat.selectedTeamDefinitionId) {
    teamChatMessageList.innerHTML = '<div class="team-chat-empty">\u9009\u62e9\u56e2\u961f\u540e\uff0c\u8fd9\u91cc\u4f1a\u663e\u793a\u6d4b\u8bd5\u5bf9\u8bdd\u3002</div>';
    return;
  }
  if (!state.teamChat.selectedThreadRecordId && !state.teamChat.messages.length) {
    teamChatMessageList.innerHTML =
      '<div class="team-chat-empty">\u5f53\u524d\u662f\u65b0\u4f1a\u8bdd\uff0c\u53d1\u9001\u7b2c\u4e00\u6761\u6d88\u606f\u5f00\u59cb\u6d4b\u8bd5\u56e2\u961f\u3002</div>';
    return;
  }
  teamChatMessageList.innerHTML = state.teamChat.messages.length
    ? state.teamChat.messages.map((item) => teamChatMessageMarkup(item)).join("")
    : '<div class="team-chat-empty">\u8fd9\u6761\u4f1a\u8bdd\u8fd8\u6ca1\u6709\u6d88\u606f\u3002</div>';
}

function renderTeamChatHeader() {
  const definition = currentTeamChatDefinition();
  const thread = currentTeamChatThread();
  if (teamChatTitle) {
    teamChatTitle.textContent = definition?.name || "\u9009\u62e9\u4e00\u4e2a\u56e2\u961f";
  }
  if (teamChatThreadMeta) {
    const threadId = state.teamChat.selectedSessionThreadId || teamChatThreadSessionId(thread);
    teamChatThreadMeta.textContent = threadId
      ? `thread_id\uff1a${threadId}`
      : "\u65b0\u4f1a\u8bdd\u4f1a\u81ea\u52a8\u751f\u6210 uuidv7 thread_id";
  }
}

function renderTeamChatStatus() {
  const hasDefinition = Boolean(state.teamChat.selectedTeamDefinitionId);
  const disabled = !hasDefinition || state.teamChat.sending;
  if (teamChatInput) {
    teamChatInput.disabled = disabled;
  }
  if (teamChatSend) {
    teamChatSend.disabled = disabled;
    teamChatSend.textContent = state.teamChat.sending ? "\u53d1\u9001\u4e2d..." : "\u53d1\u9001";
  }
}

function renderTeamChat() {
  populateTeamChatTeamOptions();
  renderTeamChatHeader();
  renderTeamChatThreads();
  renderTeamChatMessages();
  renderTeamChatStatus();
  scheduleTeamChatRunPolling();
}

function scrollTeamChatToBottom() {
  if (!teamChatMessageList) {
    return;
  }
  requestAnimationFrame(() => {
    teamChatMessageList.scrollTop = teamChatMessageList.scrollHeight;
  });
}

function startNewTeamChatThread() {
  state.teamChat.selectedThreadRecordId = null;
  state.teamChat.selectedSessionThreadId = "";
  state.teamChat.messages = [];
  state.teamChat.draftMode = true;
  hideResult(teamChatResult);
  renderTeamChat();
  resizeTeamChatInput();
  teamChatInput?.focus();
}

async function ensureTeamChatPage(force = false) {
  if (!state.loaded.teamDefinitions || force) {
    await loadTeamDefinitions();
    state.loaded.teamDefinitions = true;
  }
  if (
    state.teamChat.selectedTeamDefinitionId &&
    !state.teamDefinitions.some((item) => item.id === state.teamChat.selectedTeamDefinitionId)
  ) {
    state.teamChat.selectedTeamDefinitionId = null;
    state.teamChat.selectedThreadRecordId = null;
    state.teamChat.selectedSessionThreadId = "";
    state.teamChat.messages = [];
    state.teamChat.draftMode = false;
  }
  if (!state.teamChat.selectedTeamDefinitionId && state.teamDefinitions.length) {
    state.teamChat.selectedTeamDefinitionId = state.teamDefinitions[0].id;
  }
  populateTeamChatTeamOptions();
  if (!state.teamChat.selectedTeamDefinitionId) {
    state.teamChat.threads = [];
    state.teamChat.messages = [];
    renderTeamChat();
    return;
  }
  await refreshTeamChatThreads(state.teamChat.selectedTeamDefinitionId, {
    preserveSelection: true,
    allowEmptySelection: state.teamChat.draftMode,
  });
  await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
  if (state.teamChat.selectedThreadRecordId) {
    state.teamChat.draftMode = false;
  }
  renderTeamChat();
  scrollTeamChatToBottom();
}

async function selectTeamChatTeam(teamDefinitionId, { allowEmptySelection = false } = {}) {
  state.teamChat.selectedTeamDefinitionId = teamDefinitionId || null;
  state.teamChat.selectedThreadRecordId = null;
  state.teamChat.selectedSessionThreadId = "";
  state.teamChat.messages = [];
  state.teamChat.draftMode = false;
  hideResult(teamChatResult);
  if (!state.teamChat.selectedTeamDefinitionId) {
    renderTeamChat();
    return;
  }
  await refreshTeamChatThreads(state.teamChat.selectedTeamDefinitionId, {
    preserveSelection: false,
    allowEmptySelection,
  });
  await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
  renderTeamChat();
  scrollTeamChatToBottom();
}

async function selectTeamChatThread(threadRecordId) {
  state.teamChat.selectedThreadRecordId = threadRecordId || null;
  const thread = currentTeamChatThread();
  state.teamChat.selectedSessionThreadId = teamChatThreadSessionId(thread);
  state.teamChat.draftMode = false;
  await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
  hideResult(teamChatResult);
  renderTeamChat();
  scrollTeamChatToBottom();
}

async function submitTeamChatMessage() {
  if (state.teamChat.sending) {
    return;
  }
  const teamDefinitionId = state.teamChat.selectedTeamDefinitionId;
  const text = String(teamChatInput?.value || "").trim();
  if (!teamDefinitionId) {
    throw new Error("\u8bf7\u5148\u9009\u62e9\u4e00\u4e2a\u56e2\u961f\u3002");
  }
  if (!text) {
    throw new Error("\u6d88\u606f\u4e0d\u80fd\u4e3a\u7a7a\u3002");
  }
  state.teamChat.sending = true;
  hideResult(teamChatResult);
  renderTeamChatStatus();
  try {
    const response = await api(`/api/agent-center/team-definitions/${teamDefinitionId}/chat/messages`, {
      method: "POST",
      body: JSON.stringify({
        message: text,
        thread_id: state.teamChat.selectedSessionThreadId || null,
      }),
    });
    state.teamChat.selectedSessionThreadId = String(response.thread_id || response.thread?.thread_id || "").trim();
    state.teamChat.draftMode = false;
    if (teamChatInput) {
      teamChatInput.value = "";
    }
    resizeTeamChatInput();
    await refreshTeamChatThreads(teamDefinitionId, {
      selectRecordId: String(response.thread?.id || "").trim(),
      selectSessionThreadId: state.teamChat.selectedSessionThreadId,
      preserveSelection: false,
    });
    await loadTeamChatMessages(state.teamChat.selectedThreadRecordId);
    renderTeamChat();
    scrollTeamChatToBottom();
    if (response.interrupted) {
      showResult(teamChatResult, {
        message:
          "\u672c\u6b21\u56e2\u961f\u6d4b\u8bd5\u5df2\u8fdb\u5165\u5ba1\u6279\u7b49\u5f85\uff0c\u5ba1\u6279\u5b8c\u6210\u540e\u7ed3\u679c\u4f1a\u81ea\u52a8\u56de\u5199\u5230\u5f53\u524d\u4f1a\u8bdd\u3002",
        thread_id: state.teamChat.selectedSessionThreadId || null,
      });
    }
  } finally {
    state.teamChat.sending = false;
    renderTeamChatStatus();
  }
}

async function openTeamChatPageForTeamDefinition(teamDefinitionId) {
  state.teamChat.selectedTeamDefinitionId = teamDefinitionId || null;
  state.teamChat.selectedThreadRecordId = null;
  state.teamChat.selectedSessionThreadId = "";
  state.teamChat.messages = [];
  state.teamChat.draftMode = false;
  await switchPage("team-chat");
}

function providerPreset(type = providerType.value) {
  return state.providerTypes.find((item) => item.provider_type === type) || null;
}

function retrievalSettingsSnapshot() {
  return state.retrievalSettings?.settings || { embedding: { mode: "disabled" }, rerank: { mode: "disabled" } };
}

async function requireKnowledgeEmbeddingConfiguration() {
  if (!state.loaded.retrievalSettings) {
    await loadRetrievalSettings();
    state.loaded.retrievalSettings = true;
  }
  const embedding = retrievalSettingsSnapshot().embedding || {};
  const mode = String(embedding.mode || "disabled").trim().toLowerCase();
  const configured =
    mode === "provider"
      ? Boolean(String(embedding.provider_id || "").trim() && String(embedding.model_name || embedding.model || "").trim())
      : mode === "local"
        ? Boolean(
            String(embedding.local_model_id || "").trim() ||
              String(embedding.model_path || "").trim() ||
              String(embedding.model_name || embedding.model || "").trim(),
          )
        : false;
  if (configured) {
    return;
  }
  throw new Error("当前未配置 Embed 模型，请先前往 Agent Center 的 Embedding / Rerank 配置中设置 Embed 模型。");
}

function selectCurrentLabel(select) {
  if (!select) {
    return "";
  }
  const option = select.options?.[select.selectedIndex >= 0 ? select.selectedIndex : 0] || null;
  return String(option?.textContent || option?.label || "").trim();
}

function setRetrievalFieldVisibility(wrap, field, visible) {
  if (wrap) {
    wrap.classList.toggle("hidden", !visible);
    wrap.hidden = !visible;
    wrap.style.display = visible ? "" : "none";
    wrap.setAttribute("aria-hidden", visible ? "false" : "true");
  }
  if (field) {
    field.hidden = !visible;
    field.setAttribute("aria-hidden", visible ? "false" : "true");
  }
}

function setRetrievalFieldGroupVisibility(group, visible) {
  if (!group) {
    return;
  }
  group.classList.toggle("hidden", !visible);
  group.hidden = !visible;
  group.style.display = visible ? "" : "none";
  group.setAttribute("aria-hidden", visible ? "false" : "true");
}

function syncRetrievalDynamicBlock(kind) {
  const settings = retrievalSettingsSnapshot();
  const isEmbedding = kind === "embedding";
  const modeSelect = isEmbedding ? retrievalEmbeddingMode : retrievalRerankMode;
  const fieldsGroup = isEmbedding ? retrievalEmbeddingFields : retrievalRerankFields;
  const providerWrap = isEmbedding ? retrievalEmbeddingProviderWrap : retrievalRerankProviderWrap;
  const providerSelect = isEmbedding ? retrievalEmbeddingProvider : retrievalRerankProvider;
  const modelWrap = isEmbedding ? retrievalEmbeddingModelWrap : retrievalRerankModelWrap;
  const modelSelect = isEmbedding ? retrievalEmbeddingModel : retrievalRerankModel;
  const localWrap = isEmbedding ? retrievalEmbeddingLocalModelWrap : retrievalRerankLocalModelWrap;
  const localSelect = isEmbedding ? retrievalEmbeddingLocalModel : retrievalRerankLocalModel;
  const providerModelType = isEmbedding ? "embedding" : "rerank";
  const localKind = isEmbedding ? "embedding" : "rerank";
  const localTypeLabel = isEmbedding ? "Embed" : "Rerank";
  const mode = modeSelect?.value || "disabled";
  const currentSettings = (isEmbedding ? settings.embedding : settings.rerank) || {};

  const providerItems = providerOptionsByModelType(providerModelType);
  renderProviderSelect(providerSelect, providerItems, providerSelect?.value || currentSettings.provider_id || "");
  const providerModels = providerModelsByType(providerSelect?.value || "", providerModelType);
  renderModelSelect(modelSelect, providerModels, modelSelect?.value || currentSettings.model_name || "");

  const localOptions = retrievalLocalModelOptions(localKind, currentSettings, localSelect?.value || "");
  renderSingleSelect(
    localSelect,
    localOptions.items,
    localOptions.selectedValue,
    `暂无本地 ${localTypeLabel} 模型`,
  );

  const providerEnabled = mode === "provider";
  const localEnabled = mode === "local";
  setRetrievalFieldGroupVisibility(fieldsGroup, providerEnabled || localEnabled);
  setRetrievalFieldVisibility(providerWrap, providerSelect, providerEnabled);
  setRetrievalFieldVisibility(modelWrap, modelSelect, providerEnabled);
  setRetrievalFieldVisibility(localWrap, localSelect, localEnabled);

  if (providerSelect) {
    providerSelect.disabled = !providerEnabled || !providerItems.length;
  }
  if (modelSelect) {
    modelSelect.disabled = !providerEnabled || !providerModels.length;
  }
  if (localSelect) {
    localSelect.disabled = !localEnabled;
  }
}

function syncRetrievalDynamicForm() {
  syncRetrievalDynamicBlock("embedding");
  syncRetrievalDynamicBlock("rerank");
  renderRetrievalSummary();
}

function syncRetrievalProviderOptions() {
  syncRetrievalDynamicForm();
}

function syncRetrievalModelOptions() {
  syncRetrievalDynamicForm();
}

function fillRetrievalSettingsForm() {
  const settings = retrievalSettingsSnapshot();
  if (retrievalEmbeddingMode) {
    retrievalEmbeddingMode.value = settings.embedding?.mode || "disabled";
  }
  if (retrievalRerankMode) {
    retrievalRerankMode.value = settings.rerank?.mode || "disabled";
  }
  syncRetrievalDynamicForm();
  renderRetrievalSettings();
}

function buildRetrievalPayloadFromForm() {
  const embeddingLocalSelection = parseRetrievalLocalModelSelection(retrievalEmbeddingLocalModel?.value || "");
  const rerankLocalSelection = parseRetrievalLocalModelSelection(retrievalRerankLocalModel?.value || "");
  return {
    embedding:
      retrievalEmbeddingMode.value === "provider"
        ? {
            mode: "provider",
            provider_id: retrievalEmbeddingProvider.value,
            model_name: retrievalEmbeddingModel.value,
          }
        : retrievalEmbeddingMode.value === "local"
          ? embeddingLocalSelection.local_model_id
            ? {
                mode: "local",
                local_model_id: embeddingLocalSelection.local_model_id,
              }
            : {
                mode: "local",
                model_name: embeddingLocalSelection.model_name || DEFAULT_LOCAL_EMBEDDING_MODEL,
              }
        : { mode: "disabled" },
    rerank:
      retrievalRerankMode.value === "provider"
        ? {
            mode: "provider",
            provider_id: retrievalRerankProvider.value,
            model_name: retrievalRerankModel.value,
          }
        : retrievalRerankMode.value === "local"
          ? rerankLocalSelection.local_model_id
            ? {
                mode: "local",
                local_model_id: rerankLocalSelection.local_model_id,
              }
            : {
                mode: "local",
                model_name: rerankLocalSelection.model_name || DEFAULT_LOCAL_RERANK_MODEL,
              }
        : { mode: "disabled" },
  };
}

function retrievalSummarySnapshot() {
  const settings = retrievalSettingsSnapshot();
  const embeddingMode = retrievalEmbeddingMode?.value || settings.embedding?.mode || "disabled";
  const rerankMode = retrievalRerankMode?.value || settings.rerank?.mode || "disabled";
  const embeddingProvider =
    state.providers.find((item) => item.id === (retrievalEmbeddingProvider?.value || "")) || null;
  const rerankProvider =
    state.providers.find((item) => item.id === (retrievalRerankProvider?.value || "")) || null;
  const embeddingLocalSelection = parseRetrievalLocalModelSelection(retrievalEmbeddingLocalModel?.value || "");
  const rerankLocalSelection = parseRetrievalLocalModelSelection(retrievalRerankLocalModel?.value || "");
  return {
    embedding:
      embeddingMode === "provider"
        ? {
            mode: "provider",
            provider_id: retrievalEmbeddingProvider?.value || settings.embedding?.provider_id || "",
            provider_name: embeddingProvider?.name || settings.embedding?.provider_name || "",
            model_name: retrievalEmbeddingModel?.value || settings.embedding?.model_name || "",
          }
        : embeddingMode === "local"
          ? {
              mode: "local",
              model_name: embeddingLocalSelection.model_name || settings.embedding?.model_name || DEFAULT_LOCAL_EMBEDDING_MODEL,
              model_label: selectCurrentLabel(retrievalEmbeddingLocalModel) || settings.embedding?.model_label || "",
            }
          : { mode: "disabled" },
    rerank:
      rerankMode === "provider"
        ? {
            mode: "provider",
            provider_id: retrievalRerankProvider?.value || settings.rerank?.provider_id || "",
            provider_name: rerankProvider?.name || settings.rerank?.provider_name || "",
            model_name: retrievalRerankModel?.value || settings.rerank?.model_name || "",
          }
        : rerankMode === "local"
          ? {
              mode: "local",
              model_name: rerankLocalSelection.model_name || settings.rerank?.model_name || DEFAULT_LOCAL_RERANK_MODEL,
              model_label: selectCurrentLabel(retrievalRerankLocalModel) || settings.rerank?.model_label || "",
            }
          : { mode: "disabled" },
  };
}

function renderRetrievalSummary() {
  const settings = retrievalSummarySnapshot();
  if (retrievalSummary) {
    const embeddingLabel =
      settings.embedding?.mode === "provider"
        ? `${settings.embedding.provider_name || settings.embedding.provider_id || "-"} / ${settings.embedding.model_name || "-"}`
        : settings.embedding?.mode === "local"
          ? `Local HF / ${settings.embedding.model_label || settings.embedding.model_name || DEFAULT_LOCAL_EMBEDDING_MODEL}`
        : "关闭";
    const rerankLabel =
      settings.rerank?.mode === "provider"
        ? `${settings.rerank.provider_name || settings.rerank.provider_id || "-"} / ${settings.rerank.model_name || "-"}`
        : settings.rerank?.mode === "local"
          ? `Local Flag / ${settings.rerank.model_label || settings.rerank.model_name || DEFAULT_LOCAL_RERANK_MODEL}`
        : "关闭";
    retrievalSummary.innerHTML = [
      chip("Embedding", embeddingLabel),
      chip("Rerank", rerankLabel),
      chip("更新时间", state.retrievalSettings?.updated_at || "-"),
    ].join("");
  }
}

function renderRetrievalSettings() {
  const warnings = state.retrievalSettings?.warnings || [];
  renderRetrievalSummary();
  if (warnings.length) {
    showResult(retrievalSettingsResult, { warnings });
  } else {
    hideResult(retrievalSettingsResult);
  }
}

function buildRetrievalSaveResult(payload) {
  const runtimeApplied = payload?.runtime_applied || payload?.applied || null;
  const knowledgeApplied = payload?.knowledge_applied || null;
  const settings = payload?.settings || { embedding: { mode: "disabled" }, rerank: { mode: "disabled" } };
  const usesLocal = settings.embedding?.mode === "local" || settings.rerank?.mode === "local";
  const result = {
    message: "检索配置已保存",
    settings,
  };
  if (knowledgeApplied) {
    result.knowledge_applied = knowledgeApplied;
  }
  if (runtimeApplied) {
    result.runtime_memory_applied = runtimeApplied;
  }
  if (usesLocal) {
    result.note =
      "知识库检索以 knowledge_applied 为准；runtime_memory_applied 是运行时记忆模块结果，当前本地模式不会在该模块启用。";
  }
  return result;
}

function openLocalModelModal() {
  localModelModal?.classList.remove("hidden");
}

function closeLocalModelModal() {
  localModelModal?.classList.add("hidden");
  hideResult(localModelModalResult);
}

function inferUploadSourceName(entries) {
  const files = normalizeSkillImportEntries(entries);
  if (!files.length) {
    return "";
  }
  const roots = Array.from(new Set(files.map((item) => item.path.split("/")[0] || item.path)));
  const hasNested = files.some((item) => item.path.includes("/"));
  if (hasNested && roots.length === 1) {
    return roots[0];
  }
  return roots[0] || fileNameFromPath(files[0].path || "");
}

function syncLocalModelPathPreview() {
  if (!localModelPath) {
    return;
  }
  const current =
    state.localModels.find((item) => item.id === state.editingLocalModelId) ||
    state.localModelPage.items.find((item) => item.id === state.editingLocalModelId) ||
    null;
  const sourceName = inferUploadSourceName(state.localModelUploadFiles);
  localModelPath.value = sourceName ? `models/${sourceName}` : current?.model_path || "";
}

function renderLocalModelUploadSelection() {
  localModelDropzone?.classList.toggle("drag-active", Boolean(state.localModelUploadDragActive));
  const files = Array.isArray(state.localModelUploadFiles) ? state.localModelUploadFiles : [];
  if (!localModelUploadSelection) {
    syncLocalModelPathPreview();
    return;
  }
  if (!files.length) {
    localModelUploadSelection.classList.add("empty");
    localModelUploadSelection.innerHTML = '<span class="skill-import-selection-placeholder">未选择待上传模型文件</span>';
    syncLocalModelPathPreview();
    return;
  }
  const roots = Array.from(new Set(files.map((item) => item.path.split("/")[0] || item.path))).slice(0, 3);
  const totalBytes = files.reduce((sum, item) => sum + Number(item.file?.size || 0), 0);
  const suffix = roots.length < new Set(files.map((item) => item.path.split("/")[0] || item.path)).size ? " ..." : "";
  localModelUploadSelection.classList.remove("empty");
  localModelUploadSelection.innerHTML = `
    <strong title="${escapeAttribute(roots.join(" / ") + suffix)}">${escapeHtml(roots.join(" / ") + suffix)}</strong>
    <div class="skill-import-selection-meta">
      <span class="skill-import-selection-stat">${escapeHtml(`${files.length} 个文件`)}</span>
      <span class="skill-import-selection-stat">${escapeHtml(formatFileSize(totalBytes))}</span>
    </div>
  `;
  syncLocalModelPathPreview();
}

function resetLocalModelUploadState() {
  state.localModelUploadFiles = [];
  state.localModelUploadDragActive = false;
  if (localModelFolderInput) {
    localModelFolderInput.value = "";
  }
  setLocalModelUploadBusy(false);
  renderLocalModelUploadSelection();
}

function setLocalModelUploadFiles(entries, { append = false } = {}) {
  const normalized = normalizeSkillImportEntries(entries);
  state.localModelUploadFiles = append
    ? normalizeSkillImportEntries([...(state.localModelUploadFiles || []), ...normalized])
    : normalized;
  state.localModelUploadDragActive = false;
  hideResult(localModelModalResult);
  renderLocalModelUploadSelection();
}

function setLocalModelUploadBusy(busy) {
  state.localModelUploadBusy = Boolean(busy);
  localModelDropzone?.classList.toggle("busy", state.localModelUploadBusy);
  if (localModelFolderInput) {
    localModelFolderInput.disabled = state.localModelUploadBusy;
  }
  if (localModelUploadFolderButton) {
    localModelUploadFolderButton.disabled = state.localModelUploadBusy;
  }
  if (localModelSave) {
    localModelSave.disabled = state.localModelUploadBusy;
  }
}

async function buildLocalModelUploadPayload() {
  const files = normalizeSkillImportEntries(state.localModelUploadFiles);
  if (!files.length) {
    return null;
  }
  return {
    source_name: inferUploadSourceName(files),
    files: await Promise.all(
      files.map(async (item) => ({
        path: item.path,
        content_base64: await readFileAsBase64(item.file),
      })),
    ),
  };
}

function resetLocalModelForm({ openModal = false } = {}) {
  state.editingLocalModelId = null;
  if (localModelName) {
    localModelName.value = "";
  }
  if (localModelType) {
    localModelType.value = "Embed";
  }
  if (localModelPath) {
    localModelPath.value = "";
  }
  if (localModelModalTitle) {
    localModelModalTitle.textContent = "新增本地模型";
  }
  resetLocalModelUploadState();
  hideResult(localModelResult);
  hideResult(localModelModalResult);
  renderLocalModels();
  if (openModal) {
    openLocalModelModal();
  }
}

function fillLocalModelForm(item, { openModal = false } = {}) {
  state.editingLocalModelId = item.id || null;
  if (localModelName) {
    localModelName.value = item.name || "";
  }
  if (localModelType) {
    localModelType.value = item.model_type || "Embed";
  }
  if (localModelPath) {
    localModelPath.value = item.model_path || "";
  }
  if (localModelModalTitle) {
    localModelModalTitle.textContent = "编辑本地模型";
  }
  resetLocalModelUploadState();
  if (localModelPath) {
    localModelPath.value = item.model_path || "";
  }
  hideResult(localModelResult);
  hideResult(localModelModalResult);
  renderLocalModels();
  if (openModal) {
    openLocalModelModal();
  }
}

function buildLocalModelPayloadFromForm() {
  return {
    id: state.editingLocalModelId,
    name: localModelName?.value?.trim() || "",
    model_type: localModelType?.value || "Embed",
    model_path: localModelPath?.value?.trim() || "",
  };
}

function renderLocalModels() {
  if (!localModelList) {
    return;
  }
  localModelList.innerHTML = state.localModelPage.items.length
    ? state.localModelPage.items
        .map((item) => {
          const updatedAt = formatTeamChatTime(item.updated_at) || item.updated_at || "-";
          const pathText = item.path_display || item.model_path || "-";
          return `
            <article class="provider-row local-model-row${item.id === state.editingLocalModelId ? " active" : ""}">
              <div class="provider-main">
                <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
              </div>
              <div class="provider-cell">
                <strong>${escapeHtml(LOCAL_MODEL_TYPE_LABELS[item.model_type] || item.model_type || "-")}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(pathText)}">${escapeHtml(pathText)}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(item.updated_at || "-")}">${escapeHtml(updatedAt)}</strong>
              </div>
              <div class="provider-row-actions">
                <button type="button" data-local-model-edit="${escapeAttribute(item.id || "")}">编辑</button>
                <button type="button" class="ghost warn" data-local-model-delete="${escapeAttribute(item.id || "")}">删除</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail"><strong>暂无本地模型</strong><p>先上传一个本地模型文件夹。</p></div>';
  renderOffsetPagination(state.localModelPage, localModelPaginationMeta, "local-model-page");
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

function openPluginImportModal() {
  pluginImportModal?.classList.remove("hidden");
}

function closePluginImportModal() {
  pluginImportModal?.classList.add("hidden");
  setPluginImportMode("import");
  resetPluginImportState();
  hideResult(pluginImportResult);
}

function openSkillModal() {
  skillModal?.classList.remove("hidden");
}

function closeSkillModal() {
  skillModal?.classList.add("hidden");
  state.skillImportDragActive = false;
  renderSkillImportSelection();
  hideResult(skillModalResult);
}

function openSkillPreviewModal() {
  skillPreviewModal?.classList.remove("hidden");
}

function closeSkillPreviewModal() {
  skillPreviewModal?.classList.add("hidden");
  resetSkillPreviewState();
}

function resetSkillPreviewState() {
  state.skillPreview = {
    skillId: null,
    skill: null,
    files: [],
    selectedPath: "",
    fileCache: {},
    loading: false,
    fileLoading: false,
    errorText: "",
  };
  renderSkillPreview();
}

function skillPreviewBodyMarkdown(preview) {
  const defaultPath = skillPreviewDefaultPath(preview?.files || []);
  if (!defaultPath) {
    return "";
  }
  const file = preview?.fileCache?.[defaultPath] || null;
  if (!file || !file.is_text) {
    return "";
  }
  const raw = String(file.content || "").trim();
  if (!raw.startsWith("---")) {
    return raw;
  }
  const closing = raw.indexOf("\n---", 3);
  if (closing < 0) {
    return raw;
  }
  return raw.slice(closing + 4).trim();
}

function skillPreviewDefaultPath(files) {
  const items = Array.isArray(files) ? files : [];
  const skillFile = items.find((item) => String(item.path || "") === "SKILL.md") || items.find((item) => String(item.path || "").endsWith("/SKILL.md"));
  return String((skillFile || items[0] || {}).path || "").trim();
}

function renderSkillPreview() {
  if (!skillPreviewModal) {
    return;
  }
  const preview = state.skillPreview || {};
  const skill = preview.skill || null;
  const files = Array.isArray(preview.files) ? preview.files : [];
  const selectedPath = String(preview.selectedPath || "").trim();
  const selectedFile = selectedPath ? preview.fileCache?.[selectedPath] || null : null;
  const groups = skill ? skillGroupsForItem(skill) : [];
  const groupSummary = groups.length ? groups.map((item) => item.name || item.key || "-").join("、") : "未分组";
  const bodyMarkdown = skill ? skillPreviewBodyMarkdown(preview) : "";

  if (skillPreviewTitle) {
    skillPreviewTitle.textContent = skill ? `${skill.name || skill.id} · Skill 预览` : "Skill 预览";
  }
  if (skillPreviewSubtitle) {
    skillPreviewSubtitle.textContent = skill ? `${skill.storage_path || skill.id || "-"} / ${groupSummary}` : preview.loading ? "正在加载 Skill 详情..." : "";
  }
  if (skillPreviewSummary) {
    skillPreviewSummary.innerHTML = skill
      ? [
          chip("分组", groupSummary),
          chip("文件", files.length || 0),
          chip("位置", skill.storage_path || "-"),
        ].join("")
      : preview.loading
        ? chip("状态", "加载中")
        : preview.errorText
          ? chip("状态", "加载失败")
          : "";
  }
  if (skillPreviewBodyMeta) {
    skillPreviewBodyMeta.textContent = bodyMarkdown ? `${bodyMarkdown.length} 字符` : "";
  }
  if (skillPreviewContent) {
    if (preview.loading && !skill) {
      skillPreviewContent.innerHTML = '<div class="detail empty compact-detail">正在加载 Skill 内容...</div>';
    } else if (preview.errorText && !skill) {
      skillPreviewContent.innerHTML = `<div class="detail empty compact-detail"><strong>加载失败</strong><p>${escapeHtml(preview.errorText)}</p></div>`;
    } else if (bodyMarkdown) {
      skillPreviewContent.innerHTML = `<div class="team-chat-bubble-body skill-preview-markdown">${renderMarkdown(bodyMarkdown)}</div>`;
    } else {
      skillPreviewContent.innerHTML = '<div class="detail empty compact-detail">该 Skill 暂无可预览的正文内容。</div>';
    }
  }
  if (skillPreviewFileMeta) {
    skillPreviewFileMeta.textContent = files.length ? `${files.length} 个文件` : preview.loading ? "读取中..." : "";
  }
  if (skillPreviewFileList) {
    skillPreviewFileList.innerHTML = files.length
      ? files
          .map((item) => {
            const path = String(item.path || "");
            return `
              <button
                type="button"
                class="skill-preview-file-button${path === selectedPath ? " active" : ""}"
                data-skill-preview-file="${escapeAttribute(path)}"
              >
                <strong>${escapeHtml(fileNameFromPath(path))}</strong>
                <span title="${escapeAttribute(path)}">${escapeHtml(path)}</span>
              </button>
            `;
          })
          .join("")
      : '<div class="detail empty compact-detail">当前 Skill 暂无可查看文件。</div>';
  }
  if (skillPreviewFileTitle) {
    skillPreviewFileTitle.innerHTML = selectedPath
      ? `<strong>${escapeHtml(fileNameFromPath(selectedPath))}</strong><span>${escapeHtml(selectedPath)}</span>`
      : "<strong>未选择文件</strong>";
  }
  if (skillPreviewFileContent) {
    if (preview.fileLoading && selectedPath) {
      skillPreviewFileContent.innerHTML = '<div class="detail empty compact-detail">正在读取文件内容...</div>';
    } else if (!selectedPath) {
      skillPreviewFileContent.innerHTML = '<div class="detail empty compact-detail">从左侧选择一个文件后即可查看内容。</div>';
    } else if (!selectedFile) {
      skillPreviewFileContent.innerHTML = '<div class="detail empty compact-detail">文件内容尚未加载。</div>';
    } else if (!selectedFile.is_text) {
      skillPreviewFileContent.innerHTML = `<div class="detail empty compact-detail"><strong>${escapeHtml(fileNameFromPath(selectedPath))}</strong><p>${escapeHtml(selectedFile.message || "该文件暂不支持预览。")}</p></div>`;
    } else if (selectedFile.is_markdown) {
      skillPreviewFileContent.innerHTML = `
        <div class="team-chat-bubble-body skill-preview-markdown">${renderMarkdown(selectedFile.content || "")}</div>
        ${selectedFile.truncated ? '<p class="field-note skill-preview-truncated">文件较大，当前仅展示前 1 MB 内容。</p>' : ""}
      `;
    } else {
      skillPreviewFileContent.innerHTML = `
        <pre class="skill-preview-code"><code>${escapeHtml(selectedFile.content || "")}</code></pre>
        ${selectedFile.truncated ? '<p class="field-note skill-preview-truncated">文件较大，当前仅展示前 1 MB 内容。</p>' : ""}
      `;
    }
  }
}

async function loadSkillPreviewFile(path, { force = false } = {}) {
  const skillId = String(state.skillPreview.skillId || "").trim();
  const targetPath = String(path || "").trim();
  if (!skillId || !targetPath) {
    return;
  }
  state.skillPreview.selectedPath = targetPath;
  if (!force && state.skillPreview.fileCache?.[targetPath]) {
    state.skillPreview.fileLoading = false;
    renderSkillPreview();
    return;
  }
  state.skillPreview.fileLoading = true;
  renderSkillPreview();
  try {
    const params = new URLSearchParams({ path: targetPath });
    const payload = await api(`/api/agent-center/skills/${skillId}/file-content?${params.toString()}`);
    if (String(state.skillPreview.skillId || "").trim() !== skillId) {
      return;
    }
    state.skillPreview.fileCache[targetPath] = payload;
    state.skillPreview.fileLoading = false;
    renderSkillPreview();
  } catch (error) {
    if (String(state.skillPreview.skillId || "").trim() !== skillId) {
      return;
    }
    state.skillPreview.fileCache[targetPath] = {
      path: targetPath,
      is_text: false,
      message: error?.message || "读取文件失败。",
    };
    state.skillPreview.fileLoading = false;
    renderSkillPreview();
  }
}

async function openSkillPreview(skillId) {
  const targetSkillId = String(skillId || "").trim();
  if (!targetSkillId) {
    return;
  }
  resetSkillPreviewState();
  state.skillPreview.skillId = targetSkillId;
  state.skillPreview.loading = true;
  openSkillPreviewModal();
  renderSkillPreview();
  try {
    const [skill, filesPayload] = await Promise.all([
      api(`/api/agent-center/skills/${targetSkillId}`),
      api(`/api/agent-center/skills/${targetSkillId}/files`),
    ]);
    if (String(state.skillPreview.skillId || "").trim() !== targetSkillId) {
      return;
    }
    state.skillPreview.skill = skill;
    state.skillPreview.files = Array.isArray(filesPayload.items) ? filesPayload.items : [];
    state.skillPreview.loading = false;
    renderSkillPreview();
    const defaultPath = skillPreviewDefaultPath(state.skillPreview.files);
    if (defaultPath) {
      await loadSkillPreviewFile(defaultPath);
    }
  } catch (error) {
    if (String(state.skillPreview.skillId || "").trim() !== targetSkillId) {
      return;
    }
    state.skillPreview.loading = false;
    state.skillPreview.errorText = error?.message || "加载 Skill 预览失败。";
    renderSkillPreview();
  }
}

function openStaticMemoryModal(mode = currentStaticMemoryMode()) {
  syncStaticMemoryEditorMode(mode);
  staticMemoryModal?.classList.remove("hidden");
}

function closeStaticMemoryModal() {
  staticMemoryModal?.classList.add("hidden");
  hideResult(staticMemoryModalResult);
}

function setPluginEditorState(options = {}) {
  const { manifest = {}, config = {}, secretFieldPaths = [] } = options || {};
  const nextManifest = manifest && typeof manifest === "object" ? clone(manifest) : {};
  const nextConfig = deepMerge(dictOrEmpty(nextManifest.runtime), config && typeof config === "object" ? config : {});
  const nextRecord = Object.prototype.hasOwnProperty.call(options || {}, "record")
    ? options?.record && typeof options.record === "object"
      ? clone(options.record)
      : null
    : pluginEditorRecord()
      ? clone(pluginEditorRecord())
      : null;
  state.pluginEditor = {
    manifest: nextManifest,
    schema: nextManifest.config_schema && typeof nextManifest.config_schema === "object" ? clone(nextManifest.config_schema) : null,
    config: nextConfig,
    secretFieldPaths: Array.isArray(secretFieldPaths) ? [...secretFieldPaths] : [],
    record: nextRecord,
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
  if (!preserveEntered || !pluginName.value.trim()) {
    pluginName.value = normalized.name || pluginName.value || "";
  }
  if (!preserveEntered || !pluginDescription.value.trim()) {
    pluginDescription.value = normalized.description || pluginDescription.value || "";
  }
}

async function syncPluginManifestFromInstallPath() {
  return Promise.resolve();
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

function resetTeamDefinitionPreviewState() {
  teamDefinitionPreviewState.payload = null;
  teamDefinitionPreviewState.loading = false;
  teamDefinitionPreviewState.errorText = "";
}

function openTeamDefinitionPreviewModal() {
  teamDefinitionPreviewModal?.classList.remove("hidden");
}

function closeTeamDefinitionPreviewModal() {
  resetTeamDefinitionPreviewState();
  teamDefinitionPreviewModal?.classList.add("hidden");
  if (teamDefinitionPreviewTitle) {
    teamDefinitionPreviewTitle.textContent = "团队树预览";
  }
  if (teamDefinitionPreviewSummary) {
    teamDefinitionPreviewSummary.innerHTML = "";
  }
  if (teamDefinitionPreviewTree) {
    teamDefinitionPreviewTree.innerHTML = "";
  }
}

function teamDefinitionPreviewPayload(payload, { loading = false, errorText = "" } = {}) {
  teamDefinitionPreviewState.payload = payload || null;
  teamDefinitionPreviewState.loading = loading;
  teamDefinitionPreviewState.errorText = errorText;
  renderTeamDefinitionPreview();
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
  pluginName.value = "";
  pluginDescription.value = "";
  setPluginEditorState({ manifest: {}, config: {}, secretFieldPaths: [], record: null });
  pluginModalTitle.textContent = "编辑插件";
  hideResult(pluginModalResult);
}

function resetPluginImportState() {
  state.pluginImportFiles = [];
  state.pluginImportDragActive = false;
  state.pluginImportBusy = false;
  if (pluginImportDirectoryInput) {
    pluginImportDirectoryInput.value = "";
  }
  renderPluginImportSelection();
  setPluginImportBusy(false);
}

function renderPluginImportSelection() {
  pluginImportDropzone?.classList.toggle("drag-active", Boolean(state.pluginImportDragActive));
  const files = Array.isArray(state.pluginImportFiles) ? state.pluginImportFiles : [];
  if (!pluginImportSelection) {
    return;
  }
  if (!files.length) {
    pluginImportSelection.classList.add("empty");
    pluginImportSelection.innerHTML = `
      <span class="skill-import-selection-placeholder">未选择文件夹</span>
    `;
    return;
  }
  const roots = Array.from(new Set(files.map((item) => item.path.split("/")[0] || item.path)));
  const previewRoots = roots.slice(0, 3);
  const pluginYamlCount = files.filter((item) => item.path.split("/").pop() === "plugin.yaml").length;
  const suffix = previewRoots.length < roots.length ? " ..." : "";
  pluginImportSelection.classList.remove("empty");
  pluginImportSelection.innerHTML = `
    <strong title="${escapeAttribute(previewRoots.join(" / ") + suffix)}">${escapeHtml(previewRoots.join(" / ") + suffix)}</strong>
    <div class="skill-import-selection-meta">
      <span class="skill-import-selection-stat">${escapeHtml(`${files.length} 个文件`)}</span>
      <span class="skill-import-selection-stat">${escapeHtml(`${pluginYamlCount} 个 plugin.yaml`)}</span>
    </div>
  `;
}

function setPluginImportFiles(entries) {
  state.pluginImportFiles = normalizeSkillImportEntries(entries);
  state.pluginImportDragActive = false;
  hideResult(pluginImportResult);
  renderPluginImportSelection();
}

function setPluginImportBusy(busy) {
  state.pluginImportBusy = Boolean(busy);
  pluginImportDropzone?.classList.toggle("busy", state.pluginImportBusy);
  if (pluginImportDirectoryInput) {
    pluginImportDirectoryInput.disabled = state.pluginImportBusy;
  }
  if (pluginImportValidate) {
    pluginImportValidate.disabled = !state.pluginImportFiles.length || state.pluginImportBusy;
  }
  if (pluginImportSave) {
    pluginImportSave.disabled = !state.pluginImportFiles.length || state.pluginImportBusy;
  }
  if (pluginImportDropzoneTitle) {
    pluginImportDropzoneTitle.textContent = "点击上传插件目录";
  }
  if (pluginImportDropzoneHint) {
    pluginImportDropzoneHint.textContent = "支持单个插件目录，或包含多个插件目录的集合目录";
  }
  if (pluginImportDropzoneTrigger) {
    pluginImportDropzoneTrigger.textContent = "选择文件夹";
  }
}

function openPluginUploadModal() {
  resetPluginImportState();
  hideResult(pluginImportResult);
  openPluginImportModal();
  pluginImportDropzone?.focus();
}

async function buildPluginUploadPayload() {
  const files = normalizeSkillImportEntries(state.pluginImportFiles);
  if (!files.length) {
    throw new Error("请先选择要导入的插件目录。");
  }
  return {
    files: await Promise.all(
      files.map(async (item) => ({
        path: item.path,
        content_base64: await readFileAsBase64(item.file),
      })),
    ),
  };
}

async function scanSelectedPluginImportFiles() {
  const payload = await buildPluginUploadPayload();
  setPluginImportBusy(true);
  try {
    const result = await api("/api/agent-center/plugins/scan-upload", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    showResult(pluginImportResult, result);
    return result;
  } finally {
    setPluginImportBusy(false);
  }
}

function resetPluginImportState() {
  state.pluginImportFiles = [];
  state.pluginImportDragActive = false;
  state.pluginImportBusy = false;
  state.pluginImportScanResult = null;
  if (pluginImportDirectoryInput) {
    pluginImportDirectoryInput.value = "";
  }
  renderPluginImportSelection();
}

function pluginImportTargetLabel() {
  return String(state.pluginImportTargetName || state.pluginImportTargetKey || "\u5f53\u524d\u63d2\u4ef6").trim();
}

function setPluginImportMode(mode, plugin = null) {
  const isReupload = mode === "reupload";
  const manifest = plugin && typeof plugin === "object" ? { ...(plugin.manifest_json || plugin.manifest || {}) } : {};
  state.pluginImportMode = isReupload ? "reupload" : "import";
  state.pluginImportTargetId = isReupload ? String(plugin?.id || "").trim() : null;
  state.pluginImportTargetKey = isReupload ? String(plugin?.key || manifest.key || "").trim() : "";
  state.pluginImportTargetVersion = isReupload ? String(plugin?.version || manifest.version || "v1").trim() || "v1" : "";
  state.pluginImportTargetName = isReupload ? String(pluginDisplayName(plugin || { name: manifest.name || manifest.key || "" }) || "").trim() : "";
  state.pluginImportScanResult = null;
  renderPluginImportModeUi();
}

function renderPluginImportModeUi() {
  const isReupload = state.pluginImportMode === "reupload";
  const hasFiles = Array.isArray(state.pluginImportFiles) && state.pluginImportFiles.length > 0;
  const targetLabel = pluginImportTargetLabel();
  if (pluginImportModalTitle) {
    pluginImportModalTitle.textContent = isReupload
      ? `\u91cd\u65b0\u4e0a\u4f20\u63d2\u4ef6: ${targetLabel}`
      : "\u65b0\u589e / \u5bfc\u5165\u63d2\u4ef6";
  }
  if (pluginImportDropzone) {
    pluginImportDropzone.setAttribute(
      "aria-label",
      isReupload
        ? `\u91cd\u65b0\u4e0a\u4f20\u63d2\u4ef6 ${targetLabel}\uff0c\u6216\u5c06\u63d2\u4ef6\u76ee\u5f55\u62d6\u5230\u8fd9\u91cc`
        : "\u70b9\u51fb\u9009\u62e9\u63d2\u4ef6\u76ee\u5f55\uff0c\u6216\u5c06\u63d2\u4ef6\u76ee\u5f55\u62d6\u5230\u8fd9\u91cc",
    );
  }
  if (pluginImportDropzoneTitle) {
    pluginImportDropzoneTitle.textContent = isReupload ? "\u70b9\u51fb\u91cd\u65b0\u4e0a\u4f20\u63d2\u4ef6\u76ee\u5f55" : "\u70b9\u51fb\u4e0a\u4f20\u63d2\u4ef6\u76ee\u5f55";
  }
  if (pluginImportDropzoneHint) {
    pluginImportDropzoneHint.textContent = isReupload
      ? `\u4ec5\u5141\u8bb8\u4e0a\u4f20\u4e0e ${targetLabel} Key / Version \u4e00\u81f4\u7684\u63d2\u4ef6\u5305`
      : "\u652f\u6301\u5355\u4e2a\u63d2\u4ef6\u76ee\u5f55\uff0c\u6216\u5305\u542b\u591a\u4e2a\u63d2\u4ef6\u76ee\u5f55\u7684\u96c6\u5408\u76ee\u5f55";
  }
  if (pluginImportDropzoneTrigger) {
    pluginImportDropzoneTrigger.textContent = isReupload ? "\u91cd\u65b0\u9009\u62e9\u6587\u4ef6\u5939" : "\u9009\u62e9\u6587\u4ef6\u5939";
  }
  if (pluginImportValidate) {
    pluginImportValidate.disabled = !hasFiles || state.pluginImportBusy;
  }
  if (pluginImportSave) {
    pluginImportSave.textContent = isReupload ? "\u91cd\u65b0\u4e0a\u4f20\u5e76\u8986\u76d6" : "\u5bfc\u5165\u63d2\u4ef6";
    pluginImportSave.disabled = !hasFiles || state.pluginImportBusy;
  }
}

function renderPluginImportSelection() {
  pluginImportDropzone?.classList.toggle("drag-active", Boolean(state.pluginImportDragActive));
  const files = Array.isArray(state.pluginImportFiles) ? state.pluginImportFiles : [];
  if (!pluginImportSelection) {
    renderPluginImportModeUi();
    return;
  }
  if (!files.length) {
    pluginImportSelection.classList.add("empty");
    pluginImportSelection.innerHTML = `
      <span class="skill-import-selection-placeholder">\u672a\u9009\u62e9\u6587\u4ef6\u5939</span>
    `;
    renderPluginImportModeUi();
    return;
  }
  const roots = Array.from(new Set(files.map((item) => item.path.split("/")[0] || item.path)));
  const previewRoots = roots.slice(0, 3);
  const pluginYamlCount = files.filter((item) => item.path.split("/").pop() === "plugin.yaml").length;
  const suffix = previewRoots.length < roots.length ? " ..." : "";
  pluginImportSelection.classList.remove("empty");
  pluginImportSelection.innerHTML = `
    <strong title="${escapeAttribute(previewRoots.join(" / ") + suffix)}">${escapeHtml(previewRoots.join(" / ") + suffix)}</strong>
    <div class="skill-import-selection-meta">
      <span class="skill-import-selection-stat">${escapeHtml(`${files.length} \u4e2a\u6587\u4ef6`)}</span>
      <span class="skill-import-selection-stat">${escapeHtml(`${pluginYamlCount} \u4e2a plugin.yaml`)}</span>
    </div>
  `;
  renderPluginImportModeUi();
}

function setPluginImportFiles(entries) {
  state.pluginImportFiles = normalizeSkillImportEntries(entries);
  state.pluginImportDragActive = false;
  state.pluginImportScanResult = null;
  hideResult(pluginImportResult);
  renderPluginImportSelection();
}

function setPluginImportBusy(busy) {
  state.pluginImportBusy = Boolean(busy);
  pluginImportDropzone?.classList.toggle("busy", state.pluginImportBusy);
  if (pluginImportDirectoryInput) {
    pluginImportDirectoryInput.disabled = state.pluginImportBusy;
  }
  renderPluginImportModeUi();
}

function openPluginUploadModal() {
  setPluginImportMode("import");
  resetPluginImportState();
  hideResult(pluginImportResult);
  openPluginImportModal();
  pluginImportDropzone?.focus();
}

function openPluginReuploadModal(plugin) {
  const manifest = { ...((plugin && typeof plugin === "object" ? plugin.manifest_json || plugin.manifest || {} : {}) || {}) };
  const pluginKey = String(plugin?.key || manifest.key || "").trim();
  if (!plugin?.id || !pluginKey) {
    throw new Error("\u5f53\u524d\u63d2\u4ef6\u7f3a\u5c11 Key \u6216 ID\uff0c\u65e0\u6cd5\u91cd\u65b0\u4e0a\u4f20\u3002");
  }
  setPluginImportMode("reupload", plugin);
  resetPluginImportState();
  hideResult(pluginImportResult);
  openPluginImportModal();
  pluginImportDropzone?.focus();
}

async function buildPluginUploadPayload() {
  const files = normalizeSkillImportEntries(state.pluginImportFiles);
  if (!files.length) {
    throw new Error("\u8bf7\u5148\u9009\u62e9\u8981\u5bfc\u5165\u7684\u63d2\u4ef6\u76ee\u5f55\u3002");
  }
  return {
    files: await Promise.all(
      files.map(async (item) => ({
        path: item.path,
        content_base64: await readFileAsBase64(item.file),
      })),
    ),
  };
}

function ensurePluginImportMatchesTarget(scan) {
  state.pluginImportScanResult = scan || null;
  if (state.pluginImportMode !== "reupload") {
    return scan;
  }
  const targetId = String(state.pluginImportTargetId || "").trim();
  const targetKey = String(state.pluginImportTargetKey || "").trim();
  const targetVersion = String(state.pluginImportTargetVersion || "v1").trim() || "v1";
  const targetLabel = pluginImportTargetLabel();
  const plugins = Array.isArray(scan?.plugins) ? scan.plugins : [];
  const validPlugins = plugins.filter((item) => item?.is_valid && item?.manifest);
  if (!validPlugins.length) {
    throw new Error("\u91cd\u65b0\u4e0a\u4f20\u672a\u68c0\u6d4b\u5230\u53ef\u7528\u63d2\u4ef6\u5305\u3002");
  }
  if (validPlugins.length !== 1) {
    throw new Error("\u91cd\u65b0\u4e0a\u4f20\u53ea\u80fd\u5305\u542b 1 \u4e2a\u63d2\u4ef6\u5305\u3002");
  }
  const pluginScan = validPlugins[0];
  const manifest = { ...(pluginScan.manifest || {}) };
  const uploadKey = String(manifest.key || "").trim();
  const uploadVersion = String(manifest.version || "v1").trim() || "v1";
  if (!uploadKey || uploadKey !== targetKey || uploadVersion !== targetVersion) {
    throw new Error(
      `\u91cd\u65b0\u4e0a\u4f20\u53ea\u80fd\u8986\u76d6\u5f53\u524d\u63d2\u4ef6\u3002\u76ee\u6807\u662f ${targetLabel} (${targetKey}@${targetVersion})\uff0c\u4f46\u4e0a\u4f20\u5305\u662f ${uploadKey || "-"}@${uploadVersion}\u3002`,
    );
  }
  const existingPluginId = String(pluginScan.existing_plugin_id || "").trim();
  if (existingPluginId && targetId && existingPluginId !== targetId) {
    throw new Error("\u5f53\u524d\u4e0a\u4f20\u5305\u4f1a\u8986\u76d6\u5176\u4ed6\u63d2\u4ef6\uff0c\u8bf7\u68c0\u67e5\u4e0a\u4f20\u5185\u5bb9\u3002");
  }
  return scan;
}

async function scanSelectedPluginImportFiles() {
  const payload = await buildPluginUploadPayload();
  setPluginImportBusy(true);
  try {
    const result = await api("/api/agent-center/plugins/scan-upload", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.pluginImportScanResult = result;
    ensurePluginImportMatchesTarget(result);
    showResult(pluginImportResult, result);
    return result;
  } finally {
    setPluginImportBusy(false);
  }
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
  populateProviderTypeOptions(provider.provider_type || "");
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
  pluginName.value = plugin.name || "";
  pluginDescription.value = plugin.description || "";
  setPluginEditorState({
    manifest,
    config: plugin.config_json || {},
    secretFieldPaths: plugin.secret_field_paths || [],
    record: plugin,
  });
  pluginModalTitle.textContent = "编辑插件";
  hideResult(pluginModalResult);
  openPluginModal();
}

function resetSkillForm({ openModal = false } = {}) {
  state.editingSkillId = null;
  setSkillModalMode("import");
  resetSkillImportState();
  renderSkillImportGroupOptions([]);
  hideResult(skillModalResult);
  renderSkills();
  if (openModal) {
    openSkillModal();
  }
}

function fillSkillForm(skill, { openModal = false } = {}) {
  const groups = skillGroupsForItem(skill);
  state.editingSkillId = skill.id;
  setSkillModalMode("reupload");
  resetSkillImportState();
  renderSkillImportGroupOptions(groups.map((item) => item.id).filter(Boolean));
  hideResult(skillModalResult);
  renderSkills();
  if (openModal) {
    openSkillModal();
  }
}

function setSkillModalMode(mode) {
  state.skillModalMode = mode === "reupload" ? "reupload" : "import";
  const isImportMode = state.skillModalMode === "import";
  skillImportPanel?.classList.toggle("hidden", false);
  skillEditorPanel?.classList.toggle("hidden", true);
  if (skillModalTitle) {
    skillModalTitle.textContent = isImportMode ? "导入 Skill" : "重新上传 Skill";
  }
  if (skillImportDropzone) {
    skillImportDropzone.setAttribute(
      "aria-label",
      isImportMode ? "点击选择 Skill 文件夹，或将文件夹拖到这里" : "点击重新选择 Skill 文件夹，或将新文件夹拖到这里",
    );
  }
  if (skillImportDropzoneTitle) {
    skillImportDropzoneTitle.textContent = isImportMode ? "点击上传文件夹" : "点击重新上传文件夹";
  }
  if (skillImportDropzoneHint) {
    skillImportDropzoneHint.textContent = isImportMode ? "或拖拽文件夹到这里" : "或拖拽新的文件夹到这里";
  }
  if (skillImportDropzoneTrigger) {
    skillImportDropzoneTrigger.textContent = isImportMode ? "选择文件夹" : "重新选择文件夹";
  }
  if (skillValidate) {
    skillValidate.classList.toggle("hidden", false);
    skillValidate.disabled = !state.skillImportFiles.length || state.skillImportBusy;
  }
  if (skillSave) {
    skillSave.textContent = isImportMode ? "导入 Skill" : "重新上传并覆盖";
    skillSave.disabled = !state.skillImportFiles.length || state.skillImportBusy;
  }
}

function normalizeSkillImportPath(path) {
  return String(path || "")
    .replace(/\\/g, "/")
    .replace(/^\/+/, "")
    .trim();
}

function normalizeSkillImportEntries(entries) {
  const seen = new Set();
  return (entries || [])
    .map((item) => ({
      file: item?.file || null,
      path: normalizeSkillImportPath(item?.path || item?.relativePath || item?.file?.webkitRelativePath || item?.file?.name || ""),
    }))
    .filter((item) => item.file && item.path)
    .filter((item) => {
      if (seen.has(item.path)) {
        return false;
      }
      seen.add(item.path);
      return true;
    })
    .sort((left, right) => left.path.localeCompare(right.path));
}

function resetSkillImportState() {
  state.skillImportFiles = [];
  state.skillImportDragActive = false;
  state.skillImportBusy = false;
  if (skillImportDirectoryInput) {
    skillImportDirectoryInput.value = "";
  }
  renderSkillImportSelection();
  setSkillModalMode(state.skillModalMode);
}

function renderSkillImportSelection() {
  skillImportDropzone?.classList.toggle("drag-active", Boolean(state.skillImportDragActive));
  const files = Array.isArray(state.skillImportFiles) ? state.skillImportFiles : [];
  if (!skillImportSelection) {
    setSkillModalMode(state.skillModalMode);
    return;
  }
  if (!files.length) {
    skillImportSelection.classList.add("empty");
    skillImportSelection.innerHTML = `
      <span class="skill-import-selection-placeholder">未选择文件夹</span>
    `;
    setSkillModalMode(state.skillModalMode);
    return;
  }
  const roots = Array.from(new Set(files.map((item) => item.path.split("/")[0] || item.path))).slice(0, 3);
  const skillMdCount = files.filter((item) => item.path.split("/").pop() === "SKILL.md").length;
  const suffix = roots.length < new Set(files.map((item) => item.path.split("/")[0] || item.path)).size ? " ..." : "";
  skillImportSelection.classList.remove("empty");
  skillImportSelection.innerHTML = `
    <strong title="${escapeAttribute(roots.join(" / ") + suffix)}">${escapeHtml(roots.join(" / ") + suffix)}</strong>
    <div class="skill-import-selection-meta">
      <span class="skill-import-selection-stat">${escapeHtml(`${files.length} 个文件`)}</span>
      <span class="skill-import-selection-stat">${escapeHtml(`${skillMdCount} 个 Skill`)}</span>
    </div>
  `;
  setSkillModalMode(state.skillModalMode);
}

function setSkillImportFiles(entries) {
  state.skillImportFiles = normalizeSkillImportEntries(entries);
  state.skillImportDragActive = false;
  hideResult(skillModalResult);
  renderSkillImportSelection();
}

function setSkillImportBusy(busy) {
  state.skillImportBusy = Boolean(busy);
  skillImportDropzone?.classList.toggle("busy", state.skillImportBusy);
  if (skillImportDirectoryInput) {
    skillImportDirectoryInput.disabled = state.skillImportBusy;
  }
  setSkillModalMode(state.skillModalMode);
}

function openSkillImportModal() {
  state.editingSkillId = null;
  setSkillModalMode("import");
  resetSkillImportState();
  renderSkillImportGroupOptions([]);
  hideResult(skillModalResult);
  openSkillModal();
  skillImportDropzone?.focus();
}

function readFileAsBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      const index = result.indexOf(",");
      resolve(index >= 0 ? result.slice(index + 1) : result);
    };
    reader.onerror = () => reject(reader.error || new Error("读取文件失败。"));
    reader.readAsDataURL(file);
  });
}

async function buildSkillUploadPayload() {
  const files = normalizeSkillImportEntries(state.skillImportFiles);
  if (!files.length) {
    throw new Error("请先选择要导入的 Skill 文件夹。");
  }
  const groupIds = skillImportGroups ? getMultiSelectValues(skillImportGroups).map((value) => String(value || "").trim()).filter(Boolean) : [];
  return {
    recursive: true,
    group_ids: groupIds,
    target_skill_id: String(state.editingSkillId || "").trim(),
    files: await Promise.all(
      files.map(async (item) => ({
        path: item.path,
        content_base64: await readFileAsBase64(item.file),
      })),
    ),
  };
}

function readDirectoryEntries(reader) {
  return new Promise((resolve, reject) => {
    const items = [];
    const next = () => {
      reader.readEntries(
        (batch) => {
          if (!batch.length) {
            resolve(items);
            return;
          }
          items.push(...batch);
          next();
        },
        (error) => reject(error || new Error("读取目录失败。")),
      );
    };
    next();
  });
}

async function collectSkillImportEntriesFromEntry(entry, prefix = "") {
  if (!entry) {
    return [];
  }
  if (entry.isFile) {
    return new Promise((resolve, reject) => {
      entry.file(
        (file) => resolve([{ file, path: normalizeSkillImportPath(`${prefix}${file.name}`) }]),
        (error) => reject(error || new Error("读取拖入文件失败。")),
      );
    });
  }
  if (!entry.isDirectory) {
    return [];
  }
  const children = await readDirectoryEntries(entry.createReader());
  let files = [];
  for (const child of children) {
    files = files.concat(await collectSkillImportEntriesFromEntry(child, `${prefix}${entry.name}/`));
  }
  return files;
}

async function collectSkillImportEntriesFromInput(fileList) {
  return normalizeSkillImportEntries(
    Array.from(fileList || []).map((file) => ({
      file,
      path: normalizeSkillImportPath(file.webkitRelativePath || file.name),
    })),
  );
}

async function collectSkillImportEntriesFromDataTransfer(dataTransfer) {
  const items = Array.from(dataTransfer?.items || []);
  const entries = items.map((item) => (typeof item.webkitGetAsEntry === "function" ? item.webkitGetAsEntry() : null)).filter(Boolean);
  if (entries.length) {
    let files = [];
    for (const entry of entries) {
      files = files.concat(await collectSkillImportEntriesFromEntry(entry));
    }
    return normalizeSkillImportEntries(files);
  }
  return collectSkillImportEntriesFromInput(dataTransfer?.files || []);
}

async function scanSelectedSkillImportFiles() {
  const payload = await buildSkillUploadPayload();
  setSkillImportBusy(true);
  try {
    const result = await api("/api/agent-center/skills/scan-upload", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    showResult(skillModalResult, result);
    return result;
  } finally {
    setSkillImportBusy(false);
  }
}

function renderSkillManagementView() {
  const isSkillsView = state.skillManagementView !== "groups";
  skillManagementSkillsView?.classList.toggle("hidden", !isSkillsView);
  skillManagementGroupsView?.classList.toggle("hidden", isSkillsView);
  skillViewSkills?.classList.toggle("active", isSkillsView);
  skillViewSkills?.classList.toggle("ghost", !isSkillsView);
  skillViewGroups?.classList.toggle("active", !isSkillsView);
  skillViewGroups?.classList.toggle("ghost", isSkillsView);
  if (skillOpenCreate) {
    skillOpenCreate.textContent = "新增";
    skillOpenCreate.dataset.mode = isSkillsView ? "skill" : "group";
  }
}

function setSkillManagementView(view) {
  state.skillManagementView = view === "groups" ? "groups" : "skills";
  renderSkillManagementView();
}

function focusSkillGroupPageOn(groupId) {
  const targetId = String(groupId || "").trim();
  if (!targetId) {
    return;
  }
  const index = state.skillGroupCatalog.findIndex((item) => String(item.id || "") === targetId);
  if (index < 0) {
    return;
  }
  const limit = Math.max(1, Number(state.skillGroupPage.limit || 10));
  state.skillGroupPage.offset = Math.floor(index / limit) * limit;
}

function openSkillGroupModal() {
  skillGroupModal?.classList.remove("hidden");
}

function closeSkillGroupModal() {
  skillGroupModal?.classList.add("hidden");
  hideResult(skillGroupModalResult);
}

function resetSkillGroupForm() {
  state.editingSkillGroupId = null;
  if (skillGroupManagementName) {
    skillGroupManagementName.value = "";
  }
  if (skillGroupManagementDescription) {
    skillGroupManagementDescription.value = "";
  }
  renderSkillGroupSkillOptions([]);
  if (skillGroupModalTitle) {
    skillGroupModalTitle.textContent = "新增分组";
  }
  hideResult(skillGroupModalResult);
  renderSkillGroupManagementList();
}

function fillSkillGroupForm(group) {
  state.editingSkillGroupId = group.id || null;
  if (skillGroupManagementName) {
    skillGroupManagementName.value = group.name || "";
  }
  if (skillGroupManagementDescription) {
    skillGroupManagementDescription.value = group.description || "";
  }
  renderSkillGroupSkillOptions(skillIdsForGroup(group));
  if (skillGroupModalTitle) {
    skillGroupModalTitle.textContent = "编辑分组";
  }
  hideResult(skillGroupModalResult);
  renderSkillGroupManagementList();
}

async function openSkillGroupManagementView(groupId = "") {
  await switchPage("skills");
  await ensureSkillGroupCatalog(true);
  focusSkillGroupPageOn(groupId);
  await ensureSkillGroupManagementPage(true);
  state.editingSkillGroupId = groupId || null;
  renderSkillGroupManagementList();
  setSkillManagementView("groups");
}

function renderSkillGroupManagementList() {
  if (!skillGroupManagementList) {
    return;
  }
  const items = Array.isArray(state.skillGroupPage.items) ? state.skillGroupPage.items : [];
  skillGroupManagementList.innerHTML = items.length
    ? items
        .map((group) => {
          const active = Boolean(group.id) && group.id === state.editingSkillGroupId;
          const countText = `${group.count || 0}`;
          const canManage = Boolean(group.id);
          return `
            <article class="provider-row skill-group-management-row${active ? " active" : ""}">
              <div class="provider-main">
                <strong title="${escapeAttribute(group.name || group.key || group.id || "-")}">${escapeHtml(group.name || group.key || group.id || "-")}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(group.description || "未填写分组简介")}">${escapeHtml(group.description || "未填写分组简介")}</strong>
              </div>
              <div class="provider-cell">
                <strong>${escapeHtml(countText)}</strong>
              </div>
              <div class="provider-row-actions">
                <button type="button" data-skill-group-edit="${escapeAttribute(group.id || "")}" ${canManage ? "" : "disabled"}>编辑</button>
                <button type="button" class="ghost warn" data-skill-group-delete="${escapeAttribute(group.id || "")}" ${canManage ? "" : "disabled"}>删除</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail"><strong>暂无 Skill 分组</strong><p>先创建一个分组，再把 Skill 归进去。</p></div>';
  renderOffsetPagination(state.skillGroupPage, skillGroupPaginationMeta, "skill-group-page");
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
    ["本地模型", state.summary.local_model_count || 0],
    ["插件", state.summary.plugin_count || 0],
    ["技能", state.summary.skill_count || 0],
    ["角色管理", state.summary.static_memory_count || 0],
    ["知识库", state.summary.knowledge_base_count || 0],
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
          const description = String(item.description || manifest.description || "").trim() || "\u6682\u65e0\u8bf4\u660e";
          const actionNames = Array.isArray(manifest.actions)
            ? manifest.actions.map((entry) => String((entry || {}).name || "").trim()).filter(Boolean)
            : [];
          const actionText = actionNames.length ? actionNames.join(", ") : "\u65e0\u52a8\u4f5c";
          return `
            <article class="resource-row plugin-row">
              <div class="resource-main">
                <strong title="${escapeAttribute(pluginDisplayName(item))}">${escapeHtml(pluginDisplayName(item))}</strong>
              </div>
              <div class="resource-cell">
                <strong title="${escapeAttribute(description)}">${escapeHtml(description)}</strong>
              </div>
              <div class="resource-cell">
                <strong title="${escapeAttribute(actionText)}">${escapeHtml(actionText)}</strong>
              </div>
              <div class="resource-row-actions">
                <button type="button" data-plugin-edit="${item.id}">\u7f16\u8f91</button>
                <button type="button" class="ghost" data-plugin-validate="${item.id}">\u6821\u9a8c</button>
                <button type="button" class="ghost" data-plugin-reupload="${item.id}">\u91cd\u65b0\u4e0a\u4f20</button>
                <button type="button" class="ghost warn" data-plugin-delete="${item.id}">\u5220\u9664</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail">\u6682\u65e0\u63d2\u4ef6\uff0c\u5148\u5bfc\u5165\u4e00\u4e2a\u63d2\u4ef6\u3002</div>';
  renderOffsetPagination(state.pluginPage, pluginPaginationMeta, "plugin-page");
}

function renderSkills() {
  if (!skillList) {
    return;
  }
  skillList.innerHTML = state.skillPage.items.length
      ? state.skillPage.items
        .map((item) => {
          const groups = skillGroupsForItem(item);
          const groupNameSummary = groups.length ? groups.map((entry) => entry.name || entry.key || "-").join("、") : "未分组";
          const description = String(item.description || "").trim() || "暂无简介";
          return `
            <article class="provider-row skill-management-row">
              <div class="provider-main">
                <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(groupNameSummary)}">${escapeHtml(groupNameSummary)}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(description)}">${escapeHtml(description)}</strong>
              </div>
              <div class="provider-row-actions skill-row-actions">
                <button type="button" class="ghost" data-skill-preview="${item.id}">预览</button>
                <button type="button" data-skill-edit="${item.id}">重新上传</button>
                <button type="button" class="ghost warn" data-skill-delete="${item.id}">删除</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail"><strong>暂无 Skill</strong><p>先创建第一个 Skill，并为它归一个分组。</p></div>';
  renderOffsetPagination(state.skillPage, skillPaginationMeta, "skill-page");
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
  showResult(teamTemplateResult, { error: "Legacy 团队模板已移除。" });
}

async function previewTeamGraph() {
  showResult(teamTemplateResult, { error: "Legacy 团队模板已移除。" });
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
            active: item.id === state.editingTeamTemplateId,
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
  teamDefinitionList.innerHTML = state.teamDefinitionPage.items.length
    ? state.teamDefinitionPage.items
        .map((item) => {
          const spec = dictOrEmpty(item.spec_json);
          const hierarchy = teamDefinitionHierarchySpec(spec);
          const directChildCount = hierarchy.children.length || hierarchy.legacyMembers.length;
          const childAgentCount = hierarchy.children.filter((child) => child.kind === "agent").length;
          const childTeamCount = hierarchy.children.filter((child) => child.kind === "team").length;
          const description = item.description || "未填写团队简介";
          const leadLabel = hierarchy.lead.source_ref ? teamDefinitionMemberSourceLabel(hierarchy.lead) : "未配置 Lead Agent";
          const leadSummary = hierarchy.legacyMembers.length && !hierarchy.lead.source_ref ? "旧版团队定义" : leadLabel;
          const subagentSummary =
            hierarchy.children.length || hierarchy.lead.source_ref
              ? `直属 ${directChildCount} / Agent ${childAgentCount} / Team ${childTeamCount}`
              : hierarchy.legacyMembers.length
                ? `旧版成员 ${hierarchy.legacyMembers.length}`
                : "未配置";
          return `
            <article class="provider-row team-management-row">
              <div class="provider-main">
                <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(description)}">${escapeHtml(description)}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(leadSummary)}">${escapeHtml(leadSummary)}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(subagentSummary)}">${escapeHtml(subagentSummary)}</strong>
              </div>
              <div class="provider-row-actions">
                <button type="button" data-team-definition-edit="${item.id}">编辑</button>
                <button type="button" class="ghost warn" data-team-definition-delete="${item.id}">删除</button>
                <button type="button" class="ghost" data-team-definition-test="${item.id}">测试</button>
                <button type="button" class="ghost" data-team-definition-preview="${item.id}">预览</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail"><strong>暂无团队管理项</strong><p>先创建第一个团队。</p></div>';
  renderOffsetPagination(state.teamDefinitionPage, teamDefinitionPaginationMeta, "team-definition-page");
}

function teamDefinitionPreviewMessageMarkup(title, body, tone = "") {
  return `
    <article class="team-preview-empty${tone ? ` ${tone}` : ""}">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(body)}</p>
    </article>
  `;
}

function teamDefinitionPreviewSummaryMarkup(preview) {
  if (!preview || typeof preview !== "object") {
    return "";
  }
  const items = [];
  if ("team_count" in preview) {
    items.push(chip("Team", preview.team_count || 0));
  }
  if ("agent_count" in preview) {
    items.push(chip("Agent", preview.agent_count || 0));
  }
  if ("node_count" in preview) {
    items.push(chip("节点", preview.node_count || 0));
  }
  if ("edge_count" in preview) {
    items.push(chip("边", preview.edge_count || 0));
  }
  if (preview.hierarchy_mode) {
    items.push(chip("结构", preview.hierarchy_mode));
  }
  if (preview.execution_mode) {
    items.push(chip("模式", preview.execution_mode));
  }
  return items.join("");
}

function teamDefinitionPreviewErrorText(error) {
  const payload = errorResult(error);
  const fallback =
    (typeof payload?.error === "string" && payload.error.trim()) || (typeof error?.message === "string" && error.message.trim()) || "预览生成失败。";
  return formatApiError(payload, fallback);
}

function teamHierarchyNodeType(node) {
  return String(node?.node_type || "agent").trim().toLowerCase() === "team" ? "team" : "agent";
}

function teamHierarchyNodeTitle(node) {
  return node?.name || node?.delegate_name || node?.runtime_key || node?.key || "未命名节点";
}

function teamHierarchyGraphCardMarkup(node, { isRoot = false } = {}) {
  const title = teamHierarchyNodeTitle(node);
  return `
    <article class="team-preview-graph-card${isRoot ? " root" : ""}" title="${escapeAttribute(title)}">
      <strong class="team-preview-graph-title">${escapeHtml(title)}</strong>
    </article>
  `;
}

function teamHierarchyGraphAgentMarkup(node, { isRoot = false } = {}) {
  return `<div class="team-preview-graph-node-shell">${teamHierarchyGraphCardMarkup(node, { isRoot })}</div>`;
}

function teamHierarchyGraphTeamMarkup(node, { isRoot = false } = {}) {
  if (!node || typeof node !== "object") {
    return "";
  }
  const teamName = teamHierarchyNodeTitle(node);
  const leadNode = node.lead && typeof node.lead === "object" ? node.lead : null;
  const children = Array.isArray(node.children) ? node.children : [];
  const childMarkup = children
    .map((child) => `<div class="team-preview-graph-child">${teamHierarchyGraphNodeMarkup(child)}</div>`)
    .join("");
  return `
    <section class="team-preview-team-box${isRoot ? " root" : ""}">
      <span class="team-preview-team-label">${escapeHtml(teamName)}</span>
      <div class="team-preview-team-content">
        <div class="team-preview-team-lead">
          ${
            leadNode
              ? teamHierarchyGraphAgentMarkup(leadNode, { isRoot })
              : `<div class="team-preview-graph-node-shell">${teamHierarchyGraphCardMarkup(node, { isRoot })}</div>`
          }
        </div>
        ${
          childMarkup
            ? `
              <div class="team-preview-graph-branch">
                <div class="team-preview-graph-children">
                  ${childMarkup}
                </div>
              </div>
            `
            : ""
        }
      </div>
    </section>
  `;
}

function teamHierarchyGraphNodeMarkup(node, { isRoot = false } = {}) {
  if (!node || typeof node !== "object") {
    return "";
  }
  const nodeType = teamHierarchyNodeType(node);
  if (nodeType === "team") {
    return teamHierarchyGraphTeamMarkup(node, { isRoot });
  }
  return teamHierarchyGraphAgentMarkup(node, { isRoot });
}

function renderTeamDefinitionPreview() {
  const payload = teamDefinitionPreviewState.payload || {};
  const loading = Boolean(teamDefinitionPreviewState.loading);
  const errorText = String(teamDefinitionPreviewState.errorText || "").trim();
  const teamDefinition = payload?.team_definition || {};
  const hierarchy = payload?.hierarchy && typeof payload.hierarchy === "object" ? payload.hierarchy : null;
  const title = teamDefinition.name || hierarchy?.name || "团队树预览";
  const description = String(teamDefinition.description || hierarchy?.description || "").trim();
  const summaryMarkup = loading ? chip("状态", "生成中") : errorText ? chip("状态", "失败") : teamDefinitionPreviewSummaryMarkup(payload?.preview) || chip("状态", "已生成");

  if (teamDefinitionPreviewTitle) {
    teamDefinitionPreviewTitle.textContent = `${title} · 团队树预览`;
  }
  if (teamDefinitionPreviewSummary) {
    teamDefinitionPreviewSummary.innerHTML = `
      ${description ? `<p class="team-preview-description">${escapeHtml(description)}</p>` : ""}
      <div class="team-preview-chip-row">${summaryMarkup}</div>
    `;
  }
  if (!teamDefinitionPreviewTree) {
    return;
  }
  if (loading) {
    teamDefinitionPreviewTree.innerHTML = teamDefinitionPreviewMessageMarkup(
      "正在生成树状结构",
      "将基于当前团队定义递归展开 Team、Lead Agent 和所有 SubAgent。",
    );
    return;
  }
  if (errorText) {
    teamDefinitionPreviewTree.innerHTML = teamDefinitionPreviewMessageMarkup("预览生成失败", errorText, "error");
    return;
  }
  if (!hierarchy) {
    teamDefinitionPreviewTree.innerHTML = teamDefinitionPreviewMessageMarkup(
      "暂无树状结构",
      "当前团队编译结果没有返回 hierarchy 数据。",
    );
    return;
  }
  teamDefinitionPreviewTree.classList.add("graph-view");
  teamDefinitionPreviewTree.innerHTML = `<div class="team-preview-graph-stage">${teamHierarchyGraphNodeMarkup(hierarchy, { isRoot: true })}</div>`;
}

function renderRuns() {
  renderOffsetPagination(state.runPage, runPaginationMeta, "run-page");
  runList.innerHTML = state.runPage.items.length
    ? state.runPage.items
        .map((item) => {
          const title = item.task_title || item.summary || item.id;
          const subtitle = item.task_prompt || item.id;
          const blueprint = item.blueprint_name || item.blueprint_id || "-";
          const statusSummary = item.current_node_id ? `${item.status} / ${item.current_node_id}` : item.status;
          const updatedAt = formatTeamChatTime(item.updated_at) || item.updated_at || "-";
          return `
            <article class="provider-row runtime-row${item.id === state.selectedRunId ? " active" : ""}">
              <div class="provider-main">
                <strong title="${escapeAttribute(title)}">${escapeHtml(title)}</strong>
                <span title="${escapeAttribute(subtitle)}">${escapeHtml(subtitle)}</span>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(blueprint)}">${escapeHtml(blueprint)}</strong>
                <span title="${escapeAttribute(item.blueprint_description || "")}">${escapeHtml(item.blueprint_description || "运行蓝图")}</span>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(statusSummary)}">${escapeHtml(item.status || "-")}</strong>
                <span title="${escapeAttribute(statusSummary)}">${escapeHtml(item.current_node_id || "待处理")}</span>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(updatedAt)}">${escapeHtml(updatedAt)}</strong>
                <span title="${escapeAttribute(item.started_at || "")}">${escapeHtml(item.started_at ? `开始于 ${formatTeamChatTime(item.started_at) || item.started_at}` : "未开始")}</span>
              </div>
              <div class="provider-row-actions">
                ${
                  item.status === "waiting_approval"
                    ? `<button type="button" class="warn" data-run-resume="${item.id}">恢复</button>`
                    : ""
                }
                <button type="button" class="ghost" data-run-open="${item.id}">查看</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail"><strong>暂无 Run</strong><p>启动任务后显示。</p></div>';
}

function renderApprovalsLegacy() {
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

function approvalStatusLabel(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "approved") {
    return "已通过";
  }
  if (normalized === "rejected") {
    return "已拒绝";
  }
  return "待审批";
}

function approvalStatusBadge(status) {
  const normalized = String(status || "").trim().toLowerCase() || "pending";
  return `<span class="approval-status-badge ${escapeHtml(normalized)}">${escapeHtml(approvalStatusLabel(normalized))}</span>`;
}

function approvalResultSummary(item) {
  const resolution = dictOrEmpty(item?.resolution_json);
  const comment = String(resolution.comment || "").trim();
  const createdAt = formatTeamChatTime(item?.created_at) || item?.created_at || "-";
  const resolvedAt = formatTeamChatTime(item?.resolved_at) || item?.resolved_at || "";
  const statusLabel = approvalStatusLabel(item?.status);
  if (state.approvalPage.view === "history") {
    return {
      primary: statusLabel,
      secondary: [resolvedAt ? `审批时间 ${resolvedAt}` : "", comment ? `备注：${comment}` : ""].filter(Boolean).join(" / ") || "—",
    };
  }
  return {
    primary: statusLabel,
    secondary: `创建时间 ${createdAt}`,
  };
}

function approvalMetadata(item) {
  return dictOrEmpty(item?.metadata_json);
}

function approvalScope(item) {
  return String(approvalMetadata(item).scope || "").trim();
}

function approvalAllowedDecisions(item) {
  const metadata = approvalMetadata(item);
  const values = Array.isArray(metadata.allowed_decisions)
    ? metadata.allowed_decisions.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  return values.length ? Array.from(new Set(values)) : ["approve", "reject"];
}

function approvalSupportsDecision(item, decision) {
  const normalized = String(decision || "").trim();
  return approvalAllowedDecisions(item).includes(normalized);
}

function approvalActionRequests(item) {
  return Array.isArray(approvalMetadata(item).action_requests)
    ? approvalMetadata(item).action_requests.filter((entry) => isRecord(entry)).map((entry) => clone(entry))
    : [];
}

function approvalPendingResultText(item) {
  return String(approvalMetadata(item).pending_result_text || "").trim();
}

function approvalSupportsEdit(item) {
  if (!approvalSupportsDecision(item, "edit")) {
    return false;
  }
  const scope = approvalScope(item);
  if (scope === "tool_interrupt") {
    return approvalActionRequests(item).length > 0;
  }
  if (scope === "final_delivery") {
    return Boolean(approvalPendingResultText(item));
  }
  return false;
}

function approvalScopeLabel(item) {
  const scope = approvalScope(item);
  if (scope === "tool_interrupt") {
    return "工具调用";
  }
  if (scope === "final_delivery") {
    return "最终交付";
  }
  return "审批";
}

function approvalActionName(action, index) {
  return String(action?.name || action?.action || `动作 ${index + 1}`).trim() || `动作 ${index + 1}`;
}

function renderApprovalEditSummary(item) {
  const title = String(item?.title || item?.id || "-").trim() || "-";
  const runId = String(item?.run_id || "").trim();
  const scopeLabel = approvalScopeLabel(item);
  const summary = `${scopeLabel}${runId ? ` · Run ${runId}` : ""}`;
  return `
    <div class="approval-edit-summary">
      <strong>${escapeHtml(title)}</strong>
      <span>${escapeHtml(summary)}</span>
    </div>
  `;
}

function renderApprovalEditFields(item) {
  if (approvalScope(item) === "tool_interrupt") {
    const actions = approvalActionRequests(item);
    return actions
      .map((action, index) => {
        const args = action.args === undefined ? {} : action.args;
        return `
          <article class="approval-edit-card">
            <div class="approval-edit-card-head">
              <div>
                <strong>${escapeHtml(`动作 ${index + 1}`)}</strong>
                <span>${escapeHtml(approvalActionName(action, index))}</span>
              </div>
            </div>
            <label>
              <span>动作参数 JSON</span>
              <textarea data-approval-edit-action="${index}" rows="10">${escapeHtml(prettyJson(args))}</textarea>
            </label>
          </article>
        `;
      })
      .join("");
  }
  if (approvalScope(item) === "final_delivery") {
    return `
      <article class="approval-edit-card">
        <div class="approval-edit-card-head">
          <div>
            <strong>最终交付</strong>
            <span>编辑后的内容会作为最终回复继续发送。</span>
          </div>
        </div>
        <label>
          <span>交付内容</span>
          <textarea data-approval-edit-body rows="14">${escapeHtml(approvalPendingResultText(item))}</textarea>
        </label>
      </article>
    `;
  }
  return '<div class="detail empty compact-detail"><strong>当前审批不支持编辑</strong></div>';
}

function openApprovalEditModal(item) {
  const record = clone(item || {});
  state.approvalEditor.item = record;
  if (approvalEditModalTitle) {
    approvalEditModalTitle.textContent = approvalScope(record) === "final_delivery" ? "编辑最终交付" : "编辑工具调用";
  }
  if (approvalEditSummary) {
    approvalEditSummary.innerHTML = renderApprovalEditSummary(record);
  }
  if (approvalEditFields) {
    approvalEditFields.innerHTML = renderApprovalEditFields(record);
  }
  hideResult(approvalEditResult);
  approvalEditModal?.classList.remove("hidden");
}

function closeApprovalEditModal() {
  approvalEditModal?.classList.add("hidden");
  state.approvalEditor.item = null;
  if (approvalEditSummary) {
    approvalEditSummary.innerHTML = "";
  }
  if (approvalEditFields) {
    approvalEditFields.innerHTML = "";
  }
  hideResult(approvalEditResult);
}

function buildApprovalEditMetadata(item) {
  const scope = approvalScope(item);
  if (scope === "tool_interrupt") {
    const decisions = approvalActionRequests(item).map((action, index) => {
      const field = approvalEditFields?.querySelector(`[data-approval-edit-action="${index}"]`);
      if (!(field instanceof HTMLTextAreaElement)) {
        throw new Error(`找不到动作 ${index + 1} 的编辑输入框。`);
      }
      let parsedArgs;
      try {
        parsedArgs = safeParseJson(field.value, {});
      } catch (error) {
        throw new Error(`动作 ${index + 1} 的参数 JSON 无法解析。`);
      }
      if (!isRecord(parsedArgs)) {
        throw new Error(`动作 ${index + 1} 的参数必须是 JSON 对象。`);
      }
      return {
        type: "edit",
        edited_action: {
          ...clone(action),
          args: parsedArgs,
        },
      };
    });
    return { decisions };
  }
  if (scope === "final_delivery") {
    const field = approvalEditFields?.querySelector("[data-approval-edit-body]");
    if (!(field instanceof HTMLTextAreaElement)) {
      throw new Error("找不到最终交付的编辑输入框。");
    }
    const editedBody = field.value.trim();
    if (!editedBody) {
      throw new Error("最终交付内容不能为空。");
    }
    return { edited_body: editedBody };
  }
  throw new Error("当前审批不支持编辑。");
}

async function resolveApprovalAndResume({ approvalId, runId, approved, comment, metadata = {} }) {
  if (!approvalId) {
    throw new Error("审批记录不存在。");
  }
  if (!runId) {
    throw new Error("审批关联的 Run 不存在。");
  }
  await api(`/api/approvals/${approvalId}/resolve`, {
    method: "POST",
    body: JSON.stringify({ approved, comment, metadata }),
  });
  const payload = await api(`/api/runs/${runId}/resume`, {
    method: "POST",
    body: JSON.stringify({}),
  });
  state.selectedRunId = payload.run.id;
  invalidateData("runs", "approvals", "controlPlane");
  return payload;
}

function approvalRowMarkup(item) {
  const title = String(item?.title || item?.id || "-").trim() || "-";
  const detail = String(item?.detail || "").trim() || "无审批内容";
  const runId = String(item?.run_id || "").trim();
  const result = approvalResultSummary(item);
  const runAction = runId ? `<button type="button" class="ghost" data-run-open="${escapeAttribute(runId)}">查看 Run</button>` : "";
  const actionButtons =
    state.approvalPage.view === "history"
      ? [runAction]
      : [
          approvalSupportsDecision(item, "approve")
            ? `<button type="button" data-approval-approve="${escapeAttribute(item.id || "")}" data-run-id="${escapeAttribute(runId)}">批准</button>`
            : "",
          approvalSupportsEdit(item)
            ? `<button type="button" class="ghost" data-approval-edit="${escapeAttribute(item.id || "")}">编辑</button>`
            : "",
          approvalSupportsDecision(item, "reject")
            ? `<button type="button" class="ghost warn" data-approval-reject="${escapeAttribute(item.id || "")}" data-run-id="${escapeAttribute(runId)}">拒绝</button>`
            : "",
          runAction,
        ];
  const actions = actionButtons.filter(Boolean).join("") || "<span>—</span>";
  return `
    <article class="provider-row approval-row">
      <div class="provider-main">
        <strong title="${escapeAttribute(title)}">${escapeHtml(title)}</strong>
        <span title="${escapeAttribute(runId || "")}">${escapeHtml(runId ? `run ${runId}` : "-")}</span>
      </div>
      <div class="provider-cell">
        <strong title="${escapeAttribute(detail)}">${escapeHtml(detail)}</strong>
        <span title="${escapeAttribute(detail)}">${escapeHtml(detail)}</span>
      </div>
      <div class="provider-cell">
        <strong>${approvalStatusBadge(item?.status)}</strong>
        <span title="${escapeAttribute(result.secondary)}">${escapeHtml(result.secondary)}</span>
      </div>
      <div class="provider-row-actions">
        ${actions}
      </div>
    </article>
  `;
}

function renderApprovals() {
  if (approvalsPanelTitle) {
    approvalsPanelTitle.textContent = state.approvalPage.view === "history" ? "历史审批" : "待审批事项";
  }
  approvalsViewPending?.classList.toggle("active", state.approvalPage.view === "pending");
  approvalsViewPending?.classList.toggle("ghost", state.approvalPage.view !== "pending");
  approvalsViewHistory?.classList.toggle("active", state.approvalPage.view === "history");
  approvalsViewHistory?.classList.toggle("ghost", state.approvalPage.view !== "history");
  if (approvalPageSize) {
    approvalPageSize.value = String(state.approvalPage.limit || 10);
  }
  renderOffsetPagination(state.approvalPage, approvalPaginationMeta, "approval-page");
  approvalList.innerHTML = state.approvalPage.items.length
    ? state.approvalPage.items.map((item) => approvalRowMarkup(item)).join("")
    : state.approvalPage.view === "history"
      ? '<div class="detail empty compact-detail"><strong>暂无历史审批</strong><p>已完成的审批会显示在这里。</p></div>'
      : '<div class="detail empty compact-detail"><strong>暂无待审批事项</strong><p>当前没有需要处理的审批。</p></div>';
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
  const current = pluginEditorRecord() || state.pluginPage.items.find((item) => item.id === state.editingPluginId) || null;
  const baseManifest = pluginEditorManifest();
  const key = String(current?.key || baseManifest.key || "").trim();
  const version = String(current?.version || baseManifest.version || "v1").trim() || "v1";
  const pluginType = String(current?.plugin_type || baseManifest.plugin_type || "toolset").trim() || "toolset";
  const installPath = String(current?.install_path || "").trim() || null;
  const name = pluginName.value.trim() || String(current?.name || baseManifest.name || "").trim();
  if (!key) {
    throw new Error("插件标识缺失，请重新导入该插件。");
  }
  if (!name) {
    throw new Error("插件名称不能为空。");
  }
  return {
    id: state.editingPluginId,
    key,
    name,
    version,
    plugin_type: pluginType,
    description: pluginDescription.value.trim(),
    install_path: installPath,
    manifest: {
      ...baseManifest,
      key,
      name,
      version,
      plugin_type: pluginType,
      description: pluginDescription.value.trim(),
    },
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
      const matched = collection.find((item) => item.id === normalized || item.key === normalized || item.name === normalized) || null;
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

function formatFileSize(value) {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "-";
  }
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }
  if (bytes < 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function openKnowledgeBaseModal() {
  syncKnowledgeBaseModalMode();
  knowledgeBaseModal?.classList.remove("hidden");
  if (state.editingKnowledgeBaseId) {
    knowledgeBaseDropzone?.focus();
    return;
  }
  knowledgeBaseName?.focus();
}

function closeKnowledgeBaseModal() {
  clearKnowledgeEmbeddingJobPolling();
  knowledgeBaseModal?.classList.add("hidden");
  hideResult(knowledgeBaseModalResult);
}

function syncKnowledgeBaseModalMode() {
  const isEditing = Boolean(state.editingKnowledgeBaseId);
  if (knowledgeBaseTransferLayout) {
    knowledgeBaseTransferLayout.hidden = !isEditing;
  }
  if (knowledgeBaseSave) {
    knowledgeBaseSave.textContent = isEditing ? "保存知识库" : "创建知识库";
  }
}

function knowledgeBaseStageItemId(kind, path, extra = "") {
  return `${kind}:${path}:${extra}`;
}

function normalizeKnowledgeBaseStagedItems(entries) {
  return normalizeSkillImportEntries(entries).map((item) => ({
    id: knowledgeBaseStageItemId("file", item.path, `${item.file?.size || 0}:${item.file?.lastModified || 0}`),
    kind: "file",
    file: item.file,
    path: item.path,
    title: fileNameFromPath(item.path || item.file?.name || "") || item.path,
    size: Number(item.file?.size || 0),
    preview: "",
    content_text: "",
    source_label: "本地上传",
  }));
}

function buildKnowledgeBaseStagedDocumentItem(document, contentText = "") {
  const path = String(document?.source_path || document?.title || document?.id || "document").trim();
  return {
    id: knowledgeBaseStageItemId("document", path, String(document?.id || "")),
    kind: "document",
    file: null,
    path,
    title: String(document?.title || fileNameFromPath(path) || document?.id || "文档").trim(),
    size: Number(document?.file_size || 0),
    preview: String(document?.preview || "").trim(),
    content_text: String(contentText || "").trim(),
    source_label: "从知识库移出",
  };
}

function mergeKnowledgeBaseStagedItems(currentItems, nextItems) {
  const merged = new Map();
  [...(currentItems || []), ...(nextItems || [])].forEach((item) => {
    const key = String(item?.path || "").trim().toLowerCase() || String(item?.id || "");
    if (!key) {
      return;
    }
    if (!merged.has(key) || item.kind === "file") {
      merged.set(key, item);
    }
  });
  return Array.from(merged.values()).sort((left, right) => String(left.path || "").localeCompare(String(right.path || "")));
}

function formatKnowledgeDocumentMetaTime(value) {
  if (value == null || value === "") {
    return "-";
  }
  if (typeof value === "number") {
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toLocaleString("zh-CN", {
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      });
    }
  }
  return formatTeamChatTime(value) || String(value || "").trim() || "-";
}

function knowledgeDocumentTypeLabel(item) {
  const candidate = String(item?.path || item?.source_path || item?.title || "").trim();
  const fileName = fileNameFromPath(candidate);
  const extensionIndex = fileName.lastIndexOf(".");
  if (extensionIndex > -1 && extensionIndex < fileName.length - 1) {
    return fileName.slice(extensionIndex + 1).toUpperCase();
  }
  if (item?.kind === "document") {
    return "文档";
  }
  return "文件";
}

function knowledgeDocumentHoverSummary(item) {
  const timeValue = item?.updated_at || item?.created_at || item?.file?.lastModified || "";
  return ["时间: " + formatKnowledgeDocumentMetaTime(timeValue), "类型: " + knowledgeDocumentTypeLabel(item), "大小: " + formatFileSize(item?.file_size || item?.size || 0)].join("\n");
}

function knowledgeDocumentDisplayName(item) {
  return String(
    fileNameFromPath(item?.source_path || item?.path || item?.title || "") || item?.title || item?.path || item?.source_path || item?.id || "-",
  ).trim();
}

function selectedKnowledgeStageIds() {
  const active = new Set((state.knowledgeBaseUploadFiles || []).map((item) => item.id).filter(Boolean));
  return Array.from(new Set(state.knowledgeBaseStagedSelection || [])).filter((item) => active.has(item));
}

function syncKnowledgeBasePoolFilterControls() {
  if (knowledgeBasePoolQuery) {
    knowledgeBasePoolQuery.value = state.knowledgeBasePoolPage.query || "";
  }
}

function syncKnowledgeBaseStageSelection() {
  state.knowledgeBaseStagedSelection = selectedKnowledgeStageIds();
}

function toggleKnowledgeBaseStageSelection(stageId, selected) {
  const current = new Set(selectedKnowledgeStageIds());
  if (selected) {
    current.add(stageId);
  } else {
    current.delete(stageId);
  }
  state.knowledgeBaseStagedSelection = Array.from(current);
  renderKnowledgeBaseUploadSelection();
}

function setAllKnowledgeBaseStageSelections(selected) {
  state.knowledgeBaseStagedSelection = selected
    ? (state.knowledgeBaseUploadFiles || []).map((item) => item.id).filter(Boolean)
    : [];
  renderKnowledgeBaseUploadSelection();
}

function renderKnowledgeBaseUploadSelection() {
  knowledgeBaseDropzone?.classList.toggle("drag-active", Boolean(state.knowledgeBaseUploadDragActive));
  syncKnowledgeBasePoolFilterControls();
  syncKnowledgeBaseStageSelection();
  const files = Array.isArray(state.knowledgeBaseUploadFiles) ? state.knowledgeBaseUploadFiles : [];
  const selectedIds = new Set(selectedKnowledgeStageIds());
  const selectedCount = selectedIds.size;
  const hasFilters = Boolean(String(state.knowledgeBasePoolPage.query || "").trim());
  const allSelected = files.length > 0 && selectedCount === files.length;
  const partiallySelected = selectedCount > 0 && selectedCount < files.length;
  if (knowledgeBaseStageSelectAll) {
    knowledgeBaseStageSelectAll.checked = allSelected;
    knowledgeBaseStageSelectAll.indeterminate = partiallySelected;
    knowledgeBaseStageSelectAll.disabled = state.knowledgeBaseUploadBusy || !files.length;
  }
  if (knowledgeBaseStageSelectionActions) {
    knowledgeBaseStageSelectionActions.classList.toggle("hidden", !selectedCount);
    knowledgeBaseStageSelectionActions.hidden = !selectedCount;
  }
  if (knowledgeBaseStageRemove) {
    knowledgeBaseStageRemove.disabled = state.knowledgeBaseUploadBusy || !selectedCount;
  }
  if (knowledgeBaseStageMove) {
    knowledgeBaseStageMove.disabled = state.knowledgeBaseUploadBusy || !selectedCount;
  }
  if (!knowledgeBaseUploadSelection) {
    return;
  }
  if (!files.length) {
    knowledgeBaseUploadSelection.classList.add("empty");
    knowledgeBaseUploadSelection.innerHTML = `<span class="skill-import-selection-placeholder">${escapeHtml(
      hasFilters ? "当前筛选条件下没有匹配文档。" : "文档池暂无可用文档",
    )}</span>`;
    return;
  }
  knowledgeBaseUploadSelection.classList.remove("empty");
  knowledgeBaseUploadSelection.innerHTML = files
    .map(
      (item) => `
        <article
          class="knowledge-stage-card${selectedIds.has(item.id) ? " active" : ""}"
          data-knowledge-stage-open="${escapeAttribute(item.id || "")}"
          title="${escapeAttribute(knowledgeDocumentHoverSummary(item))}"
        >
          <div class="knowledge-stage-card-head">
            <div class="knowledge-document-select-cell">
              <input
                type="checkbox"
                data-knowledge-stage-select="${escapeAttribute(item.id || "")}"
                ${selectedIds.has(item.id) ? "checked" : ""}
                ${state.knowledgeBaseUploadBusy ? "disabled" : ""}
                aria-label="选择文档池文档 ${escapeAttribute(item.title || item.path || "-")}"
              />
            </div>
            <div class="knowledge-stage-card-main">
              <strong>${escapeHtml(knowledgeDocumentDisplayName(item))}</strong>
            </div>
          </div>
        </article>
      `,
    )
    .join("");
}

function resetKnowledgeBaseUploadState() {
  window.clearTimeout(knowledgeBasePoolQueryTimer);
  state.knowledgeBaseUploadFiles = [];
  state.knowledgeBaseStagedSelection = [];
  state.knowledgeBaseUploadDragActive = false;
  state.knowledgeBaseUploadBusy = false;
  state.knowledgeBasePoolPage = {
    ...state.knowledgeBasePoolPage,
    items: [],
    total: 0,
    limit: Math.max(1, Number(state.knowledgeBasePoolPage.limit || 8)),
    offset: 0,
    query: "",
  };
  if (knowledgeBaseFileInput) {
    knowledgeBaseFileInput.value = "";
  }
  if (knowledgeBaseFolderInput) {
    knowledgeBaseFolderInput.value = "";
  }
  syncKnowledgeBasePoolFilterControls();
  renderKnowledgeBaseUploadSelection();
}

function setKnowledgeBasePoolDocuments(items) {
  state.knowledgeBaseUploadFiles = Array.isArray(items) ? items : [];
  state.knowledgeBasePoolPage.items = state.knowledgeBaseUploadFiles;
  syncKnowledgeBaseStageSelection();
  renderKnowledgeBaseUploadSelection();
}

function setKnowledgeBaseUploadBusy(busy) {
  state.knowledgeBaseUploadBusy = Boolean(busy);
  knowledgeBaseDropzone?.classList.toggle("busy", state.knowledgeBaseUploadBusy);
  if (knowledgeBaseFileInput) {
    knowledgeBaseFileInput.disabled = state.knowledgeBaseUploadBusy;
  }
  if (knowledgeBaseFolderInput) {
    knowledgeBaseFolderInput.disabled = state.knowledgeBaseUploadBusy;
  }
  if (knowledgeBaseUploadFolder) {
    knowledgeBaseUploadFolder.disabled = state.knowledgeBaseUploadBusy;
  }
  if (knowledgeBasePoolQuery) {
    knowledgeBasePoolQuery.disabled = state.knowledgeBaseUploadBusy;
  }
  if (knowledgeBaseStageSelectAll) {
    knowledgeBaseStageSelectAll.disabled = state.knowledgeBaseUploadBusy || !(state.knowledgeBaseUploadFiles || []).length;
  }
  if (knowledgeBaseStageRemove) {
    knowledgeBaseStageRemove.disabled = state.knowledgeBaseUploadBusy || !selectedKnowledgeStageIds().length;
  }
  if (knowledgeBaseStageMove) {
    knowledgeBaseStageMove.disabled = state.knowledgeBaseUploadBusy || !selectedKnowledgeStageIds().length;
  }
  if (knowledgeBaseDocumentMoveBack) {
    knowledgeBaseDocumentMoveBack.disabled = state.knowledgeBaseUploadBusy || state.knowledgeBaseDocumentActionBusy || !selectedKnowledgeDocumentIds().length;
  }
  if (knowledgeBaseSave) {
    knowledgeBaseSave.disabled = state.knowledgeBaseUploadBusy;
  }
}

async function buildKnowledgePoolUploadPayload(entries) {
  const files = normalizeSkillImportEntries(entries);
  if (!files.length) {
    return null;
  }
  return {
    files: await Promise.all(
      files.map(async (item) => ({
        path: item.path,
        content_base64: await readFileAsBase64(item.file),
      })),
    ),
  };
}

async function loadKnowledgeBasePoolDocuments() {
  if (!state.editingKnowledgeBaseId) {
    setKnowledgeBasePoolDocuments([]);
    state.knowledgeBasePoolPage.total = 0;
    state.knowledgeBasePoolPage.offset = 0;
    return;
  }
  const params = new URLSearchParams();
  const query = String(state.knowledgeBasePoolPage.query || "").trim();
  if (query) {
    params.set("query", query);
  }
  params.set("exclude_knowledge_base_id", state.editingKnowledgeBaseId);
  const payload = await api(`/api/agent-center/knowledge-pool-documents${params.toString() ? `?${params.toString()}` : ""}`);
  setKnowledgeBasePoolDocuments(payload.items || []);
  state.knowledgeBasePoolPage.total = payload.total || 0;
  state.knowledgeBasePoolPage.limit = payload.limit || state.knowledgeBasePoolPage.limit;
  state.knowledgeBasePoolPage.offset = 0;
  state.knowledgeBasePoolPage.query = payload.filters?.query ?? query;
}

async function refreshKnowledgeBasePoolDocuments({ resetOffset = false, preferredIds = [] } = {}) {
  if (resetOffset || (preferredIds || []).length) {
    state.knowledgeBasePoolPage.offset = 0;
  }
  await loadKnowledgeBasePoolDocuments();
  const active = new Set((state.knowledgeBaseUploadFiles || []).map((item) => item.id).filter(Boolean));
  const wanted = Array.from(new Set((preferredIds || []).map((item) => String(item || "").trim()).filter(Boolean))).filter((item) => active.has(item));
  if (wanted.length) {
    state.knowledgeBaseStagedSelection = wanted;
  } else {
    syncKnowledgeBaseStageSelection();
  }
  renderKnowledgeBaseUploadSelection();
}

async function uploadKnowledgeBasePoolEntries(entries) {
  const knowledgeBaseId = state.editingKnowledgeBaseId;
  if (!knowledgeBaseId) {
    throw new Error("请先保存知识库，再上传文件。");
  }
  const payload = await buildKnowledgePoolUploadPayload(entries);
  if (!payload) {
    return null;
  }
  payload.knowledge_base_id = knowledgeBaseId;
  setKnowledgeBaseUploadBusy(true);
  hideResult(knowledgeBaseModalResult);
  try {
    const result = await api("/api/agent-center/knowledge-pool-documents/upload", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    await refreshKnowledgeBasePoolDocuments({ preferredIds: (result.items || []).map((item) => item.id) });
    showResult(knowledgeBaseModalResult, buildKnowledgeStageTransferResult("已上传到文档池。", result.items || [], result.skipped || []));
    return result;
  } finally {
    setKnowledgeBaseUploadBusy(false);
    renderKnowledgeBaseUploadSelection();
  }
}

async function deleteKnowledgeBasePoolDocuments(documentIds) {
  const ids = Array.from(new Set((documentIds || []).map((item) => String(item || "").trim()).filter(Boolean)));
  if (!ids.length) {
    throw new Error("请先选择左侧文档池中的文档。");
  }
  const result = await api("/api/agent-center/knowledge-pool-documents/actions", {
    method: "POST",
    body: JSON.stringify({
      action: "delete",
      document_ids: ids,
    }),
  });
  await refreshKnowledgeBasePoolDocuments();
  showResult(knowledgeBaseModalResult, buildKnowledgeStageTransferResult("已删除文档池中的选中文档。", result.items || [], result.skipped || []));
  return result;
}

function selectedKnowledgeDocumentIds() {
  const active = new Set((state.knowledgeBaseDocuments || []).map((item) => item.id).filter(Boolean));
  return Array.from(new Set(state.knowledgeBaseDocumentSelection || [])).filter((item) => active.has(item));
}

function syncKnowledgeBaseDocumentFilterControls() {
  if (knowledgeBaseDocumentQuery) {
    knowledgeBaseDocumentQuery.value = state.knowledgeBaseDocumentPage.query || "";
  }
  if (knowledgeBaseDocumentStatus) {
    knowledgeBaseDocumentStatus.value = state.knowledgeBaseDocumentPage.embeddingStatus || "all";
  }
}

function syncKnowledgeBaseDocumentSelection() {
  state.knowledgeBaseDocumentSelection = selectedKnowledgeDocumentIds();
}

function toggleKnowledgeBaseDocumentSelection(documentId, selected) {
  const current = new Set(selectedKnowledgeDocumentIds());
  if (selected) {
    current.add(documentId);
  } else {
    current.delete(documentId);
  }
  state.knowledgeBaseDocumentSelection = Array.from(current);
  renderKnowledgeBaseDocuments();
}

function setAllKnowledgeBaseDocumentSelections(selected) {
  state.knowledgeBaseDocumentSelection = selected ? (state.knowledgeBaseDocuments || []).map((item) => item.id).filter(Boolean) : [];
  renderKnowledgeBaseDocuments();
}

function setKnowledgeBaseDocumentActionBusy(busy) {
  state.knowledgeBaseDocumentActionBusy = Boolean(busy);
  const disabled = state.knowledgeBaseDocumentActionBusy;
  const hasSelection = selectedKnowledgeDocumentIds().length > 0;
  const hasKnowledgeBase = Boolean(state.editingKnowledgeBaseId);
  if (knowledgeDocumentEmbedAdd) {
    knowledgeDocumentEmbedAdd.disabled = disabled || !hasKnowledgeBase;
    if (disabled) {
      knowledgeDocumentEmbedAdd.textContent = "处理中...";
    } else {
      syncKnowledgeDocumentBulkActionLabel();
    }
  }
  if (knowledgeDocumentSelectAll) {
    knowledgeDocumentSelectAll.disabled = disabled || !(state.knowledgeBaseDocuments || []).length;
  }
  if (knowledgeBaseDocumentMoveBack) {
    knowledgeBaseDocumentMoveBack.hidden = !hasSelection;
    knowledgeBaseDocumentMoveBack.disabled = disabled || state.knowledgeBaseUploadBusy || !selectedKnowledgeDocumentIds().length;
  }
  if (knowledgeBaseDocumentQuery) {
    knowledgeBaseDocumentQuery.disabled = disabled;
  }
  if (knowledgeBaseDocumentStatus) {
    knowledgeBaseDocumentStatus.disabled = disabled;
  }
}

function syncKnowledgeDocumentBulkActionLabel() {
  if (!knowledgeDocumentEmbedAdd) {
    return;
  }
  knowledgeDocumentEmbedAdd.dataset.bulkEmbeddingMode = "save";
  knowledgeDocumentEmbedAdd.textContent = "保存并嵌入";
}

function buildKnowledgeDocumentEmbeddingResult(payload) {
  const message = String(payload?.message || "知识库文件嵌入操作已完成。").trim();
  const skipped = Array.isArray(payload?.skipped) ? payload.skipped : [];
  const lines = [message];
  if (skipped.length) {
    lines.push(`跳过 ${skipped.length} 个文件。`);
    skipped.slice(0, 3).forEach((item) => {
      const title = String(item?.title || item?.id || "未命名文件").trim();
      const detail = String(item?.message || "当前文件无需执行该操作。").trim();
      lines.push(`- ${title}: ${detail}`);
    });
    if (skipped.length > 3) {
      lines.push(`其余 ${skipped.length - 3} 个文件未展开。`);
    }
  }
  return lines.join("\n");
}

function knowledgeDocumentActionPendingText(action, count) {
  const total = Math.max(1, Number(count || 0));
  if (action === "delete") {
    return `正在删除 ${total} 个文件的嵌入，请等待...`;
  }
  if (action === "reembed") {
    return `正在重嵌 ${total} 个文件，请等待...`;
  }
  if (action === "add") {
    return `正在嵌入 ${total} 个文件，请等待...`;
  }
  return `正在保存并嵌入 ${total} 个文件，请等待...`;
}

function renderKnowledgeBaseDocuments() {
  if (!knowledgeBaseDocumentList) {
    return;
  }
  syncKnowledgeBaseDocumentFilterControls();
  syncKnowledgeBaseDocumentSelection();
  const documents = Array.isArray(state.knowledgeBaseDocuments) ? state.knowledgeBaseDocuments : [];
  const selectedIds = new Set(selectedKnowledgeDocumentIds());
  const selectedCount = selectedIds.size;
  const query = String(state.knowledgeBaseDocumentPage.query || "").trim();
  const embeddingStatus = String(state.knowledgeBaseDocumentPage.embeddingStatus || "all").trim() || "all";
  const hasFilters = Boolean(query || embeddingStatus !== "all");
  const allSelected = documents.length > 0 && selectedCount === documents.length;
  const partiallySelected = selectedCount > 0 && selectedCount < documents.length;
  syncKnowledgeDocumentBulkActionLabel();
  if (knowledgeBaseDocumentMoveBack) {
    knowledgeBaseDocumentMoveBack.hidden = !selectedCount;
  }
  if (knowledgeDocumentSelectAll) {
    knowledgeDocumentSelectAll.checked = allSelected;
    knowledgeDocumentSelectAll.indeterminate = partiallySelected;
  }
  setKnowledgeBaseDocumentActionBusy(state.knowledgeBaseDocumentActionBusy);
  knowledgeBaseDocumentList.innerHTML = documents.length
    ? documents
        .map(
          (item) => `
            <article
              class="knowledge-document-card${selectedIds.has(item.id) ? " active" : ""}"
              data-knowledge-document-open="${escapeAttribute(item.id || "")}"
              title="${escapeAttribute(knowledgeDocumentHoverSummary(item))}"
            >
              <div class="knowledge-document-card-head">
                <div class="knowledge-document-select-cell">
                  <input
                    type="checkbox"
                    data-knowledge-document-select="${escapeAttribute(item.id || "")}"
                    ${selectedIds.has(item.id) ? "checked" : ""}
                    ${state.knowledgeBaseDocumentActionBusy ? "disabled" : ""}
                    aria-label="选择文件 ${escapeAttribute(item.title || item.source_path || item.id || "-")}"
                  />
                </div>
                <div class="knowledge-document-card-main">
                  <strong>${escapeHtml(knowledgeDocumentDisplayName(item))}</strong>
                </div>
              </div>
            </article>
          `,
        )
        .join("")
    : `<div class="detail empty compact-detail">${escapeHtml(hasFilters ? "当前筛选条件下没有匹配文件。" : "当前知识库还没有入库文件。")}</div>`;
}

function resetKnowledgeBaseDocumentBrowser() {
  window.clearTimeout(knowledgeBaseDocumentQueryTimer);
  state.knowledgeBaseDocuments = [];
  state.knowledgeBaseDocumentPage = {
    ...state.knowledgeBaseDocumentPage,
    items: [],
    total: 0,
    limit: Math.max(1, Number(state.knowledgeBaseDocumentPage.limit || 8)),
    offset: 0,
    query: "",
    embeddingStatus: "all",
  };
  state.knowledgeBaseDocumentSelection = [];
  state.knowledgeBaseDocumentActionBusy = false;
  syncKnowledgeBaseDocumentFilterControls();
}

function buildKnowledgeStageTransferResult(message, moved = [], skipped = []) {
  const lines = [message];
  if (moved.length) {
    lines.push(`成功处理 ${moved.length} 个文档。`);
  }
  if (skipped.length) {
    lines.push(`跳过 ${skipped.length} 个文档。`);
    skipped.slice(0, 4).forEach((item) => {
      lines.push(`- ${item.title || item.id || "未命名文档"}: ${item.message || "操作失败"}`);
    });
    if (skipped.length > 4) {
      lines.push(`其余 ${skipped.length - 4} 个未展开。`);
    }
  }
  return lines.join("\n");
}

async function moveSelectedKnowledgeStageItemsToDocuments() {
  const knowledgeBaseId = state.editingKnowledgeBaseId;
  if (!knowledgeBaseId) {
    throw new Error("请先保存知识库，再把左侧文档池中的文档移入知识库。");
  }
  const selectedIds = selectedKnowledgeStageIds();
  if (!selectedIds.length) {
    throw new Error("请先选择左侧文档池中的文档。");
  }
  setKnowledgeBaseUploadBusy(true);
  setKnowledgeBaseDocumentActionBusy(true);
  renderKnowledgeBaseUploadSelection();
  renderKnowledgeBaseDocuments();
  hideResult(knowledgeBaseModalResult);
  try {
    const result = await api(`/api/agent-center/knowledge-bases/${encodeURIComponent(knowledgeBaseId)}/pool-documents`, {
      method: "POST",
      body: JSON.stringify({
        document_ids: selectedIds,
      }),
    });
    invalidateData("knowledgeBaseRefs", "knowledgeBasePage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureKnowledgeBasesPage(true);
    await refreshKnowledgeBaseDocuments({ resetOffset: true });
    await refreshKnowledgeBasePoolDocuments();
    showResult(
      knowledgeBaseModalResult,
      buildKnowledgeStageTransferResult("已将左侧文档池中的文档移入知识库。", result.items || [], result.skipped || []),
    );
    return result;
  } finally {
    setKnowledgeBaseUploadBusy(false);
    setKnowledgeBaseDocumentActionBusy(false);
    renderKnowledgeBaseUploadSelection();
    renderKnowledgeBaseDocuments();
  }
}

async function moveSelectedKnowledgeDocumentsToStage() {
  const knowledgeBaseId = state.editingKnowledgeBaseId;
  if (!knowledgeBaseId) {
    throw new Error("请先打开一个知识库。");
  }
  const selectedIds = selectedKnowledgeDocumentIds();
  if (!selectedIds.length) {
    throw new Error("请先选择右侧知识库文档。");
  }
  if (!window.confirm(`确认将选中的 ${selectedIds.length} 个知识库文档移回文档池？`)) {
    return null;
  }
  const selectedDocuments = selectedIds
    .map((id) => state.knowledgeBaseDocuments.find((item) => item.id === id) || null)
    .filter(Boolean);
  const movableDocuments = selectedDocuments.filter((item) => item.pool_document_id);
  const preferredPoolIds = movableDocuments.map((item) => item.pool_document_id).filter(Boolean);
  const movedIds = [];
  const skipped = selectedDocuments
    .filter((item) => !item.pool_document_id)
    .map((item) => ({
      id: item.id,
      title: item.title || item.source_path || item.id,
      message: "该文档不是从文档池加入的，不能移回。",
    }));
  setKnowledgeBaseUploadBusy(true);
  setKnowledgeBaseDocumentActionBusy(true);
  renderKnowledgeBaseUploadSelection();
  renderKnowledgeBaseDocuments();
  hideResult(knowledgeBaseModalResult);
  try {
    for (const document of movableDocuments) {
      try {
        await api(`/api/agent-center/knowledge-documents/${encodeURIComponent(document.id)}`, { method: "DELETE" });
        movedIds.push(document.id);
      } catch (error) {
        skipped.push({
          id: document.id,
          title: document.title || document.source_path || document.id,
          message: error?.message || "移回文档池失败。",
        });
      }
    }
    if (movedIds.length) {
      const job = await api(`/api/agent-center/knowledge-bases/${encodeURIComponent(knowledgeBaseId)}/documents/embeddings`, {
        method: "POST",
        body: JSON.stringify({
          action: "save",
          document_ids: movedIds,
        }),
      });
      await waitForKnowledgeEmbeddingJob(job);
    }
    invalidateData("knowledgeBaseRefs", "knowledgeBasePage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureKnowledgeBasesPage(true);
    await refreshKnowledgeBaseDocuments();
    await refreshKnowledgeBasePoolDocuments({ preferredIds: preferredPoolIds });
    showResult(
      knowledgeBaseModalResult,
      buildKnowledgeStageTransferResult("已将右侧知识库文档移回文档池。", movedIds, skipped),
    );
    return { movedIds, skipped };
  } finally {
    setKnowledgeBaseUploadBusy(false);
    setKnowledgeBaseDocumentActionBusy(false);
    renderKnowledgeBaseUploadSelection();
    renderKnowledgeBaseDocuments();
  }
}

async function runKnowledgeDocumentEmbeddingAction(action, documentIds) {
  const knowledgeBaseId = state.editingKnowledgeBaseId;
  if (!knowledgeBaseId) {
    throw new Error("请先打开一个知识库。");
  }
  const ids = Array.from(new Set((documentIds || []).map((item) => String(item || "").trim()).filter(Boolean)));
  const normalizedAction = String(action || "").trim().toLowerCase();
  if (!ids.length && !["", "save", "sync"].includes(normalizedAction)) {
    throw new Error("请至少选择一个文件。");
  }
  if (normalizedAction === "delete") {
    const scopeLabel = ids.length === 1 ? "这个文件的已有嵌入" : `选中的 ${ids.length} 个文件嵌入`;
    if (!window.confirm(`确认删除${scopeLabel}？`)) {
      return null;
    }
  }
  setKnowledgeBaseDocumentActionBusy(true);
  renderKnowledgeBaseDocuments();
  try {
    if (normalizedAction === "add" || normalizedAction === "reembed") {
      await requireKnowledgeEmbeddingConfiguration();
    }
    const job = await api(`/api/agent-center/knowledge-bases/${encodeURIComponent(knowledgeBaseId)}/documents/embeddings`, {
      method: "POST",
      body: JSON.stringify({
        action: normalizedAction || action,
        document_ids: ids,
      }),
    });
    const payload = (await waitForKnowledgeEmbeddingJob(job)).result || {};
    invalidateData("knowledgeBaseRefs", "knowledgeBasePage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureKnowledgeBasesPage(true);
    await loadKnowledgeBaseDocuments(knowledgeBaseId);
    renderKnowledgeBaseDocuments();
    showResult(knowledgeBaseModalResult, buildKnowledgeDocumentEmbeddingResult(payload));
    return payload;
  } finally {
    setKnowledgeBaseDocumentActionBusy(false);
    renderKnowledgeBaseDocuments();
  }
}

async function runKnowledgeDocumentBulkEmbeddingAction(documentIds) {
  void documentIds;
  return runKnowledgeDocumentEmbeddingAction("save", []);
}

function resetKnowledgeBaseForm({ openModal = false } = {}) {
  clearKnowledgeEmbeddingJobPolling();
  state.editingKnowledgeBaseId = null;
  resetKnowledgeBaseDocumentBrowser();
  if (knowledgeBaseName) {
    knowledgeBaseName.value = "";
  }
  if (knowledgeBaseModalTitle) {
    knowledgeBaseModalTitle.textContent = "新增知识库";
  }
  resetKnowledgeBaseUploadState();
  void refreshKnowledgeBasePoolDocuments();
  hideResult(knowledgeBaseResult);
  hideResult(knowledgeBaseModalResult);
  syncKnowledgeBaseModalMode();
  renderKnowledgeBaseDocuments();
  renderKnowledgeBases();
  if (openModal) {
    openKnowledgeBaseModal();
  }
}

async function fillKnowledgeBaseForm(item, { openModal = false } = {}) {
  clearKnowledgeEmbeddingJobPolling();
  state.editingKnowledgeBaseId = item.id || null;
  resetKnowledgeBaseDocumentBrowser();
  if (knowledgeBaseName) {
    knowledgeBaseName.value = item.name || "";
  }
  if (knowledgeBaseModalTitle) {
    knowledgeBaseModalTitle.textContent = "编辑知识库";
  }
  resetKnowledgeBaseUploadState();
  await loadKnowledgeBaseDocuments(item.id);
  await refreshKnowledgeBasePoolDocuments();
  hideResult(knowledgeBaseResult);
  hideResult(knowledgeBaseModalResult);
  syncKnowledgeBaseModalMode();
  renderKnowledgeBaseDocuments();
  renderKnowledgeBases();
  if (openModal) {
    openKnowledgeBaseModal();
  }
}

function buildKnowledgeBasePayloadFromForm() {
  return {
    id: state.editingKnowledgeBaseId,
    name: knowledgeBaseName.value.trim(),
  };
}

function renderKnowledgeBases() {
  if (!knowledgeBaseList) {
    return;
  }
  knowledgeBaseList.innerHTML = state.knowledgeBasePage.items.length
    ? state.knowledgeBasePage.items
        .map((item) => {
          const updatedAt = formatTeamChatTime(item.updated_at) || item.updated_at || "-";
          const embeddingModelName = String(item.embedding_model_label || item.embedding_model_name || "-").trim() || "-";
          return `
            <article class="provider-row knowledge-base-row${item.id === state.editingKnowledgeBaseId ? " active" : ""}">
              <div class="provider-main">
                <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
              </div>
              <div class="provider-cell">
                <strong>${escapeHtml(item.file_count || item.document_count || 0)}</strong>
              </div>
              <div class="provider-cell">
                <strong>${escapeHtml(item.vector_count || 0)}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(embeddingModelName)}">${escapeHtml(embeddingModelName)}</strong>
              </div>
              <div class="provider-cell">
                <strong title="${escapeAttribute(item.updated_at || "-")}">${escapeHtml(updatedAt)}</strong>
              </div>
              <div class="provider-row-actions">
                <button type="button" data-knowledge-base-edit="${escapeAttribute(item.id || "")}">编辑</button>
                <button type="button" class="ghost warn" data-knowledge-base-delete="${escapeAttribute(item.id || "")}">删除</button>
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="detail empty compact-detail"><strong>暂无知识库</strong><p>先创建第一个知识库并上传文件。</p></div>';
  renderOffsetPagination(state.knowledgeBasePage, knowledgeBasePaginationMeta, "knowledge-base-page");
}

function openReviewPolicyModal() {
  reviewPolicyModal?.classList.remove("hidden");
}

function closeReviewPolicyModal() {
  reviewPolicyModal?.classList.add("hidden");
  hideResult(reviewPolicyModalResult);
}

function compactLabelSummary(labels, emptyLabel = "未配置") {
  const items = Array.from(new Set((labels || []).map((value) => String(value || "").trim()).filter(Boolean)));
  if (!items.length) {
    return emptyLabel;
  }
  if (items.length <= 2) {
    return items.join("、");
  }
  return `${items.slice(0, 2).join("、")} 等 ${items.length} 项`;
}

function reviewPolicyDecisionValues(spec) {
  const rules = reviewPolicyRulesFromSpec(spec);
  if (rules.length) {
    return Array.from(
      new Set(
        rules.flatMap((rule) => (rule.allowed_decisions || []).map((value) => String(value || "").trim()).filter(Boolean)),
      ),
    );
  }
  return Array.from(new Set((spec?.allowed_decisions || []).map((value) => String(value || "").trim()).filter(Boolean)));
}

function reviewPolicyDecisionLabels(spec) {
  return reviewPolicyDecisionValues(spec).map((value) => reviewPolicyDecisionTypeLabel(value) || value);
}

function reviewPolicyPluginActionLabels(spec) {
  return Array.from(
    new Set(
      reviewPolicyRulesFromSpec(spec).map((item) => {
        const pluginLabel = reviewPolicyPluginLabel(item.plugin_key) || item.plugin_key;
        const actionLabel = item.action === "*" ? "\u5168\u90e8\u52a8\u4f5c" : item.action;
        return `${pluginLabel} / ${actionLabel}`;
      }),
    ),
  );
}

function resetReviewPolicyForm({ openModal = false } = {}) {
  state.editingReviewPolicyId = null;
  state.reviewPolicyBaseSpec = {};
  reviewPolicyName.value = "";
  setReviewPolicyRules([]);
  if (reviewPolicyModalTitle) {
    reviewPolicyModalTitle.textContent = "新增审核策略";
  }
  hideResult(reviewPolicyResult);
  hideResult(reviewPolicyModalResult);
  renderReviewPolicies();
  if (openModal) {
    openReviewPolicyModal();
  }
}

function fillReviewPolicyForm(item, { openModal = false } = {}) {
  const spec = dictOrEmpty(item?.spec_json);
  state.editingReviewPolicyId = item.id;
  state.reviewPolicyBaseSpec = clone(spec);
  reviewPolicyName.value = item.name || "";
  setReviewPolicyRules(reviewPolicyRulesFromSpec(spec));
  if (reviewPolicyModalTitle) {
    reviewPolicyModalTitle.textContent = "编辑审核策略";
  }
  hideResult(reviewPolicyResult);
  hideResult(reviewPolicyModalResult);
  renderReviewPolicies();
  if (openModal) {
    openReviewPolicyModal();
  }
}

function buildReviewPolicyPayloadFromForm() {
  const spec = clone(state.reviewPolicyBaseSpec || {});
  const conditions = dictOrEmpty(spec.conditions);
  const rules = reviewPolicyRulesValue()
    .filter((item) => item.plugin_key || item.action || (item.allowed_decisions || []).length)
    .map((item) => normalizeReviewPolicyRule(item));
  if (!rules.length) {
    throw new Error("\u8bf7\u81f3\u5c11\u6dfb\u52a0\u4e00\u6761\u7b56\u7565\u3002");
  }
  rules.forEach((rule, index) => {
    if (!rule.plugin_key) {
      throw new Error(`\u7b56\u7565 ${index + 1} \u672a\u9009\u62e9\u63d2\u4ef6\u3002`);
    }
    if (!rule.action) {
      throw new Error(`\u7b56\u7565 ${index + 1} \u672a\u9009\u62e9\u63d2\u4ef6\u52a8\u4f5c\u3002`);
    }
    if (!(rule.allowed_decisions || []).length) {
      throw new Error(`\u7b56\u7565 ${index + 1} \u81f3\u5c11\u9700\u8981\u9009\u62e9\u4e00\u79cd\u51b3\u7b56\u7c7b\u578b\u3002`);
    }
  });
  conditions.plugin_actions = rules.map((item) => ({ plugin_key: item.plugin_key, action: item.action }));
  delete conditions.rules;
  delete conditions.plugin_keys;
  delete conditions.actions;
  delete conditions.risk_tags;
  delete conditions.message_types;
  delete conditions.memory_scopes;
  delete conditions.scopes;
  delete conditions.memory_kinds;
  delete spec.triggers;
  spec.allowed_decisions = Array.from(new Set(rules.flatMap((item) => item.allowed_decisions || [])));
  spec.rules = rules;
  delete spec.actions;
  spec.conditions = conditions;
  return {
    id: state.editingReviewPolicyId,
    name: reviewPolicyName.value.trim(),
    version: "v1",
    spec,
  };
}

function reviewPolicyRowMarkup(item) {
  const spec = dictOrEmpty(item.spec_json);
  const pluginActionSummary = compactLabelSummary(reviewPolicyPluginActionLabels(spec), "未配置插件动作");
  const decisionLabels = reviewPolicyDecisionLabels(spec);
  const decisionSummary = decisionLabels.length ? decisionLabels.join("、") : "未配置决策类型";
  return `
    <article class="provider-row review-policy-row">
      <div class="provider-main">
        <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
      </div>
      <div class="provider-cell">
        <strong title="${escapeAttribute(pluginActionSummary)}">${escapeHtml(pluginActionSummary)}</strong>
      </div>
      <div class="provider-cell">
        <strong title="${escapeAttribute(decisionSummary)}">${escapeHtml(decisionSummary)}</strong>
      </div>
      <div class="provider-row-actions">
        <button type="button" data-review-policy-edit="${item.id}">编辑</button>
        <button type="button" class="ghost warn" data-review-policy-delete="${item.id}">删除</button>
      </div>
    </article>
  `;
}

function renderReviewPolicies() {
  reviewPolicyList.innerHTML = state.reviewPolicyPage.items.length
    ? state.reviewPolicyPage.items.map((item) => reviewPolicyRowMarkup(item)).join("")
    : '<div class="detail empty compact-detail"><strong>暂无审核策略</strong><p>先创建第一个审核策略。</p></div>';
  renderOffsetPagination(state.reviewPolicyPage, reviewPolicyPaginationMeta, "review-policy-page");
}

function renderMemoryProfiles() {
  /*
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
function syncAgentDefinitionModelOptions(selectedModel = "") {
  */
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
  setMultiSelectValues(agentDefinitionReviewPolicies, []);
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
  setMultiSelectValues(
    agentDefinitionReviewPolicies,
    normalizeResourceSelections(spec.review_policy_refs || [], state.reviewPolicies),
  );
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
  spec.review_policy_refs = getMultiSelectValues(agentDefinitionReviewPolicies);
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
  const reviewPolicyLabels = normalizeResourceSelections(spec.review_policy_refs || [], state.reviewPolicies).map(
    (value) => agentDefinitionReferenceLabel(state.reviewPolicies, value, value),
  );
  const reviewPolicyLabel = compactLabelSummary(reviewPolicyLabels, "未配置审核策略");
  const resourceSummary =
    `角色管理: ${staticMemoryLabel} / 插件 ${pluginCount} / 技能 ${skillCount} / ` +
    `知识库: ${knowledgeBaseLabel} / 审核: ${reviewPolicyLabel}`;
  return `
    <article class="provider-row agent-management-row">
      <div class="provider-main">
        <strong title="${escapeAttribute(item.name || item.id || "-")}">${escapeHtml(item.name || item.id || "-")}</strong>
      </div>
      <div class="provider-cell">
        <strong title="${escapeAttribute(item.description || "未填写 Agent 简介")}">${escapeHtml(item.description || "未填写 Agent 简介")}</strong>
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
  if (teamDefinitionDescription) {
    teamDefinitionDescription.value = "";
  }
  renderTeamDefinitionLeadAgentOptions("");
  if (teamDefinitionLeadAgentDefinition) {
    teamDefinitionLeadAgentDefinition.value = "";
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
  if (teamDefinitionDescription) {
    teamDefinitionDescription.value = definition.description || "";
  }
  renderTeamDefinitionLeadAgentOptions(hierarchy.lead.source_ref);
  if (teamDefinitionLeadAgentDefinition && hierarchy.lead.source_ref) {
    teamDefinitionLeadAgentDefinition.value = hierarchy.lead.source_ref;
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
  const leadAgentDefinitionRef = normalizeTeamDefinitionReference("agent_definition", teamDefinitionLeadAgentDefinition?.value || "");
  if (!leadAgentDefinitionRef) {
    throw new Error("请先选择 Lead Agent。");
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
  spec.workspace_id = "local-workspace";
  spec.lead = {
    kind: "agent",
    source_kind: "agent_definition",
    agent_definition_ref: leadAgentDefinitionRef,
  };
  spec.children = children.map((member) =>
    member.source_kind === "team_definition"
      ? {
          kind: "team",
          source_kind: "team_definition",
          team_definition_ref: member.source_ref,
        }
      : {
          kind: "agent",
          source_kind: "agent_definition",
          agent_definition_ref: member.source_ref,
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
    description: teamDefinitionDescription?.value.trim() || "",
    version: "v1",
    spec,
  };
}

async function loadControlPlane() {
  const payload = await api("/api/control-plane");
  state.summary = payload.summary || {};
  state.storage = payload.storage || null;
  state.providerTypes = payload.provider_types || [];
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

async function loadLocalModels() {
  const payload = await api("/api/agent-center/local-models");
  state.localModels = payload.items || [];
}

async function loadRetrievalSettings() {
  const payload = await api("/api/agent-center/retrieval-settings");
  state.retrievalSettings = {
    settings: payload.settings || { embedding: { mode: "disabled" }, rerank: { mode: "disabled" } },
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

async function loadLocalModelPage() {
  const params = new URLSearchParams({
    limit: String(state.localModelPage.limit || 10),
    offset: String(state.localModelPage.offset || 0),
  });
  if (state.localModelPage.query) {
    params.set("query", state.localModelPage.query);
  }
  const payload = await api(`/api/agent-center/local-models?${params.toString()}`);
  state.localModelPage.items = payload.items || [];
  state.localModelPage.total = payload.total || 0;
  state.localModelPage.limit = payload.limit || state.localModelPage.limit;
  state.localModelPage.offset = payload.offset || 0;
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
  state.skillGroups = payload.groups || [];
}

async function loadSkillGroupCatalog() {
  const payload = await api("/api/agent-center/skill-groups");
  state.skillGroupCatalog = payload.items || [];
}

async function loadSkillGroupPage() {
  const params = new URLSearchParams({
    limit: String(state.skillGroupPage.limit || 10),
    offset: String(state.skillGroupPage.offset || 0),
  });
  const payload = await api(`/api/agent-center/skill-groups?${params.toString()}`);
  state.skillGroupPage.items = payload.items || [];
  state.skillGroupPage.total = payload.total || 0;
  state.skillGroupPage.limit = payload.limit || state.skillGroupPage.limit;
  state.skillGroupPage.offset = payload.offset || 0;
}

async function loadSkillPage() {
  const params = new URLSearchParams({
    limit: String(state.skillPage.limit || 10),
    offset: String(state.skillPage.offset || 0),
  });
  if (state.skillPage.query) {
    params.set("query", state.skillPage.query);
  }
  if (state.skillPage.groupKey) {
    params.set("group_key", state.skillPage.groupKey);
  }
  const payload = await api(`/api/agent-center/skills?${params.toString()}`);
  state.skillPage.items = payload.items || [];
  state.skillPage.total = payload.total || 0;
  state.skillPage.limit = payload.limit || state.skillPage.limit;
  state.skillPage.offset = payload.offset || 0;
  state.skillGroups = payload.groups || [];
}

async function loadReviewPolicies() {
  const payload = await api("/api/agent-center/review-policies");
  state.reviewPolicies = payload.items || [];
}

async function loadReviewPolicyPage() {
  const params = new URLSearchParams({
    limit: String(state.reviewPolicyPage.limit || 10),
    offset: String(state.reviewPolicyPage.offset || 0),
  });
  const payload = await api(`/api/agent-center/review-policies?${params.toString()}`);
  state.reviewPolicyPage.items = payload.items || [];
  state.reviewPolicyPage.total = payload.total || 0;
  state.reviewPolicyPage.limit = payload.limit || state.reviewPolicyPage.limit;
  state.reviewPolicyPage.offset = payload.offset || 0;
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

async function loadKnowledgeBasePage() {
  const params = new URLSearchParams({
    limit: String(state.knowledgeBasePage.limit || 10),
    offset: String(state.knowledgeBasePage.offset || 0),
  });
  const payload = await api(`/api/agent-center/knowledge-bases?${params.toString()}`);
  state.knowledgeBasePage.items = payload.items || [];
  state.knowledgeBasePage.total = payload.total || 0;
  state.knowledgeBasePage.limit = payload.limit || state.knowledgeBasePage.limit;
  state.knowledgeBasePage.offset = payload.offset || 0;
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
    state.knowledgeBaseDocumentPage.items = [];
    state.knowledgeBaseDocumentPage.total = 0;
    state.knowledgeBaseDocumentPage.offset = 0;
    state.knowledgeBaseDocumentSelection = [];
    return;
  }
  const params = new URLSearchParams();
  const query = String(state.knowledgeBaseDocumentPage.query || "").trim();
  const embeddingStatus = String(state.knowledgeBaseDocumentPage.embeddingStatus || "all").trim() || "all";
  if (query) {
    params.set("query", query);
  }
  if (embeddingStatus && embeddingStatus !== "all") {
    params.set("embedding_status", embeddingStatus);
  }
  const payload = await api(`/api/agent-center/knowledge-bases/${encodeURIComponent(knowledgeBaseId)}/documents?${params.toString()}`);
  state.knowledgeBaseDocuments = payload.items || [];
  state.knowledgeBaseDocumentPage.items = state.knowledgeBaseDocuments;
  state.knowledgeBaseDocumentPage.total = payload.total || 0;
  state.knowledgeBaseDocumentPage.limit = payload.limit || state.knowledgeBaseDocumentPage.limit;
  state.knowledgeBaseDocumentPage.offset = 0;
  state.knowledgeBaseDocumentPage.query = payload.filters?.query ?? query;
  state.knowledgeBaseDocumentPage.embeddingStatus = payload.filters?.embedding_status ?? embeddingStatus;
  syncKnowledgeBaseDocumentSelection();
}

async function refreshKnowledgeBaseDocuments({ resetOffset = false } = {}) {
  if (resetOffset) {
    state.knowledgeBaseDocumentPage.offset = 0;
  }
  if (!state.editingKnowledgeBaseId) {
    renderKnowledgeBaseDocuments();
    return;
  }
  await loadKnowledgeBaseDocuments(state.editingKnowledgeBaseId);
  renderKnowledgeBaseDocuments();
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
  populateTaskOptions();
}

async function loadTeamDefinitionPage() {
  const params = new URLSearchParams({
    limit: String(state.teamDefinitionPage.limit || 10),
    offset: String(state.teamDefinitionPage.offset || 0),
  });
  const payload = await api(`/api/agent-center/team-definitions?${params.toString()}`);
  state.teamDefinitionPage.items = payload.items || [];
  state.teamDefinitionPage.total = payload.total || 0;
  state.teamDefinitionPage.limit = payload.limit || state.teamDefinitionPage.limit;
  state.teamDefinitionPage.offset = payload.offset || 0;
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

async function loadRunPage() {
  const params = new URLSearchParams({
    limit: String(state.runPage.limit || 10),
    offset: String(state.runPage.offset || 0),
  });
  const payload = await api(`/api/runs?${params.toString()}`);
  state.runPage.items = payload.items || [];
  state.runPage.total = payload.total || 0;
  state.runPage.limit = payload.limit || state.runPage.limit;
  state.runPage.offset = payload.offset || 0;
}

async function loadApprovals() {
  const params = new URLSearchParams({
    limit: String(state.approvalPage.limit || 10),
    offset: String(state.approvalPage.offset || 0),
    view: String(state.approvalPage.view || "pending"),
  });
  const payload = await api(`/api/approvals?${params.toString()}`);
  state.approvals = payload.items || [];
  state.approvalPage.items = payload.items || [];
  state.approvalPage.total = payload.total || 0;
  state.approvalPage.limit = payload.limit || state.approvalPage.limit;
  state.approvalPage.offset = payload.offset || 0;
  state.approvalPage.view = payload.view || state.approvalPage.view;
  const editingApprovalId = String(state.approvalEditor.item?.id || "").trim();
  if (editingApprovalId) {
    const refreshed = state.approvalPage.items.find((item) => String(item?.id || "").trim() === editingApprovalId);
    if (refreshed) {
      state.approvalEditor.item = clone(refreshed);
    } else {
      closeApprovalEditModal();
    }
  }
}

async function loadRunDetail(runId) {
  const payload = await api(`/api/runs/${runId}`);
  state.selectedRunId = runId;
  const conversationThreadId = resolveConversationThreadId(payload);
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
    `会话 Thread：${conversationThreadId || "-"}\n` +
    `摘要：${payload.run.summary || ""}\n\n` +
    `步骤\n${steps || "- 无"}\n\n` +
    `产物\n${artifacts || "- 无"}\n\n` +
    `最近事件\n${events || "- 无"}\n\n` +
    `工作区文件\n${files || "- 无"}`;
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
  populateProviderTypeOptions(providerType.value || "");
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
  populateProviderTypeOptions(providerType.value || "");
  providerPageSize.value = String(state.providerPage.limit || 10);
  renderProviders();
}

async function ensureLocalModelsPage(force = false) {
  if (!state.loaded.localModelRefs || force) {
    await loadLocalModels();
    state.loaded.localModelRefs = true;
  }
  if (!state.loaded.localModelPage || force) {
    await loadLocalModelPage();
    state.loaded.localModelPage = true;
  }
  if (localModelPageSize) {
    localModelPageSize.value = String(state.localModelPage.limit || 10);
  }
  if (state.editingLocalModelId && !state.localModels.some((item) => item.id === state.editingLocalModelId)) {
    resetLocalModelForm();
    closeLocalModelModal();
  }
  renderLocalModels();
}

async function ensureRetrievalSettingsPage(force = false) {
  if (!state.loaded.providerRefs || force) {
    await loadProviders();
    state.loaded.providerRefs = true;
  }
  if (!state.loaded.localModelRefs || force) {
    await loadLocalModels();
    state.loaded.localModelRefs = true;
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

async function ensureSkillGroupCatalog(force = false) {
  if (!state.loaded.skillGroupCatalog || force) {
    await loadSkillGroupCatalog();
    state.loaded.skillGroupCatalog = true;
  }
  if (state.editingSkillGroupId && !state.skillGroupCatalog.some((item) => item.id === state.editingSkillGroupId)) {
    state.editingSkillGroupId = null;
  }
  renderSkillImportGroupOptions();
}

async function ensureSkillGroupManagementPage(force = false) {
  if (!state.loaded.skillGroupPage || force) {
    await loadSkillGroupPage();
    state.loaded.skillGroupPage = true;
  }
  if (skillGroupPageSize) {
    skillGroupPageSize.value = String(state.skillGroupPage.limit || 10);
  }
  renderSkillGroupManagementList();
}

async function ensureSkillsPage(force = false) {
  await ensureSkillGroupCatalog(force);
  await ensureSkillGroupManagementPage(force);
  if (!state.loaded.skillRefs || force) {
    await loadSkills();
    state.loaded.skillRefs = true;
  }
  if (!state.loaded.skillPage || force) {
    await loadSkillPage();
    state.loaded.skillPage = true;
  }
  if (state.skillPage.groupKey && !state.skillGroups.some((item) => String(item.key || "") === state.skillPage.groupKey)) {
    state.skillPage.groupKey = "";
    await loadSkillPage();
  }
  if (skillPageSize) {
    skillPageSize.value = String(state.skillPage.limit || 10);
  }
  renderSkillGroupSkillOptions();
  if (state.editingSkillId) {
    const current = state.skills.find((item) => item.id === state.editingSkillId) || null;
    if (!current) {
      resetSkillForm();
      closeSkillModal();
    } else if (force && !skillModal?.classList.contains("hidden")) {
      fillSkillForm(current);
    }
  }
  if (state.editingSkillGroupId) {
    const currentGroup = state.skillGroupCatalog.find((item) => item.id === state.editingSkillGroupId) || null;
    if (!currentGroup) {
      resetSkillGroupForm();
      closeSkillGroupModal();
    } else if (force && !skillGroupModal?.classList.contains("hidden")) {
      fillSkillGroupForm(currentGroup);
    }
  }
  renderSkills();
  renderSkillManagementView();
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
  if (!state.loaded.knowledgeBasePage || force) {
    await loadKnowledgeBasePage();
    state.loaded.knowledgeBasePage = true;
  }
  if (knowledgeBasePageSize) {
    knowledgeBasePageSize.value = String(state.knowledgeBasePage.limit || 10);
  }
  if (state.editingKnowledgeBaseId && !state.knowledgeBases.some((item) => item.id === state.editingKnowledgeBaseId)) {
    resetKnowledgeBaseForm();
    closeKnowledgeBaseModal();
  }
  renderKnowledgeBases();
  renderKnowledgeBaseDocuments();
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
  if (!state.loaded.reviewPolicyPage || force) {
    await loadReviewPolicyPage();
    state.loaded.reviewPolicyPage = true;
  }
  if (reviewPolicyPageSize) {
    reviewPolicyPageSize.value = String(state.reviewPolicyPage.limit || 10);
  }
  renderReviewPolicyRules();
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
    renderReviewPolicies();
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
  if (!state.loaded.agentDefinitionRefs || force) {
    await loadAgentDefinitions();
    state.loaded.agentDefinitionRefs = true;
  }
  if (!state.loaded.teamDefinitions || force) {
    await loadTeamDefinitions();
    state.loaded.teamDefinitions = true;
  }
  if (!state.loaded.teamDefinitionPage || force) {
    await loadTeamDefinitionPage();
    state.loaded.teamDefinitionPage = true;
  }
  if (teamDefinitionPageSize) {
    teamDefinitionPageSize.value = String(state.teamDefinitionPage.limit || 10);
  }
  if (state.editingTeamDefinitionId) {
    const current = state.teamDefinitions.find((item) => item.id === state.editingTeamDefinitionId) || null;
    if (!current) {
      resetTeamDefinitionForm();
    } else {
      if (force) {
        fillTeamDefinitionForm(current);
      } else {
        renderTeamDefinitionLeadAgentOptions(teamDefinitionLeadAgentDefinition?.value || "");
        renderTeamDefinitionMembers();
        renderTeamDefinitions();
      }
    }
  } else {
    renderTeamDefinitionLeadAgentOptions(teamDefinitionLeadAgentDefinition?.value || "");
    renderTeamDefinitionMembers();
    renderTeamDefinitions();
  }
}

async function ensureRuntimePage(force = false) {
  if (!state.loaded.teamDefinitions || force) {
    await loadTeamDefinitions();
    state.loaded.teamDefinitions = true;
  }
  if (!state.loaded.runs || force) {
    await loadRunPage();
    state.loaded.runs = true;
  }
  populateTaskOptions();
  if (runPageSize) {
    runPageSize.value = String(state.runPage.limit || 10);
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
    case "local-models":
      await ensureLocalModelsPage(force);
      break;
    case "retrieval-config":
      await ensureRetrievalSettingsPage(force);
      break;
    case "plugins":
      await ensurePluginsPage(force);
      break;
    case "skills":
      await ensureSkillsPage(force);
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
    case "agent-definitions":
      await ensureAgentDefinitionsPage(force);
      break;
    case "team-definitions":
      await ensureTeamDefinitionsPage(force);
      break;
    case "team-chat":
      await ensureTeamChatPage(force);
      break;
    case "runtime":
      await ensureRuntimePage(force);
      break;
    case "approvals":
      await ensureApprovalsPage(force);
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
        settings: payload.settings || { embedding: { mode: "disabled" }, rerank: { mode: "disabled" } },
        warnings: [],
        updated_at: payload.updated_at || null,
      };
      invalidateData("retrievalSettings", "controlPlane");
      fillRetrievalSettingsForm();
      showResult(retrievalSettingsResult, buildRetrievalSaveResult(payload));
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

localModelForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(localModelResult);
  hideResult(localModelModalResult);
  try {
    const payload = buildLocalModelPayloadFromForm();
    if (!payload.name) {
      throw new Error("本地模型名称不能为空。");
    }
    if (!state.editingLocalModelId && !state.localModelUploadFiles.length && !payload.model_path) {
      throw new Error("请先选择要上传的模型文件夹。");
    }
    const uploadPayload = await buildLocalModelUploadPayload();
    const method = state.editingLocalModelId ? "PUT" : "POST";
    const path = state.editingLocalModelId
      ? `/api/agent-center/local-models/${state.editingLocalModelId}`
      : "/api/agent-center/local-models";
    const requestPayload = uploadPayload ? { ...payload, ...uploadPayload } : payload;
    if (uploadPayload) {
      setLocalModelUploadBusy(true);
    }
    let saved = null;
    try {
      saved = await api(path, { method, body: JSON.stringify(requestPayload) });
    } finally {
      if (uploadPayload) {
        setLocalModelUploadBusy(false);
      }
    }
    if (!state.editingLocalModelId) {
      state.localModelPage.offset = 0;
    }
    invalidateData("localModelRefs", "localModelPage", "retrievalSettings", "controlPlane");
    await ensureLocalModelsPage(true);
    closeLocalModelModal();
    showResult(localModelResult, {
      message: uploadPayload ? "本地模型已上传并保存" : "本地模型已保存",
      id: saved?.id || payload.id || null,
      path: saved?.model_path || payload.model_path || "",
    });
  } catch (error) {
    setLocalModelUploadBusy(false);
    showResult(localModelModalResult, errorResult(error));
  }
});

skillForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(skillModalResult);
  try {
    const isReupload = state.skillModalMode === "reupload";
    setSkillImportBusy(true);
    try {
      const payload = await buildSkillUploadPayload();
      const result = await api("/api/agent-center/skills/import-upload", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      invalidateData("skillGroupCatalog", "skillGroupPage", "skillRefs", "skillPage", "agentDefinitionRefs", "agentTemplateRefs", "controlPlane");
      if (!isReupload) {
        state.skillPage.offset = 0;
      }
      await ensureSkillsPage(true);
      if ((result.imported_count || 0) > 0) {
        closeSkillModal();
      } else {
        showResult(skillModalResult, result);
      }
      return;
      if ((result.imported_count || 0) > 0) {
        closeSkillModal();
        showResult(skillResult, {
          ...result,
          message: isReupload ? "Skill 已重新上传并覆盖" : "Skill 已导入",
        });
        showResult(pluginResult, {
          ...result,
          message: isReupload ? "\u63d2\u4ef6\u5df2\u91cd\u65b0\u4e0a\u4f20\u5e76\u8986\u76d6" : "\u63d2\u4ef6\u5df2\u5bfc\u5165\u5230\u6258\u7ba1\u76ee\u5f55",
        });
      } else {
        showResult(skillModalResult, result);
      }
    } finally {
      setSkillImportBusy(false);
    }
  } catch (error) {
    showResult(skillModalResult, errorResult(error));
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
  hideResult(knowledgeBaseModalResult);
  try {
    const payload = buildKnowledgeBasePayloadFromForm();
    if (!payload.name) {
      throw new Error("知识库名称不能为空。");
    }
    const method = state.editingKnowledgeBaseId ? "PUT" : "POST";
    const path = state.editingKnowledgeBaseId
      ? `/api/agent-center/knowledge-bases/${state.editingKnowledgeBaseId}`
      : "/api/agent-center/knowledge-bases";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    if (!state.editingKnowledgeBaseId) {
      state.knowledgeBasePage.offset = 0;
    }
    invalidateData("knowledgeBaseRefs", "knowledgeBasePage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureKnowledgeBasesPage(true);
    const refreshed =
      state.knowledgeBases.find((item) => item.id === saved.id) ||
      (await api(`/api/agent-center/knowledge-bases/${saved.id}`));
    await fillKnowledgeBaseForm(refreshed);
    showResult(knowledgeBaseModalResult, "知识库已保存。可继续从左侧文档池移入右侧知识库。");
  } catch (error) {
    showResult(knowledgeBaseModalResult, errorResult(error));
  }
});

reviewPolicyForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(reviewPolicyResult);
  hideResult(reviewPolicyModalResult);
  try {
    const payload = buildReviewPolicyPayloadFromForm();
    const method = state.editingReviewPolicyId ? "PUT" : "POST";
    const path = state.editingReviewPolicyId
      ? `/api/agent-center/review-policies/${state.editingReviewPolicyId}`
      : "/api/agent-center/review-policies";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    if (!state.editingReviewPolicyId) {
      state.reviewPolicyPage.offset = 0;
    }
    invalidateData("reviewPolicyRefs", "reviewPolicyPage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
    await ensureReviewPoliciesPage(true);
    const refreshed =
      state.reviewPolicies.find((item) => item.id === saved.id) ||
      (await api(`/api/agent-center/review-policies/${saved.id}`));
    fillReviewPolicyForm(refreshed, { openModal: true });
    showResult(reviewPolicyModalResult, { message: "审核策略已保存", id: saved.id });
  } catch (error) {
    showResult(reviewPolicyModalResult, errorResult(error));
  }
});

/*
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
*/

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

agentTemplateForm?.addEventListener("submit", async (event) => {
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
    const isCreate = !state.editingTeamDefinitionId;
    const payload = buildTeamDefinitionPayloadFromForm();
    const method = state.editingTeamDefinitionId ? "PUT" : "POST";
    const path = state.editingTeamDefinitionId
      ? `/api/agent-center/team-definitions/${state.editingTeamDefinitionId}`
      : "/api/agent-center/team-definitions";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    state.editingTeamDefinitionId = saved.id;
    state.teamDefinitionBaseSpec = clone(dictOrEmpty(saved.spec_json));
    if (isCreate) {
      state.teamDefinitionPage.offset = 0;
    }
    invalidateData("teamDefinitions", "teamDefinitionPage", "controlPlane");
    await ensureTeamDefinitionsPage(true);
    closeTeamDefinitionModal();
    showResult(teamDefinitionResult, { message: "团队管理项已保存", id: saved.id });
  } catch (error) {
    showResult(teamDefinitionModalResult, errorResult(error));
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
      const activeThreadId = taskNewSession?.checked ? "" : currentTaskSessionThreadId(taskTeamDefinition.value);
      if (activeThreadId) {
        payload.thread_id = activeThreadId;
      }
      runBundle = await api(`/api/agent-center/team-definitions/${taskTeamDefinition.value}/tasks`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
    } else {
      throw new Error("请选择 TeamDefinition。");
    }
    const conversationThreadId = resolveConversationThreadId(runBundle);
    if (state.selectedTaskTeamDefinitionId) {
      setTaskSessionThreadId(state.selectedTaskTeamDefinitionId, conversationThreadId);
    }
    if (taskNewSession) {
      taskNewSession.checked = false;
    }
    renderTaskSessionHint();
    state.selectedRunId = runBundle.run.id;
    state.runPage.offset = 0;
    invalidateData("runs", "approvals", "controlPlane");
    await switchPage("runtime", { force: true });
    await loadRunDetail(runBundle.run.id);
    showResult(taskResult, {
      message: "Run 已启动",
      run_id: runBundle.run.id,
      status: runBundle.run.status,
      conversation_thread_id: conversationThreadId || null,
    });
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
localModelOpenCreate?.addEventListener("click", async () => {
  await ensureLocalModelsPage(true);
  resetLocalModelForm({ openModal: true });
});
localModelModalCloseButtons.forEach((button) => button.addEventListener("click", closeLocalModelModal));
localModelCancel?.addEventListener("click", closeLocalModelModal);
localModelPageSize?.addEventListener("change", async () => {
  state.localModelPage.limit = Number(localModelPageSize.value || 10);
  state.localModelPage.offset = 0;
  await ensureLocalModelsPage(true);
});
skillOpenCreate?.addEventListener("click", async () => {
  if (state.skillManagementView === "groups") {
    await ensureSkillsPage(true);
    setSkillManagementView("groups");
    resetSkillGroupForm();
    openSkillGroupModal();
    skillGroupManagementName?.focus();
    return;
  }
  await ensureSkillsPage(true);
  setSkillManagementView("skills");
  openSkillImportModal();
});
skillViewSkills?.addEventListener("click", () => {
  setSkillManagementView("skills");
});
skillViewGroups?.addEventListener("click", async () => {
  await ensureSkillsPage(true);
  if (!state.editingSkillGroupId) {
    resetSkillGroupForm();
  }
  setSkillManagementView("groups");
});
skillGroupOpenManageInline?.addEventListener("click", async () => {
  const selectedGroupIds = skillImportGroups ? getMultiSelectValues(skillImportGroups) : [];
  closeSkillModal();
  await openSkillGroupManagementView(selectedGroupIds[0] || "");
});
skillImportDirectoryInput?.addEventListener("change", async (event) => {
  try {
    const entries = await collectSkillImportEntriesFromInput(event.target?.files || []);
    setSkillImportFiles(entries);
    if (entries.length) {
      await scanSelectedSkillImportFiles();
    }
  } catch (error) {
    showResult(skillModalResult, errorResult(error));
  }
});
skillImportDropzone?.addEventListener("click", () => {
  if (!state.skillImportBusy) {
    skillImportDirectoryInput?.click();
  }
});
skillImportDropzone?.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    if (!state.skillImportBusy) {
      skillImportDirectoryInput?.click();
    }
  }
});
["dragenter", "dragover"].forEach((eventName) => {
  skillImportDropzone?.addEventListener(eventName, (event) => {
    event.preventDefault();
    if (state.skillModalMode !== "import") {
      return;
    }
    state.skillImportDragActive = true;
    renderSkillImportSelection();
  });
});
["dragleave", "dragend"].forEach((eventName) => {
  skillImportDropzone?.addEventListener(eventName, (event) => {
    event.preventDefault();
    const related = event.relatedTarget;
    if (related && skillImportDropzone?.contains?.(related)) {
      return;
    }
    state.skillImportDragActive = false;
    renderSkillImportSelection();
  });
});
skillImportDropzone?.addEventListener("drop", async (event) => {
  event.preventDefault();
  if (state.skillModalMode !== "import") {
    return;
  }
  try {
    const entries = await collectSkillImportEntriesFromDataTransfer(event.dataTransfer);
    setSkillImportFiles(entries);
    if (entries.length) {
      await scanSelectedSkillImportFiles();
    }
  } catch (error) {
    state.skillImportDragActive = false;
    renderSkillImportSelection();
    showResult(skillModalResult, errorResult(error));
  }
});
skillModalCloseButtons.forEach((button) => button.addEventListener("click", closeSkillModal));
skillCancel?.addEventListener("click", closeSkillModal);
skillPreviewCloseButtons.forEach((button) => button.addEventListener("click", closeSkillPreviewModal));
skillValidate?.addEventListener("click", async () => {
  hideResult(skillModalResult);
  try {
    await scanSelectedSkillImportFiles();
  } catch (error) {
    showResult(skillModalResult, errorResult(error));
  }
});
skillGroupModalCloseButtons.forEach((button) => button.addEventListener("click", closeSkillGroupModal));
skillGroupCancel?.addEventListener("click", () => {
  closeSkillGroupModal();
});
skillPageSize?.addEventListener("change", async () => {
  state.skillPage.limit = Number(skillPageSize.value || 10);
  state.skillPage.offset = 0;
  await ensureSkillsPage(true);
});
skillGroupPageSize?.addEventListener("change", async () => {
  state.skillGroupPage.limit = Number(skillGroupPageSize.value || 10);
  state.skillGroupPage.offset = 0;
  await ensureSkillGroupManagementPage(true);
});
skillGroupForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(skillGroupModalResult);
  hideResult(skillGroupResult);
  try {
    const payload = {
      id: state.editingSkillGroupId,
      name: skillGroupManagementName?.value?.trim() || "",
      description: skillGroupManagementDescription?.value?.trim() || "",
      skill_ids: getMultiSelectValues(skillGroupSkills),
    };
    const method = state.editingSkillGroupId ? "PUT" : "POST";
    const path = state.editingSkillGroupId
      ? `/api/agent-center/skill-groups/${state.editingSkillGroupId}`
      : "/api/agent-center/skill-groups";
    const saved = await api(path, { method, body: JSON.stringify(payload) });
    state.editingSkillGroupId = saved.id;
    invalidateData("skillGroupCatalog", "skillGroupPage", "skillRefs", "skillPage", "agentDefinitionRefs", "agentTemplateRefs", "controlPlane");
    await ensureSkillGroupCatalog(true);
    focusSkillGroupPageOn(saved.id);
    await ensureSkillsPage(true);
    closeSkillGroupModal();
    renderSkillGroupManagementList();
    showResult(skillGroupResult, { message: "Skill 分组已保存", id: saved.id });
  } catch (error) {
    showResult(skillGroupModalResult, errorResult(error));
  }
});
agentDefinitionPageSize?.addEventListener("change", async () => {
  state.agentDefinitionPage.limit = Number(agentDefinitionPageSize.value || 10);
  state.agentDefinitionPage.offset = 0;
  await ensureAgentDefinitionsPage(true);
});
teamDefinitionPageSize?.addEventListener("change", async () => {
  state.teamDefinitionPage.limit = Number(teamDefinitionPageSize.value || 10);
  state.teamDefinitionPage.offset = 0;
  await ensureTeamDefinitionsPage(true);
});
runPageSize?.addEventListener("change", async () => {
  state.runPage.limit = Number(runPageSize.value || 10);
  state.runPage.offset = 0;
  await ensureRuntimePage(true);
});
approvalsViewPending?.addEventListener("click", async () => {
  state.approvalPage.view = "pending";
  state.approvalPage.offset = 0;
  await ensureApprovalsPage(true);
});
approvalsViewHistory?.addEventListener("click", async () => {
  state.approvalPage.view = "history";
  state.approvalPage.offset = 0;
  await ensureApprovalsPage(true);
});
approvalPageSize?.addEventListener("change", async () => {
  state.approvalPage.limit = Number(approvalPageSize.value || 10);
  state.approvalPage.offset = 0;
  await ensureApprovalsPage(true);
});
approvalEditModalCloseButtons.forEach((button) => button.addEventListener("click", closeApprovalEditModal));
approvalEditCancel?.addEventListener("click", closeApprovalEditModal);
approvalEditForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(approvalEditResult);
  try {
    const item = state.approvalEditor.item;
    if (!item) {
      throw new Error("审批项不存在。");
    }
    const payload = await resolveApprovalAndResume({
      approvalId: String(item.id || "").trim(),
      runId: String(item.run_id || "").trim(),
      approved: true,
      comment: "Edited from control plane.",
      metadata: buildApprovalEditMetadata(item),
    });
    closeApprovalEditModal();
    await switchPage("runtime", { force: true });
    await loadRunDetail(payload.run.id);
  } catch (error) {
    showResult(approvalEditResult, errorResult(error));
  }
});
teamChatTeamDefinition?.addEventListener("change", async () => {
  try {
    await selectTeamChatTeam(teamChatTeamDefinition.value || "", { allowEmptySelection: false });
  } catch (error) {
    showResult(teamChatResult, errorResult(error));
  }
});
teamChatNewThread?.addEventListener("click", () => {
  startNewTeamChatThread();
});
teamChatOpenTeam?.addEventListener("click", async () => {
  await switchPage("team-definitions");
});
teamChatForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await submitTeamChatMessage();
  } catch (error) {
    showResult(teamChatResult, errorResult(error));
  }
});
teamChatInput?.addEventListener("keydown", async (event) => {
  if (event.key !== "Enter" || event.shiftKey || event.isComposing) {
    return;
  }
  event.preventDefault();
  try {
    await submitTeamChatMessage();
  } catch (error) {
    showResult(teamChatResult, errorResult(error));
  }
});
teamChatInput?.addEventListener("input", () => {
  resizeTeamChatInput();
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
retrievalEmbeddingLocalModel?.addEventListener("change", () => {
  syncRetrievalDynamicForm();
});
retrievalRerankLocalModel?.addEventListener("change", () => {
  syncRetrievalDynamicForm();
});
retrievalSettingsRefresh?.addEventListener("click", async () => {
  try {
    invalidateData("retrievalSettings", "providerRefs", "localModelRefs", "controlPlane");
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
  openPluginUploadModal();
});
pluginModalCloseButtons.forEach((button) => button.addEventListener("click", closePluginModal));
pluginCancel.addEventListener("click", closePluginModal);
pluginImportModalCloseButtons.forEach((button) => button.addEventListener("click", closePluginImportModal));
pluginImportCancel?.addEventListener("click", closePluginImportModal);
pluginImportValidate?.addEventListener("click", async () => {
  hideResult(pluginImportResult);
  try {
    await scanSelectedPluginImportFiles();
  } catch (error) {
    showResult(pluginImportResult, errorResult(error));
  }
});
pluginImportForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  hideResult(pluginImportResult);
  hideResult(pluginResult);
  try {
    const isReupload = state.pluginImportMode === "reupload";
    setPluginImportBusy(true);
    try {
      const payload = await buildPluginUploadPayload();
      const scan =
        state.pluginImportScanResult ||
        (await api("/api/agent-center/plugins/scan-upload", {
          method: "POST",
          body: JSON.stringify(payload),
        }));
      state.pluginImportScanResult = scan;
      ensurePluginImportMatchesTarget(scan);
      const result = await api("/api/agent-center/plugins/import-upload", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      invalidateData("pluginPage", "pluginRefs", "agentDefinitionRefs", "agentTemplateRefs", "controlPlane");
      if (!isReupload) {
        state.pluginPage.offset = 0;
      }
      await ensurePluginsPage(true);
      if ((result.imported_count || 0) > 0) {
        closePluginImportModal();
        showResult(pluginResult, {
          ...result,
          message: "插件已导入到受管目录，可直接用于 DeepAgents Tools。",
        });
      } else {
        showResult(pluginImportResult, result);
      }
    } finally {
      setPluginImportBusy(false);
    }
  } catch (error) {
    setPluginImportBusy(false);
    showResult(pluginImportResult, errorResult(error));
  }
});
pluginImportDirectoryInput?.addEventListener("change", async (event) => {
  try {
    const entries = await collectSkillImportEntriesFromInput(event.target?.files || []);
    setPluginImportFiles(entries);
    if (entries.length) {
      await scanSelectedPluginImportFiles();
    }
  } catch (error) {
    showResult(pluginImportResult, errorResult(error));
  }
});
pluginImportDropzone?.addEventListener("click", () => {
  if (!state.pluginImportBusy) {
    pluginImportDirectoryInput?.click();
  }
});
pluginImportDropzone?.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    if (!state.pluginImportBusy) {
      pluginImportDirectoryInput?.click();
    }
  }
});
["dragenter", "dragover"].forEach((eventName) => {
  pluginImportDropzone?.addEventListener(eventName, (event) => {
    event.preventDefault();
    state.pluginImportDragActive = true;
    renderPluginImportSelection();
  });
});
["dragleave", "dragend"].forEach((eventName) => {
  pluginImportDropzone?.addEventListener(eventName, (event) => {
    event.preventDefault();
    const related = event.relatedTarget;
    if (related && pluginImportDropzone?.contains?.(related)) {
      return;
    }
    state.pluginImportDragActive = false;
    renderPluginImportSelection();
  });
});
pluginImportDropzone?.addEventListener("drop", async (event) => {
  event.preventDefault();
  try {
    const entries = await collectSkillImportEntriesFromDataTransfer(event.dataTransfer);
    setPluginImportFiles(entries);
    if (entries.length) {
      await scanSelectedPluginImportFiles();
    }
  } catch (error) {
    state.pluginImportDragActive = false;
    renderPluginImportSelection();
    showResult(pluginImportResult, errorResult(error));
  }
});
responsibilitySpecOpenRoleCreate?.addEventListener("click", async () => {
  await ensureResponsibilitySpecsPage(true);
  resetStaticMemoryForm("role");
  openStaticMemoryModal("role");
});
staticMemoryModalCloseButtons.forEach((button) => button.addEventListener("click", closeStaticMemoryModal));
staticMemoryCancel?.addEventListener("click", closeStaticMemoryModal);
localModelUploadFolderButton?.addEventListener("click", () => {
  localModelFolderInput?.click();
});
localModelFolderInput?.addEventListener("change", async () => {
  try {
    setLocalModelUploadFiles(await collectSkillImportEntriesFromInput(localModelFolderInput.files));
  } catch (error) {
    showResult(localModelModalResult, errorResult(error));
  }
});
localModelDropzone?.addEventListener("click", () => {
  if (!state.localModelUploadBusy) {
    localModelFolderInput?.click();
  }
});
localModelDropzone?.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    if (!state.localModelUploadBusy) {
      localModelFolderInput?.click();
    }
  }
});
localModelDropzone?.addEventListener("dragenter", (event) => {
  event.preventDefault();
  if (state.localModelUploadBusy) {
    return;
  }
  state.localModelUploadDragActive = true;
  renderLocalModelUploadSelection();
});
localModelDropzone?.addEventListener("dragover", (event) => {
  event.preventDefault();
  if (state.localModelUploadBusy) {
    return;
  }
  state.localModelUploadDragActive = true;
  renderLocalModelUploadSelection();
});
localModelDropzone?.addEventListener("dragleave", (event) => {
  if (state.localModelUploadBusy) {
    return;
  }
  if (!localModelDropzone?.contains(event.relatedTarget)) {
    state.localModelUploadDragActive = false;
    renderLocalModelUploadSelection();
  }
});
localModelDropzone?.addEventListener("drop", async (event) => {
  event.preventDefault();
  if (state.localModelUploadBusy) {
    return;
  }
  state.localModelUploadDragActive = false;
  renderLocalModelUploadSelection();
  try {
    setLocalModelUploadFiles(await collectSkillImportEntriesFromDataTransfer(event.dataTransfer));
  } catch (error) {
    showResult(localModelModalResult, errorResult(error));
  }
});
knowledgeBaseOpenCreate?.addEventListener("click", async () => {
  await ensureKnowledgeBasesPage(true);
  resetKnowledgeBaseForm({ openModal: true });
  await refreshKnowledgeBasePoolDocuments();
});
knowledgeBaseModalCloseButtons.forEach((button) => button.addEventListener("click", closeKnowledgeBaseModal));
knowledgeBaseCancel?.addEventListener("click", closeKnowledgeBaseModal);
knowledgeBasePageSize?.addEventListener("change", async () => {
  state.knowledgeBasePage.limit = Number(knowledgeBasePageSize.value || 10);
  state.knowledgeBasePage.offset = 0;
  await ensureKnowledgeBasesPage(true);
});
knowledgeBaseStageSelectAll?.addEventListener("change", () => {
  setAllKnowledgeBaseStageSelections(Boolean(knowledgeBaseStageSelectAll.checked));
});
knowledgeBaseStageRemove?.addEventListener("click", () => {
  const selectedIds = selectedKnowledgeStageIds();
  if (!selectedIds.length) {
    return;
  }
  if (!window.confirm(`确认从文档池删除选中的 ${selectedIds.length} 个文档？`)) {
    return;
  }
  deleteKnowledgeBasePoolDocuments(selectedIds).catch((error) => {
    showResult(knowledgeBaseModalResult, errorResult(error));
  });
});
knowledgeBaseStageMove?.addEventListener("click", async () => {
  try {
    await moveSelectedKnowledgeStageItemsToDocuments();
  } catch (error) {
    showResult(knowledgeBaseModalResult, errorResult(error));
  }
});
knowledgeBaseDocumentMoveBack?.addEventListener("click", async () => {
  try {
    await moveSelectedKnowledgeDocumentsToStage();
  } catch (error) {
    showResult(knowledgeBaseModalResult, errorResult(error));
  }
});
knowledgeBaseDocumentQuery?.addEventListener("input", () => {
  state.knowledgeBaseDocumentPage.query = knowledgeBaseDocumentQuery.value.trim();
  window.clearTimeout(knowledgeBaseDocumentQueryTimer);
  knowledgeBaseDocumentQueryTimer = window.setTimeout(async () => {
    await refreshKnowledgeBaseDocuments({ resetOffset: true });
  }, 220);
});
knowledgeBaseDocumentStatus?.addEventListener("change", async () => {
  state.knowledgeBaseDocumentPage.embeddingStatus = knowledgeBaseDocumentStatus.value || "all";
  await refreshKnowledgeBaseDocuments({ resetOffset: true });
});
knowledgeBasePoolQuery?.addEventListener("input", () => {
  state.knowledgeBasePoolPage.query = knowledgeBasePoolQuery.value.trim();
  window.clearTimeout(knowledgeBasePoolQueryTimer);
  knowledgeBasePoolQueryTimer = window.setTimeout(async () => {
    await refreshKnowledgeBasePoolDocuments({ resetOffset: true });
  }, 220);
});
knowledgeDocumentSelectAll?.addEventListener("change", () => {
  setAllKnowledgeBaseDocumentSelections(Boolean(knowledgeDocumentSelectAll.checked));
});
knowledgeDocumentEmbedAdd?.addEventListener("click", async () => {
  try {
    await runKnowledgeDocumentBulkEmbeddingAction();
  } catch (error) {
    showResult(knowledgeBaseModalResult, errorResult(error));
  }
});
knowledgeBaseUploadFolder?.addEventListener("click", () => {
  knowledgeBaseFolderInput?.click();
});
knowledgeBaseFileInput?.addEventListener("change", async () => {
  try {
    await uploadKnowledgeBasePoolEntries(await collectSkillImportEntriesFromInput(knowledgeBaseFileInput.files));
  } catch (error) {
    showResult(knowledgeBaseModalResult, errorResult(error));
  }
});
knowledgeBaseFolderInput?.addEventListener("change", async () => {
  try {
    await uploadKnowledgeBasePoolEntries(await collectSkillImportEntriesFromInput(knowledgeBaseFolderInput.files));
  } catch (error) {
    showResult(knowledgeBaseModalResult, errorResult(error));
  }
});
knowledgeBaseDropzone?.addEventListener("click", (event) => {
  if (event.target instanceof Element && event.target.closest("button")) {
    return;
  }
  if (!state.knowledgeBaseUploadBusy) {
    knowledgeBaseFileInput?.click();
  }
});
knowledgeBaseDropzone?.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    if (!state.knowledgeBaseUploadBusy) {
      knowledgeBaseFileInput?.click();
    }
  }
});
knowledgeBaseDropzone?.addEventListener("dragenter", (event) => {
  event.preventDefault();
  if (state.knowledgeBaseUploadBusy) {
    return;
  }
  state.knowledgeBaseUploadDragActive = true;
  renderKnowledgeBaseUploadSelection();
});
knowledgeBaseDropzone?.addEventListener("dragover", (event) => {
  event.preventDefault();
  if (state.knowledgeBaseUploadBusy) {
    return;
  }
  state.knowledgeBaseUploadDragActive = true;
  renderKnowledgeBaseUploadSelection();
});
knowledgeBaseDropzone?.addEventListener("dragleave", (event) => {
  if (state.knowledgeBaseUploadBusy) {
    return;
  }
  if (!knowledgeBaseDropzone?.contains(event.relatedTarget)) {
    state.knowledgeBaseUploadDragActive = false;
    renderKnowledgeBaseUploadSelection();
  }
});
knowledgeBaseDropzone?.addEventListener("drop", async (event) => {
  event.preventDefault();
  if (state.knowledgeBaseUploadBusy) {
    return;
  }
  state.knowledgeBaseUploadDragActive = false;
  renderKnowledgeBaseUploadSelection();
  try {
    await uploadKnowledgeBasePoolEntries(await collectSkillImportEntriesFromDataTransfer(event.dataTransfer));
  } catch (error) {
    showResult(knowledgeBaseModalResult, errorResult(error));
  }
});
knowledgeBaseUploadSelection?.addEventListener("change", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  const stageId = String(target.dataset.knowledgeStageSelect || "").trim();
  if (!stageId) {
    return;
  }
  toggleKnowledgeBaseStageSelection(stageId, target.checked);
});
knowledgeBaseDocumentList?.addEventListener("change", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  const documentId = String(target.dataset.knowledgeDocumentSelect || "").trim();
  if (!documentId) {
    return;
  }
  toggleKnowledgeBaseDocumentSelection(documentId, target.checked);
});
reviewPolicyOpenCreate?.addEventListener("click", async () => {
  await ensureReviewPoliciesPage(true);
  resetReviewPolicyForm({ openModal: true });
});
reviewPolicyRuleAdd?.addEventListener("click", () => {
  addReviewPolicyRule();
});
reviewPolicyModalCloseButtons.forEach((button) => button.addEventListener("click", closeReviewPolicyModal));
reviewPolicyCancel?.addEventListener("click", closeReviewPolicyModal);
reviewPolicyPageSize?.addEventListener("change", async () => {
  state.reviewPolicyPage.limit = Number(reviewPolicyPageSize.value || 10);
  state.reviewPolicyPage.offset = 0;
  await ensureReviewPoliciesPage(true);
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
agentTemplateOpenCreate?.addEventListener("click", async () => {
  await ensureAgentTemplateEditorData(true);
  resetAgentTemplateForm();
  openAgentTemplateModal();
});
agentTemplateModalCloseButtons.forEach((button) => button.addEventListener("click", closeAgentTemplateModal));
agentTemplateCancel?.addEventListener("click", closeAgentTemplateModal);
agentTemplateProvider?.addEventListener("change", () => {
  renderModelSelect(agentTemplateModel, providerModelsByType(agentTemplateProvider?.value || "", "chat"));
});
agentTemplatePageSize?.addEventListener("change", async () => {
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
teamDefinitionPreviewCloseButtons.forEach((button) => button.addEventListener("click", closeTeamDefinitionPreviewModal));
teamDefinitionCancel?.addEventListener("click", closeTeamDefinitionModal);

taskTeamDefinition?.addEventListener("change", () => {
  state.selectedTaskTeamDefinitionId = taskTeamDefinition.value || null;
  renderTaskSessionHint();
});
taskNewSession?.addEventListener("change", renderTaskSessionHint);

document.addEventListener("click", async (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }

  const teamChatResultClose = event.target.closest("[data-team-chat-result-close]");
  if (teamChatResultClose) {
    hideResult(teamChatResult);
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
      await switchPage("providers");
      const provider = await api(`/api/agent-center/providers/${providerEdit.dataset.providerEdit}`);
      fillProviderForm(provider);
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

  const localModelPageButton = event.target.closest("[data-local-model-page]");
  if (localModelPageButton && !localModelPageButton.hasAttribute("disabled")) {
    state.localModelPage.offset = Number(localModelPageButton.dataset.localModelPage || 0);
    await ensureLocalModelsPage(true);
    return;
  }

  const skillPageButton = event.target.closest("[data-skill-page]");
  if (skillPageButton && !skillPageButton.hasAttribute("disabled")) {
    state.skillPage.offset = Number(skillPageButton.dataset.skillPage || 0);
    await ensureSkillsPage(true);
    return;
  }

  const knowledgeBasePageButton = event.target.closest("[data-knowledge-base-page]");
  if (knowledgeBasePageButton && !knowledgeBasePageButton.hasAttribute("disabled")) {
    state.knowledgeBasePage.offset = Number(knowledgeBasePageButton.dataset.knowledgeBasePage || 0);
    await ensureKnowledgeBasesPage(true);
    return;
  }

  const reviewPolicyPageButton = event.target.closest("[data-review-policy-page]");
  if (reviewPolicyPageButton && !reviewPolicyPageButton.hasAttribute("disabled")) {
    state.reviewPolicyPage.offset = Number(reviewPolicyPageButton.dataset.reviewPolicyPage || 0);
    await ensureReviewPoliciesPage(true);
    return;
  }

  const approvalPageButton = event.target.closest("[data-approval-page]");
  if (approvalPageButton && !approvalPageButton.hasAttribute("disabled")) {
    state.approvalPage.offset = Number(approvalPageButton.dataset.approvalPage || 0);
    await ensureApprovalsPage(true);
    return;
  }

  const skillPreviewFileButton = event.target.closest("[data-skill-preview-file]");
  if (skillPreviewFileButton) {
    await loadSkillPreviewFile(skillPreviewFileButton.dataset.skillPreviewFile || "");
    return;
  }

  const skillGroupPageButton = event.target.closest("[data-skill-group-page]");
  if (skillGroupPageButton && !skillGroupPageButton.hasAttribute("disabled")) {
    state.skillGroupPage.offset = Number(skillGroupPageButton.dataset.skillGroupPage || 0);
    await ensureSkillGroupManagementPage(true);
    return;
  }

  const skillPreviewButton = event.target.closest("[data-skill-preview]");
  if (skillPreviewButton) {
    try {
      await openSkillPreview(skillPreviewButton.dataset.skillPreview || "");
    } catch (error) {
      showSkillPageError(error);
    }
    return;
  }

  const skillEdit = event.target.closest("[data-skill-edit]");
  if (skillEdit) {
    try {
      await switchPage("skills");
      const skill = state.skills.find((item) => item.id === skillEdit.dataset.skillEdit) || (await api(`/api/agent-center/skills/${skillEdit.dataset.skillEdit}`));
      fillSkillForm(skill, { openModal: true });
    } catch (error) {
      showSkillPageError(error);
    }
    return;
  }

  const skillDelete = event.target.closest("[data-skill-delete]");
  if (skillDelete) {
    const skillId = skillDelete.dataset.skillDelete;
    const skill = state.skillPage.items.find((item) => item.id === skillId) || state.skills.find((item) => item.id === skillId) || null;
    const skillNameText = skill?.name || skillId;
    if (!window.confirm(`确认删除 Skill“${skillNameText}”？`)) {
      return;
    }
    try {
      if (state.skillPage.items.length === 1 && state.skillPage.offset > 0) {
        state.skillPage.offset = Math.max(0, state.skillPage.offset - state.skillPage.limit);
      }
      await api(`/api/agent-center/skills/${skillId}`, { method: "DELETE" });
      if (state.editingSkillId === skillId) {
        resetSkillForm();
        closeSkillModal();
      }
      invalidateData("skillGroupCatalog", "skillGroupPage", "skillRefs", "skillPage", "agentDefinitionRefs", "agentTemplateRefs", "controlPlane");
      await ensureSkillsPage(true);
    } catch (error) {
      showSkillPageError(error);
    }
    return;
  }

  const skillGroupEdit = event.target.closest("[data-skill-group-edit]");
  if (skillGroupEdit) {
    try {
      await ensureSkillsPage(true);
      const group =
        state.skillGroupCatalog.find((item) => item.id === skillGroupEdit.dataset.skillGroupEdit) ||
        (await api(`/api/agent-center/skill-groups/${skillGroupEdit.dataset.skillGroupEdit}`));
      fillSkillGroupForm(group);
      setSkillManagementView("groups");
      openSkillGroupModal();
    } catch (error) {
      showResult(skillGroupResult, errorResult(error));
    }
    return;
  }

  const skillGroupDelete = event.target.closest("[data-skill-group-delete]");
  if (skillGroupDelete) {
    const groupId = skillGroupDelete.dataset.skillGroupDelete;
    const group = state.skillGroupCatalog.find((item) => item.id === groupId) || null;
    const groupNameText = group?.name || groupId;
    if (!window.confirm(`确认删除 Skill 分组“${groupNameText}”？删除后该分组下的 Skill 会变为未分组。`)) {
      return;
    }
    try {
      if (state.skillGroupPage.items.length === 1 && state.skillGroupPage.offset > 0) {
        state.skillGroupPage.offset = Math.max(0, state.skillGroupPage.offset - state.skillGroupPage.limit);
      }
      await api(`/api/agent-center/skill-groups/${groupId}`, { method: "DELETE" });
      if (state.editingSkillGroupId === groupId) {
        resetSkillGroupForm();
        closeSkillGroupModal();
      }
      invalidateData("skillGroupCatalog", "skillGroupPage", "skillRefs", "skillPage", "agentDefinitionRefs", "agentTemplateRefs", "controlPlane");
      await ensureSkillGroupCatalog(true);
      await ensureSkillsPage(true);
      setSkillManagementView("groups");
      showResult(skillGroupResult, { message: "Skill 分组已删除", id: groupId });
    } catch (error) {
      showResult(skillGroupResult, errorResult(error));
    }
    return;
  }

  const agentDefinitionPageButton = event.target.closest("[data-agent-definition-page]");
  if (agentDefinitionPageButton && !agentDefinitionPageButton.hasAttribute("disabled")) {
    state.agentDefinitionPage.offset = Number(agentDefinitionPageButton.dataset.agentDefinitionPage || 0);
    await ensureAgentDefinitionsPage(true);
    return;
  }

  const teamDefinitionPageButton = event.target.closest("[data-team-definition-page]");
  if (teamDefinitionPageButton && !teamDefinitionPageButton.hasAttribute("disabled")) {
    state.teamDefinitionPage.offset = Number(teamDefinitionPageButton.dataset.teamDefinitionPage || 0);
    await ensureTeamDefinitionsPage(true);
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

  const runPageButton = event.target.closest("[data-run-page]");
  if (runPageButton && !runPageButton.hasAttribute("disabled")) {
    state.runPage.offset = Number(runPageButton.dataset.runPage || 0);
    await ensureRuntimePage(true);
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
        throw new Error("\u63d2\u4ef6\u672a\u914d\u7f6e\u5b89\u88c5\u8def\u5f84\u3002");
      }
      await api("/api/agent-center/plugins/validate-package", {
        method: "POST",
        body: JSON.stringify({ path: plugin.install_path }),
      });
      showResult(pluginResult, "\u63d2\u4ef6\u6821\u9a8c\u901a\u8fc7");
    } catch (error) {
      showResult(pluginResult, "\u63d2\u4ef6\u6821\u9a8c\u672a\u901a\u8fc7");
    }
    return;
  }

  const pluginReupload = event.target.closest("[data-plugin-reupload]");
  if (pluginReupload) {
    try {
      const pluginId = String(pluginReupload.dataset.pluginReupload || "").trim();
      const plugin =
        state.pluginPage.items.find((item) => item.id === pluginId) ||
        state.plugins.find((item) => item.id === pluginId) ||
        (await api(`/api/agent-center/plugins/${pluginId}`));
      openPluginReuploadModal(plugin);
    } catch (error) {
      showResult(pluginResult, errorResult(error));
    }
    return;
  }

  const pluginDelete = event.target.closest("[data-plugin-delete]");
  if (pluginDelete) {
    const pluginId = pluginDelete.dataset.pluginDelete;
    const plugin =
      state.pluginPage.items.find((item) => item.id === pluginId) || state.plugins.find((item) => item.id === pluginId) || null;
    const pluginNameText = pluginDisplayName(plugin || { id: pluginId, name: pluginId });
    if (!window.confirm(`\u786e\u8ba4\u5220\u9664\u63d2\u4ef6\u201c${pluginNameText}\u201d\uff1f`)) {
      return;
    }
    try {
      if (state.pluginPage.items.length === 1 && state.pluginPage.offset > 0) {
        state.pluginPage.offset = Math.max(0, state.pluginPage.offset - state.pluginPage.limit);
      }
      await api(`/api/agent-center/plugins/${pluginId}`, { method: "DELETE" });
      if (state.editingPluginId === pluginId) {
        resetPluginForm();
        closePluginModal();
      }
      invalidateData("pluginPage", "pluginRefs");
      await ensurePluginsPage(true);
      showResult(pluginResult, { message: "\u63d2\u4ef6\u5df2\u5220\u9664", id: pluginId });
    } catch (error) {
      showResult(pluginResult, errorResult(error));
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
      await fillKnowledgeBaseForm(item, { openModal: true });
    } catch (error) {
      showResult(knowledgeBaseResult, errorResult(error));
    }
    return;
  }

  const localModelEdit = event.target.closest("[data-local-model-edit]");
  if (localModelEdit) {
    try {
      await switchPage("local-models");
      const item =
        state.localModels.find((entry) => entry.id === localModelEdit.dataset.localModelEdit) ||
        (await api(`/api/agent-center/local-models/${localModelEdit.dataset.localModelEdit}`));
      fillLocalModelForm(item, { openModal: true });
    } catch (error) {
      showResult(localModelResult, errorResult(error));
    }
    return;
  }

  const localModelDelete = event.target.closest("[data-local-model-delete]");
  if (localModelDelete) {
    const itemId = localModelDelete.dataset.localModelDelete;
    const item = state.localModels.find((entry) => entry.id === itemId) || null;
    if (!window.confirm(`确认删除本地模型“${item?.name || item?.id || itemId}”？`)) {
      return;
    }
    try {
      if (state.localModelPage.items.length === 1 && state.localModelPage.offset > 0) {
        state.localModelPage.offset = Math.max(0, state.localModelPage.offset - state.localModelPage.limit);
      }
      await api(`/api/agent-center/local-models/${itemId}`, { method: "DELETE" });
      if (state.editingLocalModelId === itemId) {
        resetLocalModelForm();
        closeLocalModelModal();
      }
      invalidateData("localModelRefs", "localModelPage", "retrievalSettings", "controlPlane");
      await ensureLocalModelsPage(true);
      showResult(localModelResult, { message: "本地模型已删除", id: itemId });
    } catch (error) {
      showResult(localModelResult, errorResult(error));
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
      if (state.knowledgeBasePage.items.length === 1 && state.knowledgeBasePage.offset > 0) {
        state.knowledgeBasePage.offset = Math.max(0, state.knowledgeBasePage.offset - state.knowledgeBasePage.limit);
      }
      await api(`/api/agent-center/knowledge-bases/${itemId}`, { method: "DELETE" });
      if (state.editingKnowledgeBaseId === itemId) {
        resetKnowledgeBaseForm();
        closeKnowledgeBaseModal();
      }
      invalidateData("knowledgeBaseRefs", "knowledgeBasePage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
      await ensureKnowledgeBasesPage(true);
      showResult(knowledgeBaseResult, { message: "知识库已删除", id: itemId });
    } catch (error) {
      showResult(knowledgeBaseResult, errorResult(error));
    }
    return;
  }

  const knowledgeDocumentDelete = event.target.closest("[data-knowledge-document-delete]");
  const knowledgeDocumentEmbeddingAction = event.target.closest("[data-knowledge-document-embedding-action]");
  if (knowledgeDocumentEmbeddingAction) {
    const action = String(knowledgeDocumentEmbeddingAction.dataset.knowledgeDocumentEmbeddingAction || "").trim();
    const documentId = String(knowledgeDocumentEmbeddingAction.dataset.knowledgeDocumentId || "").trim();
    if (!action || !documentId) {
      return;
    }
    try {
      await runKnowledgeDocumentEmbeddingAction(action, [documentId]);
    } catch (error) {
      showResult(knowledgeBaseModalResult, errorResult(error));
    }
    return;
  }

  if (knowledgeDocumentDelete) {
    const documentId = knowledgeDocumentDelete.dataset.knowledgeDocumentDelete;
    if (!documentId) {
      return;
    }
    if (!window.confirm("确认删除这个已入库文件？")) {
      return;
    }
    try {
      await api(`/api/agent-center/knowledge-documents/${documentId}`, { method: "DELETE" });
      invalidateData("knowledgeBaseRefs", "knowledgeBasePage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
      await ensureKnowledgeBasesPage(true);
      if (state.editingKnowledgeBaseId) {
        await refreshKnowledgeBaseDocuments();
        await refreshKnowledgeBasePoolDocuments();
      }
      showResult(knowledgeBaseModalResult, { message: "文件已从知识库移除", id: documentId });
    } catch (error) {
      showResult(knowledgeBaseModalResult, errorResult(error));
    }
    return;
  }

  const knowledgeStageOpen = event.target.closest("[data-knowledge-stage-open]");
  if (knowledgeStageOpen && !event.target.closest("button, input, label, a")) {
    const stageId = String(knowledgeStageOpen.dataset.knowledgeStageOpen || "").trim();
    if (stageId) {
      const selected = new Set(selectedKnowledgeStageIds());
      toggleKnowledgeBaseStageSelection(stageId, !selected.has(stageId));
    }
    return;
  }

  const knowledgeDocumentOpen = event.target.closest("[data-knowledge-document-open]");
  if (knowledgeDocumentOpen && !event.target.closest("button, input, label, a")) {
    const documentId = String(knowledgeDocumentOpen.dataset.knowledgeDocumentOpen || "").trim();
    if (documentId) {
      const selected = new Set(selectedKnowledgeDocumentIds());
      toggleKnowledgeBaseDocumentSelection(documentId, !selected.has(documentId));
    }
    return;
  }

  const reviewPolicyRuleRemove = event.target.closest("[data-review-policy-rule-remove]");
  if (reviewPolicyRuleRemove) {
    removeReviewPolicyRule(Number(reviewPolicyRuleRemove.dataset.reviewPolicyRuleRemove));
    return;
  }

  const reviewPolicyRuleToggleAll = event.target.closest("[data-review-policy-rule-toggle-all]");
  if (reviewPolicyRuleToggleAll) {
    toggleReviewPolicyRuleAllDecisions(Number(reviewPolicyRuleToggleAll.dataset.reviewPolicyRuleToggleAll));
    return;
  }

  const reviewPolicyEdit = event.target.closest("[data-review-policy-edit]");
  if (reviewPolicyEdit) {
    try {
      await switchPage("review-policies");
      const item =
        state.reviewPolicies.find((entry) => entry.id === reviewPolicyEdit.dataset.reviewPolicyEdit) ||
        (await api(`/api/agent-center/review-policies/${reviewPolicyEdit.dataset.reviewPolicyEdit}`));
      fillReviewPolicyForm(item, { openModal: true });
    } catch (error) {
      showResult(reviewPolicyResult, errorResult(error));
    }
    return;
  }

  const reviewPolicyDelete = event.target.closest("[data-review-policy-delete]");
  if (reviewPolicyDelete) {
    const itemId = reviewPolicyDelete.dataset.reviewPolicyDelete;
    const item = state.reviewPolicies.find((entry) => entry.id === itemId) || null;
    if (!window.confirm(`确认删除审核策略“${item?.name || itemId}”？`)) {
      return;
    }
    try {
      if (state.reviewPolicyPage.items.length === 1 && state.reviewPolicyPage.offset > 0) {
        state.reviewPolicyPage.offset = Math.max(0, state.reviewPolicyPage.offset - state.reviewPolicyPage.limit);
      }
      await api(`/api/agent-center/review-policies/${itemId}`, { method: "DELETE" });
      if (state.editingReviewPolicyId === itemId) {
        resetReviewPolicyForm();
        closeReviewPolicyModal();
      }
      invalidateData("reviewPolicyRefs", "reviewPolicyPage", "agentDefinitionRefs", "teamDefinitions", "controlPlane");
      await ensureReviewPoliciesPage(true);
      showResult(reviewPolicyResult, { message: "审核策略已删除", id: itemId });
    } catch (error) {
      showResult(reviewPolicyResult, errorResult(error));
    }
    return;
  }

  /*
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
  */

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

  const teamDefinitionDelete = event.target.closest("[data-team-definition-delete]");
  if (teamDefinitionDelete) {
    const definitionId = teamDefinitionDelete.dataset.teamDefinitionDelete;
    const definition =
      state.teamDefinitionPage.items.find((item) => item.id === definitionId) || state.teamDefinitions.find((item) => item.id === definitionId) || null;
    const definitionName = definition?.name || definitionId;
    if (!window.confirm(`确认删除团队“${definitionName}”？`)) {
      return;
    }
    try {
      if (state.teamDefinitionPage.items.length === 1 && state.teamDefinitionPage.offset > 0) {
        state.teamDefinitionPage.offset = Math.max(0, state.teamDefinitionPage.offset - state.teamDefinitionPage.limit);
      }
      await api(`/api/agent-center/team-definitions/${definitionId}`, { method: "DELETE" });
      if (state.editingTeamDefinitionId === definitionId) {
        resetTeamDefinitionForm();
        closeTeamDefinitionModal();
      }
      invalidateData("teamDefinitions", "teamDefinitionPage", "controlPlane");
      await ensureTeamDefinitionsPage(true);
      showResult(teamDefinitionResult, { message: "团队已删除", id: definitionId });
    } catch (error) {
      showResult(teamDefinitionResult, errorResult(error));
    }
    return;
  }

  const teamDefinitionTest = event.target.closest("[data-team-definition-test]");
  if (teamDefinitionTest) {
    try {
      await openTeamChatPageForTeamDefinition(teamDefinitionTest.dataset.teamDefinitionTest || null);
    } catch (error) {
      showResult(teamChatResult, errorResult(error));
    }
    return;
  }

  const teamDefinitionPreview = event.target.closest("[data-team-definition-preview]");
  if (teamDefinitionPreview) {
    const definitionId = teamDefinitionPreview.dataset.teamDefinitionPreview;
    const definition =
      state.teamDefinitionPage.items.find((item) => item.id === definitionId) || state.teamDefinitions.find((item) => item.id === definitionId) || null;
    const previewSeed = definition ? { team_definition: definition } : { team_definition: { id: definitionId, name: definitionId } };
    resetTeamDefinitionPreviewState();
    openTeamDefinitionPreviewModal();
    teamDefinitionPreviewPayload(previewSeed, { loading: true });
    try {
      const payload = await api(`/api/agent-center/team-definitions/${definitionId}/compile`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      teamDefinitionPreviewPayload(payload);
    } catch (error) {
      teamDefinitionPreviewPayload(previewSeed, { errorText: teamDefinitionPreviewErrorText(error) });
    }
    return;
  }

  const teamChatThreadDeleteButton = event.target.closest("[data-team-chat-thread-delete]");
  if (teamChatThreadDeleteButton) {
    const threadRecordId = String(teamChatThreadDeleteButton.dataset.teamChatThreadDelete || "").trim();
    const thread = state.teamChat.threads.find((item) => String(item.id || "") === threadRecordId) || null;
    const threadTitle = thread?.title || "该会话";
    if (!window.confirm(`确认删除团队测试会话“${threadTitle}”？删除后不可恢复。`)) {
      return;
    }
    try {
      await deleteTeamChatThread(threadRecordId);
    } catch (error) {
      showResult(teamChatResult, errorResult(error));
    }
    return;
  }

  const teamChatThreadButton = event.target.closest("[data-team-chat-thread]");
  if (teamChatThreadButton) {
    try {
      await selectTeamChatThread(teamChatThreadButton.dataset.teamChatThread);
    } catch (error) {
      showResult(teamChatResult, errorResult(error));
    }
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
      const payload = await resolveApprovalAndResume({
        approvalId: approveButton.dataset.approvalApprove,
        runId: approveButton.dataset.runId,
        approved: true,
        comment: "Approved from control plane.",
      });
      await switchPage("runtime", { force: true });
      await loadRunDetail(payload.run.id);
    } catch (error) {
      showResult(taskResult, { error: error.message });
    }
    return;
  }

  const editButton = event.target.closest("[data-approval-edit]");
  if (editButton) {
    const approvalId = String(editButton.dataset.approvalEdit || "").trim();
    const item =
      state.approvalPage.items.find((entry) => String(entry?.id || "").trim() === approvalId) ||
      state.approvals.find((entry) => String(entry?.id || "").trim() === approvalId);
    if (!item) {
      showResult(taskResult, { error: "审批项不存在。" });
      return;
    }
    openApprovalEditModal(item);
    return;
  }

  const rejectButton = event.target.closest("[data-approval-reject]");
  if (rejectButton) {
    try {
      const payload = await resolveApprovalAndResume({
        approvalId: rejectButton.dataset.approvalReject,
        runId: rejectButton.dataset.runId,
        approved: false,
        comment: "Rejected from control plane.",
      });
      await switchPage("runtime", { force: true });
      await loadRunDetail(payload.run.id);
    } catch (error) {
      showResult(taskResult, { error: error.message });
    }
    return;
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
  const reviewPolicyRuleField = event.target.closest("[data-review-policy-rule-field]");
  if (reviewPolicyRuleField) {
    updateReviewPolicyRuleField(
      Number(reviewPolicyRuleField.dataset.ruleIndex),
      reviewPolicyRuleField.dataset.reviewPolicyRuleField,
      fieldValue(reviewPolicyRuleField),
    );
  }
  const reviewPolicyRuleDecision = event.target.closest("[data-review-policy-rule-decision]");
  if (reviewPolicyRuleDecision) {
    toggleReviewPolicyRuleDecision(
      Number(reviewPolicyRuleDecision.dataset.ruleIndex),
      reviewPolicyRuleDecision.dataset.reviewPolicyRuleDecision,
      reviewPolicyRuleDecision.checked,
    );
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
resizeTeamChatInput();
window.addEventListener("resize", syncTeamChatViewportHeight);
window.visualViewport?.addEventListener("resize", syncTeamChatViewportHeight);
window.visualViewport?.addEventListener("scroll", syncTeamChatViewportHeight);

(async function bootstrap() {
  try {
    await switchPage(state.activePage);
  } catch (error) {
    showResult(taskResult, { error: error.message });
  }
})();
