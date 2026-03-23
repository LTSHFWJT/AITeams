const state = {
  summary: {},
  storage: null,
  providerTypes: [],
  recentBuilds: [],
  recentRuns: [],
  providers: [],
  providerPage: {
    items: [],
    total: 0,
    limit: 10,
    offset: 0,
    query: "",
    providerType: "",
  },
  plugins: [],
  pluginPage: {
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
  teamTemplates: [],
  builds: [],
  blueprints: [],
  runs: [],
  approvals: [],
  activePage: "overview",
  activeNavSection: "overview",
  editingProviderId: null,
  editingPluginId: null,
  editingAgentTemplateId: null,
  editingTeamTemplateId: null,
  selectedTeamTemplateId: null,
  selectedBuildId: null,
  selectedBlueprintId: null,
  selectedRunId: null,
  providerEditor: {
    models: [],
    savedModels: [],
    editingModelIndex: null,
  },
  loaded: {
    controlPlane: false,
    providerTypes: false,
    providerRefs: false,
    providerPage: false,
    pluginRefs: false,
    pluginPage: false,
    agentTemplateRefs: false,
    agentTemplatePage: false,
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
  plugins: "resources",
  "agent-templates": "resources",
  "team-templates": "orchestration",
  builds: "delivery",
  runtime: "delivery",
  approvals: "delivery",
  blueprints: "delivery",
};

const SECTION_DEFAULT_PAGE = {
  overview: "overview",
  resources: "providers",
  orchestration: "team-templates",
  delivery: "builds",
};

const MODEL_TYPE_LABELS = {
  chat: "\u804a\u5929",
  embedding: "\u5d4c\u5165",
  rerank: "\u91cd\u6392",
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
const pluginModalResult = document.querySelector("#plugin-modal-result");

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

function populatePluginOptions() {
  agentTemplatePlugins.innerHTML =
    state.plugins.map((item) => `<option value="${item.id}">${escapeHtml(item.name)} / ${escapeHtml(item.version)}</option>`).join("") ||
    '<option value="">暂无插件</option>';
}

function populateTeamTemplateOptions() {
  const options = state.teamTemplates
    .map((item) => `<option value="${item.id}" ${item.id === state.selectedTeamTemplateId ? "selected" : ""}>${escapeHtml(item.name)}</option>`)
    .join("");
  buildTeamTemplate.innerHTML = options || '<option value="">暂无团队模板</option>';
}

function populateTaskOptions() {
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
}

function providerPreset(type = providerType.value) {
  return state.providerTypes.find((item) => item.provider_type === type) || null;
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

function openAgentTemplateModal() {
  agentTemplateModal.classList.remove("hidden");
}

function closeAgentTemplateModal() {
  agentTemplateModal.classList.add("hidden");
  hideResult(agentTemplateModalResult);
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
  pluginModalTitle.textContent = "新增插件";
  hideResult(pluginModalResult);
}

function resetAgentTemplateForm() {
  state.editingAgentTemplateId = null;
  agentTemplateName.value = "";
  agentTemplateRole.value = "";
  agentTemplateModel.value = "";
  agentTemplateSkills.value = "";
  agentTemplateGoal.value = "";
  agentTemplateInstructions.value = "";
  agentTemplateDescription.value = "";
  agentTemplateMemoryPolicy.value = "agent_private";
  if (state.providers.length) {
    agentTemplateProvider.value = state.providers[0].id;
  }
  setMultiSelectValues(agentTemplatePlugins, []);
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
  pluginModalTitle.textContent = "编辑插件";
  hideResult(pluginModalResult);
  openPluginModal();
}

function fillAgentTemplateForm(template) {
  state.editingAgentTemplateId = template.id;
  const spec = template.spec_json || {};
  agentTemplateName.value = template.name || "";
  agentTemplateRole.value = template.role || "";
  agentTemplateProvider.value = spec.provider_ref || "";
  agentTemplateModel.value = spec.model || "";
  agentTemplateMemoryPolicy.value = spec.memory_policy || "agent_private";
  agentTemplateSkills.value = (spec.skills || []).join(", ");
  agentTemplateGoal.value = spec.goal || "";
  agentTemplateInstructions.value = spec.instructions || "";
  agentTemplateDescription.value = template.description || "";
  setMultiSelectValues(agentTemplatePlugins, spec.plugin_refs || []);
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
    ["Agent 模板", state.summary.agent_template_count || 0],
    ["团队模板", state.summary.team_template_count || 0],
    ["Build", state.summary.build_count || 0],
    ["蓝图", state.summary.blueprint_count || 0],
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
                <strong title="${item.has_secret ? "\u5df2\u4fdd\u5b58\u5bc6\u94a5" : "\u672a\u4fdd\u5b58\u5bc6\u94a5"}">${escapeHtml(config.base_url || "-")}</strong>
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
  return {
    id: state.editingPluginId,
    key: pluginKey.value.trim(),
    name: pluginName.value.trim(),
    version: pluginVersion.value.trim() || "v1",
    plugin_type: pluginType.value.trim() || "toolset",
    description: pluginDescription.value.trim(),
    install_path: pluginInstallPath.value.trim() || null,
    manifest: {
      workbench_key: pluginWorkbenchKey.value.trim(),
      tools: commaListToArray(pluginTools.value),
      permissions: commaListToArray(pluginPermissions.value),
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
      model: agentTemplateModel.value.trim(),
      memory_policy: agentTemplateMemoryPolicy.value,
      plugin_refs: getMultiSelectValues(agentTemplatePlugins),
      skills: commaListToArray(agentTemplateSkills.value),
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

async function loadProviders() {
  const payload = await api("/api/agent-center/providers");
  state.providers = payload.items || [];
  populateProviderOptions();
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
  populateProviderOptions();
  populatePluginOptions();
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
    case "plugins":
      await ensurePluginsPage(force);
      break;
    case "agent-templates":
      await ensureAgentTemplatesPage(force);
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
    invalidateData("providerPage", "providerRefs", "controlPlane");
    await ensureProvidersPage(true);
    closeProviderModal();
    providerResult?.classList.remove("empty");
    showResult(providerResult, { message: "\u63d0\u4f9b\u65b9\u5df2\u4fdd\u5b58", id: saved.id });
  } catch (error) {
    providerResult?.classList.remove("empty");
    showResult(providerResult, errorResult(error));
  }
});

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
    if (taskBuild.value) {
      payload.build_id = taskBuild.value;
    } else if (taskBlueprint.value) {
      payload.blueprint_id = taskBlueprint.value;
    } else {
      throw new Error("请选择 Build 或内部蓝图。");
    }
    const runBundle = await api("/api/task-releases", {
      method: "POST",
      body: JSON.stringify(payload),
    });
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
agentTemplatePageSize.addEventListener("change", async () => {
  state.agentTemplatePage.limit = Number(agentTemplatePageSize.value || 10);
  state.agentTemplatePage.offset = 0;
  await ensureAgentTemplatesPage(true);
});
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
});

taskBlueprint.addEventListener("change", () => {
  state.selectedBlueprintId = taskBlueprint.value || null;
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
      invalidateData("providerPage", "providerRefs", "controlPlane");
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
  const memberField = event.target.closest("[data-team-member-field]");
  if (memberField) {
    updateMemberField(Number(memberField.dataset.memberIndex), memberField.dataset.teamMemberField, memberField.value);
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

(async function bootstrap() {
  try {
    await switchPage(state.activePage);
  } catch (error) {
    showResult(taskResult, { error: error.message });
  }
})();
