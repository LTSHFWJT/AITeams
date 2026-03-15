const state = {
  presets: [],
  providers: [],
  agents: [],
  skills: [],
  sessions: [],
  knowledgeDocuments: [],
  providerPage: { items: [], count: 0, limit: 10, offset: 0 },
  agentPage: { items: [], count: 0, limit: 10, offset: 0 },
  providerFilters: { name: "", provider_type: "", model: "" },
  agentFilters: { name: "", role: "", provider_id: "", model: "" },
  activePage: "home",
  activeAgentModule: "providers",
  activeKnowledgeModule: "skills",
  activeSessionId: null,
  activeSkillId: null,
  activeKnowledgeId: null,
  editingProviderId: null,
  editingAgentId: null,
  editingSkillId: null,
  editingKnowledgeId: null,
  taskAgentsLoaded: false,
};

const pageButtons = Array.from(document.querySelectorAll("[data-page-target]"));
const pageViews = Array.from(document.querySelectorAll("[data-page]"));
const agentModuleButtons = Array.from(document.querySelectorAll("[data-agent-module-target]"));
const agentModuleViews = Array.from(document.querySelectorAll("[data-agent-module]"));
const knowledgeModuleButtons = Array.from(document.querySelectorAll("[data-knowledge-module-target]"));
const knowledgeModuleViews = Array.from(document.querySelectorAll("[data-knowledge-module]"));

const providerForm = document.querySelector("#provider-form");
const agentForm = document.querySelector("#agent-form");
const runForm = document.querySelector("#run-form");
const skillForm = document.querySelector("#skill-form");
const skillSearchForm = document.querySelector("#skill-search-form");
const knowledgeForm = document.querySelector("#knowledge-form");
const knowledgeSearchForm = document.querySelector("#knowledge-search-form");

const providerTypeSelect = document.querySelector("#provider-type");
const providerList = document.querySelector("#provider-list");
const providerListSummary = document.querySelector("#provider-list-summary");
const providerPageSizeSelect = document.querySelector("#provider-page-size");
const providerPagination = document.querySelector("#provider-pagination");
const providerFilterForm = document.querySelector("#provider-filter-form");
const providerFilterTypeSelect = document.querySelector("#provider-filter-type");
const providerFilterResetBtn = document.querySelector("#provider-filter-reset");
const providerTestBtn = document.querySelector("#provider-test-btn");
const providerCancelBtn = document.querySelector("#provider-cancel-btn");
const providerFormMode = document.querySelector("#provider-form-mode");
const providerFormResult = document.querySelector("#provider-form-result");
const providerApiKeyHint = document.querySelector("#provider-api-key-hint");
const providerTestResult = document.querySelector("#provider-test-result");
const providerAdvanced = document.querySelector("#provider-advanced");
const providerPresetHint = document.querySelector("#provider-preset-hint");
const providerApiVersionField = document.querySelector("[data-provider-field='api_version']");
const providerOrganizationField = document.querySelector("[data-provider-field='organization']");
const providerSubmitButton = providerForm.querySelector("button[type='submit']");
const legacyProviderInputs = Array.from(document.querySelectorAll("#provider-legacy-fields input, #provider-legacy-extra textarea"));

const agentProviderSelect = document.querySelector("#agent-provider");
const leadAgentSelect = document.querySelector("#lead-agent");
const agentList = document.querySelector("#agent-list");
const agentListSummary = document.querySelector("#agent-list-summary");
const agentPageSizeSelect = document.querySelector("#agent-page-size");
const agentPagination = document.querySelector("#agent-pagination");
const agentFilterForm = document.querySelector("#agent-filter-form");
const agentFilterProviderSelect = document.querySelector("#agent-filter-provider");
const agentFilterResetBtn = document.querySelector("#agent-filter-reset");
const agentCheckboxes = document.querySelector("#agent-checkboxes");
const agentCancelBtn = document.querySelector("#agent-cancel-btn");
const agentFormMode = document.querySelector("#agent-form-mode");
const agentFormResult = document.querySelector("#agent-form-result");
const agentSubmitButton = agentForm.querySelector("button[type='submit']");

const sessionList = document.querySelector("#session-list");
const sessionMeta = document.querySelector("#session-meta");
const transcript = document.querySelector("#transcript");
const memoryView = document.querySelector("#memory-view");
const runError = document.querySelector("#run-error");

const skillList = document.querySelector("#skill-list");
const skillDetail = document.querySelector("#skill-detail");
const skillResult = document.querySelector("#skill-result");
const skillResetBtn = document.querySelector("#skill-reset-btn");
const skillCancelBtn = document.querySelector("#skill-cancel-btn");
const skillFormMode = document.querySelector("#skill-form-mode");
const skillSubmitButton = skillForm.querySelector("button[type='submit']");
const skillImportSingleBtn = document.querySelector("#skill-import-single-btn");
const skillImportBatchBtn = document.querySelector("#skill-import-batch-btn");
const skillImportSingleInput = document.querySelector("#skill-import-single-input");
const skillImportBatchInput = document.querySelector("#skill-import-batch-input");
const skillImportSummary = document.querySelector("#skill-import-summary");
const skillReferenceFilesInput = document.querySelector("#skill-reference-files");
const skillTemplateFilesInput = document.querySelector("#skill-template-files");
const skillScriptFilesInput = document.querySelector("#skill-script-files");
const skillAssetFilesInput = document.querySelector("#skill-asset-files");
const skillUploadSummary = document.querySelector("#skill-upload-summary");

const knowledgeList = document.querySelector("#knowledge-list");
const knowledgeDetail = document.querySelector("#knowledge-detail");
const knowledgeResult = document.querySelector("#knowledge-result");
const knowledgeResetBtn = document.querySelector("#knowledge-reset-btn");
const knowledgeCancelBtn = document.querySelector("#knowledge-cancel-btn");
const knowledgeFormMode = document.querySelector("#knowledge-form-mode");
const knowledgeSubmitButton = knowledgeForm.querySelector("button[type='submit']");

const homeSessionList = document.querySelector("#home-session-list");
const homeAgentList = document.querySelector("#home-agent-list");
const homeKnowledgeList = document.querySelector("#home-knowledge-list");

for (const input of legacyProviderInputs) {
  if (input.name) {
    input.name = `legacy_${input.name}`;
  }
  input.disabled = true;
}

function showJson(target, value) {
  target.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  target.classList.remove("hidden");
}

function hide(target) {
  target.classList.add("hidden");
  target.textContent = "";
}

function safeJson(text) {
  if (!text.trim()) {
    return {};
  }
  return JSON.parse(text);
}

function formatJson(value) {
  if (!value || (typeof value === "object" && !Object.keys(value).length)) {
    return "";
  }
  return JSON.stringify(value, null, 2);
}

function formatStructuredValue(value) {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

function parseStructuredText(text) {
  const trimmed = text.trim();
  if (!trimmed) {
    return null;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return trimmed;
  }
}

function parseCommaList(text) {
  return text
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function latestSkillVersion(skill) {
  const versions = Array.isArray(skill?.versions) ? [...skill.versions] : [];
  versions.sort((left, right) => String(right?.created_at || "").localeCompare(String(left?.created_at || "")));
  return versions[0] || null;
}

function skillAssetCount(skill) {
  return Number(skill?.asset_summary?.total || skill?.assets?.length || 0);
}

function normalizeRelativePath(path) {
  return String(path || "")
    .replaceAll("\\", "/")
    .replace(/^\/+/, "")
    .replace(/\/{2,}/g, "/");
}

function fileCategoryPath(category, fileName) {
  return `${category}/${fileName}`;
}

async function fileToBase64(file) {
  const buffer = await file.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  const chunkSize = 0x8000;
  let binary = "";
  for (let index = 0; index < bytes.length; index += chunkSize) {
    const chunk = bytes.subarray(index, index + chunkSize);
    binary += String.fromCharCode(...chunk);
  }
  return btoa(binary);
}

async function fileToAssetPayload(file, relativePath) {
  return {
    relative_path: normalizeRelativePath(relativePath),
    mime_type: file.type || null,
    content_base64: await fileToBase64(file),
  };
}

async function buildCategoryAssetPayload(input, category) {
  const files = Array.from(input?.files || []);
  const assets = [];
  for (const file of files) {
    assets.push(await fileToAssetPayload(file, fileCategoryPath(category, file.name)));
  }
  return assets;
}

async function buildSkillAttachmentPayload() {
  const groups = await Promise.all([
    buildCategoryAssetPayload(skillReferenceFilesInput, "references"),
    buildCategoryAssetPayload(skillTemplateFilesInput, "templates"),
    buildCategoryAssetPayload(skillScriptFilesInput, "scripts"),
    buildCategoryAssetPayload(skillAssetFilesInput, "assets"),
  ]);
  return groups.flat();
}

function selectedSkillUploadCount() {
  return (
    Array.from(skillReferenceFilesInput?.files || []).length +
    Array.from(skillTemplateFilesInput?.files || []).length +
    Array.from(skillScriptFilesInput?.files || []).length +
    Array.from(skillAssetFilesInput?.files || []).length
  );
}

function activeSkillRecord() {
  return skillById(state.editingSkillId) || skillById(state.activeSkillId);
}

function updateSkillUploadSummary() {
  const existingCount = skillAssetCount(activeSkillRecord());
  const pendingCount = selectedSkillUploadCount();
  if (!existingCount && !pendingCount) {
    skillUploadSummary.textContent = "当前未选择附带文件。";
    return;
  }
  if (existingCount && !pendingCount) {
    skillUploadSummary.textContent = `当前 skill 已保存 ${existingCount} 个附带文件。`;
    return;
  }
  if (!existingCount && pendingCount) {
    skillUploadSummary.textContent = `本次将上传 ${pendingCount} 个附带文件。`;
    return;
  }
  skillUploadSummary.textContent = `当前 skill 已保存 ${existingCount} 个附带文件，本次将追加 ${pendingCount} 个文件。`;
}

async function buildImportItemFromFiles(files, trimSegments, folderName, sourceKind) {
  const entries = files
    .map((file) => {
      const rawPath = normalizeRelativePath(file.webkitRelativePath || file.name);
      const parts = rawPath.split("/").slice(trimSegments);
      return { file, relativePath: normalizeRelativePath(parts.join("/")) };
    })
    .filter((item) => item.relativePath && item.relativePath !== ".");
  const skillEntry = entries.find((item) => item.relativePath === "SKILL.md");
  if (!skillEntry) {
    throw new Error(`文件夹 ${folderName} 缺少 SKILL.md`);
  }
  const assets = [];
  for (const entry of entries) {
    if (entry.relativePath === "SKILL.md") {
      continue;
    }
    assets.push(await fileToAssetPayload(entry.file, entry.relativePath));
  }
  return {
    folder_name: folderName,
    source_kind: sourceKind,
    status: "draft",
    skill_markdown: await skillEntry.file.text(),
    assets,
  };
}

async function buildImportItemsFromSingleFolder(files) {
  if (!files.length) {
    throw new Error("请选择一个 skill 文件夹。");
  }
  const rootName = normalizeRelativePath(files[0].webkitRelativePath || files[0].name).split("/")[0] || "skill";
  return [await buildImportItemFromFiles(files, 1, rootName, "single-folder-import")];
}

async function buildImportItemsFromParentFolder(files) {
  if (!files.length) {
    throw new Error("请选择一个父文件夹。");
  }
  const grouped = new Map();
  for (const file of files) {
    const parts = normalizeRelativePath(file.webkitRelativePath || file.name).split("/");
    if (parts.length < 2) {
      continue;
    }
    const childFolder = parts[1];
    if (!grouped.has(childFolder)) {
      grouped.set(childFolder, []);
    }
    grouped.get(childFolder).push(file);
  }
  const items = [];
  for (const [folderName, childFiles] of grouped.entries()) {
    const hasSkillFile = childFiles.some((file) => {
      const parts = normalizeRelativePath(file.webkitRelativePath || file.name).split("/").slice(2);
      return normalizeRelativePath(parts.join("/")) === "SKILL.md";
    });
    if (!hasSkillFile) {
      continue;
    }
    items.push(await buildImportItemFromFiles(childFiles, 2, folderName, "batch-folder-import"));
  }
  if (!items.length) {
    throw new Error("所选父文件夹下没有找到包含 SKILL.md 的 skill 子目录。");
  }
  return items;
}

async function submitSkillImports(items, summaryText) {
  const result = await api("/api/skills/import", { method: "POST", body: JSON.stringify({ items }) });
  await bootstrap();
  if (result.items?.length) {
    await loadSkillDetail(result.items[0].id);
  }
  const message = {
    message: summaryText,
    imported: (result.items || []).map((item) => ({ id: item.id, name: item.name })),
    errors: result.errors || [],
  };
  if (skillImportSummary) {
    skillImportSummary.textContent = `${summaryText} Imported ${(result.items || []).length} skill(s).`;
  }
  showJson(skillResult, message);
  switchPage("knowledge");
  switchKnowledgeModule("skills");
}

function uniqueById(items) {
  const seen = new Set();
  return items.filter((item) => {
    const id = item?.id || item?.skill_id || item?.document_id;
    if (!id || seen.has(id)) {
      return false;
    }
    seen.add(id);
    return true;
  });
}

function providerById(providerId) {
  return uniqueById([...state.providers, ...state.providerPage.items]).find((item) => item.id === providerId) || null;
}

function providerPresetByType(providerType) {
  return state.presets.find((item) => item.provider_type === providerType) || null;
}

function agentById(agentId) {
  return uniqueById([...state.agents, ...state.agentPage.items]).find((item) => item.id === agentId) || null;
}

function skillById(skillId) {
  return uniqueById(state.skills).find((item) => item.id === skillId) || null;
}

function replaceById(items, nextItem) {
  return uniqueById([nextItem, ...items.filter((item) => (item.id || item.skill_id || item.document_id) !== nextItem.id)]);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || response.statusText);
  }
  return response.json();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function buildQueryString(params) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params || {})) {
    if (value === null || value === undefined) {
      continue;
    }
    const normalized = String(value).trim();
    if (!normalized) {
      continue;
    }
    search.set(key, normalized);
  }
  return search.toString();
}

function switchPage(pageName) {
  state.activePage = pageName;
  for (const button of pageButtons) {
    button.classList.toggle("active", button.dataset.pageTarget === pageName);
  }
  for (const view of pageViews) {
    view.classList.toggle("active", view.dataset.page === pageName);
  }
}

function switchAgentModule(moduleName) {
  state.activeAgentModule = moduleName;
  for (const button of agentModuleButtons) {
    button.classList.toggle("active", button.dataset.agentModuleTarget === moduleName);
  }
  for (const view of agentModuleViews) {
    view.classList.toggle("active", view.dataset.agentModule === moduleName);
  }
}

function switchKnowledgeModule(moduleName) {
  state.activeKnowledgeModule = moduleName;
  for (const button of knowledgeModuleButtons) {
    button.classList.toggle("active", button.dataset.knowledgeModuleTarget === moduleName);
  }
  for (const view of knowledgeModuleViews) {
    view.classList.toggle("active", view.dataset.knowledgeModule === moduleName);
  }
}

function pageStats(meta) {
  const count = Number(meta.count || 0);
  const limit = Math.max(1, Number(meta.limit || 10));
  const offset = Math.max(0, Number(meta.offset || 0));
  const itemCount = Array.isArray(meta.items) ? meta.items.length : 0;
  const start = count ? offset + 1 : 0;
  const end = count ? Math.min(offset + itemCount, count) : 0;
  const page = count ? Math.floor(offset / limit) + 1 : 1;
  const totalPages = Math.max(1, Math.ceil(count / limit));
  return { count, limit, offset, itemCount, start, end, page, totalPages };
}

function renderPagination(target, meta, kind) {
  const stats = pageStats(meta);
  const prevOffset = Math.max(0, stats.offset - stats.limit);
  const nextOffset = stats.offset + stats.limit;
  target.innerHTML = `
    <div class="page-metric">${stats.count ? `Showing ${stats.start}-${stats.end} of ${stats.count}` : "No records"}</div>
    <div class="pagination-actions">
      <button type="button" class="ghost page-button" data-page-kind="${kind}" data-page-offset="${prevOffset}" ${stats.offset <= 0 ? "disabled" : ""}>
        Previous
      </button>
      <span class="page-metric">Page ${stats.page} / ${stats.totalPages}</span>
      <button type="button" class="ghost page-button" data-page-kind="${kind}" data-page-offset="${nextOffset}" ${stats.page >= stats.totalPages ? "disabled" : ""}>
        Next
      </button>
    </div>
  `;
}

function renderCompactCards(items, emptyText) {
  if (!items.length) {
    return `<div class="empty-note">${emptyText}</div>`;
  }
  return items
    .map(
      (item) => `
        <article class="compact-card" ${item.dataset || ""}>
          <h4>${escapeHtml(item.title)}</h4>
          <div class="muted">${escapeHtml(item.subtitle || "")}</div>
          <p>${escapeHtml((item.body || "").slice(0, 100))}${(item.body || "").length > 100 ? "..." : ""}</p>
        </article>
      `
    )
    .join("");
}

function renderStats() {
  document.querySelector("#provider-count").textContent = String(state.providerPage.count || state.providers.length);
  document.querySelector("#agent-count").textContent = String(state.agentPage.count || state.agents.length);
  document.querySelector("#session-count").textContent = String(state.sessions.length);
  document.querySelector("#knowledge-count").textContent = String(state.knowledgeDocuments.length + state.skills.length);
}

function renderHome() {
  homeSessionList.innerHTML = renderCompactCards(
    state.sessions.slice(0, 5).map((item) => ({
      id: item.id,
      title: item.title || "未命名任务",
      subtitle: `${item.status} / ${item.participant_count || 0} agents`,
      body: item.user_prompt || "",
      dataset: `data-session-id="${item.id}" data-page-link="tasks"`,
    })),
    "暂无任务。"
  );

  homeAgentList.innerHTML = renderCompactCards(
    state.agentPage.items.slice(0, 5).map((item) => ({
      id: item.id,
      title: item.name,
      subtitle: `${item.role} / ${item.resolved_model}`,
      body: item.system_prompt || "",
      dataset: `data-agent-edit="${item.id}" data-page-link="agents"`,
    })),
    "暂无 Agent。"
  );

  homeKnowledgeList.innerHTML = renderCompactCards(
    [
      ...state.skills.slice(0, 3).map((item) => ({
        id: item.id,
        title: item.name,
        subtitle: `技能 / ${item.status || "启用"}`,
        body: item.description || "",
        dataset: `data-skill-id="${item.id}" data-page-link="knowledge" data-knowledge-module-link="skills"`,
      })),
      ...state.knowledgeDocuments.slice(0, 2).map((item) => ({
        id: item.id,
        title: item.title,
        subtitle: `RAG / ${(item.chunks || []).length} 个切块`,
        body: item.metadata?.source_name || item.metadata?.namespace_key || "RAG 文档",
        dataset: `data-knowledge-id="${item.id}" data-page-link="knowledge" data-knowledge-module-link="rag"`,
      })),
    ],
    "暂无技能或 RAG 资产。"
  );
}

function refreshProviderPresetOptions() {
  providerTypeSelect.innerHTML = state.presets.map((item) => `<option value="${item.provider_type}">${item.label}</option>`).join("");
}

function renderProviderFilterOptions() {
  providerFilterTypeSelect.innerHTML = [
    `<option value="">All Types</option>`,
    ...state.presets.map((item) => `<option value="${item.provider_type}">${item.label}</option>`),
  ].join("");
  providerFilterForm.elements.name.value = state.providerFilters.name || "";
  providerFilterForm.elements.provider_type.value = state.providerFilters.provider_type || "";
  providerFilterForm.elements.model.value = state.providerFilters.model || "";
}

function renderAgentFilterOptions() {
  agentFilterProviderSelect.innerHTML = [
    `<option value="">All Providers</option>`,
    ...state.providers.map((item) => `<option value="${item.id}">${escapeHtml(item.name)}</option>`),
  ].join("");
  agentFilterForm.elements.name.value = state.agentFilters.name || "";
  agentFilterForm.elements.role.value = state.agentFilters.role || "";
  agentFilterForm.elements.provider_id.value = state.agentFilters.provider_id || "";
  agentFilterForm.elements.model.value = state.agentFilters.model || "";
}

function providerHasAdvancedValues() {
  return Boolean(
    providerForm.elements.api_version.value.trim() ||
      providerForm.elements.organization.value.trim() ||
      providerForm.elements.extra_headers.value.trim() ||
      providerForm.elements.extra_config.value.trim()
  );
}

function syncProviderPresetMeta({ autoOpen = false } = {}) {
  const preset = providerPresetByType(providerTypeSelect.value);
  const showApiVersion = Boolean(providerForm.elements.api_version.value.trim()) || Boolean(preset?.supports_api_version);
  const showOrganization = Boolean(providerForm.elements.organization.value.trim()) || Boolean(preset?.supports_organization);

  providerApiVersionField.classList.toggle("hidden", !showApiVersion);
  providerOrganizationField.classList.toggle("hidden", !showOrganization);
  providerPresetHint.textContent =
    preset?.quickstart_hint || "Most providers only need a model and API key. Keep advanced options for special cases.";

  if (providerForm.elements.base_url.placeholder !== (preset?.default_base_url || "")) {
    providerForm.elements.base_url.placeholder = preset?.default_base_url || "";
  }
  if (!providerForm.elements.api_version.value.trim() && preset?.default_api_version) {
    providerForm.elements.api_version.value = preset.default_api_version;
  }
  if (autoOpen) {
    providerAdvanced.open = Boolean(preset?.show_advanced_by_default) || providerHasAdvancedValues();
  }
}

function applyPreset(providerType) {
  const preset = providerPresetByType(providerType);
  if (!preset) {
    return;
  }
  if (!providerForm.elements.base_url.value && preset.use_default_base_url_when_blank) {
    providerForm.elements.base_url.value = preset.default_base_url || "";
  }
  if (!providerForm.elements.model.value) {
    providerForm.elements.model.value = preset.default_model || "";
  }
  syncProviderPresetMeta({ autoOpen: true });
}

function buildProviderPayload() {
  return {
    name: providerForm.elements.name.value.trim(),
    provider_type: providerForm.elements.provider_type.value,
    base_url: providerForm.elements.base_url.value.trim() || null,
    api_key: providerForm.elements.api_key.value.trim() || null,
    model: providerForm.elements.model.value.trim(),
    api_version: providerForm.elements.api_version.value.trim() || null,
    organization: providerForm.elements.organization.value.trim() || null,
    extra_headers: safeJson(providerForm.elements.extra_headers.value),
    extra_config: safeJson(providerForm.elements.extra_config.value),
    clear_api_key: providerForm.elements.clear_api_key.checked,
  };
}

function providerFilterPayload() {
  return {
    name: providerFilterForm.elements.name.value.trim(),
    provider_type: providerFilterForm.elements.provider_type.value,
    model: providerFilterForm.elements.model.value.trim(),
  };
}

function agentFilterPayload() {
  return {
    name: agentFilterForm.elements.name.value.trim(),
    role: agentFilterForm.elements.role.value.trim(),
    provider_id: agentFilterForm.elements.provider_id.value,
    model: agentFilterForm.elements.model.value.trim(),
  };
}

function providerPageQuery({ offset = state.providerPage.offset, limit = state.providerPage.limit } = {}) {
  return buildQueryString({ limit, offset, ...state.providerFilters });
}

function agentPageQuery({ offset = state.agentPage.offset, limit = state.agentPage.limit } = {}) {
  return buildQueryString({ limit, offset, ...state.agentFilters });
}

function syncProviderFormMode() {
  const editing = Boolean(state.editingProviderId);
  providerFormMode.textContent = editing ? `Editing Provider: ${providerById(state.editingProviderId)?.name || state.editingProviderId}` : "";
  providerFormMode.classList.toggle("hidden", !editing);
  providerCancelBtn.classList.toggle("hidden", !editing);
  providerApiKeyHint.classList.toggle("hidden", !editing);
  providerSubmitButton.textContent = editing ? "Update Provider" : "Create Provider";
}

function resetProviderForm({ keepMessage = false } = {}) {
  state.editingProviderId = null;
  providerForm.reset();
  if (state.presets.length) {
    providerTypeSelect.value = state.presets[0].provider_type;
  }
  providerForm.elements.base_url.value = "";
  providerForm.elements.model.value = "";
  providerForm.elements.api_version.value = "";
  providerForm.elements.organization.value = "";
  providerForm.elements.extra_headers.value = "";
  providerForm.elements.extra_config.value = "";
  providerForm.elements.clear_api_key.checked = false;
  providerAdvanced.open = false;
  applyPreset(providerTypeSelect.value);
  syncProviderFormMode();
  hide(providerTestResult);
  if (!keepMessage) {
    hide(providerFormResult);
  }
}

function populateProviderForm(providerId) {
  const provider = providerById(providerId);
  if (!provider) {
    return;
  }
  state.editingProviderId = provider.id;
  providerForm.elements.name.value = provider.name || "";
  providerForm.elements.provider_type.value = provider.provider_type || "";
  providerForm.elements.base_url.value = provider.base_url || "";
  providerForm.elements.api_key.value = "";
  providerForm.elements.api_version.value = provider.api_version || "";
  providerForm.elements.organization.value = provider.organization || "";
  providerForm.elements.model.value = provider.model || "";
  providerForm.elements.extra_headers.value = formatJson(provider.extra_headers);
  providerForm.elements.extra_config.value = formatJson(provider.extra_config);
  providerForm.elements.clear_api_key.checked = false;
  hide(providerTestResult);
  hide(providerFormResult);
  syncProviderPresetMeta({ autoOpen: true });
  syncProviderFormMode();
  switchPage("agents");
  switchAgentModule("providers");
}

function syncAgentFormMode() {
  const editing = Boolean(state.editingAgentId);
  agentFormMode.textContent = editing ? `Editing Agent: ${agentById(state.editingAgentId)?.name || state.editingAgentId}` : "";
  agentFormMode.classList.toggle("hidden", !editing);
  agentCancelBtn.classList.toggle("hidden", !editing);
  agentSubmitButton.textContent = editing ? "Update Agent" : "Create Agent";
}

function resetAgentForm({ keepMessage = false } = {}) {
  state.editingAgentId = null;
  agentForm.reset();
  if (state.providers.length) {
    agentProviderSelect.value = state.providers[0].id;
  }
  agentForm.elements.temperature.value = "0.2";
  syncAgentFormMode();
  if (!keepMessage) {
    hide(agentFormResult);
  }
}

function populateAgentForm(agentId) {
  const agent = agentById(agentId);
  if (!agent) {
    return;
  }
  state.editingAgentId = agent.id;
  agentForm.elements.name.value = agent.name || "";
  agentForm.elements.role.value = agent.role || "";
  agentForm.elements.system_prompt.value = agent.system_prompt || "";
  agentForm.elements.provider_id.value = agent.provider_id || "";
  agentForm.elements.model_override.value = agent.model_override || "";
  agentForm.elements.temperature.value = String(agent.temperature ?? 0.2);
  agentForm.elements.max_tokens.value = agent.max_tokens ?? "";
  hide(agentFormResult);
  syncAgentFormMode();
  switchPage("agents");
  switchAgentModule("agents");
}

async function buildSkillPayload() {
  const assets = await buildSkillAttachmentPayload();
  return {
    name: skillForm.elements.name.value.trim(),
    description: skillForm.elements.description.value.trim(),
    skill_markdown: skillForm.elements.skill_markdown.value.trim(),
    workflow: parseStructuredText(skillForm.elements.workflow.value),
    tools: parseCommaList(skillForm.elements.tools.value),
    topics: parseCommaList(skillForm.elements.topics.value),
    metadata: safeJson(skillForm.elements.metadata.value),
    status: skillForm.elements.status.value,
    assets,
    source_kind: state.editingSkillId ? "manual-edit" : "manual-create",
  };
}

function syncSkillFormMode() {
  const editing = Boolean(state.editingSkillId);
  skillFormMode.textContent = editing ? `正在编辑技能：${skillById(state.editingSkillId)?.name || state.editingSkillId}` : "";
  skillFormMode.classList.toggle("hidden", !editing);
  skillCancelBtn.classList.toggle("hidden", !editing);
  skillSubmitButton.textContent = editing ? "更新技能" : "保存技能";
}

function resetSkillForm({ keepMessage = false } = {}) {
  state.editingSkillId = null;
  skillForm.reset();
  skillForm.elements.status.value = "draft";
  syncSkillFormMode();
  updateSkillUploadSummary();
  if (!keepMessage) {
    hide(skillResult);
  }
}

async function populateSkillForm(skillId) {
  const payload = await api(`/api/skills/${skillId}`);
  state.editingSkillId = payload.id;
  state.skills = replaceById(state.skills, payload);
  const latestVersion = latestSkillVersion(payload);
  const tools = Array.from(new Set((payload.bindings || []).map((item) => item.tool_name).filter(Boolean)));
  const topics = Array.isArray(payload.metadata?.topics) ? payload.metadata.topics : [];

  skillForm.elements.name.value = payload.name || "";
  skillForm.elements.description.value = payload.description || "";
  skillForm.elements.status.value = payload.status || "draft";
  skillForm.elements.skill_markdown.value = payload.skill_markdown || latestVersion?.prompt_template || "";
  skillForm.elements.workflow.value = formatStructuredValue(latestVersion?.workflow);
  skillForm.elements.tools.value = tools.join(", ");
  skillForm.elements.topics.value = topics.join(", ");
  skillForm.elements.metadata.value = formatJson(payload.metadata);
  hide(skillResult);
  syncSkillFormMode();
  updateSkillUploadSummary();
  switchPage("knowledge");
  switchKnowledgeModule("skills");
  renderHome();
}

function buildKnowledgePayload() {
  return {
    title: knowledgeForm.elements.title.value.trim(),
    source_name: knowledgeForm.elements.source_name.value.trim() || null,
    text: knowledgeForm.elements.text.value.trim(),
  };
}

function syncKnowledgeFormMode() {
  const editing = Boolean(state.editingKnowledgeId);
  knowledgeFormMode.textContent = editing ? `正在编辑文档：${state.editingKnowledgeId}` : "";
  knowledgeFormMode.classList.toggle("hidden", !editing);
  knowledgeCancelBtn.classList.toggle("hidden", !editing);
  knowledgeSubmitButton.textContent = editing ? "更新文档" : "保存文档";
}

function documentText(document) {
  return (document.chunks || []).map((item) => item.content || "").join("\n\n");
}

function resetKnowledgeForm({ keepMessage = false } = {}) {
  state.editingKnowledgeId = null;
  knowledgeForm.reset();
  syncKnowledgeFormMode();
  if (!keepMessage) {
    hide(knowledgeResult);
  }
}

async function populateKnowledgeForm(documentId) {
  const payload = await api(`/api/rag/documents/${documentId}`);
  state.editingKnowledgeId = payload.id;
  state.knowledgeDocuments = replaceById(state.knowledgeDocuments, payload);
  knowledgeForm.elements.title.value = payload.title || "";
  knowledgeForm.elements.source_name.value = payload.metadata?.source_name || "";
  knowledgeForm.elements.text.value = documentText(payload);
  hide(knowledgeResult);
  syncKnowledgeFormMode();
  switchPage("knowledge");
  switchKnowledgeModule("rag");
  renderHome();
}

function renderProviders() {
  renderProviderFilterOptions();
  providerList.innerHTML = state.providerPage.items.length
    ? state.providerPage.items
        .map(
          (item) => `
            <article class="card">
              <div class="message-head">
                <div>
                  <h3>${escapeHtml(item.name)}</h3>
                  <div class="card-meta">
                    <span>${escapeHtml(item.provider_type)}</span>
                    <span>${escapeHtml(item.api_key_masked || "no key")}</span>
                    <span>${item.agent_count || 0} agents</span>
                  </div>
                </div>
                <div class="actions">
                  <button type="button" class="ghost" data-provider-edit="${item.id}">Edit</button>
                  <button type="button" class="ghost danger" data-provider-delete="${item.id}">Delete</button>
                </div>
              </div>
              <div class="chip-row">
                <span class="chip">${escapeHtml(item.model)}</span>
                <span class="chip" title="${escapeHtml(item.base_url || "default preset url")}">${item.base_url ? "custom endpoint" : "preset endpoint"}</span>
              </div>
            </article>
          `
        )
        .join("")
    : `<div class="empty-note">No providers yet.</div>`;

  const providerStats = pageStats(state.providerPage);
  providerListSummary.textContent = providerStats.count
    ? `Showing ${providerStats.start}-${providerStats.end} of ${providerStats.count} providers`
    : "No providers";
  providerPageSizeSelect.value = String(state.providerPage.limit);
  renderPagination(providerPagination, state.providerPage, "providers");

  const providerOptions = state.providers
    .map((item) => `<option value="${item.id}">${escapeHtml(item.name)} / ${escapeHtml(item.model)}</option>`)
    .join("");
  agentProviderSelect.innerHTML = providerOptions;
  if (state.editingAgentId) {
    const editingAgent = agentById(state.editingAgentId);
    if (editingAgent) {
      agentProviderSelect.value = editingAgent.provider_id;
    }
  } else if (state.providers.length) {
    agentProviderSelect.value = state.providers[0].id;
  }
}

function renderAgents() {
  renderAgentFilterOptions();
  agentList.innerHTML = state.agentPage.items.length
    ? state.agentPage.items
        .map(
          (item) => `
            <article class="card">
              <div class="message-head">
                <div>
                  <h3>${escapeHtml(item.name)}</h3>
                  <div class="card-meta">
                    <span>${escapeHtml(item.role)}</span>
                    <span>${escapeHtml(item.provider_name)}</span>
                  </div>
                </div>
                <div class="actions">
                  <button type="button" class="ghost" data-agent-edit="${item.id}">Edit</button>
                  <button type="button" class="ghost danger" data-agent-delete="${item.id}">Delete</button>
                </div>
              </div>
              <p>${escapeHtml((item.system_prompt || "").slice(0, 88))}${(item.system_prompt || "").length > 88 ? "..." : ""}</p>
              <div class="chip-row">
                <span class="chip">${escapeHtml(item.resolved_model)}</span>
                <span class="chip">temp ${item.temperature}</span>
                <span class="chip">${item.participant_count || 0} sessions</span>
                <span class="chip">${item.message_count || 0} messages</span>
              </div>
              <div class="card-actions-row">
                <a href="#" class="inline-link" data-memory-agent="${item.id}" data-page-link="tasks">View Memory</a>
              </div>
            </article>
          `
        )
        .join("")
    : `<div class="empty-note">No agents yet.</div>`;

  const agentStats = pageStats(state.agentPage);
  agentListSummary.textContent = agentStats.count ? `Showing ${agentStats.start}-${agentStats.end} of ${agentStats.count} agents` : "No agents";
  agentPageSizeSelect.value = String(state.agentPage.limit);
  renderPagination(agentPagination, state.agentPage, "agents");

  if (state.editingAgentId) {
    const editingAgent = agentById(state.editingAgentId);
    if (editingAgent) {
      agentForm.elements.provider_id.value = editingAgent.provider_id;
    }
  }

  if (!state.taskAgentsLoaded) {
    leadAgentSelect.innerHTML = "";
    agentCheckboxes.innerHTML = `<div class="empty-note">Open Task Management to load the full agent list.</div>`;
    return;
  }

  const agentOptions = state.agents.map((item) => `<option value="${item.id}">${escapeHtml(item.name)}</option>`).join("");
  leadAgentSelect.innerHTML = agentOptions;
  agentCheckboxes.innerHTML = state.agents.length
    ? state.agents
        .map(
          (item, index) => `
            <label class="checkbox-item">
              <input type="checkbox" name="agent_ids" value="${item.id}" ${index === 0 ? "checked" : ""} />
              <span>
                <strong>${escapeHtml(item.name)}</strong><br />
                <span class="muted">${escapeHtml(item.role)} / ${escapeHtml(item.resolved_model)}</span>
              </span>
            </label>
          `
        )
        .join("")
    : `<div class="empty-note">No available agents.</div>`;
}

function renderSessions() {
  sessionList.innerHTML = state.sessions.length
    ? state.sessions
        .map(
          (item) => `
            <article class="session-card" data-session-id="${item.id}">
              <strong>${escapeHtml(item.title || "Untitled Session")}</strong>
              <div class="muted">${escapeHtml(item.status)} / ${item.participant_count || 0} agents / ${item.message_count || 0} messages</div>
              <p>${escapeHtml((item.user_prompt || "").slice(0, 86))}${(item.user_prompt || "").length > 86 ? "..." : ""}</p>
            </article>
          `
        )
        .join("")
    : `<div class="empty-note">暂无任务记录。</div>`;
}

function renderSkillList(items = state.skills, { searchMode = false } = {}) {
  if (!items.length) {
    skillList.innerHTML = `<div class="empty-note">${searchMode ? "没有匹配的技能。" : "暂无技能。"}</div>`;
    return;
  }
  skillList.innerHTML = items
    .map((item) => {
      const skillId = item.id || item.skill_id;
      const version = item.latest_version?.version || item.version || "v1";
      const body = item.description || item.text || "";
      const score = item.score ? `<span class="chip">score ${Number(item.score).toFixed(3)}</span>` : "";
      const assetChip = skillAssetCount(item) ? `<span class="chip">${skillAssetCount(item)} files</span>` : "";
      const sourceChip = item.source_kind ? `<span class="chip">${escapeHtml(item.source_kind)}</span>` : "";
      return `
        <article class="card" data-skill-id="${skillId}">
          <div class="message-head">
            <div>
              <h3>${escapeHtml(item.name || skillId)}</h3>
              <div class="card-meta">
                <span>${escapeHtml(item.status || "active")}</span>
                <span>${escapeHtml(version)}</span>
              </div>
            </div>
            <div class="actions">
              <button type="button" class="ghost" data-skill-edit="${skillId}">Edit</button>
              <button type="button" class="ghost danger" data-skill-delete="${skillId}">删除</button>
            </div>
          </div>
          <p>${escapeHtml(body.slice(0, 140))}${body.length > 140 ? "..." : ""}</p>
          <div class="chip-row">${score}${assetChip}${sourceChip}</div>
        </article>
      `;
    })
    .join("");
}

function renderSkillDetail(payload) {
  const versions = Array.isArray(payload.versions) ? payload.versions : [];
  const latestVersion = latestSkillVersion(payload);
  const tools = Array.from(new Set((payload.bindings || []).map((item) => item.tool_name).filter(Boolean)));
  const topics = Array.isArray(payload.metadata?.topics) ? payload.metadata.topics : [];
  const assets = Array.isArray(payload.assets) ? payload.assets : [];
  const skillMarkdown = payload.skill_markdown || latestVersion?.prompt_template || "";
  const versionList = versions.length
    ? versions
        .map(
          (item) => `
            <article class="memory-card">
              <div class="message-head">
                <strong>${escapeHtml(item.version || "version")}</strong>
                <span class="muted">${escapeHtml(item.created_at || "")}</span>
              </div>
              <div class="card-meta">
                <span>${escapeHtml(String(item.asset_summary?.total || item.assets?.length || 0))} files</span>
              </div>
            </article>
          `
        )
        .join("")
    : `<div class="empty-note">暂无版本记录。</div>`;

  const assetList = assets.length
    ? `
      <div class="asset-list">
        ${assets
          .map(
            (item) => `
              <article class="asset-card">
                <div class="message-head">
                  <strong>${escapeHtml(item.relative_path || item.file_name || item.object_id || "file")}</strong>
                  <span class="muted">${escapeHtml(item.mime_type || "")}</span>
                </div>
                <div class="card-meta">
                  <span>${escapeHtml(item.category || "other")}</span>
                  <span>${escapeHtml(String(item.size_bytes || 0))} bytes</span>
                  <span>${item.is_text ? "text" : "binary"}</span>
                </div>
              </article>
            `
          )
          .join("")}
      </div>
    `
    : `<div class="empty-note">暂无附带文档。</div>`;

  skillDetail.classList.remove("empty");
  skillDetail.innerHTML = `
    <div class="message-head">
      <div>
        <strong>${escapeHtml(payload.name || payload.id)}</strong>
        <div class="muted">${escapeHtml(payload.id)}</div>
      </div>
      <div class="chip-row">
        <span class="chip">${escapeHtml(payload.status || "active")}</span>
        <span class="chip">${versions.length} versions</span>
        <span class="chip">${skillAssetCount(payload)} files</span>
        ${payload.folder_name ? `<span class="chip">${escapeHtml(payload.folder_name)}</span>` : ""}
      </div>
    </div>
    <div class="detail-body">${escapeHtml(payload.description || "")}</div>
    <div class="chip-row">
      ${tools.map((tool) => `<span class="chip">${escapeHtml(tool)}</span>`).join("")}
      ${topics.map((topic) => `<span class="chip">${escapeHtml(topic)}</span>`).join("")}
    </div>
    ${
      skillMarkdown || latestVersion?.workflow
        ? `
          <div class="list-grid">
            ${
              skillMarkdown
                ? `
            <article class="memory-card">
              <div class="message-head">
                <strong>SKILL.md</strong>
              </div>
              <pre class="detail-code">${escapeHtml(skillMarkdown)}</pre>
            </article>
            `
                : ""
            }
            ${
              latestVersion?.workflow
                ? `
            <article class="memory-card">
              <div class="message-head">
                <strong>Workflow</strong>
              </div>
              <div class="detail-body">${escapeHtml(formatStructuredValue(latestVersion.workflow) || "")}</div>
            </article>
            `
                : ""
            }
          </div>
        `
        : ""
    }
    <article class="memory-card">
      <div class="message-head">
        <strong>附带文档</strong>
        <span class="muted">${skillAssetCount(payload)} files</span>
      </div>
      ${assetList}
    </article>
    <div class="list-grid">${versionList}</div>
  `;
  updateSkillUploadSummary();
}

function renderKnowledgeList(items = state.knowledgeDocuments, { searchMode = false } = {}) {
  if (!items.length) {
    knowledgeList.innerHTML = `<div class="empty-note">${searchMode ? "没有匹配的 RAG 结果。" : "暂无 RAG 文档。"}</div>`;
    return;
  }
  knowledgeList.innerHTML = items
    .map((item) => {
      const documentId = item.document_id || item.id;
      const title = item.title || item.document_title || item.id;
      const body = item.text || item.summary || (item.metadata ? JSON.stringify(item.metadata) : "");
      const isSearchResult = Boolean(item.document_id);
      return `
        <article class="card" data-knowledge-id="${documentId}">
          <div class="message-head">
            <div>
              <h3>${escapeHtml(title)}</h3>
              <div class="card-meta">
                <span>${isSearchResult ? "检索命中" : "RAG 文档"}</span>
                ${item.score ? `<span>score ${Number(item.score).toFixed(3)}</span>` : ""}
              </div>
            </div>
            <div class="actions">
              <button type="button" class="ghost" data-knowledge-edit="${documentId}">Edit</button>
              <button type="button" class="ghost danger" data-knowledge-delete="${documentId}">删除</button>
            </div>
          </div>
          <p>${escapeHtml(body.slice(0, 160))}${body.length > 160 ? "..." : ""}</p>
          <div class="chip-row">
            ${item.document_id ? `<span class="chip">${escapeHtml(item.document_id)}</span>` : ""}
            ${item.metadata?.source_name ? `<span class="chip">${escapeHtml(item.metadata.source_name)}</span>` : ""}
            ${item.chunks ? `<span class="chip">${item.chunks.length} 个切块</span>` : ""}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderKnowledgeDetail(payload) {
  const chunkList = (payload.chunks || [])
    .map(
      (chunk) => `
        <article class="memory-card">
          <div class="message-head">
            <strong>Chunk ${chunk.chunk_index + 1}</strong>
            <span class="muted">${chunk.tokens || chunk.content?.length || 0}</span>
          </div>
          <div class="detail-body">${escapeHtml(chunk.content || "")}</div>
        </article>
      `
    )
    .join("");

  knowledgeDetail.classList.remove("empty");
  knowledgeDetail.innerHTML = `
    <div class="message-head">
      <div>
        <strong>${escapeHtml(payload.title || payload.id)}</strong>
        <div class="muted">${escapeHtml(payload.id)}</div>
      </div>
      <div class="chip-row">
        <span class="chip">${(payload.chunks || []).length} 个切块</span>
        <span class="chip">${escapeHtml(payload.status || "active")}</span>
        ${payload.metadata?.source_name ? `<span class="chip">${escapeHtml(payload.metadata.source_name)}</span>` : ""}
      </div>
    </div>
    <div class="detail-body">${escapeHtml(JSON.stringify(payload.metadata || {}, null, 2))}</div>
    <div class="list-grid">${chunkList || '<div class="empty-note">暂无可展示切块。</div>'}</div>
  `;
}

async function loadSession(sessionId) {
  const payload = await api(`/api/sessions/${sessionId}`);
  state.activeSessionId = sessionId;

  sessionMeta.classList.remove("empty");
  sessionMeta.innerHTML = `
    <strong>${escapeHtml(payload.session.title || "未命名任务")}</strong>
    <div class="chip-row">
      <span class="chip">${escapeHtml(payload.session.status)}</span>
      <span class="chip">${escapeHtml(payload.session.strategy)}</span>
      <span class="chip">rounds ${payload.session.rounds}</span>
      <span class="chip">lead ${escapeHtml(payload.session.lead_agent_name || "n/a")}</span>
    </div>
    <p>${escapeHtml(payload.session.final_summary || "暂无最终总结。")}</p>
  `;

  transcript.innerHTML = payload.messages
    .map(
      (message) => `
        <article class="message-card">
          <div class="message-head">
            <div>
              <div class="message-role">${escapeHtml(message.role)}</div>
              <strong>${escapeHtml(message.agent_name || "User")}</strong>
            </div>
            <div class="muted">Round ${message.round_index}</div>
          </div>
          <div class="message-body">${escapeHtml(message.content)}</div>
          ${
            message.references?.length
              ? `<details><summary class="memory-link">查看召回记忆</summary>${message.references
                  .map(
                    (ref) => `
                      <div class="memory-card">
                        <strong>${escapeHtml(ref.memory_id || "memory")}</strong>
                        <div class="muted">score ${Number(ref.score || 0).toFixed(3)}</div>
                        <div class="message-body">${escapeHtml(ref.summary || "")}</div>
                      </div>
                    `
                  )
                  .join("")}</details>`
              : ""
          }
        </article>
      `
    )
    .join("");

  memoryView.innerHTML = payload.participants
    .map(
      (item) => `
        <div class="memory-card">
          <strong>${escapeHtml(item.agent_name)}</strong>
          <div class="muted">${escapeHtml(item.agent_role)}</div>
          <a href="#" class="memory-link" data-memory-agent="${item.agent_id}" data-page-link="tasks">加载 Agent 记忆</a>
        </div>
      `
    )
    .join("");
}

async function loadAgentMemory(agentId, query = "") {
  const params = new URLSearchParams();
  params.set("limit", "8");
  if (query) {
    params.set("query", query);
  }
  const payload = await api(`/api/agents/${agentId}/memory?${params.toString()}`);
  memoryView.innerHTML = `
    <div class="memory-card">
      <strong>${escapeHtml(payload.agent.name)} 的记忆</strong>
      <div class="muted">${payload.results.length} records</div>
    </div>
    ${payload.results
      .map(
        (item) => `
          <article class="memory-card">
            <div class="message-head">
              <strong>${escapeHtml(item.memory_type || item.scope || "memory")}</strong>
              <span class="muted">${item.score ? Number(item.score).toFixed(3) : ""}</span>
            </div>
            <div class="message-body">${escapeHtml(item.text || item.summary || "")}</div>
          </article>
        `
      )
      .join("")}
  `;
}

async function loadSkillDetail(skillId) {
  const payload = await api(`/api/skills/${skillId}`);
  state.activeSkillId = skillId;
  state.skills = replaceById(state.skills, payload);
  renderSkillDetail(payload);
  renderHome();
}

async function loadKnowledgeDocument(documentId) {
  const payload = await api(`/api/rag/documents/${documentId}`);
  state.activeKnowledgeId = documentId;
  state.knowledgeDocuments = replaceById(state.knowledgeDocuments, payload);
  renderKnowledgeDetail(payload);
  renderHome();
}

async function loadSkillsIndex() {
  const payload = await api("/api/skills?limit=50");
  state.skills = payload.items || [];
  renderSkillList();
}

async function loadKnowledgeIndex() {
  const payload = await api("/api/rag/documents?limit=50");
  state.knowledgeDocuments = payload.items || [];
  renderKnowledgeList();
}

async function loadProviderPage({ offset = state.providerPage.offset, limit = state.providerPage.limit } = {}) {
  const payload = await api(`/api/providers?${providerPageQuery({ offset, limit })}`);
  if (payload.count > 0 && offset >= payload.count) {
    const lastOffset = Math.max(0, Math.floor((payload.count - 1) / limit) * limit);
    return loadProviderPage({ offset: lastOffset, limit });
  }
  state.providerPage = {
    items: payload.items || [],
    count: payload.count || 0,
    limit: payload.limit || limit,
    offset: payload.offset || 0,
  };
  renderProviders();
  renderStats();
}

async function loadAgentPage({ offset = state.agentPage.offset, limit = state.agentPage.limit } = {}) {
  const payload = await api(`/api/agents?${agentPageQuery({ offset, limit })}`);
  if (payload.count > 0 && offset >= payload.count) {
    const lastOffset = Math.max(0, Math.floor((payload.count - 1) / limit) * limit);
    return loadAgentPage({ offset: lastOffset, limit });
  }
  state.agentPage = {
    items: payload.items || [],
    count: payload.count || 0,
    limit: payload.limit || limit,
    offset: payload.offset || 0,
  };
  renderAgents();
  renderStats();
  renderHome();
}

async function ensureTaskAgentsLoaded(force = false) {
  if (state.taskAgentsLoaded && !force) {
    return;
  }
  const payload = await api("/api/agents?all=1");
  state.agents = payload.items || [];
  state.taskAgentsLoaded = true;
  renderAgents();
}

async function bootstrap() {
  const taskAgentsPromise = state.taskAgentsLoaded || state.activePage === "tasks" ? api("/api/agents?all=1") : Promise.resolve(null);
  const providerPageQueryString = providerPageQuery();
  const agentPageQueryString = agentPageQuery();
  const [catalog, providers, taskAgents, providerPage, agentPage, sessions, skills, rag] = await Promise.all([
    api("/api/catalog/providers"),
    api("/api/providers?all=1"),
    taskAgentsPromise,
    api(`/api/providers?${providerPageQueryString}`),
    api(`/api/agents?${agentPageQueryString}`),
    api("/api/sessions"),
    api("/api/skills?limit=50"),
    api("/api/rag/documents?limit=50"),
  ]);

  state.presets = catalog.items || [];
  state.providers = providers.items || [];
  if (taskAgents) {
    state.agents = taskAgents.items || [];
    state.taskAgentsLoaded = true;
  } else if (!state.taskAgentsLoaded) {
    state.agents = [];
  }
  state.providerPage = {
    items: providerPage.items || [],
    count: providerPage.count || 0,
    limit: providerPage.limit || state.providerPage.limit,
    offset: providerPage.offset || 0,
  };
  state.agentPage = {
    items: agentPage.items || [],
    count: agentPage.count || 0,
    limit: agentPage.limit || state.agentPage.limit,
    offset: agentPage.offset || 0,
  };
  state.sessions = sessions.items || [];
  state.skills = skills.items || [];
  state.knowledgeDocuments = rag.items || [];

  refreshProviderPresetOptions();
  renderProviders();
  renderAgents();
  renderSessions();
  renderSkillList();
  renderKnowledgeList();
  renderStats();
  renderHome();
  applyPreset(providerTypeSelect.value);
  switchAgentModule(state.activeAgentModule);
  switchKnowledgeModule(state.activeKnowledgeModule);

  if (state.editingProviderId && !providerById(state.editingProviderId)) {
    resetProviderForm();
  } else {
    syncProviderFormMode();
  }
  if (state.editingAgentId && !agentById(state.editingAgentId)) {
    resetAgentForm();
  } else {
    syncAgentFormMode();
  }
  if (state.editingSkillId && !skillById(state.editingSkillId)) {
    resetSkillForm();
  } else {
    syncSkillFormMode();
    updateSkillUploadSummary();
  }
  if (state.editingKnowledgeId && !state.knowledgeDocuments.some((item) => item.id === state.editingKnowledgeId)) {
    resetKnowledgeForm();
  } else {
    syncKnowledgeFormMode();
  }

  if (state.activeSessionId && state.sessions.some((item) => item.id === state.activeSessionId)) {
    await loadSession(state.activeSessionId);
  }
  if (state.activeSkillId && state.skills.some((item) => item.id === state.activeSkillId)) {
    await loadSkillDetail(state.activeSkillId);
  }
  if (state.activeKnowledgeId && state.knowledgeDocuments.some((item) => item.id === state.activeKnowledgeId)) {
    await loadKnowledgeDocument(state.activeKnowledgeId);
  }
}

providerTypeSelect.addEventListener("change", () => {
  providerForm.elements.base_url.value = "";
  providerForm.elements.model.value = "";
  providerForm.elements.api_version.value = "";
  providerForm.elements.organization.value = "";
  applyPreset(providerTypeSelect.value);
});

providerPageSizeSelect.addEventListener("change", async () => {
  state.providerPage.limit = Number(providerPageSizeSelect.value || 10);
  state.providerPage.offset = 0;
  await loadProviderPage({ offset: 0, limit: state.providerPage.limit });
});

agentPageSizeSelect.addEventListener("change", async () => {
  state.agentPage.limit = Number(agentPageSizeSelect.value || 10);
  state.agentPage.offset = 0;
  await loadAgentPage({ offset: 0, limit: state.agentPage.limit });
});

providerFilterForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  state.providerFilters = providerFilterPayload();
  state.providerPage.offset = 0;
  await loadProviderPage({ offset: 0, limit: state.providerPage.limit });
});

providerFilterResetBtn.addEventListener("click", async () => {
  state.providerFilters = { name: "", provider_type: "", model: "" };
  providerFilterForm.reset();
  state.providerPage.offset = 0;
  await loadProviderPage({ offset: 0, limit: state.providerPage.limit });
});

agentFilterForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  state.agentFilters = agentFilterPayload();
  state.agentPage.offset = 0;
  await loadAgentPage({ offset: 0, limit: state.agentPage.limit });
});

agentFilterResetBtn.addEventListener("click", async () => {
  state.agentFilters = { name: "", role: "", provider_id: "", model: "" };
  agentFilterForm.reset();
  state.agentPage.offset = 0;
  await loadAgentPage({ offset: 0, limit: state.agentPage.limit });
});

providerCancelBtn.addEventListener("click", () => {
  resetProviderForm();
});

agentCancelBtn.addEventListener("click", () => {
  resetAgentForm();
});

skillCancelBtn.addEventListener("click", () => {
  resetSkillForm();
});

knowledgeCancelBtn.addEventListener("click", () => {
  resetKnowledgeForm();
});

for (const input of [skillReferenceFilesInput, skillTemplateFilesInput, skillScriptFilesInput, skillAssetFilesInput]) {
  input?.addEventListener("change", () => {
    updateSkillUploadSummary();
  });
}

skillImportSingleBtn?.addEventListener("click", () => {
  skillImportSingleInput?.click();
});

skillImportBatchBtn?.addEventListener("click", () => {
  skillImportBatchInput?.click();
});

skillImportSingleInput?.addEventListener("change", async () => {
  hide(skillResult);
  try {
    const items = await buildImportItemsFromSingleFolder(Array.from(skillImportSingleInput.files || []));
    await submitSkillImports(items, "Single skill folder imported.");
  } catch (error) {
    showJson(skillResult, { error: error.message });
  } finally {
    skillImportSingleInput.value = "";
  }
});

skillImportBatchInput?.addEventListener("change", async () => {
  hide(skillResult);
  try {
    const items = await buildImportItemsFromParentFolder(Array.from(skillImportBatchInput.files || []));
    await submitSkillImports(items, "Batch skill import completed.");
  } catch (error) {
    showJson(skillResult, { error: error.message });
  } finally {
    skillImportBatchInput.value = "";
  }
});

providerForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hide(providerTestResult);
  hide(providerFormResult);
  try {
    const isEditing = Boolean(state.editingProviderId);
    const payload = buildProviderPayload();
    const path = isEditing ? `/api/providers/${state.editingProviderId}` : "/api/providers";
    const method = isEditing ? "PUT" : "POST";
    const result = await api(path, { method, body: JSON.stringify(payload) });
    state.providerPage.offset = 0;
    resetProviderForm({ keepMessage: true });
    await bootstrap();
    showJson(providerFormResult, { message: isEditing ? "Provider updated" : "Provider created", id: result.id, name: result.name });
    switchPage("agents");
    switchAgentModule("providers");
  } catch (error) {
    showJson(providerFormResult, { error: error.message });
  }
});

providerTestBtn.addEventListener("click", async () => {
  hide(providerTestResult);
  try {
    const payload = {
      config: {
        ...buildProviderPayload(),
        id: state.editingProviderId,
        name: providerForm.elements.name.value.trim() || "temp-provider",
      },
      prompt: "Reply with READY and mention which provider protocol you are using.",
    };
    const result = await api("/api/providers/test", { method: "POST", body: JSON.stringify(payload) });
    showJson(providerTestResult, result);
  } catch (error) {
    showJson(providerTestResult, { error: error.message });
  }
});

agentForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hide(agentFormResult);
  try {
    const isEditing = Boolean(state.editingAgentId);
    const payload = {
      name: agentForm.elements.name.value.trim(),
      role: agentForm.elements.role.value.trim(),
      system_prompt: agentForm.elements.system_prompt.value.trim(),
      provider_id: agentForm.elements.provider_id.value,
      model_override: agentForm.elements.model_override.value.trim() || null,
      temperature: Number(agentForm.elements.temperature.value || 0.2),
      max_tokens: agentForm.elements.max_tokens.value ? Number(agentForm.elements.max_tokens.value) : null,
      collaboration_style: "specialist",
      metadata: {},
    };
    const path = isEditing ? `/api/agents/${state.editingAgentId}` : "/api/agents";
    const method = isEditing ? "PUT" : "POST";
    const result = await api(path, { method, body: JSON.stringify(payload) });
    state.agentPage.offset = 0;
    resetAgentForm({ keepMessage: true });
    await bootstrap();
    if (state.taskAgentsLoaded) {
      await ensureTaskAgentsLoaded(true);
    }
    showJson(agentFormResult, { message: isEditing ? "Agent updated" : "Agent created", id: result.id, name: result.name });
    switchPage("agents");
    switchAgentModule("agents");
  } catch (error) {
    showJson(agentFormResult, { error: error.message });
  }
});

skillForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hide(skillResult);
  try {
    const isEditing = Boolean(state.editingSkillId);
    const payload = await buildSkillPayload();
    const path = isEditing ? `/api/skills/${state.editingSkillId}` : "/api/skills";
    const method = isEditing ? "PUT" : "POST";
    const result = await api(path, { method, body: JSON.stringify(payload) });
    resetSkillForm({ keepMessage: true });
    await bootstrap();
    await loadSkillDetail(result.id);
    showJson(skillResult, { message: isEditing ? "技能已更新" : "技能已创建", id: result.id, name: result.name });
    switchPage("knowledge");
    switchKnowledgeModule("skills");
  } catch (error) {
    showJson(skillResult, { error: error.message });
  }
});

skillSearchForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const query = skillSearchForm.elements.query.value.trim();
  if (!query) {
    renderSkillList();
    return;
  }
  try {
    const payload = await api(`/api/skills/search?query=${encodeURIComponent(query)}&limit=20`);
    renderSkillList(payload.items || [], { searchMode: true });
    switchPage("knowledge");
    switchKnowledgeModule("skills");
  } catch (error) {
    showJson(skillResult, { error: error.message });
  }
});

skillResetBtn.addEventListener("click", async () => {
  skillSearchForm.reset();
  await loadSkillsIndex();
});

knowledgeForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hide(knowledgeResult);
  try {
    const isEditing = Boolean(state.editingKnowledgeId);
    const payload = buildKnowledgePayload();
    const path = isEditing ? `/api/rag/documents/${state.editingKnowledgeId}` : "/api/rag/documents";
    const method = isEditing ? "PUT" : "POST";
    const result = await api(path, { method, body: JSON.stringify(payload) });
    resetKnowledgeForm({ keepMessage: true });
    await bootstrap();
    await loadKnowledgeDocument(result.id);
    showJson(knowledgeResult, { message: isEditing ? "RAG 文档已更新" : "RAG 文档已创建", id: result.id, title: result.title });
    switchPage("knowledge");
    switchKnowledgeModule("rag");
  } catch (error) {
    showJson(knowledgeResult, { error: error.message });
  }
});

knowledgeSearchForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const query = knowledgeSearchForm.elements.query.value.trim();
  if (!query) {
    renderKnowledgeList();
    return;
  }
  try {
    const payload = await api(`/api/rag/search?query=${encodeURIComponent(query)}&limit=20`);
    renderKnowledgeList(payload.items || [], { searchMode: true });
    switchPage("knowledge");
    switchKnowledgeModule("rag");
  } catch (error) {
    showJson(knowledgeResult, { error: error.message });
  }
});

knowledgeResetBtn.addEventListener("click", async () => {
  knowledgeSearchForm.reset();
  await loadKnowledgeIndex();
});

runForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  hide(runError);
  await ensureTaskAgentsLoaded();
  const selectedAgentIds = Array.from(runForm.querySelectorAll("input[name='agent_ids']:checked")).map((item) => item.value);
  if (!selectedAgentIds.length) {
    showJson(runError, { error: "Select at least one agent." });
    return;
  }
  try {
    const payload = {
      title: runForm.elements.title.value.trim() || null,
      prompt: runForm.elements.prompt.value.trim(),
      rounds: Number(runForm.elements.rounds.value || 1),
      lead_agent_id: runForm.elements.lead_agent_id.value || selectedAgentIds[0],
      agent_ids: selectedAgentIds,
    };
    const result = await api("/api/collaborations/run", { method: "POST", body: JSON.stringify(payload) });
    await bootstrap();
    await loadSession(result.session.id);
    switchPage("tasks");
  } catch (error) {
    showJson(runError, { error: error.message });
  }
});

document.addEventListener("click", async (event) => {
  const pageTrigger = event.target.closest("[data-page-target], [data-page-link]");
  if (pageTrigger) {
    const pageName = pageTrigger.dataset.pageTarget || pageTrigger.dataset.pageLink;
    if (pageName) {
      switchPage(pageName);
      if (pageTrigger.dataset.knowledgeModuleLink) {
        switchKnowledgeModule(pageTrigger.dataset.knowledgeModuleLink);
      }
      if (pageName === "tasks") {
        await ensureTaskAgentsLoaded();
      }
    }
  }

  const agentModuleTrigger = event.target.closest("[data-agent-module-target]");
  if (agentModuleTrigger) {
    event.preventDefault();
    switchPage("agents");
    switchAgentModule(agentModuleTrigger.dataset.agentModuleTarget);
    return;
  }

  const knowledgeModuleTrigger = event.target.closest("[data-knowledge-module-target]");
  if (knowledgeModuleTrigger) {
    event.preventDefault();
    switchPage("knowledge");
    switchKnowledgeModule(knowledgeModuleTrigger.dataset.knowledgeModuleTarget);
    return;
  }

  const providerPageTrigger = event.target.closest("[data-page-kind='providers']");
  if (providerPageTrigger && !providerPageTrigger.hasAttribute("disabled")) {
    event.preventDefault();
    await loadProviderPage({
      offset: Number(providerPageTrigger.dataset.pageOffset || 0),
      limit: state.providerPage.limit,
    });
    return;
  }

  const agentPageTrigger = event.target.closest("[data-page-kind='agents']");
  if (agentPageTrigger && !agentPageTrigger.hasAttribute("disabled")) {
    event.preventDefault();
    await loadAgentPage({
      offset: Number(agentPageTrigger.dataset.pageOffset || 0),
      limit: state.agentPage.limit,
    });
    return;
  }

  const providerEditTrigger = event.target.closest("[data-provider-edit]");
  if (providerEditTrigger) {
    event.preventDefault();
    populateProviderForm(providerEditTrigger.dataset.providerEdit);
    return;
  }

  const providerDeleteTrigger = event.target.closest("[data-provider-delete]");
  if (providerDeleteTrigger) {
    event.preventDefault();
    const providerId = providerDeleteTrigger.dataset.providerDelete;
    const provider = providerById(providerId);
    if (!provider) {
      return;
    }
    if (!window.confirm(`Delete provider "${provider.name}"?`)) {
      return;
    }
    hide(providerFormResult);
    try {
      await api(`/api/providers/${providerId}`, { method: "DELETE" });
      state.providerPage.offset = 0;
      if (state.editingProviderId === providerId) {
        resetProviderForm({ keepMessage: true });
      }
      await bootstrap();
      showJson(providerFormResult, { message: "Provider deleted", id: providerId, name: provider.name });
      switchPage("agents");
      switchAgentModule("providers");
    } catch (error) {
      showJson(providerFormResult, { error: error.message });
      switchPage("agents");
      switchAgentModule("providers");
    }
    return;
  }

  const agentEditTrigger = event.target.closest("[data-agent-edit]");
  if (agentEditTrigger) {
    event.preventDefault();
    populateAgentForm(agentEditTrigger.dataset.agentEdit);
    return;
  }

  const agentDeleteTrigger = event.target.closest("[data-agent-delete]");
  if (agentDeleteTrigger) {
    event.preventDefault();
    const agentId = agentDeleteTrigger.dataset.agentDelete;
    const agent = agentById(agentId);
    if (!agent) {
      return;
    }
    if (!window.confirm(`Delete agent "${agent.name}"?`)) {
      return;
    }
    hide(agentFormResult);
    try {
      await api(`/api/agents/${agentId}`, { method: "DELETE" });
      state.agentPage.offset = 0;
      if (state.editingAgentId === agentId) {
        resetAgentForm({ keepMessage: true });
      }
      await bootstrap();
      if (state.taskAgentsLoaded) {
        await ensureTaskAgentsLoaded(true);
      }
      showJson(agentFormResult, { message: "Agent deleted", id: agentId, name: agent.name });
      switchPage("agents");
      switchAgentModule("agents");
    } catch (error) {
      showJson(agentFormResult, { error: error.message });
      switchPage("agents");
      switchAgentModule("agents");
    }
    return;
  }

  const skillEditTrigger = event.target.closest("[data-skill-edit]");
  if (skillEditTrigger) {
    event.preventDefault();
    await populateSkillForm(skillEditTrigger.dataset.skillEdit);
    return;
  }

  const skillDeleteTrigger = event.target.closest("[data-skill-delete]");
  if (skillDeleteTrigger) {
    event.preventDefault();
    const skillId = skillDeleteTrigger.dataset.skillDelete;
    const skill = skillById(skillId);
    if (!skill) {
      return;
    }
    if (!window.confirm(`确认删除技能“${skill.name}”吗？`)) {
      return;
    }
    hide(skillResult);
    try {
      await api(`/api/skills/${skillId}`, { method: "DELETE" });
      if (state.editingSkillId === skillId) {
        resetSkillForm({ keepMessage: true });
      }
      if (state.activeSkillId === skillId) {
        state.activeSkillId = null;
        skillDetail.classList.add("empty");
        skillDetail.textContent = "选择一个技能查看详情。";
      }
      await bootstrap();
      showJson(skillResult, { message: "技能已删除", id: skillId, name: skill.name });
      switchPage("knowledge");
      switchKnowledgeModule("skills");
    } catch (error) {
      showJson(skillResult, { error: error.message });
      switchPage("knowledge");
      switchKnowledgeModule("skills");
    }
    return;
  }

  const knowledgeEditTrigger = event.target.closest("[data-knowledge-edit]");
  if (knowledgeEditTrigger) {
    event.preventDefault();
    await populateKnowledgeForm(knowledgeEditTrigger.dataset.knowledgeEdit);
    return;
  }

  const knowledgeDeleteTrigger = event.target.closest("[data-knowledge-delete]");
  if (knowledgeDeleteTrigger) {
    event.preventDefault();
    const documentId = knowledgeDeleteTrigger.dataset.knowledgeDelete;
    const document = state.knowledgeDocuments.find((item) => item.id === documentId) || { id: documentId, title: documentId };
    if (!window.confirm(`确认删除 RAG 文档“${document.title}”吗？`)) {
      return;
    }
    hide(knowledgeResult);
    try {
      await api(`/api/rag/documents/${documentId}`, { method: "DELETE" });
      if (state.editingKnowledgeId === documentId) {
        resetKnowledgeForm({ keepMessage: true });
      }
      if (state.activeKnowledgeId === documentId) {
        state.activeKnowledgeId = null;
        knowledgeDetail.classList.add("empty");
        knowledgeDetail.textContent = "选择一个 RAG 文档或检索命中查看详情。";
      }
      await bootstrap();
      showJson(knowledgeResult, { message: "RAG 文档已删除", id: documentId, title: document.title });
      switchPage("knowledge");
      switchKnowledgeModule("rag");
    } catch (error) {
      showJson(knowledgeResult, { error: error.message });
      switchPage("knowledge");
      switchKnowledgeModule("rag");
    }
    return;
  }

  const sessionTrigger = event.target.closest("[data-session-id]");
  if (sessionTrigger) {
    event.preventDefault();
    switchPage("tasks");
    await ensureTaskAgentsLoaded();
    await loadSession(sessionTrigger.dataset.sessionId);
    return;
  }

  const memoryTrigger = event.target.closest("[data-memory-agent]");
  if (memoryTrigger) {
    event.preventDefault();
    switchPage("tasks");
    await ensureTaskAgentsLoaded();
    await loadAgentMemory(memoryTrigger.dataset.memoryAgent);
    return;
  }

  const skillTrigger = event.target.closest("[data-skill-id]");
  if (skillTrigger) {
    event.preventDefault();
    switchPage("knowledge");
    switchKnowledgeModule("skills");
    await loadSkillDetail(skillTrigger.dataset.skillId);
    return;
  }

  const knowledgeTrigger = event.target.closest("[data-knowledge-id]");
  if (knowledgeTrigger) {
    event.preventDefault();
    switchPage("knowledge");
    switchKnowledgeModule("rag");
    await loadKnowledgeDocument(knowledgeTrigger.dataset.knowledgeId);
  }
});

switchPage("home");
switchAgentModule("providers");
switchKnowledgeModule("skills");
bootstrap().catch((error) => {
  showJson(runError, { error: error.message });
});
