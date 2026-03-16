const PREVIEW_ROW_COUNT = 10;
const STORAGE_KEY = "processMiningLastResult";
const FLOW_SELECTION_STORAGE_KEY = "processMiningFlowSelection";
const TRANSITION_DRILLDOWN_STORAGE_KEY = "processMiningTransitionDrilldown";
const FILTER_SLOT_KEYS = ["filter_value_1", "filter_value_2", "filter_value_3"];
const DEFAULT_FILTER_LABELS = {
    filter_value_1: "グループ/カテゴリー フィルター①",
    filter_value_2: "グループ/カテゴリー フィルター②",
    filter_value_3: "グループ/カテゴリー フィルター③",
};

const form = document.getElementById("analyze-form");
const submitButton = document.getElementById("submit-button");
const statusPanel = document.getElementById("status-panel");
const summaryPanel = document.getElementById("summary-panel");
const resultPanels = document.getElementById("result-panels");
const exportExcelToggle = document.getElementById("export-excel-toggle");
const excelExportSettings = document.getElementById("excel-export-settings");
const csvFileInput = document.getElementById("csv-file-input");
const columnSourceNote = document.getElementById("column-source-note");
const diagnosticsPanel = document.getElementById("log-diagnostics-panel");
const filterSelectionNote = document.getElementById("filter-selection-note");
const initialProfilePayloadElement = document.getElementById("initial-profile-payload");

const caseIdColumnSelect = document.getElementById("case-id-column-select");
const activityColumnSelect = document.getElementById("activity-column-select");
const timestampColumnSelect = document.getElementById("timestamp-column-select");

const filterColumnRefs = [
    {
        slot: "filter_value_1",
        columnSelect: document.getElementById("filter-column-1-select"),
        valueSelect: document.getElementById("filter-value-1-select"),
        valueLabel: document.getElementById("filter-value-1-label"),
    },
    {
        slot: "filter_value_2",
        columnSelect: document.getElementById("filter-column-2-select"),
        valueSelect: document.getElementById("filter-value-2-select"),
        valueLabel: document.getElementById("filter-value-2-label"),
    },
    {
        slot: "filter_value_3",
        columnSelect: document.getElementById("filter-column-3-select"),
        valueSelect: document.getElementById("filter-value-3-select"),
        valueLabel: document.getElementById("filter-value-3-label"),
    },
];

let isLoadingProfile = false;
let isAnalyzing = false;
let profileRequestVersion = 0;
let currentProfilePayload = loadInitialProfilePayload();

function setStatus(message, type = "info") {
    statusPanel.textContent = message;
    statusPanel.className = `status-panel ${type}`;
}

function hideStatus() {
    statusPanel.className = "status-panel hidden";
    statusPanel.textContent = "";
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function loadInitialProfilePayload() {
    if (!initialProfilePayloadElement) {
        return {
            headers: [],
            default_selection: {},
            column_settings: { filters: [] },
            diagnostics: null,
            source_file_name: "",
        };
    }

    try {
        return JSON.parse(initialProfilePayloadElement.textContent || "{}");
    } catch {
        return {
            headers: [],
            default_selection: {},
            column_settings: { filters: [] },
            diagnostics: null,
            source_file_name: "",
        };
    }
}

function syncExcelExportSettings() {
    const enabled = Boolean(exportExcelToggle?.checked);
    if (excelExportSettings) {
        excelExportSettings.classList.toggle("hidden", !enabled);
    }
}

function syncSubmitState() {
    submitButton.disabled = isLoadingProfile || isAnalyzing;
}

function buildPatternDetailHref(runId, patternIndex) {
    return `/analysis/patterns/${encodeURIComponent(String(patternIndex))}?run_id=${encodeURIComponent(runId)}`;
}

function buildAnalysisDetailHref(analysisKey, runId) {
    const path = `/analysis/${encodeURIComponent(analysisKey)}`;
    return runId ? `${path}?run_id=${encodeURIComponent(runId)}` : path;
}

function buildTable(rows, options = {}) {
    if (!rows.length) {
        return '<p class="empty-state">表示できるデータがありません。</p>';
    }

    const { analysisKey = "", runId = "" } = options;
    const headers = Object.keys(rows[0]).filter((header) => !header.startsWith("__"));
    const headHtml = headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("");
    const bodyHtml = rows.map((row) => {
        const cells = headers.map((header) => {
            const cellValue = escapeHtml(row[header]);
            const isWideHeader = [
                "処理順パターン",
                "アクティビティ名",
                "アクティビティ",
                "前処理アクティビティ名",
                "後処理アクティビティ名",
            ].includes(header);
            const isPatternLink = (
                analysisKey === "pattern"
                && header === "処理順パターン"
                && runId
                && Number.isInteger(row.__rowIndex)
            );

            if (isPatternLink) {
                return `
                    <td class="table-cell--wide">
                        <div class="cell-scroll-wrapper">
                            <a href="${buildPatternDetailHref(runId, row.__rowIndex)}" class="table-link">${cellValue}</a>
                        </div>
                    </td>
                `;
            }

            if (isWideHeader) {
                return `
                    <td class="table-cell--wide">
                        <div class="cell-scroll-wrapper">${cellValue}</div>
                    </td>
                `;
            }

            return `<td>${cellValue}</td>`;
        }).join("");

        return `<tr>${cells}</tr>`;
    }).join("");

    return `
        <div class="table-wrap">
            <table>
                <thead><tr>${headHtml}</tr></thead>
                <tbody>${bodyHtml}</tbody>
            </table>
        </div>
    `;
}

function saveLatestResult(data) {
    try {
        sessionStorage.setItem(STORAGE_KEY, JSON.stringify(data));
        sessionStorage.removeItem(FLOW_SELECTION_STORAGE_KEY);
        sessionStorage.removeItem(TRANSITION_DRILLDOWN_STORAGE_KEY);
    } catch {
        try {
            const fallbackData = {
                run_id: data.run_id,
                source_file_name: data.source_file_name,
                selected_analysis_keys: data.selected_analysis_keys,
                case_count: data.case_count,
                event_count: data.event_count,
                applied_filters: data.applied_filters,
                column_settings: data.column_settings,
                analyses: {},
            };
            sessionStorage.setItem(STORAGE_KEY, JSON.stringify(fallbackData));
            sessionStorage.removeItem(FLOW_SELECTION_STORAGE_KEY);
            sessionStorage.removeItem(TRANSITION_DRILLDOWN_STORAGE_KEY);
        } catch {
            // Ignore storage failures and keep the current render only.
        }
    }
}

function loadLatestResult() {
    const storedValue = sessionStorage.getItem(STORAGE_KEY);
    if (!storedValue) {
        return null;
    }

    try {
        return JSON.parse(storedValue);
    } catch {
        sessionStorage.removeItem(STORAGE_KEY);
        return null;
    }
}

function getCurrentMappingState() {
    return {
        case_id_column: String(caseIdColumnSelect?.value || "").trim(),
        activity_column: String(activityColumnSelect?.value || "").trim(),
        timestamp_column: String(timestampColumnSelect?.value || "").trim(),
        filter_column_1: String(filterColumnRefs[0]?.columnSelect?.value || "").trim(),
        filter_column_2: String(filterColumnRefs[1]?.columnSelect?.value || "").trim(),
        filter_column_3: String(filterColumnRefs[2]?.columnSelect?.value || "").trim(),
    };
}

function getCurrentFilterValueState() {
    return Object.fromEntries(
        filterColumnRefs.map((filterRef) => [
            filterRef.slot,
            String(filterRef.valueSelect?.value || "").trim(),
        ])
    );
}

function getProfileFilterDefinitions(profilePayload) {
    const rawDefinitions = Array.isArray(profilePayload?.column_settings?.filters)
        ? profilePayload.column_settings.filters
        : [];
    const definitionMap = new Map(
        rawDefinitions.map((definition) => [definition.slot, definition])
    );

    return FILTER_SLOT_KEYS.map((slot) => {
        const fallbackLabel = DEFAULT_FILTER_LABELS[slot];
        const definition = definitionMap.get(slot) || {};
        return {
            slot,
            label: definition.label || fallbackLabel,
            column_name: definition.column_name || "",
        };
    });
}

function getDiagnosticFilterDefinitions(profilePayload) {
    const rawDefinitions = Array.isArray(profilePayload?.diagnostics?.filters)
        ? profilePayload.diagnostics.filters
        : [];
    const definitionMap = new Map(rawDefinitions.map((definition) => [definition.slot, definition]));

    return getProfileFilterDefinitions(profilePayload).map((definition) => ({
        ...definition,
        options: Array.isArray(definitionMap.get(definition.slot)?.options)
            ? definitionMap.get(definition.slot).options
            : [],
    }));
}

function getPreferredSelection(options, ...candidates) {
    for (const candidate of candidates) {
        if (candidate && options.includes(candidate)) {
            return candidate;
        }
    }
    return "";
}

function replaceSelectOptions(selectElement, options, selectedValue, placeholder = "選択してください") {
    if (!selectElement) {
        return;
    }

    selectElement.replaceChildren();
    selectElement.append(new Option(placeholder, ""));
    options.forEach((optionValue) => {
        selectElement.append(new Option(optionValue, optionValue, false, optionValue === selectedValue));
    });
    selectElement.value = selectedValue || "";
}

function renderColumnSelectors(profilePayload) {
    const headers = Array.isArray(profilePayload?.headers) ? profilePayload.headers : [];
    const defaultSelection = profilePayload?.default_selection || {};
    const currentMappingState = getCurrentMappingState();
    const filterDefinitions = getProfileFilterDefinitions(profilePayload);

    replaceSelectOptions(
        caseIdColumnSelect,
        headers,
        getPreferredSelection(
            headers,
            currentMappingState.case_id_column,
            profilePayload?.column_settings?.case_id_column,
            defaultSelection.case_id_column,
        )
    );
    replaceSelectOptions(
        activityColumnSelect,
        headers,
        getPreferredSelection(
            headers,
            currentMappingState.activity_column,
            profilePayload?.column_settings?.activity_column,
            defaultSelection.activity_column,
        )
    );
    replaceSelectOptions(
        timestampColumnSelect,
        headers,
        getPreferredSelection(
            headers,
            currentMappingState.timestamp_column,
            profilePayload?.column_settings?.timestamp_column,
            defaultSelection.timestamp_column,
        )
    );

    filterColumnRefs.forEach((filterRef, index) => {
        const definition = filterDefinitions[index];
        const selectedColumn = getPreferredSelection(
            headers,
            currentMappingState[`filter_column_${index + 1}`],
            definition.column_name,
        );
        replaceSelectOptions(filterRef.columnSelect, headers, selectedColumn, "未設定");
    });
}

function renderFilterValueSelectors(profilePayload) {
    const currentFilterValues = getCurrentFilterValueState();
    const filterDefinitions = getDiagnosticFilterDefinitions(profilePayload);
    const selectedFilters = filterDefinitions
        .filter((definition) => definition.column_name)
        .map((definition) => `${definition.label}: ${definition.column_name}`);

    filterColumnRefs.forEach((filterRef, index) => {
        const definition = filterDefinitions[index];
        const options = Array.isArray(definition.options) ? definition.options : [];
        const selectedValue = getPreferredSelection(
            options,
            currentFilterValues[definition.slot],
        );

        if (filterRef.valueLabel) {
            filterRef.valueLabel.textContent = definition.label;
        }

        replaceSelectOptions(filterRef.valueSelect, options, selectedValue, "全て");
        filterRef.valueSelect.disabled = !definition.column_name;
    });

    if (!filterSelectionNote) {
        return;
    }

    filterSelectionNote.textContent = selectedFilters.length
        ? `現在の絞り込み対象列: ${selectedFilters.join(" / ")}`
        : "フィルター列を設定すると、絞り込み条件を選択できます。";
}

function buildMissingCountText(diagnostics) {
    const missingCounts = diagnostics?.missing_counts || {};
    const items = [
        `Case ID ${missingCounts.case_id ?? "-"}`,
        `Activity ${missingCounts.activity ?? "-"}`,
        `Timestamp ${missingCounts.timestamp ?? "-"}`,
    ];
    return items.join(" / ");
}

function buildLogPeriodText(diagnostics) {
    if (!diagnostics?.time_range?.min || !diagnostics?.time_range?.max) {
        return "Case ID / Activity / Timestamp を選択すると表示します。";
    }
    return `${diagnostics.time_range.min} 〜 ${diagnostics.time_range.max}`;
}

function renderDiagnostics(profilePayload) {
    if (!diagnosticsPanel) {
        return;
    }

    const diagnostics = profilePayload?.diagnostics;
    if (!diagnostics) {
        diagnosticsPanel.innerHTML = '<p class="empty-state">ログを読み込むと、件数・期間・列情報を表示します。</p>';
        return;
    }

    const columns = Array.isArray(diagnostics.columns) ? diagnostics.columns : [];
    const columnRowsHtml = columns.map((column) => `
        <tr>
            <td>${escapeHtml(column.name)}</td>
            <td>${escapeHtml((column.sample_values || []).join(", ") || "-")}</td>
            <td>${escapeHtml(column.unique_count ?? "-")}</td>
            <td>${escapeHtml(column.missing_count ?? "-")}</td>
        </tr>
    `).join("");

    diagnosticsPanel.innerHTML = `
        <div class="diagnostics-summary-grid">
            <article class="diagnostic-card">
                <span class="summary-label">総ケース数</span>
                <strong>${escapeHtml(diagnostics.case_count ?? "-")}</strong>
            </article>
            <article class="diagnostic-card">
                <span class="summary-label">総イベント数</span>
                <strong>${escapeHtml(diagnostics.event_count ?? "-")}</strong>
            </article>
            <article class="diagnostic-card">
                <span class="summary-label">ログ期間</span>
                <strong>${escapeHtml(buildLogPeriodText(diagnostics))}</strong>
            </article>
            <article class="diagnostic-card">
                <span class="summary-label">欠損件数</span>
                <strong>${escapeHtml(buildMissingCountText(diagnostics))}</strong>
            </article>
        </div>
        <p class="panel-note">ヘッダー一覧: ${escapeHtml((diagnostics.headers || []).join(", "))}</p>
        <div class="table-wrap diagnostics-table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>列名</th>
                        <th>サンプル値</th>
                        <th>ユニーク件数</th>
                        <th>欠損件数</th>
                    </tr>
                </thead>
                <tbody>${columnRowsHtml}</tbody>
            </table>
        </div>
    `;
}

function updateColumnSourceNote(sourceFileName) {
    if (!columnSourceNote) {
        return;
    }

    if (!sourceFileName) {
        columnSourceNote.textContent = "CSV を読み込むと、列候補とログ診断を表示します。";
        return;
    }

    columnSourceNote.innerHTML = `現在は <code>${escapeHtml(sourceFileName)}</code> の内容を基準に表示しています。`;
}

function renderProfilePayload(profilePayload) {
    currentProfilePayload = profilePayload || currentProfilePayload;
    renderColumnSelectors(currentProfilePayload);
    renderFilterValueSelectors(currentProfilePayload);
    renderDiagnostics(currentProfilePayload);
    updateColumnSourceNote(currentProfilePayload?.source_file_name || "");
}

function appendMappingSettings(formData) {
    const mappingState = getCurrentMappingState();
    Object.entries(mappingState).forEach(([fieldName, fieldValue]) => {
        if (fieldValue) {
            formData.set(fieldName, fieldValue);
        }
    });
}

async function fetchLogProfile(file) {
    const formData = new FormData();
    if (file) {
        formData.append("csv_file", file);
    }
    appendMappingSettings(formData);

    const response = await fetch("/api/csv-headers", {
        method: "POST",
        body: formData,
    });
    const payload = await response.json();

    if (!response.ok) {
        throw new Error(payload.error || "ログ診断情報の取得に失敗しました。");
    }

    return payload;
}

async function refreshLogProfile() {
    const selectedFile = csvFileInput?.files?.[0] || null;
    const currentVersion = profileRequestVersion + 1;
    profileRequestVersion = currentVersion;
    isLoadingProfile = true;
    syncSubmitState();
    setStatus("ログ情報を読み込んでいます...", "info");

    try {
        const payload = await fetchLogProfile(selectedFile);
        if (currentVersion !== profileRequestVersion) {
            return;
        }

        renderProfilePayload(payload);
        setStatus("列候補とログ診断を更新しました。", "success");
    } catch (error) {
        if (currentVersion !== profileRequestVersion) {
            return;
        }
        setStatus(error.message, "error");
    } finally {
        if (currentVersion === profileRequestVersion) {
            isLoadingProfile = false;
            syncSubmitState();
        }
    }
}

function validateColumnSelections() {
    const caseIdColumn = String(caseIdColumnSelect?.value || "").trim();
    const activityColumn = String(activityColumnSelect?.value || "").trim();
    const timestampColumn = String(timestampColumnSelect?.value || "").trim();

    if (!caseIdColumn || !activityColumn || !timestampColumn) {
        return "Case ID列 / Activity列 / Timestamp列 を選択してください。";
    }

    if (new Set([caseIdColumn, activityColumn, timestampColumn]).size !== 3) {
        return "Case ID列 / Activity列 / Timestamp列 にはそれぞれ異なる列を選択してください。";
    }

    return "";
}

function validateFilterColumnSelections() {
    const selectedColumns = filterColumnRefs
        .map((filterRef) => String(filterRef.columnSelect?.value || "").trim())
        .filter(Boolean);

    if (selectedColumns.length !== new Set(selectedColumns).size) {
        return "グループ/カテゴリー フィルター①〜③ にはそれぞれ異なる列を選択してください。";
    }

    return "";
}

function validateAnalyzeForm() {
    if (!Array.isArray(currentProfilePayload?.headers) || !currentProfilePayload.headers.length) {
        return "ログ情報を取得できていません。ファイルを選び直してください。";
    }

    const columnError = validateColumnSelections();
    if (columnError) {
        return columnError;
    }

    const filterColumnError = validateFilterColumnSelections();
    if (filterColumnError) {
        return filterColumnError;
    }

    const selectedAnalysisCount = form.querySelectorAll('input[name="analysis_keys"]:checked').length;
    if (selectedAnalysisCount === 0) {
        return "少なくとも 1 つの分析を選択してください。";
    }

    return "";
}

function getDownloadFileName(response, fallbackFileName) {
    const disposition = response.headers.get("Content-Disposition") || "";
    const utf8FileNameMatch = disposition.match(/filename\*=UTF-8''([^;]+)/i);
    if (utf8FileNameMatch) {
        return decodeURIComponent(utf8FileNameMatch[1]);
    }

    const fileNameMatch = disposition.match(/filename="([^"]+)"/i);
    if (fileNameMatch) {
        return fileNameMatch[1];
    }

    return fallbackFileName;
}

async function downloadExcelArchive(data) {
    setStatus("分析は完了しました。Excel ZIP を準備しています...", "info");
    const response = await fetch(`/api/runs/${encodeURIComponent(data.run_id)}/excel-archive`);

    if (!response.ok) {
        const payload = await response.json();
        throw new Error(payload.detail || payload.error || "Excel ZIP の生成に失敗しました。");
    }

    const archiveBlob = await response.blob();
    const fallbackFileName = `${data.source_file_name.replace(/\.[^.]+$/, "") || "analysis"}_analysis_excels.zip`;
    const downloadFileName = getDownloadFileName(response, fallbackFileName);
    const downloadUrl = URL.createObjectURL(archiveBlob);
    const downloadLink = document.createElement("a");
    downloadLink.href = downloadUrl;
    downloadLink.download = downloadFileName;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    downloadLink.remove();
    URL.revokeObjectURL(downloadUrl);
}

function buildAppliedFilterSummary(appliedFilters = {}, columnSettings = {}) {
    const filterDefinitions = Array.isArray(columnSettings?.filters) ? columnSettings.filters : [];
    const labelMap = new Map(filterDefinitions.map((definition) => [definition.slot, definition.label]));
    const appliedItems = FILTER_SLOT_KEYS
        .filter((slot) => Boolean(appliedFilters?.[slot]))
        .map((slot) => `${labelMap.get(slot) || DEFAULT_FILTER_LABELS[slot]}: ${appliedFilters[slot]}`);

    return appliedItems.length ? appliedItems.join(" / ") : "フィルタ未適用";
}

function renderSummary(data) {
    const appliedFilterSummary = buildAppliedFilterSummary(data.applied_filters, data.column_settings);
    summaryPanel.className = "summary-panel";
    summaryPanel.innerHTML = `
        <article class="summary-card">
            <span class="summary-label">入力ファイル</span>
            <strong>${escapeHtml(data.source_file_name)}</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">ケース数</span>
            <strong>${escapeHtml(data.case_count)}</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">イベント数</span>
            <strong>${escapeHtml(data.event_count)}</strong>
        </article>
        <p class="summary-inline-note">適用条件: ${escapeHtml(appliedFilterSummary)}</p>
    `;
}

function buildResultHeader(analysisKey, analysis, previewRows) {
    const totalRowCount = analysis.row_count ?? analysis.rows.length;
    const previewMessage = totalRowCount > previewRows.length
        ? `先頭 ${previewRows.length} 件を表示 / 全 ${totalRowCount} 件`
        : `全 ${totalRowCount} 件を表示`;

    return `
        <div class="result-header">
            <div>
                <h2>${escapeHtml(analysis.analysis_name)}</h2>
                <p class="result-meta">${escapeHtml(previewMessage)}</p>
            </div>
            <a href="${buildAnalysisDetailHref(analysisKey, analysis.run_id || "")}" class="detail-link">詳細ページ</a>
        </div>
    `;
}

function renderAnalysisPanels(analyses, runId) {
    resultPanels.innerHTML = "";

    Object.entries(analyses || {}).forEach(([analysisKey, analysis]) => {
        const previewRows = (analysis.rows || [])
            .slice(0, PREVIEW_ROW_COUNT)
            .map((row, index) => ({ ...row, __rowIndex: index }));
        const section = document.createElement("section");
        section.className = "result-panel";
        section.innerHTML = `
            ${buildResultHeader(analysisKey, { ...analysis, run_id: runId }, previewRows)}
            ${buildTable(previewRows, { analysisKey, runId })}
        `;
        resultPanels.appendChild(section);
    });
}

function renderDashboard(data) {
    renderSummary(data);
    renderAnalysisPanels(data.analyses, data.run_id || "");
}

form.addEventListener("submit", async (event) => {
    event.preventDefault();

    const validationError = validateAnalyzeForm();
    if (validationError) {
        setStatus(validationError, "error");
        return;
    }

    const shouldExportExcel = Boolean(exportExcelToggle?.checked);
    isAnalyzing = true;
    syncSubmitState();
    setStatus("分析を実行しています...", "info");
    summaryPanel.className = "summary-panel hidden";
    resultPanels.innerHTML = "";

    try {
        const formData = new FormData(form);
        const response = await fetch("/api/analyze", {
            method: "POST",
            body: formData,
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "分析に失敗しました。");
        }

        saveLatestResult(data);
        renderDashboard(data);

        if (shouldExportExcel) {
            await downloadExcelArchive(data);
            setStatus("分析が完了し、Excel ZIP をダウンロードしました。", "success");
        } else {
            setStatus("分析が完了しました。", "success");
        }
    } catch (error) {
        setStatus(error.message, "error");
    } finally {
        isAnalyzing = false;
        syncSubmitState();
    }
});

csvFileInput?.addEventListener("change", () => {
    void refreshLogProfile();
});

[caseIdColumnSelect, activityColumnSelect, timestampColumnSelect, ...filterColumnRefs.map((filterRef) => filterRef.columnSelect)]
    .forEach((element) => {
        element?.addEventListener("change", () => {
            hideStatus();
            void refreshLogProfile();
        });
    });

filterColumnRefs.forEach((filterRef) => {
    filterRef.valueSelect?.addEventListener("change", hideStatus);
});

const latestResult = loadLatestResult();
if (latestResult) {
    renderDashboard(latestResult);
}

if (exportExcelToggle) {
    exportExcelToggle.addEventListener("change", syncExcelExportSettings);
    syncExcelExportSettings();
}

renderProfilePayload(currentProfilePayload);
syncSubmitState();
hideStatus();
