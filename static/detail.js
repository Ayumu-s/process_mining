const STORAGE_KEY = "processMiningLastResult";
const FLOW_SELECTION_STORAGE_KEY = "processMiningFlowSelection";
const DETAIL_ROW_LIMIT = 500;
const RENDERING_LIMIT = 1200; // Stricter limit for total elements
const EDGE_LIMIT = 800;      // Stricter limit for paths specifically
const AGGRESSIVE_LIMIT = 3000; // Threshold for extreme reduction

function debounce(func, wait) {
    let timeout;
    return (...args) => {
        clearTimeout(timeout);
        timeout = setTimeout(() => func(...args), wait);
    };
}

const analysisKey = document.body.dataset.analysisKey;
const statusPanel = document.getElementById("detail-status-panel");
const summaryPanel = document.getElementById("detail-summary-panel");
const chartPanel = document.getElementById("detail-chart-panel");
const chartTitle = document.getElementById("detail-chart-title");
const chartNote = document.getElementById("detail-chart-note");
const chartContainer = document.getElementById("detail-chart");
const resultPanel = document.getElementById("detail-result-panel");
const detailPageTitle = document.getElementById("detail-page-title");
const detailPageCopy = document.getElementById("detail-page-copy");
const FILTER_SLOT_KEYS = ["filter_value_1", "filter_value_2", "filter_value_3"];
const DEFAULT_FILTER_LABELS = {
    filter_value_1: "グループ/カテゴリー フィルター①",
    filter_value_2: "グループ/カテゴリー フィルター②",
    filter_value_3: "グループ/カテゴリー フィルター③",
};
const DEFAULT_DETAIL_FILTERS = Object.freeze({
    date_from: "",
    date_to: "",
    filter_value_1: "",
    filter_value_2: "",
    filter_value_3: "",
});
let activeDetailFilters = { ...DEFAULT_DETAIL_FILTERS };
let detailPageAnalysisLoader = null;

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

function cloneDetailFilters(filters = {}) {
    return {
        date_from: String(filters.date_from || "").trim(),
        date_to: String(filters.date_to || "").trim(),
        filter_value_1: String(filters.filter_value_1 || "").trim(),
        filter_value_2: String(filters.filter_value_2 || "").trim(),
        filter_value_3: String(filters.filter_value_3 || "").trim(),
    };
}

function buildDefaultFilterDefinitions(columnSettings = {}) {
    const rawDefinitions = Array.isArray(columnSettings?.filters) ? columnSettings.filters : [];
    const rawDefinitionMap = new Map(rawDefinitions.map((definition) => [definition.slot, definition]));

    return FILTER_SLOT_KEYS.map((slot) => {
        const rawDefinition = rawDefinitionMap.get(slot) || {};
        return {
            slot,
            label: rawDefinition.label || DEFAULT_FILTER_LABELS[slot],
            column_name: rawDefinition.column_name || "",
            options: [],
        };
    });
}

function normalizeFilterDefinitions(filters = [], columnSettings = {}) {
    const definitionMap = new Map(
        buildDefaultFilterDefinitions(columnSettings).map((definition) => [definition.slot, definition])
    );

    (Array.isArray(filters) ? filters : []).forEach((definition) => {
        if (!definitionMap.has(definition.slot)) {
            return;
        }

        definitionMap.set(definition.slot, {
            ...definitionMap.get(definition.slot),
            label: definition.label || definitionMap.get(definition.slot).label,
            column_name: definition.column_name || definitionMap.get(definition.slot).column_name,
            options: Array.isArray(definition.options) ? definition.options : [],
        });
    });

    return FILTER_SLOT_KEYS.map((slot) => definitionMap.get(slot));
}

function buildFilterQueryParams(filters = {}) {
    const normalizedFilters = cloneDetailFilters(filters);
    const params = new URLSearchParams();

    Object.entries(normalizedFilters).forEach(([filterName, filterValue]) => {
        if (filterValue) {
            params.set(filterName, filterValue);
        }
    });

    return params;
}

function hasActiveDetailFilters(filters = {}) {
    const normalizedFilters = cloneDetailFilters(filters);
    return Object.values(normalizedFilters).some((filterValue) => Boolean(filterValue));
}

function buildPatternDetailHref(runId, patternIndex) {
    return `/analysis/patterns/${encodeURIComponent(String(patternIndex))}?run_id=${encodeURIComponent(runId)}`;
}

function buildAnalysisDetailApiUrl(runId, rowOffset = 0, filters = activeDetailFilters) {
    const params = new URLSearchParams({
        row_limit: String(DETAIL_ROW_LIMIT),
        row_offset: String(Math.max(0, Number(rowOffset) || 0)),
    });
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/analyses/${encodeURIComponent(analysisKey)}?${params.toString()}`;
}

function buildTable(rows, options = {}) {
    if (!rows.length) {
        return '<p class="empty-state">表示できるデータがありません。</p>';
    }

    const { analysisKey: tableAnalysisKey = "", runId = "" } = options;
    const headers = Object.keys(rows[0]).filter((header) => !header.startsWith("__"));
    const headHtml = headers
        .map((header) => `<th>${escapeHtml(header)}</th>`)
        .join("");

    const bodyHtml = rows
        .map((row) => {
            const cells = headers
                .map((header) => {
                    const cellValue = escapeHtml(row[header]);
                    const isWideHeader = (
                        header === "処理順パターン" || 
                        header === "アクティビティ名" || 
                        header === "アクティビティ" ||
                        header === "前処理アクティビティ名" ||
                        header === "後処理アクティビティ名"
                    );
                    const isPatternLink = (
                        tableAnalysisKey === "pattern"
                        && header === "処理順パターン"
                        && runId
                        && Number.isInteger(row.__rowIndex)
                    );

                    if (isPatternLink) {
                        return `
                            <td class="table-cell--wide">
                                <div class="cell-scroll-wrapper">
                                    <a href="${buildPatternDetailHref(runId, row.__rowIndex)}" class="table-link">
                                        ${cellValue}
                                    </a>
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
                })
                .join("");
            return `<tr>${cells}</tr>`;
        })
        .join("");

    return `
        <div class="table-wrap">
            <table>
                <thead><tr>${headHtml}</tr></thead>
                <tbody>${bodyHtml}</tbody>
            </table>
        </div>
    `;
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

function getRunId(latestResult) {
    const params = new URLSearchParams(window.location.search);
    return params.get("run_id") || latestResult?.run_id || "";
}

async function fetchJson(url, fallbackMessage, timeoutMs = 30000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeoutMs);

    try {
        const response = await fetch(url, { signal: controller.signal });
        clearTimeout(id);
        const payload = await response.json();

        if (!response.ok) {
            throw new Error(payload.detail || payload.error || fallbackMessage);
        }

        return payload;
    } catch (error) {
        clearTimeout(id);
        if (error.name === "AbortError") {
            throw new Error("サーバーからの応答がタイムアウトしました。データ量を減らして再試行してください。");
        }
        throw error;
    }
}

function loadFlowSelection(runId) {
    try {
        sessionStorage.removeItem(FLOW_SELECTION_STORAGE_KEY);
    } catch {
        // Ignore storage failures.
    }

    return null;
}

function saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey) {
    try {
        sessionStorage.removeItem(FLOW_SELECTION_STORAGE_KEY);
    } catch {
        // Ignore storage failures.
    }
}

function loadAnalysisPage(runId, rowOffset = 0, filters = activeDetailFilters) {
    return fetchJson(
        buildAnalysisDetailApiUrl(runId, rowOffset, filters),
        "分析詳細の読み込みに失敗しました。"
    );
}

function buildVariantListApiUrl(runId, limit = 10, filters = activeDetailFilters) {
    const params = new URLSearchParams({
        limit: String(Math.max(0, Number(limit) || 0)),
    });
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/variants?${params.toString()}`;
}

function loadVariantList(runId, limit = 10, filters = activeDetailFilters) {
    return fetchJson(
        buildVariantListApiUrl(runId, limit, filters),
        "Variant 一覧の読み込みに失敗しました。"
    );
}

function buildBottleneckApiUrl(runId, limit = 5, variantId = null, filters = activeDetailFilters) {
    const params = new URLSearchParams({
        limit: String(Math.max(0, Number(limit) || 0)),
    });

    if (variantId !== null && variantId !== undefined) {
        params.set("variant_id", String(variantId));
    }
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/bottlenecks?${params.toString()}`;
}

function loadBottleneckSummary(runId, limit = 5, variantId = null, filters = activeDetailFilters) {
    return fetchJson(
        buildBottleneckApiUrl(runId, limit, variantId, filters),
        "Bottleneck summary could not be loaded."
    );
}

function buildTransitionCasesApiUrl(runId, fromActivity, toActivity, limit = 20, variantId = null, filters = activeDetailFilters) {
    const params = new URLSearchParams({
        from_activity: String(fromActivity || ""),
        to_activity: String(toActivity || ""),
        limit: String(Math.max(0, Number(limit) || 0)),
    });

    if (variantId !== null && variantId !== undefined) {
        params.set("variant_id", String(variantId));
    }
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/transition-cases?${params.toString()}`;
}

function loadTransitionCases(runId, fromActivity, toActivity, limit = 20, variantId = null, filters = activeDetailFilters) {
    return fetchJson(
        buildTransitionCasesApiUrl(runId, fromActivity, toActivity, limit, variantId, filters),
        "Transition cases could not be loaded."
    );
}

function buildFilterOptionsApiUrl(runId) {
    return `/api/runs/${encodeURIComponent(runId)}/filter-options`;
}

function loadFilterOptions(runId) {
    return fetchJson(
        buildFilterOptionsApiUrl(runId),
        "Filter options could not be loaded."
    );
}

function buildCaseTraceApiUrl(runId, caseId) {
    return `/api/runs/${encodeURIComponent(runId)}/cases/${encodeURIComponent(String(caseId || "").trim())}`;
}

function loadCaseTrace(runId, caseId) {
    return fetchJson(
        buildCaseTraceApiUrl(runId, caseId),
        "Case trace could not be loaded."
    );
}

function formatVariantRatio(ratio) {
    return (Number(ratio || 0) * 100).toLocaleString("ja-JP", {
        maximumFractionDigits: 2,
    });
}

function formatDurationHours(hours) {
    return `${Number(hours || 0).toLocaleString("ja-JP", {
        minimumFractionDigits: 1,
        maximumFractionDigits: 2,
    })}h`;
}

function formatDateTime(value) {
    if (!value) {
        return "";
    }

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return String(value);
    }

    return date.toLocaleString("ja-JP");
}

function formatDurationSeconds(seconds) {
    return `${Number(seconds || 0).toLocaleString("ja-JP", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
    })} sec`;
}

function buildTransitionKey(fromActivity, toActivity) {
    return `${fromActivity}__TO__${toActivity}`;
}

function getVariantSequenceText(variant) {
    return Array.isArray(variant?.activities)
        ? variant.activities.join(" → ")
        : "";
}

function buildVariantCoverageHtml(coverage) {
    if (!coverage) {
        return '<p class="panel-note">Coverage を計算できませんでした。</p>';
    }

    return `
        <span class="variant-coverage-label">Top ${escapeHtml(coverage.displayed_variant_count)} Coverage</span>
        <strong class="variant-coverage-value">${escapeHtml(formatVariantRatio(coverage.ratio))}%</strong>
        <span class="variant-coverage-sub">
            ${escapeHtml(Number(coverage.covered_case_count || 0).toLocaleString("ja-JP"))}
            / ${escapeHtml(Number(coverage.total_case_count || 0).toLocaleString("ja-JP"))} cases
        </span>
    `;
}

function buildFilterSelectionSummary(filters = {}) {
    const normalizedFilters = cloneDetailFilters(filters);
    const filterLabelMap = {
        date_from: "開始日",
        date_to: "終了日",
        department: "部署",
        channel: "チャネル",
        category: "カテゴリ",
    };
    const appliedItems = Object.entries(normalizedFilters)
        .filter(([, filterValue]) => Boolean(filterValue))
        .map(([filterName, filterValue]) => `${filterLabelMap[filterName]}: ${filterValue}`);

    return appliedItems.length
        ? appliedItems.join(" / ")
        : "フィルタ未適用";
}

function buildFilterSelectionSummary(filters = {}, filterDefinitions = []) {
    const normalizedFilters = cloneDetailFilters(filters);
    const filterLabelMap = new Map(filterDefinitions.map((definition) => [definition.slot, definition.label]));
    const appliedItems = [];

    if (normalizedFilters.date_from) {
        appliedItems.push(`開始日: ${normalizedFilters.date_from}`);
    }
    if (normalizedFilters.date_to) {
        appliedItems.push(`終了日: ${normalizedFilters.date_to}`);
    }

    FILTER_SLOT_KEYS.forEach((slot) => {
        if (normalizedFilters[slot]) {
            appliedItems.push(`${filterLabelMap.get(slot) || DEFAULT_FILTER_LABELS[slot]}: ${normalizedFilters[slot]}`);
        }
    });

    return appliedItems.length
        ? appliedItems.join(" / ")
        : "フィルタ未適用";
}

function buildVariantSelectionState(variants, selectedVariantId) {
    if (selectedVariantId === null) {
        return {
            title: "全体表示中",
            meta: "Variant を選択すると、その Variant に属するケースだけでフロー図を再描画します。",
            sequence: "現在は全ケースを使ったフロー図を表示しています。",
            titleAttribute: "全ケースを使ったフロー図を表示しています。",
        };
    }

    const selectedVariant = variants.find(
        (variant) => Number(variant.variant_id) === Number(selectedVariantId)
    );

    if (!selectedVariant) {
        return {
            title: "Variant 情報なし",
            meta: "選択中 Variant の情報を取得できませんでした。",
            sequence: "",
            titleAttribute: "",
        };
    }

    const sequenceText = getVariantSequenceText(selectedVariant);
    return {
        title: `Variant #${selectedVariant.variant_id} / ${formatVariantRatio(selectedVariant.ratio)}% / ${Number(selectedVariant.count || 0).toLocaleString("ja-JP")} cases`,
        meta: "選択中の Variant に属するケースだけでフロー図を表示しています。",
        sequence: sequenceText,
        titleAttribute: sequenceText,
    };
}

function buildVariantCardsHtml(variants, selectedVariantId = null) {
    if (!variants.length) {
        return '<p class="empty-state">表示できるVariantがありません。</p>';
    }

    return variants
        .map((variant) => {
            const isSelected = Number(variant.variant_id) === Number(selectedVariantId);
            const sequenceText = getVariantSequenceText(variant);
            const cardTitle = [
                `Variant #${variant.variant_id}`,
                `${formatVariantRatio(variant.ratio)}% / ${Number(variant.count || 0).toLocaleString("ja-JP")} cases`,
                sequenceText,
            ].join("\n");

            return `
                <button
                    type="button"
                    class="variant-card${isSelected ? " variant-card--selected" : ""}"
                    data-variant-id="${escapeHtml(variant.variant_id)}"
                    aria-pressed="${isSelected ? "true" : "false"}"
                    title="${escapeHtml(cardTitle)}"
                >
                    <span class="variant-card-rank">Variant #${escapeHtml(variant.variant_id)}</span>
                    <span class="variant-card-meta">
                        ${escapeHtml(formatVariantRatio(variant.ratio))}% / ${escapeHtml(Number(variant.count || 0).toLocaleString("ja-JP"))} cases
                    </span>
                    <span class="variant-card-sequence">${escapeHtml(sequenceText)}</span>
                </button>
            `;
        })
        .join("");
}

function buildBottleneckCardsHtml(items, kind, selectionState = {}) {
    if (!Array.isArray(items) || !items.length) {
        return '<p class="empty-state">No bottlenecks available.</p>';
    }

    return items
        .map((item, index) => {
            const title = kind === "activity"
                ? item.activity
                : `${item.from_activity} → ${item.to_activity}`;
            const itemActivity = kind === "activity" ? item.activity : "";
            const itemTransitionKey = kind === "transition"
                ? item.transition_key || buildTransitionKey(item.from_activity, item.to_activity)
                : "";
            const isSelected = kind === "activity"
                ? itemActivity === selectionState.selectedActivity
                : itemTransitionKey === selectionState.selectedTransitionKey;
            const itemTitle = [
                title,
                `Avg ${formatDurationHours(item.avg_duration_hours)}`,
                `Median ${formatDurationHours(item.median_duration_hours)}`,
                `Max ${formatDurationHours(item.max_duration_hours)}`,
            ].join("\n");

            return `
                <button
                    type="button"
                    class="bottleneck-card${isSelected ? " bottleneck-card--selected" : ""}"
                    data-bottleneck-kind="${escapeHtml(kind)}"
                    data-activity="${escapeHtml(itemActivity)}"
                    data-transition-key="${escapeHtml(itemTransitionKey)}"
                    data-from-activity="${escapeHtml(item.from_activity || "")}"
                    data-to-activity="${escapeHtml(item.to_activity || "")}"
                    aria-pressed="${isSelected ? "true" : "false"}"
                    title="${escapeHtml(itemTitle)}"
                >
                    <div class="bottleneck-card-head">
                        <span class="bottleneck-card-rank">#${escapeHtml(index + 1)}</span>
                        <strong class="bottleneck-card-title">${escapeHtml(title)}</strong>
                    </div>
                    <p class="bottleneck-card-primary">Avg ${escapeHtml(formatDurationHours(item.avg_duration_hours))}</p>
                    <p class="bottleneck-card-meta">
                        ${escapeHtml(Number(item.count || 0).toLocaleString("ja-JP"))} intervals
                        / ${escapeHtml(Number(item.case_count || 0).toLocaleString("ja-JP"))} cases
                    </p>
                    <p class="bottleneck-card-secondary">
                        Median ${escapeHtml(formatDurationHours(item.median_duration_hours))}
                        / Max ${escapeHtml(formatDurationHours(item.max_duration_hours))}
                    </p>
                </button>
            `;
        })
        .join("");
}

function buildCaseDrilldownTable(rows) {
    if (!Array.isArray(rows) || !rows.length) {
        return '<p class="empty-state">No cases available.</p>';
    }

    const tableRows = rows.map((row) => ({
        case_id: row.case_id,
        duration_sec: formatDurationSeconds(row.duration_sec),
        duration_text: row.duration_text,
        from_time: row.from_time,
        to_time: row.to_time,
    }));

    return buildTable(tableRows);
}

function buildCaseTraceSummaryHtml(caseId, summary) {
    if (!summary) {
        return "";
    }

    const summaryCards = [
        { label: "Case ID", value: caseId },
        { label: "イベント数", value: Number(summary.event_count || 0).toLocaleString("ja-JP") },
        { label: "開始時刻", value: formatDateTime(summary.start_time) },
        { label: "終了時刻", value: formatDateTime(summary.end_time) },
        { label: "総所要時間", value: summary.total_duration_text || "-" },
        { label: "総所要時間(sec)", value: formatDurationSeconds(summary.total_duration_sec) },
    ];

    return `
        <div class="case-trace-summary-grid">
            ${summaryCards.map((item) => `
                <article class="case-trace-summary-card">
                    <span class="summary-label">${escapeHtml(item.label)}</span>
                    <strong>${escapeHtml(item.value)}</strong>
                </article>
            `).join("")}
        </div>
    `;
}

function buildCaseTraceEventsTable(events) {
    if (!Array.isArray(events) || !events.length) {
        return '<p class="empty-state">表示できるイベントがありません。</p>';
    }

    const eventRows = events.map((eventRow) => ({
        "順番": eventRow.sequence_no,
        "アクティビティ": eventRow.activity,
        "時刻": formatDateTime(eventRow.timestamp),
        "次アクティビティ": eventRow.next_activity || "完了",
        "次イベントまでの待ち時間": eventRow.wait_to_next_text || "-",
    }));

    return buildTable(eventRows);
}

function applyHeatClass(targetElement, heatEntry) {
    if (!targetElement) {
        return;
    }

    for (let level = 1; level <= 5; level += 1) {
        targetElement.classList.remove(`heat-${level}`);
    }

    if (heatEntry?.heat_class) {
        targetElement.classList.add(heatEntry.heat_class);
    }
}

function collectSelectedTransitionActivities(svgElement, selectedTransitionKey) {
    const activityNames = new Set();

    if (!selectedTransitionKey) {
        return activityNames;
    }

    svgElement.querySelectorAll(".process-map-edge").forEach((edgePathElement) => {
        if ((edgePathElement.dataset.transitionKey || "") !== selectedTransitionKey) {
            return;
        }

        const sourceActivity = edgePathElement.dataset.source || "";
        const targetActivity = edgePathElement.dataset.target || "";

        if (sourceActivity) {
            activityNames.add(sourceActivity);
        }
        if (targetActivity) {
            activityNames.add(targetActivity);
        }
    });

    return activityNames;
}

function applyProcessMapDecorators(viewportElement, options = {}) {
    const {
        activityHeatmap = {},
        transitionHeatmap = {},
        selectedActivity = "",
        selectedTransitionKey = "",
    } = options;
    const svgElement = viewportElement.querySelector("svg.process-map-svg");

    if (!svgElement) {
        return;
    }

    const hasSelection = Boolean(selectedActivity || selectedTransitionKey);
    const selectedTransitionActivities = collectSelectedTransitionActivities(svgElement, selectedTransitionKey);

    svgElement.querySelectorAll(".process-map-node-group").forEach((nodeGroupElement) => {
        const nodeRectElement = nodeGroupElement.querySelector(".process-map-node");
        const activityName = nodeRectElement?.dataset.activity || "";
        const isSelected = (Boolean(selectedActivity) && activityName === selectedActivity)
            || selectedTransitionActivities.has(activityName);

        applyHeatClass(nodeRectElement, activityHeatmap[activityName]);
        nodeGroupElement.classList.toggle("is-selected", isSelected);
        nodeGroupElement.classList.toggle("is-dimmed", hasSelection && !isSelected);
    });

    svgElement.querySelectorAll(".process-map-edge").forEach((edgePathElement) => {
        const transitionKey = edgePathElement.dataset.transitionKey || "";
        const isSelected = Boolean(selectedTransitionKey) && transitionKey === selectedTransitionKey;

        applyHeatClass(edgePathElement, transitionHeatmap[transitionKey]);
        edgePathElement.classList.toggle("is-selected", isSelected);
        edgePathElement.classList.toggle("is-dimmed", hasSelection && !isSelected);
    });

    svgElement.querySelectorAll(".process-map-edge-label").forEach((edgeLabelElement) => {
        const transitionKey = edgeLabelElement.dataset.transitionKey || "";
        const isSelected = Boolean(selectedTransitionKey) && transitionKey === selectedTransitionKey;

        edgeLabelElement.classList.toggle("is-selected", isSelected);
        edgeLabelElement.classList.toggle("is-dimmed", hasSelection && !isSelected);
    });
}

function renderSummary(data, analysis) {
    const rowCount = analysis.row_count ?? analysis.rows.length;

    summaryPanel.className = "summary-panel";
    summaryPanel.innerHTML = `
        <article class="summary-card">
            <span class="summary-label">入力ファイル</span>
            <strong>${escapeHtml(data.source_file_name)}</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">ケース数 / イベント数</span>
            <strong>${escapeHtml(data.case_count)} / ${escapeHtml(data.event_count)}</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">表示件数</span>
            <strong>${escapeHtml(rowCount)}</strong>
        </article>
    `;
}

function renderResult(analysis, runId = "", onPageChange = null) {
    const rowOffset = Number(analysis.row_offset || 0);
    const rowCount = analysis.row_count ?? analysis.rows.length;
    const returnedRowCount = analysis.returned_row_count ?? analysis.rows.length;
    const tableRows = analysis.rows.map((row, index) => ({ ...row, __rowIndex: rowOffset + index }));
    const resultMeta = returnedRowCount < rowCount
        ? `全 ${escapeHtml(rowCount)} 件中、先頭 ${escapeHtml(returnedRowCount)} 件を表示`
        : `全 ${escapeHtml(rowCount)} 件を表示`;
    const pageStart = analysis.page_start_row_number ?? (returnedRowCount ? rowOffset + 1 : 0);
    const pageEnd = analysis.page_end_row_number ?? (rowOffset + returnedRowCount);
    const paginationHtml = rowCount > DETAIL_ROW_LIMIT
        ? `
            <div class="result-pagination">
                <p class="result-pagination-meta">${escapeHtml(pageStart)} - ${escapeHtml(pageEnd)} / ${escapeHtml(rowCount)} 件</p>
                <div class="result-pagination-actions">
                    <button
                        type="button"
                        id="detail-prev-page-button"
                        class="ghost-link result-pagination-button"
                        ${analysis.has_previous_page ? "" : "disabled"}
                    >
                        前のページ
                    </button>
                    <button
                        type="button"
                        id="detail-next-page-button"
                        class="ghost-link result-pagination-button"
                        ${analysis.has_next_page ? "" : "disabled"}
                    >
                        次のページ
                    </button>
                </div>
            </div>
        `
        : "";

    resultPanel.className = "result-panel";
    resultPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>${escapeHtml(analysis.analysis_name)}</h2>
                <p class="result-meta">${resultMeta}</p>
                ${returnedRowCount < rowCount ? '<p class="result-meta">大量データでは画面停止を防ぐため、詳細表は一部のみ取得しています。</p>' : ""}
                ${analysis.excel_file ? `<p class="excel-path">Excel: ${escapeHtml(analysis.excel_file)}</p>` : ""}
            </div>
        </div>
        ${buildTable(tableRows, { analysisKey, runId })}
        ${paginationHtml}
    `;

    if (!onPageChange || rowCount <= DETAIL_ROW_LIMIT) {
        return;
    }

    const previousPageButton = document.getElementById("detail-prev-page-button");
    const nextPageButton = document.getElementById("detail-next-page-button");

    if (previousPageButton && analysis.has_previous_page) {
        previousPageButton.addEventListener("click", () => {
            onPageChange(analysis.previous_row_offset ?? 0);
        });
    }

    if (nextPageButton && analysis.has_next_page) {
        nextPageButton.addEventListener("click", () => {
            onPageChange(analysis.next_row_offset ?? pageEnd);
        });
    }
}

function getInitialPatternFlowSettings(totalPatternCount) {
    if (totalPatternCount >= 50000) {
        return { patterns: 10, activities: 20, connections: 15, labels: 0 };
    }

    if (totalPatternCount >= 10000) {
        return { patterns: 15, activities: 30, connections: 20, labels: 0 };
    }

    if (totalPatternCount >= 2000) {
        return { patterns: 25, activities: 45, connections: 30, labels: 10 };
    }

    if (totalPatternCount >= 500) {
        return { patterns: 40, activities: 60, connections: 40, labels: 30 };
    }

    return { patterns: 100, activities: 100, connections: 100, labels: 100 };
}

function wrapJapaneseLabel(text, maxCharsPerLine = 12, maxLines = 2) {
    const characters = Array.from(String(text));
    const lines = [];

    for (let index = 0; index < characters.length; index += maxCharsPerLine) {
        lines.push(characters.slice(index, index + maxCharsPerLine).join(""));
    }

    if (lines.length > maxLines) {
        const visibleLines = lines.slice(0, maxLines);
        const lastLineCharacters = Array.from(visibleLines[maxLines - 1]).slice(0, Math.max(0, maxCharsPerLine - 1));
        visibleLines[maxLines - 1] = `${lastLineCharacters.join("")}…`;
        return visibleLines;
    }

    return lines;
}

function reindexLayerNodes(layerNodes) {
    layerNodes.forEach((node, index) => {
        node.orderScore = index;
    });
}

function countEdgeCrossings(edges, nodeLookup) {
    let crossingScore = 0;

    for (let leftIndex = 0; leftIndex < edges.length; leftIndex += 1) {
        const leftEdge = edges[leftIndex];
        const leftSource = nodeLookup.get(leftEdge.source);
        const leftTarget = nodeLookup.get(leftEdge.target);

        if (!leftSource || !leftTarget) {
            continue;
        }

        for (let rightIndex = leftIndex + 1; rightIndex < edges.length; rightIndex += 1) {
            const rightEdge = edges[rightIndex];
            const rightSource = nodeLookup.get(rightEdge.source);
            const rightTarget = nodeLookup.get(rightEdge.target);

            if (!rightSource || !rightTarget) {
                continue;
            }

            const sourceDiff = leftSource.orderScore - rightSource.orderScore;
            const targetDiff = leftTarget.orderScore - rightTarget.orderScore;

            if (sourceDiff === 0 || targetDiff === 0) {
                continue;
            }

            if (sourceDiff * targetDiff < 0) {
                crossingScore += Math.min(leftEdge.count, rightEdge.count);
            }
        }
    }

    return crossingScore;
}

function countLayerCrossings(layer, edges, nodeLookup) {
    const outgoingGroups = new Map();
    const incomingGroups = new Map();

    edges.forEach((edge) => {
        const sourceNode = nodeLookup.get(edge.source);
        const targetNode = nodeLookup.get(edge.target);

        if (!sourceNode || !targetNode) {
            return;
        }

        if (sourceNode.layer === layer && targetNode.layer > layer) {
            const groupKey = targetNode.layer;

            if (!outgoingGroups.has(groupKey)) {
                outgoingGroups.set(groupKey, []);
            }

            outgoingGroups.get(groupKey).push(edge);
        }

        if (targetNode.layer === layer && sourceNode.layer < layer) {
            const groupKey = sourceNode.layer;

            if (!incomingGroups.has(groupKey)) {
                incomingGroups.set(groupKey, []);
            }

            incomingGroups.get(groupKey).push(edge);
        }
    });

    let crossingScore = 0;
    outgoingGroups.forEach((groupEdges) => {
        crossingScore += countEdgeCrossings(groupEdges, nodeLookup);
    });
    incomingGroups.forEach((groupEdges) => {
        crossingScore += countEdgeCrossings(groupEdges, nodeLookup);
    });

    return crossingScore;
}

function optimizeLayerBySwaps(layerNodes, edges, nodeLookup) {
    if (layerNodes.length < 2) {
        return;
    }

    const layer = layerNodes[0].layer;
    let updated = true;

    while (updated) {
        updated = false;

        for (let index = 0; index < layerNodes.length - 1; index += 1) {
            const currentScore = countLayerCrossings(layer, edges, nodeLookup);
            const firstNode = layerNodes[index];
            const secondNode = layerNodes[index + 1];

            layerNodes[index] = secondNode;
            layerNodes[index + 1] = firstNode;
            reindexLayerNodes(layerNodes);

            const swappedScore = countLayerCrossings(layer, edges, nodeLookup);

            if (swappedScore < currentScore) {
                updated = true;
                continue;
            }

            layerNodes[index] = firstNode;
            layerNodes[index + 1] = secondNode;
            reindexLayerNodes(layerNodes);
        }
    }
}

function renderFrequencyChart(analysis) {
    const chartRows = analysis.rows.slice(0, 15);

    if (!chartRows.length) {
        chartPanel.className = "result-panel";
        chartTitle.textContent = "頻度分析グラフ";
        chartNote.textContent = "表示できるデータがありません。";
        chartContainer.innerHTML = '<p class="empty-state">表示できるデータがありません。</p>';
        return;
    }

    const maxEventCount = Math.max(...chartRows.map((row) => Number(row["イベント件数"]) || 0), 1);
    const chartWidth = 1400;
    const labelWidth = 210;
    const barAreaWidth = 860;
    const infoWidth = 220;
    const chartLeft = 18;
    const barStartX = chartLeft + labelWidth + 12;
    const infoStartX = barStartX + barAreaWidth + 20;
    const chartHeight = 92 + chartRows.length * 44;
    const scaleValues = [0, 0.25, 0.5, 0.75, 1];

    const gridLines = scaleValues
        .map((rate) => {
            const x = barStartX + barAreaWidth * rate;
            return `
                <line x1="${x}" y1="42" x2="${x}" y2="${chartHeight - 16}" class="frequency-svg-grid"></line>
                <text x="${x}" y="26" text-anchor="${rate === 0 ? "start" : rate === 1 ? "end" : "middle"}" class="frequency-svg-scale">
                    ${escapeHtml(Math.round(maxEventCount * rate).toLocaleString("ja-JP"))}
                </text>
            `;
        })
        .join("");

    const rowsSvg = chartRows
        .map((row, index) => {
            const activityName = row["アクティビティ"];
            const eventCount = Number(row["イベント件数"]) || 0;
            const averageDuration = row["平均時間(分)"];
            const barWidth = Math.max(12, (eventCount / maxEventCount) * barAreaWidth);
            const rowCenterY = 62 + index * 44;
            const labelLines = wrapJapaneseLabel(activityName);
            const labelSvg = labelLines
                .map((line, lineIndex) => {
                    const lineOffset = labelLines.length === 1 ? 0 : lineIndex === 0 ? -8 : 10;
                    return `
                        <text x="${chartLeft}" y="${rowCenterY + lineOffset}" class="frequency-svg-label">
                            ${escapeHtml(line)}
                        </text>
                    `;
                })
                .join("");

            return `
                ${labelSvg}
                <rect x="${barStartX}" y="${rowCenterY - 10}" width="${barAreaWidth}" height="20" rx="10" ry="10" class="frequency-svg-track"></rect>
                <rect x="${barStartX}" y="${rowCenterY - 10}" width="${barWidth}" height="20" rx="10" ry="10" class="frequency-svg-bar"></rect>
                <text x="${infoStartX}" y="${rowCenterY - 2}" class="frequency-svg-count">
                    ${escapeHtml(eventCount.toLocaleString("ja-JP"))}件
                </text>
                <text x="${infoStartX}" y="${rowCenterY + 14}" class="frequency-svg-avg">
                    平均${escapeHtml(String(averageDuration))}分
                </text>
            `;
        })
        .join("");

    chartPanel.className = "result-panel";
    chartTitle.textContent = "頻度分析グラフ";
    chartNote.textContent = "左がアクティビティ名、中央の棒がイベント件数、右が件数と平均時間(分)です。";
    chartContainer.innerHTML = `
        <svg
            class="frequency-chart-svg"
            viewBox="0 0 ${chartWidth} ${chartHeight}"
            role="img"
            aria-label="頻度分析グラフ"
            preserveAspectRatio="xMinYMin meet"
        >
            ${gridLines}
            ${rowsSvg}
        </svg>
    `;
}

function renderTransitionChart(analysis) {
    const chartRows = analysis.rows.slice(0, 15);

    if (!chartRows.length) {
        chartPanel.className = "result-panel";
        chartTitle.textContent = "前後処理分析グラフ";
        chartNote.textContent = "表示できるデータがありません。";
        chartContainer.innerHTML = '<p class="empty-state">表示できるデータがありません。</p>';
        return;
    }

    const maxTransitionCount = Math.max(...chartRows.map((row) => Number(row["遷移件数"]) || 0), 1);
    const chartWidth = 1400;
    const labelWidth = 250;
    const barAreaWidth = 820;
    const chartLeft = 18;
    const barStartX = chartLeft + labelWidth + 12;
    const infoStartX = barStartX + barAreaWidth + 20;
    const chartHeight = 92 + chartRows.length * 52;
    const scaleValues = [0, 0.25, 0.5, 0.75, 1];

    const gridLines = scaleValues
        .map((rate) => {
            const x = barStartX + barAreaWidth * rate;
            return `
                <line x1="${x}" y1="42" x2="${x}" y2="${chartHeight - 16}" class="transition-svg-grid"></line>
                <text x="${x}" y="26" text-anchor="${rate === 0 ? "start" : rate === 1 ? "end" : "middle"}" class="transition-svg-scale">
                    ${escapeHtml(Math.round(maxTransitionCount * rate).toLocaleString("ja-JP"))}
                </text>
            `;
        })
        .join("");

    const rowsSvg = chartRows
        .map((row, index) => {
            const fromActivity = row["前処理アクティビティ名"];
            const toActivity = row["後処理アクティビティ名"];
            const transitionLabel = `${fromActivity} → ${toActivity}`;
            const transitionCount = Number(row["遷移件数"]) || 0;
            const avgWaitingTime = row["平均待ち時間(分)"];
            const transitionRatio = row["遷移比率(%)"];
            const barWidth = Math.max(12, (transitionCount / maxTransitionCount) * barAreaWidth);
            const rowCenterY = 68 + index * 52;

            return `
                <text x="${chartLeft}" y="${rowCenterY + 4}" class="transition-svg-label">
                    ${escapeHtml(transitionLabel)}
                </text>
                <rect x="${barStartX}" y="${rowCenterY - 10}" width="${barAreaWidth}" height="20" rx="10" ry="10" class="transition-svg-track"></rect>
                <rect x="${barStartX}" y="${rowCenterY - 10}" width="${barWidth}" height="20" rx="10" ry="10" class="transition-svg-bar"></rect>
                <text x="${infoStartX}" y="${rowCenterY - 2}" class="transition-svg-count">
                    ${escapeHtml(transitionCount.toLocaleString("ja-JP"))}件 (${escapeHtml(String(transitionRatio))}%)
                </text>
                <text x="${infoStartX}" y="${rowCenterY + 14}" class="transition-svg-avg">
                    平均待ち${escapeHtml(String(avgWaitingTime))}分
                </text>
            `;
        })
        .join("");

    chartPanel.className = "result-panel";
    chartTitle.textContent = "前後処理分析グラフ";
    chartNote.textContent = "左が前処理→後処理、中央の棒が遷移件数、右が件数比率と平均待ち時間(分)です。";
    chartContainer.innerHTML = `
        <svg
            class="transition-chart-svg"
            viewBox="0 0 ${chartWidth} ${chartHeight}"
            role="img"
            aria-label="前後処理分析グラフ"
            preserveAspectRatio="xMinYMin meet"
        >
            ${gridLines}
            ${rowsSvg}
        </svg>
    `;
}

function buildProcessFlowData(patternRows, transitionRows = [], frequencyRows = []) {
    const nodeMap = new Map();
    const edgeMap = new Map();

    function ensureNode(name) {
        if (!nodeMap.has(name)) {
            nodeMap.set(name, {
                name,
                weight: 0,
                caseWeight: 0,
                positionTotal: 0,
                positionWeight: 0,
                incoming: 0,
                outgoing: 0,
                layerScore: 0,
                layer: 0,
                orderScore: 0,
            });
        }

        return nodeMap.get(name);
    }

    frequencyRows.forEach((row) => {
        const activityName = String(row["アクティビティ"] || "").trim();

        if (!activityName) {
            return;
        }

        const node = ensureNode(activityName);
        node.weight = Math.max(node.weight, Number(row["イベント件数"]) || 0);
        node.caseWeight = Math.max(node.caseWeight, Number(row["ケース数"]) || 0);

        // Extract duration metrics for tooltips
        if (row["平均時間(分)"] !== undefined) {
            node.avgDuration = Number(row["平均時間(分)"]) || 0;
        }
        if (row["最大時間(分)"] !== undefined) {
            node.maxDuration = Number(row["最大時間(分)"]) || 0;
        }
    });

    patternRows.forEach((row) => {
        const caseCount = Number(row["ケース数"]) || 0;
        const steps = String(row["処理順パターン"])
            .split("→")
            .map((step) => step.trim())
            .filter(Boolean);

        steps.forEach((step, stepIndex) => {
            const node = ensureNode(step);
            node.positionTotal += stepIndex * caseCount;
            node.positionWeight += caseCount;

            if (node.weight === 0) {
                node.weight += caseCount;
            }

            if (node.caseWeight === 0) {
                node.caseWeight += caseCount;
            }

            if (stepIndex === steps.length - 1) {
                return;
            }

            const nextStep = steps[stepIndex + 1];
            ensureNode(nextStep);

            if (transitionRows.length) {
                return;
            }

            const edgeKey = `${step}|||${nextStep}`;

            if (!edgeMap.has(edgeKey)) {
                edgeMap.set(edgeKey, {
                    source: step,
                    target: nextStep,
                    count: 0,
                });
            }

            edgeMap.get(edgeKey).count += caseCount;
        });
    });

    transitionRows.forEach((row) => {
        const sourceName = String(row["前処理アクティビティ名"] || "").trim();
        const targetName = String(row["後処理アクティビティ名"] || "").trim();
        const transitionCount = Number(row["遷移件数"]) || 0;

        if (!sourceName || !targetName || transitionCount <= 0) {
            return;
        }

        ensureNode(sourceName);
        ensureNode(targetName);

        const edgeKey = `${sourceName}|||${targetName}`;

        if (!edgeMap.has(edgeKey)) {
            edgeMap.set(edgeKey, {
                source: sourceName,
                target: targetName,
                count: 0,
            });
        }

        edgeMap.get(edgeKey).count += transitionCount;
    });

    const nodes = Array.from(nodeMap.values());
    const edges = Array.from(edgeMap.values())
        .filter((edge) => edge.source !== edge.target)
        .sort((left, right) => right.count - left.count);
    const nodeLookup = new Map(nodes.map((node) => [node.name, node]));
    const nodesByLayer = new Map();

    edges.forEach((edge) => {
        const sourceNode = nodeLookup.get(edge.source);
        const targetNode = nodeLookup.get(edge.target);

        if (sourceNode) {
            sourceNode.outgoing += edge.count;
        }

        if (targetNode) {
            targetNode.incoming += edge.count;
        }
    });

    nodes.forEach((node) => {
        node.layerScore = node.positionWeight
            ? node.positionTotal / node.positionWeight
            : 0;
        node.layer = Math.max(0, Math.round(node.layerScore));

        if (node.weight === 0) {
            node.weight = Math.max(node.incoming, node.outgoing, node.caseWeight, 1);
        }
    });

    const rawLayers = Array.from(new Set(nodes.map((node) => node.layer))).sort((left, right) => left - right);
    const compactLayerMap = new Map(rawLayers.map((layer, index) => [layer, index]));

    nodes.forEach((node) => {
        node.layer = compactLayerMap.get(node.layer) || 0;

        if (!nodesByLayer.has(node.layer)) {
            nodesByLayer.set(node.layer, []);
        }

        nodesByLayer.get(node.layer).push(node);
    });

    const maxLayer = Math.max(...nodes.map((node) => node.layer), 0);

    for (let layer = 0; layer <= maxLayer; layer += 1) {
        const layerNodes = nodesByLayer.get(layer) || [];
        layerNodes.sort((left, right) => {
            if (right.weight !== left.weight) {
                return right.weight - left.weight;
            }

            return left.name.localeCompare(right.name, "ja");
        });

        reindexLayerNodes(layerNodes);
    }

    for (let iteration = 0; iteration < 6; iteration += 1) {
        for (let layer = 1; layer <= maxLayer; layer += 1) {
            const layerNodes = nodesByLayer.get(layer) || [];
            layerNodes.sort((left, right) => {
                const leftEdges = edges.filter((edge) => edge.target === left.name);
                const rightEdges = edges.filter((edge) => edge.target === right.name);
                const leftWeight = leftEdges.reduce((total, edge) => {
                    const sourceNode = nodeLookup.get(edge.source);
                    const distance = sourceNode ? Math.max(1, left.layer - sourceNode.layer) : 1;
                    return total + edge.count / distance;
                }, 0);
                const rightWeight = rightEdges.reduce((total, edge) => {
                    const sourceNode = nodeLookup.get(edge.source);
                    const distance = sourceNode ? Math.max(1, right.layer - sourceNode.layer) : 1;
                    return total + edge.count / distance;
                }, 0);
                const leftScore = leftEdges.reduce((total, edge) => {
                    const sourceNode = nodeLookup.get(edge.source);
                    const distance = sourceNode ? Math.max(1, left.layer - sourceNode.layer) : 1;
                    return sourceNode ? total + sourceNode.orderScore * (edge.count / distance) : total;
                }, 0);
                const rightScore = rightEdges.reduce((total, edge) => {
                    const sourceNode = nodeLookup.get(edge.source);
                    const distance = sourceNode ? Math.max(1, right.layer - sourceNode.layer) : 1;
                    return sourceNode ? total + sourceNode.orderScore * (edge.count / distance) : total;
                }, 0);
                const leftAverage = leftWeight ? leftScore / leftWeight : left.orderScore;
                const rightAverage = rightWeight ? rightScore / rightWeight : right.orderScore;

                if (leftAverage !== rightAverage) {
                    return leftAverage - rightAverage;
                }

                return right.weight - left.weight;
            });

            reindexLayerNodes(layerNodes);
        }

        for (let layer = maxLayer - 1; layer >= 0; layer -= 1) {
            const layerNodes = nodesByLayer.get(layer) || [];
            layerNodes.sort((left, right) => {
                const leftEdges = edges.filter((edge) => edge.source === left.name);
                const rightEdges = edges.filter((edge) => edge.source === right.name);
                const leftWeight = leftEdges.reduce((total, edge) => {
                    const targetNode = nodeLookup.get(edge.target);
                    const distance = targetNode ? Math.max(1, targetNode.layer - left.layer) : 1;
                    return total + edge.count / distance;
                }, 0);
                const rightWeight = rightEdges.reduce((total, edge) => {
                    const targetNode = nodeLookup.get(edge.target);
                    const distance = targetNode ? Math.max(1, targetNode.layer - right.layer) : 1;
                    return total + edge.count / distance;
                }, 0);
                const leftScore = leftEdges.reduce((total, edge) => {
                    const targetNode = nodeLookup.get(edge.target);
                    const distance = targetNode ? Math.max(1, targetNode.layer - left.layer) : 1;
                    return targetNode ? total + targetNode.orderScore * (edge.count / distance) : total;
                }, 0);
                const rightScore = rightEdges.reduce((total, edge) => {
                    const targetNode = nodeLookup.get(edge.target);
                    const distance = targetNode ? Math.max(1, targetNode.layer - right.layer) : 1;
                    return targetNode ? total + targetNode.orderScore * (edge.count / distance) : total;
                }, 0);
                const leftAverage = leftWeight ? leftScore / leftWeight : left.orderScore;
                const rightAverage = rightWeight ? rightScore / rightWeight : right.orderScore;

                if (leftAverage !== rightAverage) {
                    return leftAverage - rightAverage;
                }

                return right.weight - left.weight;
            });

            reindexLayerNodes(layerNodes);
        }
    }

    for (let layer = 1; layer < maxLayer; layer += 1) {
        const layerNodes = nodesByLayer.get(layer) || [];
        optimizeLayerBySwaps(layerNodes, edges, nodeLookup);
    }
    
    // CELONIS STYLE: Extract main spine
    // Find highest throughput path from top to bottom
    const mainSpineNodes = new Set();
    const mainSpineEdges = new Set();
    
    if (nodes.length > 0) {
        // Start from node with layer 0 and highest weight
        let currentNodes = nodes.filter(n => n.layer === 0).sort((a, b) => b.weight - a.weight);
        if(currentNodes.length > 0) {
            let currentNode = currentNodes[0];
            mainSpineNodes.add(currentNode.name);
            
            // Greedily follow the heaviest outgoing edge
            while (currentNode) {
                const outgoing = edges.filter(e => e.source === currentNode.name && nodeLookup.get(e.target).layer > currentNode.layer);
                if (outgoing.length === 0) break;
                
                // Sort by weight/count
                outgoing.sort((a, b) => b.count - a.count);
                const heaviestEdge = outgoing[0];
                const nextNodeName = heaviestEdge.target;
                
                // Prevent infinite loops just in case
                if(mainSpineNodes.has(nextNodeName)) break;
                
                mainSpineEdges.add(getProcessFlowEdgeKey(heaviestEdge));
                mainSpineNodes.add(nextNodeName);
                
                currentNode = nodeLookup.get(nextNodeName);
            }
        }
    }
    
    // Tag nodes and edges
    nodes.forEach(node => {
        node.isMainSpine = mainSpineNodes.has(node.name);
    });
    
    edges.forEach(edge => {
        edge.isMainSpine = mainSpineEdges.has(getProcessFlowEdgeKey(edge));
    });

    return { nodes, edges, mainSpineNodes, mainSpineEdges };
}

function filterProcessFlowData(sourceNodes, sourceEdges, activityPercent = 100, connectionPercent = 100) {
    if (!sourceNodes.length || !sourceEdges.length) {
        return {
            nodes: [],
            edges: [],
            totalNodeCount: sourceNodes.length,
            totalEdgeCount: sourceEdges.length,
        };
    }

    const activityLimit = Math.min(
        sourceNodes.length,
        Math.max(2, Math.ceil(sourceNodes.length * (activityPercent / 100)))
    );
    const selectedNodes = [...sourceNodes]
        .sort((left, right) => {
            if (right.weight !== left.weight) {
                return right.weight - left.weight;
            }

            return left.name.localeCompare(right.name, "ja");
        })
        .slice(0, activityLimit);
    const selectedNodeNames = new Set(selectedNodes.map((node) => node.name));
    const candidateEdges = sourceEdges.filter((edge) => {
        return selectedNodeNames.has(edge.source) && selectedNodeNames.has(edge.target);
    });

    const connectionLimit = candidateEdges.length
        ? Math.min(
            candidateEdges.length,
            Math.max(1, Math.ceil(candidateEdges.length * (connectionPercent / 100)))
        )
        : 0;
    const selectedEdges = candidateEdges.slice(0, connectionLimit);
    const connectedNodeNames = new Set();

    selectedEdges.forEach((edge) => {
        connectedNodeNames.add(edge.source);
        connectedNodeNames.add(edge.target);
    });

    const visibleNodes = selectedNodes
        .filter((node) => connectedNodeNames.has(node.name))
        .map((node) => ({ ...node }));
    const visibleEdges = selectedEdges.map((edge) => ({ ...edge }));

    return {
        nodes: visibleNodes,
        edges: visibleEdges,
        totalNodeCount: sourceNodes.length,
        totalEdgeCount: sourceEdges.length,
    };
}

function getProcessFlowEdgeKey(edge) {
    return `${edge.source}|||${edge.target}`;
}

function buildProcessMapLabelState(edges, labelPercent = 100) {
    const sortedEdges = [...edges]
        .sort((left, right) => {
            if (right.count !== left.count) {
                return right.count - left.count;
            }

            return getProcessFlowEdgeKey(left).localeCompare(getProcessFlowEdgeKey(right), "ja");
        });

    const clampedLabelPercent = Math.max(0, Math.min(100, labelPercent));
    const labelLimit = clampedLabelPercent <= 0
        ? 0
        : Math.min(
            sortedEdges.length,
            Math.max(1, Math.ceil(sortedEdges.length * (clampedLabelPercent / 100)))
        );
    const visibleLabelKeys = new Set(
        sortedEdges
            .slice(0, labelLimit)
            .map((edge) => getProcessFlowEdgeKey(edge))
    );

    return {
        visibleLabelKeys,
        visibleLabelCount: visibleLabelKeys.size,
        totalLabelCount: sortedEdges.length,
    };
}

function downloadBlob(blob, fileName) {
    const objectUrl = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = objectUrl;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(objectUrl);
}

function buildProcessMapExportSvg() {
    const svgElement = document.querySelector("#process-map-viewport svg");

    if (!svgElement) {
        return null;
    }

    const clonedSvg = svgElement.cloneNode(true);
    
    // Reset transform for export to ensure it saves at 100% original size
    const exportWrap = clonedSvg.querySelector("g.viewport-wrap");
    if (exportWrap) {
        exportWrap.style.transform = "none";
    }
    
    const viewBox = clonedSvg.getAttribute("viewBox") || "0 0 1200 600";
    const [, , widthValue, heightValue] = viewBox.split(" ").map(Number);
    const exportStyles = `
        .process-map-edge {
            fill: none;
            stroke: #2458d3;
            stroke-linecap: round;
        }
        .process-map-edge--return {
            stroke: #6f83aa;
            stroke-dasharray: 10 8;
        }
        .process-map-edge-label {
            fill: rgba(36, 88, 211, 0.82);
            font-size: 10px;
            font-weight: 700;
            text-anchor: middle;
            paint-order: stroke;
            stroke: #ffffff;
            stroke-width: 4px;
            stroke-linejoin: round;
            font-family: inherit;
        }
        .process-map-edge-label--return {
            fill: rgba(207, 122, 69, 0.88);
        }
        .process-map-node {
            stroke-width: 1.2;
        }
        .process-map-node-label {
            font-size: 14px;
            font-weight: 700;
            font-family: "BIZ UDPGothic", "Yu Gothic UI", sans-serif;
        }
    `;

    clonedSvg.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    clonedSvg.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
    clonedSvg.setAttribute("width", String(widthValue || 1200));
    clonedSvg.setAttribute("height", String(heightValue || 600));

    const backgroundRect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    backgroundRect.setAttribute("x", "0");
    backgroundRect.setAttribute("y", "0");
    backgroundRect.setAttribute("width", "100%");
    backgroundRect.setAttribute("height", "100%");
    backgroundRect.setAttribute("fill", "#f5f7fa"); // Unified with var(--bg)
    clonedSvg.insertBefore(backgroundRect, clonedSvg.firstChild);

    const styleElement = document.createElementNS("http://www.w3.org/2000/svg", "style");
    styleElement.textContent = exportStyles;
    clonedSvg.insertBefore(styleElement, clonedSvg.firstChild);

    return {
        svgText: new XMLSerializer().serializeToString(clonedSvg),
        width: widthValue || 1200,
        height: heightValue || 600,
    };
}

function exportProcessMapSvg(fileName) {
    const exportData = buildProcessMapExportSvg();

    if (!exportData) {
        return;
    }

    downloadBlob(
        new Blob([exportData.svgText], { type: "image/svg+xml;charset=utf-8" }),
        fileName
    );
}

function exportProcessMapPng(fileName) {
    const exportData = buildProcessMapExportSvg();

    if (!exportData) {
        return;
    }

    const image = new Image();
    const svgBlob = new Blob([exportData.svgText], { type: "image/svg+xml;charset=utf-8" });
    const objectUrl = URL.createObjectURL(svgBlob);

    image.onload = () => {
        const scale = 2;
        const canvas = document.createElement("canvas");
        const context = canvas.getContext("2d");

        canvas.width = exportData.width * scale;
        canvas.height = exportData.height * scale;

        if (!context) {
            URL.revokeObjectURL(objectUrl);
            return;
        }

        context.scale(scale, scale);
        context.drawImage(image, 0, 0, exportData.width, exportData.height);
        canvas.toBlob((blob) => {
            if (blob) {
                downloadBlob(blob, fileName);
            }

            URL.revokeObjectURL(objectUrl);
        }, "image/png");
    };

    image.onerror = () => {
        URL.revokeObjectURL(objectUrl);
    };

    image.src = objectUrl;
}

function calculateProcessFlowLayout(nodes, edges, options = {}) {
    if (!nodes.length) return { chartWidth: 0, chartHeight: 0, mainSpineX: 0 };

    const compactMode = Boolean(options.compactMode);
    const chartLeft = 40;
    const chartTop = 60;
    const baseNodeWidth = 140;
    const baseNodeHeight = 44;
    const layerGap = 280;
    const rowGap = compactMode ? 184 : 200;
    const returnRouteBaseOffset = compactMode ? 150 : 210;
    const returnRouteLayerOffset = compactMode ? 24 : 32;
    const nodesByLayer = new Map();

    nodes.forEach((node) => {
        if (!nodesByLayer.has(node.layer)) {
            nodesByLayer.set(node.layer, []);
        }
        nodesByLayer.get(node.layer).push(node);
    });

    const layerKeys = Array.from(nodesByLayer.keys()).sort((a, b) => a - b);
    const maxNodesInLayer = Math.max(...Array.from(nodesByLayer.values()).map(arr => arr.length), 1);
    const svgWidth = Math.max(1460, maxNodesInLayer * layerGap + 400); 
    const mainSpineX = Math.floor(svgWidth / 2) - Math.floor(baseNodeWidth / 2);

    layerKeys.forEach((layerKey) => {
        const layerNodes = nodesByLayer.get(layerKey);
        const spineNodes = layerNodes.filter(n => n.isMainSpine);
        const branchNodes = layerNodes.filter(n => !n.isMainSpine);
        
        let nextLeftOffset = 1;
        let nextRightOffset = 1;
        
        if (spineNodes.length > 0) {
            spineNodes[0].centerX = mainSpineX + (baseNodeWidth / 2);
            spineNodes[0].y = chartTop + layerKey * rowGap;
        }
        
        branchNodes.sort((a, b) => b.weight - a.weight);
        branchNodes.forEach((node, idx) => {
            node.y = chartTop + layerKey * rowGap;
            if (idx % 2 === 0) {
                node.centerX = mainSpineX + (nextRightOffset * layerGap) + (baseNodeWidth / 2);
                nextRightOffset++;
            } else {
                node.centerX = mainSpineX - (nextLeftOffset * layerGap) + (baseNodeWidth / 2);
                node.centerX = Math.max(chartLeft + (baseNodeWidth / 2), node.centerX);
                nextLeftOffset++;
            }
        });
    });

    const maxNodeWeightForSize = Math.max(...nodes.map((node) => node.weight), 1);
    nodes.forEach(node => {
        const scale = Math.sqrt(node.weight / maxNodeWeightForSize);
        node.calcWidth = baseNodeWidth + scale * 80;
        node.calcHeight = baseNodeHeight + scale * 20;
        node.x = node.centerX - (node.calcWidth / 2);
    });

    const nodeLookup = new Map(nodes.map(n => [n.name, n]));
    const maxNodeBottom = Math.max(...nodes.map(n => n.y + n.calcHeight), chartTop + baseNodeHeight);
    
    let maxRouteY = maxNodeBottom;
    edges.forEach(edge => {
        const sourceNode = nodeLookup.get(edge.source);
        const targetNode = nodeLookup.get(edge.target);
        if (sourceNode && targetNode && targetNode.layer <= sourceNode.layer) {
            const startY = sourceNode.y + sourceNode.calcHeight;
            const endY = targetNode.y;
            const routeY = Math.max(startY, endY)
                + returnRouteBaseOffset
                + Math.abs(targetNode.layer - sourceNode.layer) * returnRouteLayerOffset;
            if (routeY > maxRouteY) maxRouteY = routeY;
        }
    });

    const bottomPadding = compactMode ? 36 : 60;
    return {
        chartWidth: svgWidth,
        chartHeight: Math.max(compactMode ? 320 : 400, maxRouteY + bottomPadding),
        mainSpineX: mainSpineX
    };
}

function renderProcessFlowMapFromData(flowData, options = {}) {
    const activityPercent = Number(options.activityPercent ?? 100);
    const connectionPercent = Number(options.connectionPercent ?? 100);
    const labelPercent = Number(options.labelPercent ?? 100);
    const compactMode = Boolean(options.compactMode);
    const filteredData = filterProcessFlowData(
        flowData.nodes,
        flowData.edges,
        activityPercent,
        connectionPercent
    );
    const { nodes, edges } = filteredData;

    if (!nodes.length) {
        return '<p class="empty-state">フロー図を作れるデータがありません。</p>';
    }

    // Reuse pre-calculated layout if available, otherwise calculate once
    const layout = calculateProcessFlowLayout(nodes, edges, { compactMode });
    const { chartWidth, chartHeight, mainSpineX } = layout;
    const layerGap = 280;
    const returnRouteBaseOffset = compactMode ? 150 : 210;
    const returnRouteLayerOffset = compactMode ? 24 : 32;

    const nodeLookup = new Map(nodes.map(n => [n.name, n]));
    const maxEdgeCount = Math.max(...edges.map(e => e.count), 1);
    const maxNodeWeight = Math.max(...nodes.map(n => n.weight), 1);
    const labelState = buildProcessMapLabelState(edges, labelPercent);
    const outgoingEdgeMap = new Map();
    const incomingEdgeMap = new Map();

    edges.forEach(edge => {
        if (!outgoingEdgeMap.has(edge.source)) outgoingEdgeMap.set(edge.source, []);
        if (!incomingEdgeMap.has(edge.target)) incomingEdgeMap.set(edge.target, []);
        outgoingEdgeMap.get(edge.source).push(edge);
        incomingEdgeMap.get(edge.target).push(edge);
    });

    nodes.forEach(node => {
        const outEdges = outgoingEdgeMap.get(node.name) || [];
        const inEdges = incomingEdgeMap.get(node.name) || [];

        outEdges.sort((a, b) => {
            const aX = (nodeLookup.get(a.target) || {x: 0}).x;
            const bX = (nodeLookup.get(b.target) || {x: 0}).x;
            return aX !== bX ? aX - bX : b.count - a.count;
        }).forEach((edge, i) => {
            edge.sourceOffsetX = edge.isMainSpine ? node.x + (node.calcWidth / 2) : node.x + 8 + ((i + 1) * (node.calcWidth - 16)) / (outEdges.length + 1);
        });

        inEdges.sort((a, b) => {
            const aX = (nodeLookup.get(a.source) || {x: 0}).x;
            const bX = (nodeLookup.get(b.source) || {x: 0}).x;
            return aX !== bX ? aX - bX : b.count - a.count;
        }).forEach((edge, i) => {
            edge.targetOffsetX = edge.isMainSpine ? node.x + (node.calcWidth / 2) : node.x + 8 + ((i + 1) * (node.calcWidth - 16)) / (inEdges.length + 1);
        });
    });

    const edgesSvg = edges.map(edge => {
        const s = nodeLookup.get(edge.source);
        const t = nodeLookup.get(edge.target);
        if (!s || !t) return "";

        const isSpine = edge.isMainSpine;
        const isBack = t.layer <= s.layer;
        const edgeWeight = edge.count / maxEdgeCount;
        let opacity, strokeWidth;
        if (isSpine) { opacity = 0.9; strokeWidth = 14; }
        else if (isBack) { opacity = 0.28 + edgeWeight * 0.18; strokeWidth = 2.2 + edgeWeight * 1.4; }
        else {
            opacity = 0.16 + edgeWeight * 0.48;
            strokeWidth = 1.2 + edgeWeight * 8.6;
        }

        const startX = edge.sourceOffsetX, startY = s.y + s.calcHeight;
        const endX = edge.targetOffsetX, endY = t.y;
        let pathD = "", lblX = 0, lblY = 0;

        if (!isBack) {
            if (isSpine) { pathD = `M ${startX} ${startY} L ${endX} ${endY}`; lblX = (startX + endX) / 2 + 10; lblY = (startY + endY) / 2; }
            else {
                const off = Math.max(120, (endY - startY) * 0.5);
                pathD = `M ${startX} ${startY} C ${startX} ${startY + off}, ${endX} ${endY - off}, ${endX} ${endY}`;
                lblX = (startX + endX) / 2 + 5; lblY = (startY + endY) / 2;
            }
        } else {
            const rY = Math.max(startY, endY)
                + returnRouteBaseOffset
                + Math.abs(t.layer - s.layer) * returnRouteLayerOffset;
            const rXOff = s.x >= mainSpineX ? 220 + layerGap : -(220 + layerGap); 
            pathD = `M ${startX} ${startY} C ${startX} ${rY}, ${endX + rXOff} ${rY}, ${endX} ${endY}`;
            lblX = endX + rXOff / 2; lblY = rY - 10;
        }

        const showLabel = labelState.visibleLabelKeys.has(getProcessFlowEdgeKey(edge));
        const transitionKey = buildTransitionKey(edge.source, edge.target);
        const strokeColor = isSpine
            ? "#0a3b8c"
            : isBack
                ? "#6f83aa"
                : "#2d5ec4";
        return `
            <path d="${pathD}" class="${isBack ? "process-map-edge process-map-edge--return" : "process-map-edge"}" marker-end="url(#${isBack ? "process-map-arrow-return" : "process-map-arrow"})" data-source="${escapeHtml(edge.source)}" data-target="${escapeHtml(edge.target)}" data-transition-key="${escapeHtml(transitionKey)}" style="stroke-width: ${strokeWidth}; opacity: ${opacity}; fill: none; stroke: var(--edge-heat-stroke, ${strokeColor}); filter: var(--edge-heat-filter, none);"></path>
            ${showLabel ? `<text x="${lblX}" y="${lblY}" class="${isBack ? "process-map-edge-label process-map-edge-label--return" : "process-map-edge-label"}" data-source="${escapeHtml(edge.source)}" data-target="${escapeHtml(edge.target)}" data-transition-key="${escapeHtml(transitionKey)}">${escapeHtml(edge.count.toLocaleString("ja-JP"))}件</text>` : ""}
        `;
    }).join("");

    const nodesSvg = nodes.map(node => {
        const lines = wrapJapaneseLabel(node.name, 10, 2);
        const isSpine = node.isMainSpine, isStart = node.incoming === 0, isEnd = node.outgoing === 0;
        const ratio = node.weight / maxNodeWeight;
        let fill, stroke, lblCol, strokeW, rx;

        if (isStart) { fill = "rgba(38, 166, 91, 0.9)"; stroke = "#1e8248"; lblCol = "#ffffff"; strokeW = "2.5"; rx = "32"; }
        else if (isEnd) { fill = "rgba(28, 43, 89, 0.9)"; stroke = "#13204a"; lblCol = "#ffffff"; strokeW = "2.5"; rx = "32"; }
        else if (isSpine) { fill = `rgba(18, 55, 148, ${0.4 + ratio * 0.6})`; stroke = "#0a2e7a"; lblCol = "#ffffff"; strokeW = "2.5"; rx = "14"; }
        else {
            fill = `rgba(48, 96, 212, ${0.1 + ratio * 0.5})`;
            stroke = `rgba(35, 75, 176, ${0.3 + ratio * 0.4})`;
            lblCol = ratio >= 0.55 ? "#ffffff" : "#1f335e";
            strokeW = "1.2"; rx = "14";
        }

        const labelSvg = lines.map((line, i) => {
            const yOff = lines.length > 1 ? (i - (lines.length - 1) / 2) * 16 : 0;
            return `<text x="${node.x + node.calcWidth / 2}" y="${node.y + node.calcHeight / 2 + yOff}" class="process-map-node-label" text-anchor="middle" dominant-baseline="middle" alignment-baseline="middle" style="fill: ${lblCol}; pointer-events: none;">${escapeHtml(line)}</text>`;
        }).join("");

        let tooltip = `【${node.name}】\n実行回数: ${node.weight.toLocaleString('ja-JP')}件`;
        if (node.avgDuration !== undefined) tooltip += `\n平均処理時間: ${node.avgDuration.toFixed(1)}分`;

        return `
            <g class="process-map-node-group" data-node="${escapeHtml(node.name)}" style="cursor: pointer;">
                <title>${escapeHtml(tooltip)}</title>
                <rect x="${node.x}" y="${node.y}" width="${node.calcWidth}" height="${node.calcHeight}" rx="${rx}" ry="${rx}" class="process-map-node" data-activity="${escapeHtml(node.name)}" style="fill: ${fill}; stroke: var(--node-heat-stroke, ${stroke}); stroke-width: ${strokeW}; filter: var(--node-heat-filter, url(#drop-shadow));"></rect>
                ${labelSvg}
            </g>
        `;
    }).join("");

    return `
        <div class="process-map-wrap">
            <svg class="process-map-svg" width="${chartWidth}" height="${chartHeight}" viewBox="0 0 ${chartWidth} ${chartHeight}" role="img" aria-label="業務全体フロー図" preserveAspectRatio="xMinYMin meet">
                <defs>
                    <filter id="drop-shadow" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="0" dy="8" stdDeviation="6" flood-color="rgba(10, 20, 40, 0.15)" /></filter>
                    <filter id="drop-shadow-heat-4" x="-25%" y="-25%" width="150%" height="150%">
                        <feDropShadow in="SourceAlpha" dx="0" dy="8" stdDeviation="6" flood-color="rgba(10, 20, 40, 0.15)" result="shadow"></feDropShadow>
                        <feFlood flood-color="#d78b74" flood-opacity="0.11" result="heatFill"></feFlood>
                        <feComposite in="heatFill" in2="SourceGraphic" operator="in" result="innerTint"></feComposite>
                        <feDropShadow in="SourceAlpha" dx="0" dy="0" stdDeviation="2.4" flood-color="rgba(214, 120, 82, 0.32)" result="glow"></feDropShadow>
                        <feMerge>
                            <feMergeNode in="shadow"></feMergeNode>
                            <feMergeNode in="glow"></feMergeNode>
                            <feMergeNode in="innerTint"></feMergeNode>
                            <feMergeNode in="SourceGraphic"></feMergeNode>
                        </feMerge>
                    </filter>
                    <filter id="drop-shadow-heat-5" x="-30%" y="-30%" width="160%" height="160%">
                        <feDropShadow in="SourceAlpha" dx="0" dy="8" stdDeviation="6" flood-color="rgba(10, 20, 40, 0.15)" result="shadow"></feDropShadow>
                        <feFlood flood-color="#d66f5c" flood-opacity="0.14" result="heatFill"></feFlood>
                        <feComposite in="heatFill" in2="SourceGraphic" operator="in" result="innerTint"></feComposite>
                        <feDropShadow in="SourceAlpha" dx="0" dy="0" stdDeviation="3.1" flood-color="rgba(196, 88, 70, 0.38)" result="glow"></feDropShadow>
                        <feMerge>
                            <feMergeNode in="shadow"></feMergeNode>
                            <feMergeNode in="glow"></feMergeNode>
                            <feMergeNode in="innerTint"></feMergeNode>
                            <feMergeNode in="SourceGraphic"></feMergeNode>
                        </feMerge>
                    </filter>
                    <filter id="edge-heat-glow-4" x="-20%" y="-20%" width="140%" height="140%">
                        <feDropShadow dx="0" dy="0" stdDeviation="1.5" flood-color="rgba(208, 118, 78, 0.22)" />
                    </filter>
                    <filter id="edge-heat-glow-5" x="-25%" y="-25%" width="150%" height="150%">
                        <feDropShadow dx="0" dy="0" stdDeviation="2.0" flood-color="rgba(192, 92, 68, 0.28)" />
                    </filter>
                    <marker id="process-map-arrow" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" refX="11" refY="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#2458d3"></path></marker>
                    <marker id="process-map-arrow-return" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" refX="11" refY="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#6f83aa"></path></marker>
                </defs>
                <g class="viewport-wrap">
                    ${edgesSvg}
                    ${nodesSvg}
                </g>
            </svg>
            <div class="process-map-zoom-indicator" style="position: absolute; bottom: 16px; right: 16px; background: rgba(255, 255, 255, 0.9); padding: 4px 10px; border-radius: 4px; font-size: 14px; font-weight: 700; color: #1f335e; box-shadow: 0 2px 6px rgba(0,0,0,0.15); pointer-events: none;">100%</div>
        </div>
    `;
}

function renderProcessFlowMap(patternRows, transitionRows = [], frequencyRows = [], options = {}) {
    const flowData = buildProcessFlowData(patternRows, transitionRows, frequencyRows);
    return renderProcessFlowMapFromData(flowData, options);
}

function renderProcessMapEmpty(message) {
    return `
        <div class="process-map-empty">
            <p>${escapeHtml(message)}</p>
        </div>
    `;
}

function renderProcessHeatLegend() {
    const legendLevels = ["heat-1", "heat-2", "heat-3", "heat-4", "heat-5"];

    return `
        <section class="process-explorer-legend" aria-label="Heatmap legend">
            <div class="process-explorer-control-head">
                <span>Heatmap</span>
                <strong>Avg wait</strong>
            </div>
            <p class="process-explorer-legend-copy">平均待ち時間ベースの Heatmap です。ノードは activity、線は transition の待ち時間を表します。</p>
            <div class="process-explorer-legend-scale">
                <span class="process-explorer-legend-boundary">Low</span>
                <div class="process-explorer-legend-swatches">
                    ${legendLevels.map((levelClassName) => `
                        <div class="process-explorer-legend-item">
                            <span class="process-explorer-legend-node ${levelClassName}"></span>
                            <span class="process-explorer-legend-edge ${levelClassName}"></span>
                        </div>
                    `).join("")}
                </div>
                <span class="process-explorer-legend-boundary">High</span>
            </div>
        </section>
    `;
}

async function initializePatternFlowExplorer(runId) {
    const mapViewport = document.getElementById("process-map-viewport");
    const patternsSlider = document.getElementById("process-map-patterns-slider");
    const activitiesSlider = document.getElementById("process-map-activities-slider");
    const connectionsSlider = document.getElementById("process-map-connections-slider");
    const labelsSlider = document.getElementById("process-map-labels-slider");
    const patternsValue = document.getElementById("process-map-patterns-value");
    const activitiesValue = document.getElementById("process-map-activities-value");
    const connectionsValue = document.getElementById("process-map-connections-value");
    const labelsValue = document.getElementById("process-map-labels-value");
    const patternsMeta = document.getElementById("process-map-patterns-meta");
    const activitiesMeta = document.getElementById("process-map-activities-meta");
    const connectionsMeta = document.getElementById("process-map-connections-meta");
    const labelsMeta = document.getElementById("process-map-labels-meta");
    const exportSvgButton = document.getElementById("process-map-export-svg");
    const exportPngButton = document.getElementById("process-map-export-png");
    const variantList = document.getElementById("variant-list");
    const variantResetButton = document.getElementById("variant-reset-button");
    const variantCoverageMeta = document.getElementById("variant-coverage-meta");
    const variantSelectionTitle = document.getElementById("variant-selection-title");
    const variantSelectionMeta = document.getElementById("variant-selection-meta");
    const variantSelectionSequence = document.getElementById("variant-selection-sequence");
    const activityBottleneckList = document.getElementById("activity-bottleneck-list");
    const transitionBottleneckList = document.getElementById("transition-bottleneck-list");
    const transitionCasePanel = document.getElementById("transition-case-panel");
    const filterForm = document.getElementById("detail-filter-form");
    const filterDateFromInput = document.getElementById("detail-filter-date-from");
    const filterDateToInput = document.getElementById("detail-filter-date-to");
    const filterApplyButton = document.getElementById("detail-filter-apply");
    const filterResetButton = document.getElementById("detail-filter-reset");
    const filterSummaryMeta = document.getElementById("detail-filter-summary");
    const filterCountMeta = document.getElementById("detail-filter-counts");
    const caseTraceForm = document.getElementById("case-trace-form");
    const caseTraceInput = document.getElementById("case-trace-input");
    const caseTraceResult = document.getElementById("case-trace-result");
    const filterSlotRefs = FILTER_SLOT_KEYS.map((slot, index) => ({
        slot,
        labelElement: document.getElementById(`detail-filter-label-${index + 1}`),
        selectElement: document.getElementById(`detail-filter-value-${index + 1}`),
    }));
    let selectedVariantId = null;
    let selectedActivity = "";
    let selectedTransitionKey = "";
    let variants = [];
    let variantCoverage = null;
    let variantErrorMessage = "";
    let bottleneckSummary = null;
    let bottleneckErrorMessage = "";
    let transitionCaseRows = [];
    let transitionCaseErrorMessage = "";
    let searchedCaseId = "";
    let caseTracePayload = null;
    let caseTraceErrorMessage = "";
    let filterDefinitions = buildDefaultFilterDefinitions();
    let defaultAppliedFilters = cloneDetailFilters(activeDetailFilters);
    let filteredCounts = {
        caseCount: 0,
        eventCount: 0,
    };
    const savedFlowSelection = loadFlowSelection(runId);

    if (savedFlowSelection) {
        selectedVariantId = savedFlowSelection.variant_id ?? null;
        selectedActivity = savedFlowSelection.selected_activity || "";
        selectedTransitionKey = savedFlowSelection.selected_transition_key || "";
    }

    if (!runId) {
        if (mapViewport) {
            mapViewport.innerHTML = renderProcessMapEmpty("分析結果が見つかりません。TOP 画面から再度実行してください。");
        }
        return;
    }

    if (
        !mapViewport
        || !patternsSlider
        || !activitiesSlider
        || !connectionsSlider
        || !labelsSlider
        || !variantList
        || !variantResetButton
        || !variantCoverageMeta
        || !variantSelectionTitle
        || !variantSelectionMeta
        || !variantSelectionSequence
        || !activityBottleneckList
        || !transitionBottleneckList
        || !transitionCasePanel
        || !filterForm
        || !filterDateFromInput
        || !filterDateToInput
        || !filterApplyButton
        || !filterResetButton
        || !filterSummaryMeta
        || !filterCountMeta
        || !caseTraceForm
        || !caseTraceInput
        || !caseTraceResult
        || filterSlotRefs.some((filterRef) => !filterRef.labelElement || !filterRef.selectElement)
    ) {
        return;
    }

    let requestVersion = 0;

    function resolveTransitionLabel(transitionItem, transitionKey = "") {
        if (transitionItem) {
            return `${transitionItem.from_activity} → ${transitionItem.to_activity}`;
        }
        return String(transitionKey || "").replace("__TO__", " → ");
    }

    function populateFilterSelect(selectElement, values, selectedValue = "") {
        const optionsHtml = ['<option value="">全て</option>']
            .concat(
                (Array.isArray(values) ? values : []).map((value) => (
                    `<option value="${escapeHtml(value)}"${value === selectedValue ? " selected" : ""}>${escapeHtml(value)}</option>`
                ))
            )
            .join("");

        selectElement.innerHTML = optionsHtml;
    }

    function syncFilterControls() {
        filterDateFromInput.value = activeDetailFilters.date_from || "";
        filterDateToInput.value = activeDetailFilters.date_to || "";
        populateFilterSelect(filterDepartmentSelect, filterOptions.department, activeDetailFilters.department);
        populateFilterSelect(filterChannelSelect, filterOptions.channel, activeDetailFilters.channel);
        populateFilterSelect(filterCategorySelect, filterOptions.category, activeDetailFilters.category);
        filterResetButton.disabled = !hasActiveDetailFilters(activeDetailFilters);
    }

    function renderFilterSummary() {
        filterSummaryMeta.textContent = buildFilterSelectionSummary(activeDetailFilters);
        filterCountMeta.textContent = `対象ケース数 ${Number(filteredCounts.caseCount || 0).toLocaleString("ja-JP")} / 対象イベント数 ${Number(filteredCounts.eventCount || 0).toLocaleString("ja-JP")}`;
    }

    function readFilterFormState() {
        return cloneDetailFilters({
            date_from: filterDateFromInput.value,
            date_to: filterDateToInput.value,
            department: filterDepartmentSelect.value,
            channel: filterChannelSelect.value,
            category: filterCategorySelect.value,
        });
    }

    function syncFilterControls() {
        filterDateFromInput.value = activeDetailFilters.date_from || "";
        filterDateToInput.value = activeDetailFilters.date_to || "";
        filterSlotRefs.forEach((filterRef) => {
            const definition = filterDefinitions.find((item) => item.slot === filterRef.slot) || {
                slot: filterRef.slot,
                label: DEFAULT_FILTER_LABELS[filterRef.slot],
                column_name: "",
                options: [],
            };

            filterRef.labelElement.textContent = definition.label;
            populateFilterSelect(filterRef.selectElement, definition.options, activeDetailFilters[filterRef.slot]);
            filterRef.selectElement.disabled = !definition.column_name;
        });
        filterResetButton.disabled = !hasActiveDetailFilters(activeDetailFilters);
    }

    function renderFilterSummary() {
        filterSummaryMeta.textContent = buildFilterSelectionSummary(activeDetailFilters, filterDefinitions);
        filterCountMeta.textContent = `対象ケース数 ${Number(filteredCounts.caseCount || 0).toLocaleString("ja-JP")} / 対象イベント数 ${Number(filteredCounts.eventCount || 0).toLocaleString("ja-JP")}`;
    }

    function readFilterFormState() {
        return cloneDetailFilters({
            date_from: filterDateFromInput.value,
            date_to: filterDateToInput.value,
            filter_value_1: filterSlotRefs[0].selectElement.value,
            filter_value_2: filterSlotRefs[1].selectElement.value,
            filter_value_3: filterSlotRefs[2].selectElement.value,
        });
    }

    function resetSelectionState() {
        selectedVariantId = null;
        selectedActivity = "";
        selectedTransitionKey = "";
        transitionCaseRows = [];
        transitionCaseErrorMessage = "";
        saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
    }

    async function applyFilters(nextFilters) {
        filterApplyButton.disabled = true;
        filterResetButton.disabled = true;
        activeDetailFilters = cloneDetailFilters(nextFilters);
        resetSelectionState();
        syncFilterControls();
        renderFilterSummary();

        try {
            await refreshVariantSummary();
            await refreshBottleneckSummary();
            syncVariantPanel();
            syncBottleneckPanel();
            if (typeof detailPageAnalysisLoader === "function") {
                await detailPageAnalysisLoader(0);
            }
            await updateProcessMap();
        } finally {
            filterApplyButton.disabled = false;
            syncFilterControls();
        }
    }

    function renderTransitionCasePanel() {
        const hasTransitionSelection = Boolean(selectedTransitionKey);
        transitionCasePanel.className = "result-panel";

        if (!hasTransitionSelection) {
            const hasActivitySelection = Boolean(selectedActivity);
            const emptyMessage = hasActivitySelection
                ? `Activity ボトルネック「${selectedActivity}」を選択中です。遷移別ボトルネックを選択するとケース一覧を表示します。`
                : "遷移別ボトルネックを選択すると、時間の長いケースを表示します。";
            transitionCasePanel.innerHTML = `
                <div class="result-header">
                    <div>
                        <h2>Transition Case Drilldown</h2>
                        <p class="result-meta">${escapeHtml(emptyMessage)}</p>
                    </div>
                </div>
                <p class="empty-state">${escapeHtml(emptyMessage)}</p>
            `;
            return;
        }

        const selectedTransition = (bottleneckSummary?.transition_bottlenecks || []).find((item) => {
            const transitionKey = item.transition_key || buildTransitionKey(item.from_activity, item.to_activity);
            return transitionKey === selectedTransitionKey;
        });
        const transitionLabel = resolveTransitionLabel(selectedTransition, selectedTransitionKey);

        if (transitionCaseErrorMessage) {
            transitionCasePanel.innerHTML = `
                <div class="result-header">
                    <div>
                        <h2>Transition Case Drilldown</h2>
                        <p class="result-meta">${escapeHtml(transitionLabel)}</p>
                    </div>
                </div>
                <p class="empty-state">${escapeHtml(transitionCaseErrorMessage)}</p>
            `;
            return;
        }

        transitionCasePanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>Transition Case Drilldown</h2>
                    <p class="result-meta">${escapeHtml(transitionLabel)}</p>
                </div>
            </div>
            <p class="panel-note">上位 20 件を表示します。duration 降順です。</p>
            ${buildCaseDrilldownTable(transitionCaseRows)}
        `;
    }

    async function loadSelectedTransitionCases() {
        if (!selectedTransitionKey) {
            transitionCaseRows = [];
            transitionCaseErrorMessage = "";
            renderTransitionCasePanel();
            return;
        }

        const selectedTransition = (bottleneckSummary?.transition_bottlenecks || []).find((item) => {
            const transitionKey = item.transition_key || buildTransitionKey(item.from_activity, item.to_activity);
            return transitionKey === selectedTransitionKey;
        });

        if (!selectedTransition) {
            transitionCaseRows = [];
            transitionCaseErrorMessage = "Transition details are not available.";
            renderTransitionCasePanel();
            return;
        }

        transitionCaseRows = [];
        transitionCaseErrorMessage = "";
        transitionCasePanel.className = "result-panel";
        transitionCasePanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>Transition Case Drilldown</h2>
                    <p class="result-meta">${escapeHtml(resolveTransitionLabel(selectedTransition))}</p>
                </div>
            </div>
            <p class="panel-note">読み込み中...</p>
        `;

        try {
            const payload = await loadTransitionCases(
                runId,
                selectedTransition.from_activity,
                selectedTransition.to_activity,
                20,
                selectedVariantId,
                activeDetailFilters,
            );
            transitionCaseRows = Array.isArray(payload.cases) ? payload.cases : [];
        } catch (error) {
            transitionCaseErrorMessage = error.message;
        }

        renderTransitionCasePanel();
    }

    function renderCaseTracePanel() {
        if (!searchedCaseId) {
            caseTraceResult.innerHTML = '<p class="empty-state">Case ID を入力すると、ケースの通過順序と待ち時間を表示します。</p>';
            return;
        }

        if (caseTraceErrorMessage) {
            caseTraceResult.innerHTML = `<p class="empty-state">${escapeHtml(caseTraceErrorMessage)}</p>`;
            return;
        }

        if (!caseTracePayload) {
            caseTraceResult.innerHTML = '<p class="panel-note">読み込み中...</p>';
            return;
        }

        if (!caseTracePayload.found) {
            caseTraceResult.innerHTML = `<p class="empty-state">Case ID「${escapeHtml(searchedCaseId)}」は見つかりませんでした。</p>`;
            return;
        }

        caseTraceResult.innerHTML = `
            ${buildCaseTraceSummaryHtml(caseTracePayload.case_id, caseTracePayload.summary)}
            <p class="panel-note">run 全体から検索したケース履歴です。時刻順にイベントを表示しています。</p>
            ${buildCaseTraceEventsTable(caseTracePayload.events || [])}
        `;
    }

    async function searchCaseTrace(caseId) {
        const normalizedCaseId = String(caseId || "").trim();
        caseTraceInput.value = normalizedCaseId;
        searchedCaseId = normalizedCaseId;
        caseTracePayload = null;
        caseTraceErrorMessage = "";

        if (!normalizedCaseId) {
            renderCaseTracePanel();
            return;
        }

        // Keep case lookup stable even when a variant is selected.
        renderCaseTracePanel();

        try {
            caseTracePayload = await loadCaseTrace(runId, normalizedCaseId);
        } catch (error) {
            caseTraceErrorMessage = error.message;
        }

        renderCaseTracePanel();
    }

    async function refreshBottleneckSummary() {
        try {
            bottleneckErrorMessage = "";
            bottleneckSummary = await loadBottleneckSummary(runId, 5, selectedVariantId, activeDetailFilters);
            filteredCounts = {
                caseCount: Number(bottleneckSummary?.filtered_case_count || 0),
                eventCount: Number(bottleneckSummary?.filtered_event_count || 0),
            };
            renderFilterSummary();
            if (selectedActivity && !(bottleneckSummary.activity_bottlenecks || []).some((item) => item.activity === selectedActivity)) {
                selectedActivity = "";
            }
            if (selectedTransitionKey && !(bottleneckSummary.transition_bottlenecks || []).some((item) => {
                const transitionKey = item.transition_key || buildTransitionKey(item.from_activity, item.to_activity);
                return transitionKey === selectedTransitionKey;
            })) {
                selectedTransitionKey = "";
                transitionCaseRows = [];
                transitionCaseErrorMessage = "";
            }
            saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
        } catch (error) {
            bottleneckSummary = null;
            bottleneckErrorMessage = error.message;
            filteredCounts = {
                caseCount: 0,
                eventCount: 0,
            };
            renderFilterSummary();
        }
    }

    async function refreshVariantSummary() {
        try {
            variantErrorMessage = "";
            const variantPayload = await loadVariantList(runId, 10, activeDetailFilters);
            variants = Array.isArray(variantPayload.variants) ? variantPayload.variants : [];
            variantCoverage = variantPayload.coverage || null;
            filteredCounts = {
                caseCount: Number(variantPayload.filtered_case_count || 0),
                eventCount: Number(variantPayload.filtered_event_count || 0),
            };
            renderFilterSummary();

            if (selectedVariantId !== null && !variants.some((variant) => Number(variant.variant_id) === Number(selectedVariantId))) {
                selectedVariantId = null;
                selectedActivity = "";
                selectedTransitionKey = "";
                transitionCaseRows = [];
                transitionCaseErrorMessage = "";
                saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
            }
        } catch (error) {
            variantErrorMessage = error.message;
            variants = [];
            variantCoverage = null;
            filteredCounts = {
                caseCount: 0,
                eventCount: 0,
            };
            renderFilterSummary();
        }
    }

    function syncBottleneckPanel() {
        if (bottleneckErrorMessage) {
            activityBottleneckList.innerHTML = `<p class="empty-state">${escapeHtml(bottleneckErrorMessage)}</p>`;
            transitionBottleneckList.innerHTML = `<p class="empty-state">${escapeHtml(bottleneckErrorMessage)}</p>`;
            renderTransitionCasePanel();
            return;
        }

        activityBottleneckList.innerHTML = buildBottleneckCardsHtml(
            bottleneckSummary?.activity_bottlenecks || [],
            "activity",
            { selectedActivity, selectedTransitionKey }
        );
        transitionBottleneckList.innerHTML = buildBottleneckCardsHtml(
            bottleneckSummary?.transition_bottlenecks || [],
            "transition",
            { selectedActivity, selectedTransitionKey }
        );

        activityBottleneckList.querySelectorAll("[data-bottleneck-kind='activity']").forEach((buttonElement) => {
            buttonElement.addEventListener("click", async () => {
                const activityName = buttonElement.dataset.activity || "";
                selectedActivity = selectedActivity === activityName ? "" : activityName;
                selectedTransitionKey = "";
                transitionCaseRows = [];
                transitionCaseErrorMessage = "";
                saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
                syncBottleneckPanel();
                await updateProcessMap();
                renderTransitionCasePanel();
            });
        });

        transitionBottleneckList.querySelectorAll("[data-bottleneck-kind='transition']").forEach((buttonElement) => {
            buttonElement.addEventListener("click", async () => {
                const transitionKey = buttonElement.dataset.transitionKey || "";
                selectedTransitionKey = selectedTransitionKey === transitionKey ? "" : transitionKey;
                selectedActivity = "";
                saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
                syncBottleneckPanel();
                await updateProcessMap();
                await loadSelectedTransitionCases();
            });
        });

        renderTransitionCasePanel();
    }

    function syncVariantPanel() {
        if (variantErrorMessage) {
            variantList.innerHTML = `<p class="empty-state">${escapeHtml(variantErrorMessage)}</p>`;
            variantCoverageMeta.innerHTML = '<p class="panel-note">Coverage could not be loaded.</p>';
            variantSelectionTitle.textContent = "Variant data is unavailable";
            variantSelectionMeta.textContent = "Variant list could not be loaded.";
            variantSelectionSequence.textContent = "";
            variantSelectionSequence.title = "";
            variantResetButton.disabled = true;
            patternsSlider.disabled = false;
            activitiesSlider.disabled = false;
            connectionsSlider.disabled = false;
            return;
        }

        variantList.innerHTML = buildVariantCardsHtml(variants, selectedVariantId);
        variantResetButton.disabled = selectedVariantId === null;
        patternsSlider.disabled = selectedVariantId !== null;
        activitiesSlider.disabled = selectedVariantId !== null;
        connectionsSlider.disabled = selectedVariantId !== null;
        variantCoverageMeta.innerHTML = buildVariantCoverageHtml(variantCoverage);

        const selectionState = buildVariantSelectionState(variants, selectedVariantId);
        variantSelectionTitle.textContent = selectionState.title;
        variantSelectionMeta.textContent = selectionState.meta;
        variantSelectionSequence.textContent = selectionState.sequence;
        variantSelectionSequence.title = selectionState.titleAttribute;

        variantList.querySelectorAll("[data-variant-id]").forEach((buttonElement) => {
            buttonElement.addEventListener("click", async () => {
                const clickedVariantId = Number(buttonElement.dataset.variantId);
                const nextVariantId = selectedVariantId === clickedVariantId
                    ? null
                    : clickedVariantId;
                await applyVariantSelection(nextVariantId);
            });
        });
    }

    async function applyVariantSelection(nextVariantId) {
        selectedVariantId = nextVariantId;
        selectedActivity = "";
        selectedTransitionKey = "";
        transitionCaseRows = [];
        transitionCaseErrorMessage = "";
        saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
        await refreshBottleneckSummary();
        syncVariantPanel();
        syncBottleneckPanel();
        await updateProcessMap();
    }

    async function updateProcessMap() {
        const currentVersion = requestVersion + 1;
        requestVersion = currentVersion;
        const patternPercent = Number(patternsSlider.value);
        const activityPercent = Number(activitiesSlider.value);
        const connectionPercent = Number(connectionsSlider.value);
        const labelPercent = Number(labelsSlider.value);

        // Update labels instantly
        patternsValue.textContent = `${patternPercent}%`;
        activitiesValue.textContent = `${activityPercent}%`;
        connectionsValue.textContent = `${connectionPercent}%`;
        labelsValue.textContent = `${labelPercent}%`;

        mapViewport.innerHTML = renderProcessMapEmpty("フロー図を読み込んでいます...");

        try {
            const params = new URLSearchParams({
                pattern_percent: String(patternPercent),
                activity_percent: String(activityPercent),
                connection_percent: String(connectionPercent),
            });
            if (selectedVariantId !== null) {
                params.set("variant_id", String(selectedVariantId));
            }
            buildFilterQueryParams(activeDetailFilters).forEach((value, key) => {
                params.set(key, value);
            });

            const snapshot = await fetchJson(
                `/api/runs/${encodeURIComponent(runId)}/pattern-flow?${params.toString()}`,
                "処理フロー図の読み込みに失敗しました。"
            );

            if (currentVersion !== requestVersion) {
                return;
            }

            const flowData = snapshot.flow_data || { nodes: [], edges: [] };
            const labelState = buildProcessMapLabelState(flowData.edges || [], labelPercent);
            filteredCounts = {
                caseCount: Number(snapshot.filtered_case_count || 0),
                eventCount: Number(snapshot.filtered_event_count || 0),
            };
            renderFilterSummary();

            if (snapshot.selected_variant) {
                patternsMeta.textContent = `Variant #${snapshot.selected_variant.variant_id} / 1 pattern`;
            } else {
                patternsMeta.textContent = `${snapshot.pattern_window.used_pattern_count} / ${snapshot.pattern_window.effective_pattern_count} patterns`;
            }
            activitiesMeta.textContent = `${snapshot.activity_window.visible_activity_count} / ${snapshot.activity_window.available_activity_count} activities`;
            connectionsMeta.textContent = `${snapshot.connection_window.visible_connection_count} / ${snapshot.connection_window.available_connection_count} connections`;
            labelsMeta.textContent = `${labelState.visibleLabelCount} / ${labelState.totalLabelCount} labels`;

            if (!flowData.nodes.length) {
                mapViewport.innerHTML = renderProcessMapEmpty("表示できるフロー図がありません。表示率を広げてください。");
                return;
            }

            const edgeCount = flowData.edges.length;
            const totalToRender = flowData.nodes.length + edgeCount;
            if (totalToRender > AGGRESSIVE_LIMIT) {
                mapViewport.innerHTML = renderProcessMapEmpty(
                    `図が複雑すぎるため（要素数: ${totalToRender.toLocaleString()}）、現在の表示率では描画を停止しました。スライダーを下げてください。`
                );
                return;
            }

            if (totalToRender > RENDERING_LIMIT || edgeCount > EDGE_LIMIT) {
                const reason = totalToRender > RENDERING_LIMIT
                    ? `要素数: ${totalToRender.toLocaleString()}`
                    : `線の数: ${edgeCount.toLocaleString()}`;
                mapViewport.innerHTML = renderProcessMapEmpty(
                    `図が複雑すぎるため（${reason}）、ブラウザのフリーズを防ぐために描画を停止しました。スライダーで表示率を下げてください。`
                );
                return;
            }

            mapViewport.innerHTML = renderProcessFlowMapFromData(flowData, {
                labelPercent,
                activityPercent: 100,
                connectionPercent: 100,
                compactMode: selectedVariantId !== null,
            });
            applyProcessMapDecorators(mapViewport, {
                activityHeatmap: bottleneckSummary?.activity_heatmap || {},
                transitionHeatmap: bottleneckSummary?.transition_heatmap || {},
                selectedActivity,
                selectedTransitionKey,
            });
            attachProcessMapInteractions(mapViewport);
        } catch (error) {
            if (currentVersion !== requestVersion) {
                return;
            }
            patternsMeta.textContent = "";
            activitiesMeta.textContent = "";
            connectionsMeta.textContent = "";
            labelsMeta.textContent = "";
            filteredCounts = {
                caseCount: 0,
                eventCount: 0,
            };
            renderFilterSummary();
            mapViewport.innerHTML = renderProcessMapEmpty(error.message);
        }
    }

    const debouncedUpdate = debounce(updateProcessMap, 300);

    patternsSlider.addEventListener("input", () => {
        patternsValue.textContent = `${patternsSlider.value}%`;
        debouncedUpdate();
    });
    activitiesSlider.addEventListener("input", () => {
        activitiesValue.textContent = `${activitiesSlider.value}%`;
        debouncedUpdate();
    });
    connectionsSlider.addEventListener("input", () => {
        connectionsValue.textContent = `${connectionsSlider.value}%`;
        debouncedUpdate();
    });
    labelsSlider.addEventListener("input", () => {
        labelsValue.textContent = `${labelsSlider.value}%`;
        debouncedUpdate();
    });
    
    // We also want real-time label change without a network request ideally, but 'input' event 
    // for all might cause lag. Use 'change' instead.
    
    if (exportSvgButton) {
        exportSvgButton.addEventListener("click", () => {
            exportProcessMapSvg(
                `process_flow_map_${patternsSlider.value}_${activitiesSlider.value}_${connectionsSlider.value}_${labelsSlider.value}.svg`
            );
        });
    }
    if (exportPngButton) {
        exportPngButton.addEventListener("click", () => {
            exportProcessMapPng(
                `process_flow_map_${patternsSlider.value}_${activitiesSlider.value}_${connectionsSlider.value}_${labelsSlider.value}.png`
            );
        });
    }

    caseTraceForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        await searchCaseTrace(caseTraceInput.value);
    });

    variantResetButton.addEventListener("click", () => {
        void applyVariantSelection(null);
    });

    filterForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        await applyFilters(readFilterFormState());
    });

    filterResetButton.addEventListener("click", async (event) => {
        event.preventDefault();
        await applyFilters(defaultAppliedFilters);
    });

    try {
        const filterOptionsPayload = await loadFilterOptions(runId);
        filterDefinitions = normalizeFilterDefinitions(
            filterOptionsPayload.options?.filters,
            filterOptionsPayload.column_settings,
        );
        defaultAppliedFilters = cloneDetailFilters(filterOptionsPayload.applied_filters || activeDetailFilters);
        activeDetailFilters = cloneDetailFilters(filterOptionsPayload.applied_filters || activeDetailFilters);
    } catch (error) {
        filterDefinitions = buildDefaultFilterDefinitions();
        defaultAppliedFilters = cloneDetailFilters(activeDetailFilters);
    }

    syncFilterControls();
    renderFilterSummary();

    try {
        const variantPayload = await loadVariantList(runId, 10, activeDetailFilters);
        variants = Array.isArray(variantPayload.variants) ? variantPayload.variants : [];
        variantCoverage = variantPayload.coverage || null;
        filteredCounts = {
            caseCount: Number(variantPayload.filtered_case_count || 0),
            eventCount: Number(variantPayload.filtered_event_count || 0),
        };
        renderFilterSummary();
        if (selectedVariantId !== null && !variants.some((variant) => Number(variant.variant_id) === Number(selectedVariantId))) {
            selectedVariantId = null;
            selectedActivity = "";
            selectedTransitionKey = "";
            saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
        }
    } catch (error) {
        variantList.innerHTML = `<p class="empty-state">${escapeHtml(error.message)}</p>`;
        variantCoverageMeta.innerHTML = '<p class="panel-note">Coverage を取得できませんでした。</p>';
        variantSelectionTitle.textContent = "取得失敗";
        variantSelectionMeta.textContent = "Variant 一覧の読み込みに失敗しました。";
        variantSelectionSequence.textContent = "";
    }

    await refreshVariantSummary();
    await refreshBottleneckSummary();

    syncVariantPanel();
    syncBottleneckPanel();
    renderCaseTracePanel();
    if (selectedTransitionKey) {
        await loadSelectedTransitionCases();
    }
    await updateProcessMap();
}

function attachProcessMapInteractions(viewportElement) {
    const svgElement = viewportElement.querySelector("svg.process-map-svg");
    const zoomIndicator = viewportElement.querySelector(".process-map-zoom-indicator");
    if (!svgElement) return;

    let isDragging = false;
    let startPanX = 0;
    let startPanY = 0;
    
    // Check if we already have a transform applied
    let currentScale = 1;
    let currentPanX = 0;
    let currentPanY = 0;

    // We need a wrapper inside the SVG to apply transforms, or apply to root
    const rootMatrix = svgElement.createSVGMatrix();

    svgElement.style.cursor = "grab";

    svgElement.addEventListener("mousedown", (e) => {
        isDragging = true;
        svgElement.style.cursor = "grabbing";
        startPanX = e.clientX - currentPanX;
        startPanY = e.clientY - currentPanY;
        e.preventDefault();
    });

    window.addEventListener("mousemove", (e) => {
        if (!isDragging) return;
        currentPanX = e.clientX - startPanX;
        currentPanY = e.clientY - startPanY;
        applyTransform();
    });

    window.addEventListener("mouseup", () => {
        isDragging = false;
        if(svgElement) svgElement.style.cursor = "grab";
    });

    svgElement.addEventListener("wheel", (e) => {
        e.preventDefault();
        
        const zoomIntensity = 0.1;
        const delta = e.deltaY > 0 ? -zoomIntensity : zoomIntensity;
        
        // Calculate new scale
        let newScale = currentScale * (1 + delta);
        newScale = Math.max(0.1, Math.min(newScale, 5)); // Limit zoom
        
        // Zoom towards cursor
        const rect = svgElement.getBoundingClientRect();
        const cursorX = e.clientX - rect.left;
        const cursorY = e.clientY - rect.top;
        
        currentPanX = cursorX - (cursorX - currentPanX) * (newScale / currentScale);
        currentPanY = cursorY - (cursorY - currentPanY) * (newScale / currentScale);
        
        currentScale = newScale;
        applyTransform();
    }, { passive: false });
    
    // Set up click-to-focus highlighting
    svgElement.addEventListener("click", (e) => {
        // If they were just dragging, ignore the click
        if (isDragging) return;

        const nodeGroup = e.target.closest(".process-map-node-group");
        
        if (!nodeGroup) {
            // Clicked background, clear focus
            svgElement.classList.remove("is-focusing");
            svgElement.querySelectorAll(".is-focused").forEach(el => el.classList.remove("is-focused"));
            return;
        }

        const focusedNodeName = nodeGroup.getAttribute("data-node");
        if (!focusedNodeName) return;

        // Toggle focus off if clicking the already focused node
        if (nodeGroup.classList.contains("is-focused") && svgElement.classList.contains("is-focusing")) {
            svgElement.classList.remove("is-focusing");
            svgElement.querySelectorAll(".is-focused").forEach(el => el.classList.remove("is-focused"));
            return;
        }

        // Apply focusing state
        svgElement.classList.add("is-focusing");
        svgElement.querySelectorAll(".is-focused").forEach(el => el.classList.remove("is-focused"));

        // Focus the clicked node
        nodeGroup.classList.add("is-focused");

        // Find and focus connected edges and their other connected nodes
        const edges = svgElement.querySelectorAll(".process-map-edge, .process-map-edge-label");
        const connectedNodes = new Set();
        
        edges.forEach(edge => {
            const source = edge.getAttribute("data-source");
            const target = edge.getAttribute("data-target");

            if (source === focusedNodeName || target === focusedNodeName) {
                edge.classList.add("is-focused");
                connectedNodes.add(source);
                connectedNodes.add(target);
            }
        });

        // Focus the connected nodes
        const allNodes = svgElement.querySelectorAll(".process-map-node-group");
        allNodes.forEach(n => {
            if (connectedNodes.has(n.getAttribute("data-node"))) {
                n.classList.add("is-focused");
            }
        });
    });
    
    function applyTransform() {
        const gWrap = svgElement.querySelector(".viewport-wrap");
        if (!gWrap) return;
        
        const transformString = `translate(${currentPanX}px, ${currentPanY}px) scale(${currentScale})`;
        gWrap.style.transform = transformString;
        gWrap.style.transformOrigin = "0 0";
        
        if (zoomIndicator) {
            zoomIndicator.textContent = Math.round(currentScale * 100) + "%";
        }
    }
    
    // Apply initial transform to wrap properly
    applyTransform();
}

function renderPatternChart(analysis, runId) {
    const initialFlowSettings = getInitialPatternFlowSettings(analysis.row_count ?? analysis.rows.length);

    if (!analysis.rows.length) {
        chartPanel.className = "result-panel";
        chartTitle.textContent = "処理順パターン図";
        chartNote.textContent = "表示できるデータがありません。";
        chartContainer.innerHTML = '<p class="empty-state">表示できるデータがありません。</p>';
        return;
    }

    chartPanel.className = "result-panel";
    chartTitle.textContent = "業務全体フロー図";
    chartNote.textContent = "大量データでは初期表示を自動で絞っています。Variant を選ぶと、そのVariantに属するケースだけでフロー図を描き直します。";
    chartContainer.innerHTML = `
        <section class="detail-filter-panel">
            <div class="result-header">
                <div>
                    <h2>フィルタ</h2>
                    <p class="result-meta">期間と属性で絞り込み、フロー図と分析結果を再計算します。</p>
                </div>
            </div>
            <form id="detail-filter-form" class="detail-filter-form">
                <label class="detail-filter-field">
                    <span>開始日</span>
                    <input id="detail-filter-date-from" type="date" name="date_from">
                </label>
                <label class="detail-filter-field">
                    <span>終了日</span>
                    <input id="detail-filter-date-to" type="date" name="date_to">
                </label>
                <label class="detail-filter-field">
                    <span>部署</span>
                    <select id="detail-filter-department" name="department">
                        <option value="">全て</option>
                    </select>
                </label>
                <label class="detail-filter-field">
                    <span>チャネル</span>
                    <select id="detail-filter-channel" name="channel">
                        <option value="">全て</option>
                    </select>
                </label>
                <label class="detail-filter-field">
                    <span>カテゴリ</span>
                    <select id="detail-filter-category" name="category">
                        <option value="">全て</option>
                    </select>
                </label>
                <div class="detail-filter-actions">
                    <button id="detail-filter-apply" type="submit" class="detail-link process-explorer-button process-explorer-button--primary">適用</button>
                    <button id="detail-filter-reset" type="button" class="ghost-link process-explorer-button">リセット</button>
                </div>
            </form>
            <div class="detail-filter-meta">
                <p id="detail-filter-summary" class="panel-note">フィルタ未適用</p>
                <p id="detail-filter-counts" class="panel-note">対象ケース数 0 / 対象イベント数 0</p>
            </div>
        </section>
        <div class="process-explorer-shell">
            <div class="process-explorer-map-panel">
                <div id="process-map-viewport" class="process-map-viewport"></div>
            </div>
            <aside class="process-explorer-sidebar">
                <div class="process-explorer-export">
                    <button id="process-map-export-svg" type="button" class="detail-link process-explorer-button">SVG保存</button>
                    <button id="process-map-export-png" type="button" class="detail-link process-explorer-button">PNG保存</button>
                </div>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Patterns</span>
                        <strong id="process-map-patterns-value">${initialFlowSettings.patterns}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-patterns-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="10"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.patterns}"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-patterns-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Activities</span>
                        <strong id="process-map-activities-value">${initialFlowSettings.activities}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-activities-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="10"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.activities}"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-activities-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Connections</span>
                        <strong id="process-map-connections-value">${initialFlowSettings.connections}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-connections-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="10"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.connections}"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-connections-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Labels</span>
                        <strong id="process-map-labels-value">${initialFlowSettings.labels}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-labels-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="0"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.labels}"
                        >
                        <span class="process-explorer-slider-bottom">0%</span>
                    </div>
                    <p id="process-map-labels-meta" class="process-explorer-meta"></p>
                </section>
                ${renderProcessHeatLegend()}
            </aside>
        </div>
        <section class="variant-panel">
            <div class="result-header variant-panel-header">
                <div>
                    <h3>Variant Analysis</h3>
                    <p class="result-meta">Top 10 Variant を表示します。クリックすると対象 Variant のケースだけでフロー図を更新します。</p>
                </div>
                <button id="variant-reset-button" type="button" class="ghost-link process-explorer-button">全体表示</button>
            </div>
            <div class="variant-panel-summary">
                <article id="variant-coverage-meta" class="variant-coverage-card">
                    <span class="variant-coverage-label">Coverage</span>
                    <strong class="variant-coverage-value">計算中...</strong>
                </article>
                <article class="variant-selection-card">
                    <p id="variant-selection-title" class="variant-selection-title">全体表示中</p>
                    <p id="variant-selection-meta" class="panel-note">Variant を選択すると、その Variant に属するケースだけでフロー図を再描画します。</p>
                    <p id="variant-selection-sequence" class="variant-selection-sequence">現在は全ケースを使ったフロー図を表示しています。</p>
                </article>
            </div>
            <div id="variant-list" class="variant-list"></div>
        </section>
        <section class="bottleneck-panel">
            <div class="result-header variant-panel-header">
                <div>
                    <h3>Bottleneck Analysis</h3>
                    <p class="result-meta">Top 5 waits ranked by average time to the next event.</p>
                </div>
            </div>
            <div class="bottleneck-grid">
                <article class="bottleneck-group">
                    <div class="bottleneck-group-head">
                        <h4>Activity Bottlenecks</h4>
                        <p class="panel-note">Average wait by activity.</p>
                    </div>
                    <div id="activity-bottleneck-list" class="bottleneck-list">
                        <p class="panel-note">Loading...</p>
                    </div>
                </article>
                <article class="bottleneck-group">
                    <div class="bottleneck-group-head">
                        <h4>Transition Bottlenecks</h4>
                        <p class="panel-note">Average wait by transition.</p>
                    </div>
                    <div id="transition-bottleneck-list" class="bottleneck-list">
                        <p class="panel-note">Loading...</p>
                    </div>
                </article>
            </div>
        </section>
        <section id="transition-case-panel" class="result-panel">
            <div class="result-header">
                <div>
                    <h2>Transition Case Drilldown</h2>
                    <p class="result-meta">Select a transition bottleneck to inspect the slowest cases.</p>
                </div>
            </div>
            <p class="empty-state">No transition selected.</p>
        </section>
        <section id="case-trace-panel" class="result-panel">
            <div class="result-header">
                <div>
                    <h2>Case ID 検索 / ケース追跡</h2>
                    <p class="result-meta">run 全体から Case ID を検索し、通過イベントと待ち時間を確認します。</p>
                </div>
            </div>
            <form id="case-trace-form" class="case-trace-form">
                <input
                    id="case-trace-input"
                    class="case-trace-input"
                    type="text"
                    name="case_id"
                    placeholder="Case ID を入力"
                    autocomplete="off"
                    spellcheck="false"
                >
                <button type="submit" class="detail-link process-explorer-button process-explorer-button--primary">検索</button>
            </form>
            <div id="case-trace-result" class="case-trace-result">
                <p class="empty-state">Case ID を入力すると、ケースの通過順序と待ち時間を表示します。</p>
            </div>
        </section>
    `;
    return initializePatternFlowExplorer(runId);
}

function renderPatternChart(analysis, runId) {
    const initialFlowSettings = getInitialPatternFlowSettings(analysis.row_count ?? analysis.rows.length);

    if (!analysis.rows.length) {
        chartPanel.className = "result-panel";
        chartTitle.textContent = "業務全体フロー図";
        chartNote.textContent = "表示できるデータがありません。";
        chartContainer.innerHTML = '<p class="empty-state">表示できるデータがありません。</p>';
        return;
    }

    chartPanel.className = "result-panel";
    chartTitle.textContent = "業務全体フロー図";
    chartNote.textContent = "大きなデータでは初期表示を自動で絞っています。Variant を選ぶと、その Variant に属するケースだけでフロー図を再描画します。";
    chartContainer.innerHTML = `
        <section class="detail-filter-panel">
            <div class="result-header">
                <div>
                    <h2>フィルタ</h2>
                    <p class="result-meta">期間と任意のグループ/カテゴリー条件で絞り込み、フロー図と分析結果を再計算します。</p>
                </div>
            </div>
            <form id="detail-filter-form" class="detail-filter-form">
                <label class="detail-filter-field">
                    <span>開始日</span>
                    <input id="detail-filter-date-from" type="date" name="date_from">
                </label>
                <label class="detail-filter-field">
                    <span>終了日</span>
                    <input id="detail-filter-date-to" type="date" name="date_to">
                </label>
                <label class="detail-filter-field">
                    <span id="detail-filter-label-1">グループ/カテゴリー フィルター①</span>
                    <select id="detail-filter-value-1" name="filter_value_1">
                        <option value="">全て</option>
                    </select>
                </label>
                <label class="detail-filter-field">
                    <span id="detail-filter-label-2">グループ/カテゴリー フィルター②</span>
                    <select id="detail-filter-value-2" name="filter_value_2">
                        <option value="">全て</option>
                    </select>
                </label>
                <label class="detail-filter-field">
                    <span id="detail-filter-label-3">グループ/カテゴリー フィルター③</span>
                    <select id="detail-filter-value-3" name="filter_value_3">
                        <option value="">全て</option>
                    </select>
                </label>
                <div class="detail-filter-actions">
                    <button id="detail-filter-apply" type="submit" class="detail-link process-explorer-button process-explorer-button--primary">適用</button>
                    <button id="detail-filter-reset" type="button" class="ghost-link process-explorer-button">リセット</button>
                </div>
            </form>
            <div class="detail-filter-meta">
                <p id="detail-filter-summary" class="panel-note">フィルタ未適用</p>
                <p id="detail-filter-counts" class="panel-note">対象ケース数 0 / 対象イベント数 0</p>
            </div>
        </section>
        <div class="process-explorer-shell">
            <div class="process-explorer-map-panel">
                <div id="process-map-viewport" class="process-map-viewport"></div>
            </div>
            <aside class="process-explorer-sidebar">
                <div class="process-explorer-export">
                    <button id="process-map-export-svg" type="button" class="detail-link process-explorer-button">SVG保存</button>
                    <button id="process-map-export-png" type="button" class="detail-link process-explorer-button">PNG保存</button>
                </div>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Patterns</span>
                        <strong id="process-map-patterns-value">${initialFlowSettings.patterns}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-patterns-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="10"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.patterns}"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-patterns-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Activities</span>
                        <strong id="process-map-activities-value">${initialFlowSettings.activities}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-activities-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="10"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.activities}"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-activities-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Connections</span>
                        <strong id="process-map-connections-value">${initialFlowSettings.connections}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-connections-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="10"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.connections}"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-connections-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Labels</span>
                        <strong id="process-map-labels-value">${initialFlowSettings.labels}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-labels-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="0"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.labels}"
                        >
                        <span class="process-explorer-slider-bottom">0%</span>
                    </div>
                    <p id="process-map-labels-meta" class="process-explorer-meta"></p>
                </section>
                ${renderProcessHeatLegend()}
            </aside>
        </div>
        <section class="variant-panel">
            <div class="result-header variant-panel-header">
                <div>
                    <h3>Variant Analysis</h3>
                    <p class="result-meta">Top 10 Variant を表示します。クリックすると対象 Variant のケースだけでフロー図を更新します。</p>
                </div>
                <button id="variant-reset-button" type="button" class="ghost-link process-explorer-button">全体表示</button>
            </div>
            <div class="variant-panel-summary">
                <article id="variant-coverage-meta" class="variant-coverage-card">
                    <span class="variant-coverage-label">Coverage</span>
                    <strong class="variant-coverage-value">計算中...</strong>
                </article>
                <article class="variant-selection-card">
                    <p id="variant-selection-title" class="variant-selection-title">全体表示中</p>
                    <p id="variant-selection-meta" class="panel-note">Variant を選択すると、その Variant に属するケースだけでフロー図を再描画します。</p>
                    <p id="variant-selection-sequence" class="variant-selection-sequence">現在は全ケースを使ったフロー図を表示しています。</p>
                </article>
            </div>
            <div id="variant-list" class="variant-list"></div>
        </section>
        <section class="bottleneck-panel">
            <div class="result-header variant-panel-header">
                <div>
                    <h3>Bottleneck Analysis</h3>
                    <p class="result-meta">Top 5 waits ranked by average time to the next event.</p>
                </div>
            </div>
            <div class="bottleneck-grid">
                <article class="bottleneck-group">
                    <div class="bottleneck-group-head">
                        <h4>Activity Bottlenecks</h4>
                        <p class="panel-note">Average wait by activity.</p>
                    </div>
                    <div id="activity-bottleneck-list" class="bottleneck-list">
                        <p class="panel-note">Loading...</p>
                    </div>
                </article>
                <article class="bottleneck-group">
                    <div class="bottleneck-group-head">
                        <h4>Transition Bottlenecks</h4>
                        <p class="panel-note">Average wait by transition.</p>
                    </div>
                    <div id="transition-bottleneck-list" class="bottleneck-list">
                        <p class="panel-note">Loading...</p>
                    </div>
                </article>
            </div>
        </section>
        <section id="transition-case-panel" class="result-panel">
            <div class="result-header">
                <div>
                    <h2>Transition Case Drilldown</h2>
                    <p class="result-meta">遷移別ボトルネックを選択すると、時間の長いケースを表示します。</p>
                </div>
            </div>
            <p class="empty-state">遷移別ボトルネックを選択すると、時間の長いケースを表示します。</p>
        </section>
        <section id="case-trace-panel" class="result-panel">
            <div class="result-header">
                <div>
                    <h2>Case ID 検索 / ケース追跡</h2>
                    <p class="result-meta">run 全体から Case ID を検索し、通過イベントと待ち時間を確認します。</p>
                </div>
            </div>
            <form id="case-trace-form" class="case-trace-form">
                <input
                    id="case-trace-input"
                    class="case-trace-input"
                    type="text"
                    name="case_id"
                    placeholder="Case ID を入力"
                    autocomplete="off"
                    spellcheck="false"
                >
                <button type="submit" class="detail-link process-explorer-button process-explorer-button--primary">検索</button>
            </form>
            <div id="case-trace-result" class="case-trace-result">
                <p class="empty-state">Case ID を入力すると、ケースの通過順序と待ち時間を表示します。</p>
            </div>
        </section>
    `;
    return initializePatternFlowExplorer(runId);
}

async function renderChart(analysis, runId) {
    chartPanel.className = "result-panel hidden";
    chartContainer.innerHTML = "";

    if (analysisKey === "frequency") {
        renderFrequencyChart(analysis);
        return;
    }

    if (analysisKey === "transition") {
        renderTransitionChart(analysis);
        return;
    }

    if (analysisKey === "pattern") {
        await renderPatternChart(analysis, runId);
    }
}

async function renderDetailPage() {
    const latestResult = loadLatestResult();
    const runId = getRunId(latestResult);
    let detailRequestVersion = 0;
    activeDetailFilters = { ...DEFAULT_DETAIL_FILTERS };
    detailPageAnalysisLoader = null;

    if (!analysisKey) {
        setStatus("分析キーを特定できませんでした。", "error");
        return;
    }

    if (!runId) {
        setStatus("分析結果が見つかりません。TOP 画面で分析を実行してから詳細ページを開いてください。", "error");
        return;
    }

    setStatus("詳細を読み込んでいます...", "info");

    try {
        const detailData = await loadAnalysisPage(runId, 0, activeDetailFilters);
        const analysis = detailData.analyses[analysisKey];

        if (!analysis) {
            throw new Error("指定した分析結果が見つかりません。");
        }

        activeDetailFilters = cloneDetailFilters(detailData.applied_filters || DEFAULT_DETAIL_FILTERS);

        const renderAnalysisPage = async (rowOffset) => {
            const currentVersion = detailRequestVersion + 1;
            detailRequestVersion = currentVersion;
            setStatus("表を読み込んでいます...", "info");

            try {
                const pageData = await loadAnalysisPage(runId, rowOffset, activeDetailFilters);

                if (currentVersion !== detailRequestVersion) {
                    return;
                }

                const pageAnalysis = pageData.analyses[analysisKey];
                if (!pageAnalysis) {
                    throw new Error("指定した分析結果が見つかりません。");
                }

                renderSummary(pageData, pageAnalysis);
                renderResult(pageAnalysis, runId, renderAnalysisPage);
                hideStatus();
            } catch (error) {
                if (currentVersion !== detailRequestVersion) {
                    return;
                }

                setStatus(error.message, "error");
            }
        };

        detailPageAnalysisLoader = renderAnalysisPage;
        detailPageTitle.textContent = analysis.analysis_name;
        detailPageCopy.textContent = "指定した分析実行の全件結果を表示しています。";
        renderSummary(detailData, analysis);
        await renderChart(analysis, runId);
        renderResult(analysis, runId, renderAnalysisPage);
        hideStatus();
    } catch (error) {
        summaryPanel.className = "summary-panel hidden";
        chartPanel.className = "result-panel hidden";
        resultPanel.className = "result-panel hidden";
        setStatus(error.message, "error");
    }
}

void renderDetailPage();
