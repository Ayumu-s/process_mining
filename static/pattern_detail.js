const patternIndex = Number(document.body.dataset.patternIndex);
const statusPanel = document.getElementById("pattern-status-panel");
const summaryPanel = document.getElementById("pattern-summary-panel");
const bottleneckPanel = document.getElementById("pattern-bottleneck-panel");
const stepPanel = document.getElementById("pattern-step-panel");
const drilldownPanel = document.getElementById("pattern-drilldown-panel");
const casePanel = document.getElementById("pattern-case-panel");
const pageTitle = document.getElementById("pattern-page-title");
const pageCopy = document.getElementById("pattern-page-copy");
const stepsFlow = document.getElementById("pattern-steps-flow");
const prevLink = document.getElementById("pattern-prev-link");
const nextLink = document.getElementById("pattern-next-link");
const backLink = document.getElementById("pattern-back-link");

const DETAIL_FETCH_TIMEOUT_MS = 120000;
const TRANSITION_CASE_FETCH_LIMIT = 40;
const CASE_PAGE_SIZE = 8;

let selectedTransitionKey = "";
let currentRunId = "";
let drilldownRows = [];
let drilldownErrorMessage = "";
let caseTab = "examples";
let caseExamplesPage = 1;
let transitionCasesPage = 1;

const sharedUi = window.ProcessMiningShared;
const {
    buildTable,
    buildTransitionKey,
    escapeHtml,
    fetchJson,
    formatDateTime,
    formatDurationSeconds,
    formatNumber,
    getRunId,
    loadLatestResult,
} = sharedUi;
const setStatus = (message, type = "info") => sharedUi.setStatus(statusPanel, message, type);
const hideStatus = () => sharedUi.hideStatus(statusPanel);

function buildPatternListHref(runId = currentRunId) {
    return runId
        ? `/analysis/pattern?run_id=${encodeURIComponent(runId)}`
        : "/analysis/pattern";
}

function buildDashboardHref(runId = currentRunId) {
    return runId
        ? `/?run_id=${encodeURIComponent(runId)}`
        : "/";
}

function buildPatternDetailHref(targetPatternIndex, runId = currentRunId) {
    const base = `/analysis/patterns/${encodeURIComponent(String(targetPatternIndex))}`;
    return runId
        ? `${base}?run_id=${encodeURIComponent(runId)}`
        : base;
}

function buildPatternTransitionCasesApiUrl(runId, fromActivity, toActivity, limit = TRANSITION_CASE_FETCH_LIMIT) {
    const params = new URLSearchParams({
        from_activity: String(fromActivity || ""),
        to_activity: String(toActivity || ""),
        pattern_index: String(patternIndex),
        limit: String(Math.max(0, Number(limit) || 0)),
    });

    return `/api/runs/${encodeURIComponent(runId)}/transition-cases?${params.toString()}`;
}

function loadPatternTransitionCases(runId, fromActivity, toActivity, limit = TRANSITION_CASE_FETCH_LIMIT) {
    return fetchJson(
        buildPatternTransitionCasesApiUrl(runId, fromActivity, toActivity, limit),
        "遷移ケース詳細の取得に失敗しました。",
        DETAIL_FETCH_TIMEOUT_MS,
    );
}

function buildPatternDetailApiUrl(runId) {
    return `/api/runs/${encodeURIComponent(runId)}/patterns/${encodeURIComponent(String(patternIndex))}`;
}

function loadPatternDetail(runId) {
    return fetchJson(
        buildPatternDetailApiUrl(runId),
        "パターン詳細の取得に失敗しました。",
        DETAIL_FETCH_TIMEOUT_MS,
    );
}

function getStepMetrics(detail) {
    return Array.isArray(detail?.step_metrics) ? detail.step_metrics : [];
}

function getPatternSteps(detail) {
    if (Array.isArray(detail?.pattern_steps) && detail.pattern_steps.length) {
        return detail.pattern_steps.map((step) => String(step || "").trim()).filter(Boolean);
    }

    return String(detail?.pattern || "")
        .split("→")
        .map((step) => step.trim())
        .filter(Boolean);
}

function getTransitionKeyFromMetric(metric) {
    return metric?.transition_key || buildTransitionKey(metric?.activity || "", metric?.next_activity || "");
}

function findSelectedMetric(detail) {
    return getStepMetrics(detail).find((row) => getTransitionKeyFromMetric(row) === selectedTransitionKey) || null;
}

function compactPatternLabel(patternSteps) {
    if (!patternSteps.length) {
        return "ルート未設定";
    }

    if (patternSteps.length <= 4) {
        return patternSteps.join(" → ");
    }

    return `${patternSteps.slice(0, 3).join(" → ")} → ...（全${patternSteps.length}工程）`;
}

function buildSummaryCard(label, value, modifierClass = "") {
    return `
        <article class="summary-card${modifierClass ? ` ${modifierClass}` : ""}">
            <span class="summary-label">${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
        </article>
    `;
}

function renderPatternSteps(patternSteps) {
    if (!patternSteps.length) {
        return "";
    }

    return `
        <div class="pattern-steps">
            ${patternSteps.map((step, index) => `
                <div class="pattern-step-chip">
                    <span class="pattern-step-index">${index + 1}</span>
                    <span>${escapeHtml(step)}</span>
                </div>
            `).join("")}
        </div>
    `;
}

function buildStepMetricRowsHtml(stepMetrics) {
    if (!stepMetrics.length) {
        return '<p class="empty-state">表示できる遷移データがありません。</p>';
    }

    const maxAverage = Math.max(...stepMetrics.map((row) => Number(row.avg_duration_min) || 0), 0);
    const tableRowsHtml = stepMetrics.map((row) => {
        const transitionKey = getTransitionKeyFromMetric(row);
        const isSelected = transitionKey === selectedTransitionKey;
        const isMaxAverage = Number(row.avg_duration_min || 0) === maxAverage;
        const tooltip = [
            `遷移: ${row.transition_label}`,
            `ケース数: ${formatNumber(row.case_count)}`,
            `平均: ${formatNumber(row.avg_duration_min)} 分`,
            `中央値: ${formatNumber(row.median_duration_min)} 分`,
            `最小: ${formatNumber(row.min_duration_min)} 分`,
            `最大: ${formatNumber(row.max_duration_min)} 分`,
            `待機シェア: ${formatNumber(row.wait_share_pct)}%`,
        ].join("\n");

        return `
            <tr
                class="transition-step-row${isSelected ? " transition-step-row--selected" : ""}${isMaxAverage ? " transition-step-row--max" : ""}"
                data-transition-key="${escapeHtml(transitionKey)}"
                data-from-activity="${escapeHtml(row.activity)}"
                data-to-activity="${escapeHtml(row.next_activity)}"
                tabindex="0"
                aria-selected="${isSelected ? "true" : "false"}"
                title="${escapeHtml(tooltip)}"
            >
                <td>${escapeHtml(row.sequence_no)}</td>
                <td class="table-cell--wide">
                    <div class="cell-scroll-wrapper">
                        ${escapeHtml(row.transition_label)}
                        ${isMaxAverage ? '<span class="transition-step-badge">最大</span>' : ""}
                    </div>
                </td>
                <td>${escapeHtml(formatNumber(row.case_count))}</td>
                <td>${escapeHtml(formatNumber(row.avg_duration_min))}</td>
                <td>${escapeHtml(formatNumber(row.median_duration_min))}</td>
                <td>${escapeHtml(formatNumber(row.wait_share_pct))}%</td>
            </tr>
        `;
    }).join("");

    return `
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>工程</th>
                        <th>遷移</th>
                        <th>ケース数</th>
                        <th>平均所要時間(分)</th>
                        <th>中央値(分)</th>
                        <th>待機シェア(%)</th>
                    </tr>
                </thead>
                <tbody>${tableRowsHtml}</tbody>
            </table>
        </div>
    `;
}

function updatePatternNavLinks(runId, patternCount) {
    const patternListHref = buildPatternListHref(runId);
    const dashboardHref = buildDashboardHref(runId);
    const previousIndex = patternIndex - 1;
    const nextIndex = patternIndex + 1;
    const hasPrevious = previousIndex >= 0;
    const hasNext = Number.isInteger(patternCount) ? nextIndex < patternCount : true;

    if (backLink) {
        backLink.href = patternListHref;
    }

    document.querySelectorAll('a[href="/analysis/pattern"]').forEach((linkElement) => {
        linkElement.href = patternListHref;
    });
    document.querySelectorAll('a[href="/"]').forEach((linkElement) => {
        linkElement.href = dashboardHref;
    });

    if (prevLink) {
        prevLink.href = hasPrevious ? buildPatternDetailHref(previousIndex, runId) : patternListHref;
        prevLink.classList.toggle("is-disabled", !hasPrevious);
        prevLink.setAttribute("aria-disabled", hasPrevious ? "false" : "true");
        prevLink.tabIndex = hasPrevious ? 0 : -1;
    }

    if (nextLink) {
        nextLink.href = hasNext ? buildPatternDetailHref(nextIndex, runId) : patternListHref;
        nextLink.classList.toggle("is-disabled", !hasNext);
        nextLink.setAttribute("aria-disabled", hasNext ? "false" : "true");
        nextLink.tabIndex = hasNext ? 0 : -1;
    }
}

function renderSummary(detail) {
    const patternSteps = getPatternSteps(detail);
    const bottleneckLabel = detail.bottleneck_transition?.transition_label || "該当なし";
    const repeatFlag = String(detail.repeat_flag || "").trim() || "なし";
    const fastestPatternFlag = String(detail.fastest_pattern_flag || "").trim() || "該当なし";
    const reviewFlag = String(detail.review_flag || "").trim();
    const commentText = String(detail.simple_comment || "").trim();

    const noteItems = [
        reviewFlag ? `確認区分: ${reviewFlag}` : "",
        commentText,
    ].filter(Boolean);

    summaryPanel.className = "summary-panel summary-panel--pattern-detail";
    summaryPanel.innerHTML = `
        ${buildSummaryCard("パターン", `#${patternIndex + 1}`)}
        ${buildSummaryCard("ケース数 / 構成比", `${formatNumber(detail.case_count)}件 / ${formatNumber(detail.case_ratio_pct)}%`)}
        ${buildSummaryCard("平均 / 中央処理時間", `${formatNumber(detail.avg_case_duration_min)}分 / ${formatNumber(detail.median_case_duration_min)}分`)}
        ${buildSummaryCard("繰り返し", repeatFlag)}
        ${buildSummaryCard("繰り返し率区分 / 差分", `${String(detail.repeat_rate_band || "-")} / ${formatNumber(detail.avg_case_duration_diff_min || 0)}分`)}
        ${buildSummaryCard("改善優先度 / 全体影響度", `${formatNumber(detail.improvement_priority_score || 0)} / ${formatNumber(detail.overall_impact_pct || 0)}%`)}
        ${buildSummaryCard("最短処理", fastestPatternFlag)}
        ${buildSummaryCard("最大ボトルネック", bottleneckLabel, "summary-card--warning")}
        ${buildSummaryCard("代表ルート", compactPatternLabel(patternSteps), "summary-card--wide")}
        ${noteItems.length ? `
            <article class="summary-card summary-card--wide summary-note-card">
                <span class="summary-label">コメント</span>
                <strong>${escapeHtml(noteItems.join(" / "))}</strong>
            </article>
        ` : ""}
    `;
}

function renderStepPanel(detail) {
    stepPanel.className = "result-panel";
    stepPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>工程別の所要時間</h2>
                <p class="result-meta">行をクリックすると、該当遷移だけのケースを下部タブで確認できます。</p>
            </div>
        </div>
        ${buildStepMetricRowsHtml(getStepMetrics(detail))}
    `;
}

function renderBottleneckPanel(detail) {
    const patternSteps = getPatternSteps(detail);
    const stepMetrics = getStepMetrics(detail);
    const maxAverage = Math.max(...stepMetrics.map((row) => Number(row.avg_duration_min) || 0), 1);
    const bottleneck = detail.bottleneck_transition;

    const calloutHtml = bottleneck
        ? `
            <div class="bottleneck-callout">
                <strong>最大ボトルネック: ${escapeHtml(bottleneck.transition_label)}</strong>
                <p class="panel-note">平均 ${escapeHtml(formatNumber(bottleneck.avg_duration_min))} 分 / 中央値 ${escapeHtml(formatNumber(bottleneck.median_duration_min))} 分 / 最大 ${escapeHtml(formatNumber(bottleneck.max_duration_min))} 分</p>
            </div>
        `
        : `
            <div class="bottleneck-callout">
                <strong>ボトルネック遷移は見つかりませんでした。</strong>
            </div>
        `;

    const barsHtml = stepMetrics.map((row) => {
        const transitionKey = getTransitionKeyFromMetric(row);
        const isBottleneck = bottleneck && row.sequence_no === bottleneck.sequence_no;
        const isSelected = transitionKey === selectedTransitionKey;
        const widthPercent = maxAverage > 0
            ? Math.max(8, (Number(row.avg_duration_min || 0) / maxAverage) * 100)
            : 0;
        const tooltip = [
            row.transition_label,
            `ケース数: ${formatNumber(row.case_count)}`,
            `平均: ${formatNumber(row.avg_duration_min)} 分`,
            `中央値: ${formatNumber(row.median_duration_min)} 分`,
            `最大: ${formatNumber(row.max_duration_min)} 分`,
            `待機シェア: ${formatNumber(row.wait_share_pct)}%`,
        ].join("\n");

        return `
            <button
                type="button"
                class="bottleneck-bar-card${isBottleneck ? " bottleneck-bar-card--highlight" : ""}${isSelected ? " bottleneck-bar-card--selected" : ""}"
                data-transition-key="${escapeHtml(transitionKey)}"
                data-from-activity="${escapeHtml(row.activity)}"
                data-to-activity="${escapeHtml(row.next_activity)}"
                aria-pressed="${isSelected ? "true" : "false"}"
                title="${escapeHtml(tooltip)}"
            >
                <div class="bottleneck-bar-head">
                    <p class="bottleneck-bar-label">${escapeHtml(row.transition_label)}</p>
                    <span class="bottleneck-bar-value">${escapeHtml(formatNumber(row.avg_duration_min))} 分</span>
                </div>
                <div class="bottleneck-bar-track">
                    <div class="bottleneck-bar-fill" style="width: ${widthPercent}%"></div>
                </div>
                <p class="bottleneck-bar-meta">ケース ${escapeHtml(formatNumber(row.case_count))} / クリックで対象ケースを表示</p>
            </button>
        `;
    }).join("");

    bottleneckPanel.className = "result-panel";
    bottleneckPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>ボトルネック分析</h2>
                <p class="result-meta">平均所要時間の長い工程を上から並べています。詳細値はホバーで確認できます。</p>
            </div>
        </div>
        <p class="panel-note">${escapeHtml(compactPatternLabel(patternSteps))}</p>
        ${renderPatternSteps(patternSteps)}
        ${calloutHtml}
        <div class="bottleneck-bars">
            ${barsHtml || '<p class="empty-state">表示できる遷移はありません。</p>'}
        </div>
    `;
}

function buildMiniKpi(label, value) {
    return `
        <article class="detail-mini-kpi">
            <span class="detail-mini-kpi-label">${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
        </article>
    `;
}

async function renderDrilldownPanel(detail) {
    drilldownPanel.className = "result-panel";

    if (!selectedTransitionKey) {
        drilldownPanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>選択中の遷移</h2>
                    <p class="result-meta">上のボトルネックバーまたは工程表から遷移を選択してください。</p>
                </div>
            </div>
            <p class="empty-state">遷移を選ぶと、所要時間の要約とケース一覧を表示します。</p>
        `;
        return;
    }

    const selectedMetric = findSelectedMetric(detail);
    const transitionLabel = selectedMetric
        ? selectedMetric.transition_label
        : selectedTransitionKey.replace("__TO__", " → ");

    drilldownPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>選択中の遷移</h2>
                <p class="result-meta">${escapeHtml(transitionLabel)}</p>
            </div>
        </div>
        <p class="panel-note">ケース詳細を読み込み中です。</p>
    `;

    if (selectedMetric && !drilldownRows.length && !drilldownErrorMessage) {
        try {
            const payload = await loadPatternTransitionCases(
                currentRunId,
                selectedMetric.activity,
                selectedMetric.next_activity,
                TRANSITION_CASE_FETCH_LIMIT,
            );
            drilldownRows = Array.isArray(payload.cases) ? payload.cases : [];
        } catch (error) {
            drilldownErrorMessage = error.message;
        }
    }

    if (drilldownErrorMessage) {
        drilldownPanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>選択中の遷移</h2>
                    <p class="result-meta">${escapeHtml(transitionLabel)}</p>
                </div>
            </div>
            <p class="empty-state">${escapeHtml(drilldownErrorMessage)}</p>
        `;
        return;
    }

    if (!selectedMetric) {
        drilldownPanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>選択中の遷移</h2>
                    <p class="result-meta">${escapeHtml(transitionLabel)}</p>
                </div>
            </div>
            <p class="empty-state">選択した遷移の指標を表示できません。</p>
        `;
        return;
    }

    drilldownPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>選択中の遷移</h2>
                <p class="result-meta">${escapeHtml(transitionLabel)}</p>
            </div>
        </div>
        <div class="detail-inline-kpis">
            ${buildMiniKpi("ケース数", `${formatNumber(selectedMetric.case_count)}件`)}
            ${buildMiniKpi("平均所要時間", `${formatNumber(selectedMetric.avg_duration_min)}分`)}
            ${buildMiniKpi("中央値", `${formatNumber(selectedMetric.median_duration_min)}分`)}
            ${buildMiniKpi("最大", `${formatNumber(selectedMetric.max_duration_min)}分`)}
            ${buildMiniKpi("待機シェア", `${formatNumber(selectedMetric.wait_share_pct)}%`)}
        </div>
        <p class="panel-note">ケース一覧は下のタブで確認できます。${drilldownRows.length ? `現在 ${formatNumber(drilldownRows.length)} 件を表示可能です。` : ""}</p>
    `;
}

function getVisibleRows(rows, currentPage, pageSize) {
    const safePage = Math.max(1, Number(currentPage) || 1);
    const startIndex = (safePage - 1) * pageSize;
    return rows.slice(startIndex, startIndex + pageSize);
}

function buildPaginationHtml(totalRows, currentPage, pageSize, targetName) {
    const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
    if (totalPages <= 1) {
        return "";
    }

    const start = (currentPage - 1) * pageSize + 1;
    const end = Math.min(totalRows, currentPage * pageSize);

    return `
        <div class="result-pagination">
            <p class="result-pagination-meta">${escapeHtml(formatNumber(start))} - ${escapeHtml(formatNumber(end))} / ${escapeHtml(formatNumber(totalRows))} 件</p>
            <div class="result-pagination-actions">
                <button
                    type="button"
                    class="btn btn-secondary btn-sm result-pagination-button"
                    data-pagination-target="${escapeHtml(targetName)}"
                    data-pagination-direction="prev"
                    ${currentPage <= 1 ? "disabled" : ""}
                >
                    前へ
                </button>
                <button
                    type="button"
                    class="btn btn-secondary btn-sm result-pagination-button"
                    data-pagination-target="${escapeHtml(targetName)}"
                    data-pagination-direction="next"
                    ${currentPage >= totalPages ? "disabled" : ""}
                >
                    次へ
                </button>
            </div>
        </div>
    `;
}

function renderRepresentativeCases(detail) {
    const rows = Array.isArray(detail.case_examples) ? detail.case_examples : [];
    if (!rows.length) {
        return '<p class="empty-state">代表ケースを表示できません。</p>';
    }

    const visibleRows = getVisibleRows(rows, caseExamplesPage, CASE_PAGE_SIZE);
    const tableRows = visibleRows.map((row) => ({
        "ケースID": row.case_id,
        "総処理時間(分)": formatNumber(row.case_total_duration_min),
        "開始日時": formatDateTime(row.start_time),
        "終了日時": formatDateTime(row.end_time),
    }));

    return `
        <details class="case-list-details" open>
            <summary>代表ケース一覧</summary>
            <div class="case-list-details-body">
                ${buildTable(tableRows)}
                ${buildPaginationHtml(rows.length, caseExamplesPage, CASE_PAGE_SIZE, "examples")}
            </div>
        </details>
    `;
}

function renderTransitionCases(detail) {
    if (!selectedTransitionKey) {
        return '<p class="empty-state">上の遷移を選択すると、対象ケース一覧を表示します。</p>';
    }

    if (drilldownErrorMessage) {
        return `<p class="empty-state">${escapeHtml(drilldownErrorMessage)}</p>`;
    }

    if (!drilldownRows.length) {
        return '<p class="empty-state">選択した遷移に該当するケースはありません。</p>';
    }

    const selectedMetric = findSelectedMetric(detail);
    const titleText = selectedMetric?.transition_label || "選択中の遷移ケース";
    const visibleRows = getVisibleRows(drilldownRows, transitionCasesPage, CASE_PAGE_SIZE);
    const tableRows = visibleRows.map((row) => ({
        "ケースID": row.case_id,
        "遷移所要時間": row.duration_text || `${formatDurationSeconds(row.duration_sec)} 秒`,
        "開始日時": formatDateTime(row.from_time),
        "終了日時": formatDateTime(row.to_time),
    }));

    return `
        <details class="case-list-details" open>
            <summary>${escapeHtml(titleText)}</summary>
            <div class="case-list-details-body">
                ${buildTable(tableRows)}
                ${buildPaginationHtml(drilldownRows.length, transitionCasesPage, CASE_PAGE_SIZE, "transition")}
            </div>
        </details>
    `;
}

function renderCasePanel(detail) {
    const hasTransitionSelection = Boolean(selectedTransitionKey);
    const activeTab = hasTransitionSelection ? caseTab : "examples";
    caseTab = activeTab;

    casePanel.className = "result-panel";
    casePanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>ケース一覧</h2>
                <p class="result-meta">代表ケースと、選択した遷移に属するケースを切り替えて確認できます。</p>
            </div>
        </div>
        <div class="pattern-case-tabs" role="tablist" aria-label="ケース一覧タブ">
            <button
                type="button"
                class="pattern-case-tab${activeTab === "examples" ? " is-active" : ""}"
                data-case-tab="examples"
                role="tab"
                aria-selected="${activeTab === "examples" ? "true" : "false"}"
            >
                代表ケース
            </button>
            <button
                type="button"
                class="pattern-case-tab${activeTab === "transition" ? " is-active" : ""}"
                data-case-tab="transition"
                role="tab"
                aria-selected="${activeTab === "transition" ? "true" : "false"}"
                ${hasTransitionSelection ? "" : "disabled"}
            >
                選択遷移ケース
            </button>
        </div>
        <div class="pattern-case-tab-panel">
            ${activeTab === "transition" ? renderTransitionCases(detail) : renderRepresentativeCases(detail)}
        </div>
    `;
}

async function selectTransition(detail, nextTransitionKey) {
    const resolvedTransitionKey = String(nextTransitionKey || "");
    selectedTransitionKey = selectedTransitionKey === resolvedTransitionKey ? "" : resolvedTransitionKey;
    drilldownRows = [];
    drilldownErrorMessage = "";
    transitionCasesPage = 1;
    caseTab = selectedTransitionKey ? "transition" : "examples";
    renderBottleneckPanel(detail);
    renderStepPanel(detail);
    bindTransitionSelection(detail);
    await renderDrilldownPanel(detail);
    renderCasePanel(detail);
    bindCasePanel(detail);
}

function bindTransitionSelection(detail) {
    const bindSelectHandler = (element) => {
        element.addEventListener("click", async () => {
            await selectTransition(detail, element.dataset.transitionKey || "");
        });
        element.addEventListener("keydown", async (event) => {
            if (event.key !== "Enter" && event.key !== " ") {
                return;
            }
            event.preventDefault();
            await selectTransition(detail, element.dataset.transitionKey || "");
        });
    };

    bottleneckPanel.querySelectorAll("[data-transition-key]").forEach((buttonElement) => {
        bindSelectHandler(buttonElement);
    });

    stepPanel.querySelectorAll("[data-transition-key]").forEach((rowElement) => {
        bindSelectHandler(rowElement);
    });
}

function bindCasePanel(detail) {
    casePanel.querySelectorAll("[data-case-tab]").forEach((buttonElement) => {
        buttonElement.addEventListener("click", () => {
            const nextTab = buttonElement.dataset.caseTab || "examples";
            if (nextTab === "transition" && !selectedTransitionKey) {
                return;
            }
            caseTab = nextTab;
            renderCasePanel(detail);
            bindCasePanel(detail);
        });
    });

    casePanel.querySelectorAll("[data-pagination-target]").forEach((buttonElement) => {
        buttonElement.addEventListener("click", () => {
            const direction = buttonElement.dataset.paginationDirection;
            const target = buttonElement.dataset.paginationTarget;
            if (target === "examples") {
                caseExamplesPage += direction === "next" ? 1 : -1;
                caseExamplesPage = Math.max(1, caseExamplesPage);
            } else if (target === "transition") {
                transitionCasesPage += direction === "next" ? 1 : -1;
                transitionCasesPage = Math.max(1, transitionCasesPage);
            }
            renderCasePanel(detail);
            bindCasePanel(detail);
        });
    });
}

async function renderPatternDetailPage() {
    const latestResult = loadLatestResult();
    const runId = getRunId(latestResult);
    currentRunId = runId;

    if (!runId) {
        setStatus("分析結果が見つかりません。TOP 画面から分析を実行してから詳細ページを開いてください。", "error");
        return;
    }

    if (!Number.isInteger(patternIndex)) {
        setStatus("パターン番号が不正です。", "error");
        return;
    }

    setStatus("パターン詳細を読み込んでいます...", "info");

    try {
        const detail = await loadPatternDetail(runId);
        const patternRows = latestResult?.analyses?.pattern?.rows;
        const patternCount = Array.isArray(patternRows) ? patternRows.length : null;
        const patternSteps = getPatternSteps(detail);
        const shortTitle = `Pattern #${patternIndex + 1}`;
        const caseCountText = `${Number(detail.case_count || 0).toLocaleString("ja-JP")}件`;
        const ratioText = `${formatNumber(detail.case_ratio_pct)}%`;

        updatePatternNavLinks(runId, patternCount);

        document.title = `${shortTitle} | ProcessLens`;
        pageTitle.innerHTML = `${escapeHtml(shortTitle)} <span class="pattern-title-meta">${escapeHtml(caseCountText)} / ${escapeHtml(ratioText)}</span>`;
        pageCopy.textContent = "選択した処理順パターンに属するケースだけを表示し、工程ごとの所要時間とボトルネックを確認できます。";

        if (stepsFlow) {
            if (patternSteps.length) {
                stepsFlow.innerHTML = patternSteps.map((step, index) => {
                    const chipHtml = `<span class="pattern-step-chip"><span>${escapeHtml(step)}</span></span>`;
                    const arrowHtml = index < patternSteps.length - 1
                        ? '<span class="pattern-step-arrow" aria-hidden="true">→</span>'
                        : "";
                    return chipHtml + arrowHtml;
                }).join("");
                stepsFlow.classList.remove("hidden");
            } else {
                stepsFlow.classList.add("hidden");
                stepsFlow.innerHTML = "";
            }
        }

        selectedTransitionKey = detail.bottleneck_transition?.transition_key || "";
        drilldownRows = [];
        drilldownErrorMessage = "";
        caseExamplesPage = 1;
        transitionCasesPage = 1;
        caseTab = selectedTransitionKey ? "transition" : "examples";

        renderSummary(detail);
        renderBottleneckPanel(detail);
        renderStepPanel(detail);
        bindTransitionSelection(detail);
        await renderDrilldownPanel(detail);
        renderCasePanel(detail);
        bindCasePanel(detail);
        hideStatus();
    } catch (error) {
        setStatus(error.message, "error");
    }
}

renderPatternDetailPage();
