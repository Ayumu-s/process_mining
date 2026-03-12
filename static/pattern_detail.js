const STORAGE_KEY = "processMiningLastResult";

const patternIndex = Number(document.body.dataset.patternIndex);
const statusPanel = document.getElementById("pattern-status-panel");
const summaryPanel = document.getElementById("pattern-summary-panel");
const bottleneckPanel = document.getElementById("pattern-bottleneck-panel");
const stepPanel = document.getElementById("pattern-step-panel");
const casePanel = document.getElementById("pattern-case-panel");
const pageTitle = document.getElementById("pattern-page-title");
const pageCopy = document.getElementById("pattern-page-copy");

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

function formatNumber(value) {
    return Number(value || 0).toLocaleString("ja-JP", {
        maximumFractionDigits: 2,
    });
}

function formatDateTime(value) {
    if (!value) {
        return "";
    }

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return escapeHtml(value);
    }

    return date.toLocaleString("ja-JP");
}

function buildTable(rows) {
    if (!rows.length) {
        return '<p class="empty-state">表示できるデータがありません。</p>';
    }

    const headers = Object.keys(rows[0]);
    const headHtml = headers
        .map((header) => `<th>${escapeHtml(header)}</th>`)
        .join("");

    const bodyHtml = rows
        .map((row) => {
            const cells = headers
                .map((header) => `<td>${escapeHtml(row[header])}</td>`)
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

function getRunId(latestResult) {
    const params = new URLSearchParams(window.location.search);
    return params.get("run_id") || latestResult?.run_id || "";
}

function renderSummary(detail) {
    const bottleneckLabel = detail.bottleneck_transition
        ? detail.bottleneck_transition.transition_label
        : "算出なし";

    summaryPanel.className = "summary-panel";
    summaryPanel.innerHTML = `
        <article class="summary-card">
            <span class="summary-label">入力ファイル</span>
            <strong>${escapeHtml(detail.source_file_name)}</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">ケース数 / 構成比</span>
            <strong>${escapeHtml(detail.case_count)} / ${escapeHtml(formatNumber(detail.case_ratio_pct))}%</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">平均 / 中央ケース時間(分)</span>
            <strong>${escapeHtml(formatNumber(detail.avg_case_duration_min))} / ${escapeHtml(formatNumber(detail.median_case_duration_min))}</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">最大ボトルネック</span>
            <strong>${escapeHtml(bottleneckLabel)}</strong>
        </article>
    `;
}

function renderPatternSteps(patternSteps) {
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

function renderBottleneckPanel(detail) {
    const stepMetrics = detail.step_metrics || [];
    const maxAverage = Math.max(...stepMetrics.map((row) => Number(row.avg_duration_min) || 0), 1);
    const bottleneck = detail.bottleneck_transition;

    const calloutHtml = bottleneck
        ? `
            <div class="bottleneck-callout">
                <strong>最も待ち時間が大きい遷移: ${escapeHtml(bottleneck.transition_label)}</strong>
                <p class="panel-note">
                    平均 ${escapeHtml(formatNumber(bottleneck.avg_duration_min))} 分、
                    中央値 ${escapeHtml(formatNumber(bottleneck.median_duration_min))} 分、
                    最大 ${escapeHtml(formatNumber(bottleneck.max_duration_min))} 分、
                    全待ち時間の ${escapeHtml(formatNumber(bottleneck.wait_share_pct))}% を占めています。
                </p>
            </div>
        `
        : `
            <div class="bottleneck-callout">
                <strong>ボトルネックを算出できませんでした。</strong>
            </div>
        `;

    const barsHtml = stepMetrics.map((row) => {
        const isBottleneck = bottleneck && row.sequence_no === bottleneck.sequence_no;
        const widthPercent = maxAverage > 0
            ? Math.max(6, (Number(row.avg_duration_min) / maxAverage) * 100)
            : 0;

        return `
            <article class="bottleneck-bar-card${isBottleneck ? " bottleneck-bar-card--highlight" : ""}">
                <div class="bottleneck-bar-head">
                    <p class="bottleneck-bar-label">${escapeHtml(row.transition_label)}</p>
                    <span class="bottleneck-bar-value">平均 ${escapeHtml(formatNumber(row.avg_duration_min))} 分</span>
                </div>
                <div class="bottleneck-bar-track">
                    <div class="bottleneck-bar-fill" style="width: ${widthPercent}%"></div>
                </div>
                <p class="bottleneck-bar-meta">
                    ケース数 ${escapeHtml(row.case_count)} /
                    中央値 ${escapeHtml(formatNumber(row.median_duration_min))} 分 /
                    最大 ${escapeHtml(formatNumber(row.max_duration_min))} 分 /
                    待ち時間比率 ${escapeHtml(formatNumber(row.wait_share_pct))}%
                </p>
            </article>
        `;
    }).join("");

    bottleneckPanel.className = "result-panel";
    bottleneckPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>ボトルネック確認</h2>
                <p class="result-meta">このパターンの各遷移で、次工程に進むまでの待ち時間を比較しています。</p>
            </div>
        </div>
        <p class="panel-note">${escapeHtml(detail.pattern)}</p>
        ${renderPatternSteps(detail.pattern_steps)}
        ${calloutHtml}
        <div class="bottleneck-bars">
            ${barsHtml}
        </div>
    `;
}

function renderStepPanel(detail) {
    const stepRows = detail.step_metrics.map((row) => ({
        "順番": row.sequence_no,
        "遷移": row.transition_label,
        "ケース数": row.case_count,
        "平均待ち時間(分)": formatNumber(row.avg_duration_min),
        "中央値(分)": formatNumber(row.median_duration_min),
        "最小(分)": formatNumber(row.min_duration_min),
        "最大(分)": formatNumber(row.max_duration_min),
        "待ち時間比率(%)": formatNumber(row.wait_share_pct),
    }));

    stepPanel.className = "result-panel";
    stepPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>遷移別の待ち時間</h2>
                <p class="result-meta">どの工程間で滞留しているかを数値で確認できます。</p>
            </div>
        </div>
        ${buildTable(stepRows)}
    `;
}

function renderCasePanel(detail) {
    const caseRows = detail.case_examples.map((row) => ({
        "ケースID": row.case_id,
        "総所要時間(分)": formatNumber(row.case_total_duration_min),
        "開始時刻": formatDateTime(row.start_time),
        "終了時刻": formatDateTime(row.end_time),
    }));

    casePanel.className = "result-panel";
    casePanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>時間の長いケース</h2>
                <p class="result-meta">同じ処理順パターンでも時間がかかっているケースを上位から表示します。</p>
            </div>
        </div>
        ${buildTable(caseRows)}
    `;
}

async function renderPatternDetailPage() {
    const latestResult = loadLatestResult();
    const runId = getRunId(latestResult);

    if (!runId) {
        setStatus("分析結果が見つかりません。TOP 画面で再分析してください。", "error");
        return;
    }

    if (!Number.isInteger(patternIndex)) {
        setStatus("処理順パターン番号を取得できません。", "error");
        return;
    }

    setStatus("処理順パターンの詳細を読み込んでいます...", "info");

    try {
        const response = await fetch(`/api/runs/${encodeURIComponent(runId)}/patterns/${encodeURIComponent(String(patternIndex))}`);
        const detail = await response.json();

        if (!response.ok) {
            throw new Error(detail.detail || detail.error || "処理順パターン詳細の取得に失敗しました。");
        }

        document.title = `${detail.analysis_name} 詳細 | Process Mining Workbench`;
        pageTitle.textContent = `処理順パターン ${patternIndex + 1} の詳細`;
        pageCopy.textContent = "選択したパターンに属するケースだけを抽出し、遷移ごとの待ち時間からボトルネックを確認します。";

        renderSummary(detail);
        renderBottleneckPanel(detail);
        renderStepPanel(detail);
        renderCasePanel(detail);
        hideStatus();
    } catch (error) {
        setStatus(error.message, "error");
    }
}

renderPatternDetailPage();
