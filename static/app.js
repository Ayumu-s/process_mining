const PREVIEW_ROW_COUNT = 10;
const STORAGE_KEY = "processMiningLastResult";

const form = document.getElementById("analyze-form");
const submitButton = document.getElementById("submit-button");
const statusPanel = document.getElementById("status-panel");
const summaryPanel = document.getElementById("summary-panel");
const resultPanels = document.getElementById("result-panels");

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

function buildPatternDetailHref(runId, patternIndex) {
    return `/analysis/patterns/${encodeURIComponent(String(patternIndex))}?run_id=${encodeURIComponent(runId)}`;
}

function buildAnalysisDetailHref(analysisKey, runId) {
    const path = `/analysis/${encodeURIComponent(analysisKey)}`;

    if (!runId) {
        return path;
    }

    return `${path}?run_id=${encodeURIComponent(runId)}`;
}

function buildTable(rows, options = {}) {
    if (!rows.length) {
        return '<p class="empty-state">表示できるデータがありません。</p>';
    }

    const { analysisKey = "", runId = "" } = options;
    const headers = Object.keys(rows[0]).filter((header) => !header.startsWith("__"));
    const headHtml = headers
        .map((header) => `<th>${escapeHtml(header)}</th>`)
        .join("");

    const bodyHtml = rows
        .map((row) => {
            const cells = headers
                .map((header) => {
                    const cellValue = escapeHtml(row[header]);
                    const isPatternLink = (
                        analysisKey === "pattern"
                        && header === "処理順パターン"
                        && runId
                        && Number.isInteger(row.__rowIndex)
                    );

                    if (isPatternLink) {
                        return `
                            <td>
                                <a href="${buildPatternDetailHref(runId, row.__rowIndex)}" class="table-link">
                                    ${cellValue}
                                </a>
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

function saveLatestResult(data) {
    try {
        sessionStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    } catch {
        try {
            const fallbackData = {
                run_id: data.run_id,
                source_file_name: data.source_file_name,
                selected_analysis_keys: data.selected_analysis_keys,
                case_count: data.case_count,
                event_count: data.event_count,
                analyses: {},
            };
            sessionStorage.setItem(STORAGE_KEY, JSON.stringify(fallbackData));
        } catch {
            // Ignore storage failures and keep the in-memory rendered result only.
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

function renderSummary(data) {
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
    `;
}

function buildResultHeader(analysisKey, analysis, previewRows) {
    const excelLine = analysis.excel_file
        ? `<p class="excel-path">Excel: ${escapeHtml(analysis.excel_file)}</p>`
        : "";
    const totalRowCount = analysis.row_count ?? analysis.rows.length;

    const previewMessage = totalRowCount > previewRows.length
        ? `先頭 ${previewRows.length} 件を表示 / 全 ${totalRowCount} 件`
        : `全 ${totalRowCount} 件を表示`;

    return `
        <div class="result-header">
            <div>
                <h2>${escapeHtml(analysis.analysis_name)}</h2>
                <p class="result-meta">${escapeHtml(previewMessage)}</p>
                ${excelLine}
            </div>
            <a href="${buildAnalysisDetailHref(analysisKey, analysis.run_id || "")}" class="detail-link">詳細ページ</a>
        </div>
    `;
}

function renderAnalysisPanels(analyses, runId) {
    resultPanels.innerHTML = "";

    Object.entries(analyses).forEach(([analysisKey, analysis]) => {
        const section = document.createElement("section");
        section.className = "result-panel";
        const analysisWithRunId = { ...analysis, run_id: runId };

        const previewRows = analysis.rows
            .slice(0, PREVIEW_ROW_COUNT)
            .map((row, index) => ({ ...row, __rowIndex: index }));

        section.innerHTML = `
            ${buildResultHeader(analysisKey, analysisWithRunId, previewRows)}
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

    submitButton.disabled = true;
    setStatus("分析を実行しています...", "info");
    summaryPanel.className = "summary-panel hidden";
    resultPanels.innerHTML = "";

    try {
        const response = await fetch("/api/analyze", {
            method: "POST",
            body: new FormData(form),
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "分析に失敗しました。");
        }

        saveLatestResult(data);
        renderDashboard(data);
        setStatus("分析が完了しました。", "success");
    } catch (error) {
        setStatus(error.message, "error");
    } finally {
        submitButton.disabled = false;
    }
});

const latestResult = loadLatestResult();

if (latestResult) {
    renderDashboard(latestResult);
}

hideStatus();
