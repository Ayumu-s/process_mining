const STORAGE_KEY = "processMiningLastResult";
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

function buildAnalysisDetailApiUrl(runId, rowOffset = 0) {
    const params = new URLSearchParams({
        row_limit: String(DETAIL_ROW_LIMIT),
        row_offset: String(Math.max(0, Number(rowOffset) || 0)),
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

function loadAnalysisPage(runId, rowOffset = 0) {
    return fetchJson(
        buildAnalysisDetailApiUrl(runId, rowOffset),
        "分析詳細の読み込みに失敗しました。"
    );
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
            stroke: #cf7a45;
            stroke-dasharray: 8 6;
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

function calculateProcessFlowLayout(nodes, edges) {
    if (!nodes.length) return { chartWidth: 0, chartHeight: 0, mainSpineX: 0 };

    const chartLeft = 40;
    const chartTop = 60;
    const baseNodeWidth = 140;
    const baseNodeHeight = 44;
    const layerGap = 280;
    const rowGap = 200;
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
            const routeY = Math.max(startY, endY) + 210 + Math.abs(targetNode.layer - sourceNode.layer) * 32;
            if (routeY > maxRouteY) maxRouteY = routeY;
        }
    });

    const bottomPadding = 60;
    return {
        chartWidth: svgWidth,
        chartHeight: Math.max(400, maxRouteY + bottomPadding),
        mainSpineX: mainSpineX
    };
}

function renderProcessFlowMapFromData(flowData, options = {}) {
    const activityPercent = Number(options.activityPercent ?? 100);
    const connectionPercent = Number(options.connectionPercent ?? 100);
    const labelPercent = Number(options.labelPercent ?? 100);
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
    const layout = calculateProcessFlowLayout(nodes, edges);
    const { chartWidth, chartHeight, mainSpineX } = layout;
    const layerGap = 280;

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
        let opacity, strokeWidth;
        if (isSpine) { opacity = 0.9; strokeWidth = 14; }
        else if (isBack) { opacity = 0.08; strokeWidth = 1.0; }
        else {
            opacity = 0.10 + (edge.count / maxEdgeCount) * 0.50;
            strokeWidth = 1.0 + (edge.count / maxEdgeCount) * 9.0;
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
            const rY = Math.max(startY, endY) + 210 + Math.abs(t.layer - s.layer) * 32;
            const rXOff = s.x >= mainSpineX ? 220 + layerGap : -(220 + layerGap); 
            pathD = `M ${startX} ${startY} C ${startX} ${rY}, ${endX + rXOff} ${rY}, ${endX} ${endY}`;
            lblX = endX + rXOff / 2; lblY = rY - 10;
        }

        const showLabel = labelState.visibleLabelKeys.has(getProcessFlowEdgeKey(edge));
        return `
            <path d="${pathD}" class="${isBack ? "process-map-edge process-map-edge--return" : "process-map-edge"}" marker-end="url(#${isBack ? "process-map-arrow-return" : "process-map-arrow"})" data-source="${escapeHtml(edge.source)}" data-target="${escapeHtml(edge.target)}" style="stroke-width: ${strokeWidth}; opacity: ${opacity}; fill: none; ${isSpine ? "stroke: #0a3b8c;" : "stroke: currentColor;"}"></path>
            ${showLabel ? `<text x="${lblX}" y="${lblY}" class="${isBack ? "process-map-edge-label process-map-edge-label--return" : "process-map-edge-label"}" data-source="${escapeHtml(edge.source)}" data-target="${escapeHtml(edge.target)}">${escapeHtml(edge.count.toLocaleString("ja-JP"))}件</text>` : ""}
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
                <rect x="${node.x}" y="${node.y}" width="${node.calcWidth}" height="${node.calcHeight}" rx="${rx}" ry="${rx}" class="process-map-node" style="fill: ${fill}; stroke: ${stroke}; stroke-width: ${strokeW};" filter="url(#drop-shadow)"></rect>
                ${labelSvg}
            </g>
        `;
    }).join("");

    return `
        <div class="process-map-wrap">
            <svg class="process-map-svg" width="${chartWidth}" height="${chartHeight}" viewBox="0 0 ${chartWidth} ${chartHeight}" role="img" aria-label="業務全体フロー図" preserveAspectRatio="xMinYMin meet">
                <defs>
                    <filter id="drop-shadow" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="0" dy="8" stdDeviation="6" flood-color="rgba(10, 20, 40, 0.15)" /></filter>
                    <marker id="process-map-arrow" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" refX="11" refY="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#2458d3"></path></marker>
                    <marker id="process-map-arrow-return" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" refX="11" refY="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#cf7a45"></path></marker>
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

    if (!runId) {
        if (mapViewport) {
            mapViewport.innerHTML = renderProcessMapEmpty("分析結果が見つかりません。TOP 画面から再度実行してください。");
        }
        return;
    }

    if (!mapViewport || !patternsSlider || !activitiesSlider || !connectionsSlider || !labelsSlider) {
        return;
    }

    let requestVersion = 0;

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
            const snapshot = await fetchJson(
                `/api/runs/${encodeURIComponent(runId)}/pattern-flow?pattern_percent=${encodeURIComponent(String(patternPercent))}&activity_percent=${encodeURIComponent(String(activityPercent))}&connection_percent=${encodeURIComponent(String(connectionPercent))}`,
                "処理フロー図の読み込みに失敗しました。"
            );

            if (currentVersion !== requestVersion) {
                return;
            }

            const flowData = snapshot.flow_data || { nodes: [], edges: [] };
            const labelState = buildProcessMapLabelState(flowData.edges || [], labelPercent);

            patternsMeta.textContent = `${snapshot.pattern_window.used_pattern_count} / ${snapshot.pattern_window.effective_pattern_count} patterns`;
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
    const chartRows = analysis.rows.slice(0, 8);
    const initialFlowSettings = getInitialPatternFlowSettings(analysis.row_count ?? analysis.rows.length);

    if (!chartRows.length) {
        chartPanel.className = "result-panel";
        chartTitle.textContent = "処理順パターン図";
        chartNote.textContent = "表示できるデータがありません。";
        chartContainer.innerHTML = '<p class="empty-state">表示できるデータがありません。</p>';
        return;
    }

    const maxCaseCount = Math.max(...chartRows.map((row) => Number(row["ケース数"]) || 0), 1);
    const cardsHtml = chartRows
        .map((row, index) => {
            const patternSteps = String(row["処理順パターン"]).split("→");
            const caseCount = Number(row["ケース数"]) || 0;
            const caseRatio = row["ケース比率(%)"];
            const averageCaseDuration = row["平均ケース時間(分)"];
            const widthPercent = Math.max(8, (caseCount / maxCaseCount) * 100);
            const stepsHtml = patternSteps
                .map((step, stepIndex) => {
                    const arrow = stepIndex === patternSteps.length - 1
                        ? ""
                        : '<span class="pattern-flow-arrow">→</span>';

                    return `
                        <div class="pattern-flow-step-group">
                            <div class="pattern-flow-step">${escapeHtml(step)}</div>
                            ${arrow}
                        </div>
                    `;
                })
                .join("");

            return `
                <article class="pattern-flow-card">
                    <div class="pattern-flow-card-head">
                        <div>
                            <p class="pattern-flow-rank">Pattern ${index + 1}</p>
                            <h3>${escapeHtml(caseCount.toLocaleString("ja-JP"))}ケース</h3>
                        </div>
                        <div class="pattern-flow-meta">
                            <span>${escapeHtml(String(caseRatio))}%</span>
                            <span>平均${escapeHtml(String(averageCaseDuration))}分</span>
                        </div>
                    </div>
                    <div class="pattern-flow-meter">
                        <div class="pattern-flow-meter-bar" style="width: ${widthPercent}%"></div>
                    </div>
                    <div class="pattern-flow-steps">
                        ${stepsHtml}
                    </div>
                </article>
            `;
        })
        .join("");

    chartPanel.className = "result-panel";
    chartTitle.textContent = "業務全体フロー図";
    chartNote.textContent = "大量データでは初期表示を自動で絞っています。右側のバーで表示率を調整できます。ノードは件数が多いほど濃く、線は件数が多いほど太く表示します。";
    chartContainer.innerHTML = `
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
            </aside>
        </div>
        <div class="pattern-flow-list">
            ${cardsHtml}
        </div>
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
        const detailData = await loadAnalysisPage(runId, 0);
        const analysis = detailData.analyses[analysisKey];

        if (!analysis) {
            throw new Error("指定した分析結果が見つかりません。");
        }

        const renderAnalysisPage = async (rowOffset) => {
            const currentVersion = detailRequestVersion + 1;
            detailRequestVersion = currentVersion;
            setStatus("表を読み込んでいます...", "info");

            try {
                const pageData = await loadAnalysisPage(runId, rowOffset);

                if (currentVersion !== detailRequestVersion) {
                    return;
                }

                const pageAnalysis = pageData.analyses[analysisKey];
                if (!pageAnalysis) {
                    throw new Error("指定した分析結果が見つかりません。");
                }

                renderResult(pageAnalysis, runId, renderAnalysisPage);
                hideStatus();
            } catch (error) {
                if (currentVersion !== detailRequestVersion) {
                    return;
                }

                setStatus(error.message, "error");
            }
        };

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
