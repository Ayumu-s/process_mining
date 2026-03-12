const STORAGE_KEY = "processMiningLastResult";

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
                    const isPatternLink = (
                        tableAnalysisKey === "pattern"
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

function renderSummary(data, analysis) {
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
            <strong>${escapeHtml(analysis.rows.length)}</strong>
        </article>
    `;
}

function renderResult(analysis, runId = "") {
    const tableRows = analysis.rows.map((row, index) => ({ ...row, __rowIndex: index }));
    resultPanel.className = "result-panel";
    resultPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>${escapeHtml(analysis.analysis_name)}</h2>
                <p class="result-meta">全 ${escapeHtml(analysis.rows.length)} 件を表示</p>
                ${analysis.excel_file ? `<p class="excel-path">Excel: ${escapeHtml(analysis.excel_file)}</p>` : ""}
            </div>
        </div>
        ${buildTable(tableRows, { analysisKey, runId })}
    `;
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

    return { nodes, edges };
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

function buildProcessFlowPath(edge, sourceNode, targetNode, nodeWidth) {
    const startX = sourceNode.x + nodeWidth;
    const startY = edge.sourceOffsetY;
    const endX = targetNode.x;
    const endY = edge.targetOffsetY;

    if (targetNode.layer > sourceNode.layer) {
        const curveOffset = Math.max(96, (endX - startX) * 0.42);
        return `M ${startX} ${startY} C ${startX + curveOffset} ${startY}, ${endX - curveOffset} ${endY}, ${endX} ${endY}`;
    }

    const routeX = getProcessFlowRouteX(sourceNode, targetNode, nodeWidth);
    return `M ${startX} ${startY} C ${routeX} ${startY}, ${routeX} ${endY}, ${endX} ${endY}`;
}

function getProcessFlowRouteX(sourceNode, targetNode, nodeWidth) {
    const startX = sourceNode.x + nodeWidth;
    const endX = targetNode.x;

    return Math.max(startX, endX) + 210 + Math.abs(targetNode.layer - sourceNode.layer) * 32;
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
            stroke: rgba(255, 250, 242, 0.96);
            stroke-width: 4px;
            stroke-linejoin: round;
            font-family: "BIZ UDPGothic", "Yu Gothic UI", sans-serif;
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
    backgroundRect.setAttribute("fill", "#fffaf2");
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

    if (!nodes.length || !edges.length) {
        return '<p class="empty-state">フロー図を作れるデータがありません。</p>';
    }

    const chartLeft = 40;
    const chartTop = 36;
    const nodeWidth = 176;
    const nodeHeight = 48;
    const layerGap = 300;
    const rowGap = 78;
    const nodesByLayer = new Map();

    nodes
        .sort((left, right) => {
            if (left.layer !== right.layer) {
                return left.layer - right.layer;
            }

            if (left.orderScore !== right.orderScore) {
                return left.orderScore - right.orderScore;
            }

            return right.weight - left.weight;
        })
        .forEach((node) => {
            if (!nodesByLayer.has(node.layer)) {
                nodesByLayer.set(node.layer, []);
            }

            nodesByLayer.get(node.layer).push(node);
        });

    const layerKeys = Array.from(nodesByLayer.keys()).sort((left, right) => left - right);

    layerKeys.forEach((layerKey) => {
        const layerNodes = nodesByLayer.get(layerKey);
        layerNodes.forEach((node, index) => {
            node.x = chartLeft + layerKey * layerGap;
            node.y = chartTop + index * rowGap;
        });
    });

    const nodeLookup = new Map(nodes.map((node) => [node.name, node]));
    const maxNodeRight = Math.max(...nodes.map((node) => node.x + nodeWidth), chartLeft + nodeWidth);
    const maxRouteX = edges.reduce((currentMax, edge) => {
        const sourceNode = nodeLookup.get(edge.source);
        const targetNode = nodeLookup.get(edge.target);

        if (!sourceNode || !targetNode || targetNode.layer > sourceNode.layer) {
            return currentMax;
        }

        return Math.max(currentMax, getProcessFlowRouteX(sourceNode, targetNode, nodeWidth));
    }, maxNodeRight);
    const chartWidth = Math.max(
        1460,
        maxNodeRight + 180,
        maxRouteX + 150
    );
    const chartHeight = Math.max(
        360,
        chartTop + Math.max(...nodes.map((node) => node.y), 0) + nodeHeight + 48
    );
    const maxEdgeCount = Math.max(...edges.map((edge) => edge.count), 1);
    const maxNodeWeight = Math.max(...nodes.map((node) => node.weight), 1);
    const labelState = buildProcessMapLabelState(edges, labelPercent);
    const outgoingEdgeMap = new Map();
    const incomingEdgeMap = new Map();

    edges.forEach((edge) => {
        if (!outgoingEdgeMap.has(edge.source)) {
            outgoingEdgeMap.set(edge.source, []);
        }

        if (!incomingEdgeMap.has(edge.target)) {
            incomingEdgeMap.set(edge.target, []);
        }

        outgoingEdgeMap.get(edge.source).push(edge);
        incomingEdgeMap.get(edge.target).push(edge);
    });

    nodes.forEach((node) => {
        const outgoingEdges = outgoingEdgeMap.get(node.name) || [];
        const incomingEdges = incomingEdgeMap.get(node.name) || [];

        outgoingEdges
            .sort((left, right) => {
                const leftTarget = nodeLookup.get(left.target);
                const rightTarget = nodeLookup.get(right.target);
                const leftY = leftTarget ? leftTarget.y : 0;
                const rightY = rightTarget ? rightTarget.y : 0;

                if (leftY !== rightY) {
                    return leftY - rightY;
                }

                return right.count - left.count;
            })
            .forEach((edge, index) => {
                const usableHeight = nodeHeight - 16;
                edge.sourceOffsetY = node.y + 8 + ((index + 1) * usableHeight) / (outgoingEdges.length + 1);
            });

        incomingEdges
            .sort((left, right) => {
                const leftSource = nodeLookup.get(left.source);
                const rightSource = nodeLookup.get(right.source);
                const leftY = leftSource ? leftSource.y : 0;
                const rightY = rightSource ? rightSource.y : 0;

                if (leftY !== rightY) {
                    return leftY - rightY;
                }

                return right.count - left.count;
            })
            .forEach((edge, index) => {
                const usableHeight = nodeHeight - 16;
                edge.targetOffsetY = node.y + 8 + ((index + 1) * usableHeight) / (incomingEdges.length + 1);
            });
    });

    const edgesSvg = edges
        .map((edge) => {
            const sourceNode = nodeLookup.get(edge.source);
            const targetNode = nodeLookup.get(edge.target);

            if (!sourceNode || !targetNode) {
                return "";
            }

            const opacity = 0.14 + (edge.count / maxEdgeCount) * 0.86;
            const strokeWidth = 2 + (edge.count / maxEdgeCount) * 8;
            const isBackward = targetNode.layer <= sourceNode.layer;
            const routeX = isBackward
                ? getProcessFlowRouteX(sourceNode, targetNode, nodeWidth)
                : 0;
            const edgeClassName = isBackward
                ? "process-map-edge process-map-edge--return"
                : "process-map-edge";
            const edgeLabelClassName = isBackward
                ? "process-map-edge-label process-map-edge-label--return"
                : "process-map-edge-label";
            const edgeMarkerId = isBackward
                ? "process-map-arrow-return"
                : "process-map-arrow";
            const edgeLabelX = isBackward
                ? routeX - 44
                : (sourceNode.x + targetNode.x + nodeWidth) / 2;
            const edgeLabelY = (edge.sourceOffsetY + edge.targetOffsetY) / 2 - 10;
            const showLabel = labelState.visibleLabelKeys.has(getProcessFlowEdgeKey(edge));

            return `
                <path
                    d="${buildProcessFlowPath(edge, sourceNode, targetNode, nodeWidth)}"
                    class="${edgeClassName}"
                    marker-end="url(#${edgeMarkerId})"
                    style="stroke-width: ${strokeWidth}; opacity: ${opacity};"
                ></path>
                ${showLabel ? `
                    <text x="${edgeLabelX}" y="${edgeLabelY}" class="${edgeLabelClassName}">
                        ${escapeHtml(edge.count.toLocaleString("ja-JP"))}件
                    </text>
                ` : ""}
            `;
        })
        .join("");

    const nodesSvg = nodes
        .map((node) => {
            const lines = wrapJapaneseLabel(node.name, 10, 2);
            const weightRatio = node.weight / maxNodeWeight;
            const fillColor = `rgba(48, 96, 212, ${0.1 + weightRatio * 0.78})`;
            const strokeColor = `rgba(35, 75, 176, ${0.3 + weightRatio * 0.62})`;
            const labelColor = weightRatio >= 0.55 ? "#ffffff" : "#1f335e";
            const labelSvg = lines
                .map((line, index) => {
                    const y = lines.length === 1
                        ? node.y + 27
                        : node.y + 21 + index * 15;

                    return `
                        <text x="${node.x + 16}" y="${y}" class="process-map-node-label" style="fill: ${labelColor};">
                            ${escapeHtml(line)}
                        </text>
                    `;
                })
                .join("");

            return `
                <rect
                    x="${node.x}"
                    y="${node.y}"
                    width="${nodeWidth}"
                    height="${nodeHeight}"
                    rx="14"
                    ry="14"
                    class="process-map-node"
                    style="fill: ${fillColor}; stroke: ${strokeColor};"
                ></rect>
                ${labelSvg}
            `;
        })
        .join("");

    return `
        <div class="process-map-wrap">
            <svg
                class="process-map-svg"
                width="${chartWidth}"
                height="${chartHeight}"
                viewBox="0 0 ${chartWidth} ${chartHeight}"
                role="img"
                aria-label="業務全体フロー図"
                preserveAspectRatio="xMinYMin meet"
            >
                <defs>
                    <marker
                        id="process-map-arrow"
                        markerUnits="userSpaceOnUse"
                        markerWidth="12"
                        markerHeight="12"
                        refX="11"
                        refY="6"
                        orient="auto"
                    >
                        <path d="M 0 0 L 10 5 L 0 10 z" fill="#2458d3"></path>
                    </marker>
                    <marker
                        id="process-map-arrow-return"
                        markerUnits="userSpaceOnUse"
                        markerWidth="12"
                        markerHeight="12"
                        refX="11"
                        refY="6"
                        orient="auto"
                    >
                        <path d="M 0 0 L 10 5 L 0 10 z" fill="#cf7a45"></path>
                    </marker>
                </defs>
                ${edgesSvg}
                ${nodesSvg}
            </svg>
        </div>
    `;
}

function renderProcessFlowMap(patternRows, transitionRows = [], frequencyRows = [], options = {}) {
    const flowData = buildProcessFlowData(patternRows, transitionRows, frequencyRows);
    return renderProcessFlowMapFromData(flowData, options);
}

function initializePatternFlowExplorer(analysis, latestResult) {
    const mapViewport = document.getElementById("process-map-viewport");
    const activitiesSlider = document.getElementById("process-map-activities-slider");
    const connectionsSlider = document.getElementById("process-map-connections-slider");
    const labelsSlider = document.getElementById("process-map-labels-slider");
    const activitiesValue = document.getElementById("process-map-activities-value");
    const connectionsValue = document.getElementById("process-map-connections-value");
    const labelsValue = document.getElementById("process-map-labels-value");
    const activitiesMeta = document.getElementById("process-map-activities-meta");
    const connectionsMeta = document.getElementById("process-map-connections-meta");
    const labelsMeta = document.getElementById("process-map-labels-meta");
    const exportSvgButton = document.getElementById("process-map-export-svg");
    const exportPngButton = document.getElementById("process-map-export-png");

    if (!mapViewport || !activitiesSlider || !connectionsSlider || !labelsSlider) {
        return;
    }

    const transitionRows = latestResult.analyses.transition
        ? latestResult.analyses.transition.rows
        : [];
    const frequencyRows = latestResult.analyses.frequency
        ? latestResult.analyses.frequency.rows
        : [];
    const flowData = buildProcessFlowData(analysis.rows, transitionRows, frequencyRows);

    function updateProcessMap() {
        const activityPercent = Number(activitiesSlider.value);
        const connectionPercent = Number(connectionsSlider.value);
        const labelPercent = Number(labelsSlider.value);
        const filteredData = filterProcessFlowData(
            flowData.nodes,
            flowData.edges,
            activityPercent,
            connectionPercent
        );
        const labelState = buildProcessMapLabelState(filteredData.edges, labelPercent);

        activitiesValue.textContent = `${activityPercent}%`;
        connectionsValue.textContent = `${connectionPercent}%`;
        labelsValue.textContent = `${labelPercent}%`;
        activitiesMeta.textContent = `${filteredData.nodes.length} / ${filteredData.totalNodeCount} activities`;
        connectionsMeta.textContent = `${filteredData.edges.length} / ${filteredData.totalEdgeCount} connections`;
        labelsMeta.textContent = `${labelState.visibleLabelCount} / ${labelState.totalLabelCount} labels`;
        mapViewport.innerHTML = renderProcessFlowMapFromData(flowData, {
            activityPercent,
            connectionPercent,
            labelPercent,
        });
    }

    activitiesSlider.addEventListener("input", updateProcessMap);
    connectionsSlider.addEventListener("input", updateProcessMap);
    labelsSlider.addEventListener("input", updateProcessMap);
    if (exportSvgButton) {
        exportSvgButton.addEventListener("click", () => {
            exportProcessMapSvg(
                `process_flow_map_${activitiesSlider.value}_${connectionsSlider.value}_${labelsSlider.value}.svg`
            );
        });
    }
    if (exportPngButton) {
        exportPngButton.addEventListener("click", () => {
            exportProcessMapPng(
                `process_flow_map_${activitiesSlider.value}_${connectionsSlider.value}_${labelsSlider.value}.png`
            );
        });
    }
    updateProcessMap();
}

function renderPatternChart(analysis, latestResult) {
    const chartRows = analysis.rows.slice(0, 8);

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
    chartNote.textContent = "右側のバーで表示率を絞れます。ノードは件数が多いほど濃く、線は件数が多いほど太く濃く表示しています。下に上位8件の処理順パターンも表示しています。";
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
                        <span>Activities</span>
                        <strong id="process-map-activities-value">100%</strong>
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
                            value="100"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-activities-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Connections</span>
                        <strong id="process-map-connections-value">100%</strong>
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
                            value="100"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-connections-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>Labels</span>
                        <strong id="process-map-labels-value">100%</strong>
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
                            value="100"
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
    initializePatternFlowExplorer(analysis, latestResult);
}

function renderChart(analysis, latestResult) {
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
        renderPatternChart(analysis, latestResult);
    }
}

function renderDetailPage() {
    const latestResult = loadLatestResult();

    if (!latestResult) {
        setStatus("TOP 画面で分析を実行してから詳細ページを開いてください。", "error");
        return;
    }

    const analysis = latestResult.analyses[analysisKey];

    if (!analysis) {
        setStatus("指定した分析結果が見つかりません。TOP 画面で分析対象を確認してください。", "error");
        return;
    }

    detailPageTitle.textContent = analysis.analysis_name;
    detailPageCopy.textContent = "直前に実行した分析結果の全件を表示しています。";
    renderSummary(latestResult, analysis);
    renderChart(analysis, latestResult);
    renderResult(analysis, latestResult.run_id || "");
}

renderDetailPage();
