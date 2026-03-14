import math
from collections import defaultdict

from 共通スクリプト.data_loader import prepare_event_log, read_csv_data
from 共通スクリプト.Excel出力.excel_exporter import (
    convert_analysis_result_to_records,
    export_analysis_to_excel,
)
from 共通スクリプト.分析.前後処理分析.transition_analysis import (
    ANALYSIS_CONFIG as TRANSITION_ANALYSIS_CONFIG,
    create_transition_analysis,
)
from 共通スクリプト.分析.処理順パターン分析.pattern_analysis import (
    ANALYSIS_CONFIG as PATTERN_ANALYSIS_CONFIG,
    create_pattern_analysis,
)
from 共通スクリプト.分析.頻度分析.frequency_analysis import (
    ANALYSIS_CONFIG as FREQUENCY_ANALYSIS_CONFIG,
    create_frequency_analysis,
)


ANALYSIS_DEFINITIONS = {
    "frequency": {
        "create_function": create_frequency_analysis,
        "config": FREQUENCY_ANALYSIS_CONFIG,
    },
    "transition": {
        "create_function": create_transition_analysis,
        "config": TRANSITION_ANALYSIS_CONFIG,
    },
    "pattern": {
        "create_function": create_pattern_analysis,
        "config": PATTERN_ANALYSIS_CONFIG,
    },
}

DEFAULT_ANALYSIS_KEYS = ["frequency", "transition", "pattern"]

FLOW_FREQUENCY_ACTIVITY_COLUMN = "アクティビティ"
FLOW_FREQUENCY_EVENT_COUNT_COLUMN = "イベント件数"
FLOW_FREQUENCY_CASE_COUNT_COLUMN = "ケース数"
FLOW_TRANSITION_FROM_COLUMN = "前処理アクティビティ名"
FLOW_TRANSITION_TO_COLUMN = "後処理アクティビティ名"
FLOW_TRANSITION_COUNT_COLUMN = "遷移件数"
FLOW_PATTERN_CASE_COUNT_COLUMN = "ケース数"
FLOW_PATTERN_COLUMN = "処理順パターン"
FLOW_PATH_SEPARATOR = "→"
FLOW_PATTERN_CAP = 500
FLOW_LAYOUT_SWEEP_ITERATIONS = 4


def get_available_analysis_definitions():
    return ANALYSIS_DEFINITIONS.copy()


def resolve_analysis_keys(selected_analysis_keys=None):
    if selected_analysis_keys is None:
        analysis_keys = DEFAULT_ANALYSIS_KEYS
    else:
        analysis_keys = selected_analysis_keys

    if not analysis_keys:
        raise ValueError("少なくとも1つの分析を選択してください。")

    return analysis_keys


def load_prepared_event_log(
    file_source,
    case_id_column,
    activity_column,
    timestamp_column,
):
    raw_df = read_csv_data(
        file_path=file_source,
        case_id_column=case_id_column,
        activity_column=activity_column,
    )
    return prepare_event_log(
        df=raw_df,
        case_id_column=case_id_column,
        activity_column=activity_column,
        timestamp_column=timestamp_column,
    )


def analyze_prepared_event_log(
    prepared_df,
    selected_analysis_keys=None,
    output_root_dir=None,
    export_excel=False,
):
    analysis_keys = resolve_analysis_keys(selected_analysis_keys)
    analysis_results = {}

    for analysis_key in analysis_keys:
        if analysis_key not in ANALYSIS_DEFINITIONS:
            raise ValueError(f"未対応の分析キーです: {analysis_key}")

        definition = ANALYSIS_DEFINITIONS[analysis_key]
        result_df = definition["create_function"](prepared_df)
        analysis_config = definition["config"]

        excel_file = None
        if export_excel:
            excel_file = export_analysis_to_excel(
                df=result_df,
                output_root_dir=output_root_dir,
                analysis_name=analysis_config["analysis_name"],
                output_file_name=analysis_config["output_file_name"],
                sheet_name=analysis_config["sheet_name"],
                display_columns=analysis_config["display_columns"],
            )

        analysis_results[analysis_key] = {
            "analysis_name": analysis_config["analysis_name"],
            "sheet_name": analysis_config["sheet_name"],
            "output_file_name": analysis_config["output_file_name"],
            "rows": convert_analysis_result_to_records(
                result_df,
                analysis_config["display_columns"],
            ),
            "excel_file": str(excel_file.resolve()) if excel_file else None,
        }

    return {
        "case_count": int(prepared_df["case_id"].nunique()),
        "event_count": int(len(prepared_df)),
        "analyses": analysis_results,
    }


def build_case_pattern_table(prepared_df):
    return (
        prepared_df.sort_values(["case_id", "sequence_no"])
        .groupby("case_id")["activity"]
        .apply(lambda series: FLOW_PATH_SEPARATOR.join(series.tolist()))
        .reset_index(name="pattern")
    )


def clamp_flow_percent(percent):
    try:
        numeric_percent = int(percent)
    except (TypeError, ValueError):
        numeric_percent = 0

    return max(0, min(100, numeric_percent))


def _calculate_flow_limit(total_count, percent, minimum=1):
    if total_count <= 0 or percent <= 0:
        return 0

    return min(total_count, max(minimum, math.ceil(total_count * (percent / 100))))


def _parse_pattern_steps(row):
    pattern = str(row.get(FLOW_PATTERN_COLUMN) or "").strip()
    if not pattern:
        return []

    return [
        step.strip()
        for step in pattern.split(FLOW_PATH_SEPARATOR)
        if step.strip()
    ]


def _build_flow_graph(pattern_rows, transition_rows=None, frequency_rows=None):
    transition_rows = transition_rows or []
    frequency_rows = frequency_rows or []
    node_map = {}
    edge_map = {}

    def ensure_node(name):
        node_name = str(name or "").strip()
        if not node_name:
            return None

        if node_name not in node_map:
            node_map[node_name] = {
                "name": node_name,
                "weight": 0,
                "caseWeight": 0,
                "positionTotal": 0,
                "positionWeight": 0,
                "incoming": 0,
                "outgoing": 0,
                "layerScore": 0,
                "layer": 0,
                "orderScore": 0,
            }

        return node_map[node_name]

    for row in frequency_rows:
        activity_name = str(row.get(FLOW_FREQUENCY_ACTIVITY_COLUMN) or "").strip()
        if not activity_name:
            continue

        node = ensure_node(activity_name)
        node["weight"] = max(node["weight"], int(row.get(FLOW_FREQUENCY_EVENT_COUNT_COLUMN) or 0))
        node["caseWeight"] = max(node["caseWeight"], int(row.get(FLOW_FREQUENCY_CASE_COUNT_COLUMN) or 0))

    for row in pattern_rows:
        case_count = int(row.get(FLOW_PATTERN_CASE_COUNT_COLUMN) or 0)
        steps = _parse_pattern_steps(row)

        for step_index, step in enumerate(steps):
            node = ensure_node(step)
            node["positionTotal"] += step_index * case_count
            node["positionWeight"] += case_count

            if node["weight"] == 0:
                node["weight"] = case_count

            if node["caseWeight"] == 0:
                node["caseWeight"] = case_count

            if step_index == len(steps) - 1:
                continue

            next_step = steps[step_index + 1]
            ensure_node(next_step)

            if transition_rows:
                continue

            edge_key = (step, next_step)
            if edge_key not in edge_map:
                edge_map[edge_key] = {
                    "source": step,
                    "target": next_step,
                    "count": 0,
                }

            edge_map[edge_key]["count"] += case_count

    for row in transition_rows:
        source_name = str(row.get(FLOW_TRANSITION_FROM_COLUMN) or "").strip()
        target_name = str(row.get(FLOW_TRANSITION_TO_COLUMN) or "").strip()
        transition_count = int(row.get(FLOW_TRANSITION_COUNT_COLUMN) or 0)

        if not source_name or not target_name or transition_count <= 0:
            continue

        ensure_node(source_name)
        ensure_node(target_name)

        edge_key = (source_name, target_name)
        if edge_key not in edge_map:
            edge_map[edge_key] = {
                "source": source_name,
                "target": target_name,
                "count": 0,
            }

        edge_map[edge_key]["count"] = max(edge_map[edge_key]["count"], transition_count)

    nodes = list(node_map.values())
    edges = [
        edge
        for edge in edge_map.values()
        if edge["source"] != edge["target"] and edge["count"] > 0
    ]
    node_lookup = {node["name"]: node for node in nodes}

    for edge in edges:
        source_node = node_lookup.get(edge["source"])
        target_node = node_lookup.get(edge["target"])

        if source_node:
            source_node["outgoing"] += edge["count"]

        if target_node:
            target_node["incoming"] += edge["count"]

    for node in nodes:
        if node["positionWeight"] > 0:
            node["layerScore"] = node["positionTotal"] / node["positionWeight"]
        else:
            node["layerScore"] = 0

        node["layer"] = max(0, round(node["layerScore"]))

        if node["weight"] == 0:
            node["weight"] = max(node["incoming"], node["outgoing"], node["caseWeight"], 1)

        if node["caseWeight"] == 0:
            node["caseWeight"] = max(node["incoming"], node["outgoing"], node["weight"], 1)

    return _apply_flow_layout(nodes, edges)


def _filter_flow_graph(nodes, edges, activity_percent=100, connection_percent=100):
    total_node_count = len(nodes)
    total_edge_count = len(edges)

    if not total_node_count or not total_edge_count:
        return {
            "nodes": [],
            "edges": [],
            "available_activity_count": total_node_count,
            "visible_activity_count": 0,
            "available_connection_count": total_edge_count,
            "visible_connection_count": 0,
        }

    requested_activity_percent = clamp_flow_percent(activity_percent)
    requested_connection_percent = clamp_flow_percent(connection_percent)
    activity_limit = _calculate_flow_limit(
        total_node_count,
        requested_activity_percent,
        minimum=2 if total_node_count > 1 else 1,
    )

    selected_nodes = sorted(
        nodes,
        key=lambda node: (-node["weight"], node["name"]),
    )[:activity_limit]
    selected_node_names = {node["name"] for node in selected_nodes}

    candidate_edges = [
        edge
        for edge in edges
        if edge["source"] in selected_node_names and edge["target"] in selected_node_names
    ]
    connection_limit = _calculate_flow_limit(
        len(candidate_edges),
        requested_connection_percent,
    )
    selected_edges = candidate_edges[:connection_limit]

    visible_node_names = set()
    for edge in selected_edges:
        visible_node_names.add(edge["source"])
        visible_node_names.add(edge["target"])

    visible_nodes = [
        {
            **node,
        }
        for node in selected_nodes
        if node["name"] in visible_node_names
    ]
    visible_edges = [
        {
            **edge,
        }
        for edge in selected_edges
    ]
    # Re-index orderScore to keep it compact for the subset, but keep layer/weight from parent
    nodes_by_layer = defaultdict(list)
    for n in visible_nodes:
        nodes_by_layer[n["layer"]].append(n)
    for layer in nodes_by_layer:
        nodes_by_layer[layer].sort(key=lambda x: (x.get("orderScore", 0), x["name"]))
        for i, n in enumerate(nodes_by_layer[layer]):
            n["orderScore"] = i

    return {
        "nodes": visible_nodes,
        "edges": visible_edges,
        "available_activity_count": total_node_count,
        "visible_activity_count": len(visible_nodes),
        "available_connection_count": total_edge_count,
        "visible_connection_count": len(visible_edges),
    }


def _reindex_layer_nodes(layer_nodes):
    for index, node in enumerate(layer_nodes):
        node["orderScore"] = index


def _count_edge_crossings(edges, node_lookup):
    crossing_score = 0

    for left_index, left_edge in enumerate(edges):
        left_source = node_lookup.get(left_edge["source"])
        left_target = node_lookup.get(left_edge["target"])

        if not left_source or not left_target:
            continue

        for right_edge in edges[left_index + 1:]:
            right_source = node_lookup.get(right_edge["source"])
            right_target = node_lookup.get(right_edge["target"])

            if not right_source or not right_target:
                continue

            source_diff = left_source["orderScore"] - right_source["orderScore"]
            target_diff = left_target["orderScore"] - right_target["orderScore"]

            if source_diff == 0 or target_diff == 0:
                continue

            if source_diff * target_diff < 0:
                crossing_score += min(left_edge["count"], right_edge["count"])

    return crossing_score


def _count_layer_crossings(layer, edges, node_lookup):
    outgoing_groups = defaultdict(list)
    incoming_groups = defaultdict(list)

    for edge in edges:
        source_node = node_lookup.get(edge["source"])
        target_node = node_lookup.get(edge["target"])

        if not source_node or not target_node:
            continue

        if source_node["layer"] == layer and target_node["layer"] > layer:
            outgoing_groups[target_node["layer"]].append(edge)

        if target_node["layer"] == layer and source_node["layer"] < layer:
            incoming_groups[source_node["layer"]].append(edge)

    return sum(
        _count_edge_crossings(group_edges, node_lookup)
        for group_edges in outgoing_groups.values()
    ) + sum(
        _count_edge_crossings(group_edges, node_lookup)
        for group_edges in incoming_groups.values()
    )


def _optimize_layer_by_swaps(layer_nodes, edges, node_lookup, max_swaps=100):
    if len(layer_nodes) < 2:
        return

    layer = layer_nodes[0]["layer"]
    updated = True
    swap_count = 0

    while updated and swap_count < max_swaps:
        updated = False

        for index in range(len(layer_nodes) - 1):
            current_score = _count_layer_crossings(layer, edges, node_lookup)
            first_node = layer_nodes[index]
            second_node = layer_nodes[index + 1]

            layer_nodes[index], layer_nodes[index + 1] = second_node, first_node
            _reindex_layer_nodes(layer_nodes)

            swapped_score = _count_layer_crossings(layer, edges, node_lookup)
            if swapped_score < current_score:
                updated = True
                swap_count += 1
                if swap_count >= max_swaps:
                    break
                continue

            layer_nodes[index], layer_nodes[index + 1] = first_node, second_node
            _reindex_layer_nodes(layer_nodes)


def _incoming_barycenter(node, edges, node_lookup):
    total_weight = 0
    total_score = 0

    for edge in edges:
        if edge["target"] != node["name"]:
            continue

        source_node = node_lookup.get(edge["source"])
        if not source_node or source_node["layer"] >= node["layer"]:
            continue

        distance = max(1, node["layer"] - source_node["layer"])
        weight = edge["count"] / distance
        total_weight += weight
        total_score += source_node["orderScore"] * weight

    if total_weight == 0:
        return node["orderScore"]

    return total_score / total_weight


def _outgoing_barycenter(node, edges, node_lookup):
    total_weight = 0
    total_score = 0

    for edge in edges:
        if edge["source"] != node["name"]:
            continue

        target_node = node_lookup.get(edge["target"])
        if not target_node or target_node["layer"] <= node["layer"]:
            continue

        distance = max(1, target_node["layer"] - node["layer"])
        weight = edge["count"] / distance
        total_weight += weight
        total_score += target_node["orderScore"] * weight

    if total_weight == 0:
        return node["orderScore"]

    return total_score / total_weight


def _apply_flow_layout(nodes, edges):
    if not nodes:
        return [], []

    edges = sorted(edges, key=lambda edge: (-edge["count"], edge["source"], edge["target"]))

    layer_values = sorted({node["layer"] for node in nodes})
    layer_map = {layer_value: index for index, layer_value in enumerate(layer_values)}
    nodes_by_layer = defaultdict(list)

    for node in nodes:
        node["layer"] = layer_map[node["layer"]]
        nodes_by_layer[node["layer"]].append(node)

    for layer in sorted(nodes_by_layer):
        nodes_by_layer[layer].sort(
            key=lambda node: (node["layerScore"], -node["weight"], node["name"])
        )
        _reindex_layer_nodes(nodes_by_layer[layer])

    node_lookup = {node["name"]: node for node in nodes}
    max_layer = max(nodes_by_layer) if nodes_by_layer else 0

    # Repeat the sweep so dense graphs keep a stable left-to-right order.
    for _ in range(FLOW_LAYOUT_SWEEP_ITERATIONS):
        for layer in range(1, max_layer + 1):
            layer_nodes = nodes_by_layer.get(layer, [])
            layer_nodes.sort(
                key=lambda node: (
                    _incoming_barycenter(node, edges, node_lookup),
                    -node["weight"],
                    node["name"],
                )
            )
            _reindex_layer_nodes(layer_nodes)

        for layer in range(max_layer - 1, -1, -1):
            layer_nodes = nodes_by_layer.get(layer, [])
            layer_nodes.sort(
                key=lambda node: (
                    _outgoing_barycenter(node, edges, node_lookup),
                    -node["weight"],
                    node["name"],
                )
            )
            _reindex_layer_nodes(layer_nodes)

    for layer in range(1, max_layer):
        # Dense graph safety: don't spend too much time on huge layers
        layer_nodes = nodes_by_layer.get(layer, [])
        if len(layer_nodes) > 50:
            continue
        _optimize_layer_by_swaps(layer_nodes, edges, node_lookup, max_swaps=50)

    ordered_nodes = []
    for layer in sorted(nodes_by_layer):
        ordered_nodes.extend(
            sorted(
                nodes_by_layer[layer],
                key=lambda node: (node["orderScore"], -node["weight"], node["name"]),
            )
        )

    return ordered_nodes, edges


def create_pattern_flow_snapshot(
    pattern_rows,
    frequency_rows=None,
    pattern_percent=10,
    pattern_count=None,
    activity_percent=40,
    connection_percent=30,
    pattern_cap=FLOW_PATTERN_CAP,
):
    frequency_rows = frequency_rows or []
    cap = max(0, int(pattern_cap or 0))
    requested_pattern_percent = clamp_flow_percent(pattern_percent)
    requested_activity_percent = clamp_flow_percent(activity_percent)
    requested_connection_percent = clamp_flow_percent(connection_percent)
    sorted_pattern_rows = sorted(
        pattern_rows,
        key=lambda row: (
            -int(row.get(FLOW_PATTERN_CASE_COUNT_COLUMN) or 0),
            str(row.get(FLOW_PATTERN_COLUMN) or ""),
        ),
    )

    effective_pattern_count = min(len(sorted_pattern_rows), cap)
    requested_pattern_count = None if pattern_count is None else max(0, int(pattern_count or 0))
    if requested_pattern_count is None:
        used_pattern_count = _calculate_flow_limit(
            effective_pattern_count,
            requested_pattern_percent,
        )
    else:
        used_pattern_count = min(effective_pattern_count, requested_pattern_count)
    selected_pattern_rows = sorted_pattern_rows[:used_pattern_count]
    nodes, edges = _build_flow_graph(
        pattern_rows=selected_pattern_rows,
        transition_rows=[],
        frequency_rows=frequency_rows,
    )
    filtered_graph = _filter_flow_graph(
        nodes=nodes,
        edges=edges,
        activity_percent=requested_activity_percent,
        connection_percent=requested_connection_percent,
    )

    return {
        "pattern_window": {
            "requested_percent": requested_pattern_percent,
            "requested_count": requested_pattern_count,
            "total_pattern_count": len(sorted_pattern_rows),
            "effective_pattern_count": effective_pattern_count,
            "used_pattern_count": used_pattern_count,
            "cap": cap,
        },
        "activity_window": {
            "requested_percent": requested_activity_percent,
            "available_activity_count": filtered_graph["available_activity_count"],
            "visible_activity_count": filtered_graph["visible_activity_count"],
        },
        "connection_window": {
            "requested_percent": requested_connection_percent,
            "available_connection_count": filtered_graph["available_connection_count"],
            "visible_connection_count": filtered_graph["visible_connection_count"],
        },
        "flow_data": {
            "nodes": filtered_graph["nodes"],
            "edges": filtered_graph["edges"],
        },
    }


def create_pattern_bottleneck_details(prepared_df, pattern):
    case_pattern_df = build_case_pattern_table(prepared_df)
    matched_case_ids = case_pattern_df.loc[case_pattern_df["pattern"] == pattern, "case_id"]

    if matched_case_ids.empty:
        raise ValueError("指定した処理順パターンが見つかりません。")

    pattern_df = (
        prepared_df[prepared_df["case_id"].isin(matched_case_ids)]
        .sort_values(["case_id", "sequence_no"])
        .copy()
    )
    pattern_df["next_activity"] = pattern_df.groupby("case_id")["activity"].shift(-1)
    transition_df = pattern_df[pattern_df["next_activity"].notna()].copy()

    if transition_df.empty:
        step_metrics = []
        bottleneck_transition = None
    else:
        step_metrics_df = (
            transition_df.groupby(["sequence_no", "activity", "next_activity"])
            .agg(
                case_count=("case_id", "count"),
                avg_duration_min=("duration_min", "mean"),
                median_duration_min=("duration_min", "median"),
                min_duration_min=("duration_min", "min"),
                max_duration_min=("duration_min", "max"),
                total_duration_min=("duration_min", "sum"),
            )
            .reset_index()
            .sort_values(["sequence_no", "activity", "next_activity"])
            .reset_index(drop=True)
        )
        numeric_columns = [
            "avg_duration_min",
            "median_duration_min",
            "min_duration_min",
            "max_duration_min",
            "total_duration_min",
        ]
        step_metrics_df[numeric_columns] = step_metrics_df[numeric_columns].round(2)

        total_wait_min = step_metrics_df["total_duration_min"].sum()
        if total_wait_min > 0:
            step_metrics_df["wait_share_pct"] = (
                step_metrics_df["total_duration_min"] / total_wait_min * 100
            ).round(2)
        else:
            step_metrics_df["wait_share_pct"] = 0.0

        step_metrics_df["transition_label"] = (
            step_metrics_df["activity"] + " → " + step_metrics_df["next_activity"]
        )
        step_metrics = [
            {
                "sequence_no": int(row["sequence_no"]),
                "activity": row["activity"],
                "next_activity": row["next_activity"],
                "case_count": int(row["case_count"]),
                "avg_duration_min": float(row["avg_duration_min"]),
                "median_duration_min": float(row["median_duration_min"]),
                "min_duration_min": float(row["min_duration_min"]),
                "max_duration_min": float(row["max_duration_min"]),
                "total_duration_min": float(row["total_duration_min"]),
                "wait_share_pct": float(row["wait_share_pct"]),
                "transition_label": row["transition_label"],
            }
            for row in step_metrics_df.to_dict(orient="records")
        ]

        bottleneck_row = step_metrics_df.sort_values(
            [
                "avg_duration_min",
                "median_duration_min",
                "max_duration_min",
                "sequence_no",
            ],
            ascending=[False, False, False, True],
        ).iloc[0]
        bottleneck_transition = {
            "sequence_no": int(bottleneck_row["sequence_no"]),
            "from_activity": bottleneck_row["activity"],
            "to_activity": bottleneck_row["next_activity"],
            "transition_label": bottleneck_row["transition_label"],
            "avg_duration_min": float(bottleneck_row["avg_duration_min"]),
            "median_duration_min": float(bottleneck_row["median_duration_min"]),
            "max_duration_min": float(bottleneck_row["max_duration_min"]),
            "wait_share_pct": float(bottleneck_row["wait_share_pct"]),
        }

    case_summary_df = (
        pattern_df.groupby("case_id")
        .agg(
            start_time=("start_time", "min"),
            end_time=("next_time", "max"),
            case_total_duration_min=("duration_min", "sum"),
        )
        .reset_index()
        .sort_values(["case_total_duration_min", "case_id"], ascending=[False, True])
        .reset_index(drop=True)
    )
    case_summary_df["case_total_duration_min"] = case_summary_df["case_total_duration_min"].round(2)

    total_case_count = prepared_df["case_id"].nunique()
    matched_case_count = int(case_summary_df["case_id"].nunique())

    return {
        "pattern": pattern,
        "pattern_steps": pattern.split(FLOW_PATH_SEPARATOR),
        "case_count": matched_case_count,
        "case_ratio_pct": round(matched_case_count / total_case_count * 100, 2),
        "avg_case_duration_min": round(float(case_summary_df["case_total_duration_min"].mean()), 2),
        "median_case_duration_min": round(float(case_summary_df["case_total_duration_min"].median()), 2),
        "min_case_duration_min": round(float(case_summary_df["case_total_duration_min"].min()), 2),
        "max_case_duration_min": round(float(case_summary_df["case_total_duration_min"].max()), 2),
        "bottleneck_transition": bottleneck_transition,
        "step_metrics": step_metrics,
        "case_examples": [
            {
                "case_id": row["case_id"],
                "start_time": row["start_time"].isoformat(),
                "end_time": row["end_time"].isoformat(),
                "case_total_duration_min": float(row["case_total_duration_min"]),
            }
            for row in case_summary_df.head(20).to_dict(orient="records")
        ],
    }


def analyze_event_log(
    file_source,
    case_id_column,
    activity_column,
    timestamp_column,
    selected_analysis_keys=None,
    output_root_dir=None,
    export_excel=False,
):
    prepared_df = load_prepared_event_log(
        file_source=file_source,
        case_id_column=case_id_column,
        activity_column=activity_column,
        timestamp_column=timestamp_column,
    )
    return analyze_prepared_event_log(
        prepared_df=prepared_df,
        selected_analysis_keys=selected_analysis_keys,
        output_root_dir=output_root_dir,
        export_excel=export_excel,
    )
