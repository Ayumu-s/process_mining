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
        .apply(lambda series: "→".join(series.tolist()))
        .reset_index(name="pattern")
    )


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
        "pattern_steps": pattern.split("→"),
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
