ANALYSIS_CONFIG = {
    "analysis_name": "前後処理分析",
    "sheet_name": "前後処理分析",
    "output_file_name": "前後処理分析.xlsx",
    "display_columns": {
        "from_activity": "前処理アクティビティ名",
        "to_activity": "後処理アクティビティ名",
        "transition_count": "遷移件数",
        "case_count": "ケース数",
        "from_total_duration_min": "前処理合計時間(分)",
        "from_avg_duration_min": "前処理平均時間(分)",
        "to_total_duration_min": "後処理合計時間(分)",
        "to_avg_duration_min": "後処理平均時間(分)",
        "total_waiting_time_min": "合計待ち時間(分)",
        "avg_waiting_time_min": "平均待ち時間(分)",
        "transition_ratio_pct": "遷移比率(%)",
    },
}



def create_transition_analysis(df):
    work = df.copy()

    # ケース内の次イベントを横持ちにして遷移を集計します。
    work["next_activity"] = work.groupby("case_id")["activity"].shift(-1)
    work["next_start_time"] = work.groupby("case_id")["start_time"].shift(-1)
    work["next_duration_min"] = work.groupby("case_id")["duration_min"].shift(-1)

    work = work.dropna(subset=["next_activity"]).copy()

    work["waiting_time_min"] = (
        (work["next_start_time"] - work["next_time"]).dt.total_seconds() / 60
    )

    result = (
        work.groupby(["activity", "next_activity"])
        .agg(
            transition_count=("case_id", "count"),
            case_count=("case_id", "nunique"),
            from_total_duration_min=("duration_min", "sum"),
            from_avg_duration_min=("duration_min", "mean"),
            to_total_duration_min=("next_duration_min", "sum"),
            to_avg_duration_min=("next_duration_min", "mean"),
            total_waiting_time_min=("waiting_time_min", "sum"),
            avg_waiting_time_min=("waiting_time_min", "mean"),
        )
        .reset_index()
        .rename(columns={"activity": "from_activity", "next_activity": "to_activity"})
    )

    total_transitions = result["transition_count"].sum()
    result["transition_ratio_pct"] = (result["transition_count"] / total_transitions * 100).round(2)

    numeric_cols = [
        "from_total_duration_min",
        "from_avg_duration_min",
        "to_total_duration_min",
        "to_avg_duration_min",
        "total_waiting_time_min",
        "avg_waiting_time_min",
    ]
    result[numeric_cols] = result[numeric_cols].round(2)

    result = result.sort_values(
        ["transition_count", "from_activity", "to_activity"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    ordered_columns = [
        "from_activity",
        "to_activity",
        "transition_count",
        "case_count",
        "from_total_duration_min",
        "from_avg_duration_min",
        "to_total_duration_min",
        "to_avg_duration_min",
        "total_waiting_time_min",
        "avg_waiting_time_min",
        "transition_ratio_pct",
    ]

    return result[ordered_columns]
