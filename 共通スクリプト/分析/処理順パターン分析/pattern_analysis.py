from pathlib import Path

import pandas as pd

from 共通スクリプト.Excel出力.excel_exporter import export_dataframe_to_excel


ANALYSIS_NAME = "処理順パターン分析"
SHEET_NAME = "処理順パターン分析"
OUTPUT_FILE_NAME = "処理順パターン分析.xlsx"
DISPLAY_COLUMNS = {
    "pattern": "処理順パターン",
    "case_count": "ケース数",
    "avg_case_duration_min": "平均ケース時間(分)",
    "median_case_duration_min": "中央値ケース時間(分)",
    "min_case_duration_min": "最小ケース時間(分)",
    "max_case_duration_min": "最大ケース時間(分)",
    "case_ratio_pct": "ケース比率(%)",
}


def create_pattern_analysis(df):
    # ケースごとの処理順を1本のパターン文字列にまとめます。
    case_path = (
        df.sort_values(["case_id", "sequence_no"])
        .groupby("case_id")["activity"]
        .apply(lambda series: "→".join(series.tolist()))
        .reset_index(name="pattern")
    )

    case_duration = (
        df.groupby("case_id")
        .agg(start_time=("start_time", "min"), next_time=("next_time", "max"))
        .reset_index()
    )
    case_duration["case_total_duration_min"] = (
        (case_duration["next_time"] - case_duration["start_time"]).dt.total_seconds() / 60
    ).round(2)

    merged = case_path.merge(
        case_duration[["case_id", "case_total_duration_min"]],
        on="case_id",
        how="left",
    )

    result = (
        merged.groupby("pattern")
        .agg(
            case_count=("case_id", "count"),
            avg_case_duration_min=("case_total_duration_min", "mean"),
            median_case_duration_min=("case_total_duration_min", "median"),
            min_case_duration_min=("case_total_duration_min", "min"),
            max_case_duration_min=("case_total_duration_min", "max"),
        )
        .reset_index()
    )

    total_cases = merged["case_id"].nunique()
    result["case_ratio_pct"] = (result["case_count"] / total_cases * 100).round(2)

    numeric_cols = [
        "avg_case_duration_min",
        "median_case_duration_min",
        "min_case_duration_min",
        "max_case_duration_min",
    ]
    result[numeric_cols] = result[numeric_cols].round(2)

    return result.sort_values(["case_count", "pattern"], ascending=[False, True]).reset_index(drop=True)


def run_pattern_analysis(df, output_root_dir):
    result = create_pattern_analysis(df)
    excel_result = result.rename(columns=DISPLAY_COLUMNS)
    output_file = Path(output_root_dir) / ANALYSIS_NAME / OUTPUT_FILE_NAME
    exported_path = export_dataframe_to_excel(excel_result, output_file, SHEET_NAME)
    print(f"{ANALYSIS_NAME} 出力完了: {exported_path.resolve()}")
    return exported_path
