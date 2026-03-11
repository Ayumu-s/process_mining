from pathlib import Path

import pandas as pd

from 共通スクリプト.Excel出力.excel_exporter import export_dataframe_to_excel


ANALYSIS_NAME = "頻度分析"
SHEET_NAME = "頻度分析"
OUTPUT_FILE_NAME = "頻度分析.xlsx"
DISPLAY_COLUMNS = {
    "activity": "アクティビティ",
    "event_count": "イベント件数",
    "case_count": "ケース数",
    "total_duration_min": "合計時間(分)",
    "avg_duration_min": "平均時間(分)",
    "median_duration_min": "中央値時間(分)",
    "min_duration_min": "最小時間(分)",
    "max_duration_min": "最大時間(分)",
    "event_ratio_pct": "イベント比率(%)",
}


def create_frequency_analysis(df):
    # アクティビティごとの件数と処理時間を集計します。
    result = (
        df.groupby("activity")
        .agg(
            event_count=("activity", "count"),
            case_count=("case_id", "nunique"),
            total_duration_min=("duration_min", "sum"),
            avg_duration_min=("duration_min", "mean"),
            median_duration_min=("duration_min", "median"),
            min_duration_min=("duration_min", "min"),
            max_duration_min=("duration_min", "max"),
        )
        .reset_index()
    )

    total_events = len(df)
    result["event_ratio_pct"] = (result["event_count"] / total_events * 100).round(2)

    numeric_cols = [
        "total_duration_min",
        "avg_duration_min",
        "median_duration_min",
        "min_duration_min",
        "max_duration_min",
    ]
    result[numeric_cols] = result[numeric_cols].round(2)

    return result.sort_values(["event_count", "activity"], ascending=[False, True]).reset_index(drop=True)


def run_frequency_analysis(df, output_root_dir):
    result = create_frequency_analysis(df)
    excel_result = result.rename(columns=DISPLAY_COLUMNS)
    output_file = Path(output_root_dir) / ANALYSIS_NAME / OUTPUT_FILE_NAME
    exported_path = export_dataframe_to_excel(excel_result, output_file, SHEET_NAME)
    print(f"{ANALYSIS_NAME} 出力完了: {exported_path.resolve()}")
    return exported_path
