from pathlib import Path

from 共通スクリプト.data_loader import load_and_prepare_data
from 共通スクリプト.分析.前後処理分析.transition_analysis import run_transition_analysis
from 共通スクリプト.分析.処理順パターン分析.pattern_analysis import run_pattern_analysis
from 共通スクリプト.分析.頻度分析.frequency_analysis import run_frequency_analysis


INPUT_FILE = Path("sample_event_log.csv")
OUTPUT_ROOT_DIR = Path("出力ファイル")
# 入力CSVのヘッダー名をここで合わせます。
CASE_ID_COLUMN = "case_id"
ACTIVITY_COLUMN = "activity"
TIMESTAMP_COLUMN = "start_time"


def main():
    # 入力CSVを分析用の共通フォーマットへ整形します。
    df_input = load_and_prepare_data(
        file_path=INPUT_FILE,
        case_id_column=CASE_ID_COLUMN,
        activity_column=ACTIVITY_COLUMN,
        timestamp_column=TIMESTAMP_COLUMN,
    )

    # 実行しない分析は、下の行をコメントアウトしてください。
    run_frequency_analysis(df_input, OUTPUT_ROOT_DIR)
    run_transition_analysis(df_input, OUTPUT_ROOT_DIR)
    run_pattern_analysis(df_input, OUTPUT_ROOT_DIR)


if __name__ == "__main__":
    main()
