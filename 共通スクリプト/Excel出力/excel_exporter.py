from pathlib import Path

import pandas as pd


def format_analysis_result(df, display_columns=None):
    if display_columns is None:
        return df.copy()

    return df.rename(columns=display_columns)


def export_dataframe_to_excel(
    df,
    output_file,
    sheet_name,
):
    output_path = Path(output_file)
    # 分析ごとの出力先フォルダが無ければ作成します。
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    except PermissionError as exc:
        raise PermissionError(
            f"{output_path} に書き込めません。Excelで開いている場合は閉じて再実行してください。"
        ) from exc

    return output_path


def export_analysis_to_excel(
    df,
    output_root_dir,
    analysis_name,
    output_file_name,
    sheet_name,
    display_columns=None,
):
    output_file = Path(output_root_dir) / analysis_name / output_file_name
    excel_df = format_analysis_result(df, display_columns)
    return export_dataframe_to_excel(excel_df, output_file, sheet_name)


def convert_analysis_result_to_records(
    df,
    display_columns=None,
):
    api_df = format_analysis_result(df, display_columns)
    return api_df.to_dict(orient="records")
