from io import BytesIO
from pathlib import Path

import pandas as pd


def format_analysis_result(df, display_columns=None):
    if display_columns is None:
        return df.copy()

    return df.rename(columns=display_columns)


def build_excel_bytes(
    df,
    sheet_name,
):
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    return buffer.getvalue()


def export_dataframe_to_excel(
    df,
    output_file,
    sheet_name,
):
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        output_path.write_bytes(build_excel_bytes(df, sheet_name))
    except PermissionError as exc:
        raise PermissionError(
            f"{output_path} に書き込めません。Excel で開いている場合は閉じてから再実行してください。"
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
