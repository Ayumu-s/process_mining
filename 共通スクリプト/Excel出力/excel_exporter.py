from pathlib import Path

import pandas as pd


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
