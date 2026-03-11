import pandas as pd


def load_and_prepare_data(
    file_path,
    case_id_column,
    activity_column,
    timestamp_column,
):
    df = pd.read_csv(
        file_path,
        dtype={case_id_column: str, activity_column: str},
    )

    required_columns = [case_id_column, activity_column, timestamp_column]
    for column_name in required_columns:
        if column_name not in df.columns:
            raise ValueError(f"入力CSVに必要な列がありません: {column_name}")

    # 指定ヘッダーを内部で使う共通列名へそろえます。
    df = df[required_columns].rename(
        columns={
            case_id_column: "case_id",
            activity_column: "activity",
            timestamp_column: "timestamp",
        }
    )

    if df[["case_id", "activity", "timestamp"]].isna().any().any():
        raise ValueError("case_id, activity, timestamp に空欄があります。")

    if (
        (df["case_id"].str.strip() == "").any()
        or (df["activity"].str.strip() == "").any()
        or (df["timestamp"].astype(str).str.strip() == "").any()
    ):
        raise ValueError("case_id, activity, timestamp いずれかに空文字があります。")

    # 同一timestampがある場合に入力順を保てるように元の並びを持っておく
    df["input_order"] = range(len(df))

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    if df["timestamp"].isna().any():
        raise ValueError("timestamp に日付変換できない値があります。")

    df = df.sort_values(["case_id", "timestamp", "input_order"]).reset_index(drop=True)

    # 次イベント時刻との差分から分析用の時間列を作ります。
    df["start_time"] = df["timestamp"]
    df["next_time"] = df.groupby("case_id")["timestamp"].shift(-1).fillna(df["timestamp"])

    df["duration_sec"] = (df["next_time"] - df["start_time"]).dt.total_seconds()
    df["duration_min"] = (df["duration_sec"] / 60).round(2)

    if (df["duration_sec"] < 0).any():
        raise ValueError("case内のtimestampの並びに不正があります。")

    df["sequence_no"] = df.groupby("case_id").cumcount() + 1
    df["event_count_in_case"] = df.groupby("case_id")["activity"].transform("count")

    return df.drop(columns=["input_order"])
