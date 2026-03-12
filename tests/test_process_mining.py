from pathlib import Path
import unittest

import pandas as pd

from 共通スクリプト.analysis_service import create_pattern_bottleneck_details
from 共通スクリプト.data_loader import load_and_prepare_data, prepare_event_log
from 共通スクリプト.分析.前後処理分析.transition_analysis import create_transition_analysis
from 共通スクリプト.分析.処理順パターン分析.pattern_analysis import create_pattern_analysis
from 共通スクリプト.分析.頻度分析.frequency_analysis import create_frequency_analysis


ROOT_DIR = Path(__file__).resolve().parents[1]
SAMPLE_FILE = ROOT_DIR / "sample_event_log.csv"


class ProcessMiningTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prepared_df = load_and_prepare_data(
            file_path=SAMPLE_FILE,
            case_id_column="case_id",
            activity_column="activity",
            timestamp_column="start_time",
        )

    def test_prepare_event_log_adds_analysis_columns(self):
        expected_columns = [
            "case_id",
            "activity",
            "timestamp",
            "start_time",
            "next_time",
            "duration_sec",
            "duration_min",
            "sequence_no",
            "event_count_in_case",
        ]
        self.assertEqual(expected_columns, self.prepared_df.columns.tolist())

        first_row = self.prepared_df.iloc[0]
        self.assertEqual("C001", first_row["case_id"])
        self.assertEqual("受付", first_row["activity"])
        self.assertEqual(120.0, first_row["duration_sec"])
        self.assertEqual(2.0, first_row["duration_min"])
        self.assertEqual(1, first_row["sequence_no"])
        self.assertEqual(4, first_row["event_count_in_case"])

        last_case_row = self.prepared_df[self.prepared_df["case_id"] == "C001"].iloc[-1]
        self.assertEqual(0.0, last_case_row["duration_sec"])
        self.assertEqual(last_case_row["start_time"], last_case_row["next_time"])

    def test_prepare_event_log_raises_when_required_column_is_missing(self):
        invalid_df = pd.DataFrame(
            [
                {"case_id": "C001", "activity": "受付"},
            ]
        )

        with self.assertRaisesRegex(ValueError, "入力CSVに必要な列がありません"):
            prepare_event_log(
                df=invalid_df,
                case_id_column="case_id",
                activity_column="activity",
                timestamp_column="start_time",
            )

    def test_frequency_analysis_returns_expected_summary(self):
        result = create_frequency_analysis(self.prepared_df)

        self.assertEqual(["確認", "受付", "完了", "承認", "差戻し"], result["activity"].tolist())

        top_row = result.iloc[0]
        self.assertEqual("確認", top_row["activity"])
        self.assertEqual(8, top_row["event_count"])
        self.assertEqual(6, top_row["case_count"])
        self.assertEqual(77.0, top_row["total_duration_min"])
        self.assertEqual(30.77, top_row["event_ratio_pct"])

    def test_transition_analysis_returns_expected_transitions(self):
        result = create_transition_analysis(self.prepared_df)

        first_row = result.iloc[0]
        self.assertEqual("受付", first_row["from_activity"])
        self.assertEqual("確認", first_row["to_activity"])
        self.assertEqual(6, first_row["transition_count"])
        self.assertEqual(6, first_row["case_count"])
        self.assertEqual(30.0, first_row["transition_ratio_pct"])

        confirm_to_completion = result[
            (result["from_activity"] == "確認") & (result["to_activity"] == "完了")
        ].iloc[0]
        self.assertEqual(2, confirm_to_completion["transition_count"])
        self.assertEqual(0.0, confirm_to_completion["to_total_duration_min"])

    def test_pattern_analysis_returns_expected_patterns(self):
        result = create_pattern_analysis(self.prepared_df)

        self.assertEqual(3, len(result))

        first_row = result.iloc[0]
        self.assertEqual("受付→確認→完了", first_row["pattern"])
        self.assertEqual(2, first_row["case_count"])
        self.assertEqual(6.5, first_row["avg_case_duration_min"])
        self.assertEqual(33.33, first_row["case_ratio_pct"])

    def test_pattern_bottleneck_details_returns_transition_metrics(self):
        pattern = "受付→確認→承認→完了"
        detail = create_pattern_bottleneck_details(self.prepared_df, pattern)

        self.assertEqual(pattern, detail["pattern"])
        self.assertEqual(2, detail["case_count"])
        self.assertEqual(33.33, detail["case_ratio_pct"])
        self.assertEqual(18.5, detail["avg_case_duration_min"])
        self.assertEqual(3, len(detail["step_metrics"]))
        self.assertEqual("確認", detail["bottleneck_transition"]["from_activity"])
        self.assertEqual("承認", detail["bottleneck_transition"]["to_activity"])
        self.assertEqual(10.0, detail["bottleneck_transition"]["avg_duration_min"])
        self.assertEqual(["C004", "C001"], [row["case_id"] for row in detail["case_examples"][:2]])


    def test_pattern_flow_snapshot_filters_top_patterns_nodes_and_edges(self):
        import importlib.util

        module_path = ROOT_DIR / "共通スクリプト" / "analysis_service.py"
        spec = importlib.util.spec_from_file_location("analysis_service_local", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        pattern_rows = [
            {"ケース数": 90, "処理順パターン": "申請受付→内容確認→処理完了"},
            {"ケース数": 70, "処理順パターン": "申請受付→一次承認→処理完了"},
            {"ケース数": 50, "処理順パターン": "申請受付→差戻し→再提出→処理完了"},
            {"ケース数": 30, "処理順パターン": "申請受付→自動処理→処理完了"},
        ]

        snapshot = module.create_pattern_flow_snapshot(
            pattern_rows=pattern_rows,
            pattern_percent=50,
            activity_percent=60,
            connection_percent=50,
            pattern_cap=4,
        )

        self.assertEqual(2, snapshot["pattern_window"]["used_pattern_count"])
        self.assertEqual(4, snapshot["activity_window"]["available_activity_count"])
        self.assertEqual(4, snapshot["connection_window"]["available_connection_count"])
        self.assertTrue(snapshot["flow_data"]["nodes"])
        self.assertTrue(snapshot["flow_data"]["edges"])
        self.assertTrue(
            all("layer" in node and "orderScore" in node for node in snapshot["flow_data"]["nodes"])
        )


if __name__ == "__main__":
    unittest.main()
