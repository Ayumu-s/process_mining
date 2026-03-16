from io import BytesIO
import unittest
from unittest import mock
from zipfile import ZipFile

from fastapi.testclient import TestClient

import web_app


class WebAppTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(web_app.app)

    def analyze_uploaded_csv(self, csv_text, analysis_keys=None, extra_data=None):
        response = self.client.post(
            "/api/analyze",
            data={
                "analysis_keys": analysis_keys or ["frequency", "pattern"],
                **(extra_data or {}),
            },
            files={"csv_file": ("custom_log.csv", BytesIO(csv_text.encode("utf-8")), "text/csv")},
        )
        self.assertEqual(200, response.status_code)
        return response.json()["run_id"]

    def test_detail_script_is_served(self):
        response = self.client.get("/static/detail.js")

        self.assertEqual(200, response.status_code)
        self.assertIn("renderDetailPage", response.text)

    def test_index_page_renders_excel_option_unchecked_by_default(self):
        response = self.client.get("/")

        self.assertEqual(200, response.status_code)
        self.assertIn("Excel ZIP", response.text)
        self.assertIn('<select name="case_id_column"', response.text)
        self.assertIn('<select name="activity_column"', response.text)
        self.assertIn('<select name="timestamp_column"', response.text)
        self.assertNotIn('id="select-output-directory-button"', response.text)
        self.assertNotIn('name="export_excel" checked', response.text)
        self.assertIn("/static/style.css?v=", response.text)
        self.assertIn("/static/app.js?v=", response.text)

    def test_analysis_detail_page_uses_versioned_static_assets(self):
        response = self.client.get("/analysis/pattern")

        self.assertEqual(200, response.status_code)
        self.assertIn("/static/style.css?v=", response.text)
        self.assertIn("/static/detail.js?v=", response.text)

    def test_analysis_detail_api_returns_full_rows(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "transition", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        detail_response = self.client.get(f"/api/runs/{run_id}/analyses/frequency")
        self.assertEqual(200, detail_response.status_code)

        payload = detail_response.json()
        frequency_analysis = payload["analyses"]["frequency"]

        self.assertGreater(frequency_analysis["row_count"], 0)
        self.assertEqual(frequency_analysis["row_count"], len(frequency_analysis["rows"]))

    def test_analysis_detail_api_supports_row_limit(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "transition", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        detail_response = self.client.get(f"/api/runs/{run_id}/analyses/pattern?row_limit=2")
        self.assertEqual(200, detail_response.status_code)

        payload = detail_response.json()
        pattern_analysis = payload["analyses"]["pattern"]

        self.assertEqual(2, pattern_analysis["returned_row_count"])
        self.assertEqual(0, pattern_analysis["row_offset"])
        self.assertGreaterEqual(pattern_analysis["row_count"], pattern_analysis["returned_row_count"])
        self.assertEqual(2, len(pattern_analysis["rows"]))

    def test_analysis_detail_api_supports_row_offset_pagination_metadata(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        detail_response = self.client.get(
            f"/api/runs/{run_id}/analyses/pattern?row_limit=1&row_offset=1"
        )
        self.assertEqual(200, detail_response.status_code)

        payload = detail_response.json()
        pattern_analysis = payload["analyses"]["pattern"]

        self.assertEqual(1, pattern_analysis["returned_row_count"])
        self.assertEqual(1, pattern_analysis["row_offset"])
        self.assertEqual(2, pattern_analysis["page_start_row_number"])
        self.assertEqual(2, pattern_analysis["page_end_row_number"])
        self.assertTrue(pattern_analysis["has_previous_page"])
        self.assertTrue(pattern_analysis["has_next_page"])
        self.assertEqual(0, pattern_analysis["previous_row_offset"])
        self.assertEqual(2, pattern_analysis["next_row_offset"])

    def test_excel_archive_api_returns_zip_binary(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={
                "analysis_keys": ["frequency", "transition"],
                "export_excel": "on",
            },
        )
        self.assertEqual(200, analyze_response.status_code)

        payload = analyze_response.json()
        self.assertIsNone(payload["analyses"]["frequency"]["excel_file"])

        run_id = payload["run_id"]
        excel_response = self.client.get(f"/api/runs/{run_id}/excel-archive")

        self.assertEqual(200, excel_response.status_code)
        self.assertEqual(
            "application/zip",
            excel_response.headers["content-type"],
        )
        self.assertIn("attachment;", excel_response.headers["content-disposition"])
        self.assertTrue(excel_response.content.startswith(b"PK"))

        with ZipFile(BytesIO(excel_response.content)) as archive_file:
            self.assertEqual(
                {"頻度分析.xlsx", "前後処理分析.xlsx"},
                set(archive_file.namelist()),
            )

    def test_pattern_flow_api_accepts_exact_pattern_count(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        flow_response = self.client.get(f"/api/runs/{run_id}/pattern-flow?pattern_count=1")

        self.assertEqual(200, flow_response.status_code)
        payload = flow_response.json()
        self.assertEqual(1, payload["pattern_window"]["requested_count"])
        self.assertEqual(1, payload["pattern_window"]["used_pattern_count"])
        self.assertGreaterEqual(len(payload["flow_data"]["nodes"]), 1)

    def test_pattern_flow_api_reuses_cached_snapshot(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        flow_path = (
            f"/api/runs/{run_id}/pattern-flow"
            "?pattern_percent=20&activity_percent=30&connection_percent=20"
        )

        with mock.patch(
            "web_app.create_pattern_flow_snapshot",
            wraps=web_app.create_pattern_flow_snapshot,
        ) as snapshot_mock:
            first_response = self.client.get(flow_path)
            second_response = self.client.get(flow_path)

        self.assertEqual(200, first_response.status_code)
        self.assertEqual(200, second_response.status_code)
        self.assertEqual(first_response.json(), second_response.json())
        self.assertEqual(1, snapshot_mock.call_count)

    def test_variant_list_api_returns_top_variants(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        variant_response = self.client.get(f"/api/runs/{run_id}/variants?limit=2")

        self.assertEqual(200, variant_response.status_code)
        payload = variant_response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual(2, len(payload["variants"]))
        self.assertEqual(1, payload["variants"][0]["variant_id"])
        self.assertEqual(["受付", "確認", "完了"], payload["variants"][0]["activities"])
        self.assertEqual(2, payload["coverage"]["displayed_variant_count"])
        self.assertEqual(4, payload["coverage"]["covered_case_count"])
        self.assertEqual(0.6667, payload["coverage"]["ratio"])

    def test_pattern_flow_api_supports_variant_filter(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        flow_response = self.client.get(f"/api/runs/{run_id}/pattern-flow?variant_id=1")

        self.assertEqual(200, flow_response.status_code)
        payload = flow_response.json()
        self.assertEqual(1, payload["selected_variant"]["variant_id"])
        self.assertEqual(1, payload["pattern_window"]["used_pattern_count"])
        self.assertTrue(payload["flow_data"]["nodes"])

    def test_bottleneck_list_api_returns_ranked_activity_and_transition_rows(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(f"/api/runs/{run_id}/bottlenecks?limit=2")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual(2, payload["limit"])
        self.assertEqual(2, len(payload["activity_bottlenecks"]))
        self.assertEqual(2, len(payload["transition_bottlenecks"]))
        self.assertEqual("確認", payload["activity_bottlenecks"][0]["activity"])
        self.assertEqual(577.5, payload["activity_bottlenecks"][0]["avg_duration_sec"])
        self.assertEqual(0.16, payload["activity_bottlenecks"][0]["avg_duration_hours"])
        self.assertEqual("heat-5", payload["activity_heatmap"]["確認"]["heat_class"])
        self.assertEqual("確認", payload["transition_bottlenecks"][0]["from_activity"])
        self.assertEqual("差戻し", payload["transition_bottlenecks"][0]["to_activity"])
        self.assertEqual("確認__TO__差戻し", payload["transition_bottlenecks"][0]["transition_key"])
        self.assertEqual(0.2, payload["transition_bottlenecks"][0]["avg_duration_hours"])
        self.assertEqual("heat-5", payload["transition_heatmap"]["確認__TO__差戻し"]["heat_class"])

    def test_transition_case_drilldown_api_returns_slowest_cases(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(
            f"/api/runs/{run_id}/transition-cases"
            "?from_activity=確認&to_activity=差戻し&limit=5"
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual("確認", payload["from_activity"])
        self.assertEqual("差戻し", payload["to_activity"])
        self.assertEqual("確認__TO__差戻し", payload["transition_key"])
        self.assertEqual("確認 → 差戻し", payload["transition_label"])
        self.assertEqual(2, payload["returned_case_count"])
        self.assertEqual(2, len(payload["cases"]))
        self.assertEqual("C002", payload["cases"][0]["case_id"])
        self.assertEqual(1020.0, payload["cases"][0]["duration_sec"])
        self.assertEqual("17m 0s", payload["cases"][0]["duration_text"])

    def test_case_trace_api_returns_case_timeline(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(f"/api/runs/{run_id}/cases/C001")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertTrue(payload["found"])
        self.assertEqual("C001", payload["case_id"])
        self.assertEqual(4, payload["summary"]["event_count"])
        self.assertEqual(900.0, payload["summary"]["total_duration_sec"])
        self.assertEqual("15m 0s", payload["summary"]["total_duration_text"])
        self.assertEqual(4, len(payload["events"]))
        self.assertEqual(1, payload["events"][0]["sequence_no"])
        self.assertEqual(120.0, payload["events"][0]["wait_to_next_sec"])
        self.assertIsNone(payload["events"][-1]["wait_to_next_sec"])

    def test_case_trace_api_returns_not_found_payload(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(f"/api/runs/{run_id}/cases/C999")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertFalse(payload["found"])
        self.assertEqual("C999", payload["case_id"])
        self.assertIsNone(payload["summary"])
        self.assertEqual([], payload["events"])

    def test_filter_options_api_returns_available_values(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b,group_c",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web,A",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web,A",
                    "C002,Submit,2024-01-01 10:00:00,HR,Mail,B",
                    "C002,Reject,2024-01-03 10:00:00,HR,Mail,B",
                    "C003,Submit,2024-01-04 08:00:00,Sales,API,A",
                    "C003,Approve,2024-01-05 08:00:00,Sales,API,A",
                ]
            ),
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        response = self.client.get(f"/api/runs/{run_id}/filter-options")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("group_a", payload["column_settings"]["filters"][0]["column_name"])
        self.assertEqual(["HR", "Sales"], payload["options"]["filters"][0]["options"])
        self.assertEqual(["API", "Mail", "Web"], payload["options"]["filters"][1]["options"])
        self.assertEqual(["A", "B"], payload["options"]["filters"][2]["options"])

    def test_analysis_detail_api_supports_filters(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b,group_c",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web,A",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web,A",
                    "C002,Submit,2024-01-01 10:00:00,HR,Mail,B",
                    "C002,Reject,2024-01-03 10:00:00,HR,Mail,B",
                    "C003,Submit,2024-01-04 08:00:00,Sales,API,A",
                    "C003,Approve,2024-01-05 08:00:00,Sales,API,A",
                ]
            ),
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        response = self.client.get(
            f"/api/runs/{run_id}/analyses/frequency?filter_value_1=Sales&date_from=2024-01-02"
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(2, payload["case_count"])
        self.assertEqual(3, payload["event_count"])
        self.assertEqual("Sales", payload["applied_filters"]["filter_value_1"])
        self.assertEqual("2024-01-02", payload["applied_filters"]["date_from"])

    def test_bottleneck_and_variant_api_support_filters(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b,group_c",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web,A",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web,A",
                    "C002,Submit,2024-01-01 10:00:00,HR,Mail,B",
                    "C002,Reject,2024-01-03 10:00:00,HR,Mail,B",
                    "C003,Submit,2024-01-04 08:00:00,Sales,API,A",
                    "C003,Approve,2024-01-05 08:00:00,Sales,API,A",
                ]
            ),
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        variant_response = self.client.get(f"/api/runs/{run_id}/variants?filter_value_2=Web")
        bottleneck_response = self.client.get(f"/api/runs/{run_id}/bottlenecks?filter_value_3=A")

        self.assertEqual(200, variant_response.status_code)
        self.assertEqual(200, bottleneck_response.status_code)

        variant_payload = variant_response.json()
        bottleneck_payload = bottleneck_response.json()

        self.assertEqual(1, variant_payload["filtered_case_count"])
        self.assertEqual(2, variant_payload["filtered_event_count"])
        self.assertEqual("Web", variant_payload["applied_filters"]["filter_value_2"])
        self.assertEqual(2, bottleneck_payload["filtered_case_count"])
        self.assertEqual(4, bottleneck_payload["filtered_event_count"])
        self.assertEqual("A", bottleneck_payload["applied_filters"]["filter_value_3"])

    def test_pattern_detail_api_uses_pattern_index_not_display_text(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        run_data = web_app.get_run_data(run_id)
        pattern_rows = run_data["result"]["analyses"]["pattern"]["rows"]

        if pattern_rows:
            first_row = pattern_rows[0]
            for key in list(first_row.keys()):
                if "pattern" in str(key).lower() or "繝代ち繝ｼ繝ｳ" in str(key) or "蜃ｦ逅" in str(key):
                    first_row[key] = ""

        response = self.client.get(f"/api/runs/{run_id}/patterns/0")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual(0, payload["pattern_index"])
        self.assertTrue(payload["pattern"])

    def test_csv_headers_api_returns_sample_headers_without_upload(self):
        response = self.client.post("/api/csv-headers", data={})

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("sample_event_log.csv", payload["source_file_name"])
        self.assertEqual(["case_id", "activity", "start_time", "end_time"], payload["headers"])
        self.assertEqual("case_id", payload["default_selection"]["case_id_column"])
        self.assertEqual("case_id", payload["column_settings"]["case_id_column"])
        self.assertEqual(3, len(payload["column_settings"]["filters"]))
        self.assertIn("diagnostics", payload)

    def test_csv_headers_api_returns_uploaded_headers(self):
        csv_bytes = "申請ID,処理名,日時\nA001,受付,2024-01-01 09:00:00\n".encode("utf-8")

        response = self.client.post(
            "/api/csv-headers",
            files={"csv_file": ("custom_log.csv", BytesIO(csv_bytes), "text/csv")},
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("custom_log.csv", payload["source_file_name"])
        self.assertEqual(["申請ID", "処理名", "日時"], payload["headers"])
        self.assertEqual("", payload["default_selection"]["case_id_column"])

    def test_analyze_api_rejects_duplicate_selected_columns(self):
        response = self.client.post(
            "/api/analyze",
            data={
                "case_id_column": "case_id",
                "activity_column": "case_id",
                "timestamp_column": "start_time",
                "analysis_keys": ["frequency"],
            },
        )

        self.assertEqual(400, response.status_code)
        payload = response.json()
        self.assertIn("異なる列", payload["error"])


    def test_csv_headers_api_returns_uploaded_headers(self):
        csv_bytes = "case_no,step_name,event_at,division\nA001,Submit,2024-01-01 09:00:00,Sales\n".encode("utf-8")

        response = self.client.post(
            "/api/csv-headers",
            files={"csv_file": ("custom_log.csv", BytesIO(csv_bytes), "text/csv")},
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("custom_log.csv", payload["source_file_name"])
        self.assertEqual(["case_no", "step_name", "event_at", "division"], payload["headers"])
        self.assertEqual(1, payload["diagnostics"]["event_count"])
        self.assertEqual("", payload["default_selection"]["case_id_column"])


if __name__ == "__main__":
    unittest.main()
