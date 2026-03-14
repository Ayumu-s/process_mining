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

    def test_detail_script_is_served(self):
        response = self.client.get("/static/detail.js")

        self.assertEqual(200, response.status_code)
        self.assertIn("renderDetailPage", response.text)

    def test_index_page_renders_excel_option_unchecked_by_default(self):
        response = self.client.get("/")

        self.assertEqual(200, response.status_code)
        self.assertIn("Excel ZIP", response.text)
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


if __name__ == "__main__":
    unittest.main()
