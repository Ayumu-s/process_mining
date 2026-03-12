import unittest

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


if __name__ == "__main__":
    unittest.main()
