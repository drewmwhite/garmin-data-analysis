from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fastapi import HTTPException

from api.app import get_dataset, health_check, list_datasets


class ApiTests(unittest.TestCase):
    def test_health_check_returns_ok(self) -> None:
        self.assertEqual(health_check(), {"status": "ok"})

    @patch("api.app.get_dataset_records")
    def test_list_datasets_returns_summaries(self, mock_get_dataset_records) -> None:
        mock_get_dataset_records.side_effect = [
            [{"calendarDate": "2026-04-01", "score": 91}],
            [{"calendarDate": "2026-04-01", "valueInML": 750}],
            [{"calendarDate": "2026-04-01", "vo2MaxValue": 47}],
        ]

        payload = list_datasets()
        self.assertEqual(len(payload["datasets"]), 3)
        self.assertEqual(payload["datasets"][0]["slug"], "sleep")
        self.assertEqual(payload["datasets"][0]["record_count"], 1)

    @patch("api.app.get_dataset_records")
    def test_get_dataset_applies_limit(self, mock_get_dataset_records) -> None:
        mock_get_dataset_records.return_value = [
            {"calendarDate": "2026-04-01", "score": 91},
            {"calendarDate": "2026-04-02", "score": 89},
        ]

        payload = get_dataset("sleep", limit=1)
        self.assertEqual(payload["dataset"]["slug"], "sleep")
        self.assertEqual(payload["returned_records"], 1)
        self.assertEqual(len(payload["records"]), 1)

    def test_get_dataset_returns_404_for_unknown_slug(self) -> None:
        with self.assertRaises(HTTPException) as context:
            get_dataset("nope")

        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(context.exception.detail, "Unknown dataset: nope")


if __name__ == "__main__":
    unittest.main()
