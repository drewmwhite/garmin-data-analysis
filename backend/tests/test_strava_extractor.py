from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "backend" / "src"))

from extraction.strava_extractor import StravaExtractor


class StravaExtractorCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cache_root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fetch_methods_write_cache_files(self) -> None:
        extractor = StravaExtractor(
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
            cache_root=self.cache_root,
        )

        activity_batch = [
            {
                "id": 123,
                "name": "Lunch Run",
                "start_date_local": "2026-04-11T12:00:00+00:00",
                "type": "Run",
                "distance": 8046.7,
                "moving_time": 2400,
                "elapsed_time": 2450,
                "total_elevation_gain": 80.0,
                "end_latlng": None,
                "max_speed": 4.1,
                "average_speed": 3.35,
                "has_heartrate": True,
                "average_heartrate": 151.0,
                "max_heartrate": 171.0,
                "map": {"summary_polyline": None},
            }
        ]
        activity_detail = {
            "id": 123,
            "laps": [
                {
                    "id": 1,
                    "lap_index": 1,
                    "average_cadence": 82.0,
                    "average_heartrate": 151.0,
                    "average_speed": 3.35,
                    "distance": 1000.0,
                    "max_heartrate": 171.0,
                    "max_speed": 4.1,
                    "moving_time": 300,
                    "split": 1,
                }
            ],
        }

        with patch.object(
            extractor,
            "_get",
            side_effect=[{"id": 99, "firstname": "Drew", "lastname": "White"}, activity_batch, [], activity_detail],
        ):
            extractor.fetch_athlete()
            activities = extractor.fetch_activities()
            laps = extractor.fetch_laps(123)

        self.assertEqual(len(activities), 1)
        self.assertEqual(len(laps), 1)

        run_dir = extractor.cache_run_dir
        self.assertTrue((run_dir / "athlete.json").exists())
        self.assertTrue((run_dir / "activities" / "page-0001.json").exists())
        self.assertTrue((run_dir / "activity_details" / "123.json").exists())

        cached_activities = StravaExtractor.load_cached_activities(run_dir)
        self.assertEqual([row["id"] for row in cached_activities], [123])

        cached_detail = json.loads((run_dir / "activity_details" / "123.json").read_text(encoding="utf-8"))
        self.assertEqual(cached_detail["activity_id"], 123)


if __name__ == "__main__":
    unittest.main()
