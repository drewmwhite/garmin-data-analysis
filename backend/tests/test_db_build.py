from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "backend" / "src"))

from db import build as db_build


class BuildStravaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = duckdb.connect()
        self.conn.execute(
            """
            CREATE TABLE strava_activities (
                id BIGINT,
                name VARCHAR,
                start_date_local VARCHAR,
                type VARCHAR,
                distance DOUBLE,
                moving_time BIGINT,
                elapsed_time BIGINT,
                total_elevation_gain DOUBLE,
                end_latlng VARCHAR,
                max_speed DOUBLE,
                average_speed DOUBLE,
                average_heartrate DOUBLE,
                max_heartrate DOUBLE,
                summary_polyline VARCHAR
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE strava_laps (
                workout_id BIGINT,
                average_cadence DOUBLE,
                average_heartrate DOUBLE,
                average_speed DOUBLE,
                distance DOUBLE,
                lap_id BIGINT,
                lap_index INTEGER,
                max_heartrate DOUBLE,
                max_speed DOUBLE,
                moving_time BIGINT,
                split INTEGER
            )
            """
        )

    def tearDown(self) -> None:
        self.conn.close()

    @patch.dict(
        "os.environ",
        {
            "STRAVA_CLIENT_ID": "client-id",
            "STRAVA_CLIENT_SECRET": "client-secret",
            "STRAVA_REFRESH_TOKEN": "refresh-token",
        },
        clear=False,
    )
    def test_build_strava_only_fetches_new_activities_and_missing_laps(self) -> None:
        self.conn.executemany(
            "INSERT INTO strava_activities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    100,
                    "Existing Run",
                    "2024-04-01T06:00:00+00:00",
                    "Run",
                    5000.0,
                    1500,
                    1550,
                    40.0,
                    None,
                    4.0,
                    3.3,
                    150.0,
                    170.0,
                    None,
                ),
                (
                    200,
                    "Newest Existing Run",
                    "2024-04-02T06:30:00+00:00",
                    "Run",
                    8000.0,
                    2400,
                    2450,
                    60.0,
                    None,
                    4.2,
                    3.4,
                    152.0,
                    172.0,
                    None,
                ),
                (
                    300,
                    "Existing Ride",
                    "2024-04-01T17:00:00+00:00",
                    "Ride",
                    20000.0,
                    3600,
                    3650,
                    150.0,
                    None,
                    10.0,
                    5.5,
                    140.0,
                    160.0,
                    None,
                ),
            ],
        )
        self.conn.execute(
            "INSERT INTO strava_laps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [200, None, 152.0, 3.4, 1000.0, 1, 1, 172.0, 4.2, 300, 1],
        )

        extractor = MagicMock()
        extractor.fetch_activities.return_value = [
            {
                "id": 400,
                "name": "New Run",
                "start_date_local": "2024-04-03T06:45:00+00:00",
                "type": "Run",
                "distance": 10000.0,
                "moving_time": 3000,
                "elapsed_time": 3050,
                "total_elevation_gain": 75.0,
                "end_latlng": None,
                "max_speed": 4.3,
                "average_speed": 3.5,
                "average_heartrate": 154.0,
                "max_heartrate": 174.0,
                "summary_polyline": None,
            }
        ]
        extractor.iter_laps_batched.return_value = [
            (
                [
                    {
                        "workout_id": 400,
                        "average_cadence": None,
                        "average_heartrate": 154.0,
                        "average_speed": 3.5,
                        "distance": 1000.0,
                        "lap_id": 401,
                        "lap_index": 1,
                        "max_heartrate": 174.0,
                        "max_speed": 4.3,
                        "moving_time": 300,
                        "split": 1,
                    },
                    {
                        "workout_id": 100,
                        "average_cadence": None,
                        "average_heartrate": 150.0,
                        "average_speed": 3.3,
                        "distance": 1000.0,
                        "lap_id": 101,
                        "lap_index": 1,
                        "max_heartrate": 170.0,
                        "max_speed": 4.0,
                        "moving_time": 305,
                        "split": 1,
                    },
                ],
                [400, 100],
            )
        ]

        with patch.object(db_build, "StravaExtractor", return_value=extractor):
            db_build.build_strava(self.conn)

        extractor.fetch_activities.assert_called_once_with(
            after=db_build._iso_to_unix_timestamp("2024-04-02 06:30:00")
        )
        extractor.iter_laps_batched.assert_called_once_with([400, 100])

        activity_ids = {
            row[0] for row in self.conn.execute("SELECT id FROM strava_activities").fetchall()
        }
        self.assertEqual(activity_ids, {100, 200, 300, 400})

        lap_workout_ids = {
            row[0] for row in self.conn.execute("SELECT DISTINCT workout_id FROM strava_laps").fetchall()
        }
        self.assertEqual(lap_workout_ids, {100, 200, 400})

    @patch.dict(
        "os.environ",
        {
            "STRAVA_CLIENT_ID": "client-id",
            "STRAVA_CLIENT_SECRET": "client-secret",
            "STRAVA_REFRESH_TOKEN": "refresh-token",
        },
        clear=False,
    )
    def test_build_strava_skips_lap_fetch_when_all_runs_already_have_laps(self) -> None:
        self.conn.execute(
            "INSERT INTO strava_activities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                200,
                "Existing Run",
                "2024-04-02T06:30:00+00:00",
                "Run",
                8000.0,
                2400,
                2450,
                60.0,
                None,
                4.2,
                3.4,
                152.0,
                172.0,
                None,
            ],
        )
        self.conn.execute(
            "INSERT INTO strava_laps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [200, None, 152.0, 3.4, 1000.0, 1, 1, 172.0, 4.2, 300, 1],
        )

        extractor = MagicMock()
        extractor.fetch_activities.return_value = []

        with patch.object(db_build, "StravaExtractor", return_value=extractor):
            db_build.build_strava(self.conn)

        extractor.fetch_activities.assert_called_once_with(
            after=db_build._iso_to_unix_timestamp("2024-04-02 06:30:00")
        )
        extractor.iter_laps_batched.assert_not_called()


if __name__ == "__main__":
    unittest.main()
