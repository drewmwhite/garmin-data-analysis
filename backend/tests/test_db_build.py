from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "backend" / "src"))

from db import build as db_build


def _strava_overlap_after(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int((parsed - timedelta(days=30)).timestamp())


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
        self.conn.execute(
            """
            CREATE TABLE strava_lap_fetch_status (
                workout_id BIGINT PRIMARY KEY,
                fetched_at TIMESTAMP,
                lap_count INTEGER
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
    def test_build_strava_refetches_overlap_window_and_missing_laps(self) -> None:
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
            after=_strava_overlap_after("2024-04-02T06:30:00+00:00")
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

        fetch_status_ids = {
            row[0] for row in self.conn.execute("SELECT workout_id FROM strava_lap_fetch_status").fetchall()
        }
        self.assertEqual(fetch_status_ids, {100, 400})

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
            after=_strava_overlap_after("2024-04-02T06:30:00+00:00")
        )
        extractor.iter_laps_batched.assert_not_called()

    @patch.dict(
        "os.environ",
        {
            "STRAVA_CLIENT_ID": "client-id",
            "STRAVA_CLIENT_SECRET": "client-secret",
            "STRAVA_REFRESH_TOKEN": "refresh-token",
        },
        clear=False,
    )
    def test_build_strava_marks_zero_lap_runs_as_fetched(self) -> None:
        self.conn.executemany(
            "INSERT INTO strava_activities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
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
                ),
            ],
        )

        extractor = MagicMock()
        extractor.fetch_activities.return_value = []
        extractor.iter_laps_batched.return_value = [([], [200])]

        with patch.object(db_build, "StravaExtractor", return_value=extractor):
            db_build.build_strava(self.conn)

        extractor.iter_laps_batched.assert_called_once_with([200])
        status_rows = self.conn.execute(
            "SELECT workout_id, lap_count FROM strava_lap_fetch_status"
        ).fetchall()
        self.assertEqual(status_rows, [(200, 0)])

        extractor = MagicMock()
        extractor.fetch_activities.return_value = []

        with patch.object(db_build, "StravaExtractor", return_value=extractor):
            db_build.build_strava(self.conn)

        extractor.fetch_activities.assert_called_once_with(
            after=_strava_overlap_after("2024-04-02T06:30:00+00:00")
        )
        extractor.iter_laps_batched.assert_not_called()

    @patch.dict(
        "os.environ",
        {
            "STRAVA_CLIENT_ID": "client-id",
            "STRAVA_CLIENT_SECRET": "client-secret",
            "STRAVA_REFRESH_TOKEN": "refresh-token",
        },
        clear=False,
    )
    def test_build_strava_recovers_missed_run_inside_overlap_window(self) -> None:
        self.conn.execute(
            "INSERT INTO strava_activities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                200,
                "Newest Existing Run",
                "2024-04-10T06:30:00+00:00",
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
        extractor.fetch_activities.return_value = [
            {
                "id": 150,
                "name": "Recovered Run",
                "start_date_local": "2024-04-05T06:45:00+00:00",
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
                        "workout_id": 150,
                        "average_cadence": None,
                        "average_heartrate": 154.0,
                        "average_speed": 3.5,
                        "distance": 1000.0,
                        "lap_id": 151,
                        "lap_index": 1,
                        "max_heartrate": 174.0,
                        "max_speed": 4.3,
                        "moving_time": 300,
                        "split": 1,
                    }
                ],
                [150],
            )
        ]

        with patch.object(db_build, "StravaExtractor", return_value=extractor):
            db_build.build_strava(self.conn)

        extractor.fetch_activities.assert_called_once_with(
            after=_strava_overlap_after("2024-04-10T06:30:00+00:00")
        )
        extractor.iter_laps_batched.assert_called_once_with([150])

        activity_ids = {
            row[0] for row in self.conn.execute("SELECT id FROM strava_activities").fetchall()
        }
        self.assertEqual(activity_ids, {150, 200})

        lap_workout_ids = {
            row[0] for row in self.conn.execute("SELECT DISTINCT workout_id FROM strava_laps").fetchall()
        }
        self.assertEqual(lap_workout_ids, {150, 200})

    @patch.dict(
        "os.environ",
        {
            "STRAVA_CLIENT_ID": "client-id",
            "STRAVA_CLIENT_SECRET": "client-secret",
            "STRAVA_REFRESH_TOKEN": "refresh-token",
        },
        clear=False,
    )
    def test_build_strava_never_uploads_to_s3(self) -> None:
        extractor = MagicMock()
        extractor.fetch_activities.return_value = []

        with (
            patch.object(db_build, "StravaExtractor", return_value=extractor),
            patch.object(db_build, "s3_upload") as s3_upload_mock,
        ):
            db_build.build_strava(self.conn, s3_base="s3://bucket/prefix")

        s3_upload_mock.assert_not_called()

    @patch.dict(
        "os.environ",
        {
            "STRAVA_CLIENT_ID": "client-id",
            "STRAVA_CLIENT_SECRET": "client-secret",
            "STRAVA_REFRESH_TOKEN": "refresh-token",
        },
        clear=False,
    )
    def test_build_strava_recent_days_upserts_recent_activities_and_laps(self) -> None:
        self.conn.execute(
            "INSERT INTO strava_activities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                200,
                "Old Name",
                "2024-04-10T06:30:00+00:00",
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
        self.conn.execute(
            """
            INSERT INTO strava_lap_fetch_status (workout_id, fetched_at, lap_count)
            VALUES (200, TIMESTAMP '2024-04-10 07:00:00', 1)
            """
        )

        extractor = MagicMock()
        extractor.fetch_activities.return_value = [
            {
                "id": 200,
                "name": "Updated Name",
                "start_date_local": "2024-04-10T06:30:00+00:00",
                "type": "Run",
                "distance": 8050.0,
                "moving_time": 2390,
                "elapsed_time": 2440,
                "total_elevation_gain": 65.0,
                "end_latlng": None,
                "max_speed": 4.4,
                "average_speed": 3.6,
                "average_heartrate": 153.0,
                "max_heartrate": 173.0,
                "summary_polyline": None,
            }
        ]
        extractor.iter_laps_batched.return_value = [
            (
                [
                    {
                        "workout_id": 200,
                        "average_cadence": None,
                        "average_heartrate": 153.0,
                        "average_speed": 3.6,
                        "distance": 1600.0,
                        "lap_id": 2,
                        "lap_index": 1,
                        "max_heartrate": 173.0,
                        "max_speed": 4.4,
                        "moving_time": 380,
                        "split": 1,
                    }
                ],
                [200],
            )
        ]

        fixed_now = datetime(2024, 4, 14, 12, 0, tzinfo=timezone.utc)
        with (
            patch.object(db_build, "StravaExtractor", return_value=extractor),
            patch.object(db_build, "_utc_now", return_value=fixed_now),
        ):
            db_build.build_strava(self.conn, strava_recent_days=30)

        extractor.fetch_activities.assert_called_once_with(
            after=int((fixed_now - timedelta(days=30)).timestamp())
        )
        extractor.iter_laps_batched.assert_called_once_with([200])

        activity_row = self.conn.execute(
            "SELECT name, distance FROM strava_activities WHERE id = 200"
        ).fetchone()
        self.assertEqual(activity_row, ("Updated Name", 8050.0))

        lap_rows = self.conn.execute(
            "SELECT workout_id, lap_id, distance FROM strava_laps WHERE workout_id = 200"
        ).fetchall()
        self.assertEqual(lap_rows, [(200, 2, 1600.0)])

        status_rows = self.conn.execute(
            "SELECT workout_id, lap_count FROM strava_lap_fetch_status WHERE workout_id = 200"
        ).fetchall()
        self.assertEqual(status_rows, [(200, 1)])

    def test_build_strava_can_replay_cached_json_without_api_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            (cache_dir / "activities").mkdir(parents=True, exist_ok=True)
            (cache_dir / "activity_details").mkdir(parents=True, exist_ok=True)

            (cache_dir / "activities" / "page-0001.json").write_text(
                json.dumps(
                    {
                        "raw_activities": [
                            {
                                "id": 500,
                                "name": "Cached Run",
                                "start_date_local": "2024-04-04T06:45:00+00:00",
                                "type": "Run",
                                "distance": 10000.0,
                                "moving_time": 3000,
                                "elapsed_time": 3050,
                                "total_elevation_gain": 75.0,
                                "end_latlng": None,
                                "max_speed": 4.3,
                                "average_speed": 3.5,
                                "has_heartrate": True,
                                "average_heartrate": 154.0,
                                "max_heartrate": 174.0,
                                "map": {"summary_polyline": None},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (cache_dir / "activity_details" / "500.json").write_text(
                json.dumps(
                    {
                        "activity_id": 500,
                        "laps": [
                            {
                                "id": 501,
                                "lap_index": 1,
                                "average_cadence": None,
                                "average_heartrate": 154.0,
                                "average_speed": 3.5,
                                "distance": 1000.0,
                                "max_heartrate": 174.0,
                                "max_speed": 4.3,
                                "moving_time": 300,
                                "split": 1,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            db_build.build_strava(self.conn, strava_cache_dir=str(cache_dir))

        activity_ids = {
            row[0] for row in self.conn.execute("SELECT id FROM strava_activities").fetchall()
        }
        self.assertEqual(activity_ids, {500})

        lap_workout_ids = {
            row[0] for row in self.conn.execute("SELECT DISTINCT workout_id FROM strava_laps").fetchall()
        }
        self.assertEqual(lap_workout_ids, {500})


class S3UploadTests(unittest.TestCase):
    def test_s3_upload_omits_partition_clause_when_partition_list_is_empty(self) -> None:
        conn = MagicMock()

        db_build.s3_upload(
            conn,
            "strava_laps",
            "s3://bucket/prefix",
            "SELECT * FROM strava_laps",
            [],
        )

        executed_sql = conn.execute.call_args.args[0]
        self.assertIn("FORMAT PARQUET", executed_sql)
        self.assertIn("OVERWRITE_OR_IGNORE TRUE", executed_sql)
        self.assertNotIn("PARTITION_BY", executed_sql)


if __name__ == "__main__":
    unittest.main()
