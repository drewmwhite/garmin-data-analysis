from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from services import duckdb_service, training_plan_service


class TrainingPlanServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.duckdb"
        conn = duckdb.connect(str(self.db_path))
        conn.execute(
            """
            CREATE TABLE unified_activities (
                activity_id VARCHAR,
                data_source VARCHAR,
                name VARCHAR,
                sport VARCHAR,
                start_time TIMESTAMP,
                total_distance_m DOUBLE,
                moving_time_s DOUBLE,
                elapsed_time_s DOUBLE,
                total_elevation_gain_m DOUBLE,
                avg_heart_rate DOUBLE,
                max_heart_rate DOUBLE,
                avg_speed_ms DOUBLE,
                max_speed_ms DOUBLE,
                total_calories DOUBLE,
                avg_cadence DOUBLE
            )
            """
        )
        today = date.today()
        rows = [
            ("1", "strava", "Evening Run", "run", today.isoformat(), 8046.7, 2400, 2500, 90, 150, 170, 3.2, 4.0, None, None),
            ("2", "garmin", "", "running", (today - timedelta(days=7)).isoformat(), 9656.1, 3200, 3300, 120, 148, 168, 3.0, 3.9, 700, 82),
            ("3", "strava", "Lunch Ride", "ride", (today - timedelta(days=3)).isoformat(), 32186.9, 4200, 4300, 240, 138, 158, 7.4, 9.0, None, None),
            ("4", "strava", "Recovery Walk", "walk", (today - timedelta(days=2)).isoformat(), 3218.7, 2100, 2200, 15, 101, 116, 1.5, 2.0, None, None),
        ]
        conn.executemany("INSERT INTO unified_activities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.close()

        self.db_patch = patch("services.training_plan_service.DB_PATH", self.db_path)
        self.duckdb_db_patch = patch("services.duckdb_service.DB_PATH", self.db_path)
        self.db_patch.start()
        self.duckdb_db_patch.start()
        duckdb_service._conn = None
        duckdb_service._conn_mtime = 0.0

    def tearDown(self) -> None:
        self.db_patch.stop()
        self.duckdb_db_patch.stop()
        if duckdb_service._conn is not None:
            duckdb_service._conn.close()
            duckdb_service._conn = None
        self.temp_dir.cleanup()

    def _mock_plan(self, race_date: str) -> dict:
        start = date.today()
        return {
            "plan_title": "Build to race day",
            "overview": "A progressive plan with recovery built in.",
            "weeks": [
                {
                    "week_number": 1,
                    "week_start": start.isoformat(),
                    "week_end": (start + timedelta(days=6)).isoformat(),
                    "focus": "Rebuild consistency",
                    "summary": "Easy aerobic work plus strength and mobility.",
                    "workouts": [
                        {
                            "workout_date": start.isoformat(),
                            "discipline": "running",
                            "title": "Easy run",
                            "description": "Comfortable aerobic run.",
                            "duration_minutes": 40,
                            "distance_miles": 4.0,
                            "intensity": "easy",
                            "is_rest_day": False,
                            "is_cross_training": False,
                            "mobility_notes": "",
                            "strength_notes": "",
                            "injury_notes": "Monitor symptoms.",
                        },
                        {
                            "workout_date": (start + timedelta(days=1)).isoformat(),
                            "discipline": "mobility",
                            "title": "Mobility session",
                            "description": "Yoga and nerve glide work.",
                            "duration_minutes": 25,
                            "distance_miles": None,
                            "intensity": "recovery",
                            "is_rest_day": False,
                            "is_cross_training": True,
                            "mobility_notes": "Hip and hamstring mobility.",
                            "strength_notes": "",
                            "injury_notes": "Keep effort easy.",
                        },
                    ],
                },
                {
                    "week_number": 2,
                    "week_start": (start + timedelta(days=7)).isoformat(),
                    "week_end": date.fromisoformat(race_date).isoformat(),
                    "focus": "Race week",
                    "summary": "Keep volume low and arrive fresh.",
                    "workouts": [
                        {
                            "workout_date": race_date,
                            "discipline": "running",
                            "title": "Race day",
                            "description": "Target effort and fuel early.",
                            "duration_minutes": 300,
                            "distance_miles": 50.0,
                            "intensity": "race",
                            "is_rest_day": False,
                            "is_cross_training": False,
                            "mobility_notes": "",
                            "strength_notes": "",
                            "injury_notes": "Warm up carefully.",
                        }
                    ],
                },
            ],
        }

    def _mock_plan_with_post_race_workout(self, race_date: str) -> dict:
        payload = self._mock_plan(race_date)
        payload["weeks"][-1]["workouts"].append(
            {
                "workout_date": (date.fromisoformat(race_date) + timedelta(days=1)).isoformat(),
                "discipline": "running",
                "title": "Too late shakeout",
                "description": "This should be removed.",
                "duration_minutes": 20,
                "distance_miles": 2.0,
                "intensity": "easy",
                "is_rest_day": False,
                "is_cross_training": False,
                "mobility_notes": "",
                "strength_notes": "",
                "injury_notes": "",
            }
        )
        payload["weeks"][-1]["week_end"] = (date.fromisoformat(race_date) + timedelta(days=1)).isoformat()
        return payload

    def test_get_training_history_summary_normalizes_sports(self) -> None:
        summary = training_plan_service.get_training_history_summary()

        sport_mix = {row["discipline"]: row["session_count"] for row in summary["sport_mix"]}
        self.assertEqual(sport_mix["running"], 2)
        self.assertEqual(sport_mix["cycling"], 1)
        self.assertEqual(sport_mix["walking"], 1)
        self.assertLessEqual(len(summary["recent_workouts"]), 6)
        self.assertIn("weekly_volume_overall", summary)
        self.assertIn("weekly_volume_by_top_sport", summary)

    def test_generate_training_plan_reuses_cached_match_without_second_openai_call(self) -> None:
        request_payload = {
            "race_type": "running",
            "race_date": (date.today() + timedelta(days=8)).isoformat(),
            "goal_time": "8h 30m",
            "event_name_or_distance": "50-mile trail race",
            "area_of_emphasis": "Durability",
            "injury_history": "Recent sciatica",
            "other_thoughts": "Prefer conservative progression.",
            "include_strength": True,
            "include_mobility": True,
            "equipment": "Pull-up bar, dumbbells",
            "preferred_days": ["tuesday"],
            "blocked_days": [],
            "triathlon_disciplines": [],
            "triathlon_notes": "",
        }

        with patch(
            "services.training_plan_service._call_openai_plan",
            return_value=self._mock_plan(request_payload["race_date"]),
        ) as mock_call:
            first = training_plan_service.generate_training_plan(request_payload)
            second = training_plan_service.generate_training_plan(request_payload)

        self.assertEqual(first["plan"]["status"], "active")
        self.assertEqual(second["plan"]["status"], "active")
        self.assertEqual(first["plan"]["plan_id"], second["plan"]["plan_id"])
        self.assertEqual(mock_call.call_count, 1)

        plans = training_plan_service.list_training_plans()
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["status"], "active")

    def test_get_upcoming_plan_workouts_returns_next_seven_days(self) -> None:
        request_payload = {
            "race_type": "running",
            "race_date": (date.today() + timedelta(days=8)).isoformat(),
            "goal_time": None,
            "event_name_or_distance": "10k",
            "area_of_emphasis": "Return to consistency",
            "injury_history": "",
            "other_thoughts": "",
            "include_strength": False,
            "include_mobility": True,
            "equipment": "",
            "preferred_days": [],
            "blocked_days": [],
            "triathlon_disciplines": [],
            "triathlon_notes": "",
        }

        with patch("services.training_plan_service._call_openai_plan", return_value=self._mock_plan(request_payload["race_date"])):
            training_plan_service.generate_training_plan(request_payload)

        upcoming = training_plan_service.get_upcoming_plan_workouts(days=7)
        self.assertIsNotNone(upcoming["plan"])
        self.assertGreaterEqual(len(upcoming["days"]), 2)
        for item in upcoming["days"]:
            workout_day = date.fromisoformat(item["workout_date"])
            self.assertLessEqual((workout_day - date.today()).days, 6)

    def test_generate_training_plan_trims_post_race_workouts_and_saves_artifact(self) -> None:
        request_payload = {
            "race_type": "running",
            "race_date": (date.today() + timedelta(days=8)).isoformat(),
            "goal_time": None,
            "event_name_or_distance": "10k",
            "area_of_emphasis": "Return to consistency",
            "injury_history": "",
            "other_thoughts": "",
            "include_strength": False,
            "include_mobility": True,
            "equipment": "",
            "preferred_days": [],
            "blocked_days": [],
            "triathlon_disciplines": [],
            "triathlon_notes": "",
        }

        artifact_dir = Path(self.temp_dir.name) / "artifacts"

        with patch("services.training_plan_service.LOG_DIR", artifact_dir), patch(
            "services.training_plan_service._call_openai_plan",
            return_value=self._mock_plan_with_post_race_workout(request_payload["race_date"]),
        ):
            plan = training_plan_service.generate_training_plan(request_payload)

        self.assertIsNotNone(plan["plan"])
        all_workouts = [w for week in plan["weeks"] for w in week["workouts"]]
        self.assertNotIn(
            (date.fromisoformat(request_payload["race_date"]) + timedelta(days=1)).isoformat(),
            [w["workout_date"] for w in all_workouts],
        )
        artifacts = list((artifact_dir / "training_plan_responses").glob("*.json"))
        self.assertEqual(len(artifacts), 1)

    def test_update_training_plan_workout_updates_fields_and_week_assignment(self) -> None:
        request_payload = {
            "race_type": "running",
            "race_date": (date.today() + timedelta(days=8)).isoformat(),
            "goal_time": None,
            "event_name_or_distance": "10k",
            "area_of_emphasis": "Return to consistency",
            "injury_history": "",
            "other_thoughts": "",
            "include_strength": False,
            "include_mobility": True,
            "equipment": "",
            "preferred_days": [],
            "blocked_days": [],
            "triathlon_disciplines": [],
            "triathlon_notes": "",
        }

        with patch("services.training_plan_service._call_openai_plan", return_value=self._mock_plan(request_payload["race_date"])):
            plan = training_plan_service.generate_training_plan(request_payload)

        workout = plan["weeks"][0]["workouts"][0]
        updated = training_plan_service.update_training_plan_workout(
            workout["workout_id"],
            {
                "workout_date": plan["weeks"][1]["week_start"],
                "discipline": "cycling",
                "title": "Edited workout",
                "description": "Updated description.",
                "duration_minutes": 55,
                "distance_miles": 18.5,
                "intensity": "steady",
                "is_rest_day": False,
                "is_cross_training": True,
                "mobility_notes": "Open hips.",
                "strength_notes": "Light lifting.",
                "injury_notes": "Back off if symptoms return.",
            },
        )

        week_one_ids = {item["workout_id"] for item in updated["weeks"][0]["workouts"]}
        week_two_workout = next(item for item in updated["weeks"][1]["workouts"] if item["workout_id"] == workout["workout_id"])

        self.assertNotIn(workout["workout_id"], week_one_ids)
        self.assertEqual(week_two_workout["title"], "Edited workout")
        self.assertEqual(week_two_workout["discipline"], "cycling")
        self.assertEqual(week_two_workout["duration_minutes"], 55)
        self.assertEqual(week_two_workout["distance_miles"], 18.5)
        self.assertTrue(week_two_workout["is_cross_training"])

    def test_update_training_plan_workout_rejects_date_outside_plan(self) -> None:
        request_payload = {
            "race_type": "running",
            "race_date": (date.today() + timedelta(days=8)).isoformat(),
            "goal_time": None,
            "event_name_or_distance": "10k",
            "area_of_emphasis": "Return to consistency",
            "injury_history": "",
            "other_thoughts": "",
            "include_strength": False,
            "include_mobility": True,
            "equipment": "",
            "preferred_days": [],
            "blocked_days": [],
            "triathlon_disciplines": [],
            "triathlon_notes": "",
        }

        with patch("services.training_plan_service._call_openai_plan", return_value=self._mock_plan(request_payload["race_date"])):
            plan = training_plan_service.generate_training_plan(request_payload)

        workout = plan["weeks"][0]["workouts"][0]
        with self.assertRaises(ValueError):
            training_plan_service.update_training_plan_workout(
                workout["workout_id"],
                {
                    "workout_date": (date.fromisoformat(request_payload["race_date"]) + timedelta(days=14)).isoformat(),
                    "discipline": "running",
                    "title": "Edited workout",
                    "description": "Updated description.",
                    "duration_minutes": 55,
                    "distance_miles": 18.5,
                    "intensity": "steady",
                    "is_rest_day": False,
                    "is_cross_training": False,
                    "mobility_notes": "",
                    "strength_notes": "",
                    "injury_notes": "",
                },
            )


if __name__ == "__main__":
    unittest.main()
