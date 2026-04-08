from __future__ import annotations

import sys
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pydantic import ValidationError

from api.app import (
    TrainingPlanRequest,
    create_training_plan,
    get_active_training_plan_endpoint,
    get_upcoming_training_plan,
    health_check,
)


class ApiTests(unittest.TestCase):
    def test_health_check_returns_ok(self) -> None:
        self.assertEqual(health_check(), {"status": "ok"})

    @patch("api.app.generate_training_plan")
    def test_generate_training_plan_endpoint_returns_service_payload(self, mock_generate) -> None:
        mock_generate.return_value = {"plan": {"plan_id": "plan-1"}, "weeks": []}

        request = TrainingPlanRequest(
            race_type="running",
            race_date=date.today() + timedelta(days=30),
            goal_time="4h 00m",
            event_name_or_distance="Marathon",
            area_of_emphasis="Durability",
            injury_history="None",
            other_thoughts="Prefer two quality sessions max.",
            include_strength=True,
            include_mobility=True,
            equipment="Dumbbells",
            preferred_days=["tuesday", "thursday"],
            blocked_days=["monday"],
            triathlon_disciplines=[],
            triathlon_notes="",
        )

        payload = create_training_plan(request)
        self.assertEqual(payload["plan"]["plan_id"], "plan-1")
        mock_generate.assert_called_once()

    def test_generate_training_plan_endpoint_rejects_past_race_date(self) -> None:
        with self.assertRaises(ValidationError):
            TrainingPlanRequest(
                race_type="running",
                race_date=date.today() - timedelta(days=1),
                event_name_or_distance="Half marathon",
            )

    @patch("api.app.get_active_training_plan")
    def test_active_training_plan_endpoint_returns_empty_shape_when_missing(self, mock_get_active) -> None:
        mock_get_active.return_value = None

        payload = get_active_training_plan_endpoint()
        self.assertIsNone(payload["plan"])
        self.assertEqual(payload["weeks"], [])

    @patch("api.app.get_upcoming_plan_workouts")
    def test_upcoming_training_plan_endpoint_returns_service_payload(self, mock_get_upcoming) -> None:
        mock_get_upcoming.return_value = {
            "plan": {"plan_id": "plan-1"},
            "days": [{"workout_date": date.today().isoformat(), "title": "Easy run"}],
        }

        payload = get_upcoming_training_plan(days=7)
        self.assertEqual(payload["days"][0]["title"], "Easy run")


if __name__ == "__main__":
    unittest.main()
