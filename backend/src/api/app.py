from __future__ import annotations

from datetime import date
from typing import Literal
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

from services.duckdb_service import (
    get_activities_for_date,
    get_activity_calendar,
    get_activity_calendar_sports,
    get_activity_calendar_years,
    get_activity_records,
    get_activity_session,
    get_activity_sessions,
    get_activity_sport_summary,
    get_daily_steps,
    get_daily_summaries,
    get_dataset_records,
    get_heart_rate_trends,
    get_hydration,
    get_sleep,
    get_sleep_weekly_stats,
    get_strava_activities,
    get_strava_laps,
    get_strava_months,
    get_stress,
    get_unified_activities,
    get_vo2_max,
    get_vo2_max_trends,
    list_datasets,
)
from services.training_plan_service import (
    generate_training_plan,
    get_active_training_plan,
    get_upcoming_plan_workouts,
    list_training_plans,
    update_training_plan_workout,
)


DAY_NAMES = {
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
}


class TrainingPlanRequest(BaseModel):
    race_type: Literal["running", "triathlon"]
    race_date: date
    goal_time: str | None = None
    event_name_or_distance: str = Field(min_length=1)
    area_of_emphasis: str = ""
    injury_history: str = ""
    other_thoughts: str = ""
    include_strength: bool = False
    include_mobility: bool = False
    equipment: str = ""
    preferred_days: list[str] = Field(default_factory=list)
    blocked_days: list[str] = Field(default_factory=list)
    triathlon_disciplines: list[str] = Field(default_factory=list)
    triathlon_notes: str = ""

    @field_validator("preferred_days", "blocked_days")
    @classmethod
    def validate_day_names(cls, values: list[str]) -> list[str]:
        normalized = []
        for value in values:
            day = value.strip().lower()
            if day not in DAY_NAMES:
                raise ValueError(f"Invalid day name: {value}")
            if day not in normalized:
                normalized.append(day)
        return normalized

    @field_validator("triathlon_disciplines")
    @classmethod
    def validate_disciplines(cls, values: list[str]) -> list[str]:
        allowed = {"swim", "bike", "run"}
        normalized = []
        for value in values:
            discipline = value.strip().lower()
            if discipline not in allowed:
                raise ValueError(f"Invalid triathlon discipline: {value}")
            if discipline not in normalized:
                normalized.append(discipline)
        return normalized

    @field_validator("race_date")
    @classmethod
    def validate_race_date(cls, value: date) -> date:
        if value < date.today():
            raise ValueError("race_date must be today or in the future.")
        return value

    def to_service_payload(self) -> dict[str, Any]:
        return {
            **self.model_dump(),
            "race_date": self.race_date.isoformat(),
        }


class TrainingPlanWorkoutUpdateRequest(BaseModel):
    workout_date: date
    discipline: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str = ""
    duration_minutes: int | None = Field(default=None, ge=0)
    distance_miles: float | None = Field(default=None, ge=0)
    intensity: str = ""
    is_rest_day: bool = False
    is_cross_training: bool = False
    mobility_notes: str = ""
    strength_notes: str = ""
    injury_notes: str = ""

    @field_validator("discipline", "title", "description", "intensity", "mobility_notes", "strength_notes", "injury_notes")
    @classmethod
    def strip_text_fields(cls, value: str) -> str:
        return value.strip()

    def to_service_payload(self) -> dict[str, Any]:
        return {
            **self.model_dump(),
            "workout_date": self.workout_date.isoformat(),
        }


app = FastAPI(
    title="Garmin Data Extraction API",
    version="0.4.0",
    description="REST API for Garmin health and activity data, powered by DuckDB.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/api/v1/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Datasets (generic)
# ---------------------------------------------------------------------------

@app.get("/api/v1/datasets")
def list_datasets_endpoint() -> dict[str, Any]:
    return {"datasets": list_datasets()}


@app.get("/api/v1/datasets/{dataset_slug}")
def get_dataset(
    dataset_slug: str,
    limit: int | None = Query(default=None, ge=1, le=10000),
) -> dict[str, Any]:
    result = get_dataset_records(dataset_slug, limit=limit)
    if not result:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset_slug!r}")
    return result


# ---------------------------------------------------------------------------
# Sleep
# ---------------------------------------------------------------------------

@app.get("/api/v1/sleep")
def list_sleep(
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    limit: int = Query(default=90, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_sleep(date_from=date_from, date_to=date_to, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Hydration
# ---------------------------------------------------------------------------

@app.get("/api/v1/hydration")
def list_hydration(
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    limit: int = Query(default=90, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_hydration(date_from=date_from, date_to=date_to, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# VO2 Max
# ---------------------------------------------------------------------------

@app.get("/api/v1/vo2max")
def list_vo2_max(
    sport: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_vo2_max(sport=sport, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Daily Summaries
# ---------------------------------------------------------------------------

@app.get("/api/v1/daily-summaries")
def list_daily_summaries(
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    limit: int = Query(default=90, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_daily_summaries(date_from=date_from, date_to=date_to, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Stress
# ---------------------------------------------------------------------------

@app.get("/api/v1/stress")
def list_stress(
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    stress_type: str | None = Query(default=None),
    limit: int = Query(default=90, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_stress(
        date_from=date_from,
        date_to=date_to,
        stress_type=stress_type,
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

@app.get("/api/v1/analytics/sleep/weekly")
def analytics_sleep_weekly(
    weeks: int = Query(default=52, ge=1, le=260),
) -> dict[str, Any]:
    return {"data": get_sleep_weekly_stats(weeks=weeks)}


@app.get("/api/v1/analytics/steps/daily")
def analytics_daily_steps(
    days: int = Query(default=365, ge=7, le=1825),
) -> dict[str, Any]:
    return {"data": get_daily_steps(days=days)}


@app.get("/api/v1/analytics/heart-rate/trends")
def analytics_hr_trends(
    days: int = Query(default=365, ge=7, le=1825),
) -> dict[str, Any]:
    return {"data": get_heart_rate_trends(days=days)}


@app.get("/api/v1/analytics/vo2max/trends")
def analytics_vo2max_trends() -> dict[str, Any]:
    return {"data": get_vo2_max_trends()}


@app.get("/api/v1/analytics/activity-calendar")
def analytics_activity_calendar(
    year: int = Query(default=2024, ge=2000, le=2100),
    sport: str | None = Query(default=None),
) -> dict[str, Any]:
    return {"year": year, "days": get_activity_calendar(year, sport=sport)}


@app.get("/api/v1/analytics/activity-calendar/years")
def analytics_activity_calendar_years() -> dict[str, Any]:
    return {"years": get_activity_calendar_years()}


@app.get("/api/v1/analytics/activity-calendar/sports")
def analytics_activity_calendar_sports() -> dict[str, Any]:
    return {"sports": get_activity_calendar_sports()}


@app.get("/api/v1/analytics/activities-for-date")
def analytics_activities_for_date(
    date: str = Query(description="Date in YYYY-MM-DD format"),
) -> dict[str, Any]:
    return {"date": date, "activities": get_activities_for_date(date)}


@app.get("/api/v1/analytics/activities/summary")
def analytics_activity_summary() -> dict[str, Any]:
    return {"data": get_activity_sport_summary()}


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

@app.get("/api/v1/activities")
def list_activities(
    sport: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    sort_by: str = Query(default="start_time"),
    sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_activity_sessions(
        sport=sport,
        date_from=date_from,
        date_to=date_to,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )


@app.get("/api/v1/activities/{activity_id}")
def get_activity(activity_id: str) -> dict[str, Any]:
    session = get_activity_session(activity_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Activity {activity_id!r} not found.")
    return {"activity_id": activity_id, "session": session}


@app.get("/api/v1/activities/{activity_id}/records")
def get_activity_timeseries(activity_id: str) -> dict[str, Any]:
    records = get_activity_records(activity_id)
    return {"activity_id": activity_id, "record_count": len(records), "records": records}


# ---------------------------------------------------------------------------
# Strava
# ---------------------------------------------------------------------------

@app.get("/api/v1/strava/months")
def list_strava_months() -> dict[str, Any]:
    return {"months": get_strava_months()}


@app.get("/api/v1/strava/activities")
def list_strava_activities(
    sport: str | None = Query(default=None),
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_strava_activities(
        sport=sport,
        year=year,
        month=month,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )


@app.get("/api/v1/strava/activities/{activity_id}/laps")
def list_strava_laps(activity_id: int) -> dict[str, Any]:
    laps = get_strava_laps(activity_id)
    return {"activity_id": activity_id, "lap_count": len(laps), "laps": laps}


# ---------------------------------------------------------------------------
# Unified activities (Garmin + Strava)
# ---------------------------------------------------------------------------

@app.get("/api/v1/activities/unified")
def list_unified_activities(
    sport: str | None = Query(default=None),
    data_source: str | None = Query(default=None, pattern="^(garmin|strava)$"),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    sort_by: str = Query(default="start_time"),
    sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_unified_activities(
        sport=sport,
        data_source=data_source,
        date_from=date_from,
        date_to=date_to,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# Training plans
# ---------------------------------------------------------------------------


@app.post("/api/v1/training-plans/generate")
def create_training_plan(request: TrainingPlanRequest) -> dict[str, Any]:
    try:
        return generate_training_plan(request.to_service_payload())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/training-plans/active")
def get_active_training_plan_endpoint() -> dict[str, Any]:
    plan = get_active_training_plan()
    if plan is None:
        return {"plan": None, "weeks": [], "history_summary": None, "prompt_payload": None, "wizard_input": None}
    return plan


@app.get("/api/v1/training-plans/upcoming")
def get_upcoming_training_plan(days: int = Query(default=7, ge=1, le=21)) -> dict[str, Any]:
    return get_upcoming_plan_workouts(days=days)


@app.get("/api/v1/training-plans")
def list_training_plans_endpoint() -> dict[str, Any]:
    return {"plans": list_training_plans()}


@app.put("/api/v1/training-plans/workouts/{workout_id}")
def update_training_plan_workout_endpoint(
    workout_id: str,
    request: TrainingPlanWorkoutUpdateRequest,
) -> dict[str, Any]:
    try:
        return update_training_plan_workout(workout_id, request.to_service_payload())
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
