from __future__ import annotations

import json
import logging
import os
import socket
import threading
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb
from dotenv import load_dotenv

from services.duckdb_service import DB_PATH, REPO_ROOT

load_dotenv(REPO_ROOT / ".env")

_WRITE_LOCK = threading.Lock()

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5")
DEFAULT_OPENAI_TIMEOUT_SECONDS = float(os.environ.get("OPENAI_TIMEOUT_SECONDS", "600"))
DEFAULT_OPENAI_REASONING_EFFORT = os.environ.get("OPENAI_REASONING_EFFORT", "low")
LOG_DIR = REPO_ROOT / "logs"
TRAINING_PLAN_LOG_PATH = LOG_DIR / "training_plan_service.log"


def _configure_logger() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("training_plan_service")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(TRAINING_PLAN_LOG_PATH)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = _configure_logger()

_SPORT_ALIASES = {
    "run": "running",
    "running": "running",
    "ride": "cycling",
    "cycling": "cycling",
    "swim": "swimming",
    "swimming": "swimming",
    "walk": "walking",
    "walking": "walking",
    "hike": "hiking",
    "hiking": "hiking",
    "yoga": "yoga",
    "strength": "strength",
    "strength training": "strength",
    "mobility": "mobility",
}

TRAINING_PLAN_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["plan_title", "overview", "weeks"],
    "properties": {
        "plan_title": {"type": "string"},
        "overview": {"type": "string"},
        "weeks": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["week_number", "week_start", "week_end", "focus", "summary", "workouts"],
                "properties": {
                    "week_number": {"type": "integer"},
                    "week_start": {"type": "string", "format": "date"},
                    "week_end": {"type": "string", "format": "date"},
                    "focus": {"type": "string"},
                    "summary": {"type": "string"},
                    "workouts": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "workout_date",
                                "discipline",
                                "title",
                                "description",
                                "duration_minutes",
                                "distance_miles",
                                "intensity",
                                "is_rest_day",
                                "is_cross_training",
                                "mobility_notes",
                                "strength_notes",
                                "injury_notes",
                            ],
                            "properties": {
                                "workout_date": {"type": "string", "format": "date"},
                                "discipline": {"type": "string"},
                                "title": {"type": "string"},
                                "description": {"type": "string"},
                                "duration_minutes": {"type": "integer", "minimum": 0},
                                "distance_miles": {
                                    "anyOf": [
                                        {"type": "number", "minimum": 0},
                                        {"type": "null"},
                                    ]
                                },
                                "intensity": {
                                    "type": "string",
                                    "enum": ["rest", "recovery", "easy", "steady", "moderate", "hard", "race"],
                                },
                                "is_rest_day": {"type": "boolean"},
                                "is_cross_training": {"type": "boolean"},
                                "mobility_notes": {"type": "string"},
                                "strength_notes": {"type": "string"},
                                "injury_notes": {"type": "string"},
                            },
                        },
                    },
                },
            },
        },
    },
}


def _normalized_sport_case_sql(column: str) -> str:
    return f"""
    CASE
        WHEN LOWER(COALESCE({column}, '')) IN ('run', 'running') THEN 'running'
        WHEN LOWER(COALESCE({column}, '')) IN ('ride', 'cycling') THEN 'cycling'
        WHEN LOWER(COALESCE({column}, '')) IN ('swim', 'swimming') THEN 'swimming'
        WHEN LOWER(COALESCE({column}, '')) IN ('walk', 'walking') THEN 'walking'
        WHEN LOWER(COALESCE({column}, '')) IN ('hike', 'hiking') THEN 'hiking'
        WHEN LOWER(COALESCE({column}, '')) IN ('strength', 'strength training') THEN 'strength'
        ELSE LOWER(COALESCE({column}, 'other'))
    END
    """


def _normalize_sport_name(value: str | None) -> str:
    key = (value or "other").strip().lower()
    return _SPORT_ALIASES.get(key, key or "other")


def _connect_rw() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise RuntimeError(
            f"garmin.duckdb not found at {DB_PATH}. Run `python db/build.py` to build the database first."
        )
    return duckdb.connect(str(DB_PATH))


def _query_rows(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    conn = duckdb.connect(str(DB_PATH))
    try:
        rel = conn.execute(sql, params or [])
        columns = [desc[0] for desc in rel.description]
        return [dict(zip(columns, row)) for row in rel.fetchall()]
    finally:
        conn.close()


def ensure_training_plan_tables(conn: duckdb.DuckDBPyConnection | None = None) -> None:
    owns_conn = conn is None
    if owns_conn:
        conn = _connect_rw()

    try:
        assert conn is not None
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS training_plans (
                plan_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP,
                archived_at TIMESTAMP,
                status VARCHAR,
                model_name VARCHAR,
                race_type VARCHAR,
                race_date DATE,
                goal_time VARCHAR,
                event_name_or_distance VARCHAR,
                plan_title VARCHAR,
                overview VARCHAR,
                request_payload_json VARCHAR,
                history_summary_json VARCHAR,
                prompt_payload_json VARCHAR,
                response_json VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS training_plan_weeks (
                plan_id VARCHAR,
                week_number INTEGER,
                week_start DATE,
                week_end DATE,
                focus VARCHAR,
                summary VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS training_plan_workouts (
                workout_id VARCHAR,
                plan_id VARCHAR,
                week_number INTEGER,
                workout_date DATE,
                discipline VARCHAR,
                title VARCHAR,
                description VARCHAR,
                duration_minutes INTEGER,
                distance_miles DOUBLE,
                intensity VARCHAR,
                is_rest_day BOOLEAN,
                is_cross_training BOOLEAN,
                mobility_notes VARCHAR,
                strength_notes VARCHAR,
                injury_notes VARCHAR
            )
            """
        )
    finally:
        if owns_conn and conn is not None:
            conn.close()


def get_training_history_summary() -> dict[str, Any]:
    logger.info("Building training history summary from unified_activities.")
    sport_case = _normalized_sport_case_sql("sport")
    recent_workouts = _query_rows(
        f"""
        SELECT
            CAST(start_time AS DATE) AS workout_date,
            {sport_case} AS discipline,
            ROUND(total_distance_m / 1609.344, 2) AS distance_miles,
            ROUND(moving_time_s / 60.0, 1) AS duration_minutes,
            ROUND(COALESCE(avg_heart_rate, 0), 0) AS avg_heart_rate,
            ROUND(COALESCE(total_elevation_gain_m, 0), 0) AS elevation_gain_m
        FROM unified_activities
        WHERE start_time IS NOT NULL
        ORDER BY start_time DESC
        LIMIT 6
        """
    )
    weekly_volume_overall = _query_rows(
        f"""
        SELECT
            DATE_TRUNC('week', CAST(start_time AS DATE)) AS week_start,
            COUNT(*) AS session_count,
            ROUND(SUM(COALESCE(total_distance_m, 0)) / 1609.344, 1) AS total_miles,
            ROUND(SUM(COALESCE(moving_time_s, 0)) / 3600.0, 1) AS total_hours
        FROM unified_activities
        WHERE start_time >= CURRENT_DATE - INTERVAL 84 DAY
        GROUP BY 1
        ORDER BY week_start DESC
        LIMIT 8
        """
    )
    sport_mix = _query_rows(
        f"""
        SELECT
            {sport_case} AS discipline,
            COUNT(*) AS session_count
        FROM unified_activities
        WHERE start_time >= CURRENT_DATE - INTERVAL 90 DAY
        GROUP BY 1
        ORDER BY session_count DESC, discipline
        LIMIT 5
        """
    )
    top_disciplines = [row["discipline"] for row in sport_mix[:3]]
    weekly_volume_by_top_sport = (
        _query_rows(
            f"""
            SELECT
                DATE_TRUNC('week', CAST(start_time AS DATE)) AS week_start,
                {sport_case} AS discipline,
                COUNT(*) AS session_count,
                ROUND(SUM(COALESCE(total_distance_m, 0)) / 1609.344, 1) AS total_miles,
                ROUND(SUM(COALESCE(moving_time_s, 0)) / 3600.0, 1) AS total_hours
            FROM unified_activities
            WHERE start_time >= CURRENT_DATE - INTERVAL 56 DAY
              AND {sport_case} IN ({", ".join(["?"] * len(top_disciplines))})
            GROUP BY 1, 2
            ORDER BY week_start DESC, discipline
            LIMIT 18
            """,
            top_disciplines,
        )
        if top_disciplines
        else []
    )
    consistency_row = _query_rows(
        f"""
        WITH weekly AS (
            SELECT
                DATE_TRUNC('week', CAST(start_time AS DATE)) AS week_start,
                COUNT(*) AS session_count,
                ROUND(SUM(COALESCE(total_distance_m, 0)) / 1609.344, 1) AS total_miles,
                ROUND(SUM(COALESCE(moving_time_s, 0)) / 3600.0, 1) AS total_hours
            FROM unified_activities
            WHERE start_time >= CURRENT_DATE - INTERVAL 84 DAY
            GROUP BY 1
        )
        SELECT
            COUNT(*) AS weeks_observed,
            COALESCE(ROUND(AVG(session_count), 1), 0) AS avg_sessions_per_week,
            COALESCE(ROUND(MAX(total_miles), 1), 0) AS max_weekly_miles,
            COALESCE(ROUND(AVG(total_miles), 1), 0) AS avg_weekly_miles,
            COALESCE(ROUND(AVG(total_hours), 1), 0) AS avg_weekly_hours
        FROM weekly
        """
    )
    consistency = consistency_row[0] if consistency_row else {
        "weeks_observed": 0,
        "avg_sessions_per_week": 0,
        "max_weekly_miles": 0,
        "avg_weekly_miles": 0,
        "avg_weekly_hours": 0,
    }
    return {
        "recent_workouts": recent_workouts,
        "weekly_volume_overall": weekly_volume_overall,
        "weekly_volume_by_top_sport": weekly_volume_by_top_sport,
        "sport_mix": sport_mix,
        "consistency": consistency,
    }


def _build_prompt_payload(request_payload: dict[str, Any], history_summary: dict[str, Any]) -> dict[str, Any]:
    planning_start_date = date.today().isoformat()
    return {
        "today": planning_start_date,
        "planning_rules": {
            "single_user": True,
            "goal": "Create a progressive day-by-day training plan in 7-day weeks until race day.",
            "respect_injury_history": True,
            "respect_blocked_days": True,
            "use_preferred_days_as_soft_preferences": True,
            "include_strength": request_payload["include_strength"],
            "include_mobility": request_payload["include_mobility"],
            "start_date": planning_start_date,
            "return_compact_json_only": True,
        },
        "athlete_request": request_payload,
        "history_summary": history_summary,
    }


def _extract_output_text(response_payload: dict[str, Any]) -> str:
    fragments: list[str] = []
    for item in response_payload.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                text = content.get("text")
                if text:
                    fragments.append(text)
    if fragments:
        return "".join(fragments)
    raise RuntimeError("OpenAI response did not contain any output_text content.")


def _response_artifact_dir() -> Path:
    path = LOG_DIR / "training_plan_responses"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_response_artifact(
    *,
    request_payload: dict[str, Any],
    prompt_payload: dict[str, Any],
    raw_response_payload: dict[str, Any],
    normalized_response_payload: dict[str, Any] | None = None,
    issues: list[str] | None = None,
) -> Path:
    artifact_path = _response_artifact_dir() / (
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4()}.json"
    )
    artifact = {
        "saved_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "request_payload": request_payload,
        "prompt_payload": prompt_payload,
        "raw_response_payload": raw_response_payload,
        "normalized_response_payload": normalized_response_payload,
        "issues": issues or [],
    }
    artifact_path.write_text(json.dumps(artifact, indent=2, sort_keys=True, default=str))
    logger.info("Saved training plan response artifact to %s", artifact_path)
    return artifact_path


def _call_openai_plan(prompt_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY is missing; cannot generate training plan.")
        raise RuntimeError("OPENAI_API_KEY is not set in the environment.")

    athlete_request = prompt_payload.get("athlete_request", {})
    prompt_text = json.dumps(prompt_payload, indent=2, sort_keys=True, default=str)
    logger.info(
        "Starting OpenAI training plan request. model=%s reasoning=%s timeout_seconds=%s race_type=%s race_date=%s event=%s prompt_chars=%s recent_workouts=%s weekly_rows=%s",
        DEFAULT_OPENAI_MODEL,
        DEFAULT_OPENAI_REASONING_EFFORT,
        int(DEFAULT_OPENAI_TIMEOUT_SECONDS),
        athlete_request.get("race_type"),
        athlete_request.get("race_date"),
        athlete_request.get("event_name_or_distance"),
        len(prompt_text),
        len(prompt_payload.get("history_summary", {}).get("recent_workouts", [])),
        len(prompt_payload.get("history_summary", {}).get("weekly_volume_overall", []))
        + len(prompt_payload.get("history_summary", {}).get("weekly_volume_by_top_sport", [])),
    )

    request_payload = {
        "model": DEFAULT_OPENAI_MODEL,
        "reasoning": {"effort": DEFAULT_OPENAI_REASONING_EFFORT},
        "instructions": (
            "You are a careful endurance coach. Return only JSON that matches the provided schema. "
            "Create a safe week-by-week training plan for the athlete request. "
            "Respect injury history, reduce overload, avoid scheduling workouts on blocked days, "
            "and include day-by-day entries for every date from the planning start date through race day. "
            "Use triathlon disciplines when race_type is triathlon; otherwise focus on the relevant disciplines. "
            "When strength or mobility are enabled, include them as concrete sessions. "
            "Keep workout descriptions concise and practical."
        ),
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": prompt_text,
                    }
                ],
            }
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "training_plan",
                "strict": True,
                "schema": TRAINING_PLAN_SCHEMA,
            }
        },
    }

    body = json.dumps(request_payload).encode("utf-8")
    req = Request(
        OPENAI_RESPONSES_URL,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        started_at = perf_counter()
        with urlopen(req, timeout=DEFAULT_OPENAI_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
        duration = perf_counter() - started_at
        logger.info(
            "OpenAI request completed successfully in %.2fs. response_id=%s",
            duration,
            payload.get("id"),
        )
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        logger.exception("OpenAI request failed with HTTP %s. response=%s", exc.code, detail)
        raise RuntimeError(f"OpenAI request failed with HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        logger.exception("OpenAI request failed before response. reason=%s", exc.reason)
        raise RuntimeError(f"OpenAI request failed: {exc.reason}") from exc
    except (TimeoutError, socket.timeout) as exc:
        logger.exception(
            "OpenAI request timed out after %ss while generating training plan.",
            int(DEFAULT_OPENAI_TIMEOUT_SECONDS),
        )
        raise RuntimeError(
            "OpenAI request timed out while generating the training plan. "
            f"Current timeout is {int(DEFAULT_OPENAI_TIMEOUT_SECONDS)} seconds."
        ) from exc

    text = _extract_output_text(payload)
    try:
        parsed = json.loads(text)
        logger.info(
            "Parsed OpenAI structured response successfully. weeks=%s",
            len(parsed.get("weeks", [])) if isinstance(parsed, dict) else "unknown",
        )
        return parsed
    except json.JSONDecodeError as exc:
        logger.exception("OpenAI response was not valid JSON. output_text_length=%s", len(text))
        raise RuntimeError("OpenAI response was not valid JSON.") from exc


def _normalize_plan_structure(
    plan_payload: dict[str, Any],
    request_payload: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    if not isinstance(plan_payload.get("weeks"), list) or not plan_payload["weeks"]:
        raise ValueError("Plan response is missing weeks.")

    normalized = json.loads(json.dumps(plan_payload, default=str))
    issues: list[str] = []
    race_day = date.fromisoformat(request_payload["race_date"])
    blocked = {day.lower() for day in request_payload.get("blocked_days", [])}
    seen_dates: set[str] = set()
    normalized_weeks: list[dict[str, Any]] = []

    for week in normalized["weeks"]:
        workouts = week.get("workouts")
        if not isinstance(workouts, list) or not workouts:
            issues.append(f"Dropped week without workouts: {week.get('week_number')}")
            continue

        kept_workouts: list[dict[str, Any]] = []
        for workout in workouts:
            try:
                workout_day = date.fromisoformat(workout["workout_date"])
            except Exception:
                issues.append(f"Dropped workout with invalid date: {workout.get('title', 'Untitled')}")
                continue

            if workout_day > race_day:
                issues.append(
                    f"Removed post-race workout {workout.get('title', 'Untitled')} on {workout['workout_date']}"
                )
                continue
            if workout_day.strftime("%A").lower() in blocked and not workout.get("is_rest_day"):
                issues.append(
                    f"Blocked-day workout kept as requested output: {workout.get('title', 'Untitled')} on {workout['workout_date']}"
                )
            kept_workouts.append(workout)
            seen_dates.add(workout["workout_date"])

        if not kept_workouts:
            issues.append(f"Dropped empty week after normalization: {week.get('week_number')}")
            continue

        kept_workouts.sort(key=lambda item: item["workout_date"])
        week_start = kept_workouts[0]["workout_date"]
        week_end = kept_workouts[-1]["workout_date"]
        normalized_weeks.append(
            {
                **week,
                "week_start": week_start,
                "week_end": week_end,
                "workouts": kept_workouts,
            }
        )

    if not normalized_weeks:
        raise ValueError("Plan response had no usable workouts after normalization.")

    if request_payload["race_date"] not in seen_dates:
        issues.append("Inserted placeholder race-day workout because the model omitted race day.")
        race_workout = {
            "workout_date": request_payload["race_date"],
            "discipline": "triathlon" if request_payload.get("race_type") == "triathlon" else "running",
            "title": "Race Day",
            "description": f"{request_payload.get('event_name_or_distance', 'Goal event')} day.",
            "duration_minutes": 0,
            "distance_miles": None,
            "intensity": "race",
            "is_rest_day": False,
            "is_cross_training": False,
            "mobility_notes": "",
            "strength_notes": "",
            "injury_notes": "",
        }
        normalized_weeks[-1]["workouts"].append(race_workout)
        normalized_weeks[-1]["workouts"].sort(key=lambda item: item["workout_date"])
        normalized_weeks[-1]["week_end"] = request_payload["race_date"]
        seen_dates.add(request_payload["race_date"])

    for index, week in enumerate(normalized_weeks, start=1):
        week["week_number"] = index
        week["week_start"] = week["workouts"][0]["workout_date"]
        week["week_end"] = week["workouts"][-1]["workout_date"]

    normalized["weeks"] = normalized_weeks
    return normalized, issues


def _serialize_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _fetch_plan_bundle(
    conn: duckdb.DuckDBPyConnection, where_clause: str, params: list[Any]
) -> dict[str, Any] | None:
    plan_rows = conn.execute(
        f"""
        SELECT
            plan_id,
            created_at,
            archived_at,
            status,
            model_name,
            race_type,
            CAST(race_date AS DATE) AS race_date,
            goal_time,
            event_name_or_distance,
            plan_title,
            overview,
            request_payload_json,
            history_summary_json,
            prompt_payload_json,
            response_json
        FROM training_plans
        {where_clause}
        ORDER BY created_at DESC
        LIMIT 1
        """,
        params,
    ).fetchall()
    if not plan_rows:
        return None

    row = plan_rows[0]
    columns = [
        "plan_id",
        "created_at",
        "archived_at",
        "status",
        "model_name",
        "race_type",
        "race_date",
        "goal_time",
        "event_name_or_distance",
        "plan_title",
        "overview",
        "request_payload_json",
        "history_summary_json",
        "prompt_payload_json",
        "response_json",
    ]
    plan = dict(zip(columns, row))
    weeks = conn.execute(
        """
        SELECT week_number, CAST(week_start AS DATE), CAST(week_end AS DATE), focus, summary
        FROM training_plan_weeks
        WHERE plan_id = ?
        ORDER BY week_number
        """,
        [plan["plan_id"]],
    ).fetchall()
    workouts = conn.execute(
        """
        SELECT
            workout_id,
            week_number,
            CAST(workout_date AS DATE) AS workout_date,
            discipline,
            title,
            description,
            duration_minutes,
            distance_miles,
            intensity,
            is_rest_day,
            is_cross_training,
            mobility_notes,
            strength_notes,
            injury_notes
        FROM training_plan_workouts
        WHERE plan_id = ?
        ORDER BY workout_date, title
        """,
        [plan["plan_id"]],
    ).fetchall()

    week_map: dict[int, dict[str, Any]] = {}
    for week_number, week_start, week_end, focus, summary in weeks:
        week_map[week_number] = {
            "week_number": week_number,
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "focus": focus,
            "summary": summary,
            "workouts": [],
        }

    for workout in workouts:
        (
            workout_id,
            week_number,
            workout_date,
            discipline,
            title,
            description,
            duration_minutes,
            distance_miles,
            intensity,
            is_rest_day,
            is_cross_training,
            mobility_notes,
            strength_notes,
            injury_notes,
        ) = workout
        week_map.setdefault(
            week_number,
            {
                "week_number": week_number,
                "week_start": None,
                "week_end": None,
                "focus": "",
                "summary": "",
                "workouts": [],
            },
        )["workouts"].append(
            {
                "workout_id": workout_id,
                "workout_date": workout_date.isoformat(),
                "discipline": discipline,
                "title": title,
                "description": description,
                "duration_minutes": duration_minutes,
                "distance_miles": distance_miles,
                "intensity": intensity,
                "is_rest_day": is_rest_day,
                "is_cross_training": is_cross_training,
                "mobility_notes": mobility_notes,
                "strength_notes": strength_notes,
                "injury_notes": injury_notes,
            }
        )

    return {
        "plan": {
            "plan_id": plan["plan_id"],
            "created_at": plan["created_at"].isoformat() if plan["created_at"] else None,
            "archived_at": plan["archived_at"].isoformat() if plan["archived_at"] else None,
            "status": plan["status"],
            "model_name": plan["model_name"],
            "race_type": plan["race_type"],
            "race_date": plan["race_date"].isoformat() if plan["race_date"] else None,
            "goal_time": plan["goal_time"],
            "event_name_or_distance": plan["event_name_or_distance"],
            "plan_title": plan["plan_title"],
            "overview": plan["overview"],
        },
        "weeks": [week_map[key] for key in sorted(week_map)],
        "history_summary": json.loads(plan["history_summary_json"]),
        "prompt_payload": json.loads(plan["prompt_payload_json"]),
        "wizard_input": json.loads(plan["request_payload_json"]),
        "response_payload": json.loads(plan["response_json"]),
    }


def _fetch_matching_plan_bundle(
    conn: duckdb.DuckDBPyConnection,
    request_payload_json: str,
    history_summary_json: str,
) -> dict[str, Any] | None:
    return _fetch_plan_bundle(
        conn,
        "WHERE request_payload_json = ? AND history_summary_json = ?",
        [request_payload_json, history_summary_json],
    )


def generate_training_plan(request_payload: dict[str, Any]) -> dict[str, Any]:
    logger.info(
        "Generating training plan. race_type=%s race_date=%s goal_time_provided=%s event=%s",
        request_payload.get("race_type"),
        request_payload.get("race_date"),
        bool(request_payload.get("goal_time")),
        request_payload.get("event_name_or_distance"),
    )
    history_summary = get_training_history_summary()
    prompt_payload = _build_prompt_payload(request_payload, history_summary)
    request_payload_json = _serialize_json(request_payload)
    history_summary_json = _serialize_json(history_summary)

    with _WRITE_LOCK:
        conn = _connect_rw()
        try:
            ensure_training_plan_tables(conn)
            cached_plan = _fetch_matching_plan_bundle(conn, request_payload_json, history_summary_json)
            if cached_plan is not None:
                logger.info(
                    "Reusing cached training plan without a new OpenAI call. plan_id=%s",
                    cached_plan["plan"]["plan_id"],
                )
                if cached_plan["plan"]["status"] != "active":
                    now = datetime.now(timezone.utc).replace(tzinfo=None)
                    conn.execute("BEGIN TRANSACTION")
                    try:
                        conn.execute(
                            "UPDATE training_plans SET status = 'archived', archived_at = ? WHERE status = 'active'",
                            [now],
                        )
                        conn.execute(
                            "UPDATE training_plans SET status = 'active', archived_at = NULL WHERE plan_id = ?",
                            [cached_plan["plan"]["plan_id"]],
                        )
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                    cached_plan = _fetch_plan_bundle(conn, "WHERE plan_id = ?", [cached_plan["plan"]["plan_id"]])
                return cached_plan
        finally:
            conn.close()

    response_payload = _call_openai_plan(prompt_payload)
    normalized_response_payload, normalization_issues = _normalize_plan_structure(
        response_payload,
        request_payload,
    )
    artifact_path = _write_response_artifact(
        request_payload=request_payload,
        prompt_payload=prompt_payload,
        raw_response_payload=response_payload,
        normalized_response_payload=normalized_response_payload,
        issues=normalization_issues,
    )
    if normalization_issues:
        logger.warning(
            "Normalized OpenAI training plan response with %s issue(s). artifact=%s issues=%s",
            len(normalization_issues),
            artifact_path,
            " | ".join(normalization_issues),
        )

    plan_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    with _WRITE_LOCK:
        conn = _connect_rw()
        try:
            ensure_training_plan_tables(conn)
            conn.execute("BEGIN TRANSACTION")
            logger.info("Archiving previous active plan before saving new plan_id=%s.", plan_id)
            conn.execute(
                "UPDATE training_plans SET status = 'archived', archived_at = ? WHERE status = 'active'",
                [created_at],
            )
            conn.execute(
                """
                INSERT INTO training_plans (
                    plan_id,
                    created_at,
                    archived_at,
                    status,
                    model_name,
                    race_type,
                    race_date,
                    goal_time,
                    event_name_or_distance,
                    plan_title,
                    overview,
                    request_payload_json,
                    history_summary_json,
                    prompt_payload_json,
                    response_json
                ) VALUES (?, ?, NULL, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    plan_id,
                    created_at,
                    DEFAULT_OPENAI_MODEL,
                    request_payload["race_type"],
                    request_payload["race_date"],
                    request_payload.get("goal_time"),
                    request_payload["event_name_or_distance"],
                    normalized_response_payload["plan_title"],
                    normalized_response_payload["overview"],
                    request_payload_json,
                    history_summary_json,
                    _serialize_json(prompt_payload),
                    _serialize_json(normalized_response_payload),
                ],
            )
            for week in normalized_response_payload["weeks"]:
                conn.execute(
                    """
                    INSERT INTO training_plan_weeks (
                        plan_id, week_number, week_start, week_end, focus, summary
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        plan_id,
                        week["week_number"],
                        week["week_start"],
                        week["week_end"],
                        week["focus"],
                        week["summary"],
                    ],
                )
                for workout in week["workouts"]:
                    conn.execute(
                        """
                        INSERT INTO training_plan_workouts (
                            workout_id,
                            plan_id,
                            week_number,
                            workout_date,
                            discipline,
                            title,
                            description,
                            duration_minutes,
                            distance_miles,
                            intensity,
                            is_rest_day,
                            is_cross_training,
                            mobility_notes,
                            strength_notes,
                            injury_notes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            str(uuid.uuid4()),
                            plan_id,
                            week["week_number"],
                            workout["workout_date"],
                            _normalize_sport_name(workout["discipline"]),
                            workout["title"],
                            workout["description"],
                            workout["duration_minutes"],
                            workout["distance_miles"],
                            workout["intensity"],
                            workout["is_rest_day"],
                            workout["is_cross_training"],
                            workout["mobility_notes"],
                            workout["strength_notes"],
                            workout["injury_notes"],
                        ],
                    )
            conn.execute("COMMIT")
            logger.info(
                "Saved training plan successfully. plan_id=%s weeks=%s",
                plan_id,
                len(normalized_response_payload.get("weeks", [])),
            )
        except Exception:
            conn.execute("ROLLBACK")
            logger.exception("Failed to save generated training plan. plan_id=%s", plan_id)
            raise
        finally:
            conn.close()

    result = get_active_training_plan()
    if result is None:
        raise RuntimeError("Training plan was generated but could not be reloaded.")
    return result


def get_active_training_plan() -> dict[str, Any] | None:
    with _WRITE_LOCK:
        conn = _connect_rw()
        try:
            ensure_training_plan_tables(conn)
            plan = _fetch_plan_bundle(conn, "WHERE status = 'active'", [])
            logger.info("Loaded active training plan. found=%s", bool(plan))
            return plan
        finally:
            conn.close()


def list_training_plans() -> list[dict[str, Any]]:
    with _WRITE_LOCK:
        conn = _connect_rw()
        try:
            ensure_training_plan_tables(conn)
            rows = conn.execute(
                """
                SELECT
                    plan_id,
                    created_at,
                    archived_at,
                    status,
                    race_type,
                    CAST(race_date AS DATE) AS race_date,
                    goal_time,
                    event_name_or_distance,
                    plan_title
                FROM training_plans
                ORDER BY created_at DESC
                """
            ).fetchall()
        finally:
            conn.close()

    plans = []
    for row in rows:
        plans.append(
            {
                "plan_id": row[0],
                "created_at": row[1].isoformat() if row[1] else None,
                "archived_at": row[2].isoformat() if row[2] else None,
                "status": row[3],
                "race_type": row[4],
                "race_date": row[5].isoformat() if row[5] else None,
                "goal_time": row[6],
                "event_name_or_distance": row[7],
                "plan_title": row[8],
            }
        )
    return plans


def get_upcoming_plan_workouts(days: int = 7) -> dict[str, Any]:
    active_plan = get_active_training_plan()
    if active_plan is None:
        logger.info("Requested upcoming workouts, but no active plan exists.")
        return {"plan": None, "days": []}

    today = date.today()
    end_day = today + timedelta(days=max(days, 1) - 1)
    days_list: list[dict[str, Any]] = []
    for week in active_plan["weeks"]:
        for workout in week["workouts"]:
            workout_day = date.fromisoformat(workout["workout_date"])
            if today <= workout_day <= end_day:
                days_list.append(
                    {
                        **workout,
                        "day_name": workout_day.strftime("%A"),
                        "week_number": week["week_number"],
                    }
                )
    days_list.sort(key=lambda item: item["workout_date"])
    logger.info("Loaded upcoming plan workouts. days_requested=%s workouts_returned=%s", days, len(days_list))
    return {"plan": active_plan["plan"], "days": days_list}
