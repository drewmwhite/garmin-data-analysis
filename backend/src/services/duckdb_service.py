"""
DuckDB service layer for the Garmin data API.

Queries garmin.duckdb — built once by running:
    python db/build.py

All query methods return plain dicts/lists, ready for JSON serialisation.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[3]
DB_PATH = REPO_ROOT / "garmin.duckdb"

# Table metadata used by the /datasets endpoint
DATASET_META: dict[str, dict[str, str]] = {
    "sleep_records": {
        "slug": "sleep",
        "title": "Sleep",
        "description": "Nightly sleep records with flattened wellness metrics.",
    },
    "hydration_records": {
        "slug": "hydration",
        "title": "Hydration",
        "description": "Hydration log entries for daily intake analysis.",
    },
    "vo2_max_records": {
        "slug": "vo2max",
        "title": "VO2 Max",
        "description": "Training VO2 max records derived from activity history.",
    },
    "daily_summaries": {
        "slug": "daily-summaries",
        "title": "Daily Summaries",
        "description": "Daily wellness summary: steps, calories, HR, intensity minutes.",
    },
    "daily_stress_aggregators": {
        "slug": "daily-stress",
        "title": "Daily Stress",
        "description": "All-day stress aggregates (TOTAL / AWAKE / ASLEEP) per calendar date.",
    },
    "activity_sessions": {
        "slug": "activity-sessions",
        "title": "Activity Sessions",
        "description": "Per-activity session summaries from .fit files.",
    },
    "strava_activities": {
        "slug": "strava-activities",
        "title": "Strava Activities",
        "description": "Activity summaries fetched from the Strava API.",
    },
    "strava_laps": {
        "slug": "strava-laps",
        "title": "Strava Laps",
        "description": "Lap-level detail for Strava run activities.",
    },
}


# ---------------------------------------------------------------------------
# Connection — one read-only connection per thread
#
# DuckDB connections are not thread-safe.  FastAPI runs sync endpoints in a
# thread pool, so sharing one connection across threads causes crashes.
# threading.local() gives each worker thread its own connection.  Multiple
# read-only connections to the same file are supported by DuckDB.
#
# When db/build.py atomically replaces garmin.duckdb the file mtime changes.
# Each thread detects this on its next request and reconnects to the new file.
# ---------------------------------------------------------------------------

_local = threading.local()   # per-thread state: .conn, .mtime
_mtime_lock = threading.Lock()
_latest_mtime: float = 0.0   # last known mtime, updated by any thread


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Return this thread's read-only DuckDB connection.

    Opens a new connection on first use and reconnects automatically when the
    database file is replaced by a fresh build.
    """
    global _latest_mtime

    if not DB_PATH.exists():
        raise RuntimeError(
            f"garmin.duckdb not found at {DB_PATH}. "
            "Run `python db/build.py` to build the database first."
        )

    file_mtime = DB_PATH.stat().st_mtime

    # Broadcast a file change so all threads know to reconnect.
    with _mtime_lock:
        if file_mtime != _latest_mtime:
            _latest_mtime = file_mtime

    thread_conn: duckdb.DuckDBPyConnection | None = getattr(_local, "conn", None)
    thread_mtime: float = getattr(_local, "mtime", 0.0)

    if thread_conn is not None and thread_mtime == file_mtime:
        return thread_conn

    # First connect or file was replaced — open a fresh connection for this thread.
    if thread_conn is not None:
        try:
            thread_conn.close()
        except Exception:
            pass

    _local.conn = duckdb.connect(str(DB_PATH), read_only=True)
    _local.mtime = file_mtime
    return _local.conn


def _rows_to_dicts(sql: str, params: list | None = None) -> list[dict[str, Any]]:
    """Execute a query and return results as a list of dicts."""
    conn = _get_conn()
    rel = conn.execute(sql, params or [])
    columns = [desc[0] for desc in rel.description]
    return [dict(zip(columns, row)) for row in rel.fetchall()]


def _scalar(sql: str, params: list | None = None) -> Any:
    conn = _get_conn()
    row = conn.execute(sql, params or []).fetchone()
    return row[0] if row is not None else None


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------

def list_datasets() -> list[dict[str, Any]]:
    """Return metadata + row/column counts for all known tables."""
    results = []
    conn = _get_conn()
    for table, meta in DATASET_META.items():
        try:
            count = _scalar(f"SELECT COUNT(*) FROM {table}")
            if count is None:
                continue
            columns = _rows_to_dicts(f"DESCRIBE {table}")
            col_names = [c["column_name"] for c in columns]
            results.append({
                **meta,
                "record_count": count,
                "column_count": len(col_names),
                "sample_columns": col_names[:6],
            })
        except Exception:
            pass  # table not yet built or view references missing table
    return results


def get_dataset_records(slug: str, limit: int | None = None) -> dict[str, Any]:
    """Return records and metadata for a dataset by its slug."""
    table = next((t for t, m in DATASET_META.items() if m["slug"] == slug), None)
    if table is None:
        return {}

    count = _scalar(f"SELECT COUNT(*) FROM {table}")
    columns = _rows_to_dicts(f"DESCRIBE {table}")
    col_names = [c["column_name"] for c in columns]

    sql = f"SELECT * FROM {table}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    records = _rows_to_dicts(sql)

    return {
        "slug": slug,
        "record_count": count,
        "column_count": len(col_names),
        "sample_columns": col_names[:6],
        "returned_records": len(records),
        "records": records,
    }


# ---------------------------------------------------------------------------
# Sleep
# ---------------------------------------------------------------------------

def get_sleep(
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 90,
    offset: int = 0,
) -> dict[str, Any]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if date_from:
        where_clauses.append("CAST(calendarDate AS DATE) >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where_clauses.append("CAST(calendarDate AS DATE) <= CAST(? AS DATE)")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total = _scalar(f"SELECT COUNT(*) FROM sleep_records {where}", params)
    records = _rows_to_dicts(
        f"SELECT * FROM sleep_records {where} ORDER BY calendarDate DESC LIMIT {int(limit)} OFFSET {int(offset)}",
        params,
    )
    return {"total": total, "returned": len(records), "records": records}


# ---------------------------------------------------------------------------
# Hydration
# ---------------------------------------------------------------------------

def get_hydration(
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 90,
    offset: int = 0,
) -> dict[str, Any]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if date_from:
        where_clauses.append("CAST(calendarDate AS DATE) >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where_clauses.append("CAST(calendarDate AS DATE) <= CAST(? AS DATE)")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total = _scalar(f"SELECT COUNT(*) FROM hydration_records {where}", params)
    records = _rows_to_dicts(
        f"SELECT * FROM hydration_records {where} ORDER BY calendarDate DESC LIMIT {int(limit)} OFFSET {int(offset)}",
        params,
    )
    return {"total": total, "returned": len(records), "records": records}


# ---------------------------------------------------------------------------
# VO2 Max
# ---------------------------------------------------------------------------

def get_vo2_max(
    sport: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if sport:
        where_clauses.append("LOWER(sport) = LOWER(?)")
        params.append(sport)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total = _scalar(f"SELECT COUNT(*) FROM vo2_max_records {where}", params)
    records = _rows_to_dicts(
        f"SELECT * FROM vo2_max_records {where} ORDER BY calendarDate DESC LIMIT {int(limit)} OFFSET {int(offset)}",
        params,
    )
    return {"total": total, "returned": len(records), "records": records}


# ---------------------------------------------------------------------------
# Daily Summaries
# ---------------------------------------------------------------------------

def get_daily_summaries(
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 90,
    offset: int = 0,
) -> dict[str, Any]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if date_from:
        where_clauses.append("CAST(calendarDate AS DATE) >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where_clauses.append("CAST(calendarDate AS DATE) <= CAST(? AS DATE)")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total = _scalar(f"SELECT COUNT(*) FROM daily_summaries {where}", params)
    records = _rows_to_dicts(
        f"SELECT * FROM daily_summaries {where} ORDER BY calendarDate DESC LIMIT {int(limit)} OFFSET {int(offset)}",
        params,
    )
    return {"total": total, "returned": len(records), "records": records}


# ---------------------------------------------------------------------------
# Stress
# ---------------------------------------------------------------------------

def get_stress(
    date_from: str | None = None,
    date_to: str | None = None,
    stress_type: str | None = None,
    limit: int = 90,
    offset: int = 0,
) -> dict[str, Any]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if date_from:
        where_clauses.append("CAST(calendar_date AS DATE) >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where_clauses.append("CAST(calendar_date AS DATE) <= CAST(? AS DATE)")
        params.append(date_to)
    if stress_type:
        where_clauses.append("UPPER(stress_type) = UPPER(?)")
        params.append(stress_type)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total = _scalar(f"SELECT COUNT(*) FROM daily_stress_aggregators {where}", params)
    records = _rows_to_dicts(
        f"SELECT * FROM daily_stress_aggregators {where} ORDER BY calendar_date DESC LIMIT {int(limit)} OFFSET {int(offset)}",
        params,
    )
    return {"total": total, "returned": len(records), "records": records}


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def get_sleep_weekly_stats(weeks: int = 52) -> list[dict[str, Any]]:
    """Weekly average sleep duration (hours) and overall sleep score."""
    return _rows_to_dicts(
        f"""
        SELECT
            DATE_TRUNC('week', CAST(calendarDate AS DATE))  AS week_start,
            ROUND(AVG(deepSleepSeconds + lightSleepSeconds + remSleepSeconds) / 3600.0, 2) AS avg_sleep_hours,
            ROUND(AVG(sleepScores_overallScore), 1)         AS avg_sleep_score,
            COUNT(*)                                        AS nights
        FROM sleep_records
        WHERE calendarDate IS NOT NULL
        GROUP BY week_start
        ORDER BY week_start DESC
        LIMIT {int(weeks)}
        """
    )


def get_daily_steps(days: int = 365) -> list[dict[str, Any]]:
    """Daily step counts with a 7-day rolling average."""
    return _rows_to_dicts(
        f"""
        WITH base AS (
            SELECT
                CAST(calendarDate AS DATE) AS date,
                totalSteps                 AS steps
            FROM daily_summaries
            WHERE totalSteps IS NOT NULL
            ORDER BY date DESC
            LIMIT {int(days)}
        )
        SELECT
            date,
            steps,
            ROUND(AVG(steps) OVER (
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 0) AS rolling_7d_avg
        FROM base
        ORDER BY date ASC
        """
    )


def get_heart_rate_trends(days: int = 365) -> list[dict[str, Any]]:
    """Daily resting heart rate with a 7-day rolling average."""
    return _rows_to_dicts(
        f"""
        WITH base AS (
            SELECT
                CAST(calendarDate AS DATE) AS date,
                restingHeartRate           AS resting_hr
            FROM daily_summaries
            WHERE restingHeartRate IS NOT NULL AND restingHeartRate > 0
            ORDER BY date DESC
            LIMIT {int(days)}
        )
        SELECT
            date,
            resting_hr,
            ROUND(AVG(resting_hr) OVER (
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 1) AS rolling_7d_avg
        FROM base
        ORDER BY date ASC
        """
    )


def get_vo2_max_trends() -> list[dict[str, Any]]:
    """VO2 max value over time, per sport."""
    return _rows_to_dicts(
        """
        SELECT
            CAST(calendarDate AS DATE) AS date,
            sport,
            ROUND(vo2MaxValue, 1)      AS vo2_max
        FROM vo2_max_records
        WHERE vo2MaxValue IS NOT NULL
        ORDER BY calendarDate ASC
        """
    )


def get_activity_sport_summary() -> list[dict[str, Any]]:
    """Aggregated stats per sport across all recorded sessions."""
    return _rows_to_dicts(
        """
        SELECT
            LOWER(sport)                                AS sport,
            COUNT(*)                                    AS total_activities,
            ROUND(SUM(total_distance) / 1000.0, 1)     AS total_distance_km,
            ROUND(AVG(total_distance) / 1000.0, 2)     AS avg_distance_km,
            ROUND(AVG(avg_heart_rate), 0)               AS avg_heart_rate,
            ROUND(SUM(total_calories), 0)               AS total_calories,
            ROUND(AVG(total_elapsed_time) / 60.0, 1)   AS avg_duration_min
        FROM activity_sessions
        WHERE sport IS NOT NULL
        GROUP BY sport
        ORDER BY total_activities DESC
        """
    )


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

_ALLOWED_SORT_COLUMNS = {
    "start_time", "total_distance", "total_calories",
    "avg_heart_rate", "avg_speed", "total_ascent",
}


def get_activity_sessions(
    sport: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    sort_by: str = "start_time",
    sort_dir: str = "desc",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Return filtered, sorted, and paginated activity sessions."""
    if sort_by not in _ALLOWED_SORT_COLUMNS:
        sort_by = "start_time"
    sort_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    where_clauses: list[str] = []
    params: list[Any] = []

    if sport:
        where_clauses.append("LOWER(sport) = LOWER(?)")
        params.append(sport)
    if date_from:
        where_clauses.append("CAST(start_time AS DATE) >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where_clauses.append("CAST(start_time AS DATE) <= CAST(? AS DATE)")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total = _scalar(f"SELECT COUNT(*) FROM activity_sessions {where}", params)
    sessions = _rows_to_dicts(
        f"""
        SELECT * FROM activity_sessions
        {where}
        ORDER BY {sort_by} {sort_dir} NULLS LAST
        LIMIT {int(limit)} OFFSET {int(offset)}
        """,
        params,
    )
    return {"total": total, "returned": len(sessions), "sessions": sessions}


def get_activity_session(activity_id: str) -> dict[str, Any] | None:
    """Return a single activity session by activity_id."""
    rows = _rows_to_dicts(
        "SELECT * FROM activity_sessions WHERE activity_id = ? LIMIT 1",
        [activity_id],
    )
    return rows[0] if rows else None


def get_activity_records(activity_id: str) -> list[dict[str, Any]]:
    """Return time-series records for a single activity, ordered by timestamp."""
    return _rows_to_dicts(
        "SELECT * FROM activity_records WHERE activity_id = ? ORDER BY timestamp",
        [activity_id],
    )


# ---------------------------------------------------------------------------
# Strava
# ---------------------------------------------------------------------------

def get_activity_calendar(year: int, sport: str | None = None) -> list[dict[str, Any]]:
    """Return one row per active day in the given year across all sources."""
    where = "WHERE start_time IS NOT NULL AND year(CAST(start_time AS DATE)) = ?"
    params: list[Any] = [int(year)]
    if sport:
        where += " AND LOWER(sport) = LOWER(?)"
        params.append(sport)
    return _rows_to_dicts(
        f"""
        SELECT
            CAST(start_time AS DATE)                AS date,
            COUNT(*)                                AS activity_count,
            STRING_AGG(DISTINCT LOWER(sport), ', ') AS sports
        FROM unified_activities
        {where}
        GROUP BY CAST(start_time AS DATE)
        ORDER BY date
        """,
        params,
    )


def get_activity_calendar_years() -> list[int]:
    """Return distinct years that have activity data, newest first."""
    rows = _rows_to_dicts(
        """
        SELECT DISTINCT year(CAST(start_time AS DATE)) AS year
        FROM unified_activities
        WHERE start_time IS NOT NULL
        ORDER BY year DESC
        """
    )
    return [r["year"] for r in rows]


def get_activity_calendar_sports() -> list[str]:
    """Return distinct sport types across all activity data, sorted."""
    rows = _rows_to_dicts(
        """
        SELECT DISTINCT LOWER(sport) AS sport
        FROM unified_activities
        WHERE sport IS NOT NULL
        ORDER BY sport
        """
    )
    return [r["sport"] for r in rows]


def get_activities_for_date(date: str) -> list[dict[str, Any]]:
    """Return all activities on a given date (YYYY-MM-DD)."""
    return _rows_to_dicts(
        """
        SELECT
            activity_id, data_source, name, sport,
            start_time, total_distance_m, moving_time_s, avg_heart_rate
        FROM unified_activities
        WHERE CAST(start_time AS DATE) = CAST(? AS DATE)
        ORDER BY start_time
        """,
        [date],
    )


def get_strava_months() -> list[dict[str, Any]]:
    """Return one row per calendar month that has Strava activities."""
    return _rows_to_dicts(
        """
        SELECT
            year(CAST(start_date_local AS TIMESTAMP))  AS year,
            month(CAST(start_date_local AS TIMESTAMP)) AS month,
            COUNT(*)                                   AS activity_count,
            ROUND(SUM(distance) / 1609.344, 1)         AS total_distance_mi,
            SUM(moving_time)                           AS total_moving_time_s
        FROM strava_activities
        GROUP BY year, month
        ORDER BY year DESC, month DESC
        """
    )


def get_strava_activities(
    sport: str | None = None,
    year: int | None = None,
    month: int | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if sport:
        where_clauses.append("LOWER(type) = LOWER(?)")
        params.append(sport)
    if year:
        where_clauses.append("year(CAST(start_date_local AS TIMESTAMP)) = ?")
        params.append(int(year))
    if month:
        where_clauses.append("month(CAST(start_date_local AS TIMESTAMP)) = ?")
        params.append(int(month))
    if date_from:
        where_clauses.append("CAST(start_date_local AS DATE) >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where_clauses.append("CAST(start_date_local AS DATE) <= CAST(? AS DATE)")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total = _scalar(f"SELECT COUNT(*) FROM strava_activities {where}", params)
    activities = _rows_to_dicts(
        f"SELECT * FROM strava_activities {where} ORDER BY start_date_local DESC"
        f" LIMIT {int(limit)} OFFSET {int(offset)}",
        params,
    )
    return {"total": total, "returned": len(activities), "activities": activities}


def get_strava_laps(activity_id: int | str) -> list[dict[str, Any]]:
    return _rows_to_dicts(
        "SELECT * FROM strava_laps WHERE workout_id = ? ORDER BY lap_index",
        [int(activity_id)],
    )


# ---------------------------------------------------------------------------
# Unified activities (Garmin + Strava)
# ---------------------------------------------------------------------------

_UNIFIED_SORT_COLUMNS = {
    "start_time", "total_distance_m", "moving_time_s",
    "avg_heart_rate", "avg_speed_ms", "total_elevation_gain_m",
}


def get_unified_activities(
    sport: str | None = None,
    data_source: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    sort_by: str = "start_time",
    sort_dir: str = "desc",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    if sort_by not in _UNIFIED_SORT_COLUMNS:
        sort_by = "start_time"
    sort_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    where_clauses: list[str] = []
    params: list[Any] = []

    if sport:
        where_clauses.append("LOWER(sport) = LOWER(?)")
        params.append(sport)
    if data_source:
        where_clauses.append("data_source = ?")
        params.append(data_source.lower())
    if date_from:
        where_clauses.append("CAST(start_time AS DATE) >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where_clauses.append("CAST(start_time AS DATE) <= CAST(? AS DATE)")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    total = _scalar(f"SELECT COUNT(*) FROM unified_activities {where}", params)
    activities = _rows_to_dicts(
        f"""
        SELECT * FROM unified_activities
        {where}
        ORDER BY {sort_by} {sort_dir} NULLS LAST
        LIMIT {int(limit)} OFFSET {int(offset)}
        """,
        params,
    )
    return {"total": total, "returned": len(activities), "activities": activities}
