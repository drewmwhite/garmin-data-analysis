"""
DuckDB service layer for the Garmin data API.

Queries garmin.duckdb — built once by running:
    python db/build.py

All query methods return plain dicts/lists, ready for JSON serialisation.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[4]
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
}


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_conn() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise RuntimeError(
            f"garmin.duckdb not found at {DB_PATH}. "
            "Run `python db/build.py` to build the database first."
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


def _rows_to_dicts(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> list[dict[str, Any]]:
    """Execute a query and return results as a list of dicts."""
    rel = conn.execute(sql, params or [])
    columns = [desc[0] for desc in rel.description]
    return [dict(zip(columns, row)) for row in rel.fetchall()]


def _scalar(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> Any:
    return conn.execute(sql, params or []).fetchone()[0]


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------

def list_datasets() -> list[dict[str, Any]]:
    """Return metadata + row/column counts for all known tables."""
    conn = _get_conn()
    try:
        results = []
        for table, meta in DATASET_META.items():
            try:
                count = _scalar(conn, f"SELECT COUNT(*) FROM {table}")
                columns = _rows_to_dicts(conn, f"DESCRIBE {table}")
                col_names = [c["column_name"] for c in columns]
                results.append({
                    **meta,
                    "record_count": count,
                    "column_count": len(col_names),
                    "sample_columns": col_names[:6],
                })
            except duckdb.CatalogException:
                pass  # table not yet built
        return results
    finally:
        conn.close()


def get_dataset_records(slug: str, limit: int | None = None) -> dict[str, Any]:
    """Return records and metadata for a dataset by its slug."""
    # Map slug → table name
    table = next((t for t, m in DATASET_META.items() if m["slug"] == slug), None)
    if table is None:
        return {}

    conn = _get_conn()
    try:
        count = _scalar(conn, f"SELECT COUNT(*) FROM {table}")
        columns = _rows_to_dicts(conn, f"DESCRIBE {table}")
        col_names = [c["column_name"] for c in columns]

        sql = f"SELECT * FROM {table}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        records = _rows_to_dicts(conn, sql)

        return {
            "slug": slug,
            "record_count": count,
            "column_count": len(col_names),
            "sample_columns": col_names[:6],
            "returned_records": len(records),
            "records": records,
        }
    finally:
        conn.close()


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

    conn = _get_conn()
    try:
        total = _scalar(conn, f"SELECT COUNT(*) FROM activity_sessions {where}", params)
        sessions = _rows_to_dicts(
            conn,
            f"""
            SELECT * FROM activity_sessions
            {where}
            ORDER BY {sort_by} {sort_dir} NULLS LAST
            LIMIT {int(limit)} OFFSET {int(offset)}
            """,
            params,
        )
        return {"total": total, "returned": len(sessions), "sessions": sessions}
    finally:
        conn.close()


def get_activity_session(activity_id: str) -> dict[str, Any] | None:
    """Return a single activity session by activity_id."""
    conn = _get_conn()
    try:
        rows = _rows_to_dicts(
            conn,
            "SELECT * FROM activity_sessions WHERE activity_id = ? LIMIT 1",
            [activity_id],
        )
        return rows[0] if rows else None
    finally:
        conn.close()


def get_activity_records(activity_id: str) -> list[dict[str, Any]]:
    """Return time-series records for a single activity, ordered by timestamp."""
    conn = _get_conn()
    try:
        return _rows_to_dicts(
            conn,
            "SELECT * FROM activity_records WHERE activity_id = ? ORDER BY timestamp",
            [activity_id],
        )
    finally:
        conn.close()
