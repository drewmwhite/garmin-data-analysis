"""
Build the local DuckDB database from raw Garmin data files, and optionally
upload each table to S3 as Hive-partitioned Parquet.

Run this once (or whenever your raw data changes) to populate garmin.duckdb.
After building, the FastAPI backend queries the .duckdb file directly —
no re-parsing of JSON or FIT files on every request.

Usage:
    python db/build.py                                      # build all tables locally
    python db/build.py --table sleep                        # rebuild one table
    python db/build.py --table activity-records [--limit N] # test with N FIT files
    python db/build.py --bucket my-bucket                   # build + upload to S3
    python db/build.py --bucket my-bucket --prefix garmin   # custom S3 prefix

S3 upload is skipped unless --bucket is provided (or S3_BUCKET env var is set).
AWS credentials are read from the standard environment variables:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION

S3 partition layout:
    sleep/                year=2024/part-0.parquet
    hydration_records/    year=2024/part-0.parquet
    vo2_max_records/      year=2024/part-0.parquet
    daily_summaries/      year=2024/part-0.parquet
    daily_stress_aggregators/ year=2024/part-0.parquet
    activity_sessions/    sport=running/year=2024/part-0.parquet
    activity_records/     year=2024/month=1/part-0.parquet

The database file is written to the repo root as garmin.duckdb.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

# Load .env from the repo root so credentials are available without
# manually exporting them in the shell first.
load_dotenv(REPO_ROOT / ".env")
DB_PATH = REPO_ROOT / "garmin.duckdb"
STAGING_DB_PATH = REPO_ROOT / "garmin_staging.duckdb"

sys.path.insert(0, str(REPO_ROOT / "backend" / "src"))

from extraction.extractor import (  # noqa: E402
    GarminDataExtractor,
    DEFAULT_SLEEP_DATA_DIR,
    DEFAULT_HYDRATION_DATA_DIR,
    DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR,
    DEFAULT_DAILY_SUMMARY_DATA_DIR,
)
from extraction.fit_extractor import (  # noqa: E402
    GarminFitExtractor,
    DEFAULT_ACTIVITY_FIT_DATA_DIR,
)
from extraction.strava_extractor import StravaExtractor  # noqa: E402
from services.training_plan_service import ensure_training_plan_tables  # noqa: E402


# ---------------------------------------------------------------------------
# Connection + S3
# ---------------------------------------------------------------------------

def open_db() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(STAGING_DB_PATH))


def configure_s3(conn: duckdb.DuckDBPyConnection) -> None:
    """Install/load httpfs and configure AWS credentials from the environment."""
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    conn.execute(f"SET s3_region='{region}';")
    if key:
        conn.execute(f"SET s3_access_key_id='{key}';")
    if secret:
        conn.execute(f"SET s3_secret_access_key='{secret}';")


def s3_upload(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    s3_base: str,
    select: str,
    partition_by: list[str],
) -> None:
    """Export a table to S3 as Hive-partitioned Parquet via DuckDB's COPY."""
    s3_path = f"{s3_base}/{table}/"
    cols = ", ".join(partition_by)
    conn.execute(f"""
        COPY ({select})
        TO '{s3_path}'
        (FORMAT PARQUET, PARTITION_BY ({cols}), OVERWRITE_OR_IGNORE TRUE)
    """)
    print(f"    uploaded to {s3_path} (partitioned by {cols})")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _drop_and_create(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    """Replace a table with the contents of a DataFrame."""
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count:,} rows written")


def _append_df(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    """Append rows to a table, creating it first if needed."""
    if df.empty:
        return 0

    existing_tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    if table not in existing_tables:
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        return len(df)

    table_cols = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
    for col in table_cols:
        if col not in df.columns:
            df[col] = None
    df = df[table_cols]
    conn.execute(f"INSERT INTO {table} SELECT * FROM df")
    return len(df)


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that contain unhashable nested objects (lists/dicts).
    DuckDB can store these as JSON strings if needed, but for clean tabular
    queries we strip them here. Complex nested data is handled in dedicated tables.
    """
    drop_cols = [
        col for col in df.columns
        if df[col].apply(lambda v: isinstance(v, (list, dict))).any()
    ]
    if drop_cols:
        print(f"    Dropping nested columns: {drop_cols}")
        df = df.drop(columns=drop_cols)
    return df


def _existing_tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }


def _iso_to_unix_timestamp(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------

def build_sleep(
    conn: duckdb.DuckDBPyConnection, limit: int | None = None, s3_base: str | None = None
) -> None:
    print("Building sleep_records...")
    df = GarminDataExtractor(data_dir=DEFAULT_SLEEP_DATA_DIR).load_sleep_dataframe()
    df = _clean_df(df)
    if limit:
        df = df.head(limit)
    _drop_and_create(conn, "sleep_records", df)
    if s3_base:
        s3_upload(
            conn, "sleep_records", s3_base,
            "SELECT *, year(calendarDate::DATE) AS year FROM sleep_records",
            ["year"],
        )


def build_hydration(
    conn: duckdb.DuckDBPyConnection, limit: int | None = None, s3_base: str | None = None
) -> None:
    print("Building hydration_records...")
    df = GarminDataExtractor(data_dir=DEFAULT_HYDRATION_DATA_DIR).load_hydration_dataframe()
    # uuid is nested {'uuid': '...'} → flattened to uuid_uuid by extractor
    if "uuid_uuid" in df.columns:
        df = df.rename(columns={"uuid_uuid": "uuid"})
    df = _clean_df(df)
    if limit:
        df = df.head(limit)
    _drop_and_create(conn, "hydration_records", df)
    if s3_base:
        s3_upload(
            conn, "hydration_records", s3_base,
            "SELECT *, year(calendarDate::DATE) AS year FROM hydration_records",
            ["year"],
        )


def build_vo2_max(
    conn: duckdb.DuckDBPyConnection, limit: int | None = None, s3_base: str | None = None
) -> None:
    print("Building vo2_max_records...")
    df = GarminDataExtractor(data_dir=DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR).load_activity_vo2_max_dataframe()
    if "activityUuid_uuid" in df.columns:
        df = df.rename(columns={"activityUuid_uuid": "activity_uuid"})
    df = _clean_df(df)
    if limit:
        df = df.head(limit)
    _drop_and_create(conn, "vo2_max_records", df)
    if s3_base:
        s3_upload(
            conn, "vo2_max_records", s3_base,
            "SELECT *, year(calendarDate::DATE) AS year FROM vo2_max_records",
            ["year"],
        )


def build_daily_summaries(
    conn: duckdb.DuckDBPyConnection, limit: int | None = None, s3_base: str | None = None
) -> None:
    print("Building daily_summaries...")
    extractor = GarminDataExtractor(data_dir=DEFAULT_DAILY_SUMMARY_DATA_DIR)
    raw_records = extractor.extract_daily_summary_records()
    if limit:
        raw_records = raw_records[:limit]

    # --- Stress aggregators (separate table) ---
    stress_rows: list[dict] = []
    for record in raw_records:
        date = record.get("calendarDate")
        aggregator_list = record.get("allDayStress_aggregatorList", [])
        if not isinstance(aggregator_list, list):
            continue
        for agg in aggregator_list:
            if not isinstance(agg, dict) or not agg.get("type"):
                continue
            stress_rows.append({
                "calendar_date": date,
                "stress_type": agg.get("type"),
                "average_stress_level": agg.get("averageStressLevel"),
                "average_stress_level_intensity": agg.get("averageStressLevelIntensity"),
                "max_stress_level": agg.get("maxStressLevel"),
                "stress_intensity_count": agg.get("stressIntensityCount"),
                "stress_duration": agg.get("stressDuration"),
                "rest_duration": agg.get("restDuration"),
                "low_duration": agg.get("lowDuration"),
                "medium_duration": agg.get("mediumDuration"),
                "high_duration": agg.get("highDuration"),
                "activity_duration": agg.get("activityDuration"),
                "total_duration": agg.get("totalDuration"),
                "total_stress_count": agg.get("totalStressCount"),
                "total_stress_intensity": agg.get("totalStressIntensity"),
                "stress_off_wrist_count": agg.get("stressOffWristCount"),
                "stress_too_active_count": agg.get("stressTooActiveCount"),
                "uncategorized_duration": agg.get("uncategorizedDuration"),
            })

    if stress_rows:
        stress_df = pd.DataFrame(stress_rows)
        _drop_and_create(conn, "daily_stress_aggregators", stress_df)
        if s3_base:
            s3_upload(
                conn, "daily_stress_aggregators", s3_base,
                "SELECT *, year(calendar_date::DATE) AS year FROM daily_stress_aggregators",
                ["year"],
            )

    # --- Main daily summaries table ---
    df = extractor.load_daily_summary_dataframe()
    df = _clean_df(df)  # drops allDayStress_aggregatorList and any other nested lists
    if limit:
        df = df.head(limit)
    _drop_and_create(conn, "daily_summaries", df)
    if s3_base:
        s3_upload(
            conn, "daily_summaries", s3_base,
            "SELECT *, year(calendarDate::DATE) AS year FROM daily_summaries",
            ["year"],
        )


def build_activity_sessions(
    conn: duckdb.DuckDBPyConnection, limit: int | None = None, s3_base: str | None = None
) -> None:
    print("Building activity_sessions (parsing all FIT files — may take a few minutes)...")
    df = GarminFitExtractor(data_dir=DEFAULT_ACTIVITY_FIT_DATA_DIR).load_activity_session_dataframe()
    df = _clean_df(df)
    if limit:
        df = df.head(limit)
    _drop_and_create(conn, "activity_sessions", df)
    if s3_base:
        s3_upload(
            conn, "activity_sessions", s3_base,
            "SELECT *, lower(coalesce(sport, 'unknown')) AS sport_part, year(start_time) AS year"
            " FROM activity_sessions",
            ["sport_part", "year"],
        )


def build_activity_records(
    conn: duckdb.DuckDBPyConnection, limit: int | None = None, s3_base: str | None = None
) -> None:
    """Build the activity_records table by processing FIT files in batches.

    Processes BATCH_SIZE files at a time and appends to the table to avoid
    loading tens of millions of rows into memory at once.
    """
    BATCH_SIZE = 100
    print("Building activity_records (time-series — this will take a while)...")

    extractor = GarminFitExtractor(data_dir=DEFAULT_ACTIVITY_FIT_DATA_DIR)
    fit_files = extractor._list_fit_files()
    if limit:
        fit_files = fit_files[:limit]

    total_files = len(fit_files)
    total_rows = 0
    table_created = False
    table_cols: list[str] = []

    conn.execute("DROP TABLE IF EXISTS activity_records")

    for batch_start in range(0, total_files, BATCH_SIZE):
        batch = fit_files[batch_start : batch_start + BATCH_SIZE]
        rows: list[dict] = []

        for path in batch:
            try:
                fit_bytes = extractor._read_fit_bytes(path)
                rows.extend(extractor._parse_records_from_fit(fit_bytes, path))
            except Exception as exc:
                print(f"\n  WARNING: skipping {path}: {exc}")

        if not rows:
            continue

        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df = _clean_df(df)

        if not table_created:
            conn.execute("CREATE TABLE activity_records AS SELECT * FROM df")
            table_cols = [row[0] for row in conn.execute("DESCRIBE activity_records").fetchall()]
            table_created = True
        else:
            # Align df to the table schema: add missing cols as NaN, drop extras
            for col in table_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[table_cols]
            conn.execute("INSERT INTO activity_records SELECT * FROM df")

        total_rows += len(df)
        done = min(batch_start + BATCH_SIZE, total_files)
        print(f"  {done}/{total_files} files, {total_rows:,} rows", end="\r", flush=True)

    print(f"\n  activity_records: {total_rows:,} rows written")
    if s3_base:
        print("  Uploading activity_records to S3...")
        s3_upload(
            conn, "activity_records", s3_base,
            "SELECT *, year(timestamp) AS year, month(timestamp) AS month FROM activity_records",
            ["year", "month"],
        )


def build_strava(
    conn: duckdb.DuckDBPyConnection, limit: int | None = None, s3_base: str | None = None
) -> None:
    """Fetch activities and laps from the Strava API and store them in DuckDB.

    Requires STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, and STRAVA_REFRESH_TOKEN
    environment variables. Skipped silently if credentials are missing.

    Laps are inserted in batches as they are fetched so that progress is
    preserved even if the run is interrupted or the daily rate limit is hit.
    Re-running only requests activities newer than the latest row already in
    strava_activities, and only fetches laps for run activities missing from
    strava_laps.
    """
    missing = [
        v for v in ("STRAVA_CLIENT_ID", "STRAVA_CLIENT_SECRET", "STRAVA_REFRESH_TOKEN")
        if not os.environ.get(v)
    ]
    if missing:
        print(f"  Skipping Strava — missing env vars: {', '.join(missing)}")
        return

    print("Building strava_activities + strava_laps (fetching from Strava API)...")
    extractor = StravaExtractor()

    existing_tables = _existing_tables(conn)

    # --- Activities ---
    existing_activity_ids: set[int] = set()
    after: int | None = None
    if "strava_activities" in existing_tables:
        existing_activity_ids = {
            int(row[0])
            for row in conn.execute("SELECT id FROM strava_activities").fetchall()
            if row[0] is not None
        }
        latest_start = conn.execute(
            "SELECT MAX(CAST(start_date_local AS TIMESTAMP))::VARCHAR FROM strava_activities"
        ).fetchone()[0]
        if latest_start:
            after = _iso_to_unix_timestamp(latest_start)

    activities = extractor.fetch_activities(after=after)
    new_activities = [
        activity for activity in activities if int(activity["id"]) not in existing_activity_ids
    ]
    if limit:
        new_activities = new_activities[:limit]

    if new_activities:
        activities_df = _clean_df(pd.DataFrame(new_activities))
        written = _append_df(conn, "strava_activities", activities_df)
        print(f"  strava_activities: {written:,} new rows written")
    else:
        print("  strava_activities already up to date.")

    if s3_base and "strava_activities" in _existing_tables(conn):
        s3_upload(
            conn, "strava_activities", s3_base,
            "SELECT *, year(CAST(start_date_local AS TIMESTAMP)) AS year FROM strava_activities",
            ["year"],
        )

    if "strava_activities" not in _existing_tables(conn):
        print("  No Strava activities available.")
        return

    # --- Laps (incremental, batch insert) ---
    run_ids = [
        int(row[0])
        for row in conn.execute(
            "SELECT id FROM strava_activities WHERE LOWER(COALESCE(type, '')) = 'run' ORDER BY start_date_local DESC"
        ).fetchall()
        if row[0] is not None
    ]

    if "strava_laps" in existing_tables:
        done_ids = {
            row[0]
            for row in conn.execute("SELECT DISTINCT workout_id FROM strava_laps").fetchall()
        }
        remaining = [rid for rid in run_ids if rid not in done_ids]
        if done_ids:
            print(f"  Resuming: {len(done_ids)} activities already have laps, {len(remaining)} remaining.")
    else:
        remaining = run_ids

    if not remaining:
        print("  strava_laps already up to date.")
        return

    print(f"  Fetching laps for {len(remaining)} run activities (batch insert as we go)...")
    total_laps = 0

    for batch_laps, batch_ids in extractor.iter_laps_batched(remaining):
        if not batch_laps:
            continue
        laps_df = _clean_df(pd.DataFrame(batch_laps))
        total_laps += _append_df(conn, "strava_laps", laps_df)
        print(f"  Saved {len(laps_df)} laps ({total_laps} total so far)...")

    print(f"  strava_laps: {total_laps} rows written")
    if s3_base and total_laps:
        s3_upload(conn, "strava_laps", s3_base, "SELECT * FROM strava_laps", [])


def build_unified_activities_view(conn: duckdb.DuckDBPyConnection) -> None:
    """Create a unified_activities view combining Garmin FIT sessions and Strava activities.

    Both sources are normalized to a common set of columns with a data_source
    discriminator ('garmin' or 'strava').
    """
    print("Building unified_activities view...")

    # Check which source tables exist
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }

    parts: list[str] = []

    if "activity_sessions" in existing:
        parts.append("""
        SELECT
            CAST(activity_id AS VARCHAR)    AS activity_id,
            'garmin'                        AS data_source,
            CAST(NULL AS VARCHAR)           AS name,
            LOWER(sport)                    AS sport,
            start_time,
            total_distance                  AS total_distance_m,
            total_timer_time                AS moving_time_s,
            total_elapsed_time              AS elapsed_time_s,
            total_ascent                    AS total_elevation_gain_m,
            avg_heart_rate,
            max_heart_rate,
            avg_speed                       AS avg_speed_ms,
            max_speed                       AS max_speed_ms,
            total_calories,
            avg_cadence
        FROM activity_sessions
        """)

    if "strava_activities" in existing:
        parts.append("""
        SELECT
            CAST(id AS VARCHAR)             AS activity_id,
            'strava'                        AS data_source,
            name,
            LOWER(type)                     AS sport,
            CAST(start_date_local AS TIMESTAMP) AS start_time,
            distance                        AS total_distance_m,
            CAST(moving_time AS DOUBLE)     AS moving_time_s,
            CAST(elapsed_time AS DOUBLE)    AS elapsed_time_s,
            total_elevation_gain            AS total_elevation_gain_m,
            average_heartrate               AS avg_heart_rate,
            max_heartrate                   AS max_heart_rate,
            average_speed                   AS avg_speed_ms,
            max_speed                       AS max_speed_ms,
            CAST(NULL AS DOUBLE)            AS total_calories,
            CAST(NULL AS DOUBLE)            AS avg_cadence
        FROM strava_activities
        """)

    if not parts:
        print("  No source tables found — skipping unified_activities view.")
        return

    combined_sql = "\nUNION ALL\n".join(parts)

    # Deduplicate: same calendar day + same sport + same duration bucket (5-min)
    # → keep the Strava row when both sources have the same activity.
    # This handles the gap where Garmin export stops but Strava continues,
    # while also preventing double-counting when both sources cover the same workout.
    dedup_view_sql = f"""
    CREATE VIEW unified_activities AS
    WITH _combined AS (
        {combined_sql}
    )
    SELECT * FROM _combined
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            CAST(start_time AS DATE),
            LOWER(COALESCE(sport, 'unknown')),
            ROUND(COALESCE(moving_time_s, 0) / 300)
        ORDER BY
            CASE data_source WHEN 'strava' THEN 0 ELSE 1 END
    ) = 1
    """

    conn.execute("DROP VIEW IF EXISTS unified_activities")
    conn.execute(dedup_view_sql)
    rows = conn.execute(
        "SELECT data_source, COUNT(*) FROM unified_activities GROUP BY data_source ORDER BY data_source"
    ).fetchall()
    total = sum(r[1] for r in rows)
    breakdown = ", ".join(f"{r[0]}: {r[1]:,}" for r in rows)
    print(f"  unified_activities view: {total:,} deduplicated rows ({breakdown})")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

ALL_TABLES = [
    "sleep",
    "hydration",
    "vo2max",
    "daily-summaries",
    "activity-sessions",
    "activity-records",
    "strava",
    "view",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build garmin.duckdb from raw data files")
    parser.add_argument(
        "--table",
        default="all",
        choices=["all"] + ALL_TABLES,
        help="Which table to build (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit each table to N rows/files (useful for quick test runs)",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("S3_BUCKET"),
        help="S3 bucket to upload Parquet files to (or set S3_BUCKET env var). "
             "Omit to skip S3 upload.",
    )
    parser.add_argument(
        "--prefix",
        default=os.environ.get("S3_PREFIX", "garmin"),
        help="S3 key prefix (default: 'garmin', or set S3_PREFIX env var)",
    )
    args = parser.parse_args()

    s3_base: str | None = None
    if args.bucket:
        s3_base = f"s3://{args.bucket}/{args.prefix.strip('/')}"

    print(f"Database: {DB_PATH}")
    print(f"Staging:  {STAGING_DB_PATH}")
    if s3_base:
        print(f"S3 upload: {s3_base}")
    print()

    # Copy the existing DB into staging so partial builds (--table X) keep
    # all other tables.  The web app keeps reading the original file the
    # whole time; it is only replaced atomically at the very end.
    if DB_PATH.exists():
        print(f"Copying {DB_PATH.name} → {STAGING_DB_PATH.name} …")
        shutil.copy2(DB_PATH, STAGING_DB_PATH)
    else:
        STAGING_DB_PATH.unlink(missing_ok=True)

    conn = open_db()
    if s3_base:
        configure_s3(conn)

    tables = ALL_TABLES if args.table == "all" else [args.table]

    builders = {
        "sleep":              lambda: build_sleep(conn, limit=args.limit, s3_base=s3_base),
        "hydration":          lambda: build_hydration(conn, limit=args.limit, s3_base=s3_base),
        "vo2max":             lambda: build_vo2_max(conn, limit=args.limit, s3_base=s3_base),
        "daily-summaries":    lambda: build_daily_summaries(conn, limit=args.limit, s3_base=s3_base),
        "activity-sessions":  lambda: build_activity_sessions(conn, limit=args.limit, s3_base=s3_base),
        "activity-records":   lambda: build_activity_records(conn, limit=args.limit, s3_base=s3_base),
        "strava":             lambda: build_strava(conn, limit=args.limit, s3_base=s3_base),
        "view":               lambda: None,  # no-op — view is always rebuilt below
    }

    try:
        for table in tables:
            print()
            builders[table]()

        print()
        build_unified_activities_view(conn)
        ensure_training_plan_tables(conn)

        conn.close()

        # Atomically replace the live DB with the newly built staging file.
        # os.replace is atomic on POSIX — the web app never sees a partial file.
        os.replace(STAGING_DB_PATH, DB_PATH)
        size_mb = DB_PATH.stat().st_size / 1_048_576
        print(f"\nDone. garmin.duckdb is {size_mb:.1f} MB")

    except Exception:
        conn.close()
        STAGING_DB_PATH.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    main()
