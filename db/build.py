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
import sys
from pathlib import Path

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "garmin.duckdb"

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


# ---------------------------------------------------------------------------
# Connection + S3
# ---------------------------------------------------------------------------

def open_db() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


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
    if s3_base:
        print(f"S3 upload: {s3_base}")
    print()

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
    }

    for table in tables:
        print()
        builders[table]()

    conn.close()
    size_mb = DB_PATH.stat().st_size / 1_048_576
    print(f"\nDone. garmin.duckdb is {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
