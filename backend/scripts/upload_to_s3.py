"""Upload all Garmin data to S3 as partitioned Parquet files.

Writes Hive-style partitioned Parquet so DuckDB and Athena can skip
irrelevant partitions at query time instead of scanning everything.

Partition layout:
  activity_sessions/  sport=running/year=2024/part-0.parquet
  activity_records/   year=2024/month=01/part-0.parquet
  sleep/              year=2024/part-0.parquet
  hydration/          year=2024/part-0.parquet
  daily_summaries/    year=2024/part-0.parquet
  vo2_max/            year=2024/part-0.parquet

Usage:
    python backend/scripts/upload_to_s3.py --bucket my-bucket --prefix garmin
    python backend/scripts/upload_to_s3.py --dataset activity-sessions
    python backend/scripts/upload_to_s3.py --dataset activity-records [--limit 100]

    # Or via environment variables:
    S3_BUCKET=my-bucket S3_PREFIX=garmin python backend/scripts/upload_to_s3.py

AWS credentials are read from the standard environment variables:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

# Allow running from the repo root without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from extraction.extractor import (  # noqa: E402
    GarminDataExtractor,
    DEFAULT_SLEEP_DATA_DIR,
    DEFAULT_HYDRATION_DATA_DIR,
    DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR,
    DEFAULT_DAILY_SUMMARY_DATA_DIR,
)
from extraction.fit_extractor import GarminFitExtractor  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload Garmin data to S3 as partitioned Parquet files."
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("S3_BUCKET"),
        help="S3 bucket name (or set S3_BUCKET env var)",
    )
    parser.add_argument(
        "--prefix",
        default=os.environ.get("S3_PREFIX", "garmin"),
        help="S3 key prefix / folder (default: 'garmin', or set S3_PREFIX env var)",
    )
    parser.add_argument(
        "--dataset",
        default="all",
        choices=[
            "all",
            "activity-sessions",
            "activity-records",
            "sleep",
            "hydration",
            "vo2max",
            "daily-summaries",
        ],
        help="Which dataset to upload (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="For activity-records: limit to first N FIT files (useful for testing)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Partition helpers
# ---------------------------------------------------------------------------

def _add_year_month(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Add integer year and month columns derived from a timestamp column."""
    col = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df = df.copy()
    df["year"] = col.dt.year.astype("Int32")
    df["month"] = col.dt.month.astype("Int32")
    return df


def _add_year(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Add an integer year column derived from a timestamp column."""
    col = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df = df.copy()
    df["year"] = col.dt.year.astype("Int32")
    return df


def _write_partitioned(df: pd.DataFrame, s3_path: str, partition_cols: list[str]) -> None:
    """Write a DataFrame as Hive-partitioned Parquet to S3."""
    df.to_parquet(s3_path, partition_cols=partition_cols, index=False, engine="pyarrow")


# ---------------------------------------------------------------------------
# Per-dataset upload functions
# ---------------------------------------------------------------------------

def upload_activity_sessions(bucket: str, prefix: str) -> None:
    print("Extracting activity sessions...")
    df = GarminFitExtractor().load_activity_session_dataframe()
    print(f"  {len(df)} sessions")

    # Normalise sport to lowercase string for clean partition names
    df["sport"] = df["sport"].fillna("unknown").str.lower()
    df = _add_year(df, "start_time")
    df = df.dropna(subset=["year"])

    s3_path = f"s3://{bucket}/{prefix}/activity_sessions/"
    print(f"  Writing partitioned Parquet to {s3_path} ...")
    _write_partitioned(df, s3_path, partition_cols=["sport", "year"])
    print("  Done.")


def upload_activity_records(bucket: str, prefix: str, limit: int | None = None) -> None:
    print("Extracting activity records (time-series) — this may take a while...")
    df = GarminFitExtractor().load_activity_record_dataframe(activity_limit=limit)
    print(f"  {len(df):,} data points")

    df = _add_year_month(df, "timestamp")
    df = df.dropna(subset=["year", "month"])

    s3_path = f"s3://{bucket}/{prefix}/activity_records/"
    print(f"  Writing partitioned Parquet to {s3_path} ...")
    _write_partitioned(df, s3_path, partition_cols=["year", "month"])
    print("  Done.")


def upload_sleep(bucket: str, prefix: str) -> None:
    print("Extracting sleep records...")
    df = GarminDataExtractor(data_dir=DEFAULT_SLEEP_DATA_DIR).load_sleep_dataframe()
    print(f"  {len(df)} records")

    df = _add_year(df, "calendarDate")
    s3_path = f"s3://{bucket}/{prefix}/sleep/"
    print(f"  Writing partitioned Parquet to {s3_path} ...")
    _write_partitioned(df, s3_path, partition_cols=["year"])
    print("  Done.")


def upload_hydration(bucket: str, prefix: str) -> None:
    print("Extracting hydration records...")
    df = GarminDataExtractor(data_dir=DEFAULT_HYDRATION_DATA_DIR).load_hydration_dataframe()
    print(f"  {len(df)} records")

    df = _add_year(df, "calendarDate")
    s3_path = f"s3://{bucket}/{prefix}/hydration/"
    print(f"  Writing partitioned Parquet to {s3_path} ...")
    _write_partitioned(df, s3_path, partition_cols=["year"])
    print("  Done.")


def upload_vo2_max(bucket: str, prefix: str) -> None:
    print("Extracting VO2 max records...")
    df = GarminDataExtractor(data_dir=DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR).load_activity_vo2_max_dataframe()
    print(f"  {len(df)} records")

    df = _add_year(df, "calendarDate")
    s3_path = f"s3://{bucket}/{prefix}/vo2_max/"
    print(f"  Writing partitioned Parquet to {s3_path} ...")
    _write_partitioned(df, s3_path, partition_cols=["year"])
    print("  Done.")


def upload_daily_summaries(bucket: str, prefix: str) -> None:
    print("Extracting daily summaries...")
    df = GarminDataExtractor(data_dir=DEFAULT_DAILY_SUMMARY_DATA_DIR).load_daily_summary_dataframe()
    print(f"  {len(df)} records")

    # allDayStress is a nested object — drop it (not Parquet-serializable as-is)
    stress_cols = [c for c in df.columns if "allDayStress" in c or "aggregator" in c.lower()]
    if stress_cols:
        df = df.drop(columns=stress_cols)

    df = _add_year(df, "calendarDate")
    s3_path = f"s3://{bucket}/{prefix}/daily_summaries/"
    print(f"  Writing partitioned Parquet to {s3_path} ...")
    _write_partitioned(df, s3_path, partition_cols=["year"])
    print("  Done.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

UPLOAD_ORDER = [
    "sleep",
    "hydration",
    "vo2max",
    "daily-summaries",
    "activity-sessions",
    "activity-records",
]


def main() -> None:
    args = parse_args()

    if not args.bucket:
        sys.exit(
            "Error: S3 bucket is required. "
            "Pass --bucket <name> or set the S3_BUCKET environment variable."
        )

    bucket = args.bucket
    prefix = args.prefix.strip("/")
    datasets = UPLOAD_ORDER if args.dataset == "all" else [args.dataset]

    for dataset in datasets:
        print(f"\n{'='*50}\nDataset: {dataset}\n{'='*50}")
        if dataset == "sleep":
            upload_sleep(bucket, prefix)
        elif dataset == "hydration":
            upload_hydration(bucket, prefix)
        elif dataset == "vo2max":
            upload_vo2_max(bucket, prefix)
        elif dataset == "daily-summaries":
            upload_daily_summaries(bucket, prefix)
        elif dataset == "activity-sessions":
            upload_activity_sessions(bucket, prefix)
        elif dataset == "activity-records":
            upload_activity_records(bucket, prefix, limit=args.limit)

    print("\nUpload complete.")


if __name__ == "__main__":
    main()
