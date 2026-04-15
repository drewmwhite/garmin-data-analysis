# Garmin Data Extraction

A local pipeline for importing Garmin exports, loading them into DuckDB, and exploring them through a FastAPI API and a simple frontend.

## What This Repo Does

This project reads Garmin wellness JSON exports and Garmin activity `.fit` files from a local `data/` directory, builds a local `garmin.duckdb` database, and serves that data through:

- a FastAPI backend in `backend/src`
- a static frontend in `frontend/`

The database is built ahead of time, so the API does not need to re-parse large JSON and FIT files on every request.

## Repository Layout

```text
.
├── backend/
│   ├── scripts/
│   │   └── upload_to_s3.py
│   ├── src/
│   │   ├── api/app.py
│   │   ├── extraction/
│   │   └── services/
│   └── tests/
├── db/
│   └── build.py
├── data/                       Raw Garmin export files (not committed)
├── frontend/
├── garmin.duckdb               Generated local DuckDB database (gitignored)
├── .env.example
└── Makefile
```

## End-to-End Setup

### 1. Clone the repo and create a virtualenv

```bash
git clone <your-repo-url>
cd garmin-data-extraction

python3 -m venv venv
source venv/bin/activate
```

### 2. Install dependencies

Use either the Make target or the direct pip command.

```bash
make install
```

```bash
pip install -r backend/requirements.txt
```

### 3. Create your local `.env`

```bash
cp .env.example .env
```

For a local Garmin-only setup, no environment variables are required to build `garmin.duckdb` from local files.

These values are optional unless you use the related features:

- `STRAVA_CLIENT_ID`, `STRAVA_CLIENT_SECRET`, `STRAVA_REFRESH_TOKEN`: only for Strava imports
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `S3_BUCKET`, `S3_PREFIX`: only for S3/Parquet upload
- `OPENAI_API_KEY`, `OPENAI_MODEL`, `OPENAI_REASONING_EFFORT`, `OPENAI_TIMEOUT_SECONDS`: only for training-plan generation features
- `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `DATABASE_URL`: not needed for the local DuckDB workflow

If you are only importing Garmin data locally, you can leave unused values blank or remove them from `.env`.

### 4. Export your Garmin data

This repo expects a full Garmin export, not a few manually downloaded CSVs.

Garmin’s current support guidance is to request your personal data archive from your Garmin account/data-management flow. Garmin then prepares the archive and emails you a download link. Garmin notes that export generation can take about 48 hours, and sometimes longer depending on account size.

Official Garmin support pages:

- https://support.garmin.com/en-US/?faq=W1TvTPW8JZ6LfJSfK512Q8
- https://support.garmin.com/en-IN/?faq=q22kMdCbU23NUT2Wmspz16

After Garmin emails the archive link:

1. Download the export ZIP.
2. Extract it locally.
3. Copy the relevant folders/files into this repo’s `data/` directory using the layout below.

### 5. Place the export into the expected `data/` layout

The extractors look for these exact repo-local locations:

```text
data/
├── sleep/
├── hydration/
├── activity_vo2_max/
├── daily_summary/
└── DI_CONNECT/
    └── activity-data/
```

Expected file patterns:

- `data/sleep/*_sleepData.json`
- `data/hydration/HydrationLogFile_*.json`
- `data/activity_vo2_max/ActivityVo2Max_*.json`
- `data/daily_summary/UDSFile_*.json`
- `data/DI_CONNECT/activity-data/*.fit`

Typical post-unzip mapping looks like this:

| Garmin export content | Copy into this repo |
| --- | --- |
| sleep JSON files matching `*_sleepData.json` | `data/sleep/` |
| hydration JSON files matching `HydrationLogFile_*.json` | `data/hydration/` |
| VO2 max JSON files matching `ActivityVo2Max_*.json` | `data/activity_vo2_max/` |
| daily summary JSON files matching `UDSFile_*.json` | `data/daily_summary/` |
| activity FIT files under `DI_CONNECT/activity-data/` | `data/DI_CONNECT/activity-data/` |

If the `data/` folders do not exist yet, create them before copying files.

### 6. Build the DuckDB database

Use either the Make target or the direct build command.

```bash
make build
```

```bash
python db/build.py
```

This reads the raw Garmin files, writes tables into a staging database, rebuilds the unified activity view and training-plan tables, and then atomically replaces the live `garmin.duckdb` file. That staging flow matters because the API can keep reading the old database until the new one is fully built.

### 7. Start the API

```bash
make api
```

```bash
PYTHONPATH=backend/src python -m uvicorn api.app:app --reload --port 8200
```

The API will be available at `http://localhost:8200`.

### 8. Open the frontend

You can either open `frontend/index.html` directly or serve it locally:

```bash
make frontend-serve
```

Then open `http://localhost:8080`.

## Why DuckDB

DuckDB is a good fit here because this project is a local analytics workflow, not a multi-user transactional app.

- It is a single local file, so there is no separate database server to install or keep running.
- It is columnar, which makes read-heavy analytical queries fast.
- It works well for wide JSON-derived wellness tables and large activity-record datasets.
- It is easy to query directly from Python for ad hoc analysis.
- It keeps the setup simple for anyone cloning the repo.

In this project specifically, DuckDB lets us do one heavier import step up front and then keep API reads fast and predictable.

## How the Database Build Works

`db/build.py` is the entrypoint for database construction.

At a high level it:

1. Loads `.env` from the repo root.
2. Copies the existing `garmin.duckdb` into `garmin_staging.duckdb` when present.
3. Rebuilds one table or all tables from local source files.
4. Rebuilds the unified activity view.
5. Ensures training-plan tables exist.
6. Atomically replaces `garmin.duckdb` with the staging file.

Supported local build modes:

```bash
# Full rebuild
python db/build.py

# Rebuild one table
python db/build.py --table sleep
python db/build.py --table hydration
python db/build.py --table vo2max
python db/build.py --table daily-summaries
python db/build.py --table activity-sessions
python db/build.py --table activity-records
python db/build.py --table strava
python db/build.py --table view

# Quick test run with fewer rows/files
python db/build.py --table activity-records --limit 100
python db/build.py --table activity-sessions --limit 25

# Strava imports
python db/build.py --table strava
python db/build.py --table strava --strava-recent-days 30
python db/build.py --table strava --strava-cache-dir logs/strava_api/<run-dir>

# Build and upload Parquet to S3
python db/build.py --bucket my-bucket --prefix garmin
```

Makefile equivalents:

```bash
make build
make build-table TABLE=sleep
make build-table TABLE=activity-records LIMIT=100
make build-strava
make build-strava-30d
make build-strava-cache CACHE_DIR=logs/strava_api/<run-dir>
make build-s3 BUCKET=my-bucket PREFIX=garmin
```

Notes:

- `--limit` is useful for quick parsing checks when working with large FIT exports.
- Partial builds preserve other tables by starting from the existing database copy.
- S3 upload is optional and only runs when `--bucket` or `S3_BUCKET` is provided.

## Database Tables

| Table | Source | Description |
| --- | --- | --- |
| `sleep_records` | `data/sleep/*.json` | Nightly sleep records with flattened sleep-score metrics |
| `hydration_records` | `data/hydration/*.json` | Hydration log entries |
| `vo2_max_records` | `data/activity_vo2_max/*.json` | VO2 max readings |
| `daily_summaries` | `data/daily_summary/*.json` | Daily wellness summaries |
| `daily_stress_aggregators` | `data/daily_summary/*.json` | Daily stress aggregates |
| `activity_sessions` | `data/DI_CONNECT/activity-data/*.fit` | One row per recorded activity |
| `activity_records` | `data/DI_CONNECT/activity-data/*.fit` | Time-series activity records |
| `strava_activities` | Strava API or cached Strava JSON | Optional Strava activity summaries |
| `strava_laps` | Strava API or cached Strava JSON | Optional Strava lap data |

## API Reference

Base URL: `http://localhost:8200`

### Health

```http
GET /api/v1/health
→ {"status": "ok"}
```

### Datasets

```http
GET /api/v1/datasets
→ {"datasets": [{slug, title, description, record_count, column_count, sample_columns}, ...]}

GET /api/v1/datasets/{slug}?limit=50
→ {slug, record_count, column_count, sample_columns, returned_records, records: [...]}
```

Available slugs: `sleep`, `hydration`, `vo2max`, `daily-summaries`, `daily-stress`, `activity-sessions`, `strava-activities`, `strava-laps`

### Activities

```http
GET /api/v1/activities
  ?sport=running
  &date_from=2024-01-01
  &date_to=2024-12-31
  &sort_by=start_time
  &sort_dir=desc
  &limit=50
  &offset=0
→ {total, returned, sessions: [...]}

GET /api/v1/activities/{activity_id}
→ {activity_id, session: {...}}

GET /api/v1/activities/{activity_id}/records
→ {activity_id, record_count, records: [...]}
```

`sort_by` options: `start_time`, `total_distance`, `total_calories`, `avg_heart_rate`, `avg_speed`, `total_ascent`

## Querying DuckDB Directly

`garmin.duckdb` is a standard DuckDB database file, so you can query it directly for analysis.

```python
import duckdb

conn = duckdb.connect("garmin.duckdb", read_only=True)

conn.execute("""
    SELECT calendarDate, score_overall
    FROM sleep_records
    ORDER BY calendarDate DESC
    LIMIT 30
""").df()

conn.execute("""
    SELECT timestamp, heart_rate, speed, altitude
    FROM activity_records
    WHERE activity_id = '100309038740'
    ORDER BY timestamp
""").df()

conn.close()
```

## Optional S3 / Parquet Upload

You can export datasets to S3 as Hive-partitioned Parquet for cloud analytics.

```bash
make upload-s3 BUCKET=my-bucket PREFIX=garmin
make upload-s3-dataset BUCKET=my-bucket DATASET=activity-sessions PREFIX=garmin
```

```bash
python backend/scripts/upload_to_s3.py --bucket my-bucket --prefix garmin
python backend/scripts/upload_to_s3.py --bucket my-bucket --prefix garmin --dataset activity-sessions
```

This requires AWS credentials in `.env` or your shell environment.

## Running Tests

```bash
make test
```

```bash
python -m unittest discover -s backend/tests -v
```
