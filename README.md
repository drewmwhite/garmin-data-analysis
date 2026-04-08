# Garmin Data Extraction

A personal data pipeline for exploring Garmin fitness and wellness data. Extracts records from local Garmin data exports (JSON and `.fit` files), loads them into a local DuckDB database, and serves them through a FastAPI REST API consumed by a vanilla JS frontend.

---

## How it works

```text
data/                          Raw Garmin export files (JSON + .fit binaries)
    ├── sleep/
    ├── hydration/
    ├── daily_summary/
    ├── activity_vo2_max/
    └── DI_CONNECT/activity-data/   ~18,000 .fit files

         ↓  python db/build.py   (run once, or on data updates)

garmin.duckdb                  Local columnar database — all data pre-loaded,
                               queries are instant with no file re-parsing

         ↓  uvicorn api:app

FastAPI REST API               Serves filtered, paginated JSON to the frontend
frontend/                      Vanilla JS dashboard — activity browser, charts
```

---

## Repository layout

```text
.
├── backend/
│   ├── scripts/
│   │   └── upload_to_s3.py     Upload data to S3 as partitioned Parquet
│   ├── src/
│   │   ├── api/app.py          FastAPI application
│   │   ├── extraction/         Data extractors (JSON + FIT parsers)
│   │   └── services/
│   │       ├── duckdb_service.py   Query layer for garmin.duckdb
│   │       └── data_service.py    (legacy — superseded by duckdb_service)
│   ├── tests/
│   └── requirements.txt
├── db/
│   └── build.py                Builds garmin.duckdb from raw data files
├── data/                       Raw Garmin export files (not version-controlled)
├── frontend/                   Static HTML/CSS/JS dashboard
├── garmin.duckdb               Generated database file (gitignored)
└── .env                        Local credentials (gitignored, see .env.example)
```

---

## Quickstart

### 1. Install dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r backend/requirements.txt
```

### 2. Build the database

Load all raw data into `garmin.duckdb`. Run this once, and again whenever your raw data files change.

```bash
python db/build.py
```

To build a single table or test with a subset of FIT files:

```bash
python db/build.py --table sleep
python db/build.py --table activity-records --limit 100
```

Available tables: `sleep`, `hydration`, `vo2max`, `daily-summaries`, `activity-sessions`, `activity-records`

### 3. Start the API

```bash
PYTHONPATH=backend/src uvicorn api:app --reload --port 8200
```

### 4. Open the frontend

Open `frontend/index.html` in a browser (with the API running on port 8200).

---

## Database tables

| Table | Source | Description |
| --- | --- | --- |
| `sleep_records` | `data/sleep/*.json` | Nightly sleep with sleep score columns |
| `hydration_records` | `data/hydration/*.json` | Per-entry hydration logs |
| `vo2_max_records` | `data/activity_vo2_max/*.json` | VO2 max readings per activity |
| `daily_summaries` | `data/daily_summary/*.json` | Steps, calories, HR, intensity minutes |
| `daily_stress_aggregators` | `data/daily_summary/*.json` | Stress levels by type (TOTAL / AWAKE / ASLEEP) |
| `activity_sessions` | `data/DI_CONNECT/activity-data/*.fit` | One row per activity (sport, distance, HR, pace, GPS) |
| `activity_records` | `data/DI_CONNECT/activity-data/*.fit` | Time-series ~1 Hz (HR, speed, cadence, altitude, GPS) |

---

## API reference

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

Available slugs: `sleep`, `hydration`, `vo2max`, `daily-summaries`, `daily-stress`, `activity-sessions`

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

---

## S3 / Parquet upload (optional)

Upload all data to S3 as Hive-partitioned Parquet for cheap cloud analytics with DuckDB or Athena.

```bash
# Set credentials in .env or environment
export S3_BUCKET=my-bucket
export S3_PREFIX=garmin

python backend/scripts/upload_to_s3.py
python backend/scripts/upload_to_s3.py --dataset activity-sessions
python backend/scripts/upload_to_s3.py --dataset activity-records --limit 100
```

Partition layout on S3:

```text
garmin/
  activity_sessions/sport=running/year=2024/part-0.parquet
  activity_records/year=2024/month=01/part-0.parquet
  sleep/year=2024/part-0.parquet
  ...
```

Query from S3 with DuckDB (no running server needed):

```python
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-east-1'")

conn.execute("""
    SELECT * FROM read_parquet('s3://my-bucket/garmin/activity_sessions/**/*.parquet',
                               hive_partitioning = true)
    WHERE sport = 'running' AND year = 2024
""").df()
```

---

## Environment variables

Copy `.env.example` to `.env` and fill in values as needed.

| Variable | Used by | Purpose |
| --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | `upload_to_s3.py` | AWS credentials for S3 upload |
| `AWS_SECRET_ACCESS_KEY` | `upload_to_s3.py` | AWS credentials for S3 upload |
| `AWS_DEFAULT_REGION` | `upload_to_s3.py` | AWS region |
| `S3_BUCKET` | `upload_to_s3.py` | Destination S3 bucket |
| `S3_PREFIX` | `upload_to_s3.py` | Key prefix (default: `garmin`) |
| `SUPABASE_URL` | (unused) | Reserved for future use |
| `SUPABASE_SERVICE_KEY` | (unused) | Reserved for future use |

---

## Querying DuckDB directly

The `garmin.duckdb` file is a standard DuckDB database. Query it directly for ad-hoc analysis:

```python
import duckdb
conn = duckdb.connect("garmin.duckdb", read_only=True)

# Sleep trends
conn.execute("""
    SELECT calendarDate, score_overall, deepSleepSeconds / 3600.0 AS deep_hours
    FROM sleep_records
    WHERE score_overall IS NOT NULL
    ORDER BY calendarDate DESC
    LIMIT 30
""").df()

# Running volume by month
conn.execute("""
    SELECT
        DATE_TRUNC('month', start_time) AS month,
        COUNT(*) AS runs,
        SUM(total_distance) / 1000 AS total_km
    FROM activity_sessions
    WHERE sport = 'running'
    GROUP BY 1
    ORDER BY 1 DESC
""").df()

# HR during a specific activity
conn.execute("""
    SELECT timestamp, heart_rate, speed, altitude
    FROM activity_records
    WHERE activity_id = '100309038740'
    ORDER BY timestamp
""").df()

conn.close()
```

---

## Running tests

```bash
python -m unittest discover -s backend/tests -v
```
