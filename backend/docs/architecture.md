# Architecture

## Data flow

```text
Raw files (local)
  data/sleep/*.json
  data/hydration/*.json
  data/daily_summary/*.json
  data/activity_vo2_max/*.json
  data/DI_CONNECT/activity-data/*.fit   (~18,000 files, ~350 MB)
          │
          │  db/build.py  (run once)
          ▼
  garmin.duckdb   ←── persistent columnar database, gitignored
          │
          │  FastAPI + duckdb_service.py  (per request)
          ▼
  REST API  →  frontend
```

## Layers

### Extraction (`backend/src/extraction/`)

Two extractors, each read-only against local files:

- **`GarminDataExtractor`** — reads JSON export files. Flattens one level of nested objects so every record is a flat dict. Adds `source_file`, `file_start_date`, `file_end_date` metadata.
- **`GarminFitExtractor`** — reads binary `.fit` files using `fitparse`. Supports local paths and S3 URIs via `fsspec`. Converts GPS semicircles to decimal degrees. Returns two record types: session-level summaries and time-series (~1 Hz) records.

### Build (`db/build.py`)

Runs once to populate `garmin.duckdb`. For each dataset:

1. Calls the appropriate extractor to get a pandas DataFrame.
2. Drops any columns containing nested objects (lists/dicts) that can't be stored as flat columns.
3. Writes the DataFrame into a DuckDB table using `CREATE TABLE AS SELECT * FROM df`.

Activity records (potentially 27M+ rows) are processed in batches of 100 FIT files at a time to avoid OOM. Daily stress aggregators are extracted from the nested `allDayStress.aggregatorList` field and written to a separate `daily_stress_aggregators` table.

### Service (`backend/src/services/duckdb_service.py`)

Opens `garmin.duckdb` in read-only mode for each request. All query logic lives here — filtering, sorting, and pagination are handled in SQL rather than Python. Returns plain dicts ready for JSON serialisation.

Connection is opened and closed per-call. DuckDB read-only connections are safe for concurrent use.

### API (`backend/src/api/app.py`)

Thin FastAPI layer. Routes map directly to `duckdb_service` functions. No business logic in the route handlers.

## Database tables

| Table | Rows (approx.) | Source |
| --- | --- | --- |
| `sleep_records` | ~1,900 | `data/sleep/*.json` |
| `hydration_records` | ~1,600 | `data/hydration/*.json` |
| `vo2_max_records` | ~175 | `data/activity_vo2_max/*.json` |
| `daily_summaries` | ~1,900 | `data/daily_summary/*.json` |
| `daily_stress_aggregators` | ~5,700 | `data/daily_summary/*.json` |
| `activity_sessions` | ~18,000 | `data/DI_CONNECT/activity-data/*.fit` |
| `activity_records` | ~10–27M | `data/DI_CONNECT/activity-data/*.fit` |

## S3 / Parquet (optional)

`backend/scripts/upload_to_s3.py` writes Hive-partitioned Parquet to S3 instead of a local database. The partition strategy is:

- `activity_sessions`: partitioned by `sport / year`
- `activity_records`: partitioned by `year / month`
- All other tables: partitioned by `year`

These files can be queried directly with DuckDB (`read_parquet(..., hive_partitioning=true)`) or AWS Athena without running any server. Cost is effectively S3 storage only (~$0.023/GB/month).
