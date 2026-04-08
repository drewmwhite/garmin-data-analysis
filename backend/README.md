# Backend

Python extraction library, FastAPI application, and DuckDB service layer.

## Structure

```text
backend/
├── scripts/
│   └── upload_to_s3.py       Upload data to S3 as partitioned Parquet
├── src/
│   ├── api/app.py            FastAPI application and route definitions
│   ├── extraction/
│   │   ├── extractor.py      JSON data extractor (sleep, hydration, VO2 max, daily summary)
│   │   ├── fit_extractor.py  Binary .fit file extractor (activity sessions + time-series)
│   │   └── runner.py         Standalone extraction runner for local testing
│   └── services/
│       ├── duckdb_service.py Query layer — all API queries go through here
│       └── data_service.py   Legacy file-based service (superseded)
├── tests/
│   ├── test_api.py
│   └── test_extractor.py
└── requirements.txt
```

## Setup

```bash
pip install -r backend/requirements.txt
```

## Build the database

Before starting the API, build `garmin.duckdb` from raw data files (run from repo root):

```bash
python db/build.py
```

See the [root README](../README.md) for full build options.

## Start the API

```bash
PYTHONPATH=backend/src uvicorn api:app --reload --port 8200
```

The API will return a 500 error on any request if `garmin.duckdb` has not been built yet.

## Run tests

```bash
python -m unittest discover -s backend/tests -v
```

## Key modules

### `extraction/extractor.py` — `GarminDataExtractor`

Loads JSON export files for sleep, hydration, VO2 max, and daily summary data.
Flattens one level of nested JSON objects into flat column names (e.g. `sleepScores_overallScore`).

```python
from extraction.extractor import GarminDataExtractor
extractor = GarminDataExtractor(data_dir="data/sleep")
records = extractor.extract_sleep_records()   # list of dicts
df = extractor.load_sleep_dataframe()         # pandas DataFrame
```

### `extraction/fit_extractor.py` — `GarminFitExtractor`

Parses binary Garmin `.fit` files. Supports local paths and S3 URIs via `fsspec`.

```python
from extraction.fit_extractor import GarminFitExtractor
extractor = GarminFitExtractor()
sessions = extractor.extract_activity_session_records(activity_limit=100)
records = extractor.extract_activity_record_records(activity_limit=10)
```

### `services/duckdb_service.py`

All API query logic. Opens `garmin.duckdb` in read-only mode per request.

| Function | Description |
| --- | --- |
| `list_datasets()` | Row/column counts for all tables |
| `get_dataset_records(slug, limit)` | Records from any dataset table |
| `get_activity_sessions(...)` | Filtered, sorted, paginated sessions |
| `get_activity_session(activity_id)` | Single session by ID |
| `get_activity_records(activity_id)` | Time-series rows for one activity |
