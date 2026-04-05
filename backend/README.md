# Backend

The backend workspace contains the Garmin extraction library, the FastAPI application, service-layer code, and backend-focused tests.

## Structure

- `src/extraction/`: reusable extraction logic and extraction runner
- `src/services/`: dataset service and summary helpers
- `src/api/`: FastAPI application module
- `tests/`: backend unit tests
- `docs/`: backend design notes and future API documentation

## Local setup

Create or activate a Python 3.11+ virtual environment, then install dependencies:

```bash
./venv/bin/pip install -r backend/requirements.txt
```

If you prefer installing the backend as a package:

```bash
./venv/bin/pip install -e backend
```

## Run the extractor

Run the extraction workflow in isolation:

```bash
PYTHONPATH=backend/src ./venv/bin/python -m extraction.runner
```

Or execute it as a package module once `backend/src` is on `PYTHONPATH` or the package is installed:

```bash
PYTHONPATH=backend/src ./venv/bin/python -m extraction.runner
```

## Run the API

Start the FastAPI server from the repo root:

```bash
PYTHONPATH=backend/src ./venv/bin/uvicorn api:app --reload --port 8200
```

Available endpoints:

- `GET /api/v1/health`
- `GET /api/v1/datasets`
- `GET /api/v1/datasets/{dataset_slug}?limit=3`

Or run the backend tests directly:

```bash
./venv/bin/python -m unittest discover -s backend/tests -v
```
