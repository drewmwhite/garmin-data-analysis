# Backend

The backend workspace contains the Garmin extraction library and backend-focused tests.

## Structure

- `src/garmin_data_extraction/`: reusable extraction logic and runner modules
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

Use the root compatibility entrypoint to run the extraction workflow in isolation:

```bash
./venv/bin/python main.py
```

You can also run the backend runner directly:

```bash
./venv/bin/python backend/src/garmin_data_extraction/runner.py
```

Or execute it as a package module once `backend/src` is on `PYTHONPATH` or the package is installed:

```bash
PYTHONPATH=backend/src ./venv/bin/python -m garmin_data_extraction
```

Or run the backend tests directly:

```bash
./venv/bin/python -m unittest discover -s backend/tests -v
```

## Future API location

When you add a web server, place the transport layer in a dedicated module under `src/garmin_data_extraction/api/` or an adjacent `src/garmin_data_extraction/server/` package so the extraction library remains reusable.
