# Garmin Data Extraction Monorepo

This repository is now organized as a frontend/backend monorepo:

- `backend/`: Python extraction library, tests, and backend documentation
- `frontend/`: static vanilla HTML/CSS/JS application shell
- `main.py`: root runner for executing the Python extraction flow in isolation
- `requirements.txt`: root convenience dependency entrypoint for the backend workspace

## Repository layout

```text
.
├── backend/
│   ├── docs/
│   ├── src/garmin_data_extraction/
│   └── tests/
├── frontend/
│   ├── assets/
│   ├── scripts/
│   └── styles/
├── data/
├── main.py
├── requirements.txt
└── README.md
```

## Backend workflow

Install backend dependencies into your virtual environment:

```bash
./venv/bin/pip install -r requirements.txt
```

Run the extractor from the root of the repo:

```bash
./venv/bin/python main.py
```

Run backend tests:

```bash
./venv/bin/python -m unittest discover -s backend/tests -v
```

## Frontend workflow

The frontend has no build step. Open `frontend/index.html` directly in a browser, or serve the repo root with a simple static file server if you want browser-like routing and caching behavior.

## Notes

- The repo root is intentionally minimal. Backend imports should target `garmin_data_extraction` from `backend/src/`.
- Data exports remain local under `data/` and are still excluded from version control.
