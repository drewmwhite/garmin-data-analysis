# Frontend

This workspace is a static vanilla frontend that now fetches live data from the backend FastAPI service.

## Files

- `index.html`: application shell
- `styles/main.css`: design system tokens and layout styling
- `scripts/app.js`: API fetches, dataset summaries, and preview rendering
- `assets/`: reserved for future images, icons, or local mock payloads

## Run locally

Start the backend API first:

```bash
PYTHONPATH=backend/src ./venv/bin/uvicorn api:app --reload --port 8200
```

Then open `frontend/index.html` in a browser, or serve the repository root with a static file server for a cleaner local workflow.

No framework, bundler, or package manager is required in this pass.
