# Backend Architecture

The backend is intentionally library-first. Extraction and transformation logic lives in the package, while entrypoints stay thin so a future CLI or HTTP API can reuse the same core code.
