from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from services.data_service import (
    DATASET_CONFIGS,
    build_dataset_summary,
    get_dataset_records,
    list_dataset_configs,
)


app = FastAPI(
    title="Garmin Data Extraction API",
    version="0.1.0",
    description="REST API for Garmin sleep, hydration, and VO2 max datasets.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _resolve_records(dataset_slug: str) -> list[dict[str, Any]]:
    if dataset_slug not in DATASET_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset_slug}")
    return get_dataset_records(dataset_slug)


@app.get("/api/v1/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/datasets")
def list_datasets() -> dict[str, Any]:
    datasets = []

    for config in list_dataset_configs():
        records = get_dataset_records(config.slug)
        datasets.append(build_dataset_summary(config.slug, records))

    return {"datasets": datasets}


@app.get("/api/v1/datasets/{dataset_slug}")
def get_dataset(
    dataset_slug: str,
    limit: int | None = Query(default=None, ge=1, le=10000),
) -> dict[str, Any]:
    records = _resolve_records(dataset_slug)
    response_records = records[:limit] if limit is not None else records

    return {
        "dataset": build_dataset_summary(dataset_slug, records),
        "records": response_records,
        "returned_records": len(response_records),
    }
