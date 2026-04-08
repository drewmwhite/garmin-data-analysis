from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from services.data_service import (
    DATASET_CONFIGS,
    build_dataset_summary,
    get_activity_records,
    get_activity_session,
    get_activity_sessions,
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


@app.get("/api/v1/activities")
def list_activities(
    sport: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    sort_by: str = Query(default="start_time"),
    sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return get_activity_sessions(
        sport=sport,
        date_from=date_from,
        date_to=date_to,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )


@app.get("/api/v1/activities/{activity_id}")
def get_activity(activity_id: str) -> dict[str, Any]:
    session = get_activity_session(activity_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Activity {activity_id!r} not found.")
    return {"activity_id": activity_id, "session": session}


@app.get("/api/v1/activities/{activity_id}/records")
def get_activity_timeseries(activity_id: str) -> dict[str, Any]:
    records = get_activity_records(activity_id)
    return {"activity_id": activity_id, "record_count": len(records), "records": records}
