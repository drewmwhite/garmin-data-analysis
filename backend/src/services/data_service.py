from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from extraction.extractor import (
    DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR,
    DEFAULT_HYDRATION_DATA_DIR,
    DEFAULT_SLEEP_DATA_DIR,
    GarminDataExtractor,
)
from extraction.fit_extractor import (
    DEFAULT_ACTIVITY_FIT_DATA_DIR,
    GarminFitExtractor,
)


@dataclass(frozen=True)
class DatasetConfig:
    slug: str
    title: str
    description: str
    data_dir: str | Path
    extract_method_name: str
    extractor_class: type = field(default=GarminDataExtractor)
    extract_kwargs: dict = field(default_factory=dict)


DATASET_CONFIGS: dict[str, DatasetConfig] = {
    "sleep": DatasetConfig(
        slug="sleep",
        title="Sleep",
        description="Nightly sleep records with flattened wellness metrics.",
        data_dir=DEFAULT_SLEEP_DATA_DIR,
        extract_method_name="extract_sleep_records",
    ),
    "hydration": DatasetConfig(
        slug="hydration",
        title="Hydration",
        description="Hydration log entries for daily intake analysis.",
        data_dir=DEFAULT_HYDRATION_DATA_DIR,
        extract_method_name="extract_hydration_records",
    ),
    "activity-vo2-max": DatasetConfig(
        slug="activity-vo2-max",
        title="Activity VO2 Max",
        description="Training VO2 max records derived from activity history.",
        data_dir=DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR,
        extract_method_name="extract_activity_vo2_max_records",
    ),
    "activity-sessions": DatasetConfig(
        slug="activity-sessions",
        title="Activity Sessions",
        description="Per-activity session summaries parsed from .fit files (sport, distance, HR, pace, etc.).",
        data_dir=DEFAULT_ACTIVITY_FIT_DATA_DIR,
        extract_method_name="extract_activity_session_records",
        extractor_class=GarminFitExtractor,
        extract_kwargs={"activity_limit": 500},
    ),
    "activity-records": DatasetConfig(
        slug="activity-records",
        title="Activity Records",
        description="Time-series data points (~1 Hz) from .fit activity files (HR, speed, GPS, cadence, etc.).",
        data_dir=DEFAULT_ACTIVITY_FIT_DATA_DIR,
        extract_method_name="extract_activity_record_records",
        extractor_class=GarminFitExtractor,
        extract_kwargs={"activity_limit": 50},
    ),
}


def list_dataset_configs() -> list[DatasetConfig]:
    return list(DATASET_CONFIGS.values())


def get_dataset_config(dataset_slug: str) -> DatasetConfig:
    return DATASET_CONFIGS[dataset_slug]


def get_dataset_records(dataset_slug: str) -> list[dict[str, Any]]:
    config = get_dataset_config(dataset_slug)
    extractor = config.extractor_class(data_dir=config.data_dir)
    extract_method = getattr(extractor, config.extract_method_name)
    return extract_method(**config.extract_kwargs)


def build_dataset_summary(dataset_slug: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    config = get_dataset_config(dataset_slug)
    column_count = len(records[0]) if records else 0
    sample_columns = list(records[0].keys())[:6] if records else []

    return {
        "slug": config.slug,
        "title": config.title,
        "description": config.description,
        "record_count": len(records),
        "column_count": column_count,
        "sample_columns": sample_columns,
    }
