from __future__ import annotations

import sys
from pathlib import Path


if __package__ in (None, ""):
    SRC_ROOT = Path(__file__).resolve().parents[1]
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))

from extraction import (
    DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR,
    DEFAULT_DAILY_SUMMARY_DATA_DIR,
    DEFAULT_HYDRATION_DATA_DIR,
    DEFAULT_PACEBANDS_DATA_DIR,
    GarminDataExtractor,
)


def run_isolated_extraction() -> None:
    datasets = (
        ("sleep", GarminDataExtractor(), "load_sleep_data"),
        ("hydration", GarminDataExtractor(data_dir=DEFAULT_HYDRATION_DATA_DIR), "load_hydration_data"),
        (
            "activity VO2 max",
            GarminDataExtractor(data_dir=DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR),
            "load_activity_vo2_max_data",
        ),
        (
            "daily summary",
            GarminDataExtractor(data_dir=DEFAULT_DAILY_SUMMARY_DATA_DIR),
            "load_daily_summary_data",
        ),
        ("pacebands", GarminDataExtractor(data_dir=DEFAULT_PACEBANDS_DATA_DIR), "load_pacebands_data"),
    )

    for dataset_name, extractor, load_method_name in datasets:
        
        print("=" * 50)
        
        try:
            dataframe, json_output = getattr(extractor, load_method_name)()
        except Exception as exc:
            print(f"Failed to load {dataset_name} data: {exc}")
            continue

        print(f"Loaded {dataset_name} dataframe and JSON.")
        print(f"Rows: {len(dataframe)}")
        print(f"Columns: {len(dataframe.columns)}")
        # print("Column sample:", ", ".join(dataframe.columns[:10]))
        # print(dataframe.head())
        # print(dataframe.tail())
        # print(f"JSON length: {len(json_output)} characters")


def main() -> None:
    run_isolated_extraction()


if __name__ == "__main__":
    main()
