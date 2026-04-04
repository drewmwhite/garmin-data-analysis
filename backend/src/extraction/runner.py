from __future__ import annotations

from extraction import GarminDataExtractor


def run_isolated_extraction() -> None:
    datasets = (
        ("sleep", GarminDataExtractor(), "load_sleep_data"),
        ("hydration", GarminDataExtractor(data_dir="data/hydration"), "load_hydration_data"),
        (
            "activity VO2 max",
            GarminDataExtractor(data_dir="data/activity_vo2_max"),
            "load_activity_vo2_max_data",
        ),
    )

    for dataset_name, extractor, load_method_name in datasets:
        try:
            dataframe, json_output = getattr(extractor, load_method_name)()
        except Exception as exc:
            print(f"Failed to load {dataset_name} data: {exc}")
            continue

        print(f"Loaded {dataset_name} dataframe and JSON.")
        print(f"Rows: {len(dataframe)}")
        print(f"Columns: {len(dataframe.columns)}")
        print("Column sample:", ", ".join(dataframe.columns[:10]))
        print(dataframe.head())
        print(dataframe.tail())
        print(f"JSON length: {len(json_output)} characters")


def main() -> None:
    run_isolated_extraction()


if __name__ == "__main__":
    main()
