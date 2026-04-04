from garmin_extractor import GarminDataExtractor


if __name__ == "__main__":
    sleep_extractor = GarminDataExtractor()
    hydration_extractor = GarminDataExtractor(data_dir="data/hydration")
    vo2_max_extractor = GarminDataExtractor(data_dir="data/activity_vo2_max")

    try:
        sleep_df, sleep_json = sleep_extractor.load_sleep_data()
    except Exception as exc:
        print(f"Failed to load sleep data: {exc}")
    else:
        print("Loaded sleep dataframe and JSON.")
        print(f"Rows: {len(sleep_df)}")
        print(f"Columns: {len(sleep_df.columns)}")
        print("Column sample:", ", ".join(sleep_df.columns[:10]))
        print(sleep_df.head())
        print(sleep_df.tail())
        print(f"JSON length: {len(sleep_json)} characters")

    try:
        hydration_df, hydration_json = hydration_extractor.load_hydration_data()
    except Exception as exc:
        print(f"Failed to load hydration data: {exc}")
    else:
        print("Loaded hydration dataframe and JSON.")
        print(f"Rows: {len(hydration_df)}")
        print(f"Columns: {len(hydration_df.columns)}")
        print("Column sample:", ", ".join(hydration_df.columns[:10]))
        print(hydration_df.head())
        print(hydration_df.tail())
        print(f"JSON length: {len(hydration_json)} characters")

    try:
        vo2_max_df, vo2_max_json = vo2_max_extractor.load_activity_vo2_max_data()
    except Exception as exc:
        print(f"Failed to load activity VO2 max data: {exc}")
    else:
        print("Loaded activity VO2 max dataframe and JSON.")
        print(f"Rows: {len(vo2_max_df)}")
        print(f"Columns: {len(vo2_max_df.columns)}")
        print("Column sample:", ", ".join(vo2_max_df.columns[:10]))
        print(vo2_max_df.head())
        print(vo2_max_df.tail())
        print(f"JSON length: {len(vo2_max_json)} characters")
