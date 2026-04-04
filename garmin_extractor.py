from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


SLEEP_DATA_GLOB = "*_sleepData.json"
DEFAULT_SLEEP_DATA_DIR = Path("data/sleep")
HYDRATION_DATA_GLOB = "HydrationLogFile_*.json"
DEFAULT_HYDRATION_DATA_DIR = Path("data/hydration")
ACTIVITY_VO2_MAX_DATA_GLOB = "ActivityVo2Max_*.json"
DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR = Path("data/activity_vo2_max")
SLEEP_TIMESTAMP_COLUMNS = (
    "sleepStartTimestampGMT",
    "sleepEndTimestampGMT",
    "spo2SleepSummary_sleepMeasurementStartGMT",
    "spo2SleepSummary_sleepMeasurementEndGMT",
)
SLEEP_DATE_COLUMNS = ("calendarDate", "file_start_date", "file_end_date")
SLEEP_SORT_COLUMNS = ("calendarDate", "sleepStartTimestampGMT")
HYDRATION_TIMESTAMP_COLUMNS = ("persistedTimestampGMT", "timestampLocal")
HYDRATION_DATE_COLUMNS = ("calendarDate", "file_start_date", "file_end_date")
HYDRATION_SORT_COLUMNS = ("calendarDate", "timestampLocal", "persistedTimestampGMT")
ACTIVITY_VO2_MAX_TIMESTAMP_COLUMNS = ("timestampGmt",)
ACTIVITY_VO2_MAX_DATE_COLUMNS = ("calendarDate", "file_start_date", "file_end_date")
ACTIVITY_VO2_MAX_SORT_COLUMNS = ("calendarDate", "timestampGmt")


class GarminDataExtractor:
    def __init__(self, data_dir: str | Path = DEFAULT_SLEEP_DATA_DIR) -> None:
        self.data_dir = Path(data_dir)

    @staticmethod
    def _parse_file_date_range(file_path: Path) -> tuple[str | None, str | None]:
        matches = re.findall(r"\d{4}-\d{2}-\d{2}|\d{8}", file_path.stem)
        if len(matches) < 2:
            return None, None
        return (
            GarminDataExtractor._normalize_date_token(matches[0]),
            GarminDataExtractor._normalize_date_token(matches[1]),
        )

    @staticmethod
    def _normalize_date_token(date_token: str) -> str:
        if re.fullmatch(r"\d{8}", date_token):
            return f"{date_token[:4]}-{date_token[4:6]}-{date_token[6:]}"
        return date_token

    @staticmethod
    def _flatten_record(record: dict[str, Any]) -> dict[str, Any]:
        flattened: dict[str, Any] = {}

        for key, value in record.items():
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    flattened[f"{key}_{nested_key}"] = nested_value
                continue
            flattened[key] = value

        return flattened

    def _list_files(self, file_glob: str) -> list[Path]:
        files = sorted(self.data_dir.glob(file_glob))
        if not files:
            raise FileNotFoundError(
                f"No files matching {file_glob!r} were found in {self.data_dir}."
            )
        return files

    def _load_records(
        self,
        file_glob: str,
        *,
        flatten: bool = True,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for data_file in self._list_files(file_glob):
            with data_file.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

            if not isinstance(payload, list):
                raise ValueError(
                    f"Expected {data_file} to contain a top-level JSON list, "
                    f"found {type(payload).__name__}."
                )

            file_start_date, file_end_date = self._parse_file_date_range(data_file)

            for record in payload:
                if not isinstance(record, dict):
                    raise ValueError(
                        f"Expected each record in {data_file} to be a JSON object, "
                        f"found {type(record).__name__}."
                    )

                extracted_record = (
                    self._flatten_record(record) if flatten else dict(record)
                )
                extracted_record["source_file"] = data_file.name
                extracted_record["source_path"] = str(data_file)
                extracted_record["file_start_date"] = file_start_date
                extracted_record["file_end_date"] = file_end_date
                rows.append(extracted_record)

        return rows

    @staticmethod
    def _records_to_dataframe(
        records: list[dict[str, Any]],
        *,
        date_columns: tuple[str, ...],
        timestamp_columns: tuple[str, ...],
        sort_columns: tuple[str, ...],
    ) -> "pd.DataFrame":
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "pandas is required to load Garmin data into a dataframe. "
                "Install dependencies with `pip install -r requirements.txt`."
            ) from exc

        dataframe = pd.DataFrame(records)

        for column in date_columns:
            if column in dataframe.columns:
                dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")

        for column in timestamp_columns:
            if column in dataframe.columns:
                dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")

        present_sort_columns = [
            column for column in sort_columns if column in dataframe.columns
        ]
        if present_sort_columns:
            dataframe = dataframe.sort_values(present_sort_columns).reset_index(
                drop=True
            )
        else:
            dataframe = dataframe.reset_index(drop=True)

        return dataframe

    @staticmethod
    def _records_to_json(records: list[dict[str, Any]], *, indent: int = 2) -> str:
        return json.dumps(records, indent=indent, default=str)

    def extract_sleep_records(self) -> list[dict[str, Any]]:
        return self._load_records(SLEEP_DATA_GLOB, flatten=True)

    def load_sleep_dataframe(self) -> "pd.DataFrame":
        return self._records_to_dataframe(
            self.extract_sleep_records(),
            date_columns=SLEEP_DATE_COLUMNS,
            timestamp_columns=SLEEP_TIMESTAMP_COLUMNS,
            sort_columns=SLEEP_SORT_COLUMNS,
        )

    def load_sleep_data(self) -> tuple["pd.DataFrame", str]:
        records = self.extract_sleep_records()
        dataframe = self._records_to_dataframe(
            records,
            date_columns=SLEEP_DATE_COLUMNS,
            timestamp_columns=SLEEP_TIMESTAMP_COLUMNS,
            sort_columns=SLEEP_SORT_COLUMNS,
        )
        json_output = self._records_to_json(records)
        return dataframe, json_output

    def extract_hydration_records(self) -> list[dict[str, Any]]:
        return self._load_records(HYDRATION_DATA_GLOB, flatten=True)

    def load_hydration_dataframe(self) -> "pd.DataFrame":
        return self._records_to_dataframe(
            self.extract_hydration_records(),
            date_columns=HYDRATION_DATE_COLUMNS,
            timestamp_columns=HYDRATION_TIMESTAMP_COLUMNS,
            sort_columns=HYDRATION_SORT_COLUMNS,
        )

    def load_hydration_data(self) -> tuple["pd.DataFrame", str]:
        records = self.extract_hydration_records()
        dataframe = self._records_to_dataframe(
            records,
            date_columns=HYDRATION_DATE_COLUMNS,
            timestamp_columns=HYDRATION_TIMESTAMP_COLUMNS,
            sort_columns=HYDRATION_SORT_COLUMNS,
        )
        json_output = self._records_to_json(records)
        return dataframe, json_output

    def extract_activity_vo2_max_records(self) -> list[dict[str, Any]]:
        return self._load_records(ACTIVITY_VO2_MAX_DATA_GLOB, flatten=True)

    def load_activity_vo2_max_dataframe(self) -> "pd.DataFrame":
        return self._records_to_dataframe(
            self.extract_activity_vo2_max_records(),
            date_columns=ACTIVITY_VO2_MAX_DATE_COLUMNS,
            timestamp_columns=ACTIVITY_VO2_MAX_TIMESTAMP_COLUMNS,
            sort_columns=ACTIVITY_VO2_MAX_SORT_COLUMNS,
        )

    def load_activity_vo2_max_data(self) -> tuple["pd.DataFrame", str]:
        records = self.extract_activity_vo2_max_records()
        dataframe = self._records_to_dataframe(
            records,
            date_columns=ACTIVITY_VO2_MAX_DATE_COLUMNS,
            timestamp_columns=ACTIVITY_VO2_MAX_TIMESTAMP_COLUMNS,
            sort_columns=ACTIVITY_VO2_MAX_SORT_COLUMNS,
        )
        json_output = self._records_to_json(records)
        return dataframe, json_output


def load_sleep_dataframe(
    data_dir: str | Path = DEFAULT_SLEEP_DATA_DIR,
) -> "pd.DataFrame":
    extractor = GarminDataExtractor(data_dir=data_dir)
    return extractor.load_sleep_dataframe()


def load_hydration_dataframe(
    data_dir: str | Path = DEFAULT_HYDRATION_DATA_DIR,
) -> "pd.DataFrame":
    extractor = GarminDataExtractor(data_dir=data_dir)
    return extractor.load_hydration_dataframe()


def load_activity_vo2_max_dataframe(
    data_dir: str | Path = DEFAULT_ACTIVITY_VO2_MAX_DATA_DIR,
) -> "pd.DataFrame":
    extractor = GarminDataExtractor(data_dir=data_dir)
    return extractor.load_activity_vo2_max_dataframe()
