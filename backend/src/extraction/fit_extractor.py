from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = REPO_ROOT / "data"

ACTIVITY_FIT_DATA_GLOB = "*.fit"
DEFAULT_ACTIVITY_FIT_DATA_DIR = DATA_ROOT / "DI_CONNECT" / "activity-data"

# Semicircles-to-degrees conversion factor for GPS coordinates stored in FIT files
_SEMICIRCLES_TO_DEGREES = 180.0 / (2**31)

# Fields to pull from the `session` FIT message (skip enhanced_* duplicates and unknowns)
_SESSION_FIELDS = (
    "sport",
    "sub_sport",
    "start_time",
    "timestamp",
    "total_elapsed_time",
    "total_timer_time",
    "total_distance",
    "total_calories",
    "avg_heart_rate",
    "max_heart_rate",
    "avg_speed",
    "max_speed",
    "avg_cadence",
    "max_cadence",
    "avg_running_cadence",
    "max_running_cadence",
    "total_strides",
    "total_cycles",
    "total_ascent",
    "total_descent",
    "num_laps",
    "first_lap_index",
    "start_position_lat",
    "start_position_long",
    "event",
    "event_type",
    "trigger",
    "message_index",
)

# Fields to pull from the `file_id` FIT message
_FILE_ID_FIELDS = (
    "type",
    "manufacturer",
    "garmin_product",
    "serial_number",
    "time_created",
    "number",
)

# Fields to pull from each `record` FIT message
_RECORD_FIELDS = (
    "timestamp",
    "heart_rate",
    "cadence",
    "speed",
    "distance",
    "altitude",
    "power",
    "temperature",
    "position_lat",
    "position_long",
)

ACTIVITY_FIT_SESSION_TIMESTAMP_COLUMNS = ("start_time", "timestamp", "device_time_created")
ACTIVITY_FIT_SESSION_DATE_COLUMNS: tuple[()] = ()
ACTIVITY_FIT_SESSION_SORT_COLUMNS = ("start_time",)

ACTIVITY_FIT_RECORD_TIMESTAMP_COLUMNS = ("timestamp",)
ACTIVITY_FIT_RECORD_DATE_COLUMNS: tuple[()] = ()
ACTIVITY_FIT_RECORD_SORT_COLUMNS = ("activity_id", "timestamp")


class GarminFitExtractor:
    """Extracts Garmin activity data from binary .fit files.

    Supports local paths and S3 URIs (e.g. ``s3://bucket/prefix``) via fsspec.
    Install ``s3fs`` alongside ``fsspec`` to enable S3 access:

        pip install s3fs

    Usage::

        # Local
        extractor = GarminFitExtractor()
        df = extractor.load_activity_session_dataframe()

        # S3
        extractor = GarminFitExtractor(data_dir="s3://my-bucket/garmin/activity-data")
        df = extractor.load_activity_session_dataframe()
    """

    def __init__(self, data_dir: str | Path = DEFAULT_ACTIVITY_FIT_DATA_DIR) -> None:
        self.data_dir = str(data_dir)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_filesystem(self) -> tuple[Any, str]:
        """Return an (fsspec filesystem, root path) pair for ``self.data_dir``."""
        try:
            import fsspec
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "fsspec is required for file I/O. "
                "Install it with `pip install fsspec`."
            ) from exc
        return fsspec.url_to_fs(self.data_dir)

    def _list_fit_files(self, max_files: int | None = None) -> list[str]:
        """Return sorted list of .fit file paths under ``self.data_dir``.

        Args:
            max_files: Hard cap on the number of files returned. Useful for
                testing — limits total I/O regardless of file type.
        """
        fs, root_path = self._get_filesystem()
        pattern = f"{root_path.rstrip('/')}/*.fit"
        files = sorted(fs.glob(pattern))
        if not files:
            raise FileNotFoundError(
                f"No .fit files found in {self.data_dir!r}."
            )
        if max_files is not None:
            files = files[:max_files]
        return files

    def _read_fit_bytes(self, path: str) -> bytes:
        """Read raw bytes from ``path`` using fsspec (works for local and S3)."""
        fs, _ = self._get_filesystem()
        with fs.open(path, "rb") as f:
            return f.read()

    @staticmethod
    def _parse_activity_id(file_path: str) -> str:
        """Extract the numeric activity ID from a filename like ``user@email.com_12345.fit``."""
        stem = Path(file_path).stem  # e.g. "drew.m.white51@gmail.com_100309038740"
        match = re.search(r"_(\d+)$", stem)
        return match.group(1) if match else stem

    @staticmethod
    def _convert_gps(raw_value: Any) -> float | None:
        """Convert a FIT semicircle GPS value to decimal degrees."""
        if raw_value is None:
            return None
        try:
            return int(raw_value) * _SEMICIRCLES_TO_DEGREES
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Low-level file parsing
    # ------------------------------------------------------------------

    def _parse_session_from_fit(
        self, fit_bytes: bytes, file_path: str
    ) -> dict[str, Any] | None:
        """Parse one .fit file and return a session-level record, or None if not an activity."""
        try:
            from fitparse import FitFile
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "fitparse is required to parse .fit files. "
                "Install it with `pip install fitparse`."
            ) from exc

        ff = FitFile(fit_bytes)

        file_id_data: dict[str, Any] = {}
        session_data: dict[str, Any] = {}

        for msg in ff.get_messages():
            if msg.name == "file_id":
                for field_name in _FILE_ID_FIELDS:
                    val = msg.get_value(field_name)
                    if val is not None:
                        file_id_data[f"device_{field_name}"] = val

            elif msg.name == "session":
                for field_name in _SESSION_FIELDS:
                    val = msg.get_value(field_name)
                    if val is not None:
                        session_data[field_name] = val

        # Only emit records for activity-type files
        if str(file_id_data.get("device_type", "")).lower() != "activity":
            return None

        activity_id = self._parse_activity_id(file_path)
        source_file = Path(file_path).name

        record: dict[str, Any] = {
            "activity_id": activity_id,
            "source_file": source_file,
        }
        record.update(file_id_data)
        record.update(session_data)

        # Convert GPS semicircles → decimal degrees
        for gps_field in ("start_position_lat", "start_position_long"):
            if gps_field in record:
                record[gps_field] = self._convert_gps(record[gps_field])

        return record

    def _parse_records_from_fit(
        self, fit_bytes: bytes, file_path: str
    ) -> list[dict[str, Any]]:
        """Parse one .fit file and return a list of time-series record dicts.

        Returns an empty list if the file is not an activity type.
        """
        try:
            from fitparse import FitFile
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "fitparse is required to parse .fit files. "
                "Install it with `pip install fitparse`."
            ) from exc

        ff = FitFile(fit_bytes)

        is_activity = False
        rows: list[dict[str, Any]] = []
        activity_id = self._parse_activity_id(file_path)
        source_file = Path(file_path).name

        for msg in ff.get_messages():
            if msg.name == "file_id":
                if str(msg.get_value("type")).lower() == "activity":
                    is_activity = True

            elif msg.name == "record":
                row: dict[str, Any] = {
                    "activity_id": activity_id,
                    "source_file": source_file,
                }
                for field_name in _RECORD_FIELDS:
                    val = msg.get_value(field_name)
                    if val is not None:
                        row[field_name] = val

                # Convert GPS semicircles → decimal degrees
                for gps_field in ("position_lat", "position_long"):
                    if gps_field in row:
                        row[gps_field] = self._convert_gps(row[gps_field])

                rows.append(row)

        return rows if is_activity else []

    # ------------------------------------------------------------------
    # Public extraction API
    # ------------------------------------------------------------------

    def extract_activity_session_records(
        self,
        activity_limit: int | None = None,
        max_files: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return one record per activity .fit file with session-level summary stats.

        Args:
            activity_limit: Stop after yielding this many activity sessions. Monitoring
                and other non-activity files don't count toward the limit.
            max_files: Hard cap on total files read (including non-activity files).
                Useful for fast test runs — set to a small number like 50.
        """
        results: list[dict[str, Any]] = []
        activities_found = 0
        for path in self._list_fit_files(max_files=max_files):
            if activity_limit is not None and activities_found >= activity_limit:
                break
            try:
                fit_bytes = self._read_fit_bytes(path)
                record = self._parse_session_from_fit(fit_bytes, path)
            except Exception as exc:
                # Skip corrupt or unreadable files without crashing the whole run
                results.append({"source_file": Path(path).name, "_parse_error": str(exc)})
                continue
            if record is not None:
                results.append(record)
                activities_found += 1
        return results

    def extract_activity_record_records(
        self,
        activity_limit: int | None = None,
        max_files: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return one row per data point (≈1 Hz) across activity .fit files.

        Args:
            activity_limit: Stop after processing this many activity files. Each
                activity typically contributes hundreds to thousands of rows.
                Strongly recommended — omitting this across 18k+ files can
                produce tens of millions of rows.
            max_files: Hard cap on total files read (including non-activity files).
                Useful for fast test runs — set to a small number like 50.
        """
        results: list[dict[str, Any]] = []
        activities_found = 0
        for path in self._list_fit_files(max_files=max_files):
            if activity_limit is not None and activities_found >= activity_limit:
                break
            try:
                fit_bytes = self._read_fit_bytes(path)
                rows = self._parse_records_from_fit(fit_bytes, path)
            except Exception as exc:
                results.append({"source_file": Path(path).name, "_parse_error": str(exc)})
                continue
            if rows:
                results.extend(rows)
                activities_found += 1
        return results

    def load_activity_session_dataframe(
        self, activity_limit: int | None = None, max_files: int | None = None
    ) -> "pd.DataFrame":
        """Load session-level records into a sorted pandas DataFrame."""
        return _records_to_dataframe(
            self.extract_activity_session_records(
                activity_limit=activity_limit, max_files=max_files
            ),
            timestamp_columns=ACTIVITY_FIT_SESSION_TIMESTAMP_COLUMNS,
            date_columns=ACTIVITY_FIT_SESSION_DATE_COLUMNS,
            sort_columns=ACTIVITY_FIT_SESSION_SORT_COLUMNS,
        )

    def load_activity_session_data(
        self, activity_limit: int | None = None, max_files: int | None = None
    ) -> tuple["pd.DataFrame", str]:
        """Load session-level records and return ``(DataFrame, JSON string)``."""
        records = self.extract_activity_session_records(
            activity_limit=activity_limit, max_files=max_files
        )
        dataframe = _records_to_dataframe(
            records,
            timestamp_columns=ACTIVITY_FIT_SESSION_TIMESTAMP_COLUMNS,
            date_columns=ACTIVITY_FIT_SESSION_DATE_COLUMNS,
            sort_columns=ACTIVITY_FIT_SESSION_SORT_COLUMNS,
        )
        return dataframe, _records_to_json(records)

    def load_activity_record_dataframe(
        self, activity_limit: int | None = None, max_files: int | None = None
    ) -> "pd.DataFrame":
        """Load time-series records into a sorted pandas DataFrame."""
        return _records_to_dataframe(
            self.extract_activity_record_records(
                activity_limit=activity_limit, max_files=max_files
            ),
            timestamp_columns=ACTIVITY_FIT_RECORD_TIMESTAMP_COLUMNS,
            date_columns=ACTIVITY_FIT_RECORD_DATE_COLUMNS,
            sort_columns=ACTIVITY_FIT_RECORD_SORT_COLUMNS,
        )

    def load_activity_record_data(
        self, activity_limit: int | None = None, max_files: int | None = None
    ) -> tuple["pd.DataFrame", str]:
        """Load time-series records and return ``(DataFrame, JSON string)``."""
        records = self.extract_activity_record_records(
            activity_limit=activity_limit, max_files=max_files
        )
        dataframe = _records_to_dataframe(
            records,
            timestamp_columns=ACTIVITY_FIT_RECORD_TIMESTAMP_COLUMNS,
            date_columns=ACTIVITY_FIT_RECORD_DATE_COLUMNS,
            sort_columns=ACTIVITY_FIT_RECORD_SORT_COLUMNS,
        )
        return dataframe, _records_to_json(records)


# ------------------------------------------------------------------
# Module-level helpers (shared logic, no class dependency)
# ------------------------------------------------------------------

def _records_to_dataframe(
    records: list[dict[str, Any]],
    *,
    timestamp_columns: tuple[str, ...],
    date_columns: tuple[str, ...],
    sort_columns: tuple[str, ...],
) -> "pd.DataFrame":
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas is required to load data into a dataframe. "
            "Install dependencies with `pip install -r backend/requirements.txt`."
        ) from exc

    dataframe = pd.DataFrame(records)

    for column in (*date_columns, *timestamp_columns):
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce", utc=True)

    present_sort = [c for c in sort_columns if c in dataframe.columns]
    if present_sort:
        dataframe = dataframe.sort_values(present_sort).reset_index(drop=True)
    else:
        dataframe = dataframe.reset_index(drop=True)

    return dataframe


def _records_to_json(records: list[dict[str, Any]], *, indent: int = 2) -> str:
    return json.dumps(records, indent=indent, default=str)


# ------------------------------------------------------------------
# Module-level convenience functions
# ------------------------------------------------------------------

def load_activity_session_dataframe(
    data_dir: str | Path = DEFAULT_ACTIVITY_FIT_DATA_DIR,
    activity_limit: int | None = None,
    max_files: int | None = None,
) -> "pd.DataFrame":
    return GarminFitExtractor(data_dir=data_dir).load_activity_session_dataframe(
        activity_limit=activity_limit, max_files=max_files
    )


def load_activity_record_dataframe(
    data_dir: str | Path = DEFAULT_ACTIVITY_FIT_DATA_DIR,
    activity_limit: int | None = None,
    max_files: int | None = None,
) -> "pd.DataFrame":
    return GarminFitExtractor(data_dir=data_dir).load_activity_record_dataframe(
        activity_limit=activity_limit, max_files=max_files
    )
