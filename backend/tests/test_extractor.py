from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from garmin_data_extraction import GarminDataExtractor


class ParseFileDateRangeTests(unittest.TestCase):
    def test_parse_sleep_filename_dates(self) -> None:
        start_date, end_date = GarminDataExtractor._parse_file_date_range(
            Path("2025-12-23_2026-04-02_91745541_sleepData.json")
        )

        self.assertEqual(start_date, "2025-12-23")
        self.assertEqual(end_date, "2026-04-02")

    def test_parse_hydration_filename_dates(self) -> None:
        start_date, end_date = GarminDataExtractor._parse_file_date_range(
            Path("HydrationLogFile_2025-12-21_2026-03-31.json")
        )

        self.assertEqual(start_date, "2025-12-21")
        self.assertEqual(end_date, "2026-03-31")

    def test_parse_activity_vo2_max_filename_dates(self) -> None:
        start_date, end_date = GarminDataExtractor._parse_file_date_range(
            Path("ActivityVo2Max_20201228_20210407_91745541.json")
        )

        self.assertEqual(start_date, "2020-12-28")
        self.assertEqual(end_date, "2021-04-07")


class ActivityVo2MaxExtractionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temp_dir = tempfile.TemporaryDirectory()
        cls.data_dir = Path(cls._temp_dir.name)
        source_dir = Path("data/activity_vo2_max")

        for source_file in source_dir.glob("ActivityVo2Max_*.json"):
            shutil.copy2(source_file, cls.data_dir / source_file.name)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temp_dir.cleanup()

    def test_extract_activity_vo2_max_records_adds_metadata_and_flattens(self) -> None:
        extractor = GarminDataExtractor(data_dir=self.data_dir)

        records = extractor.extract_activity_vo2_max_records()

        self.assertGreater(len(records), 0)
        first_record = records[0]
        self.assertIn("activityUuid_uuid", first_record)
        self.assertEqual(first_record["file_start_date"], "2020-12-28")
        self.assertEqual(first_record["file_end_date"], "2021-04-07")
        self.assertTrue(first_record["source_file"].startswith("ActivityVo2Max_"))

    def test_load_activity_vo2_max_dataframe_parses_and_sorts_columns(self) -> None:
        extractor = GarminDataExtractor(data_dir=self.data_dir)

        dataframe = extractor.load_activity_vo2_max_dataframe()

        self.assertGreater(len(dataframe), 0)
        self.assertEqual(str(dataframe["calendarDate"].dtype), "datetime64[ns]")
        self.assertEqual(str(dataframe["file_start_date"].dtype), "datetime64[ns]")
        self.assertEqual(str(dataframe["file_end_date"].dtype), "datetime64[ns]")
        self.assertTrue(str(dataframe["timestampGmt"].dtype).startswith("datetime64"))
        sorted_frame = dataframe.sort_values(["calendarDate", "timestampGmt"]).reset_index(
            drop=True
        )
        self.assertTrue(dataframe.equals(sorted_frame))


if __name__ == "__main__":
    unittest.main()
