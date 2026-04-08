from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from services import duckdb_service


class DuckDbServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.duckdb"
        self.db_path.touch()
        self.db_patch = patch("services.duckdb_service.DB_PATH", self.db_path)
        self.db_patch.start()
        duckdb_service._conn = None
        duckdb_service._conn_mtime = 0.0

    def tearDown(self) -> None:
        self.db_patch.stop()
        if duckdb_service._conn is not None:
            duckdb_service._conn.close()
            duckdb_service._conn = None
        self.temp_dir.cleanup()

    @patch("services.duckdb_service.duckdb.connect")
    def test_get_conn_opens_database_once_with_default_configuration(self, mock_connect: Mock) -> None:
        fake_conn = Mock()
        mock_connect.return_value = fake_conn

        conn = duckdb_service._get_conn()

        self.assertIs(conn, fake_conn)
        mock_connect.assert_called_once_with(str(self.db_path))


if __name__ == "__main__":
    unittest.main()
