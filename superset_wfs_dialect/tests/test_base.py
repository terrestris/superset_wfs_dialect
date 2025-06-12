import unittest
from unittest.mock import patch, MagicMock
from superset_wfs_dialect.base import Connection, Cursor

class TestConnection(unittest.TestCase):
    @patch("superset_wfs_dialect.base.WebFeatureService")
    @patch("superset_wfs_dialect.base.requests.get")
    def test_connection_initialization(self, mock_requests, mock_wfs):
        mock_requests.return_value = MagicMock(status_code=200)
        mock_wfs.return_value = MagicMock()

        conn = Connection(base_url="https://example.com/geoserver/ows", username="user", password="pass")

        self.assertEqual(conn.base_url, "https://example.com/geoserver/ows")
        self.assertEqual(conn.username, "user")
        self.assertEqual(conn.password, "pass")
        mock_wfs.assert_called_once_with(url="https://example.com/geoserver/ows", version="2.0.0", username="user", password="pass")

    @patch("superset_wfs_dialect.base.WebFeatureService")
    def test_cursor(self, mock_wfs):
        mock_wfs.return_value = MagicMock()

        conn = Connection()
        cursor = conn.cursor()
        self.assertIsInstance(cursor, Cursor)

class TestCursor(unittest.TestCase):
    @patch("superset_wfs_dialect.base.WebFeatureService")
    @patch("superset_wfs_dialect.base.requests.get")
    def test_execute_dummy_query(self, mock_requests, mock_wfs):
        mock_requests.return_value = MagicMock(status_code=200)
        mock_wfs.return_value = MagicMock()

        conn = Connection()
        cursor = conn.cursor()

        cursor.execute("SELECT 1")

        self.assertEqual(cursor.data, [{"dummy": 1}])
        self.assertEqual(cursor.description, [("dummy", "int", None, None, None, None, True)])

    @patch("superset_wfs_dialect.base.sqlglot.parse_one")
    @patch("superset_wfs_dialect.base.WebFeatureService")
    @patch("superset_wfs_dialect.base.requests.get")
    def test_execute_invalid_query(self, mock_requests, mock_wfs, mock_parse_one):
        mock_requests.return_value = MagicMock(status_code=200)
        mock_wfs.return_value = MagicMock()

        mock_parse_one.side_effect = ValueError("Invalid SQL query")

        conn = Connection()
        cursor = conn.cursor()

        with self.assertRaises(ValueError) as context:
            cursor.execute("INVALID SQL")

        self.assertIn("Invalid SQL query", str(context.exception))

if __name__ == "__main__":
    unittest.main()

