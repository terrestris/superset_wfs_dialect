import unittest
from unittest.mock import patch, mock_open
from superset_wfs_dialect.sql_logger import SQLLogger

class TestSQLLogger(unittest.TestCase):

    @patch("os.environ.get", return_value="false")
    @patch("builtins.open", new_callable=mock_open)
    def test_logging_disabled(self, mock_open, mock_env):
        logger = SQLLogger()
        logger.log_sql("SELECT * FROM table")
        mock_open.assert_not_called()

    @patch("os.environ.get", return_value="true")
    @patch("builtins.open", new_callable=mock_open)
    def test_logging_enabled(self, mock_open, mock_env):
        logger = SQLLogger()
        logger.log_sql("SELECT * FROM table")
        mock_open.assert_called_once_with("/app/sql_log.csv", "a", newline="", encoding="utf-8")

    @patch("os.environ.get", return_value="true")
    @patch("builtins.open", new_callable=mock_open)
    def test_logging_with_parameters(self, mock_open, mock_env):
        logger = SQLLogger()
        logger.log_sql("SELECT * FROM table", {"param1": "value1", "param2": "value2"})
        handle = mock_open()
        handle.write.assert_called_once()
        self.assertIn("param1=value1,param2=value2", handle.write.call_args[0][0])

    @patch("os.environ.get", return_value="true")
    @patch("builtins.open", new_callable=mock_open)
    def test_logging_with_none_parameters(self, mock_open, mock_env):
        logger = SQLLogger()
        logger.log_sql("SELECT * FROM table", None)
        handle = mock_open()
        handle.write.assert_called_once()
        self.assertIn("---", handle.write.call_args[0][0])
        
if __name__ == "__main__":
    unittest.main()
    