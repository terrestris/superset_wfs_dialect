import unittest
from unittest.mock import MagicMock
from superset_wfs_dialect.dialect import WfsDialect
from sqlalchemy.types import Integer, String

class TestWfsDialect(unittest.TestCase):
    def setUp(self):
        self.dialect = WfsDialect()

    def test_create_connect_args(self):
        url = MagicMock()
        url.host = "example.com"
        url.port = 8080
        url.database = "testdb"
        url.username = "user"
        url.password = "pass"

        expected_args = (
            [],
            {
                "base_url": "https://example.com:8080/testdb",
                "username": "user",
                "password": "pass",
            },
        )
        self.assertEqual(self.dialect.create_connect_args(url), expected_args)

    def test_get_schema_names(self):
        connection = MagicMock()
        result = self.dialect.get_schema_names(connection)
        self.assertEqual(result, ["default"])

    def test_has_table(self):
        connection = MagicMock()
        table_name = "test_table"
        schema = "test_schema"
        result = self.dialect.has_table(connection, table_name, schema)
        self.assertTrue(result)

    def test_get_table_names(self):
        connection = MagicMock()
        wfs_mock = MagicMock()
        wfs_mock.contents.keys.return_value = ["layer1", "layer2"]
        connection.connection.connection.wfs = wfs_mock

        result = self.dialect.get_table_names(connection)
        self.assertEqual(result, ["layer1", "layer2"])

    def test_get_columns(self):
        connection = MagicMock()
        wfs_mock = MagicMock()
        wfs_mock.get_schema.return_value = {
            "properties": {"id": "int", "name": "string", "age": "integer"},
            "required": ["id", "age"],
            "geometry_column": "geom",
        }
        connection.connection.connection.wfs = wfs_mock

        expected_columns = [
                {"name": "id", "type": Integer, "nullable": False, "default": None},
                {"name": "name", "type": String, "nullable": True, "default": None},
                {"name": "age", "type": Integer, "nullable": False, "default": None},
                {"name": "geom", "type": String, "nullable": True, "default": None},
        ]

        result = self.dialect.get_columns(connection, "test_table")
        for expected, actual in zip(expected_columns, result):
            self.assertEqual(expected["name"], actual["name"])
            self.assertEqual(expected["nullable"], actual["nullable"])
            self.assertEqual(expected["default"], actual["default"])
            self.assertEqual(expected["type"], type(actual["type"]))

if __name__ == "__main__":
    unittest.main()
