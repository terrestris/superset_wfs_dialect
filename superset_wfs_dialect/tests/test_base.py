import unittest
from unittest.mock import patch, MagicMock
from superset_wfs_dialect.base import Connection, Cursor
import sqlglot


class TestConnection(unittest.TestCase):
    @patch("superset_wfs_dialect.base.WebFeatureService")
    @patch("superset_wfs_dialect.base.requests.get")
    def test_connection_initialization(self, mock_requests, mock_wfs):
        mock_requests.return_value = MagicMock(status_code=200)
        mock_wfs.return_value = MagicMock()

        conn = Connection(
            base_url="https://example.com/geoserver/ows",
            username="user",
            password="pass",
        )

        self.assertEqual(conn.base_url, "https://example.com/geoserver/ows")
        self.assertEqual(conn.username, "user")
        self.assertEqual(conn.password, "pass")
        mock_wfs.assert_called_once_with(
            url="https://example.com/geoserver/ows",
            version="2.0.0",
            username="user",
            password="pass",
        )

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
        self.assertEqual(
            cursor.description, [("dummy", "int", None, None, None, None, True)]
        )

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


class TestApplyOrder(unittest.TestCase):
    def setUp(self):
        self.cursor = Cursor(MagicMock())

    def test_sort_by_column_asc(self):
        data = [
            {"name": "B"},
            {"name": "a"},
            {"name": "C"},
            {"name": None},
        ]

        class DummyOrder:
            def __init__(self, name, desc=False):
                self.this = MagicMock()
                self.this.name = name
                self.args = {"desc": desc}

        class DummyOrderExpr:
            expressions = [DummyOrder("name", desc=False)]

        ast = MagicMock()
        ast.args = {"order": DummyOrderExpr()}
        self.cursor._apply_order(ast, data, aggregation_info=[])
        self.assertEqual([row["name"] for row in data], ["B", "C", "a", None])

    def test_sort_by_column_desc(self):
        data = [
            {"name": "B"},
            {"name": "a"},
            {"name": "C"},
            {"name": None},
        ]

        class DummyOrder:
            def __init__(self, name, desc=True):
                self.this = MagicMock()
                self.this.name = name
                self.args = {"desc": desc}

        class DummyOrderExpr:
            expressions = [DummyOrder("name", desc=True)]

        ast = MagicMock()
        ast.args = {"order": DummyOrderExpr()}
        self.cursor._apply_order(ast, data, aggregation_info=[])
        self.assertEqual([row["name"] for row in data], [None, "a", "C", "B"])

    def test_sort_by_metric_alias(self):
        data = [
            {"gattung": "A", "AVG(baumhoehe)": 2},
            {"gattung": "B", "AVG(baumhoehe)": 1},
            {"gattung": "C", "AVG(baumhoehe)": 3},
        ]

        class DummyOrder:
            def __init__(self, name, desc=False):
                self.this = MagicMock()
                self.this.name = name
                self.args = {"desc": desc}

        class DummyOrderExpr:
            expressions = [DummyOrder("AVG(baumhoehe)", desc=False)]

        ast = MagicMock()
        ast.args = {"order": DummyOrderExpr()}
        aggregation_info = [
            {
                "class": MagicMock(__name__="Avg"),
                "propertyname": "baumhoehe",
                "alias": "AVG(baumhoehe)",
            }
        ]
        self.cursor._apply_order(ast, data, aggregation_info=aggregation_info)
        self.assertEqual([row["gattung"] for row in data], ["B", "A", "C"])

    def test_case_sensitive_sort(self):
        data = [
            {"name": "a"},
            {"name": "B"},
            {"name": "A"},
            {"name": "b"},
        ]

        class DummyOrder:
            def __init__(self, name, desc=False):
                self.this = MagicMock()
                self.this.name = name
                self.args = {"desc": desc}

        class DummyOrderExpr:
            expressions = [DummyOrder("name", desc=False)]

        ast = MagicMock()
        ast.args = {"order": DummyOrderExpr()}
        self.cursor._apply_order(ast, data, aggregation_info=[])
        self.assertEqual([row["name"] for row in data], ["A", "B", "a", "b"])

    def test_count_aggregation(self):
        cursor = Cursor(MagicMock())

        all_features = [
            {"group": "A", "type": "x"},
            {"group": "A", "type": "x"},
            {"group": "A", "type": "y"},
            {"group": "B", "type": "x"},
        ]

        aggregation_info = [
            {
                "class": sqlglot.exp.Count,
                "propertyname": "type",
                "alias": "COUNT(type)",
                "groupby": "group",
            }
        ]

        result = cursor._aggregate_features(all_features, aggregation_info)

        expected = [{"group": "A", "COUNT(type)": 3}, {"group": "B", "COUNT(type)": 1}]

        self.assertEqual(result, expected)

    def test_count_distinct_aggregation(self):
        cursor = Cursor(MagicMock())

        all_features = [
            {"group": "A", "type": "x"},
            {"group": "A", "type": "x"},
            {"group": "A", "type": "y"},
            {"group": "B", "type": "x"},
            {"group": "B", "type": "x"},
        ]

        aggregation_info = [
            {
                "class": "count_distinct",
                "propertyname": "type",
                "alias": "COUNT_DISTINCT(type)",
                "groupby": "group",
            }
        ]

        result = cursor._aggregate_features(all_features, aggregation_info)

        expected = [
            {"group": "A", "COUNT_DISTINCT(type)": 2},
            {"group": "B", "COUNT_DISTINCT(type)": 1},
        ]

        self.assertEqual(result, expected)

    def test_convert_value(self):
        cursor = Cursor(MagicMock())

        self.assertEqual(cursor._convert_value("123", "string"), "123")
        self.assertEqual(cursor._convert_value("some text", "string"), "some text")
        self.assertEqual(cursor._convert_value("123", "integer"), 123)
        self.assertEqual(cursor._convert_value("123", "int"), 123)
        self.assertEqual(cursor._convert_value("123", "short"), 123)
        self.assertEqual(cursor._convert_value("123", "byte"), 123)
        self.assertEqual(cursor._convert_value("123.45", "float"), 123.45)
        self.assertEqual(cursor._convert_value("123.45", "double"), 123.45)
        self.assertEqual(cursor._convert_value("123.45", "decimal"), 123.45)
        self.assertEqual(cursor._convert_value("3000000000", "long"), 3_000_000_000)
        self.assertEqual(cursor._convert_value("true", "boolean"), True)
        self.assertEqual(cursor._convert_value("false", "boolean"), False)
        self.assertEqual(cursor._convert_value("1", "boolean"), True)
        self.assertEqual(cursor._convert_value("0", "boolean"), False)
        self.assertEqual(cursor._convert_value("yes", "boolean"), True)
        self.assertEqual(cursor._convert_value("no", "boolean"), False)
        self.assertEqual(cursor._convert_value("2023-10-05", "date"), "2023-10-05")
        self.assertEqual(cursor._convert_value("2023", "dateTime"), "2023")
        self.assertIsNone(cursor._convert_value(None, "string"))
        # Test invalid integer conversion
        self.assertEqual(cursor._convert_value("abc", "integer"), "abc")


if __name__ == "__main__":
    unittest.main()
