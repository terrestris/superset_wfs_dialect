import unittest
from unittest.mock import patch, MagicMock, ANY
from superset_wfs_dialect.base import Connection, Cursor, AggregationInfo
from .conftest import create_mock_wfs_instance
import sqlglot
import sqlglot.expressions

from typing import List


class TestConnection(unittest.TestCase):
    @patch("superset_wfs_dialect.base.WebFeatureService_2_0_0")
    @patch("superset_wfs_dialect.base.requests.get")
    def test_connection_initialization(self, mock_requests, mock_wfs):
        mock_requests.return_value = MagicMock(status_code=200)
        mock_wfs.return_value = create_mock_wfs_instance()

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
            parse_remote_metadata=False,
            timeout=30,
            auth=ANY,
        )

    @patch("superset_wfs_dialect.base.WebFeatureService_2_0_0")
    def test_cursor(self, mock_wfs):
        mock_wfs.return_value = create_mock_wfs_instance()

        conn = Connection()
        cursor = conn.cursor()
        self.assertIsInstance(cursor, Cursor)


class TestCursor(unittest.TestCase):
    @patch("superset_wfs_dialect.base.WebFeatureService_2_0_0")
    @patch("superset_wfs_dialect.base.requests.get")
    def test_execute_dummy_query(self, mock_requests, mock_wfs):
        mock_requests.return_value = MagicMock(status_code=200)
        mock_wfs.return_value = create_mock_wfs_instance()

        conn = Connection()
        cursor = conn.cursor()

        cursor.execute("SELECT 1")

        self.assertEqual(cursor.data, [{"dummy": 1}])
        self.assertEqual(
            cursor.description, [("dummy", "int", None, None, None, None, True)]
        )

    @patch("superset_wfs_dialect.base.sqlglot.parse_one")
    @patch("superset_wfs_dialect.base.WebFeatureService_2_0_0")
    @patch("superset_wfs_dialect.base.requests.get")
    def test_execute_invalid_query(self, mock_requests, mock_wfs, mock_parse_one):
        mock_requests.return_value = MagicMock(status_code=200)
        mock_wfs.return_value = create_mock_wfs_instance()
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
        aggregation_info: List[AggregationInfo] = [
            {
                "class_": MagicMock(__name__="Avg"),
                "propertyname": "baumhoehe",
                "alias": "AVG(baumhoehe)",
                "groupby": "gattung",
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

        aggregation_info: List[AggregationInfo] = [
            {
                "class_": sqlglot.expressions.Count,
                "propertyname": "type",
                "alias": "COUNT(type)",
                "groupby": "group",
            }
        ]

        result = cursor._aggregate_rows(all_features, aggregation_info)

        expected = [
            {"group": "A", "type": "x", "COUNT(type)": 3},
            {"group": "B", "type": "x", "COUNT(type)": 1},
        ]

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

        aggregation_info: List[AggregationInfo] = [
            {
                "class_": "count_distinct",
                "propertyname": "type",
                "alias": "COUNT_DISTINCT(type)",
                "groupby": "group",
            }
        ]

        result = cursor._aggregate_rows(all_features, aggregation_info)

        expected = [
            {"group": "A", "type": "x", "COUNT_DISTINCT(type)": 2},
            {"group": "B", "type": "x", "COUNT_DISTINCT(type)": 1},
        ]

        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
