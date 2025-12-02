import unittest
from unittest.mock import MagicMock, patch
from superset_wfs_dialect.base import Cursor, Connection
import sqlglot
from owslib.fes2 import (
    And,
    Filter,
    Not,
    Or,
    PropertyIsEqualTo,
    PropertyIsGreaterThan,
    PropertyIsLessThan,
    PropertyIsLike,
    PropertyIsNotEqualTo,
)


class TestGetFilterFromExpression(unittest.TestCase):
    def setUp(self):
        self.patcher_wfs = patch("superset_wfs_dialect.base.WebFeatureService_2_0_0")
        self.patcher_requests = patch("superset_wfs_dialect.base.requests.get")

        mock_wfs = self.patcher_wfs.start()
        mock_requests = self.patcher_requests.start()

        mock_requests.return_value = MagicMock(status_code=200)
        mock_wfs.return_value = MagicMock()

        self.connection = Connection(
            base_url="https://example.com/geoserver/ows",
            username="user",
            password="pass",
        )
        self.cursor = Cursor(self.connection)

    def tearDown(self):
        self.patcher_wfs.stop()
        self.patcher_requests.stop()

    def test_equality_filter(self):
        expression = sqlglot.parse_one("column = 'value'")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, PropertyIsEqualTo)
        self.assertEqual(filter_result.filter.propertyname, "column")
        self.assertEqual(filter_result.filter.literal, "value")

    def test_inequality_filter(self):
        expression = sqlglot.parse_one("column != 'value'")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, PropertyIsNotEqualTo)
        self.assertEqual(filter_result.filter.propertyname, "column")
        self.assertEqual(filter_result.filter.literal, "value")

    def test_greater_than_filter(self):
        expression = sqlglot.parse_one("column > 10")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, PropertyIsGreaterThan)
        self.assertEqual(filter_result.filter.propertyname, "column")
        self.assertEqual(filter_result.filter.literal, "10")

    def test_less_than_filter(self):
        expression = sqlglot.parse_one("column < 10")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, PropertyIsLessThan)
        self.assertEqual(filter_result.filter.propertyname, "column")
        self.assertEqual(filter_result.filter.literal, "10")

    def test_in_filter(self):
        expression = sqlglot.parse_one("column IN ('value1', 'value2')")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, Or)
        subfilters = filter_result.filter.operations
        self.assertEqual(len(subfilters), 2)
        self.assertIsInstance(subfilters[0], PropertyIsEqualTo)
        self.assertEqual(subfilters[0].propertyname, "column")
        self.assertEqual(subfilters[0].literal, "value1")
        self.assertIsInstance(subfilters[1], PropertyIsEqualTo)
        self.assertEqual(subfilters[1].propertyname, "column")
        self.assertEqual(subfilters[1].literal, "value2")

    def test_like_filter_case_sensitive(self):
        expression = sqlglot.parse_one("column LIKE 'value%'")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, PropertyIsLike)
        self.assertEqual(filter_result.filter.propertyname, "column")
        self.assertEqual(filter_result.filter.literal, "value%")
        self.assertFalse(filter_result.filter.matchCase)

    def test_like_filter_case_insensitive(self):
        expression = sqlglot.parse_one("LOWER(column) LIKE 'value%'")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, PropertyIsLike)
        self.assertEqual(filter_result.filter.propertyname, "column")
        self.assertEqual(filter_result.filter.literal, "value%")
        self.assertFalse(filter_result.filter.matchCase)

    def test_and_filter(self):
        expression = sqlglot.parse_one("column1 = 'value1' AND column2 = 'value2'")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, And)
        self.assertEqual(len(filter_result.filter.operations), 2)
        self.assertIsInstance(filter_result.filter.operations[0], PropertyIsEqualTo)
        self.assertEqual(filter_result.filter.operations[0].propertyname, "column1")
        self.assertEqual(filter_result.filter.operations[0].literal, "value1")
        self.assertIsInstance(filter_result.filter.operations[1], PropertyIsEqualTo)
        self.assertEqual(filter_result.filter.operations[1].propertyname, "column2")
        self.assertEqual(filter_result.filter.operations[1].literal, "value2")

    def test_not_filter(self):
        expression = sqlglot.parse_one("NOT column = 'value'")
        filter_result = self.cursor._get_filter_from_expression(expression)
        self.assertIsInstance(filter_result, Filter)
        self.assertIsInstance(filter_result.filter, Not)
        self.assertEqual(len(filter_result.filter.operations), 1)
        self.assertIsInstance(filter_result.filter.operations[0], PropertyIsEqualTo)
        self.assertEqual(filter_result.filter.operations[0].propertyname, "column")
        self.assertEqual(filter_result.filter.operations[0].literal, "value")


if __name__ == "__main__":
    unittest.main()
