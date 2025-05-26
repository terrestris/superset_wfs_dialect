import requests
import math
import logging
import sqlglot
import sqlglot.expressions
from io import BytesIO
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any, Tuple
from owslib.wfs import WebFeatureService
from owslib.fes2 import *
from owslib.feature.wfs200 import WebFeatureService_2_0_0
from .gml_parser import GMLParser
from .sql_logger import SQLLogger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Connection:
    def __init__(self, base_url="https://localhost/geoserver/ows"):
        self.base_url = base_url
        self.wfs: WebFeatureService_2_0_0 = WebFeatureService(url=base_url, version='2.0.0')

    def cursor(self):
        return Cursor(self)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

class Cursor:
    def __init__(self, connection: Connection):
        self.connection = connection
        self.data: List[Dict[str, Any]] = []
        # https://peps.python.org/pep-0249/#description
        self.description: Optional[List[Tuple[str, str, None, None, None, None, bool]]] = None
        self._index: int = 0
        # Dict of { 'name': 'alias' } for requested columns
        self.requested_columns: Dict = {}
        self.typename: Optional[str] = None
        self.propertynames: List[str] = ['*']
        self.sql_logger = SQLLogger()
        self.rowcount: Optional[int] = None

    def execute(self, operation: str, parameters: Optional[Dict] = None) -> None:
        operation = operation.strip()

        self.sql_logger.log_sql(operation, parameters)

        if operation.lower() == "select 1":
            self._handle_dummy_query()
            return

        ast = self._parse_sql(operation)
        self.typename = self._extract_typename(ast)
        self.propertynames = self._extract_propertynames(ast)
        self.requested_columns = self._extract_requested_columns(ast)
        limit = self._extract_limit(ast)
        filterXml = self._extract_filter(ast)
        aggregation_info = self._get_aggregationinfo(ast)

        logger.info("Requesting WFS layer %s", self.typename)

        all_features = self._fetch_all_features(self.typename, filterXml)
        aggregated_data = self._aggregate_features(all_features, aggregation_info)
        self._apply_limit(aggregated_data, limit)
        # self._apply_order(ast, aggregated_data, aggregation_info)

        self.data = aggregated_data
        self.rowcount = len(self.data)
        self.description = self._generate_description()
        self._index = 0

    def _handle_dummy_query(self):
        self.data = [{"dummy": 1}]
        self.description = [("dummy", "int", None, None, None, None, True)]


    # Parse SQL using sqlglot
    def _parse_sql(self, operation: str):
        try:
            return sqlglot.parse_one(operation)
        except Exception as e:
            raise ValueError(f"Invalid SQL query: {e}")


    def _extract_typename(self, ast):
        if not isinstance(ast, sqlglot.expressions.Select):
            raise ValueError("Only SELECT statements are supported")

        from_expr = ast.args.get("from")
        if not from_expr:
            raise ValueError("FROM statement is required")

        # Get typename
        table_expr = ast.find(sqlglot.exp.Table)
        return table_expr.this.name if table_expr else None

    def _extract_propertynames(self, ast):
        '''Extracts property names from the SQL AST.
        Returns a list of property names.
        Returns an empty list if no properties are specified.
        '''
        propertynames = []

        # Get property names
        for col in ast.expressions:
            if isinstance(col.this, sqlglot.exp.Column):
                name = col.this.name
            elif isinstance(col.this, sqlglot.exp.AggFunc):
                name = col.this.this.name
            elif isinstance(col.this, sqlglot.exp.Literal):
                name = str(col.this)
            else:
                name = str(col.this)
            if not name:
                continue

            # add the name to the list if it is not already present
            if name not in propertynames:
                propertynames.append(name)

        return propertynames

    def _extract_requested_columns(self, ast):
        '''Extracts requested columns from the SQL AST.
        Returns a dictionary of { 'property_name': 'alias' }.
        Returns an empty dictionary if no columns are specified.
        '''
        requested_columns = {}

        # Get property names
        for col in ast.expressions:
            # name should be the statement before "AS"
            name = col.this.name if isinstance(col.this, sqlglot.exp.Column) else str(col.this)
            if not name:
                continue
            # alias should be the statement after "AS"
            alias = col.alias
            requested_columns[name] = alias if alias else name

        return requested_columns


    def _extract_limit(self, ast):
        # Get Limit
        limit_expr = ast.find(sqlglot.exp.Limit)
        if limit_expr:
            return int(limit_expr.args["expression"].this)
        return None


    def _extract_filter(self, ast):
        # Get Filter
        where_expr = ast.find(sqlglot.exp.Where)
        if where_expr:
            filter = self._get_filter_from_expression(where_expr.this)
            filterXml = ET.tostring(filter.toXML()).decode("utf-8")
            logger.debug("Filter: %s", filterXml)
            return filterXml
        return None

    def _fetch_all_features(self, typename, filterXml):
        # If we have an aggregation, we have to recursively call the WFS until all features are fetched
        # and then aggregate them in Python

        # fetch as many features as possible with one request
        server_side_maxfeatures = self._get_server_side_max_features(typename=typename)
        if server_side_maxfeatures is not None:
            limit = server_side_maxfeatures
        else:
            total_features = self._get_feature_count(typename=typename)
            if total_features / limit > 100:
                # reduce requests if there are too many features to never reach 100 requests
                limit = self._round_up_to_nearest_power(total_features / 100)

        startindex = 0
        all_features = []
        logger.info("Fetching features for aggregation")
        while True:
            # Fetch features with pagination
            logger.info("Fetching features from %s to %s", startindex, startindex + limit)
            features = self._get_features(
                typename=typename,
                limit=limit,
                filterXml=filterXml,
                startindex=startindex
            )
            if not features:
                break
            all_features.extend(features)
            startindex += len(features)
        return all_features

    def _aggregate_features(self, all_features, aggregation_info):
        # If no aggregation is requested, return all features
        if not aggregation_info:
            return all_features

        group_by_prperty = aggregation_info[0]["groupby"]

        # check if all aggregations are for the same property, if not raise an error
        if not all(agg["groupby"] == group_by_prperty for agg in aggregation_info):
            raise ValueError("All aggregations must be for the same property")
        grouped_data = {}

        for feature in all_features:
            group_value = feature.get(group_by_prperty)
            if group_value not in grouped_data:
                grouped_data[group_value] = []
            grouped_data[group_value].append(feature)
        aggregated_data = []

        for group_value, features in grouped_data.items():
            aggregated_data.append({group_by_prperty: group_value})

            for agg_info in aggregation_info:
                agg_class = agg_info["class"]
                agg_prop = agg_info["propertyname"]
                agg_alias = agg_info.get("alias", None)

                if agg_class == sqlglot.exp.Avg:
                    agg_value = sum(float(f.get(agg_prop, 0)) for f in features) / len(features)
                elif agg_class == sqlglot.exp.Sum:
                    agg_value = sum(float(f.get(agg_prop, 0)) for f in features)
                elif agg_class == sqlglot.exp.Count:
                    agg_value = len(features)
                elif agg_class == sqlglot.exp.Max:
                    agg_value = max(float(f.get(agg_prop, 0)) for f in features)
                elif agg_class == sqlglot.exp.Min:
                    agg_value = min(float(f.get(agg_prop, 0)) for f in features)
                else:
                    raise ValueError("Unsupported aggregation class")
                aggregated_data[-1][agg_alias if agg_alias else agg_prop] = agg_value

        return aggregated_data

    def _apply_limit(self, data, row_limit):
        if row_limit is not None:
            data[:] = data[:row_limit]

    # ORDER BY may refer to a column or to an aggregated metric expression,
    # and thus requires more context to resolve correctly
    # TODO: Implement proper ordering support based on expression analysis.
    def _apply_order(self, ast, data, aggregation_info):
        order_expr = ast.args.get("order")
        if order_expr:
            for order in order_expr.expressions:
                order_col = order.this.name
                reverse = order.args.get("desc", False)
                # This assumes order_col is a plain column name, which is not always true.
                # Also, ordering by metric expressions is currently unsupported.
                data.sort(key=lambda row: row.get(order_col), reverse=reverse)

    def _round_up_to_nearest_power(n):
        base = 10 ** math.floor(math.log10(n))
        if n <= base:
            return base
        elif n <= 2 * base:
            return 2 * base
        elif n <= 5 * base:
            return 5 * base
        else:
            return 10 * base

    def _get_server_side_max_features(self, typename: str) -> int:
        base_url = self.connection.base_url
        url = f"{base_url}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetCapabilities&typename={typename}"
        requests.get(url)
        response = requests.get(url)
        if response.status_code == 200:
            try:
                capabilities_ast = ET.fromstring(response.text)
            except ET.ParseError as e:
                logger.error("Fehler beim Parsen der WFS-Antwort: %s", e)
                raise ValueError("Fehler beim Parsen der WFS-Antwort")

            # this is version 2.0.0 specific and could be different in 1.1.0
            count_default_element = capabilities_ast.find(".//ows:Constraint[@name='CountDefault']/ows:DefaultValue", namespaces={"ows": "http://www.opengis.net/ows/1.1"})
            if count_default_element is None:
                logger.info("Error when fetching the CountDefault elements")
                return None

            count_default = int(count_default_element.text)
            logger.info("Maximum number of features: %s", count_default)
            return count_default

    def _get_feature_count(
        self,
        typename: str,
        filterXml: Optional[str] = None,
    ) -> int:
        base_url = self.connection.base_url
        url = f"{base_url}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename={typename}&resultType=hits"

        response = None
        if filterXml:
            response = requests.post(
                url,
                data=filterXml,
                headers={"Content-Type": "application/xml"}
            )
        else:
            response = requests.get(url)

        if response.status_code == 200:
            try:
                count_ast = ET.fromstring(response.text)
            except ET.ParseError as e:
                logger.error("Error while parsing the WFS answer: %s", e)
                raise ValueError("Error while parsing the WFS answer")

            count = int(count_ast.attrib['numberMatched'])
            logger.info("Number of features: %s", count)
            return count

        else:
            raise ValueError(f"Error when requesting the number of features: {response.status_code}")


    def _get_features(
        self,
        typename: str,
        limit: Optional[int] = None,
        filterXml: Optional[str] = None,
        startindex: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        wfs = self.connection.wfs

        propertyname = None if filterXml and self.propertynames == ["*"] else self.propertynames

        response: BytesIO = wfs.getfeature(
            typename=typename,
            maxfeatures=limit,
            propertyname=propertyname,
            filter=filterXml,
            startindex=startindex,
            method='POST' if filterXml else 'GET'
        )

        gmlparser = GMLParser(
            geometry_column=self.connection.wfs.get_schema(self.typename).get("geometry_column")
        )

        try:
            xml_text = response.read().decode("utf-8")
            logger.debug("WFS response: %s", xml_text)
            return gmlparser.parse(xml_text)
        except ET.ParseError as e:
            logger.error("Error parsing the WFS response: %s", e)
            raise ValueError("Error parsing the WFS response")
        except OSError as e:
            logger.error("Error when reading the WFS response: %s", e)
            raise ValueError("Error when reading the WFS response")
        except Exception as e:
            logger.error("Error parsing the WFS response: %s", e)
            raise

    def _get_aggregationinfo(self, ast: sqlglot.expressions.Select) -> List[Dict[str, Any]]:
        aggregation_classes = [
            sqlglot.exp.Avg,
            sqlglot.exp.Sum,
            sqlglot.exp.Count,
            # handle CountDistinct
            sqlglot.exp.Max,
            sqlglot.exp.Min
        ]

        aggregation_info = []
        aggregation_class = None
        for cls in aggregation_classes:
            aggregation = ast.find_all(cls)

            for agg in aggregation:
                aggregation_class = cls
                aggregation_property = agg.this.name
                aggregation_alias = agg.parent.alias_or_name

                if not aggregation_class:
                    continue

                groupby = ast.find(sqlglot.exp.Group)
                if groupby:
                    groupby_property = groupby.find(sqlglot.exp.Column).this.name

                aggregation_info.append({
                    "class": aggregation_class,
                    "propertyname": aggregation_property,
                    "alias": aggregation_alias,
                    "groupby": groupby_property
                })
                break

        return aggregation_info

    def _get_filter_from_expression(self, expression, is_root: bool = True) -> str:
        supported_expressions = [
            sqlglot.expressions.EQ,
            sqlglot.expressions.NEQ,
            sqlglot.expressions.GT,
            sqlglot.expressions.GTE,
            sqlglot.expressions.LT,
            sqlglot.expressions.LTE,
            sqlglot.expressions.And,
            # sqlglot.expressions.In,
            # sqlglot.expressions.Not,
        ]

        if not isinstance(expression, tuple(supported_expressions)):
            raise ValueError("Unsupported filter expression:", expression.__class__.__name__)

        filter = None

        # Handle AND
        if isinstance(expression, sqlglot.expressions.And):
            filter = And([
                self._get_filter_from_expression(expression.this, is_root=False),
                self._get_filter_from_expression(expression.args["expression"], is_root=False)
            ])
        # Handle equality
        elif isinstance(expression, sqlglot.expressions.EQ):
            propertyname = expression.this.name
            literal = expression.args["expression"].name
            filter = PropertyIsEqualTo(propertyname=propertyname, literal=literal)
        # Handle inequality
        elif isinstance(expression, sqlglot.expressions.NEQ):
            propertyname = expression.this.name
            literal = expression.args["expression"].name
            filter = PropertyIsNotEqualTo(propertyname=propertyname, literal=literal)
        # Handle greater than
        elif isinstance(expression, sqlglot.expressions.GT):
            propertyname = expression.this.name
            literal = expression.args["expression"].name
            filter = PropertyIsGreaterThan(propertyname=propertyname, literal=literal)
        # Handle greater than or equal
        elif isinstance(expression, sqlglot.expressions.GTE):
            propertyname = expression.this.name
            literal = expression.args["expression"].name
            filter = PropertyIsGreaterThanOrEqualTo(propertyname=propertyname, literal=literal)
        # Handle less than
        elif isinstance(expression, sqlglot.expressions.LT):
            propertyname = expression.this.name
            literal = expression.args["expression"].name
            filter = PropertyIsLessThan(propertyname=propertyname, literal=literal)
        # Handle less than or equal
        elif isinstance(expression, sqlglot.expressions.LTE):
            propertyname = expression.this.name
            literal = expression.args["expression"].name
            filter = PropertyIsLessThanOrEqualTo(propertyname=propertyname, literal=literal)

        if not filter:
            raise ValueError("Unsupported filter expression")

        return Filter(filter) if is_root else filter

    def _generate_description(self):
        """Generates the column description in the correct order."""
        description = []

        if not self.data:
            description = []
            return

        if not self.requested_columns:
            # For SELECT * all columns in the order in which they appear
            description = [
                (col, self._get_column_type(col), None, None, None, None, True) for col in self.data[0].keys()
            ]
        else:
            # Otherwise only the requested columns in the correct order
            description = [
                (col, self._get_column_type(col), None, None, None, None, True) for col in self.requested_columns.values()
            ]

        return description

    # TODO: Implement a proper method to get the column type from the WFS schema
    def _get_column_type(self, column_name: str) -> str:
        """Returns the type of the column."""
        return "string"

    def _get_row_values(self, row: Dict[str, Any]) -> tuple:
        """Returns the values in the correct order."""
        if not self.requested_columns:
            # For SELECT * all columns in the order in which they appear
            return tuple(row.values())
        else:
            # Otherwise only the requested columns in the correct order
            return tuple(row.get(col) for col in self.requested_columns.values())

    def fetchall(self):
        return [self._get_row_values(row) for row in self.data]

    def fetchone(self):
        if self._index >= len(self.data):
            return None
        row = self._get_row_values(self.data[self._index])
        self._index += 1
        return row

    def fetchmany(self, size=1):
        end = self._index + size
        rows = [self._get_row_values(row) for row in self.data[self._index:end]]
        self._index = min(end, len(self.data))
        return rows

    def close(self):
        pass

def connect(*args, **kwargs):
    base_url = kwargs.get("base_url", "https://localhost/geoserver/ows")
    print(args, kwargs)
    return Connection(base_url)

class FakeDbApi:
    paramstyle = "pyformat"

    def connect(self, *args, **kwargs):
        return connect(*args, **kwargs)

    class Error(Exception):
        pass

dbapi = FakeDbApi()
