import requests
import math
import logging
import sqlglot
import sqlglot.expressions
import xml.etree.ElementTree as ET
import orjson
from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union

from owslib.fes2 import (
    And,
    Filter,
    Not,
    Or,
    PropertyIsEqualTo,
    PropertyIsGreaterThan,
    PropertyIsGreaterThanOrEqualTo,
    PropertyIsLessThan,
    PropertyIsLessThanOrEqualTo,
    PropertyIsLike,
    PropertyIsNotEqualTo,
    PropertyIsNull,
)
from owslib.feature.wfs200 import WebFeatureService_2_0_0
from owslib.util import openURL, Authentication

# from .wkt_parser import WKTParser
from .sql_logger import SQLLogger

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

GEOMETRY_COLUMN_NAME = "geom"


class Geometry(TypedDict):
    type: str
    coordinates: Union[List[float], List[List[float]]]


class Feature(TypedDict):
    type: str
    geometry: Geometry
    properties: dict


class FeatureCollection(TypedDict):
    type: str
    features: List[Feature]


class AggregationInfo(TypedDict):
    """
    Information about an aggregation to be performed.

    Attributes:
    class_: Any
        The aggregation class (e.g., sqlglot.expressions.Avg).
    propertyname: str
        The property name to aggregate on.
    alias: Optional[str]
        The alias for the aggregated value.
    groupby: str
        The property name to group by.
    """

    class_: Any
    propertyname: str
    alias: Optional[str]
    groupby: str


class Connection:
    def __init__(
        self,
        base_url="https://localhost/geoserver/ows",
        username=None,
        password=None,
        max_workers=5,
    ):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.feature_type_schemas = {}
        self.server_info = {}
        self.wfs_output_format = None
        self.max_workers = max_workers

        wfs_args = {"url": base_url, "version": "2.0.0"}

        if username and password:
            wfs_args["username"] = username
            wfs_args["password"] = password

        # Use the correct class for WFS 2.0.0 and only pass valid arguments
        self.wfs: WebFeatureService_2_0_0 = WebFeatureService_2_0_0(
            **wfs_args,
            parse_remote_metadata=False,
            timeout=30,
            # circumvent OWSLib auth issues
            auth=Authentication(),
        )

        self.wfs_output_format = self._get_output_format()

        # Initial DescribeFeatureType for all available layers
        self._cache_feature_type_schemas()

    def cursor(self):
        return Cursor(self)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def _get_output_format(self):
        """
        Determine the best available output format for WFS requests.
        Prefer application/json (GeoServer) or geojson (ArcGIS Server).
        :return: The preferred output format as a string.
        """
        if self.wfs.operations is None:
            return
        get_feature_op = next(
            (op for op in self.wfs.operations if op.name == "GetFeature"), None
        )

        preferred = None
        output_formats = []
        if get_feature_op:
            output_formats = get_feature_op.parameters["outputFormat"]["values"]
            for fmt in output_formats:
                fmt_lower = fmt.lower()
                if "application/json" in fmt_lower:
                    return "application/json"
                if "geojson" in fmt_lower and preferred is None:
                    preferred = "geojson"

        if preferred is None:
            logger.error(
                "No suitable output format found for WFS GetFeature request for server at %s",
                self.base_url,
            )
            logger.error("Output formats available: %s", output_formats)
            raise ValueError(
                "No suitable output format found for WFS GetFeature requests"
            )

        return preferred

    def _cache_feature_type_schemas(self):
        """
        Get schema of every feature type and store in a dictionary.
        """
        for feature_type in self.wfs.contents:
            schema = self.wfs.get_schema(typename=feature_type)
            self.feature_type_schemas[feature_type] = schema


class Cursor:
    def __init__(self, connection: Connection):
        self.connection = connection
        self.data: List[Any] = []
        # https://peps.python.org/pep-0249/#description
        self.description: Optional[
            List[Tuple[str, str, None, None, None, None, bool]]
        ] = None
        self._index: int = 0
        # Dict of { 'name': 'alias' } for requested columns
        self.requested_columns: Dict = {}
        self.typename: Optional[str] = None
        self.propertynames: List[str] = ["*"]
        self.sql_logger = SQLLogger()
        self.rowcount: Optional[int] = None

    def execute(self, operation: str, parameters: Optional[Dict] = None) -> None:
        """
        Executes a SQL operation (SELECT statement) against the WFS server.
        The results are stored in the cursor's data attribute.

        :param operation: The SQL operation to execute.
        :param parameters: Optional parameters for the SQL operation.
        :return: None
        """
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
        if not isinstance(ast, sqlglot.expressions.Select):
            raise ValueError("Only SELECT statements are supported for aggregation")
        aggregation_info = self._get_aggregationinfo(ast)

        if ast.args.get("distinct"):
            if len(self.propertynames) == 1:
                col = self.propertynames[0]
                alias = self.requested_columns.get(col, col)
                all_features = self._fetch_all_features(self.typename, filterXml)
                all_rows = [self._feature_to_row(feature) for feature in all_features]
                unique_values = sorted(
                    {str(r.get(col)) for r in all_rows if r.get(col) is not None}
                )
                self.data = [(v,) for v in unique_values]
                self.requested_columns = {alias: alias}
                self.rowcount = len(self.data)
                self.description = [
                    (alias, self._get_column_type(alias), None, None, None, None, True)
                ]
                self._index = 0
                return
            else:
                raise ValueError("DISTINCT is only supported for single column queries")

        logger.info("Requesting WFS layer %s", self.typename)

        all_features = self._fetch_all_features(self.typename, filterXml)
        all_rows = [self._feature_to_row(feature) for feature in all_features]
        aggregated_data = self._aggregate_rows(all_rows, aggregation_info)
        self._apply_limit(aggregated_data, limit)
        self._apply_order(ast, aggregated_data, aggregation_info)

        self.data = aggregated_data
        self.rowcount = len(self.data)
        self.description = self._generate_description()
        self._index = 0

    def _handle_dummy_query(self):
        self.data = [{"dummy": 1}]
        self.description = [("dummy", "int", None, None, None, None, True)]

    # Parse SQL using sqlglot
    def _parse_sql(self, operation: str):
        """
        Parses the SQL operation using sqlglot.

        :param operation: The SQL operation to parse.
        :return: The parsed SQL AST.
        """
        try:
            return sqlglot.parse_one(operation)
        except Exception as e:
            raise ValueError(f"Invalid SQL query: {e}")

    def _extract_typename(self, ast):
        """
        Extracts the WFS typename (layer) from the SQL AST.

        :param ast: The SQL AST.
        :return: The WFS typename.
        """
        if not isinstance(ast, sqlglot.expressions.Select):
            raise ValueError("Only SELECT statements are supported")

        # Get typename
        table_expr = ast.find(sqlglot.expressions.Table)
        return table_expr.this.name if table_expr else None

    def _extract_propertynames(self, ast):
        """Extracts property names from the SQL AST.
        Returns a list of property names.
        Returns an empty list if no properties are specified.

        :param ast: The SQL AST.
        :return: A list of property names.
        """
        propertynames = []

        # Get property names
        for col in ast.expressions:
            if isinstance(col.this, sqlglot.expressions.Column):
                name = col.this.name
            elif isinstance(col.this, sqlglot.expressions.AggFunc):
                name = col.this.this.name
            elif isinstance(col.this, sqlglot.expressions.Literal):
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
        """Extracts requested columns from the SQL AST.
        Returns a dictionary of { 'property_name': 'alias' }.
        Returns an empty dictionary if no columns are specified.

        :param ast: The SQL AST.
        :return: A dictionary of requested columns.
        """
        requested_columns = {}

        # Get property names
        for col in ast.expressions:
            # name should be the statement before "AS"
            name = (
                col.this.name
                if isinstance(col.this, sqlglot.expressions.Column)
                else str(col.this)
            )
            if not name:
                continue
            # alias should be the statement after "AS"
            alias = col.alias
            requested_columns[name] = alias if alias else name

        return requested_columns

    def _extract_limit(self, ast):
        """
        Extracts limit from the SQL AST.

        :param ast: The SQL AST.
        :return: The limit as an integer, or None if no limit is specified.
        """
        limit_expr = ast.find(sqlglot.expressions.Limit)
        if limit_expr:
            return int(limit_expr.args["expression"].this)
        return None

    def _extract_filter(self, ast):
        """
        Extracts filter from the SQL AST and converts it to WFS Filter XML.

        :param ast: The SQL AST.
        :return: The WFS Filter XML as a string.
        """
        where_expr = ast.find(sqlglot.expressions.Where)
        if where_expr:
            filter = self._get_filter_from_expression(where_expr.this)
            filterXml = ET.tostring(filter.toXML()).decode("utf-8")
            logger.debug("### WFS Filter XML:\n%s", filterXml)
            return filterXml
        return None

    def _feature_to_row(self, feature: Feature) -> dict:
        """
        Converts a WFS feature to a dictionary row.
        This is the expected return type for Superset.

        :param feature: The WFS feature to convert.
        :return: A dictionary representing the row.
        """
        props = feature.get("properties") or {}
        row = dict(props)
        row["id"] = feature.get("id")
        geom = feature.get("geometry")
        row[GEOMETRY_COLUMN_NAME] = orjson.dumps(geom).decode() if geom else None
        return row

    def _fetch_all_features(self, typename, filterXml) -> List[Feature]:
        """
        Fetches all features from the WFS server, handling pagination if necessary.
        Uses parallel requests to improve performance.

        :param typename: The WFS typename (layer) to fetch features from.
        :param filterXml: The WFS Filter XML to apply to the request.
        :return: A list of features.
        """
        # If we have an aggregation, we have to recursively call the WFS until all features are fetched
        # and then aggregate them in Python
        limit = 10000

        # fetch as many features as possible with one request
        server_side_maxfeatures = self._get_server_side_max_features(typename=typename)
        logger.debug("### Server-side maximum features: %s", server_side_maxfeatures)

        # Get the total number of features to calculate the number of requests needed
        total_features = self._get_feature_count(typename=typename, filterXml=filterXml)
        logger.debug("### Total features available: %s", total_features)

        if total_features == 0:
            return []

        if server_side_maxfeatures is not None:
            limit = server_side_maxfeatures
        else:
            if total_features / limit > 100:
                # reduce requests if there are too many features to never reach 100 requests
                limit = self._round_up_to_nearest_power(n=(total_features / 100))

        # Calculate the number of requests needed
        num_requests = math.ceil(total_features / limit) if limit > 0 else 1
        logger.debug("### Will make %s requests with limit %s", num_requests, limit)

        # If only one request is needed, fetch directly
        if num_requests == 1:
            logger.debug("Fetching all features in a single request")
            feature_collection = self._get_FeatureCollection(
                typename=typename,
                limit=limit,
                filterXml=filterXml,
                startindex=0,
            )
            return feature_collection.get("features", []) if feature_collection else []

        # Create a helper function for fetching a single page
        def fetch_page(start_idx):
            logger.info("Fetching features from %s to %s", start_idx, start_idx + limit)
            try:
                feature_collection = self._get_FeatureCollection(
                    typename=typename,
                    limit=limit,
                    filterXml=filterXml,
                    startindex=start_idx,
                )
                if feature_collection:
                    return (start_idx, feature_collection.get("features", []))
                return (start_idx, [])
            except Exception as e:
                logger.error("Error fetching features at index %s: %s", start_idx, e)
                return (start_idx, [])

        # Fetch all pages in parallel
        max_workers = self.connection.max_workers
        logger.debug("Using %s parallel workers for fetching features", max_workers)
        logger.debug("Fetching features for aggregation")

        # Create all startindex values
        startindexes = [i * limit for i in range(num_requests)]

        # Fetch pages in parallel, limiting concurrent requests to max_workers
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit only max_workers requests at a time
            future_to_startindex = {}
            startindex_iter = iter(startindexes)

            # Initially submit up to max_workers requests
            for _ in range(min(max_workers, len(startindexes))):
                idx = next(startindex_iter, None)
                if idx is not None:
                    future = executor.submit(fetch_page, idx)
                    future_to_startindex[future] = idx

            # As each request completes, submit the next one
            while future_to_startindex:
                for future in as_completed(future_to_startindex):
                    idx = future_to_startindex.pop(future)
                    start_idx, features = future.result()
                    if features:
                        results.append((start_idx, features))

                    # Submit the next request if there are any left
                    next_idx = next(startindex_iter, None)
                    if next_idx is not None:
                        new_future = executor.submit(fetch_page, next_idx)
                        future_to_startindex[new_future] = next_idx

                    # Break to refresh the as_completed iterator
                    break

        # Sort results by startindex to maintain order
        results.sort(key=lambda x: x[0])

        # Flatten the list of feature lists
        all_features = [
            feature for _, feature_list in results for feature in feature_list
        ]

        logger.debug("### Fetched %s features total", len(all_features))
        return all_features

    def _aggregate_rows(
        self, all_rows, aggregation_info: List[AggregationInfo]
    ) -> List[dict]:
        """
        Aggregates rows based on the provided aggregation information.

        :param all_rows: The list of all rows to aggregate.
        :param aggregation_info: The aggregation information.
        :return: The aggregated rows.
        """
        # If no aggregation is requested, return all rows
        if not aggregation_info:
            return all_rows

        group_by_property = aggregation_info[0]["groupby"]

        # check if all aggregations are for the same property, if not raise an error
        if not all(agg["groupby"] == group_by_property for agg in aggregation_info):
            raise ValueError("All aggregations must be for the same property")
        grouped_data = {}

        for row in all_rows:
            group_value = row.get(group_by_property)
            if group_value not in grouped_data:
                grouped_data[group_value] = []
            grouped_data[group_value].append(row)
        aggregated_data = []

        for group_value, rows in grouped_data.items():
            # Copy properties from the first row of the group, but keep the group value
            aggregated_row = dict(rows[0]) if rows else {}
            aggregated_row[group_by_property] = group_value
            aggregated_data.append(aggregated_row)

            for agg_info in aggregation_info:
                agg_class = agg_info["class_"]
                agg_prop = agg_info["propertyname"]
                agg_alias = agg_info.get("alias", None)

                aggregation_functions = {
                    sqlglot.expressions.Avg: lambda: sum(
                        f.get(agg_prop, 0) for f in rows
                    )
                    / len(rows),
                    sqlglot.expressions.Sum: lambda: sum(
                        f.get(agg_prop, 0) for f in rows
                    ),
                    sqlglot.expressions.Count: lambda: len(rows),
                    "count_distinct": lambda: len(
                        set(
                            f.get(agg_prop) for f in rows if f.get(agg_prop) is not None
                        )
                    ),
                    sqlglot.expressions.Max: lambda: max(
                        f.get(agg_prop, 0) for f in rows
                    ),
                    sqlglot.expressions.Min: lambda: min(
                        f.get(agg_prop, 0) for f in rows
                    ),
                }

                if agg_class not in aggregation_functions:
                    raise ValueError("Unsupported aggregation class")
                agg_value = aggregation_functions[agg_class]()
                aggregated_data[-1][agg_alias if agg_alias else agg_prop] = agg_value

        return aggregated_data

    def _apply_limit(self, data, row_limit):
        """
        Applies a row limit to the data. It modifies the data list in place.

        :param data: The data to apply the limit to.
        :param row_limit: The row limit to apply.
        :return: None
        """
        if row_limit is not None:
            data[:] = data[:row_limit]

    # ORDER BY may refer to a column or to an aggregated metric expression
    def _apply_order(self, ast, data, aggregation_info: List[AggregationInfo]):
        """
        Applies ordering to the data based on the SQL AST. It modifies the data list in place.

        :param ast: The SQL AST.
        :param data: The data to apply the ordering to.
        :param aggregation_info: The aggregation information.
        :return: None
        """
        order_expr = ast.args.get("order")
        if not order_expr:
            return
        for order in order_expr.expressions:
            # default: sort by column name
            order_col = None
            reverse = order.args.get("desc", False)

            # metric or column
            if hasattr(order.this, "name"):
                # column
                order_col = order.this.name
            elif hasattr(order.this, "sql") and hasattr(order.this, "args"):
                # aggregated metric
                metric_func = order.this.__class__.__name__.upper()
                metric_col = (
                    order.this.args.get("this").name
                    if order.this.args.get("this")
                    else None
                )

                agg_alias = None
                for agg in aggregation_info:
                    if (
                        agg["class_"].__name__.upper() == metric_func
                        and agg["propertyname"] == metric_col
                    ):
                        agg_alias = agg.get("alias") or metric_col
                        break
                order_col = agg_alias or f"{metric_func.lower()}_{metric_col}"
            else:
                order_col = str(order.this)

            # None vlaues will be set to the end (ASC) or beginning (DESC)
            def sort_key(row):
                val = row.get(order_col)
                # None values always last (ASC) or first (DESC)
                if val is None:
                    return (True, None)
                # Try to sort numerically if possible
                try:
                    return (False, float(val))
                except (TypeError, ValueError):
                    return (False, str(val))

            data.sort(key=sort_key, reverse=reverse)

    def _round_up_to_nearest_power(self, n) -> int:
        """
        Rounds up n to the nearest power of 1, 2, 5, or 10.
        :param n: The number to round up.
        :return: The rounded number.
        """
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
        """
        Gets the server-side maximum number of features for a given typename
        from the WFS GetCapabilities response.

        :param typename: The WFS typename (layer).
        :return: The maximum number of features as an integer, or None if not found.
        """
        base_url = self.connection.base_url

        # TODO: use self.connection.wfs.getcapabilities() !Does not support typename parameter!
        url = f"{base_url}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetCapabilities&typename={typename}"
        logger.debug("#### GetCapabilities URL used: %s", url)
        response = requests.get(url)
        if response.status_code == 200:
            try:
                capabilities_ast = ET.fromstring(response.text)
            except ET.ParseError as e:
                logger.error("Error when parsing the WFS response: %s", e)
                raise ValueError("Error when parsing the WFS response")

            # this is version 2.0.0 specific and could be different in 1.1.0
            count_default_element = capabilities_ast.find(
                ".//ows:Constraint[@name='CountDefault']/ows:DefaultValue",
                namespaces={"ows": "http://www.opengis.net/ows/1.1"},
            )
            if count_default_element is None:
                logger.error("Error when fetching the CountDefault elements")
                return 0

            count_default = (
                int(count_default_element.text) if count_default_element.text else 0
            )
            logger.info("Maximum number of features: %s", count_default)
            return count_default
        else:
            raise ValueError(
                f"Error when requesting the WFS capabilities: {response.status_code}"
            )

    def _get_feature_count(
        self,
        typename: str,
        filterXml: Optional[str] = None,
    ) -> int:
        """
        Gets the number of features for a given typename from the WFS server.

        :param typename: The WFS typename (layer).
        :param filterXml: Optional WFS Filter XML to apply to the request.
        :return: The number of features as an integer.
        """
        base_url = self.connection.base_url
        # TODO: use self.connection.wfs.getfeature() !Does not support resultType!
        url = f"{base_url}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typenames={typename}&resultType=hits"
        logger.debug("### Filter XML: %s", filterXml)
        logger.debug("#### URL: %s", url)
        response = None
        if filterXml:
            auth = None
            if self.connection.username and self.connection.password:
                auth = (self.connection.username, self.connection.password)
            response = requests.post(
                url,
                data=filterXml,
                headers={"Content-Type": "application/xml"},
                auth=auth,
            )
        else:
            auth = None
            if self.connection.username and self.connection.password:
                auth = (self.connection.username, self.connection.password)
            response = requests.get(url, auth=auth)

        if response.status_code == 200:
            try:
                count_ast = ET.fromstring(response.text)
            except ET.ParseError as e:
                logger.error("Error while parsing the WFS answer: %s", e)
                raise ValueError("Error while parsing the WFS answer")

            count = int(count_ast.attrib["numberMatched"])
            logger.info("Number of features: %s", count)
            return count

        else:
            raise ValueError(
                f"Error when requesting the number of features: {response.status_code}"
            )

    def _get_FeatureCollection(
        self,
        typename: str,
        limit: Optional[int] = None,
        filterXml: Optional[str] = None,
        startindex: Optional[int] = None,
    ) -> FeatureCollection:
        """
        Gets a FeatureCollection from the WFS server. Handles both GET and POST methods
        depending on whether a filterXml is provided.

        :param typename: The WFS typename (layer).
        :param limit: The maximum number of features to fetch.
        :param filterXml: Optional WFS Filter XML to apply to the request.
        :param startindex: The starting index for pagination.
        :return: The FeatureCollection as a dictionary.
        """
        wfs = self.connection.wfs

        propertyname = (
            None if filterXml and self.propertynames == ["*"] else self.propertynames
        )

        fiona_schema = wfs.get_schema(typename)
        if fiona_schema is not None and propertyname is not None:
            geometry_column = fiona_schema.get("geometry_column")
            propertyname = [
                geometry_column if x == GEOMETRY_COLUMN_NAME else x
                for x in propertyname
            ]

        params = {
            "typename": typename,
            "maxfeatures": limit,
            "startindex": startindex,
            "method": "POST" if filterXml else "GET",
            "outputFormat": self.connection.wfs_output_format or "application/json",
            "srsname": "EPSG:4326",
        }
        if filterXml:
            params["filter"] = filterXml
        else:
            params["propertyname"] = propertyname

        response = wfs.getfeature(**params)

        featuresString = response.read().decode("utf-8")
        return orjson.loads(featuresString)

    def _get_aggregationinfo(
        self, ast: sqlglot.expressions.Select
    ) -> List[AggregationInfo]:
        """
        Extracts aggregation information from the SQL AST.

        :param ast: The SQL AST.
        :return: A list of AggregationInfo dictionaries.
        """
        aggregation_classes = [
            sqlglot.expressions.Avg,
            sqlglot.expressions.Sum,
            sqlglot.expressions.Count,
            sqlglot.expressions.Max,
            sqlglot.expressions.Min,
        ]

        aggregation_info = []
        aggregation_class = None
        for cls in aggregation_classes:
            aggregation = ast.find_all(cls)

            for agg in aggregation:
                if isinstance(agg, sqlglot.expressions.Count) and isinstance(
                    agg.this, sqlglot.expressions.Distinct
                ):
                    aggregation_class = "count_distinct"
                else:
                    aggregation_class = cls
                if isinstance(agg.this, sqlglot.expressions.Column):
                    aggregation_property = agg.this.name

                elif isinstance(agg.this, sqlglot.expressions.Distinct):
                    expressions = agg.this.args.get("expressions", [])
                    if expressions and isinstance(
                        expressions[0], sqlglot.expressions.Column
                    ):
                        aggregation_property = expressions[0].name
                    else:
                        aggregation_property = None
                else:
                    aggregation_property = None

                if not agg.parent:
                    continue

                aggregation_alias = agg.parent.alias_or_name

                if not aggregation_class:
                    continue

                groupby = ast.find(sqlglot.expressions.Group)
                groupby_property = None
                if groupby:
                    column = groupby.find(sqlglot.expressions.Column)
                    if column:
                        groupby_property = column.this.name

                aggregation_info.append(
                    {
                        "class_": aggregation_class,
                        "propertyname": aggregation_property,
                        "alias": aggregation_alias,
                        "groupby": groupby_property,
                    }
                )
                break

        return aggregation_info

    def _get_filter_from_expression(self, expression, is_root: bool = True) -> Any:
        """
        Converts a sqlglot expression into an OWSLib filter.
        :param expression: The sqlglot expression to convert
        :param is_root: Whether this is the root filter (should be wrapped in a Filter object)
        :return: An OWSLib Filter or related filter expression object
        """
        supported_expressions = [
            sqlglot.expressions.EQ,
            sqlglot.expressions.NEQ,
            sqlglot.expressions.GT,
            sqlglot.expressions.GTE,
            sqlglot.expressions.LT,
            sqlglot.expressions.LTE,
            sqlglot.expressions.And,
            sqlglot.expressions.In,
            sqlglot.expressions.Not,
            sqlglot.expressions.Paren,
            sqlglot.expressions.Like,
            sqlglot.expressions.Is,
        ]

        if not isinstance(expression, tuple(supported_expressions)):
            raise ValueError(
                "Unsupported filter expression:", expression.__class__.__name__
            )

        featuretype_schema = self.connection.feature_type_schemas.get(self.typename)
        if not featuretype_schema:
            raise ValueError(
                "Could not retrieve feature type schema for typename:", self.typename
            )

        featuretype_geometry_name = featuretype_schema.get("geometry_column")
        if not featuretype_geometry_name:
            raise ValueError(
                "Could not determine geometry column for typename:", self.typename
            )

        propertyname = expression.this.name

        if propertyname == GEOMETRY_COLUMN_NAME:
            raise ValueError("Geometry filters are not supported")
            # TODO: when reenabling geometry filters, make sure to set
            # propertyname = featuretype_geometry_name

        filter = None
        # Handle parentheses
        if isinstance(expression, sqlglot.expressions.Paren):
            inner_expression = expression.this
            filter = self._get_filter_from_expression(inner_expression, is_root=False)
            return Filter(filter) if is_root else filter
        # Handle AND
        elif isinstance(expression, sqlglot.expressions.And):
            filter = And(
                [
                    self._get_filter_from_expression(expression.this, is_root=False),
                    self._get_filter_from_expression(
                        expression.args["expression"], is_root=False
                    ),
                ]
            )
        # Handle NOT
        elif isinstance(expression, sqlglot.expressions.Not):
            inner_expression = expression.this
            innerFilter = self._get_filter_from_expression(
                inner_expression, is_root=False
            )
            filter = Not([innerFilter])
        # Handle equality
        elif isinstance(expression, sqlglot.expressions.EQ):
            literal = expression.args["expression"].name
            filter = PropertyIsEqualTo(propertyname=propertyname, literal=literal)
        # Handle inequality
        elif isinstance(expression, sqlglot.expressions.NEQ):
            literal = expression.args["expression"].name
            filter = PropertyIsNotEqualTo(propertyname=propertyname, literal=literal)
        # Handle LIKE
        elif isinstance(expression, sqlglot.expressions.Like):
            matchcase = False
            like_propertyname = propertyname
            if isinstance(expression.this, sqlglot.expressions.Lower):
                # If the expression is a LOWER function, we need to extract the property name
                matchcase = False
                like_propertyname = expression.this.this.name
            literal = expression.args["expression"].name
            filter = PropertyIsLike(
                propertyname=like_propertyname, literal=literal, matchCase=matchcase
            )
        # Handle greater than
        elif isinstance(expression, sqlglot.expressions.GT):
            literal = expression.args["expression"].name
            filter = PropertyIsGreaterThan(propertyname=propertyname, literal=literal)
        # Handle greater than or equal
        elif isinstance(expression, sqlglot.expressions.GTE):
            literal = expression.args["expression"].name
            filter = PropertyIsGreaterThanOrEqualTo(
                propertyname=propertyname, literal=literal
            )
        # Handle less than
        elif isinstance(expression, sqlglot.expressions.LT):
            literal = expression.args["expression"].name
            filter = PropertyIsLessThan(propertyname=propertyname, literal=literal)
        # Handle less than or equal
        elif isinstance(expression, sqlglot.expressions.LTE):
            literal = expression.args["expression"].name
            filter = PropertyIsLessThanOrEqualTo(
                propertyname=propertyname, literal=literal
            )
        # Handle in
        elif isinstance(expression, sqlglot.expressions.In):
            literals = [lit.name for lit in expression.args["expressions"]]
            if len(literals) == 1:
                filter = PropertyIsEqualTo(
                    propertyname=propertyname, literal=literals[0]
                )
            else:
                # Combine multiple IN conditions with OR
                subfilters = [
                    PropertyIsEqualTo(propertyname=propertyname, literal=lit)
                    for lit in literals
                ]
                filter = Or(subfilters)
        # Handle IS NULL
        elif isinstance(expression, sqlglot.expressions.Is):
            check_expr = expression.args["expression"]
            if isinstance(check_expr, sqlglot.expressions.Null):
                filter = PropertyIsNull(propertyname=propertyname)
            else:
                raise ValueError("Unsupported IS expression")

        logger.debug("######## Filter: %s", filter)

        if not filter:
            raise ValueError("Unsupported filter expression")

        return Filter(filter) if is_root else filter

    def _generate_description(self):
        """
        Generates the column description in the correct order.

        :return: The column description as a list of tuples.
        """
        description = []

        if not self.data:
            description = []
            return

        if not self.requested_columns:
            # For SELECT * all columns in the order in which they appear
            description = [
                (col, self._get_column_type(col), None, None, None, None, True)
                for col in self.data[0].keys()
            ]
        else:
            # Otherwise only the requested columns in the correct order
            description = [
                (col, self._get_column_type(col), None, None, None, None, True)
                for col in self.requested_columns.values()
            ]

        return description

    # TODO: Implement a proper method to get the column type from the WFS schema
    def _get_column_type(self, column_name: str) -> str:
        """
        Returns the type of the column.

        :param column_name: The name of the column.
        :return: The type of the column as a string.
        """
        return "string"

    def _get_row_values(self, row: Any) -> tuple:
        """
        Returns the values in the correct order.

        :param row: The row to get the values from.
        :return: The row values as a tuple.
        """
        # If row is already a tuple (DISTINCT-Block), just return it
        if isinstance(row, tuple):
            return row
        if not self.requested_columns:
            # For SELECT * all columns in the order in which they appear
            return tuple(row.values())
        else:
            # Otherwise only the requested columns in the correct order
            return tuple(row.get(col) for col in self.requested_columns.values())

    def fetchall(self):
        """
        Fetches all rows from the cursor's data.

        :return: A list of all rows.
        """
        return [self._get_row_values(row) for row in self.data]

    def fetchone(self):
        """
        Fetches the next row from the cursor's data.

        :return: The next row as a tuple, or None if no more rows are available.
        """
        if self._index >= len(self.data):
            return None
        row = self._get_row_values(self.data[self._index])
        self._index += 1
        return row

    def fetchmany(self, size=1):
        """
        Fetches the next set of rows from the cursor's data.

        :param size: The number of rows to fetch.
        :return: A list of rows.
        """
        end = self._index + size
        rows = [self._get_row_values(row) for row in self.data[self._index : end]]
        self._index = min(end, len(self.data))
        return rows

    def close(self):
        pass


def connect(*args, **kwargs):
    base_url = kwargs.get("base_url", "https://localhost/geoserver/ows")
    username = kwargs.get("username")
    password = kwargs.get("password")
    return Connection(base_url=base_url, username=username, password=password)


class FakeDbApi:
    paramstyle = "pyformat"

    def connect(self, *args, **kwargs):
        return connect(*args, **kwargs)

    class Error(Exception):
        pass


dbapi = FakeDbApi()
