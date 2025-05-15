import sqlglot.expressions
from owslib.wfs import WebFeatureService
from owslib.fes2 import *
from owslib.feature.wfs200 import WebFeatureService_2_0_0
from io import BytesIO
import sqlglot
import logging
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any, Tuple
import logging

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

    def rollback(self):
        pass

class Cursor:
    def __init__(self, connection: Connection):
        self.connection = connection
        self.data: List[Dict[str, Any]] = []
        self.description: Optional[List[Tuple[str, str, None, None, None, None, bool]]] = None
        self._index: int = 0
        self.requested_columns: List[str] = ["*"]

    def execute(self, operation: str, parameters: Optional[Dict] = None) -> None:
        operation = operation.strip()

        if operation.lower() == "select 1":
            self.data = [{"dummy": 1}]
            self.description = [("dummy", "int", None, None, None, None, True)]
            return

        # Parse SQL using sqlglot
        try:
            ast = sqlglot.parse_one(operation)
        except Exception as e:
            raise ValueError(f"Ungültige SQL-Anfrage: {e}")

        if not isinstance(ast, sqlglot.expressions.Select):
            raise ValueError("Nur SELECT-Anfragen werden unterstützt")

        from_expr = ast.args.get("from")
        if not from_expr:
            raise ValueError("FROM-Klausel fehlt")

        aggregation_info = self._get_aggregationinfo(ast)

        # Get typename
        table_expr = ast.find(sqlglot.exp.Table)
        typename = table_expr.this.name if table_expr else None

        # Get property names
        self.requested_columns = [col.alias_or_name for col in ast.expressions]

        # strip propname from aggregation wrapper AVG(propname), COUNT(propname), ...
        self.requested_columns = [
            col.split("(")[-1].split(")")[0] if "(" in col else col
            for col in self.requested_columns
        ]

        # Get Limit
        limit_expr = ast.find(sqlglot.exp.Limit)
        limit = int(limit_expr.expression.name) if limit_expr else None

        # Get Filter
        where_expr = ast.find(sqlglot.exp.Where)
        if where_expr:
            filter = self._get_filter_from_expression(where_expr.this)
            filterXml = ET.tostring(filter.toXML()).decode("utf-8")
            logger.debug("Filter: %s", filterXml)
        else:
            filterXml = None

        logger.info("Requesting WFS layer %s", typename)

        # If we have no aggregation, we can fetch the features directly
        if not aggregation_info:
            self.data = self._get_features(
                typename=typename,
                limit=limit,
                filterXml=filterXml
            )
            self._generate_description()
            self._index = 0
            return

        # If we have an aggregation, we have to recursively call the WFS until all features are fetched
        # and then aggregate them in Python
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

        # Perform aggregation in Python
        agg_class = aggregation_info["class"]
        agg_prop = aggregation_info["propertyname"]
        agg_groupby = aggregation_info["groupby"]
        grouped_data = {}

        for feature in all_features:
            group_value = feature.get(agg_groupby)
            if group_value not in grouped_data:
                grouped_data[group_value] = []
            grouped_data[group_value].append(feature)
        aggregated_data = []

        for group_value, features in grouped_data.items():
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
            aggregated_data.append({agg_groupby: group_value, agg_prop: agg_value})

        self.data = aggregated_data
        self._generate_description()
        self._index = 0

    def _get_features(
        self,
        typename: str,
        limit: Optional[int] = None,
        filterXml: Optional[str] = None,
        startindex: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        wfs = self.connection.wfs
        response: BytesIO = wfs.getfeature(
            typename=typename,
            maxfeatures=limit,
            propertyname=self.requested_columns,
            filter=filterXml,
            startindex=startindex,
            method='POST' if filterXml else 'GET'
        )

        try:
            xml_text = response.read().decode("utf-8")
            logger.debug("WFS response: %s", xml_text)
            return self._parse_gml(xml_text)
        except ET.ParseError as e:
            logger.error("Fehler beim Parsen der WFS-Antwort: %s", e)
            raise ValueError("Fehler beim Parsen der WFS-Antwort")
        except OSError as e:
            logger.error("Fehler beim Lesen der WFS-Antwort: %s", e)
            raise ValueError("Fehler beim Lesen der WFS-Antwort")
        except Exception as e:
            logger.error("Fehler beim Parsen der WFS-Antwort: %s", e)
            raise

    def _get_aggregationinfo(self, ast):
        aggregation_classes = [
            sqlglot.exp.Avg,
            sqlglot.exp.Sum,
            sqlglot.exp.Count,
            # handle CountDistinct
            sqlglot.exp.Max,
            sqlglot.exp.Min
        ]

        aggregation_class = None
        aggregation_property = None

        for cls in aggregation_classes:
            aggregation = ast.find(cls)
            if aggregation:
                aggregation_class = cls
                aggregation_property = aggregation.this.name
                break

        if not aggregation_class:
            return None

        groupby_property = ast.find(sqlglot.exp.Group).find(sqlglot.exp.Column).this.name

        if not groupby_property:
            raise ValueError("Aggregation ohne GROUP BY ist nicht unterstützt")

        aggregation_info = {
            "class": aggregation_class,
            "propertyname": aggregation_property,
            "groupby": groupby_property
        }

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

    def _parse_gml(self, xml_text: str) -> List[Dict[str, str]]:
        """Parse GML XML response into a list of feature dictionaries.

        Args:
            xml_text: The GML XML text to parse

        Returns:
            A list of dictionaries containing the feature properties
        """
        ns = {
            "wfs": "http://www.opengis.net/wfs/2.0",
            "gml": "http://www.opengis.net/gml/3.2",
        }
        root = ET.fromstring(xml_text)
        members = root.findall(".//wfs:member", ns)
        features = []

        for member in members:
            feature_elem = next(iter(member), None)
            if feature_elem is None:
                continue
            props = {}
            for elem in feature_elem:
                tag = elem.tag.split("}")[-1]
                if elem.text and elem.text.strip():
                    props[tag] = elem.text.strip()
            if props:
                features.append(props)
        return features

    def _generate_description(self):
        """Generiert die Spaltenbeschreibung in der richtigen Reihenfolge."""
        if not self.data:
            self.description = []
            return

        if self.requested_columns == ["*"]:
            # Bei SELECT * alle Spalten in der Reihenfolge wie sie kommen
            self.description = [
                (col, "string", None, None, None, None, True) for col in self.data[0].keys()
            ]
        else:
            # Sonst nur die angefragten Spalten in der richtigen Reihenfolge
            self.description = [
                (col, "string", None, None, None, None, True) for col in self.requested_columns
            ]

    def _get_row_values(self, row: Dict[str, Any]) -> tuple:
        """Gibt die Werte in der richtigen Reihenfolge zurück."""
        if self.requested_columns == ["*"]:
            # Bei SELECT * alle Werte in der Reihenfolge wie sie kommen
            return tuple(row.values())
        else:
            # Sonst nur die angefragten Spalten in der richtigen Reihenfolge
            return tuple(row.get(col) for col in self.requested_columns)

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
