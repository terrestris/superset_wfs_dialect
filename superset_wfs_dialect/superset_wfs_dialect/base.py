import sqlglot.expressions
from owslib.wfs import WebFeatureService
from owslib.fes2 import *
from io import BytesIO
import sqlglot
import logging
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any, Tuple
import logging
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Connection:
    def __init__(self, base_url="https://localhost/geoserver/ows"):
        self.base_url = base_url
        self.wfs = WebFeatureService(url=base_url, version='2.0.0')

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

        # Get typename
        table_expr = ast.find(sqlglot.exp.Table)
        typename = table_expr.this.name if table_expr else None

        # Get property names
        self.requested_columns = [col.alias_or_name for col in ast.expressions]

        # Get Limit
        limit_expr = ast.find(sqlglot.exp.Limit)
        limit = int(limit_expr.expression.name) if limit_expr else None

        # Get Filter
        where_expr = ast.find(sqlglot.exp.Where)
        if where_expr:
            filter = self._get_filter_from_expression(where_expr)
            logger.debug("Filter: %s", filter)
        else:
            filter = None

        logger.info("Requesting WFS layer %s", typename)

        # TODO fix filter
        wfs = self.connection.wfs
        response: BytesIO = wfs.getfeature(
            typename=typename,
            maxfeatures=limit,
            propertyname=self.requested_columns,
            filter=filter,
        )

        try:
            xml_text = response.read().decode("utf-8")
            logger.debug("WFS response: %s", xml_text)
            self.data = self._parse_gml(xml_text)
        except ET.ParseError as e:
            logger.error("Fehler beim Parsen der WFS-Antwort: %s", e)
            raise ValueError("Fehler beim Parsen der WFS-Antwort")
        except OSError as e:
            logger.error("Fehler beim Lesen der WFS-Antwort: %s", e)
            raise ValueError("Fehler beim Lesen der WFS-Antwort")
        except Exception as e:
            logger.error("Fehler beim Parsen der WFS-Antwort: %s", e)
            raise

        self._generate_description()
        self._index = 0

    def _get_filter_from_expression(self, where_expr: sqlglot.exp.Where):

        if not isinstance(where_expr, sqlglot.exp.Where):
            raise ValueError("Ungültige WHERE-Klausel")

        filter = None

        # Handle equality
        if isinstance(where_expr.this, sqlglot.expressions.EQ):
            propertyname = where_expr.this.this.name
            literal = where_expr.this.args["expression"].name
            filter = PropertyIsEqualTo(propertyname=propertyname, literal=literal)

        #TODO handle other expressions

        return filter

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
