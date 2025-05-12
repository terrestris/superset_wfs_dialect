import requests
import warnings
import re
import xml.etree.ElementTree as ET
from .wfs_client import WfsQueryBuilder
from owslib.wfs import WebFeatureService

class Connection:
    def __init__(self, base_url="https://localhost/geoserver/ows"):
        self.base_url = base_url
        self.wfs = WebFeatureService(url=base_url, version='2.0.0')

    def cursor(self):
        return Cursor(self.base_url)

    def close(self):
        pass

    def rollback(self):
        pass

class Cursor:
    def __init__(self, base_url):
        # self.builder = WfsQueryBuilder(base_url, version="2.0.0")
        self.data = []
        self.description = None
        self._index = 0

    # TODO use sqlpaser
    def execute(self, operation, parameters=None):
        import re
        import requests

        operation = operation.strip()

        if operation.lower() == "select 1":
            self.data = [{"dummy": 1}]
            self.description = [("dummy", "int", None, None, None, None, True)]
            return

        match = re.match(
            r'SELECT\s+(?P<columns>.+?)\s+FROM\s+(?P<schema>\w+)\."(?P<table>\w+)"(?:\s+GROUP BY\s+(?P<group>.+?))?(?:\s+LIMIT\s+(?P<limit>\d+))?',
            operation,
            re.IGNORECASE,
        )

        if not match:
            raise ValueError(f"Nur 'SELECT * FROM schema.\"table\" [LIMIT n]' wird aktuell unterstützt. (Erhalten: {operation})")

        schema = match.group("schema")
        table = match.group("table")
        columns_raw = match.group("columns")
        limit = int(match.group("limit")) if match.group("limit") else None

        # Spalten extrahieren
        columns = [c.strip('" ') for c in columns_raw.split(",")]

        property_names = None if columns == ["*"] else columns

        typename = f"{schema}:{table}"
        url = self.builder.build_getfeature_url(typename, max_features=limit, property_names=property_names)
        print("Generierte WFS-URL:", url)

        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"Fehler beim Abrufen von WFS-Daten: {response.status_code}")

        try:
            geojson = response.json()
            self.data = [feature["properties"] for feature in geojson.get("features", [])]
        except Exception:
            self.data = self._parse_gml(response.text)

        self._generate_description()
        self._index = 0


    # TODO use gml/xml paser
    def _parse_gml(self, xml_text):
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
        if self.data:
            first_row = self.data[0]
            self.description = [
                (key, type(value).__name__, None, None, None, None, True)
                for key, value in first_row.items()
            ]
        else:
            self.description = []

    def fetchall(self):
        return [tuple(row.values()) for row in self.data]

    def fetchone(self):
        if self._index >= len(self.data):
            return None
        row = tuple(self.data[self._index].values())
        self._index += 1
        return row

    def fetchmany(self, size=1):
        end = self._index + size
        rows = [tuple(row.values()) for row in self.data[self._index:end]]
        self._index = min(end, len(self.data))
        return rows

    def close(self):
        pass

def connect(*args, **kwargs):
    base_url = kwargs.get("base_url", "https://localhost/geoserver/ows")
    print(args, kwargs)
    return Connection(base_url)

class FakeDbApi:
    paramstyle = "pyformat"  # oder "qmark", falls du lieber ? als Platzhalter verwendest

    def connect(self, *args, **kwargs):
        return connect(*args, **kwargs)
    
    class Error(Exception):
        pass

dbapi = FakeDbApi()