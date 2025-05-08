import requests
import warnings
import re
import xml.etree.ElementTree as ET
from .wfs_client import WfsQueryBuilder

class Connection:
    def __init__(self, base_url="https://localhost/geoserver/ows"):
        self.base_url = base_url

    def cursor(self):
        return Cursor(self.base_url)

    def close(self):
        pass

class Cursor:
    def __init__(self, base_url):
        self.builder = WfsQueryBuilder(base_url, version="2.0.0", prefer_json=False)
        self.data = []
        self.description = None
        self._index = 0

    def execute(self, operation, parameters=None):
        operation = operation.strip()
        match = re.match(r"SELECT \* FROM ([\w:]+)(?: LIMIT (\d+))?", operation, re.IGNORECASE)

        if not match:
            raise ValueError("Nur 'SELECT * FROM layer [LIMIT n]' wird aktuell unterstützt.")

        layer_name = match.group(1)
        limit = match.group(2)
        limit = int(limit) if limit else None

        url = self.builder.build_getfeature_url(layer_name, max_features=limit)
        print("Generierte WFS-URL:", url)
        response = requests.get(url, verify=False)

        if response.status_code != 200:
            raise RuntimeError(f"Fehler beim Abrufen von WFS-Daten: {response.status_code}")

        try:
            geojson = response.json()
            self.data = [feature["properties"] for feature in geojson.get("features", [])]
        except Exception:
            self.data = self._parse_gml(response.text)

        self._generate_description()
        self._index = 0

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
        return self.data

    def fetchone(self):
        if self._index >= len(self.data):
            return None
        row = self.data[self._index]
        self._index += 1
        return row

    def fetchmany(self, size=1):
        end = self._index + size
        rows = self.data[self._index:end]
        self._index = min(end, len(self.data))
        return rows

    def close(self):
        pass

def connect(*args, **kwargs):
    base_url = kwargs.get("base_url", "https://localhost/geoserver/ows")
    return Connection(base_url)

connect.paramstyle = "qmark"