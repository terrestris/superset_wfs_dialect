from typing import List, Dict
import xml.etree.ElementTree as ET

class GMLParser:
    """
    A class to parse GML (Geography Markup Language) data.
    """

    def __init__(self, geometry_column: str):
        """
        Initialize the GMLParser with the geometry column name.

        Args:
            geometry_column: The name of the geometry column
        """
        self._geometry_column = geometry_column

    def parse(self, xml_text: str) -> List[Dict[str, str]]:
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

              if tag == self._geometry_column:
                  gml_elem = list(elem)[0]
                  props[tag] = self._gml_to_wkt(gml_elem)
              elif elem.text and elem.text.strip():
                  props[tag] = elem.text.strip()
          if props:
              features.append(props)
      return features

    def _parse_coords(self, pos_text: str) -> str:
        """Parse coordinate pairs from a space-separated string."""
        coords = pos_text.strip().split()
        coord_pairs = []
        for i in range(0, len(coords), 2):
            coord_pairs.append(f"{coords[i]} {coords[i+1]}")
        return ", ".join(coord_pairs)

    def _parse_point(self, elem):
        """Parse a GML Point element into WKT format."""
        pos = elem.find('./gml:pos', {'gml': 'http://www.opengis.net/gml/3.2'})
        if pos is not None:
            coords = pos.text.strip()
            return f"POINT({coords})"
        raise ValueError("Invalid Point format")

    def _parse_linestring(self, elem):
        """Parse a GML LineString element into WKT format."""
        pos_list = elem.find('./gml:posList', {'gml': 'http://www.opengis.net/gml/3.2'})
        if pos_list is not None:
            coords = self._parse_coords(pos_list.text)
            return f"LINESTRING({coords})"
        raise ValueError("Invalid LineString format")

    def _parse_polygon(self, elem):
        """Parse a GML Polygon element into WKT format."""
        ns = {'gml': 'http://www.opengis.net/gml/3.2'}
        rings = []

        # Parse exterior ring
        exterior = elem.find('./gml:exterior/gml:LinearRing/gml:posList', ns)
        if exterior is not None:
            rings.append(self._parse_coords(exterior.text))

        # Parse interior rings
        for interior in elem.findall('./gml:interior/gml:LinearRing/gml:posList', ns):
            rings.append(self._parse_coords(interior.text))

        return f"POLYGON(({'), ('.join(rings)}))"

    def _parse_multipoint(self, elem):
        """Parse a GML MultiPoint element into WKT format."""
        ns = {'gml': 'http://www.opengis.net/gml/3.2'}
        points = []
        for point in elem.findall('.//gml:Point/gml:pos', ns):
            points.append(f"({point.text.strip()})")
        return f"MULTIPOINT({', '.join(points)})"

    def _parse_multilinestring(self, elem):
        """Parse a GML MultiCurve element into WKT format."""
        ns = {'gml': 'http://www.opengis.net/gml/3.2'}
        lines = []
        for line in elem.findall('.//gml:LineString/gml:posList', ns):
            lines.append(f"({self._parse_coords(line.text)})")
        return f"MULTILINESTRING({', '.join(lines)})"

    def _parse_multipolygon(self, elem):
        """Parse a GML MultiSurface element into WKT format."""
        ns = {'gml': 'http://www.opengis.net/gml/3.2'}
        polygons = []
        for polygon in elem.findall('.//gml:Polygon', ns):
            # Remove the POLYGON() wrapper from _parse_polygon result but keep the inner brackets
            wkt = self._parse_polygon(polygon)[7:]
            polygons.append(wkt)
        return f"MULTIPOLYGON({', '.join(polygons)})"

    def _gml_to_wkt(self, gml_elem) -> str:
        """Convert a GML geometry element to WKT format.

        Args:
            gml_elem: XML element containing GML geometry

        Returns:
            WKT representation of the geometry including SRID
        """
        # Extract SRID from srsName attribute
        srid = None
        srs_name = gml_elem.get('srsName')
        if srs_name:
            # Handle format like "urn:ogc:def:crs:EPSG::25833"
            srid = srs_name.split(':')[-1]

        # Get geometry type from tag name
        tag = gml_elem.tag.split('}')[-1]

        # Map GML tags to parsing functions
        parse_funcs = {
            'Point': self._parse_point,
            'LineString': self._parse_linestring,
            'Polygon': self._parse_polygon,
            'MultiPoint': self._parse_multipoint,
            'MultiCurve': self._parse_multilinestring,
            'MultiSurface': self._parse_multipolygon
        }

        if tag not in parse_funcs:
            raise ValueError(f"Unsupported geometry type: {tag}")

        wkt = parse_funcs[tag](gml_elem)

        if srid:
            return f"SRID={srid};{wkt}"
        return wkt
