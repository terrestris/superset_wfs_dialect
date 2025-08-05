import logging
from owslib.fes2 import Equals
from owslib.gml import Point
from pyproj import CRS
import re

from .gml_geoms import MultiLineString, MultiPoint, Polygon, LineString, MultiPolygon

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WKTParser:
    """
    A class to convert WKT geometries into GML filter fragments.
    """

    def __init__(self):
        self.x_index = 0
        self.y_index = 1
        self._srid = None

    def _parse_wkt_string(self, wkt: str) -> str:
        """Extract SRID and geometry string from WKT"""
        if not wkt.startswith("SRID="):
            raise ValueError(f"Unsupported geometry format: {wkt}")
        srid_part, geom_part = wkt.split(";", 1)
        self._srid = srid_part.split("=")[1]
        logger.debug(
            f"### Parsed SRID: {self._srid}, Geometry part: {geom_part}")
        return geom_part

    def _set_axis_order(self):
        """Set the axis order"""
        try:
            crs = CRS.from_epsg(int(self._srid))
            if crs.axis_info[0].direction == "north":
                self.x_index = 1
                self.y_index = 0
            else:
                self.x_index = 0
                self.y_index = 1
        except Exception:
            logger.warning(
                f"Cannot identify axis order of CRS '{self._srid}'. Defaulting to 'easting'."
            )
            self.x_index = 0
            self.y_index = 1

    def _parse_point(self, coords_text: str) -> tuple[str, str]:
        """
        Parse POINT coordinates and return an ordered list of the point.
        """
        coords = coords_text.strip().split()

        x = coords[self.x_index]
        y = coords[self.y_index]
        return x, y

    def _parse_multipoint(self, coords_text: str) -> list[str]:
        """
        Parse MULTIPOINT and return list of points,
        using _parse_point() for each point.
        """
        coords_text = coords_text.strip()

        if coords_text.startswith("(") and coords_text.endswith(")"):
            coords_text = coords_text[1:-1].strip()

        point_blocks = re.split(r"\s*\),\s*\(", coords_text)

        result = []
        for block in point_blocks:
            clean = block.strip("() ")
            x, y = self._parse_point(clean)
            result.append((x, y))

        return result

    def _parse_polygon(self, coords_text: str) -> str:
        """
        Parse POLYGON coordinates return an ordered list of the polygon.
        """
        coords = [pair.strip().split() for pair in coords_text.split(",")]

        reordered_coords = [
            f"{pair[self.x_index]} {pair[self.y_index]}" for pair in coords
        ]
        return " ".join(reordered_coords)

    def _parse_multipolygon(self, coords_text: str) -> list[str]:
        """
        Parse MULTIPOLYGON coordinates and return an ordered list of polygons,
        using _parse_polygon() for each polygon.
        """
        coords_text = coords_text.strip()

        if coords_text.startswith("(") and coords_text.endswith(")"):
            coords_text = coords_text[1:-1].strip()

        polygon_blocks = coords_text.split(")), ((")
        poslists = []

        for block in polygon_blocks:
            clean_block = block.strip("() ")
            coords = [pair.strip().split() for pair in clean_block.split(",")]
            reordered_coords = [
                f"{pair[self.x_index]} {pair[self.y_index]}" for pair in coords
            ]
            poslists.append(" ".join(reordered_coords))

        return poslists

    def _parse_linestring(self, coords_text: str) -> str:
        """
        Parse LINESTRING WKT and returns an ordered list of line positions.
        """
        coords = [pair.strip().split() for pair in coords_text.split(",")]

        poslist = " ".join([
            f"{pair[self.x_index]} {pair[self.y_index]}"
            for pair in coords
        ])
        return poslist

    def _parse_multilinestring(self, coords_text: str) -> list[str]:
        """
        Parse MULTILINESTRING and return a list of posList strings,
        using _parse_linestring() for each line.
        """
        coords_text = coords_text.strip()

        if coords_text.startswith("(") and coords_text.endswith(")"):
            coords_text = coords_text[1:-1].strip()

        line_blocks = re.split(r"\s*\),\s*\(", coords_text)

        return [self._parse_linestring(block.strip("() ")) for block in line_blocks]

    def parse(self, propertyname: str, wkt: str) -> Equals:
        """
        Convert WKT to a wrapped GML Equals filter

        Args:
            propertyname: The geometry column name
            wkt: The full WKT string

        Returns:
            Equals filter
        """
        geom_part = self._parse_wkt_string(wkt)
        self._set_axis_order()

        if geom_part.startswith("POINT("):
            coords_text = geom_part[6:-1].strip()
            x, y = self._parse_point(coords_text)

            point = Point(
                id=None,
                srsName=f"urn:ogc:def:crs:EPSG::{self._srid}",
                pos=(x, y)
            )

            return Equals(propertyname, point)

        elif geom_part.startswith("MULTIPOINT(("):
            coords_text = geom_part[11:-2].strip()
            pointlist = self._parse_multipoint(coords_text)

            multipoint = MultiPoint(
                id=None,
                srsName=f"urn:ogc:def:crs:EPSG::{self._srid}",
                points=pointlist
            )

            return Equals(propertyname, multipoint)

        elif geom_part.startswith("POLYGON(("):
            coords_text = geom_part[9:-2].strip()
            coords = self._parse_polygon(coords_text)

            polygon = Polygon(
                id=None,
                srsName=f"urn:ogc:def:crs:EPSG::{self._srid}",
                exterior=coords,
            )
            return Equals(propertyname, polygon)

        elif geom_part.startswith("MULTIPOLYGON((("):
            coords_text = geom_part[14:-3].strip()
            coordslists = self._parse_multipolygon(coords_text)

            return Equals(
                propertyname,
                MultiPolygon(
                    id=None,
                    srsName=f"urn:ogc:def:crs:EPSG::{self._srid}",
                    polygons=coordslists
                )
            )

        elif geom_part.startswith("LINESTRING("):
            coords_text = geom_part[11:-1].strip()
            coords = self._parse_linestring(coords_text)

            linestring = LineString(
                id=None,
                srsName=f"urn:ogc:def:crs:EPSG::{self._srid}",
                poslist=coords,
            )
            return Equals(propertyname, linestring)

        elif geom_part.startswith("MULTILINESTRING(("):
            coords_text = geom_part[17:-2].strip()
            coordslists = self._parse_multilinestring(coords_text)

            multilinestring = MultiLineString(
                id=None,
                srsName=f"urn:ogc:def:crs:EPSG::{self._srid}",
                lines=coordslists,
            )
            return Equals(propertyname, multilinestring)

        else:
            raise NotImplementedError("Geometry is not supported")
