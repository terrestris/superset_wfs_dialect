from dataclasses import dataclass, field
from lxml import etree
from owslib.gml import AbstractGeometryType, Point
from owslib.util import nspath_eval

from .namespaces import NAMESPACES


def prefix(tag: str) -> str:
    return nspath_eval(tag, NAMESPACES)

@dataclass
class MultiPoint(AbstractGeometryType):
    points: list[tuple[str, str]] = field(default_factory=list)

    def toXML(self):
        node = etree.Element(prefix("gml:MultiPoint"))

        if self.srsName:
            node.set("srsName", self.srsName)
        if self.id:
            node.set("id", self.id)

        for x, y in self.points:
            member = etree.SubElement(node, prefix("gml:pointMember"))

            point = Point(pos=(x, y), id=None)
            point.srsName = self.srsName
            member.append(point.toXML())

        return node

@dataclass
class Polygon(AbstractGeometryType):
    exterior: str = ""

    def toXML(self):
        node = etree.Element(prefix("gml:Polygon"))

        if self.srsName:
            node.set("srsName", self.srsName)
        if self.id:
            node.set("id", self.id)

        exterior_el = etree.SubElement(node, prefix("gml:exterior"))
        ring = etree.SubElement(exterior_el, prefix("gml:LinearRing"))
        pos_list = etree.SubElement(ring, prefix("gml:posList"))
        pos_list.text = self.exterior

        return node


@dataclass
class MultiPolygon(AbstractGeometryType):
    polygons: list[str] = field(default_factory=list)

    def toXML(self):
        node = etree.Element(prefix("gml:MultiSurface"))

        if self.srsName:
            node.set("srsName", self.srsName)
        if self.id:
            node.set("id", self.id)

        for idx, poslist in enumerate(self.polygons, start=1):
            member = etree.SubElement(node, prefix("gml:surfaceMember"))

            poly = Polygon(
                id=f"{self.id}.geom.{idx}" if self.id else None,
                srsName=self.srsName,
                exterior=poslist.strip()
            )
            member.append(poly.toXML())

        return node


@dataclass
class LineString(AbstractGeometryType):
    poslist: str = ""

    def toXML(self):
        node = etree.Element(prefix("gml:LineString"))

        if self.srsName:
            node.set("srsName", self.srsName)
        if self.id:
            node.set("id", self.id)

        pos_list = etree.SubElement(node, prefix("gml:posList"))
        pos_list.text = self.poslist

        return node


@dataclass
class MultiLineString(AbstractGeometryType):
    lines: list[str] = field(default_factory=list)

    def toXML(self):
        node = etree.Element(prefix("gml:MultiCurve"))

        if self.srsName:
            node.set("srsName", self.srsName)
        if self.id:
            node.set("id", self.id)

        for idx, line_poslist in enumerate(self.lines, start=1):
            member = etree.SubElement(node, prefix("gml:curveMember"))

            line = LineString(
                id=None,
                srsName=self.srsName,
                poslist=line_poslist.strip()
            )
            member.append(line.toXML())

        return node
