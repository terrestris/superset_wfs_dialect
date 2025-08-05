import unittest
import xml.etree.ElementTree as ET

from superset_wfs_dialect.wkt_parser import WKTParser


class TestWKTParser(unittest.TestCase):
    def setUp(self):
        self.parser = WKTParser()

    def test_parse_valid_point_epsg25833(self):
        wkt = "SRID=25833;POINT(316316.4879 5691509.2954)"
        filter_obj = self.parser.parse("geom", wkt)
        xml = ET.tostring(filter_obj.toXML(), encoding="utf-8").decode("utf-8")
        self.assertIn("urn:ogc:def:crs:EPSG::25833", xml)
        self.assertIn("<ns0:ValueReference>geom</ns0:ValueReference>", xml)
        self.assertRegex(xml, r"<ns1:pos>316316\.4879\s+5691509\.2954</ns1:pos>")

    def test_parse_valid_point_epsg4258_axis_order(self):
        wkt = "SRID=4258;POINT(6.95 50.93)"
        filter_obj = self.parser.parse("geom", wkt)
        xml = ET.tostring(filter_obj.toXML(), encoding="utf-8").decode("utf-8")
        self.assertIn("urn:ogc:def:crs:EPSG::4258", xml)
        self.assertIn("<ns0:ValueReference>geom</ns0:ValueReference>", xml)
        self.assertRegex(xml, r"<ns1:pos>50\.93\d*\s+6\.95\d*</ns1:pos>")

    def test_parse_invalid_wkt_missing_srid(self):
        wkt = "POINT(6.95 50.93)"
        with self.assertRaises(ValueError):
            self.parser.parse("geom", wkt)

    def test_parse_unsupported_geometry(self):
        wkt = "SRID=25833;CIRCULARSTRING(0 0, 1 1, 2 1)"
        with self.assertRaises(NotImplementedError):
            self.parser.parse("geom", wkt)


if __name__ == "__main__":
    unittest.main()
