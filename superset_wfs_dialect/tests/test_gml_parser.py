import unittest
from superset_wfs_dialect.gml_parser import GMLParser
import xml.etree.ElementTree as ET

class TestGMLParser(unittest.TestCase):
    def setUp(self):
        self.parser = GMLParser("geom")
        self.ns = {
            'gml': 'http://www.opengis.net/gml/3.2',
            'wfs': 'http://www.opengis.net/wfs/2.0'
        }

    def test_point_conversion(self):
        gml = '''
            <gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::25833" srsDimension="2">
                <gml:pos>316316.4879 5691509.2954</gml:pos>
            </gml:Point>
        '''
        gml_elem = ET.fromstring(gml)
        expected = "SRID=25833;POINT(316316.4879 5691509.2954)"
        self.assertEqual(self.parser._gml_to_wkt(gml_elem), expected)

    def test_linestring_conversion(self):
        gml = '''
            <gml:LineString xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::25833" srsDimension="2">
                <gml:posList>316316.4879 5691509.2954 316910.3008 5691796.1469</gml:posList>
            </gml:LineString>
        '''
        gml_elem = ET.fromstring(gml)
        expected = "SRID=25833;LINESTRING(316316.4879 5691509.2954, 316910.3008 5691796.1469)"
        self.assertEqual(self.parser._gml_to_wkt(gml_elem), expected)

    def test_polygon_conversion(self):
        gml = '''
            <gml:Polygon xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::25833" srsDimension="2">
                <gml:exterior>
                    <gml:LinearRing>
                        <gml:posList>
                            316316.4879 5691509.2954 316910.3008 5691796.1469
                            316910.3008 5691509.2954 316316.4879 5691509.2954
                        </gml:posList>
                    </gml:LinearRing>
                </gml:exterior>
                <gml:interior>
                    <gml:LinearRing>
                        <gml:posList>
                            316416.4879 5691609.2954 316810.3008 5691696.1469
                            316810.3008 5691609.2954 316416.4879 5691609.2954
                        </gml:posList>
                    </gml:LinearRing>
                </gml:interior>
            </gml:Polygon>
        '''
        gml_elem = ET.fromstring(gml)
        expected = "SRID=25833;POLYGON((316316.4879 5691509.2954, 316910.3008 5691796.1469, " \
                  "316910.3008 5691509.2954, 316316.4879 5691509.2954), " \
                  "(316416.4879 5691609.2954, 316810.3008 5691696.1469, " \
                  "316810.3008 5691609.2954, 316416.4879 5691609.2954))"
        self.assertEqual(self.parser._gml_to_wkt(gml_elem), expected)

    def test_multipoint_conversion(self):
        gml = '''
            <gml:MultiPoint xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::25833" srsDimension="2">
                <gml:pointMember>
                    <gml:Point>
                        <gml:pos>316316.4879 5691509.2954</gml:pos>
                    </gml:Point>
                </gml:pointMember>
                <gml:pointMember>
                    <gml:Point>
                        <gml:pos>316910.3008 5691796.1469</gml:pos>
                    </gml:Point>
                </gml:pointMember>
            </gml:MultiPoint>
        '''
        gml_elem = ET.fromstring(gml)
        expected = "SRID=25833;MULTIPOINT((316316.4879 5691509.2954), (316910.3008 5691796.1469))"
        self.assertEqual(self.parser._gml_to_wkt(gml_elem), expected)

    def test_multilinestring_conversion(self):
        gml = '''
            <gml:MultiCurve xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::25833" srsDimension="2">
                <gml:curveMember>
                    <gml:LineString>
                        <gml:posList>316316.4879 5691509.2954 316910.3008 5691796.1469</gml:posList>
                    </gml:LineString>
                </gml:curveMember>
                <gml:curveMember>
                    <gml:LineString>
                        <gml:posList>316316.4879 5691796.1469 316910.3008 5691509.2954</gml:posList>
                    </gml:LineString>
                </gml:curveMember>
            </gml:MultiCurve>
        '''
        gml_elem = ET.fromstring(gml)
        expected = "SRID=25833;MULTILINESTRING((316316.4879 5691509.2954, 316910.3008 5691796.1469), " \
                  "(316316.4879 5691796.1469, 316910.3008 5691509.2954))"
        self.assertEqual(self.parser._gml_to_wkt(gml_elem), expected)

    def test_multipolygon_conversion(self):
        gml = '''
            <gml:MultiSurface xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::25833" srsDimension="2">
                <gml:surfaceMember>
                    <gml:Polygon>
                        <gml:exterior>
                            <gml:LinearRing>
                                <gml:posList>
                                    316316.4879 5691509.2954 316910.3008 5691796.1469
                                    316910.3008 5691509.2954 316316.4879 5691509.2954
                                </gml:posList>
                            </gml:LinearRing>
                        </gml:exterior>
                    </gml:Polygon>
                </gml:surfaceMember>
                <gml:surfaceMember>
                    <gml:Polygon>
                        <gml:exterior>
                            <gml:LinearRing>
                                <gml:posList>
                                    316416.4879 5691609.2954 316810.3008 5691696.1469
                                    316810.3008 5691609.2954 316416.4879 5691609.2954
                                </gml:posList>
                            </gml:LinearRing>
                        </gml:exterior>
                    </gml:Polygon>
                </gml:surfaceMember>
            </gml:MultiSurface>
        '''
        gml_elem = ET.fromstring(gml)
        expected = "SRID=25833;MULTIPOLYGON(((316316.4879 5691509.2954, 316910.3008 5691796.1469, " \
                  "316910.3008 5691509.2954, 316316.4879 5691509.2954)), " \
                  "((316416.4879 5691609.2954, 316810.3008 5691696.1469, " \
                  "316810.3008 5691609.2954, 316416.4879 5691609.2954)))"
        self.assertEqual(self.parser._gml_to_wkt(gml_elem), expected)

if __name__ == '__main__':
    unittest.main()
