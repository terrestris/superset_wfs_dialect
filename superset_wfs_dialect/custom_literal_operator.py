from owslib.etree import etree
from owslib import util
from owslib.fes2 import OgcExpression, namespaces

# Simple class that patches the issue of OWSLib not supporting
# left-hand side literals in filters.
class CustomLiteralOperator(OgcExpression):
    def __init__(self, propertyoperator, leftSide, rightSide):
        self.propertyoperator = propertyoperator
        self.leftSide = leftSide
        self.rightSide = rightSide

    def toXML(self):
        node0 = etree.Element(util.nspath_eval(self.propertyoperator, namespaces))
        etree.SubElement(node0, util.nspath_eval('fes:Literal', namespaces)).text = self.leftSide
        etree.SubElement(node0, util.nspath_eval('fes:Literal', namespaces)).text = self.rightSide
        return node0
