import urllib.parse

class WfsQueryBuilder:
    def __init__(self, base_url, version="2.0.0"):
        self.base_url = base_url.rstrip("?")
        self.version = version

    def build_getfeature_url(self, typename, max_features=None, property_names=None):
        params = {
            "service": "WFS",
            "version": self.version,
            "request": "GetFeature",
            "typenames": typename
        }

        if max_features:
            params["count"] = str(max_features)

        if property_names:
            params["propertyName"] = ",".join(property_names)

        return self.base_url + "?" + urllib.parse.urlencode(params)
