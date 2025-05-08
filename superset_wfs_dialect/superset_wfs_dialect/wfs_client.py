import urllib.parse

class WfsQueryBuilder:
    def __init__(self, base_url, version="2.0.0", prefer_json=False):
        self.base_url = base_url.rstrip("?")
        self.version = version
        self.prefer_json = prefer_json

    def build_getfeature_url(self, layer_name, max_features=None):
        params = {
            "service": "WFS",
            "version": self.version,
            "request": "GetFeature",
            "typeName": layer_name,
        }
        if max_features:
            if self.version.startswith("2."):
                params["count"] = str(max_features)
            else:
                params["maxFeatures"] = str(max_features)

        if self.prefer_json:
            params["outputFormat"] = "application/json"

        return f"{self.base_url}?{urllib.parse.urlencode(params)}"
