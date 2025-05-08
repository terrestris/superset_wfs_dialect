from .base import Connection

class WfsDBAPI:
    paramstyle = "qmark"

    @staticmethod
    def connect(*args, **kwargs):
        base_url = kwargs.get("base_url", "https://localhost/geoserver/ows")
        return Connection(base_url)
