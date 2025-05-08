from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import URL
from .base import dbapi
import superset_wfs_dialect

class WfsDialect(DefaultDialect):
    name = "wfs"
    driver = "wfs"
    supports_alter = False
    supports_unicode_statements = True

    def create_connect_args(self, url):
        scheme = "https"
        host = url.host
        port = f":{url.port}" if url.port else ""
        path = f"/{url.database}" if url.database else ""

        base_url = f"{scheme}://{host}{port}{path}"

        return [], {"base_url": base_url}
    
    @classmethod
    def dbapi(cls):
        return superset_wfs_dialect

    @classmethod
    def import_dbapi(cls):
        return dbapi

from sqlalchemy.dialects import registry
registry.register("wfs", "superset_wfs_dialect.dialect", "WfsDialect")
