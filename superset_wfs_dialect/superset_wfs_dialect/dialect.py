from sqlalchemy.engine.default import DefaultDialect
from .base import connect

class WfsDialect(DefaultDialect):
    name = "wfs"
    driver = "wfs"
    supports_alter = False
    supports_unicode_statements = True

    dbapi = connect

    def create_connect_args(self, url):
        return [], {}

from sqlalchemy.dialects import registry
registry.register("wfs", "superset_wfs_dialect.dialect", "WfsDialect")

assert hasattr(connect, "paramstyle"), "connect hat kein paramstyle!"
assert not hasattr(connect(), "paramstyle"), "connect() sollte KEIN paramstyle haben"
