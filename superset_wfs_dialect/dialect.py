from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects import registry
from .base import FakeDbApi
import superset_wfs_dialect
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("WfsDialect module loaded")

type_map = {
    "int": sqltypes.Integer(),
    "integer": sqltypes.Integer(),
    "long": sqltypes.BigInteger(),
    "float": sqltypes.Float(),
    "double": sqltypes.Float(),
    "string": sqltypes.String(),
    "boolean": sqltypes.Boolean(),
    "date": sqltypes.Date(),
    "datetime": sqltypes.DateTime(),
    "time": sqltypes.Time(),
    "byte": sqltypes.Integer(),
    "short": sqltypes.Integer()
}

class WfsDialect(DefaultDialect):
    name = "wfs"
    driver = "wfs"
    supports_alter = False
    supports_unicode_statements = True
    supports_statement_cache = False

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
        return FakeDbApi

    # TODO use GetCapabilites & DescribeFeatureType
    def get_schema_names(self, connection, **kw):
        logger.info("get_schema_names() aufgerufen")
        return ['default']

    def has_table(self, connection, table_name, schema=None):
        logger.info("has_table(schema=%s, table=%s)", schema, table_name)
        return True

    def get_view_names(self, connection, schema=None, **kw):
        logger.info("get_view_names() aufgerufen für schema=%s", schema)
        return []

    def get_table_names(self, connection, schema=None, **kw):
        logger.info("get_table_names() aufgerufen mit schema=%s", schema)

        wfs = connection.connection.connection.wfs
        layer_keys = list(wfs.contents.keys())

        return layer_keys

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        wfs = connection.connection.connection.wfs
        fiona_schema = wfs.get_schema(table_name)
        columns = []

        for key, value in fiona_schema.get('properties').items():
            coltype = type_map.get(value, sqltypes.String())
            required = key in fiona_schema.get('required', [])

            columns.append({
                "name": key,
                "type": coltype,
                "nullable": not required,
                "default": None,
            })

        columns.append({
            "name": fiona_schema.get('geometry_column'),
            "type": sqltypes.String(),
            "nullable": True,
            "default": None,
        })

        return columns


registry.register("wfs", "superset_wfs_dialect.dialect", "WfsDialect")
