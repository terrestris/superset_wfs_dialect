# Superset example configuration file for registering the WFS dialect

SECRET_KEY = "kK4w2z9/X4bCDiQh5I1ZFKGzvSFWkqj2XfNflh/VKQ0="

from sqlalchemy.dialects import registry
registry.register("wfs", "superset_wfs_dialect.dialect", "WfsDialect")
