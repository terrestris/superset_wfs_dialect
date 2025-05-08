from sqlalchemy.dialects import registry
from sqlalchemy import create_engine, text

registry.register("wfs", "superset_wfs_dialect.dialect", "WfsDialect")
engine = create_engine("wfs://geoportal.stadt-koeln.de/arcgis/services/basiskarten/adressen_stadtteil/MapServer/WFSServer")

with engine.connect() as connection:
    result = connection.execute(text("SELECT * FROM adressen_stadtteil:Raderthal LIMIT 5"))
    for row in result:
        print(row)