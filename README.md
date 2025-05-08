# superset_wfs_dialect

SQLAlchemy dialect for OGC WFS as a Superset plugin.

Checkout the project.

Run via terminal:

```bash
cd ./superset_wfs_dialect # within the project root
python3 -m venv venv
source venv/bin/activate
pip install -e .
python tests/test_query.py
```

Start superset with plugin:

```bash
docker build -t superset-with-wfs .
docker compose up -d
```

Open [http://localhost:8088/](http://localhost:8088/).

To add a new database:

- select Data > Connect database in th submenu
- choose "Other" at the list of "Supported Databases"
- insert the SQLAlchemy URI (`wfs://[...]`)
- test the connection
