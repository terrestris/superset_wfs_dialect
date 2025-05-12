# superset_wfs_dialect

SQLAlchemy dialect for OGC WFS as a Superset plugin.

Checkout the project.

## Prerequisites

Create a virtual environment and install the required packages.

```bash
python3 -m venv venv
source venv/bin/activate
```

or via VS-Code:

https://code.visualstudio.com/docs/python/python-tutorial#_create-a-virtual-environment


## Installation

Run via terminal:

```bash
cd ./superset_wfs_dialect # within the project root
pip install -e .
python tests/test_query.py
```

or:

```bash
cd ./superset_wfs_dialect/superset_wfs_dialect
python test_wfs.py
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
