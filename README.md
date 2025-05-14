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

<https://code.visualstudio.com/docs/python/python-tutorial#_create-a-virtual-environment>

:warning: Currently the setup requires a local installation of `owslib`. Checkout this branch
<https://github.com/KaiVolland/OWSLib/tree/arcgis-server-support> and set the path in the `.env` file.

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
docker compose up -d --build
```

Open [http://localhost:8088/](http://localhost:8088/).

To add a new database:

- select Data > Connect database in th submenu
- choose "Other" at the list of "Supported Databases"
- insert the SQLAlchemy URI (`wfs://[...]`)
- test the connection
