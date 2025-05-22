# SQLAlchemy dialect for OGC WFS

SQLAlchemy dialect for OGC WFS as a Superset plugin.

## Register the dialect

TBD
<!-- This section should contain the documentaion of how to use the package without development -->

## Development

### Prerequisites for development

- Docker Engine >= version 28
- python >= version 3.10.12
- Checkout this project
- :warning: Currently the setup requires a local installation of `owslib`. Checkout OWSLib
  <https://github.com/geopython/OWSLib> and set the path in the `.env` file (see `.env.example`).  
  When a version >= 0.33.0 will be released, this can be omitted.

### Installation

For debugging and code completion run via terminal:

```bash
cd ./superset_wfs_dialect
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

**or** create a virtual environment via VS Code:

<https://code.visualstudio.com/docs/python/python-tutorial#_create-a-virtual-environment>.

Start superset with the registered plugin:

```bash
docker compose up -d --build
```

### Debugging during development

Debugging can be activated via the VS Code during development using `F5`.
Please note that the Python interpreter is selected from the previously created venv.
Breakpoints set in VS Code are then taken into account.

### Start the application

<!-- markdownlint-disable MD033 -->
Open <a
  href="http://localhost:8088/"
  target="_blank"
  rel="noopener noreferrer">
    http://localhost:8088/
  </a>.
<!-- markdownlint-enable MD033 -->

To add a new database:

- select Data > Connect database in th submenu
- choose "Other" at the list of "Supported Databases"
- insert the SQLAlchemy URI (`wfs://[...]`)
- test the connection

### Docker Image and Deployment

The Docker image is automatically built and published to GitHub Container Registry (ghcr.io) through GitHub Actions. The workflow is triggered on:

- Every push to the `main` branch
- Every tag push starting with 'v' (e.g., v1.0.0)

The following Docker tags are created:

- `latest` - Always points to the latest version from the main branch
- `vX.Y.Z` - Full semantic version (e.g., v1.2.3)
- `X.Y` - Major.Minor version (e.g., 1.2)
- `X` - Major version (e.g., 1)

You can pull the image using:

```bash
docker pull ghcr.io/terrestris/superset_wfs_dialect:[VERSION|latest]
```
