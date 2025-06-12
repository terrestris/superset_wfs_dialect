[![Coverage](https://sq.terrestris.de/api/project_badges/measure?project=superset_wfs_dialect&metric=coverage&token=sqb_88a5e9f0f1eba432a07a9cd20ae27a6c4337271a)](https://sq.terrestris.de/dashboard?id=superset_wfs_dialect)
[![Reliability Rating](https://sq.terrestris.de/api/project_badges/measure?project=superset_wfs_dialect&metric=software_quality_reliability_rating&token=sqb_88a5e9f0f1eba432a07a9cd20ae27a6c4337271a)](https://sq.terrestris.de/dashboard?id=superset_wfs_dialect)
[![Maintainability Rating](https://sq.terrestris.de/api/project_badges/measure?project=superset_wfs_dialect&metric=software_quality_maintainability_rating&token=sqb_88a5e9f0f1eba432a07a9cd20ae27a6c4337271a)](https://sq.terrestris.de/dashboard?id=superset_wfs_dialect)
[![Security Rating](https://sq.terrestris.de/api/project_badges/measure?project=superset_wfs_dialect&metric=software_quality_security_rating&token=sqb_88a5e9f0f1eba432a07a9cd20ae27a6c4337271a)](https://sq.terrestris.de/dashboard?id=superset_wfs_dialect)

# SQLAlchemy dialect for OGC WFS

SQLAlchemy dialect for OGC WFS as a Superset plugin.

## Register the dialect

Create a `requirements-local.txt` file according to the
[superset documentation](https://superset.apache.org/docs/configuration/databases#2-install-the-driver-in-the-container)
and insert following line:

```
superset_wfs_dialect
```

The dialect must then be registered in your superset config file,
e.g. `superset_config_docker.py` when using the
[docker setup](https://superset.apache.org/docs/configuration/databases#2-install-the-driver-in-the-container):

```python
from sqlalchemy.dialects import registry
registry.register("wfs", "superset_wfs_dialect.dialect", "WfsDialect")
```

Start/restart superset and continue as described in the [Start the application section](#start-the-application).

## Add a WFS database connection:

- select Data > Connect database in the submenu
- choose "Other" at the list of "Supported Databases"
- insert the SQLAlchemy URI to a WFS `wfs://[...]` (i.e. replace `https://` of your WFS URL with `wfs://`)
- test the connection
- create a dataset
- create a chart/dashboard

## Development

### Prerequisites for development

- Docker Engine >= version 28
- python >= version 3.10.12
- Checkout this project

### Installation

For debugging and code completion run via terminal within the project root:

```bash
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
When in development mode, open <a
  href="http://localhost:8088/"
  target="_blank"
  rel="noopener noreferrer">
    http://localhost:8088/
  </a>. Otherwise, please open the corresponding URL to the installed superset instance.
<!-- markdownlint-enable MD033 -->

## Publishing a Development Version to PyPI

### Requirements

- You must be on the main branch
- Your working directory must be clean (no uncommitted changes)
- You have push access to the repository
- A valid `PYPI_TOKEN` is configured in GitHub Secrets (used by the GitHub Actions workflow)

### Releasing a new version

1. Run the release script with the desired version number (e.g. `0.0.1`):

    ```bash
    ./release.sh 0.0.1
    ```

    This will:

    - Update the `version` field in `setup.py`
    - Commit the change to `main`
    - Create a Git tag e.g. `0.0.1`
    - Push the tag to GitHub

2. The GitHub Actions workflow will be triggered by the tag:

    - It will build the package
    - Upload it to PyPI

### Notes

- Versions must follow the format `X.Y.Z` (e.g. `0.1.0`)
