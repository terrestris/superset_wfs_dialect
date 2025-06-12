[![Coverage](https://sq.terrestris.de/api/project_badges/measure?project=superset_wfs_dialect&metric=coverage&token=sqb_88a5e9f0f1eba432a07a9cd20ae27a6c4337271a)](https://sq.terrestris.de/dashboard?id=superset_wfs_dialect)
[![Reliability Rating](https://sq.terrestris.de/api/project_badges/measure?project=superset_wfs_dialect&metric=software_quality_reliability_rating&token=sqb_88a5e9f0f1eba432a07a9cd20ae27a6c4337271a)](https://sq.terrestris.de/dashboard?id=superset_wfs_dialect)
[![Maintainability Rating](https://sq.terrestris.de/api/project_badges/measure?project=superset_wfs_dialect&metric=software_quality_maintainability_rating&token=sqb_88a5e9f0f1eba432a07a9cd20ae27a6c4337271a)](https://sq.terrestris.de/dashboard?id=superset_wfs_dialect)
[![Security Rating](https://sq.terrestris.de/api/project_badges/measure?project=superset_wfs_dialect&metric=software_quality_security_rating&token=sqb_88a5e9f0f1eba432a07a9cd20ae27a6c4337271a)](https://sq.terrestris.de/dashboard?id=superset_wfs_dialect)

# SQLAlchemy dialect for OGC WFS

SQLAlchemy dialect for OGC WFS as a Superset plugin.

## Register the dialect

The plugin can currently be installed via the test instance of the [Python Package Index](https://test.pypi.org/project/superset-wfs-dialect/):

```bash
pip install -i https://test.pypi.org/simple/ superset-wfs-dialect
```

The dialect must then be registered in the the local `superset_config.py`:

```python
from sqlalchemy.dialects import registry
registry.register("wfs", "superset_wfs_dialect.dialect", "WfsDialect")
```

Start/restart superset and continue as described in the [Start the application section](#start-the-application).

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

To add a new database:

- select Data > Connect database in the submenu
- choose "Other" at the list of "Supported Databases"
- insert the SQLAlchemy URI to a WFS (`wfs://[...]`)
- test the connection
- create a dataset
- create a chart/dashboard

## Publishing a Development Version to TestPyPI

### Requirements

- You must be on the main branch
- Your working directory must be clean (no uncommitted changes)
- You have push access to the repository
- A valid `TEST_PYPI_TOKEN` is configured in GitHub Secrets (used by the GitHub Actions workflow)

### Releasing a dev version

1. Run the release script with the desired version number (e.g. `0.0.1dev2`):

    ```bash
    ./release.sh 0.0.1dev2
    ```

    This will:

    - Update the `version` field in `setup.py`
    - Commit the change to `main`
    - Create a Git tag e.g. `0.0.1dev2`
    - Push the tag to GitHub

2. The GitHub Actions workflow will be triggered by the tag:

    - It will build the package
    - Upload it to TestPyPI

### Notes

- Versions must follow the format `X.Y.Z` or `X.Y.ZdevN` (e.g. `0.1.0dev3`)
