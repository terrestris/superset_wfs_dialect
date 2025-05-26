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
