# SQLAlchemy dialect for OGC WFS

SQLAlchemy dialect for OGC WFS as a Superset plugin.

## Using the Custom Superset 5 Image with the WFS Dialect Plugin

A prebuilt Docker image based on Superset 5 and including the `superset-wfs-dialect` plugin is available on GitHub Container Registry (GHCR). This image is built from the `Dockerfile.superset-wfs` and contains the plugin version as specified in the image tag.

### 1. Pulling the Image

You can pull the image with a specific plugin version and commit hash (replace `0.0.1.dev3` and `abc1234` with the desired plugin version and commit hash):

```bash
docker pull ghcr.io/terrestris/superset_wfs_dialect:5.0.0rc2-wfs-0.0.1.dev3-abc1234
```

This ensures you get the exact build matching the plugin version and code state. For the latest build (which may change), you can still use:

```bash
docker pull ghcr.io/terrestris/superset_wfs_dialect:latest
```

### 2. Preparing Required Files and Folders

Before running Superset, you need to create the following on your host system:

- A folder for persistent Superset data:

  ```bash
  mkdir -p ./superset/data
  ```

- A configuration file for Superset:
  - Copy the provided `superset_config.py` from this repository to your working directory. This file already contains the necessary configuration for using the WFS dialect.

- Set permissions so that Docker can write to the data folder:

  ```bash
  chmod 777 ./superset/data
  ```

  (For production, set more restrictive permissions and ensure the folder is owned by the correct user.)

### 3. Starting Superset

#### Option A: Using `docker run`

```bash
docker run -p 8088:8088 \
  --name superset \
  -e SUPERSET_ENV=production \
  -e DEBUG_MODE=false \
  -v ./superset/data:/app/superset_home \
  -v ./superset_config.py:/app/pythonpath/superset_config.py:ro \
  --restart unless-stopped \
  ghcr.io/terrestris/superset_wfs_dialect:latest
```

- Add `-d` to run in the background.

#### Option B: Using Docker Compose

Add the following service to your `docker-compose.yml`:

```yaml
services:
  superset:
    image: ghcr.io/terrestris/superset_wfs_dialect:latest
    ports:
      - "8088:8088"
    environment:
      - SUPERSET_ENV=production
      - DEBUG_MODE=false
    volumes:
      - ./superset/data:/app/superset_home
      - ./superset_config.py:/app/pythonpath/superset_config.py:ro
    restart: unless-stopped
```

Then start with:

```bash
docker compose up
```

Superset will be available at [http://localhost:8088/](http://localhost:8088/). Use the default credentials `admin:admin` as login.  
Use the tag if a specific version should be used (e.g. `5.0.0rc2-wfs-0.0.1.dev3-abc1234`) instead of `latest`.

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
