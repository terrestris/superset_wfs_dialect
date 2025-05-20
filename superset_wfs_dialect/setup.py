from setuptools import setup, find_packages

setup(
    name="superset_wfs_dialect",
    version="0.1.0",
    description="SQLAlchemy dialect for OGC WFS",
    author='terrestris GmbH & Co. KG',
    author_email='info@terrestris.de',
    packages=find_packages(),
    install_requires=[
        "sqlalchemy>=1.4",
        "requests",
        "OWSLib",
        "sqlglot>=21.0.0",
        "debugpy",
        "lxml"
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "wfs = superset_wfs_dialect.dialect:WfsDialect"
        ]
    },
)
