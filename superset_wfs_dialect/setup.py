from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="superset_wfs_dialect",
    version="0.1.0",
    description="SQLAlchemy dialect for OGC WFS",
    author='terrestris GmbH & Co. KG',
    author_email='info@terrestris.de',
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        "sqlalchemy.dialects": [
            "wfs = superset_wfs_dialect.dialect:WfsDialect"
        ]
    },
)
