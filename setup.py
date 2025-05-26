from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="superset_wfs_dialect",
    version="0.0.1",
    description="SQLAlchemy dialect for OGC WFS",
    license="Apache-2.0",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    include_package_data=True,
    author='terrestris GmbH & Co. KG',
    author_email='info@terrestris.de',
    url='https://github.com/terrestris/superset_wfs_dialect',
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Development Status :: 3 - Alpha",
    ],
    entry_points={
        'sqlalchemy.dialects': [
            'wfs = superset_wfs_dialect.dialect:WfsDialect',
        ],
    },
    python_requires='>=3.10',
)
