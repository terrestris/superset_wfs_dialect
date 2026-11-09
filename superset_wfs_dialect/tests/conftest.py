"""
Shared test configuration and utilities for superset_wfs_dialect tests.
"""

from unittest.mock import MagicMock

try:
    from superset.app import SupersetApp
    from superset.initialization import SupersetAppInitializer
except ImportError:
    pass
else:
    # Engine spec schemas create encrypted fields at import time, before any app exists.
    _app = SupersetApp(__name__)
    _app.config.from_object("superset.config")
    _app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite://"
    _app.config["SECRET_KEY"] = "test-secret"
    _app.config["TESTING"] = True
    _app.config["WTF_CSRF_ENABLED"] = False
    _app.config["RATELIMIT_ENABLED"] = False
    _app.config["CACHE_CONFIG"] = {}
    _app.config["DATA_CACHE_CONFIG"] = {}
    SupersetAppInitializer(_app).init_app()
    _app.app_context().push()


def create_mock_wfs_instance(output_formats=None):
    """
    Create a mock WFS instance with operations configured.

    :param output_formats: List of output formats. Defaults to ["application/json", "text/xml"]
    :return: A configured MagicMock WFS instance
    """
    if output_formats is None:
        output_formats = ["application/json", "text/xml"]

    mock_wfs_instance = MagicMock()
    mock_operation = MagicMock()
    mock_operation.name = "GetFeature"
    mock_operation.parameters = {"outputFormat": {"values": output_formats}}
    mock_wfs_instance.operations = [mock_operation]
    return mock_wfs_instance
