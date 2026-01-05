"""
Shared test configuration and utilities for superset_wfs_dialect tests.
"""

from unittest.mock import MagicMock


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
