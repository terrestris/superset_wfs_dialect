import json
from unittest.mock import MagicMock

import pytest

pytest.importorskip("superset")

from superset_wfs_dialect.engine_spec import (  # noqa: E402
    WfsEngineSpec,
    WfsParametersSchema,
)


def test_build_sqlalchemy_uri_drops_ogc_query_keys():
    uri = WfsEngineSpec.build_sqlalchemy_uri(
        {
            "host": "https://example.com:8443/geoserver/wfs?SERVICE=WFS&version=2.0.0&foo=bar",
            "username": "user",
            "password": "secret",
        }
    )

    assert uri == "wfs://user:secret@example.com:8443/geoserver/wfs?foo=bar"


def test_get_parameters_from_uri_returns_https_host():
    parameters = WfsEngineSpec.get_parameters_from_uri(
        "wfs://user:secret@example.com/geoserver/wfs"
    )

    assert parameters == {
        "username": "user",
        "password": "secret",
        "host": "https://example.com/geoserver/wfs",
    }


def test_validate_parameters_requires_host():
    errors = WfsEngineSpec.validate_parameters({"parameters": {"username": "user"}})

    assert len(errors) == 1
    assert errors[0].extra["missing"] == ["host"]
    assert WfsEngineSpec.validate_parameters(
        {"parameters": {"host": "https://example.com/wfs"}}
    ) == []


def test_encrypted_extra_is_passed_to_dialect_unchanged():
    client_info = {"id": "id", "secret": "secret", "token_request_uri": "https://t"}
    database = MagicMock()
    database.encrypted_extra = json.dumps({"oauth2_client_info": client_info})
    params: dict = {}

    WfsEngineSpec.update_params_from_encrypted_extra(database, params)

    assert params == {"oauth2_client_info": client_info}


def test_update_params_from_encrypted_extra_without_oauth2():
    database = MagicMock()
    database.encrypted_extra = None
    params: dict = {}

    WfsEngineSpec.update_params_from_encrypted_extra(database, params)

    assert params == {}


def test_parameters_schema_accepts_oauth2_client_info():
    loaded = WfsParametersSchema().load(
        {"host": "https://example.com/wfs", "oauth2_client_info": "{}"}
    )

    assert loaded["oauth2_client_info"] == "{}"


def test_parameters_json_schema_marks_oauth2_client_info_encrypted():
    schema = WfsEngineSpec.parameters_json_schema()

    assert schema["required"] == ["host"]
    assert schema["properties"]["oauth2_client_info"]["x-encrypted-extra"] is True
    assert "x-encrypted-extra" not in schema["properties"]["host"]


def test_client_secret_is_masked():
    assert "$.oauth2_client_info.secret" in WfsEngineSpec.encrypted_extra_sensitive_fields
