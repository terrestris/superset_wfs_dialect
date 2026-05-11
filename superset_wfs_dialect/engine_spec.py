from typing import Any, TypedDict

from flask_babel import gettext as __
from marshmallow import fields, Schema
from sqlalchemy.engine.url import URL

from superset.databases.utils import make_url_safe
from superset.db_engine_specs.base import BaseEngineSpec, BasicParametersMixin
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType


class WfsParametersSchema(Schema):
    username = fields.String(
        required=False, allow_none=True, metadata={"description": __("Username")}
    )
    password = fields.String(allow_none=True, metadata={"description": __("Password")})
    host = fields.String(
        required=True, metadata={"description": __("WFS URL")}
    )


class WfsParametersType(TypedDict, total=False):
    username: str | None
    password: str | None
    host: str


class WfsPropertiesType(TypedDict):
    parameters: WfsParametersType


class WfsParametersMixin(BasicParametersMixin):
    default_driver = "wfs"
    parameters_schema = WfsParametersSchema()


class WfsEngineSpec(WfsParametersMixin, BaseEngineSpec):
    engine = "wfs"
    engine_name = "OGC WFS"
    drivers = {"wfs": "OGC WFS"}

    sqlalchemy_uri_placeholder = "[wfs|https]://host:port/path"
    disable_ssh_tunneling = True
    allows_joins = False
    allows_subqueries = False
    supports_file_upload = False
    supports_dynamic_schema = False

    @classmethod
    def validate_parameters(
        cls, properties: WfsPropertiesType
    ) -> list[SupersetError]:
        errors: list[SupersetError] = []

        required = {"host"}
        parameters = properties.get("parameters", {})
        present = {key for key in parameters if parameters.get(key, ())}

        if missing := sorted(required - present):
            errors.append(
                SupersetError(
                    message=f'One or more parameters are missing: {", ".join(missing)}',
                    error_type=SupersetErrorType.CONNECTION_MISSING_PARAMETERS_ERROR,
                    level=ErrorLevel.WARNING,
                    extra={"missing": missing},
                ),
            )

        return errors

    @classmethod
    def build_sqlalchemy_uri(  # pylint: disable=unused-argument
        cls,
        parameters: WfsParametersType,
        encrypted_extra: dict[str, str] | None = None,
    ) -> str:
        host = parameters["host"].replace("http://", "").replace("https://", "").replace("wfs://", "")

        return str(
            URL.create(
                "wfs",
                username=parameters.get("username"),
                password=parameters.get("password"),
                host=host,
            )
        )

    @classmethod
    def get_parameters_from_uri(  # pylint: disable=unused-argument
        cls, uri: str, encrypted_extra: dict[str, Any] | None = None
    ) -> WfsParametersType:
        url = make_url_safe(uri)
        cleaned_url = URL.create("wfs", host=url.host, database=url.database)

        return {
            "username": url.username,
            "password": url.password,
            "host": str(cleaned_url),
        }
