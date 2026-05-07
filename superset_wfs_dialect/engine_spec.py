from typing import Any, TypedDict

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from flask_babel import gettext as __
from marshmallow import fields, Schema
from sqlalchemy.engine.url import URL

from superset.databases.utils import make_url_safe
from superset.databases.schemas import encrypted_field_properties, EncryptedString
from superset.db_engine_specs.base import BaseEngineSpec, BasicParametersMixin
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType

from superset.models.core import Database

ma_plugin = MarshmallowPlugin()


class WfsParametersSchema(Schema):
    username = fields.String(
        required=False, allow_none=True, metadata={"description": __("Username")}
    )
    password = fields.String(allow_none=True, metadata={"description": __("Password")})
    host = fields.String(
        required=True, metadata={"description": __("WFS URL")}
    )
    # This will create the oauth2 form fields in the UI. However, data
    # is passed back as `oauth2_client_info` in the encrypted_extra. So
    # we have to be careful when to use which name.
    oauth2_client = EncryptedString(
        required=False,
        metadata={
            "description": __("OAuth2 client info"),
            "field_name": "oauth2_client_info",
        },
    )


class WfsParametersType(TypedDict, total=False):
    username: str | None
    password: str | None
    host: str
    oauth2_client: str | None


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

    encrypted_extra_sensitive_fields: set[str] = {"$.oauth2_client_info.secret"}

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

    @staticmethod
    def update_params_from_encrypted_extra(
        database: Database,
        params: dict[str, Any],
    ) -> None:
        """
        Rename `oauth2_client_info` to `oauth2_client` to pass validation.
        """
        BaseEngineSpec.update_params_from_encrypted_extra(database, params)

        if "oauth2_client_info" in params:
            params["oauth2_client"] = params.pop("oauth2_client_info")


    @classmethod
    def parameters_json_schema(cls) -> Any:
        """
        Return configuration parameters as OpenAPI.
        """
        if not cls.parameters_schema:
            return None

        spec = APISpec(
            title="Database Parameters",
            version="1.0.0",
            openapi_version="3.0.0",
            plugins=[ma_plugin],
        )

        ma_plugin.init_spec(spec)
        ma_plugin.converter.add_attribute_function(encrypted_field_properties)
        spec.components.schema(cls.__name__, schema=cls.parameters_schema)
        return spec.to_dict()["components"]["schemas"][cls.__name__]
