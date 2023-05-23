"""Create secrets and secret scopes to be used by Pushcart.

Wrapper class around the Databricks Secrets API used in setting Pushcart-specific
secrets to be read by data pipelines.

Example:
-------
    secrets_wrapper = SecretsWrapper(api_client)
    secrets_wrapper.create_scope_if_not_exists("pushcart")
    secrets_wrapper.push_secrets("pushcart", secrets_dict)

Notes:
-----
Needs a Databricks CLI ApiClient to be configured and connected to a Databricks
environment.

"""

import logging

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi
from pydantic import Field, constr, dataclasses, validate_arguments, validator

from pushcart_deploy.validation import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class SecretsWrapper:
    """Wrapper around the Databricks Secrets API.

    Manage secrets in a Databricks workspace. It allows creating a secret scope if it
    does not exist and pushing secrets to the scope.
    """

    client: ApiClient

    @validator("client")
    @classmethod
    def check_api_client(cls, value: ApiClient) -> ApiClient:
        """Validate that the ApiClient object is properly initialized."""
        return validate_databricks_api_client(value)

    def __post_init_post_parse__(self) -> None:
        """Initialize logger."""
        self.log = logging.getLogger(__name__)

        self.secrets_api = SecretApi(self.client)

    @validate_arguments
    def create_scope_if_not_exists(
        self,
        secret_scope_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
            regex=r"^[A-Za-z0-9\-_.]{1,128}$",
        ) = "pushcart",  # noqa: S107
    ) -> None:
        """Create a secret scope if it does not exist in the workspace."""
        scopes = self.secrets_api.list_scopes()["scopes"]
        if secret_scope_name not in [scope["name"] for scope in scopes]:
            self.secrets_api.create_scope(
                initial_manage_principal="users",
                scope=secret_scope_name,
                scope_backend_type="DATABRICKS",
                backend_azure_keyvault=None,
            )
            self.log.info(f"Created secret scope {secret_scope_name}")

    @validate_arguments
    def push_secrets(
        self,
        secret_scope_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
            regex=r"^[A-Za-z0-9\-_.]{1,128}$",
        ) = "pushcart",  # noqa: S107
        secrets_dict: dict[
            constr(
                strip_whitespace=True,
                to_lower=True,
                strict=True,
                min_length=1,
                regex=r"^[A-Za-z0-9\-_.]{1,128}$",
            ),
            str,
        ] = Field(  # noqa: B008
            default_factory=dict,
        ),
    ) -> None:
        """Pushes secrets to a secret scope in the workspace."""
        if not secrets_dict:
            self.log.warning("No secrets to push to secret scope")
            return

        self.create_scope_if_not_exists(secret_scope_name)

        for key, value in secrets_dict.items():
            self.secrets_api.put_secret(secret_scope_name, key, value, bytes_value=None)
            self.log.info(f"Put secret '{key}' in '{secret_scope_name}' secret scope.")
