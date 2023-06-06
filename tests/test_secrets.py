import pytest
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi

from pushcart_deploy.databricks_api.secrets_wrapper import SecretsWrapper


@pytest.fixture(autouse=True)
def mock_api_client():
    api_client = ApiClient(host="https://databricks.sample.com")

    return api_client


class TestSecretsWrapper:
    def test_create_scope_if_not_exists_success(self, mocker, mock_api_client):
        """Tests that create_scope_if_not_exists creates a new scope if it does not exist."""
        mock_list_scopes = mocker.patch.object(
            SecretApi, "list_scopes", return_value={"scopes": []}
        )
        mock_create_scope = mocker.patch.object(SecretApi, "create_scope")
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_wrapper.create_scope_if_not_exists("test_scope")

        mock_list_scopes.assert_called_once()
        mock_create_scope.assert_called_once_with(
            initial_manage_principal="users",
            scope="test_scope",
            scope_backend_type="DATABRICKS",
            backend_azure_keyvault=None,
        )

    def test_create_scope_if_not_exists_already_exists(self, mocker, mock_api_client):
        """Tests that create_scope_if_not_exists does not create a new scope if it already
        exists.
        """
        mock_list_scopes = mocker.patch.object(SecretApi, "list_scopes")
        mock_list_scopes.return_value = {"scopes": [{"name": "test_scope"}]}
        mock_create_scope = mocker.patch.object(SecretApi, "create_scope")
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_wrapper.create_scope_if_not_exists("test_scope")

        mock_list_scopes.assert_called_once()
        mock_create_scope.assert_not_called()

    def test_push_secrets_empty_dict(self, mocker, mock_api_client):
        """Tests that push_secrets does not push secrets if secrets_dict is empty."""
        mock_create_scope_if_not_exists = mocker.patch.object(
            SecretsWrapper,
            "create_scope_if_not_exists",
        )
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_wrapper.push_secrets()

        mock_create_scope_if_not_exists.assert_not_called()

    def test_push_secrets_success(self, mocker, mock_api_client):
        """Tests that push_secrets pushes secrets to an existing scope."""
        mock_secrets_api_put_secret = mocker.patch.object(SecretApi, "put_secret")
        mocker.patch.object(
            SecretApi,
            "list_scopes",
            return_value={"scopes": [{"name": "pushcart"}]},
        )

        mock_api_client.default_headers = {"Authorization": "Bearer test_token"}
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_dict = {"test_key": "test_value"}
        secret_scope_name = "pushcart"

        secrets_wrapper.push_secrets(
            secret_scope_name=secret_scope_name,
            secrets_dict=secrets_dict,
        )

        mock_secrets_api_put_secret.assert_called_once_with(
            secret_scope_name,
            "test_key",
            "test_value",
            bytes_value=None,
        )

    def test_invalid_secret_scope_name(self, mocker, mock_api_client):
        """Tests that an invalid secret_scope_name raises an error."""
        mocker.patch.object(
            SecretApi,
            "list_scopes",
            return_value={"scopes": [{"name": "pushcart"}]},
        )
        mock_api_client.default_headers = {"Authorization": "Bearer test_token"}
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_dict = {"test_key": "test_value"}
        secret_scope_name = "#invalid_scope_name"

        with pytest.raises(ValueError):
            secrets_wrapper.push_secrets(
                secret_scope_name=secret_scope_name,
                secrets_dict=secrets_dict,
            )

    def test_invalid_key_or_value(self, mocker, mock_api_client):
        """Test that an invalid key or value in secrets_dict raises an error."""
        mocker.patch.object(
            SecretApi,
            "list_scopes",
            return_value={"scopes": [{"name": "pushcart"}]},
        )
        mock_api_client.default_headers = {"Authorization": "Bearer test_token"}
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_dict = {"#invalid_key": "test_value"}
        secret_scope_name = "pushcart"

        with pytest.raises(ValueError):
            secrets_wrapper.push_secrets(
                secret_scope_name=secret_scope_name,
                secrets_dict=secrets_dict,
            )
