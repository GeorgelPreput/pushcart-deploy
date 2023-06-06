import os
from dataclasses import dataclass

import dacite
import pytest
from databricks.sdk import GitCredentialsAPI
from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi

from pushcart_deploy.databricks_api.repos_wrapper import ReposWrapper


@pytest.fixture(autouse=True)
def mock_api_client():
    api_client = ApiClient(host="https://databricks.sample.com")

    return api_client


class TestReposWrapper:
    def test_get_or_create_git_credentials_happy_path(self, mocker, mock_api_client):
        """Tests that the function successfully creates Git credentials when they do
        not exist and returns the credential ID."""
        mocker.patch.dict(
            os.environ,
            {
                "PUSHCART_CONFIG_GIT_USERNAME": "test_user",
                "PUSHCART_CONFIG_GIT_TOKEN": "test_token",
            },
        )

        @dataclass
        class MockGitCredentialsAPI:
            credential_id: str
            git_username: str

        mocker.patch.object(GitCredentialsAPI, "list", return_value=[])
        mocker.patch.object(
            GitCredentialsAPI,
            "create",
            return_value=dacite.from_dict(
                data_class=MockGitCredentialsAPI,
                data={"credential_id": "123", "git_username": "test_user"},
            ),
        )

        credential_id = ReposWrapper(
            mock_api_client, "./tests/data"
        ).get_or_create_git_credentials()

        assert credential_id == "123"

    def test_create_repo_success(self, mocker, mock_api_client):
        """Tests that the get_or_create_repo method successfully creates a new repository."""
        mocker.patch.object(ReposApi, "__init__", return_value=None)
        mocker.patch.object(ReposApi, "get_repo_id", return_value=None)
        mocker.patch.object(ReposApi, "create", return_value={"id": "456"})
        mocker.patch(
            "pushcart_deploy.configuration.get_config_from_file",
            return_value={"key": "value"},
        )
        mocker.patch.object(WorkspaceApi, "__init__", return_value=None)
        workspace_mkdirs = mocker.patch.object(
            WorkspaceApi, "mkdirs", return_value=None
        )

        repo_id = ReposWrapper(mock_api_client, "./tests/data").get_or_create_repo()

        workspace_mkdirs.assert_called_once_with(workspace_path="/Repos/pushcart")

        assert repo_id == "456"

    def test_get_existing_repo_success(self, mocker, mock_api_client):
        """Tests that the get_or_create_repo method successfully retrieves an existing repository."""
        mocker.patch.object(ReposApi, "__init__", return_value=None)
        mocker.patch.object(ReposApi, "get_repo_id", return_value="123")

        repo_id = ReposWrapper(mock_api_client, "./tests/data").get_or_create_repo()
        assert repo_id == "123"

    def test_update_happy_path(self, mocker, mock_api_client):
        """Tests that update updates repository with new branch."""
        mocker.patch.object(ReposApi, "update", return_value=None)
        repos_wrapper = ReposWrapper(mock_api_client, "./tests/data")
        repos_wrapper.repo_id = "123"

        repos_wrapper.update()

        ReposApi.update.assert_called_once_with(repo_id="123", branch="main", tag=None)

    def test_update_invalid_repo(self, mock_api_client):
        """Tests that the update method raises a ValueError if attempting to update
        repository before initializing with get_or_create_repo.
        """
        with pytest.raises(ValueError):
            ReposWrapper(mock_api_client, "./tests/data").update()

    def test_update_invalid_input(self, mock_api_client):
        """Tests that the update method raises a ValueError if the repo was not
        initialized.
        """
        repos_wrapper = ReposWrapper(mock_api_client, "./tests/data")
        repos_wrapper.repo_id = None

        with pytest.raises(ValueError):
            repos_wrapper.update()
