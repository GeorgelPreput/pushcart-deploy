"""Create or sync a Git repository containing Pushcart configuration files.

Wrapper class around the Databricks Repos API.

Example:
-------
    repos_wrapper = ReposWrapper(api_client, local_config_dir_path)
    repos_wrapper.get_or_create_git_credentials()
    repos_wrapper.get_or_create_repo()
    repos_wrapper.update()

Notes:
-----
Needs a Databricks CLI ApiClient to be configured and connected to a Databricks
environment.

"""
import asyncio
import logging
import os
from pathlib import Path

from databricks.sdk import GitCredentialsAPI
from databricks.sdk.core import ApiClient as SdkApiClient
from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi
from pydantic import DirectoryPath, dataclasses
from requests.exceptions import HTTPError

from pushcart_deploy.configuration import expect_at_most_one_file, get_config_from_file
from pushcart_deploy.validation import PydanticArbitraryTypesConfig


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class ReposWrapper:
    """Wrapper around the Databricks Repos API.

    Allows users to get or create a repository, update the repository with a new
    branch, and detect the Git provider from a given URL. Also handles Git
    credentials.
    """

    api_client: ApiClient
    config_dir: DirectoryPath

    def __post_init__(self) -> None:
        """Initialize logger and object configuration."""
        self.log = logging.getLogger(__name__)

        self.workspace_api = WorkspaceApi(self.api_client)
        self.repos_api = ReposApi(self.api_client)
        self.git_creds = GitCredentialsAPI(SdkApiClient())
        self.repo_id = None

        settings_path = expect_at_most_one_file(
            self.config_dir / "setup" / "pushcart-deploy",
        )
        self.settings = asyncio.run(get_config_from_file(settings_path))

    def get_or_create_git_credentials(self) -> str:
        """Check if Git credentials exist in Databricks Repos and create them if not.

        Returns
        -------
        str
            Unique ID for Git credential object in Databricks Environment.
        """
        git_username = os.environ[self.settings["git_username_envvar"]]
        git_token = os.environ[self.settings["git_token_envvar"]]

        existing_creds = [
            c for c in self.git_creds.list() if c.git_username == git_username
        ]

        if existing_creds:
            self.log.info(
                f"Found existing Git credentials for user {existing_creds[0].git_username}",
            )
            return existing_creds[0].credential_id

        new_creds = self.git_creds.create(
            git_provider=self.settings["git_provider"],
            git_username=git_username,
            personal_access_token=git_token,
        )
        self.log.info(f"Created Git credentials for user {new_creds.git_username}")

        return new_creds.credential_id

    def get_or_create_repo(self) -> str:
        """Get or create a repository with a given user, Git URL and Git provider (if not detected from URL)."""
        git_repo = self.settings["git_url"].split("/")[-1].replace(".git", "")
        repo_path = (Path("/Repos") / self.settings["repos_user"] / git_repo).as_posix()

        try:
            self.repo_id = self.repos_api.get_repo_id(path=repo_path)
        except (HTTPError, ValueError, RuntimeError):
            self.log.warning("Failed to get repo ID")

        if not self.repo_id:
            self.log.warning(
                f"Repo not found, cloning from URL: {self.settings['git_url']}",
            )

            self.workspace_api.mkdirs(
                workspace_path=f"/Repos/{self.settings['repos_user']}",
            )

            repo = self.repos_api.create(
                self.settings["git_url"],
                self.settings["git_provider"],
                repo_path,
            )
            self.repo_id = repo["id"]

        self.log.info(f"Repository ID: {self.repo_id}")

        return self.repo_id

    def update(self) -> None:
        """Update the Databricks repository with a new branch."""
        if not self.repo_id:
            msg = "Repo not initialized. Please first run get_or_create_repo()"
            raise ValueError(msg)

        # TODO: Support Git tags as well
        self.repos_api.update(
            repo_id=self.repo_id,
            branch=self.settings["git_branch"],
            tag=None,
        )
