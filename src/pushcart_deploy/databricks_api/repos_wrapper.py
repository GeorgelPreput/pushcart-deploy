"""Create or sync a Git repository containing Pushcart configuration files.

Wrapper class around the Databricks Repos API.

Example:
-------
    repos_wrapper = ReposWrapper(api_client)
    repos_wrapper.get_or_create_repo("pushcart", "https://github.com/GeorgelPreput/pushcart-config")
    repos_wrapper.update("pushcart", "main")

Notes:
-----
Needs a Databricks CLI ApiClient to be configured and connected to a Databricks
environment.

"""

import logging
import re
from pathlib import Path

from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import HttpUrl, constr, dataclasses, validate_arguments, validator
from requests.exceptions import HTTPError

from pushcart_deploy.validation import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class ReposWrapper:
    """Wrapper around the Databricks Repos API.

    Allows users to get or create a repository, update the repository with a new
    branch, and detect the Git provider from a given URL.

    Returns
    -------
    ReposWrapper
        Wrapper object to sync a Pushcart configurations repo to the environment.

    Raises
    ------
    ValueError
        Git provider must be provided explicitly, unless it can be derived from the repo URL.
    ValueError
        Can only update a repo that has been initialized.
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

        self.repos_api = ReposApi(self.client)
        self.repo_id = None

    @staticmethod
    @validate_arguments
    def _detect_git_provider(repo_url: str) -> str:
        """Detect the Git provider from a given URL."""
        providers = {
            "gitHub": r"(?:https?://|git@)github\.com[:/]",
            "bitbucketCloud": r"(?:https?://|git@)bitbucket\.org[:/]",
            "gitLab": r"(?:https?://|git@)gitlab\.com[:/]",
            "azureDevOpsServices": r"(?:https?://|git@ssh?\.?)([\w-]+@)?\.?dev\.azure\.com[:/]",
            "gitHubEnterprise": r"(?:https?://|git@)([\w-]+)\.github(?:usercontent)?\.com[:/]",
            "bitbucketServer": r"(?:https?://|git@)([\w-]+)\.bitbucket(?:usercontent)?\.com[:/]",
            "gitLabEnterpriseEdition": r"(?:https?://|git@)([\w-]+)\.gitlab(?:usercontent)?\.com[:/]",
            "awsCodeCommit": r"(?:https?://|git@)git-codecommit\.[^/]+\.amazonaws\.com[:/]",
        }

        for provider, regex in providers.items():
            if re.match(regex, repo_url):
                return provider

        msg = "Could not detect Git provider from URL. Please specify git_provider explicitly."
        raise ValueError(msg)

    @validate_arguments
    def get_or_create_repo(
        self,
        repo_user: constr(min_length=1, strict=True, regex=r"^[^'\"]*$"),
        git_url: HttpUrl,
        git_provider: constr(
            min_length=1,
            strict=True,
            regex="^(gitHub|bitbucketCloud|gitLab|azureDevOpsServices|gitHubEnterprise|bitbucketServer|gitLabEnterpriseEdition|awsCodeCommit)$",
        )
        | None = None,
    ) -> str:
        """Get or create a repository with a given user, Git URL and Git provider (if not detected from URL)."""
        if not git_provider:
            self.log.warning(
                "No Git provider specified. Attempting to guess based on URL.",
            )
            git_provider = self._detect_git_provider(git_url)

        git_repo = git_url.split("/")[-1].replace(".git", "")

        repo_path = (Path("/Repos") / repo_user / git_repo).as_posix()
        try:
            self.repo_id = self.repos_api.get_repo_id(path=repo_path)
        except (HTTPError, ValueError, RuntimeError):
            self.log.warning("Failed to get repo ID")

        if not self.repo_id:
            self.log.warning(f"Repo not found, cloning from URL: {git_url}")

            repo = self.repos_api.create(git_url, git_provider, repo_path)
            self.repo_id = repo["id"]

        self.log.info(f"Repository ID: {self.repo_id}")

        return self.repo_id

    @validate_arguments
    def update(
        self,
        git_branch: constr(min_length=1, strict=True, regex=r"^[^'\"]*$"),
    ) -> None:
        """Update the Databricks repository with a new branch."""
        if not self.repo_id:
            msg = "Repo not initialized. Please first run get_or_create_repo()"
            raise ValueError(
                msg,
            )

        # TODO: Support Git tags as well
        self.repos_api.update(self.repo_id, branch=git_branch, tag=None)
