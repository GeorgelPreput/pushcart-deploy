"""Setup module for deploying Pushcart configuration files.

Example:
-------
    ```bash
    pushcart-deploy --config-dir ~/source/pushcart-config
    ```

    ```python
    setup = Setup("~/source/pushcart-config")
    setup.deploy()
    ```

Notes:
-----
Can be run from the command line, or from within a Python context.
Requires Databricks CLI to already be configured for your target Databricks environment

"""

import logging

import click
from databricks_cli.configure.config import provide_api_client
from databricks_cli.sdk.api_client import ApiClient
from dotenv import load_dotenv
from pydantic import DirectoryPath, dataclasses

from pushcart_deploy import Metadata
from pushcart_deploy.databricks_api import (
    JobsWrapper,
    PipelinesWrapper,
    ReposWrapper,
    Scheduler,
)


@dataclasses.dataclass
class Setup:
    """Runs a Pushcart deployment."""

    config_dir: DirectoryPath
    non_destructive: bool

    @provide_api_client
    def __post_init_post_parse__(self, api_client: ApiClient) -> None:
        """Initialize logger.

        Parameters
        ----------
        api_client : ApiClient
            Used to log target Databricks environment URL
        """
        logging.basicConfig(level=logging.INFO)
        self.log = logging.getLogger(__name__)

        self.jobs_wrapper = JobsWrapper(api_client)
        self.pipelines_wrapper = PipelinesWrapper(api_client, self.config_dir)
        self.metadata = Metadata(self.config_dir)
        self.scheduler = Scheduler(api_client, self.config_dir)
        self.repos = ReposWrapper(api_client, self.config_dir)

        if load_dotenv(dotenv_path=self.config_dir / "setup" / ".env"):
            self.log.info("Loaded environment variables from file")

        self.log.info(f"Deploying Pushcart to Databricks Workspace: {api_client.url}")

    def deploy(self) -> None:
        """Start a deployment of Pushcart data pipeline configurations."""
        _ = self.repos.get_or_create_git_credentials()
        repo_id = self.repos.get_or_create_repo()
        self.repos.update()

        self.metadata.create_backend_objects()

        metadata_pipelines = self.metadata.get_pipeline_list_from_backend_objects()
        workflows_pipelines = self.pipelines_wrapper.get_pipelines_list()
        workflows_jobs = self.jobs_wrapper.get_jobs_list()

        if not self.non_destructive:
            obsolete_pipelines = self.scheduler.get_obsolete_pipelines_list(
                metadata_pipelines,
                workflows_pipelines,
            )
            self.scheduler.delete_obsolete_pipelines(obsolete_pipelines)
            self.scheduler.delete_obsolete_jobs(obsolete_pipelines, workflows_jobs)

        new_pipelines = self.scheduler.get_new_pipelines_list(
            metadata_pipelines,
            workflows_pipelines,
        )
        existing_pipelines = self.scheduler.get_matching_pipelines_list(
            metadata_pipelines,
            workflows_pipelines,
        )
        all_pipelines = self.scheduler.create_or_update_pipelines(
            repo_id,
            new_pipelines + existing_pipelines,
        )
        self.scheduler.create_or_update_jobs(all_pipelines)


@click.command()
@click.option("--config-dir", "-c", help="Deployment configuration directory path")
@click.option("--profile", "-p", help="Databricks CLI profile to use (optional)")
@click.option(
    "--nd",
    is_flag=True,
    show_default=True,
    default=False,
    help="Non destructive mode, does not delete unrecognized pipelines / jobs",
)
def deploy(
    config_dir: str,
    nd: bool,
    profile: str = None,  # Derived from context by @provide_api_client  # noqa: ARG001
) -> None:
    """Run a Pushcart deployment from CLI.

    Parameters
    ----------
    config_dir : str
        Root directory where the Pushcart configuration files reside.
    profile : str, optional
        Databricks CLI profile to be used, by default None
    nd: bool, optional
        Non destructive mode, does not delete pipelines and jobs not found in metadata
    """
    d = Setup(config_dir=config_dir, non_destructive=nd)
    d.deploy()


if __name__ == "__main__":
    deploy(auto_envvar_prefix="PUSHCART")
