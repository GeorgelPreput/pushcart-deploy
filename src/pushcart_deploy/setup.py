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
from pydantic import DirectoryPath, dataclasses

from pushcart_deploy import Metadata


@dataclasses.dataclass
class Setup:
    """Runs a Pushcart deployment."""

    config_dir: DirectoryPath

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

        self.log.info(f"Deploying Pushcart to Databricks Workspace: {api_client.url}")

    def deploy(self) -> None:
        """Start a deployment of Pushcart data pipeline configurations."""
        metadata = Metadata(self.config_dir)
        metadata.create_backend_objects()


@click.command()
@click.option("--config-dir", "-c", help="Deployment configuration directory path")
@click.option("--profile", "-p", help="Databricks CLI profile to use (optional)")
def deploy(
    config_dir: str,
    profile: str = None,  # Derived from context by @provide_api_client  # noqa: ARG001
) -> None:
    """Run a Pushcart deployment from CLI.

    Parameters
    ----------
    config_dir : str
        Root directory where the Pushcart configuration files reside.
    profile : str, optional
        Databricks CLI profile to be used, by default None
    """
    d = Setup(config_dir)
    d.deploy()


if __name__ == "__main__":
    deploy(auto_envvar_prefix="PUSHCART")
