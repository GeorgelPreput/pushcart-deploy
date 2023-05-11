import logging

import click
from databricks_cli.configure.config import provide_api_client
from databricks_cli.sdk.api_client import ApiClient
from pydantic import DirectoryPath, dataclasses

from pushcart_deploy import Metadata


@dataclasses.dataclass
class Setup:
    config_dir: DirectoryPath

    @provide_api_client
    def __post_init_post_parse__(self, api_client: ApiClient) -> None:
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

        self.log.info(f"Deploying Pushcart to Databricks Workspace: {api_client.url}")

    def deploy(self) -> None:
        metadata = Metadata(self.config_dir)
        metadata.create_backend_objects()


@click.command()
@click.option("--config-dir", "-c", help="Deployment configuration directory path")
@click.option("--profile", "-p", help="Databricks CLI profile to use (optional)")
def deploy(
    config_dir: str,
    profile: str = None,  # Derived from context by @provide_api_client  # noqa: ARG001
) -> None:
    d = Setup(config_dir)
    d.deploy()


if __name__ == "__main__":
    deploy(auto_envvar_prefix="PUSHCART")
