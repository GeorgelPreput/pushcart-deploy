"""Manages Pushcart DLT pipelines.

Wrapper class around the Databricks Delta Live Tables API, creating, retrieving,
updating and deleting jobs.

Example:
-------
    pipelines_wrapper = PipelinesWrapper(api_client)
    pipeline_id = pipelines_wrapper.create_pipeline(pipeline_settings_dict)

    if found_pipeline_id := pipelines_wrapper.get_pipeline(pipeline_name):
        pipelines_wrapper.update_pipeline(found_pipeline_id, pipeline_settings_dict)
        pipelines_wrapper.delete(found_pipeline_id)

Notes:
-----
Needs a Databricks CLI ApiClient to be configured and connected to a Databricks
environment.

"""
import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.dbutils import RemoteDbUtils
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import DirectoryPath, dataclasses, validate_call

from pushcart_deploy.validation import PydanticArbitraryTypesConfig


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class PipelinesWrapper:
    """Manages Databricks DLT Pipelines.

    Provides methods for creating, updating and deleting DLT pipelines.
    """

    api_client: ApiClient
    config_dir: DirectoryPath

    def __post_init__(self) -> None:
        """Initialize the logger instance and create an instance of PipelinesApi."""
        self.log = logging.getLogger(__name__)

        self.workspace = WorkspaceClient()
        self.pipelines_api = PipelinesApi(self.api_client)
        self.dbutils = RemoteDbUtils()

    def get_pipelines_list(self) -> list:
        """Get a list of all the pipelines currently defined in Databricks Workflows.

        Returns
        -------
        list
            a list of dict containing pipeline ids and names, e.g. [ { pipeline_name: pipeline_id }, ... ]
        """
        pipelines = self.workspace.pipelines.list_pipelines()

        return [
            {
                "name": p.name,
                "pipeline_id": p.pipeline_id,
            }
            for p in pipelines
        ]

    def get_pipeline_id(self, pipeline_name: str) -> str:
        """Retrieve a pipeline ID by name.

        Parameters
        ----------
        pipeline_name : str
            Name of DLT pipeline as shown in Databricks Workflows.

        Returns
        -------
        str
            Pipeline ID
        """
        pipelines_filtered = {
            p["name"]: p["pipeline_id"] for p in self.get_pipelines_list()
        }

        return pipelines_filtered.get(pipeline_name)

    @validate_call
    def create_pipeline(self, pipeline_settings: dict, repo_path: str) -> str:
        """Create a DLT pipeline using the provided settings.

        Parameters
        ----------
        pipeline_settings : dict
            Settings dictionary for DLT pipelines as described in the Databricks Delta
            Live Tables API specification
        repo_path : str
            Databrics Repos path, passed as settings_dir to API

        Returns
        -------
        str
            String containing the ID for the newly-created DLT pipeline
        """
        pipeline = self.pipelines_api.create(
            pipeline_settings,
            repo_path,
            allow_duplicate_names=False,
        )

        self.log.info(
            f"Created pipeline {pipeline_settings['name']} with ID: {pipeline['pipeline_id']}",
        )
        return pipeline["pipeline_id"]

    @validate_call
    def update_pipeline(self, pipeline_settings: dict, repo_path: str) -> str:
        """Update an existing pipeline with new settings.

        Parameters
        ----------
        pipeline_settings : dict
            Dictionary holding pipeline settings as per Databricks Delta Live Tables
            API specification.
        repo_path : str
            Databrics Repos path, passed as settings_dir to API

        Returns
        -------
        str
            ID of pipeline that has been updated.
        """
        self.pipelines_api.edit(
            pipeline_settings,
            repo_path,
            allow_duplicate_names=False,
        )
        self.log.info(
            f"Updated pipeline {pipeline_settings['name']} with ID: {pipeline_settings['id']}",
        )
        return pipeline_settings["id"]

    @validate_call
    def delete_pipeline(self, pipeline_id: str) -> None:
        """Delete a pipeline from DLT workflows.

        Parameters
        ----------
        pipeline_id : str
            Pipeline ID as found in Databricks Workflows Delta Live Tables
        """
        self.pipelines_api.delete(pipeline_id=pipeline_id)
        self.dbutils.fs.rm(f"dbfs:/pipelines/{pipeline_id}", recurse=True)
        self.log.info(f"Deleted pipeline {pipeline_id}")
