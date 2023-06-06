"""Helper module handling pipeline and job sets."""

import logging

from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import DirectoryPath, dataclasses, validate_arguments

from pushcart_deploy.databricks_api import JobsWrapper, PipelinesWrapper
from pushcart_deploy.databricks_api.settings import JobSettings, PipelineSettings
from pushcart_deploy.validation import PydanticArbitraryTypesConfig


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class Scheduler:
    """Helper class for handling DLT pipelines and job scheduling.

    Defines logic for determining which pipelines and associated jobs are new, obsolete
    or in need of updating during a release process.
    """

    api_client: ApiClient
    config_dir: DirectoryPath

    def __post_init_post_parse__(self) -> None:
        """Initialize the logger and create instances of needed classes."""
        self.log = logging.getLogger(__name__)
        self.jobs_wrapper = JobsWrapper(self.api_client)
        self.pipelines_wrapper = PipelinesWrapper(self.api_client, self.config_dir)
        self.repos_api = ReposApi(self.api_client)

    @validate_arguments
    def get_obsolete_pipelines_list(
        self,
        metadata_pipelines: list,
        workflows_pipelines: list,
    ) -> list:
        """Get a list of obsolete pipelines.

        This method checks a list of currently scheduled pipelines against a list of
        pipelines from metadata. It returns a list of pipelines that are found in the
        scheduled list but not in the metadata list, i.e., obsolete pipelines. Please
        note that this solution assumes that pipeline_name is unique across all
        dictionaries in both metadata_pipelines and scheduled_pipelines.

        Parameters
        ----------
        metadata_pipelines : list
            List of dictionaries representing the current pipelines in the metadata.

        workflows_pipelines : list
            List of dictionaries representing the pipelines in Databricks Workflows.

        Returns
        -------
        list
            List of dictionaries representing obsolete pipelines.
            [{
                "pipeline_name": pipeline_name,
                "pipeline_id": pipeline_id
            }, ...]
        """
        metadata_pipelines_set = {
            pipeline["pipeline_name"] for pipeline in metadata_pipelines
        }

        return [
            pipeline
            for pipeline in workflows_pipelines
            if pipeline["pipeline_name"] not in metadata_pipelines_set
        ]

    @validate_arguments
    def get_new_pipelines_list(
        self,
        metadata_pipelines: list,
        workflows_pipelines: list,
    ) -> list:
        """Get a list of new pipelines.

        This function checks a list of pipelines from metadata against a list of
        currently scheduled pipelines. It returns a list of pipelines that are found in
        the metadata list but not in the scheduled list, i.e., new pipelines. Please
        note that this solution assumes that pipeline_name is unique across all
        dictionaries in both metadata_pipelines and scheduled_pipelines.

        Parameters
        ----------
        metadata_pipelines : list
            List of dictionaries representing the current pipelines in the metadata.

        workflows_pipelines : list
            List of dictionaries representing the scheduled pipelines.

        Returns
        -------
        list
            List of dictionaries representing the new pipelines.
            [{
                "target_schema_name": target_schema_name,
                "pipeline_name": pipeline_name,
                "pipeline_id": None,
            }, ...]
        """
        workflows_pipelines_set = {
            pipeline["pipeline_name"] for pipeline in workflows_pipelines
        }

        return [
            pipeline
            for pipeline in metadata_pipelines
            if pipeline["pipeline_name"] not in workflows_pipelines_set
        ]

    @validate_arguments
    def get_matching_pipelines_list(
        self,
        metadata_pipelines: list,
        workflows_pipelines: list,
    ) -> list:
        """Get a list of matching pipelines.

        This function checks a list of pipelines from metadata against a list of
        currently scheduled pipelines. It returns a list of pipelines that are found
        in both the metadata list and the scheduled list, i.e., matching pipelines.
        Please note that this solution assumes that pipeline_name is unique across all
        dictionaries in both metadata_pipelines and scheduled_pipelines.

        Parameters
        ----------
        metadata_pipelines : list
            List of dictionaries representing the current pipelines in the metadata.

        workflows_pipelines : list
            List of dictionaries representing the scheduled pipelines.

        Returns
        -------
        list
            List of dictionaries representing the matching pipelines, in the format
            [{
                "target_catalog_name": target_catalog_name,
                "target_schema_name": target_schema_name,
                "pipeline_name": pipeline_name,
                "pipeline_id": pipeline_id,
            }, ...]
        """
        scheduled_dict = {
            d["pipeline_name"]: d["pipeline_id"] for d in workflows_pipelines
        }

        matching_pipelines = []

        for pipeline in metadata_pipelines:
            pipeline_name = pipeline["pipeline_name"]
            if pipeline_name in scheduled_dict:
                matching_pipeline = {
                    "target_catalog_name": pipeline["target_catalog_name"],
                    "target_schema_name": pipeline["target_schema_name"],
                    "pipeline_name": pipeline_name,
                    "pipeline_id": scheduled_dict[pipeline_name],
                }
                matching_pipelines.append(matching_pipeline)

        return matching_pipelines

    @validate_arguments
    def delete_obsolete_pipelines(self, obsolete_pipelines: list) -> None:
        """Remove all obsolete DLT pipelines from Workflows.

        Parameters
        ----------
        obsolete_pipelines : list
            List of pipeline IDs not found anymore in the metadata tables
        """
        self.log.info("Removing obsolete pipelines")
        for p in obsolete_pipelines:
            self.pipelines_wrapper.delete_pipeline(pipeline_id=p["pipeline_id"])

    @validate_arguments
    def create_or_update_pipelines(
        self,
        repo_id: str,
        metadata_pipelines: list,
    ) -> list:
        """Update or create new DLT pipelines from a list of pipelines available in the metadata.

        Parameters
        ----------
        repo_id : str
            ID of Databricks Repos used to point to the appropriate Pipeline notebook
        metadata_pipelines : list
            List of pipelines available in metadata, in the form of
            [{
                "target_catalog_name": target_catalog_name,
                "target_schema_name": target_schema_name,
                "pipeline_name": pipeline_name,
                "pipeline_id": pipeline_id | None,
            }, ...]

        Returns
        -------
        list
            List of created DLT pipelines, in the form of
            [{
                "target_catalog_name": target_catalog_name,
                "target_schema_name": target_schema_name,
                "pipeline_name": pipeline_name,
                "pipeline_id": pipeline_id,
            }, ...]
        """
        pipelines = []
        repo_path = self.repos_api.get(repo_id)["path"]

        self.log.info("Creating new DLT pipelines")
        for pipeline in metadata_pipelines:
            libraries = [
                {
                    "notebook": {"path": f"{repo_path}/runner/pipeline"},
                },
            ]
            configuration = {
                "pushcart.pipeline_name": pipeline["pipeline_name"],
            }

            if not (pipeline_id := pipeline["pipeline_id"]):
                pipeline_id = self.pipelines_wrapper.get_pipeline_id(
                    pipeline["pipeline_name"],
                )

            pipeline_settings = PipelineSettings(
                self.api_client,
                self.config_dir,
            ).load_pipeline_settings(
                target_catalog_name=pipeline["target_catalog_name"],
                target_schema_name=pipeline["target_schema_name"],
                pipeline_name=pipeline["pipeline_name"],
                libraries=libraries,
                configuration=configuration,
                pipeline_id=pipeline_id,
            )

            if not pipeline_id:
                self.log.warning(
                    f"Pipeline not found: {pipeline['pipeline_name']}. Creating a new one",
                )
                pipeline_id = self.pipelines_wrapper.create_pipeline(
                    pipeline_settings,
                    repo_path,
                )
            else:
                pipeline_id = self.pipelines_wrapper.update_pipeline(
                    pipeline_settings,
                    repo_path,
                )

            pipelines.append(
                {
                    "target_catalog_name": pipeline["target_catalog_name"],
                    "target_schema_name": pipeline["target_schema_name"],
                    "pipeline_name": pipeline["pipeline_name"],
                    "pipeline_id": pipeline_id,
                },
            )

        return pipelines

    @validate_arguments
    def create_or_update_jobs(self, pipelines: list) -> list:
        """Create or update jobs for a list of DLT pipelines.

        Parameters
        ----------
        pipelines : list
            List of DLT pipelines, in the form of [{
                "target_schema_name": target_schema_name,
                "target_catalog_name": target_catalog_name,
                "pipeline_name": pipeline_name,
                "pipeline_id": pipeline_id,
            }, ...]

        Returns
        -------
        list
            Job IDs for updated or newly created Databricks Workflows jobs
        """
        jobs = []

        self.log.info("(Re)scheduling pipelines")

        for p in pipelines:
            job_settings = JobSettings(self.config_dir).load_job_settings(
                target_catalog_name=p["target_catalog_name"],
                target_schema_name=p["target_schema_name"],
                pipeline_name=p["pipeline_name"],
                pipeline_id=p["pipeline_id"],
            )

            job_id = self.jobs_wrapper.get_job_id(p["pipeline_name"])

            if not job_id:
                self.log.warning(
                    f"Job not found: {p['pipeline_name']}. Creating a new one",
                )
                jobs.append(self.jobs_wrapper.create_job(job_settings))
            else:
                jobs.append(self.jobs_wrapper.update_job(job_id, job_settings))

        return jobs

    def delete_obsolete_jobs(
        self,
        obsolete_pipelines: list,
        workflows_jobs: list,
    ) -> None:
        """Remove jobs that schedule obsolete pipelines.

        Parameters
        ----------
        obsolete_pipelines : list
            List of dictionaries representing obsolete pipelines in the format
            [{ "pipeline_name": pipeline_name,
               "pipeline_id": pipeline_id
            }, ...]
        workflows_jobs : list
            List of jobs scheduled in Databricks Workflows, having the format
            [{ "job_name": job_name,
               "job_id": job_id,
               "pipeline_id": pipeline_id | None
            }, ...]
        """
        for p in obsolete_pipelines:
            for j in workflows_jobs:
                if (
                    j["name"] == p["pipline_name"]
                    or j["pipeline_id"] == p["pipeline_id"]
                ):
                    self.jobs_wrapper.delete_job(j["job_id"])
