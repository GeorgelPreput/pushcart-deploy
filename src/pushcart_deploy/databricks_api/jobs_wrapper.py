"""Manage Pushcart data pipeline jobs.

Wrapper class around the Databricks Jobs API, creating, retrieving, updating and
deleting jobs.

Example:
-------
    jobs_wrapper = JobsWrapper(api_client)
    job_id = jobs_wrapper.create_job(job_settings_dict)

    if found_job_id := jobs_wrapper.get_job(job_name):
        jobs_wrapper.update_job(found_job_id, job_settings_dict)
        jobs_wrapper.delete_job(found_job_id)

Notes:
-----
Needs a Databricks CLI ApiClient to be configured and connected to a Databricks
environment.

"""

import logging

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import dataclasses, validate_call

from pushcart_deploy.validation import PydanticArbitraryTypesConfig


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class JobsWrapper:
    """Manages Databricks jobs.

    Provides methods for creating, retrieving, and deleting jobs, as well as for
    running jobs and retrieving their status. Uses the JobSettings class to load
    job settings from a JSON file or string, or to retrieve default job settings
    for checkpoint, pipeline, and release jobs.
    """

    api_client: ApiClient

    def __post_init__(self) -> None:
        """Initialize the logger instance and creates instances of JobsApi and RunsApi."""
        self.log = logging.getLogger(__name__)

        self.jobs_api = JobsApi(self.api_client)
        self.runs_api = RunsApi(self.api_client)

    def get_jobs_list(self) -> list:
        """Get a list of jobs scheduled in Databricks Workflows.

        Returns
        -------
        list
            List of jobs, having the format [{ job_name: job_id }, ...]
        """
        return [
            {
                "job_name": job["settings"]["name"],
                "job_id": job["job_id"],
                "pipeline_id": job["settings"]["tasks"][0]
                .get("pipeline_job", {})
                .get("pipeline_id", None),
            }
            for job in self.jobs_api.list_jobs().get("jobs", [])
            if job["settings"].get("tasks")
        ]

    def get_job_id(self, job_name: str) -> str:
        """Retrieve a job ID by name."""
        jobs = self.jobs_api.list_jobs().get("jobs", [])
        jobs_filtered = [j for j in jobs if j["settings"]["name"] == job_name]

        return jobs_filtered[0]["job_id"] if jobs_filtered else None

    def create_job(self, job_settings: dict) -> str:
        """Create a new job using the provided job settings."""
        job = self.jobs_api.create_job(job_settings)
        self.log.info(f"Created job {job_settings['name']} with ID: {job['job_id']}")

        return job["job_id"]

    def update_job(self, job_id: str, job_settings: dict) -> str:
        """Update a job using the provided job settings."""
        self.jobs_api.reset_job({"job_id": job_id, "new_settings": job_settings})
        self.log.info(f"Updated job {job_settings['name']} with ID: {job_id}")

        return job_id

    @validate_call
    def delete_job(self, job_id: str) -> None:
        """Delete a job by ID.

        Parameters
        ----------
        job_id : str
            ID of Databricks Workflows job to be deleted
        """
        job_name = self.jobs_api.get_job(job_id=job_id)["settings"]["name"]
        self.jobs_api.delete_job(job_id=job_id)

        self.log.info(f"Deleted job {job_name} ({job_id})")
