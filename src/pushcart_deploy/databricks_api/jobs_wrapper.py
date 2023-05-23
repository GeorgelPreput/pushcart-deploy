"""Manage Pushcart data pipeline jobs.

Wrapper class around the Databricks Jobs API, creating, retrieving, updating and
deleting jobs.

Example:
-------
    jobs_wrapper = JobsWrapper(api_client)
    checkpoint_job = jobs_wrapper.get_or_create_checkpoint_job("/path/to/settings.json")
    jobs_wrapper.run(checkpoint_job)
    jobs_wrapper.delete(checkpoint_job)

Notes:
-----
Needs a Databricks CLI ApiClient to be configured and connected to a Databricks
environment.

"""

import logging
from pathlib import Path
from time import sleep

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import dataclasses, validate_arguments, validator

from pushcart_deploy.databricks_api.job_settings import JobSettings
from pushcart_deploy.validation import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class JobsWrapper:
    """Manages Databricks jobs.

    Provides methods for creating, retrieving, and deleting jobs, as well as for
    running jobs and retrieving their status. Uses the JobSettings class to load
    job settings from a JSON file or string, or to retrieve default job settings
    for checkpoint, pipeline, and release jobs.
    """

    client: ApiClient

    @validator("client")
    @classmethod
    def check_api_client(cls, value: ApiClient) -> ApiClient:
        """Validate the provided ApiClient."""
        return validate_databricks_api_client(value)

    def __post_init_post_parse__(self) -> None:
        """Initialize the logger instance and creates instances of JobsApi and RunsApi."""
        self.log = logging.getLogger(__name__)

        self.jobs_api = JobsApi(self.client)
        self.runs_api = RunsApi(self.client)

    @validate_arguments
    def get_or_create_checkpoint_job(self, settings_json: Path | None = None) -> str:
        """Retrieve or create a checkpoint job using the provided job settings."""
        job_settings = JobSettings(self.client).load_job_settings(
            settings_json,
            "checkpoint",
        )

        return self.get_or_create_job(job_settings)

    def _get_job(self, job_name: str) -> list:
        """Retrieve a job by name."""
        jobs = self.jobs_api.list_jobs().get("jobs", [])
        jobs_filtered = [j for j in jobs if j["settings"]["name"] == job_name]

        return jobs_filtered[0]["job_id"] if jobs_filtered else None

    def _create_job(self, job_settings: dict) -> str:
        """Create a new job using the provided job settings."""
        job = self.jobs_api.create_job(job_settings)
        self.log.info(f"Created job {job_settings['name']} with ID: {job['job_id']}")

        return job["job_id"]

    @validate_arguments
    def get_or_create_job(self, job_settings: dict) -> str:
        """Retrieve or create a job using the provided job settings."""
        job_name = job_settings.get("name")

        if not job_name:
            msg = "Please provide a job name in the job settings"
            raise ValueError(msg)

        job_id = self._get_job(job_name)

        if not job_id:
            self.log.warning(f"Job not found: {job_name}. Creating a new one.")
            return self._create_job(job_settings)

        self.jobs_api.reset_job({"job_id": job_id, "new_settings": job_settings})

        self.log.info(f"Job ID: {job_id}")

        return job_id

    @validate_arguments
    def run_job(self, job_id: str) -> tuple[str, str]:
        """Run a job and retrieve its status."""
        run_id = self.jobs_api.run_now(
            job_id=job_id,
            jar_params=None,
            notebook_params=None,
            python_params=None,
            spark_submit_params=None,
        )["run_id"]

        job_status = "PENDING"
        job = {}

        while job_status in ["PENDING", "RUNNING"]:
            sleep(2)
            job = self.runs_api.get_run(run_id)
            job_status = job["state"]["life_cycle_state"]

            self.log.info(f"Job is {job_status}: {job['run_page_url']}")

        return (
            job["state"].get("result_state", job["state"]["life_cycle_state"]),
            job["run_page_url"],
        )

    @validate_arguments
    def delete_job(self, job_id: str) -> None:
        """Delete a job by ID."""
        job_name = self.jobs_api.get_job(job_id=job_id)["settings"]["name"]
        self.jobs_api.delete_job(job_id=job_id)

        self.log.info(f"Deleted job {job_name} ({job_id})")
