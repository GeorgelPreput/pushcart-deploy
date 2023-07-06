"""Load Databricks job settings for a data pipeline from file or from defaults.

Job settings may come in JSON, TOML or YAML formats when loaded from file

Example:
-------
    job_settings = JobSettings(api_client)
    settings_from_file = job_settings.load_job_settings(settings_path="/path/to/pipeline.json")
    default_settings = job_settings.load_job_settings(default_settings="checkpoint")

Notes:
-----
Needs a Databricks CLI ApiClient to be configured and connected to a Databricks
environment.

"""

import logging

from pydantic import DirectoryPath, dataclasses, validate_call

from pushcart_deploy.databricks_api.settings import BaseSettings
from pushcart_deploy.validation import PydanticArbitraryTypesConfig


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class JobSettings:
    """Manages job settings for Databricks jobs.

    Provides methods for loading job settings from a JSON file or string, as well as
    for retrieving default job settings for checkpoint and pipeline jobs.
    """

    config_dir: DirectoryPath

    def __post_init__(self) -> None:
        """Initialize logger and BaseSettings."""
        self.log = logging.getLogger(__name__)
        self.base_settings = BaseSettings(self.config_dir)

    def _update_job_settings(
        self,
        job_settings: dict,
        pipeline_name: str,
        pipeline_id: str,
    ) -> None:
        """Dynamically update job settings with data about current pipeline.

        Parameters
        ----------
        job_settings : dict
            Job settings template to be filled in, whether loaded from file or default.
        pipeline_name : str
            Pipeline name, as displayed in the DLT Workflows. Must be unique.
        pipeline_id : str
            ID of an existing pipeline.
        """
        job_settings["name"] = pipeline_name
        job_settings["tasks"][0]["task_key"] = pipeline_name
        job_settings["tasks"][0]["pipeline_task"]["pipeline_id"] = pipeline_id

    @validate_call
    def load_job_settings(
        self,
        target_catalog_name: str,
        target_schema_name: str,
        pipeline_name: str,
        pipeline_id: str,
    ) -> dict:
        """Load job settings from a file, or retrieve default job settings if none are provided.

        Parameters
        ----------
        target_schema_name : str
            A schema name for persisting pipeline output data.
        target_catalog_name : str
            A catalog name for persisting pipeline output data.
        pipeline_name : str
            Pipeline name, as displayed in the DLT Workflows. Must be unique.
        pipeline_id : str
            ID of an existing pipeline.

        Returns
        -------
        dict
            Dictionary of job settings, as per Databricks Jobs API specification.
        """
        job_settings = self.base_settings.load_settings(
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
            pipeline_name=pipeline_name,
            settings_file_name="_job_settings",
        )

        if not job_settings:
            self.log.info("Creating job using default settings")
            job_settings = self._get_default_job_settings()

        self._update_job_settings(
            job_settings=job_settings,
            pipeline_name=pipeline_name,
            pipeline_id=pipeline_id,
        )

        return job_settings

    @staticmethod
    def _get_default_job_settings() -> dict:
        return {
            "name": "",
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "",
                    "timeout_seconds": 0,
                    "pipeline_task": {
                        "pipeline_id": "",
                        "full_refresh": "false",
                    },
                },
            ],
            "schedule": {
                "quartz_cron_expression": "0 0 0/4 ? * * *",
                "timezone_id": "GMT",
                "pause_status": "UNPAUSED",
            },
            "email_notifications": {},
            "format": "MULTI_TASK",
        }
