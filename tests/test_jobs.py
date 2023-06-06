import pytest
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient

from pushcart_deploy.databricks_api.jobs_wrapper import JobsWrapper
from pushcart_deploy.databricks_api.settings import JobSettings


@pytest.fixture(autouse=True)
def mock_api_client():
    api_client = ApiClient(host="https://databricks.sample.com")

    return api_client


class TestJobSettings:
    def test_load_job_settings_from_valid_file(self, mocker):
        """Tests that job settings can be loaded from a valid JSON file."""
        job = JobSettings("./tests/data")
        loaded_settings = job.load_job_settings(
            target_catalog_name="sample_catalog",
            target_schema_name="sample_schema",
            pipeline_name="sample_pipeline",
            pipeline_id="01234",
        )

        assert loaded_settings == {
            "name": "sample_pipeline",
            "tasks": [
                {
                    "task_key": "sample_pipeline",
                    "pipeline_task": {"pipeline_id": "01234"},
                }
            ],
            "timeout_seconds": 60,
        }

    def test_load_job_settings_default_when_file_invalid(self, mocker, mock_api_client):
        """Tests that the default settings are returned if the given file could not be
        loaded.
        """
        job_settings = JobSettings("./tests/data")

        result = job_settings.load_job_settings(
            target_catalog_name="invalid_catalog",
            target_schema_name="invalid_schema",
            pipeline_name="invalid_pipeline",
            pipeline_id="01234",
        )
        assert result == {
            "name": "invalid_pipeline",
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "invalid_pipeline",
                    "timeout_seconds": 0,
                    "pipeline_task": {
                        "pipeline_id": "01234",
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


class TestJobsWrapper:
    def test_update_existing_job(self, mocker, mock_api_client):
        """Tests that update_job retrieves an existing job when one exists and
        resets it to current settings.
        """
        mocker.patch.object(JobsApi, "reset_job", return_value=None)

        job_settings = {"name": "test_job", "job_type": "python"}
        jobs_wrapper = JobsWrapper(mock_api_client)

        job_id = jobs_wrapper.update_job(job_id="12345", job_settings=job_settings)

        assert job_id == "12345"
        jobs_wrapper.jobs_api.reset_job.assert_called_once_with(
            {"job_id": "12345", "new_settings": job_settings},
        )

    def test_create_new_job(self, mocker, mock_api_client):
        """Tests that get_or_create_job creates a new job when one does not exist."""
        mocker.patch.object(JobsApi, "create_job", return_value={"job_id": 1234})

        job_settings = {"name": "test_job"}

        jobs_wrapper = JobsWrapper(mock_api_client)
        job_id = jobs_wrapper.create_job(job_settings)

        assert job_id == 1234
        jobs_wrapper.jobs_api.create_job.assert_called_once_with(job_settings)

    def test_delete_job(self, mocker, mock_api_client):
        """Tests that delete_job deletes a job."""
        mocker.patch.object(
            JobsApi,
            "get_job",
            return_value={"settings": {"name": "test_job"}},
        )
        mocker.patch.object(JobsApi, "delete_job", return_value=None)

        job_id = "test_job_id"

        jobs_wrapper = JobsWrapper(mock_api_client)
        jobs_wrapper.delete_job(job_id)

        jobs_wrapper.jobs_api.delete_job.assert_called_once_with(job_id=job_id)
