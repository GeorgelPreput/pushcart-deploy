import pytest
from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient

from pushcart_deploy.databricks_api import JobsWrapper, PipelinesWrapper, Scheduler
from pushcart_deploy.databricks_api.settings import JobSettings


@pytest.fixture(autouse=True)
def mock_api_client():
    api_client = ApiClient(host="https://databricks.sample.com")

    return api_client


class TestScheduler:
    def test_get_obsolete_pipelines_list_happy(self, mock_api_client):
        """Tests that get_obsolete_pipelines_list returns the correct list."""
        scheduler = Scheduler(api_client=mock_api_client, config_dir="./tests/data")
        metadata_pipelines = [
            {"pipeline_name": "pipeline1", "pipeline_id": "123"},
            {"pipeline_name": "pipeline2", "pipeline_id": "456"},
        ]
        workflows_pipelines = [
            {"pipeline_name": "pipeline1", "pipeline_id": "123"},
            {"pipeline_name": "pipeline3", "pipeline_id": "789"},
        ]
        expected_result = [{"pipeline_name": "pipeline3", "pipeline_id": "789"}]
        assert (
            scheduler.get_obsolete_pipelines_list(
                metadata_pipelines, workflows_pipelines
            )
            == expected_result
        )

    def test_get_matching_pipelines_list_happy(self, mock_api_client):
        """Tests that get_matching_pipelines_list returns the correct list."""
        scheduler = Scheduler(api_client=mock_api_client, config_dir="./tests/data")
        metadata_pipelines = [
            {
                "target_catalog_name": "catalog1",
                "target_schema_name": "schema1",
                "pipeline_name": "pipeline1",
                "pipeline_id": "123",
            },
            {
                "target_catalog_name": "catalog2",
                "target_schema_name": "schema2",
                "pipeline_name": "pipeline2",
                "pipeline_id": "456",
            },
        ]
        workflow_pipelines = [
            {"pipeline_name": "pipeline1", "pipeline_id": "123"},
            {"pipeline_name": "pipeline3", "pipeline_id": "789"},
        ]
        expected_result = [
            {
                "target_catalog_name": "catalog1",
                "target_schema_name": "schema1",
                "pipeline_name": "pipeline1",
                "pipeline_id": "123",
            }
        ]
        assert (
            scheduler.get_matching_pipelines_list(
                metadata_pipelines, workflow_pipelines
            )
            == expected_result
        )

    def test_get_new_pipelines_list_edge(self, mock_api_client):
        """Tests that get_new_pipelines_list handles an empty scheduled_pipelines list correctly."""
        scheduler = Scheduler(api_client=mock_api_client, config_dir="./tests/data")
        metadata_pipelines = [
            {"pipeline_name": "pipeline1", "pipeline_id": "123"},
            {"pipeline_name": "pipeline2", "pipeline_id": "456"},
        ]
        workflows_pipelines = []
        expected_result = metadata_pipelines
        assert (
            scheduler.get_new_pipelines_list(metadata_pipelines, workflows_pipelines)
            == expected_result
        )

    def test_create_or_update_pipelines_edge(self, mocker, mock_api_client):
        """Tests that create_or_update_pipelines handles an empty metadata_pipelines list correctly."""
        scheduler = Scheduler(api_client=mock_api_client, config_dir="./tests/data")

        mocker.patch.object(ReposApi, "__init__", return_value=None)
        mocker.patch.object(
            ReposApi,
            "get",
            return_value={"id": "123", "path": "/Repos/pushcart/pushcart-config"},
        )

        metadata_pipelines = []
        expected_result = []
        assert (
            scheduler.create_or_update_pipelines("123", metadata_pipelines)
            == expected_result
        )

    def test_create_or_update_jobs_general(self, mocker, mock_api_client):
        """Tests that create_or_update_jobs creates or updates jobs correctly."""
        scheduler = Scheduler(api_client=mock_api_client, config_dir="./tests/data")
        pipelines = [
            {
                "target_catalog_name": "sample_catalog",
                "target_schema_name": "sample_schema",
                "pipeline_name": "pipeline1",
                "pipeline_id": "123",
            },
            {
                "target_catalog_name": "sample_catalog",
                "target_schema_name": "sample_schema",
                "pipeline_name": "pipeline2",
                "pipeline_id": "456",
            },
        ]
        mocker.patch.object(
            scheduler.jobs_wrapper, "get_job_id", side_effect=[None, "789"]
        )
        create_job = mocker.patch.object(
            scheduler.jobs_wrapper, "create_job", return_value="123"
        )
        update_job = mocker.patch.object(
            scheduler.jobs_wrapper, "update_job", return_value="789"
        )
        expected_result = ["123", "789"]
        assert scheduler.create_or_update_jobs(pipelines) == expected_result
        create_job.assert_called_once()
        update_job.assert_called_once()

    def test_delete_obsolete_pipelines_happy(self, mocker, mock_api_client):
        """Tests that delete_obsolete_pipelines deletes pipelines correctly."""
        scheduler = Scheduler(api_client=mock_api_client, config_dir="./tests/data")
        obsolete_pipelines = [{"pipeline_name": "pipeline1", "pipeline_id": "123"}]
        mocker.patch.object(scheduler.pipelines_wrapper, "delete_pipeline")
        scheduler.delete_obsolete_pipelines(obsolete_pipelines)
        scheduler.pipelines_wrapper.delete_pipeline.assert_called_once_with(
            pipeline_id="123"
        )

    def test_delete_obsolete_pipelines_edge(self, mocker, mock_api_client):
        """Tests that delete_obsolete_pipelines handles an empty obsolete_pipelines list correctly."""
        delete_pipeline = mocker.patch.object(
            PipelinesWrapper, "delete_pipeline", return_value=None
        )
        scheduler = Scheduler(api_client=mock_api_client, config_dir="./tests/data")
        scheduler.pipelines_wrapper = delete_pipeline
        scheduler.delete_obsolete_pipelines(obsolete_pipelines=[])
        delete_pipeline.assert_not_called()

    def test_create_or_update_jobs_edge(self, mocker, mock_api_client):
        """Test that create_or_update_jobs handles an empty pipelines list correctly"""
        load_job_settings = mocker.patch.object(
            JobSettings, "load_job_settings", return_value={}
        )
        create_job = mocker.patch.object(
            JobsWrapper, "create_job", return_value={"job_id": "123"}
        )
        update_job = mocker.patch.object(
            JobsWrapper, "update_job", return_value={"job_id": "123"}
        )

        scheduler = Scheduler(api_client=mock_api_client, config_dir="./tests/data")
        scheduler.create_or_update_jobs(pipelines=[])

        load_job_settings.assert_not_called()
        create_job.assert_not_called()
        update_job.assert_not_called()
