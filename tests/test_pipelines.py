import pytest
from databricks.sdk.dbutils import _FsUtil
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.sdk.api_client import ApiClient

from pushcart_deploy.databricks_api import PipelinesWrapper
from pushcart_deploy.databricks_api.settings import PipelineSettings


@pytest.fixture(autouse=True)
def mock_api_client():
    api_client = ApiClient(host="https://databricks.sample.com")

    return api_client


class TestPipelineSettings:
    def test_load_pipeline_settings_from_file(self, mock_api_client):
        """Tests that pipeline settings are loaded from file correctly."""
        pipeline_settings = PipelineSettings(mock_api_client, "./tests/data")

        result = pipeline_settings.load_pipeline_settings(
            target_catalog_name="sample_catalog",
            target_schema_name="sample_schema",
            pipeline_name="sample_pipeline",
            libraries=[{"notebook": {"path": "/test/notebook"}}],
            configuration={"test_config": "test_value"},
        )

        assert result == {
            "name": "sample_pipeline",
            "catalog": "sample_catalog",
            "target": "sample_schema",
            "channel": "PREVIEW",
            "clusters": [
                {
                    "label": "default",
                    "node_type_id": "test_node_type",
                    "autoscale": {"min_workers": 2, "max_workers": 4},
                }
            ],
            "libraries": [{"notebook": {"path": "/test/notebook"}}],
            "continuous": "true",
            "configuration": {"test_config": "test_value"},
        }

    def test_load_default_pipeline_settings(self, mocker, mock_api_client):
        """Tests that default pipeline settings are loaded correctly when no file is found."""
        pipeline_settings = PipelineSettings(mock_api_client, "./tests/data")

        mocker.patch.object(
            ClusterApi,
            "list_node_types",
            return_value={
                "node_types": [
                    {
                        "node_type_id": "smallest_node_type",
                        "num_cores": 4,
                        "memory_mb": 4096,
                        "num_gpus": 0,
                        "is_deprecated": False,
                        "is_hidden": False,
                        "photon_driver_capable": True,
                        "photon_worker_capable": True,
                    }
                ]
            },
        )

        result = pipeline_settings.load_pipeline_settings(
            target_catalog_name="invalid_catalog",
            target_schema_name="invalid_schema",
            pipeline_name="invalid_pipeline",
            libraries=[{"notebook": {"path": "/test/notebook"}}],
            configuration={"test_config": "test_value"},
        )

        assert result == {
            "name": "invalid_pipeline",
            "catalog": "invalid_catalog",
            "target": "invalid_schema",
            "channel": "PREVIEW",
            "clusters": [
                {
                    "label": "default",
                    "node_type_id": "smallest_node_type",
                    "autoscale": {
                        "min_workers": 1,
                        "max_workers": 5,
                    },
                },
            ],
            "libraries": [{"notebook": {"path": "/test/notebook"}}],
            "continuous": "false",
            "configuration": {"test_config": "test_value"},
        }

    def test_load_pipeline_settings_no_photon_node(self, mocker, mock_api_client):
        """Tests that an error is raised when no Photon-capable node type is available."""
        mocker.patch.object(
            ClusterApi, "list_node_types", return_value={"node_types": []}
        )

        pipeline_settings = PipelineSettings(mock_api_client, "./tests/data")

        with pytest.raises(Exception):
            pipeline_settings.load_pipeline_settings(
                target_catalog_name="invalid_catalog",
                target_schema_name="invalid_schema",
                pipeline_name="invalid_pipeline",
                libraries=[{"notebook": {"path": "/test/notebook"}}],
                configuration={"test_config": "test_value"},
            )

    def test_update_pipeline_settings(self, mock_api_client):
        """Tests that pipeline settings are updated correctly with current pipeline details."""
        pipeline_settings = PipelineSettings(mock_api_client, "./tests/data")

        pipeline_settings_dict = {
            "name": "",
            "catalog": "",
            "target": "",
            "libraries": [
                {
                    "notebook": {
                        "path": "",
                    },
                },
            ],
            "configuration": {
                "pushcart.pipeline_name": "",
            },
        }

        pipeline_name = "test_pipeline"
        target_catalog_name = "test_catalog"
        target_schema_name = "test_schema"
        libraries = [{"notebook": {"path": "/test/notebook"}}]
        configuration = {"test_config": "test_value"}
        pipeline_id = "123"

        pipeline_settings._update_pipeline_settings(
            pipeline_settings=pipeline_settings_dict,
            pipeline_name=pipeline_name,
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
            libraries=libraries,
            configuration=configuration,
            pipeline_id=pipeline_id,
        )

        assert pipeline_settings_dict == {
            "name": "test_pipeline",
            "catalog": "test_catalog",
            "target": "test_schema",
            "libraries": [
                {
                    "notebook": {
                        "path": "/test/notebook",
                    },
                },
            ],
            "configuration": {
                "test_config": "test_value",
            },
            "id": "123",
        }

    def test_get_smallest_cluster_node_type(self, mocker, mock_api_client):
        """Tests that the smallest Photon-capable cluster node type is retrieved correctly."""
        mock_list_node_types = mocker.patch.object(ClusterApi, "list_node_types")
        mock_list_node_types.return_value = {
            "node_types": [
                {
                    "node_type_id": "test_node_type_1",
                    "num_cores": 2,
                    "memory_mb": 4096,
                    "num_gpus": 0,
                    "is_deprecated": False,
                    "is_hidden": False,
                    "photon_driver_capable": True,
                    "photon_worker_capable": True,
                },
                {
                    "node_type_id": "test_node_type_2",
                    "num_cores": 4,
                    "memory_mb": 8192,
                    "num_gpus": 1,
                    "is_deprecated": False,
                    "is_hidden": False,
                    "photon_driver_capable": False,
                    "photon_worker_capable": False,
                },
                {
                    "node_type_id": "test_node_type_3",
                    "num_cores": 4,
                    "memory_mb": 8192,
                    "num_gpus": 1,
                    "is_deprecated": False,
                    "is_hidden": False,
                    "photon_driver_capable": True,
                    "photon_worker_capable": True,
                },
            ],
        }

        pipeline_settings = PipelineSettings(mock_api_client, "./tests/data")
        result = pipeline_settings._get_smallest_cluster_node_type()

        assert result == "test_node_type_1"


class TestPipelineWrapper:
    def test_get_pipelines_list(self, mocker, mock_api_client):
        """Tests that the method returns a list of dictionaries containing pipeline names and IDs."""
        mocker.patch.object(
            PipelinesApi,
            "list",
            return_value=[
                {"name": "pipeline1", "pipeline_id": "1234"},
                {"name": "pipeline2", "pipeline_id": "5678"},
            ],
        )

        pipelines_wrapper = PipelinesWrapper(
            api_client=mock_api_client, config_dir="./tests/data"
        )
        pipelines_list = pipelines_wrapper.get_pipelines_list()
        assert isinstance(pipelines_list, list)
        assert all(isinstance(p, dict) for p in pipelines_list)
        assert all("name" in p and "pipeline_id" in p for p in pipelines_list)

    def test_get_pipeline_id_existing(self, mocker, mock_api_client):
        """Tests that the method returns the correct pipeline ID for an existing pipeline name."""
        mocker.patch.object(
            PipelinesWrapper,
            "get_pipelines_list",
            return_value=[
                {"name": "pipeline1", "pipeline_id": "1234"},
                {"name": "pipeline2", "pipeline_id": "5678"},
            ],
        )

        pipelines_wrapper = PipelinesWrapper(
            api_client=mock_api_client, config_dir="./tests/data"
        )
        pipeline_id = pipelines_wrapper.get_pipeline_id("pipeline1")
        assert pipeline_id == "1234"

    def test_get_pipeline_id_nonexistent(self, mocker, mock_api_client):
        """Tests that the method returns None for a non-existent pipeline name."""
        mocker.patch.object(
            PipelinesWrapper,
            "get_pipelines_list",
            return_value=[
                {"name": "pipeline1", "pipeline_id": "1234"},
                {"name": "pipeline2", "pipeline_id": "5678"},
            ],
        )

        pipelines_wrapper = PipelinesWrapper(
            api_client=mock_api_client, config_dir="./tests/data"
        )
        pipeline_id = pipelines_wrapper.get_pipeline_id("pipeline3")
        assert pipeline_id is None

    def test_create_pipeline(self, mocker, mock_api_client):
        """Tests that the method creates a new pipeline with valid settings and repo path, and returns the pipeline ID."""
        mocker.patch.object(
            PipelinesApi,
            "create",
            return_value={"name": "pipeline1", "pipeline_id": "1234"},
        )

        pipelines_wrapper = PipelinesWrapper(
            api_client=mock_api_client, config_dir="./tests/data"
        )
        pipeline_id = pipelines_wrapper.create_pipeline(
            {"name": "pipeline1"}, "/path/to/repo"
        )
        assert pipeline_id == "1234"

    def test_update_pipeline(self, mocker, mock_api_client):
        """Tests that the method updates an existing pipeline with valid settings and repo path, and returns the pipeline ID."""
        mocker.patch.object(PipelinesApi, "edit")

        pipelines_wrapper = PipelinesWrapper(
            api_client=mock_api_client, config_dir="./tests/data"
        )
        pipeline_id = pipelines_wrapper.update_pipeline(
            {"id": "1234", "name": "pipeline1"}, "/path/to/repo"
        )
        assert pipeline_id == "1234"

    def test_delete_pipeline_existing(self, mocker, mock_api_client):
        """Tests that the method deletes an existing pipeline with a valid pipeline ID."""
        mock_delete = mocker.patch.object(PipelinesApi, "delete", return_value=None)
        mock_rm = mocker.patch.object(_FsUtil, "rm", return_value=None)

        pipelines_wrapper = PipelinesWrapper(mock_api_client, "./tests/data")

        pipeline_id = "12345"
        pipelines_wrapper.delete_pipeline(pipeline_id)

        mock_delete.assert_called_once_with(pipeline_id=pipeline_id)
        mock_rm.assert_called_once_with(f"dbfs:/pipelines/{pipeline_id}", recurse=True)
