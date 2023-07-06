"""Load Databricks DLT pipeline settings from file or from defaults.

Job settings may come in JSON, TOML or YAML formats when loaded from file
"""

import logging
import operator

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk.api_client import ApiClient
from methodtools import lru_cache
from pydantic import DirectoryPath, dataclasses, validate_call

from pushcart_deploy.databricks_api.settings import BaseSettings
from pushcart_deploy.validation import PydanticArbitraryTypesConfig


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class PipelineSettings:
    """Manages settings for DLT pipelines.

    Provides methods for loading pipeline settings from a JSON file or string, as well
    as for retrieving default settings for DLT pipelines.
    """

    api_client: ApiClient
    config_dir: DirectoryPath

    def __post_init__(self) -> None:
        """Initialize logger and dependent API classes."""
        self.log = logging.getLogger(__name__)
        self.cluster_api = ClusterApi(self.api_client)
        self.base_settings = BaseSettings(self.config_dir)

    @lru_cache(maxsize=1)
    def _get_smallest_cluster_node_type(self) -> str:
        """Retrieve the smallest Photon-capable cluster node type from a Databricks cluster.

        Returns
        -------
        str
            String containing the smallest cluster node type available in current cloud
        """
        node_types = [
            t
            for t in self.cluster_api.list_node_types()["node_types"]
            if all(
                [
                    not t["is_deprecated"],
                    not t["is_hidden"],
                    t["photon_driver_capable"],
                    t["photon_worker_capable"],
                ],
            )
        ]

        if not node_types:
            msg = "No Photon-capable node type could be selected"
            self.log.error(msg)
            raise RuntimeError(msg)

        node = sorted(
            node_types,
            key=operator.itemgetter("num_cores", "memory_mb", "num_gpus"),
        )[0]["node_type_id"]
        self.log.info(f"Using node type ID: {node}")

        return node

    def _update_pipeline_settings(  # noqa: PLR0913
        self,
        pipeline_settings: dict,
        pipeline_name: str,
        target_catalog_name: str,
        target_schema_name: str,
        libraries: list[dict],
        configuration: dict[str, str],
        pipeline_id: str | None = None,
    ) -> None:
        """Dynamically update pipeline settings with current pipeline details.

        Parameters
        ----------
        pipeline_settings: dict
            Settings template to fill in, whether custom or default.
        pipeline_name : str
            Pipeline name, as displayed in the DLT Workflows. Must be unique.
        target_schema_name : str
            A schema name for persisting pipeline output data.
        target_catalog_name : str
            A catalog name for persisting pipeline output data.
        libraries : list[dict]
            The notebooks containing the pipeline code and any dependencies required to
            run the pipeline.
        configuration : dict[str, str]
            A list of key-value pairs to add to the Spark configuration of the cluster
            that will run the pipeline.
        pipeline_id : str | None, optional
            ID of an existing pipeline, by default None
        """
        pipeline_settings["name"] = pipeline_name
        pipeline_settings["catalog"] = target_catalog_name
        pipeline_settings["target"] = target_schema_name
        pipeline_settings["libraries"] = libraries
        pipeline_settings["configuration"] = configuration

        if pipeline_id:
            pipeline_settings["id"] = pipeline_id

    @validate_call
    def load_pipeline_settings(  # noqa: PLR0913
        self,
        target_catalog_name: str,
        target_schema_name: str,
        pipeline_name: str,
        libraries: list[dict],
        configuration: dict[str, str],
        pipeline_id: str | None = None,
    ) -> dict:
        """Load pipeline settings from file, or use default pipeline settings if none are provided.

        Parameters
        ----------
        pipeline_name : str
            Pipeline name, as displayed in the DLT Workflows. Must be unique.
        target_schema_name : str
            A schema name for persisting pipeline output data.
        target_catalog_name : str
            A catalog name for persisting pipeline output data.
        libraries : list[dict]
            The notebooks containing the pipeline code and any dependencies required to
            run the pipeline.
        configuration : dict[str, str]
            A list of key-value pairs to add to the Spark configuration of the cluster
            that will run the pipeline.
        pipeline_id : str | None, optional
            ID of an existing pipeline, by default None

        Returns
        -------
        dict
            Dictionary of DLT pipeline settings, as per Databricks Delta Live Tables
            API specification.
        """
        pipeline_settings = self.base_settings.load_settings(
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
            pipeline_name=pipeline_name,
            settings_file_name="_pipeline_settings",
        )

        if not pipeline_settings:
            self.log.info("Creating pipeline using default settings")
            pipeline_settings = self._get_default_pipeline_settings()

        self._update_pipeline_settings(
            pipeline_settings=pipeline_settings,
            pipeline_name=pipeline_name,
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
            libraries=libraries,
            configuration=configuration,
            pipeline_id=pipeline_id,
        )

        return pipeline_settings

    def _get_default_pipeline_settings(self) -> dict:
        smallest_cluster_node_type = self._get_smallest_cluster_node_type()
        return {
            "name": "",
            "catalog": "",
            "target": "",
            "channel": "PREVIEW",
            "clusters": [
                {
                    "label": "default",
                    "node_type_id": smallest_cluster_node_type,
                    "autoscale": {
                        "min_workers": 1,
                        "max_workers": 5,
                    },
                },
            ],
            "libraries": [
                {
                    "notebook": {
                        "path": "",
                    },
                },
            ],
            "continuous": "false",
            "configuration": {
                "pushcart.pipeline_name": "",
            },
        }
