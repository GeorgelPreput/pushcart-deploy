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

import asyncio
import logging
import operator
from functools import lru_cache
from pathlib import Path

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import constr, dataclasses, validate_arguments, validator

from pushcart_deploy.configuration import get_config_from_file
from pushcart_deploy.validation import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@lru_cache(maxsize=1)
@validate_arguments(config={"arbitrary_types_allowed": True})
def _get_smallest_cluster_node_type(client: ApiClient = None) -> str:
    """Retrieve the smallest Photon-capable cluster node type from a Databricks cluster."""
    cluster_api = ClusterApi(client)

    log = logging.getLogger(__name__)

    node_types = [
        t
        for t in cluster_api.list_node_types()["node_types"]
        if t["is_deprecated"] is False
        and t["is_hidden"] is False
        and t["photon_driver_capable"] is True
        and t["photon_worker_capable"] is True
    ]

    if not node_types:
        log.error("No Photon-capable node type could be selected")
        return None

    node = sorted(
        node_types,
        key=operator.itemgetter("num_cores", "memory_mb", "num_gpus"),
    )[0]["node_type_id"]
    log.info(f"Using node type ID: {node}")

    return node


@lru_cache(maxsize=1)
@validate_arguments(config={"arbitrary_types_allowed": True})
def _get_newest_spark_version(client: ApiClient = None) -> str:
    """Retrieve the newest version of Apache Spark that is not labeled as "ML" and is an LTS version."""
    cluster_api = ClusterApi(client)

    log = logging.getLogger(__name__)

    spark_versions = [
        v
        for v in cluster_api.spark_versions()["versions"]
        if "ML" not in v["name"] and "LTS" in v["name"]
    ]

    if not spark_versions:
        log.error("No spark versions.")
        return None

    version = sorted(
        spark_versions,
        key=lambda x: float(x["name"].split(" LTS ")[0]),
        reverse=True,
    )[0]["key"]
    log.info(f"Using Spark version: {version}")

    return version


@lru_cache(maxsize=50)
@validate_arguments(config={"arbitrary_types_allowed": True})
def _get_existing_cluster_id(
    client: ApiClient = None,
    cluster_name: constr(min_length=1, strict=True) = None,
) -> str:
    """Retrieve the ID of an existing Databricks cluster by its name."""
    cluster_api = ClusterApi(client)

    log = logging.getLogger(__name__)

    clusters = cluster_api.list_clusters().get("clusters", [])
    clusters_filtered = [c for c in clusters if c["cluster_name"] == cluster_name]

    if not clusters_filtered:
        log.error(f"Cluster not found: {cluster_name}")
        return None

    cluster_id = clusters_filtered[0]["cluster_id"]
    log.info(f"Cluster ID: {cluster_id}")

    return cluster_id


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class JobSettings:
    """Manages job settings for Databricks jobs.

    Provides methods for loading job settings from a JSON file or string, as well as
    for retrieving default job settings for checkpoint, pipeline, and release jobs.
    """

    client: ApiClient

    @validator("client")
    @classmethod
    def check_api_client(cls, value: ApiClient) -> ApiClient:
        """Validate the provided ApiClient."""
        return validate_databricks_api_client(value)

    def __post_init_post_parse__(self) -> None:
        """Initialize logger."""
        self.log = logging.getLogger(__name__)

    @validate_arguments(config={"arbitrary_types_allowed": True})
    def load_job_settings(
        self,
        settings_path: Path | None = None,
        default_settings: constr(
            min_length=1,
            strict=True,
            regex="^(checkpoint|pipeline)$",
        )
        | None = None,
    ) -> dict:
        """Load job settings from a file, or retrieve default job settings if none are provided."""
        job_settings = None

        if settings_path:
            job_settings = asyncio.run(get_config_from_file(settings_path))

        if not job_settings:
            self.log.info("Creating job using default settings")

            if settings_path and not default_settings:
                msg = "Failed to load provided job settings, and default settings were not specified."
                raise RuntimeError(
                    msg,
                )

            job_settings = self._get_default_job_settings(default_settings)

        return job_settings

    def _get_default_job_settings(
        self,
        settings_name: constr(
            min_length=1,
            strict=True,
            regex=r"^(checkpoint|pipeline)$",
        ),
    ) -> dict:
        """Retrieve default job settings for checkpoint and pipeline jobs."""
        settings_map = {
            "checkpoint": _get_default_checkpoint_job_settings,
            "pipeline": _get_default_pipeline_job_settings,
        }

        settings_getter = settings_map.get(settings_name)

        if not settings_getter:
            msg = "Could not find default settings for {settings_name}"
            raise ValueError(msg)

        return settings_getter(self.client)


def _get_default_pipeline_job_settings(
    client: ApiClient = None,  # noqa: ARG001
) -> dict:
    return {}


def _get_default_checkpoint_job_settings(client: ApiClient = None) -> dict:
    return {
        "name": "release",
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "release",
                "python_wheel_task": {
                    "package_name": "pushcart",
                    "entry_point": "pushcart-release",
                    "named_parameters": {"--workspace-url": client.url},
                },
                "job_cluster_key": "release",
                "libraries": [{"pypi": {"package": "pushcart"}}],
                "max_retries": 1,
                "min_retry_interval_millis": 15000,
                "retry_on_timeout": False,
                "timeout_seconds": 0,
                "email_notifications": {},
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "release",
                "new_cluster": {
                    "spark_version": _get_newest_spark_version(client),
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "node_type_id": _get_smallest_cluster_node_type(client),
                    "driver_node_type_id": _get_smallest_cluster_node_type(client),
                    "custom_tags": {"ResourceClass": "SingleNode"},
                    "enable_elastic_disk": True,
                    "runtime_engine": "PHOTON",
                    "num_workers": 0,
                },
            },
        ],
        "format": "MULTI_TASK",
    }
