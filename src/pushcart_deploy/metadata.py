"""Module to transfer configuration files into Databricks metadata tables.

Example:
-------
    metadata = Metadata("/home/<username>/source/pushcart-config")
    metadata.create_backend_objects()

Notes:
-----
Requires Databricks Connect v2, and a cluster with Databricks Runtime >= 13
Requires Databricks CLI to already be configured for your target Databricks environment

"""

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import asdict
from glob import glob
from itertools import groupby
from operator import itemgetter
from pathlib import Path

from databricks.connect import DatabricksSession
from pydantic import DirectoryPath, dataclasses

from pushcart_deploy.configuration import (
    Configuration,
    get_config_from_file,
    get_transformations_from_csv,
)
from pushcart_deploy.validation import sanitize_empty_objects


@dataclasses.dataclass
class Metadata:
    """Read pipeline configuration files and create backend objects in Databricks.

    Returns
    -------
    Metadata
        Holds methods for reading, parsing and enriching configuration files, then
        writing them to the Databricks environment into metadata tables:
        - pushcart.sources
        - pushcart.transformations
        - pushcart.destinations
    """

    config_dir: DirectoryPath

    def __post_init_post_parse__(self) -> None:
        """Initialize logger."""
        self.log = logging.getLogger(__name__)

    @staticmethod
    async def _load_pipeline_with_metadata(file_path: str) -> dict:
        config = await get_config_from_file(file_path)
        if config is None:
            return None

        file_path_obj = Path(file_path)
        config["pipeline_name"] = file_path_obj.parent.name
        config["target_schema_name"] = file_path_obj.parent.parent.name

        if config.get("transformations"):
            for transformation in config["transformations"]:
                if (
                    transformation.get("config")
                    and not Path(transformation["config"]).is_file()
                ):
                    transformation["config"] = str(
                        file_path_obj.parent.joinpath(
                            Path(transformation["config"]),
                        ).resolve(),
                    )

        return config

    async def _collect_pipeline_configs(self) -> list:
        pipeline_files = []

        for extensions in ["*.json", "*.toml", "*.yaml", "*.yml"]:
            pipeline_files.extend(
                glob(f"{self.config_dir}/pipelines/**/{extensions}", recursive=True),
            )

        pipeline_tasks = [self._load_pipeline_with_metadata(f) for f in pipeline_files]

        return await asyncio.gather(*pipeline_tasks)

    @staticmethod
    async def _enrich_sources_config(sources_config: list) -> None:
        for source_dict in sources_config:
            if isinstance(source_dict.get("params"), dict):
                source_dict["params"] = json.dumps(source_dict.get("params", {}))

    @staticmethod
    async def _enrich_transformations_config(transformations_config: list) -> None:
        for transformation_config in transformations_config:
            if csv_path := transformation_config.get("config"):
                async for row in get_transformations_from_csv(Path(csv_path).resolve()):
                    row["column_order"] = (
                        int(row["column_order"])
                        if str(row["column_order"]).isdigit()
                        else None
                    )
                    row["origin"] = transformation_config["origin"]
                    row["target"] = transformation_config["target"]

                    if row.get("validation_rule") and row.get("validation_action"):
                        row["validations"] = [
                            {
                                "validation_rule": row["validation_rule"],
                                "validation_action": row["validation_action"],
                            },
                        ]
                        del row["validation_rule"]
                        del row["validation_action"]

                    transformations_config.append(row)
                transformations_config.remove(transformation_config)

    @staticmethod
    async def _enrich_destinations_config(destinations_config: list) -> None:
        pass

    async def _enrich_pipeline_configs(self, pipeline_configs: list) -> None:
        enrichment_func = {
            "sources": self._enrich_sources_config,
            "transformations": self._enrich_transformations_config,
            "destinations": self._enrich_destinations_config,
        }

        await asyncio.gather(
            *[
                enrichment_func[stage_name](stage_config)
                for pipeline_config in pipeline_configs
                for stage_name, stage_config in pipeline_config.items()
                if stage_name in enrichment_func
            ],
        )

    @staticmethod
    def _group_pipeline_configs(pipeline_configs: list) -> list:
        sorted_pipeline_configs = sorted(
            pipeline_configs,
            key=itemgetter("target_schema_name", "pipeline_name"),
        )
        grouped_elements = groupby(
            sorted_pipeline_configs,
            key=itemgetter("target_schema_name", "pipeline_name"),
        )

        grouped_pipeline_configs = []

        for (schema, pipeline), group in grouped_elements:
            merged_pipeline_stages_dict = defaultdict(list)
            for d in group:
                for k, v in d.items():
                    merged_pipeline_stages_dict[k].extend(v) if isinstance(
                        v,
                        list,
                    ) else merged_pipeline_stages_dict[k].append(v)

            merged_pipeline_stages_dict["target_schema_name"] = schema
            merged_pipeline_stages_dict["pipeline_name"] = pipeline
            grouped_pipeline_configs.append(dict(merged_pipeline_stages_dict))

        return grouped_pipeline_configs

    def _validate_pipeline_configs(self, pipeline_configs: list) -> None:
        grouped_pipeline_configs = self._group_pipeline_configs(pipeline_configs)

        validated_pipeline_configs = []

        for pipeline_config in grouped_pipeline_configs:
            validated_pipeline_configs.append(Configuration(**pipeline_config))

        return validated_pipeline_configs

    def _create_metadata_tables(self, pipeline_configs: list) -> None:
        spark = DatabricksSession.builder.getOrCreate()

        spark.sql("CREATE DATABASE IF NOT EXISTS pushcart")

        for stage_name in ["sources", "destinations", "transformations"]:
            stage_df = spark.createDataFrame(
                sanitize_empty_objects(
                    [
                        stage_element
                        for pipeline_config in pipeline_configs
                        for stage_element in asdict(pipeline_config)[stage_name]
                    ],
                    drop_empty=True,
                ),
            )
            stage_df.write.option("mergeSchema", "true").saveAsTable(
                f"pushcart.{stage_name}",
                format="delta",
                mode="overwrite",
            )

            self.log.info(f"Wrote {stage_name} metadata table.")

    def create_backend_objects(self) -> None:
        """Create metadata tables holding pipeline stages."""
        pipeline_configs = asyncio.run(self._collect_pipeline_configs())
        asyncio.run(self._enrich_pipeline_configs(pipeline_configs))
        validated_pipeline_configs = self._validate_pipeline_configs(pipeline_configs)
        self._create_metadata_tables(validated_pipeline_configs)
