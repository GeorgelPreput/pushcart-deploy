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
from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import asdict
from glob import glob
from pathlib import Path

from databricks.connect import DatabricksSession
from pydantic import DirectoryPath, dataclasses

from pushcart_deploy.configuration import (
    Configuration,
    get_config_from_file,
    get_transformations_from_csv,
    get_transformations_from_sql,
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

    def __post_init__(self) -> None:
        """Initialize logger."""
        self.log = logging.getLogger(__name__)

    @staticmethod
    async def _load_pipeline_with_metadata(file_path: str) -> dict:
        config = await get_config_from_file(file_path)
        if config is None:
            return None

        file_path_obj = Path(file_path)
        metadata = {
            "pipeline_name": file_path_obj.parent.name,
            "target_schema_name": file_path_obj.parent.parent.name,
            "target_catalog_name": file_path_obj.parent.parent.parent.name,
        }

        for key in ["sources", "transformations", "destinations"]:
            if key in config:
                for item in config[key]:
                    item.update(metadata)

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

        for extension in ["*.json", "*.toml", "*.yaml", "*.yml"]:
            pipeline_files.extend(
                glob(f"{self.config_dir}/pipelines/**/[!_]{extension}", recursive=True),
            )

        pipeline_tasks = [self._load_pipeline_with_metadata(f) for f in pipeline_files]

        return await asyncio.gather(*pipeline_tasks)

    @staticmethod
    async def _enrich_sources_config(sources_config: list) -> None:
        for source_dict in sources_config:
            if isinstance(source_dict.get("params"), dict):
                source_dict["params"] = json.dumps(source_dict.get("params", {}))

        return {"sources": sources_config}

    @staticmethod
    async def _handle_csv_transformations(
        transformation: dict,
        config_path: Path,
    ) -> AsyncIterator[dict]:
        async for row in get_transformations_from_csv(config_path.resolve()):
            row["column_order"] = (
                int(row["column_order"]) if str(row["column_order"]).isdigit() else None
            )

            row.update(
                origin=transformation["origin"],
                target=transformation["target"],
                target_catalog_name=transformation["target_catalog_name"],
                target_schema_name=transformation["target_schema_name"],
                pipeline_name=transformation["pipeline_name"],
            )

            if row.get("validation_rule") and row.get("validation_action"):
                row["validations"] = [
                    {
                        "validation_rule": row["validation_rule"],
                        "validation_action": row["validation_action"],
                    },
                ]
                del row["validation_rule"], row["validation_action"]

            yield row

    @staticmethod
    async def _handle_sql_transformations(
        transformation: dict,
        config_path: Path,
    ) -> dict:
        sql_transformation = deepcopy(transformation)
        sql_transformation["sql_query"] = await get_transformations_from_sql(
            config_path.resolve(),
        )

        del sql_transformation["config"]

        return sql_transformation

    async def _enrich_transformations_config(
        self,
        transformations_config: list,
    ) -> None:
        enriched_transformations = []

        for t in transformations_config:
            if t.get("config"):
                config_path = Path(t["config"])
                if config_path.suffix == ".csv":
                    async for row in self._handle_csv_transformations(t, config_path):
                        enriched_transformations.append(row)
                elif config_path.suffix == ".sql":
                    enriched_transformations.append(
                        await self._handle_sql_transformations(t, config_path),
                    )
                else:
                    msg = "Transformation configurations can only be .csv or .sql files"
                    raise TypeError(msg)

        return {
            "transformations": [t for t in transformations_config if "config" not in t]
            + enriched_transformations,
        }

    @staticmethod
    async def _enrich_destinations_config(destinations_config: list) -> None:
        return {"destinations": destinations_config}

    async def _enrich_pipeline_configs(self, pipeline_configs: list) -> None:
        enrichment_func = {
            "sources": self._enrich_sources_config,
            "transformations": self._enrich_transformations_config,
            "destinations": self._enrich_destinations_config,
        }

        return await asyncio.gather(
            *[
                enrichment_func[stage_name](stage_config)
                for pipeline_config in pipeline_configs
                for stage_name, stage_config in pipeline_config.items()
                if stage_name in enrichment_func
            ],
        )

    def _validate_pipeline_configs(self, pipeline_configs: list) -> None:
        validated_pipeline_configs = []

        for pipeline_config in pipeline_configs:
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
        enriched_pipeline_configs = asyncio.run(
            self._enrich_pipeline_configs(pipeline_configs),
        )
        validated_pipeline_configs = self._validate_pipeline_configs(
            enriched_pipeline_configs,
        )
        self._create_metadata_tables(validated_pipeline_configs)

    @staticmethod
    def get_pipeline_list_from_backend_objects() -> list:
        """Get a list of dicts with all the pipelines available in the metadata tables.

        Returns
        -------
        dict
            a dict with pipeline items, e.g. [{ pipeline_name: target_schema_name }, ...]
        """
        spark = DatabricksSession.builder.getOrCreate()

        pipelines_df = spark.createDataFrame(
            [],
            schema="struct<target_catalog_name:string,target_schema_name:string,pipeline_name:string>",
        )

        for stage in ["sources", "transformations", "destinations"]:
            pipelines_df = pipelines_df.union(
                spark.table(f"pushcart.{stage}").select(
                    "target_catalog_name",
                    "target_schema_name",
                    "pipeline_name",
                ),
            )

        return [
            {
                "target_catalog_name": row["target_catalog_name"],
                "target_schema_name": row["target_schema_name"],
                "name": row["pipeline_name"],
                "pipeline_id": None,
            }
            for row in pipelines_df.distinct().collect()
        ]
