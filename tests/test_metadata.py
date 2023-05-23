import asyncio
from pathlib import PosixPath

import pytest
from pydantic.types import DirectoryPath

from pushcart_deploy.configuration import Configuration
from pushcart_deploy.metadata import Metadata

"""
Code Analysis

Main functionalities:
The Metadata class reads pipeline configuration files and creates backend objects in Databricks. It holds methods for reading, parsing, and enriching configuration files, then writing them to the Databricks environment into metadata tables: pushcart.sources, pushcart.transformations, pushcart.destinations.

Methods:
- `_load_pipeline_with_metadata(file_path: str) -> dict`: loads a pipeline configuration file into a dictionary and enriches it with metadata.
- `_collect_pipeline_configs() -> list`: collects pipeline configuration files from the specified directory and returns them as a list.
- `_enrich_sources_config(sources_config: list) -> None`: enriches the sources configuration with metadata.
- `_enrich_transformations_config(transformations_config: list) -> None`: enriches the transformations configuration with metadata.
- `_enrich_destinations_config(destinations_config: list) -> None`: enriches the destinations configuration with metadata.
- `_enrich_pipeline_configs(pipeline_configs: list) -> None`: enriches the pipeline configurations with metadata.
- `_group_pipeline_configs(pipeline_configs: list) -> list`: groups pipeline configurations by target schema and pipeline name.
- `_validate_pipeline_configs(pipeline_configs: list) -> None`: validates pipeline configurations.
- `_create_metadata_tables(pipeline_configs: list) -> None`: creates metadata tables holding pipeline stages.
- `create_backend_objects() -> None`: creates metadata tables holding pipeline stages.

Fields:
- `config_dir: DirectoryPath`: the directory containing the pipeline configuration files.
"""


class TestMetadata:
    @pytest.mark.asyncio
    async def test_metadata_creation_with_valid_directory_path(self):
        """Tests that a Metadata object is created successfully with a valid directory path."""
        input_path = "./tests/data"
        metadata = Metadata(input_path)

        assert metadata.config_dir == PosixPath(input_path)

    @pytest.mark.asyncio
    async def test_pipeline_configs_loaded_and_parsed_successfully(self):
        """Tests that pipeline configuration files are loaded and parsed successfully."""
        metadata = Metadata("./tests/data")
        pipeline_configs = await metadata._collect_pipeline_configs()

        assert len(pipeline_configs) == 1

    @pytest.mark.asyncio
    async def test_pipeline_configs_enriched_successfully(self):
        """Tests that enrichment functions are applied to pipeline configurations successfully."""

        metadata = Metadata("./tests/data")
        pipeline_configs = await metadata._collect_pipeline_configs()
        await metadata._enrich_pipeline_configs(pipeline_configs)

        assert any(
            ["column_order" in t for t in pipeline_configs[0]["transformations"]]
        )

    def test_validated_pipeline_configs_created_successfully(self):
        """Tests that validated pipeline configurations are created successfully."""
        metadata = Metadata("./tests/data")
        pipeline_configs = asyncio.run(metadata._collect_pipeline_configs())
        asyncio.run(metadata._enrich_pipeline_configs(pipeline_configs))
        validated_pipeline_configs = metadata._validate_pipeline_configs(
            pipeline_configs
        )

        assert isinstance(validated_pipeline_configs[0], Configuration)
