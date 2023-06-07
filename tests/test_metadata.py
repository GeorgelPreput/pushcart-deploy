import asyncio
from pathlib import PosixPath

import pytest

from pushcart_deploy.configuration import Configuration
from pushcart_deploy.metadata import Metadata


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
        enriched_pipeline_configs = await metadata._enrich_pipeline_configs(
            pipeline_configs
        )

        assert any(
            [
                "column_order" in t
                for t in enriched_pipeline_configs[0]["transformations"]
            ]
        )

    def test_validated_pipeline_configs_created_successfully(self):
        """Tests that validated pipeline configurations are created successfully."""
        metadata = Metadata("./tests/data")
        pipeline_configs = asyncio.run(metadata._collect_pipeline_configs())
        enriched_pipeline_configs = asyncio.run(
            metadata._enrich_pipeline_configs(pipeline_configs)
        )
        validated_pipeline_configs = metadata._validate_pipeline_configs(
            enriched_pipeline_configs
        )

        assert isinstance(validated_pipeline_configs[0], Configuration)
