"""Base settings loader to be used by PipelineSettings and JobSettings.

Loads a configuration from file. Settings may come in JSON, TOML or YAML formats.
"""

import asyncio
import logging
from pathlib import Path

from pydantic import DirectoryPath, constr, dataclasses, validate_arguments

from pushcart_deploy.configuration import expect_at_most_one_file, get_config_from_file
from pushcart_deploy.validation import PydanticArbitraryTypesConfig


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class BaseSettings:
    """Base Databricks API settings class.

    Retrieves pipeline/job settings file, if one exists.
    """

    config_dir: DirectoryPath

    def __post_init_post_parse__(self) -> None:
        """Initialize logger."""
        self.log = logging.getLogger(__name__)

    def get_settings_path_for_pipeline(
        self,
        target_catalog_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
        ),
        target_schema_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
        ),
        pipeline_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
        ),
        settings_file_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
            regex=r"^(_job_settings|_pipeline_settings)$",
        ),
    ) -> Path:
        """Look for a settings file in the pipeline directory.

        Parameters
        ----------
        target_catalog_name : str
            Target catalog name for pipeline. Used in composing settings path
        target_schema_name : str
            Target schema name for pipeline. Used in composing settings path
        pipeline_name : str
            Pipeline name, as in DLT Workflows. Used in composing settings path
        settings_file_name : str
            _pipeline_settings or _job_settings

        Returns
        -------
        Path
            Path object to settings file.
        """
        base_path = (
            Path(self.config_dir)
            / "pipelines"
            / target_catalog_name
            / target_schema_name
            / pipeline_name
            / settings_file_name
        )

        return expect_at_most_one_file(base_path)

    @validate_arguments
    def load_settings(
        self,
        target_catalog_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
        ),
        target_schema_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
        ),
        pipeline_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
        ),
        settings_file_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
            regex=r"^(_job_settings|_pipeline_settings)$",
        ),
    ) -> dict:
        """Load pipeline/job settings from file, if one exists.

        Parameters
        ----------
        target_catalog_name : str
            Target catalog name for pipeline. Used in composing settings path
        target_schema_name : str
            Target schema name for pipeline. Used in composing settings path
        pipeline_name : str
            Pipeline name, as in DLT Workflows. Used in composing settings path
        settings_file_name : str
            _pipeline_settings or _job_settings

        Returns
        -------
        dict
            Dictionary containing API settings
        """
        settings = None

        if settings_path := self.get_settings_path_for_pipeline(
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
            pipeline_name=pipeline_name,
            settings_file_name=settings_file_name,
        ):
            settings = asyncio.run(get_config_from_file(settings_path))

        if not settings:
            self.log.info(
                f"Settings file not found: {settings_file_name}.[json|toml|yaml].",
            )

        return settings
