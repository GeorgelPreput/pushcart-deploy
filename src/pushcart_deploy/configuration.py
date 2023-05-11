import csv
import json
import logging
from collections import defaultdict
from collections.abc import AsyncIterator
from io import StringIO
from itertools import groupby
from pathlib import Path

import aiofiles
import tomli
import yaml
from pydantic import (
    Field,
    conint,
    conlist,
    constr,
    dataclasses,
    root_validator,
    validate_arguments,
    validator,
)


async def get_transformations_from_csv(csv_path: Path | str) -> AsyncIterator[dict]:
    """Return transformations from the metadata .csv line-by-line.

    Parameters
    ----------
    csv_path : Path | str
        Path to an existing .csv file

    Returns
    -------
    AsyncIterator[dict]
        Iterates over transformation specifications in the input file

    Yields
    ------
    Iterator[AsyncIterator[dict]]
        One row containing details for one transformation step
    """
    if isinstance(csv_path, str):
        csv_path = Path(csv_path)

    async with aiofiles.open(csv_path, "r") as aio_file:
        contents = await aio_file.read()
        csv_file = StringIO(contents)
        reader = csv.DictReader(csv_file)

        for row in reader:
            yield row


@validate_arguments
async def get_config_from_file(settings_path: Path | str) -> dict | None:
    """Load a configuration file into a dictionary. Supported formats are JSON, YAML, and TOML.

    Args:
    ----
        settings_path (Path): The path to the configuration file.

    Returns:
    -------
        dict | None: The configuration data as a dictionary, or None if an error occurred.
    """
    loaders = defaultdict(
        lambda: None,
        {
            ".json": json.loads,
            ".toml": tomli.loads,
            ".yaml": yaml.safe_load,
            ".yml": yaml.safe_load,
        },
    )

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    try:
        if isinstance(settings_path, str):
            settings_path = Path(settings_path)

        if settings_path.exists():
            ext = settings_path.suffix
            async with aiofiles.open(settings_path, "r") as settings_file:
                contents = await settings_file.read()

                return loaders[ext](contents)
    except FileNotFoundError:
        log.warning(f"File not found: {settings_path.as_posix()}")
    except OSError:
        log.warning(f"Could not open file: {settings_path.as_posix()}")
    except (json.JSONDecodeError, yaml.error.YAMLError, tomli.TOMLDecodeError):
        log.warning(f"File is not valid: {settings_path.as_posix()}")
    except TypeError:
        log.warning(f"Unsupported file type: {settings_path.as_posix()}")
    except RuntimeError as e:
        log.warning(f"Skipping: {settings_path.as_posix()} Encountered: {e}")


def _get_multiple_validations_with_same_rule(validations: dict) -> dict:
    """Group a list of validations by their rule and return only the groups that have more than one validation with the same rule."""
    validation_groups = {
        k: [v["validation_action"] for v in v]
        for k, v in groupby(
            sorted(validations, key=lambda v: str(v["validation_rule"]).strip()),
            lambda r: str(r["validation_rule"]).strip(),
        )
    }

    return {k: v for k, v in validation_groups.items() if len(v) > 1}


@dataclasses.dataclass
class Validation:
    """The Validation class is designed to provide a way to define validation rules and
    actions for data. The class has two main fields: validation_rule and
    validation_action. The validation_rule field is a Spark SQL string that defines the
    rule that the input data must follow, while the validation_action field is a string
    that defines the action to take if the input data fails to meet the validation rule.

    Fields:
    The Validation class has two main fields: validation_rule and validation_action.
    The validation_rule field is a string that defines the rule that the input data
    must follow. It has a minimum length of 1 character, which ensures that the input
    data is not empty. The validation_action field is a string that defines the action
    to take if the input data fails to meet the validation rule. It must be one of
    three values: LOG, DROP, or FAIL. If the input data fails to meet the validation
    rule, the action specified in this field will be taken.
    """

    validation_rule: constr(min_length=1, strict=True)
    validation_action: constr(to_upper=True, strict=True, regex=r"\A(LOG|DROP|FAIL)\Z")

    def __getitem__(self, item):
        # Without this Pydantic throws ValidationError: object is not subscriptable
        return self.__getattribute__(item)


@dataclasses.dataclass
class ClusterAutoscale:
    min_workers: int
    max_workers: int
    mode: constr(to_upper=True, strict=True, regex=r"\A(ENHANCED|LEGACY)\Z")


@dataclasses.dataclass
class Cluster:
    label: constr(to_lower=True, strict=True, regex=r"\A(default|maintenance)\Z")
    node_type_id: constr(min_length=1, strict=True)
    spark_conf: dict[str, str] | None = Field(default_factory=dict)
    aws_attributes: dict[str, str] | None = Field(default_factory=dict)
    driver_node_type_id: constr(min_length=1, strict=True) | None = None
    ssh_public_keys: list[str] | None = Field(default_factory=list)
    custom_tags: dict[str, str] | None = Field(default_factory=dict)
    cluster_log_conf: dict[str, dict[str, str]] | None = Field(default_factory=dict)
    spark_env_vars: dict[str, str] | None = Field(default_factory=dict)
    init_scripts: list[dict[str, dict[str, str]]] | None = Field(
        default_factory=list,
    )
    instance_pool_id: constr(min_length=1, strict=True) | None = None
    driver_instance_pool_id: constr(min_length=1, strict=True) | None = None
    policy_id: constr(min_length=1, strict=True) | None = None
    num_workers: int | None = None
    autoscale: ClusterAutoscale | None = None

    @root_validator(pre=True)
    @classmethod
    def check_only_one_of_autoscale_or_num_workers_defined(cls, values):
        """Root validator method that checks that only one of the autoscale or num_workers
        fields is defined and that at least one of them is defined.
        """
        if not any(values[v] for v in ["autoscale", "num_workers"]):
            msg = "No cluster defined. Please provide either autoscale or a num_workers"
            raise ValueError(
                msg,
            )
        if all(values[t] for t in ["autoscale", "num_workers"]):
            msg = "Only one of autoscale or num_workers allowed"
            raise ValueError(msg)
        return values


@dataclasses.dataclass
class Source:
    """The Source class is designed to represent a data source and its associated
    metadata. It can handle different types of data sources, including local files,
    remote URLs, and non-empty strings. The class also allows for optional parameters
    and validations to be associated with the data source.

    Fields:
    The Source class has five main fields:
    - origin: represents the data source itself, and can be of type Path, AnyUrl, or
      constr.
    - type: represents the type of data contained in the source, and is a string with a
      minimum length of 1.
    - target: represents the destination for the data, and is a string with a minimum
      length of 1.
    - params: represents optional parameters associated with the data source, and is a
      dictionary.
    - validations: optional validation rules associated with the data source. Each
      Validation object has a validation_rule field (Spark SQL) and a validation_action
      field (a string that must be either "LOG", "DROP", or "FAIL").
    """

    origin: constr(min_length=1, strict=True)
    datatype: constr(min_length=1, strict=True)
    target: constr(min_length=1, strict=True)
    params: str | None = None
    validations: list[Validation] | None = Field(default_factory=list)

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value):
        """Validator that checks that there are no multiple validations with the same rule."""
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            msg = f"Different actions for the same validation:\n{fails}"
            raise ValueError(msg)
        return value


@dataclasses.dataclass
class Transformation:
    """The Transformation class is designed to represent a data transformation with
    optional validation rules. It ensures that only one of the config or sql_query
    fields is defined and that at least one of them is defined. It also allows for a
    list of Validation objects to be included to ensure that the transformation meets
    certain criteria.

    Fields:
    - origin: a required string field that represents the data view to be transformed.
    - target: a required string field that represents the desired output of the
      transformation.
    - config: an optional FilePath field that represents the configuration file to be
      used for the transformation.
    - sql_query: an optional string field that represents the SQL query to be used for
      the transformation.
    - validations: an optional list of Validation objects that represent validation
      rules to be applied to the transformation.
    """

    origin: constr(min_length=1, strict=True)
    target: constr(min_length=1, strict=True)
    column_order: conint(ge=1) | None = None
    source_column_name: constr(strict=True) | None = None
    source_column_type: constr(
        strict=True,
        regex="\\A(string|int|double|date|timestamp|boolean|struct|array|map)\\Z",
    ) | None = None
    dest_column_name: constr(strict=True) | None = None
    dest_column_type: constr(
        strict=True,
        regex="\\A(string|int|double|date|timestamp|boolean|struct|array|map)\\Z",
    ) | None = None
    transform_function: constr(strict=True) | None = None
    sql_query: constr(min_length=1, strict=True) | None = None
    default_value: constr(strict=True) | None = None
    validations: list[Validation] | None = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_only_one_of_config_or_sql_query_defined(cls, values):
        """Root validator method that checks that only one of the config or sql_query
        fields is defined and that at least one of them is defined.
        """
        if not any(values.get(v) for v in ["column_order", "sql_query"]):
            msg = f"No transformation defined. Please provide either a config or a sql_query.\nGot: {values}"
            raise ValueError(msg)
        if all(values.get(t) for t in ["column_order", "sql_query"]):
            msg = f"Only one of config or sql_query allowed.Got: {values}"
            raise ValueError(msg)
        return values

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value: dict) -> dict:
        """Validate that there are no multiple validations with the same rule."""
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            msg = f"Different actions for the same validation:\n{fails}"
            raise ValueError(msg)
        return value


@dataclasses.dataclass
class Destination:
    """Represents a Delta table destination for a batch of data. It has fields for the
    source data view, the destination table, the path to the destination, the mode of
    writing (append or upsert), keys and sequence_by for upsert mode, and optional
    validations. The class provides validation for the fields and checks that the keys
    and sequence_by fields are defined for upsert mode. It also checks that there are
    no multiple validations with the same rule.

    Fields:
    - origin: a string that represents the data view to be written to the destination.
    - target: a string that represents the destination table.
    - path: an optional string that represents the file path for the destination Delta
      table. If not set, the Delta table will be managed by Databricks
    - mode: a string that represents the mode of writing (append or upsert).
    - keys: an optional list of strings that represents the keys for upsert mode.
    - sequence_by: an optional string that represents the sequence_by field for upsert
      mode.
    - validations: an optional list of Validation objects that represent validation
      rules and actions for the data.
    """

    origin: constr(min_length=1, strict=True)
    target: constr(min_length=1, strict=True)
    mode: constr(min_length=1, strict=True, regex=r"^(append|upsert)$")
    path: Path | None = None
    keys: list[constr(min_length=1, strict=True)] | None = Field(
        default_factory=list,
    )
    sequence_by: constr(min_length=1, strict=True) | None = None
    validations: list[Validation] | None = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_keys_and_sequence_for_upsert(cls, values):
        """Root validator that checks that the keys and sequence_by fields are defined for
        upsert mode.
        """
        if values.get("mode") == "upsert" and not all(
            values[v] for v in ["keys", "sequence_by"]
        ):
            msg = "Mode upsert requires that keys and sequence_by are defined"
            raise ValueError(
                msg,
            )
        return values

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value):
        """Validator that checks that there are no multiple validations with the same rule."""
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            msg = f"Different actions for the same validation:\n{fails}"
            raise ValueError(msg)
        return value

    @validator("path", pre=False, always=True)
    @classmethod
    def convert_to_absolute_string(cls, value: Path | None) -> str | None:
        """Validator that converts the Path object to its absolute POSIX representation."""
        if value:
            return value.absolute().as_posix()

        return None


@dataclasses.dataclass
class Configuration:
    """Represents a configuration file for a data pipeline.

    Returns
    -------
    Configuration
        Contains optional lists of Cluster, Source, Transformation, and Destination
        objects, which define the stages of the pipeline. Provides validation to ensure
        that at least one stage is defined in the configuration file.

    Raises
    ------
    ValueError
        At least one stage definition (Source, Transformation, Destination) must exist.
    ValueError
        All values in the "target" fields of all pipeline stages taken together must be
        unique.
    """

    clusters: conlist(Cluster, max_items=1) | None = None
    sources: list[Source] | None = Field(default_factory=list)
    transformations: list[Transformation] | None = Field(default_factory=list)
    destinations: list[Destination] | None = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_at_least_one_stage_defined(cls, values: dict) -> dict:
        """Check that at least one of the sources, transformations, or destinations fields is defined in the configuration file."""
        if not any(v in ["sources", "transformations", "destinations"] for v in values):
            msg = "No stage definition found. Please define at least one of: sources, transformations, destinations"
            raise ValueError(msg)
        return values

    @root_validator(pre=True)
    @classmethod
    def check_all_dlt_target_objects_are_unique(cls, values: dict) -> dict:
        """Check that no values of the "target" fields of "sources", "transformations" and "destinations", taken together, overlap."""
        sources = values.get("sources", [])
        transformations = values.get("transformations", [])
        destinations = values.get("destinations", [])

        target_values = (
            [source["target"] for source in sources]
            + [
                transformation["target"]
                for transformation in transformations
                if transformation.get("sql_query")
            ]
            + [destination["target"] for destination in destinations]
        )

        duplicates = [
            value for value in set(target_values) if target_values.count(value) > 1
        ]

        if duplicates:
            msg = f"Duplicate 'target' values found: {', '.join(duplicates)}"
            raise ValueError(msg)

        return values
