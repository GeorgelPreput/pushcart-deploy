from pathlib import Path

import pytest
from pydantic_core._pydantic_core import ValidationError

from pushcart_deploy.configuration import (
    Configuration,
    Destination,
    Source,
    Transformation,
    Validation,
    _get_multiple_validations_with_same_rule,
    get_config_from_file,
)


class TestValidation:
    def test_validation_happy_path(self):
        """Tests that the validation_action field matches one of the three allowed values
        and that validation_rule is a string.
        """
        for action in ["LOG", "DROP", "FAIL"]:
            validation = Validation(validation_rule="test", validation_action=action)

            assert validation.validation_action in ["LOG", "DROP", "FAIL"]
            assert isinstance(validation.validation_rule, str)

    def test_validation_rule_string(self):
        """Tests that the validation_rule field is a string."""
        with pytest.raises(ValueError) as e:
            Validation(validation_rule=123, validation_action="LOG")
        assert "Input should be a valid string" in str(e.value)

    def test_validation_rule_min_length(self):
        """Tests that the validation_rule field has a minimum length of 1."""
        with pytest.raises(ValueError) as e:
            Validation(validation_rule="", validation_action="LOG")
        assert "String should have at least 1 characters" in str(e.value)


class TestGetConfigFromFile:
    @pytest.mark.asyncio()
    async def test_get_config_from_file_valid_file_path(self, mocker):
        """Tests that the function successfully loads a valid JSON/YAML/TOML file."""
        test_file = Path(
            "tests/data/pipelines/sample_catalog/sample_schema/sample_pipeline/_job_settings.json"
        )
        test_data = {"name": "", "timeout_seconds": 60}

        mocker.patch("json.loads", return_value=test_data)

        result = await get_config_from_file(test_file)

        assert result == test_data

    @pytest.mark.asyncio()
    async def test_get_config_from_file_invalid_file_path(self):
        """Tests that the function returns None when an invalid file path is provided."""
        test_file = Path("invalid_path.json")

        result = await get_config_from_file(test_file)

        assert result is None


class TestGetMultipleValidationsWithSameRule:
    def test_one_rule_multiple_validations(self):
        """Tests that the function groups validations correctly when the validations list contains only one rule with multiple validations."""
        validations = [
            {"validation_rule": "rule1", "validation_action": "action1"},
            {"validation_rule": "rule1", "validation_action": "action2"},
            {"validation_rule": "rule1", "validation_action": "action3"},
        ]
        expected_output = {"rule1": ["action1", "action2", "action3"]}
        assert _get_multiple_validations_with_same_rule(validations) == expected_output

    def test_different_rules(self):
        """Tests that the function groups validations correctly when the validations list contains validations with different rules."""
        validations = [
            {"validation_rule": "rule1", "validation_action": "action1"},
            {"validation_rule": "rule2", "validation_action": "action2"},
            {"validation_rule": "rule1", "validation_action": "action3"},
            {"validation_rule": "rule2", "validation_action": "action4"},
        ]
        expected_output = {
            "rule1": ["action1", "action3"],
            "rule2": ["action2", "action4"],
        }
        assert _get_multiple_validations_with_same_rule(validations) == expected_output

    def test_empty_list(self):
        """Tests that the function returns an empty dictionary when the validations list is empty."""
        validations = []
        expected_output = {}
        assert _get_multiple_validations_with_same_rule(validations) == expected_output

    def test_one_validation(self):
        """Tests that the function returns an empty dictionary when the validations list contains only one validation."""
        validations = [{"validation_rule": "rule1", "validation_action": "action1"}]
        result = _get_multiple_validations_with_same_rule(validations)
        assert result == {}

    def test_invalid_data_types(self):
        """Tests that the function handles invalid data types in the validations list."""
        validations = [
            {"validation_rule": "rule1", "validation_action": "action1"},
            "invalid_data",
        ]

        with pytest.raises(TypeError):
            _get_multiple_validations_with_same_rule(validations)

    def test_duplicate_validations(self):
        """Tests that the function handles duplicate validations in the validations list."""
        validations = [
            {"validation_rule": "rule1", "validation_action": "action1"},
            {"validation_rule": "rule2", "validation_action": "action2"},
            {"validation_rule": "rule1", "validation_action": "action3"},
            {"validation_rule": "rule2", "validation_action": "action4"},
            {"validation_rule": "rule3", "validation_action": "action5"},
            {"validation_rule": "rule4", "validation_action": "action6"},
            {"validation_rule": "rule4", "validation_action": "action7"},
            {"validation_rule": "rule4", "validation_action": "action8"},
        ]
        result = _get_multiple_validations_with_same_rule(validations)
        assert result == {
            "rule1": ["action1", "action3"],
            "rule2": ["action2", "action4"],
            "rule4": ["action6", "action7", "action8"],
        }


class TestSource:
    def test_create_source_with_valid_data(self):
        """Tests that a source object can be created with valid data, type, and target fields."""
        target_catalog_name = "sample_catalog"
        target_schema_name = "sample_schema"
        pipeline_name = "sample_pipeline"

        origin = "./tests/data/sample_catalog/sample_schema/sample_pipeline/example.csv"
        type = "csv"
        target = "my_view"
        source = Source(
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
            pipeline_name=pipeline_name,
            origin=origin,
            datatype=type,
            target=target,
        )
        assert source.origin == origin
        assert source.datatype == type
        assert source.target == target

    def test_create_source_with_optional_params_and_validations(self):
        """Tests that a source object can be created with optional params and validations fields."""
        target_catalog_name = "sample_catalog"
        target_schema_name = "sample_schema"
        pipeline_name = "sample_pipeline"

        origin = "./tests/data/sample_catalog/sample_schema/sample_pipeline/example.csv"
        type = "csv"
        target = "my_view"
        params = '{"delimiter": ","}'
        validation_rule = "column_1 > 0"
        validation_action = "LOG"
        validations = [
            {
                "validation_rule": validation_rule,
                "validation_action": validation_action,
            },
        ]
        source = Source(
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
            pipeline_name=pipeline_name,
            origin=origin,
            datatype=type,
            target=target,
            params=params,
            validations=validations,
        )
        assert source.origin == origin
        assert source.datatype == type
        assert source.target == target
        assert source.params == params
        assert source.validations == [
            Validation(
                validation_rule=validation_rule,
                validation_action=validation_action,
            ),
        ]

    def test_create_source_with_invalid_data(self):
        """Tests that a source object cannot be created with empty strings in the
        target_catalog_name, target_schema_name, pipeline_name, origin, type and target fields.
        """
        with pytest.raises(ValueError):
            Source(
                target_catalog_name="",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="./tests/data/sample_catalog/sample_schema/sample_pipeline/example.csv",
                datatype="csv",
                target="my_view",
            )
        with pytest.raises(ValueError):
            Source(
                target_catalog_name="sample_catalog",
                target_schema_name="",
                pipeline_name="sample_pipeline",
                origin="./tests/data/sample_catalog/sample_schema/sample_pipeline/example.csv",
                datatype="csv",
                target="my_view",
            )
        with pytest.raises(ValueError):
            Source(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="",
                origin="./tests/data/sample_catalog/sample_schema/sample_pipeline/example.csv",
                datatype="csv",
                target="my_view",
            )
        with pytest.raises(ValueError):
            Source(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="",
                datatype="csv",
                target="my_view",
            )
        with pytest.raises(ValueError):
            Source(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="./tests/data/sample_catalog/sample_schema/sample_pipeline/example.csv",
                datatype="",
                target="my_view",
            )
        with pytest.raises(ValueError):
            Source(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="./tests/data/sample_catalog/sample_schema/sample_pipeline/example.csv",
                datatype="csv",
                target="",
            )

    def test_source_allows_optional_params_and_validations(self):
        """Test that the Source class allows for optional parameters and validations to be
        associated with the data source.
        """
        source_with_params = Source(
            target_catalog_name="sample_catalog",
            target_schema_name="sample_schema",
            pipeline_name="sample_pipeline",
            origin="data/example.csv",
            datatype="csv",
            target="my_view",
            params='{"delimiter": ","}',
        )
        source_with_validations = Source(
            target_catalog_name="sample_catalog",
            target_schema_name="sample_schema",
            pipeline_name="sample_pipeline",
            origin="data/example.csv",
            datatype="csv",
            target="my_view",
            validations=[Validation(validation_rule="rule1", validation_action="LOG")],
        )

        assert source_with_params.params is not None
        assert source_with_validations.validations is not None


class TestTransformation:
    def test_transformation_with_config_and_validations(self):
        """Tests that a transformation with a config file and validations is correctly
        instantiated and validated.
        """
        transformation = Transformation(
            target_catalog_name="sample_catalog",
            target_schema_name="sample_schema",
            pipeline_name="sample_pipeline",
            origin="some data",
            target="some output",
            column_order=1,
            source_column_name="column_1",
            source_column_type="string",
            dest_column_name="ColumnOne",
            dest_column_type="string",
            sql_query=None,
            validations=[
                {"validation_rule": "rule1", "validation_action": "LOG"},
                {"validation_rule": "rule2", "validation_action": "DROP"},
            ],
        )
        assert transformation.target_catalog_name == "sample_catalog"
        assert transformation.target_schema_name == "sample_schema"
        assert transformation.pipeline_name == "sample_pipeline"
        assert transformation.origin == "some data"
        assert transformation.target == "some output"
        assert transformation.column_order == 1
        assert transformation.source_column_name == "column_1"
        assert transformation.source_column_type == "string"
        assert transformation.dest_column_name == "ColumnOne"
        assert transformation.dest_column_type == "string"
        assert transformation.validations[0].validation_rule == "rule1"
        assert transformation.validations[0].validation_action == "LOG"
        assert transformation.validations[1].validation_rule == "rule2"
        assert transformation.validations[1].validation_action == "DROP"

    def test_transformation_without_validations(self):
        """Tests that a transformation without validations is correctly instantiated and
        validated.
        """
        transformation = Transformation(
            target_catalog_name="sample_catalog",
            target_schema_name="sample_schema",
            pipeline_name="sample_pipeline",
            origin="some data",
            target="some output",
            sql_query="SELECT * FROM table",
        )
        assert transformation.target_catalog_name == "sample_catalog"
        assert transformation.target_schema_name == "sample_schema"
        assert transformation.pipeline_name == "sample_pipeline"
        assert transformation.origin == "some data"
        assert transformation.target == "some output"
        assert transformation.sql_query == "SELECT * FROM table"
        assert transformation.validations == []

    def test_transformation_with_empty_fields(self):
        """Tests that a transformation with empty origin or target fields is correctly
        validated.
        """
        with pytest.raises(ValueError):
            Transformation(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="",
                target="my_output_view",
                sql_query="SELECT * FROM table",
            )
        with pytest.raises(ValueError):
            Transformation(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="my_input_view",
                target="",
                sql_query="SELECT * FROM table",
            )

    def test_transformation_with_both_column_metadata_and_sql_query_defined(self):
        """Tests that a transformation with both per-column metadata-driven transforms
        and sql query fields defined is correctly validated.
        """
        with pytest.raises(ValueError):
            Transformation(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="my_input_view",
                target="my_output_view",
                source_column_name="input_column",
                dest_column_name="output_column",
                sql_query="SELECT * FROM table",
            )

    def test_transformation_with_no_config_or_sql_query_defined(self):
        """Tests that a transformation with neither config nor sql query fields defined is
        correctly validated.
        """
        with pytest.raises(ValueError):
            Transformation(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="test",
                target="test",
            )

    def test_multiple_validations_with_same_rule(self):
        """Tests that multiple validation objects with the same validation rule are
        correctly handled.
        """
        validation1 = Validation(validation_rule="rule1", validation_action="LOG")
        validation2 = Validation(validation_rule="rule1", validation_action="DROP")

        with pytest.raises(ValueError) as e:
            Transformation(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="my_input_view",
                target="my_output_view",
                source_column_name="input_column",
                dest_column_name="output_column",
                validations=[validation1, validation2],
            )
        assert "Different actions for the same validation" in str(e.value)


class TestDestination:
    def test_valid_input_data(self):
        """Tests that the destination class can be instantiated with valid input data."""
        dest = Destination(
            target_catalog_name="sample_catalog",
            target_schema_name="sample_schema",
            pipeline_name="sample_pipeline",
            origin="my_data",
            target="my_table",
            path="/path/to/destination",
            mode="upsert",
            keys=["key1", "key2"],
            sequence_by="seq_col",
            validations=[
                {"validation_rule": "col1 > 0", "validation_action": "LOG"},
                {"validation_rule": "col2 < 100", "validation_action": "DROP"},
            ],
        )
        assert dest.target_catalog_name == "sample_catalog"
        assert dest.target_schema_name == "sample_schema"
        assert dest.pipeline_name == "sample_pipeline"
        assert dest.origin == "my_data"
        assert dest.target == "my_table"
        assert dest.path == "/path/to/destination"
        assert dest.mode == "upsert"
        assert dest.keys == ["key1", "key2"]
        assert dest.sequence_by == "seq_col"
        assert len(dest.validations) == 2

    def test_missing_required_fields(self):
        """Tests that the destination class raises a validation error when required fields
        are missing.
        """
        with pytest.raises(ValidationError):
            Destination(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="my_data",
                mode="append",
            )
        with pytest.raises(ValidationError):
            Destination(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="my_data",
                mode="append",
                target=None,
            )

    def test_invalid_mode_value(self):
        """Tests that the destination class raises a ValueError when an invalid mode value
        is provided.
        """
        with pytest.raises(ValueError):
            Destination(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="my_data",
                target="my_table",
                path="/path/to/destination",
                mode="invalid_mode",
            )

    def test_missing_keys_or_sequence_by_for_upsert(self):
        """Tests that the destination class raises a ValueError when mode is upsert but
        keys or sequence_by fields are missing.
        """
        with pytest.raises(KeyError):
            Destination(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="test_origin",
                target="test_target",
                path="test_path",
                mode="upsert",
            )

    def test_invalid_input_data_for_fields_with_constraints(self):
        """Tests that the destination class raises a ValueError when invalid input data is
        provided for fields with constraints.
        """
        with pytest.raises(ValueError):
            Destination(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="",
                target="test_target",
                path="test_path",
                mode="append",
                keys=["test_key"],
                sequence_by="test_sequence",
            )

    def test_multiple_validations_with_same_rule(self):
        """Tests that the destination class raises a ValueError when there are multiple
        validations with the same rule.
        """
        with pytest.raises(ValueError):
            Destination(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="test_origin",
                target="test_target",
                path="test_path",
                mode="append",
                keys=["test_key"],
                sequence_by="test_sequence",
                validations=[
                    Validation(validation_rule="rule1", validation_action="LOG"),
                    Validation(validation_rule="rule1", validation_action="DROP"),
                ],
            )


class TestConfiguration:
    def test_all_fields_defined(self):
        """Tests that a configuration object can be created with all three fields defined."""
        config_dict = {
            "sources": [
                {
                    "target_catalog_name": "sample_catalog",
                    "target_schema_name": "sample_schema",
                    "pipeline_name": "sample_pipeline",
                    "origin": "path/to/data",
                    "datatype": "csv",
                    "target": "temp_table",
                },
            ],
            "transformations": [
                {
                    "target_catalog_name": "sample_catalog",
                    "target_schema_name": "sample_schema",
                    "pipeline_name": "sample_pipeline",
                    "origin": "temp_table",
                    "target": "output_table",
                    "sql_query": "SELECT * FROM temp_table",
                },
            ],
            "destinations": [
                {
                    "target_catalog_name": "sample_catalog",
                    "target_schema_name": "sample_schema",
                    "pipeline_name": "sample_pipeline",
                    "origin": "output_table",
                    "target": "delta_table",
                    "mode": "append",
                },
            ],
        }
        config = Configuration(**config_dict)

        assert config.sources == [
            Source(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="path/to/data",
                datatype="csv",
                target="temp_table",
            ),
        ]
        assert config.transformations == [
            Transformation(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="temp_table",
                target="output_table",
                sql_query="SELECT * FROM temp_table",
            ),
        ]
        assert config.destinations == [
            Destination(
                target_catalog_name="sample_catalog",
                target_schema_name="sample_schema",
                pipeline_name="sample_pipeline",
                origin="output_table",
                target="delta_table",
                mode="append",
            ),
        ]

    def test_invalid_stage_object(self):
        """Tests that a valueerror is raised when an invalid stage object is provided to the configuration class."""
        with pytest.raises(ValueError):
            invalid_source = [
                {
                    "origin": "path/to/data",
                    "type": "csv",
                    "target": "temp_table",
                }
            ]
            Configuration(**{"sources": invalid_source})

    def test_empty_configuration_object(self):
        """Tests that an empty configuration object cannot be created."""
        with pytest.raises(ValueError) as e:
            Configuration()

        assert "No stage definition found" in str(e.value)
