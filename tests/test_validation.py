import pytest

from pushcart_deploy.validation import (
    _is_empty,
    _sanitize_object,
    sanitize_dict_fields,
    sanitize_empty_objects,
    sanitize_list_elements,
)


class Test_IsEmpty:
    def test_non_empty_string(self):
        """Tests that a non-empty string returns False."""
        assert not _is_empty("hello world")

    def test_empty_string(self):
        """Tests that an empty string returns True."""
        assert _is_empty("")

    def test_empty_dict(self):
        """Tests that an empty dictionary returns True."""
        assert _is_empty({})

    def test_dict_with_only_boolean_or_integer_values(self):
        """Tests that a dictionary with only boolean or integer values returns False."""
        assert not _is_empty({"key1": True, "key2": False, "key3": 0, "key4": 1})

    def test_empty_list(self):
        """Tests that an empty list returns True."""
        assert _is_empty([])

    def test_list_with_only_boolean_or_integer_values(self):
        """Tests that a list with only boolean or integer values returns False."""
        assert not _is_empty([True, False, 0, 1])


class Test_SanitizeObject:
    def test_sanitize_object_non_empty_string(self):
        """Tests that the function sanitizes a non-empty string correctly."""
        input_str = "hello world"
        assert _sanitize_object(input_str) == input_str

    def test_sanitize_object_non_empty_dict(self):
        """Tests that the function sanitizes a non-empty dictionary correctly."""
        input_dict = {"name": "John", "age": 30}
        expected_output = {"name": "John", "age": 30}
        assert _sanitize_object(input_dict) == expected_output

    def test_sanitize_object_empty_input(self):
        """Tests that the function returns None for empty input."""
        assert _sanitize_object("") is None

    def test_sanitize_object_non_string_dict_key(self):
        """Tests that the function handles non-string, non-dictionary, and non-list input correctly."""
        input_obj = 123
        assert _sanitize_object(input_obj) == input_obj

    def test_sanitize_object_non_empty_list(self):
        """Tests that the function sanitizes a non-empty list correctly."""
        input_list = [1, 2, 3]
        assert _sanitize_object(input_list) == input_list


class Test_SanitizeElementsInList:
    def test_sanitize_elements_in_list_happy(self):
        """Tests that the function correctly sanitizes a list with non-empty elements."""
        input_list = [1, "hello", {"key": "value"}, True]
        expected_output = [1, "hello", {"key": "value"}, True]
        assert sanitize_list_elements(input_list) == expected_output

    def test_sanitize_elements_in_list_empty(self):
        """Tests that the function correctly sanitizes a list with empty elements."""
        input_list = ["", {}, [], False]
        expected_output = [None, None, None, False]
        assert sanitize_list_elements(input_list) == expected_output

    def test_sanitize_elements_in_list_only_none(self):
        """Tests that the function correctly handles a list with only None elements."""
        input_list = [None, None, None]
        expected_output = [None, None, None]
        assert sanitize_list_elements(input_list) == expected_output

    def test_sanitize_elements_in_list_only_bool(self):
        """Tests that the function correctly handles a list with only boolean elements."""
        input_list = [True, False, True]
        expected_output = [True, False, True]
        assert sanitize_list_elements(input_list) == expected_output

    def test_sanitize_elements_in_list_mixed(self):
        """Tests that the function correctly sanitizes a list with mixed types of elements."""
        input_list = ["", 0, {"key": "value"}, False]
        expected_output = [None, 0, {"key": "value"}, False]
        assert sanitize_list_elements(input_list) == expected_output

    def test_sanitize_elements_in_list_empty_list(self):
        """Tests that the function correctly handles an empty list."""
        input_list = []
        expected_output = []
        assert sanitize_list_elements(input_list) == expected_output


class TestSantizeDictFields:
    def test_sanitize_dict_fields_happy(self):
        """Tests that a dictionary with non-empty values is sanitized correctly."""
        input_dict = {"name": "John", "age": 30, "is_student": True}
        expected_output = {"name": "John", "age": 30, "is_student": True}
        assert sanitize_dict_fields(input_dict) == expected_output

    def test_sanitize_dict_fields_empty(self):
        """Tests that a dictionary with empty values is sanitized correctly."""
        input_dict = {"name": "", "age": None, "is_student": False}
        expected_output = {"name": None, "age": None, "is_student": False}
        assert sanitize_dict_fields(input_dict) == expected_output

    def test_sanitize_dict_fields_dots(self):
        """Tests that keys containing dots are replaced with underscores."""
        input_dict = {"first.name": "John", "last.name": "Doe"}
        expected_output = {"first_name": "John", "last_name": "Doe"}
        assert sanitize_dict_fields(input_dict) == expected_output

    def test_sanitize_dict_fields_nonstring(self):
        """Tests that keys containing non-string values are handled correctly."""
        input_dict = {1: "one", 2: "two", 3: "three"}
        expected_output = {"1": "one", "2": "two", "3": "three"}
        assert sanitize_dict_fields(input_dict) == expected_output

    def test_sanitize_dict_fields_types(self):
        """Tests that values of different types (e.g. string, int, bool) are handled correctly."""
        input_dict = {"name": "", "age": 30, "is_student": False}
        expected_output = {"name": None, "age": 30, "is_student": False}
        assert sanitize_dict_fields(input_dict) == expected_output

    def test_sanitize_dict_fields_nested(self):
        """Tests that a dictionary with nested dictionaries and lists is sanitized correctly."""
        input_dict = {
            "name": "John",
            "age": 30,
            "grades": [80, 90, 95],
            "info": {"city": "New York", "state": "NY"},
        }
        expected_output = {
            "name": "John",
            "age": 30,
            "grades": [80, 90, 95],
            "info": {"city": "New York", "state": "NY"},
        }
        assert sanitize_dict_fields(input_dict) == expected_output


class TestSanitizeEmptyObjects:
    def test_non_empty_input(self):
        """Tests that a non-empty input returns a sanitized version of the input object."""
        input_obj = {"a": "", "b": {"c": [], "d": [1, 2, None]}, "e": None}
        expected_output = {"a": None, "b": {"c": None, "d": [1, 2, None]}, "e": None}
        assert sanitize_empty_objects(input_obj) == expected_output

    def test_drop_empty_true(self):
        """Tests that empty values are dropped from the output when drop_empty is True."""
        input_obj = {"a": "", "b": {"c": [], "d": [1, 2, None]}, "e": None}
        expected_output = {
            "b": {"d": [1, 2]},
        }
        assert sanitize_empty_objects(input_obj, drop_empty=True) == expected_output

    def test_empty_input(self):
        """Tests that an empty input returns an empty list."""
        assert sanitize_empty_objects([]) == []

    def test_invalid_input(self):
        """Tests that an input object that contains values that are not of valid types raises a TypeError."""
        with pytest.raises(TypeError):
            sanitize_empty_objects("not a dict or list")

    def test_circular_reference(self):
        """Tests that the function handles circular references in the input object."""
        input_obj = {"a": {"b": None}}
        input_obj["a"]["b"] = input_obj

        with pytest.raises(RecursionError):
            sanitize_empty_objects(input_obj)

    def test_drop_empty_false(self):
        """Tests that empty values are replaced with None in the output when drop_empty is False."""
        input_obj = {"a": "", "b": {"c": [], "d": [1, 2, None]}, "e": None}
        expected_output = {"a": None, "b": {"c": None, "d": [1, 2, None]}, "e": None}
        assert sanitize_empty_objects(input_obj, drop_empty=False) == expected_output

    def test_input_none(self):
        assert sanitize_empty_objects(None) is None
