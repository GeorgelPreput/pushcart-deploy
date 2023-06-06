"""Classes and functions to validate Pushcart deployment parameters."""

from typing import Any


class PydanticArbitraryTypesConfig:
    """Pydantic configuration to allow type-checking on arbitrary class types."""

    arbitrary_types_allowed = True


def _is_empty(obj: str | dict | list) -> bool:
    return (
        isinstance(obj, str)
        and not obj.strip()
        or isinstance(obj, dict)
        and not any(obj.values())
        and all(
            not isinstance(n, bool) and not isinstance(n, int) for n in obj.values()
        )
        or isinstance(obj, list)
        and not any(obj)
        and all(not isinstance(n, bool) and not isinstance(n, int) for n in obj)
    )


def _sanitize_object(obj: Any, drop_empty: bool = False) -> Any:  # noqa: ANN401
    if _is_empty(obj):
        return None
    if isinstance(obj, dict):
        return sanitize_dict_fields(obj, drop_empty)
    if isinstance(obj, list):
        return sanitize_list_elements(obj, drop_empty)
    return obj


def sanitize_list_elements(
    list_to_sanitize: list[Any],
    drop_empty: bool = False,
) -> list[Any]:
    """Sanitize elements in a list by replacing empty values with None.

    Parameters
    ----------
    list_to_sanitize : list[Any]
        List to sanitize.
    drop_empty : bool, optional
        Whether to drop empty elements altogether, by default False

    Returns
    -------
    list[Any]
        Sanitized version of input list, with empty values turned to None or dropped
    """
    elements = [_sanitize_object(v, drop_empty) for v in list_to_sanitize]
    if drop_empty:
        return [e for e in elements if e is not None]
    return elements


def sanitize_dict_fields(
    dict_to_sanitize: dict[str, Any],
    drop_empty: bool = False,
) -> dict[str, Any]:
    """Sanitize fields in a dict by replacing empty values with None.

    Parameters
    ----------
    dict_to_sanitize : dict[str, Any]
        Dictionary to sanitize
    drop_empty : bool, optional
        Whether to drop empty fields altogether, by default False

    Returns
    -------
    dict[str, Any]
        A sanitized version of the input dictionary, with empty values replaced by
        None or dropped if drop_empty is True.
    """
    fields = {
        str(k).replace(".", "_"): _sanitize_object(v, drop_empty)
        for k, v in dict_to_sanitize.items()
    }
    if drop_empty:
        return {k: v for k, v in fields.items() if v is not None}
    return fields


def sanitize_empty_objects(obj: dict | list, drop_empty: bool = False) -> dict | list:
    """Drop or replace empty values in a nested dictionary or list with None.

    Parameters
    ----------
    obj : dict | list
        List (of dicts) or nested dict to drop empty values from.
    drop_empty : bool, optional
        Whether to unset empty keys/elements altogether, by default False

    Returns
    -------
    dict | list
        Sanitized version of input object, with empty values turned to None or dropped

    Raises
    ------
    TypeError
        Input object must be a dict or a list.
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        return sanitize_dict_fields(obj, drop_empty)
    if isinstance(obj, list):
        return sanitize_list_elements(obj, drop_empty)

    msg = f"Object must be a dict or a list. Got {type(obj)}: {str(obj)}"
    raise TypeError(msg)
