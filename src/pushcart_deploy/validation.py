from databricks_cli.sdk.api_client import ApiClient
from pydantic import dataclasses, validator


def validate_databricks_api_client(client: ApiClient = None) -> ApiClient:
    """Validate the input parameter 'client' of type 'ApiClient' and ensure that it has been properly initialized before returning it."""
    if not client:
        msg = "ApiClient must have a value"
        raise ValueError(msg)

    if not isinstance(client, ApiClient):
        msg = "Client must be of type databricks_cli.sdk.api_client.ApiClient"
        raise TypeError(
            msg,
        )

    if not client.url or not client.default_headers:
        msg = "ApiClient has not been properly initialized"
        raise ValueError(msg)

    return client


class PydanticArbitraryTypesConfig:
    arbitrary_types_allowed = True


@dataclasses.dataclass
class HttpAuthToken:
    Authorization: str
    Content_Type: str = "text/json"

    @validator("Authorization")
    @classmethod
    def check_authorization(cls, value: str) -> str:
        if not value.startswith("Bearer "):
            msg = "Authorization must use a bearer token"
            raise ValueError(msg)
        return value


def _is_empty(obj: str | dict | list) -> bool:
    if isinstance(obj, str) and not obj.strip():
        return True
    if (
        isinstance(obj, dict)
        and not any(obj.values())
        and not any(isinstance(n, bool) for n in obj.values())
        and not any(isinstance(n, int) for n in obj.values())
    ):
        return True
    if (
        isinstance(obj, list)
        and not any(obj)
        and not any(isinstance(n, bool) for n in obj)
        and not any(isinstance(n, int) for n in obj)
    ):
        return True

    return False


def _sanitize_empty_elements(list_to_sanitize: list, drop_empty: bool = False) -> list:
    elements = [
        None
        if _is_empty(v)
        else _santize_empty_fields(v, drop_empty)
        if isinstance(v, dict)
        else _sanitize_empty_elements(v, drop_empty)
        if isinstance(v, list)
        else v
        for v in list_to_sanitize
    ]
    if drop_empty:
        return [e for e in elements if e is not None]
    else:
        return elements


def _santize_empty_fields(dict_to_sanitize: dict, drop_empty: bool = False) -> dict:
    fields = {
        k.replace(".", "_"): None
        if _is_empty(v)
        else _santize_empty_fields(v, drop_empty)
        if isinstance(v, dict)
        else _sanitize_empty_elements(v, drop_empty)
        if isinstance(v, list)
        else v
        for k, v in dict_to_sanitize.items()
    }
    if drop_empty:
        return {k: v for k, v in fields.items() if v is not None}
    else:
        return fields


def sanitize_empty_objects(obj: dict | list, drop_empty: bool = False) -> dict | list:
    if isinstance(obj, dict):
        return _santize_empty_fields(obj, drop_empty)
    elif isinstance(obj, list):
        return _sanitize_empty_elements(obj, drop_empty)
    else:
        msg = f"Object must be a dict or a list. Got {type(obj)}: {str(obj)}"
        raise TypeError(msg)
