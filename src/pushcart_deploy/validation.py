from databricks_cli.sdk.api_client import ApiClient
from pydantic import dataclasses, validator


def validate_databricks_api_client(client: ApiClient = None) -> ApiClient:
    """
    Validate the input parameter 'client' of type 'ApiClient' and ensure that it has
    been properly initialized before returning it.
    """
    if not client:
        raise ValueError("ApiClient must have a value")

    if not isinstance(client, ApiClient):
        raise TypeError(
            "Client must be of type databricks_cli.sdk.api_client.ApiClient"
        )

    if not client.url or not client.default_headers:
        raise ValueError("ApiClient has not been properly initialized")

    return client


class PydanticArbitraryTypesConfig:
    arbitrary_types_allowed = True


@dataclasses.dataclass
class HttpAuthToken:
    Authorization: str
    Content_Type: str = "text/json"

    @validator("Authorization")
    @classmethod
    def check_authorization(cls, value):
        if not value.startswith("Bearer "):
            raise ValueError("Authorization must use a bearer token")
        return value


def _is_empty(obj) -> bool:
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


def _sanitize_empty_elements(l: list, drop_empty=False) -> list:
    elements = [
        None
        if _is_empty(v)
        else _santize_empty_fields(v, drop_empty)
        if isinstance(v, dict)
        else _sanitize_empty_elements(v, drop_empty)
        if isinstance(v, list)
        else v
        for v in l
    ]
    if drop_empty:
        return [e for e in elements if e is not None]
    else:
        return elements


def _santize_empty_fields(d: dict, drop_empty=False) -> dict:
    fields = {
        k.replace(".", "_"): None
        if _is_empty(v)
        else _santize_empty_fields(v, drop_empty)
        if isinstance(v, dict)
        else _sanitize_empty_elements(v, drop_empty)
        if isinstance(v, list)
        else v
        for k, v in d.items()
    }
    if drop_empty:
        return {k: v for k, v in fields.items() if v is not None}
    else:
        return fields


def sanitize_empty_objects(o, drop_empty=False):
    if isinstance(o, dict):
        return _santize_empty_fields(o, drop_empty)
    elif isinstance(o, list):
        return _sanitize_empty_elements(o, drop_empty)
    else:
        raise TypeError(f"Object must be a dict or a list. Got {type(o)}: {str(o)}")
