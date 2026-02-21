from ._connect import connect
from ._errors import (
    ColumnCountMismatchError,
    ColumnNotFoundError,
    NullabilityError,
    TypeMismatchError,
    UnsupportedTypeError,
    ValidationError,
)
from ._query import Execute, Query
from ._sync_client import SyncClient
from ._validate import FearOfSQL, collect_errors

__all__ = [
    "AsyncClient",
    "ColumnCountMismatchError",
    "ColumnNotFoundError",
    "Execute",
    "FearOfSQL",
    "NullabilityError",
    "Query",
    "SyncClient",
    "TypeMismatchError",
    "UnsupportedTypeError",
    "ValidationError",
    "collect_errors",
    "connect",
]


def __getattr__(name: str) -> type:  # pragma: no cover
    if name == "AsyncClient":
        from ._async_client import AsyncClient  # noqa: PLC0415

        return AsyncClient
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
