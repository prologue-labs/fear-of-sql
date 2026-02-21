from collections.abc import Mapping, Sequence
from typing import Any, Protocol


# Minimal PEP 249 (DB-API 2.0) protocols.
# Based on _typeshed.dbapi:
# https://github.com/python/typeshed/blob/main/stdlib/_typeshed/dbapi.pyi
class DBAPICursor(Protocol):
    @property
    def description(self) -> Sequence[Sequence[Any]] | None: ...
    @property
    def rowcount(self) -> int: ...
    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] = ...,
        /,
    ) -> object: ...
    def fetchone(self) -> Sequence[Any] | None: ...
    def fetchall(self) -> Sequence[Sequence[Any]]: ...


class DBAPIConnection(Protocol):
    def cursor(self) -> DBAPICursor: ...
