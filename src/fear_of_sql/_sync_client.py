from typing import TypeVar

from ._dbapi import DBAPIConnection
from ._query import Execute, Query

T = TypeVar("T")


class SyncClient:
    def __init__(self, conn: DBAPIConnection) -> None:
        self._conn = conn

    def fetch_one(self, sql: str, result_type: type[T], *args: object) -> T:
        return Query(sql, result_type, *args).fetch_one_sync(self._conn)

    def fetch_optional(self, sql: str, result_type: type[T], *args: object) -> T | None:
        return Query(sql, result_type, *args).fetch_optional_sync(self._conn)

    def fetch_all(self, sql: str, result_type: type[T], *args: object) -> list[T]:
        return Query(sql, result_type, *args).fetch_all_sync(self._conn)

    def execute(self, sql: str, *args: object) -> None:
        Execute(sql, *args).execute_sync(self._conn)

    def execute_rows(self, sql: str, *args: object) -> int:
        return Execute(sql, *args).execute_rows_sync(self._conn)
