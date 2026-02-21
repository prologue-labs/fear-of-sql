from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from ._query import Execute, Query

if TYPE_CHECKING:
    import asyncpg
    import psycopg

T = TypeVar("T")


class AsyncClient:
    def __init__(
        self,
        executor: asyncpg.Pool | asyncpg.Connection | psycopg.AsyncConnection,
    ) -> None:
        self._executor = executor

    async def fetch_one(self, sql: str, result_type: type[T], *args: object) -> T:
        return await Query(sql, result_type, *args).fetch_one(self._executor)

    async def fetch_optional(
        self, sql: str, result_type: type[T], *args: object
    ) -> T | None:
        return await Query(sql, result_type, *args).fetch_optional(self._executor)

    async def fetch_all(self, sql: str, result_type: type[T], *args: object) -> list[T]:
        return await Query(sql, result_type, *args).fetch_all(self._executor)

    async def execute(self, sql: str, *args: object) -> None:
        await Execute(sql, *args).execute(self._executor)

    async def execute_rows(self, sql: str, *args: object) -> int:
        return await Execute(sql, *args).execute_rows(self._executor)
