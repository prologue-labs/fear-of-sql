from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import asyncpg

from ._compat import Template, render
from ._executor import AsyncExecutor, AsyncpgExecutor

try:
    import psycopg

    from ._psycopg_executor import PsycopgExecutor

    _PSYCOPG_TYPES: tuple[type, ...] = (psycopg.AsyncConnection,)
except ImportError:  # pragma: no cover
    PsycopgExecutor = None  # type: ignore[assignment,misc]
    _PSYCOPG_TYPES = ()

try:
    import sqlalchemy.ext.asyncio

    _SA_ASYNC_SESSION_TYPES: tuple[type, ...] = (
        sqlalchemy.ext.asyncio.AsyncSession,
    )
    _SA_ASYNC_CONN_TYPES: tuple[type, ...] = (
        sqlalchemy.ext.asyncio.AsyncConnection,
    )
except ImportError:  # pragma: no cover
    _SA_ASYNC_SESSION_TYPES = ()
    _SA_ASYNC_CONN_TYPES = ()


if TYPE_CHECKING:
    from ._dbapi import DBAPIConnection, DBAPICursor


T = TypeVar("T")


def _execute_sync(
    conn: DBAPIConnection,
    sql: str,
    args: tuple[object, ...],
) -> DBAPICursor:
    cursor = conn.cursor()
    cursor.execute(sql, args) if args else cursor.execute(sql)
    return cursor


async def _async_executor(
    executor: asyncpg.Pool
    | asyncpg.Connection
    | psycopg.AsyncConnection
    | sqlalchemy.ext.asyncio.AsyncSession
    | sqlalchemy.ext.asyncio.AsyncConnection,
) -> AsyncExecutor:
    if isinstance(executor, _SA_ASYNC_CONN_TYPES):
        pool_proxied = await executor.get_raw_connection()  # type: ignore[attr-defined]
        executor = pool_proxied.driver_connection
    elif isinstance(executor, _SA_ASYNC_SESSION_TYPES):
        sa_connection = await executor.connection()  # type: ignore[attr-defined]
        pool_proxied = await sa_connection.get_raw_connection()
        executor = pool_proxied.driver_connection

    if isinstance(executor, (asyncpg.Connection, asyncpg.Pool)):
        return AsyncpgExecutor(executor)
    if PsycopgExecutor is not None and isinstance(executor, _PSYCOPG_TYPES):
        return PsycopgExecutor(executor)  # type: ignore[arg-type]

    msg = f"unsupported executor type: {type(executor).__name__}"
    raise TypeError(msg)


def _col_names(cursor: DBAPICursor) -> list[str]:
    if cursor.description is None:  # pragma: no cover
        msg = "query returned no description"
        raise RuntimeError(msg)
    return [desc[0] for desc in cursor.description]


def _construct_result(result_type: type[T], row: Mapping[str, Any]) -> T:
    if dataclasses.is_dataclass(result_type) or hasattr(
        result_type, "model_fields"
    ):
        return result_type(**row)
    return next(iter(row.values()))  # type: ignore[no-any-return]


def _construct_dbapi_result(
    result_type: type[T], cols: list[str], row: Sequence[Any]
) -> T:
    if dataclasses.is_dataclass(result_type) or hasattr(
        result_type, "model_fields"
    ):
        return result_type(**dict(zip(cols, row, strict=True)))
    return row[0]  # type: ignore[no-any-return]


class BaseQuery:
    sql: str
    args: tuple[object, ...]


class Query(BaseQuery, Generic[T]):
    result_type: type[T]

    def __init__(
        self,
        sql: Template | str,
        result_type: type[T],
        *args: object,
    ) -> None:
        if isinstance(sql, Template):
            rendered = render(sql)
            self.sql = rendered.sql
            self.args = rendered.params
        else:
            self.sql = sql
            self.args = args
        self.result_type = result_type

    async def fetch_one(
        self,
        executor: asyncpg.Pool | asyncpg.Connection | psycopg.AsyncConnection,
    ) -> T:
        async_executor = await _async_executor(executor)
        row = await async_executor.fetch_one(self.sql, self.args)
        if row is None:
            msg = "fetch_one: query returned no rows"
            raise RuntimeError(msg)
        return _construct_result(self.result_type, row)

    async def fetch_optional(
        self,
        executor: asyncpg.Pool | asyncpg.Connection | psycopg.AsyncConnection,
    ) -> T | None:
        async_executor = await _async_executor(executor)
        row = await async_executor.fetch_one(self.sql, self.args)
        if row is None:
            return None
        return _construct_result(self.result_type, row)

    async def fetch_all(
        self,
        executor: asyncpg.Pool | asyncpg.Connection | psycopg.AsyncConnection,
    ) -> list[T]:
        async_executor = await _async_executor(executor)
        rows = await async_executor.fetch_all(self.sql, self.args)
        return [_construct_result(self.result_type, row) for row in rows]

    def fetch_one_sync(self, conn: DBAPIConnection) -> T:
        cursor = _execute_sync(conn, str(self.sql), self.args)
        row = cursor.fetchone()
        if row is None:
            msg = "fetch_one: query returned no rows"
            raise RuntimeError(msg)
        return _construct_dbapi_result(
            self.result_type, _col_names(cursor), row
        )

    def fetch_optional_sync(self, conn: DBAPIConnection) -> T | None:
        cursor = _execute_sync(conn, str(self.sql), self.args)
        row = cursor.fetchone()
        if row is None:
            return None
        return _construct_dbapi_result(
            self.result_type, _col_names(cursor), row
        )

    def fetch_all_sync(self, conn: DBAPIConnection) -> list[T]:
        cursor = _execute_sync(conn, str(self.sql), self.args)
        cols = _col_names(cursor)
        return [
            _construct_dbapi_result(self.result_type, cols, row)
            for row in cursor.fetchall()
        ]


class Execute(BaseQuery):
    def __init__(
        self,
        sql: Template | str,
        *args: object,
    ) -> None:
        if isinstance(sql, Template):
            rendered = render(sql)
            self.sql = rendered.sql
            self.args = rendered.params
        else:
            self.sql = sql
            self.args = args

    async def execute(
        self,
        executor: asyncpg.Pool | asyncpg.Connection | psycopg.AsyncConnection,
    ) -> None:
        async_executor = await _async_executor(executor)
        await async_executor.execute(self.sql, self.args)

    async def execute_rows(
        self,
        executor: asyncpg.Pool | asyncpg.Connection | psycopg.AsyncConnection,
    ) -> int:
        async_executor = await _async_executor(executor)
        return await async_executor.execute(self.sql, self.args)

    def execute_sync(self, conn: DBAPIConnection) -> None:
        _execute_sync(conn, str(self.sql), self.args)

    def execute_rows_sync(self, conn: DBAPIConnection) -> int:
        cursor = _execute_sync(conn, str(self.sql), self.args)
        return int(cursor.rowcount)
