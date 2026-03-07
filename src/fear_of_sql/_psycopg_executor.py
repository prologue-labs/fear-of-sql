from collections.abc import Mapping, Sequence
from typing import Any

import psycopg
from psycopg.rows import dict_row


class PsycopgExecutor:
    def __init__(self, executor: psycopg.AsyncConnection[dict[str, Any]]) -> None:
        self._executor = executor

    async def fetch_one(
        self, sql: str, args: tuple[object, ...]
    ) -> Mapping[str, Any] | None:
        cursor = self._executor.cursor(row_factory=dict_row)
        await cursor.execute(sql, args or None)  # pyright: ignore[reportArgumentType]
        return await cursor.fetchone()

    async def fetch_all(
        self, sql: str, args: tuple[object, ...]
    ) -> Sequence[Mapping[str, Any]]:
        cursor = self._executor.cursor(row_factory=dict_row)
        await cursor.execute(sql, args or None)  # pyright: ignore[reportArgumentType]
        return await cursor.fetchall()

    async def execute(self, sql: str, args: tuple[object, ...]) -> int:
        cursor = await self._executor.execute(sql, args or None)  # pyright: ignore[reportArgumentType]
        return cursor.rowcount
