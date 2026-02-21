from collections.abc import Mapping, Sequence
from typing import Any, Protocol

import asyncpg


class AsyncExecutor(Protocol):
    async def fetch_one(
        self, sql: str, args: tuple[object, ...]
    ) -> Mapping[str, Any] | None: ...

    async def fetch_all(
        self, sql: str, args: tuple[object, ...]
    ) -> Sequence[Mapping[str, Any]]: ...

    async def execute(self, sql: str, args: tuple[object, ...]) -> int: ...


class AsyncpgExecutor:
    def __init__(self, executor: asyncpg.Pool | asyncpg.Connection) -> None:
        self._executor = executor

    async def fetch_one(
        self, sql: str, args: tuple[object, ...]
    ) -> Mapping[str, Any] | None:
        return await self._executor.fetchrow(sql, *args)  # type: ignore[no-any-return]

    async def fetch_all(
        self, sql: str, args: tuple[object, ...]
    ) -> Sequence[Mapping[str, Any]]:
        return await self._executor.fetch(sql, *args)  # type: ignore[no-any-return]

    async def execute(self, sql: str, args: tuple[object, ...]) -> int:
        result = await self._executor.execute(sql, *args)
        return int(result.split()[-1])
