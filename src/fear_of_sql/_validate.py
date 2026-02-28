from __future__ import annotations

import datetime
import inspect
import logging
import types
import uuid
from collections.abc import Callable
from decimal import Decimal
from typing import NamedTuple, ParamSpec, TypeVar

import pg8000.dbapi
import pg8000.native

from ._compat import Template, render
from ._connect import _connect_from_url
from ._describe import describe
from ._errors import (
    ColumnNotFoundError,
    ValidationError,
)
from ._explain import collect_explain_nullability
from ._query import BaseQuery, Query
from ._resolve import (
    ExpectedScalar,
    check_column,
    check_scalar,
    collect_catalog_nullability,
    extract_expected,
    find_column,
    resolve,
)

logger = logging.getLogger("fear_of_sql")

T = TypeVar("T", bound=BaseQuery)
P = ParamSpec("P")


class DummyArg(NamedTuple):
    param_name: str
    value: object


_DUMMY_VALUES: dict[type, object] = {
    str: "",
    int: 0,
    float: 0.0,
    bool: False,
    bytes: b"",
    datetime.date: datetime.date(2000, 1, 1),
    datetime.time: datetime.time(),
    datetime.datetime: datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc),
    datetime.timedelta: datetime.timedelta(),
    Decimal: Decimal(0),
    uuid.UUID: uuid.UUID(int=0),
    dict: {},
    list: [],
}


def _make_dummy_args(fn: Callable[..., BaseQuery]) -> list[DummyArg]:
    sig = inspect.signature(fn)
    result: list[DummyArg] = []
    for param_name, param in sig.parameters.items():
        annotation = param.annotation
        if annotation is inspect.Parameter.empty:
            msg = f"{fn.__name__}: parameter {param_name!r} has no type annotation"
            raise TypeError(msg)
        dummy_value = _DUMMY_VALUES.get(annotation)
        if dummy_value is None and hasattr(annotation, "__origin__"):
            dummy_value = _DUMMY_VALUES.get(annotation.__origin__)
        if dummy_value is None:
            msg = f"{fn.__name__}: no dummy value for type {annotation!r} on parameter {param_name!r}"
            raise TypeError(msg)
        result.append(DummyArg(param_name, dummy_value))
    return result


class FearOfSQL:
    def __init__(self) -> None:
        self._queries: list[Callable[..., BaseQuery]] = []

    def query(self, fn: Callable[P, T]) -> Callable[P, T]:
        self._queries.append(fn)
        return fn

    def validate_all(
        self, conn: pg8000.native.Connection | str,
    ) -> int:
        if isinstance(conn, str):
            native_conn = _connect_from_url(conn)
            try:
                return self._validate(native_conn)
            finally:
                native_conn.close()
        return self._validate(conn)

    def _validate(self, conn: pg8000.native.Connection) -> int:
        count = 0
        for fn in self._queries:
            kwargs = {arg.param_name: arg.value for arg in _make_dummy_args(fn)}
            query_obj = fn(**kwargs)
            sql_oneline = " ".join(str(query_obj.sql).split())
            result_type = (
                query_obj.result_type if isinstance(query_obj, Query) else None
            )
            for error in collect_errors(conn, query_obj.sql, result_type):
                logger.warning(
                    "ERR: %s — %s — %s",
                    fn.__name__,
                    error,
                    sql_oneline,
                )
                error.query_name = fn.__name__
                error.sql = str(query_obj.sql)
                raise error
            logger.info(
                "ok: %s — %s",
                fn.__name__,
                sql_oneline,
            )
            count += 1
        return count


def collect_errors(
    conn: pg8000.native.Connection,
    sql: Template | str,
    result_type: type | types.UnionType | None = None,
) -> list[ValidationError]:
    sql_str = render(sql).sql if isinstance(sql, Template) else sql
    converted_sql, _ = pg8000.dbapi.convert_paramstyle("format", sql_str, ())
    prepared_stmt, unresolved, origins, query_overrides = describe(conn, converted_sql)
    if result_type is None:
        prepared_stmt.close()
        return []
    try:
        catalog_nullability = collect_catalog_nullability(
            conn,
            origins,
        )
        explain_nullability = collect_explain_nullability(
            conn,
            prepared_stmt,
            unresolved,
        )
    finally:
        prepared_stmt.close()
    resolved = resolve(
        unresolved,
        catalog_nullability,
        explain_nullability,
        query_overrides,
    )
    expected = extract_expected(result_type)

    if isinstance(expected, ExpectedScalar):
        return check_scalar(resolved, expected)

    errors: list[ValidationError] = []
    for exp in expected:
        match find_column(resolved, exp.name):
            case ColumnNotFoundError() as err:
                errors.append(err)
            case col:
                errors.extend(check_column(col, exp.allowed_types))
    return errors
