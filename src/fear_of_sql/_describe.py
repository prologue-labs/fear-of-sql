from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pg8000.native  # noqa: TC002

from ._errors import UnsupportedTypeError
from ._types import PG_TYPES


@dataclass(frozen=True)
class UnresolvedColumn:
    name: str
    python_type: type

    @classmethod
    def from_pg(
        cls,
        name: str,
        data: dict[str, Any],
    ) -> UnresolvedColumn:
        pg = PG_TYPES.get(data["type_oid"])
        if pg is None:
            raise UnsupportedTypeError(
                type_oid=data["type_oid"],
                column=name,
            )
        return cls(name=name, python_type=pg.python_type)


@dataclass(frozen=True)
class ColumnOrigin:
    name: str
    table_oid: int
    column_attrnum: int

    @classmethod
    def from_pg(
        cls,
        name: str,
        data: dict[str, Any],
    ) -> ColumnOrigin:
        return cls(
            name=name,
            table_oid=data["table_oid"],
            column_attrnum=data["column_attrnum"],
        )


@dataclass(frozen=True)
class NullabilityOverride:
    name: str
    is_nullable: bool


def _parse_column_name_nullability_override(
    column_info: dict[str, Any],
) -> tuple[str, bool | None]:
    raw_name = column_info["name"]
    if raw_name.endswith("!"):
        return raw_name[:-1], False
    if raw_name.endswith("?"):
        return raw_name[:-1], True
    return raw_name, None


def describe(
    conn: pg8000.native.Connection,
    sql: str,
) -> tuple[
    pg8000.native.PreparedStatement,
    list[UnresolvedColumn],
    list[ColumnOrigin],
    list[NullabilityOverride],
]:
    prepared_statement = conn.prepare(sql)
    cols = prepared_statement.cols
    if cols is None:
        return prepared_statement, [], [], []

    unresolved = []
    origins = []
    overrides = []
    for col in cols:
        name, nullable_override = _parse_column_name_nullability_override(col)
        unresolved.append(UnresolvedColumn.from_pg(name, col))
        origins.append(ColumnOrigin.from_pg(name, col))
        if nullable_override is not None:
            overrides.append(
                NullabilityOverride(
                    name=name,
                    is_nullable=nullable_override,
                )
            )
    return prepared_statement, unresolved, origins, overrides
