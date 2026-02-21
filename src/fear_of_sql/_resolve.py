import types
from dataclasses import dataclass, fields, is_dataclass
from typing import Any

import pg8000.native

from ._describe import (
    ColumnOrigin,
    NullabilityOverride,
    UnresolvedColumn,
)
from ._errors import (
    ColumnCountMismatchError,
    ColumnNotFoundError,
    NullabilityError,
    TypeMismatchError,
    ValidationError,
)


@dataclass(frozen=True)
class ResolvedColumn:
    name: str
    mapped_type: type
    nullable: bool


@dataclass(frozen=True)
class Nullable:
    name: str
    nullable: bool


@dataclass(frozen=True)
class ExpectedColumn:
    name: str
    allowed_types: list[type]


@dataclass(frozen=True)
class ExpectedScalar:
    allowed_types: list[type]


def collect_catalog_nullability(
    conn: pg8000.native.Connection,
    origins: list[ColumnOrigin],
) -> list[Nullable]:
    nullability_info = []

    for origin in origins:
        if origin.table_oid == 0:
            nullability_info.append(
                Nullable(name=origin.name, nullable=True),
            )
            continue

        result = conn.run(
            "SELECT attnotnull FROM pg_catalog.pg_attribute "
            "WHERE attrelid = :oid AND attnum = :num",
            oid=origin.table_oid,
            num=origin.column_attrnum,
        )
        if not result:  # pragma: no cover
            msg = (
                f"pg_attribute row not found for"
                f" OID {origin.table_oid},"
                f" attnum {origin.column_attrnum}"
            )
            raise RuntimeError(msg)
        nullability_info.append(
            Nullable(
                name=origin.name,
                nullable=not result[0][0],
            )
        )
    return nullability_info


def extract_expected(
    result_type: Any,
) -> ExpectedScalar | list[ExpectedColumn]:
    def _unwrap_types(t: Any) -> list[type]:
        if isinstance(t, types.UnionType):
            return list(t.__args__)
        return [t]

    if is_dataclass(result_type):
        return [
            ExpectedColumn(
                name=field.name,
                allowed_types=_unwrap_types(field.type),
            )
            for field in fields(result_type)
        ]
    if hasattr(result_type, "model_fields"):
        return [
            ExpectedColumn(
                name=name,
                allowed_types=_unwrap_types(field.annotation),
            )
            for name, field in result_type.model_fields.items()
        ]
    return ExpectedScalar(
        allowed_types=_unwrap_types(result_type),
    )


def check_column(
    col: ResolvedColumn,
    allowed_types: list[type],
) -> list[ValidationError]:
    errors: list[ValidationError] = []
    if col.mapped_type not in allowed_types:
        errors.append(
            TypeMismatchError(
                column=col.name,
                expected=allowed_types,
                actual=col.mapped_type,
            )
        )
    if col.nullable and type(None) not in allowed_types:
        errors.append(NullabilityError(column=col.name))
    return errors


def resolve(
    cols: list[UnresolvedColumn],
    catalog_nullability: list[Nullable],
    explain_overrides: list[NullabilityOverride],
    query_overrides: list[NullabilityOverride],
) -> list[ResolvedColumn]:
    null_map = {n.name: n.nullable for n in catalog_nullability}
    for override in explain_overrides:
        null_map[override.name] = override.is_nullable
    for override in query_overrides:
        null_map[override.name] = override.is_nullable

    return [
        ResolvedColumn(
            name=col.name,
            mapped_type=col.python_type,
            nullable=null_map[col.name],
        )
        for col in cols
    ]


def check_scalar(
    resolved: list[ResolvedColumn],
    expected: ExpectedScalar,
) -> list[ValidationError]:
    if len(resolved) != 1:
        return [
            ColumnCountMismatchError(
                expected=1,
                actual=len(resolved),
            )
        ]
    return check_column(resolved[0], expected.allowed_types)


def find_column(
    resolved: list[ResolvedColumn],
    name: str,
) -> ResolvedColumn | ColumnNotFoundError:
    for col in resolved:
        if col.name == name:
            return col
    return ColumnNotFoundError(column=name)
