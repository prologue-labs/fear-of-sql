from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

import pg8000.native  # noqa: TC002

from ._describe import NullabilityOverride, UnresolvedColumn


class JoinType(str, Enum):
    LEFT = "Left"
    RIGHT = "Right"
    FULL = "Full"

    @classmethod
    def from_raw(cls, value: str | None) -> JoinType | None:
        try:
            return cls(value) if value else None
        except ValueError:
            return None


class ParentRelation(str, Enum):
    INNER = "Inner"
    OUTER = "Outer"

    @classmethod
    def from_raw(
        cls,
        value: str | None,
    ) -> ParentRelation | None:
        try:
            return cls(value) if value else None
        except ValueError:
            return None


@dataclass(frozen=True)
class PlanNode:
    join_type: JoinType | None
    parent_relation: ParentRelation | None
    output: list[str]
    children: list[PlanNode]


def _parse_plan(raw: dict[str, Any]) -> PlanNode:
    return PlanNode(
        join_type=JoinType.from_raw(raw.get("Join Type")),
        parent_relation=ParentRelation.from_raw(
            raw.get("Parent Relationship"),
        ),
        output=raw.get("Output", []),
        children=[_parse_plan(p) for p in raw.get("Plans", [])],
    )


def _visit_plan(
    plan: PlanNode,
    root_outputs: list[str],
    nullables: list[bool],
) -> None:
    if plan.join_type == JoinType.FULL or plan.parent_relation == ParentRelation.INNER:
        for col in plan.output:
            if col in root_outputs:
                nullables[root_outputs.index(col)] = True

    if plan.join_type in (JoinType.LEFT, JoinType.RIGHT):
        for child in plan.children:
            _visit_plan(child, root_outputs, nullables)


def collect_explain_nullability(
    conn: pg8000.native.Connection,
    prepared_statement: pg8000.native.PreparedStatement,
    cols: list[UnresolvedColumn],
) -> list[NullabilityOverride]:
    stmt = prepared_statement.name_bin.rstrip(
        b"\x00",
    ).decode("ascii")

    param_result = conn.run(
        "SELECT coalesce(array_length(parameter_types, 1), 0) "
        "FROM pg_prepared_statements WHERE name = :name",
        name=stmt,
    )
    if param_result is None:  # pragma: no cover
        msg = f"pg_prepared_statements row not found for statement '{stmt}'"
        raise RuntimeError(msg)

    [[param_count]] = param_result
    nulls = ", ".join(["NULL"] * param_count)
    params_clause = f"({nulls})" if nulls else ""

    result = conn.run(f"EXPLAIN (VERBOSE, FORMAT JSON) EXECUTE {stmt}{params_clause}")

    if result is None:  # pragma: no cover
        msg = "EXPLAIN returned no rows"
        raise RuntimeError(msg)

    [[plan_wrapper]] = result
    plan_dict = plan_wrapper[0]["Plan"]
    plan = _parse_plan(plan_dict)
    nullables = [False] * len(plan.output)
    _visit_plan(plan, plan.output, nullables)

    return [
        NullabilityOverride(
            name=cols[i].name,
            is_nullable=True,
        )
        for i, is_nullable in enumerate(nullables)
        if is_nullable
    ]
