from __future__ import annotations

from string.templatelib import Template
from typing import NamedTuple


class RenderedQuery(NamedTuple):
    sql: str
    params: tuple[object, ...]


def render(template: Template) -> RenderedQuery:
    parts: list[str] = []
    params: list[object] = []
    for i, s in enumerate(template.strings):
        parts.append(s)
        if i < len(template.interpolations):
            parts.append(f"${i + 1}")
            params.append(template.interpolations[i].value)
    return RenderedQuery("".join(parts), tuple(params))
