from __future__ import annotations

import sys

__all__ = ["Template", "render"]

if sys.version_info >= (3, 14):
    from string.templatelib import Template

    from ._render import render
else:  # pragma: no cover

    class Template:
        pass

    def render(template: Template):  # noqa: ARG001
        msg = "t-strings require Python 3.14+"
        raise RuntimeError(msg)
