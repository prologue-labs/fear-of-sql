from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import urlparse

import pg8000.native


def _connect_from_url(url: str) -> pg8000.native.Connection:
    parsed = urlparse(url)
    return pg8000.native.Connection(
        parsed.username or "",
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        database=(parsed.path or "/").lstrip("/"),
        password=parsed.password,
    )


@contextmanager
def connect(url: str) -> Iterator[pg8000.native.Connection]:
    conn = _connect_from_url(url)
    try:
        yield conn
    finally:
        conn.close()
