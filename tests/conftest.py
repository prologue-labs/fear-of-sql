import sys

import asyncpg
import pg8000.dbapi
import psycopg
import pytest
import pytest_asyncio

import fear_of_sql as fos

if sys.version_info < (3, 14):
    collect_ignore = ["test_tstrings.py"]

DB_URL = "postgresql://user:password@localhost:5433/fear_of_sql_test"


@pytest.fixture
def conn():
    with fos.connect(DB_URL) as conn:
        yield conn


@pytest.fixture
def dbapi_conn():
    conn = pg8000.dbapi.connect(
        user="user",
        host="localhost",
        port=5433,
        database="fear_of_sql_test",
        password="password",
    )
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


@pytest_asyncio.fixture
async def asyncpg_pool():
    pool = await asyncpg.create_pool(DB_URL)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def psycopg_conn():
    conn = await psycopg.AsyncConnection.connect(DB_URL)
    yield conn
    await conn.close()
