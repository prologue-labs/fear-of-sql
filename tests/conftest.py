import pathlib
import sys

import asyncpg
import pg8000.dbapi
import psycopg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
)

import fear_of_sql as fos

if sys.version_info < (3, 14):
    collect_ignore = ["test_tstrings.py"]

DB_URL = "postgresql://user:password@localhost:5433/fear_of_sql_test"
SA_ASYNCPG_URL = DB_URL.replace("postgresql://", "postgresql+asyncpg://")
SA_PSYCOPG_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://")
SETUP_SQL = pathlib.Path(__file__).parent / "setup.sql"


@pytest.fixture(scope="session", autouse=True)
def _seed_db():
    conn = pg8000.dbapi.connect(
        user="user",
        host="localhost",
        port=5433,
        database="fear_of_sql_test",
        password="password",
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("DROP SCHEMA public CASCADE")
    cursor.execute("CREATE SCHEMA public")
    cursor.execute(SETUP_SQL.read_text())
    conn.close()


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


@pytest_asyncio.fixture
async def sa_async_session():
    engine = create_async_engine(SA_ASYNCPG_URL)
    async with AsyncSession(engine) as session:
        yield session
    await engine.dispose()


@pytest_asyncio.fixture
async def sa_async_conn():
    engine = create_async_engine(SA_ASYNCPG_URL)
    async with engine.connect() as conn:
        yield conn
    await engine.dispose()


@pytest_asyncio.fixture
async def sa_psycopg_session():
    engine = create_async_engine(SA_PSYCOPG_URL)
    async with AsyncSession(engine) as session:
        yield session
    await engine.dispose()


@pytest_asyncio.fixture
async def sa_psycopg_conn():
    engine = create_async_engine(SA_PSYCOPG_URL)
    async with engine.connect() as conn:
        yield conn
    await engine.dispose()


