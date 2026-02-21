import datetime
import uuid
from dataclasses import dataclass
from decimal import Decimal

import pg8000
import pytest
from conftest import DB_URL
from pydantic import BaseModel
from pydantic.dataclasses import dataclass as pydantic_dataclass

from fear_of_sql import (
    AsyncClient,
    ColumnCountMismatchError,
    ColumnNotFoundError,
    Execute,
    FearOfSQL,
    NullabilityError,
    Query,
    SyncClient,
    TypeMismatchError,
    UnsupportedTypeError,
    ValidationError,
    collect_errors,
)


@dataclass
class Card:
    id: int
    front: str
    back: str


def test_scalar_query(conn):
    errors = collect_errors(conn, "SELECT front FROM cards", str)
    assert not errors


def test_parameterized_query(conn):
    errors = collect_errors(conn, "SELECT front FROM cards WHERE id = $1", str)
    assert not errors


def test_no_table(conn):
    with pytest.raises(pg8000.DatabaseError):
        collect_errors(
            conn,
            "SELECT count(*) as count FROM cardz",
            int,
        )


def test_type_mismatch(conn):
    errors = collect_errors(
        conn,
        "SELECT count(*) as count FROM cards",
        str,
    )
    assert any(isinstance(e, TypeMismatchError) for e in errors)


def test_scalar_multi_column(conn):
    errors = collect_errors(
        conn,
        "SELECT front, back FROM cards",
        str,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], ColumnCountMismatchError)
    assert errors[0].expected == 1
    assert errors[0].actual == 2


def test_nullable_scalar(conn):
    errors = collect_errors(
        conn,
        "SELECT notes FROM cards",
        str | None,
    )
    assert not errors


def test_scalar_union_multi_column(conn):
    errors = collect_errors(
        conn,
        "SELECT front, back FROM cards",
        str | None,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], ColumnCountMismatchError)


def test_dataclass_query(conn):
    @dataclass
    class Card:
        front: str

    errors = collect_errors(
        conn,
        "SELECT front FROM cards",
        Card,
    )
    assert not errors


def test_dataclass_multi_query(conn):
    @dataclass
    class Card:
        front: str
        back: str

    errors = collect_errors(
        conn,
        "SELECT front, back FROM cards",
        Card,
    )
    assert not errors


def test_dataclass_field_order(conn):
    @dataclass
    class Card:
        back: str
        front: str

    errors = collect_errors(
        conn,
        "SELECT front, back FROM cards",
        Card,
    )
    assert not errors


def test_dataclass_mismatch_query(conn):
    @dataclass
    class Card:
        front: str
        back: str

    errors = collect_errors(
        conn,
        "SELECT front FROM cards",
        Card,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], ColumnNotFoundError)
    assert errors[0].column == "back"


def test_parameterized_multi_column(conn):
    @dataclass
    class Card:
        id: int
        front: str
        back: str

    errors = collect_errors(
        conn,
        "SELECT id, front, back FROM cards WHERE front = $1",
        Card,
    )
    assert not errors


def test_parameterized_multiple_params(conn):
    @dataclass
    class Card:
        id: int

    errors = collect_errors(
        conn,
        "SELECT id FROM cards WHERE front = $1 AND back = $2",
        Card,
    )
    assert not errors


def test_extra_sql_columns_ignored(conn):
    @dataclass
    class Card:
        front: str

    errors = collect_errors(
        conn,
        "SELECT front, back FROM cards",
        Card,
    )
    assert not errors


def test_basemodel(conn):
    class CardModel(BaseModel):
        front: str
        back: str

    errors = collect_errors(conn, "SELECT front, back FROM cards", CardModel)
    assert not errors


def test_pydantic_dataclass(conn):
    @pydantic_dataclass
    class PydanticCard:
        front: str
        back: str

    errors = collect_errors(conn, "SELECT front, back FROM cards", PydanticCard)
    assert not errors


def test_nullable_column(conn):
    @dataclass
    class Card:
        notes: str | None

    errors = collect_errors(
        conn,
        "SELECT notes FROM cards",
        Card,
    )
    assert not errors


def test_nullable_err(conn):
    @dataclass
    class Card:
        notes: str

    errors = collect_errors(
        conn,
        "SELECT notes FROM cards",
        Card,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], NullabilityError)
    assert errors[0].column == "notes"


def test_expression_assumed_nullable(conn):
    errors = collect_errors(
        conn,
        "SELECT count(*) as count FROM cards",
        int | None,
    )
    assert not errors


def test_expression_strict(conn):
    """Expressions assumed nullable — int without None is flagged."""
    errors = collect_errors(
        conn,
        "SELECT count(*) as count FROM cards",
        int,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], NullabilityError)


def test_left_join_nullable_correct_types(conn):
    @dataclass
    class CardReview:
        front: str
        score: int | None

    errors = collect_errors(
        conn,
        "SELECT cards.front, reviews.score FROM cards LEFT JOIN reviews ON false",
        CardReview,
    )
    assert not errors


def test_right_join_nullable(conn):
    @dataclass
    class ReviewCard:
        front: str
        score: int

    errors = collect_errors(
        conn,
        "SELECT cards.front, reviews.score FROM cards RIGHT JOIN reviews ON false",
        ReviewCard,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], NullabilityError)
    assert errors[0].column == "front"


def test_full_join_nullable(conn):
    @dataclass
    class CardReview:
        front: str
        score: int

    errors = collect_errors(
        conn,
        "SELECT cards.front, reviews.score FROM cards FULL JOIN reviews ON false",
        CardReview,
    )
    assert len(errors) == 2
    assert all(isinstance(e, NullabilityError) for e in errors)


def test_inner_join_preserves_nullability(conn):
    @dataclass
    class CardReview:
        front: str
        score: int

    errors = collect_errors(
        conn,
        "SELECT cards.front, reviews.score FROM cards INNER JOIN reviews ON cards.id = reviews.card_id",
        CardReview,
    )
    assert not errors


def test_left_join_subquery_nullable(conn):
    @dataclass
    class Result:
        val: int
        score: int

    errors = collect_errors(
        conn,
        'SELECT sq.val as "val!", reviews.score FROM (SELECT 1 AS val) sq LEFT JOIN reviews ON sq.val = reviews.card_id',
        Result,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], NullabilityError)
    assert errors[0].column == "score"


def test_correlated_subquery(conn):
    @dataclass
    class CardWithMax:
        front: str
        max_score: int | None

    errors = collect_errors(
        conn,
        (
            "SELECT front,"
            " (SELECT max(score) FROM reviews WHERE reviews.card_id = cards.id)"
            ' AS "max_score"'
            " FROM cards"
        ),
        CardWithMax,
    )
    assert not errors


def test_expression_known_not_null(conn):
    errors = collect_errors(
        conn,
        'SELECT count(*) as "count!" FROM cards',
        int,
    )
    assert not errors


def test_non_null_override_dataclass(conn):
    @dataclass
    class CountResult:
        count: int

    errors = collect_errors(
        conn,
        'SELECT count(*) as "count!" FROM cards',
        CountResult,
    )
    assert not errors


def test_non_null_override_redundant(conn):
    errors = collect_errors(
        conn,
        'SELECT front as "front!" FROM cards',
        str,
    )
    assert not errors


def test_non_null_override_redundant_dataclass(conn):
    @dataclass
    class Card:
        front: str

    errors = collect_errors(
        conn,
        'SELECT front as "front!" FROM cards',
        Card,
    )
    assert not errors


def test_non_null_override_on_nullable_column(conn):
    errors = collect_errors(
        conn,
        'SELECT notes as "notes!" FROM cards',
        str,
    )
    assert not errors


def test_nullable_override_forces_error(conn):
    """? on NOT NULL column forces nullable — str without None is rejected."""
    errors = collect_errors(
        conn,
        'SELECT front as "front?" FROM cards',
        str,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], NullabilityError)


def test_nullable_override_with_union(conn):
    """? on NOT NULL column — str | None accepted regardless."""
    errors = collect_errors(
        conn,
        'SELECT front as "front?" FROM cards',
        str | None,
    )
    assert not errors


def test_nullable_override_dataclass(conn):
    @dataclass
    class Card:
        front: str | None

    errors = collect_errors(
        conn,
        'SELECT front as "front?" FROM cards',
        Card,
    )
    assert not errors


def test_nullable_override_redundant_on_nullable(conn):
    """? on already nullable column is a no-op."""
    errors = collect_errors(
        conn,
        'SELECT notes as "notes?" FROM cards',
        str | None,
    )
    assert not errors


def test_left_join_nullable_override(conn):
    """LEFT JOIN with ? — user annotates score as nullable, types int | None."""

    @dataclass
    class CardReview:
        front: str
        score: int | None

    errors = collect_errors(
        conn,
        'SELECT cards.front, reviews.score as "score?" FROM cards LEFT JOIN reviews ON false',
        CardReview,
    )
    assert not errors


def test_left_join_nullable_override_catches_error(conn):
    """LEFT JOIN with ? — user annotates score as nullable but types int. Caught."""

    @dataclass
    class CardReview:
        front: str
        score: int

    errors = collect_errors(
        conn,
        'SELECT cards.front, reviews.score as "score?" FROM cards LEFT JOIN reviews ON false',
        CardReview,
    )
    assert len(errors) == 1
    assert isinstance(errors[0], NullabilityError)
    assert errors[0].column == "score"


@pytest.mark.parametrize(
    ("sql", "expected_type"),
    [
        ('SELECT true::boolean AS "val!"', bool),
        ('SELECT 1::int2 AS "val!"', int),
        ('SELECT 42::int4 AS "val!"', int),
        ('SELECT 9999999999::int8 AS "val!"', int),
        ('SELECT 6429.686::real AS "val!"', float),
        ('SELECT 122509.6546896::double precision AS "val!"', float),
        ('SELECT 282.00::numeric AS "val!"', Decimal),
        ('''SELECT 'text_39256'::text AS "val!"''', str),
        ('''SELECT 'varchar_28289'::varchar AS "val!"''', str),
        ('''SELECT 'trxc'::char(4) AS "val!"''', str),
        ('''SELECT 'name_65302'::name AS "val!"''', str),
        ('''SELECT 'a'::"char" AS "val!"''', str),
        ('''SELECT '\\xDEADBEEF'::bytea AS "val!"''', bytes),
        ('SELECT 133326::oid AS "val!"', int),
        ('''SELECT DATE '2000-02-07' AS "val!"''', datetime.date),
        ('''SELECT TIME '07:32:38.027824' AS "val!"''', datetime.time),
        ('''SELECT '2017-04-23 20:44:34'::timestamp AS "val!"''', datetime.datetime),
        (
            '''SELECT TIMESTAMPTZ '2013-04-15 18:17:51+00' AS "val!"''',
            datetime.datetime,
        ),
        ('''SELECT INTERVAL '28 days 1 hours' AS "val!"''', datetime.timedelta),
        (
            '''SELECT 'b2b9437a-28df-6ec4-ce4a-2bbdc241330b'::uuid AS "val!"''',
            uuid.UUID,
        ),
        ('SELECT 433.53::money AS "val!"', str),
        ('''SELECT '{"key": "value"}'::json AS "val!"''', object),
        ('''SELECT '{"key": "value"}'::jsonb AS "val!"''', object),
        ('SELECT ARRAY[true]::boolean[] AS "val!"', list[bool]),
        ('SELECT ARRAY[1]::int2[] AS "val!"', list[int]),
        ('SELECT ARRAY[1]::int4[] AS "val!"', list[int]),
        ('SELECT ARRAY[1]::int8[] AS "val!"', list[int]),
        ('SELECT ARRAY[1.0]::float4[] AS "val!"', list[float]),
        ('SELECT ARRAY[1.0]::float8[] AS "val!"', list[float]),
        ('SELECT ARRAY[1.0]::numeric[] AS "val!"', list[Decimal]),
        ("SELECT ARRAY['a']::text[] AS \"val!\"", list[str]),
        ("SELECT ARRAY['a']::varchar[] AS \"val!\"", list[str]),
        ("SELECT ARRAY['a']::char(1)[] AS \"val!\"", list[str]),
        ("SELECT ARRAY['a']::name[] AS \"val!\"", list[str]),
        ('SELECT ARRAY[\'a\']::"char"[] AS "val!"', list[str]),
        ("SELECT ARRAY['\\xDEAD']::bytea[] AS \"val!\"", list[bytes]),
        ('SELECT ARRAY[1]::oid[] AS "val!"', list[int]),
        ("SELECT ARRAY[DATE '2000-02-07']::date[] AS \"val!\"", list[datetime.date]),
        ("SELECT ARRAY[TIME '07:32:38']::time[] AS \"val!\"", list[datetime.time]),
        (
            "SELECT ARRAY['2017-04-23 20:44:34'::timestamp]::timestamp[] AS \"val!\"",
            list[datetime.datetime],
        ),
        (
            "SELECT ARRAY[TIMESTAMPTZ '2013-04-15 18:17:51+00']::timestamptz[] AS \"val!\"",
            list[datetime.datetime],
        ),
        (
            "SELECT ARRAY[INTERVAL '28 days 1 hours']::interval[] AS \"val!\"",
            list[datetime.timedelta],
        ),
        (
            "SELECT ARRAY['b2b9437a-28df-6ec4-ce4a-2bbdc241330b'::uuid]::uuid[] AS \"val!\"",
            list[uuid.UUID],
        ),
        ('SELECT ARRAY[433.53]::money[] AS "val!"', list[str]),
        ("SELECT ARRAY['{}'::json]::json[] AS \"val!\"", list[object]),
        ("SELECT ARRAY['{}'::jsonb]::jsonb[] AS \"val!\"", list[object]),
    ],
)
def test_type_mapping(conn, sql, expected_type):
    assert not collect_errors(conn, sql, expected_type)


def test_unsupported_type_raises(conn):
    with pytest.raises(UnsupportedTypeError):
        collect_errors(
            conn,
            '''SELECT '127.0.0.1'::inet AS "val!"''',
            str,
        )


def test_mutation_no_result_type(conn):
    errors = collect_errors(
        conn,
        "INSERT INTO cards (front, back) VALUES ($1, $2)",
    )
    assert not errors


def test_mutation_bad_table(conn):
    with pytest.raises(pg8000.DatabaseError):
        collect_errors(
            conn,
            "INSERT INTO nonexistent (front) VALUES ($1)",
        )


def test_sync_fetch_one(dbapi_conn):
    client = SyncClient(dbapi_conn)
    card = client.fetch_one(
        "SELECT id, front, back FROM cards WHERE id = 1",
        Card,
    )
    assert card == Card(id=1, front="bonjour", back="hello")


def test_sync_fetch_one_no_rows(dbapi_conn):
    client = SyncClient(dbapi_conn)
    with pytest.raises(RuntimeError, match="no rows"):
        client.fetch_one(
            "SELECT id, front, back FROM cards WHERE id = 9999",
            Card,
        )


def test_sync_fetch_optional(dbapi_conn):
    client = SyncClient(dbapi_conn)
    card = client.fetch_optional(
        "SELECT id, front, back FROM cards WHERE id = 1",
        Card,
    )
    assert card == Card(id=1, front="bonjour", back="hello")


def test_sync_fetch_optional_none(dbapi_conn):
    client = SyncClient(dbapi_conn)
    card = client.fetch_optional(
        "SELECT id, front, back FROM cards WHERE id = 9999",
        Card,
    )
    assert card is None


def test_sync_fetch_all(dbapi_conn):
    client = SyncClient(dbapi_conn)
    cards = client.fetch_all(
        "SELECT id, front, back FROM cards ORDER BY id",
        Card,
    )
    assert len(cards) == 3
    assert cards[0] == Card(id=1, front="bonjour", back="hello")


def test_sync_fetch_all_empty(dbapi_conn):
    client = SyncClient(dbapi_conn)
    cards = client.fetch_all(
        "SELECT id, front, back FROM cards WHERE id = 9999",
        Card,
    )
    assert cards == []


def test_sync_execute(dbapi_conn):
    client = SyncClient(dbapi_conn)
    client.execute(
        "INSERT INTO cards (front, back) VALUES (%s, %s)",
        "test",
        "test_back",
    )


def test_sync_execute_rows(dbapi_conn):
    client = SyncClient(dbapi_conn)
    client.execute(
        "INSERT INTO cards (front, back) VALUES (%s, %s)",
        "to_delete",
        "bye",
    )
    count = client.execute_rows(
        "DELETE FROM cards WHERE front = %s",
        "to_delete",
    )
    assert count == 1


def test_query_fetch_one_sync(dbapi_conn):
    query = Query("SELECT id, front, back FROM cards WHERE id = %s", Card, 1)
    card = query.fetch_one_sync(dbapi_conn)
    assert card == Card(id=1, front="bonjour", back="hello")


def test_query_fetch_one_sync_no_rows(dbapi_conn):
    query = Query(
        "SELECT id, front, back FROM cards WHERE id = %s",
        Card,
        9999,
    )
    with pytest.raises(RuntimeError, match="no rows"):
        query.fetch_one_sync(dbapi_conn)


def test_query_fetch_optional_sync(dbapi_conn):
    query = Query("SELECT id, front, back FROM cards WHERE id = %s", Card, 1)
    card = query.fetch_optional_sync(dbapi_conn)
    assert card == Card(id=1, front="bonjour", back="hello")


def test_query_fetch_optional_sync_none(dbapi_conn):
    query = Query(
        "SELECT id, front, back FROM cards WHERE id = %s",
        Card,
        9999,
    )
    assert query.fetch_optional_sync(dbapi_conn) is None


def test_query_fetch_all_sync(dbapi_conn):
    query = Query("SELECT id, front, back FROM cards ORDER BY id", Card)
    cards = query.fetch_all_sync(dbapi_conn)
    assert len(cards) == 3


def test_execute_sync(dbapi_conn):
    exe = Execute(
        "INSERT INTO cards (front, back) VALUES (%s, %s)",
        "sync_test",
        "sync_back",
    )
    exe.execute_sync(dbapi_conn)


def test_execute_rows_sync(dbapi_conn):
    Execute(
        "INSERT INTO cards (front, back) VALUES (%s, %s)",
        "sync_del",
        "bye",
    ).execute_sync(dbapi_conn)
    exe = Execute("DELETE FROM cards WHERE front = %s", "sync_del")
    assert exe.execute_rows_sync(dbapi_conn) == 1


@pytest.mark.asyncio
async def test_async_fetch_one(asyncpg_pool):
    client = AsyncClient(asyncpg_pool)
    card = await client.fetch_one(
        "SELECT id, front, back FROM cards WHERE id = $1",
        Card,
        1,
    )
    assert card == Card(id=1, front="bonjour", back="hello")


@pytest.mark.asyncio
async def test_async_fetch_one_no_rows(asyncpg_pool):
    client = AsyncClient(asyncpg_pool)
    with pytest.raises(RuntimeError, match="no rows"):
        await client.fetch_one(
            "SELECT id, front, back FROM cards WHERE id = $1",
            Card,
            9999,
        )


@pytest.mark.asyncio
async def test_async_fetch_optional(asyncpg_pool):
    client = AsyncClient(asyncpg_pool)
    card = await client.fetch_optional(
        "SELECT id, front, back FROM cards WHERE id = $1",
        Card,
        1,
    )
    assert card == Card(id=1, front="bonjour", back="hello")


@pytest.mark.asyncio
async def test_async_fetch_optional_none(asyncpg_pool):
    client = AsyncClient(asyncpg_pool)
    card = await client.fetch_optional(
        "SELECT id, front, back FROM cards WHERE id = $1",
        Card,
        9999,
    )
    assert card is None


@pytest.mark.asyncio
async def test_async_fetch_all(asyncpg_pool):
    client = AsyncClient(asyncpg_pool)
    cards = await client.fetch_all(
        "SELECT id, front, back FROM cards ORDER BY id",
        Card,
    )
    assert len(cards) == 3
    assert cards[0] == Card(id=1, front="bonjour", back="hello")


@pytest.mark.asyncio
async def test_async_execute(asyncpg_pool):
    client = AsyncClient(asyncpg_pool)
    await client.execute(
        "INSERT INTO cards (front, back) VALUES ($1, $2)",
        "async_test",
        "async_back",
    )
    await client.execute(
        "DELETE FROM cards WHERE front = $1",
        "async_test",
    )


@pytest.mark.asyncio
async def test_async_execute_rows(asyncpg_pool):
    client = AsyncClient(asyncpg_pool)
    await client.execute(
        "INSERT INTO cards (front, back) VALUES ($1, $2)",
        "async_del",
        "bye",
    )
    count = await client.execute_rows(
        "DELETE FROM cards WHERE front = $1",
        "async_del",
    )
    assert count == 1


@pytest.mark.asyncio
async def test_query_fetch_one_async(asyncpg_pool):
    query = Query(
        "SELECT id, front, back FROM cards WHERE id = $1",
        Card,
        1,
    )
    card = await query.fetch_one(asyncpg_pool)
    assert card == Card(id=1, front="bonjour", back="hello")


@pytest.mark.asyncio
async def test_query_fetch_optional_async(asyncpg_pool):
    query = Query(
        "SELECT id, front, back FROM cards WHERE id = $1",
        Card,
        1,
    )
    card = await query.fetch_optional(asyncpg_pool)
    assert card == Card(id=1, front="bonjour", back="hello")


@pytest.mark.asyncio
async def test_query_fetch_optional_async_none(asyncpg_pool):
    query = Query(
        "SELECT id, front, back FROM cards WHERE id = $1",
        Card,
        9999,
    )
    assert await query.fetch_optional(asyncpg_pool) is None


@pytest.mark.asyncio
async def test_query_fetch_all_async(asyncpg_pool):
    query = Query("SELECT id, front, back FROM cards ORDER BY id", Card)
    cards = await query.fetch_all(asyncpg_pool)
    assert len(cards) == 3


@pytest.mark.asyncio
async def test_execute_async(asyncpg_pool):
    exe = Execute(
        "INSERT INTO cards (front, back) VALUES ($1, $2)",
        "exec_test",
        "exec_back",
    )
    await exe.execute(asyncpg_pool)
    await Execute("DELETE FROM cards WHERE front = $1", "exec_test").execute(
        asyncpg_pool
    )


@pytest.mark.asyncio
async def test_execute_rows_async(asyncpg_pool):
    await Execute(
        "INSERT INTO cards (front, back) VALUES ($1, $2)",
        "exec_del",
        "bye",
    ).execute(asyncpg_pool)
    exe = Execute("DELETE FROM cards WHERE front = $1", "exec_del")
    assert await exe.execute_rows(asyncpg_pool) == 1


def test_query_decorator_preserves_function():
    fear = FearOfSQL()

    @fear.query
    def my_query(front: str) -> Query[Card]:
        return Query(
            "SELECT id, front, back FROM cards WHERE front = $1",
            Card,
            front,
        )

    result = my_query("bonjour")
    assert isinstance(result, Query)
    assert result.args == ("bonjour",)


def test_validate_passes(conn):
    fear = FearOfSQL()

    @fear.query
    def list_cards() -> Query[Card]:
        return Query(
            "SELECT id, front, back FROM cards ORDER BY id",
            Card,
        )

    @fear.query
    def add_card(front: str, back: str) -> Execute:
        return Execute(
            "INSERT INTO cards (front, back) VALUES ($1, $2)",
            front,
            back,
        )

    fear.validate_all(conn)


def test_validate_raises_on_bad_query(conn):
    fear = FearOfSQL()

    @fear.query
    def bad_query() -> Query[Card]:
        return Query("SELECT id FROM cards", Card)

    with pytest.raises(ValidationError) as exc_info:
        fear.validate_all(conn)

    assert exc_info.value.query_name == "bad_query"
    assert exc_info.value.sql == "SELECT id FROM cards"


def test_validate_with_url():
    """validate() accepts a DSN string directly."""
    fear = FearOfSQL()

    @fear.query
    def list_cards() -> Query[Card]:
        return Query(
            "SELECT id, front, back FROM cards ORDER BY id",
            Card,
        )

    fear.validate_all(DB_URL)


def test_validate_list_param(conn):
    fear = FearOfSQL()

    @fear.query
    def cards_by_ids(ids: list[int]) -> Query[Card]:
        return Query(
            "SELECT id, front, back FROM cards WHERE id = ANY($1)",
            Card,
            ids,
        )

    fear.validate_all(conn)


def test_validate_no_annotation(conn):
    fear = FearOfSQL()

    @fear.query
    def bad(x) -> Query[Card]:
        return Query("SELECT $1", Card, x)

    with pytest.raises(TypeError, match="no type annotation"):
        fear.validate_all(conn)


def test_validate_unsupported_param_type(conn):
    fear = FearOfSQL()

    @fear.query
    def bad(x: complex) -> Query[Card]:
        return Query("SELECT $1", Card, x)

    with pytest.raises(TypeError, match="no dummy value"):
        fear.validate_all(conn)


def test_validate_all_verbose(capsys, conn):
    fear = FearOfSQL()

    @fear.query
    def list_cards() -> Query[Card]:
        return Query("SELECT id, front, back FROM cards", result_type=Card)

    fear.validate_all(conn, verbose=True)

    captured = capsys.readouterr()
    assert "ok: list_cards" in captured.err


def test_validate_all_verbose_error(capsys, conn):
    fear = FearOfSQL()

    @fear.query
    def list_cards() -> Query[int]:
        return Query("SELECT id, front, back FROM cards", result_type=int)

    with pytest.raises(ValidationError):
        fear.validate_all(conn, verbose=True)

    captured = capsys.readouterr()
    assert "ERR: list_cards" in captured.err


@pytest.mark.asyncio
async def test_asyncpg_executor(asyncpg_pool):
    fear = FearOfSQL()

    @dataclass
    class Row:
        id: int

    @fear.query
    def rows() -> Query[Row]:
        return Query("SELECT 1 AS id", result_type=Row)

    assert (await rows().fetch_one(asyncpg_pool)).id == 1
    assert await rows().fetch_all(asyncpg_pool) == [Row(id=1)]
    row = await rows().fetch_optional(asyncpg_pool)
    assert row
    assert row.id == 1

    exe = Execute(
        "INSERT INTO cards (front, back) VALUES ($1, $2)", "asyncpg_test", "bye"
    )
    await exe.execute(asyncpg_pool)
    exe2 = Execute("DELETE FROM cards WHERE front = $1", "asyncpg_test")
    assert await exe2.execute_rows(asyncpg_pool) == 1


@pytest.mark.asyncio
async def test_psycopg_executor(psycopg_conn):
    fear = FearOfSQL()

    @dataclass
    class Row:
        id: int

    @fear.query
    def rows() -> Query[Row]:
        return Query("SELECT 1 AS id", result_type=Row)

    assert (await rows().fetch_one(psycopg_conn)).id == 1
    assert await rows().fetch_all(psycopg_conn) == [Row(id=1)]
    row = await rows().fetch_optional(psycopg_conn)
    assert row
    assert row.id == 1

    exe = Execute(
        "INSERT INTO cards (front, back) VALUES (%s, %s)", "psycopg_test", "bye"
    )
    await exe.execute(psycopg_conn)
    exe2 = Execute("DELETE FROM cards WHERE front = %s", "psycopg_test")
    assert await exe2.execute_rows(psycopg_conn) == 1


@pytest.mark.asyncio
async def test_unsupported_executor():
    q = Query("SELECT 1 AS id", result_type=int)
    with pytest.raises(TypeError, match="unsupported executor type"):
        await q.fetch_one("not a real executor")  # type: ignore[arg-type]


def test_parameterized_query_format_style(conn):
    errors = collect_errors(
        conn,
        "SELECT id FROM cards WHERE front = %s AND back = %s",
        int,
    )
    assert not errors


@pytest.mark.parametrize(
    ("sql", "result_type", "expected"),
    [
        ("SELECT 1 AS val", int, 1),
        (
            "SELECT 'b2b9437a-28df-6ec4-ce4a-2bbdc241330b'::uuid AS val",
            uuid.UUID,
            uuid.UUID("b2b9437a-28df-6ec4-ce4a-2bbdc241330b"),
        ),
        (
            "SELECT TIMESTAMPTZ '2017-04-23 20:44:34+00' AS val",
            datetime.datetime,
            datetime.datetime(2017, 4, 23, 20, 44, 34, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_scalar_fetch_one_sync(dbapi_conn, sql, result_type, expected):
    assert Query(sql, result_type).fetch_one_sync(dbapi_conn) == expected


@pytest.mark.parametrize(
    ("sql", "result_type", "expected"),
    [
        ("SELECT 1 AS val", int, 1),
        (
            "SELECT 'b2b9437a-28df-6ec4-ce4a-2bbdc241330b'::uuid AS val",
            uuid.UUID,
            uuid.UUID("b2b9437a-28df-6ec4-ce4a-2bbdc241330b"),
        ),
        (
            "SELECT TIMESTAMPTZ '2017-04-23 20:44:34+00' AS val",
            datetime.datetime,
            datetime.datetime(2017, 4, 23, 20, 44, 34, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_scalar_fetch_optional_sync(dbapi_conn, sql, result_type, expected):
    assert Query(sql, result_type).fetch_optional_sync(dbapi_conn) == expected


@pytest.mark.parametrize(
    ("sql", "result_type", "expected"),
    [
        ("SELECT 1 AS val", int, [1]),
        ("SELECT 'hello' AS val", str, ["hello"]),
    ],
)
def test_scalar_fetch_all_sync(dbapi_conn, sql, result_type, expected):
    assert Query(sql, result_type).fetch_all_sync(dbapi_conn) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sql", "result_type", "expected"),
    [
        ("SELECT 1 AS val", int, 1),
        (
            "SELECT 'b2b9437a-28df-6ec4-ce4a-2bbdc241330b'::uuid AS val",
            uuid.UUID,
            uuid.UUID("b2b9437a-28df-6ec4-ce4a-2bbdc241330b"),
        ),
        (
            "SELECT TIMESTAMPTZ '2017-04-23 20:44:34+00' AS val",
            datetime.datetime,
            datetime.datetime(2017, 4, 23, 20, 44, 34, tzinfo=datetime.timezone.utc),
        ),
    ],
)
async def test_scalar_fetch_one_async(asyncpg_pool, sql, result_type, expected):
    assert await Query(sql, result_type).fetch_one(asyncpg_pool) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sql", "result_type", "expected"),
    [
        ("SELECT 1 AS val", int, 1),
        (
            "SELECT 'b2b9437a-28df-6ec4-ce4a-2bbdc241330b'::uuid AS val",
            uuid.UUID,
            uuid.UUID("b2b9437a-28df-6ec4-ce4a-2bbdc241330b"),
        ),
        (
            "SELECT TIMESTAMPTZ '2017-04-23 20:44:34+00' AS val",
            datetime.datetime,
            datetime.datetime(2017, 4, 23, 20, 44, 34, tzinfo=datetime.timezone.utc),
        ),
    ],
)
async def test_scalar_fetch_optional_async(asyncpg_pool, sql, result_type, expected):
    assert await Query(sql, result_type).fetch_optional(asyncpg_pool) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sql", "result_type", "expected"),
    [
        ("SELECT 1 AS val", int, [1]),
        ("SELECT 'hello' AS val", str, ["hello"]),
    ],
)
async def test_scalar_fetch_all_async(asyncpg_pool, sql, result_type, expected):
    assert await Query(sql, result_type).fetch_all(asyncpg_pool) == expected
