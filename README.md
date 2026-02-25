# fear-of-sql

[![PyPI](https://img.shields.io/pypi/v/fear-of-sql)](https://pypi.org/project/fear-of-sql/)

[sqlx](https://github.com/launchbadge/sqlx)-inspired query validation
for PostgreSQL in Python. Validate your SQL against a real database
schema at startup, not at runtime. Supports t-string interpolation
(Python 3.14+).

## Overview

```python
import fear_of_sql as fos

fear = fos.FearOfSQL()

# t-string query with parameters (requires python 3.14+)
@fear.query
def get_user(user_id: int) -> fos.Query[User]:
    return fos.Query(
        t"SELECT id, name, email FROM users WHERE id = {user_id}",
        result_type=User,
    )

# string SQL query with positional parameters (pre-3.14 or optional for 3.14+)
@fear.query
def get_user(user_id: int) -> fos.Query[User]:
    return fos.Query(
        "SELECT id, name, email FROM users WHERE id = $1",
        User,
        user_id,
    )

# validates all decorated queries against your DB
fear.validate_all("postgresql://localhost/mydb")

# execution helpers, fetch_one, fetch_all, fetch_optional, execute
user = await get_user(user_id=42).fetch_one(pool)
users = await list_users().fetch_all(pool)
maybe_user = await find_user("foo").fetch_optional(pool)
await delete_user(user_id=1).execute(pool)
```

Or validate raw SQL strings directly:

```python
with fos.connect("postgresql://localhost/mydb") as conn:
    errors = fos.collect_errors(conn, "SELECT id, name FROM users", User)
```

Catch errors at startup, not at runtime:

```python
# wrong type — id is an integer in the database
>>> fos.collect_errors(conn, "SELECT id FROM flashcards", str)
> TypeMismatchError("column 'id': expected ['str'], got int")

# wrong field type in a dataclass (pydantic also supported)
@dataclass
class Flashcard:
    id: int
    front: str
    back: int  # actually text in the database

>>> fos.collect_errors(conn, "SELECT id, front, back FROM flashcards", Flashcard)
> TypeMismatchError("column 'back': expected ['int'], got str")

# table doesn't exist
>>> fos.collect_errors(conn, "SELECT id FROM not_a_table", int)
> DatabaseError: relation "not_a_table" does not exist
```

## Drivers

### Validation

Validation requires `pg8000.native` as it exposes the column
metadata (`table_oid`, `column_attrnum`) needed for nullability
inference via `pg_catalog`. DB-API 2.0 compatible drivers do
not expose this information.


### Additional driver support

`asyncpg` is used by default for async execution helpers.

`psycopg` is supported, but optional:

```sh
pip install fear-of-sql[psycopg]
```

### Execution


| Driver | Async | Sync |
|---|---|---|
| asyncpg | yes | — |
| psycopg | yes | yes |
| pg8000 (DB-API) | — | yes |

Queries can use either `$1` or `%s` parameter style, or t-string interpolation
on supported Python versions (3.14+).


## Nullability overrides

Nullability is inferred automatically from `pg_catalog` and `EXPLAIN` plan
analysis (including joins). For cases the inference can't handle, override
with `!` or `?` column aliases:

```python
# count(*) is always non-null, but Postgres can't prove it — force not-null
Query('SELECT count(*) AS "count!" FROM users', result_type=int)

# force a NOT NULL column to be treated as nullable
Query(r'SELECT id AS "id?" FROM flashcards', result_type=int | None)
```

Same convention as [sqlx](https://github.com/launchbadge/sqlx).

## Supported Types

| PostgreSQL | Python mapping |
|---|---|
| `bool` | `bool` |
| `int2`, `int4`, `int8` | `int` |
| `float4`, `float8` | `float` |
| `numeric` | `Decimal` |
| `text`, `varchar`, `char`, `name` | `str` |
| `bytea` | `bytes` |
| `uuid` | `uuid.UUID` |
| `date` | `datetime.date` |
| `time` | `datetime.time` |
| `timestamp`, `timestamptz` | `datetime.datetime` |
| `interval` | `datetime.timedelta` |
| `json`, `jsonb` | `object` |
| `money` | `str` |
| `oid` | `int` |
| All above as arrays | `list[T]` |


## Known Limitations

### Unsupported PostgreSQL Types

The following built-in types are not yet supported and will raise
`UnsupportedTypeError` during validation:

- Network types: `inet`, `cidr`, `macaddr`
- Geometric types: `point`, `line`, `box`, `lseg`, `path`, `polygon`, `circle`
- Range types: `int4range`, `int8range`, `numrange`, `daterange`, `tsrange`, `tstzrange`
- Bit string types: `bit`, `varbit`
- Full-text search: `tsvector`, `tsquery`
- XML: `xml`

To work around these issues, do not decorate the query functions (if using
them), or do not pass them into the query validation.

### User-Defined Types

Custom enums, composite types, and domain types use dynamically-assigned OIDs
and require catalog lookup at validation time. Not yet implemented, but
supported by the validation architecture.

### Extension Types

Types from extensions (`hstore`, `ltree`, `citext`, etc.) are not supported.