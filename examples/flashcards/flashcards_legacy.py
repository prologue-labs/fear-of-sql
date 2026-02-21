import asyncio
from contextlib import asynccontextmanager

import asyncpg
import typer
from pydantic.dataclasses import dataclass
from rich.console import Console

import fear_of_sql


app = typer.Typer()


@dataclass
class Card:
    id: int
    front: str
    back: str


LIST_QUERY = "SELECT id, front, back FROM cards ORDER BY id", Card
FIND_QUERY = "SELECT id, front, back FROM cards WHERE front = $1", Card
ADD_EXECUTE = "INSERT INTO cards (front, back) VALUES ($1, $2)", None
DELETE_EXECUTE = "DELETE FROM cards WHERE id = $1", None

QUERIES: list[tuple[str, type | None]] = [
    LIST_QUERY,
    FIND_QUERY,
    ADD_EXECUTE,
    DELETE_EXECUTE,
]


DB_URL = "postgresql://user:password@localhost:5433/fear_of_sql_test"


console = Console()


def validate_all():
    with fear_of_sql.connect(DB_URL) as conn:
        for sql, result_type in QUERIES:
            errors = fear_of_sql.collect_errors(conn, sql, result_type)
            if errors:
                console.print(f"  [red]✗[/red] {sql}")
                for e in errors:
                    console.print(f"    [red]✗[/red] {e}")
                raise SystemExit(1)
            console.print(f"  [green]✓[/green] {sql}")


@asynccontextmanager
async def connect():
    p = await asyncpg.create_pool(DB_URL)
    try:
        yield fear_of_sql.AsyncClient(p)
    finally:
        await p.close()


@app.command()
def add(front: str, back: str):
    """Add a new flashcard."""

    async def _add():
        async with connect() as client:
            sql, _ = ADD_EXECUTE
            await client.execute(sql, front, back)
            print(f"  Added: {front} → {back}")

    asyncio.run(_add())


@app.command("list")
def list_cards():
    """List all flashcards."""

    async def _list_cards():
        async with connect() as client:
            sql, result_type = LIST_QUERY
            cards = await client.fetch_all(sql, result_type)
            for card in cards:
                print(f"  [{card.id}] {card.front} → {card.back}")

    asyncio.run(_list_cards())


@app.command()
def find(text: str):
    """Find a card by front text."""

    async def _find():
        async with connect() as client:
            query, return_type = FIND_QUERY
            card = await client.fetch_optional(query, return_type, text)
            if card:
                print(f"  [{card.id}] {card.front} → {card.back}")
            else:
                print(f"  No card found for '{text}'")

    asyncio.run(_find())


@app.command()
def delete(id: int):
    """Delete a flashcard by ID."""

    async def _delete():
        async with connect() as client:
            sql, _ = DELETE_EXECUTE
            await client.execute(sql, id)
            print(f"  Deleted: {id}")

    asyncio.run(_delete())


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    validate_all()

    if ctx.invoked_subcommand is None:
        list_cards()


if __name__ == "__main__":
    app()
