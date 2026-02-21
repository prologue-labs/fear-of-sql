import asyncio
from contextlib import asynccontextmanager

import asyncpg
import typer
from pydantic.dataclasses import dataclass

import fear_of_sql as fos

DB_URL = "postgresql://user:password@localhost:5433/fear_of_sql_test"

app = typer.Typer()
fear = fos.FearOfSQL()

@dataclass
class Card:
    id: int
    front: str
    back: str


@fear.query
def list_query() -> fos.Query[Card]:
    return fos.Query("SELECT id, front, back FROM cards ORDER BY id", result_type=Card)


@fear.query
def find_query(front: str) -> fos.Query[Card]:
    return fos.Query(
        t"SELECT id, front, back FROM cards WHERE front = {front}",
        result_type=Card
    )


@fear.query
def add_query(front: str, back: str) -> fos.Execute:
    return fos.Execute(
        t"INSERT INTO cards (front, back) VALUES ({front}, {back})"
    )


@fear.query
def delete_query(card_id: int) -> fos.Execute:
    return fos.Execute(t"DELETE FROM cards WHERE id = {card_id}")


@asynccontextmanager
async def connect():
    conn = await asyncpg.create_pool(DB_URL)
    # conn = await psycopg.AsyncConnection.connect(DB_URL)  # for psycopg
    try:
        yield conn
    finally:
        await conn.close()


@app.command()
def add(front: str, back: str):
    """Add a new flashcard."""

    async def _add():
        async with connect() as conn:
            await add_query(front=front, back=back).execute(conn)
            print(f"  Added: {front} → {back}")

    asyncio.run(_add())


@app.command("list")
def list_cards():
    """List all flashcards."""

    async def _list_cards():
        async with connect() as conn:
            cards = await list_query().fetch_all(conn)
            for card in cards:
                print(f"  [{card.id}] {card.front} → {card.back}")

    asyncio.run(_list_cards())


@app.command()
def find(text: str):
    """Find a card by front text."""

    async def _find():
        async with connect() as conn:
            card = await find_query(front=text).fetch_optional(conn)
            if card:
                print(f"  [{card.id}] {card.front} → {card.back}")
            else:
                print(f"  No card found for '{text}'")

    asyncio.run(_find())


@app.command()
def delete(id: int):
    """Delete a flashcard by ID."""

    async def _delete():
        async with connect() as conn:
            await delete_query(id).execute(conn)
            print(f"  Deleted: {id}")

    asyncio.run(_delete())


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    fear.validate_all(DB_URL, verbose=True)

    if ctx.invoked_subcommand is None:
        list_cards()


if __name__ == "__main__":
    app()
