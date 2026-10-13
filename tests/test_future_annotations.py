from __future__ import annotations

from dataclasses import dataclass

from fear_of_sql import FearOfSQL, Query, collect_errors


def test_validate_with_future_annotations(conn):
    fear = FearOfSQL()

    @fear.query
    def get_card(card_id: int) -> Query[int]:
        return Query("SELECT id FROM cards WHERE id = $1", int, card_id)

    fear.validate_all(conn)


def test_dataclass_result_with_future_annotations(conn):
    @dataclass
    class Card:
        id: int
        front: str

    errors = collect_errors(conn, "SELECT id, front FROM cards", Card)
    assert not errors
