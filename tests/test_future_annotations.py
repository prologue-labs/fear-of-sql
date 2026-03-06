from __future__ import annotations

from fear_of_sql import FearOfSQL, Query


def test_validate_with_future_annotations(conn):
    fear = FearOfSQL()

    @fear.query
    def get_card(card_id: int) -> Query[int]:
        return Query("SELECT id FROM cards WHERE id = $1", int, card_id)

    fear.validate_all(conn)
