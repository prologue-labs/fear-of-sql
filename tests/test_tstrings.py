from fear_of_sql import (
    collect_errors, Execute, Query
)


def test_scalar_tstring_query(conn):
    errors = collect_errors(conn, t"SELECT front FROM cards", str)
    assert not errors

def test_scalar_tstring_interpolation(conn):
    card_id = 1
    errors = collect_errors(conn, t"SELECT front FROM cards WHERE id = {card_id}", str)
    assert not errors

def test_query_tstring(conn):
    from dataclasses import dataclass

    @dataclass
    class Card:
        front: str

    card_id = 1
    q = Query(t"SELECT front FROM cards WHERE id = {card_id}", Card)
    assert q.sql == "SELECT front FROM cards WHERE id = $1"
    assert q.args == (1,)


def test_execute_tstring():
    e = Execute(t"INSERT INTO cards (front, back) VALUES ({'hi'}, {'bye'})")
    assert e.sql == "INSERT INTO cards (front, back) VALUES ($1, $2)"
    assert e.args == ("hi", "bye")
