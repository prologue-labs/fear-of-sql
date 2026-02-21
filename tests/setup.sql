CREATE TABLE cards (
    id          serial PRIMARY KEY,
    front       text NOT NULL,
    back        text NOT NULL,
    notes       text,
    active      boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reviews (
    id          serial PRIMARY KEY,
    card_id     integer NOT NULL REFERENCES cards(id),
    score       integer NOT NULL,
    reviewed_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO cards (front, back) VALUES
    ('bonjour', 'hello'),
    ('안녕하세요', 'hello');

INSERT INTO cards (front, back, notes) VALUES
    ('hola', 'hi', 'informal');

INSERT INTO reviews (card_id, score) VALUES
    (1, 5),
    (1, 4),
    (2, 3);

