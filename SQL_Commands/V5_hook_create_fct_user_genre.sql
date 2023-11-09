CREATE TABLE IF NOT EXISTS musicschema.fct_user_genre
(
    id PRIMARY KEY  NOT NULL,
    user_id INTEGER,
    genre_id INTEGER
);

INSERT INTO musicschema.fct_user_genre
(id, genre_id)