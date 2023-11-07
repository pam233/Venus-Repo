CREATE TABLE IF NOT EXISTS {target_schema}.fct_user_genre
(
    id PRIMARY KEY  NOT NULL,
    user_id INTEGER,
    genre_id INTEGER
);

-- CREATE INDEX for user id
-- CREATE INDEX for genre id

INSERT INTO {target_schema}.fct_user_genre
(user_id, genre_id)