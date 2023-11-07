CREATE TABLE IF NOT EXISTS {target.schema}.dim_genre
(
    id PRIMARY KEY NOT NULL,
    genre_id INTEGER,
    genre_name TEXT
);

CREATE INDEX IF NOT EXISTS idx_genre_id {target_schema}.dim_genre ON (genre_id);

INSERT INTO {target_schema}.dim_genre
(genre_id, genre_name)
SELECT 
    genre_id,
    genre_name
FROM {target_schema}.stg_genre
ON CONFLICT(user_id)
DO UPDATE SET 
    genre_id = excluded.genre_id
    genre_name = excluded.genre_name

