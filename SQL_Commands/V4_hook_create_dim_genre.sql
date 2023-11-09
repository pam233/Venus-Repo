CREATE TABLE IF NOT EXISTS musicschema.dim_genre
(
    id PRIMARY KEY NOT NULL,
    playlist_genre TEXT
);

CREATE INDEX IF NOT EXISTS idx_genre_id musicschema.dim_genre ON (id);

INSERT INTO {musicschema}.dim_genre
(id, playlist_genre)
SELECT 
    id,
   playlist_genre
FROM {musicschema}.stg_kaggle_spotify_songs
ON CONFLICT(id)
DO UPDATE SET 
    id = excluded.id
    palylist_genre = excluded.playlist_genre

