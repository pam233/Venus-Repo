CREATE TABLE IF NOT EXISTS musicschema.dim_genre
(
    id int PRIMARY KEY,
    playlist_genre TEXT
);

INSERT INTO musicschema.dim_genre
(id, playlist_genre)
SELECT 
    id,
    playlist_genre
FROM musicschema.stg_kaggle_spotify_songs
ON CONFLICT(id)
DO UPDATE SET 
    id = excluded.id,
    playlist_genre = excluded.playlist_genre;
