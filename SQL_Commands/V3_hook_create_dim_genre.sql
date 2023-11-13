CREATE TABLE IF NOT EXISTS musicschema.dim_genre
(
    id SERIAL PRIMARY KEY,
    playlist_genre TEXT
);


INSERT INTO musicschema.dim_genre
(id, playlist_genre)
SELECT 
    track_id,
    playlist_genre
FROM musicschema.stg_kaggle_spotify_tracks
