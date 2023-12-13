CREATE TABLE IF NOT EXISTS musicschema.dim_songs
(
   
    track_id INT PRIMARY KEY,
    track_name TEXT,
    track_artist TEXT,
    playlist_genre TEXT
);

INSERT INTO musicschema.dim_songs
(track_id, track_name, track_artist, playlist_genre)
SELECT 
    track_id,
    track_name,
    track_artist,
    playlist_genre
FROM musicschema.stg_kaggle_spotify_tracks
ON CONFLICT(track_id)
DO UPDATE SET 
    track_name = excluded.track_name,
    track_artist = excluded.track_artist,
    playlist_genre = excluded.playlist_genre;
