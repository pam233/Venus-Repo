CREATE TABLE IF NOT EXISTS musicschema.fct_song_genre
(
    id int PRIMARY KEY,
    track_name TEXT,
    track_artist TEXT,
    playlist_genre TEXT
);

INSERT INTO musicschema.fct_song_genre
(id, track_name, track_artist, playlist_genre)
SELECT 
    id,
    track_name,
    track_artist,
    playlist_genre     
FROM musicschema.stg_kaggle_spotify_tracks
ON CONFLICT(id, playlist_genre)
DO UPDATE SET 
    track_name = EXCLUDED.track_name,
    track_artist = EXCLUDED.track_artist;
