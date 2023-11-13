CREATE TABLE musicschema.stg_kaggle_spotify_tracks (
    track_id INT PRIMARY KEY,
    track_name TEXT,
    track_artist TEXT,
    track_popularity INT,
    track_album_name TEXT,
    track_album_release_date DATE,
    playlist_name TEXT,
    playlist_id INT,
    playlist_genre TEXT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    valence FLOAT,
    tempo INT,
    duration_ms INT
);