CREATE TABLE IF NOT EXISTS musicschema.agg_playlist
(
    id serial PRIMARY KEY,
    playlist_id INT,
    playlist_name TEXT,
    playlist_genre TEXT,
    average_track_popularity INT,
    average_energy FLOAT,
    average_valence FLOAT,
    average_tempo FLOAT,
    total_duration_ms INT
);

INSERT INTO musicschema.agg_playlist
(playlist_id, playlist_name, playlist_genre, average_track_popularity, average_energy, average_valence, average_tempo, total_duration_ms)
SELECT
    playlist_id,
    playlist_name,
    playlist_genre,
    AVG(track_popularity) AS average_track_popularity,
    AVG(energy) AS average_energy,
    AVG(valence) AS average_valence,
    AVG(tempo) AS average_tempo,
    SUM(duration_ms) AS total_duration_ms
FROM musicschema.stg_kaggle_spotify_tracks
GROUP BY playlist_id, playlist_name, playlist_genre
ON CONFLICT(id) 
DO UPDATE SET
    playlist_id = excluded.playlist_id,
    playlist_name = excluded.playlist_name,
    playlist_genre = excluded.playlist_genre,
    average_track_popularity = excluded.average_track_popularity,
    average_energy = excluded.average_energy,
    average_valence = excluded.average_valence,
    average_tempo = excluded.average_tempo,
    total_duration_ms = excluded.total_duration_ms;

