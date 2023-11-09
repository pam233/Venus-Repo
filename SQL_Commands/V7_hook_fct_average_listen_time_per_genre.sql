CREATE TABLE IF NOT EXISTS musicschema.fct_average_listen_time_per_genre
(
    id SERIAL PRIMARY KEY,
    playlist_genre TEXT,
    average_listen_time NUMERIC
);
INSERT INTO musicschema.fct_average_listen_time_per_genre (playlist_genre, average_listen_time)
SELECT
    playlist_genre,
    AVG(duration_ms) AS average_listen_time
FROM
    stg_kaggle_spotify_genre
GROUP BY
    playlist_genre;
