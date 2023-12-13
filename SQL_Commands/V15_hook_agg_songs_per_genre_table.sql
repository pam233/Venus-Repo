CREATE TABLE IF NOT EXISTS musicschema.agg_songs_per_genre

(
    genre_name TEXT PRIMARY KEY,
    number_of_songs INTEGER

);
 
INSERT INTO musicschema.agg_songs_per_genre
    (genre_name, number_of_songs)

SELECT
    LOWER(s.playlist_genre) AS genre_name,
    COUNT(s.track_id) AS number_of_songs

FROM
    musicschema.stg_kaggle_spotify_tracks s

GROUP BY
    LOWER(s.playlist_genre)
ON CONFLICT (genre_name) DO UPDATE
    SET number_of_songs = EXCLUDED.number_of_songs;