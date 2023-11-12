-- Create the aggregate table
CREATE TABLE IF NOT EXISTS musicschema.aggregate_songs_per_genre
(
    genre_id INTEGER PRIMARY KEY,
    genre_name TEXT,
    number_of_songs INTEGER
);


INSERT INTO musicschema.aggregate_songs_per_genre
    (genre_id, genre_name, number_of_songs)
SELECT
    gm.genre_id,
    gm.genre_name_one AS genre_name,
    COUNT(s.track_id) AS number_of_songs
FROM
    musicschema.stg_kaggle_spotify_songs s
JOIN
    musicschema.genre_mapping gm ON s.genre = gm.genre_name_one
GROUP BY
    gm.genre_id, gm.genre_name_one;


INSERT INTO musicschema.aggregate_songs_per_genre
    (genre_id, genre_name, number_of_songs)
SELECT
    gm.genre_id,
    gm.genre_name_one AS genre_name,
    COUNT(s.track_id) AS number_of_songs
FROM
    musicschema.stg_kaggle_spotify_songs s
JOIN
    musicschema.genre_mapping gm ON s.genre = gm.genre_name_one
GROUP BY
    gm.genre_id, gm.genre_name_one
ON CONFLICT (genre_id) DO UPDATE
    SET number_of_songs = EXCLUDED.number_of_songs;
