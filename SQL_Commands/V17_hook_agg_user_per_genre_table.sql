CREATE TABLE IF NOT EXISTS musicschema.aggregate_users_per_genre
(
    genre_id INTEGER PRIMARY KEY,
    genre_name TEXT,
    number_of_users INTEGER
);

INSERT INTO musicschema.aggregate_users_per_genre
    (genre_id, genre_name, number_of_users)
SELECT
    gm.genre_id,
    gm.genre_name_one AS genre_name,
    COUNT(u.id) AS number_of_users
FROM
    musicschema.stg_kaggle_spotify_users u
JOIN
    musicschema.genre_mapping gm ON u.fav_music_genre = gm.genre_name_one
GROUP BY
    gm.genre_id, gm.genre_name_one;


INSERT INTO musicschema.aggregate_users_per_genre
    (genre_id, genre_name, number_of_users)
SELECT
    gm.genre_id,
    gm.genre_name_one AS genre_name,
    COUNT(u.id) AS number_of_users
FROM
    musicschema.stg_kaggle_spotify_users u
JOIN
    musicschema.genre_mapping gm ON u.fav_music_genre = gm.genre_name_one
GROUP BY
    gm.genre_id, gm.genre_name_one
ON CONFLICT (genre_id) DO UPDATE
    SET number_of_users = EXCLUDED.number_of_users;