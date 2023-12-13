CREATE TABLE IF NOT EXISTS musicschema.agg_users_per_genre

(
    genre_name TEXT PRIMARY KEY,
    number_of_users INTEGER

);
 
INSERT INTO musicschema.agg_users_per_genre
    (genre_name, number_of_users)

SELECT
    LOWER(u.fav_music_genre) AS genre_name,
    COUNT(u.id) AS number_of_users

FROM
    musicschema.stg_kaggle_spotify_users u
GROUP BY
    LOWER(u.fav_music_genre)
ON CONFLICT (genre_name) DO UPDATE
    SET number_of_users = EXCLUDED.number_of_users;
