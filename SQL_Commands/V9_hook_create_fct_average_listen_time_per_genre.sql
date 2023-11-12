CREATE TABLE IF NOT EXISTS musicschema.fct_average_listen_time_per_genre
(
    id int PRIMARY KEY,
    playlist_genre TEXT,
    average_listen_time INT
);

INSERT INTO musicschema.fct_average_listen_time_per_genre
(id, playlist_genre, average_listen_time)
SELECT 
    id,
    fav_music_genre AS playlist_genre,
    AVG(music_time_slot) AS average_listen_time
FROM musicschema.stg_kaggle_spotify_users
GROUP BY id, fav_music_genre
ON CONFLICT(id, playlist_genre)
DO UPDATE SET 
    average_listen_time = EXCLUDED.average_listen_time;
