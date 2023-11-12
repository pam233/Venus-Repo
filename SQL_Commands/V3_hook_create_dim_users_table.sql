CREATE TABLE IF NOT EXISTS musicschema.dim_user
(
    id int PRIMARY KEY,
    age TEXT,
    gender TEXT, 
    fav_music_genre TEXT,
    music_time_slot TEXT,
    music_lis_frequency TEXT,
    music_satisfaction TEXT
);

INSERT INTO musicschema.dim_user
(id, age, gender, fav_music_genre, music_time_slot, music_lis_frequency, music_satisfaction)
SELECT 
    id,
    age,
    gender,
    fav_music_genre,
    music_time_slot,
    music_lis_frequency,
    music_satisfaction
FROM musicschema.stg_kaggle_spotify_users
ON CONFLICT(id)
DO UPDATE SET 
    age = excluded.age,
    gender = excluded.gender,
    fav_music_genre = excluded.fav_music_genre,
    music_time_slot = excluded.music_time_slot,
    music_lis_frequency = excluded.music_lis_frequency,
    music_satisfaction = excluded.music_satisfaction;
