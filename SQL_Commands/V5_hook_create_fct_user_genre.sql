CREATE TABLE IF NOT EXISTS musicschema.fct_user_genre
(
    user_id INTEGER,
    age TEXT,
    gender TEXT,
    fav_music_genre TEXT
);

INSERT INTO musicschema.fct_user_genre
    (user_id, age, gender, fav_music_genre)
SELECT 
    id,
    age,
    gender,
    fav_music_genre
FROM musicschema.stg_kaggle_spotify_users
ON CONFLICT(user_id, fav_music_genre)
DO UPDATE SET 
    age = EXCLUDED.age,
    gender = EXCLUDED.gender;
