CREATE TABLE IF NOT EXISTS musicschema.fct_user_genre
(
    user_id INTEGER,
    age TEXT,
    gender TEXT,
    fav_music_genre TEXT,
    CONSTRAINT unique_user_genre_constraint UNIQUE (user_id, fav_music_genre)
);

INSERT INTO musicschema.fct_user_genre
    (user_id, age, gender, fav_music_genre)
SELECT 
    id,
    age,
    gender,
    fav_music_genre
FROM musicschema.stg_kaggle_spotify_users
ON CONFLICT ON CONSTRAINT unique_user_genre_constraint
DO UPDATE SET 
    age = EXCLUDED.age,
    gender = EXCLUDED.gender;
