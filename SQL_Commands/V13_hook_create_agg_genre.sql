CREATE TABLE IF NOT EXISTS musicschema.agg_genre
(
    id serial PRIMARY KEY,
    fav_music_genre TEXT,
    total_users INT,
    male_users INT,
    female_users INT,
    average_age INT
);

-- Insert or update data
INSERT INTO musicschema.agg_genre
(fav_music_genre, total_users, male_users, female_users, average_age)
SELECT
    fav_music_genre,
    COUNT(DISTINCT id) AS total_users,
    SUM(CASE WHEN gender = 'Male' THEN 1 ELSE 0 END) AS male_users,
    SUM(CASE WHEN gender = 'Female' THEN 1 ELSE 0 END) AS female_users,
    AVG(CAST(LEFT(age, POSITION('-' IN age) - 1) AS INT)) AS average_age
FROM musicschema.stg_kaggle_spotify_users
GROUP BY fav_music_genre
ON CONFLICT(id) 
DO UPDATE SET
    total_users = excluded.total_users,
    male_users = excluded.male_users,
    female_users = excluded.female_users,
    average_age = excluded.average_age;
