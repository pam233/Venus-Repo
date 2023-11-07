CREATE TABLE IF NOT EXISTS {target_schema}.agg_user
(
    id PRIMARY KEY NOT NULL,
    user_id INTEGER,
    total_genre NUMERIC
);

INSERT INTO {target_schema}.agg_user
SELECT 
    user_id,
    COUNT(DISTINCT genre_id) AS total_genre
FROM {target_schema}.fct_user_genre
WHERE fct_user_genre.user_id NOT IN 
(
    SELECT 
        user_id
    FROM {target_schema}.agg_user
);

UPDATE {target_schema}.agg_user
SET total_genre = subquery.total_genre
FROM
(
    SELECT 
        user_id,
        COUNT(DISTINCT genre_id) AS total_genre
    FROM {target_schema}.fct_user_genre
) subquery
WHERE agg_user.user_id = subquery.user_id