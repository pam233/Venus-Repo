CREATE TABLE musicschema.agg_top_ten_artists AS
SELECT 
    DISTINCT track_artist AS artist,
    MAX(track_popularity) AS popularity_rate
FROM 
    musicschema.stg_kaggle_spotify_tracks
GROUP BY 
    track_artist
ORDER BY 
    popularity_rate DESC
LIMIT 10;