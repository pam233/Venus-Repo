CREATE TABLE musicschema.agg_top_ten_albums AS
SELECT 
    track_artist AS artist,
    track_name,
    track_album_name,
    track_album_release_date,
    playlist_genre,
    MAX(track_popularity) AS popularity_rate
FROM 
    musicschema.stg_kaggle_spotify_tracks
GROUP BY 
    track_artist, track_name, track_album_name, track_album_release_date, playlist_genre
ORDER BY 
    popularity_rate DESC
LIMIT 10;
