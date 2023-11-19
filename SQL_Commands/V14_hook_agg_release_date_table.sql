CREATE TABLE IF NOT EXISTS musicschema.agg_release_date
(
    track_album_release_date TEXT,
    total_tracks INT,
    average_track_popularity INT
);

INSERT INTO musicschema.agg_release_date
(track_album_release_date, total_tracks, average_track_popularity)
SELECT
    track_album_release_date,
    COUNT(DISTINCT track_id) AS total_tracks,
    AVG(CAST(track_popularity AS INT)) AS average_track_popularity
FROM musicschema.stg_kaggle_spotify_tracks
GROUP BY track_album_release_date