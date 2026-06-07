{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM read_parquet('s3://{{ env_var("ANALYTICS_ZONE_BUCKET", "analytics-zone") }}/cricket/events_clean.parquet')
)

SELECT
    event_id,
    event_name,
    event_date,
    season,
    league_name,
    league_id,
    home_team_id,
    home_team_name,
    home_score,
    away_team_id,
    away_team_name,
    away_score,
    event_time,
    venue,
    venue_id,
    city,
    country,
    status,
    result_description,
    group_name,
    sport_name,
    thumbnail_url,
    video_url,
    load_timestamp,
    load_date
FROM src
WHERE event_id IS NOT NULL
  AND event_date IS NOT NULL
