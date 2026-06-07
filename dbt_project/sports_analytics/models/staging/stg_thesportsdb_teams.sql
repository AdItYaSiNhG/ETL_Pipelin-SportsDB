{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM read_parquet('s3://{{ env_var("ANALYTICS_ZONE_BUCKET", "analytics-zone") }}/cricket/teams_clean.parquet')
)

SELECT
    team_id,
    team_name,
    team_short,
    team_alternate,
    formed_year,
    sport_name,
    league_name,
    league_id,
    stadium_name,
    stadium_location,
    stadium_capacity,
    country,
    team_badge_url,
    website,
    keywords,
    gender,
    description_en,
    load_timestamp,
    load_date
FROM src
WHERE team_id IS NOT NULL
