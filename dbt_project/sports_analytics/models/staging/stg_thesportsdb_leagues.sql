{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM read_parquet('s3://{{ env_var("ANALYTICS_ZONE_BUCKET", "analytics-zone") }}/cricket/leagues_clean.parquet')
)

SELECT
    league_id,
    league_name,
    sport_name,
    league_alternate_name,
    formed_year,
    first_event_date,
    country,
    website,
    description_en,
    load_timestamp,
    load_date
FROM src
WHERE league_id IS NOT NULL
