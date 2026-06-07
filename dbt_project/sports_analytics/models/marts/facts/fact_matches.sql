{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_thesportsdb_events') }}
)

SELECT
    event_id,
    event_name,
    event_date,
    season,
    league_id,
    league_name,
    home_team_id,
    home_team_name,
    home_score,
    away_team_id,
    away_team_name,
    away_score,
    venue,
    venue_id,
    city,
    country,
    status,
    result_description,
    group_name,
    CASE
        WHEN home_score IS NULL OR away_score IS NULL THEN NULL
        WHEN home_score > away_score THEN 'H'
        WHEN home_score < away_score THEN 'A'
        ELSE 'D'
    END AS result_code,
    CASE
        WHEN home_score IS NULL OR away_score IS NULL THEN NULL
        ELSE home_score + away_score
    END AS total_runs,
    CASE
        WHEN result_description IS NULL OR result_description = '' THEN NULL
        WHEN LOWER(result_description) LIKE '%won by%runs%' OR LOWER(result_description) LIKE '%won by%run%' THEN 'runs'
        WHEN LOWER(result_description) LIKE '%won by%wickets%' OR LOWER(result_description) LIKE '%won by%wkts%' THEN 'wickets'
        WHEN LOWER(result_description) LIKE '%tied%' THEN 'tied'
        WHEN LOWER(result_description) LIKE '%no result%' OR LOWER(result_description) LIKE '%abandoned%' THEN 'no_result'
        ELSE 'other'
    END AS win_margin_type,
    load_timestamp
FROM stg

{% if is_incremental() %}
WHERE load_timestamp > (SELECT COALESCE(MAX(load_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
