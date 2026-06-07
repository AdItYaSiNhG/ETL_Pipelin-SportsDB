{{ config(materialized='table') }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_thesportsdb_teams') }}
)

SELECT
    team_id,
    team_name,
    team_short,
    team_alternate,
    formed_year,
    sport_name,
    league_id,
    league_name,
    stadium_name,
    stadium_location,
    stadium_capacity,
    country,
    gender,
    team_badge_url,
    website,
    keywords,
    description_en,
    load_timestamp
FROM stg
QUALIFY ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY load_timestamp DESC) = 1
