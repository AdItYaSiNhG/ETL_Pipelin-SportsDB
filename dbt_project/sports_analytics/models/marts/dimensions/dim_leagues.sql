{{ config(materialized='table') }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_thesportsdb_leagues') }}
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
    load_timestamp
FROM stg
QUALIFY ROW_NUMBER() OVER (PARTITION BY league_id ORDER BY load_timestamp DESC) = 1
