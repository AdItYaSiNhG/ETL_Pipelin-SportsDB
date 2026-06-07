{{ config(materialized='table') }}

WITH date_range AS (
    SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date
    FROM {{ ref('stg_thesportsdb_events') }}
),
span AS (
    SELECT
        min_date,
        max_date,
        CAST(DATEDIFF('day', min_date, max_date) AS INTEGER) + 1 AS day_count
    FROM date_range
),
calendar AS (
    SELECT
        CAST(min_date AS DATE) + INTERVAL (n) DAY AS date_day
    FROM span
    CROSS JOIN UNNEST(generate_series(0, GREATEST(day_count - 1, 0))) AS t(n)
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day)  AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day)   AS day_of_month,
    EXTRACT(DOW FROM date_day)   AS day_of_week,
    EXTRACT(WEEK FROM date_day)  AS week_of_year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    STRFTIME(date_day, '%A')     AS day_name,
    STRFTIME(date_day, '%B')     AS month_name,
    (EXTRACT(MONTH FROM date_day) IN (12, 1, 2)) AS is_winter,
    (EXTRACT(MONTH FROM date_day) IN (6, 7, 8))  AS is_summer
FROM calendar
WHERE date_day IS NOT NULL
