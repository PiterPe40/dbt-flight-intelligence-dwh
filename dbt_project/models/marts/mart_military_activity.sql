{{
    config(
        materialized='table',
        tags=['marts']
    )
}}

/*
    Mart: Military activity summary by country and date.

    Provides a comprehensive view of military flight activity including
    baseline comparisons, rolling averages, and trend indicators.
    Works in tandem with mart_military_anomaly_scores (Python model)
    which adds Z-score based anomaly detection.
*/

with daily as (
    select * from {{ ref('int_daily_military_counts') }}
),

with_rolling as (
    select
        *,

        -- 7-day rolling average (smooths daily noise)
        avg(military_flight_count) over (
            partition by country_code
            order by date_id
            rows between 6 preceding and current row
        ) as rolling_7d_avg,

        -- 30-day rolling average (baseline)
        avg(military_flight_count) over (
            partition by country_code
            order by date_id
            rows between 29 preceding and current row
        ) as rolling_30d_avg,

        -- 30-day rolling standard deviation
        stddev(military_flight_count) over (
            partition by country_code
            order by date_id
            rows between 29 preceding and current row
        ) as rolling_30d_std,

        -- Day-over-day change
        military_flight_count - lag(military_flight_count) over (
            partition by country_code
            order by date_id
        ) as day_over_day_change,

        -- Week-over-week change (same day of week, 1 week ago)
        military_flight_count - lag(military_flight_count, 7) over (
            partition by country_code
            order by date_id
        ) as week_over_week_change,

        -- Rank within country (highest activity days)
        row_number() over (
            partition by country_code
            order by military_flight_count desc
        ) as activity_rank_desc

    from daily
)

select
    date_id,
    country_code,
    day_of_week,
    military_flight_count,
    unique_aircraft_count,

    -- Category breakdown
    transport_flights,
    fighter_flights,
    training_flights,
    tanker_flights,
    surveillance_flights,

    -- Rolling averages
    round(rolling_7d_avg, 1)                        as rolling_7d_avg,
    round(rolling_30d_avg, 1)                       as rolling_30d_avg,
    round(rolling_30d_std, 2)                       as rolling_30d_std,

    -- Simple Z-score (SQL-based, approximate)
    case
        when rolling_30d_std > 0 then
            round(
                (military_flight_count - rolling_30d_avg) / rolling_30d_std,
                2
            )
        else 0
    end as simple_z_score,

    -- Change metrics
    day_over_day_change,
    week_over_week_change,

    -- Deviation from baseline
    round(
        (military_flight_count - rolling_30d_avg) / nullif(rolling_30d_avg, 0) * 100,
        1
    ) as pct_above_baseline,

    -- Distance and route metrics
    avg_distance_km,
    max_distance_km,
    unique_origins,
    unique_destinations,

    -- Rank
    activity_rank_desc

from with_rolling
