{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

/*
    Intermediate model: Daily military flight counts per country.

    Aggregates military flights by date, country, and day of week.
    This is the input for the Military Activity Anomaly Detector
    (Z-score algorithm in mart_military_anomaly_scores).
*/

with daily_counts as (
    select
        flight_date                                     as date_id,
        extract(dow from flight_date)                   as day_of_week,
        military_country                                as country_code,

        -- Flight counts
        count(*)                                        as military_flight_count,
        count(distinct icao24)                          as unique_aircraft_count,

        -- Category breakdown
        sum(case when military_category = 'TRANSPORT' then 1 else 0 end)
            as transport_flights,
        sum(case when military_category = 'FIGHTER' then 1 else 0 end)
            as fighter_flights,
        sum(case when military_category = 'TRAINING' then 1 else 0 end)
            as training_flights,
        sum(case when military_category = 'TANKER' then 1 else 0 end)
            as tanker_flights,
        sum(case when military_category = 'SURVEILLANCE' then 1 else 0 end)
            as surveillance_flights,
        sum(case when military_category = 'UNKNOWN' then 1 else 0 end)
            as unknown_flights,

        -- Distance stats
        avg(distance_km)                                as avg_distance_km,
        max(distance_km)                                as max_distance_km,

        -- Route diversity
        count(distinct origin_airport_id)               as unique_origins,
        count(distinct destination_airport_id)           as unique_destinations

    from {{ ref('int_military_flights') }}
    where military_country is not null
    group by 1, 2, 3
)

select * from daily_counts
where military_flight_count >= {{ var('min_flights_threshold', 3) }}
