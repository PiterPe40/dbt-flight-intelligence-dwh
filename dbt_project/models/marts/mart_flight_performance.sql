{{
    config(
        materialized='table',
        tags=['marts']
    )
}}

/*
    Mart: Flight route performance metrics.

    Aggregates flight data by route (origin → destination) to show
    average duration, distance, and frequency — useful for identifying
    the busiest and most efficient air corridors.
*/

with route_stats as (
    select
        origin_airport_id,
        destination_airport_id,
        origin_airport_name,
        destination_airport_name,
        origin_country,
        destination_country,

        -- Volume metrics
        count(*)                                        as total_flights,
        count(distinct flight_date)                     as active_days,
        count(distinct icao24)                          as unique_aircraft,

        -- Duration stats (minutes)
        round(avg(flight_duration_min), 1)              as avg_duration_min,
        round(min(flight_duration_min), 1)              as min_duration_min,
        round(max(flight_duration_min), 1)              as max_duration_min,
        round(stddev(flight_duration_min), 1)           as stddev_duration_min,

        -- Distance stats (km)
        round(avg(distance_km), 1)                      as avg_distance_km,

        -- Time patterns
        round(avg(case when is_weekend then 1.0 else 0.0 end) * 100, 1)
            as weekend_pct,

        -- Military ratio
        round(
            sum(case when is_military then 1 else 0 end) * 100.0 / count(*), 1
        ) as military_pct,

        min(flight_date)                                as first_flight,
        max(flight_date)                                as last_flight

    from {{ ref('int_flights_enriched') }}
    where
        origin_airport_id is not null
        and destination_airport_id is not null
    group by 1, 2, 3, 4, 5, 6
    having count(*) >= 5  -- Only routes with at least 5 flights
)

select
    *,
    -- Flights per active day (daily frequency)
    round(total_flights * 1.0 / nullif(active_days, 0), 2) as flights_per_day,

    -- Route efficiency: lower ratio = more efficient (less time per km)
    round(avg_duration_min / nullif(avg_distance_km, 0), 4) as min_per_km,

    -- Route type classification
    case
        when origin_country = destination_country then 'DOMESTIC'
        else 'INTERNATIONAL'
    end as route_type,

    -- Route tier
    case
        when total_flights >= 500 then 'TIER_1_MAJOR'
        when total_flights >= 100 then 'TIER_2_REGULAR'
        when total_flights >= 20 then 'TIER_3_MINOR'
        else 'TIER_4_RARE'
    end as route_tier

from route_stats
