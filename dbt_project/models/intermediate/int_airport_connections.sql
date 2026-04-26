{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

/*
    Intermediate model: Airport connection pairs.

    Aggregates flights into origin-destination pairs with connection
    frequency. This is the edge list for the airport network graph.
*/

with connections as (
    select
        origin_airport_id,
        destination_airport_id,
        origin_airport_name,
        destination_airport_name,
        origin_country,
        destination_country,

        -- Connection metrics
        count(*)                                    as flight_count,
        count(distinct flight_date)                 as active_days,
        avg(flight_duration_min)                    as avg_duration_min,
        avg(distance_km)                            as avg_distance_km,

        -- Military vs civilian breakdown
        sum(case when is_military then 1 else 0 end)   as military_flights,
        sum(case when not is_military then 1 else 0 end) as civilian_flights,

        min(flight_date)                            as first_flight_date,
        max(flight_date)                            as last_flight_date

    from {{ ref('int_flights_enriched') }}
    where
        origin_airport_id is not null
        and destination_airport_id is not null
        and origin_airport_id != destination_airport_id
    group by 1, 2, 3, 4, 5, 6
)

select
    *,
    -- Domestic vs international flag
    case
        when origin_country = destination_country then 'DOMESTIC'
        else 'INTERNATIONAL'
    end as route_type,

    -- Frequency category
    case
        when flight_count >= 100 then 'HIGH_FREQUENCY'
        when flight_count >= 20 then 'MEDIUM_FREQUENCY'
        else 'LOW_FREQUENCY'
    end as frequency_category

from connections
