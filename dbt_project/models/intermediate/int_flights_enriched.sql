{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

/*
    Intermediate model: Flights enriched with airport and aircraft metadata.

    Joins raw flights with airport coordinates to calculate:
    - Flight duration (minutes)
    - Great-circle distance (haversine formula)
    - Military flag (based on ICAO24 hex ranges)
    - Date dimensions
*/

with flights as (
    select * from {{ ref('stg_opensky__flights') }}
),

airports as (
    select * from {{ ref('stg_opensky__airports') }}
),

military_ranges as (
    select * from {{ ref('stg_opensky__military_ranges') }}
),

enriched as (
    select
        f.flight_id,
        f.icao24,
        f.callsign,

        -- Timestamps
        f.first_seen_at,
        f.last_seen_at,
        cast(f.first_seen_at as date)                   as flight_date,

        -- Flight duration in minutes
        (extract(epoch from f.last_seen_at) - extract(epoch from f.first_seen_at)) / 60.0
            as flight_duration_min,

        -- Origin airport
        f.origin_airport_id,
        orig.airport_name                               as origin_airport_name,
        orig.city                                       as origin_city,
        orig.country_code                               as origin_country,
        orig.latitude                                   as origin_lat,
        orig.longitude                                  as origin_lon,

        -- Destination airport
        f.destination_airport_id,
        dest.airport_name                               as destination_airport_name,
        dest.city                                       as destination_city,
        dest.country_code                               as destination_country,
        dest.latitude                                   as destination_lat,
        dest.longitude                                  as destination_lon,

        -- Great-circle distance (Haversine formula) in km
        case
            when orig.latitude is not null
                 and orig.longitude is not null
                 and dest.latitude is not null
                 and dest.longitude is not null
            then
                6371.0 * 2 * asin(sqrt(
                    power(sin(radians(dest.latitude - orig.latitude) / 2), 2)
                    + cos(radians(orig.latitude))
                      * cos(radians(dest.latitude))
                      * power(sin(radians(dest.longitude - orig.longitude) / 2), 2)
                ))
            else null
        end as distance_km,

        -- Military classification
        case
            when mr.country_code is not null then true
            else false
        end as is_military,

        mr.country_code                                 as military_country,
        mr.military_branch,

        -- Date dimensions
        extract(year from f.first_seen_at)              as flight_year,
        extract(month from f.first_seen_at)             as flight_month,
        extract(dow from f.first_seen_at)               as day_of_week,
        case
            when extract(dow from f.first_seen_at) in (0, 6) then true
            else false
        end as is_weekend,

        -- Metadata
        f.source_date

    from flights f

    left join airports orig
        on f.origin_airport_id = orig.airport_id

    left join airports dest
        on f.destination_airport_id = dest.airport_id

    -- Military classification: check if ICAO24 falls within any military range
    left join military_ranges mr
        on f.icao24 >= mr.range_start
        and f.icao24 <= mr.range_end
)

select * from enriched
