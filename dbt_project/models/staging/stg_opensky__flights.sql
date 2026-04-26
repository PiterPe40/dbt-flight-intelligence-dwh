{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

/*
    Staging model: Raw flights from OpenSky Network API.

    Minimal transformation — rename columns to snake_case,
    cast types, handle NULLs. NO business logic here.

    Source: raw_flights table loaded by the Airflow DAG.
*/

with source as (
    select * from {{ source('opensky', 'raw_flights') }}
),

renamed as (
    select
        -- Primary identifier
        {{ dbt_utils.generate_surrogate_key(['icao24', 'firstSeen', 'estDepartureAirport']) }}
            as flight_id,

        -- Aircraft identifier
        lower(trim(icao24))                     as icao24,

        -- Timestamps (Unix → Timestamp)
        to_timestamp(firstSeen)                 as first_seen_at,
        to_timestamp(lastSeen)                  as last_seen_at,

        -- Airports
        upper(trim(estDepartureAirport))        as origin_airport_id,
        upper(trim(estArrivalAirport))          as destination_airport_id,

        -- Callsign
        upper(trim(callsign))                   as callsign,

        -- Distance estimates (meters)
        estDepartureAirportHorizDistance         as departure_horiz_distance_m,
        estDepartureAirportVertDistance          as departure_vert_distance_m,
        estArrivalAirportHorizDistance           as arrival_horiz_distance_m,
        estArrivalAirportVertDistance            as arrival_vert_distance_m,

        -- Candidate counts
        departureAirportCandidatesCount          as departure_candidates_count,
        arrivalAirportCandidatesCount            as arrival_candidates_count,

        -- Metadata
        loaded_at,
        source_date

    from source
    where
        -- Basic quality filters
        icao24 is not null
        and firstSeen is not null
        and lastSeen is not null
        and lastSeen > firstSeen  -- arrival must be after departure
)

select * from renamed
