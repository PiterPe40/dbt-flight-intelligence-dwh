{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

/*
    Staging model: Airport metadata from seed file.

    Maps ICAO codes to airport names, cities, countries, and coordinates.
    Source: seeds/airport_metadata.csv
*/

with source as (
    select * from {{ ref('airport_metadata') }}
),

cleaned as (
    select
        upper(trim(airport_id))         as airport_id,
        trim(airport_name)              as airport_name,
        trim(city)                      as city,
        upper(trim(country_code))       as country_code,
        trim(country_name)              as country_name,
        cast(latitude as double)        as latitude,
        cast(longitude as double)       as longitude,
        trim(timezone)                  as timezone,
        trim(airport_type)              as airport_type

    from source
    where airport_id is not null
)

select * from cleaned
