{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

/*
    Staging model: Military ICAO24 address ranges from seed file.

    ICAO24 hex codes are allocated by country, and certain ranges
    are assigned to military operators. This table maps hex ranges
    to countries and military branches.

    Source: seeds/military_icao24_ranges.csv
*/

with source as (
    select * from {{ ref('military_icao24_ranges') }}
),

cleaned as (
    select
        lower(trim(range_start))        as range_start,
        lower(trim(range_end))          as range_end,
        upper(trim(country_code))       as country_code,
        trim(country_name)              as country_name,
        trim(military_branch)           as military_branch,
        trim(notes)                     as notes

    from source
    where range_start is not null
)

select * from cleaned
