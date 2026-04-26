{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

/*
    Intermediate model: Military flights only.

    Filters enriched flights to include only those whose ICAO24 address
    falls within known military hex ranges. Adds military-specific
    categorization based on callsign patterns and flight characteristics.
*/

with military as (
    select
        *,

        -- Categorize military flights by callsign patterns
        case
            -- Transport aircraft patterns
            when callsign like 'RCH%'   then 'TRANSPORT'      -- US Air Mobility Command
            when callsign like 'RRR%'   then 'TRANSPORT'      -- US Air Force
            when callsign like 'GAF%'   then 'TRANSPORT'      -- German Air Force
            when callsign like 'PLF%'   then 'TRANSPORT'      -- Polish Air Force
            when callsign like 'CNV%'   then 'TRANSPORT'      -- Convair / military transport

            -- Tanker / refueling
            when callsign like 'LAGR%'  then 'TANKER'
            when callsign like 'TITAN%' then 'TANKER'

            -- Training patterns
            when callsign like 'VIPER%' then 'TRAINING'
            when callsign like 'HAWK%'  then 'TRAINING'

            -- Fighter / tactical (shorter callsigns, specific patterns)
            when callsign like 'VENOM%' then 'FIGHTER'
            when callsign like 'RAGE%'  then 'FIGHTER'

            -- Reconnaissance / surveillance
            when callsign like 'HOMER%' then 'SURVEILLANCE'
            when callsign like 'FORTE%' then 'SURVEILLANCE'    -- Global Hawk

            else 'UNKNOWN'
        end as military_category,

        -- Flight distance category
        case
            when distance_km is null then 'UNKNOWN'
            when distance_km < 500 then 'SHORT_RANGE'
            when distance_km < 2000 then 'MEDIUM_RANGE'
            else 'LONG_RANGE'
        end as range_category

    from {{ ref('int_flights_enriched') }}
    where is_military = true
)

select * from military
