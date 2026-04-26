{{
    config(
        materialized='table',
        tags=['marts']
    )
}}

/*
    Mart: Airport network metrics (SQL-based).

    Calculates degree centrality and connection diversity for each airport.
    For advanced metrics (PageRank, betweenness), see the Python model
    mart_pagerank_airports.py which uses NetworkX.
*/

with outbound as (
    select
        origin_airport_id                           as airport_id,
        count(distinct destination_airport_id)      as out_degree,
        sum(flight_count)                           as outbound_flights,
        count(distinct destination_country)         as outbound_countries
    from {{ ref('int_airport_connections') }}
    group by 1
),

inbound as (
    select
        destination_airport_id                      as airport_id,
        count(distinct origin_airport_id)           as in_degree,
        sum(flight_count)                           as inbound_flights,
        count(distinct origin_country)              as inbound_countries
    from {{ ref('int_airport_connections') }}
    group by 1
),

airports as (
    select * from {{ ref('stg_opensky__airports') }}
),

network_metrics as (
    select
        coalesce(o.airport_id, i.airport_id)        as airport_id,

        -- Degree metrics
        coalesce(o.out_degree, 0)                   as out_degree,
        coalesce(i.in_degree, 0)                    as in_degree,
        coalesce(o.out_degree, 0) + coalesce(i.in_degree, 0)
            as total_degree,

        -- Volume metrics
        coalesce(o.outbound_flights, 0)             as outbound_flights,
        coalesce(i.inbound_flights, 0)              as inbound_flights,
        coalesce(o.outbound_flights, 0) + coalesce(i.inbound_flights, 0)
            as total_flights,

        -- Connectivity (unique countries served)
        coalesce(o.outbound_countries, 0)           as outbound_countries,
        coalesce(i.inbound_countries, 0)            as inbound_countries

    from outbound o
    full outer join inbound i on o.airport_id = i.airport_id
)

select
    n.airport_id,
    a.airport_name,
    a.city,
    a.country_code,
    a.latitude,
    a.longitude,

    -- Degree centrality
    n.out_degree,
    n.in_degree,
    n.total_degree,

    -- Volume
    n.outbound_flights,
    n.inbound_flights,
    n.total_flights,

    -- International connectivity
    n.outbound_countries,
    n.inbound_countries,

    -- Hub classification
    case
        when n.total_degree >= 100 then 'MAJOR_HUB'
        when n.total_degree >= 50 then 'REGIONAL_HUB'
        when n.total_degree >= 20 then 'MEDIUM_AIRPORT'
        when n.total_degree >= 5 then 'SMALL_AIRPORT'
        else 'MINOR_AIRPORT'
    end as hub_category,

    -- Asymmetry ratio (departure-heavy vs arrival-heavy)
    case
        when n.total_flights > 0 then
            round(
                (n.outbound_flights - n.inbound_flights) * 1.0 / n.total_flights,
                3
            )
        else 0
    end as flow_asymmetry

from network_metrics n
left join airports a on n.airport_id = a.airport_id

order by n.total_flights desc
