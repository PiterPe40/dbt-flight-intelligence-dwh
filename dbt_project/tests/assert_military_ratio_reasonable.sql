/*
    Custom dbt test: Military flight ratio is reasonable.

    Military flights typically make up < 10% of all air traffic.
    If the ratio exceeds 20%, there may be a data quality issue
    with the ICAO24 military range mapping (too broad ranges).
*/

with stats as (
    select
        count(*) as total_flights,
        sum(case when is_military then 1 else 0 end) as military_flights,
        sum(case when is_military then 1 else 0 end) * 100.0 / count(*) as military_pct
    from {{ ref('int_flights_enriched') }}
)

select *
from stats
where military_pct > 20.0
