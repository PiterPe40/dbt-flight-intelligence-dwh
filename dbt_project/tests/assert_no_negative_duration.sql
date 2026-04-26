/*
    Custom dbt test: No negative flight durations.

    Flights must have a positive duration (arrival after departure).
    Any negative values indicate data quality issues in the source.
*/

select
    flight_id,
    flight_duration_min,
    first_seen_at,
    last_seen_at
from {{ ref('int_flights_enriched') }}
where flight_duration_min < 0
