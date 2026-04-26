/*
    Custom dbt test: No flights longer than 24 hours.

    The maximum realistic flight duration is ~24 hours (e.g., Singapore-New York).
    Anything longer indicates a data issue (wrong timestamps, duplicate records).
*/

select
    flight_id,
    flight_duration_min,
    origin_airport_id,
    destination_airport_id
from {{ ref('int_flights_enriched') }}
where flight_duration_min > 1440  -- 24 hours * 60 minutes
