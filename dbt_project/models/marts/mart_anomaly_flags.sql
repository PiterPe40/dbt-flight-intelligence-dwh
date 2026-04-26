{{
    config(
        materialized='table',
        tags=['marts']
    )
}}

/*
    Mart: Anomaly flags — days with abnormal military activity.

    Filters mart_military_activity to show only days where the
    simple Z-score exceeds the configured threshold. For more
    sophisticated anomaly detection (per day-of-week baseline),
    see the Python model mart_military_anomaly_scores.
*/

with activity as (
    select * from {{ ref('mart_military_activity') }}
),

anomalies as (
    select
        *,

        -- Anomaly level based on Z-score
        case
            when simple_z_score >= 3.5 then 'CRITICAL'
            when simple_z_score >= 2.5 then 'HIGH'
            when simple_z_score >= 1.5 then 'ELEVATED'
            else 'NORMAL'
        end as anomaly_level,

        -- Alert flag
        case
            when simple_z_score >= {{ var('anomaly_zscore_threshold', 2.5) }}
            then true
            else false
        end as is_anomaly,

        -- Context: was there a spike in specific categories?
        case
            when fighter_flights > rolling_7d_avg * 0.3 then 'FIGHTER_SPIKE'
            when transport_flights > rolling_7d_avg * 0.5 then 'TRANSPORT_SURGE'
            when surveillance_flights > 0 then 'SURVEILLANCE_ACTIVE'
            else 'GENERAL_INCREASE'
        end as anomaly_context

    from activity
    where simple_z_score >= 1.5  -- At least ELEVATED
)

select * from anomalies
order by date_id desc, simple_z_score desc
