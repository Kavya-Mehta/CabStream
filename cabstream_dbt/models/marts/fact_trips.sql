-- fact_trips.sql
-- Central fact table: one row per trip
-- All measurable facts + foreign keys to dimensions

with silver as (
    select * from {{ ref('stg_silver_trips') }}
),

fact as (
    select
        -- Trip metrics (the facts)
        fare_amount,
        tip_amount,
        total_amount,
        trip_distance,
        passenger_count,
        payment_type,

        -- Calculated metrics
        round(
    (epoch(dropoff_datetime) - epoch(pickup_datetime)) / 60.0,
    2
) as trip_duration_minutes,

        round(
            case
                when trip_distance > 0
                then fare_amount / trip_distance
                else null
            end,
            2
        ) as fare_per_mile,

        round(
            case
                when fare_amount > 0
                then tip_amount / fare_amount * 100
                else 0
            end,
            2
        ) as tip_pct,

        -- Foreign keys to dimensions
        pickup_location_id,
        dropoff_location_id,
        pickup_hour,
        pickup_month,
        pickup_year,
        pickup_dow,
        is_weekend,
        is_rush_hour,
        weather_condition,
        weather_temp_c,
        weather_precip_mm,
        weather_wind_kmh,

        -- Timestamps
        pickup_datetime,
        dropoff_datetime,

        -- Pipeline metadata
        vendor_id,
        kafka_offset,
        ingestion_timestamp,
        silver_timestamp

    from silver
    where pickup_datetime is not null
      and dropoff_datetime is not null
      and fare_amount > 0
)

select * from fact