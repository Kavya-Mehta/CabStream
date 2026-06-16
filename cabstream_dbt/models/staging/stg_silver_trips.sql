-- stg_silver_trips.sql
-- Reads directly from Silver Delta table via DuckDB
-- Why staging layer: creates a clean contract between
-- raw Silver data and Gold business models.
-- If Silver schema changes, only this file needs updating.

with source as (
    select *
    from delta_scan(
        'C:/Users/kavya/OneDrive/Desktop/CabStream/data/silver/taxi_trips'
    )
),

renamed as (
    select
        -- Trip identifiers
        vendor_id,
        
        -- Timestamps
        pickup_datetime,
        dropoff_datetime,
        
        -- Time dimensions (already calculated in Silver)
        pickup_hour,
        pickup_month,
        pickup_year,
        pickup_dow,
        is_weekend,
        is_rush_hour,
        
        -- Location
        pickup_location_id,
        dropoff_location_id,
        
        -- Trip metrics
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        
        -- Weather (joined in Silver)
        weather_temp_c,
        weather_precip_mm,
        weather_wind_kmh,
        weather_condition,
        
        -- Metadata
        kafka_partition,
        kafka_offset,
        ingestion_timestamp,
        silver_timestamp

    from source
)

select * from renamed