-- dim_weather.sql
-- Weather dimension from Silver enrichment

with weather as (
    select distinct
        weather_condition,
        weather_temp_c,
        weather_precip_mm,
        weather_wind_kmh
    from {{ ref('stg_silver_trips') }}
    where weather_condition is not null
),

enriched as (
    select
        weather_condition,
        avg(weather_temp_c)    as avg_temp_c,
        avg(weather_precip_mm) as avg_precip_mm,
        avg(weather_wind_kmh)  as avg_wind_kmh,
        case weather_condition
            when 'Rain'  then 'Precipitation expected, higher demand likely'
            when 'Snow'  then 'Severe weather, very high demand'
            when 'Clear' then 'Good conditions, normal demand'
            else 'Unknown conditions'
        end as demand_impact
    from weather
    group by weather_condition
)

select * from enriched