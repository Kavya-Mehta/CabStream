-- dim_time.sql
-- Time dimension built from trip pickup hours
-- Why build from actual data: only includes hours that exist in trips

with trips as (
    select distinct
        pickup_hour,
        pickup_month,
        pickup_year,
        pickup_dow,
        is_weekend,
        is_rush_hour
    from {{ ref('stg_silver_trips') }}
    where pickup_datetime is not null
),

enriched as (
    select
        pickup_hour,
        pickup_month,
        pickup_year,
        pickup_dow,
        is_weekend,
        is_rush_hour,
        case pickup_month
            when 1  then 'January'
            when 2  then 'February'
            when 3  then 'March'
            when 4  then 'April'
            when 5  then 'May'
            when 6  then 'June'
            when 7  then 'July'
            when 8  then 'August'
            when 9  then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name,
        case
            when pickup_hour between 6  and 11 then 'Morning'
            when pickup_hour between 12 and 16 then 'Afternoon'
            when pickup_hour between 17 and 20 then 'Evening'
            else 'Night'
        end as time_of_day
    from trips
)

select * from enriched