# schema.py
# Gold layer schema context for Text-to-SQL agent
# Injected into every Claude prompt so the model knows
# exact table names, columns, types, and business meaning

SCHEMA_CONTEXT = """
You are a SQL expert working with NYC taxi and rideshare trip data stored in Delta Lake.
The database contains the following tables:

TABLE: fact_trips_yellow
Description: Yellow taxi trips in New York City (2019-2025), weather-enriched
Rows: 252,320,653
Columns:
  - date_key (INT): Date in YYYYMMDD format, joins to dim_time
  - pickup_location_id (LONG): Pickup zone ID, joins to dim_zone
  - dropoff_location_id (LONG): Dropoff zone ID, joins to dim_zone
  - pickup_hour (INT): Hour of pickup (0-23)
  - year (INT): Trip year (2019-2025)
  - month (INT): Trip month (1-12)
  - trip_distance (DOUBLE): Trip distance in miles
  - fare_amount (DOUBLE): Base fare in USD
  - tip_amount (DOUBLE): Tip in USD
  - total_amount (DOUBLE): Total charged in USD
  - passenger_count (DOUBLE): Number of passengers (1-6)
  - payment_type (LONG): 1=Credit card, 2=Cash, 3=No charge, 4=Dispute
  - rate_code (DOUBLE): 1=Standard, 2=JFK, 3=Newark, 4=Nassau, 5=Negotiated, 6=Group
  - is_weekend (BOOLEAN): True if pickup on Saturday or Sunday
  - is_rush_hour (BOOLEAN): True if weekday 7-9am or 4-7pm
  - weather_temp_c (DOUBLE): Temperature in Celsius at pickup time
  - weather_wind_kmh (DOUBLE): Wind speed in km/h at pickup time
  - weather_precip_mm (DOUBLE): Precipitation in mm at pickup time
  - taxi_type (STRING): Always 'yellow'

TABLE: fact_trips_fhvhv
Description: High-volume for-hire vehicle trips (Uber, Lyft, Via, Juno) in NYC (2019-2025)
Rows: 1,295,824,950
Columns:
  - date_key (INT): Date in YYYYMMDD format, joins to dim_time
  - pickup_location_id (LONG): Pickup zone ID, joins to dim_zone
  - dropoff_location_id (LONG): Dropoff zone ID, joins to dim_zone
  - pickup_hour (INT): Hour of pickup (0-23)
  - year (INT): Trip year (2019-2025)
  - month (INT): Trip month (1-12)
  - trip_distance (DOUBLE): Trip distance in miles
  - fare_amount (DOUBLE): Base passenger fare in USD
  - tip_amount (DOUBLE): Tip in USD
  - driver_pay (DOUBLE): Amount paid to driver in USD
  - trip_time (LONG): Trip duration in seconds
  - company (STRING): 'Uber', 'Lyft', 'Via', or 'Juno'
  - hvfhs_license_num (STRING): License number (HV0003=Uber, HV0005=Lyft, HV0004=Via, HV0002=Juno)
  - shared_request_flag (STRING): 'Y' if passenger requested shared ride
  - shared_match_flag (STRING): 'Y' if ride was matched as shared
  - wav_request_flag (STRING): 'Y' if wheelchair accessible vehicle requested
  - is_weekend (BOOLEAN): True if pickup on Saturday or Sunday
  - is_rush_hour (BOOLEAN): True if weekday 7-9am or 4-7pm
  - taxi_type (STRING): Always 'fhvhv'

TABLE: dim_time
Description: Date dimension (2019-2025)
Rows: 2,557
Columns:
  - date_key (INT): Date in YYYYMMDD format, primary key
  - date (DATE): Calendar date
  - year (INT): Year
  - month (INT): Month number (1-12)
  - day (INT): Day of month
  - day_of_week (INT): 1=Sunday, 7=Saturday
  - day_name (STRING): Full day name (Monday, Tuesday, etc.)
  - quarter (INT): Quarter (1-4)
  - week_of_year (INT): Week number
  - is_weekend (BOOLEAN): True if Saturday or Sunday
  - year_month (STRING): Format 'YYYY-MM'

TABLE: dim_zone
Description: NYC Taxi Zone lookup (265 zones)
Rows: 265
Columns:
  - location_id (LONG): Zone ID, primary key
  - borough (STRING): NYC borough (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)
  - zone (STRING): Zone name (e.g. 'Times Sq/Theatre District')
  - service_zone (STRING): 'Yellow Zone', 'Boro Zone', 'Airports', 'EWR'

IMPORTANT RULES:
- Always use LIMIT to cap results (default LIMIT 100, max LIMIT 1000)
- Only SELECT statements are allowed
- Join dim_zone using pickup_location_id = location_id or dropoff_location_id = location_id
- Join dim_time using date_key
- For COVID analysis, filter year=2020 and month=4
- For rideshare vs yellow comparison, query both fact tables separately or use UNION ALL with taxi_type
- borough values: 'Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island', 'EWR'
"""

# Short schema summary for token-efficient prompts
SCHEMA_SUMMARY = """
Tables: fact_trips_yellow (252M rows), fact_trips_fhvhv (1.295B rows), dim_time (2,557 rows), dim_zone (265 rows)
Key joins: fact.pickup_location_id = dim_zone.location_id | fact.date_key = dim_time.date_key
Key filters: year (2019-2025), month (1-12), company (Uber/Lyft/Via/Juno), borough, is_weekend, is_rush_hour
"""