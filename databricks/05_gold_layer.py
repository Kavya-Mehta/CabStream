# Databricks notebook source
ADLS_SILVER_YELLOW = "abfss://delta@cabstreamdata.dfs.core.windows.net/silver/taxi_trips_enriched"
ADLS_SILVER_FHVHV = "abfss://delta@cabstreamdata.dfs.core.windows.net/silver/fhvhv_trips"
ADLS_GOLD = "abfss://delta@cabstreamdata.dfs.core.windows.net/gold"

print("Gold config ready")
print(f"Yellow Silver: {ADLS_SILVER_YELLOW}")
print(f"FHVHV Silver:  {ADLS_SILVER_FHVHV}")
print(f"Gold output:   {ADLS_GOLD}")

# COMMAND ----------

from pyspark.sql import functions as F

# Generate date dimension 2019-2025
date_range = spark.sql("""
    SELECT explode(sequence(
        to_date('2019-01-01'), 
        to_date('2025-12-31'), 
        interval 1 day
    )) AS date
""")

dim_time = date_range.select(
    F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
    F.col("date"),
    F.year("date").alias("year"),
    F.month("date").alias("month"),
    F.dayofmonth("date").alias("day"),
    F.dayofweek("date").alias("day_of_week"),
    F.date_format("date", "EEEE").alias("day_name"),
    F.quarter("date").alias("quarter"),
    F.weekofyear("date").alias("week_of_year"),
    F.when(F.dayofweek("date").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
    F.date_format("date", "yyyy-MM").alias("year_month")
)

dim_time.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{ADLS_GOLD}/dim_time")

print(f"dim_time rows: {dim_time.count():,}")
dim_time.show(5)

# COMMAND ----------

# NYC Taxi Zone lookup — 265 zones
# Download directly from NYC TLC
import urllib.request
import pandas as pd
from io import StringIO

zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
response = urllib.request.urlopen(zone_url)
zone_csv = response.read().decode("utf-8")

zone_pd = pd.read_csv(StringIO(zone_csv))
print(f"Zone lookup rows: {len(zone_pd)}")
print(zone_pd.head())

dim_zone = spark.createDataFrame(zone_pd) \
    .withColumnRenamed("LocationID", "location_id") \
    .withColumnRenamed("Borough", "borough") \
    .withColumnRenamed("Zone", "zone") \
    .withColumnRenamed("service_zone", "service_zone")

dim_zone.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{ADLS_GOLD}/dim_zone")

print(f"dim_zone rows: {dim_zone.count()}")
dim_zone.show(5)

# COMMAND ----------

from pyspark.sql import functions as F

# Read enriched Silver
yellow_silver = spark.read.format("delta").load(ADLS_SILVER_YELLOW)

# Build fact table
fact_yellow = yellow_silver.select(
    # Keys
    F.date_format("tpep_pickup_datetime", "yyyyMMdd").cast("int").alias("date_key"),
    F.col("PULocationID").alias("pickup_location_id"),
    F.col("DOLocationID").alias("dropoff_location_id"),
    F.col("pickup_hour"),
    F.col("puYear").alias("year"),
    F.col("puMonth").alias("month"),
    # Trip metrics
    F.col("trip_distance"),
    F.col("fare_amount"),
    F.col("tip_amount"),
    F.col("total_amount"),
    F.col("passenger_count"),
    F.col("payment_type"),
    F.col("RatecodeID").alias("rate_code"),
    # Time flags
    F.col("is_weekend"),
    F.col("is_rush_hour"),
    # Weather
    F.col("weather_temp_c"),
    F.col("weather_wind_kmh"),
    F.col("weather_precip_mm"),
    # Source
    F.lit("yellow").alias("taxi_type")
)

fact_yellow.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .save(f"{ADLS_GOLD}/fact_trips_yellow")

count = spark.read.format("delta").load(f"{ADLS_GOLD}/fact_trips_yellow").count()
print(f"fact_trips_yellow rows: {count:,}")

# COMMAND ----------

# Read FHVHV Silver
fhvhv_silver = spark.read.format("delta").load(ADLS_SILVER_FHVHV)

fact_fhvhv = fhvhv_silver.select(
    # Keys
    F.date_format("pickup_datetime", "yyyyMMdd").cast("int").alias("date_key"),
    F.col("PULocationID").alias("pickup_location_id"),
    F.col("DOLocationID").alias("dropoff_location_id"),
    F.col("pickup_hour"),
    F.col("puYear").alias("year"),
    F.col("puMonth").alias("month"),
    # Trip metrics
    F.col("trip_miles").alias("trip_distance"),
    F.col("base_passenger_fare").alias("fare_amount"),
    F.col("tips").alias("tip_amount"),
    F.col("driver_pay"),
    F.col("trip_time"),
    # Company
    F.col("company"),
    F.col("hvfhs_license_num"),
    # Flags
    F.col("shared_request_flag"),
    F.col("shared_match_flag"),
    F.col("wav_request_flag"),
    F.col("is_weekend"),
    F.col("is_rush_hour"),
    # Source
    F.lit("fhvhv").alias("taxi_type")
)

fact_fhvhv.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .save(f"{ADLS_GOLD}/fact_trips_fhvhv")

count = spark.read.format("delta").load(f"{ADLS_GOLD}/fact_trips_fhvhv").count()
print(f"fact_trips_fhvhv rows: {count:,}")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

ADLS_GOLD = "abfss://delta@cabstreamdata.dfs.core.windows.net/gold"

# Read Gold tables
fact_yellow = spark.read.format("delta").load(f"{ADLS_GOLD}/fact_trips_yellow")
fact_fhvhv = spark.read.format("delta").load(f"{ADLS_GOLD}/fact_trips_fhvhv")
dim_time = spark.read.format("delta").load(f"{ADLS_GOLD}/dim_time")
dim_zone = spark.read.format("delta").load(f"{ADLS_GOLD}/dim_zone")

yellow_count = fact_yellow.count()
fhvhv_count = fact_fhvhv.count()

print("=== CABSTREAM GOLD LAYER COMPLETE ===")
print(f"fact_trips_yellow:  {yellow_count:,}")
print(f"fact_trips_fhvhv:   {fhvhv_count:,}")
print(f"Combined fact rows: {yellow_count + fhvhv_count:,}")
print(f"dim_time rows:      {dim_time.count():,}")
print(f"dim_zone rows:      {dim_zone.count():,}")
print()
print("=== QUICK BUSINESS METRICS ===")

# COVID impact
pre_covid = fact_yellow.filter((F.col("year") == 2020) & (F.col("month") == 1)).count()
covid = fact_yellow.filter((F.col("year") == 2020) & (F.col("month") == 4)).count()
print(f"Yellow taxi Jan 2020: {pre_covid:,}")
print(f"Yellow taxi Apr 2020: {covid:,} (COVID collapse)")

# Uber vs Yellow 2024
yellow_2024 = fact_yellow.filter(F.col("year") == 2024).count()
fhvhv_2024 = fact_fhvhv.filter(F.col("year") == 2024).count()
print(f"Yellow taxi 2024:  {yellow_2024:,}")
print(f"Uber/Lyft 2024:    {fhvhv_2024:,}")
print(f"Rideshare dominance: {fhvhv_2024 / (yellow_2024 + fhvhv_2024) * 100:.1f}%")