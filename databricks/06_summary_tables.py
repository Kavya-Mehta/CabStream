# Databricks notebook source
from pyspark.sql import functions as F

ADLS_GOLD = "abfss://delta@cabstreamdata.dfs.core.windows.net/gold"
ADLS_SUMMARY = "abfss://delta@cabstreamdata.dfs.core.windows.net/summary"

# Read Gold tables
yellow = spark.read.format("delta").load(f"{ADLS_GOLD}/fact_trips_yellow")
fhvhv = spark.read.format("delta").load(f"{ADLS_GOLD}/fact_trips_fhvhv")
dim_zone = spark.read.format("delta").load(f"{ADLS_GOLD}/dim_zone")

print("Gold tables loaded")
print(f"  yellow: {yellow.count():,} rows")
print(f"  fhvhv: {fhvhv.count():,} rows")
print(f"  dim_zone: {dim_zone.count():,} rows")

# COMMAND ----------

from pyspark.sql import functions as F

# ── 1. MONTHLY TRIPS ─────────────────────────────────────────────────────────
monthly_yellow = yellow.groupBy("year", "month") \
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.sum("total_amount"), 0).alias("total_revenue")
    ).withColumn("taxi_type", F.lit("Yellow Taxi"))

monthly_fhvhv = fhvhv.groupBy("year", "month", "company") \
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.sum("fare_amount"), 0).alias("total_revenue")
    ).withColumnRenamed("company", "taxi_type")

monthly = monthly_yellow.unionByName(monthly_fhvhv, allowMissingColumns=True)
monthly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ADLS_SUMMARY}/monthly_trips")
print(f"monthly_trips: {monthly.count():,} rows")

# ── 2. BOROUGH SUMMARY ───────────────────────────────────────────────────────
borough = yellow.join(
    dim_zone.select("location_id", "borough", "zone"),
    yellow.pickup_location_id == dim_zone.location_id
).groupBy("year", "borough") \
.agg(
    F.count("*").alias("trips"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
    F.round(F.sum("total_amount"), 0).alias("total_revenue"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance")
).withColumn("taxi_type", F.lit("Yellow Taxi"))

borough.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ADLS_SUMMARY}/borough_summary")
print(f"borough_summary: {borough.count():,} rows")

# ── 3. ZONE SUMMARY ──────────────────────────────────────────────────────────
zone = yellow.join(
    dim_zone.select("location_id", "borough", "zone", "service_zone"),
    yellow.pickup_location_id == dim_zone.location_id
).groupBy("zone", "borough", "service_zone") \
.agg(
    F.count("*").alias("trips"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    F.round(F.sum("total_amount"), 0).alias("total_revenue")
).orderBy(F.desc("trips"))

zone.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ADLS_SUMMARY}/zone_summary")
print(f"zone_summary: {zone.count():,} rows")

# ── 4. HOURLY SUMMARY ────────────────────────────────────────────────────────
hourly = yellow.groupBy("pickup_hour", "is_weekend", "is_rush_hour") \
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.avg("tip_amount") / F.nullif(F.avg("fare_amount"), F.lit(0)) * 100, 2).alias("avg_tip_pct"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance")
    )

hourly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ADLS_SUMMARY}/hourly_summary")
print(f"hourly_summary: {hourly.count():,} rows")

# ── 5. WEATHER SUMMARY (with borough) ────────────────────────────────────────
weather = yellow.join(
    dim_zone.select("location_id", "borough"),
    yellow.pickup_location_id == dim_zone.location_id
).withColumn("temp_bucket",
    F.when(F.col("weather_temp_c") < 0, "Freezing (<0C)")
     .when(F.col("weather_temp_c") < 10, "Cold (0-10C)")
     .when(F.col("weather_temp_c") < 20, "Mild (10-20C)")
     .when(F.col("weather_temp_c") < 30, "Warm (20-30C)")
     .otherwise("Hot (>30C)")
).withColumn("rain_bucket",
    F.when(F.col("weather_precip_mm") == 0, "No rain")
     .when(F.col("weather_precip_mm") < 2, "Light rain")
     .when(F.col("weather_precip_mm") < 10, "Moderate rain")
     .otherwise("Heavy rain")
).groupBy("borough", "temp_bucket", "rain_bucket") \
.agg(
    F.count("*").alias("trips"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance")
).filter(F.col("temp_bucket").isNotNull())

weather.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ADLS_SUMMARY}/weather_summary")
print(f"weather_summary: {weather.count():,} rows")

print("\n=== ALL SUMMARY TABLES COMPLETE ===")