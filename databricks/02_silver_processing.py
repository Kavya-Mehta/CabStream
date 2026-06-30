# Databricks notebook source
# Silver Processing — NYC Yellow Taxi
# Reads from Bronze ADLS Delta, validates, deduplicates, writes Silver Delta

ADLS_BRONZE = "abfss://delta@cabstreamdata.dfs.core.windows.net/bronze/taxi_trips"
ADLS_SILVER = "abfss://delta@cabstreamdata.dfs.core.windows.net/silver/taxi_trips"

print("Config ready")
print(f"Bronze: {ADLS_BRONZE}")
print(f"Silver: {ADLS_SILVER}")

# COMMAND ----------

from pyspark.sql import functions as F

# Read Bronze Delta
bronze_df = spark.read.format("delta").load(ADLS_BRONZE)

print(f"Bronze row count: {bronze_df.count():,}")
print(f"Bronze schema:")
bronze_df.printSchema()
bronze_df.show(5)

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

# ── 1. READ BRONZE ───────────────────────────────────────────────────────────
bronze_df = spark.read.format("delta").load(ADLS_BRONZE)

# ── 2. ADD TIME DIMENSIONS ───────────────────────────────────────────────────
df = bronze_df \
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
    .withColumn("pickup_month", F.month("tpep_pickup_datetime")) \
    .withColumn("pickup_year", F.year("tpep_pickup_datetime")) \
    .withColumn("pickup_dow", F.dayofweek("tpep_pickup_datetime")) \
    .withColumn("is_weekend", F.col("pickup_dow").isin([1, 7]).cast("boolean")) \
    .withColumn("is_rush_hour", (
        (F.col("pickup_dow").between(2, 6)) &
        (F.col("pickup_hour").between(7, 9) | F.col("pickup_hour").between(16, 19))
    ).cast("boolean")) \
    .withColumn("pickup_date_str", F.date_format("tpep_pickup_datetime", "yyyy-MM-dd"))

# ── 3. VALIDATE ──────────────────────────────────────────────────────────────
df = df \
    .withColumn("valid_fare", F.col("fare_amount").between(0, 500)) \
    .withColumn("valid_distance", F.col("trip_distance").between(0, 100)) \
    .withColumn("valid_passengers", F.col("passenger_count").between(1, 6)) \
    .withColumn("valid_pickup", F.col("tpep_pickup_datetime").isNotNull()) \
    .withColumn("valid_dropoff", F.col("tpep_dropoff_datetime").isNotNull()) \
    .withColumn("valid_locations", F.col("PULocationID").between(1, 265) & F.col("DOLocationID").between(1, 265)) \
    .withColumn("valid_total", F.col("total_amount").between(0, 1000))

all_checks = ["valid_fare", "valid_distance", "valid_passengers",
              "valid_pickup", "valid_dropoff", "valid_locations", "valid_total"]
is_valid = reduce(lambda a, b: a & b, [F.col(c) for c in all_checks])

good_df = df.filter(is_valid).drop(*all_checks)
bad_df = df.filter(~is_valid)

good_count = good_df.count()
bad_count = bad_df.count()
total = bronze_df.count()

print(f"Good records:              {good_count:,}")
print(f"Bad records (dead-letter): {bad_count:,}")
print(f"Pass rate:                 {good_count / total * 100:.1f}%")

# COMMAND ----------

# ── 4. DEDUPLICATE ───────────────────────────────────────────────────────────
dedup_keys = ["tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount"]

before_dedup = good_df.count()
good_df = good_df.dropDuplicates(dedup_keys)
after_dedup = good_df.count()

print(f"Before dedup: {before_dedup:,}")
print(f"After dedup:  {after_dedup:,}")
print(f"Duplicates removed: {before_dedup - after_dedup:,}")

# ── 5. WRITE SILVER ───────────────────────────────────────────────────────────
good_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("puYear", "puMonth") \
    .save(ADLS_SILVER)

print(f"\nSilver written: {ADLS_SILVER}")
print(f"Final Silver row count: {after_dedup:,}")

# ── 6. WRITE DEAD-LETTER ──────────────────────────────────────────────────────
ADLS_DEAD_LETTER = "abfss://delta@cabstreamdata.dfs.core.windows.net/dead_letter/taxi_trips"

bad_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(ADLS_DEAD_LETTER)

print(f"Dead-letter written: {ADLS_DEAD_LETTER}")
print(f"Dead-letter row count: {bad_count:,}")