# Databricks notebook source
ADLS_FHVHV_BRONZE = "abfss://delta@cabstreamdata.dfs.core.windows.net/bronze/fhvhv_trips"
ADLS_FHVHV_SILVER = "abfss://delta@cabstreamdata.dfs.core.windows.net/silver/fhvhv_trips"
ADLS_FHVHV_DEAD_LETTER = "abfss://delta@cabstreamdata.dfs.core.windows.net/dead_letter/fhvhv_trips"

print("Config ready")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_fhvhv = spark.read.format("delta").load(ADLS_FHVHV_BRONZE)
print(f"Bronze FHVHV row count: {bronze_fhvhv.count():,}")
bronze_fhvhv.printSchema()
bronze_fhvhv.show(5)

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

# ── 1. ADD TIME DIMENSIONS ───────────────────────────────────────────────────
df = bronze_fhvhv \
    .withColumn("pickup_hour", F.hour("pickup_datetime")) \
    .withColumn("pickup_month", F.month("pickup_datetime")) \
    .withColumn("pickup_year", F.year("pickup_datetime")) \
    .withColumn("pickup_dow", F.dayofweek("pickup_datetime")) \
    .withColumn("is_weekend", F.col("pickup_dow").isin([1, 7]).cast("boolean")) \
    .withColumn("is_rush_hour", (
        (F.col("pickup_dow").between(2, 6)) &
        (F.col("pickup_hour").between(7, 9) | F.col("pickup_hour").between(16, 19))
    ).cast("boolean")) \
    .withColumn("pickup_date_str", F.date_format("pickup_datetime", "yyyy-MM-dd"))

# ── 2. MAP COMPANY NAME ──────────────────────────────────────────────────────
df = df.withColumn("company", F.when(F.col("hvfhs_license_num") == "HV0002", "Juno")
                               .when(F.col("hvfhs_license_num") == "HV0003", "Uber")
                               .when(F.col("hvfhs_license_num") == "HV0004", "Via")
                               .when(F.col("hvfhs_license_num") == "HV0005", "Lyft")
                               .otherwise("Unknown"))

# ── 3. VALIDATE ───────────────────────────────────────────────────────────────
df = df \
    .withColumn("valid_fare", F.col("base_passenger_fare").between(0, 500)) \
    .withColumn("valid_miles", F.col("trip_miles").between(0, 200)) \
    .withColumn("valid_time", F.col("trip_time").between(0, 14400)) \
    .withColumn("valid_pickup", F.col("pickup_datetime").isNotNull()) \
    .withColumn("valid_dropoff", F.col("dropoff_datetime").isNotNull() & (F.col("dropoff_datetime") > F.lit("1971-01-01"))) \
    .withColumn("valid_locations", F.col("PULocationID").between(1, 265) & F.col("DOLocationID").between(1, 265)) \
    .withColumn("valid_driver_pay", F.col("driver_pay").between(0, 500))

all_checks = ["valid_fare", "valid_miles", "valid_time", "valid_pickup",
              "valid_dropoff", "valid_locations", "valid_driver_pay"]
is_valid = reduce(lambda a, b: a & b, [F.col(c) for c in all_checks])

good_fhvhv = df.filter(is_valid).drop(*all_checks)
bad_fhvhv = df.filter(~is_valid)

good_count = good_fhvhv.count()
bad_count = bad_fhvhv.count()
total = bronze_fhvhv.count()

print(f"Good records:              {good_count:,}")
print(f"Bad records (dead-letter): {bad_count:,}")
print(f"Pass rate:                 {good_count / total * 100:.1f}%")

# COMMAND ----------

from delta.tables import DeltaTable

ADLS_FHVHV_SILVER = "abfss://delta@cabstreamdata.dfs.core.windows.net/silver/fhvhv_trips"
ADLS_FHVHV_DEAD_LETTER = "abfss://delta@cabstreamdata.dfs.core.windows.net/dead_letter/fhvhv_trips"

YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]

for year in YEARS:
    print(f"\nProcessing year {year}...")
    
    year_df = good_fhvhv.filter(F.col("puYear") == year)
    year_count = year_df.count()
    print(f"  Records: {year_count:,}")
    
    year_df.dropDuplicates([
        "pickup_datetime", "PULocationID", "DOLocationID",
        "base_passenger_fare", "driver_pay"
    ]).write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("puYear", "puMonth") \
        .save(ADLS_FHVHV_SILVER)
    
    print(f"  Year {year} written to Silver ✓")

# Dead-letter
bad_fhvhv.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(ADLS_FHVHV_DEAD_LETTER)

print(f"\n=== FHVHV SILVER COMPLETE ===")
print(f"Dead-letter: {bad_fhvhv.count():,} records")

# COMMAND ----------

from delta.tables import DeltaTable

fhvhv_silver = DeltaTable.forPath(spark, ADLS_FHVHV_SILVER).toDF()
fhvhv_total = fhvhv_silver.count()
print(f"FHVHV Silver total rows:  {fhvhv_total:,}")

yellow_silver = DeltaTable.forPath(spark, "abfss://delta@cabstreamdata.dfs.core.windows.net/silver/taxi_trips").toDF()
yellow_total = yellow_silver.count()
print(f"Yellow Silver total rows: {yellow_total:,}")

print(f"Combined Silver total:    {fhvhv_total + yellow_total:,}")