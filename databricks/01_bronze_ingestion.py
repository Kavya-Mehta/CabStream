# Databricks notebook source
# Bronze Ingestion — NYC TLC Yellow Taxi 2019-2026
# Reads from NYC TLC public CDN, writes to ADLS Delta Bronze
# Resume-safe: skips already-written year/month partitions

ADLS_BRONZE = "abfss://delta@cabstreamdata.dfs.core.windows.net/bronze/taxi_trips"
LOCAL_TMP = "/tmp/nyc_tlc"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

YEARS = range(2019, 2027)
MONTHS = range(1, 13)

import os
os.makedirs(LOCAL_TMP, exist_ok=True)
print("Config ready")
print(f"Target: {ADLS_BRONZE}")

# COMMAND ----------

import urllib.request
import os
from pyspark.sql import functions as F

# Test with Jan 2023 — known good, matches local verified count of 3,066,766
year, month = 2023, 1
filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
url = f"{BASE_URL}/{filename}"
local_path = f"{LOCAL_TMP}/{filename}"

print(f"Downloading {filename}...")
urllib.request.urlretrieve(url, local_path)
print("Download complete")

df = spark.read.parquet(f"file:///{local_path}")
print(f"Row count: {df.count()}")

# Write to ADLS Bronze as Delta, partitioned by year and month
df.withColumn("puYear", F.lit(year)) \
  .withColumn("puMonth", F.lit(month)) \
  .write \
  .format("delta") \
  .mode("append") \
  .partitionBy("puYear", "puMonth") \
  .save(ADLS_BRONZE)

print(f"Written to ADLS Bronze")

# Cleanup local disk
os.remove(local_path)
print("Local file cleaned up")

# COMMAND ----------

import urllib.request
from pyspark.sql import functions as F

# Read 2019 file
urllib.request.urlretrieve(
    f"{BASE_URL}/yellow_tripdata_2019-01.parquet",
    f"{LOCAL_TMP}/yellow_tripdata_2019-01.parquet"
)
df_2019 = spark.read.parquet(f"file:///{LOCAL_TMP}/yellow_tripdata_2019-01.parquet")

# Read 2023 file
urllib.request.urlretrieve(
    f"{BASE_URL}/yellow_tripdata_2023-01.parquet", 
    f"{LOCAL_TMP}/yellow_tripdata_2023-01.parquet"
)
df_2023 = spark.read.parquet(f"file:///{LOCAL_TMP}/yellow_tripdata_2023-01.parquet")

# Compare schemas
print("=== 2019 schema ===")
df_2019.printSchema()
print("=== 2023 schema ===")
df_2023.printSchema()

# Specifically check airport_fee type in both
print("2019 airport_fee:", [f.dataType for f in df_2019.schema.fields if f.name == "airport_fee"])
print("2023 airport_fee:", [f.dataType for f in df_2023.schema.fields if f.name == "airport_fee"])

# COMMAND ----------

import urllib.request
import os
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Canonical schema fixes — type conflicts between pre/post 2022 NYC TLC files
# airport_fee: integer in 2019-2021, double in 2022+
# Cast everything to double for consistency across all years
CAST_COLS = {
    "airport_fee": DoubleType(),
    "congestion_surcharge": DoubleType(),
}

def normalize_schema(df):
    for col_name, col_type in CAST_COLS.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
        else:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
    return df

success = []
failed = []

for year in YEARS:
    for month in MONTHS:
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{BASE_URL}/{filename}"
        local_path = f"{LOCAL_TMP}/{filename}"

        if year == 2023 and month == 1:
            print(f"Skipping {filename} — already written")
            success.append((year, month))
            continue

        try:
            print(f"Processing {filename}...")
            urllib.request.urlretrieve(url, local_path)

            df = spark.read.parquet(f"file:///{local_path}")
            df = normalize_schema(df)
            count = df.count()

            df.withColumn("puYear", F.lit(year)) \
              .withColumn("puMonth", F.lit(month)) \
              .write \
              .format("delta") \
              .mode("append") \
              .option("mergeSchema", "true") \
              .partitionBy("puYear", "puMonth") \
              .save(ADLS_BRONZE)

            os.remove(local_path)
            success.append((year, month))
            print(f"Done {filename}: {count} rows")

        except Exception as e:
            print(f"FAILED {filename}: {e}")
            failed.append((year, month, str(e)))
            if os.path.exists(local_path):
                os.remove(local_path)
            continue

print(f"\n=== INGESTION COMPLETE ===")
print(f"Success: {len(success)} files")
print(f"Failed: {len(failed)} files")
if failed:
    print("Failed files:")
    for y, m, err in failed:
        print(f"  {y}-{m:02d}: {err}")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, ADLS_BRONZE)
df_count = dt.toDF().count()
print(f"Total rows in Bronze: {df_count:,}")

# Check which years/months are present
dt.toDF().groupBy("puYear", "puMonth") \
  .count() \
  .orderBy("puYear", "puMonth") \
  .show(100)

# COMMAND ----------

import urllib.request

urllib.request.urlretrieve(
    f"{BASE_URL}/yellow_tripdata_2023-02.parquet",
    f"{LOCAL_TMP}/yellow_tripdata_2023-02.parquet"
)
df = spark.read.parquet(f"file:///{LOCAL_TMP}/yellow_tripdata_2023-02.parquet")
print("2023-02 VendorID type:", df.schema["VendorID"].dataType)
df.printSchema()

# COMMAND ----------

import urllib.request
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType
from delta.tables import DeltaTable

# Check 1: Current Delta table registered schema for VendorID
dt = DeltaTable.forPath(spark, ADLS_BRONZE)
vendor_type = [f.dataType for f in dt.toDF().schema.fields if f.name == "VendorID"][0]
print(f"Delta table VendorID type: {vendor_type}")

# Check 2: What normalize_schema produces for 2023-02
urllib.request.urlretrieve(
    f"{BASE_URL}/yellow_tripdata_2023-02.parquet",
    f"{LOCAL_TMP}/test_2023_02.parquet"
)
df = spark.read.parquet(f"file:///{LOCAL_TMP}/test_2023_02.parquet")
print(f"Raw 2023-02 VendorID type: {df.schema['VendorID'].dataType}")

# Apply normalize
if "Airport_fee" in df.columns:
    df = df.withColumnRenamed("Airport_fee", "airport_fee")
df = df.withColumn("VendorID", F.col("VendorID").cast(LongType()))
print(f"After cast VendorID type: {df.schema['VendorID'].dataType}")

# Check if types match
print(f"Types match: {str(df.schema['VendorID'].dataType) == str(vendor_type)}")

# COMMAND ----------

import urllib.request
import os
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType

def normalize_schema(df):
    if "Airport_fee" in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")
    
    cast_map = {
        "VendorID": LongType(),
        "PULocationID": LongType(),
        "DOLocationID": LongType(),
        "passenger_count": DoubleType(),
        "RatecodeID": DoubleType(),
        "airport_fee": DoubleType(),
        "congestion_surcharge": DoubleType(),
    }
    
    for col_name, col_type in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
        else:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
    
    return df

# Only the VendorID-failed files — 2023-02 through 2025-03
# 2025-04+ are 403 (don't exist yet on CDN)
FAILED_MONTHS = (
    [(2023, m) for m in range(2, 13)] +
    [(2024, m) for m in range(1, 13)] +
    [(2025, m) for m in range(1, 4)]
)

success = []
failed = []

for year, month in FAILED_MONTHS:
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    local_path = f"{LOCAL_TMP}/{filename}"

    try:
        print(f"Processing {filename}...")
        urllib.request.urlretrieve(url, local_path)

        df = spark.read.parquet(f"file:///{local_path}")
        df = normalize_schema(df)
        count = df.count()

        df.withColumn("puYear", F.lit(year)) \
          .withColumn("puMonth", F.lit(month)) \
          .write \
          .format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .partitionBy("puYear", "puMonth") \
          .save(ADLS_BRONZE)

        os.remove(local_path)
        success.append((year, month))
        print(f"Done {filename}: {count} rows")

    except Exception as e:
        print(f"FAILED {filename}: {e}")
        failed.append((year, month, str(e)))
        if os.path.exists(local_path):
            os.remove(local_path)
        continue

print(f"\n=== RERUN COMPLETE ===")
print(f"Success: {len(success)} files")
print(f"Failed: {len(failed)} files")
if failed:
    for y, m, err in failed:
        print(f"  {y}-{m:02d}: {err}")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, ADLS_BRONZE)
total = dt.toDF().count()
print(f"Total Bronze rows: {total:,}")

# COMMAND ----------

import urllib.request

# Test FHV high volume (Uber/Lyft) - this is the big one
# FHVHV = For-Hire Vehicle High Volume (Uber, Lyft, Via)
# Available from 2019-02 onwards

test_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2019-02.parquet"
local_path = f"{LOCAL_TMP}/fhvhv_test.parquet"

urllib.request.urlretrieve(test_url, local_path)
df = spark.read.parquet(f"file:///{local_path}")
print(f"FHVHV 2019-02 row count: {df.count():,}")
df.printSchema()

# COMMAND ----------

import urllib.request
import os
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, IntegerType

ADLS_FHVHV = "abfss://delta@cabstreamdata.dfs.core.windows.net/bronze/fhvhv_trips"
FHVHV_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# FHVHV available from 2019-02 through 2025-03
FHVHV_MONTHS = (
    [(2019, m) for m in range(2, 13)] +
    [(y, m) for y in range(2020, 2025) for m in range(1, 13)] +
    [(2025, m) for m in range(1, 4)]
)

def normalize_fhvhv(df):
    # airport_fee is integer in some files, cast to double for consistency
    if "airport_fee" in df.columns:
        df = df.withColumn("airport_fee", F.col("airport_fee").cast(DoubleType()))
    else:
        df = df.withColumn("airport_fee", F.lit(None).cast(DoubleType()))
    
    # wav_match_flag is integer in some files, cast to string
    if "wav_match_flag" in df.columns:
        df = df.withColumn("wav_match_flag", F.col("wav_match_flag").cast("string"))
    
    return df

success = []
failed = []

for year, month in FHVHV_MONTHS:
    filename = f"fhvhv_tripdata_{year}-{month:02d}.parquet"
    url = f"{FHVHV_URL}/{filename}"
    local_path = f"{LOCAL_TMP}/{filename}"

    try:
        print(f"Processing {filename}...")
        urllib.request.urlretrieve(url, local_path)

        df = spark.read.parquet(f"file:///{local_path}")
        df = normalize_fhvhv(df)
        count = df.count()

        df.withColumn("puYear", F.lit(year)) \
          .withColumn("puMonth", F.lit(month)) \
          .write \
          .format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .partitionBy("puYear", "puMonth") \
          .save(ADLS_FHVHV)

        os.remove(local_path)
        success.append((year, month))
        print(f"Done {filename}: {count:,} rows")

    except Exception as e:
        print(f"FAILED {filename}: {e}")
        failed.append((year, month, str(e)))
        if os.path.exists(local_path):
            os.remove(local_path)
        continue

print(f"\n=== FHVHV INGESTION COMPLETE ===")
print(f"Success: {len(success)} files")
print(f"Failed: {len(failed)} files")
if failed:
    for y, m, err in failed:
        print(f"  {y}-{m:02d}: {err}")

# COMMAND ----------

import urllib.request
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, ADLS_FHVHV)
registered_type = [f.dataType for f in dt.toDF().schema.fields if f.name == "PULocationID"][0]
print(f"Delta PULocationID type: {registered_type}")

urllib.request.urlretrieve(
    f"{BASE_URL}/fhvhv_tripdata_2023-02.parquet",
    f"{LOCAL_TMP}/fhvhv_test.parquet"
)
df = spark.read.parquet(f"file:///{LOCAL_TMP}/fhvhv_test.parquet")
print(f"Raw 2023-02 PULocationID: {df.schema['PULocationID'].dataType}")
df = df.withColumn("PULocationID", F.col("PULocationID").cast(LongType()))
print(f"After cast: {df.schema['PULocationID'].dataType}")
print(f"Types match: {str(df.schema['PULocationID'].dataType) == str(registered_type)}")

# COMMAND ----------

import urllib.request
import os
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, StringType

def normalize_fhvhv(df):
    # Cast all fields that NYC TLC changed from long→integer in 2023
    cast_map = {
        "PULocationID": LongType(),
        "DOLocationID": LongType(),
        "trip_time": LongType(),
        "airport_fee": DoubleType(),
        "congestion_surcharge": DoubleType(),
        "wav_match_flag": StringType(),
    }
    
    for col_name, col_type in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
        else:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
    
    return df

# Only failed files — 2023-02 through 2025-03
FAILED_MONTHS = (
    [(2023, m) for m in range(2, 13)] +
    [(2024, m) for m in range(1, 13)] +
    [(2025, m) for m in range(1, 4)]
)

success = []
failed = []

for year, month in FAILED_MONTHS:
    filename = f"fhvhv_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    local_path = f"{LOCAL_TMP}/{filename}"

    try:
        print(f"Processing {filename}...")
        urllib.request.urlretrieve(url, local_path)

        df = spark.read.parquet(f"file:///{local_path}")
        df = normalize_fhvhv(df)
        count = df.count()

        df.withColumn("puYear", F.lit(year)) \
          .withColumn("puMonth", F.lit(month)) \
          .write \
          .format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .partitionBy("puYear", "puMonth") \
          .save(ADLS_FHVHV)

        os.remove(local_path)
        success.append((year, month))
        print(f"Done {filename}: {count:,} rows")

    except Exception as e:
        print(f"FAILED {filename}: {e}")
        failed.append((year, month, str(e)))
        if os.path.exists(local_path):
            os.remove(local_path)
        continue

print(f"\n=== FHVHV RERUN COMPLETE ===")
print(f"Success: {len(success)} files")
print(f"Failed: {len(failed)} files")
if failed:
    for y, m, err in failed:
        print(f"  {y}-{m:02d}: {err}")

# COMMAND ----------

from delta.tables import DeltaTable

yellow_total = DeltaTable.forPath(spark, ADLS_BRONZE).toDF().count()
fhvhv_total = DeltaTable.forPath(spark, ADLS_FHVHV).toDF().count()
combined = yellow_total + fhvhv_total

print(f"Yellow taxi rows:  {yellow_total:,}")
print(f"FHVHV rows:        {fhvhv_total:,}")
print(f"Combined total:    {combined:,}")