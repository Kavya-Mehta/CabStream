# Databricks notebook source
import requests
import pandas as pd
from io import StringIO
from pyspark.sql import functions as F

ADLS_WEATHER = "abfss://raw@cabstreamdata.dfs.core.windows.net/weather/"
STATION_ID = "72505394728"
YEARS = list(range(2019, 2026))

def fetch_weather_year(year):
    url = "https://www.ncei.noaa.gov/access/services/data/v1"
    params = {
        "dataset": "global-hourly",
        "stations": STATION_ID,
        "startDate": f"{year}-01-01T00:00:00",
        "endDate": f"{year}-12-31T23:59:59",
        "format": "csv",
        "includeAttributes": "false",
        "units": "metric"
    }
    response = requests.get(url, params=params, timeout=120)
    print(f"Year {year}: status {response.status_code}")
    if response.status_code != 200:
        print(f"  Error: {response.text[:200]}")
        return None
    return response.text

success = []
failed = []

for year in YEARS:
    print(f"Fetching {year}...")
    csv_text = fetch_weather_year(year)
    if csv_text is None:
        failed.append(year)
        continue

    df = pd.read_csv(StringIO(csv_text), low_memory=False)
    print(f"  {year}: {len(df):,} rows")

    spark_df = spark.createDataFrame(df.astype(str))
    spark_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{ADLS_WEATHER}nyc_weather_{year}")

    success.append(year)
    print(f"  {year} written to ADLS ✓")

print(f"\n=== WEATHER INGESTION COMPLETE ===")
print(f"Success: {success}")
print(f"Failed: {failed}")

# COMMAND ----------

# Read one year to see raw column structure
raw_df = spark.read \
    .option("header", "true") \
    .csv(f"{ADLS_WEATHER}nyc_weather_2023")

print(f"Columns: {raw_df.columns}")
raw_df.show(3, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

weather_raw = spark.read \
    .option("header", "true") \
    .csv(f"{ADLS_WEATHER}nyc_weather_*")

weather_parsed = weather_raw \
    .select(
        F.col("DATE").alias("weather_datetime"),
        F.date_format("DATE", "yyyy-MM-dd").alias("pickup_date_str"),
        F.hour("DATE").alias("pickup_hour"),
        # Temperature
        F.when(
            F.get(F.split(F.col("TMP"), ","), 0).isin("+9999", "-9999"), None
        ).otherwise(
            F.get(F.split(F.col("TMP"), ","), 0).cast("double") / 10
        ).alias("weather_temp_c"),
        # Wind speed
        F.when(
            F.get(F.split(F.col("WND"), ","), 3) == "9999", None
        ).otherwise(
            F.round(F.get(F.split(F.col("WND"), ","), 3).cast("double") / 10 * 3.6, 1)
        ).alias("weather_wind_kmh"),
        # Precipitation
        F.when(
            F.col("AA1").isNull() | (F.get(F.split(F.col("AA1"), ","), 1) == "9999"), 0.0
        ).otherwise(
            F.coalesce(F.get(F.split(F.col("AA1"), ","), 1).cast("double") / 10, F.lit(0.0))
        ).alias("weather_precip_mm"),
    ) \
    .filter(F.col("weather_datetime").isNotNull())

print(f"Parsed weather rows: {weather_parsed.count():,}")
weather_parsed.show(10)

# COMMAND ----------

# Aggregate to one row per date+hour — take avg temp, max wind, sum precip
weather_hourly = weather_parsed \
    .groupBy("pickup_date_str", "pickup_hour") \
    .agg(
        F.round(F.avg("weather_temp_c"), 1).alias("weather_temp_c"),
        F.round(F.max("weather_wind_kmh"), 1).alias("weather_wind_kmh"),
        F.round(F.sum("weather_precip_mm"), 1).alias("weather_precip_mm")
    )

print(f"Hourly weather rows: {weather_hourly.count():,}")
weather_hourly.show(10)

# Write to ADLS as Delta
ADLS_WEATHER_SILVER = "abfss://delta@cabstreamdata.dfs.core.windows.net/silver/weather"

weather_hourly.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(ADLS_WEATHER_SILVER)

print(f"Weather Silver written: {ADLS_WEATHER_SILVER}")