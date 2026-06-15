import os
from dotenv import load_dotenv

load_dotenv()

os.environ['JAVA_HOME'] = 'C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.10.7-hotspot'
os.environ['PATH'] = os.environ['JAVA_HOME'] + '\\bin;' + os.environ['PATH']
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, date_format,
    round as spark_round, current_timestamp, when,
    hour, dayofweek, month, year
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# ─── Configuration ─────────────────────────────────────────────────────────────
BRONZE_PATH      = "data/bronze/taxi_trips"
SILVER_PATH      = "data/silver/taxi_trips"
DEAD_LETTER_PATH = "data/dead_letter/taxi_trips"
WEATHER_PATH     = "data/weather/nyc_weather_2023.csv"

# ─── Trip JSON Schema ──────────────────────────────────────────────────────────
TRIP_SCHEMA = StructType([
    StructField("vendor_id",           StringType(),  True),
    StructField("pickup_datetime",     StringType(),  True),
    StructField("dropoff_datetime",    StringType(),  True),
    StructField("passenger_count",     DoubleType(),  True),
    StructField("trip_distance",       DoubleType(),  True),
    StructField("pickup_location_id",  IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("fare_amount",         DoubleType(),  True),
    StructField("tip_amount",          DoubleType(),  True),
    StructField("total_amount",        DoubleType(),  True),
    StructField("payment_type",        IntegerType(), True),
    StructField("ingestion_timestamp", StringType(),  True),
])


def create_spark_session():
    spark = SparkSession.builder \
        .appName("CabStream-Silver") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.3.1") \
        .master("local[*]") \
        .getOrCreate()
    return spark


def parse_and_validate(df):
    """
    Parse raw_json string into typed columns.
    Add time dimensions and validation flags.
    Returns (good_df, bad_df)
    """
    parsed = df.select(
        from_json(col("raw_json"), TRIP_SCHEMA).alias("trip"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("ingestion_timestamp")
    ).select(
        col("trip.vendor_id"),
        to_timestamp(
            col("trip.pickup_datetime"), "yyyy-MM-dd HH:mm:ss"
        ).alias("pickup_datetime"),
        to_timestamp(
            col("trip.dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"
        ).alias("dropoff_datetime"),
        col("trip.passenger_count").cast(IntegerType()).alias("passenger_count"),
        col("trip.trip_distance").cast(DoubleType()).alias("trip_distance"),
        col("trip.pickup_location_id").cast(IntegerType()).alias("pickup_location_id"),
        col("trip.dropoff_location_id").cast(IntegerType()).alias("dropoff_location_id"),
        spark_round(col("trip.fare_amount").cast(DoubleType()), 2).alias("fare_amount"),
        spark_round(col("trip.tip_amount").cast(DoubleType()), 2).alias("tip_amount"),
        spark_round(col("trip.total_amount").cast(DoubleType()), 2).alias("total_amount"),
        col("trip.payment_type").cast(IntegerType()).alias("payment_type"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("ingestion_timestamp"),
        current_timestamp().alias("silver_timestamp")
    )

    parsed = parsed \
        .withColumn("pickup_hour",  hour(col("pickup_datetime"))) \
        .withColumn("pickup_month", month(col("pickup_datetime"))) \
        .withColumn("pickup_year",  year(col("pickup_datetime"))) \
        .withColumn("pickup_dow",   dayofweek(col("pickup_datetime"))) \
        .withColumn("is_weekend",
            when(col("pickup_dow").isin(1, 7), True).otherwise(False)
        ) \
        .withColumn("is_rush_hour",
            when(
                (col("pickup_hour").between(7, 9)) |
                (col("pickup_hour").between(17, 19)), True
            ).otherwise(False)
        )

    validated = parsed \
        .withColumn("valid_fare",
            col("fare_amount").isNotNull() &
            col("fare_amount").between(0, 500)
        ) \
        .withColumn("valid_distance",
            col("trip_distance").isNotNull() &
            col("trip_distance").between(0, 100)
        ) \
        .withColumn("valid_passengers",
            col("passenger_count").isNotNull() &
            col("passenger_count").between(1, 6)
        ) \
        .withColumn("valid_pickup",
            col("pickup_datetime").isNotNull()
        ) \
        .withColumn("valid_dropoff",
            col("dropoff_datetime").isNotNull()
        ) \
        .withColumn("valid_locations",
            col("pickup_location_id").isNotNull() &
            col("pickup_location_id").between(1, 265) &
            col("dropoff_location_id").isNotNull() &
            col("dropoff_location_id").between(1, 265)
        ) \
        .withColumn("valid_total",
            col("total_amount").isNotNull() &
            col("total_amount").between(0, 1000)
        )

    validated = validated.withColumn(
        "is_valid",
        col("valid_fare") &
        col("valid_distance") &
        col("valid_passengers") &
        col("valid_pickup") &
        col("valid_dropoff") &
        col("valid_locations") &
        col("valid_total")
    )

    good_df = validated.filter(col("is_valid") == True).drop(
        "valid_fare", "valid_distance", "valid_passengers",
        "valid_pickup", "valid_dropoff", "valid_locations",
        "valid_total", "is_valid"
    )

    bad_df = validated.filter(col("is_valid") == False).withColumn(
        "failure_timestamp", current_timestamp()
    )

    return good_df, bad_df


def add_weather(df, weather_path, spark):
    """
    Join hourly NOAA weather data to trips on date + hour.

    Why left join:
    Keeps all trips even if no weather match for that hour.
    Better than inner join which would silently drop trips.

    Why join at Silver:
    Weather is enrichment, not a dimension model.
    Silver is the correct layer for data enrichment.
    Gold builds star schema on top of enriched Silver.
    """
    weather_df = spark.read.csv(
        weather_path,
        header=True,
        inferSchema=True
    )

    trips_keyed = df.withColumn(
        "pickup_date_str",
        date_format(col("pickup_datetime"), "yyyy-MM-dd")
    )

    joined = trips_keyed.join(
        weather_df,
        (trips_keyed["pickup_date_str"] == weather_df["date"]) &
        (trips_keyed["pickup_hour"]     == weather_df["hour"]),
        how="left"
    ).drop("date", "pickup_date_str")

    joined = joined \
        .withColumnRenamed("temperature",   "weather_temp_c") \
        .withColumnRenamed("precipitation", "weather_precip_mm") \
        .withColumnRenamed("wind_speed",    "weather_wind_kmh") \
        .withColumnRenamed("condition",     "weather_condition")

    return joined


def run_silver():
    print("=" * 50)
    print("CabStream Silver Job Starting...")
    print("=" * 50)

    print("\n[1/5] Creating Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("      Spark session created")

    print("\n[2/5] Reading Bronze Delta table...")
    bronze_df    = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"      Bronze rows: {bronze_count:,}")

    print("\n[3/5] Parsing JSON and validating...")
    good_df, bad_df = parse_and_validate(bronze_df)

    print("\n[4/5] Deduplicating on business key...")
    dedup_df = good_df.dropDuplicates([
        "pickup_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount"
    ])

    good_count = dedup_df.count()
    bad_count  = bad_df.count()
    total      = good_count + bad_count
    pass_rate  = (good_count / total * 100) if total > 0 else 0

    print(f"      Good records : {good_count:,}")
    print(f"      Bad records  : {bad_count:,}")
    print(f"      Pass rate    : {pass_rate:.1f}%")

    print("\n[4.5/5] Joining NOAA weather data...")
    if os.path.exists(WEATHER_PATH):
        dedup_df = add_weather(dedup_df, WEATHER_PATH, spark)
        weather_matched = dedup_df.filter(
            col("weather_temp_c").isNotNull()
        ).count()
        weather_pct = (weather_matched / good_count * 100) if good_count > 0 else 0
        print(f"      Weather matched: {weather_matched:,} ({weather_pct:.1f}%)")
    else:
        print(f"      WARNING: {WEATHER_PATH} not found. Skipping weather join.")

    print("\n[5/5] Writing to Silver Delta table...")
    dedup_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_PATH)
    print(f"      Silver table written: {good_count:,} rows")

    if bad_count > 0:
        print(f"      Writing {bad_count:,} records to dead-letter...")
        bad_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(DEAD_LETTER_PATH)
        print(f"      Dead-letter table written: {bad_count:,} rows")

    print("\n" + "=" * 50)
    print("Silver job complete")
    print(f"Bronze rows:  {bronze_count:,}")
    print(f"Silver rows:  {good_count:,}")
    print(f"Dead-letter:  {bad_count:,}")
    print(f"Pass rate:    {pass_rate:.1f}%")
    print("=" * 50)

    spark.stop()


if __name__ == "__main__":
    run_silver()