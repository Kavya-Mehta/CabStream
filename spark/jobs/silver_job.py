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
    col, from_json, to_timestamp, round as spark_round,
    current_timestamp, lit, when, hour, dayofweek,
    month, year
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType
)

# ─── Configuration ─────────────────────────────────────────────────────────────
BRONZE_PATH      = "data/bronze/taxi_trips"
SILVER_PATH      = "data/silver/taxi_trips"
DEAD_LETTER_PATH = "data/dead_letter/taxi_trips"
OPENWEATHER_KEY  = os.getenv("OPENWEATHER_API_KEY")

# ─── Trip JSON Schema ──────────────────────────────────────────────────────────
# Why define schema explicitly:
# Spark can infer schema but it's slow and unreliable on JSON.
# Explicit schema = faster, predictable, catches upstream changes.
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
    """
    Create Spark session with Delta Lake support.
    No Kafka JAR needed here: Silver reads from Delta, not Kafka.
    """
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

    Why parse here not in Bronze:
    Bronze stores raw JSON so it never breaks if schema changes.
    Silver is where we trust the data enough to parse it.

    Why explicit casting:
    JSON values can be strings even if they look like numbers.
    Explicit cast ensures correct types in Silver.
    """
    # Step 1: Parse JSON string into struct
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

    # Step 2: Add time dimension columns
    # Why: Gold layer needs these for dim_time and time-based analysis
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

    # Step 3: Validation flags (6 categories, 14 checks total)
    # Why flags not immediate filter:
    # We want to capture WHY a record failed, not just drop it.
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

    # Step 4: Overall validity flag
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

    # Step 5: Split into good and bad
    good_df = validated.filter(col("is_valid") == True).drop(
        "valid_fare", "valid_distance", "valid_passengers",
        "valid_pickup", "valid_dropoff", "valid_locations",
        "valid_total", "is_valid"
    )

    # Dead-letter keeps all validation flags so we know what failed
    bad_df = validated.filter(col("is_valid") == False).withColumn(
        "failure_timestamp", current_timestamp()
    )

    return good_df, bad_df


def run_silver():
    print("=" * 50)
    print("CabStream Silver Job Starting...")
    print("=" * 50)

    # Validate API key loaded
    if not OPENWEATHER_KEY:
        print("WARNING: OPENWEATHER_API_KEY not found in .env")
        print("Weather join will be skipped")

    # Step 1: Create Spark session
    print("\n[1/5] Creating Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("      Spark session created")

    # Step 2: Read Bronze
    print("\n[2/5] Reading Bronze Delta table...")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"      Bronze rows: {bronze_count:,}")

    # Step 3: Parse and validate
    print("\n[3/5] Parsing JSON and validating...")
    good_df, bad_df = parse_and_validate(bronze_df)

    # Step 4: Deduplicate
    # Why these 4 columns as dedup key:
    # A unique trip = same pickup time + same pickup zone +
    #                 same dropoff zone + same fare
    # Two records matching all 4 = definitely the same trip
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

    # Step 5: Write Silver and Dead-letter
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
    else:
        print("      No bad records. Dead-letter table empty.")

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