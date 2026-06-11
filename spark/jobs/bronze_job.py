import os

# Set environment variables BEFORE importing PySpark
# Why: PySpark needs Java and Hadoop on Windows before JVM starts
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.10.7-hotspot'
os.environ['PATH'] = os.environ['JAVA_HOME'] + '\\bin;' + os.environ['PATH']
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
KAFKA_TOPIC             = "taxi-trips"
BRONZE_PATH             = "data/bronze/taxi_trips"
CHECKPOINT_PATH         = "data/checkpoints/bronze"

# ─── Version compatibility matrix (IMPORTANT) ─────────────────────────────────
# PySpark 3.5.3 → Spark 3.5.3 → Scala 2.12
# Delta 3.3.1   → compatible with Spark 3.5.3
# Kafka JAR     → spark-sql-kafka-0-10_2.12:3.5.3
# All _2.12 suffix because Spark 3.5.x uses Scala 2.12
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session():
    """
    Create Spark session with Delta Lake and Kafka support.
    
    Version matrix used here:
    - PySpark 3.5.3 (Scala 2.12)
    - delta-spark_2.12:3.3.1 (matches Spark 3.5.x)
    - spark-sql-kafka-0-10_2.12:3.5.3 (matches Spark 3.5.3)
    
    Why these specific versions:
    PySpark 3.5.3 + Delta 3.3.1 is the current production
    standard. Most companies run this exact combination.
    Azure Databricks uses it internally.
    """
    spark = SparkSession.builder \
        .appName("CabStream-Bronze") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "io.delta:delta-spark_2.12:3.3.1") \
        .config("spark.driver.extraJavaOptions",
                "-Dderby.system.home=/tmp/derby") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .master("local[*]") \
        .getOrCreate()

    return spark


def read_from_kafka(spark):
    """
    Read streaming data from Kafka topic.
    
    Why readStream:
    Creates an infinite streaming DataFrame.
    Treats Kafka topic as a table that keeps growing.
    Each micro-batch reads NEW messages since last checkpoint.
    
    Why startingOffsets earliest:
    Read all messages from beginning of topic.
    
    Why failOnDataLoss false:
    If Kafka deletes old messages, don't crash.
    """
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()


def transform_bronze(df):
    """
    Minimal transformation for Bronze layer.
    
    Why store raw_json as string:
    Bronze stores EXACTLY what arrived from Kafka.
    No parsing here. Silver handles parsing.
    If upstream schema changes, Bronze NEVER breaks.
    
    Metadata stored:
    - raw_json: entire trip as JSON string
    - kafka_partition: which partition it came from
    - kafka_offset: exact position in Kafka (resume point)
    - kafka_timestamp: when Kafka received it
    - ingestion_timestamp: when Spark processed it
    """
    return df.select(
        col("value").cast("string").alias("raw_json"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp")
    )


def write_to_bronze(df, checkpoint_path, bronze_path):
    """
    Write to Bronze Delta table using micro-batch streaming.
    
    Why Delta format:
    ACID transactions, schema enforcement, time travel.
    
    Why append mode:
    Bronze is immutable. Only add, never update/delete.
    
    Why checkpointLocation:
    Saves Spark progress after each batch.
    Crash recovery = resume from checkpoint = exactly-once.
    
    Why 30 second trigger:
    Spark wakes every 30 seconds, reads new Kafka messages,
    writes to Delta, saves checkpoint, sleeps, repeats.
    """
    return df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="30 seconds") \
        .start(bronze_path)


def main():
    print("=" * 50)
    print("CabStream Bronze Job Starting...")
    print("=" * 50)
    print(f"Kafka topic   : {KAFKA_TOPIC}")
    print(f"Kafka servers : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Bronze path   : {BRONZE_PATH}")
    print(f"Checkpoint    : {CHECKPOINT_PATH}")
    print("=" * 50)

    # Step 1: Create Spark session
    print("\n[1/4] Creating Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("      Spark session created successfully")

    # Step 2: Connect to Kafka stream
    print("\n[2/4] Connecting to Kafka stream...")
    raw_df = read_from_kafka(spark)
    print("      Connected to Kafka stream")

    # Step 3: Apply Bronze transformation
    print("\n[3/4] Applying Bronze transformation...")
    bronze_df = transform_bronze(raw_df)
    print("      Transformation defined")

    # Step 4: Start writing to Bronze Delta table
    print("\n[4/4] Starting Bronze streaming query...")
    query = write_to_bronze(bronze_df, CHECKPOINT_PATH, BRONZE_PATH)
    print("      Streaming query started")
    print("\n" + "=" * 50)
    print("Bronze job is RUNNING")
    print("Processing a new micro-batch every 30 seconds")
    print(f"Data landing in: {BRONZE_PATH}")
    print("Press Ctrl+C to stop")
    print("=" * 50 + "\n")

    # Keep job alive
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[STOP] Stopping Bronze job gracefully...")
        query.stop()
        spark.stop()
        print("[DONE] Bronze job stopped cleanly")


if __name__ == "__main__":
    main()