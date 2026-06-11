import os
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.10.7-hotspot'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['JAVA_HOME'] + '\\bin;' + os.environ['PATH'] + ';C:\\hadoop\\bin'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CabStream-Verify-Bronze") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 50)
print("Verifying Bronze Delta Table...")
print("=" * 50)

df = spark.read.format("delta").load("data/bronze/taxi_trips")
count = df.count()
print(f"Bronze table row count: {count:,}")
print(f"\nSample records:")
df.show(3, truncate=80)
print(f"\nSchema:")
df.printSchema()
print("=" * 50)

spark.stop()