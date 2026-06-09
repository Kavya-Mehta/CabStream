import json
import time
import pandas as pd
from confluent_kafka import Producer
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
KAFKA_TOPIC = 'taxi-trips'
DATA_FILE = 'data/yellow_tripdata_2023-01.parquet'
BATCH_SIZE = 1000  # rows per second (controls speed)

def delivery_report(err, msg):
    """Called once for each message to confirm delivery."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        pass  # silent on success to avoid flooding console

def serialize_trip(row):
    """Convert a DataFrame row to a JSON string."""
    trip = {
        'vendor_id': str(row.get('VendorID', '')),
        'pickup_datetime': str(row.get('tpep_pickup_datetime', '')),
        'dropoff_datetime': str(row.get('tpep_dropoff_datetime', '')),
        'passenger_count': float(row.get('passenger_count', 0) or 0),
        'trip_distance': float(row.get('trip_distance', 0) or 0),
        'pickup_location_id': int(row.get('PULocationID', 0) or 0),
        'dropoff_location_id': int(row.get('DOLocationID', 0) or 0),
        'fare_amount': float(row.get('fare_amount', 0) or 0),
        'tip_amount': float(row.get('tip_amount', 0) or 0),
        'total_amount': float(row.get('total_amount', 0) or 0),
        'payment_type': int(row.get('payment_type', 0) or 0),
        'ingestion_timestamp': datetime.utcnow().isoformat()
    }
    return json.dumps(trip)

def run_producer():
    print(f"Starting CabStream producer...")
    print(f"Reading data from: {DATA_FILE}")
    print(f"Publishing to topic: {KAFKA_TOPIC}")

    # Initialize Kafka producer
    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'cabstream-producer'
    })

    # Read Parquet file
    print("Loading Parquet file...")
    df = pd.read_parquet(DATA_FILE)
    total_rows = len(df)
    print(f"Loaded {total_rows:,} trips")

    # Publish each row to Kafka
    sent = 0
    failed = 0

    for idx, row in df.iterrows():
        try:
            message = serialize_trip(row)
            producer.produce(
                topic=KAFKA_TOPIC,
                value=message.encode('utf-8'),
                callback=delivery_report
            )
            sent += 1

            # Poll every 1000 messages to handle callbacks
            if sent % BATCH_SIZE == 0:
                producer.poll(0)
                print(f"Progress: {sent:,}/{total_rows:,} trips sent ({(sent/total_rows)*100:.1f}%)")
                time.sleep(0.1)  # small delay to simulate streaming

        except Exception as e:
            failed += 1
            print(f"Error on row {idx}: {e}")

    # Flush remaining messages
    print("Flushing remaining messages...")
    producer.flush()
    print(f"Done. Sent: {sent:,} | Failed: {failed:,}")

if __name__ == "__main__":
    run_producer()