import pandas as pd
import json
import time
from confluent_kafka import Producer

# Configuration for local Kafka
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)

TOPIC = "network-telemetry"
DELAY_BETWEEN_EVENTS = 0.01  # seconds — simulate real-time flow ingestion

def delivery_report(err, msg):
    """Callback invoked once per message to confirm delivery."""
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ [{msg.topic()}] partition={msg.partition()} offset={msg.offset()}")

def stream_csv_to_kafka(file_path: str, topic_name: str):
    df = pd.read_csv(file_path)
    total = len(df)
    
    print(f"🚀 Starting stream: {total} records → topic '{topic_name}'")
    print(f"⏱  Simulated delay: {DELAY_BETWEEN_EVENTS}s per event (~{total * DELAY_BETWEEN_EVENTS:.0f}s total)\n")

    for index, row in df.iterrows():
        payload = row.to_dict()

        producer.produce(
            topic_name,
            key=str(payload['src_ip']),          # Partition key — group by source IP
            value=json.dumps(payload, default=str),  # Serialize to JSON
            callback=delivery_report
        )

        # Flush after every message to confirm delivery before simulating delay
        producer.flush()
        time.sleep(DELAY_BETWEEN_EVENTS)

        # Progress update every 100 records
        if (index + 1) % 100 == 0:
            print(f"📡 Progress: {index + 1}/{total} records sent ({((index+1)/total*100):.1f}%)")

    print(f"\n🏁 Stream complete. {total} records sent to '{topic_name}'.")

if __name__ == "__main__":
    stream_csv_to_kafka("network_flows.csv", TOPIC)
