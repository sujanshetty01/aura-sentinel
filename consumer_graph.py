from neo4j import GraphDatabase
from confluent_kafka import Consumer
import json

# Neo4j Setup
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password123"))

def add_flow(tx, src, dst, bytes_val):
    # This Cypher query creates IPs and connects them with a "SENT_DATA" relationship
    query = (
        "MERGE (a:IP {address: $src}) "
        "MERGE (b:IP {address: $dst}) "
        "MERGE (a)-[r:SENT_DATA]->(b) "
        "ON CREATE SET r.bytes = $bytes_val "
        "ON MATCH SET r.bytes = r.bytes + $bytes_val"
    )
    tx.run(query, src=src, dst=dst, bytes_val=bytes_val)

# Kafka Setup
conf = {'bootstrap.servers': "localhost:9092", 'group.id': "graph-mapper", 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['network-telemetry'])

print("🕸️  Graph Mapper is building the network map...")

try:
    with driver.session() as session:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                # Handle bytes value ensuring it defaults to 0 if missing or invalid
                bytes_val = data.get('bytes', 0)
                session.execute_write(add_flow, data['src_ip'], data['dst_ip'], bytes_val)
            except json.JSONDecodeError:
                print(f"Failed to decode message: {msg.value()}")
            except KeyError as e:
                print(f"Missing expected key in message: {e} - Data: {data}")
            except Exception as e:
                print(f"Error processing message: {e}")
            
except KeyboardInterrupt:
    print("Shutting down graph mapper...")
finally:
    consumer.close()
    driver.close()
