import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

def generate_netflow_data(num_records=1000):
    data = []
    # Known "Malicious" IP for our ground truth labeling
    c2_server = "192.168.100.50" 
    
    for _ in range(num_records):
        is_malicious = random.random() < 0.05  # 5% chance of being a threat
        
        if is_malicious:
            # Pattern: Small packets, consistent intervals (C2 Heartbeat)
            # Use a small pool of infected IPs so they repeat and we can track their history
            src_ip = random.choice(["10.0.0.15", "10.0.0.22", "192.168.1.100", "172.16.0.50"])
            dst_ip = c2_server
            bytes_transferred = 88 # Constant tiny "heartbeat" payload
            packets = 1
            duration = 0.001
        else:
            # Pattern: Normal web/cloud traffic
            src_ip = fake.ipv4_public()
            dst_ip = fake.ipv4_public()
            bytes_transferred = random.randint(1000, 500000)
            packets = random.randint(10, 100)
            duration = random.uniform(0.1, 5.0)

        data.append({
            "timestamp": datetime.now() - timedelta(seconds=random.randint(0, 3600)),
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "src_port": random.randint(1024, 65535),
            "dst_port": 443 if not is_malicious else 8080,
            "protocol": 6, # TCP
            "bytes": bytes_transferred,
            "packets": packets,
            "duration": duration,
            "label": 1 if is_malicious else 0
        })
    
    return pd.DataFrame(data).sort_values("timestamp")

# Generate and save
df = generate_netflow_data(5000)
df.to_csv("network_flows.csv", index=False)
print("✅ Phase 1 Complete: 5,000 Flow records generated in network_flows.csv")
