import json
import numpy as np
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "aura-sentinel-v1",
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['network-telemetry'])

# In-memory storage to track patterns per IP
ip_history = defaultdict(list)

# Stats counters
total_messages   = 0
total_alerts     = 0
total_clean      = 0

def calculate_entropy(list_of_values):
    """
    Calculates Shannon Entropy to measure randomness of traffic patterns.
    - High entropy  → random, normal traffic
    - Low entropy   → repetitive, beaconing (C2/botnet) behaviour
    """
    if len(list_of_values) < 2:
        return 0.0
    values = np.array(list_of_values)
    _, counts = np.unique(values, return_counts=True)
    probs = counts / len(values)
    return float(-np.sum(probs * np.log2(probs + 1e-12)))  # +epsilon avoids log(0)

def format_ip_verdict(src_ip, entropy, history):
    """Produce a human-readable alert block."""
    byte_sizes = ", ".join(str(b) for b in history)
    return (
        f"\n{'='*60}\n"
        f"  ⚠️  ALERT  —  C2 Beaconing Suspected\n"
        f"{'='*60}\n"
        f"  Source IP  : {src_ip}\n"
        f"  Entropy    : {entropy:.4f}  (threshold < 0.5)\n"
        f"  Reason     : Repeated small payloads — suspected heartbeat\n"
        f"  Samples    : [{byte_sizes}]\n"
        f"{'='*60}\n"
    )

print("=" * 60)
print("  📡  Aura-Sentinel  |  Intelligence Layer  v1.0")
print("  Listening on topic: network-telemetry")
print("  Detection: Shannon Entropy  |  Window: 10 msgs/IP")
print("=" * 60)
print()

WINDOW_SIZE   = 10     # samples before evaluating
ENTROPY_ALERT = 0.5   # below this → suspicious

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"  [EOF] Reached end of partition {msg.partition()}")
            else:
                print(f"  [ERROR] {msg.error()}")
            continue

        # ── Parse message ────────────────────────────────────────
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"  [WARN] Failed to decode message: {e}")
            continue

        src_ip = data.get('src_ip', 'unknown')
        bytes_val = data.get('bytes', 0)
        label = data.get('label', 0)          # ground-truth (for verification)

        total_messages += 1
        ip_history[src_ip].append(bytes_val)

        # ── Evaluate window ──────────────────────────────────────
        if len(ip_history[src_ip]) >= WINDOW_SIZE:
            window   = ip_history[src_ip][-WINDOW_SIZE:]
            entropy  = calculate_entropy(window)

            if entropy < ENTROPY_ALERT:
                total_alerts += 1
                ground_truth = "✅ TRUE POSITIVE" if label == 1 else "⚠️  FALSE POSITIVE"
                print(format_ip_verdict(src_ip, entropy, window))
                print(f"  Ground-truth verification: {ground_truth}")
                print()
            else:
                total_clean += 1

            # Reset window — keeps memory bounded (production: use Redis TTL)
            ip_history[src_ip] = []

        # ── Progress ticker every 500 messages ───────────────────
        if total_messages % 500 == 0:
            print(
                f"  📊  [{total_messages:>5} msgs]  "
                f"Alerts: {total_alerts}  |  "
                f"Clean IPs evaluated: {total_clean}  |  "
                f"IPs tracked: {len(ip_history)}"
            )

except KeyboardInterrupt:
    print("\n\n  🛑  Shutting down gracefully...\n")

finally:
    consumer.close()
    print("─" * 60)
    print(f"  📋  SESSION SUMMARY")
    print(f"  Total messages consumed : {total_messages}")
    print(f"  Total alerts fired      : {total_alerts}")
    print(f"  Clean windows evaluated : {total_clean}")
    print(f"  Unique IPs tracked      : {len(ip_history)}")
    print("─" * 60)
