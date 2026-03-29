"""
╔══════════════════════════════════════════════════════════════╗
║    📡  AURA-SENTINEL  |  Prometheus Metrics Exporter  v1.0  ║
║    AIOps Dashboard — Real-time Entropy & C2 Signal Feed      ║
╚══════════════════════════════════════════════════════════════╝

Runs an HTTP server on :8000 that Prometheus scrapes.
Exposes:
  aura_entropy_score{src_ip}        — Shannon entropy per IP window
  aura_alert_total                  — total C2 alerts fired
  aura_messages_consumed_total      — total Kafka messages processed
  aura_c2_hubs_detected_total       — C2 hubs identified by remediation
  aura_bytes_total{src_ip, dst_ip}  — byte volume per flow pair
  aura_active_tracked_ips           — IPs currently in entropy window

Run alongside consumer_intelligence.py:
    python3.10 metrics_exporter.py &
    python3.10 consumer_intelligence.py
"""

import json
import math
import time
import threading
import os
from collections import defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler

from confluent_kafka import Consumer, KafkaError

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC      = "network-telemetry"
KAFKA_GROUP      = "aura-prometheus-exporter"
METRICS_PORT     = 8000
WINDOW_SIZE      = 10
ENTROPY_THRESHOLD = 0.5

# ── In-memory metric stores ────────────────────────────────────────────────────
ip_history        = defaultdict(list)          # src_ip -> list of bytes
entropy_scores    = {}                          # src_ip -> latest entropy
byte_counters     = defaultdict(int)            # (src,dst) -> total bytes
alert_total       = 0
messages_total    = 0
c2_hubs_total     = 0


# ── Shannon Entropy ────────────────────────────────────────────────────────────
def shannon_entropy(values: list) -> float:
    if len(values) < 2:
        return 0.0
    from collections import Counter
    counts = Counter(values)
    total  = len(values)
    return -sum((c / total) * math.log2(c / total + 1e-12) for c in counts.values())


# ── Prometheus text exposition format ─────────────────────────────────────────
def build_metrics() -> str:
    lines = []

    # --- aura_entropy_score gauge ---
    lines.append('# HELP aura_entropy_score Shannon entropy score per source IP (low = C2 suspect)')
    lines.append('# TYPE aura_entropy_score gauge')
    for ip, score in list(entropy_scores.items()):
        safe_ip = ip.replace('.', '_').replace(':', '_')
        lines.append(f'aura_entropy_score{{src_ip="{ip}"}} {score:.6f}')

    # --- aura_alert_total counter ---
    lines.append('# HELP aura_alert_total Total number of C2 beaconing alerts fired')
    lines.append('# TYPE aura_alert_total counter')
    lines.append(f'aura_alert_total {alert_total}')

    # --- aura_messages_consumed_total counter ---
    lines.append('# HELP aura_messages_consumed_total Total Kafka messages consumed')
    lines.append('# TYPE aura_messages_consumed_total counter')
    lines.append(f'aura_messages_consumed_total {messages_total}')

    # --- aura_c2_hubs_detected_total counter ---
    lines.append('# HELP aura_c2_hubs_detected_total Total C2 hubs identified and remediated')
    lines.append('# TYPE aura_c2_hubs_detected_total counter')
    lines.append(f'aura_c2_hubs_detected_total {c2_hubs_total}')

    # --- aura_active_tracked_ips gauge ---
    lines.append('# HELP aura_active_tracked_ips Number of source IPs currently tracked in entropy window')
    lines.append('# TYPE aura_active_tracked_ips gauge')
    lines.append(f'aura_active_tracked_ips {len(ip_history)}')

    # --- aura_bytes_total counter (top 50 flows to avoid cardinality explosion) ---
    lines.append('# HELP aura_bytes_total Total bytes observed per src→dst flow pair')
    lines.append('# TYPE aura_bytes_total counter')
    top_flows = sorted(byte_counters.items(), key=lambda x: x[1], reverse=True)[:50]
    for (src, dst), total in top_flows:
        lines.append(f'aura_bytes_total{{src_ip="{src}",dst_ip="{dst}"}} {total}')

    return '\n'.join(lines) + '\n'


# ── HTTP handler for /metrics ─────────────────────────────────────────────────
class MetricsHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress access logs

    def do_GET(self):
        if self.path == '/metrics':
            body = build_metrics().encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        global c2_hubs_total
        if self.path == '/dismantle':
            c2_hubs_total += 1
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()


# ── Kafka consumer loop (runs in a background thread) ─────────────────────────
def kafka_consumer_loop():
    global alert_total, messages_total

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id':          KAFKA_GROUP,
        'auto.offset.reset': 'latest',   # only new messages — don't replay history
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    print(f"[metrics] Kafka consumer started → topic={KAFKA_TOPIC}, group={KAFKA_GROUP}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[metrics] Kafka error: {msg.error()}")
                continue

            try:
                data       = json.loads(msg.value().decode('utf-8'))
                src_ip     = data.get('src_ip') or data.get('source_ip', 'unknown')
                dst_ip     = data.get('dst_ip') or data.get('destination_ip', 'unknown')
                bytes_val  = int(data.get('bytes', 0) or data.get('bytes_sent', 0))
            except Exception:
                continue

            messages_total += 1
            ip_history[src_ip].append(bytes_val)
            byte_counters[(src_ip, dst_ip)] += bytes_val

            # Evaluate entropy window
            if len(ip_history[src_ip]) >= WINDOW_SIZE:
                window  = ip_history[src_ip][-WINDOW_SIZE:]
                entropy = shannon_entropy(window)
                entropy_scores[src_ip] = entropy

                if entropy < ENTROPY_THRESHOLD:
                    alert_total += 1

                ip_history[src_ip] = []   # reset window

    except Exception as e:
        print(f"[metrics] Consumer crashed: {e}")
    finally:
        consumer.close()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║    📡  AURA-SENTINEL  |  Prometheus Metrics Exporter  v1.0  ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"[metrics] Starting HTTP server on :{METRICS_PORT}/metrics")
    print(f"[metrics] Add this target to prometheus.yml:")
    print(f"          - targets: ['localhost:{METRICS_PORT}']")
    print()

    # Start Kafka consumer in background
    t = threading.Thread(target=kafka_consumer_loop, daemon=True)
    t.start()

    # Start Prometheus HTTP server (blocking)
    server = HTTPServer(('0.0.0.0', METRICS_PORT), MetricsHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[metrics] Shutting down.")
