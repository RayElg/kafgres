"""Consume shipments and pretend to tell somebody."""
import json
import sys

from kafka import KafkaConsumer

BROKER = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9092"

consumer = KafkaConsumer(
    "shipments",
    bootstrap_servers=BROKER,
    group_id="notifier",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    consumer_timeout_ms=60000,
    value_deserializer=lambda b: json.loads(b) if b else None,
)
print("notifier: waiting for shipments", flush=True)
for msg in consumer:
    s = msg.value
    if not s:
        continue
    print(f"notifier: telling {s['customer']} that order {s['order_id']} "
          f"ships by {s['carrier']}", flush=True)
