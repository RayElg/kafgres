"""Maintain current stock per SKU on a compacted topic.

This is the piece that usually argues for a compacted topic: the *current* value per key
matters and the history does not. A consumer joining later replays the topic and has the
whole world without querying anyone.
"""
import json
import sys
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer

BROKER = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9092"

consumer = KafkaConsumer(
    "orders.events",
    bootstrap_servers=BROKER,
    group_id="inventory",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    consumer_timeout_ms=60000,
    value_deserializer=lambda b: json.loads(b) if b else None,
)
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode(),
    key_serializer=lambda k: k.encode() if k else None,
)

stock = defaultdict(lambda: 100)
print("inventory: waiting for orders", flush=True)
for msg in consumer:
    order = msg.value
    if not order or order.get("status") != "placed":
        continue
    sku = order["sku"]
    stock[sku] -= order["qty"]
    producer.send("inventory.state", key=sku, value={"sku": sku, "stock": stock[sku]})
    producer.flush()
    print(f"inventory: {sku} -> {stock[sku]}", flush=True)
