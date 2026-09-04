"""Consume orders, decide shipments.

An ordinary Kafka consumer in a group, producing to another topic. Nothing here knows the
broker is a Postgres extension.
"""
import json
import sys

from kafka import KafkaConsumer, KafkaProducer

BROKER = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9092"

consumer = KafkaConsumer(
    "orders.events",
    bootstrap_servers=BROKER,
    group_id="fulfilment",
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

print("fulfilment: waiting for orders", flush=True)
for msg in consumer:
    order = msg.value
    if not order or order.get("status") != "placed":
        continue
    shipment = {
        "order_id": order["order_id"],
        "customer": order["customer"],
        "sku": order["sku"],
        "qty": order["qty"],
        "carrier": "pigeon" if order["qty"] < 5 else "truck",
    }
    producer.send("shipments", key=order["customer"], value=shipment)
    producer.flush()
    print(f"fulfilment: order {order['order_id']} -> {shipment['carrier']}", flush=True)
