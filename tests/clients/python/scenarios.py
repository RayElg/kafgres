#!/usr/bin/env python3
"""kafka-python scenario runner for the conformance suite.

Pure Python, its own version negotiation, and — unlike librdkafka and Sarama — it
probes the broker's API versions on connect and *downgrades* rather than pinning, which
makes it the client most likely to exercise an older version of an API. One
machine-readable line per scenario, so the same binary can run against kafgres and a
reference Kafka and the two outputs diffed.
"""

import sys

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

def produce_consume(broker, topic):
    producer = KafkaProducer(bootstrap_servers=broker, acks=1, api_version_auto_timeout_ms=30000)
    offsets = []
    for value in (b"p1", b"p2", b"p3"):
        meta = producer.send(topic, value).get(timeout=30)
        offsets.append(meta.offset)
    producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=30000,
        group_id=None,
    )
    values = [m.value.decode() for m in consumer]
    consumer.close()
    assert len(values) == 3, f"expected 3 records, got {len(values)}: {values}"
    print(f"OK offsets={','.join(str(o) for o in offsets)} values={','.join(values)}")

def group_consume(broker, topic):
    producer = KafkaProducer(bootstrap_servers=broker, acks=1)
    for value in (b"gp1", b"gp2"):
        producer.send(topic, value).get(timeout=30)
    producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=f"{topic}-kpy-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=45000,
    )
    values = sorted(m.value.decode() for m in consumer)
    assert len(values) == 2, f"expected 2 records, got {len(values)}: {values}"
    consumer.commit()
    consumer.close()
    print(f"OK values={','.join(values)}")

def metadata(broker, topic):
    consumer = KafkaConsumer(bootstrap_servers=broker)
    partitions = consumer.partitions_for_topic(topic)
    consumer.close()
    admin = KafkaAdminClient(bootstrap_servers=broker)
    cluster = admin.describe_cluster()
    admin.close()
    assert partitions, f"no partitions for {topic}; the topic was not reported"
    print(
        f"OK partitions={len(partitions or [])} "
        f"brokers={len(cluster.get('brokers', []))} "
        f"controller={'yes' if cluster.get('controller_id', -1) >= 0 else 'no'}"
    )

def unknown_topic(broker, _topic):
    missing = "definitely-no-such-topic-conformance"
    consumer = KafkaConsumer(bootstrap_servers=broker)
    partitions = consumer.partitions_for_topic(missing)
    consumer.close()

    admin = KafkaAdminClient(bootstrap_servers=broker)
    described = admin.describe_topics([missing])
    admin.close()
    code = described[0]["error_code"] if described else "omitted"
    print(f"OK partitions={partitions} error_code={code}")

SCENARIOS = {
    "produce-consume": produce_consume,
    "group-consume": group_consume,
    "metadata": metadata,
    "unknown-topic": unknown_topic,
}

def main():
    if len(sys.argv) < 4:
        print("usage: kafka-python-conformance <broker> <scenario> <topic>")
        return 2
    broker, scenario, topic = sys.argv[1], sys.argv[2], sys.argv[3]
    fn = SCENARIOS.get(scenario)
    if fn is None:
        print(f"ERROR unknown scenario {scenario}")
        return 2
    try:
        fn(broker, topic)
    except (KafkaError, AssertionError, OSError) as e:
        print(f"ERROR {type(e).__name__}: {e}")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
