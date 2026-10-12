"""`offsetsForTimes` on both engines: the first record at or after the timestamp."""

import pytest

from conftest import sql

BROKER = "127.0.0.1:9092"


@pytest.fixture
def topic(request):
    name = f"oft-{request.node.name.replace('_', '-')[:40]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")


def test_offsets_for_times_answers_with_the_record_not_its_batch(topic):
    """A timestamp lookup answers with the first record at or after it, offset and timestamp,
    not its batch's base offset."""
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    base = 1_700_000_000_000
    p = KafkaProducer(bootstrap_servers=BROKER, linger_ms=1000, batch_size=1 << 20)
    for i in range(50):
        p.send(topic, value=b"v", partition=0, timestamp_ms=base + i * 10)
    p.flush()
    p.close()
    c = KafkaConsumer(bootstrap_servers=BROKER)
    tp = TopicPartition(topic, 0)
    for target, offset in [(base + 255, 26), (base + 260, 26), (base, 0), (base + 490, 49)]:
        got = c.offsets_for_times({tp: target})[tp]
        assert (got.offset, got.timestamp) == (offset, base + offset * 10), (target, got)
    assert c.offsets_for_times({tp: base + 1000})[tp] is None
    c.close()
