"""kafka-python: downgrades to older API versions; no transactions or idempotence."""

from harness import check_scenario, parametrize, scenarios

SCENARIOS = scenarios(
    "produce-consume", "group-consume", "records-shape", "compression", "large-records",
    "record-too-large", "multi-partition", "offsets-by-time", "seek", "long-poll",
    "offset-out-of-range", "acks", "group-two-members", "group-resume",
    "group-rebalance-leave", "group-admin", "admin-topics", "admin-configs",
    "admin-cluster", "admin-offsets", "metadata", "unknown-topic", "topic-recreate",
    "topic-limits", "many-partitions", "many-records", "seek-into-batch",
    "group-partition-growth", "compacted-null-key",
)


@parametrize("kafka-python", SCENARIOS)
def test_kafka_python_scenario(scenario):
    check_scenario("kafka-python", scenario)
