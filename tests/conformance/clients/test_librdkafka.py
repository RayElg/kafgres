"""librdkafka 2.15 through confluent-kafka."""

from harness import check_scenario, parametrize, scenarios

SCENARIOS = scenarios(
    "produce-consume", "records-shape", "compression", "large-records", "record-too-large",
    "multi-partition", "offsets-by-time", "seek", "long-poll", "offset-out-of-range",
    "idempotent-produce", "acks", "txn-read-committed", "txn-consume-transform-produce",
    "txn-fenced", "group-two-members", "group-resume", "group-rebalance-leave",
    "group-static", "group-cooperative", "group-kip848", "group-admin", "admin-topics",
    "admin-configs", "admin-cluster", "admin-delete-records", "admin-offsets", "metadata",
    "unknown-topic", "topic-recreate", "topic-limits", "many-partitions", "many-records",
    "seek-into-batch", "group-partition-growth", "group-pattern", "compacted-null-key",
)


@parametrize("librdkafka", SCENARIOS)
def test_librdkafka_scenario(scenario):
    check_scenario("librdkafka", scenario)
