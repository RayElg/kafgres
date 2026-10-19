"""Java client (kafka-clients 4.3.1): the only client for share groups and the producer and
transaction admin APIs."""

from harness import check_scenario, parametrize, scenarios

SCENARIOS = scenarios(
    "produce-consume", "records-shape", "compression", "large-records", "record-too-large",
    "multi-partition", "offsets-by-time", "seek", "long-poll", "offset-out-of-range",
    "idempotent-produce", "acks", "txn-read-committed", "txn-consume-transform-produce",
    "txn-fenced", "group-two-members", "group-resume", "group-rebalance-leave",
    "group-static", "group-cooperative", "group-kip848", "group-admin", "admin-topics",
    "admin-configs", "admin-cluster", "admin-delete-records", "admin-offsets",
    "admin-producers", "admin-quotas", "share-consume", "metadata", "unknown-topic",
    "commit-stale-generation", "topic-recreate", "topic-limits", "many-partitions",
    "many-records", "seek-into-batch", "group-partition-growth", "group-pattern",
    "group-list-filters", "txn-timeout", "compacted-null-key",
)


@parametrize("java", SCENARIOS)
def test_java_scenario(scenario):
    check_scenario("java", scenario)
