"""franz-go: speaks the newest version of every API both sides advertise."""

from harness import check_scenario, parametrize, scenarios

SCENARIOS = scenarios(
    "produce-consume", "records-shape", "compression", "large-records", "record-too-large",
    "multi-partition", "offsets-by-time", "seek", "long-poll", "offset-out-of-range",
    "idempotent-produce", "acks", "txn-read-committed", "txn-consume-transform-produce",
    "txn-fenced", "group-two-members", "group-resume", "group-rebalance-leave",
    "group-static", "group-cooperative", "group-admin", "admin-topics", "admin-configs",
    "admin-cluster", "admin-delete-records", "admin-offsets", "admin-producers",
    "admin-quotas", "metadata", "unknown-topic", "topic-recreate", "topic-limits",
    "many-partitions", "many-records", "seek-into-batch", "group-partition-growth",
    "compacted-null-key",
)


@parametrize("franz-go", SCENARIOS)
def test_franz_go_scenario(scenario):
    check_scenario("franz-go", scenario)
