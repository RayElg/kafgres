"""Sarama (Go): pins its API version and decodes transaction markers unconditionally."""

from harness import (
    KAFGRES, check_scenario, drop_prefixed, make_topic_kafgres, parametrize, run_client, RUN,
    scenarios, sql,
)

SCENARIOS = scenarios(
    "produce-consume", "group-consume", "records-shape", "compression", "large-records",
    "record-too-large", "multi-partition", "offsets-by-time", "seek", "long-poll",
    "offset-out-of-range", "idempotent-produce", "acks", "txn-read-committed",
    "txn-consume-transform-produce", "txn-fenced", "group-two-members", "group-resume",
    "group-rebalance-leave", "group-static", "group-admin", "admin-topics", "admin-configs",
    "admin-cluster", "admin-delete-records", "admin-offsets", "metadata", "unknown-topic",
    "topic-recreate", "topic-limits", "many-partitions", "many-records", "seek-into-batch",
    "group-partition-growth", "compacted-null-key",
)


@parametrize("sarama", SCENARIOS)
def test_sarama_scenario(scenario):
    check_scenario("sarama", scenario)


def test_a_null_metadata_row_still_decodes():
    """OffsetFetch answers a stored NULL `metadata` with "", which Sarama requires.

    OffsetCommit's `metadata` is nullable on the wire, but every runner commits `""`, so
    no scenario stores a NULL.
    """
    topic = f"conf-null-meta-{RUN}"
    make_topic_kafgres(topic)
    group = f"{topic}-sarama-group"
    try:
        first = run_client("sarama", KAFGRES, "group-consume", topic)
        assert first.startswith("OK "), first

        sql(f"UPDATE kafgres_offsets SET metadata = NULL WHERE group_id = '{group}'")
        affected = sql(
            f"SELECT count(*) FROM kafgres_offsets "
            f"WHERE group_id = '{group}' AND metadata IS NULL"
        )
        assert affected != "0", "no committed offsets to null out; the setup did not run"

        second = run_client("sarama", KAFGRES, "group-consume", topic)
        assert second.startswith("OK "), f"NULL metadata broke OffsetFetch: {second}"
    finally:
        drop_prefixed(topic, reference=False)
