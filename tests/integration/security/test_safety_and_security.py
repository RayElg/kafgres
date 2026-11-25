"""Safety and security invariants across CDC, ACLs, transactions, groups, and storage."""

import socket
import struct
import subprocess
import time

from conftest import (
    ADD_OFFSETS_TO_TXN,
    ADD_PARTITIONS_TO_TXN,
    BROKER_HOST,
    BROKER_PORT,
    DELETE_GROUPS,
    DELETE_RECORDS,
    DELETE_TOPICS,
    END_TXN,
    FIND_COORDINATOR,
    INIT_PRODUCER_ID,
    LIST_OFFSETS,
    OFFSET_FOR_LEADER_EPOCH,
    TXN_OFFSET_COMMIT,
    sql,
)
from support.docker import CLIENTS

ERR_NONE = 0
ERR_TRANSACTIONAL_ID_AUTH_FAILED = 53
ERR_GROUP_AUTH_FAILED = 30
ERR_NON_EMPTY_GROUP = 68
ERR_UNKNOWN_TOPIC_OR_PARTITION = 3
ERR_TOPIC_AUTH_FAILED = 29
ERR_OPERATION_NOT_ATTEMPTED = 55
ERR_REQUEST_TIMED_OUT = 7

ARCHIVE_LOCK_KEY = 0x7047_4B41_0000_0006


def psql_as(role, statement):
    return subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-U", role, "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-tAc", statement,
        ],
        capture_output=True, text=True,
    )


def produce(topic, values):
    """Through the Kafka protocol, so the test runs on either engine."""
    out = subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat",
         "-b", f"{BROKER_HOST}:{BROKER_PORT}", "-t", topic, "-P"],
        input="".join(f"{v}\n" for v in values), capture_output=True, text=True, timeout=120,
    )
    assert out.returncode == 0, out.stderr


def _read_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("broker closed connection")
        buf.extend(chunk)
    return bytes(buf)


def send_frame(sock, api_key, api_version, corr_id, body):
    client_id = b"pytest"
    header = struct.pack(">hhih", api_key, api_version, corr_id, len(client_id)) + client_id
    frame = header + body
    sock.sendall(struct.pack(">i", len(frame)) + frame)
    (size,) = struct.unpack(">i", _read_exact(sock, 4))
    resp = _read_exact(sock, size)
    (resp_corr,) = struct.unpack_from(">i", resp, 0)
    assert resp_corr == corr_id
    return resp[4:]


def _s(text):
    b = text.encode("utf-8")
    return struct.pack(">h", len(b)) + b


def _ns(text):
    if text is None:
        return struct.pack(">h", -1)
    return _s(text)


def test_cdc_mapping_runs_under_owner_privileges():
    role = "cdc_unpriv_owner"
    topic = "cdc-owner-priv-topic"
    sql("DELETE FROM kafgres_cdc_mappings WHERE mapping_name LIKE 'owner_priv_%'")
    sql("DELETE FROM kafgres_cdc_errors WHERE mapping_name LIKE 'owner_priv_%'")
    sql("DROP TABLE IF EXISTS public.owner_priv_src, public.owner_priv_secret CASCADE")
    sql(f"DROP ROLE IF EXISTS {role}")
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    sql("CREATE TABLE public.owner_priv_src (id int primary key, val text)")
    sql("CREATE TABLE public.owner_priv_secret (s text)")
    sql("INSERT INTO public.owner_priv_secret VALUES ('top-secret-value')")
    sql(f"CREATE ROLE {role} LOGIN")
    sql(f"GRANT EXECUTE ON FUNCTION kafgres_add_mapping(text, text, text, text, text, text) TO {role}")
    sql(f"GRANT SELECT ON kafgres_topics, public.owner_priv_src TO {role}")
    try:
        # Adding a mapping that references an inaccessible table fails at definition time.
        r = psql_as(
            role,
            f"SELECT kafgres_add_mapping('owner_priv_bad', 'public.owner_priv_src', '{topic}', "
            f"$$(SELECT string_agg(s, ',') FROM public.owner_priv_secret)$$, $$new.id::text$$, NULL)",
        )
        assert r.returncode != 0
        assert "permission denied" in r.stderr.lower(), r.stderr

        # Direct DML without table grants is refused.
        r = psql_as(
            role,
            f"INSERT INTO kafgres_cdc_mappings (mapping_name, source_table, topic, key_expr, value_expr) "
            f"VALUES ('owner_priv_dml', 'public.owner_priv_src', '{topic}', 'new.id::text', "
            f"$$(SELECT string_agg(s, ',') FROM public.owner_priv_secret)$$)",
        )
        assert r.returncode != 0
        assert "permission denied" in r.stderr.lower(), r.stderr

        # Even with direct table DML granted, owner spoofing is refused by the trigger and
        # expressions render under the mapping owner's privileges rather than superuser.
        sql(f"GRANT SELECT, INSERT, UPDATE ON kafgres_cdc_mappings TO {role}")
        r = psql_as(
            role,
            f"INSERT INTO kafgres_cdc_mappings (mapping_name, source_table, topic, key_expr, value_expr, owner) "
            f"VALUES ('owner_priv_spoof', 'public.owner_priv_src', '{topic}', 'new.id::text', 'new.val', 'postgres')",
        )
        assert r.returncode != 0
        assert "permission denied" in r.stderr.lower(), r.stderr

        r = psql_as(
            role,
            f"INSERT INTO kafgres_cdc_mappings (mapping_name, source_table, topic, key_expr, value_expr) "
            f"VALUES ('owner_priv_dml', 'public.owner_priv_src', '{topic}', 'new.id::text', "
            f"$$(SELECT string_agg(s, ',') FROM public.owner_priv_secret)$$)",
        )
        assert r.returncode == 0, r.stderr

        sql("SELECT kafgres_cdc_create_slot()")
        sql("INSERT INTO public.owner_priv_src VALUES (1, 'hello')")
        sql("SELECT kafgres_cdc_drain(100)")

        span = int(sql(f"SELECT coalesce(sum(offset_span), 0) FROM kafgres_partition_offsets('{topic}')"))
        assert span == 0, f"secret data leaked into topic ({span} records)"
        err_msg = sql("SELECT error FROM kafgres_cdc_errors WHERE mapping_name = 'owner_priv_dml' LIMIT 1")
        assert "permission denied" in err_msg.lower(), f"expected permission denied in dead letters, got: {err_msg!r}"

        # Revoking SELECT on the source table stops rendering via the on_error path.
        sql("DELETE FROM kafgres_cdc_mappings WHERE mapping_name = 'owner_priv_dml'")
        sql("DELETE FROM kafgres_cdc_errors WHERE mapping_name = 'owner_priv_dml'")
        r = psql_as(
            role,
            f"SELECT kafgres_add_mapping('owner_priv_ok', 'public.owner_priv_src', '{topic}', "
            f"$$new.val$$, $$new.id::text$$, NULL)",
        )
        assert r.returncode == 0, r.stderr
        sql("INSERT INTO public.owner_priv_src VALUES (2, 'allowed')")
        sql("SELECT kafgres_cdc_drain(100)")
        span = int(sql(f"SELECT coalesce(sum(offset_span), 0) FROM kafgres_partition_offsets('{topic}')"))
        assert span == 1, f"expected 1 allowed record, got {span}"

        sql(f"REVOKE SELECT ON public.owner_priv_src FROM {role}")
        sql("INSERT INTO public.owner_priv_src VALUES (3, 'after-revoke')")
        sql("SELECT kafgres_cdc_drain(100)")
        span = int(sql(f"SELECT coalesce(sum(offset_span), 0) FROM kafgres_partition_offsets('{topic}')"))
        assert span == 1, f"record rendered after SELECT was revoked ({span})"
        err_msg = sql("SELECT error FROM kafgres_cdc_errors WHERE mapping_name = 'owner_priv_ok' LIMIT 1")
        assert "permission denied" in err_msg.lower(), f"expected permission denied after revoke, got: {err_msg!r}"
    finally:
        sql("DELETE FROM kafgres_cdc_mappings WHERE mapping_name LIKE 'owner_priv_%'")
        sql("DELETE FROM kafgres_cdc_errors WHERE mapping_name LIKE 'owner_priv_%'")
        sql(f"REVOKE ALL ON kafgres_cdc_mappings, kafgres_topics, public.owner_priv_src FROM {role}")
        sql(f"REVOKE ALL ON FUNCTION kafgres_add_mapping(text, text, text, text, text, text) FROM {role}")
        sql("DROP TABLE IF EXISTS public.owner_priv_src, public.owner_priv_secret CASCADE")
        sql(f"DROP ROLE IF EXISTS {role}")
        sql(f"SELECT kafgres_drop_topic('{topic}')")


def span_of(topic):
    return int(sql(f"SELECT coalesce(sum(offset_span), 0) FROM kafgres_partition_offsets('{topic}')"))


def eventually(check, timeout=30):
    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, "condition never held"
        time.sleep(0.5)


def test_a_mapping_without_an_owner_renders_nothing_until_a_superuser_sets_one():
    """What an upgrade leaves: a mapping made before owners were recorded. It must not render
    as the superuser that ran the migration, must not be claimable by another role, and its
    changes must wait in the slot rather than be lost."""
    role = "cdc_claimer"
    topic = "cdc-ownerless-topic"
    sql("DELETE FROM kafgres_cdc_mappings WHERE mapping_name = 'ownerless'")
    sql("DROP TABLE IF EXISTS public.ownerless_src, public.ownerless_secret CASCADE")
    sql(f"DROP ROLE IF EXISTS {role}")
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    sql("CREATE TABLE public.ownerless_src (id int primary key)")
    sql("CREATE TABLE public.ownerless_secret (s text)")
    sql("INSERT INTO public.ownerless_secret VALUES ('TOP-SECRET-ROW')")
    sql(f"CREATE ROLE {role} LOGIN")
    for fn in ("kafgres_add_mapping(text, text, text, text, text, text)",
               "kafgres_remove_mapping(text)", "kafgres_snapshot_mapping(text)"):
        sql(f"GRANT EXECUTE ON FUNCTION {fn} TO {role}")
    sql(f"GRANT SELECT ON kafgres_topics, public.ownerless_src TO {role}")
    sql(f"GRANT SELECT, DELETE ON kafgres_cdc_mappings TO {role}")
    try:
        sql("SELECT kafgres_cdc_create_slot()")
        sql(f"""INSERT INTO kafgres_cdc_mappings (mapping_name, source_table, topic, value_expr, owner)
                VALUES ('ownerless', 'public.ownerless_src', '{topic}',
                        $$(SELECT string_agg(s, ',') FROM public.ownerless_secret)$$, NULL)""")
        sql("INSERT INTO public.ownerless_src VALUES (1)")
        sql("SELECT kafgres_cdc_drain(100)")
        assert span_of(topic) == 0, "an ownerless mapping rendered"
        assert sql("SELECT count(*) FROM kafgres_cdc_errors WHERE mapping_name = 'ownerless'") == "0"

        for statement in (
            f"SELECT kafgres_add_mapping('ownerless', 'public.ownerless_src', '{topic}', 'new.id::text', NULL, NULL)",
            "SELECT kafgres_remove_mapping('ownerless')",
            "SELECT kafgres_snapshot_mapping('ownerless')",
            "DELETE FROM kafgres_cdc_mappings WHERE mapping_name = 'ownerless'",
        ):
            r = psql_as(role, statement)
            assert r.returncode != 0 and "permission denied" in r.stderr.lower(), (statement, r.stderr)
        assert sql("SELECT count(*) FROM kafgres_cdc_mappings WHERE mapping_name = 'ownerless'") == "1"

        # The held change renders once an owner is set, as that owner.
        sql("UPDATE kafgres_cdc_mappings SET owner = 'postgres' WHERE mapping_name = 'ownerless'")
        sql("SELECT kafgres_cdc_drain(100)")
        eventually(lambda: span_of(topic) == 1)
    finally:
        sql("DELETE FROM kafgres_cdc_mappings WHERE mapping_name = 'ownerless'")
        sql("DELETE FROM kafgres_cdc_errors WHERE mapping_name = 'ownerless'")
        sql(f"REVOKE ALL ON kafgres_cdc_mappings, kafgres_topics FROM {role}")
        for fn in ("kafgres_add_mapping(text, text, text, text, text, text)",
                   "kafgres_remove_mapping(text)", "kafgres_snapshot_mapping(text)"):
            sql(f"REVOKE ALL ON FUNCTION {fn} FROM {role}")
        sql("DROP TABLE IF EXISTS public.ownerless_src, public.ownerless_secret CASCADE")
        sql(f"DROP ROLE IF EXISTS {role}")
        sql(f"SELECT kafgres_drop_topic('{topic}')")


def test_a_mapping_writes_its_topic_only_while_its_owner_may():
    """With ACLs on, a mapping needs WRITE on its topic for its owner, as kafgres_produce()
    does: to be defined, and on every render after."""
    role = "cdc_writer"
    topic = "cdc-acl-topic"
    sql("DELETE FROM kafgres_cdc_mappings WHERE mapping_name = 'acl_checked'")
    sql("DELETE FROM kafgres_cdc_errors WHERE mapping_name = 'acl_checked'")
    sql("DROP TABLE IF EXISTS public.acl_src CASCADE")
    sql(f"DROP ROLE IF EXISTS {role}")
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    sql("CREATE TABLE public.acl_src (id int primary key)")
    sql(f"CREATE ROLE {role} LOGIN")
    sql(f"GRANT EXECUTE ON FUNCTION kafgres_add_mapping(text, text, text, text, text, text) TO {role}")
    sql(f"GRANT SELECT ON kafgres_topics, public.acl_src TO {role}")
    add = (f"SELECT kafgres_add_mapping('acl_checked', 'public.acl_src', '{topic}', "
           f"$$new.id::text$$, NULL, NULL)")
    sql("ALTER SYSTEM SET kafgres.acls_enabled = on")
    sql("SELECT pg_reload_conf()")
    time.sleep(2)
    try:
        r = psql_as(role, add)
        assert r.returncode != 0 and "not allowed to write" in r.stderr.lower(), r.stderr

        sql(f"SELECT kafgres_add_acl('User:{role}', 'WRITE', 'TOPIC', '{topic}')")
        r = psql_as(role, add)
        assert r.returncode == 0, r.stderr
        sql("INSERT INTO public.acl_src VALUES (1)")
        sql("SELECT kafgres_cdc_drain(100)")
        eventually(lambda: span_of(topic) == 1)

        sql(f"SELECT kafgres_remove_acl('User:{role}', 'WRITE', 'TOPIC', '{topic}')")
        time.sleep(2)  # past the ACL cache's staleness bound
        sql("INSERT INTO public.acl_src VALUES (2)")
        sql("SELECT kafgres_cdc_drain(100)")
        eventually(lambda: sql(
            "SELECT count(*) FROM kafgres_cdc_errors WHERE mapping_name = 'acl_checked'") != "0")
        assert span_of(topic) == 1, "the mapping wrote after its owner lost WRITE"
        err = sql("SELECT error FROM kafgres_cdc_errors WHERE mapping_name = 'acl_checked' LIMIT 1")
        assert "not allowed to write" in err.lower(), err
    finally:
        sql("ALTER SYSTEM RESET kafgres.acls_enabled")
        sql("SELECT pg_reload_conf()")
        sql(f"SELECT kafgres_remove_acl('User:{role}', 'WRITE', 'TOPIC', '{topic}')")
        sql("DELETE FROM kafgres_cdc_mappings WHERE mapping_name = 'acl_checked'")
        sql("DELETE FROM kafgres_cdc_errors WHERE mapping_name = 'acl_checked'")
        sql(f"REVOKE ALL ON kafgres_topics FROM {role}")
        sql(f"REVOKE ALL ON FUNCTION kafgres_add_mapping(text, text, text, text, text, text) FROM {role}")
        sql("DROP TABLE IF EXISTS public.acl_src CASCADE")
        sql(f"DROP ROLE IF EXISTS {role}")
        sql(f"SELECT kafgres_drop_topic('{topic}')")
        time.sleep(2)


def test_transactional_id_acls_are_enforced():
    topic = "txn-acl-enforce-topic"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    sql("DELETE FROM kafgres_acls")
    sql("ALTER SYSTEM SET kafgres.sasl_required = off")
    sql("ALTER SYSTEM SET kafgres.acls_enabled = on")
    sql("SELECT pg_reload_conf()")
    time.sleep(2.0)
    try:
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15) as sock:
            resp = send_frame(sock, FIND_COORDINATOR, 1, 1, _s("victim-tx") + struct.pack(">b", 1))
            (_throttle, code) = struct.unpack_from(">ih", resp, 0)
            assert code == ERR_TRANSACTIONAL_ID_AUTH_FAILED, f"FindCoordinator(tx) code={code}"

            resp = send_frame(sock, INIT_PRODUCER_ID, 0, 2, _ns("victim-tx") + struct.pack(">i", 60000))
            (_throttle, code) = struct.unpack_from(">ih", resp, 0)
            assert code == ERR_TRANSACTIONAL_ID_AUTH_FAILED, f"InitProducerId(tx) code={code}"

            body = _s("victim-tx") + struct.pack(">qhi", 1, 0, 1) + _s(topic) + struct.pack(">ii", 1, 0)
            resp = send_frame(sock, ADD_PARTITIONS_TO_TXN, 0, 3, body)
            off = 4 + 4 + 2 + len(topic) + 4 + 4
            (code,) = struct.unpack_from(">h", resp, off)
            assert code == ERR_TRANSACTIONAL_ID_AUTH_FAILED, f"AddPartitionsToTxn code={code}"

            body = _s("victim-tx") + struct.pack(">qh", 1, 0) + _s("grp1")
            resp = send_frame(sock, ADD_OFFSETS_TO_TXN, 0, 4, body)
            (_throttle, code) = struct.unpack_from(">ih", resp, 0)
            assert code == ERR_TRANSACTIONAL_ID_AUTH_FAILED, f"AddOffsetsToTxn code={code}"

            body = (
                _s("victim-tx") + _s("grp1") + struct.pack(">qhi", 1, 0, 1)
                + _s(topic) + struct.pack(">iiq", 1, 0, 1) + _ns(None)
            )
            resp = send_frame(sock, TXN_OFFSET_COMMIT, 0, 5, body)
            off = 4 + 4 + 2 + len(topic) + 4 + 4
            (code,) = struct.unpack_from(">h", resp, off)
            assert code == ERR_TRANSACTIONAL_ID_AUTH_FAILED, f"TxnOffsetCommit code={code}"

            body = _s("victim-tx") + struct.pack(">qhb", 1, 0, 1)
            resp = send_frame(sock, END_TXN, 0, 6, body)
            (_throttle, code) = struct.unpack_from(">ih", resp, 0)
            assert code == ERR_TRANSACTIONAL_ID_AUTH_FAILED, f"EndTxn code={code}"

        sql("SELECT kafgres_add_acl('User:ANONYMOUS', 'WRITE', 'TRANSACTIONAL_ID', 'allowed-tx')")
        sql(f"SELECT kafgres_add_acl('User:ANONYMOUS', 'WRITE', 'TOPIC', '{topic}')")
        sql("SELECT kafgres_add_acl('User:ANONYMOUS', 'WRITE', 'TOPIC', 'txn-acl-missing-topic')")
        time.sleep(2.0)
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15) as sock:
            resp = send_frame(sock, FIND_COORDINATOR, 1, 10, _s("allowed-tx") + struct.pack(">b", 1))
            (_throttle, code) = struct.unpack_from(">ih", resp, 0)
            assert code == ERR_NONE, f"FindCoordinator(allowed-tx) code={code}"

            resp = send_frame(sock, INIT_PRODUCER_ID, 0, 11, _ns("allowed-tx") + struct.pack(">i", 60000))
            (_throttle, code, pid, epoch) = struct.unpack_from(">ihqh", resp, 0)
            assert code == ERR_NONE and pid >= 0 and epoch >= 0, (code, pid, epoch)

            body = _s("allowed-tx") + struct.pack(">qh", pid, epoch) + _s("grp1")
            resp = send_frame(sock, ADD_OFFSETS_TO_TXN, 0, 12, body)
            (_throttle, code) = struct.unpack_from(">ih", resp, 0)
            assert code == ERR_GROUP_AUTH_FAILED, f"AddOffsetsToTxn without GROUP READ code={code}"

            # One denied topic fails the whole request, as Kafka does: the authorized topic is
            # not added (OPERATION_NOT_ATTEMPTED), and an authorized missing one is unknown.
            names = [topic, "txn-acl-denied-topic", "txn-acl-missing-topic"]
            body = _s("allowed-tx") + struct.pack(">qhi", pid, epoch, len(names))
            for name in names:
                body += _s(name) + struct.pack(">ii", 1, 0)
            resp = send_frame(sock, ADD_PARTITIONS_TO_TXN, 0, 13, body)
            pos = 4
            (count,) = struct.unpack_from(">i", resp, pos)
            pos += 4
            codes = {}
            for _ in range(count):
                (n,) = struct.unpack_from(">h", resp, pos)
                name = resp[pos + 2:pos + 2 + n].decode()
                pos += 2 + n + 4
                (_part, code) = struct.unpack_from(">ih", resp, pos)
                pos += 6
                codes[name] = code
            assert codes == {
                topic: ERR_OPERATION_NOT_ATTEMPTED,
                "txn-acl-denied-topic": ERR_TOPIC_AUTH_FAILED,
                "txn-acl-missing-topic": ERR_UNKNOWN_TOPIC_OR_PARTITION,
            }, codes
            added = sql(f"SELECT count(*) FROM kafgres_txn_partitions WHERE producer_id = {pid}")
            assert added == "0", f"{added} partition(s) added despite the denial"
    finally:
        sql("DELETE FROM kafgres_acls")
        sql("ALTER SYSTEM RESET kafgres.acls_enabled")
        sql("ALTER SYSTEM RESET kafgres.sasl_required")
        sql("SELECT pg_reload_conf()")
        sql(f"SELECT kafgres_drop_topic('{topic}')")
        time.sleep(2.0)


def test_failed_delete_topics_preserves_topic_and_data():
    topic = "delete-lock-preserve-topic"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    produce(topic, ["v1", "v2"])

    locker = subprocess.Popen(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-U", "postgres", "-c",
            f"SELECT pg_advisory_lock({ARCHIVE_LOCK_KEY}); SELECT pg_sleep(10);",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        time.sleep(1.0)
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15) as sock:
            body = struct.pack(">i", 1) + _s(topic) + struct.pack(">i", 5000)
            resp = send_frame(sock, DELETE_TOPICS, 1, 1, body)
            off = 4 + 4 + 2 + len(topic)
            (code,) = struct.unpack_from(">h", resp, off)
            assert code == ERR_REQUEST_TIMED_OUT, f"a delete that waited on a lock should be retriable, got {code}"

        rows = sql(f"SELECT count(*) FROM kafgres_read('{topic}', 0)")
        assert int(rows) == 2, f"expected 2 surviving records, got {rows}"
    finally:
        locker.terminate()
        locker.wait(timeout=5)
        sql(f"SELECT kafgres_drop_topic('{topic}')")


def test_collapsed_leader_epoch_returns_defined_end_offset():
    topic = "collapsed-epoch-topic"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    try:
        tid = int(sql(f"SELECT topic_id::int FROM kafgres_topics WHERE name = '{topic}'"))
        sql(
            f"DELETE FROM kafgres_leader_epochs WHERE topic_id = {tid}::oid AND partition = 0; "
            f"INSERT INTO kafgres_leader_epochs (topic_id, partition, leader_epoch, start_offset) "
            f"VALUES ({tid}::oid, 0, 2, 0); "
            f"UPDATE kafgres_partitions SET leader_epoch = 2, epoch_start_offset = 0 "
            f"WHERE topic_id = {tid}::oid AND partition = 0;"
        )
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15) as sock:
            body = struct.pack(">i", 1) + _s(topic) + struct.pack(">iiii", 1, 0, -1, 1)
            resp = send_frame(sock, OFFSET_FOR_LEADER_EPOCH, 2, 1, body)
            off = 4 + 4 + 2 + len(topic) + 4
            (err, part, ep, end_off) = struct.unpack_from(">hiiq", resp, off)
            assert err == ERR_NONE and part == 0, (err, part)
            assert ep == 1 and end_off == 0, f"expected (epoch=1, end_offset=0), got ({ep}, {end_off})"
    finally:
        sql(f"SELECT kafgres_drop_topic('{topic}')")


def test_delete_groups_handles_share_and_consumer_protocol_groups():
    share_g = "empty-share-group-del"
    kip848_g = "live-consumer-group-del"
    topic = "share-del-topic"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    sql(f"DELETE FROM kafgres_share_groups WHERE group_id = '{share_g}'")
    sql(f"DELETE FROM kafgres_consumer_groups WHERE group_id = '{kip848_g}'")
    try:
        tid = int(sql(f"SELECT topic_id::int FROM kafgres_topics WHERE name = '{topic}'"))
        sql(f"INSERT INTO kafgres_share_groups (group_id) VALUES ('{share_g}')")
        sql(
            f"INSERT INTO kafgres_share_offsets (group_id, topic_id, partition, start_offset) "
            f"VALUES ('{share_g}', {tid}::oid, 0, 5)"
        )
        sql(f"INSERT INTO kafgres_consumer_groups (group_id, state) VALUES ('{kip848_g}', 'Stable')")
        sql(
            f"INSERT INTO kafgres_consumer_group_members (group_id, member_id, member_epoch) "
            f"VALUES ('{kip848_g}', 'm1', 1)"
        )

        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15) as sock:
            body = struct.pack(">i", 2) + _s(share_g) + _s(kip848_g)
            resp = send_frame(sock, DELETE_GROUPS, 1, 1, body)
            pos = 4
            (count,) = struct.unpack_from(">i", resp, pos)
            pos += 4
            results = {}
            for _ in range(count):
                (slen,) = struct.unpack_from(">h", resp, pos)
                pos += 2
                gid = resp[pos:pos + slen].decode()
                pos += slen
                (code,) = struct.unpack_from(">h", resp, pos)
                pos += 2
                results[gid] = code

            assert results[share_g] == ERR_NONE, f"empty share group delete failed: {results}"
            assert results[kip848_g] == ERR_NON_EMPTY_GROUP, f"live consumer-protocol group was deleted: {results}"

        assert int(sql(f"SELECT count(*) FROM kafgres_share_groups WHERE group_id = '{share_g}'")) == 0
        assert int(sql(f"SELECT count(*) FROM kafgres_share_offsets WHERE group_id = '{share_g}'")) == 0
    finally:
        sql(f"DELETE FROM kafgres_consumer_groups WHERE group_id = '{kip848_g}'")
        sql(f"DELETE FROM kafgres_share_groups WHERE group_id = '{share_g}'")
        sql(f"SELECT kafgres_drop_topic('{topic}')")


def test_offset_for_timestamp_clamped_to_log_start():
    topic = "ts-clamp-topic"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    try:
        produce(topic, [f"v{i}" for i in range(10)])
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15) as sock:
            body = struct.pack(">i", 1) + _s(topic) + struct.pack(">iiqi", 1, 0, 5, 5000)
            send_frame(sock, DELETE_RECORDS, 0, 1, body)

            body = struct.pack(">ibi", -1, 0, 1) + _s(topic) + struct.pack(">iiq", 1, 0, 1)
            resp = send_frame(sock, LIST_OFFSETS, 2, 2, body)
            off = 4 + 4 + 2 + len(topic) + 4
            (part, err, _ts, offset) = struct.unpack_from(">ihqq", resp, off)
            assert err == ERR_NONE and part == 0, (part, err)
            assert offset >= 5, f"ListOffsets returned offset {offset} below log_start_offset 5"
    finally:
        sql(f"SELECT kafgres_drop_topic('{topic}')")


def test_producer_epoch_ceiling_rotates_producer_id():
    txn_id = "epoch-ceiling-txn"
    sql(f"DELETE FROM kafgres_txns WHERE transactional_id = '{txn_id}'")
    sql(f"DELETE FROM kafgres_producers WHERE transactional_id = '{txn_id}'")
    try:
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15) as sock:
            resp = send_frame(sock, INIT_PRODUCER_ID, 0, 1, _ns(txn_id) + struct.pack(">i", 60000))
            (_throttle, code, pid1, ep1) = struct.unpack_from(">ihqh", resp, 0)
            assert code == ERR_NONE and ep1 == 0, (code, pid1, ep1)

            sql(f"UPDATE kafgres_producers SET producer_epoch = 32767 WHERE producer_id = {pid1}")

            resp = send_frame(sock, INIT_PRODUCER_ID, 0, 2, _ns(txn_id) + struct.pack(">i", 60000))
            (_throttle, code, pid2, ep2) = struct.unpack_from(">ihqh", resp, 0)
            assert code == ERR_NONE, f"InitProducerId failed at epoch 32767: code={code}"
            assert pid2 != pid1 and ep2 == 0, f"expected fresh producer_id at epoch 0, got ({pid2}, {ep2})"
    finally:
        sql(f"DELETE FROM kafgres_txns WHERE transactional_id = '{txn_id}'")
        sql(f"DELETE FROM kafgres_producers WHERE transactional_id = '{txn_id}'")
