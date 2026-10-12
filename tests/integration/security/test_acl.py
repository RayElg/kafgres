"""Authorization: `kafgres_acls`.

Kafka's model, so the assertions are about Kafka's semantics rather than ours: DENY wins,
an operation with no matching ALLOW is refused, READ implies DESCRIBE, and a listing is
filtered rather than errored.

Every test authenticates over SASL, because authorization needs a principal and the
principal is what authentication produces. The default with enforcement on and an empty
table is refusal, which is why nothing here can be tested by accident.
"""

import os
import struct
import subprocess
import tempfile
import time

import pytest

from conftest import CREATE_ACLS, DELETE_ACLS, read_legacy_string, sql

CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

USER = "kafgres_acl_user"
PASSWORD = "kafgres_acl_pass"
PRINCIPAL = f"User:{USER}"

ACL_SETTLE = 2.0

def kcat(*args, stdin=None, timeout=90):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b", BROKER,
         "-X", "security.protocol=SASL_PLAINTEXT",
         "-X", "sasl.mechanisms=SCRAM-SHA-256",
         "-X", f"sasl.username={USER}",
         "-X", f"sasl.password={PASSWORD}",
         *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

def grant(operation, resource_type, resource_name, permission="ALLOW",
          pattern="LITERAL", principal=PRINCIPAL):
    sql(
        f"SELECT kafgres_add_acl('{principal}', '{operation}', '{resource_type}', "
        f"'{resource_name}', '{permission}', '{pattern}')"
    )
    time.sleep(ACL_SETTLE)

def clear_acls():
    sql("DELETE FROM kafgres_acls")
    time.sleep(ACL_SETTLE)

@pytest.fixture(scope="module", autouse=True)
def acls_on():
    sql("SET password_encryption='scram-sha-256'; "
        f"DROP ROLE IF EXISTS {USER}; CREATE ROLE {USER} LOGIN PASSWORD '{PASSWORD}'")
    sql("ALTER SYSTEM SET kafgres.sasl_required = on")
    sql("ALTER SYSTEM SET kafgres.acls_enabled = on")
    sql("SELECT pg_reload_conf()")
    time.sleep(ACL_SETTLE)
    yield
    sql("DELETE FROM kafgres_acls")
    sql("ALTER SYSTEM RESET kafgres.sasl_required")
    sql("ALTER SYSTEM RESET kafgres.acls_enabled")
    sql("ALTER SYSTEM RESET kafgres.superusers")
    sql("SELECT pg_reload_conf()")
    sql(f"DROP ROLE IF EXISTS {USER}")
    time.sleep(ACL_SETTLE)

@pytest.fixture
def topic(request):
    name = f"p5acl-{request.node.name.replace('_', '-')[:34]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    clear_acls()
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")
    clear_acls()

def denied(out):
    text = (out.stdout + out.stderr).lower()
    return "authorization" in text or "authorized" in text

def test_nothing_is_allowed_without_a_rule(topic):
    """Enforcement on and an empty table means refusal, not "allow until configured".

    The other default is the one that bites: a broker that allows what it has no rule
    for turns "ACLs are on" into "ACLs are on for whatever someone remembered to write".
    """
    out = kcat("-t", topic, "-P", stdin="nope\n")
    assert out.returncode != 0 or denied(out), out.stdout + out.stderr
    assert log_rows(topic) == 0, "a produce with no ACL was accepted"

def log_rows(topic):
    """Records in the topic, via `kafgres_partition_offsets`, which answers on either
    storage engine — reading `kafgres_log` directly would be table-engine-only.

    A partition the broker has not touched since postmaster start reports a NULL high
    watermark, so `coalesce(..., 0)` maps untracked to 0; sound here because the broker
    stays up for the whole of each test.
    """
    return int(sql(
        f"SELECT coalesce(sum(offset_span), 0) FROM kafgres_partition_offsets('{topic}')"
    ).strip())

def test_write_allows_produce_and_does_not_allow_consume(topic):
    """WRITE is not READ. A producer credential that can also drain the topic is the
    most common way an ACL model is quietly useless."""
    grant("WRITE", "TOPIC", topic)
    assert kcat("-t", topic, "-P", stdin="a\nb\n").returncode == 0
    assert log_rows(topic) == 2

    out = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q")
    assert out.stdout.strip() == "", f"WRITE also granted read: {out.stdout!r}"

def test_read_allows_consume(topic):
    grant("WRITE", "TOPIC", topic)
    assert kcat("-t", topic, "-P", stdin="x\ny\nz\n").returncode == 0
    grant("READ", "TOPIC", topic)

    out = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q")
    assert out.stdout.split() == ["x", "y", "z"], out.stdout + out.stderr

def test_read_implies_describe(topic):
    """Kafka's implication table. Without it a consumer may fetch a topic but cannot
    discover its partitions, so the grant does not work in practice."""
    grant("READ", "TOPIC", topic)
    out = kcat("-L", "-t", topic)
    assert topic in out.stdout, out.stdout + out.stderr

def test_deny_beats_allow(topic):
    """An operator writing a DENY expects it to hold against any grant, including a
    broader one added later."""
    grant("ALL", "TOPIC", "*")
    assert kcat("-t", topic, "-P", stdin="a\n").returncode == 0
    assert log_rows(topic) == 1

    grant("WRITE", "TOPIC", topic, permission="DENY")
    out = kcat("-t", topic, "-P", stdin="b\n")
    assert out.returncode != 0 or denied(out), out.stdout + out.stderr
    assert log_rows(topic) == 1, "a DENY did not override a broader ALLOW"

def test_a_prefixed_grant_covers_the_prefix_only(topic):
    grant("WRITE", "TOPIC", "p5acl-", pattern="PREFIXED")
    assert kcat("-t", topic, "-P", stdin="a\n").returncode == 0
    assert log_rows(topic) == 1

    other = "elsewhere-acl"
    sql(f"SELECT kafgres_drop_topic('{other}')")
    sql(f"SELECT kafgres_create_topic('{other}', 1)")
    try:
        out = kcat("-t", other, "-P", stdin="a\n")
        assert out.returncode != 0 or denied(out)
        assert log_rows(other) == 0, "a prefixed grant reached outside its prefix"
    finally:
        sql(f"SELECT kafgres_drop_topic('{other}')")

def test_a_superuser_bypasses_the_table(topic):
    sql(f"ALTER SYSTEM SET kafgres.superusers = '{PRINCIPAL}'")
    sql("SELECT pg_reload_conf()")
    time.sleep(ACL_SETTLE)
    try:
        grant("ALL", "TOPIC", "*", permission="DENY")
        assert kcat("-t", topic, "-P", stdin="a\n").returncode == 0
        assert log_rows(topic) == 1
    finally:
        sql("ALTER SYSTEM RESET kafgres.superusers")
        sql("SELECT pg_reload_conf()")
        time.sleep(ACL_SETTLE)

def test_an_unnamed_listing_hides_topics_the_caller_cannot_describe(topic):
    """"Show me everything" must not enumerate what the caller may not see. Kafka
    filters here rather than erroring, and the distinction is the whole point of the
    operation: an error would still confirm the topic exists."""
    hidden = "p5acl-hidden"
    sql(f"SELECT kafgres_drop_topic('{hidden}')")
    sql(f"SELECT kafgres_create_topic('{hidden}', 1)")
    try:
        grant("READ", "TOPIC", topic)
        out = kcat("-L")
        assert topic in out.stdout, out.stdout
        assert hidden not in out.stdout, f"an unauthorized topic was listed:\n{out.stdout}"
    finally:
        sql(f"SELECT kafgres_drop_topic('{hidden}')")

def test_a_group_needs_its_own_grant(topic):
    """A topic grant is not a group grant. Consuming reads data *and* writes group
    state, and the two are authorized separately."""
    grant("WRITE", "TOPIC", topic)
    assert kcat("-t", topic, "-P", stdin="a\nb\n").returncode == 0
    grant("READ", "TOPIC", topic)

    group = "p5acl-group"
    out = kcat("-G", group, topic, "-e", "-o", "beginning", timeout=120)
    assert out.returncode != 0 or denied(out), (
        "consuming in a group with no group ACL succeeded: " + out.stdout + out.stderr
    )

    grant("READ", "GROUP", group)
    out = kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180)
    assert out.returncode == 0, out.stdout + out.stderr
    assert "a" in out.stdout and "b" in out.stdout, out.stdout

def test_a_rule_does_not_leak_across_resource_types(topic):
    """A grant on the *group* named X must not authorize the *topic* named X. Both are
    text in the same column, and conflating them is a one-word bug."""
    grant("ALL", "GROUP", topic)
    out = kcat("-t", topic, "-P", stdin="a\n")
    assert out.returncode != 0 or denied(out)
    assert log_rows(topic) == 0, "a GROUP grant authorized a TOPIC operation"

def test_offset_fetch_is_authorized_at_every_version(topic):
    """The check has to sit before the version split, not inside a branch.

    v8 moved the group into a `groups` array and kept the legacy field, so a check
    written only in the new branch leaves v1..v7 reading any group's committed offsets —
    and with them the *names* of topics the caller cannot see in Metadata.

    Driven as ANONYMOUS with SASL off, so the frame can be hand-built: the principal has
    no rules either way, and "no rule means refusal" is the property under test.
    """
    import socket as _socket
    import struct as _struct

    from conftest import BROKER_HOST, BROKER_PORT

    sql(
        f"""INSERT INTO kafgres_offsets (group_id, topic_id, partition, committed_offset)
            SELECT 'secret-group', topic_id, 0, 1 FROM kafgres_topics WHERE name = '{topic}'
            ON CONFLICT DO NOTHING"""
    )
    sql("ALTER SYSTEM SET kafgres.sasl_required = off")
    sql("SELECT pg_reload_conf()")
    time.sleep(ACL_SETTLE)
    try:
        sock = _socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
        try:
            group = b"secret-group"
            body = _struct.pack(">h", len(group)) + group + _struct.pack(">i", -1)
            header = _struct.pack(">hhi", 9, 5, 1) + _struct.pack(">h", 6) + b"pytest"
            frame = header + body
            sock.sendall(_struct.pack(">i", len(frame)) + frame)

            (size,) = _struct.unpack(">i", read_exactly(sock, 4))
            resp = read_exactly(sock, size)
            (topic_count,) = _struct.unpack_from(">i", resp, 8)
            (top_level,) = _struct.unpack_from(">h", resp, len(resp) - 2)
            assert topic_count <= 0, (
                f"OffsetFetch v5 returned {topic_count} topics for a group with no ACL"
            )
            assert top_level != 0, "OffsetFetch v5 reported success for an unauthorized group"
        finally:
            sock.close()
    finally:
        sql("ALTER SYSTEM SET kafgres.sasl_required = on")
        sql("SELECT pg_reload_conf()")
        time.sleep(ACL_SETTLE)

def read_exactly(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise AssertionError(f"peer closed after {len(buf)} of {n} bytes")
        buf += chunk
    return buf

def test_an_untyped_principal_is_refused():
    """`alice` never matches anything — the authorizer compares against `User:alice`. An
    ACL that exists and grants nothing is worse than an error, because the operator has
    a transcript saying it worked."""
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-d", "postgres", "-tAc",
         "SELECT kafgres_add_acl('alice', 'READ', 'TOPIC', 't')"],
        capture_output=True, text=True, timeout=60,
        cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."),
    )
    assert out.returncode != 0
    assert "typed" in (out.stdout + out.stderr).lower(), out.stdout + out.stderr

def test_a_misspelled_operation_is_refused():
    """The CHECK constraints are the point of managing this in SQL: an operation the
    authorizer never matches is a rule that appears to exist and does nothing."""
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-d", "postgres", "-tAc",
         "INSERT INTO kafgres_acls (principal, operation, permission, resource_type, "
         "resource_name) VALUES ('User:x', 'REED', 'ALLOW', 'TOPIC', 't')"],
        capture_output=True, text=True, timeout=60,
        cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."),
    )
    assert out.returncode != 0
    assert "check constraint" in (out.stdout + out.stderr).lower(), out.stdout + out.stderr

KAFKA = "apache/kafka:4.1.0"

def kafka_acls(*args, sasl=(USER, PASSWORD), timeout=180):
    """`kafka-acls.sh` as an operator runs it, authenticated.

    Authentication is not optional here: this module turns `sasl_required` on, and an
    unauthenticated AdminClient never gets far enough to send the request — it reports
    "Timed out waiting for a node assignment", which names the wrong subsystem entirely.
    Same JAAS plumbing as `test_sasl_auth.py`.
    """
    user, password = sasl
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "sasl.properties")
        with open(path, "w") as f:
            f.write(
                "security.protocol=SASL_PLAINTEXT\n"
                "sasl.mechanism=SCRAM-SHA-256\n"
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule"
                f' required username="{user}" password="{password}";\n'
            )
        os.chmod(d, 0o755)
        os.chmod(path, 0o644)
        return subprocess.run(
            ["docker", "run", "--rm", "--network", "host", "-v", f"{d}:/w", KAFKA,
             "/opt/kafka/bin/kafka-acls.sh", "--bootstrap-server", BROKER,
             "--command-config", "/w/sasl.properties", *args],
            capture_output=True, text=True, timeout=timeout,
        )

def stored():
    """Every rule, as `principal op permission type name pattern`."""
    rows = sql("""SELECT principal || ' ' || operation || ' ' || permission || ' '
                      || resource_type || ' ' || resource_name || ' ' || pattern_type
                    FROM kafgres_acls ORDER BY 1""")
    return sorted(r.strip() for r in rows.splitlines() if r.strip())

@pytest.fixture
def open_broker():
    """Authentication and enforcement off, for the tests that speak raw framing.

    A raw socket would otherwise have to complete a SCRAM exchange before it could send
    anything, which is `test_sasl_auth.py`'s subject rather than this one's.
    """
    sql("ALTER SYSTEM SET kafgres.sasl_required = off")
    sql("ALTER SYSTEM SET kafgres.acls_enabled = off")
    sql("SELECT pg_reload_conf()")
    time.sleep(ACL_SETTLE)
    sql("DELETE FROM kafgres_acls")
    yield
    sql("DELETE FROM kafgres_acls")
    sql("ALTER SYSTEM SET kafgres.sasl_required = on")
    sql("ALTER SYSTEM SET kafgres.acls_enabled = on")
    sql("SELECT pg_reload_conf()")
    time.sleep(ACL_SETTLE)

@pytest.fixture
def acl_admin():
    """A principal allowed to manage rules, with enforcement left on.

    Enforcement stays on because turning it off would make these tests pass without ever
    exercising the CLUSTER ALTER check the handlers do — and that check is the reason ACL
    management is not simply another topic-scoped grant: a principal who can write rules for
    one topic can write itself a rule for every other.

    Superuser rather than a self-granted ALTER on the cluster, to avoid the bootstrap where
    the rule permitting rule-management has to be created by something that is not yet
    permitted to create it.
    """
    sql(f"ALTER SYSTEM SET kafgres.superusers = '{PRINCIPAL}'")
    sql("SELECT pg_reload_conf()")
    time.sleep(ACL_SETTLE)
    sql("DELETE FROM kafgres_acls")
    yield
    sql("DELETE FROM kafgres_acls")
    sql("ALTER SYSTEM RESET kafgres.superusers")
    sql("SELECT pg_reload_conf()")
    time.sleep(ACL_SETTLE)

def test_kafka_acls_sh_add_list_remove(acl_admin):
    """Driven by the tool that defines the RPCs: an operator can use the tool they
    already have."""
    out = kafka_acls("--add", "--allow-principal", "User:alice",
                     "--operation", "Read", "--topic", "payments")
    assert out.returncode == 0, out.stderr
    assert stored() == ["User:alice READ ALLOW TOPIC payments LITERAL"], stored()

    listed = kafka_acls("--list")
    assert listed.returncode == 0, listed.stderr
    assert "User:alice" in listed.stdout and "payments" in listed.stdout, listed.stdout

    removed = kafka_acls("--remove", "--allow-principal", "User:alice",
                         "--operation", "Read", "--topic", "payments", "--force")
    assert removed.returncode == 0, removed.stderr
    assert stored() == [], stored()

def test_every_resource_shape_round_trips(acl_admin):
    """Each part of an ACL is an `int8` enum on the wire, and the numbers are protocol
    constants rather than anything derivable.

    A wrong one does not fail — it stores a rule that is *about something else*. DESCRIBE
    (8) read as ALTER (7) would grant a principal the right to change a topic it should
    only have been able to list. So this asserts the stored text for each shape rather than
    that the call succeeded.
    """
    assert kafka_acls("--add", "--deny-principal", "User:bob", "--operation", "Write",
                      "--topic", "app-", "--resource-pattern-type", "prefixed").returncode == 0
    assert kafka_acls("--add", "--allow-principal", "User:carol", "--operation", "All",
                      "--group", "*").returncode == 0
    assert kafka_acls("--add", "--allow-principal", "User:dave", "--operation", "Describe",
                      "--cluster").returncode == 0
    assert kafka_acls("--add", "--allow-principal", "User:erin", "--operation", "Write",
                      "--transactional-id", "txn-1").returncode == 0

    assert stored() == [
        "User:bob WRITE DENY TOPIC app- PREFIXED",
        "User:carol ALL ALLOW GROUP * LITERAL",
        "User:dave DESCRIBE ALLOW CLUSTER kafka-cluster LITERAL",
        "User:erin WRITE ALLOW TRANSACTIONAL_ID txn-1 LITERAL",
    ], stored()

def test_a_filtered_list_constrains_and_an_empty_one_does_not(acl_admin):
    """Kafka spells "any" two ways — the enum code 1 for typed fields and a null string for
    names — and both have to mean "do not constrain".

    Get that wrong and `--list` with no arguments matches nothing, reporting an empty ACL
    set on a broker full of rules: the most reassuring possible way to be wrong.
    """
    kafka_acls("--add", "--allow-principal", "User:alice", "--operation", "Read",
               "--topic", "payments")
    kafka_acls("--add", "--allow-principal", "User:bob", "--operation", "Write",
               "--topic", "orders")

    everything = kafka_acls("--list").stdout
    assert "User:alice" in everything and "User:bob" in everything, everything

    just_one = kafka_acls("--list", "--topic", "payments").stdout
    assert "User:alice" in just_one, just_one
    assert "User:bob" not in just_one, f"a filtered list returned an unrelated rule: {just_one}"

def test_creating_the_same_rule_twice_is_not_an_error(acl_admin):
    """Kafka treats it as success — the requested state is the actual state — and the
    table's uniqueness constraint would otherwise surface as a duplicate-key failure."""
    first = kafka_acls("--add", "--allow-principal", "User:alice", "--operation", "Read",
                       "--topic", "payments")
    second = kafka_acls("--add", "--allow-principal", "User:alice", "--operation", "Read",
                        "--topic", "payments")
    assert first.returncode == 0 and second.returncode == 0, second.stderr
    assert stored() == ["User:alice READ ALLOW TOPIC payments LITERAL"], stored()

def test_remove_reports_what_it_removed(open_broker, conn):
    """`DeleteAcls` names every rule the filter matched.

    That is how an AdminClient tells its caller what was deleted, and why the handler
    reads the matches *before* issuing the DELETE.

    Driven raw rather than through `kafka-acls.sh`, which cannot reach it: `--force`
    prints nothing, and without it the tool calls `System.console()`, null in a non-TTY
    container. v1 is the last non-flexible version.
    """
    sql("SELECT kafgres_add_acl('User:alice', 'READ', 'TOPIC', 'payments', 'ALLOW', 'LITERAL')")
    sql("SELECT kafgres_add_acl('User:bob', 'READ', 'TOPIC', 'payments', 'ALLOW', 'LITERAL')")

    body = struct.pack(">i", 1)                        # one filter
    body += struct.pack(">b", 2)                       # resource type TOPIC
    body += struct.pack(">h", len("payments")) + b"payments"
    body += struct.pack(">b", 3)                       # pattern LITERAL
    body += struct.pack(">h", -1)                      # principal: any
    body += struct.pack(">h", -1)                      # host: any
    body += struct.pack(">b", 1)                       # operation ANY
    body += struct.pack(">b", 1)                       # permission ANY
    conn.send(DELETE_ACLS, 1, 313, body)
    resp = conn.recv()

    pos = 4 + 4                                        # correlation id, throttle_time_ms
    (filters,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert filters == 1, filters
    (error,) = struct.unpack_from(">h", resp, pos)
    pos += 2
    assert error == 0, f"filter reported error {error}"
    _msg, pos = read_legacy_string(resp, pos)
    (matching,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert matching == 2, f"expected both rules reported, got {matching}"

    principals = []
    for _ in range(matching):
        pos += 2                                       # error_code
        _m, pos = read_legacy_string(resp, pos)        # error_message
        pos += 1                                       # resource_type
        _name, pos = read_legacy_string(resp, pos)
        pos += 1                                       # pattern_type
        principal, pos = read_legacy_string(resp, pos)
        _host, pos = read_legacy_string(resp, pos)
        pos += 2                                       # operation, permission
        principals.append(principal)

    assert sorted(principals) == ["User:alice", "User:bob"], principals
    assert sql("SELECT count(*) FROM kafgres_acls") == "0", "the rules were not deleted"

def test_managing_acls_needs_cluster_alter_not_a_topic_grant(topic):
    """A principal with every grant on a topic still may not write ACLs.

    Kafka scopes these RPCs to the cluster, and so does this — for a reason worth stating:
    a principal able to write rules for one topic can write itself a rule for every other,
    so a per-resource grant here would be a grant of everything wearing a narrower label.
    """
    grant("ALL", "TOPIC", topic)

    out = kafka_acls("--add", "--allow-principal", "User:mallory",
                     "--operation", "Read", "--topic", topic)
    assert denied(out), f"a topic grant was enough to write ACLs: {out.stdout}{out.stderr}"
    assert sql(
        f"SELECT count(*) FROM kafgres_acls WHERE principal = 'User:mallory'"
    ) == "0", "the rule was stored despite the refusal"

    listing = kafka_acls("--list")
    assert denied(listing), (
        f"a topic grant was enough to read every ACL: {listing.stdout}{listing.stderr}"
    )

def test_the_wire_refuses_an_untyped_principal_like_sql_does(open_broker):
    """A rule that cannot match is worse than no rule, because it reports success: the
    checker compares against `User:alice`, so a bare `alice` never fires — an operator
    gets a success transcript for a DENY that prohibits nothing."""
    body = struct.pack(">i", 1)
    body += struct.pack(">b", 2)                        # TOPIC
    body += struct.pack(">h", len("secrets")) + b"secrets"
    body += struct.pack(">b", 3)                        # LITERAL
    body += struct.pack(">h", len("alice")) + b"alice"  # untyped, deliberately
    body += struct.pack(">h", 1) + b"*"
    body += struct.pack(">b", 3)                        # READ
    body += struct.pack(">b", 2)                        # DENY
    error, message = _create_acl(body)
    assert error == 42, f"an untyped principal was accepted: error {error}"
    assert "typed" in (message or "").lower(), message
    assert sql("SELECT count(*) FROM kafgres_acls") == "0", "the unmatched rule was stored"

def test_the_wire_refuses_an_empty_resource_name(open_broker):
    """PREFIXED with an empty name grants everything.

    Every name starts with the empty string, so `starts_with(name, '')` is always true and
    the rule reads as a grant over the whole cluster. It is also nearly unrevokable over the
    wire, because an empty name in a *filter* means "any" — so no DeleteAcls filter can name
    it. Kafka rejects an empty name at creation; so does this.
    """
    body = struct.pack(">i", 1)
    body += struct.pack(">b", 2)
    body += struct.pack(">h", 0)                        # empty resource name
    body += struct.pack(">b", 4)                        # PREFIXED
    body += struct.pack(">h", len("User:bob")) + b"User:bob"
    body += struct.pack(">h", 1) + b"*"
    body += struct.pack(">b", 2)                        # ALL
    body += struct.pack(">b", 3)                        # ALLOW
    error, _ = _create_acl(body)
    assert error == 42, f"an empty prefixed name was accepted: error {error}"
    assert sql("SELECT count(*) FROM kafgres_acls") == "0"

def test_a_match_filter_finds_the_rules_that_govern_a_name(acl_admin):
    """`MATCH` means the literal, the wildcard, and every covering prefix.

    That is how `kafka-acls.sh --resource-pattern-type match` revokes everything governing a
    topic. An exact `resource_name =` comparison matches *none* of them, so the remove
    deletes nothing, reports NONE with an empty match list, and the tool prints success — a
    revoke that succeeds without revoking.
    """
    for name, pattern in (("app-orders", "LITERAL"), ("app-", "PREFIXED"),
                          ("*", "LITERAL"), ("other", "LITERAL")):
        sql(f"SELECT kafgres_add_acl('User:bob', 'READ', 'TOPIC', '{name}', 'ALLOW', '{pattern}')")

    out = kafka_acls("--list", "--topic", "app-orders",
                     "--resource-pattern-type", "match").stdout
    assert "app-orders" in out and "app-" in out and "*" in out, (
        f"a MATCH filter missed rules governing the name: {out}"
    )
    assert "other" not in out, f"a MATCH filter returned an unrelated rule: {out}"

def _create_acl(body):
    """Send one CreateAcls v1 and return (per-creation error code, message).

    Raw because `kafka-acls.sh` validates these client-side and will not send them — which
    is the point: the broker cannot rely on every client doing that.
    """
    import socket as _socket
    sock = _socket.create_connection(("127.0.0.1", 9092), timeout=10)
    try:
        header = struct.pack(">hhi", CREATE_ACLS, 1, 606) + struct.pack(">h", 6) + b"pytest"
        frame = header + body
        sock.sendall(struct.pack(">i", len(frame)) + frame)
        size = struct.unpack(">i", _recv_exactly(sock, 4))[0]
        resp = _recv_exactly(sock, size)
    finally:
        sock.close()
    pos = 4 + 4                                          # correlation id, throttle
    (count,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert count == 1, count
    (error,) = struct.unpack_from(">h", resp, pos)
    pos += 2
    message, _ = read_legacy_string(resp, pos)
    return error, message

def _recv_exactly(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf += chunk
    return buf
