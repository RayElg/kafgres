"""TLS and mTLS.

Certificates come from `tests/tls/generate.sh`, which is not checked in — a repository
with a private key in it teaches the wrong habit, and these expire. The whole module
skips if it has not been run.

Note the shape of the failure these guard against: a broker that is *supposed* to be
serving TLS but is not looks completely healthy. Clients connect, everything works, and
the credentials go over the wire in the clear. So the assertions are about refusal at
least as much as about success.
"""

import os
import subprocess
import time

import pytest

from conftest import sql

CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
CERT_DIR = os.path.join(REPO, "tests", "tls")
NEEDED = ["ca.crt", "server.crt", "server.key", "client.crt", "client.key",
          "rogue.crt", "rogue.key"]

pytestmark = pytest.mark.skipif(
    not all(os.path.exists(os.path.join(CERT_DIR, f)) for f in NEEDED),
    reason="run tests/tls/generate.sh first",
)

def reload_conf():
    """`ALTER SYSTEM` cannot run inside a transaction block, and `psql -c "A; B"` wraps
    both statements in one — so every setting here is its own statement."""
    sql("SELECT pg_reload_conf()")
    time.sleep(1.5)

def set_guc(name, value):
    sql(f"ALTER SYSTEM SET {name} = {value}")

def reset_gucs(*names):
    for name in names:
        sql(f"ALTER SYSTEM RESET {name}")

def kcat(*args, timeout=90, ca=True, client_cert=None, stdin=None):
    opts = ["-X", "security.protocol=SSL"]
    if ca:
        opts += ["-X", "ssl.ca.location=/tls/ca.crt"]
    if client_cert is not None:
        cert, key = client_cert
        opts += ["-X", f"ssl.certificate.location=/tls/{cert}",
                 "-X", f"ssl.key.location=/tls/{key}"]
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host",
         "-v", f"{os.path.abspath(CERT_DIR)}:/tls:ro", CLIENTS,
         "kcat", "-b", BROKER, *opts, *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

def leaked_sockets():
    """Broker sockets in CLOSE_WAIT, counted from the OS.

    Asking the broker would mean trusting the thing under test. CLOSE_WAIT (`08`) is
    exactly the leak: the peer sent FIN and we never closed. Counting everything that is
    not the listener does not work — a connection the broker closes *correctly* sits in
    TIME_WAIT (`06`) for a minute afterwards, so that measure reports a clean shutdown
    as a leak and passes only because it also reports the leak.
    """
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "sh", "-c",
         "grep ':2384' /proc/net/tcp | grep -c ' 08 ' || true"],
        capture_output=True, text=True, timeout=60, cwd=REPO,
    )
    return int(out.stdout.strip() or 0)

def connected(out):
    return "broker 1 at" in out.stdout

@pytest.fixture(scope="module")
def tls_server():
    """Server-side TLS only: no CA, so client certificates are neither asked for nor
    accepted."""
    set_guc("kafgres.tls_cert_file", "'/tls/server.crt'")
    set_guc("kafgres.tls_key_file", "'/tls/server.key'")
    reload_conf()
    yield
    reset_gucs("kafgres.tls_cert_file", "kafgres.tls_key_file")
    reload_conf()

@pytest.fixture
def mutual_tls(tls_server):
    """Client certificates required and verified against our CA."""
    set_guc("kafgres.tls_ca_file", "'/tls/ca.crt'")
    set_guc("kafgres.tls_client_cert_required", "on")
    reload_conf()
    yield
    reset_gucs("kafgres.tls_ca_file", "kafgres.tls_client_cert_required")
    reload_conf()

def test_a_client_connects_over_tls(tls_server):
    out = kcat("-L")
    assert connected(out), out.stdout + out.stderr

def test_produce_and_consume_over_tls(tls_server):
    """The handshake completing is not the same as the transport working. A TLS record
    can carry a partial Kafka frame and a Kafka frame can span records, so the only
    convincing check is data through it."""
    topic = "p5tls-roundtrip"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    try:
        assert kcat("-t", topic, "-P", stdin="a\nb\nc\n").returncode == 0
        back = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q")
        assert back.stdout.split() == ["a", "b", "c"], back.stdout + back.stderr
    finally:
        sql(f"SELECT kafgres_drop_topic('{topic}')")

def test_a_large_response_survives_tls_record_boundaries(tls_server):
    """A response bigger than one TLS record, read back through the transport.

    rustls hands out plaintext a record at a time, so a Fetch response spanning several
    records only reassembles if partial reads are carried across ticks. A three-record
    round trip would pass whether or not that works.
    """
    topic = "p5tls-large"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    try:
        payload = "\n".join("x" * 900 for _ in range(400)) + "\n"
        assert kcat("-t", topic, "-P", stdin=payload, timeout=180).returncode == 0
        back = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q", timeout=180)
        assert len(back.stdout.splitlines()) == 400, len(back.stdout.splitlines())
    finally:
        sql(f"SELECT kafgres_drop_topic('{topic}')")

def test_a_plaintext_client_cannot_talk_to_a_tls_listener(tls_server):
    """The listener is TLS-only. A plaintext client must fail rather than be served,
    because the failure mode on the other side of this is a broker everyone believes is
    encrypted and is not."""
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER, "-L"],
        capture_output=True, text=True, timeout=90,
    )
    assert "broker 1 at" not in out.stdout, out.stdout

def test_mtls_accepts_a_certificate_signed_by_our_ca(mutual_tls):
    out = kcat("-L", client_cert=("client.crt", "client.key"))
    assert connected(out), out.stdout + out.stderr

def test_mtls_refuses_a_client_with_no_certificate(mutual_tls):
    out = kcat("-L")
    assert not connected(out), "a client with no certificate was admitted"

def test_mtls_refuses_a_certificate_from_another_ca(mutual_tls):
    """Signed by a well-formed CA that is simply not ours. Verifying the certificate's
    structure without verifying its issuer would admit this."""
    out = kcat("-L", client_cert=("rogue.crt", "rogue.key"))
    assert not connected(out), "a certificate from an untrusted CA was admitted"

def test_the_principal_is_the_subject_dn(mutual_tls):
    """Kafka's SSL principal is the X.500 subject in RFC 2253 form, which is what any
    ACL written against an mTLS client is keyed on. The CN alone would look fine right up
    until someone wrote one."""
    assert connected(kcat("-L", client_cert=("client.crt", "client.key")))
    logs = subprocess.run(
        ["docker", "compose", "logs", "--since", "60s", "postgres"],
        capture_output=True, text=True, timeout=60, cwd=REPO,
    ).stdout
    assert "presented certificate 'CN=alice, O=kafgres'" in logs, (
        "expected the full subject DN in the log, got:\n"
        + "\n".join(l for l in logs.splitlines() if "certificate" in l)
    )

def test_a_client_certificate_satisfies_the_auth_gate(mutual_tls):
    """`SSL` as a listener security protocol means the certificate *is* the
    authentication. Demanding SASL as well would make mTLS unusable — no Kafka client
    offers both — so a verified certificate has to stand on its own."""
    set_guc("kafgres.sasl_required", "on")
    reload_conf()
    try:
        out = kcat("-L", client_cert=("client.crt", "client.key"))
        assert connected(out), (
            "a verified client certificate did not satisfy sasl_required: "
            + out.stdout + out.stderr
        )
    finally:
        reset_gucs("kafgres.sasl_required")
        reload_conf()

def test_a_cleanly_closed_tls_connection_is_reaped(tls_server):
    """A clean TLS disconnect has to actually close the connection.

    rustls returns `Ok(0)` from its reader for exactly one reason — the peer sent
    close_notify — and once that has landed `wants_read()` is false forever, so the TCP
    FIN behind it is never observed either. Treating that `Ok(0)` as "no data yet" leaks
    the `Conn` permanently. Nothing reaps it: the deadline sweep only runs under
    `sasl_required`, flush succeeds because there is nothing to write, and there is no
    idle timeout by design — at the 512-connection ceiling the broker stops accepting.

    The shutdown has to be a *clean* one: `kcat` exits without close_notify, so the
    socket EOF is seen the ordinary way and the leak never shows. This does what the
    Java client and librdkafka's own `SSL_shutdown` do.
    """
    import socket as _socket
    import ssl as _ssl
    import struct as _struct

    before = leaked_sockets()
    for _ in range(5):
        ctx = _ssl.create_default_context(cafile=os.path.join(CERT_DIR, "ca.crt"))
        raw = _socket.create_connection(("127.0.0.1", 9092), timeout=15)
        sock = ctx.wrap_socket(raw, server_hostname="localhost")
        try:
            header = _struct.pack(">hhi", 18, 0, 1) + _struct.pack(">h", 6) + b"pytest"
            sock.sendall(_struct.pack(">i", len(header)) + header)
            size = int.from_bytes(read_exactly(sock, 4), "big")
            read_exactly(sock, size)
            sock.settimeout(2)
            try:
                sock.unwrap()
            except (OSError, _ssl.SSLError):
                pass
        finally:
            raw.close()

    time.sleep(3)
    after = leaked_sockets()
    assert after <= before, (
        f"{after - before} connections left in CLOSE_WAIT after five clean TLS shutdowns"
    )

def test_a_reader_that_stops_reading_is_not_disconnected(tls_server):
    """Backpressure is not death.

    rustls caps its own plaintext buffer at 64 KiB and reports a full one as `Ok(0)` from
    `writer()`. The caller treats `Ok(0)` from a socket as fatal, so passing it straight
    through drops any consumer that reads slowly — mid-response, silently, in a permanent
    refetch loop. It has to read as `WouldBlock`, so the response stays in `outbuf` and is
    retried next tick under the existing 8 MiB ceiling.

    A client that merely reads *slowly* does not provoke this reliably — the socket keeps
    draining. This one asks for a megabyte and then stops reading entirely, with a small
    receive buffer, so the broker's send buffer and then rustls's own buffer both fill.
    """
    import socket as _socket
    import ssl as _ssl
    import struct as _struct

    topic = "p5tls-backpressure"
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("ALTER SYSTEM RESET kafgres.segment_offsets")
    sql("SELECT pg_reload_conf()")
    time.sleep(2)
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    try:
        payload = "\n".join("y" * 4000 for _ in range(400)) + "\n"
        assert kcat("-t", topic, "-P", stdin=payload, timeout=240).returncode == 0

        deadline = time.time() + 60
        hw = 0
        while time.time() < deadline:
            probe = kcat("-Q", "-t", f"{topic}:0:-1", timeout=30)
            for line in probe.stdout.splitlines():
                if "offset" in line:
                    hw = int(line.rsplit(" ", 1)[1])
            if hw >= 400:
                break
            time.sleep(1)
        assert hw >= 400, f"only {hw} of 400 records readable; the produce did not land"

        ctx = _ssl.create_default_context(cafile=os.path.join(CERT_DIR, "ca.crt"))
        raw = _socket.create_connection(("127.0.0.1", 9092), timeout=30)
        raw.setsockopt(_socket.SOL_SOCKET, _socket.SO_RCVBUF, 4096)
        sock = ctx.wrap_socket(raw, server_hostname="localhost")
        try:
            body = _struct.pack(">iiii", -1, 0, 1, 8 * 1024 * 1024)
            body += _struct.pack(">b", 0)                       # isolation_level
            body += _struct.pack(">i", 1)                       # topics
            body += _struct.pack(">h", len(topic)) + topic.encode()
            body += _struct.pack(">i", 1)                       # partitions
            body += _struct.pack(">iqi", 0, 0, 1024 * 1024)
            header = _struct.pack(">hhi", 1, 4, 77) + _struct.pack(">h", 6) + b"pytest"
            frame = header + body
            sock.sendall(_struct.pack(">i", len(frame)) + frame)

            time.sleep(5)

            sock.settimeout(60)
            size = int.from_bytes(read_exactly(sock, 4), "big")
            got = read_exactly(sock, size)
            assert len(got) == size
            settings = sql("SHOW kafgres.segment_bytes") + "/" + sql("SHOW kafgres.segment_offsets")
            assert size > 65536, (
                f"response was only {size} bytes (~{size // 4000} of 400 records, hw={hw}) "
                f"with segment_bytes/segment_offsets={settings}. The backpressure this test "
                "is about needs a response over rustls's 64 KiB buffer, so this run did not "
                "exercise it — look at why the fetch was short, and at whether it stopped on "
                "a segment boundary, before looking at backpressure."
            )
        finally:
            sock.close()
    finally:
        sql(f"SELECT kafgres_drop_topic('{topic}')")

def read_exactly(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise AssertionError(
                f"broker closed the connection after {len(buf)} of {n} bytes; "
                "backpressure was treated as a fatal write"
            )
        buf += chunk
    return buf

def test_a_broken_certificate_path_does_not_downgrade_the_listener(tls_server):
    """The one outcome worse than refusing a bad configuration is accepting it.

    A reload that cannot build a TLS config must keep the previous one. Falling back to
    plaintext because someone mistyped a path puts credentials on the wire while every
    client still reports success.
    """
    set_guc("kafgres.tls_cert_file", "'/tls/does-not-exist.crt'")
    reload_conf()
    try:
        assert connected(kcat("-L")), "the listener downgraded on a bad reload"
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER, "-L"],
            capture_output=True, text=True, timeout=90,
        )
        assert "broker 1 at" not in out.stdout, "plaintext was accepted after a bad reload"
    finally:
        set_guc("kafgres.tls_cert_file", "'/tls/server.crt'")
        reload_conf()
