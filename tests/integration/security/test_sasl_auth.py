"""SASL/SCRAM-SHA-256 against Postgres roles.

The acceptance criterion is "an authenticated client connects; an unauthenticated one is
rejected with the correct error code", so these drive real clients — and both of them,
because librdkafka and the Java client do not agree on how to echo the SCRAM nonce and a
broker that only satisfies one of them is not done.

Credentials are Postgres roles. There is no separate credential store: `pg_authid` holds
the SCRAM verifier, which is exactly the `StoredKey`/`ServerKey` pair an RFC 5802 server
needs, so the password never crosses the wire and the broker never holds a plaintext.
"""

import os
import subprocess
import tempfile
import time

import pytest

from conftest import sql

CLIENTS = "kafgres-clients"
KAFKA = "apache/kafka:4.1.0"
BROKER = "127.0.0.1:9092"

USER = "kafgres_test_user"
PASSWORD = "kafgres_test_pass"

def kcat(*args, stdin=None, timeout=90, sasl=None):
    auth = []
    if sasl is not None:
        user, password = sasl
        auth = [
            "-X", "security.protocol=SASL_PLAINTEXT",
            "-X", "sasl.mechanisms=SCRAM-SHA-256",
            "-X", f"sasl.username={user}",
            "-X", f"sasl.password={password}",
        ]
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, *auth, *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

def java_tool(script, *args, sasl=None, timeout=180):
    """Run a Java tool, optionally with a JAAS config for SCRAM."""
    if sasl is None:
        return subprocess.run(
            ["docker", "run", "--rm", "--network", "host", KAFKA,
             f"/opt/kafka/bin/{script}", "--bootstrap-server", BROKER, *args],
            capture_output=True, text=True, timeout=timeout,
        )
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
             f"/opt/kafka/bin/{script}", "--bootstrap-server", BROKER,
             "--command-config", "/w/sasl.properties", *args],
            capture_output=True, text=True, timeout=timeout,
        )

@pytest.fixture(scope="module", autouse=True)
def sasl_enabled():
    """Turn authentication on for this module only.

    Every other test file assumes it is off, which is also the shipped default — turning
    it on by default would lock out an existing deployment on upgrade.
    """
    sql("SET password_encryption='scram-sha-256'; "
        f"DROP ROLE IF EXISTS {USER}; CREATE ROLE {USER} LOGIN PASSWORD '{PASSWORD}'")
    sql("SET password_encryption='scram-sha-256'; "
        f"DROP ROLE IF EXISTS {USER}_nologin; "
        f"CREATE ROLE {USER}_nologin NOLOGIN PASSWORD '{PASSWORD}'")
    sql("ALTER SYSTEM SET kafgres.sasl_required = on")
    sql("SELECT pg_reload_conf()")
    time.sleep(1)
    yield
    sql("ALTER SYSTEM RESET kafgres.sasl_required")
    sql("SELECT pg_reload_conf()")
    sql(f"DROP ROLE IF EXISTS {USER}")
    sql(f"DROP ROLE IF EXISTS {USER}_nologin")
    time.sleep(1)

@pytest.fixture
def topic(request):
    name = f"p5a-{request.node.name.replace('_', '-')[:36]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def test_an_unauthenticated_client_is_refused():
    """No error code exists for "you have not authenticated", so the connection is
    closed — which is what upstream does. Inventing a code would teach the client to
    retry a request that can never succeed."""
    out = kcat("-L", timeout=90)
    assert "ret-test" not in out.stdout
    combined = out.stdout + out.stderr
    assert "SASL" in combined or "Disconnected" in combined, combined

def test_an_authenticated_librdkafka_client_connects():
    out = kcat("-L", sasl=(USER, PASSWORD))
    assert out.returncode == 0, out.stderr
    assert "broker 1 at" in out.stdout, out.stdout + out.stderr

def test_an_authenticated_java_client_connects():
    """Both clients, because they disagree about the nonce echo.

    RFC 5802 says client-final repeats the combined nonce the server sent, and the Java
    client does. librdkafka echoes `cnonce + <the whole r= we sent>`, so the client nonce
    appears twice. Real Kafka never notices because it does not compare the echo at all —
    it rebuilds the auth message from what it received. A broker strict enough to reject
    librdkafka passes this test's Java half and is still broken.
    """
    out = java_tool("kafka-topics.sh", "--list", sasl=(USER, PASSWORD))
    assert out.returncode == 0, out.stdout + out.stderr

def test_produce_and_consume_authenticated(topic):
    assert kcat("-t", topic, "-P", stdin="a\nb\nc\n", sasl=(USER, PASSWORD)).returncode == 0
    back = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q", sasl=(USER, PASSWORD))
    assert back.stdout.split() == ["a", "b", "c"], back.stdout + back.stderr

def failure_reason(out):
    return (out.stdout + out.stderr).lower()

def test_a_wrong_password_is_refused():
    out = kcat("-L", sasl=(USER, "not-the-password"))
    assert "broker 1 at" not in out.stdout
    assert "authentication failed" in failure_reason(out), failure_reason(out)

def test_an_unknown_user_looks_exactly_like_a_wrong_password():
    """No user-enumeration oracle.

    Answering an unknown name differently — an error at client-first rather than a
    challenge — lets an unauthenticated attacker map the role table one round trip at a
    time. The exchange runs to completion against a fixed dummy verifier instead, so both
    fail at the same step with the same message.
    """
    unknown = kcat("-L", sasl=("no-such-role-at-all", "whatever"))
    wrong = kcat("-L", sasl=(USER, "not-the-password"))

    def step_and_reason(out):
        text = out.stdout + out.stderr
        for marker in ("authentication failed: ", "Unsupported SASL mechanism"):
            if marker in text:
                return text.split(marker, 1)[1].split("(")[0].strip()
        return "no failure"

    assert step_and_reason(unknown) == step_and_reason(wrong) != "no failure", (
        f"unknown user: {step_and_reason(unknown)!r}, "
        f"wrong password: {step_and_reason(wrong)!r}"
    )

def test_a_role_without_login_is_refused():
    """Revocation in Postgres has to apply to Kafka clients too. A broker that keeps
    honouring a role the DBA has disabled makes `NOLOGIN` mean nothing."""
    out = kcat("-L", sasl=(f"{USER}_nologin", PASSWORD))
    assert "broker 1 at" not in out.stdout
    assert "authentication failed" in failure_reason(out), failure_reason(out)

def test_the_mock_salt_is_indistinguishable_from_a_real_one():
    """The oracle that sits *before* the proof step.

    `server-first` sends `s=<salt>` in the clear, so a mock salt has to match a real one
    in every observable way or an attacker maps the role table with one round trip per
    name. Postgres always generates 16 bytes, so any other length is an existence bit,
    and so is one constant for every unknown name. Read off the wire, where the leak is.
    """
    import base64
    import re
    import struct

    def server_first_salt(username):
        import socket as _socket

        from conftest import BROKER_HOST, BROKER_PORT, Connection

        sock = _socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=10)
        conn = Connection(sock)
        try:
            header = struct.pack(">hhi", 17, 1, 1) + struct.pack(">h", 6) + b"pytest"
            body = struct.pack(">h", len("SCRAM-SHA-256")) + b"SCRAM-SHA-256"
            frame = header + body
            conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
            resp = conn.recv()
            assert struct.unpack_from(">h", resp, 4)[0] == 0, "handshake refused"

            first = f"n,,n={username},r=kafgrestestnonce0123456789".encode()
            header = struct.pack(">hhi", 36, 1, 2) + struct.pack(">h", 6) + b"pytest"
            body = struct.pack(">i", len(first)) + first
            frame = header + body
            conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
            resp = conn.recv()
            (err,) = struct.unpack_from(">h", resp, 4)
            assert err == 0, f"client-first refused with {err}"
            (msg_len,) = struct.unpack_from(">h", resp, 6)
            pos = 8 if msg_len < 0 else 8 + msg_len
            (n,) = struct.unpack_from(">i", resp, pos)
            challenge = resp[pos + 4 : pos + 4 + n].decode()
            m = re.search(r"s=([^,]+)", challenge)
            assert m, challenge
            return base64.b64decode(m.group(1))
        finally:
            sock.close()

    real = server_first_salt(USER)
    ghost_a = server_first_salt("definitely-no-such-role-a")
    ghost_b = server_first_salt("definitely-no-such-role-b")
    ghost_a_again = server_first_salt("definitely-no-such-role-a")

    assert len(real) == 16, f"a real Postgres salt is 16 bytes, got {len(real)}"
    assert len(ghost_a) == 16, (
        f"mock salt is {len(ghost_a)} bytes; any length but 16 is an existence oracle"
    )
    assert ghost_a != ghost_b, "one constant for every unknown name is an oracle"
    assert ghost_a == ghost_a_again, "a mock salt that changes per attempt is an oracle"

def scram_client_messages(username, password, server_first, client_first_bare):
    """The client half of RFC 5802, so the v0 path can be driven without a client."""
    import base64
    import hashlib
    import hmac as _hmac

    fields = dict(f.split("=", 1) for f in server_first.split(",") if "=" in f[:2])
    combined, salt, iters = fields["r"], base64.b64decode(fields["s"]), int(fields["i"])

    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iters, 32)
    client_key = _hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()

    without_proof = f"c=biws,r={combined}"
    auth_message = f"{client_first_bare},{server_first},{without_proof}"
    client_sig = _hmac.new(stored_key, auth_message.encode(), hashlib.sha256).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, client_sig))
    return f"{without_proof},p={base64.b64encode(proof).decode()}"

def test_the_v0_handshake_uses_unwrapped_tokens(topic):
    """KIP-152's other framing, and the one nothing else here exercises.

    A v0 handshake means the SASL tokens that follow are bare length-prefixed blobs with
    no Kafka request header, and the replies are the same. Advertising v0 while only
    implementing the wrapped form confirms the mechanism and then fails to parse the
    first token. Dropping v0 from the advertised range is not the escape: librdkafka
    reads its presence as "this broker does SASL at all" and refuses outright. sarama
    defaults to this version.
    """
    import socket as _socket
    import struct as _struct

    from conftest import BROKER_HOST, BROKER_PORT

    sock = _socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=10)
    try:
        def send_frame(payload):
            sock.sendall(_struct.pack(">i", len(payload)) + payload)

        def recv_frame():
            (size,) = _struct.unpack(">i", exactly(4))
            return exactly(size)

        def exactly(n):
            buf = b""
            while len(buf) < n:
                chunk = sock.recv(n - len(buf))
                if not chunk:
                    raise ConnectionError("peer closed")
                buf += chunk
            return buf

        header = _struct.pack(">hhi", 17, 0, 1) + _struct.pack(">h", 6) + b"pytest"
        send_frame(header + _struct.pack(">h", 13) + b"SCRAM-SHA-256")
        resp = recv_frame()
        assert _struct.unpack_from(">h", resp, 4)[0] == 0, "v0 handshake refused"

        bare = "n=" + USER + ",r=kafgresrawnonce0123456789"
        send_frame(("n,," + bare).encode())
        server_first = recv_frame().decode()
        assert server_first.startswith("r="), server_first

        send_frame(scram_client_messages(USER, PASSWORD, server_first, bare).encode())
        server_final = recv_frame().decode()
        assert server_final.startswith("v="), server_final

        header = _struct.pack(">hhi", 3, 1, 2) + _struct.pack(">h", 6) + b"pytest"
        send_frame(header + _struct.pack(">i", 0))
        assert len(recv_frame()) > 0, "metadata after v0 auth failed"
    finally:
        sock.close()

def test_repeated_bad_proofs_close_the_connection():
    """A failed proof leaves the exchange at client-final, so the same challenge is
    retryable on one socket forever. Upstream gets a limit for free by killing the
    connection on any failure; without one this is an unauthenticated, unmetered
    password-guessing loop that also costs a transaction per attempt."""
    import socket as _socket
    import struct as _struct

    from conftest import BROKER_HOST, BROKER_PORT, Connection

    sock = _socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=10)
    conn = Connection(sock)
    try:
        header = _struct.pack(">hhi", 17, 1, 1) + _struct.pack(">h", 6) + b"pytest"
        body = _struct.pack(">h", 13) + b"SCRAM-SHA-256"
        sock.sendall(_struct.pack(">i", len(header + body)) + header + body)
        conn.recv()

        closed = False
        for i in range(6):
            first = f"n,,n={USER},r=nonce{i}0123456789abcdef".encode()
            header = _struct.pack(">hhi", 36, 1, 10 + i) + _struct.pack(">h", 6) + b"pytest"
            body = _struct.pack(">i", len(first)) + first
            try:
                sock.sendall(_struct.pack(">i", len(header + body)) + header + body)
                conn.recv()
                bad = b"c=biws,r=wrong,p=" + b"A" * 43 + b"="
                header = _struct.pack(">hhi", 36, 1, 100 + i) + _struct.pack(">h", 6) + b"pytest"
                body = _struct.pack(">i", len(bad)) + bad
                sock.sendall(_struct.pack(">i", len(header + body)) + header + body)
                conn.recv()
            except (ConnectionError, BrokenPipeError, OSError):
                closed = True
                break
        assert closed or conn.closed(timeout=5), "unlimited guesses on one connection"
    finally:
        sock.close()

def test_an_unsupported_mechanism_reports_what_we_support():
    """The mechanism list is returned on rejection as well as acceptance — that is how a
    client discovers what to retry with. Omitting it turns a recoverable mismatch into a
    dead end."""
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER, "-L",
         "-X", "security.protocol=SASL_PLAINTEXT", "-X", "sasl.mechanisms=PLAIN",
         "-X", f"sasl.username={USER}", "-X", f"sasl.password={PASSWORD}"],
        capture_output=True, text=True, timeout=90,
    )
    text = out.stdout + out.stderr
    assert "Unsupported SASL mechanism" in text, text
    assert "SCRAM-SHA-256" in text, "the supported list must come back with the rejection"
