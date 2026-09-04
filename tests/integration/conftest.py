"""Shared fixtures for kafgres integration tests.

Assumes `docker compose up -d` has the broker running.
"""

import os
import socket
import struct
import subprocess
import time

import pytest

BROKER_HOST = os.environ.get("KAFGRES_HOST", "127.0.0.1")
BROKER_PORT = int(os.environ.get("KAFGRES_PORT", "9092"))
PSQL = ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-tAc"]

API_VERSIONS = 18
METADATA = 3
PRODUCE = 0
FETCH = 1
LIST_OFFSETS = 2
OFFSET_COMMIT = 8
OFFSET_FETCH = 9
FIND_COORDINATOR = 10
JOIN_GROUP = 11
HEARTBEAT = 12
LEAVE_GROUP = 13
SYNC_GROUP = 14
DESCRIBE_GROUPS = 15
LIST_GROUPS = 16
INIT_PRODUCER_ID = 22
CREATE_TOPICS = 19
DELETE_TOPICS = 20
DELETE_RECORDS = 21
DESCRIBE_CONFIGS = 32
CREATE_PARTITIONS = 37
DELETE_GROUPS = 42
INCREMENTAL_ALTER_CONFIGS = 44
DESCRIBE_CLUSTER = 60
SASL_HANDSHAKE = 17
SASL_AUTHENTICATE = 36
OFFSET_FOR_LEADER_EPOCH = 23
OFFSET_DELETE = 47
ALTER_CONFIGS = 33
DESCRIBE_LOG_DIRS = 35
ELECT_LEADERS = 43
LIST_PARTITION_REASSIGNMENTS = 46
DESCRIBE_CLIENT_QUOTAS = 48
DESCRIBE_USER_SCRAM_CREDENTIALS = 50
CONSUMER_GROUP_HEARTBEAT = 68
CONSUMER_GROUP_DESCRIBE = 69
WRITE_TXN_MARKERS = 27
DESCRIBE_TOPIC_PARTITIONS = 75
SHARE_GROUP_HEARTBEAT = 76
SHARE_GROUP_DESCRIBE = 77
SHARE_FETCH = 78
SHARE_ACKNOWLEDGE = 79
ALTER_CLIENT_QUOTAS = 49
ALTER_USER_SCRAM_CREDENTIALS = 51
DESCRIBE_PRODUCERS = 61
DESCRIBE_TRANSACTIONS = 65
LIST_TRANSACTIONS = 66
DESCRIBE_ACLS = 29
CREATE_ACLS = 30
DELETE_ACLS = 31

ADD_PARTITIONS_TO_TXN = 24
ADD_OFFSETS_TO_TXN = 25
END_TXN = 26
TXN_OFFSET_COMMIT = 28

ADVERTISED = {
    ADD_PARTITIONS_TO_TXN: (0, 3),
    ADD_OFFSETS_TO_TXN: (0, 3),
    END_TXN: (0, 3),
    TXN_OFFSET_COMMIT: (0, 3),
    PRODUCE: (0, 13),
    FETCH: (4, 18),
    LIST_OFFSETS: (1, 6),
    METADATA: (0, 13),
    OFFSET_COMMIT: (2, 10),
    OFFSET_FETCH: (1, 10),
    FIND_COORDINATOR: (0, 6),
    JOIN_GROUP: (0, 9),
    HEARTBEAT: (0, 4),
    LEAVE_GROUP: (0, 5),
    SYNC_GROUP: (0, 5),
    DESCRIBE_GROUPS: (0, 6),
    LIST_GROUPS: (0, 5),
    INIT_PRODUCER_ID: (0, 5),
    API_VERSIONS: (0, 4),
    CREATE_TOPICS: (2, 7),
    DELETE_TOPICS: (1, 6),
    DELETE_RECORDS: (0, 2),
    DESCRIBE_CONFIGS: (1, 4),
    CREATE_PARTITIONS: (0, 3),
    DELETE_GROUPS: (0, 2),
    INCREMENTAL_ALTER_CONFIGS: (0, 1),
    DESCRIBE_CLUSTER: (0, 2),
    SASL_HANDSHAKE: (0, 1),
    SASL_AUTHENTICATE: (0, 2),
    OFFSET_FOR_LEADER_EPOCH: (2, 4),
    OFFSET_DELETE: (0, 0),
    DESCRIBE_ACLS: (1, 3),
    CREATE_ACLS: (1, 3),
    DELETE_ACLS: (1, 3),
    ALTER_CONFIGS: (0, 2),
    DESCRIBE_LOG_DIRS: (1, 5),
    ELECT_LEADERS: (1, 2),
    LIST_PARTITION_REASSIGNMENTS: (0, 0),
    DESCRIBE_CLIENT_QUOTAS: (0, 1),
    ALTER_CLIENT_QUOTAS: (0, 1),
    ALTER_USER_SCRAM_CREDENTIALS: (0, 0),
    DESCRIBE_USER_SCRAM_CREDENTIALS: (0, 0),
    CONSUMER_GROUP_HEARTBEAT: (0, 0),
    CONSUMER_GROUP_DESCRIBE: (0, 0),
    WRITE_TXN_MARKERS: (1, 1),
    DESCRIBE_TOPIC_PARTITIONS: (0, 0),
    SHARE_GROUP_HEARTBEAT: (1, 1),
    SHARE_GROUP_DESCRIBE: (1, 1),
    SHARE_FETCH: (1, 1),
    SHARE_ACKNOWLEDGE: (1, 1),
    DESCRIBE_PRODUCERS: (0, 0),
    DESCRIBE_TRANSACTIONS: (0, 0),
    LIST_TRANSACTIONS: (0, 0),
}

ADVERTISED_HOST = "127.0.0.1"

def sql(statement):
    """Run a statement against the broker's database."""
    out = subprocess.run(
        PSQL + [statement], capture_output=True, text=True, timeout=30
    )
    if out.returncode != 0:
        raise RuntimeError(f"psql failed: {out.stderr.strip()}")
    return out.stdout.strip()

@pytest.fixture(scope="session", autouse=True)
def broker_ready():
    """Fail fast with a useful message rather than a socket timeout per test."""
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=2):
                return
        except OSError:
            time.sleep(1)
    pytest.fail(
        f"no kafgres listener on {BROKER_HOST}:{BROKER_PORT} — run `docker compose up -d`"
    )

@pytest.fixture
def conn():
    s = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=10)
    yield Connection(s)
    s.close()

class Connection:
    """Raw framing, so tests assert bytes rather than a client library's
    interpretation of them."""

    def __init__(self, sock):
        self.sock = sock

    def send(self, api_key, api_version, correlation_id, body=b"", client_id="pytest"):
        header = struct.pack(">hhi", api_key, api_version, correlation_id)
        if client_id is None:
            header += struct.pack(">h", -1)
        else:
            encoded = client_id.encode()
            header += struct.pack(">h", len(encoded)) + encoded
        if self._request_header_version(api_key, api_version) == 2:
            header += b"\x00"
        frame = header + body
        self.sock.sendall(struct.pack(">i", len(frame)) + frame)

    @staticmethod
    def _request_header_version(api_key, api_version):
        if api_key == API_VERSIONS:
            return 2 if api_version >= 3 else 1
        if api_key == METADATA:
            return 2 if api_version >= 9 else 1
        if api_key == FETCH:
            return 2 if api_version >= 12 else 1
        if api_key == LIST_OFFSETS:
            return 2 if api_version >= 6 else 1
        if api_key == JOIN_GROUP:
            return 2 if api_version >= 6 else 1
        if api_key == OFFSET_COMMIT:
            return 2 if api_version >= 8 else 1
        if api_key == SYNC_GROUP:
            return 2 if api_version >= 4 else 1
        if api_key == HEARTBEAT:
            return 2 if api_version >= 4 else 1
        if api_key == OFFSET_DELETE:
            return 1
        if api_key in (DESCRIBE_ACLS, CREATE_ACLS, DELETE_ACLS):
            return 2 if api_version >= 2 else 1
        if api_key == WRITE_TXN_MARKERS:
            return 2
        if api_key == ADD_PARTITIONS_TO_TXN:
            return 2 if api_version >= 3 else 1
        if api_key == END_TXN:
            return 2 if api_version >= 3 else 1
        raise AssertionError(f"unmapped api {api_key}")

    def recv(self):
        """Read one length-prefixed response frame. Returns the body after the size
        prefix, with the response header still attached."""
        size = struct.unpack(">i", self._exactly(4))[0]
        assert 0 <= size < 100 * 1024 * 1024, f"absurd response size {size}"
        return self._exactly(size)

    def _exactly(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError(f"peer closed after {len(buf)} of {n} bytes")
            buf += chunk
        return buf

    def closed(self, timeout=5):
        """True if the peer closed the connection."""
        self.sock.settimeout(timeout)
        try:
            return self.sock.recv(1) == b""
        except (ConnectionResetError, socket.timeout):
            return True

def read_compact_string(buf, pos):
    n = buf[pos]
    pos += 1
    if n == 0:
        return None, pos
    return buf[pos : pos + n - 1].decode(), pos + n - 1

def read_legacy_string(buf, pos):
    (n,) = struct.unpack_from(">h", buf, pos)
    pos += 2
    if n < 0:
        return None, pos
    return buf[pos : pos + n].decode(), pos + n
