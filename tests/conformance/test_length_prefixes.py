import socket
import struct
import subprocess

import pytest

KAFKA_IMAGE = "apache/kafka:4.3.1"
KAFGRES = ("127.0.0.1", 9092)
REFERENCE = ("127.0.0.1", 9292)

METADATA = 3


def reference_available():
    try:
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
             "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
             f"{REFERENCE[0]}:{REFERENCE[1]}", "--list"],
            capture_output=True, text=True, timeout=90,
        )
        return out.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


needs_reference = pytest.mark.skipif(
    not reference_available(),
    reason="reference broker not running: docker compose --profile conformance up -d kafka",
)


def _s16(v): return struct.pack(">h", v)
def _s32(v): return struct.pack(">i", v)


def _header(version, client_id, flexible):
    h = _s16(METADATA) + _s16(version) + _s32(7) + client_id
    return h + b"\x00" if flexible else h


def _named(name):
    return _s16(len(name)) + name


def _v1(client_id, topics):
    return _header(1, client_id, False) + topics


def _v12(topics):
    return _header(12, _named(b"lenprefix"), True) + topics + b"\x00\x00\x00"


CASES = {
    "client_id_len_minus_2": (
        _v1(_s16(-1), _s32(-1)),
        _v1(_s16(-2), _s32(-1)),
    ),
    "v1_topics_len_minus_2": (
        _v1(_named(b"lenprefix"), _s32(-1)),
        _v1(_named(b"lenprefix"), _s32(-2)),
    ),
    "v12_topics_uvarint_fifth_byte_0x10": (
        _v12(b"\x00"),
        _v12(b"\x80\x80\x80\x80\x10"),
    ),
    "v12_topics_nonminimal_uvarint_null": (
        _v12(b"\x00"),
        _v12(b"\x80\x80\x80\x80\x00"),
    ),
}


def _recv(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError
        buf += chunk
    return buf


def _exchange(addr, frame):
    with socket.create_connection(addr, timeout=15) as sock:
        sock.sendall(_s32(len(frame)) + frame)
        try:
            size = struct.unpack(">i", _recv(sock, 4))[0]
            body = _recv(sock, size)
        except (EOFError, ConnectionResetError, socket.timeout):
            return None
    assert body[:4] == _s32(7)
    return body[4:]


def outcome(addr, case):
    canonical, variant = CASES[case]
    want = _exchange(addr, canonical)
    assert want is not None, f"{addr} rejected the canonical encoding for {case}"
    got = _exchange(addr, variant)
    if got is None:
        return "connection closed"
    if got == want:
        return "decoded as the canonical encoding"
    return "answered differently"


@needs_reference
@pytest.mark.parametrize("case", list(CASES))
def test_length_prefix_edge_cases_match_the_reference(case):
    assert outcome(KAFGRES, case) == outcome(REFERENCE, case)
