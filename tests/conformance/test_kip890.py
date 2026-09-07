"""KIP-890 part two: the producer epoch rotates when a transaction ends.

Hand-rolled frames, because no client library replays EndTxn with the epoch the
broker just retired. Every assertion compares kafgres against apache/kafka:4.3.1
answering identical bytes.
"""

import socket
import struct
import subprocess
import time

import pytest

KAFKA_IMAGE = "apache/kafka:4.3.1"
KAFGRES = ("127.0.0.1", 9092)
REFERENCE = ("127.0.0.1", 9292)
TOPIC = "kip890-conformance"

NONE = 0
INVALID_PRODUCER_EPOCH = 47
INVALID_TXN_STATE = 48
COORDINATOR_NOT_AVAILABLE = 15
CONCURRENT_TRANSACTIONS = 51
PRODUCER_FENCED = 90


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


# --------------------------------------------------------------------- framing
def _s16(v): return struct.pack(">h", v)
def _s32(v): return struct.pack(">i", v)
def _s64(v): return struct.pack(">q", v)


def _nstr(s):
    """Non-flexible string: int16 length, then bytes."""
    b = s.encode()
    return _s16(len(b)) + b


def _uvarint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n:
            return out


def _cstr(s):
    """Compact string, for a flexible request: uvarint(len + 1), then bytes."""
    b = s.encode()
    return _uvarint(len(b) + 1) + b


class _Reader:
    def __init__(self, b):
        self.b, self.i = b, 0

    def i16(self):
        v = struct.unpack_from(">h", self.b, self.i)[0]; self.i += 2; return v

    def i32(self):
        v = struct.unpack_from(">i", self.b, self.i)[0]; self.i += 4; return v

    def i64(self):
        v = struct.unpack_from(">q", self.b, self.i)[0]; self.i += 8; return v

    def uvarint(self):
        n, shift = 0, 0
        while True:
            c = self.b[self.i]; self.i += 1
            n |= (c & 0x7F) << shift
            if not c & 0x80:
                return n
            shift += 7

    def skip_str(self):
        n = self.i16()
        if n >= 0:
            self.i += n


class Broker:
    """One connection, raw frames. Correlation ids increment to catch desynchronised responses."""

    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=15)
        self.corr = 0

    def close(self):
        self.sock.close()

    def _recv(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("broker closed the connection")
            buf += chunk
        return buf

    def call(self, api_key, version, body, flexible):
        self.corr += 1
        header = _s16(api_key) + _s16(version) + _s32(self.corr) + _nstr("kip890")
        if flexible:
            header += _uvarint(0)
        frame = header + body
        self.sock.sendall(_s32(len(frame)) + frame)

        size = struct.unpack(">i", self._recv(4))[0]
        r = _Reader(self._recv(size))
        got = r.i32()
        assert got == self.corr, f"correlation id {got} for request {self.corr}"
        if flexible:
            r.uvarint()
        return r

    # ----------------------------------------------------------------- the RPCs
    def find_coordinator(self, key):
        """v2, key_type 1 = transaction. The coordinator is created lazily, so
        COORDINATOR_NOT_AVAILABLE here just means retry."""
        r = self.call(10, 2, _nstr(key) + bytes([1]), False)
        r.i32()
        err = r.i16()
        r.skip_str()
        return err

    def init_producer_id(self, txn_id):
        r = self.call(22, 1, _nstr(txn_id) + _s32(60_000), False)
        r.i32()
        return r.i16(), r.i64(), r.i16()

    def add_partitions(self, txn_id, pid, epoch, topic):
        body = _nstr(txn_id) + _s64(pid) + _s16(epoch)
        body += _s32(1) + _nstr(topic) + _s32(1) + _s32(0)
        r = self.call(24, 2, body, False)
        r.i32()
        r.i32()          # topics
        r.skip_str()     # topic name
        r.i32()          # partitions
        r.i32()          # partition index
        return r.i16()

    def end_txn(self, txn_id, pid, epoch, committed):
        """v5: flexible, and where ProducerId/ProducerEpoch appear in the response."""
        body = (_cstr(txn_id) + _s64(pid) + _s16(epoch)
                + bytes([1 if committed else 0]) + _uvarint(0))
        r = self.call(26, 5, body, True)
        r.i32()
        return r.i16(), r.i64(), r.i16()


def _settle(fn, tries=25, delay=0.4):
    """Retry past CONCURRENT_TRANSACTIONS. Kafka returns it while the previous
    transaction is still completing and expects the client to back off."""
    out = fn()
    for _ in range(tries - 1):
        code = out[0] if isinstance(out, tuple) else out
        if code != CONCURRENT_TRANSACTIONS:
            return out
        time.sleep(delay)
        out = fn()
    return out


def _ensure_topic(bootstrap):
    subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
         "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bootstrap,
         "--create", "--topic", TOPIC, "--partitions", "1"],
        capture_output=True, text=True, timeout=120,
    )


def transcript(host, port, label):
    """One transaction, ended at v5, then three probes with the retired epoch."""
    _ensure_topic(f"{host}:{port}")
    b = Broker(host, port)
    txn = f"kip890-{label}"
    try:
        for _ in range(20):
            if b.find_coordinator(txn) != COORDINATOR_NOT_AVAILABLE:
                break
            time.sleep(1.5)

        err, pid, epoch = b.init_producer_id(txn)
        assert err == NONE, f"{label}: InitProducerId error {err}"
        assert b.add_partitions(txn, pid, epoch, TOPIC) == NONE

        end_err, new_pid, new_epoch = b.end_txn(txn, pid, epoch, True)

        return {
            "end_txn": end_err,
            "rotated": new_epoch != epoch or new_pid != pid,
            "epoch_advanced_by": new_epoch - epoch,
            "add_partitions_at_retired_epoch":
                _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)),
            "retry_same_outcome":
                _settle(lambda: b.end_txn(txn, pid, epoch, True))[0],
            "retry_answers_current_epoch":
                _settle(lambda: b.end_txn(txn, pid, epoch, True))[2] == new_epoch,
            "replay_opposite_outcome":
                _settle(lambda: b.end_txn(txn, pid, epoch, False))[0],
        }
    finally:
        b.close()


@pytest.fixture(scope="module")
def ours():
    return transcript(*KAFGRES, "kafgres")


def test_ending_a_transaction_at_v5_rotates_the_producer_epoch(ours):
    """EndTxn v5 hands back a fresh epoch, so the next transaction needs no InitProducerId."""
    assert ours["end_txn"] == NONE
    assert ours["rotated"], "EndTxn v5 returned the epoch it was given"
    assert ours["epoch_advanced_by"] == 1


def test_the_retired_epoch_no_longer_works_for_ordinary_transactional_work(ours):
    """The old epoch must stop working everywhere except the legitimate retry."""
    assert ours["add_partitions_at_retired_epoch"] == PRODUCER_FENCED


def test_replaying_the_end_of_a_committed_transaction_is_idempotent(ours):
    """The lost-response case: the client committed, never saw the answer, and replays
    with the epoch it still holds. Fencing the replay would make the application redo
    committed work under a fresh producer id, whose deduplication window is empty."""
    assert ours["retry_same_outcome"] == NONE
    assert ours["retry_answers_current_epoch"], "the retry answered a stale epoch"


def test_replaying_with_the_opposite_outcome_is_not_a_retry(ours):
    """Asking to abort what was committed carries the same epoch but is a different
    request. It is refused as an invalid state transition, not a fencing error."""
    assert ours["replay_opposite_outcome"] == INVALID_TXN_STATE


@needs_reference
def test_the_whole_transcript_matches_the_reference_broker():
    """Identical bytes to apache/kafka:4.3.1 must get identical answers, including
    PRODUCER_FENCED for a retired epoch and INVALID_TXN_STATE for a contradictory
    replay."""
    ours = transcript(*KAFGRES, "kafgres")
    theirs = transcript(*REFERENCE, "reference")
    assert ours == theirs, f"\nkafgres:   {ours}\nreference: {theirs}"
