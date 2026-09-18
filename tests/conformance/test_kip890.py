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
COORDINATOR_LOAD_IN_PROGRESS = 14
COORDINATOR_NOT_AVAILABLE = 15
INVALID_REQUEST = 42
INVALID_PRODUCER_EPOCH = 47
INVALID_TXN_STATE = 48
INVALID_PRODUCER_ID_MAPPING = 49
CONCURRENT_TRANSACTIONS = 51
PRODUCER_FENCED = 90

# What a client backs off and retries rather than reports: the coordinator is still being
# elected or is still loading its partition (both seen on a freshly started reference,
# after FindCoordinator has already named it), or the previous transaction is still
# completing. None of these is an answer, so the transcript waits them out.
RETRIABLE = {COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, CONCURRENT_TRANSACTIONS}


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

    def add_partitions(self, txn_id, pid, epoch, topic, version=2):
        """v0–v2 share one non-flexible shape."""
        body = _nstr(txn_id) + _s64(pid) + _s16(epoch)
        body += _s32(1) + _nstr(topic) + _s32(1) + _s32(0)
        r = self.call(24, version, body, False)
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

    def init_producer_id_v5(self, txn_id):
        """v5, flexible: the version a transaction-V2 client negotiates."""
        body = _cstr(txn_id) + _s32(60_000) + _s64(-1) + _s16(-1) + _uvarint(0)
        r = self.call(22, 5, body, True)
        r.i32()
        return r.i16(), r.i64(), r.i16()


def _settle(fn, tries=25, delay=0.4):
    """Retry past the `RETRIABLE` codes, as a client would."""
    out = fn()
    for _ in range(tries - 1):
        code = out[0] if isinstance(out, tuple) else out
        if code not in RETRIABLE:
            return out
        time.sleep(delay)
        out = fn()
    return out


def _ensure_topic(bootstrap, topic=TOPIC):
    subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
         "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bootstrap,
         "--create", "--topic", topic, "--partitions", "1"],
        capture_output=True, text=True, timeout=120,
    )


def _coordinated(host, port, txn):
    """A connection whose transaction coordinator exists. The coordinator is created
    lazily on the reference, so FindCoordinator is retried until it names one; the
    coordinator may then still be loading, which `_settle` waits out per request."""
    b = Broker(host, port)
    for _ in range(20):
        if b.find_coordinator(txn) not in RETRIABLE:
            break
        time.sleep(1.5)
    return b


def transcript(host, port, label):
    """One transaction, ended at v5, then three probes with the retired epoch."""
    _ensure_topic(f"{host}:{port}")
    txn = f"kip890-{label}"
    b = _coordinated(host, port, txn)
    try:
        err, pid, epoch = _settle(lambda: b.init_producer_id(txn))
        assert err == NONE, f"{label}: InitProducerId error {err}"
        assert _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)) == NONE

        end_err, new_pid, new_epoch = _settle(lambda: b.end_txn(txn, pid, epoch, True))

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


def _never_begun_end_txn(host, port, txn):
    """InitProducerId v5, then EndTxn v5, with no AddPartitionsToTxn in between: the
    shape of a transaction-V2 client that ends a transaction it never produced to."""
    b = _coordinated(host, port, txn)
    try:
        err, pid, epoch = _settle(lambda: b.init_producer_id_v5(txn))
        assert err == NONE, f"InitProducerId error {err}"
        return _settle(lambda: b.end_txn(txn, pid, epoch, True))[0]
    finally:
        b.close()


def test_end_txn_v5_on_a_transaction_that_was_never_begun_is_not_fencing():
    """A transaction-V2 client is entitled to end a transaction it only initialised.
    The epoch errors are fatal to the client, so this must be INVALID_TXN_STATE, the
    state-machine answer, never INVALID_PRODUCER_EPOCH."""
    err = _never_begun_end_txn(*KAFGRES, "kip890-never-begun")
    assert err == INVALID_TXN_STATE, f"EndTxn on a never-begun transaction answered {err}"


@needs_reference
def test_the_never_begun_end_txn_answer_matches_the_reference():
    ours = _never_begun_end_txn(*KAFGRES, "kip890-never-begun-ours")
    theirs = _never_begun_end_txn(*REFERENCE, "kip890-never-begun-theirs")
    assert ours == theirs == INVALID_TXN_STATE, f"kafgres {ours}, reference {theirs}"


def test_a_real_transaction_v2_client_transacts_end_to_end():
    """The 4.3.1 Java client, against our advertised feature levels, runs as
    transaction-V2: it never sends AddPartitionsToTxn, so the produce path itself must
    begin the transaction and EndTxn v5 must end it. This is the flow no hand-rolled
    frame above replays, and the one every 4.x transactional producer uses."""
    topic = "kip890-tv2-e2e"
    _ensure_topic(f"{KAFGRES[0]}:{KAFGRES[1]}", topic)
    txn_id = f"kip890-tv2-{int(time.time())}"
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
         "/opt/kafka/bin/kafka-producer-perf-test.sh",
         "--topic", topic, "--num-records", "20", "--record-size", "200",
         "--throughput", "-1", "--transactional-id", txn_id,
         "--transaction-duration-ms", "200",
         "--command-property", f"bootstrap.servers={KAFGRES[0]}:{KAFGRES[1]}"],
        capture_output=True, text=True, timeout=300,
    )
    assert out.returncode == 0, (
        f"transactional produce failed: {out.stdout[-400:]} {out.stderr[-400:]}"
    )
    assert "20 records sent" in out.stdout, out.stdout[-400:]




def _nothing_open_transcript(host, port, label):
    """EndTxn v5 with no transaction open, both outcomes, from both empty and completed
    states. A transaction-V2 client aborts this way after a failed send, so an abort
    must succeed (and rotate the epoch, retry-able like any other); a commit is the
    state-machine refusal. Neither may fence."""
    _ensure_topic(f"{host}:{port}")
    txn = f"kip890-nothing-open-{label}"
    b = _coordinated(host, port, txn)
    try:
        err, pid, epoch = _settle(lambda: b.init_producer_id_v5(txn))
        assert err == NONE, f"{label}: InitProducerId error {err}"
        assert _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)) == NONE
        _, pid, epoch = _settle(lambda: b.end_txn(txn, pid, epoch, True))

        commit_after_commit = _settle(lambda: b.end_txn(txn, pid, epoch, True))
        abort_after_commit = _settle(lambda: b.end_txn(txn, pid, epoch, False))
        retry_empty_abort = _settle(lambda: b.end_txn(txn, pid, epoch, False))
        pid2, epoch2 = abort_after_commit[1], abort_after_commit[2]
        abort_after_abort = _settle(lambda: b.end_txn(txn, pid2, epoch2, False))
        return {
            "commit_after_commit": commit_after_commit[0],
            "abort_after_commit": abort_after_commit[0],
            "abort_after_commit_rotated": abort_after_commit[2] == epoch + 1,
            "retry_empty_abort": retry_empty_abort[0],
            "retry_empty_abort_answers_current": retry_empty_abort[2] == epoch2,
            "abort_after_abort": abort_after_abort[0],
            "abort_after_abort_rotated": abort_after_abort[2] == epoch2 + 1,
        }
    finally:
        b.close()


def test_ending_with_nothing_open_is_never_fencing():
    ours = _nothing_open_transcript(*KAFGRES, "kafgres")
    assert ours == {
        "commit_after_commit": INVALID_TXN_STATE,
        "abort_after_commit": NONE,
        "abort_after_commit_rotated": True,
        "retry_empty_abort": NONE,
        "retry_empty_abort_answers_current": True,
        "abort_after_abort": NONE,
        "abort_after_abort_rotated": True,
    }, ours


@needs_reference
def test_the_nothing_open_transcript_matches_the_reference():
    ours = _nothing_open_transcript(*KAFGRES, "kafgres")
    theirs = _nothing_open_transcript(*REFERENCE, "reference")
    assert ours == theirs, f"\nkafgres:   {ours}\nreference: {theirs}"


def _end_txn_v2(b, txn, pid, epoch, committed):
    """Pre-KIP-890 shape: no rotation, so a same-outcome replay is a plain retry."""
    body = _nstr(txn) + _s64(pid) + _s16(epoch) + bytes([1 if committed else 0])
    r = b.call(26, 2, body, False)
    r.i32()
    return r.i16()


def _replay_before_rotation_transcript(host, port, label):
    _ensure_topic(f"{host}:{port}")
    txn = f"kip890-v2-replay-{label}"
    b = _coordinated(host, port, txn)
    try:
        err, pid, epoch = _settle(lambda: b.init_producer_id(txn))
        assert err == NONE, f"{label}: InitProducerId error {err}"
        out = {
            "empty_commit": _settle(lambda: _end_txn_v2(b, txn, pid, epoch, True)),
            "empty_abort": _settle(lambda: _end_txn_v2(b, txn, pid, epoch, False)),
        }
        assert _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)) == NONE
        out["commit"] = _settle(lambda: _end_txn_v2(b, txn, pid, epoch, True))
        out["commit_again"] = _settle(lambda: _end_txn_v2(b, txn, pid, epoch, True))
        out["abort_after_commit"] = _settle(lambda: _end_txn_v2(b, txn, pid, epoch, False))
        assert _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)) == NONE
        out["abort"] = _settle(lambda: _end_txn_v2(b, txn, pid, epoch, False))
        out["abort_again"] = _settle(lambda: _end_txn_v2(b, txn, pid, epoch, False))
        out["commit_after_abort"] = _settle(lambda: _end_txn_v2(b, txn, pid, epoch, True))
        return out
    finally:
        b.close()


def test_replaying_end_txn_before_rotation_is_a_retry():
    ours = _replay_before_rotation_transcript(*KAFGRES, "kafgres")
    assert ours == {
        "empty_commit": INVALID_TXN_STATE,
        "empty_abort": INVALID_TXN_STATE,
        "commit": NONE,
        "commit_again": NONE,
        "abort_after_commit": INVALID_TXN_STATE,
        "abort": NONE,
        "abort_again": NONE,
        "commit_after_abort": INVALID_TXN_STATE,
    }, ours


@needs_reference
def test_the_replay_before_rotation_transcript_matches_the_reference():
    ours = _replay_before_rotation_transcript(*KAFGRES, "kafgres")
    theirs = _replay_before_rotation_transcript(*REFERENCE, "reference")
    assert ours == theirs, f"\nkafgres:   {ours}\nreference: {theirs}"


def _retry_after_takeover(host, port, label):
    """A retried EndTxn at the retired epoch, arriving after a new instance of the same
    transactional id ran InitProducerId: the old instance is a zombie and must be fenced,
    not answered NONE with the epoch the new instance now owns."""
    _ensure_topic(f"{host}:{port}")
    txn = f"kip890-takeover-{label}"
    b = _coordinated(host, port, txn)
    try:
        err, pid, epoch = _settle(lambda: b.init_producer_id_v5(txn))
        assert err == NONE, f"{label}: InitProducerId error {err}"
        assert _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)) == NONE
        assert _settle(lambda: b.end_txn(txn, pid, epoch, True))[0] == NONE
        assert _settle(lambda: b.init_producer_id_v5(txn))[0] == NONE
        return _settle(lambda: b.end_txn(txn, pid, epoch, True))[0]
    finally:
        b.close()


def test_a_retry_after_a_takeover_is_fenced():
    err = _retry_after_takeover(*KAFGRES, "kafgres")
    assert err == PRODUCER_FENCED, err


@needs_reference
def test_a_retry_after_a_takeover_is_fenced_on_the_reference_too():
    theirs = _retry_after_takeover(*REFERENCE, "reference")
    assert theirs == PRODUCER_FENCED, theirs


def _end_txn_at(b, version, txn, pid, epoch, committed):
    """EndTxn at any version; only the error code is read."""
    if version >= 3:
        body = (_cstr(txn) + _s64(pid) + _s16(epoch)
                + bytes([1 if committed else 0]) + _uvarint(0))
    else:
        body = _nstr(txn) + _s64(pid) + _s16(epoch) + bytes([1 if committed else 0])
    r = b.call(26, version, body, version >= 3)
    r.i32()
    return r.i16()


def _add_offsets(b, version, txn, pid, epoch, group):
    body = _nstr(txn) + _s64(pid) + _s16(epoch) + _nstr(group)
    r = b.call(25, version, body, False)
    r.i32()
    return r.i16()


def _txn_offset_commit(b, version, txn, pid, epoch, group):
    """One partition; returns its error code, which is where this API reports."""
    body = _nstr(txn) + _nstr(group) + _s64(pid) + _s16(epoch)
    body += _s32(1) + _nstr(TOPIC) + _s32(1) + _s32(0) + _s64(0)
    if version >= 2:
        body += _s32(-1)     # committed leader epoch
    body += _nstr("")
    r = b.call(28, version, body, False)
    r.i32()
    r.i32()          # topics
    r.skip_str()
    r.i32()          # partitions
    r.i32()          # partition index
    return r.i16()


def _fencing_transcript(host, port, label):
    """Every way a coordinator request can name a producer it may not act for: an epoch
    the coordinator has moved past, a producer id it never mapped to this transactional
    id, a transactional id it has never seen. The codes are version-gated:
    PRODUCER_FENCED reached the wire in v2 of each API, older clients get
    INVALID_PRODUCER_EPOCH — and TxnOffsetCommit reports per partition and stays at
    INVALID_PRODUCER_EPOCH throughout."""
    _ensure_topic(f"{host}:{port}")
    txn = f"kip890-fencing-{label}"
    b = _coordinated(host, port, txn)
    try:
        assert _settle(lambda: b.init_producer_id_v5(txn))[0] == NONE
        err, pid, epoch = _settle(lambda: b.init_producer_id_v5(txn))
        assert err == NONE and epoch >= 1, (err, epoch)
        stale, ahead, unknown = epoch - 1, epoch + 1, pid + 1_000_000
        out = {}
        for v in (1, 2, 5):
            out[f"end_txn_v{v}_stale"] = _settle(lambda: _end_txn_at(b, v, txn, pid, stale, True))
            out[f"end_txn_v{v}_ahead"] = _settle(lambda: _end_txn_at(b, v, txn, pid, ahead, True))
            out[f"end_txn_v{v}_unknown_pid"] = _settle(lambda: _end_txn_at(b, v, txn, unknown, epoch, True))
        out["end_txn_v5_unknown_txn"] = _settle(lambda: _end_txn_at(b, 5, txn + "-x", pid, epoch, True))
        out["end_txn_v5_empty_txn"] = _settle(lambda: _end_txn_at(b, 5, "", pid, epoch, True))
        # The retired pair, which a retry would be answered NONE for — but not without an id.
        assert _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)) == NONE
        _, pid, epoch = _settle(lambda: b.end_txn(txn, pid, epoch, True))
        out["end_txn_v5_empty_txn_retired_pair"] = _settle(lambda: _end_txn_at(b, 5, "", pid, epoch - 1, True))
        stale, ahead = epoch - 1, epoch + 1
        for v in (1, 2):
            out[f"add_offsets_v{v}_stale"] = _settle(lambda: _add_offsets(b, v, txn, pid, stale, "g"))
            out[f"add_offsets_v{v}_ahead"] = _settle(lambda: _add_offsets(b, v, txn, pid, ahead, "g"))
            out[f"add_offsets_v{v}_unknown_pid"] = _settle(lambda: _add_offsets(b, v, txn, unknown, epoch, "g"))
            out[f"txn_offset_commit_v{v}_stale"] = _settle(lambda: _txn_offset_commit(b, v, txn, pid, stale, "g"))
            out[f"txn_offset_commit_v{v}_ahead"] = _settle(lambda: _txn_offset_commit(b, v, txn, pid, ahead, "g"))
            out[f"txn_offset_commit_v{v}_unknown_pid"] = _settle(lambda: _txn_offset_commit(b, v, txn, unknown, epoch, "g"))
        out["add_offsets_v2_empty_txn"] = _settle(lambda: _add_offsets(b, 2, "", pid, epoch, "g"))
        for v in (1, 2):
            out[f"add_partitions_v{v}_stale"] = _settle(lambda: b.add_partitions(txn, pid, stale, TOPIC, v))
            out[f"add_partitions_v{v}_ahead"] = _settle(lambda: b.add_partitions(txn, pid, ahead, TOPIC, v))
            out[f"add_partitions_v{v}_unknown_pid"] = _settle(lambda: b.add_partitions(txn, unknown, epoch, TOPIC, v))
        out["add_partitions_v2_empty_txn"] = _settle(lambda: b.add_partitions("", pid, epoch, TOPIC))
        return out
    finally:
        b.close()


def test_fencing_codes_are_exact():
    ours = _fencing_transcript(*KAFGRES, "kafgres")
    assert ours == {
        "end_txn_v1_stale": INVALID_PRODUCER_EPOCH,
        "end_txn_v1_ahead": INVALID_PRODUCER_EPOCH,
        "end_txn_v1_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "end_txn_v2_stale": PRODUCER_FENCED,
        "end_txn_v2_ahead": PRODUCER_FENCED,
        "end_txn_v2_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "end_txn_v5_stale": PRODUCER_FENCED,
        "end_txn_v5_ahead": PRODUCER_FENCED,
        "end_txn_v5_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "end_txn_v5_unknown_txn": INVALID_PRODUCER_ID_MAPPING,
        "end_txn_v5_empty_txn": INVALID_REQUEST,
        "end_txn_v5_empty_txn_retired_pair": INVALID_REQUEST,
        "add_offsets_v1_stale": INVALID_PRODUCER_EPOCH,
        "add_offsets_v1_ahead": INVALID_PRODUCER_EPOCH,
        "add_offsets_v1_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "txn_offset_commit_v1_stale": INVALID_PRODUCER_EPOCH,
        "txn_offset_commit_v1_ahead": INVALID_PRODUCER_EPOCH,
        "txn_offset_commit_v1_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "add_offsets_v2_stale": PRODUCER_FENCED,
        "add_offsets_v2_ahead": PRODUCER_FENCED,
        "add_offsets_v2_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "txn_offset_commit_v2_stale": INVALID_PRODUCER_EPOCH,
        "txn_offset_commit_v2_ahead": INVALID_PRODUCER_EPOCH,
        "txn_offset_commit_v2_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "add_offsets_v2_empty_txn": INVALID_REQUEST,
        "add_partitions_v1_stale": INVALID_PRODUCER_EPOCH,
        "add_partitions_v1_ahead": INVALID_PRODUCER_EPOCH,
        "add_partitions_v1_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "add_partitions_v2_stale": PRODUCER_FENCED,
        "add_partitions_v2_ahead": PRODUCER_FENCED,
        "add_partitions_v2_unknown_pid": INVALID_PRODUCER_ID_MAPPING,
        "add_partitions_v2_empty_txn": INVALID_REQUEST,
    }, ours


@needs_reference
def test_the_fencing_transcript_matches_the_reference():
    ours = _fencing_transcript(*KAFGRES, "kafgres")
    theirs = _fencing_transcript(*REFERENCE, "reference")
    assert ours == theirs, f"\nkafgres:   {ours}\nreference: {theirs}"


def _takeover_transcript(host, port, label):
    """A new instance of a transactional id initialises while the old one's transaction
    is still open. The coordinator must abort what the old instance left — the reference
    answers CONCURRENT_TRANSACTIONS while its markers land, then NONE — so that nothing
    is open under the new epoch: a commit there is INVALID_TXN_STATE, never a commit of
    the dead instance's records. The old epoch is fenced."""
    _ensure_topic(f"{host}:{port}")
    txn = f"kip890-takeover-open-{label}"
    b = _coordinated(host, port, txn)
    try:
        err, pid, epoch = _settle(lambda: b.init_producer_id_v5(txn))
        assert err == NONE, f"{label}: InitProducerId error {err}"
        assert _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)) == NONE
        err, pid2, epoch2 = _settle(lambda: b.init_producer_id_v5(txn))
        return {
            "init_over_open": err,
            "same_producer_id": pid2 == pid,
            "epoch_moved": epoch2 > epoch,
            "commit_at_new_epoch": _settle(lambda: b.end_txn(txn, pid2, epoch2, True))[0],
            "old_epoch_end_txn": _settle(lambda: b.end_txn(txn, pid, epoch, True))[0],
            "old_epoch_add_partitions": _settle(lambda: b.add_partitions(txn, pid, epoch, TOPIC)),
        }
    finally:
        b.close()


def test_init_producer_id_aborts_the_open_transaction():
    ours = _takeover_transcript(*KAFGRES, "kafgres")
    assert ours == {
        "init_over_open": NONE,
        "same_producer_id": True,
        "epoch_moved": True,
        "commit_at_new_epoch": INVALID_TXN_STATE,
        "old_epoch_end_txn": PRODUCER_FENCED,
        "old_epoch_add_partitions": PRODUCER_FENCED,
    }, ours


@needs_reference
def test_the_takeover_transcript_matches_the_reference():
    ours = _takeover_transcript(*KAFGRES, "kafgres")
    theirs = _takeover_transcript(*REFERENCE, "reference")
    assert ours == theirs, f"\nkafgres:   {ours}\nreference: {theirs}"
