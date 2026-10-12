"""Build RecordBatch v2 frames by hand: tests must control producerId, producerEpoch
and baseSequence exactly, and no client library lets you do that on purpose."""

import struct

def _crc32c(data):
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ (0x82F63B78 if c & 1 else 0)
        table.append(c)
    c = 0xFFFFFFFF
    for b in data:
        c = table[(c ^ b) & 0xFF] ^ (c >> 8)
    return c ^ 0xFFFFFFFF

def _varint(n):
    """Zigzag varint, as the record format inside a batch uses."""
    n = (n << 1) ^ (n >> 31) if n >= 0 else (n << 1) ^ (n >> 31)
    n &= 0xFFFFFFFF
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out += bytes([b | 0x80])
        else:
            out += bytes([b])
            return out

def _record(offset_delta, value: bytes):
    body = b"\x00"                      # attributes
    body += _varint(0)                  # timestampDelta
    body += _varint(offset_delta)
    body += _varint(-1)                 # null key
    body += _varint(len(value)) + value
    body += _varint(0)                  # no headers
    return _varint(len(body)) + body

def record_batch(values, producer_id=-1, producer_epoch=-1, base_sequence=-1, base_offset=0,
                 last_offset_delta=None, timestamp=None):
    """One magic-v2 batch with a correct CRC.

    `baseOffset` and `partitionLeaderEpoch` are left as the producer sends them; the
    broker stamps both. `last_offset_delta` overrides the field for tests that need a
    header the client would never send; it is inside CRC coverage, so no checksum
    catches it.
    """
    records = b"".join(_record(i, v) for i, v in enumerate(values))
    ts = 1_700_000_000_000 if timestamp is None else timestamp
    if last_offset_delta is None:
        last_offset_delta = len(values) - 1

    covered = struct.pack(">h", 0)                       # attributes
    covered += struct.pack(">i", last_offset_delta)
    covered += struct.pack(">q", ts)                     # firstTimestamp
    covered += struct.pack(">q", ts)                     # maxTimestamp
    covered += struct.pack(">q", producer_id)
    covered += struct.pack(">h", producer_epoch)
    covered += struct.pack(">i", base_sequence)
    covered += struct.pack(">i", len(values))            # recordCount
    covered += records

    head = struct.pack(">i", -1)                         # partitionLeaderEpoch
    head += struct.pack(">b", 2)                         # magic
    head += struct.pack(">I", _crc32c(covered))

    body = head + covered
    return struct.pack(">q", base_offset) + struct.pack(">i", len(body)) + body

def produce_v3(topic: str, partition: int, batch: bytes, acks=1):
    """A Produce v3 request body: the newest version that is not flexible, so the
    bytes stay legible."""
    t = topic.encode()
    body = struct.pack(">h", -1)                         # transactional_id null
    body += struct.pack(">hi", acks, 30000)
    body += struct.pack(">i", 1) + struct.pack(">h", len(t)) + t
    body += struct.pack(">i", 1) + struct.pack(">i", partition)
    body += struct.pack(">i", len(batch)) + batch
    return body

def produce_v3_many(topic: str, parts, acks=1):
    """One Produce v3 request covering several partitions of one topic — the shape the
    Java producer actually sends."""
    t = topic.encode()
    body = struct.pack(">h", -1)                         # transactional_id null
    body += struct.pack(">hi", acks, 30000)
    body += struct.pack(">i", 1) + struct.pack(">h", len(t)) + t
    body += struct.pack(">i", len(parts))
    for partition, batch in parts:
        body += struct.pack(">i", partition)
        body += struct.pack(">i", len(batch)) + batch
    return body

def parse_produce_v3_many(resp):
    """-> (correlation_id, [(partition_index, error_code, base_offset), ...])"""
    pos = 0
    (correlation,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    (topics,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert topics == 1, f"expected one topic, got {topics}"
    (n,) = struct.unpack_from(">h", resp, pos)
    pos += 2 + n
    (count,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    out = []
    for _ in range(count):
        index, error = struct.unpack_from(">ih", resp, pos)
        pos += 6
        (base_offset,) = struct.unpack_from(">q", resp, pos)
        pos += 8
        pos += 8
        out.append((index, error, base_offset))
    return correlation, out

def parse_produce_v3(resp):
    """-> (correlation_id, error_code, base_offset)"""
    pos = 0
    (correlation,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    (topics,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert topics == 1, f"expected one topic, got {topics}"
    (n,) = struct.unpack_from(">h", resp, pos)
    pos += 2 + n
    (parts,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert parts == 1, f"expected one partition, got {parts}"
    _index, error = struct.unpack_from(">ih", resp, pos)
    pos += 6
    (base_offset,) = struct.unpack_from(">q", resp, pos)
    return correlation, error, base_offset
