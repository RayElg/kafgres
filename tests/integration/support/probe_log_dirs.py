"""Raw DescribeLogDirs v1: kafka-log-dirs.sh always sends an explicit partition list,
so a named topic with an *empty* one is unreachable through it.

    python3 probe_log_dirs.py <host> <port> <topic>
"""
import socket, struct, sys
host, port, topic = sys.argv[1], int(sys.argv[2]), sys.argv[3]

api_key, api_version, corr = 35, 1, 11
hdr = struct.pack(">hhi", api_key, api_version, corr)
cid = b"probe"
hdr += struct.pack(">h", len(cid)) + cid

t = topic.encode()
body = struct.pack(">i", 1)                       # topics array: 1 entry
body += struct.pack(">h", len(t)) + t             # topic name
body += struct.pack(">i", 0)                      # partitions: EMPTY array
frame = hdr + body

s = socket.create_connection((host, port), timeout=15)
s.sendall(struct.pack(">i", len(frame)) + frame)
n = struct.unpack(">i", s.recv(4))[0]
buf = b""
while len(buf) < n:
    buf += s.recv(n - len(buf))

off = 4                                            # correlation id
throttle = struct.unpack(">i", buf[off:off+4])[0]; off += 4
nres = struct.unpack(">i", buf[off:off+4])[0]; off += 4
err = struct.unpack(">h", buf[off:off+2])[0]; off += 2
dlen = struct.unpack(">h", buf[off:off+2])[0]; off += 2
logdir = buf[off:off+dlen].decode(); off += dlen
ntopics = struct.unpack(">i", buf[off:off+4])[0]; off += 4
total_parts = 0
for _ in range(ntopics):
    tl = struct.unpack(">h", buf[off:off+2])[0]; off += 2
    off += tl
    np = struct.unpack(">i", buf[off:off+4])[0]; off += 4
    total_parts += np
    off += np * (4 + 8 + 8 + 1)
print(f"results={nres} err={err} logdir={logdir!r} topics={ntopics} partitions={total_parts}")
