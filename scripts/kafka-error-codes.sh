#!/usr/bin/env bash
set -euo pipefail

IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"  # matched to the reference tag in docker-compose.yml
WORK="$(mktemp -d)"
trap 'rm -f "$WORK/kc.jar"; rmdir "$WORK" 2>/dev/null || true' EXIT

docker run --rm --entrypoint /bin/bash -v "$WORK:/out" "$IMAGE" -c \
    'cp "$(ls /opt/kafka/libs/kafka-clients-*.jar | head -1)" /out/kc.jar'

python3 - "$WORK/kc.jar" "$@" <<'PY'
import re, sys, zipfile

jar, *want = sys.argv[1:]
data = zipfile.ZipFile(jar).read("org/apache/kafka/common/protocol/Errors.class")

i, total, utf8, n = 10, int.from_bytes(data[8:10], "big"), [], 1
while n < total:
    tag = data[i]; i += 1
    if tag == 1:
        length = int.from_bytes(data[i:i + 2], "big"); i += 2
        utf8.append(data[i:i + length].decode("utf8", "replace")); i += length
    elif tag in (7, 8, 16, 19, 20): i += 2
    elif tag == 15: i += 3
    elif tag in (3, 4, 9, 10, 11, 12, 17, 18): i += 4
    elif tag in (5, 6): i += 8; n += 1
    else: sys.exit(f"unexpected constant pool tag {tag} at entry {n}")
    n += 1

seen, out = set(), []
for s in utf8:
    if re.fullmatch(r"[A-Z][A-Z0-9_]{3,}", s) and s not in seen:
        seen.add(s); out.append(s)

table = {code - 1: name for code, name in enumerate(out)}
for code, name in table.items():
    if want and not any(w == str(code) or w.upper() in name for w in want):
        continue
    print(f"{code:4d}  {name}")
PY
