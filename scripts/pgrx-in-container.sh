#!/usr/bin/env bash
# Runs inside the pgrx test image; not meant to be invoked directly. The `#[pg_test]` driver
# is a client binary, so linking the whole crate drags in Postgres symbols it never calls and
# full RELRO resolves them all eagerly at load: link once to discover which symbols the driver
# references, stub just those, and relink. The stubs trap, so a wrong assumption fails loudly.
set -euo pipefail

FILTER="${1:-}"
PG=/usr/lib/postgresql/16/bin/postgres
STUB="$CARGO_TARGET_DIR/pgstub"
BASE="-Clink-arg=-Wl,--unresolved-symbols=ignore-all"

rm -rf "$STUB"
mkdir -p "$STUB" "$CARGO_TARGET_DIR/test-pgdata"

RUSTFLAGS="$BASE" cargo test --features pg16,pg_test --no-run >/dev/null 2>&1 || true
BIN=$(ls -t "$CARGO_TARGET_DIR"/debug/deps/kafgres-* | grep -vE '\.(d|rlib|rmeta)$' | head -1)

nm -D --undefined-only "$BIN" | awk '{print $NF}' | sort -u > "$STUB/undef.txt"
nm -D --defined-only "$PG"    | awk 'NF==3 {print $2, $3}' > "$STUB/pg.txt"

python3 - "$STUB" <<'PY'
import re, sys
d = sys.argv[1]
kind = {}
for line in open(d + "/pg.txt"):
    p = line.split()
    if len(p) == 2:
        kind.setdefault(p[1], p[0])
ok = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
fns = data = 0
with open(d + "/stub.c", "w") as out:
    for name in (l.strip() for l in open(d + "/undef.txt")):
        if not ok.match(name) or name not in kind:
            continue
        if kind[name] in "TtWiI":
            out.write("void %s(void) { __builtin_trap(); }\n" % name); fns += 1
        else:
            out.write("char %s[256] __attribute__((aligned(16)));\n" % name); data += 1
print("pgrx stub: %d function, %d data symbol(s)" % (fns, data))
PY

gcc -shared -fPIC -O0 -o "$STUB/libpgstub.so" "$STUB/stub.c"

rm -f "$BIN"
export LD_LIBRARY_PATH="$STUB"
export RUSTFLAGS="$BASE -Clink-arg=-L$STUB -Clink-arg=-lpgstub"
exec cargo pgrx test pg16 $FILTER
