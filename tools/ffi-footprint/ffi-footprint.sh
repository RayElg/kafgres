#!/usr/bin/env bash
# FFI footprint of a pgrx extension: Rust source facts, server C effects from the
# server's LLVM bitcode, and linkage, joined into one report.
#
#   tools/ffi-footprint/ffi-footprint.sh [-c ffi-footprint.toml] [--pg N]... [--image IMG]
#       [--no-build] [--out DIR] [--baseline FILE] [--write-baseline FILE]
#
# Needs bash and docker. Uses host cargo and python3 (3.11+) when present, containers
# otherwise. Writes report.md, report.json and report.html to --out (default <project>/target/ffi-footprint).
set -euo pipefail

TOOL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="ffi-footprint.toml"
OUT=""
PGS=""
IMAGE_OVERRIDE=""
NO_BUILD=0
BASELINE=""
WRITE_BASELINE=""

usage() { sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2; }
while [ $# -gt 0 ]; do
    case "$1" in
        -c|--config) CONFIG="$2"; shift 2 ;;
        --pg) PGS="$PGS $2"; shift 2 ;;
        --image) IMAGE_OVERRIDE="$2"; shift 2 ;;
        --no-build) NO_BUILD=1; shift ;;
        --out) OUT="$2"; shift 2 ;;
        --baseline) BASELINE="$2"; shift 2 ;;
        --write-baseline) WRITE_BASELINE="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "unknown argument: $1" >&2; usage ;;
    esac
done
[ -f "$CONFIG" ] || { echo "config not found: $CONFIG" >&2; exit 2; }
CONFIG="$(cd "$(dirname "$CONFIG")" && pwd)/$(basename "$CONFIG")"
PROJECT="$(dirname "$CONFIG")"
OUT="${OUT:-$PROJECT/target/ffi-footprint}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

# Host Python 3.11+, or the same script in a container.
py() {
    if python3 -c 'import tomllib' 2>/dev/null; then
        python3 "$@"
    else
        docker run --rm -u "$(id -u):$(id -g)" -v "$TOOL:$TOOL:ro" -v "$PROJECT:$PROJECT" \
            -v "$OUT:$OUT" -w "$PROJECT" python:3.12-slim python3 "$@"
    fi
}

eval "$(py "$TOOL/config.py" merge "$CONFIG" "$OUT/config.json" "$OUT/scanner.json" "$OUT/server.json")"
PGS="${PGS:-$FF_PG}"

echo "== scanner"
if command -v cargo >/dev/null; then
    (cd "$TOOL/scanner" && cargo build --release -q)
    scan() { "$TOOL/scanner/target/release/ffi-footprint-scanner" "$@"; }
else
    docker run --rm -u "$(id -u):$(id -g)" -e CARGO_HOME=/tmp/cargo -v "$TOOL:$TOOL" \
        -w "$TOOL/scanner" rust:1-slim cargo build --release -q
    scan() {
        docker run --rm -u "$(id -u):$(id -g)" -v "$TOOL:$TOOL:ro" -v "$FF_CRATE:$FF_CRATE:ro" \
            -v "$OUT:$OUT:ro" rust:1-slim "$TOOL/scanner/target/release/ffi-footprint-scanner" "$@"
    }
fi

FACTS=()
CFX=()
for pg in $PGS; do
    features="${FF_FEATURES//\{pg\}/$pg}"
    echo "== pg$pg: source ($features)"
    scan --src "$FF_CRATE" --features "$features" --cfg "$FF_CFG" --config "$OUT/scanner.json" \
        > "$OUT/facts-pg$pg.json"
    py "$TOOL/config.py" symbols "$OUT/config.json" "$OUT/facts-pg$pg.json" > "$OUT/symbols-pg$pg.txt"

    image="${IMAGE_OVERRIDE:-${FF_IMAGE//\{pg\}/$pg}}"
    [ -n "$image" ] || { echo "no image: set [project] image or pass --image" >&2; exit 2; }
    if [ "$NO_BUILD" = 0 ] && [ -n "$FF_BUILD" ] && [ -z "$IMAGE_OVERRIDE" ]; then
        cmd="${FF_BUILD//\{pg\}/$pg}"
        cmd="${cmd//\{image\}/$image}"
        echo "== pg$pg: build $image"
        (cd "$FF_ROOT" && bash -c "$cmd")
    fi
    docker image inspect "$image" >/dev/null 2>&1 || { echo "image not found: $image" >&2; exit 2; }
    suffix=""
    [ "$FF_BITCODE" = source ] && suffix="-src"
    cx="ffi-footprint-cx:$(printf '%s' "$image" | tr -c 'A-Za-z0-9_.-' '_' | cut -c1-96)$suffix"
    echo "== pg$pg: analysis image $cx ($FF_BITCODE bitcode)"
    docker build -q --build-arg BASE="$image" --build-arg BITCODE="$FF_BITCODE" -t "$cx" "$TOOL" >/dev/null
    echo "== pg$pg: server effects"
    docker run --rm -u "$(id -u):$(id -g)" --entrypoint python3 -v "$TOOL:/tool:ro" -v "$OUT:/out" "$cx" \
        /tool/cfx.py --pg "$pg" --lib-name "$FF_LIB" --symbols "/out/symbols-pg$pg.txt" --conf /out/server.json \
        --out "/out/cfx-pg$pg.json"
    FACTS+=(--facts "$OUT/facts-pg$pg.json")
    CFX+=(--cfx "$OUT/cfx-pg$pg.json")
done

echo "== report"
args=(--config "$OUT/config.json" "${FACTS[@]}" "${CFX[@]}" --md "$OUT/report.md" --json "$OUT/report.json" --html "$OUT/report.html")
[ -n "$BASELINE" ] && args+=(--baseline "$(cd "$(dirname "$BASELINE")" && pwd)/$(basename "$BASELINE")")
[ -n "$WRITE_BASELINE" ] && args+=(--write-baseline "$(cd "$(dirname "$WRITE_BASELINE")" && pwd)/$(basename "$WRITE_BASELINE")")
status=0
py "$TOOL/report.py" "${args[@]}" || status=$?
exit "$status"
