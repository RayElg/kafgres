#!/usr/bin/env bash
# Compile the extension against each PostgreSQL version it has a feature for.
#
#   scripts/ci/pg-matrix-check.sh              all of 13 14 15 16 17 18
#   scripts/ci/pg-matrix-check.sh 17 18        just those
#
# `cargo check --lib --tests`, one version at a time, logs in target/pg-matrix/. Compile
# only, no link or load: a clean check is necessary, not sufficient.
set -uo pipefail
cd "$(dirname "$0")/../.."

VERSIONS="${*:-13 14 15 16 17 18}"
LOGS=target/pg-matrix
mkdir -p "$LOGS"

docker build -q -f docker/Dockerfile.pg-matrix -t kafgres-pg-matrix docker >/dev/null || {
    echo "could not build the pg-matrix image" >&2
    exit 2
}

failed=""
for v in $VERSIONS; do
    printf 'pg%s: ' "$v"
    if docker run --rm \
        -v "$PWD":/src -w /src/extension \
        -v kafgres-pg-matrix-cache:/cache -e CARGO_TARGET_DIR=/cache/target \
        kafgres-pg-matrix \
        cargo check --lib --tests --no-default-features --features "pg$v" \
        > "$LOGS/pg$v.log" 2>&1; then
        echo ok
    else
        echo "FAILED ($(grep -c '^error' "$LOGS/pg$v.log") errors, $LOGS/pg$v.log)"
        failed="$failed pg$v"
    fi
done

[ -z "$failed" ] || { echo "failed:$failed"; exit 1; }
