#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

FILTER="${1:-}"

CACHE_VOLUME=kafgres-pgrx-cache

cargo test -p kafgres-codec ${FILTER:+"$FILTER"}

docker build -q -f docker/Dockerfile --target builder -t kafgres-builder . >/dev/null
docker build -q -f docker/Dockerfile.pgrx-test -t kafgres-pgrx-test docker >/dev/null

exec docker run --rm \
    -v "$PWD":/src -w /src/extension \
    `# A named volume, not the container layer: cargo would rebuild the whole dependency` \
    `# tree per run, and image builds leave target/ root-owned while tests run as uid 1000.` \
    -v "$CACHE_VOLUME":/cache \
    -e CARGO_TARGET_DIR=/cache/target \
    `# pgrx-tests reads $USER to choose the role it connects as; the container sets none.` \
    -e USER=tester \
    `# A pgrx test binary links the extension, so it needs Postgres's symbols, which exist` \
    `# only inside a backend; pgrx's template supplies the flag for macOS and nothing for` \
    `# Linux. Scoped to this command: in .cargo/config.toml it would defeat the link check` \
    `# on the extension build too.` \
    -e RUSTFLAGS="-Clink-arg=-Wl,--unresolved-symbols=ignore-all" \
    kafgres-pgrx-test \
    sh -c 'mkdir -p "$CARGO_TARGET_DIR/test-pgdata" && cargo pgrx test pg16 '"${FILTER:+$FILTER}"
