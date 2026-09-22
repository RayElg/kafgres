#!/usr/bin/env bash
# Static analysis over the extension and codec crates, non-test code only.
#
#   scripts/static-analysis.sh [outdir]   default: docs/static-analysis/raw
#
# Outputs: complexity.md, rca/ (raw JSON), clippy-extension.json, clippy-codec.json,
# clippy-summary.md, geiger.txt, unsafe-sites.txt.
#
# The extension crate needs a Postgres install, so clippy and geiger run in the
# analysis image; complexity analysis runs on the host.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
OUT="${1:-docs/static-analysis/raw}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

# Panic-hunting lint set. `--lib` excludes `#[cfg(test)]`; pg_test stays off to also
# exclude `#[cfg(any(test, feature = "pg_test"))]`.
LINTS=(
    -W clippy::all
    -W clippy::unwrap_used -W clippy::expect_used -W clippy::panic -W clippy::unreachable
    -W clippy::todo -W clippy::unimplemented
    -W clippy::indexing_slicing -W clippy::string_slice
    -W clippy::arithmetic_side_effects -W clippy::unchecked_duration_subtraction
    -W clippy::cast_possible_truncation -W clippy::cast_possible_wrap -W clippy::cast_sign_loss
    -W clippy::cast_lossless
    -W clippy::undocumented_unsafe_blocks -W clippy::multiple_unsafe_ops_per_block
    -W clippy::missing_safety_doc -W clippy::missing_panics_doc
    -W clippy::exit -W clippy::mem_forget -W clippy::large_stack_arrays
    -W clippy::ptr_as_ptr -W clippy::transmute_ptr_to_ptr
)

echo "== complexity (rust-code-analysis-cli)"
command -v rust-code-analysis-cli >/dev/null || cargo install --locked rust-code-analysis-cli
rm -rf "$OUT/rca" && mkdir -p "$OUT/rca"
rust-code-analysis-cli -m -O json -o "$OUT/rca" -p extension/src -p codec/src
python3 scripts/complexity-report.py "$OUT/rca" "$ROOT" > "$OUT/complexity.md"

echo "== unsafe sites"
grep -rn -B1 -A2 'unsafe' extension/src codec/src --include='*.rs' \
    | grep -v '/generated/' > "$OUT/unsafe-sites.txt" || true

echo "== clippy (codec, host)"
( cd codec && cargo clippy --lib --message-format=json -- "${LINTS[@]}" ) \
    > "$OUT/clippy-codec.json" || true

echo "== analysis image"
IMG_DIR="$(mktemp -d)"
cat > "$IMG_DIR/Dockerfile" <<'EOF'
FROM kafgres-pgrx-test
USER root
RUN rustup component add clippy \
 && cargo install --locked cargo-geiger 2>&1 | tail -2 \
 && chown -R 1000 /usr/local/cargo
USER tester
EOF
docker image inspect kafgres-pgrx-test >/dev/null 2>&1 || {
    docker build -q -f docker/Dockerfile --target builder -t kafgres-builder . >/dev/null
    docker build -q -f docker/Dockerfile.pgrx-test -t kafgres-pgrx-test docker >/dev/null
}
docker build -q -t kafgres-analysis "$IMG_DIR" >/dev/null
rm -rf "$IMG_DIR"

run_in_image() {
    docker run --rm \
        -v "$ROOT":/src -w /src/extension \
        -v kafgres-pgrx-cache:/cache -e CARGO_TARGET_DIR=/cache/target \
        -e USER=tester kafgres-analysis "$@"
}

echo "== clippy (extension, in image)"
run_in_image cargo clippy --lib --message-format=json -- "${LINTS[@]}" \
    > "$OUT/clippy-extension.json" || true

echo "== cargo-geiger (in image)"
run_in_image cargo geiger --lib --output-format Ascii > "$OUT/geiger.txt" 2>&1 || true

echo "== clippy summary"
python3 scripts/clippy-summary.py "$OUT/clippy-extension.json" "$OUT/clippy-codec.json" \
    > "$OUT/clippy-summary.md"

echo "done: $OUT"
