#!/usr/bin/env bash

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TAG="${1:-$(tr -d '[:space:]' < "$HERE/KAFKA_VERSION")}"
SRC='clients/src/main/resources/common/message'
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "Vendoring Kafka message schemas at tag $TAG"

git clone --quiet --filter=blob:none --no-checkout --depth 1 \
    --branch "$TAG" https://github.com/apache/kafka.git "$WORK/kafka"

git -C "$WORK/kafka" sparse-checkout set --no-cone "/$SRC/"
git -C "$WORK/kafka" checkout --quiet
COMMIT="$(git -C "$WORK/kafka" rev-parse HEAD)"

find "$HERE/schemas" -name '*.json' -delete
mkdir -p "$HERE/schemas"
cp "$WORK/kafka/$SRC"/*.json "$HERE/schemas/"
cp "$WORK/kafka/$SRC/README.md" "$HERE/schemas/UPSTREAM-README.md"

printf '%s\n' "$TAG" > "$HERE/KAFKA_VERSION"

echo "  tag     $TAG"
echo "  commit  $COMMIT"
echo "  files   $(find "$HERE/schemas" -name '*.json' | wc -l) json"
echo
echo "Update the commit hash in codec/VENDORING.md, then diff schemas/ and read"
echo "every validVersions / flexibleVersions change before committing."
