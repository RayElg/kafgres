#!/usr/bin/env bash

set -uo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
SRC="$ROOT/extension/src"

[ -d "$SRC" ] || exit 0

if command -v rg >/dev/null 2>&1; then
    raw_scan() { rg -n --no-heading -g '*.rs' "$1" "$SRC" 2>/dev/null; }
else
    raw_scan() { grep -rn --include='*.rs' -E "$1" "$SRC" 2>/dev/null; }
fi

drop_comment_lines() {
    awk '{ text = $0; sub(/^[^:]*:[0-9]+:/, "", text)
           if (text !~ /^[[:space:]]*\/\//) print $0 }'
}

scan() { raw_scan "$1" | drop_comment_lines; }

violations=""

sql_hits=$(scan 'kafgres_log' | grep -v -E '/storage/table\.rs:|/init0[0-9]+\.rs:|/tests?/')
if [ -n "$sql_hits" ]; then
    violations+="SQL against kafgres_log outside storage/table.rs:
$sql_hits

"
fi

FS_PATTERN='std::fs|OpenOptions|File::(open|create)'
FS_PATTERN+='|PathNameOpenFile|FileWrite|FileRead|FileSync|FileTruncate|FileClose'
fs_hits=$(scan "$FS_PATTERN" | grep -v -E '/storage/segment(\.rs|/)|extension/src/tls\.rs:|/tests?/')
if [ -n "$fs_hits" ]; then
    violations+="File I/O outside the storage/segment engine:
$fs_hits

"
fi

engine_hits=$(scan '(TableStore|SegmentStore)::new\(\)' \
    | grep -v -E '/storage/mod\.rs:|/storage/table\.rs:|/storage/segment(\.rs|/)|/tests?/')
if [ -n "$engine_hits" ]; then
    violations+="A concrete engine constructed outside storage::open():
$engine_hits

"
fi

if [ -n "$violations" ]; then
    cat >&2 <<EOF
BOUNDARY VIOLATION: protocol handlers must not touch log storage directly

$violations
Every read or write of log data goes through the LogStore trait. Protocol
handlers own no storage knowledge. The boundary is what keeps both storage
engines behind one interface; a single direct access welds the protocol
layer to one engine and breaks the other one silently.

Move this behind a LogStore method, or if the file is a legitimate new
exemption, add it to scripts/check-boundary.sh and say why in the commit.
EOF
    exit 2
fi

exit 0
