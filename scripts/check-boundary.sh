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

# Every `extern "C"` function Postgres calls must carry `#[pg_guard]`: an unguarded panic
# (including a Postgres ERROR, which pgrx re-raises as a panic) unwinds into C frames that
# cannot catch it, aborts the process, and crash-recovers the whole cluster.
#
# `#[pg_extern]` functions are exempt: pgrx generates and guards their wrapper.
unguarded=$(
    find "$SRC" -name '*.rs' -not -path '*/tests/*' -print0 \
        | xargs -0 awk '
        FNR == 1 { guarded = 0; pending = 0 }
        # An attribute counts only as the attribute itself, not a mention in a comment.
        /^[[:space:]]*#\[/ {
            a = $0; sub(/\/\/.*/, "", a)
            if (a ~ /#\[[[:space:]]*(pgrx::)?pg_guard[[:space:]]*\]/) guarded = 1
            next
        }
        /^[[:space:]]*(\/\/|\/\*|\*)/ { next }
        /^[[:space:]]*$/ { next }
        {
            line = $0; sub(/\/\/.*/, "", line)
            # `extern "C"`, `extern "C-unwind"` or bare `extern`, then `fn` and a name on this
            # line or the next; `extern "C" fn(` is a pointer type.
            item = line ~ /(^|[^[:alnum:]_])extern([[:space:]]+"C[^"]*")?[[:space:]]+fn[[:space:]]+[[:alpha:]_]/
            if (!item && pending && line ~ /^[[:space:]]*fn[[:space:]]+[[:alpha:]_]/) item = 1
            if (item && !guarded)
                printf "%s:%d: %s\n", FILENAME, FNR, $0
            pending = line ~ /(^|[^[:alnum:]_])extern([[:space:]]+"C[^"]*")?[[:space:]]*$/
            if (!pending) guarded = 0
        }
    '
)
failed=0

if [ -n "$unguarded" ]; then
    cat >&2 <<EOF
GUARD VIOLATION: an \`extern "C"\` function without #[pg_guard]

$unguarded
Postgres calls these from its own C frames, which cannot catch a Rust unwind.
A panic there — or a Postgres ERROR, which pgrx converts to a panic at every
pg_sys call — aborts the process, and the postmaster answers a child dying by
signal with crash recovery for the whole cluster. Every database client is
disconnected, not just Kafka clients.

Add #[pg_guard]. If this function genuinely must not have one, exempt it in
scripts/check-boundary.sh and justify it in the commit message.
EOF
    failed=1
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
    failed=1
fi


[ "$failed" = "0" ] || exit 2
exit 0
