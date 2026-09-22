#!/usr/bin/env python3
"""Summarise `cargo clippy --message-format=json` output as a markdown table.

    scripts/clippy-summary.py clippy-a.json [clippy-b.json ...] > summary.md

Counts diagnostics per lint and per file, hand-written vs generated codec.
"""
import json
import sys
from collections import Counter, defaultdict


def load(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("reason") != "compiler-message":
                continue
            d = msg["message"]
            code = (d.get("code") or {}).get("code")
            if not code or not code.startswith("clippy::"):
                continue
            spans = [s for s in d.get("spans", []) if s.get("is_primary")] or d.get("spans", [])
            if not spans:
                continue
            s = spans[0]
            # Extension paths come out as `src/...`; codec paths as `codec/src/...`.
            fn = s["file_name"]
            if fn.startswith("src/"):
                fn = "extension/" + fn
            yield code, fn, s["line_start"], d["message"]


def main(paths):
    rows = []
    for p in paths:
        rows.extend(load(p))
    # Dedup: a span can appear twice when a crate is both a lib and a dependency.
    rows = sorted(set(rows))

    by_lint = Counter()
    by_lint_gen = Counter()
    by_file = defaultdict(Counter)
    for code, fn, _, _ in rows:
        if "/generated/" in fn:
            by_lint_gen[code] += 1
        else:
            by_lint[code] += 1
            by_file[fn][code] += 1

    print("# Clippy restriction-lint summary (non-test code)\n")
    print(f"{len(rows)} diagnostics total; {sum(by_lint.values())} in hand-written code, "
          f"{sum(by_lint_gen.values())} in the generated codec.\n")
    print("## Per lint (hand-written)\n")
    print("| lint | count | generated codec |")
    print("|---|---:|---:|")
    for code, n in by_lint.most_common():
        print(f"| `{code}` | {n} | {by_lint_gen.get(code, 0)} |")
    for code, n in by_lint_gen.most_common():
        if code not in by_lint:
            print(f"| `{code}` | 0 | {n} |")

    panic_lints = {
        "clippy::unwrap_used", "clippy::expect_used", "clippy::panic", "clippy::unreachable",
        "clippy::indexing_slicing", "clippy::string_slice", "clippy::arithmetic_side_effects",
        "clippy::unchecked_duration_subtraction", "clippy::todo", "clippy::unimplemented",
    }
    print("\n## Per file (hand-written), panic-class lints only\n")
    print("| file | unwrap/expect | panic/unreachable | index/slice | arithmetic | total |")
    print("|---|---:|---:|---:|---:|---:|")
    for fn, c in sorted(by_file.items(), key=lambda kv: -sum(v for k, v in kv[1].items() if k in panic_lints)):
        ue = c["clippy::unwrap_used"] + c["clippy::expect_used"]
        pu = c["clippy::panic"] + c["clippy::unreachable"] + c["clippy::todo"] + c["clippy::unimplemented"]
        ix = c["clippy::indexing_slicing"] + c["clippy::string_slice"]
        ar = c["clippy::arithmetic_side_effects"] + c["clippy::unchecked_duration_subtraction"]
        tot = ue + pu + ix + ar
        if tot == 0:
            continue
        print(f"| `{fn}` | {ue} | {pu} | {ix} | {ar} | {tot} |")

    print("\n## Every panic-class site (hand-written)\n")
    print("| lint | location | message |")
    print("|---|---|---|")
    for code, fn, line, msg in rows:
        if code in panic_lints and "/generated/" not in fn:
            print(f"| `{code.removeprefix('clippy::')}` | `{fn}:{line}` | {msg} |")


if __name__ == "__main__":
    main(sys.argv[1:])
