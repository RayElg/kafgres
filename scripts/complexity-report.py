#!/usr/bin/env python3
"""Turn rust-code-analysis JSON into a per-function complexity catalog.

    rust-code-analysis-cli -m -O json -o <outdir> -p <srcdir>
    scripts/complexity-report.py <outdir> <srcroot> > report.md

Skips test code: functions inside `#[cfg(test)]`/`#[cfg(any(test, ...))]` modules and
`#[test]`/`#[pg_test]` functions. Generated codec is kept but rolled up separately.
"""
import json
import os
import re
import sys
from collections import defaultdict

TEST_ATTR = re.compile(r"^\s*#\[cfg\((test|any\(test)")
TEST_FN = re.compile(r"^\s*#\[(pg_)?test")


def test_ranges(path):
    """Line ranges (1-based, inclusive) of test-gated `mod` blocks and test fns."""
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()
    ranges = []
    i = 0
    while i < len(lines):
        if TEST_ATTR.match(lines[i]) or TEST_FN.match(lines[i]):
            # Walk to the first `{` after the attribute, then brace-match.
            depth = 0
            j = i
            started = False
            while j < len(lines):
                for ch in lines[j]:
                    if ch == "{":
                        depth += 1
                        started = True
                    elif ch == "}":
                        depth -= 1
                if started and depth <= 0:
                    break
                # `#[cfg(test)] use foo;`: an item with no body, stop at the `;`.
                if not started and lines[j].rstrip().endswith(";"):
                    break
                j += 1
            ranges.append((i + 1, j + 1))
            i = j + 1
        else:
            i += 1
    return ranges


def in_ranges(line, ranges):
    return any(a <= line <= b for a, b in ranges)


def walk(space, out, path, ranges, parents=(), root=False):
    kind = space.get("kind")
    name = space.get("name") or "<anon>"
    # The root node is usually `unit` but can be tagged `function` (e.g. lib.rs); not a fn.
    if kind == "function" and not root:
        start = space["start_line"]
        if not in_ranges(start, ranges):
            m = space["metrics"]
            out.append(
                {
                    "file": path,
                    "fn": "::".join(p for p in parents if p) + ("::" if parents else "") + name,
                    "line": start,
                    "end": space["end_line"],
                    "sloc": int(m["loc"]["sloc"]),
                    "cyclomatic": int(m["cyclomatic"]["sum"]),
                    "cognitive": int(m["cognitive"]["sum"]),
                    "nargs": int(m["nargs"]["total"]),
                    "nexits": int(m["nexits"]["sum"]),
                }
            )
    next_parents = parents + ((name,) if kind in ("impl", "trait", "function") else ())
    for child in space.get("spaces", []):
        walk(child, out, path, ranges, next_parents)


def main(outdir, srcroot):
    rows = []
    for dirpath, _, files in os.walk(outdir):
        for fn in files:
            if not fn.endswith(".json") or fn == "functions.json":
                continue
            with open(os.path.join(dirpath, fn), encoding="utf-8") as f:
                doc = json.load(f)
            src = doc["name"]
            rel = os.path.relpath(src, srcroot)
            walk(doc, rows, rel, test_ranges(src), root=True)

    rows.sort(key=lambda r: (-r["cyclomatic"], r["file"], r["line"]))

    def bucket(r):
        if "/generated/" in r["file"]:
            return "codec (generated)"
        if r["file"].startswith("codec/"):
            return "codec (hand-written)"
        return "extension"

    per_bucket = defaultdict(lambda: {"fns": 0, "cyc": 0, "cog": 0, "sloc": 0, "hot": 0})
    per_file = defaultdict(lambda: {"fns": 0, "cyc": 0, "cog": 0, "sloc": 0, "max": 0})
    for r in rows:
        b = per_bucket[bucket(r)]
        b["fns"] += 1
        b["cyc"] += r["cyclomatic"]
        b["cog"] += r["cognitive"]
        b["sloc"] += r["sloc"]
        b["hot"] += r["cyclomatic"] > 15
        f = per_file[r["file"]]
        f["fns"] += 1
        f["cyc"] += r["cyclomatic"]
        f["cog"] += r["cognitive"]
        f["sloc"] += r["sloc"]
        f["max"] = max(f["max"], r["cyclomatic"])

    print("# Cyclomatic footprint (non-test code)\n")
    print("Tool: rust-code-analysis-cli (Mozilla). Cyclomatic = McCabe per function;")
    print("cognitive = SonarSource cognitive complexity. Test modules and `#[test]`/`#[pg_test]`")
    print("functions are excluded by `scripts/complexity-report.py`.\n")
    print("## Per crate\n")
    print("| bucket | functions | Σ cyclomatic | Σ cognitive | SLOC | fns with cyclomatic > 15 |")
    print("|---|---:|---:|---:|---:|---:|")
    for k in sorted(per_bucket):
        b = per_bucket[k]
        print(f"| {k} | {b['fns']} | {b['cyc']} | {b['cog']} | {b['sloc']} | {b['hot']} |")

    print("\n## Per file (hand-written only, by Σ cyclomatic)\n")
    print("| file | functions | Σ cyclomatic | Σ cognitive | SLOC | max cyclomatic |")
    print("|---|---:|---:|---:|---:|---:|")
    for k, f in sorted(per_file.items(), key=lambda kv: -kv[1]["cyc"]):
        if "/generated/" in k:
            continue
        print(f"| {k} | {f['fns']} | {f['cyc']} | {f['cog']} | {f['sloc']} | {f['max']} |")

    print("\n## Functions with cyclomatic ≥ 10 (hand-written)\n")
    print("| cyclomatic | cognitive | SLOC | function | location |")
    print("|---:|---:|---:|---|---|")
    for r in rows:
        if "/generated/" in r["file"] or r["cyclomatic"] < 10:
            continue
        print(f"| {r['cyclomatic']} | {r['cognitive']} | {r['sloc']} | `{r['fn']}` | `{r['file']}:{r['line']}` |")

    print("\n## Generated codec: top 15 by cyclomatic\n")
    print("| cyclomatic | cognitive | SLOC | function | location |")
    print("|---:|---:|---:|---|---|")
    n = 0
    for r in rows:
        if "/generated/" not in r["file"]:
            continue
        print(f"| {r['cyclomatic']} | {r['cognitive']} | {r['sloc']} | `{r['fn']}` | `{r['file']}:{r['line']}` |")
        n += 1
        if n >= 15:
            break

    with open(os.path.join(outdir, "functions.json"), "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=1)
    print(f"\nFull per-function table: `{len(rows)}` functions in `functions.json` beside the raw output.")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
