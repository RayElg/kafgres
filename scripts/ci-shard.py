#!/usr/bin/env python3
"""Print the test files for one CI shard, balanced by past run time.

    scripts/ci-shard.py <segment|table> <index> <count>

Whole files only, since tests in one share fixtures and order. A file missing from
tests/ci_durations.json weighs the median, so every file lands in some shard.
"""
import glob
import json
import os
import statistics
import sys

engine, index, count = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
os.chdir(root)

with open("tests/ci_durations.json", encoding="utf-8") as f:
    known = json.load(f)[engine]
files = sorted(glob.glob("tests/integration/test_*.py")) + sorted(
    glob.glob("tests/conformance/test_*.py")
)
default = statistics.median(known.values()) if known else 1

# Longest first into the lightest shard.
loads = [0.0] * count
shards = [[] for _ in range(count)]
for path in sorted(files, key=lambda p: -known.get(p, default)):
    i = loads.index(min(loads))
    loads[i] += known.get(path, default)
    shards[i].append(path)

mine = set(shards[index])
print(" ".join(p for p in files if p in mine))
print(f"shard {index + 1}/{count}: {len(mine)} files, ~{loads[index] / 60:.0f} min", file=sys.stderr)
